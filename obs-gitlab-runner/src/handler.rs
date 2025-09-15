use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    io::Seek,
};

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand};
use color_eyre::eyre::{Context, Report, Result, eyre};
use derivative::*;
use futures_util::StreamExt;
use gitlab_runner::{
    JobHandler, JobResult, Phase, UploadableFile,
    job::{Dependency, Job, Variable},
};
use obo_core::{
    actions::{
        Actions, DEFAULT_BUILD_INFO, DEFAULT_BUILD_LOG, DownloadBinariesAction, DputAction,
        FailedBuild, FlagSupportingExplicitValue, LOG_TAIL_2MB, MonitorAction, ObsBuildInfo,
        PruneAction,
    },
    artifacts::{ArtifactDirectory, ArtifactReader, ArtifactWriter, MissingArtifact, SaveCallback},
    monitor::PackageMonitoringOptions,
    outputln,
};
use open_build_service_api as obs;
use tokio::fs::File as AsyncFile;
use tokio_util::{
    compat::{Compat, TokioAsyncReadCompatExt},
    io::ReaderStream,
};
use tracing::{error, instrument, warn};

use crate::pipeline::{
    GeneratePipelineOptions, PipelineDownloadBinaries, generate_monitor_pipeline,
};

const DEFAULT_MONITOR_PIPELINE: &str = "obs.yml";
const DEFAULT_PIPELINE_JOB_PREFIX: &str = "obs";
const DEFAULT_ARTIFACT_EXPIRATION: &str = "3 days";

#[derive(Parser, Debug)]
struct GenerateMonitorAction {
    tag: String,
    #[clap(long)]
    rules: Option<String>,
    #[clap(long = "download-build-results-to")]
    build_results_dir: Option<Utf8PathBuf>,
    #[clap(long, default_value_t = DEFAULT_BUILD_INFO.to_owned())]
    build_info: String,
    #[clap(long, default_value_t = DEFAULT_MONITOR_PIPELINE.to_owned())]
    pipeline_out: String,
    #[clap(long, default_value_t = DEFAULT_PIPELINE_JOB_PREFIX.to_owned())]
    job_prefix: String,
    #[clap(long)]
    job_timeout: Option<String>,
    #[clap(long, default_value_t = DEFAULT_ARTIFACT_EXPIRATION.to_owned())]
    artifact_expiration: String,
    #[clap(long, default_value_t = DEFAULT_BUILD_LOG.into())]
    build_log_out: String,
}

#[derive(Parser, Debug)]
struct EchoAction {
    args: Vec<String>,
    #[clap(long, flag_supporting_explicit_value())]
    fail: bool,
    #[clap(long, flag_supporting_explicit_value())]
    uppercase: bool,
    #[clap(long, default_value = " ")]
    sep: String,
}

#[derive(Subcommand)]
enum CommonAction {
    Dput(DputAction),
    Monitor(MonitorAction),
    DownloadBinaries(DownloadBinariesAction),
    Prune {
        #[clap(long, flag_supporting_explicit_value())]
        only_if_job_unsuccessful: bool,
        #[clap(flatten)]
        args: PruneAction,
    },
}

#[derive(Subcommand)]
enum JobAction {
    #[clap(flatten)]
    Common(CommonAction),
    GenerateMonitor(GenerateMonitorAction),
    #[cfg(test)]
    Echo(EchoAction),
}

#[derive(Parser)]
#[clap(bin_name = "obs-gitlab-runner")]
#[clap(no_binary_name = true)]
struct Command {
    #[clap(subcommand)]
    action: JobAction,
}

fn get_job_variable<'job>(job: &'job Job, key: &str) -> Result<Variable<'job>> {
    job.variable(key)
        .ok_or_else(|| eyre!("Failed to get variable ${}", key))
}

#[derive(Debug, Derivative)]
#[derivative(Default)]
pub struct HandlerOptions {
    pub monitor: PackageMonitoringOptions,
    pub default_monitor_job_timeout: Option<String>,
    #[derivative(Default(value = "LOG_TAIL_2MB"))]
    pub log_tail: u64,
}

struct GitLabArtifacts<'a> {
    job: &'a Job,
    artifacts: &'a mut HashMap<Utf8PathBuf, ArtifactReader>,
}

#[async_trait]
impl ArtifactDirectory for GitLabArtifacts<'_> {
    #[instrument(skip(self, path), path = path.as_ref())]
    async fn open(&self, path: impl AsRef<Utf8Path> + Send) -> Result<ArtifactReader> {
        let path = path.as_ref();

        if let Some(file) = self.artifacts.get(path) {
            let file = file
                .try_clone()
                .await
                .wrap_err("Failed to reopen artifact")?;
            return Ok(file);
        }

        for dep in self.job.dependencies() {
            if let Some(file) = check_for_artifact(dep, path).await? {
                return Ok(file);
            }
        }

        Err(MissingArtifact(path.to_owned()).into())
    }

    #[tracing::instrument(skip(self, path, func), path = path.as_ref())]
    async fn save_with<Ret, Err, F, P>(&mut self, path: P, func: F) -> Result<Ret>
    where
        Report: From<Err>,
        Ret: Send,
        Err: Send,
        F: for<'a> SaveCallback<'a, Ret, Err> + Send,
        P: AsRef<Utf8Path> + Send,
    {
        let mut writer = ArtifactWriter::new_anon().await?;
        let ret = func(&mut writer).await?;
        self.artifacts
            .insert(path.as_ref().to_owned(), writer.into_reader().await?);
        Ok(ret)
    }
}

pub struct ObsJobHandler {
    job: Job,
    options: HandlerOptions,

    actions: Actions,
    script_failed: bool,
    artifacts: HashMap<Utf8PathBuf, ArtifactReader>,
}

impl ObsJobHandler {
    pub fn new(job: Job, client: obs::Client, options: HandlerOptions) -> Self {
        ObsJobHandler {
            job,
            options,
            actions: Actions { client },
            script_failed: false,
            artifacts: HashMap::new(),
        }
    }

    #[instrument(skip_all, fields(job = job.id()))]
    pub fn from_obs_config_in_job(job: Job, options: HandlerOptions) -> Result<Self> {
        let obs_server = get_job_variable(&job, "OBS_SERVER")?;
        let obs_user = get_job_variable(&job, "OBS_USER")?;
        let obs_password = get_job_variable(&job, "OBS_PASSWORD")?;

        let client = obs::Client::new(
            obs_server.value().try_into().wrap_err("Invalid URL")?,
            obs_user.value().to_owned(),
            obs_password.value().to_owned(),
        );
        Ok(ObsJobHandler::new(job, client, options))
    }

    fn expand_vars<'s>(
        &self,
        s: &'s str,
        quote: bool,
        expanding: &mut HashSet<String>,
    ) -> Cow<'s, str> {
        shellexpand::env_with_context_no_errors(s, |var| {
            if !expanding.insert(var.to_owned()) {
                return Some("".to_string());
            }

            let value = self.job.variable(var).map_or("", |v| v.value());
            let expanded = self.expand_vars(value, false, expanding);
            expanding.remove(var);
            Some(
                if quote {
                    shell_words::quote(expanded.as_ref())
                } else {
                    expanded
                }
                .into_owned(),
            )
        })
    }

    #[instrument(skip(self))]
    async fn generate_monitor(&mut self, args: GenerateMonitorAction) -> Result<()> {
        let mut artifacts = GitLabArtifacts {
            job: &self.job,
            artifacts: &mut self.artifacts,
        };

        let build_info_data = artifacts.read_string(&args.build_info).await?;
        let build_info: ObsBuildInfo = serde_json::from_str(&build_info_data)
            .wrap_err("Failed to parse provided build info file")?;

        let pipeline = generate_monitor_pipeline(
            &build_info.project,
            &build_info.package,
            &build_info
                .rev
                .ok_or_else(|| eyre!("Build revision was not set"))?,
            &build_info
                .srcmd5
                .ok_or_else(|| eyre!("Build srcmd5 was not set"))?,
            &build_info.enabled_repos,
            GeneratePipelineOptions {
                tags: vec![args.tag],
                artifact_expiration: args.artifact_expiration,
                prefix: args.job_prefix,
                timeout: args
                    .job_timeout
                    .or_else(|| self.options.default_monitor_job_timeout.clone()),
                rules: args.rules,
                download_binaries: if let Some(build_results_dir) = args.build_results_dir {
                    PipelineDownloadBinaries::OnSuccess {
                        build_results_dir: build_results_dir.into_string(),
                    }
                } else {
                    PipelineDownloadBinaries::Never
                },
                build_log_out: args.build_log_out.to_string(),
            },
        )?;

        artifacts
            .write(&args.pipeline_out, pipeline.as_bytes())
            .await?;
        outputln!("Wrote pipeline file '{}'.", args.pipeline_out);

        Ok(())
    }

    #[instrument(skip(self))]
    async fn command(&mut self, cmdline: &str) -> Result<()> {
        // TODO: inject user?
        let cmdline = self.expand_vars(cmdline, true, &mut HashSet::new());

        outputln!("> {}", cmdline);

        let args = shell_words::split(&cmdline).wrap_err("Invalid command line")?;
        let command = Command::try_parse_from(args)?;

        match command.action {
            JobAction::Common(action) => {
                let mut artifacts = GitLabArtifacts {
                    job: &self.job,
                    artifacts: &mut self.artifacts,
                };

                match action {
                    CommonAction::Dput(args) => self.actions.dput(args, &mut artifacts).await?,
                    CommonAction::Monitor(args) => {
                        self.actions
                            .monitor(
                                args,
                                self.options.monitor.clone(),
                                async |file| {
                                    let mut log_stream = ReaderStream::new(file);
                                    while let Some(bytes) = log_stream.next().await {
                                        let bytes = bytes.wrap_err("Failed to stream log bytes")?;
                                        self.job.trace(String::from_utf8_lossy(&bytes).as_ref());
                                    }
                                    Ok(())
                                },
                                self.options.log_tail,
                                &mut artifacts,
                            )
                            .await?
                    }
                    CommonAction::DownloadBinaries(args) => {
                        self.actions.download_binaries(args, &mut artifacts).await?
                    }
                    CommonAction::Prune {
                        only_if_job_unsuccessful: true,
                        ..
                    } if !self.script_failed => {
                        outputln!("Skipping prune: main script was successful.")
                    }
                    CommonAction::Prune { args, .. } => {
                        self.actions.prune(args, &artifacts).await?
                    }
                }
            }
            JobAction::GenerateMonitor(args) => self.generate_monitor(args).await?,
            #[cfg(test)]
            JobAction::Echo(args) => {
                use color_eyre::eyre::ensure;

                let mut output = args.args.join(&args.sep);
                if args.uppercase {
                    output = output.to_uppercase();
                }

                outputln!("{}", output);
                ensure!(!args.fail, "Failed");
            }
        }

        Ok(())
    }
}

pub struct UploadableArtifact {
    path: Utf8PathBuf,
    file: ArtifactReader,
}

#[async_trait]
impl UploadableFile for UploadableArtifact {
    type Data<'a> = Compat<ArtifactReader>;

    fn get_path(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.path.as_str())
    }

    async fn get_data(&self) -> Result<Self::Data<'_>, ()> {
        self.file
            .try_clone()
            .await
            .map(TokioAsyncReadCompatExt::compat)
            .map_err(|e| {
                warn!("Failed to clone {}: {e}", self.path);
            })
    }
}

#[async_trait]
impl JobHandler<UploadableArtifact> for ObsJobHandler {
    async fn step(&mut self, script: &[String], _phase: Phase) -> JobResult {
        for command in script {
            if let Err(err) = self.command(command).await {
                // Failed builds would already have information on them printed
                // above, so don't print anything on them again.
                if !err.is::<FailedBuild>() {
                    error!(gitlab.output = true, "Error running command: {:?}", err);
                }

                self.script_failed = true;
                return Err(());
            }
        }

        Ok(())
    }

    async fn get_uploadable_files(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = UploadableArtifact> + Send>, ()> {
        let mut files = vec![];
        for (path, file) in &mut self.artifacts {
            match file.try_clone().await {
                Ok(file) => files.push(UploadableArtifact {
                    path: path.clone(),
                    file,
                }),
                Err(err) => error!(
                    gitlab.output = true,
                    "Failed to prepare to upload {path}: {err:?}"
                ),
            }
        }

        Ok(Box::new(files.into_iter()))
    }
}

#[instrument(skip(dep), fields(dep_id = dep.id(), dep_name = dep.name()))]
async fn check_for_artifact(
    dep: Dependency<'_>,
    path: &Utf8Path,
) -> Result<Option<ArtifactReader>> {
    // This needs to be an owned type, because captured by spawn_blocking must
    // have a 'static lifetime, so we also take the opportunity to normalize the
    // path from extra trailing slashes and similar.
    let path = path.components().collect::<Utf8PathBuf>();

    // TODO: not spawn a sync environment for *every single artifact*
    if let Some(mut artifact) = dep.download().await? {
        if let Some(file) = tokio::task::spawn_blocking(move || {
            artifact
                .file(path.as_str())
                .map(|mut file| {
                    let mut temp = tempfile::tempfile()?;
                    std::io::copy(&mut file, &mut temp)?;
                    temp.rewind()?;
                    Ok::<_, Report>(temp)
                })
                .transpose()
        })
        .await??
        {
            return Ok(Some(
                ArtifactReader::from_async_file(&AsyncFile::from_std(file)).await?,
            ));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Cursor, Read},
        marker::PhantomData,
        sync::{Arc, Once},
        time::Duration,
    };

    use claims::*;
    use gitlab_runner::{GitlabLayer, Runner, RunnerBuilder};
    use gitlab_runner_mock::*;
    use obo_core::build_meta::{EnabledRepo, RepoArch};
    use obo_test_support::*;
    use obo_tests::*;
    use rstest::rstest;
    use tempfile::TempDir;
    use tracing::{Level, instrument::WithSubscriber};
    use tracing_subscriber::{Layer, Registry, filter::Targets, prelude::*};
    use zip::ZipArchive;

    use crate::logging::GitLabForwarder;

    use super::*;

    struct PutArtifactsHandler {
        artifacts: Arc<HashMap<String, Vec<u8>>>,
    }

    struct PutArtifactsFile {
        artifacts: Arc<HashMap<String, Vec<u8>>>,
        path: String,
    }

    #[async_trait]
    impl UploadableFile for PutArtifactsFile {
        type Data<'a> = Compat<Cursor<&'a Vec<u8>>>;

        fn get_path(&self) -> Cow<'_, str> {
            Cow::Borrowed(&self.path)
        }

        async fn get_data(&self) -> Result<Self::Data<'_>, ()> {
            Ok(Cursor::new(self.artifacts.get(&self.path).unwrap()).compat())
        }
    }

    #[async_trait]
    impl JobHandler<PutArtifactsFile> for PutArtifactsHandler {
        async fn step(&mut self, _script: &[String], _phase: Phase) -> JobResult {
            Ok(())
        }

        async fn get_uploadable_files(
            &mut self,
        ) -> Result<Box<dyn Iterator<Item = PutArtifactsFile> + Send>, ()> {
            Ok(Box::new(
                self.artifacts
                    .keys()
                    .map(|k| PutArtifactsFile {
                        artifacts: self.artifacts.clone(),
                        path: k.to_owned(),
                    })
                    .collect::<Vec<_>>()
                    .into_iter(),
            ))
        }
    }

    #[derive(Clone, Debug)]
    struct GitLabArtifactsHandle(MockJob);

    impl ArtifactsHandle for GitLabArtifactsHandle {}

    #[derive(Clone, Debug)]
    struct GitLabExecutionResult(MockJob);

    impl ExecutionResult for GitLabExecutionResult {
        type Artifacts = GitLabArtifactsHandle;

        fn ok(&self) -> bool {
            self.0.state() == MockJobState::Success
        }

        fn log(&self) -> String {
            String::from_utf8_lossy(&self.0.log()).into_owned()
        }

        fn artifacts(&self) -> Self::Artifacts {
            GitLabArtifactsHandle(self.0.clone())
        }
    }

    // All the GitLabRunHandlerWrapper* boilerplate is basically a trick to be
    // able to type-erase the generics of Runner::request_job(), so that you can
    // pass a job handler to the RunBuilder without having to then propagate the
    // handler's type everywhere. Ideally we could just use a lambda, but that
    // causes a variety of lifetime issues due to the various constraints around
    // async function types.
    #[async_trait]
    trait GitLabRunHandlerWrapper: Send + Sync {
        async fn request_job(&mut self, runner: &mut Runner);
    }

    struct GitLabRunHandlerWrapperImpl<
        U: UploadableFile + Send + Sync + 'static,
        H: JobHandler<U> + Send + Sync + 'static,
        Func: (FnOnce(Job) -> H) + Send + Sync + 'static,
    > {
        func: Option<Func>,
        _phantom: PhantomData<(U, H)>,
    }

    #[async_trait]
    impl<
        U: UploadableFile + Send + Sync + 'static,
        H: JobHandler<U> + Send + Sync + 'static,
        Func: (FnOnce(Job) -> H) + Send + Sync + 'static,
    > GitLabRunHandlerWrapper for GitLabRunHandlerWrapperImpl<U, H, Func>
    {
        async fn request_job(&mut self, runner: &mut Runner) {
            let handler = self
                .func
                .take()
                .expect("request_job can only be called once");
            let got_job = runner
                .request_job(move |job| futures_util::future::ready(Ok(handler(job))))
                .await
                .unwrap();
            assert!(got_job);
        }
    }

    struct GitLabRunBuilder<'context> {
        context: &'context mut GitLabTestContext,
        script: Vec<String>,
        after_script: Vec<String>,
        dependencies: Vec<MockJob>,
        variables: HashMap<String, String>,
        handler: Box<dyn GitLabRunHandlerWrapper>,
        timeout: Duration,
    }

    fn create_obs_job_handler_factory(
        options: HandlerOptions,
    ) -> impl FnOnce(Job) -> ObsJobHandler {
        move |job| assert_ok!(ObsJobHandler::from_obs_config_in_job(job, options))
    }

    impl GitLabRunBuilder<'_> {
        fn after_command(self, cmd: impl Into<String>) -> Self {
            self.after_script(&[cmd.into()])
        }

        fn after_script(mut self, script: &[String]) -> Self {
            self.after_script.extend_from_slice(script);
            self
        }

        fn variable(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
            self.variables.insert(name.into(), value.into());
            self
        }

        fn job_handler_factory<
            U: UploadableFile + Send + Sync + 'static,
            H: JobHandler<U> + Send + Sync + 'static,
            Func: (FnOnce(Job) -> H) + Send + Sync + 'static,
        >(
            mut self,
            handler_func: Func,
        ) -> Self {
            self.handler = Box::new(GitLabRunHandlerWrapperImpl {
                func: Some(handler_func),
                _phantom: PhantomData,
            });
            self
        }

        fn obs_job_handler(self, options: HandlerOptions) -> Self {
            self.job_handler_factory(create_obs_job_handler_factory(options))
        }
    }

    #[async_trait]
    impl<'context> RunBuilder<'context> for GitLabRunBuilder<'context> {
        type ArtifactsHandle = GitLabArtifactsHandle;
        type ExecutionResult = GitLabExecutionResult;

        fn script(mut self, script: &[String]) -> Self {
            self.script.extend_from_slice(script);
            self
        }

        fn artifacts(mut self, artifacts: Self::ArtifactsHandle) -> Self {
            self.dependencies.push(artifacts.0);
            self
        }

        fn timeout(mut self, timeout: Duration) -> Self {
            self.timeout = timeout;
            self
        }

        async fn go(mut self) -> Self::ExecutionResult {
            const JOB_TIMEOUT: u64 = 3600;

            let mut builder = self.context.gitlab_mock.job_builder("test".to_owned());

            builder.add_step(
                MockJobStepName::Script,
                self.script,
                JOB_TIMEOUT,
                MockJobStepWhen::OnSuccess,
                false,
            );

            if !self.after_script.is_empty() {
                builder.add_step(
                    MockJobStepName::AfterScript,
                    self.after_script,
                    JOB_TIMEOUT,
                    MockJobStepWhen::OnSuccess,
                    false,
                );
            }

            builder.add_artifact(
                None,
                false,
                vec!["*".to_owned()],
                Some(MockJobArtifactWhen::Always),
                "archive".to_owned(),
                Some("zip".to_owned()),
                None,
            );

            for dependency in self.dependencies {
                builder.dependency(dependency);
            }
            for (key, value) in self.variables {
                builder.add_variable(key, value, true, false);
            }

            builder.add_variable(
                "OBS_SERVER".to_owned(),
                self.context.obs.client.url().to_string(),
                false,
                true,
            );
            builder.add_variable("OBS_USER".to_owned(), TEST_USER.to_owned(), false, true);
            builder.add_variable("OBS_PASSWORD".to_owned(), TEST_PASS.to_owned(), false, true);

            let job = builder.build();
            self.context.gitlab_mock.enqueue_job(job.clone());

            self.handler.request_job(&mut self.context.runner).await;
            self.context.runner.wait_for_space(1).await;

            GitLabExecutionResult(job)
        }
    }

    const DEFAULT_HANDLER_OPTIONS: HandlerOptions = HandlerOptions {
        default_monitor_job_timeout: None,
        log_tail: MONITOR_TEST_LOG_TAIL,
        monitor: PackageMonitoringOptions {
            sleep_on_building: Duration::ZERO,
            sleep_on_old_status: MONITOR_TEST_OLD_STATUS_SLEEP_DURATION,
            // High limit, since we don't really test that
            // functionality in the handler tests.
            max_old_status_retries: 99,
        },
    };

    struct GitLabTestContext {
        _runner_dir: TempDir,
        gitlab_mock: GitlabRunnerMock,
        runner: Runner,
        obs: ObsContext,
    }

    #[async_trait]
    impl TestContext for GitLabTestContext {
        type ArtifactsHandle = GitLabArtifactsHandle;
        type ExecutionResult = GitLabExecutionResult;
        type RunBuilder<'context> = GitLabRunBuilder<'context>;

        fn obs(&self) -> &ObsContext {
            &self.obs
        }

        async fn inject_artifacts(
            &mut self,
            artifacts: HashMap<String, Vec<u8>>,
        ) -> Self::ArtifactsHandle {
            self.run()
                .job_handler_factory(|_| PutArtifactsHandler {
                    artifacts: Arc::new(artifacts),
                })
                .command("dummy")
                .go()
                .await
                .artifacts()
        }

        async fn fetch_artifacts(
            &self,
            handle: &Self::ArtifactsHandle,
        ) -> HashMap<String, Vec<u8>> {
            let Some(artifact) = handle.0.uploaded_artifacts().next() else {
                return Default::default();
            };

            let data = (*artifact.data).clone();
            assert!(!data.is_empty());

            let cursor = Cursor::new(data);
            let mut zip = ZipArchive::new(cursor).unwrap();

            (0..zip.len())
                .map(|i| {
                    let mut file = zip.by_index(i).unwrap();

                    let mut contents = vec![];
                    file.read_to_end(&mut contents).unwrap();

                    (file.name().to_owned(), contents)
                })
                .collect()
        }

        fn run(&mut self) -> Self::RunBuilder<'_> {
            GitLabRunBuilder {
                context: self,
                script: vec![],
                after_script: vec![],
                dependencies: vec![],
                variables: Default::default(),
                handler: Box::new(GitLabRunHandlerWrapperImpl {
                    func: Some(create_obs_job_handler_factory(DEFAULT_HANDLER_OPTIONS)),
                    _phantom: PhantomData,
                }),
                timeout: EXECUTION_DEFAULT_TIMEOUT,
            }
        }
    }

    static COLOR_EYRE_INSTALL: Once = Once::new();

    async fn with_context<T>(func: impl AsyncFnOnce(GitLabTestContext) -> T) -> T {
        COLOR_EYRE_INSTALL.call_once(|| color_eyre::install().unwrap());

        let runner_dir = tempfile::tempdir().unwrap();
        let gitlab_mock = GitlabRunnerMock::start().await;
        let (layer, jobs) = GitlabLayer::new();
        let runner = RunnerBuilder::new(
            gitlab_mock.uri(),
            gitlab_mock.runner_token().to_owned(),
            runner_dir.path().to_owned(),
            jobs,
        )
        .build()
        .await;

        let obs_mock = create_default_mock().await;
        let obs_client = create_default_client(&obs_mock);

        let ctx = GitLabTestContext {
            _runner_dir: runner_dir,
            gitlab_mock,
            runner,
            obs: ObsContext {
                client: obs_client,
                mock: obs_mock,
            },
        };

        func(ctx)
            .with_subscriber(
                Registry::default()
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_test_writer()
                            .with_filter(
                                Targets::new().with_target("obs_gitlab_runner", Level::TRACE),
                            ),
                    )
                    .with(tracing_error::ErrorLayer::default())
                    .with(GitLabForwarder::new(layer)),
            )
            .await
    }

    async fn test_monitoring_with_generation(
        context: &mut GitLabTestContext,
        dput: GitLabArtifactsHandle,
        build_info: &ObsBuildInfo,
        success: bool,
        dput_test: DputTest,
        log_test: MonitorLogTest,
        download_binaries: bool,
    ) {
        const TEST_JOB_RUNNER_TAG: &str = "test-tag";
        const TEST_MONITOR_TIMEOUT: &str = "1 day";

        let mut generate_command = format!(
            "generate-monitor {TEST_JOB_RUNNER_TAG} \
                --job-timeout '{TEST_MONITOR_TIMEOUT}' \
                --rules '[{{a: 1}}, {{b: 2}}]'"
        );
        if download_binaries {
            generate_command +=
                &format!(" --download-build-results-to {MONITOR_TEST_BUILD_RESULTS_DIR}");
        }

        let generate = context
            .run()
            .command(generate_command)
            .artifacts(dput.clone())
            .go()
            .await;
        assert!(generate.ok());

        let results = context.fetch_artifacts(&generate.artifacts()).await;
        let pipeline_yaml: serde_yaml::Value = assert_ok!(serde_yaml::from_slice(
            results.get(DEFAULT_MONITOR_PIPELINE).unwrap()
        ));
        let pipeline_map = pipeline_yaml.as_mapping().unwrap();

        assert_eq!(pipeline_map.len(), build_info.enabled_repos.len());

        for enabled in &build_info.enabled_repos {
            let monitor_job_name = format!(
                "{}-{}-{}",
                DEFAULT_PIPELINE_JOB_PREFIX, TEST_REPO, &enabled.repo_arch.arch
            );

            let monitor_map = pipeline_yaml
                .as_mapping()
                .unwrap()
                .get(monitor_job_name.as_str())
                .unwrap()
                .as_mapping()
                .unwrap();

            let artifacts = monitor_map.get("artifacts").unwrap().as_mapping().unwrap();
            assert_eq!(
                artifacts.get("expire_in").unwrap().as_str().unwrap(),
                DEFAULT_ARTIFACT_EXPIRATION
            );

            let mut artifact_paths: Vec<_> = artifacts
                .get("paths")
                .unwrap()
                .as_sequence()
                .unwrap()
                .iter()
                .map(|item| item.as_str().unwrap())
                .collect();
            artifact_paths.sort();

            if download_binaries {
                assert_eq!(
                    &artifact_paths,
                    &[DEFAULT_BUILD_LOG, MONITOR_TEST_BUILD_RESULTS_DIR]
                );
            } else {
                assert_eq!(&artifact_paths, &[DEFAULT_BUILD_LOG]);
            }

            let tags = monitor_map.get("tags").unwrap().as_sequence().unwrap();
            assert_eq!(tags.len(), 1);
            assert_eq!(tags[0].as_str().unwrap(), TEST_JOB_RUNNER_TAG);

            let timeout = monitor_map.get("timeout").unwrap().as_str().unwrap();
            assert_eq!(timeout, TEST_MONITOR_TIMEOUT);

            let rules: Vec<_> = monitor_map
                .get("rules")
                .unwrap()
                .as_sequence()
                .unwrap()
                .iter()
                .map(|v| v.as_mapping().unwrap())
                .collect();
            assert_eq!(rules.len(), 2);

            assert_eq!(rules[0].get("a").unwrap().as_i64().unwrap(), 1);
            assert_eq!(rules[1].get("b").unwrap().as_i64().unwrap(), 2);

            for script_key in ["before_script", "after_script"] {
                let script = monitor_map.get(script_key).unwrap().as_sequence().unwrap();
                assert_eq!(script.len(), 0);
            }

            let script = monitor_map
                .get("script")
                .unwrap()
                .as_sequence()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect::<Vec<_>>();

            test_monitoring(
                context,
                dput.clone(),
                build_info,
                &enabled.repo_arch,
                &script,
                success,
                dput_test,
                log_test,
                download_binaries,
            )
            .await;
        }
    }

    async fn test_prune(
        context: &mut GitLabTestContext,
        dput: GitLabArtifactsHandle,
        build_info: &ObsBuildInfo,
        only_if_job_unsuccessful: bool,
    ) {
        test_prune_missing_build_info(context).await;

        let prune = if only_if_job_unsuccessful {
            let prune = context
                .run()
                .command("echo")
                .after_command("prune --only-if-job-unsuccessful")
                .artifacts(dput.clone())
                .go()
                .await;

            assert!(prune.ok());
            assert!(prune.log().contains("Skipping prune"));

            assert_ok!(
                context
                    .obs()
                    .client
                    .project(build_info.project.clone())
                    .package(TEST_PACKAGE_1.to_owned())
                    .list(None)
                    .await
            );

            context
                .run()
                .command("echo --fail")
                .after_command("prune --only-if-job-unsuccessful")
                .artifacts(dput.clone())
                .go()
                .await
        } else {
            context
                .run()
                .command("prune")
                .artifacts(dput.clone())
                .go()
                .await
        };

        test_prune_deleted_package_1_if_branched(context, build_info, &prune).await;
    }

    #[rstest]
    #[tokio::test]
    async fn test_handler_flow(
        #[values(
            DputTest::Basic,
            DputTest::Rebuild,
            DputTest::ReusePreviousBuild,
            DputTest::Branch
        )]
        dput_test: DputTest,
        #[values(true, false)] build_success: bool,
        #[values(
            MonitorLogTest::Long,
            MonitorLogTest::Short,
            MonitorLogTest::Unavailable
        )]
        log_test: MonitorLogTest,
        #[values(true, false)] download_binaries: bool,
        #[values(true, false)] prune_only_if_job_unsuccessful: bool,
    ) {
        with_context(async |mut context| {
            let (dput, build_info) = test_dput(&mut context, dput_test).await;

            test_monitoring_with_generation(
                &mut context,
                dput.clone(),
                &build_info,
                build_success,
                dput_test,
                log_test,
                download_binaries,
            )
            .await;

            test_prune(
                &mut context,
                dput.clone(),
                &build_info,
                prune_only_if_job_unsuccessful,
            )
            .await;
        })
        .await;
    }

    #[rstest]
    #[tokio::test]
    async fn test_variable_expansion() {
        with_context(async |mut context| {
            let expansion = context
                .run()
                .variable("ESCAPED", "this should not appear")
                .variable("QUOTED", "spaces should be preserved")
                .variable("RECURSIVE", "recursion($RECURSIVE)")
                .command("echo --sep ; $MISSING $$ESCAPED $QUOTED $RECURSIVE")
                .go()
                .await;
            assert!(expansion.ok());

            assert_eq!(
                expansion.log().lines().last().unwrap(),
                ";$ESCAPED;spaces should be preserved;recursion()"
            );
        })
        .await;
    }

    #[rstest]
    #[tokio::test]
    async fn test_flag_parsing() {
        with_context(async |mut context| {
            let echo = context.run().command("echo --uppercase false").go().await;
            assert!(echo.ok());

            assert_eq!(echo.log().lines().last().unwrap(), "FALSE");

            let echo = context
                .run()
                .command("echo --uppercase=false true")
                .go()
                .await;
            assert!(echo.ok());

            assert_eq!(echo.log().lines().last().unwrap(), "true");

            let echo = context
                .run()
                .command("echo --uppercase=true false")
                .go()
                .await;
            assert!(echo.ok());

            assert_eq!(echo.log().lines().last().unwrap(), "FALSE");

            let echo = context.run().command("echo --uppercase=X false").go().await;
            assert!(!echo.ok());
        })
        .await;
    }

    #[derive(Debug, PartialEq, Eq)]
    enum GenerateMonitorTimeoutLocation {
        HandlerOption,
        Argument,
    }

    #[rstest]
    #[tokio::test]
    async fn test_generate_monitor_timeouts(
        #[values(
            None,
            Some(GenerateMonitorTimeoutLocation::HandlerOption),
            Some(GenerateMonitorTimeoutLocation::Argument)
        )]
        test: Option<GenerateMonitorTimeoutLocation>,
    ) {
        const TEST_MONITOR_TIMEOUT: &str = "10 minutes";

        with_context(async |mut context| {
            let build_info = ObsBuildInfo {
                project: TEST_PROJECT.to_owned(),
                package: TEST_PACKAGE_1.to_owned(),
                rev: Some("1".to_owned()),
                srcmd5: Some("abc".to_owned()),
                is_branched: false,
                enabled_repos: vec![EnabledRepo {
                    repo_arch: RepoArch {
                        repo: TEST_REPO.to_owned(),
                        arch: TEST_ARCH_1.to_owned(),
                    },

                    prev_endtime_for_commit: None,
                }],
            };

            let build_info = context
                .inject_artifacts(
                    [(
                        DEFAULT_BUILD_INFO.to_owned(),
                        serde_json::to_string(&build_info).unwrap().into_bytes(),
                    )]
                    .into(),
                )
                .await;

            let generate_builder = if test == Some(GenerateMonitorTimeoutLocation::Argument) {
                context.run().command(format!(
                    "generate-monitor tag --job-timeout '{TEST_MONITOR_TIMEOUT}'"
                ))
            } else {
                context.run().command("generate-monitor tag")
            }
            .artifacts(build_info);

            let generate = if test == Some(GenerateMonitorTimeoutLocation::HandlerOption) {
                generate_builder
                    .obs_job_handler(HandlerOptions {
                        default_monitor_job_timeout: Some(TEST_MONITOR_TIMEOUT.to_owned()),
                        ..DEFAULT_HANDLER_OPTIONS
                    })
                    .go()
                    .await
            } else {
                generate_builder.go().await
            };

            assert!(generate.ok());

            let results = context.fetch_artifacts(&generate.artifacts()).await;
            let pipeline_yaml: serde_yaml::Value = assert_ok!(serde_yaml::from_slice(
                results.get(DEFAULT_MONITOR_PIPELINE).unwrap()
            ));
            let pipeline_map = pipeline_yaml.as_mapping().unwrap();

            let monitor_map = pipeline_map
                .into_iter()
                .next()
                .unwrap()
                .1
                .as_mapping()
                .unwrap();

            let timeout_yaml = monitor_map.get("timeout");
            if test.is_some() {
                assert_eq!(
                    timeout_yaml.unwrap().as_str().unwrap(),
                    TEST_MONITOR_TIMEOUT
                );
            } else {
                assert_none!(timeout_yaml);
            }
        })
        .await;
    }
}
