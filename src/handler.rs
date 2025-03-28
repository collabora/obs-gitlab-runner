use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fs::File,
    io::SeekFrom,
};

use async_trait::async_trait;
use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use color_eyre::eyre::{eyre, Context, Result};
use derivative::*;
use futures_util::StreamExt;
use gitlab_runner::{
    job::{Dependency, Job, Variable},
    outputln,
    uploader::Uploader,
    JobHandler, JobResult, Phase,
};
use open_build_service_api as obs;
use serde::{Deserialize, Serialize};
use tokio::{fs::File as AsyncFile, io::AsyncSeekExt};
use tokio_util::{compat::FuturesAsyncWriteCompatExt, io::ReaderStream};
use tracing::{debug, error, instrument};

use crate::{
    artifacts::{save_to_tempfile, ArtifactDirectory},
    binaries::download_binaries,
    build_meta::{
        BuildHistoryRetrieval, BuildMeta, BuildMetaOptions, CommitBuildInfo, DisabledRepos,
        RepoArch,
    },
    monitor::{MonitoredPackage, ObsMonitor, PackageCompletion, PackageMonitoringOptions},
    pipeline::{generate_monitor_pipeline, GeneratePipelineOptions, PipelineDownloadBinaries},
    prune::prune_branch,
    retry::retry_request,
    upload::ObsDscUploader,
};

const DEFAULT_BUILD_INFO: &str = "build-info.yml";
const DEFAULT_MONITOR_PIPELINE: &str = "obs.yml";
const DEFAULT_PIPELINE_JOB_PREFIX: &str = "obs";
const DEFAULT_ARTIFACT_EXPIRATION: &str = "3 days";
const DEFAULT_BUILD_LOG: &str = "build.log";

// Our flags can all take explicit values, because it makes it easier to
// conditionally set things in the pipelines.
trait FlagSupportingExplicitValue {
    fn flag_supporting_explicit_value(self) -> Self;
}

impl FlagSupportingExplicitValue for clap::Arg<'_> {
    fn flag_supporting_explicit_value(self) -> Self {
        self.min_values(0)
            .require_equals(true)
            .required(false)
            .default_value("false")
            .default_missing_value("true")
    }
}

#[derive(Parser, Debug)]
struct DputAction {
    project: String,
    dsc: String,
    #[clap(long, default_value = "")]
    branch_to: String,
    #[clap(long, default_value_t = DEFAULT_BUILD_INFO.to_owned())]
    build_info_out: String,
    #[clap(long, parse(try_from_str), flag_supporting_explicit_value())]
    rebuild_if_unchanged: bool,
}

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
struct MonitorAction {
    #[clap(long)]
    project: String,
    #[clap(long)]
    package: String,
    #[clap(long)]
    rev: String,
    #[clap(long)]
    srcmd5: String,
    #[clap(long)]
    repository: String,
    #[clap(long)]
    arch: String,
    #[clap(long)]
    prev_endtime_for_commit: Option<u64>,
    #[clap(long)]
    build_log_out: String,
}

#[derive(Parser, Debug)]
struct DownloadBinariesAction {
    #[clap(long)]
    project: String,
    #[clap(long)]
    package: String,
    #[clap(long)]
    repository: String,
    #[clap(long)]
    arch: String,
    #[clap(long)]
    build_results_dir: Utf8PathBuf,
}

#[derive(Parser, Debug)]
struct PruneAction {
    #[clap(long, default_value_t = DEFAULT_BUILD_INFO.to_owned())]
    build_info: String,
    #[clap(long, parse(try_from_str), flag_supporting_explicit_value())]
    ignore_missing_build_info: bool,
    #[clap(long, parse(try_from_str), flag_supporting_explicit_value())]
    only_if_job_unsuccessful: bool,
}

#[cfg(test)]
#[derive(Parser, Debug)]
struct EchoAction {
    args: Vec<String>,
    #[clap(long, parse(try_from_str), flag_supporting_explicit_value())]
    fail: bool,
    #[clap(long, parse(try_from_str), flag_supporting_explicit_value())]
    uppercase: bool,
    #[clap(long, default_value = " ")]
    sep: String,
}

#[derive(Subcommand)]
enum Action {
    Dput(DputAction),
    GenerateMonitor(GenerateMonitorAction),
    Monitor(MonitorAction),
    DownloadBinaries(DownloadBinariesAction),
    Prune(PruneAction),
    #[cfg(test)]
    Echo(EchoAction),
}

#[derive(Parser)]
#[clap(bin_name = "obs-gitlab-runner")]
#[clap(no_binary_name = true)]
struct Command {
    #[clap(subcommand)]
    action: Action,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ObsBuildInfo {
    project: String,
    package: String,
    rev: Option<String>,
    srcmd5: Option<String>,
    is_branched: bool,
    enabled_repos: HashMap<RepoArch, CommitBuildInfo>,
}

impl ObsBuildInfo {
    #[instrument]
    fn save(&self) -> Result<File> {
        let mut file = tempfile::tempfile().wrap_err("Failed to create build info file")?;
        serde_yaml::to_writer(&mut file, self).wrap_err("Failed to write build info file")?;
        Ok(file)
    }
}

#[derive(Debug)]
struct FailedBuild;

impl std::fmt::Display for FailedBuild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for FailedBuild {}

const LOG_TAIL_2MB: u64 = 2 * 1024 * 1024;

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

pub struct ObsJobHandler {
    job: Job,
    client: obs::Client,
    options: HandlerOptions,

    script_failed: bool,
    artifacts: HashMap<String, AsyncFile>,
}

impl ObsJobHandler {
    pub fn new(job: Job, client: obs::Client, options: HandlerOptions) -> Self {
        ObsJobHandler {
            job,
            client,
            options,
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
    async fn run_dput(&mut self, args: DputAction) -> Result<()> {
        let branch_to = if !args.branch_to.is_empty() {
            Some(args.branch_to)
        } else {
            None
        };
        let is_branched = branch_to.is_some();

        // The upload prep and actual upload are split in two so that we can
        // already tell what the project & package name are, so build-info.yaml
        // can be written and pruning can take place regardless of the actual
        // *upload* success.
        let uploader = ObsDscUploader::prepare(
            self.client.clone(),
            args.project.clone(),
            branch_to,
            args.dsc.as_str().into(),
            self,
        )
        .await?;

        let build_info = ObsBuildInfo {
            project: uploader.project().to_owned(),
            package: uploader.package().to_owned(),
            rev: None,
            srcmd5: None,
            is_branched,
            enabled_repos: HashMap::new(),
        };
        debug!("Saving initial build info: {:?}", build_info);

        let build_info_2 = build_info.clone();
        let build_info_file = tokio::task::spawn_blocking(move || build_info_2.save()).await??;

        self.artifacts.insert(
            args.build_info_out.clone(),
            AsyncFile::from_std(build_info_file),
        );

        let initial_build_meta = BuildMeta::get_if_package_exists(
            self.client.clone(),
            build_info.project.clone(),
            build_info.package.clone(),
            &BuildMetaOptions {
                history_retrieval: BuildHistoryRetrieval::Full,
                // Getting disabled repos has to happen *after* the upload,
                // since the new version can change the supported architectures.
                disabled_repos: DisabledRepos::Keep,
            },
        )
        .await?;
        debug!(?initial_build_meta);

        let result = uploader.upload_package(self).await?;

        // If we couldn't get the metadata before because the package didn't
        // exist yet, get it now but without history, so we leave the previous
        // endtime empty (if there was no previous package, there were no
        // previous builds).
        let mut build_meta = if let Some(mut build_meta) = initial_build_meta {
            build_meta
                .remove_disabled_repos(&Default::default())
                .await?;
            build_meta
        } else {
            BuildMeta::get(
                self.client.clone(),
                build_info.project.clone(),
                build_info.package.clone(),
                &BuildMetaOptions {
                    history_retrieval: BuildHistoryRetrieval::None,
                    disabled_repos: DisabledRepos::Skip {
                        wait_options: Default::default(),
                    },
                },
            )
            .await?
        };

        if result.unchanged {
            outputln!("Package unchanged at revision {}.", result.rev);

            if args.rebuild_if_unchanged {
                retry_request(|| async {
                    self.client
                        .project(build_info.project.clone())
                        .package(build_info.package.clone())
                        .rebuild()
                        .await
                })
                .await
                .wrap_err("Failed to trigger rebuild")?;
            } else {
                // Clear out the history used to track endtime values. This is
                // normally important to make sure the monitor doesn't
                // accidentally pick up an old build result...but if we didn't
                // rebuild anything, picking up the old result is *exactly* the
                // behavior we want.
                build_meta.clear_stored_history();
            }
        } else {
            outputln!("Package uploaded with revision {}.", result.rev);
        }

        let enabled_repos = build_meta.get_commit_build_info(&result.build_srcmd5);
        let build_info = ObsBuildInfo {
            rev: Some(result.rev),
            srcmd5: Some(result.build_srcmd5),
            enabled_repos,
            ..build_info
        };
        debug!("Saving complete build info: {:?}", build_info);

        let build_info_file = tokio::task::spawn_blocking(move || build_info.save()).await??;
        self.artifacts
            .insert(args.build_info_out, AsyncFile::from_std(build_info_file));

        Ok(())
    }

    #[instrument(skip(self))]
    async fn run_generate_monitor(&mut self, args: GenerateMonitorAction) -> Result<()> {
        let build_info_data = self.get_data(&args.build_info).await?;
        let build_info: ObsBuildInfo = serde_yaml::from_slice(&build_info_data[..])
            .wrap_err("Failed to parse provided build info file")?;

        let file = generate_monitor_pipeline(
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
        self.artifacts
            .insert(args.pipeline_out.clone(), AsyncFile::from_std(file));

        outputln!("Wrote pipeline file '{}'.", args.pipeline_out);

        Ok(())
    }

    #[instrument(skip(self))]
    async fn run_monitor(&mut self, args: MonitorAction) -> Result<()> {
        let monitor = ObsMonitor::new(
            self.client.clone(),
            MonitoredPackage {
                project: args.project.clone(),
                package: args.package.clone(),
                repository: args.repository.clone(),
                arch: args.arch.clone(),
                rev: args.rev.clone(),
                srcmd5: args.srcmd5.clone(),
                prev_endtime_for_commit: args.prev_endtime_for_commit,
            },
        );

        let completion = monitor
            .monitor_package(self.options.monitor.clone())
            .await?;
        debug!("Completed with: {:?}", completion);

        let mut log_file = monitor.download_build_log().await?;
        self.artifacts.insert(
            args.build_log_out.clone(),
            log_file
                .file
                .try_clone()
                .await
                .wrap_err("Failed to clone log file")?,
        );

        match completion {
            PackageCompletion::Succeeded => {
                outputln!("Build succeeded!");
            }
            PackageCompletion::Superceded => {
                outputln!("Build was superceded by a newer revision.");
            }
            PackageCompletion::Disabled => {
                outputln!("Package is disabled for this architecture.");
            }
            PackageCompletion::Failed(reason) => {
                log_file
                    .file
                    .seek(SeekFrom::End(
                        -(std::cmp::min(self.options.log_tail, log_file.len) as i64),
                    ))
                    .await
                    .wrap_err("Failed to find length of log file")?;

                let mut log_stream = ReaderStream::new(log_file.file);
                while let Some(bytes) = log_stream.next().await {
                    let bytes = bytes.wrap_err("Failed to stream log bytes")?;
                    self.job.trace(String::from_utf8_lossy(&bytes).as_ref());
                }

                outputln!("{}", "=".repeat(64));
                outputln!(
                    "Build failed with reason '{}'.",
                    reason.to_string().to_lowercase()
                );
                outputln!("The last 2MB of the build log is printed above.");
                outputln!(
                    "(Full logs are available in the build artifact '{}'.)",
                    args.build_log_out
                );
                return Err(FailedBuild.into());
            }
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn run_download_binaries(&mut self, args: DownloadBinariesAction) -> Result<()> {
        let binaries = download_binaries(
            self.client.clone(),
            &args.project,
            &args.package,
            &args.repository,
            &args.arch,
        )
        .await?;
        let binary_count = binaries.len();

        self.artifacts.extend(
            binaries
                .into_iter()
                .map(|(path, file)| (args.build_results_dir.join(path).to_string(), file)),
        );

        outputln!("Downloaded {} artifact(s).", binary_count);
        Ok(())
    }

    #[instrument(skip(self))]
    async fn run_prune(&mut self, args: PruneAction) -> Result<()> {
        if args.only_if_job_unsuccessful && !self.script_failed {
            outputln!("Skipping prune: main script was successful.");
            return Ok(());
        }

        let build_info_data = if args.ignore_missing_build_info {
            if let Some(build_info_data) = self.get_data_or_none(&args.build_info).await? {
                build_info_data
            } else {
                outputln!(
                    "Skipping prune: build info file '{}' not found.",
                    args.build_info
                );
                return Ok(());
            }
        } else {
            self.get_data(&args.build_info).await?
        };

        let build_info: ObsBuildInfo = serde_yaml::from_slice(&build_info_data[..])
            .wrap_err("Failed to parse provided build info file")?;

        if build_info.is_branched {
            outputln!(
                "Pruning branched package {}/{}...",
                build_info.project,
                build_info.package
            );
            prune_branch(
                &self.client,
                &build_info.project,
                &build_info.package,
                build_info.rev.as_deref(),
            )
            .await?;
        } else {
            outputln!("Skipping prune: package was not branched.");
        }

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
            Action::Dput(args) => self.run_dput(args).await?,
            Action::GenerateMonitor(args) => self.run_generate_monitor(args).await?,
            Action::Monitor(args) => self.run_monitor(args).await?,
            Action::DownloadBinaries(args) => self.run_download_binaries(args).await?,
            Action::Prune(args) => self.run_prune(args).await?,
            #[cfg(test)]
            Action::Echo(args) => {
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

#[instrument(skip(file, uploader))]
async fn upload_artifact(
    name: String,
    file: &mut AsyncFile,
    uploader: &mut Uploader,
) -> Result<()> {
    let dest = uploader.file(name).await;

    file.rewind().await.wrap_err("Failed to rewind artifact")?;
    tokio::io::copy(file, &mut dest.compat_write())
        .await
        .wrap_err("Failed to copy artifact contents")?;

    Ok(())
}

#[async_trait]
impl JobHandler for ObsJobHandler {
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

    async fn upload_artifacts(&mut self, uploader: &mut Uploader) -> JobResult {
        let mut success = true;

        for (name, file) in &mut self.artifacts {
            if let Err(err) = upload_artifact(name.clone(), file, uploader).await {
                error!(gitlab.output = true, "Failed to upload {}: {:?}", name, err);
                success = false;
            }
        }

        if success {
            Ok(())
        } else {
            Err(())
        }
    }
}

#[instrument(skip(dep), fields(dep_id = dep.id(), dep_name = dep.name()))]
async fn check_for_artifact(dep: Dependency<'_>, filename: &str) -> Result<Option<AsyncFile>> {
    // Needed because anything captured by spawn_blocking must have a 'static
    // lifetime.
    let filename = filename.to_owned();

    // TODO: not spawn a sync environment for *every single artifact*
    if let Some(mut artifact) = dep.download().await? {
        if let Some(file) = tokio::task::spawn_blocking(move || {
            artifact
                .file(&filename)
                .map(|mut file| save_to_tempfile(&mut file))
                .transpose()
        })
        .await??
        {
            return Ok(Some(AsyncFile::from_std(file)));
        }
    }

    Ok(None)
}

#[async_trait]
impl ArtifactDirectory for ObsJobHandler {
    type Reader = File;

    #[instrument(skip(self))]
    async fn get_file_or_none(&self, filename: &str) -> Result<Option<AsyncFile>> {
        if let Some(file) = self.artifacts.get(filename) {
            let mut file = file
                .try_clone()
                .await
                .wrap_err("Failed to clone artifact")?;
            file.rewind().await.wrap_err("Failed to rewind artifact")?;
            return Ok(Some(file));
        }

        for dep in self.job.dependencies() {
            if let Some(file) = check_for_artifact(dep, filename).await? {
                return Ok(Some(file));
            }
        }

        Ok(None)
    }

    #[instrument(skip(self))]
    async fn get_or_none(&self, filename: &str) -> Result<Option<Self::Reader>> {
        Ok(self
            .get_file_or_none(filename)
            .await?
            .map(|f| f.try_into_std().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cmp::Ordering,
        io::{Cursor, Read},
        sync::Once,
        time::{Duration, SystemTime},
    };

    use camino::Utf8Path;
    use claim::*;
    use futures_util::{AsyncWriteExt, Future};
    use gitlab_runner::{GitlabLayer, Runner};
    use gitlab_runner_mock::*;
    use open_build_service_mock::*;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;
    use tracing::{instrument::WithSubscriber, Level};
    use tracing_subscriber::{filter::Targets, prelude::*, Layer, Registry};
    use zip::ZipArchive;

    use crate::{test_support::*, upload::compute_md5};

    use super::*;

    const JOB_TIMEOUT: u64 = 3600;
    const TEST_LOG_TAIL: u64 = 50;
    const OLD_STATUS_SLEEP_DURATION: Duration = Duration::from_millis(100);

    const DEFAULT_HANDLER_OPTIONS: HandlerOptions = HandlerOptions {
        default_monitor_job_timeout: None,
        log_tail: TEST_LOG_TAIL,
        monitor: PackageMonitoringOptions {
            sleep_on_building: Duration::ZERO,
            sleep_on_old_status: OLD_STATUS_SLEEP_DURATION,
            // High limit, since we don't really test that
            // functionality in the handler tests.
            max_old_status_retries: 99,
        },
    };

    static COLOR_EYRE_INSTALL: Once = Once::new();

    struct TestContext {
        _runner_dir: TempDir,
        gitlab_mock: GitlabRunnerMock,
        runner: Runner,

        obs_mock: ObsMock,
        obs_client: obs::Client,
    }

    #[fixture]
    async fn test_context() -> (TestContext, GitlabLayer) {
        COLOR_EYRE_INSTALL.call_once(|| color_eyre::install().unwrap());

        let runner_dir = tempfile::tempdir().unwrap();
        let gitlab_mock = GitlabRunnerMock::start().await;
        let (runner, layer) = Runner::new_with_layer(
            gitlab_mock.uri(),
            gitlab_mock.runner_token().to_owned(),
            runner_dir.path().to_owned(),
        );

        let obs_mock = create_default_mock().await;
        let obs_client = create_default_client(&obs_mock);

        (
            TestContext {
                _runner_dir: runner_dir,
                gitlab_mock,
                runner,
                obs_mock,
                obs_client,
            },
            layer,
        )
    }

    async fn with_tracing<T, Fut>(layer: GitlabLayer, future: Fut) -> T
    where
        Fut: Future<Output = T>,
    {
        future
            .with_subscriber(
                Registry::default()
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_test_writer()
                            .with_filter(
                                Targets::new()
                                    .with_targets([
                                        ("obs_gitlab_runner", Level::TRACE),
                                        ("gitlab_runner", Level::DEBUG),
                                    ])
                                    .with_default(Level::WARN),
                            ),
                    )
                    .with(tracing_error::ErrorLayer::default())
                    .with(layer),
            )
            .await
    }

    #[derive(Default)]
    struct JobSpec {
        name: String,
        dependencies: Vec<MockJob>,
        variables: HashMap<String, String>,
        script: Vec<String>,
        after_script: Vec<String>,
    }

    fn enqueue_job(context: &TestContext, spec: JobSpec) -> MockJob {
        let mut builder = context.gitlab_mock.job_builder(spec.name);

        builder.add_step(
            MockJobStepName::Script,
            spec.script,
            JOB_TIMEOUT,
            MockJobStepWhen::OnSuccess,
            false,
        );

        if !spec.after_script.is_empty() {
            builder.add_step(
                MockJobStepName::AfterScript,
                spec.after_script,
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

        for dependency in spec.dependencies {
            builder.dependency(dependency);
        }
        for (key, value) in spec.variables {
            builder.add_variable(key, value, true, false);
        }

        builder.add_variable(
            "OBS_SERVER".to_owned(),
            context.obs_client.url().to_string(),
            false,
            true,
        );
        builder.add_variable("OBS_USER".to_owned(), TEST_USER.to_owned(), false, true);
        builder.add_variable("OBS_PASSWORD".to_owned(), TEST_PASS.to_owned(), false, true);

        let job = builder.build();
        context.gitlab_mock.enqueue_job(job.clone());
        job
    }

    async fn run_handler<H, Func>(context: &mut TestContext, handler_func: Func)
    where
        H: JobHandler + Send + 'static,
        Func: (FnOnce(Job) -> H) + Send + Sync + 'static,
    {
        let got_job = context
            .runner
            .request_job(move |job| futures_util::future::ready(Ok(handler_func(job))))
            .await
            .unwrap();
        assert!(got_job);
        context.runner.wait_for_space(1).await;
    }

    struct PutArtifactsHandler {
        artifacts: HashMap<String, Vec<u8>>,
    }

    #[async_trait]
    impl JobHandler for PutArtifactsHandler {
        async fn step(&mut self, _script: &[String], _phase: Phase) -> JobResult {
            Ok(())
        }

        async fn upload_artifacts(&mut self, uploader: &mut Uploader) -> JobResult {
            for (name, content) in &self.artifacts {
                let mut file = uploader.file(name.clone()).await;
                file.write_all(content).await.unwrap();
            }

            Ok(())
        }
    }

    async fn put_artifacts(
        context: &mut TestContext,
        artifacts: HashMap<String, Vec<u8>>,
    ) -> MockJob {
        let artifacts_job = enqueue_job(
            context,
            JobSpec {
                name: "artifacts".to_owned(),
                script: vec!["dummy".to_owned()],
                ..Default::default()
            },
        );
        run_handler(context, |_| PutArtifactsHandler { artifacts }).await;
        artifacts_job
    }

    fn get_job_artifacts(job: &MockJob) -> HashMap<String, Vec<u8>> {
        let data = (*job.artifact()).clone();
        if data.is_empty() {
            return HashMap::new();
        }

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

    async fn run_obs_handler_with_options(context: &mut TestContext, options: HandlerOptions) {
        run_handler(context, move |job| {
            assert_ok!(ObsJobHandler::from_obs_config_in_job(job, options))
        })
        .await;
    }

    async fn run_obs_handler(context: &mut TestContext) {
        run_obs_handler_with_options(context, DEFAULT_HANDLER_OPTIONS).await;
    }

    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum DputTest {
        Basic,
        Rebuild,
        ReusePreviousBuild,
        Branch,
    }

    async fn test_dput(context: &mut TestContext, test: DputTest) -> (MockJob, ObsBuildInfo) {
        let test1_file = "test1";
        let test1_contents = b"123";
        let test1_md5 = compute_md5(test1_contents);

        let dsc1_file = "test1.dsc";
        let dsc1_contents = format!(
            "Source: {}\nFiles:\n {} {} {}",
            TEST_PACKAGE_1,
            test1_md5.clone(),
            test1_contents.len(),
            test1_file
        );
        let dsc1_md5 = compute_md5(dsc1_contents.as_bytes());

        let dsc1_bad_file = "test1-bad.dsc";
        let dsc1_bad_contents =
            dsc1_contents.replace(test1_file, &(test1_file.to_owned() + ".missing"));

        context.obs_mock.add_project(TEST_PROJECT.to_owned());

        context.obs_mock.add_or_update_repository(
            TEST_PROJECT,
            TEST_REPO.to_owned(),
            TEST_ARCH_1.to_owned(),
            MockRepositoryCode::Finished,
        );
        context.obs_mock.add_or_update_repository(
            TEST_PROJECT,
            TEST_REPO.to_owned(),
            TEST_ARCH_2.to_owned(),
            MockRepositoryCode::Finished,
        );

        if test == DputTest::Rebuild {
            // We also test excluded repos on rebuilds; this test makes it
            // easier, because it's not testing creating a new package, so we
            // can create it ourselves first with the desired metadata.
            context.obs_mock.add_new_package(
                TEST_PROJECT,
                TEST_PACKAGE_1.to_owned(),
                MockPackageOptions::default(),
            );
            context.obs_mock.set_package_build_status(
                TEST_PROJECT,
                TEST_REPO,
                TEST_ARCH_2,
                TEST_PACKAGE_1.to_owned(),
                MockBuildStatus::new(MockPackageCode::Disabled),
            );
        }

        let artifacts = put_artifacts(
            context,
            [
                (dsc1_file.to_owned(), dsc1_contents.as_bytes().to_vec()),
                (
                    dsc1_bad_file.to_owned(),
                    dsc1_bad_contents.as_bytes().to_vec(),
                ),
                (test1_file.to_owned(), test1_contents.to_vec()),
            ]
            .into(),
        )
        .await;

        let mut dput_command = format!("dput {} {}", TEST_PROJECT, dsc1_file);
        let mut created_project = TEST_PROJECT.to_owned();

        if test == DputTest::Branch {
            created_project += ":branched";
            dput_command += &format!(" --branch-to {}", created_project);
        }

        let dput = enqueue_job(
            context,
            JobSpec {
                name: "dput".to_owned(),
                dependencies: vec![artifacts.clone()],
                script: vec![dput_command.replace(dsc1_file, dsc1_bad_file)],
                ..Default::default()
            },
        );

        run_obs_handler(context).await;
        assert_eq!(MockJobState::Failed, dput.state());

        let results = get_job_artifacts(&dput);
        let build_info: ObsBuildInfo =
            serde_yaml::from_slice(results.get(DEFAULT_BUILD_INFO).unwrap()).unwrap();

        assert_eq!(build_info.project, created_project);
        assert_eq!(build_info.package, TEST_PACKAGE_1);
        assert_none!(build_info.rev);
        assert_eq!(build_info.is_branched, test == DputTest::Branch);

        let mut dput = enqueue_job(
            context,
            JobSpec {
                name: "dput".to_owned(),
                dependencies: vec![artifacts.clone()],
                script: vec![dput_command.clone()],
                ..Default::default()
            },
        );

        run_obs_handler(context).await;
        assert_eq!(MockJobState::Success, dput.state());

        if test == DputTest::Rebuild || test == DputTest::ReusePreviousBuild {
            context.obs_mock.add_or_update_repository(
                &created_project,
                TEST_REPO.to_owned(),
                TEST_ARCH_1.to_owned(),
                MockRepositoryCode::Building,
            );
            // Also test endtimes, since we now have an existing package to
            // modify the metadata of.
            let dir = assert_ok!(
                context
                    .obs_client
                    .project(TEST_PROJECT.to_owned())
                    .package(TEST_PACKAGE_1.to_owned())
                    .list(None)
                    .await
            );
            // Testing of reused builds never had the second arch disabled, so
            // also add that build history.
            if test == DputTest::ReusePreviousBuild {
                context.obs_mock.add_job_history(
                    TEST_PROJECT,
                    TEST_REPO,
                    TEST_ARCH_2,
                    MockJobHistoryEntry {
                        package: TEST_PACKAGE_1.to_owned(),
                        rev: dir.rev.clone().unwrap(),
                        srcmd5: dir.srcmd5.clone(),
                        ..Default::default()
                    },
                );
            }
            context.obs_mock.add_job_history(
                TEST_PROJECT,
                TEST_REPO,
                TEST_ARCH_1,
                MockJobHistoryEntry {
                    package: TEST_PACKAGE_1.to_owned(),
                    rev: dir.rev.unwrap(),
                    srcmd5: dir.srcmd5,
                    ..Default::default()
                },
            );

            context.obs_mock.set_package_build_status_for_rebuilds(
                &created_project,
                MockBuildStatus::new(MockPackageCode::Broken),
            );
            context.obs_mock.set_package_build_status(
                &created_project,
                TEST_REPO,
                TEST_ARCH_1,
                TEST_PACKAGE_1.to_owned(),
                MockBuildStatus::new(MockPackageCode::Failed),
            );

            let status = assert_ok!(
                context
                    .obs_client
                    .project(created_project.clone())
                    .package(TEST_PACKAGE_1.to_owned())
                    .status(TEST_REPO, TEST_ARCH_1)
                    .await
            );
            assert_eq!(status.code, obs::PackageCode::Failed);

            dput = enqueue_job(
                context,
                JobSpec {
                    name: "dput".to_owned(),
                    dependencies: vec![artifacts.clone()],
                    script: vec![dput_command.clone()],
                    ..Default::default()
                },
            );

            run_obs_handler(context).await;
            assert_eq!(MockJobState::Success, dput.state());

            let job_log = String::from_utf8_lossy(&dput.log()).into_owned();
            assert!(job_log.contains("unchanged"));

            let status = assert_ok!(
                context
                    .obs_client
                    .project(created_project.clone())
                    .package(TEST_PACKAGE_1.to_owned())
                    .status(TEST_REPO, TEST_ARCH_1)
                    .await
            );
            assert_eq!(status.code, obs::PackageCode::Failed);

            if test == DputTest::Rebuild {
                dput = enqueue_job(
                    context,
                    JobSpec {
                        name: "dput".to_owned(),
                        dependencies: vec![artifacts.clone()],
                        script: vec![format!("{} --rebuild-if-unchanged", dput_command)],
                        ..Default::default()
                    },
                );

                run_obs_handler(context).await;
                assert_eq!(MockJobState::Success, dput.state());

                let status = assert_ok!(
                    context
                        .obs_client
                        .project(created_project.clone())
                        .package(TEST_PACKAGE_1.to_owned())
                        .status(TEST_REPO, TEST_ARCH_1)
                        .await
                );
                assert_eq!(status.code, obs::PackageCode::Broken);

                let job_log = String::from_utf8_lossy(&dput.log()).into_owned();
                assert!(job_log.contains("unchanged"));
            }
        }

        let results = get_job_artifacts(&dput);
        let build_info: ObsBuildInfo =
            serde_yaml::from_slice(results.get(DEFAULT_BUILD_INFO).unwrap()).unwrap();

        assert_eq!(build_info.project, created_project);
        assert_eq!(build_info.package, TEST_PACKAGE_1);
        assert_some!(build_info.rev.as_deref());
        assert_eq!(build_info.is_branched, test == DputTest::Branch);

        assert_eq!(
            build_info.enabled_repos.len(),
            if test == DputTest::Rebuild { 1 } else { 2 }
        );

        let arch_1 = build_info
            .enabled_repos
            .get(&RepoArch {
                repo: TEST_REPO.to_owned(),
                arch: TEST_ARCH_1.to_owned(),
            })
            .unwrap();

        if test == DputTest::Rebuild {
            assert_some!(arch_1.prev_endtime_for_commit);
        } else {
            assert_none!(arch_1.prev_endtime_for_commit);

            let arch_2 = build_info
                .enabled_repos
                .get(&RepoArch {
                    repo: TEST_REPO.to_owned(),
                    arch: TEST_ARCH_2.to_owned(),
                })
                .unwrap();
            assert_none!(arch_2.prev_endtime_for_commit);
        }

        let mut dir = assert_ok!(
            context
                .obs_client
                .project(created_project.clone())
                .package(TEST_PACKAGE_1.to_owned())
                .list(None)
                .await
        );

        assert_eq!(dir.entries.len(), 3);
        dir.entries.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(dir.entries[0].name, "_meta");
        assert_eq!(dir.entries[1].name, test1_file);
        assert_eq!(dir.entries[1].size, test1_contents.len() as u64);
        assert_eq!(dir.entries[1].md5, test1_md5);
        assert_eq!(dir.entries[2].name, dsc1_file);
        assert_eq!(dir.entries[2].size, dsc1_contents.len() as u64);
        assert_eq!(dir.entries[2].md5, dsc1_md5);

        (dput, build_info)
    }

    #[derive(Debug, PartialEq, Eq)]
    enum MonitorLogTest {
        // Test that long logs are truncated.
        Long,
        // Test that short logs are fully shown.
        Short,
        // Test that revision mismatches result in unavailable logs.
        Unavailable,
    }

    async fn test_monitoring(
        context: &mut TestContext,
        dput: MockJob,
        build_info: &ObsBuildInfo,
        success: bool,
        dput_test: DputTest,
        log_test: MonitorLogTest,
        download_binaries: bool,
    ) {
        const TEST_JOB_RUNNER_TAG: &str = "test-tag";
        const TEST_MONITOR_TIMEOUT: &str = "1 day";
        const TEST_BUILD_RESULTS_DIR: &str = "results";
        const TEST_BUILD_RESULT: &str = "test-build-result";
        const TEST_BUILD_RESULT_CONTENTS: &[u8] = b"abcdef";

        let srcmd5_prefix = format!(
            "srcmd5 '{}' ",
            if log_test == MonitorLogTest::Unavailable {
                ZERO_REV_SRCMD5.to_owned()
            } else {
                let dir = assert_ok!(
                    context
                        .obs_client
                        .project(build_info.project.to_owned())
                        .package(TEST_PACKAGE_1.to_owned())
                        .list(None)
                        .await
                );

                if build_info.is_branched {
                    dir.linkinfo.into_iter().next().unwrap().xsrcmd5
                } else {
                    dir.srcmd5
                }
            }
        );

        let (log_contents, log_vs_limit) = if log_test == MonitorLogTest::Short {
            (srcmd5_prefix + "short", Ordering::Less)
        } else {
            (
                srcmd5_prefix + "this is a long log that will need to be trimmed when printed",
                Ordering::Greater,
            )
        };

        assert_eq!(
            log_contents.len().cmp(&(TEST_LOG_TAIL as usize)),
            log_vs_limit
        );

        let mut generate_command = format!(
            "generate-monitor {} --job-timeout '{}' --rules '[{{a: 1}}, {{b: 2}}]'",
            TEST_JOB_RUNNER_TAG, TEST_MONITOR_TIMEOUT
        );
        if download_binaries {
            generate_command += &format!(" --download-build-results-to {}", TEST_BUILD_RESULTS_DIR);
        }
        let generate = enqueue_job(
            context,
            JobSpec {
                name: "generate".to_owned(),
                dependencies: vec![dput.clone()],
                script: vec![generate_command],
                ..Default::default()
            },
        );

        run_obs_handler(context).await;
        assert_eq!(generate.state(), MockJobState::Success);

        let results = get_job_artifacts(&generate);
        let pipeline_yaml: serde_yaml::Value = assert_ok!(serde_yaml::from_slice(
            results.get(DEFAULT_MONITOR_PIPELINE).unwrap()
        ));
        let pipeline_map = pipeline_yaml.as_mapping().unwrap();

        assert_eq!(pipeline_map.len(), build_info.enabled_repos.len());

        for repo in build_info.enabled_repos.keys() {
            // Sanity check this, even though test_dput should have already
            // checked it.
            assert_eq!(repo.repo, TEST_REPO);
            assert!(
                repo.arch == TEST_ARCH_1 || repo.arch == TEST_ARCH_2,
                "unexpected arch '{}'",
                repo.arch
            );

            context.obs_mock.set_package_build_status(
                &build_info.project,
                &repo.repo,
                &repo.arch,
                TEST_PACKAGE_1.to_owned(),
                MockBuildStatus::new(if success {
                    MockPackageCode::Succeeded
                } else {
                    MockPackageCode::Failed
                }),
            );
            context.obs_mock.add_completed_build_log(
                &build_info.project,
                TEST_REPO,
                &repo.arch,
                TEST_PACKAGE_1.to_owned(),
                MockBuildLog::new(log_contents.to_owned()),
                success,
            );
            context.obs_mock.set_package_binaries(
                &build_info.project,
                TEST_REPO,
                &repo.arch,
                TEST_PACKAGE_1.to_owned(),
                [(
                    TEST_BUILD_RESULT.to_owned(),
                    MockBinary {
                        contents: TEST_BUILD_RESULT_CONTENTS.to_vec(),
                        mtime: SystemTime::now(),
                    },
                )]
                .into(),
            );

            let monitor_job_name = format!(
                "{}-{}-{}",
                DEFAULT_PIPELINE_JOB_PREFIX, TEST_REPO, &repo.arch
            );

            let monitor_map = pipeline_yaml
                .as_mapping()
                .unwrap()
                .get(&monitor_job_name.as_str().into())
                .unwrap()
                .as_mapping()
                .unwrap();

            let artifacts = monitor_map
                .get(&"artifacts".into())
                .unwrap()
                .as_mapping()
                .unwrap();
            assert_eq!(
                artifacts
                    .get(&"expire_in".into())
                    .unwrap()
                    .as_str()
                    .unwrap(),
                DEFAULT_ARTIFACT_EXPIRATION
            );

            let mut artifact_paths: Vec<_> = artifacts
                .get(&"paths".into())
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
                    &[DEFAULT_BUILD_LOG, TEST_BUILD_RESULTS_DIR]
                );
            } else {
                assert_eq!(&artifact_paths, &[DEFAULT_BUILD_LOG]);
            }

            let tags = monitor_map
                .get(&"tags".into())
                .unwrap()
                .as_sequence()
                .unwrap();
            assert_eq!(tags.len(), 1);
            assert_eq!(tags[0].as_str().unwrap(), TEST_JOB_RUNNER_TAG);

            let timeout = monitor_map
                .get(&"timeout".into())
                .unwrap()
                .as_str()
                .unwrap();
            assert_eq!(timeout, TEST_MONITOR_TIMEOUT);

            let rules: Vec<_> = monitor_map
                .get(&"rules".into())
                .unwrap()
                .as_sequence()
                .unwrap()
                .iter()
                .map(|v| v.as_mapping().unwrap())
                .collect();
            assert_eq!(rules.len(), 2);

            assert_eq!(rules[0].get(&"a".into()).unwrap().as_i64().unwrap(), 1);
            assert_eq!(rules[1].get(&"b".into()).unwrap().as_i64().unwrap(), 2);

            for script_key in ["before_script", "after_script"] {
                let script = monitor_map
                    .get(&script_key.into())
                    .unwrap()
                    .as_sequence()
                    .unwrap();
                assert_eq!(script.len(), 0);
            }

            let script = monitor_map
                .get(&"script".into())
                .unwrap()
                .as_sequence()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect::<Vec<_>>();

            let monitor = enqueue_job(
                context,
                JobSpec {
                    name: monitor_job_name.clone(),
                    dependencies: vec![dput.clone()],
                    script: script.clone(),
                    ..Default::default()
                },
            );

            if dput_test != DputTest::ReusePreviousBuild {
                // Update the endtime in the background, otherwise the monitor
                // will hang forever waiting.
                let mock = context.obs_mock.clone();
                let build_info_2 = build_info.clone();
                let repo_2 = repo.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(OLD_STATUS_SLEEP_DURATION * 10).await;
                    mock.add_job_history(
                        &build_info_2.project,
                        &repo_2.repo,
                        &repo_2.arch,
                        MockJobHistoryEntry {
                            package: build_info_2.package,
                            endtime: SystemTime::UNIX_EPOCH + Duration::from_secs(999),
                            srcmd5: build_info_2.srcmd5.unwrap(),
                            ..Default::default()
                        },
                    );
                });
            }

            assert_ok!(
                tokio::time::timeout(OLD_STATUS_SLEEP_DURATION * 20, run_obs_handler(context))
                    .await
            );
            assert_eq!(
                monitor.state(),
                if success && log_test != MonitorLogTest::Unavailable {
                    MockJobState::Success
                } else {
                    MockJobState::Failed
                }
            );

            let job_log = String::from_utf8_lossy(&monitor.log()).into_owned();

            assert_eq!(
                job_log.contains("unavailable"),
                log_test == MonitorLogTest::Unavailable
            );

            // If we reused a previous build, we're not waiting for a new build,
            // so don't check for an old build status.
            let build_actually_occurred = dput_test != DputTest::ReusePreviousBuild;
            assert_eq!(
                job_log.contains("Waiting for build status"),
                build_actually_occurred
            );

            assert_eq!(
                job_log.contains(&log_contents),
                !success && log_test == MonitorLogTest::Short
            );

            if !success && log_test == MonitorLogTest::Long {
                let log_bytes = log_contents.as_bytes();
                let truncated_log_bytes = &log_bytes[log_bytes.len() - (TEST_LOG_TAIL as usize)..];
                assert!(job_log.contains(String::from_utf8_lossy(truncated_log_bytes).as_ref()));
            }

            let results = get_job_artifacts(&monitor);
            let build_result_path = Utf8Path::new(TEST_BUILD_RESULTS_DIR)
                .join(TEST_BUILD_RESULT)
                .into_string();
            let mut has_built_result = false;

            if log_test != MonitorLogTest::Unavailable {
                let full_log = results.get(DEFAULT_BUILD_LOG).unwrap();
                assert_eq!(log_contents, String::from_utf8_lossy(full_log));

                if success && download_binaries {
                    let build_result = results.get(&build_result_path).unwrap();
                    assert_eq!(TEST_BUILD_RESULT_CONTENTS, &build_result[..]);

                    has_built_result = true;
                }
            }

            if !has_built_result {
                assert_none!(results.get(&build_result_path));
            }
        }
    }

    async fn test_prune(
        context: &mut TestContext,
        dput: MockJob,
        build_info: &ObsBuildInfo,
        only_if_job_unsuccessful: bool,
    ) {
        let prune = enqueue_job(
            context,
            JobSpec {
                name: "prune".to_owned(),
                script: vec!["prune".to_owned()],
                ..Default::default()
            },
        );

        run_obs_handler(context).await;
        assert_eq!(MockJobState::Failed, prune.state());

        let prune = enqueue_job(
            context,
            JobSpec {
                name: "prune".to_owned(),
                script: vec!["prune --ignore-missing-build-info".to_owned()],
                ..Default::default()
            },
        );

        run_obs_handler(context).await;
        assert_eq!(MockJobState::Success, prune.state());

        assert!(String::from_utf8_lossy(&prune.log()).contains("Skipping prune"));

        let prune = if only_if_job_unsuccessful {
            let prune = enqueue_job(
                context,
                JobSpec {
                    name: "prune".to_owned(),
                    dependencies: vec![dput.clone()],
                    script: vec!["echo".to_owned()],
                    after_script: vec!["prune --only-if-job-unsuccessful".to_owned()],
                    ..Default::default()
                },
            );

            run_obs_handler(context).await;
            assert_eq!(MockJobState::Success, prune.state());

            assert!(String::from_utf8_lossy(&prune.log()).contains("Skipping prune"));

            assert_ok!(
                context
                    .obs_client
                    .project(build_info.project.clone())
                    .package(TEST_PACKAGE_1.to_owned())
                    .list(None)
                    .await
            );

            enqueue_job(
                context,
                JobSpec {
                    name: "prune".to_owned(),

                    dependencies: vec![dput.clone()],
                    script: vec!["echo --fail".to_owned()],
                    after_script: vec!["prune --only-if-job-unsuccessful".to_owned()],
                    ..Default::default()
                },
            )
        } else {
            enqueue_job(
                context,
                JobSpec {
                    name: "prune".to_owned(),

                    dependencies: vec![dput.clone()],
                    script: vec!["prune".to_owned()],
                    ..Default::default()
                },
            )
        };

        run_obs_handler(context).await;
        assert_eq!(
            prune.state(),
            if only_if_job_unsuccessful {
                MockJobState::Failed
            } else {
                MockJobState::Success
            }
        );

        if build_info.is_branched {
            assert_err!(
                context
                    .obs_client
                    .project(build_info.project.clone())
                    .package(TEST_PACKAGE_1.to_owned())
                    .list(None)
                    .await
            );
        } else {
            assert!(String::from_utf8_lossy(&prune.log()).contains("package was not branched"));

            assert_ok!(
                context
                    .obs_client
                    .project(build_info.project.clone())
                    .package(TEST_PACKAGE_1.to_owned())
                    .list(None)
                    .await
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_handler_flow(
        #[future] test_context: (TestContext, GitlabLayer),
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
        let (mut context, layer) = test_context.await;
        with_tracing(layer, async {
            let (dput, build_info) = test_dput(&mut context, dput_test).await;

            test_monitoring(
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
                dput,
                &build_info,
                prune_only_if_job_unsuccessful,
            )
            .await;
        })
        .await;
    }

    #[rstest]
    #[tokio::test]
    async fn test_variable_expansion(#[future] test_context: (TestContext, GitlabLayer)) {
        let (mut context, layer) = test_context.await;
        with_tracing(layer, async {
            let job = enqueue_job(
                &context,
                JobSpec {
                    name: "expansion".to_owned(),
                    variables: [
                        ("ESCAPED".to_owned(), "this should not appear".to_owned()),
                        ("QUOTED".to_owned(), "spaces should be preserved".to_owned()),
                        ("RECURSIVE".to_owned(), "recursion($RECURSIVE)".to_owned()),
                    ]
                    .into(),
                    script: vec!["echo --sep ; $MISSING $$ESCAPED $QUOTED $RECURSIVE".to_owned()],
                    ..Default::default()
                },
            );

            run_obs_handler(&mut context).await;
            assert_eq!(job.state(), MockJobState::Success);

            let job_log = String::from_utf8_lossy(&job.log()).into_owned();
            assert_eq!(
                job_log.lines().last().unwrap(),
                ";$ESCAPED;spaces should be preserved;recursion()"
            );
        })
        .await;
    }

    #[rstest]
    #[tokio::test]
    async fn test_flag_parsing(#[future] test_context: (TestContext, GitlabLayer)) {
        let (mut context, layer) = test_context.await;
        with_tracing(layer, async {
            let job = enqueue_job(
                &context,
                JobSpec {
                    name: "flag".to_owned(),
                    script: vec!["echo --uppercase false".to_owned()],
                    ..Default::default()
                },
            );

            run_obs_handler(&mut context).await;
            assert_eq!(job.state(), MockJobState::Success);

            let job_log = String::from_utf8_lossy(&job.log()).into_owned();
            assert_eq!(job_log.lines().last().unwrap(), "FALSE");

            let job = enqueue_job(
                &context,
                JobSpec {
                    name: "flag".to_owned(),
                    script: vec!["echo --uppercase=false true".to_owned()],
                    ..Default::default()
                },
            );

            run_obs_handler(&mut context).await;
            assert_eq!(job.state(), MockJobState::Success);

            let job_log = String::from_utf8_lossy(&job.log()).into_owned();
            assert_eq!(job_log.lines().last().unwrap(), "true");

            let job = enqueue_job(
                &context,
                JobSpec {
                    name: "flag".to_owned(),
                    script: vec!["echo --uppercase=true false".to_owned()],
                    ..Default::default()
                },
            );

            run_obs_handler(&mut context).await;
            assert_eq!(job.state(), MockJobState::Success);

            let job_log = String::from_utf8_lossy(&job.log()).into_owned();
            assert_eq!(job_log.lines().last().unwrap(), "FALSE");

            let job = enqueue_job(
                &context,
                JobSpec {
                    name: "flag".to_owned(),
                    script: vec!["echo --uppercase=X false".to_owned()],
                    ..Default::default()
                },
            );

            run_obs_handler(&mut context).await;
            assert_eq!(job.state(), MockJobState::Failed);
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
        #[future] test_context: (TestContext, GitlabLayer),
        #[values(
            None,
            Some(GenerateMonitorTimeoutLocation::HandlerOption),
            Some(GenerateMonitorTimeoutLocation::Argument)
        )]
        test: Option<GenerateMonitorTimeoutLocation>,
    ) {
        const TEST_MONITOR_TIMEOUT: &str = "10 minutes";

        let (mut context, layer) = test_context.await;
        with_tracing(layer, async {
            let build_info = ObsBuildInfo {
                project: TEST_PROJECT.to_owned(),
                package: TEST_PACKAGE_1.to_owned(),
                rev: Some("1".to_owned()),
                srcmd5: Some("abc".to_owned()),
                is_branched: false,
                enabled_repos: [(
                    RepoArch {
                        repo: TEST_REPO.to_owned(),
                        arch: TEST_ARCH_1.to_owned(),
                    },
                    CommitBuildInfo {
                        prev_endtime_for_commit: None,
                    },
                )]
                .into(),
            };

            let build_info = put_artifacts(
                &mut context,
                [(
                    DEFAULT_BUILD_INFO.to_owned(),
                    serde_yaml::to_vec(&build_info).unwrap(),
                )]
                .into(),
            )
            .await;

            let mut generate_spec = JobSpec {
                name: "generate".to_owned(),
                script: vec!["generate-monitor tag".to_owned()],
                dependencies: vec![build_info],
                ..Default::default()
            };

            if test == Some(GenerateMonitorTimeoutLocation::Argument) {
                use std::fmt::Write;
                write!(
                    &mut generate_spec.script[0],
                    " --job-timeout '{TEST_MONITOR_TIMEOUT}'"
                )
                .unwrap();
            }

            let generate = enqueue_job(&context, generate_spec);

            if test == Some(GenerateMonitorTimeoutLocation::HandlerOption) {
                run_obs_handler_with_options(
                    &mut context,
                    HandlerOptions {
                        default_monitor_job_timeout: Some(TEST_MONITOR_TIMEOUT.to_owned()),
                        ..DEFAULT_HANDLER_OPTIONS
                    },
                )
                .await;
            } else {
                run_obs_handler(&mut context).await;
            }
            assert_eq!(generate.state(), MockJobState::Success);

            let results = get_job_artifacts(&generate);
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

            let timeout_yaml = monitor_map.get(&"timeout".into());
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
