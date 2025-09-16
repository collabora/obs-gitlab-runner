use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    io::{Seek, SeekFrom},
};

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use clap::{ArgAction, Parser, Subcommand};
use color_eyre::eyre::{Context, Report, Result, eyre};
use derivative::*;
use futures_util::StreamExt;
use gitlab_runner::{
    JobHandler, JobResult, Phase, UploadableFile,
    job::{Dependency, Job, Variable},
    outputln,
};
use open_build_service_api as obs;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File as AsyncFile,
    io::{AsyncSeekExt, AsyncWriteExt},
};
use tokio_util::{
    compat::{Compat, TokioAsyncReadCompatExt},
    io::ReaderStream,
};
use tracing::{debug, error, instrument, warn};

use crate::{
    artifacts::{
        ArtifactDirectory, ArtifactReader, ArtifactWriter, MissingArtifact, MissingArtifactToNone,
        SaveCallback,
    },
    binaries::download_binaries,
    build_meta::{
        BuildHistoryRetrieval, BuildMeta, BuildMetaOptions, CommitBuildInfo, DisabledRepos,
        RepoArch,
    },
    monitor::{MonitoredPackage, ObsMonitor, PackageCompletion, PackageMonitoringOptions},
    pipeline::{GeneratePipelineOptions, PipelineDownloadBinaries, generate_monitor_pipeline},
    prune::prune_branch,
    retry_request,
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

impl FlagSupportingExplicitValue for clap::Arg {
    fn flag_supporting_explicit_value(self) -> Self {
        self.num_args(0..=1)
            .require_equals(true)
            .required(false)
            .default_value("false")
            .default_missing_value("true")
            .action(ArgAction::Set)
    }
}

#[derive(Parser, Debug)]
struct DputAction {
    project: String,
    dsc: String,
    #[clap(long, default_value = "")]
    branch_to: String,
    #[clap(long, default_value_t = DEFAULT_BUILD_INFO.to_owned().into())]
    build_info_out: Utf8PathBuf,
    #[clap(long, flag_supporting_explicit_value())]
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
    #[clap(long, flag_supporting_explicit_value())]
    ignore_missing_build_info: bool,
    #[clap(long, flag_supporting_explicit_value())]
    only_if_job_unsuccessful: bool,
}

#[cfg(test)]
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
    #[instrument(skip(artifacts))]
    async fn save(self, artifacts: &mut impl ArtifactDirectory, path: &Utf8Path) -> Result<()> {
        artifacts
            .save_with(path, async |file: &mut ArtifactWriter| {
                let data =
                    serde_yaml::to_string(&self).wrap_err("Failed to serialize build info")?;
                file.write_all(data.as_bytes())
                    .await
                    .wrap_err("Failed to write build info file")?;
                Ok::<_, Report>(())
            })
            .await
    }
}

#[derive(Debug)]
struct FailedBuild;

impl std::fmt::Display for FailedBuild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
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
        let mut writer = ArtifactWriter::new().await?;
        let ret = func(&mut writer).await?;
        self.artifacts
            .insert(path.as_ref().to_owned(), writer.into_reader().await?);
        Ok(ret)
    }
}

pub struct ObsJobHandler {
    job: Job,
    client: obs::Client,
    options: HandlerOptions,

    script_failed: bool,
    artifacts: HashMap<Utf8PathBuf, ArtifactReader>,
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
        let mut artifacts = GitLabArtifacts {
            job: &self.job,
            artifacts: &mut self.artifacts,
        };

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
            &artifacts,
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
        build_info
            .clone()
            .save(&mut artifacts, &args.build_info_out)
            .await?;

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

        let result = uploader.upload_package(&artifacts).await?;

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
                retry_request!(
                    self.client
                        .project(build_info.project.clone())
                        .package(build_info.package.clone())
                        .rebuild()
                        .await
                        .wrap_err("Failed to trigger rebuild")
                )?;
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
        build_info
            .save(&mut artifacts, &args.build_info_out)
            .await?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn run_generate_monitor(&mut self, args: GenerateMonitorAction) -> Result<()> {
        let mut artifacts = GitLabArtifacts {
            job: &self.job,
            artifacts: &mut self.artifacts,
        };

        let build_info_data = artifacts.read_string(&args.build_info).await?;
        let build_info: ObsBuildInfo = serde_yaml::from_str(&build_info_data)
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
    async fn run_monitor(&mut self, args: MonitorAction) -> Result<()> {
        let mut artifacts = GitLabArtifacts {
            job: &self.job,
            artifacts: &mut self.artifacts,
        };

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

        let mut log_file = monitor
            .download_build_log(&args.build_log_out, &mut artifacts)
            .await?;

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
        let mut artifacts = GitLabArtifacts {
            job: &self.job,
            artifacts: &mut self.artifacts,
        };

        let binaries = download_binaries(
            self.client.clone(),
            &args.project,
            &args.package,
            &args.repository,
            &args.arch,
            &mut artifacts,
            &args.build_results_dir,
        )
        .await?;

        outputln!("Downloaded {} artifact(s).", binaries.paths.len());
        Ok(())
    }

    #[instrument(skip(self))]
    async fn run_prune(&mut self, args: PruneAction) -> Result<()> {
        if args.only_if_job_unsuccessful && !self.script_failed {
            outputln!("Skipping prune: main script was successful.");
            return Ok(());
        }

        let artifacts = GitLabArtifacts {
            job: &self.job,
            artifacts: &mut self.artifacts,
        };

        let build_info_data = if args.ignore_missing_build_info {
            if let Some(build_info_data) = artifacts
                .read_string(&args.build_info)
                .await
                .missing_artifact_to_none()?
            {
                build_info_data
            } else {
                outputln!(
                    "Skipping prune: build info file '{}' not found.",
                    args.build_info
                );
                return Ok(());
            }
        } else {
            artifacts.read_string(&args.build_info).await?
        };

        let build_info: ObsBuildInfo = serde_yaml::from_str(&build_info_data)
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
        cmp::Ordering,
        io::{Cursor, Read},
        sync::{Arc, Once},
        time::{Duration, SystemTime},
    };

    use camino::Utf8Path;
    use claims::*;
    use gitlab_runner::{GitlabLayer, Runner, RunnerBuilder};
    use gitlab_runner_mock::*;
    use open_build_service_mock::*;
    use rstest::rstest;
    use tempfile::TempDir;
    use tracing::{Level, instrument::WithSubscriber};
    use tracing_subscriber::{Layer, Registry, filter::Targets, prelude::*};
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

    async fn with_context<T>(func: impl AsyncFnOnce(TestContext) -> T) -> T {
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

        let ctx = TestContext {
            _runner_dir: runner_dir,
            gitlab_mock,
            runner,
            obs_mock,
            obs_client,
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

    async fn run_handler<H, U, Func>(context: &mut TestContext, handler_func: Func)
    where
        U: UploadableFile + Send + 'static,
        H: JobHandler<U> + Send + 'static,
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
        run_handler(context, |_| PutArtifactsHandler {
            artifacts: Arc::new(artifacts),
        })
        .await;
        artifacts_job
    }

    fn get_job_artifacts(job: &MockJob) -> HashMap<String, Vec<u8>> {
        let Some(artifact) = job.uploaded_artifacts().next() else {
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

        let mut dput_command = format!("dput {TEST_PROJECT} {dsc1_file}");
        let mut created_project = TEST_PROJECT.to_owned();

        if test == DputTest::Branch {
            created_project += ":branched";
            dput_command += &format!(" --branch-to {created_project}");
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
            "generate-monitor {TEST_JOB_RUNNER_TAG} \
                --job-timeout '{TEST_MONITOR_TIMEOUT}' \
                --rules '[{{a: 1}}, {{b: 2}}]'"
        );
        if download_binaries {
            generate_command += &format!(" --download-build-results-to {TEST_BUILD_RESULTS_DIR}");
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
                    &[DEFAULT_BUILD_LOG, TEST_BUILD_RESULTS_DIR]
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
    async fn test_variable_expansion() {
        with_context(async |mut context| {
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
    async fn test_flag_parsing() {
        with_context(async |mut context| {
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
                    serde_yaml::to_string(&build_info).unwrap().into_bytes(),
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
