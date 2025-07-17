use std::{collections::HashMap, io::SeekFrom};

use camino::{Utf8Path, Utf8PathBuf};
use clap::{ArgAction, Parser};
use color_eyre::eyre::{Context, Report, Result};
use open_build_service_api as obs;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, instrument};

use crate::{
    artifacts::{ArtifactDirectory, ArtifactReader, ArtifactWriter, MissingArtifactToNone},
    binaries::download_binaries,
    build_meta::{
        BuildHistoryRetrieval, BuildMeta, BuildMetaOptions, CommitBuildInfo, DisabledRepos,
        RepoArch,
    },
    monitor::{MonitoredPackage, ObsMonitor, PackageCompletion, PackageMonitoringOptions},
    outputln,
    prune::prune_branch,
    retry_request,
    upload::ObsDscUploader,
};

pub const DEFAULT_BUILD_INFO: &str = "build-info.yml";
pub const DEFAULT_BUILD_LOG: &str = "build.log";

// Our flags can all take explicit values, because it makes it easier to
// conditionally set things in the pipelines.
pub trait FlagSupportingExplicitValue {
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

#[derive(Debug)]
struct CommandBuilder {
    args: Vec<String>,
}

impl CommandBuilder {
    fn new(name: String) -> Self {
        Self { args: vec![name] }
    }

    fn add(&mut self, arg: &str, value: &str) -> &mut Self {
        self.args
            .push(format!("--{arg}={}", shell_words::quote(value)));
        self
    }

    fn build(self) -> String {
        self.args.join(" ")
    }
}

#[derive(Parser, Debug)]
pub struct DputAction {
    pub project: String,
    pub dsc: String,
    #[clap(long, default_value = "")]
    pub branch_to: String,
    #[clap(long, default_value_t = DEFAULT_BUILD_INFO.to_owned().into())]
    pub build_info_out: Utf8PathBuf,
    #[clap(long, flag_supporting_explicit_value())]
    pub rebuild_if_unchanged: bool,
}

#[derive(Parser, Debug)]
pub struct MonitorAction {
    #[clap(long)]
    pub project: String,
    #[clap(long)]
    pub package: String,
    #[clap(long)]
    pub rev: String,
    #[clap(long)]
    pub srcmd5: String,
    #[clap(long)]
    pub repository: String,
    #[clap(long)]
    pub arch: String,
    #[clap(long)]
    pub prev_endtime_for_commit: Option<u64>,
    #[clap(long)]
    pub build_log_out: String,
}

impl MonitorAction {
    pub fn generate_command(&self) -> String {
        let mut builder = CommandBuilder::new("monitor".to_owned());
        builder
            .add("project", &self.project)
            .add("package", &self.package)
            .add("rev", &self.rev)
            .add("srcmd5", &self.srcmd5)
            .add("repository", &self.repository)
            .add("arch", &self.arch)
            .add("build-log-out", &self.build_log_out);
        if let Some(endtime) = &self.prev_endtime_for_commit {
            builder.add("prev-endtime-for-commit", &endtime.to_string());
        }
        builder.build()
    }
}

#[derive(Parser, Debug)]
pub struct DownloadBinariesAction {
    #[clap(long)]
    pub project: String,
    #[clap(long)]
    pub package: String,
    #[clap(long)]
    pub repository: String,
    #[clap(long)]
    pub arch: String,
    #[clap(long)]
    pub build_results_dir: Utf8PathBuf,
}

impl DownloadBinariesAction {
    pub fn generate_command(&self) -> String {
        let mut builder = CommandBuilder::new("download-binaries".to_owned());
        builder
            .add("project", &self.project)
            .add("package", &self.package)
            .add("repository", &self.repository)
            .add("arch", &self.arch)
            .add("build-results-dir", self.build_results_dir.as_str());
        builder.build()
    }
}

#[derive(Parser, Debug)]
pub struct PruneAction {
    #[clap(long, default_value_t = DEFAULT_BUILD_INFO.to_owned())]
    pub build_info: String,
    #[clap(long, flag_supporting_explicit_value())]
    pub ignore_missing_build_info: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObsBuildInfo {
    pub project: String,
    pub package: String,
    pub rev: Option<String>,
    pub srcmd5: Option<String>,
    pub is_branched: bool,
    pub enabled_repos: HashMap<RepoArch, CommitBuildInfo>,
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

#[derive(Debug, thiserror::Error)]
#[error("Failed build")]
pub struct FailedBuild;

pub const LOG_TAIL_2MB: u64 = 2 * 1024 * 1024;

pub struct Actions {
    pub client: obs::Client,
}

impl Actions {
    #[instrument(skip_all, fields(args))]
    pub async fn dput(
        &mut self,
        args: DputAction,
        artifacts: &mut impl ArtifactDirectory,
    ) -> Result<()> {
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
            artifacts,
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
            .save(artifacts, &args.build_info_out)
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

        let result = uploader.upload_package(artifacts).await?;

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
        build_info.save(artifacts, &args.build_info_out).await?;

        Ok(())
    }

    #[instrument(skip_all, fields(args))]
    pub async fn monitor<F: Future<Output = Result<()>> + Send>(
        &mut self,
        args: MonitorAction,
        monitoring_options: PackageMonitoringOptions,
        log_tail_cb: impl FnOnce(ArtifactReader) -> F,
        log_tail_bytes: u64,
        artifacts: &mut impl ArtifactDirectory,
    ) -> Result<()> {
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

        let completion = monitor.monitor_package(monitoring_options).await?;
        debug!("Completed with: {:?}", completion);

        let mut log_file = monitor
            .download_build_log(&args.build_log_out, artifacts)
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
                        -(std::cmp::min(log_tail_bytes, log_file.len) as i64),
                    ))
                    .await
                    .wrap_err("Failed to find length of log file")?;

                log_tail_cb(log_file.file).await?;

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

    #[instrument(skip_all, fields(args))]
    pub async fn download_binaries(
        &mut self,
        args: DownloadBinariesAction,
        actions: &mut impl ArtifactDirectory,
    ) -> Result<()> {
        let binaries = download_binaries(
            self.client.clone(),
            &args.project,
            &args.package,
            &args.repository,
            &args.arch,
            actions,
            &args.build_results_dir,
        )
        .await?;

        outputln!("Downloaded {} artifact(s).", binaries.paths.len());
        Ok(())
    }

    #[instrument(skip_all, fields(args))]
    pub async fn prune(
        &mut self,
        args: PruneAction,
        artifacts: &impl ArtifactDirectory,
    ) -> Result<()> {
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
}
