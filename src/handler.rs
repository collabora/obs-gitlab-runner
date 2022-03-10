use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fs::File,
    io::SeekFrom,
};

use async_trait::async_trait;
use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use color_eyre::eyre::{Context, Result};
use futures_util::StreamExt;
use gitlab_runner::{job::Job, outputln, uploader::Uploader, JobHandler, JobResult, Phase};
use open_build_service_api as obs;
use serde::{Deserialize, Serialize};
use tokio::{fs::File as AsyncFile, io::AsyncSeekExt};
use tokio_util::{compat::FuturesAsyncWriteCompatExt, io::ReaderStream};
use tracing::{debug, error, instrument};

use crate::{
    artifacts::{save_to_tempfile, ArtifactDirectory},
    binaries::download_binaries,
    cleanup::cleanup_branch,
    monitor::{ObsMonitor, PackageCompletion, PackageMonitoringOptions},
    pipeline::{generate_monitor_pipeline, GeneratePipelineOptions},
    upload::ObsUploader,
};

const DEFAULT_BUILD_INFO: &str = "build-info.json";
const DEFAULT_MONITOR_PIPELINE: &str = "obs-monitor.yml";
const DEFAULT_PIPELINE_JOB_PREFIX: &str = "obs";
const DEFAULT_BUILD_RESULTS_DIR: &str = "results";
const DEFAULT_BUILD_LOG: &str = "build.log";

#[derive(Parser, Debug)]
struct UploadAction {
    project: String,
    dsc: String,
    #[clap(long, default_value = "")]
    branch_to: String,
    #[clap(long, default_value_t = DEFAULT_BUILD_INFO.to_owned())]
    build_info_out: String,
}

#[derive(Parser, Debug)]
struct GenerateMonitorAction {
    #[clap(long, default_value_t = DEFAULT_BUILD_INFO.to_owned())]
    build_info: String,
    #[clap(long, default_value_t = DEFAULT_MONITOR_PIPELINE.to_owned())]
    pipeline_out: String,
    #[clap(long)]
    mixin: Option<String>,
    #[clap(long, default_value_t = DEFAULT_PIPELINE_JOB_PREFIX.to_owned())]
    job_prefix: String,
    #[clap(long, default_value_t = DEFAULT_BUILD_RESULTS_DIR.into())]
    build_results_dir: Utf8PathBuf,
    #[clap(long, default_value_t = DEFAULT_BUILD_LOG.into())]
    build_log_out: String,
}

#[derive(Parser, Debug)]
struct MonitorAction {
    repository: String,
    arch: String,
    #[clap(long, default_value_t = DEFAULT_BUILD_INFO.to_owned())]
    build_info: String,
    #[clap(long, default_value_t = DEFAULT_BUILD_RESULTS_DIR.into())]
    build_results_dir: Utf8PathBuf,
    #[clap(long, default_value_t = DEFAULT_BUILD_LOG.into())]
    build_log_out: String,
}

#[derive(Parser, Debug)]
struct CleanupAction {
    #[clap(long, default_value_t = DEFAULT_BUILD_INFO.to_owned())]
    build_info: String,
    #[clap(long)]
    ignore_missing_build_info: bool,
    #[clap(long)]
    only_if_job_unsuccessful: bool,
}

#[derive(Subcommand)]
enum Action {
    Upload(UploadAction),
    GenerateMonitor(GenerateMonitorAction),
    Monitor(MonitorAction),
    Cleanup(CleanupAction),
}

#[derive(Parser)]
struct Command {
    #[clap(subcommand)]
    action: Action,
}

#[derive(Serialize, Deserialize)]
struct ObsBuildInfo {
    project: String,
    package: String,
    rev: String,
    is_branched: bool,
}

pub struct ObsJobHandler {
    job: Job,
    client: obs::Client,

    script_failed: bool,
    artifacts: HashMap<String, AsyncFile>,
}

impl ObsJobHandler {
    pub fn new(job: Job, client: obs::Client) -> Self {
        ObsJobHandler {
            job,
            client,
            script_failed: false,
            artifacts: HashMap::new(),
        }
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
    async fn run_upload(&mut self, args: UploadAction) -> Result<()> {
        let branch_to = if !args.branch_to.is_empty() {
            Some(args.branch_to)
        } else {
            None
        };

        let uploader = ObsUploader::new(self.client.clone(), args.project.clone());
        let result = uploader
            .upload_package(args.dsc.as_str().into(), branch_to.as_deref(), self)
            .await?;

        if result.unchanged {
            outputln!("Package unchanged at revision {}", result.rev);

            // TODO: control rebuild triggers via flag
            self.client
                .project(result.project.clone())
                .package(result.package.clone())
                .rebuild()
                .await
                .wrap_err("Failed to trigger rebuild")?;
        } else {
            outputln!("Package uploaded with revision {}", result.rev);
        }

        // TODO: filter out disabled repos (technically we can just catch those
        // in the monitor though?)
        let build_info = ObsBuildInfo {
            project: result.project,
            package: result.package,
            rev: result.rev,
            is_branched: branch_to.is_some(),
        };

        let mut build_info_file =
            tempfile::tempfile().wrap_err("Failed to create build info file")?;
        serde_json::to_writer(&mut build_info_file, &build_info)
            .wrap_err("Failed to write build info file")?;

        self.artifacts
            .insert(args.build_info_out, AsyncFile::from_std(build_info_file));

        Ok(())
    }

    #[instrument(skip(self))]
    async fn run_generate_monitor(&mut self, args: GenerateMonitorAction) -> Result<()> {
        let build_info_data = self.get_data(&args.build_info).await?;
        let build_info: ObsBuildInfo = serde_json::from_slice(&build_info_data[..])
            .wrap_err("Failed to parse provided build info file")?;

        let file = generate_monitor_pipeline(
            self.client.clone(),
            &build_info.project,
            &args.build_info,
            GeneratePipelineOptions {
                build_results_dir: args.build_results_dir.to_string(),
                prefix: args.job_prefix,
                mixin: args.mixin,
            },
        )
        .await?;
        self.artifacts
            .insert(args.pipeline_out.clone(), AsyncFile::from_std(file));

        outputln!("Wrote {}", args.pipeline_out);

        Ok(())
    }

    #[instrument(skip(self))]
    async fn run_monitor(&mut self, args: MonitorAction) -> Result<()> {
        const LOG_TAIL_2MB: u64 = 2 * 1024 * 1024;

        let build_info_data = self.get_data(&args.build_info).await?;
        let build_info: ObsBuildInfo = serde_json::from_slice(&build_info_data[..])
            .wrap_err("Failed to parse provided build info file")?;

        let monitor = ObsMonitor::new(
            self.client.clone(),
            build_info.project.clone(),
            build_info.package.clone(),
            args.repository.clone(),
            args.arch.clone(),
            build_info.rev,
        )
        .await?;

        let completion = monitor
            .monitor_package(PackageMonitoringOptions::default())
            .await?;
        debug!("Completed with: {:?}", completion);

        let mut log_file = monitor.download_build_log().await?;
        self.artifacts.insert(
            args.build_log_out,
            log_file
                .file
                .try_clone()
                .await
                .wrap_err("Failed to clone log file")?,
        );

        match completion {
            PackageCompletion::Succeeded => {
                outputln!("Build succeeded!");

                let binaries = download_binaries(
                    self.client.clone(),
                    &build_info.project,
                    &build_info.package,
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
            }
            PackageCompletion::Superceded => {
                outputln!("Build was superceded by a newer revision");
            }
            PackageCompletion::Disabled => {
                outputln!("Package is disabled for this architecture");
            }
            PackageCompletion::Failed(reason) => {
                log_file
                    .file
                    .seek(SeekFrom::End(
                        -(std::cmp::min(LOG_TAIL_2MB, log_file.len) as i64),
                    ))
                    .await
                    .wrap_err("Failed to find length of log file")?;

                let mut log_stream = ReaderStream::new(log_file.file);
                while let Some(bytes) = log_stream.next().await {
                    let bytes = bytes.wrap_err("Failed to stream log bytes")?;
                    self.job.trace(String::from_utf8_lossy(&bytes).as_ref());
                }

                self.job.trace("\n\n(last <=2MB of logs printed above)\n");
                outputln!("Build failed with reason '{:?}'", reason);
            }
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn run_cleanup(&mut self, args: CleanupAction) -> Result<()> {
        if args.only_if_job_unsuccessful && !self.script_failed {
            outputln!("Skipping cleanup: main script was successful");
            return Ok(());
        }

        let build_info_data = if args.ignore_missing_build_info {
            if let Some(build_info_data) = self.get_data_or_none(&args.build_info).await? {
                build_info_data
            } else {
                outputln!(
                    "Skipping cleanup: build info file '{}' not found",
                    args.build_info
                );
                return Ok(());
            }
        } else {
            self.get_data(&args.build_info).await?
        };

        let build_info: ObsBuildInfo = serde_json::from_slice(&build_info_data[..])
            .wrap_err("Failed to parse provided build info file")?;

        if build_info.is_branched {
            outputln!(
                "Cleaning up branched package {}/{}",
                build_info.project,
                build_info.package
            );
            cleanup_branch(
                &self.client,
                &build_info.project,
                &build_info.package,
                &build_info.rev,
            )
            .await?;
        } else {
            outputln!("Skipping cleanup: package was not branched");
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn command(&mut self, cmdline: &str) -> Result<()> {
        // TODO: inject user?
        let cmdline = self.expand_vars(cmdline, true, &mut HashSet::new());

        outputln!("> {}", cmdline);

        let mut args = shell_words::split(&cmdline).wrap_err("Invalid command line")?;
        // Insert the "program name" at argv[0].
        args.insert(0, "obs-gitlab-runner".to_owned());
        let command = Command::try_parse_from(args)?;

        match command.action {
            Action::Upload(args) => self.run_upload(args).await?,
            Action::GenerateMonitor(args) => self.run_generate_monitor(args).await?,
            Action::Monitor(args) => self.run_monitor(args).await?,
            Action::Cleanup(args) => self.run_cleanup(args).await?,
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
                self.script_failed = true;
                error!(gitlab.output = true, "Error running command: {:?}", err);
                return Err(());
            }
        }

        Ok(())
    }

    async fn upload_artifacts(&mut self, uploader: &mut Uploader) -> JobResult {
        let mut success = true;

        for (name, mut file) in &mut self.artifacts {
            if let Err(err) = upload_artifact(name.clone(), &mut file, uploader).await {
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
            // TODO: setup span
            // TODO: not spawn a sync environment for *every single artifact*

            // Needed because anything captured by spawn_blocking must have a
            // 'static lifetime.
            let filename = filename.to_owned();

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
