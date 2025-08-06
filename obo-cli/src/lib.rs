use std::time::Duration;

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Args, Subcommand};
use color_eyre::eyre::{Context, Report, Result, bail, eyre};
use obo_core::{
    actions::{
        Actions, DEFAULT_BUILD_INFO, DEFAULT_BUILD_LOG, DownloadBinariesAction, DputAction,
        LOG_TAIL_2MB, MonitorAction, ObsBuildInfo, PruneAction,
    },
    artifacts::{ArtifactDirectory, ArtifactReader, ArtifactWriter, MissingArtifact, SaveCallback},
    build_meta::RepoArch,
    monitor::PackageMonitoringOptions,
    outputln,
};
use open_build_service_api as obs;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::{
    fs::File as AsyncFile,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};

pub const DEFAULT_MONITOR_TABLE: &str = "obs-monitor.json";

#[derive(Debug, Deserialize, Serialize)]
pub struct MonitorCommands {
    pub monitor: String,
    pub download_binaries: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MonitorEntry {
    #[serde(flatten)]
    pub repo_arch: RepoArch,
    pub commands: MonitorCommands,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MonitorTable {
    pub entries: Vec<MonitorEntry>,
}

#[derive(Args)]
pub struct GenerateMonitorAction {
    #[clap(long, default_value_t = DEFAULT_BUILD_INFO.to_owned())]
    build_info: String,
    #[clap(long, default_value_t = DEFAULT_MONITOR_TABLE.to_owned())]
    monitor_out: String,
    #[clap(long, default_value_t = DEFAULT_BUILD_LOG.into())]
    build_log_out: String,
    #[clap(long = "download-build-results-to")]
    build_results_dir: Option<Utf8PathBuf>,
}

#[derive(Subcommand)]
pub enum CliAction {
    Dput(DputAction),
    Monitor {
        #[clap(flatten)]
        args: MonitorAction,

        // These are needed by the integration tests.
        #[clap(long, hide = true, env = "OBO_TEST_LOG_TAIL", default_value_t = LOG_TAIL_2MB)]
        log_tail: u64,
        #[clap(long, hide = true, env = "OBO_TEST_SLEEP_ON_BUILDING_MS")]
        sleep_on_building_ms: Option<u64>,
        #[clap(long, hide = true, env = "OBO_TEST_SLEEP_ON_OLD_STATUS_MS")]
        sleep_on_old_status_ms: Option<u64>,
    },
    GenerateMonitor(GenerateMonitorAction),
    DownloadBinaries(DownloadBinariesAction),
    Prune(PruneAction),
}

#[derive(Default)]
pub struct LocalFsArtifacts(pub Utf8PathBuf);

#[async_trait]
impl ArtifactDirectory for LocalFsArtifacts {
    async fn open(&self, path: impl AsRef<Utf8Path> + Send) -> Result<ArtifactReader> {
        let path = self.0.join(path.as_ref());
        AsyncFile::open(&path)
            .await
            .map(ArtifactReader::new)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    eyre!(MissingArtifact(path))
                } else {
                    eyre!(e)
                }
            })
    }

    async fn save_with<Ret, Err, F, P>(&mut self, path: P, func: F) -> Result<Ret>
    where
        Report: From<Err>,
        Ret: Send,
        Err: Send,
        F: for<'a> SaveCallback<'a, Ret, Err> + Send,
        P: AsRef<Utf8Path> + Send,
    {
        let path = self.0.join(path.as_ref());
        let parent = path.parent().unwrap_or_else(|| Utf8Path::new("."));
        tokio::fs::create_dir_all(&parent)
            .await
            .wrap_err_with(|| format!("Failed to create parents of '{path}'"))?;

        let Some(basename) = path.file_name() else {
            bail!("Invalid path: {path}");
        };
        let temp = NamedTempFile::with_prefix_in(basename, parent)
            .wrap_err("Failed to create temporary file")?;

        let mut writer = ArtifactWriter::new(AsyncFile::from_std(temp.as_file().try_clone()?));
        let ret = func(&mut writer).await?;

        writer.flush().await?;
        temp.persist(&path)?;
        Ok(ret)
    }
}

pub struct Handler {
    actions: Actions,
    artifacts: LocalFsArtifacts,
}

impl Handler {
    pub fn new(client: obs::Client, artifacts_dir: Utf8PathBuf) -> Self {
        Self {
            actions: Actions { client },
            artifacts: LocalFsArtifacts(artifacts_dir),
        }
    }

    async fn generate_monitor(&mut self, args: GenerateMonitorAction) -> Result<()> {
        let build_info_data = self.artifacts.read_string(&args.build_info).await?;
        let build_info: ObsBuildInfo = serde_json::from_str(&build_info_data)
            .wrap_err("Failed to parse provided build info file")?;

        let rev = build_info
            .rev
            .ok_or_else(|| eyre!("Build revision was not set"))?;
        let srcmd5 = build_info
            .srcmd5
            .ok_or_else(|| eyre!("Build srcmd5 was not set"))?;

        let mut table = MonitorTable { entries: vec![] };
        for enabled_repo in build_info.enabled_repos {
            table.entries.push(MonitorEntry {
                repo_arch: enabled_repo.repo_arch.clone(),
                commands: MonitorCommands {
                    monitor: MonitorAction {
                        project: build_info.project.clone(),
                        package: build_info.package.clone(),
                        repository: enabled_repo.repo_arch.repo.clone(),
                        arch: enabled_repo.repo_arch.arch.clone(),
                        rev: rev.clone(),
                        srcmd5: srcmd5.clone(),
                        prev_endtime_for_commit: enabled_repo.prev_endtime_for_commit,
                        build_log_out: args.build_log_out.clone(),
                    }
                    .generate_command(),
                    download_binaries: args.build_results_dir.clone().map(|build_results_dir| {
                        DownloadBinariesAction {
                            project: build_info.project.clone(),
                            package: build_info.package.clone(),
                            repository: enabled_repo.repo_arch.repo,
                            arch: enabled_repo.repo_arch.arch,
                            build_results_dir,
                        }
                        .generate_command()
                    }),
                },
            });
        }

        let data = serde_json::to_string(&table).wrap_err("Failed to serialize data")?;

        self.artifacts
            .write(&args.monitor_out, data.as_bytes())
            .await?;
        outputln!("Wrote monitor file '{}'.", args.monitor_out);

        Ok(())
    }

    pub async fn run(&mut self, action: CliAction) -> Result<()> {
        match action {
            CliAction::Dput(args) => self.actions.dput(args, &mut self.artifacts).await?,
            CliAction::Monitor {
                log_tail,
                sleep_on_building_ms,
                sleep_on_old_status_ms,
                args,
            } => {
                let mut options = PackageMonitoringOptions::default();
                if let Some(value) = sleep_on_building_ms {
                    options.sleep_on_building = Duration::from_millis(value);
                }
                if let Some(value) = sleep_on_old_status_ms {
                    options.sleep_on_old_status = Duration::from_millis(value);
                }

                self.actions
                    .monitor(
                        args,
                        options,
                        |file| async {
                            let mut lines = BufReader::new(file).lines();
                            while let Some(line) = lines.next_line().await? {
                                eprintln!("{line}");
                            }
                            Ok(())
                        },
                        log_tail,
                        &mut self.artifacts,
                    )
                    .await?
            }
            CliAction::GenerateMonitor(args) => self.generate_monitor(args).await?,
            CliAction::DownloadBinaries(args) => {
                self.actions
                    .download_binaries(args, &mut self.artifacts)
                    .await?
            }
            CliAction::Prune(args) => self.actions.prune(args, &self.artifacts).await?,
        }

        Ok(())
    }
}
