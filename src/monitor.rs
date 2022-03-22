use std::time::Duration;

use color_eyre::eyre::{ensure, eyre, Context, Report, Result};
use derivative::*;
use futures_util::stream::StreamExt;
use gitlab_runner::outputln;
use open_build_service_api as obs;
use tokio::{
    fs::File as AsyncFile,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use tracing::instrument;

use crate::retry::{retry_large_request, retry_request};

#[derive(Debug)]
pub enum PackageCompletion {
    Succeeded,
    Superceded,
    Failed(obs::PackageCode),
    Disabled,
}

#[derive(Debug)]
enum PackageBuildState {
    PendingStatusPosted,
    Dirty,
    Building(obs::PackageCode),
    Completed(PackageCompletion),
}

#[derive(Debug)]
pub struct LogFile {
    pub file: AsyncFile,
    pub len: u64,
}

#[derive(Clone, Debug)]
pub struct MonitoredPackage {
    pub project: String,
    pub package: String,
    pub repository: String,
    pub arch: String,
    pub rev: String,
    pub srcmd5: String,
    pub prev_bcnt_for_commit: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PackageMonitoringOptions {
    pub sleep_on_building: Duration,
    pub sleep_on_dirty: Duration,
    pub sleep_on_old_status: Duration,
}

impl Default for PackageMonitoringOptions {
    fn default() -> Self {
        PackageMonitoringOptions {
            sleep_on_building: Duration::from_secs(10),
            sleep_on_dirty: Duration::from_secs(30),
            sleep_on_old_status: Duration::from_secs(15),
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ObsMonitor {
    #[derivative(Debug = "ignore")]
    client: obs::Client,

    package: MonitoredPackage,
}

impl ObsMonitor {
    pub fn new(client: obs::Client, package: MonitoredPackage) -> ObsMonitor {
        ObsMonitor { client, package }
    }

    #[instrument(skip(self))]
    async fn get_latest_revision(&self) -> Result<String> {
        let dir = retry_request(|| async {
            self.client
                .project(self.package.project.clone())
                .package(self.package.package.clone())
                .list(None)
                .await
        })
        .await?;
        dir.rev.ok_or_else(|| eyre!("Latest revision is 0"))
    }

    #[instrument(skip(self))]
    async fn get_latest_state(&self) -> Result<PackageBuildState> {
        let latest_rev = self.get_latest_revision().await?;
        if latest_rev != self.package.rev {
            return Ok(PackageBuildState::Completed(PackageCompletion::Superceded));
        }

        let clinet_package = self
            .client
            .project(self.package.project.clone())
            .package(self.package.package.clone());

        let all_results = retry_request(|| async { clinet_package.result().await }).await?;

        // TODO: filter this in the API call instead of afterwards
        let result = all_results
            .results
            .into_iter()
            .find(|r| r.arch == self.package.arch)
            .ok_or_else(|| {
                eyre!(
                    "Failed to find results for architecture {}",
                    self.package.arch
                )
            })?;
        if result.dirty {
            return Ok(PackageBuildState::Dirty);
        }

        let status = match result.get_status(&self.package.package) {
            Some(status) => status,
            // There is a small gap after a commit for a new package where its
            // status isn't available yet.
            None => return Ok(PackageBuildState::PendingStatusPosted),
        };

        if status.dirty {
            Ok(PackageBuildState::Dirty)
        } else if status.code.is_final() {
            // Similarly to above, there is a small gap after a commit where the
            // previous build status is still posted. To ensure the build that's
            // now final is actually our own, check the build history to make
            // sure there is a build *newer* that the last bcnt we have
            // recorded.

            if let Some(prev_bcnt_for_commit) = &self.package.prev_bcnt_for_commit {
                let history = retry_request(|| async {
                    clinet_package
                        .history(&self.package.repository, &self.package.arch)
                        .await
                })
                .await
                .wrap_err("Failed to get history")?;
                if history.entries.last().map(|e| &e.bcnt) == Some(prev_bcnt_for_commit) {
                    return Ok(PackageBuildState::PendingStatusPosted);
                }
            }

            Ok(PackageBuildState::Completed(match status.code {
                obs::PackageCode::Disabled | obs::PackageCode::Excluded => {
                    PackageCompletion::Disabled
                }
                obs::PackageCode::Succeeded => PackageCompletion::Succeeded,
                code => PackageCompletion::Failed(code),
            }))
        } else {
            Ok(PackageBuildState::Building(status.code))
        }
    }

    #[instrument(skip(self, content))]
    fn check_log_md5(&self, content: &str) -> Result<()> {
        let needle = format!("srcmd5 '{}'", self.package.srcmd5);
        ensure!(
            content.contains(&needle),
            "Build logs are unavailable (overwritten by a later build revision?)"
        );

        Ok(())
    }

    #[instrument]
    pub async fn monitor_package(
        &self,
        options: PackageMonitoringOptions,
    ) -> Result<PackageCompletion> {
        let mut log_url = self.client.url().clone();
        log_url
            .path_segments_mut()
            .map_err(|_| eyre!("Failed to modify log URL"))?
            .push("package")
            .push("live_build_log")
            .push(&self.package.project)
            .push(&self.package.package)
            .push(&self.package.repository)
            .push(&self.package.arch);

        outputln!("Live build log: {}", log_url);

        let mut previous_code = None;

        loop {
            match self.get_latest_state().await? {
                PackageBuildState::Building(code) => {
                    if previous_code != Some(code) {
                        if previous_code.is_some() {
                            outputln!("Build status is now '{}'...", code);
                        } else {
                            outputln!("Monitoring build, current status is '{}'...", code);
                        }
                        previous_code = Some(code);
                    }

                    tokio::time::sleep(options.sleep_on_building).await;
                }
                PackageBuildState::Dirty => {
                    outputln!("Package is dirty, trying again later...");
                    tokio::time::sleep(options.sleep_on_dirty).await;
                }
                PackageBuildState::PendingStatusPosted => {
                    outputln!("Old build status still posted, trying again later...");
                    tokio::time::sleep(options.sleep_on_old_status).await;
                }
                PackageBuildState::Completed(reason) => {
                    return Ok(reason);
                }
            }
        }
    }

    #[instrument]
    pub async fn download_build_log(&self) -> Result<LogFile> {
        const LOG_LEN_TO_CHECK_FOR_MD5: u64 = 2500;

        let mut file = retry_large_request(|| async {
            let mut file = AsyncFile::from_std(
                tempfile::tempfile().wrap_err("Failed to create tempfile to build log")?,
            );
            let mut stream = self
                .client
                .project(self.package.project.clone())
                .package(self.package.package.clone())
                .log(&self.package.repository, &self.package.arch)
                .stream(obs::PackageLogStreamOptions::default())?;

            while let Some(bytes) = stream.next().await {
                let bytes = bytes?;
                file.write_all(&bytes)
                    .await
                    .wrap_err("Failed to download build log")?;
            }

            Ok::<AsyncFile, Report>(file)
        })
        .await?;

        let len = file
            .stream_position()
            .await
            .wrap_err("Failed to find stream position")?;
        file.rewind().await.wrap_err("Failed to rewind file")?;

        let mut buf = vec![0; std::cmp::min(LOG_LEN_TO_CHECK_FOR_MD5, len) as usize];
        file.read_exact(&mut buf)
            .await
            .wrap_err("Failed to read start of logs")?;
        self.check_log_md5(&String::from_utf8_lossy(&buf))?;

        file.rewind().await.wrap_err("Failed to rewind file")?;
        Ok(LogFile { file, len })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use claim::*;
    use obs::PackageCode;
    use open_build_service_mock::*;

    use crate::test_support::*;

    use super::*;

    #[tokio::test]
    async fn test_srcmd5_checks() {
        let srcmd5 = random_md5();
        let branch_srcmd5 = random_md5();
        let branch_xsrcmd5 = random_md5();

        let mock = create_default_mock().await;

        mock.add_project(TEST_PROJECT.to_owned());
        mock.add_new_package(
            TEST_PROJECT,
            TEST_PACKAGE_1.to_owned(),
            MockPackageOptions::default(),
        );

        mock.add_package_revision(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockRevisionOptions {
                srcmd5: srcmd5.clone(),
                ..Default::default()
            },
            HashMap::new(),
        );

        let client = create_default_client(&mock);
        let monitor = ObsMonitor::new(
            client.clone(),
            MonitoredPackage {
                project: TEST_PROJECT.to_owned(),
                package: TEST_PACKAGE_1.to_owned(),
                repository: TEST_REPO.to_owned(),
                arch: TEST_ARCH_1.to_owned(),
                rev: "1".to_owned(),
                srcmd5: srcmd5.clone(),
                prev_bcnt_for_commit: None,
            },
        );

        assert_ok!(monitor.check_log_md5(&format!("srcmd5 '{}'", srcmd5)));
        assert_err!(monitor.check_log_md5(&format!("srcmd5 'xyz123'")));
        assert_err!(monitor.check_log_md5(&format!("'{}'", srcmd5)));

        mock.branch(
            TEST_PROJECT.to_owned(),
            TEST_PACKAGE_1.to_owned(),
            TEST_PROJECT,
            TEST_PACKAGE_2.to_owned(),
            MockBranchOptions {
                srcmd5: branch_srcmd5.clone(),
                xsrcmd5: branch_xsrcmd5.clone(),
                ..Default::default()
            },
        );

        let monitor = ObsMonitor::new(
            client,
            MonitoredPackage {
                project: TEST_PROJECT.to_owned(),
                package: TEST_PACKAGE_2.to_owned(),
                repository: TEST_REPO.to_owned(),
                arch: TEST_ARCH_1.to_owned(),
                rev: "1".to_owned(),
                srcmd5: branch_xsrcmd5.clone(),
                prev_bcnt_for_commit: None,
            },
        );

        assert_ok!(monitor.check_log_md5(&format!("srcmd5 '{}'", branch_xsrcmd5)));
        assert_err!(monitor.check_log_md5(&format!("srcmd5 '{}'", srcmd5)));
        assert_err!(monitor.check_log_md5(&format!("srcmd5 '{}'", branch_srcmd5)));
    }

    #[tokio::test]
    async fn test_download_log() {
        let srcmd5 = random_md5();
        let log_content = format!(
            "srcmd5 '{}'\n
                some random logs are here
                testing 123 456",
            srcmd5
        );

        let mock = create_default_mock().await;

        mock.add_project(TEST_PROJECT.to_owned());
        mock.add_new_package(
            TEST_PROJECT,
            TEST_PACKAGE_1.to_owned(),
            MockPackageOptions::default(),
        );

        mock.add_package_revision(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockRevisionOptions {
                srcmd5: srcmd5.clone(),
                ..Default::default()
            },
            HashMap::new(),
        );

        mock.add_or_update_repository(
            TEST_PROJECT,
            TEST_REPO.to_owned(),
            TEST_ARCH_1.to_owned(),
            MockRepositoryCode::Unknown,
        );

        mock.add_completed_build_log(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildLog::new(log_content.to_owned()),
            true,
        );

        let client = create_default_client(&mock);
        let monitor = ObsMonitor::new(
            client,
            MonitoredPackage {
                project: TEST_PROJECT.to_owned(),
                package: TEST_PACKAGE_1.to_owned(),
                repository: TEST_REPO.to_owned(),
                arch: TEST_ARCH_1.to_owned(),
                rev: "1".to_owned(),
                srcmd5: srcmd5.clone(),
                prev_bcnt_for_commit: None,
            },
        );

        let mut log_file = assert_ok!(monitor.download_build_log().await);
        assert_eq!(log_file.len, log_content.len() as u64);

        let mut log = "".to_owned();
        assert_ok!(log_file.file.read_to_string(&mut log).await);
        assert_eq!(log, log_content);

        let new_srcmd5 = random_md5();
        let log_content = log_content.replace(&srcmd5, &new_srcmd5);

        mock.add_completed_build_log(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildLog::new(log_content),
            true,
        );

        let err = assert_err!(monitor.download_build_log().await);
        assert!(err.to_string().contains("unavailable"));
    }

    #[tokio::test]
    async fn test_latest_state() {
        let srcmd5 = random_md5();

        let mock = create_default_mock().await;

        mock.add_project(TEST_PROJECT.to_owned());
        mock.add_new_package(
            TEST_PROJECT,
            TEST_PACKAGE_1.to_owned(),
            MockPackageOptions::default(),
        );

        mock.add_package_revision(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockRevisionOptions {
                srcmd5: srcmd5.clone(),
                ..Default::default()
            },
            HashMap::new(),
        );

        mock.add_or_update_repository(
            TEST_PROJECT,
            TEST_REPO.to_owned(),
            TEST_ARCH_1.to_owned(),
            MockRepositoryCode::Building,
        );
        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus {
                dirty: true,
                ..Default::default()
            },
        );

        // Add the same package to another repo, to make sure it doesn't get
        // picked up by the monitor.
        mock.add_or_update_repository(
            TEST_PROJECT,
            TEST_REPO.to_owned(),
            TEST_ARCH_2.to_owned(),
            MockRepositoryCode::Broken,
        );
        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_2,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus {
                code: MockPackageCode::Broken,
                ..Default::default()
            },
        );

        let client = create_default_client(&mock);
        let monitor = ObsMonitor::new(
            client.clone(),
            MonitoredPackage {
                project: TEST_PROJECT.to_owned(),
                package: TEST_PACKAGE_1.to_owned(),
                repository: TEST_REPO.to_owned(),
                arch: TEST_ARCH_1.to_owned(),
                rev: "1".to_owned(),
                srcmd5: srcmd5.clone(),
                prev_bcnt_for_commit: None,
            },
        );

        let state = assert_ok!(monitor.get_latest_state().await);
        assert_matches!(state, PackageBuildState::Dirty);

        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus {
                code: MockPackageCode::Building,
                ..Default::default()
            },
        );

        let state = assert_ok!(monitor.get_latest_state().await);
        assert_matches!(state, PackageBuildState::Building(PackageCode::Building));

        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus {
                code: MockPackageCode::Succeeded,
                ..Default::default()
            },
        );

        let state = assert_ok!(monitor.get_latest_state().await);
        assert_matches!(
            state,
            PackageBuildState::Completed(PackageCompletion::Succeeded)
        );

        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus {
                code: MockPackageCode::Broken,
                ..Default::default()
            },
        );

        let state = assert_ok!(monitor.get_latest_state().await);
        assert_matches!(
            state,
            PackageBuildState::Completed(PackageCompletion::Failed(PackageCode::Broken))
        );

        mock.add_package_revision(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockRevisionOptions::default(),
            HashMap::new(),
        );

        let state = assert_ok!(monitor.get_latest_state().await);
        assert_matches!(
            state,
            PackageBuildState::Completed(PackageCompletion::Superceded)
        );
    }

    #[tokio::test]
    async fn test_handles_missing_status() {
        let srcmd5 = random_md5();

        let mock = create_default_mock().await;

        mock.add_project(TEST_PROJECT.to_owned());
        mock.add_new_package(
            TEST_PROJECT,
            TEST_PACKAGE_1.to_owned(),
            MockPackageOptions::default(),
        );

        mock.add_package_revision(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockRevisionOptions {
                srcmd5: srcmd5.clone(),
                ..Default::default()
            },
            HashMap::new(),
        );

        mock.add_or_update_repository(
            TEST_PROJECT,
            TEST_REPO.to_owned(),
            TEST_ARCH_1.to_owned(),
            MockRepositoryCode::Building,
        );

        let client = create_default_client(&mock);
        let monitor = ObsMonitor::new(
            client,
            MonitoredPackage {
                project: TEST_PROJECT.to_owned(),
                package: TEST_PACKAGE_1.to_owned(),
                repository: TEST_REPO.to_owned(),
                arch: TEST_ARCH_1.to_owned(),
                rev: "1".to_owned(),
                srcmd5: srcmd5.clone(),
                prev_bcnt_for_commit: None,
            },
        );

        let state = assert_ok!(monitor.get_latest_state().await);
        assert_matches!(state, PackageBuildState::PendingStatusPosted);

        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus::new(MockPackageCode::Succeeded),
        );

        let state = assert_ok!(monitor.get_latest_state().await);
        assert_matches!(
            state,
            PackageBuildState::Completed(PackageCompletion::Succeeded)
        );
    }

    #[tokio::test]
    async fn test_handles_old_bcnt_status() {
        let srcmd5 = random_md5();
        let bcnt_1 = 1;
        let bcnt_2 = 2;

        let mock = create_default_mock().await;

        mock.add_project(TEST_PROJECT.to_owned());
        mock.add_new_package(
            TEST_PROJECT,
            TEST_PACKAGE_1.to_owned(),
            MockPackageOptions::default(),
        );

        mock.add_package_revision(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockRevisionOptions {
                srcmd5: srcmd5.clone(),
                ..Default::default()
            },
            HashMap::new(),
        );

        mock.add_or_update_repository(
            TEST_PROJECT,
            TEST_REPO.to_owned(),
            TEST_ARCH_1.to_owned(),
            MockRepositoryCode::Building,
        );
        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus::new(MockPackageCode::Succeeded),
        );
        mock.add_build_history(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildHistoryEntry {
                srcmd5: srcmd5.clone(),
                bcnt: bcnt_1,
                ..Default::default()
            },
        );

        let client = create_default_client(&mock);
        let monitor = ObsMonitor::new(
            client,
            MonitoredPackage {
                project: TEST_PROJECT.to_owned(),
                package: TEST_PACKAGE_1.to_owned(),
                repository: TEST_REPO.to_owned(),
                arch: TEST_ARCH_1.to_owned(),
                rev: "1".to_owned(),
                srcmd5: srcmd5.clone(),
                prev_bcnt_for_commit: Some(bcnt_1.to_string()),
            },
        );

        let state = assert_ok!(monitor.get_latest_state().await);
        assert_matches!(state, PackageBuildState::PendingStatusPosted);

        mock.add_build_history(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildHistoryEntry {
                srcmd5: srcmd5.clone(),
                bcnt: bcnt_2,
                ..Default::default()
            },
        );

        let state = assert_ok!(monitor.get_latest_state().await);
        assert_matches!(
            state,
            PackageBuildState::Completed(PackageCompletion::Succeeded)
        );
    }
}
