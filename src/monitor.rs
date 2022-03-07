use std::time::Duration;

use color_eyre::eyre::{ensure, eyre, Result};
use derivative::*;
use futures_util::stream::StreamExt;
use gitlab_runner::outputln;
use open_build_service_api as obs;
use tracing::{debug, error, instrument};

#[derive(Debug)]
pub enum PackageCompletion {
    Succeeded,
    Superceded,
    Failed(obs::PackageCode),
    Disabled,
}

#[derive(Debug)]
enum PackageBuildState {
    Building,
    Dirty,
    Completed(PackageCompletion),
}

// TODO: do we need this?
#[derive(Debug)]
pub struct PackageMonitoringOptions {
    sleep_on_building: Duration,
    sleep_on_dirty: Duration,
}

impl Default for PackageMonitoringOptions {
    fn default() -> Self {
        PackageMonitoringOptions {
            sleep_on_building: Duration::from_secs(10),
            sleep_on_dirty: Duration::from_secs(30),
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ObsMonitor {
    #[derivative(Debug = "ignore")]
    client: obs::Client,

    project: String,
    package: String,
    repository: String,
    arch: String,

    rev: String,
    srcmd5: String,
}

impl ObsMonitor {
    pub async fn new(
        client: obs::Client,
        project: String,
        package: String,
        repository: String,
        arch: String,
        rev: String,
    ) -> Result<ObsMonitor> {
        let dir = client
            .project(project.clone())
            .package(package.clone())
            .list(Some(&rev))
            .await?;

        let srcmd5 = if let Some(link) = dir.linkinfo.into_iter().next() {
            link.xsrcmd5
        } else {
            dir.srcmd5
        };

        Ok(ObsMonitor {
            client,
            project,
            package,
            repository,
            arch,
            rev,
            srcmd5,
        })
    }

    #[instrument(skip(self))]
    async fn get_latest_revision(&self) -> Result<String> {
        let dir = self
            .client
            .project(self.project.clone())
            .package(self.package.clone())
            .list(None)
            .await?;
        dir.rev.ok_or_else(|| eyre!("Latest revision is 0"))
    }

    #[instrument(skip(self))]
    async fn get_latest_state(&self) -> Result<PackageBuildState> {
        let latest_rev = self.get_latest_revision().await?;
        if latest_rev != self.rev {
            return Ok(PackageBuildState::Completed(PackageCompletion::Superceded));
        }

        let all_results = self
            .client
            .project(self.project.clone())
            .package(self.package.clone())
            .result()
            .await?;

        // TODO: filter this in the API call instead of afterwards
        let result = all_results
            .results
            .into_iter()
            .find(|r| r.arch == self.arch)
            .ok_or_else(|| eyre!("Failed to find results for architecture {}", self.arch))?;
        if result.dirty {
            return Ok(PackageBuildState::Dirty);
        }

        let status = result
            .get_status(&self.package)
            .ok_or_else(|| eyre!("Package {} missing", self.package))?;
        if status.dirty {
            return Ok(PackageBuildState::Dirty);
        } else if status.code.is_final() {
            Ok(PackageBuildState::Completed(match status.code {
                obs::PackageCode::Disabled | obs::PackageCode::Excluded => {
                    PackageCompletion::Disabled
                }
                obs::PackageCode::Succeeded => PackageCompletion::Succeeded,
                code => PackageCompletion::Failed(code),
            }))
        } else {
            Ok(PackageBuildState::Building)
        }
    }

    #[instrument(skip(self))]
    fn check_log_md5(&self, content: &str) -> Result<()> {
        let needle = format!("srcmd5 '{}'", self.srcmd5);
        ensure!(
            content.contains(&needle),
            "Build logs are unavailable (overwritten by a later build revision?)"
        );

        Ok(())
    }

    #[instrument(skip(self))]
    async fn download_logs(
        &self,
        offset: usize,
        end: usize,
        ignore_error_if_some_logs_present: bool,
    ) -> Result<String> {
        debug!("Downloading...");

        let mut stream = self
            .client
            .project(self.project.clone())
            .package(self.package.clone())
            .log(&self.repository, &self.arch)
            .stream(obs::PackageLogStreamOptions {
                offset: Some(offset),
                end: Some(end),
            })?;

        let mut logs = "".to_owned();
        logs.reserve(end - offset);

        while let Some(bytes) = stream.next().await {
            match bytes {
                Ok(bytes) => logs.push_str(&String::from_utf8_lossy(&bytes)),
                Err(err) => {
                    error!("Error reading logs: {:?}", err);
                    if ignore_error_if_some_logs_present && !logs.is_empty() {
                        break;
                    } else {
                        return Err(eyre!(err));
                    }
                }
            }
        }

        Ok(logs)
    }

    #[instrument]
    pub async fn get_logs_tail(&self, limit: usize) -> Result<String> {
        const LOG_LEN_TO_CHECK_FOR_MD5: usize = 2500;

        let (total_size, _) = self
            .client
            .project(self.project.clone())
            .package(self.package.clone())
            .log(&self.repository, &self.arch)
            .entry()
            .await?;

        let download_entire_log = total_size <= limit;

        let offset = if download_entire_log {
            0
        } else {
            total_size - limit
        };
        let logs = self.download_logs(offset, total_size, true).await?;

        // If we already downloaded the start of the logs, just check there for
        // the MD5, otherwise download the head and check it.
        if offset == 0 && (download_entire_log || limit >= LOG_LEN_TO_CHECK_FOR_MD5) {
            self.check_log_md5(&logs)?;
        } else {
            let head = self
                .download_logs(0, LOG_LEN_TO_CHECK_FOR_MD5, false)
                .await?;
            self.check_log_md5(&head)?;
        }

        Ok(logs)
    }

    #[instrument]
    pub async fn monitor_package(
        &self,
        options: PackageMonitoringOptions,
    ) -> Result<PackageCompletion> {
        // TODO: show log url

        loop {
            match self.get_latest_state().await? {
                PackageBuildState::Building => {
                    outputln!("Still building");
                    tokio::time::sleep(options.sleep_on_building).await;
                }
                PackageBuildState::Dirty => {
                    outputln!("Package is dirty, trying again later...");
                    tokio::time::sleep(options.sleep_on_dirty).await;
                }
                PackageBuildState::Completed(reason) => {
                    return Ok(reason);
                }
            }
        }
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
        let monitor = assert_ok!(
            ObsMonitor::new(
                client.clone(),
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_1.to_owned(),
                TEST_REPO.to_owned(),
                TEST_ARCH_1.to_owned(),
                "1".to_owned(),
            )
            .await
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

        let monitor = assert_ok!(
            ObsMonitor::new(
                client,
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_2.to_owned(),
                TEST_REPO.to_owned(),
                TEST_ARCH_1.to_owned(),
                "1".to_owned(),
            )
            .await
        );

        assert_ok!(monitor.check_log_md5(&format!("srcmd5 '{}'", branch_xsrcmd5)));
        assert_err!(monitor.check_log_md5(&format!("srcmd5 '{}'", srcmd5)));
        assert_err!(monitor.check_log_md5(&format!("srcmd5 '{}'", branch_srcmd5)));
    }

    #[tokio::test]
    async fn test_get_logs() {
        let srcmd5 = random_md5();
        let log_content = format!(
            "srcmd5 '{}'\n
                some random logs are here
                ten characters: 0123456789",
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
        let monitor = assert_ok!(
            ObsMonitor::new(
                client,
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_1.to_owned(),
                TEST_REPO.to_owned(),
                TEST_ARCH_1.to_owned(),
                "1".to_owned(),
            )
            .await
        );

        let logs = assert_ok!(monitor.get_logs_tail(10).await);
        assert_eq!(logs, "0123456789");
        let logs = assert_ok!(monitor.get_logs_tail(log_content.len()).await);
        assert_eq!(logs, log_content);

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

        assert_err!(monitor.get_logs_tail(10).await);
    }

    #[tokio::test]
    async fn test_latest_state() {
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
            MockRevisionOptions::default(),
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
        let monitor = assert_ok!(
            ObsMonitor::new(
                client.clone(),
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_1.to_owned(),
                TEST_REPO.to_owned(),
                TEST_ARCH_1.to_owned(),
                "1".to_owned(),
            )
            .await
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
        assert_matches!(state, PackageBuildState::Building);

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
}
