use std::{collections::HashMap, time::Duration};

use color_eyre::eyre::{Result, WrapErr};
use open_build_service_api as obs;
use serde::{Deserialize, Serialize};
use tracing::{Instrument, debug, info_span, instrument};

use crate::{outputln, retry_request};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepoArch {
    pub repo: String,
    pub arch: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct EnabledRepo {
    #[serde(flatten)]
    pub repo_arch: RepoArch,
    pub prev_endtime_for_commit: Option<u64>,
}

#[derive(Debug)]
pub struct BuildMeta {
    client: obs::Client,
    project: String,
    package: String,
    repos: HashMap<RepoArch, obs::JobHistList>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildHistoryRetrieval {
    Full,
    None,
}

#[derive(Debug, Clone)]
pub struct BuildMetaWaitOptions {
    pub sleep_until_ready: Duration,
}

impl Default for BuildMetaWaitOptions {
    fn default() -> Self {
        Self {
            sleep_until_ready: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DisabledRepos {
    Keep,
    Skip { wait_options: BuildMetaWaitOptions },
}

#[derive(Debug)]
pub struct BuildMetaOptions {
    pub history_retrieval: BuildHistoryRetrieval,
    pub disabled_repos: DisabledRepos,
}

#[instrument(skip(client))]
async fn get_status(
    client: &obs::Client,
    project: &str,
    package: &str,
    repo: &str,
    arch: &str,
) -> Result<obs::BuildStatus> {
    let status = retry_request!({
        client
            .project(project.to_owned())
            .package(package.to_owned())
            .status(repo, arch)
            .await
            .wrap_err_with(|| format!("Failed to get status of {project}/{repo}/{arch}/{package}"))
    })?;
    debug!(?status);

    Ok(status)
}

fn is_status_empty(status: &obs::BuildStatus) -> bool {
    status.code == obs::PackageCode::Broken && matches!(status.details.as_deref(), Some("empty"))
}

#[instrument(skip(client))]
async fn get_status_when_ready(
    client: &obs::Client,
    project: &str,
    package: &str,
    repo: &str,
    arch: &str,
    options: &BuildMetaWaitOptions,
) -> Result<obs::BuildStatus> {
    let mut status = get_status(client, project, package, repo, arch).await?;

    if status.dirty || is_status_empty(&status) {
        outputln!("Waiting for package to be ready...");
        for attempt in 0.. {
            debug!(?attempt);
            tokio::time::sleep(options.sleep_until_ready).await;

            status = get_status(client, project, package, repo, arch).await?;
            if !status.dirty && !is_status_empty(&status) {
                break;
            }
        }
    }

    Ok(status)
}

impl BuildMeta {
    #[instrument(skip(client))]
    pub async fn get_if_package_exists(
        client: obs::Client,
        project: String,
        package: String,
        options: &BuildMetaOptions,
    ) -> Result<Option<BuildMeta>> {
        match Self::get(client, project, package, options).await {
            Ok(result) => Ok(Some(result)),
            Err(e) => {
                for cause in e.chain() {
                    if let Some(obs::Error::ApiError(obs::ApiError { code, .. })) =
                        cause.downcast_ref::<obs::Error>()
                        && code == "unknown_package"
                    {
                        return Ok(None);
                    }
                }

                Err(e)
            }
        }
    }

    #[instrument(skip(client))]
    pub async fn get(
        client: obs::Client,
        project: String,
        package: String,
        options: &BuildMetaOptions,
    ) -> Result<BuildMeta> {
        let project_meta = retry_request!(
            client
                .project(project.to_owned())
                .meta()
                .await
                .wrap_err("Failed to get project meta")
        )?;

        debug!(?project_meta);

        let mut repos = HashMap::new();

        for repo_meta in project_meta.repositories {
            for arch in repo_meta.arches {
                if let DisabledRepos::Skip { wait_options } = &options.disabled_repos {
                    let status = get_status_when_ready(
                        &client,
                        &project,
                        &package,
                        &repo_meta.name,
                        &arch,
                        wait_options,
                    )
                    .await?;
                    if matches!(
                        status.code,
                        obs::PackageCode::Disabled | obs::PackageCode::Excluded
                    ) {
                        debug!(repo = %repo_meta.name, %arch, "Disabling");
                        continue;
                    }
                }

                let jobhist = match options.history_retrieval {
                    BuildHistoryRetrieval::Full => retry_request!(
                        client
                            .project(project.to_owned())
                            .jobhistory(
                                &repo_meta.name,
                                &arch,
                                &obs::JobHistoryFilters::only_package(package.to_owned()),
                            )
                            .instrument(info_span!("get:jobhist", repo = %repo_meta.name, %arch),)
                            .await
                    )?,
                    BuildHistoryRetrieval::None => obs::JobHistList { jobhist: vec![] },
                };

                repos.insert(
                    RepoArch {
                        repo: repo_meta.name.clone(),
                        arch,
                    },
                    jobhist,
                );
            }
        }

        Ok(BuildMeta {
            client,
            project,
            package,
            repos,
        })
    }

    pub fn clear_stored_history(&mut self) {
        for jobhist in self.repos.values_mut() {
            jobhist.jobhist.clear();
        }
    }

    pub async fn remove_disabled_repos(&mut self, options: &BuildMetaWaitOptions) -> Result<()> {
        for repo_arch in self.repos.keys().cloned().collect::<Vec<_>>() {
            let status = get_status_when_ready(
                &self.client,
                &self.project,
                &self.package,
                &repo_arch.repo,
                &repo_arch.arch,
                options,
            )
            .await?;
            if matches!(
                status.code,
                obs::PackageCode::Disabled | obs::PackageCode::Excluded
            ) {
                debug!(repo = %repo_arch.repo, %repo_arch.arch, "Disabling");
                self.repos.remove(&repo_arch);
            }
        }

        Ok(())
    }

    pub fn get_commit_build_info(&self, srcmd5: &str) -> Vec<EnabledRepo> {
        self.repos
            .iter()
            .map(|(repo, jobhist)| EnabledRepo {
                repo_arch: repo.clone(),
                prev_endtime_for_commit: jobhist
                    .jobhist
                    .iter()
                    .rfind(|e| e.srcmd5 == srcmd5)
                    .map(|e| e.endtime),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use claims::*;
    use obo_test_support::*;
    use open_build_service_mock::*;
    use rstest::rstest;

    use super::*;

    #[tokio::test]
    async fn test_build_meta_repos() {
        let rev_1 = "1";
        let srcmd5_1 = random_md5();
        let endtime_1 = 100;

        let rev_2 = "2";
        let srcmd5_2 = random_md5();
        let endtime_2 = 200;

        let repo_arch_1 = RepoArch {
            repo: TEST_REPO.to_owned(),
            arch: TEST_ARCH_1.to_owned(),
        };

        let repo_arch_2 = RepoArch {
            repo: TEST_REPO.to_owned(),
            arch: TEST_ARCH_2.to_owned(),
        };

        let mock = create_default_mock().await;
        mock.add_project(TEST_PROJECT.to_owned());
        mock.add_new_package(
            TEST_PROJECT,
            TEST_PACKAGE_1.to_owned(),
            MockPackageOptions::default(),
        );

        mock.add_or_update_repository(
            TEST_PROJECT,
            TEST_REPO.to_owned(),
            TEST_ARCH_1.to_owned(),
            MockRepositoryCode::Finished,
        );
        mock.add_or_update_repository(
            TEST_PROJECT,
            TEST_REPO.to_owned(),
            TEST_ARCH_2.to_owned(),
            MockRepositoryCode::Finished,
        );

        mock.add_job_history(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            MockJobHistoryEntry {
                package: TEST_PACKAGE_1.to_owned(),
                rev: rev_1.to_owned(),
                srcmd5: srcmd5_1.clone(),
                endtime: SystemTime::UNIX_EPOCH + Duration::from_secs(endtime_1),
                ..Default::default()
            },
        );

        mock.add_job_history(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_2,
            MockJobHistoryEntry {
                package: TEST_PACKAGE_1.to_owned(),
                rev: rev_2.to_owned(),
                srcmd5: srcmd5_2.clone(),
                endtime: SystemTime::UNIX_EPOCH + Duration::from_secs(endtime_2),
                ..Default::default()
            },
        );

        let client = create_default_client(&mock);

        let meta = assert_ok!(
            BuildMeta::get(
                client.clone(),
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_1.to_owned(),
                &BuildMetaOptions {
                    history_retrieval: BuildHistoryRetrieval::Full,
                    disabled_repos: DisabledRepos::Skip {
                        wait_options: Default::default()
                    },
                }
            )
            .await
        );
        assert_eq!(meta.repos.len(), 2);
        assert!(meta.repos.contains_key(&repo_arch_1));
        assert!(meta.repos.contains_key(&repo_arch_2));

        let build_info = meta.get_commit_build_info(&srcmd5_1);

        let arch_1 = assert_some!(build_info.iter().find(|e| e.repo_arch == repo_arch_1));
        assert_some_eq!(arch_1.prev_endtime_for_commit, endtime_1);

        let arch_2 = assert_some!(build_info.iter().find(|e| e.repo_arch == repo_arch_2));
        assert_none!(arch_2.prev_endtime_for_commit);

        let meta = assert_ok!(
            BuildMeta::get(
                client.clone(),
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_1.to_owned(),
                &BuildMetaOptions {
                    history_retrieval: BuildHistoryRetrieval::None,
                    disabled_repos: DisabledRepos::Skip {
                        wait_options: Default::default()
                    },
                }
            )
            .await
        );
        assert_eq!(meta.repos.len(), 2);
        assert!(meta.repos.contains_key(&repo_arch_1));

        let build_info = meta.get_commit_build_info(&srcmd5_1);

        let arch_1 = assert_some!(build_info.iter().find(|e| e.repo_arch == repo_arch_1));
        assert_none!(arch_1.prev_endtime_for_commit);
        let arch_2 = assert_some!(build_info.iter().find(|e| e.repo_arch == repo_arch_2));
        assert_none!(arch_2.prev_endtime_for_commit);

        assert!(meta.repos.contains_key(&repo_arch_2));

        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus::new(MockPackageCode::Disabled),
        );

        mock.add_job_history(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            MockJobHistoryEntry {
                package: TEST_PACKAGE_1.to_owned(),
                rev: rev_1.to_owned(),
                srcmd5: srcmd5_1.clone(),
                endtime: SystemTime::UNIX_EPOCH + Duration::from_secs(endtime_2),
                ..Default::default()
            },
        );

        let meta = assert_ok!(
            BuildMeta::get(
                client.clone(),
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_1.to_owned(),
                &BuildMetaOptions {
                    history_retrieval: BuildHistoryRetrieval::Full,
                    disabled_repos: DisabledRepos::Skip {
                        wait_options: Default::default()
                    },
                }
            )
            .await
        );
        assert_eq!(meta.repos.len(), 1);

        let build_info = meta.get_commit_build_info(&srcmd5_2);

        let arch_1 = assert_some!(build_info.iter().find(|e| e.repo_arch == repo_arch_2));
        assert_some_eq!(arch_1.prev_endtime_for_commit, endtime_2);

        mock.add_job_history(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_2,
            MockJobHistoryEntry {
                package: TEST_PACKAGE_1.to_owned(),
                rev: rev_1.to_owned(),
                srcmd5: srcmd5_1.clone(),
                endtime: SystemTime::UNIX_EPOCH + Duration::from_secs(endtime_1),
                ..Default::default()
            },
        );

        let mut meta = assert_ok!(
            BuildMeta::get(
                client.clone(),
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_1.to_owned(),
                &BuildMetaOptions {
                    history_retrieval: BuildHistoryRetrieval::Full,
                    disabled_repos: DisabledRepos::Skip {
                        wait_options: Default::default()
                    },
                }
            )
            .await
        );
        assert_eq!(meta.repos.len(), 1);

        let build_info = meta.get_commit_build_info(&srcmd5_1);

        let arch_2 = assert_some!(build_info.iter().find(|e| e.repo_arch == repo_arch_2));
        assert_some_eq!(arch_2.prev_endtime_for_commit, endtime_1);

        meta.clear_stored_history();

        let build_info = meta.get_commit_build_info(&srcmd5_1);
        let arch_2 = assert_some!(build_info.iter().find(|e| e.repo_arch == repo_arch_2));
        assert_none!(arch_2.prev_endtime_for_commit);

        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_2,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus::new(MockPackageCode::Excluded),
        );

        let meta = assert_ok!(
            BuildMeta::get(
                client.clone(),
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_1.to_owned(),
                &BuildMetaOptions {
                    history_retrieval: BuildHistoryRetrieval::Full,
                    disabled_repos: DisabledRepos::Skip {
                        wait_options: Default::default()
                    },
                }
            )
            .await
        );
        assert_eq!(meta.repos.len(), 0);

        let mut meta = assert_ok!(
            BuildMeta::get(
                client.clone(),
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_1.to_owned(),
                &BuildMetaOptions {
                    history_retrieval: BuildHistoryRetrieval::Full,
                    disabled_repos: DisabledRepos::Keep,
                }
            )
            .await
        );
        assert_eq!(meta.repos.len(), 2);

        assert_ok!(meta.remove_disabled_repos(&Default::default()).await);
        assert_eq!(meta.repos.len(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_build_meta_ignores_empty(
        #[values(false, true)] skip_early: bool,
        #[values(false, true)] dirty: bool,
    ) {
        const SLEEP_DURATION: Duration = Duration::from_millis(100);

        let repo_arch_1 = RepoArch {
            repo: TEST_REPO.to_owned(),
            arch: TEST_ARCH_1.to_owned(),
        };

        let mock = create_default_mock().await;
        mock.add_project(TEST_PROJECT.to_owned());
        mock.add_new_package(
            TEST_PROJECT,
            TEST_PACKAGE_1.to_owned(),
            MockPackageOptions::default(),
        );

        mock.add_or_update_repository(
            TEST_PROJECT,
            TEST_REPO.to_owned(),
            TEST_ARCH_1.to_owned(),
            MockRepositoryCode::Finished,
        );

        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus::new(MockPackageCode::Broken),
        );

        let client = create_default_client(&mock);

        let meta = assert_ok!(
            BuildMeta::get(
                client.clone(),
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_1.to_owned(),
                &BuildMetaOptions {
                    history_retrieval: BuildHistoryRetrieval::Full,
                    disabled_repos: DisabledRepos::Skip {
                        wait_options: Default::default()
                    }
                }
            )
            .await
        );
        assert_eq!(meta.repos.len(), 1);
        assert!(meta.repos.contains_key(&repo_arch_1));

        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            if dirty {
                MockBuildStatus {
                    code: MockPackageCode::Broken,
                    dirty: true,
                    ..Default::default()
                }
            } else {
                MockBuildStatus {
                    code: MockPackageCode::Broken,
                    details: "empty".to_owned(),
                    ..Default::default()
                }
            },
        );

        tokio::spawn(async move {
            tokio::time::sleep(SLEEP_DURATION * 10).await;
            mock.set_package_build_status(
                TEST_PROJECT,
                TEST_REPO,
                TEST_ARCH_1,
                TEST_PACKAGE_1.to_owned(),
                MockBuildStatus::new(MockPackageCode::Excluded),
            );
        });

        let mut meta = assert_ok!(
            BuildMeta::get(
                client.clone(),
                TEST_PROJECT.to_owned(),
                TEST_PACKAGE_1.to_owned(),
                &BuildMetaOptions {
                    history_retrieval: BuildHistoryRetrieval::Full,
                    disabled_repos: if skip_early {
                        DisabledRepos::Skip {
                            wait_options: BuildMetaWaitOptions {
                                sleep_until_ready: SLEEP_DURATION,
                            },
                        }
                    } else {
                        DisabledRepos::Keep
                    },
                }
            )
            .await
        );

        if !skip_early {
            assert_eq!(meta.repos.len(), 1);
            assert_ok!(
                meta.remove_disabled_repos(&BuildMetaWaitOptions {
                    sleep_until_ready: SLEEP_DURATION,
                })
                .await
            );
        }

        assert_eq!(meta.repos.len(), 0);
    }
}
