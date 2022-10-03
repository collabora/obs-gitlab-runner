use std::collections::HashMap;

use color_eyre::eyre::{Result, WrapErr};
use open_build_service_api as obs;
use serde::{Deserialize, Serialize};
use tracing::{debug, info_span, instrument, Instrument};

use crate::retry::retry_request;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepoArch {
    pub repo: String,
    pub arch: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CommitBuildInfo {
    pub prev_bcnt_for_commit: Option<String>,
}

#[derive(Debug)]
pub struct BuildMeta {
    pub enabled_repos: HashMap<RepoArch, obs::JobHistList>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildHistoryRetrieval {
    Full,
    None,
}

impl BuildMeta {
    #[instrument(skip(client))]
    pub async fn get_if_package_exists(
        client: &obs::Client,
        project: &str,
        package: &str,
        history_retrieval: BuildHistoryRetrieval,
    ) -> Result<Option<BuildMeta>> {
        match Self::get(client, project, package, history_retrieval).await {
            Ok(result) => Ok(Some(result)),
            Err(e) => {
                for cause in e.chain() {
                    if let Some(obs::Error::ApiError(obs::ApiError { code, .. })) =
                        cause.downcast_ref::<obs::Error>()
                    {
                        if code == "unknown_package" {
                            return Ok(None);
                        }
                    }
                }

                Err(e)
            }
        }
    }

    #[instrument(skip(client))]
    pub async fn get(
        client: &obs::Client,
        project: &str,
        package: &str,
        history_retrieval: BuildHistoryRetrieval,
    ) -> Result<BuildMeta> {
        let project_meta =
            retry_request(|| async { client.project(project.to_owned()).meta().await })
                .await
                .wrap_err("Failed to get project meta")?;

        debug!(?project_meta);

        let mut enabled_repos = HashMap::new();

        for repo_meta in project_meta.repositories {
            for arch in repo_meta.arches {
                let status = retry_request(|| async {
                    client
                        .project(project.to_owned())
                        .package(package.to_owned())
                        .status(&repo_meta.name, &arch)
                        .await
                })
                .await
                .wrap_err_with(|| {
                    format!(
                        "Failed to get status of {}/{}/{}/{}",
                        project, repo_meta.name, arch, package
                    )
                })?;
                debug!(?status);

                if matches!(
                    status.code,
                    obs::PackageCode::Disabled | obs::PackageCode::Excluded
                ) {
                    debug!(repo = %repo_meta.name, %arch, "Disabling");
                    continue;
                }

                let jobhist = match history_retrieval {
                    BuildHistoryRetrieval::Full => {
                        retry_request(|| async {
                            client
                                .project(project.to_owned())
                                .jobhistory(
                                    &repo_meta.name,
                                    &arch,
                                    &obs::JobHistoryFilters::only_package(package.to_owned()),
                                )
                                .instrument(
                                    info_span!("get:jobhist", repo = %repo_meta.name, %arch),
                                )
                                .await
                        })
                        .await?
                    }
                    BuildHistoryRetrieval::None => obs::JobHistList { jobhist: vec![] },
                };

                enabled_repos.insert(
                    RepoArch {
                        repo: repo_meta.name.clone(),
                        arch,
                    },
                    jobhist,
                );
            }
        }

        Ok(BuildMeta { enabled_repos })
    }

    pub fn clear_stored_history(&mut self) {
        for jobhist in self.enabled_repos.values_mut() {
            jobhist.jobhist.clear();
        }
    }

    pub fn get_commit_build_info(&self, srcmd5: &str) -> HashMap<RepoArch, CommitBuildInfo> {
        let mut repos = HashMap::new();

        for (repo, jobhist) in &self.enabled_repos {
            let prev_bcnt_for_commit = jobhist
                .jobhist
                .iter()
                .filter(|e| e.srcmd5 == srcmd5)
                .last()
                .map(|e| e.bcnt.clone());

            repos.insert(
                repo.clone(),
                CommitBuildInfo {
                    prev_bcnt_for_commit,
                },
            );
        }

        repos
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use open_build_service_mock::*;

    use crate::test_support::*;

    use super::*;

    #[tokio::test]
    async fn test_build_meta_repos() {
        let rev_1 = "1";
        let srcmd5_1 = random_md5();
        let bcnt_1 = 2;

        let rev_2 = "2";
        let srcmd5_2 = random_md5();
        let bcnt_2 = 5;

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
                bcnt: bcnt_1,
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
                bcnt: bcnt_2,
                ..Default::default()
            },
        );

        let client = create_default_client(&mock);

        let meta = assert_ok!(
            BuildMeta::get(
                &client,
                TEST_PROJECT,
                TEST_PACKAGE_1,
                BuildHistoryRetrieval::Full
            )
            .await
        );
        assert_eq!(meta.enabled_repos.len(), 2);
        assert!(meta.enabled_repos.contains_key(&repo_arch_1));
        assert!(meta.enabled_repos.contains_key(&repo_arch_2));

        let build_info = meta.get_commit_build_info(&srcmd5_1);

        let arch_1 = assert_some!(build_info.get(&repo_arch_1));
        assert_some_eq!(arch_1.prev_bcnt_for_commit.as_deref(), &bcnt_1.to_string());

        let arch_2 = assert_some!(build_info.get(&repo_arch_2));
        assert_none!(arch_2.prev_bcnt_for_commit.as_deref());

        let meta = assert_ok!(
            BuildMeta::get(
                &client,
                TEST_PROJECT,
                TEST_PACKAGE_1,
                BuildHistoryRetrieval::None
            )
            .await
        );
        assert_eq!(meta.enabled_repos.len(), 2);
        assert!(meta.enabled_repos.contains_key(&repo_arch_1));

        let build_info = meta.get_commit_build_info(&srcmd5_1);

        let arch_1 = assert_some!(build_info.get(&repo_arch_1));
        assert_none!(arch_1.prev_bcnt_for_commit.as_deref());
        let arch_2 = assert_some!(build_info.get(&repo_arch_2));
        assert_none!(arch_2.prev_bcnt_for_commit.as_deref());

        assert!(meta.enabled_repos.contains_key(&repo_arch_2));

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
                bcnt: bcnt_2,
                ..Default::default()
            },
        );

        let meta = assert_ok!(
            BuildMeta::get(
                &client,
                TEST_PROJECT,
                TEST_PACKAGE_1,
                BuildHistoryRetrieval::Full
            )
            .await
        );
        assert_eq!(meta.enabled_repos.len(), 1);

        let build_info = meta.get_commit_build_info(&srcmd5_2);

        let arch_1 = assert_some!(build_info.get(&repo_arch_2));
        assert_some_eq!(arch_1.prev_bcnt_for_commit.as_deref(), &bcnt_2.to_string());

        mock.add_job_history(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_2,
            MockJobHistoryEntry {
                package: TEST_PACKAGE_1.to_owned(),
                rev: rev_1.to_owned(),
                srcmd5: srcmd5_1.clone(),
                bcnt: bcnt_1,
                ..Default::default()
            },
        );

        let mut meta = assert_ok!(
            BuildMeta::get(
                &client,
                TEST_PROJECT,
                TEST_PACKAGE_1,
                BuildHistoryRetrieval::Full
            )
            .await
        );
        assert_eq!(meta.enabled_repos.len(), 1);

        let build_info = meta.get_commit_build_info(&srcmd5_1);

        let arch_2 = assert_some!(build_info.get(&repo_arch_2));
        assert_some_eq!(arch_2.prev_bcnt_for_commit.as_deref(), &bcnt_1.to_string());

        meta.clear_stored_history();

        let build_info = meta.get_commit_build_info(&srcmd5_1);
        let arch_2 = assert_some!(build_info.get(&repo_arch_2));
        assert_none!(arch_2.prev_bcnt_for_commit.as_deref());

        mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_2,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus::new(MockPackageCode::Excluded),
        );

        let meta = assert_ok!(
            BuildMeta::get(
                &client,
                TEST_PROJECT,
                TEST_PACKAGE_1,
                BuildHistoryRetrieval::Full
            )
            .await
        );
        assert_eq!(meta.enabled_repos.len(), 0);
    }
}
