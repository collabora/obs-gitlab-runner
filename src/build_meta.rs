use std::collections::{HashMap, HashSet};

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
    pub prev_endtime_for_commit: Option<u64>,
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
        let package_meta = retry_request(|| async {
            client
                .project(project.to_owned())
                .package(package.to_owned())
                .meta()
                .await
        })
        .await
        .wrap_err("Failed to get package meta")?;

        debug!(?project_meta, ?package_meta);

        let mut disabled_repos = HashSet::new();
        let mut disabled_arches = HashSet::new();
        let mut disabled_repo_arch_combinations = HashSet::new();

        for disabled_build in &package_meta.build.disabled {
            match (&disabled_build.repository, &disabled_build.arch) {
                (Some(repo), Some(arch)) => {
                    disabled_repo_arch_combinations.insert((repo, arch));
                }
                (Some(repo), None) => {
                    disabled_repos.insert(repo);
                }
                (None, Some(arch)) => {
                    disabled_arches.insert(arch);
                }
                (None, None) => {}
            }
        }

        let mut enabled_repos = HashMap::new();

        for repo_meta in project_meta.repositories {
            for arch in repo_meta.arches {
                if disabled_repos.contains(&repo_meta.name)
                    || disabled_arches.contains(&arch)
                    || disabled_repo_arch_combinations.contains(&(&repo_meta.name, &arch))
                {
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
            let prev_endtime_for_commit = jobhist
                .jobhist
                .iter()
                .filter(|e| e.srcmd5 == srcmd5)
                .last()
                .map(|e| e.endtime);

            repos.insert(
                repo.clone(),
                CommitBuildInfo {
                    prev_endtime_for_commit,
                },
            );
        }

        repos
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use claim::*;
    use open_build_service_mock::*;

    use crate::test_support::*;

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
        assert_some_eq!(arch_1.prev_endtime_for_commit, endtime_1);

        let arch_2 = assert_some!(build_info.get(&repo_arch_2));
        assert_none!(arch_2.prev_endtime_for_commit);

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
        assert_none!(arch_1.prev_endtime_for_commit);
        let arch_2 = assert_some!(build_info.get(&repo_arch_2));
        assert_none!(arch_2.prev_endtime_for_commit);

        assert!(meta.enabled_repos.contains_key(&repo_arch_2));

        mock.set_package_metadata(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockPackageOptions {
                disabled: vec![MockPackageDisabledBuild {
                    repository: Some(TEST_REPO.to_owned()),
                    arch: Some(TEST_ARCH_1.to_owned()),
                }],
                ..Default::default()
            },
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
        assert_some_eq!(arch_1.prev_endtime_for_commit, endtime_2);

        mock.set_package_metadata(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockPackageOptions {
                disabled: vec![MockPackageDisabledBuild {
                    repository: None,
                    arch: Some(TEST_ARCH_1.to_owned()),
                }],
                ..Default::default()
            },
        );

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
        assert_some_eq!(arch_2.prev_endtime_for_commit, endtime_1);

        meta.clear_stored_history();

        let build_info = meta.get_commit_build_info(&srcmd5_1);
        let arch_2 = assert_some!(build_info.get(&repo_arch_2));
        assert_none!(arch_2.prev_endtime_for_commit);

        mock.set_package_metadata(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockPackageOptions {
                disabled: vec![MockPackageDisabledBuild {
                    repository: Some(TEST_REPO.to_owned()),
                    arch: None,
                }],
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
        assert_eq!(meta.enabled_repos.len(), 0);
    }
}
