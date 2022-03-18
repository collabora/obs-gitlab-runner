use std::collections::HashSet;

use color_eyre::eyre::{Result, WrapErr};
use open_build_service_api as obs;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::retry::retry_request;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RepoArch {
    pub repo: String,
    pub arch: String,
}

pub struct BuildMeta {
    pub enabled_repos: Vec<RepoArch>,
}

impl BuildMeta {
    #[instrument(skip(client))]
    pub async fn get(client: &obs::Client, project: &str, package: &str) -> Result<BuildMeta> {
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

        let mut enabled_repos = Vec::new();

        for repo_meta in project_meta.repositories {
            for arch in repo_meta.arches {
                if disabled_repos.contains(&repo_meta.name)
                    || disabled_arches.contains(&arch)
                    || disabled_repo_arch_combinations.contains(&(&repo_meta.name, &arch))
                {
                    debug!(repo = %repo_meta.name, %arch, "Disabling");
                    continue;
                }

                enabled_repos.push(RepoArch {
                    repo: repo_meta.name.clone(),
                    arch,
                });
            }
        }

        Ok(BuildMeta { enabled_repos })
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use open_build_service_mock::*;

    use crate::test_support::*;

    use super::*;

    #[tokio::test]
    async fn test_build_meta() {
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

        let client = create_default_client(&mock);

        let mut meta = assert_ok!(BuildMeta::get(&client, TEST_PROJECT, TEST_PACKAGE_1).await);
        assert_eq!(meta.enabled_repos.len(), 2);
        meta.enabled_repos.sort_by(|a, b| a.arch.cmp(&b.arch));

        assert_eq!(meta.enabled_repos[0].repo, TEST_REPO);
        assert_eq!(meta.enabled_repos[0].arch, TEST_ARCH_1);
        assert_eq!(meta.enabled_repos[1].repo, TEST_REPO);
        assert_eq!(meta.enabled_repos[1].arch, TEST_ARCH_2);

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

        let meta = assert_ok!(BuildMeta::get(&client, TEST_PROJECT, TEST_PACKAGE_1).await);
        assert_eq!(meta.enabled_repos.len(), 1);
        assert_eq!(meta.enabled_repos[0].repo, TEST_REPO);
        assert_eq!(meta.enabled_repos[0].arch, TEST_ARCH_2);

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

        let meta = assert_ok!(BuildMeta::get(&client, TEST_PROJECT, TEST_PACKAGE_1).await);
        assert_eq!(meta.enabled_repos.len(), 1);
        assert_eq!(meta.enabled_repos[0].repo, TEST_REPO);
        assert_eq!(meta.enabled_repos[0].arch, TEST_ARCH_2);

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

        let meta = assert_ok!(BuildMeta::get(&client, TEST_PROJECT, TEST_PACKAGE_1).await);
        assert_eq!(meta.enabled_repos.len(), 0);
    }
}
