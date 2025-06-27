use color_eyre::eyre::{Context, Result, ensure};
use gitlab_runner::outputln;
use open_build_service_api as obs;
use tracing::info;

use crate::retry::retry_request;

#[tracing::instrument(skip(client))]
pub async fn prune_branch(
    client: &obs::Client,
    project: &str,
    package: &str,
    expected_rev: Option<&str>,
) -> Result<()> {
    // Do a sanity check to make sure this project & package are actually
    // linked (i.e. we're not going to be nuking the main repository).
    let dir = retry_request(|| async {
        client
            .project(project.to_owned())
            .package(package.to_owned())
            .list(None)
            .await
    })
    .await
    .wrap_err("Failed to list package")?;
    ensure!(
        !dir.linkinfo.is_empty(),
        "Rejecting attempt to prune a non-branched package"
    );

    if let Some(expected_rev) = expected_rev {
        if dir.rev.as_deref() != Some(expected_rev) {
            info!(
                "Latest revision is {}, skipping prune",
                dir.rev.as_deref().unwrap_or("[unknown]")
            );
            return Ok(());
        }
    }

    retry_request(|| async {
        client
            .project(project.to_owned())
            .package(package.to_owned())
            .delete()
            .await
    })
    .await
    .wrap_err("Failed to delete package")?;

    outputln!("Deleted package {}/{}.", project, package);

    let packages =
        retry_request(|| async { client.project(project.to_owned()).list_packages().await })
            .await
            .wrap_err("Failed to list packages in project")?
            .entries;
    if packages.is_empty() {
        retry_request(|| async { client.project(project.to_owned()).delete().await })
            .await
            .wrap_err("Failed to delete project")?;
        outputln!("Deleted empty project {}.", project);
    } else {
        outputln!("Project has other packages, skipping deletion.");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use claims::*;
    use open_build_service_mock::*;

    use crate::test_support::*;

    use super::*;

    #[tokio::test]
    async fn test_prune() {
        let test_rev = "1";

        let mock = create_default_mock().await;
        mock.add_project(TEST_PROJECT.to_owned());
        mock.add_new_package(
            TEST_PROJECT,
            TEST_PACKAGE_1.to_owned(),
            MockPackageOptions::default(),
        );

        let test_key = mock.add_package_files(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockSourceFile {
                contents: b"abc".to_vec(),
                path: "file".to_owned(),
            },
        );
        mock.add_package_revision(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockRevisionOptions::default(),
            [(
                "file".to_owned(),
                MockEntry::from_key(&test_key, SystemTime::now()),
            )]
            .into(),
        );

        mock.branch(
            TEST_PROJECT.to_owned(),
            TEST_PACKAGE_1.to_owned(),
            TEST_PROJECT,
            TEST_PACKAGE_2.to_owned(),
            MockBranchOptions::default(),
        );

        let client = create_default_client(&mock);
        let project = client.project(TEST_PROJECT.to_owned());
        let package_1 = client
            .project(TEST_PROJECT.to_owned())
            .package(TEST_PACKAGE_1.to_owned());
        let package_2 = client
            .project(TEST_PROJECT.to_owned())
            .package(TEST_PACKAGE_2.to_owned());

        let err =
            assert_err!(prune_branch(&client, TEST_PROJECT, TEST_PACKAGE_1, Some(test_rev)).await);
        assert!(err.to_string().contains("Rejecting"));
        assert_ok!(package_1.list(None).await);

        assert_ok!(package_2.list(None).await);
        assert_ok!(prune_branch(&client, TEST_PROJECT, TEST_PACKAGE_2, Some("1000")).await);
        assert_ok!(package_2.list(None).await);

        assert_ok!(package_2.list(None).await);
        assert_ok!(prune_branch(&client, TEST_PROJECT, TEST_PACKAGE_2, Some(test_rev)).await);
        let err = assert_err!(package_2.list(None).await);
        assert_matches!(err, obs::Error::ApiError(obs::ApiError { code, .. })
            if code == "unknown_package");
        assert_ok!(project.meta().await);

        mock.branch(
            TEST_PROJECT.to_owned(),
            TEST_PACKAGE_1.to_owned(),
            TEST_PROJECT,
            TEST_PACKAGE_2.to_owned(),
            MockBranchOptions::default(),
        );
        assert_ok!(package_2.list(None).await);
        assert_ok!(prune_branch(&client, TEST_PROJECT, TEST_PACKAGE_2, None).await);
        let err = assert_err!(package_2.list(None).await);
        assert_matches!(err, obs::Error::ApiError(obs::ApiError { code, .. })
            if code == "unknown_package");
        assert_ok!(project.meta().await);

        mock.branch(
            TEST_PROJECT.to_owned(),
            TEST_PACKAGE_1.to_owned(),
            TEST_PROJECT,
            TEST_PACKAGE_2.to_owned(),
            MockBranchOptions::default(),
        );
        assert_ok!(package_1.delete().await);

        assert_ok!(project.meta().await);
        assert_ok!(package_2.list(None).await);
        assert_ok!(prune_branch(&client, TEST_PROJECT, TEST_PACKAGE_2, Some(test_rev)).await);
        let err = assert_err!(package_2.list(None).await);
        assert_matches!(err, obs::Error::ApiError(obs::ApiError { code, .. })
            if code == "unknown_project");
        let err = assert_err!(project.meta().await);
        assert_matches!(err, obs::Error::ApiError(obs::ApiError { code, .. })
            if code == "unknown_project");
    }
}
