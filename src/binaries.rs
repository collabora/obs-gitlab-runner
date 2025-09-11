use std::io;

use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::eyre::{Report, Result, WrapErr};
use futures_util::TryStreamExt;
use open_build_service_api as obs;
use tokio::io::AsyncSeekExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::{Instrument, info_span, instrument};

use crate::{
    artifacts::{ArtifactDirectory, ArtifactWriter},
    retry_request,
};

pub struct DownloadSummary {
    pub paths: Vec<Utf8PathBuf>,
}

#[instrument(skip(client, artifacts))]
pub async fn download_binaries(
    client: obs::Client,
    project: &str,
    package: &str,
    repository: &str,
    arch: &str,
    artifacts: &mut impl ArtifactDirectory,
    output_root: &Utf8Path,
) -> Result<DownloadSummary> {
    let binary_list = retry_request!(
        client
            .project(project.to_owned())
            .package(package.to_owned())
            .binaries(repository, arch)
            .await
    )?;

    let mut summary = DownloadSummary { paths: vec![] };

    for binary in binary_list.binaries {
        let dest = output_root.join(&binary.filename);

        let binary = binary.clone();
        let client = client.clone();
        artifacts
            .save_with(&dest, async move |file: &mut ArtifactWriter| {
                retry_request!(
                    async {
                        file.rewind().await.wrap_err("Failed to rewind file")?;

                        let stream = client
                            .project(project.to_owned())
                            .package(package.to_owned())
                            .binary_file(repository, arch, &binary.filename)
                            .await
                            .wrap_err("Failed to request file")?;

                        tokio::io::copy(
                            &mut stream.map_err(io::Error::other).into_async_read().compat(),
                            &mut *file,
                        )
                        .await
                        .wrap_err("Failed to download file")?;
                        Ok::<(), Report>(())
                    }
                    .instrument(info_span!("download_binaries:download", ?binary))
                    .await
                )
            })
            .await?;

        summary.paths.push(dest);
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use claims::*;
    use open_build_service_mock::*;

    use crate::{artifacts::test_support::MockArtifactDirectory, test_support::*};

    use super::*;

    #[tokio::test]
    async fn test_build_results() {
        let test_dir: &Utf8Path = "results".into();
        let test_file = "test.bin";
        let test_contents = "123980238";

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
            MockRepositoryCode::Building,
        );

        mock.set_package_binaries(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            [(
                test_file.to_owned(),
                MockBinary {
                    contents: test_contents.as_bytes().to_vec(),
                    mtime: SystemTime::UNIX_EPOCH,
                },
            )]
            .into(),
        );

        let client = create_default_client(&mock);
        let mut artifacts = MockArtifactDirectory::default();

        let summary = assert_ok!(
            download_binaries(
                client,
                TEST_PROJECT,
                TEST_PACKAGE_1,
                TEST_REPO,
                TEST_ARCH_1,
                &mut artifacts,
                test_dir,
            )
            .await
        );

        assert_eq!(summary.paths, vec![test_dir.join(test_file)]);

        let contents = assert_ok!(artifacts.read_string(test_dir.join(test_file)).await);
        assert_eq!(contents, test_contents);
    }
}
