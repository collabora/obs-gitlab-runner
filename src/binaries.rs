use std::{collections::HashMap, io};

use color_eyre::eyre::{Report, Result, WrapErr};
use futures_util::TryStreamExt;
use open_build_service_api as obs;
use tokio::{fs::File as AsyncFile, io::AsyncSeekExt};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::{Instrument, info_span, instrument};

use crate::retry::retry_request;

#[instrument(skip(client))]
pub async fn download_binaries(
    client: obs::Client,
    project: &str,
    package: &str,
    repository: &str,
    arch: &str,
) -> Result<HashMap<String, AsyncFile>> {
    let binary_list = retry_request(|| async {
        client
            .project(project.to_owned())
            .package(package.to_owned())
            .binaries(repository, arch)
            .await
    })
    .await?;
    let mut binaries = HashMap::new();

    for binary in binary_list.binaries {
        let mut dest = retry_request(|| {
            let binary = binary.clone();
            let client = client.clone();
            async move {
                let mut dest = AsyncFile::from_std(
                    tempfile::tempfile().wrap_err("Failed to create temporary file")?,
                );

                let stream = client
                    .project(project.to_owned())
                    .package(package.to_owned())
                    .binary_file(repository, arch, &binary.filename)
                    .await
                    .wrap_err("Failed to request file")?;

                tokio::io::copy(
                    &mut stream.map_err(io::Error::other).into_async_read().compat(),
                    &mut dest,
                )
                .await
                .wrap_err("Failed to download file")?;
                Ok::<AsyncFile, Report>(dest)
            }
        })
        .instrument(info_span!("download_binaries:download", ?binary))
        .await?;

        dest.rewind()
            .instrument(info_span!("download_binaries:rewind", ?binary))
            .await?;
        binaries.insert(binary.filename, dest);
    }

    Ok(binaries)
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use claims::*;
    use open_build_service_mock::*;
    use tokio::io::AsyncReadExt;

    use crate::test_support::*;

    use super::*;

    #[tokio::test]
    async fn test_build_results() {
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

        let mut binaries = assert_ok!(
            download_binaries(client, TEST_PROJECT, TEST_PACKAGE_1, TEST_REPO, TEST_ARCH_1).await
        );
        assert_eq!(binaries.len(), 1);

        let binary = assert_some!(binaries.get_mut(test_file));
        let mut contents = String::new();
        assert_ok!(binary.read_to_string(&mut contents).await);
        assert_eq!(contents, test_contents);
    }
}
