use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Report, Result, WrapErr};
use futures_util::TryStreamExt;
use open_build_service_api as obs;
use tokio::fs::File as AsyncFile;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::{info_span, instrument, Instrument};

use crate::retry::retry_request;

#[instrument(skip(client))]
pub async fn download_binaries(
    client: obs::Client,
    build_dir: &Path,
    project: &str,
    package: &str,
    repository: &str,
    arch: &str,
) -> Result<HashMap<String, PathBuf>> {
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
        let path = retry_request(|| {
            let binary = binary.clone();
            let client = client.clone();
            async move {
                let (file, path) = tempfile::Builder::new()
                    .prefix("obs-glr-")
                    .suffix(".bin")
                    .tempfile_in(build_dir)?
                    .keep()?;

                let stream = client
                    .project(project.to_owned())
                    .package(package.to_owned())
                    .binary_file(repository, arch, &binary.filename)
                    .await
                    .wrap_err("Failed to request file")?;

                tokio::io::copy(
                    &mut stream
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                        .into_async_read()
                        .compat(),
                    &mut AsyncFile::from_std(file),
                )
                .await
                .wrap_err("Failed to download file")?;
                Ok::<PathBuf, Report>(path)
            }
        })
        .instrument(info_span!("download_binaries:download", ?binary))
        .await?;

        binaries.insert(binary.filename, path);
    }

    Ok(binaries)
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use claim::*;
    use open_build_service_mock::*;
    use tokio::io::AsyncReadExt;

    use crate::test_support::*;

    use super::*;

    #[tokio::test]
    async fn test_build_results() {
        let build_dir = tempfile::Builder::new()
            .prefix("obs-glr-test-bin-")
            .tempdir()
            .unwrap();
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
            download_binaries(
                client,
                build_dir.path(),
                TEST_PROJECT,
                TEST_PACKAGE_1,
                TEST_REPO,
                TEST_ARCH_1
            )
            .await
        );
        assert_eq!(binaries.len(), 1);

        let binary = assert_some!(binaries.get_mut(test_file));
        let mut contents = String::new();
        assert_ok!(
            AsyncFile::open(binary)
                .await
                .unwrap()
                .read_to_string(&mut contents)
                .await
        );
        assert_eq!(contents, test_contents);
    }
}
