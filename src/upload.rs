use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::eyre::{ensure, eyre, Context, Report, Result};
use derivative::*;
use futures_util::{FutureExt, Stream, TryStreamExt};
use gitlab_runner::outputln;
use md5::{Digest, Md5};
use open_build_service_api as obs;
use tracing::{debug, info_span, instrument, trace, Instrument};

use crate::{
    artifacts::ArtifactDirectory,
    dsc::Dsc,
    retry::{retry_large_request, retry_request},
};

type Md5String = String;

const META_NAME: &str = "_meta";

async fn collect_byte_stream<E: std::error::Error + Send + Sync + 'static>(
    stream: impl Stream<Item = Result<Bytes, E>>,
) -> Result<Vec<u8>> {
    let mut data = vec![];
    stream
        .try_for_each(|chunk| {
            data.extend_from_slice(&chunk);
            futures_util::future::ready(Ok(()))
        })
        .await?;
    Ok(data)
}

pub fn compute_md5(data: &[u8]) -> String {
    base16ct::lower::encode_string(&Md5::digest(data))
}

// TODO: do we need this?
#[derive(Debug)]
pub struct ObsUploaderOptions {
    max_commit_attempts: usize,
}

impl Default for ObsUploaderOptions {
    fn default() -> Self {
        Self {
            max_commit_attempts: 3,
        }
    }
}

#[derive(Debug)]
pub struct UploadResult {
    pub rev: String,
    pub unchanged: bool,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ObsUploader<'artifacts, Artifacts>
where
    Artifacts: ArtifactDirectory,
{
    #[derivative(Debug = "ignore")]
    client: obs::Client,

    project: String,
    package: String,

    dsc_path: Utf8PathBuf,
    #[derivative(Debug = "ignore")]
    dsc_contents: Vec<u8>,
    dsc: Dsc,

    #[derivative(Debug = "ignore")]
    artifacts: &'artifacts Artifacts,

    options: ObsUploaderOptions,
}

impl<'artifacts, Artifacts> ObsUploader<'artifacts, Artifacts>
where
    Artifacts: ArtifactDirectory,
{
    pub async fn prepare<'a>(
        client: obs::Client,
        mut project: String,
        branch_to: Option<String>,
        dsc_path: Utf8PathBuf,
        artifacts: &'a Artifacts,
    ) -> Result<ObsUploader<'a, Artifacts>> {
        let dsc_contents = artifacts
            .get_data(dsc_path.as_str())
            .await
            .wrap_err("Failed to download dsc")?;
        let dsc: Dsc =
            rfc822_like::from_bytes(&dsc_contents[..]).wrap_err("Failed to parse dsc")?;

        let package = dsc.source.to_owned();

        if let Some(branch_to) = branch_to {
            outputln!(
                "Branching {}/{} -> {}/{}...",
                project,
                dsc.source,
                branch_to,
                dsc.source
            );

            let mut options = obs::BranchOptions {
                target_project: Some(branch_to.clone()),
                missingok: true,
                force: true,
                add_repositories_rebuild: Some(obs::RebuildMode::Local),
                add_repositories_block: Some(obs::BlockMode::Never),
                ..Default::default()
            };

            let client_package = client.project(project.clone()).package(package.clone());

            let result = retry_request(|| async { client_package.branch(&options).await }).await;
            if let Err(err) = result {
                match err.downcast_ref::<obs::Error>() {
                    Some(obs::Error::ApiError(e)) if e.code == "not_missing" => {
                        options.missingok = false;
                        retry_request(|| async { client_package.branch(&options).await }).await?;
                    }
                    _ => {
                        return Err(err);
                    }
                }
            }

            project = branch_to;
        }

        Ok(ObsUploader {
            client,
            project,
            package,
            dsc_path,
            dsc_contents,
            dsc,
            artifacts,
            options: Default::default(),
        })
    }

    pub fn project(&self) -> &str {
        &self.project
    }

    pub fn package(&self) -> &str {
        &self.package
    }

    #[instrument(skip(self))]
    async fn ensure_package_exists_and_list(&self) -> Result<obs::SourceDirectory> {
        let client_package = self
            .client
            .project(self.project.clone())
            .package(self.package.clone());

        match client_package.list(None).await {
            Ok(dir) => Ok(dir),
            Err(obs::Error::ApiError(obs::ApiError { code, .. })) if code == "unknown_package" => {
                retry_request(|| async {
                    client_package
                        .create()
                        .await
                        .wrap_err("Failed to create missing package")
                })
                .await?;
                client_package.list(None).await.map_err(|e| e.into())
            }
            Err(err) => Err(err.into()),
        }
    }

    #[instrument(skip(self))]
    async fn find_files_to_remove(
        &self,
        files: &HashMap<String, Md5String>,
    ) -> Result<HashSet<Md5String>> {
        let mut to_remove = HashSet::new();

        for file in files.keys() {
            if file.ends_with(".dsc") {
                to_remove.insert(file.clone());

                let contents = retry_request(|| async {
                    collect_byte_stream(
                        self.client
                            .project(self.project.clone())
                            .package(self.package.clone())
                            .source_file(file)
                            .await?,
                    )
                    .await
                })
                .instrument(info_span!("find_files_to_remove:download", %file))
                .await?;

                let _span = info_span!("find_files_to_remove:parse", %file);
                let dsc: Dsc = rfc822_like::from_bytes(&contents[..])?;

                to_remove.extend(dsc.files.into_iter().map(|f| f.filename));
            } else if file.ends_with(".changes") {
                to_remove.insert(file.clone());
            }
        }

        Ok(to_remove)
    }

    #[instrument(skip(self))]
    async fn get_latest_meta_md5(&self) -> Result<Md5String> {
        let dir = retry_request(|| async {
            self.client
                .project(self.project.clone())
                .package(self.package.clone())
                .list_meta(None)
                .await
        })
        .await?;
        let meta = dir
            .entries
            .into_iter()
            .find(|e| e.name == META_NAME)
            .ok_or_else(|| eyre!("Failed to find existing _meta file"))?;
        Ok(meta.md5)
    }

    #[instrument(skip(self))]
    async fn upload_file(&self, root: &Utf8Path, filename: &str) -> Result<()> {
        debug!("Uploading file");
        let file = self
            .artifacts
            .get_file(root.join(filename).as_str())
            .await?;

        retry_large_request(|| {
            file.try_clone().then(|file| async {
                let file = file.wrap_err("Failed to clone file")?;
                self.client
                    .project(self.project.clone())
                    .package(self.package.clone())
                    .upload_for_commit(filename, file)
                    .await
                    .wrap_err("Failed to upload file")?;
                Ok::<(), Report>(())
            })
        })
        .await?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn commit(
        &self,
        commit_message: &str,
        root: &Utf8Path,
        files: HashMap<String, Md5String>,
    ) -> Result<String> {
        let mut commit_files = obs::CommitFileList::new();
        for (name, md5) in files {
            commit_files.add_file_md5(name, md5);
        }

        let mut attempt = 0usize;
        loop {
            match retry_request(|| async {
                self.client
                    .project(self.project.clone())
                    .package(self.package.clone())
                    .commit(
                        &commit_files,
                        &obs::CommitOptions {
                            comment: Some(commit_message.to_owned()),
                        },
                    )
                    .await
            })
            .await?
            {
                obs::CommitResult::Success(dir) => {
                    return dir.rev.ok_or_else(|| eyre!("Revision is empty"));
                }
                obs::CommitResult::MissingEntries(missing) => {
                    trace!("Retry commit, missing {} entries", missing.entries.len());
                    debug!(?missing);

                    ensure!(
                        attempt < self.options.max_commit_attempts,
                        "Failed to push commit after {} attempts (currently missing: {})",
                        self.options.max_commit_attempts,
                        missing
                            .entries
                            .into_iter()
                            .map(|e| format!("{}@{}", e.name, e.md5))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    attempt += 1;

                    for entry in missing.entries {
                        self.upload_file(root, &entry.name).await?;
                    }
                }
            }
        }
    }

    #[instrument]
    pub async fn upload_package(&self) -> Result<UploadResult> {
        let dsc_parent = self
            .dsc_path
            .parent()
            .ok_or_else(|| eyre!("Invalid dsc path: {}", self.dsc_path))?;
        let dsc_filename = self
            .dsc_path
            .file_name()
            .ok_or_else(|| eyre!("Invalid dsc path: {}", self.dsc_path))?;

        outputln!(
            "Uploading {} to {}/{}...",
            dsc_filename,
            self.project,
            self.package
        );

        let dir = self.ensure_package_exists_and_list().await?;
        let present_files: HashMap<_, _> =
            dir.entries.into_iter().map(|e| (e.name, e.md5)).collect();

        let mut files_to_commit = present_files.clone();

        for to_remove in self.find_files_to_remove(&files_to_commit).await? {
            files_to_commit.remove(&to_remove);
        }

        for file in &self.dsc.files {
            files_to_commit.insert(file.filename.clone(), file.hash.clone());
        }

        files_to_commit.insert(META_NAME.to_owned(), self.get_latest_meta_md5().await?);
        files_to_commit.insert(dsc_filename.to_owned(), compute_md5(&self.dsc_contents[..]));

        trace!(?files_to_commit, ?present_files);

        Ok(if files_to_commit != present_files {
            UploadResult {
                rev: self
                    .commit(dsc_filename, dsc_parent, files_to_commit)
                    .await?,
                unchanged: false,
            }
        } else {
            UploadResult {
                // SAFETY: .unwrap() would only fail if the revision is unset, which
                // would only happen if this were the *empty* zero revision. Because
                // we always try to add the _meta file, our files to commit is
                // *never* empty, thus the present revision cannot be empty for this
                // block to be entered.
                rev: dir.rev.unwrap(),
                unchanged: true,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::SystemTime};

    use claim::*;
    use open_build_service_mock::*;

    use crate::{artifacts::test_support::MockArtifactDirectory, test_support::*};

    use super::*;

    const STUB_DSC: &str = "stub.dsc";

    fn add_stub_dsc(artifacts: &mut MockArtifactDirectory, package: &str) {
        artifacts.artifacts.insert(
            STUB_DSC.to_owned(),
            Arc::new(format!("Source: {}", package).as_bytes().to_vec()),
        );
    }

    #[tokio::test]
    async fn test_create_list_package() {
        let test_file = "test";
        let test_contents = b"qwerty123";

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
                path: test_file.to_owned(),
                contents: test_contents.to_vec(),
            },
        );
        mock.add_package_revision(
            TEST_PROJECT,
            TEST_PACKAGE_1,
            MockRevisionOptions::default(),
            [(
                test_file.to_owned(),
                MockEntry::from_key(&test_key, SystemTime::now()),
            )]
            .into(),
        );

        let client = create_default_client(&mock);
        let mut artifacts = MockArtifactDirectory {
            artifacts: [].into(),
        };

        add_stub_dsc(&mut artifacts, TEST_PACKAGE_1);
        let uploader = assert_ok!(
            ObsUploader::prepare(
                client.clone(),
                TEST_PROJECT.to_owned(),
                None,
                STUB_DSC.to_owned().into(),
                &artifacts,
            )
            .await
        );

        let dir = assert_ok!(uploader.ensure_package_exists_and_list().await);
        assert_eq!(dir.entries.len(), 1);
        assert_eq!(dir.entries[0].name, test_file);

        add_stub_dsc(&mut artifacts, TEST_PACKAGE_2);
        let uploader = assert_ok!(
            ObsUploader::prepare(
                client.clone(),
                TEST_PROJECT.to_owned(),
                None,
                STUB_DSC.to_owned().into(),
                &artifacts,
            )
            .await
        );
        let dir = assert_ok!(uploader.ensure_package_exists_and_list().await);
        assert_eq!(dir.entries.len(), 0);
    }

    #[tokio::test]
    async fn test_commit() {
        let test1_file = "test1";
        let test1_contents = b"abcdefg";
        let test1_md5 = compute_md5(test1_contents);

        let test2_file = "test2";
        let test2_contents = b"qwerty";
        let test2_md5 = compute_md5(test2_contents);

        let test_message = "message";

        let mock = create_default_mock().await;
        mock.add_project(TEST_PROJECT.to_owned());
        mock.add_new_package(
            TEST_PROJECT,
            TEST_PACKAGE_1.to_owned(),
            MockPackageOptions::default(),
        );

        let client = create_default_client(&mock);
        let package_1 = client
            .project(TEST_PROJECT.to_owned())
            .package(TEST_PACKAGE_1.to_owned());

        let mut artifacts = MockArtifactDirectory {
            artifacts: [
                (test1_file.to_owned(), Arc::new(test1_contents.to_vec())),
                (test2_file.to_owned(), Arc::new(test2_contents.to_vec())),
            ]
            .into(),
        };

        add_stub_dsc(&mut artifacts, TEST_PACKAGE_1);
        let uploader = assert_ok!(
            ObsUploader::prepare(
                client.clone(),
                TEST_PROJECT.to_owned(),
                None,
                STUB_DSC.to_owned().into(),
                &artifacts,
            )
            .await
        );

        assert_ok!(
            uploader
                .commit(
                    "commit",
                    "".into(),
                    [(test1_file.to_owned(), test1_md5.to_owned())].into(),
                )
                .await
        );

        let mut dir = assert_ok!(package_1.list(None).await);
        assert_eq!(dir.rev.as_deref(), Some("1"));
        assert_eq!(dir.entries.len(), 1);
        dir.entries.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(dir.entries[0].name, test1_file);
        assert_eq!(dir.entries[0].md5, test1_md5);

        assert_ok!(
            uploader
                .commit(
                    "message",
                    "".into(),
                    [
                        (test1_file.to_owned(), test1_md5.to_owned()),
                        (test2_file.to_owned(), test2_md5.to_owned()),
                    ]
                    .into(),
                )
                .await
        );

        let mut dir = assert_ok!(package_1.list(None).await);
        assert_eq!(dir.rev.as_deref(), Some("2"));
        assert_eq!(dir.entries.len(), 2);
        dir.entries.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(dir.entries[0].name, test1_file);
        assert_eq!(dir.entries[0].md5, test1_md5);
        assert_eq!(dir.entries[1].name, test2_file);
        assert_eq!(dir.entries[1].md5, test2_md5);

        let revisions = assert_ok!(package_1.revisions().await);
        let rev = assert_some!(revisions.revisions.last());
        assert_eq!(rev.comment.as_deref(), Some(test_message));

        assert_ok!(
            uploader
                .commit(
                    "message",
                    "".into(),
                    [(test2_file.to_owned(), test2_md5.to_owned())].into(),
                )
                .await
        );

        let mut dir = assert_ok!(package_1.list(None).await);
        assert_eq!(dir.rev.as_deref(), Some("3"));
        assert_eq!(dir.entries.len(), 1);
        dir.entries.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(dir.entries[0].name, test2_file);
        assert_eq!(dir.entries[0].md5, test2_md5);
    }

    #[tokio::test]
    async fn test_upload() {
        let already_present_file = "already-present";
        let already_present_contents = b"already present contents";
        let already_present_md5 = compute_md5(already_present_contents);

        let test1_file = "test1";
        let test1_contents_a = b"first contents";
        let test1_md5_a = compute_md5(test1_contents_a);
        let test1_contents_b = b"second contents";
        let test1_md5_b = compute_md5(test1_contents_b);

        let test2_file = "test2";
        let test2_contents = b"test 2 contents";
        let test2_md5 = compute_md5(test2_contents);

        let dsc1_file: &Utf8Path = "subdir/test-1.dsc".into();
        let dsc1_contents = format!(
            "Source: {}\nFiles:\n {} {} {}",
            TEST_PACKAGE_1,
            test1_md5_a,
            test1_contents_a.len(),
            test1_file,
        );
        let dsc1_md5 = compute_md5(dsc1_contents.as_bytes());

        let dsc2_file: &Utf8Path = "subdir/test-2.dsc".into();
        let dsc2_contents = format!(
            "Source: {}\nFiles:\n {} {} {}\n {} {} {}",
            TEST_PACKAGE_1,
            test1_md5_a,
            test1_contents_a.len(),
            test1_file,
            test2_md5,
            test2_contents.len(),
            test2_file,
        );
        let dsc2_md5 = compute_md5(dsc2_contents.as_bytes());

        let dsc3_file: &Utf8Path = "subdir/test-3.dsc".into();
        let dsc3_contents = format!(
            "Source: {}\nFiles:\n {} {} {}\n {} {} {}",
            TEST_PACKAGE_1,
            test1_md5_b,
            test1_contents_b.len(),
            test1_file,
            test2_md5,
            test2_contents.len(),
            test2_file,
        );
        let dsc3_md5 = compute_md5(dsc3_contents.as_bytes());

        let dsc4_file: &Utf8Path = "subdir/test-4.dsc".into();
        let dsc4_contents = format!(
            "Source: {}\nFiles:\n {} {} {}",
            TEST_PACKAGE_1,
            test1_md5_b,
            test1_contents_b.len(),
            test1_file
        );
        let dsc4_md5 = compute_md5(dsc4_contents.as_bytes());

        let mock = create_default_mock().await;
        mock.add_project(TEST_PROJECT.to_owned());

        let client = create_default_client(&mock);
        let package_1 = client
            .project(TEST_PROJECT.to_owned())
            .package(TEST_PACKAGE_1.to_owned());

        let mut artifacts = MockArtifactDirectory {
            artifacts: [
                (
                    format!("subdir/{}", test1_file),
                    Arc::new(test1_contents_a.to_vec()),
                ),
                (
                    format!("subdir/{}", test2_file),
                    Arc::new(test2_contents.to_vec()),
                ),
                (
                    dsc1_file.to_string(),
                    Arc::new(dsc1_contents.as_bytes().to_vec()),
                ),
                (
                    dsc2_file.to_string(),
                    Arc::new(dsc2_contents.as_bytes().to_vec()),
                ),
                (
                    dsc3_file.to_string(),
                    Arc::new(dsc3_contents.as_bytes().to_vec()),
                ),
                (
                    dsc4_file.to_string(),
                    Arc::new(dsc4_contents.as_bytes().to_vec()),
                ),
            ]
            .into(),
        };

        // Start adding a few source files.

        let uploader = assert_ok!(
            ObsUploader::prepare(
                client.clone(),
                TEST_PROJECT.to_owned(),
                None,
                dsc1_file.to_owned().into(),
                &artifacts
            )
            .await
        );
        let result = assert_ok!(uploader.upload_package().await);
        assert_eq!(result.rev, "1");
        assert!(!result.unchanged);

        let mut dir = assert_ok!(package_1.list(None).await);
        assert_eq!(dir.rev.as_deref(), Some("1"));
        assert_eq!(dir.entries.len(), 3);
        dir.entries.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(dir.entries[0].name, "_meta");
        assert_eq!(dir.entries[1].name, dsc1_file.file_name().unwrap());
        assert_eq!(dir.entries[1].md5, dsc1_md5);
        assert_eq!(dir.entries[2].name, test1_file);
        assert_eq!(dir.entries[2].md5, test1_md5_a);

        let revisions = assert_ok!(package_1.revisions().await);
        let revision = assert_some!(revisions.revisions.last());
        assert_eq!(
            revision.comment.as_deref(),
            Some(dsc1_file.file_name().unwrap())
        );

        // Add a new file & a pre-existing file (that should be kept).

        assert_ok!(
            package_1
                .upload_for_commit(already_present_file, already_present_contents.to_vec())
                .await
        );

        assert_ok!(
            package_1
                .commit(
                    &obs::CommitFileList::new().file_md5(
                        already_present_file.to_owned(),
                        already_present_md5.to_owned()
                    ),
                    &obs::CommitOptions::default()
                )
                .await
        );

        let uploader = assert_ok!(
            ObsUploader::prepare(
                client.clone(),
                TEST_PROJECT.to_owned(),
                None,
                dsc2_file.to_owned().into(),
                &artifacts
            )
            .await
        );
        let result = assert_ok!(uploader.upload_package().await);
        assert_eq!(result.rev, "3");
        assert!(!result.unchanged);

        let mut dir = assert_ok!(package_1.list(None).await);
        assert_eq!(dir.rev.as_deref(), Some("3"));
        assert_eq!(dir.entries.len(), 5);
        dir.entries.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(dir.entries[0].name, "_meta");
        assert_eq!(dir.entries[1].name, already_present_file);
        assert_eq!(dir.entries[1].md5, already_present_md5);
        assert_eq!(dir.entries[2].name, dsc2_file.file_name().unwrap());
        assert_eq!(dir.entries[2].md5, dsc2_md5);
        assert_eq!(dir.entries[3].name, test1_file);
        assert_eq!(dir.entries[3].md5, test1_md5_a);
        assert_eq!(dir.entries[4].name, test2_file);
        assert_eq!(dir.entries[4].md5, test2_md5);

        // Change the contents of one of the files.

        artifacts.artifacts.insert(
            format!("subdir/{}", test1_file),
            Arc::new(test1_contents_b.to_vec()),
        );

        let uploader = assert_ok!(
            ObsUploader::prepare(
                client.clone(),
                TEST_PROJECT.to_owned(),
                None,
                dsc3_file.to_owned().into(),
                &artifacts
            )
            .await
        );
        let result = assert_ok!(uploader.upload_package().await);
        assert_eq!(result.rev, "4");
        assert!(!result.unchanged);

        let mut dir = assert_ok!(package_1.list(None).await);
        assert_eq!(dir.rev.as_deref(), Some("4"));
        assert_eq!(dir.entries.len(), 5);
        dir.entries.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(dir.entries[0].name, "_meta");
        assert_eq!(dir.entries[1].name, already_present_file);
        assert_eq!(dir.entries[1].md5, already_present_md5);
        assert_eq!(dir.entries[2].name, dsc3_file.file_name().unwrap());
        assert_eq!(dir.entries[2].md5, dsc3_md5);
        assert_eq!(dir.entries[3].name, test1_file);
        assert_eq!(dir.entries[3].md5, test1_md5_b);
        assert_eq!(dir.entries[4].name, test2_file);
        assert_eq!(dir.entries[4].md5, test2_md5);

        // Remove the second test file.

        let uploader = assert_ok!(
            ObsUploader::prepare(
                client.clone(),
                TEST_PROJECT.to_owned(),
                None,
                dsc4_file.to_owned().into(),
                &artifacts
            )
            .await
        );
        let result = assert_ok!(uploader.upload_package().await);
        assert_eq!(result.rev, "5");
        assert!(!result.unchanged);

        let mut dir = assert_ok!(package_1.list(None).await);
        assert_eq!(dir.rev.as_deref(), Some("5"));
        assert_eq!(dir.entries.len(), 4);
        dir.entries.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(dir.entries[0].name, "_meta");
        assert_eq!(dir.entries[1].name, already_present_file);
        assert_eq!(dir.entries[1].md5, already_present_md5);
        assert_eq!(dir.entries[2].name, dsc4_file.file_name().unwrap());
        assert_eq!(dir.entries[2].md5, dsc4_md5);
        assert_eq!(dir.entries[3].name, test1_file);
        assert_eq!(dir.entries[3].md5, test1_md5_b);

        // Re-upload with no changes and ensure the old commit is returned.

        let result = assert_ok!(uploader.upload_package().await);
        assert_eq!(result.rev, "5");
        assert!(result.unchanged);

        // Upload to a new branch.

        let branched_project = format!("{}:branch", TEST_PROJECT);
        let uploader = assert_ok!(
            ObsUploader::prepare(
                client.clone(),
                TEST_PROJECT.to_owned(),
                Some(branched_project.clone()),
                dsc4_file.to_owned().into(),
                &artifacts
            )
            .await
        );
        assert_eq!(uploader.project(), branched_project);
        let result = assert_ok!(uploader.upload_package().await);
        // TODO: check the revision, once the mock APIs have branches
        // incorporate the origin's history
        assert!(!result.unchanged);

        let dir = assert_ok!(
            client
                .project(branched_project)
                .package(TEST_PACKAGE_1.to_owned())
                .list(None)
                .await
        );
        // XXX: the mock apis don't set the correct rev values on branch yet
        assert_matches!(dir.rev, Some(_));
    }
}
