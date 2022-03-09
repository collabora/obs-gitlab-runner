use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use camino::Utf8Path;
use color_eyre::eyre::{ensure, eyre, Context, Result};
use derivative::*;
use futures_util::{Stream, TryStreamExt};
use gitlab_runner::outputln;
use md5::{Digest, Md5};
use open_build_service_api as obs;
use tracing::{debug, instrument, trace};

use crate::{artifacts::ArtifactDirectory, dsc::Dsc};

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

fn compute_md5(data: &[u8]) -> String {
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
    pub project: String,
    pub package: String,
    pub rev: String,
    pub unchanged: bool,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ObsUploader {
    #[derivative(Debug = "ignore")]
    client: obs::Client,

    project: String,
    options: ObsUploaderOptions,
}

impl ObsUploader {
    pub fn new(client: obs::Client, project: String) -> ObsUploader {
        ObsUploader {
            client,
            project,
            options: Default::default(),
        }
    }

    #[instrument(skip(self))]
    async fn branch(&self, package_name: &str, branched_project: &str) -> Result<()> {
        let mut options = obs::BranchOptions {
            target_project: Some(branched_project.to_owned()),
            missingok: true,
            force: true,
            add_repositories_rebuild: Some(obs::RebuildMode::Local),
            add_repositories_block: Some(obs::BlockMode::Never),
            ..Default::default()
        };

        let client_package = self
            .client
            .project(self.project.clone())
            .package(package_name.to_owned());

        match client_package.branch(&options).await {
            Err(obs::Error::ApiError(e)) if e.code == "not_missing" => {
                options.missingok = false;
                client_package.branch(&options).await?;
            }
            result => {
                result?;
            }
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn ensure_package_exists_and_list(
        &self,
        project: &str,
        package: &str,
    ) -> Result<obs::SourceDirectory> {
        let client_package = self
            .client
            .project(project.to_owned())
            .package(package.to_owned());

        match client_package.list(None).await {
            Ok(dir) => Ok(dir),
            Err(obs::Error::ApiError(obs::ApiError { code, .. })) if code == "unknown_package" => {
                client_package
                    .create()
                    .await
                    .wrap_err("Failed to create missing package")?;
                client_package.list(None).await.map_err(|e| e.into())
            }
            Err(err) => return Err(err.into()),
        }
    }

    #[instrument(skip(self))]
    async fn find_files_to_remove(
        &self,
        project: &str,
        package: &str,
        files: &HashMap<String, Md5String>,
    ) -> Result<HashSet<Md5String>> {
        let mut to_remove = HashSet::new();

        for file in files.keys() {
            if file.ends_with(".dsc") {
                to_remove.insert(file.clone());

                // TODO: span with 'file' here
                let contents = collect_byte_stream(
                    self.client
                        .project(project.to_owned())
                        .package(package.to_owned())
                        .source_file(file)
                        .await?,
                )
                .await?;
                let dsc: Dsc = rfc822_like::from_bytes(&contents[..])?;
                to_remove.extend(dsc.files.into_iter().map(|f| f.filename));
            } else if file.ends_with(".changes") {
                to_remove.insert(file.clone());
            }
        }

        Ok(to_remove)
    }

    #[instrument(skip(self))]
    async fn get_latest_meta_md5(&self, project: &str, package: &str) -> Result<Md5String> {
        let dir = self
            .client
            .project(project.to_owned())
            .package(package.to_owned())
            .list_meta(None)
            .await?;
        let meta = dir
            .entries
            .into_iter()
            .find(|e| e.name == META_NAME)
            .ok_or_else(|| eyre!("Failed to find existing _meta file"))?;
        Ok(meta.md5)
    }

    #[instrument(skip(self, artifacts))]
    async fn upload_file(
        &self,
        project: &str,
        package: &str,
        root: &Utf8Path,
        filename: &str,
        artifacts: &impl ArtifactDirectory,
    ) -> Result<()> {
        let file = artifacts.get_file(root.join(filename).as_str()).await?;

        self.client
            .project(project.to_owned())
            .package(package.to_owned())
            .upload_for_commit(filename, file)
            .await
            .wrap_err("Failed to upload file")?;

        Ok(())
    }

    #[instrument(skip(self, artifacts))]
    async fn commit(
        &self,
        project: &str,
        package: &str,
        commit_message: &str,
        root: &Utf8Path,
        files: HashMap<String, Md5String>,
        artifacts: &impl ArtifactDirectory,
    ) -> Result<String> {
        let mut commit_files = obs::CommitFileList::new();
        for (name, md5) in files {
            commit_files.add_file_md5(name, md5);
        }

        let mut attempt = 0usize;
        loop {
            match self
                .client
                .project(project.to_owned())
                .package(package.to_owned())
                .commit(
                    &commit_files,
                    &obs::CommitOptions {
                        comment: Some(commit_message.to_owned()),
                    },
                )
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
                        self.upload_file(project, package, root, &entry.name, artifacts)
                            .await?;
                    }
                }
            }
        }
    }

    #[instrument(skip(artifacts))]
    pub async fn upload_package(
        &self,
        dsc_path: &Utf8Path,
        branch_to: Option<&str>,
        artifacts: &impl ArtifactDirectory,
    ) -> Result<UploadResult> {
        let dsc_parent = dsc_path
            .parent()
            .ok_or_else(|| eyre!("Invalid dsc path: {}", dsc_path))?;
        let dsc_filename = dsc_path
            .file_name()
            .ok_or_else(|| eyre!("Invalid dsc path: {}", dsc_path))?;

        let dsc_data = artifacts
            .get_data(dsc_path.as_str())
            .await
            .wrap_err("Failed to download dsc")?;
        let dsc: Dsc = rfc822_like::from_bytes(&dsc_data[..]).wrap_err("Failed to parse dsc")?;

        let project = if let Some(branch_to) = branch_to {
            outputln!(
                "Branching {}/{} -> {}/{}",
                self.project,
                dsc.source,
                branch_to,
                dsc.source
            );
            self.branch(&dsc.source, branch_to).await?;
            branch_to
        } else {
            &self.project
        };

        outputln!("Upload {} to {}/{}", dsc_filename, project, dsc.source);

        let dir = self
            .ensure_package_exists_and_list(project, &dsc.source)
            .await?;
        let present_files: HashMap<_, _> =
            dir.entries.into_iter().map(|e| (e.name, e.md5)).collect();

        let mut files_to_commit = present_files.clone();

        for to_remove in self
            .find_files_to_remove(project, &dsc.source, &files_to_commit)
            .await?
        {
            files_to_commit.remove(&to_remove);
        }

        for file in dsc.files {
            files_to_commit.insert(file.filename, file.hash);
        }

        files_to_commit.insert(
            META_NAME.to_owned(),
            self.get_latest_meta_md5(project, &dsc.source).await?,
        );
        files_to_commit.insert(dsc_filename.to_owned(), compute_md5(&dsc_data[..]));

        trace!(?files_to_commit, ?present_files);

        let (rev, unchanged) = if files_to_commit != present_files {
            (
                self.commit(
                    project,
                    &dsc.source,
                    dsc_filename,
                    dsc_parent,
                    files_to_commit,
                    artifacts,
                )
                .await?,
                false,
            )
        } else {
            // SAFETY: .unwrap() would only fail if the revision is unset, which
            // would only happen if this were the *empty* zero revision. Because
            // we always try to add the _meta file, our files to commit is
            // *never* empty, thus the present revision cannot be empty for this
            // block to be entered.
            (dir.rev.unwrap(), true)
        };

        Ok(UploadResult {
            project: project.to_owned(),
            package: dsc.source,
            rev,
            unchanged,
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
        let uploader = ObsUploader::new(client, TEST_PROJECT.to_owned());

        let dir = assert_ok!(
            uploader
                .ensure_package_exists_and_list(TEST_PROJECT, TEST_PACKAGE_1)
                .await
        );
        assert_eq!(dir.entries.len(), 1);
        assert_eq!(dir.entries[0].name, test_file);

        let dir = assert_ok!(
            uploader
                .ensure_package_exists_and_list(TEST_PROJECT, TEST_PACKAGE_2)
                .await
        );
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
        let uploader = ObsUploader::new(client.clone(), TEST_PROJECT.to_owned());
        let artifacts = MockArtifactDirectory {
            artifacts: [
                (test1_file.to_owned(), Arc::new(test1_contents.to_vec())),
                (test2_file.to_owned(), Arc::new(test2_contents.to_vec())),
            ]
            .into(),
        };

        assert_ok!(
            uploader
                .commit(
                    TEST_PROJECT,
                    TEST_PACKAGE_1,
                    "commit",
                    "".into(),
                    [(test1_file.to_owned(), test1_md5.to_owned())].into(),
                    &artifacts,
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
                    TEST_PROJECT,
                    TEST_PACKAGE_1,
                    "message",
                    "".into(),
                    [
                        (test1_file.to_owned(), test1_md5.to_owned()),
                        (test2_file.to_owned(), test2_md5.to_owned()),
                    ]
                    .into(),
                    &artifacts,
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
                    TEST_PROJECT,
                    TEST_PACKAGE_1,
                    "message",
                    "".into(),
                    [(test2_file.to_owned(), test2_md5.to_owned())].into(),
                    &artifacts,
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
        let uploader = ObsUploader::new(client.clone(), TEST_PROJECT.to_owned());
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

        let result = assert_ok!(uploader.upload_package(dsc1_file, None, &artifacts).await);
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

        let result = assert_ok!(uploader.upload_package(dsc2_file, None, &artifacts).await);
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

        let result = assert_ok!(uploader.upload_package(dsc3_file, None, &artifacts).await);
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

        let result = assert_ok!(uploader.upload_package(dsc4_file, None, &artifacts).await);
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

        let result = assert_ok!(uploader.upload_package(dsc4_file, None, &artifacts).await);
        assert_eq!(result.rev, "5");
        assert!(result.unchanged);

        // Upload to a new branch.

        let branched_project = format!("{}:branch", TEST_PROJECT);
        let result = assert_ok!(
            uploader
                .upload_package(dsc4_file, Some(&branched_project), &artifacts)
                .await
        );
        eprintln!(
            "{}",
            String::from_utf8_lossy(
                &collect_byte_stream(
                    client
                        .project(branched_project.clone())
                        .package(TEST_PACKAGE_1.to_owned())
                        .source_file("_meta")
                        .await
                        .unwrap()
                )
                .await
                .unwrap()
            )
        );
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
