use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use camino::Utf8Path;
use claims::*;
use obo_core::{
    actions::{DEFAULT_BUILD_INFO, DEFAULT_BUILD_LOG, ObsBuildInfo},
    build_meta::RepoArch,
    upload::compute_md5,
};
use obo_test_support::*;
use open_build_service_api as obs;
use open_build_service_mock::*;

#[derive(Clone)]
pub struct ObsContext {
    pub client: obs::Client,
    pub mock: ObsMock,
}

// A handle to some set of artifacts, either artificially "injected" (i.e.
// custom artifacts manually set up to be read by some command) or the outputs
// of executing a command.
pub trait ArtifactsHandle: Send + Sync + Clone + fmt::Debug {}

// The result of a command, letting you easily determine if it succeeded and
// access the output logs / artifacts.
pub trait ExecutionResult: Send + Sync + Clone + fmt::Debug {
    type Artifacts: ArtifactsHandle;

    fn ok(&self) -> bool;
    fn log(&self) -> String;
    fn artifacts(&self) -> Self::Artifacts;
}

pub const EXECUTION_DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

// Builds a set of commands to run, along with the ability to determine its
// input artifacts and timeout.
#[async_trait]
pub trait RunBuilder<'context>: Send + Sync + Sized {
    type ArtifactsHandle: ArtifactsHandle;
    type ExecutionResult: ExecutionResult;

    fn command(self, cmd: impl Into<String>) -> Self {
        self.script(&[cmd.into()])
    }

    fn script(self, cmd: &[String]) -> Self;
    fn artifacts(self, artifacts: Self::ArtifactsHandle) -> Self;
    fn timeout(self, timeout: Duration) -> Self;
    async fn go(self) -> Self::ExecutionResult;
}

// A generic "context", implemented by specific obo frontends, that allows the
// tests to manipulate artifacts and run commands in a generalized way.
#[async_trait]
pub trait TestContext: Send + Sync {
    type ArtifactsHandle: ArtifactsHandle;
    type ExecutionResult: ExecutionResult<Artifacts = Self::ArtifactsHandle>;
    type RunBuilder<'context>: RunBuilder<
            'context,
            ArtifactsHandle = Self::ArtifactsHandle,
            ExecutionResult = Self::ExecutionResult,
        > + 'context
    where
        Self: 'context;

    fn obs(&self) -> &ObsContext;

    async fn inject_artifacts(
        &mut self,
        artifacts: HashMap<String, Vec<u8>>,
    ) -> Self::ArtifactsHandle;

    async fn fetch_artifacts(&self, handle: &Self::ArtifactsHandle) -> HashMap<String, Vec<u8>>;

    fn run(&mut self) -> Self::RunBuilder<'_>;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DputTest {
    Basic,
    Rebuild,
    ReusePreviousBuild,
    Branch,
}

pub async fn test_dput<C: TestContext>(
    context: &mut C,
    test: DputTest,
) -> (C::ArtifactsHandle, ObsBuildInfo) {
    let test1_file = "test1";
    let test1_contents = b"123";
    let test1_md5 = compute_md5(test1_contents);

    let dsc1_file = "test1.dsc";
    let dsc1_contents = format!(
        "Source: {}\nFiles:\n {} {} {}",
        TEST_PACKAGE_1,
        test1_md5.clone(),
        test1_contents.len(),
        test1_file
    );
    let dsc1_md5 = compute_md5(dsc1_contents.as_bytes());

    let dsc1_bad_file = "test1-bad.dsc";
    let dsc1_bad_contents =
        dsc1_contents.replace(test1_file, &(test1_file.to_owned() + ".missing"));

    context.obs().mock.add_project(TEST_PROJECT.to_owned());

    context.obs().mock.add_or_update_repository(
        TEST_PROJECT,
        TEST_REPO.to_owned(),
        TEST_ARCH_1.to_owned(),
        MockRepositoryCode::Finished,
    );
    context.obs().mock.add_or_update_repository(
        TEST_PROJECT,
        TEST_REPO.to_owned(),
        TEST_ARCH_2.to_owned(),
        MockRepositoryCode::Finished,
    );

    if test == DputTest::Rebuild {
        // We also test excluded repos on rebuilds; this test makes it easier,
        // because it's not testing creating a new package, so we can create it
        // ourselves first with the desired metadata.
        context.obs().mock.add_new_package(
            TEST_PROJECT,
            TEST_PACKAGE_1.to_owned(),
            MockPackageOptions::default(),
        );
        context.obs().mock.set_package_build_status(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_2,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus::new(MockPackageCode::Disabled),
        );
    }

    let artifacts = context
        .inject_artifacts(
            [
                (dsc1_file.to_owned(), dsc1_contents.as_bytes().to_vec()),
                (
                    dsc1_bad_file.to_owned(),
                    dsc1_bad_contents.as_bytes().to_vec(),
                ),
                (test1_file.to_owned(), test1_contents.to_vec()),
            ]
            .into(),
        )
        .await;

    let mut dput_command = format!("dput {TEST_PROJECT} {dsc1_file}");
    let mut created_project = TEST_PROJECT.to_owned();

    if test == DputTest::Branch {
        created_project += ":branched";
        dput_command += &format!(" --branch-to {created_project}");
    }

    let dput = context
        .run()
        .command(dput_command.replace(dsc1_file, dsc1_bad_file))
        .artifacts(artifacts.clone())
        .go()
        .await;

    assert!(!dput.ok());

    let results = context.fetch_artifacts(&dput.artifacts()).await;
    let build_info: ObsBuildInfo =
        serde_yaml::from_slice(results.get(DEFAULT_BUILD_INFO).unwrap()).unwrap();

    assert_eq!(build_info.project, created_project);
    assert_eq!(build_info.package, TEST_PACKAGE_1);
    assert_none!(build_info.rev);
    assert_eq!(build_info.is_branched, test == DputTest::Branch);

    let mut dput = context
        .run()
        .command(&dput_command)
        .artifacts(artifacts.clone())
        .go()
        .await;
    assert!(dput.ok());

    if test == DputTest::Rebuild || test == DputTest::ReusePreviousBuild {
        context.obs().mock.add_or_update_repository(
            &created_project,
            TEST_REPO.to_owned(),
            TEST_ARCH_1.to_owned(),
            MockRepositoryCode::Building,
        );
        // Also test endtimes, since we now have an existing package to modify
        // the metadata of.
        let dir = assert_ok!(
            context
                .obs()
                .client
                .project(TEST_PROJECT.to_owned())
                .package(TEST_PACKAGE_1.to_owned())
                .list(None)
                .await
        );
        // Testing of reused builds never had the second arch disabled, so also
        // add that build history.
        if test == DputTest::ReusePreviousBuild {
            context.obs().mock.add_job_history(
                TEST_PROJECT,
                TEST_REPO,
                TEST_ARCH_2,
                MockJobHistoryEntry {
                    package: TEST_PACKAGE_1.to_owned(),
                    rev: dir.rev.clone().unwrap(),
                    srcmd5: dir.srcmd5.clone(),
                    ..Default::default()
                },
            );
        }
        context.obs().mock.add_job_history(
            TEST_PROJECT,
            TEST_REPO,
            TEST_ARCH_1,
            MockJobHistoryEntry {
                package: TEST_PACKAGE_1.to_owned(),
                rev: dir.rev.unwrap(),
                srcmd5: dir.srcmd5,
                ..Default::default()
            },
        );

        context.obs().mock.set_package_build_status_for_rebuilds(
            &created_project,
            MockBuildStatus::new(MockPackageCode::Broken),
        );
        context.obs().mock.set_package_build_status(
            &created_project,
            TEST_REPO,
            TEST_ARCH_1,
            TEST_PACKAGE_1.to_owned(),
            MockBuildStatus::new(MockPackageCode::Failed),
        );

        let status = assert_ok!(
            context
                .obs()
                .client
                .project(created_project.clone())
                .package(TEST_PACKAGE_1.to_owned())
                .status(TEST_REPO, TEST_ARCH_1)
                .await
        );
        assert_eq!(status.code, obs::PackageCode::Failed);

        dput = context
            .run()
            .command(&dput_command)
            .artifacts(artifacts.clone())
            .go()
            .await;
        assert!(dput.ok());

        assert!(dput.log().contains("unchanged"));

        let status = assert_ok!(
            context
                .obs()
                .client
                .project(created_project.clone())
                .package(TEST_PACKAGE_1.to_owned())
                .status(TEST_REPO, TEST_ARCH_1)
                .await
        );
        assert_eq!(status.code, obs::PackageCode::Failed);

        if test == DputTest::Rebuild {
            dput = context
                .run()
                .command(format!("{dput_command} --rebuild-if-unchanged"))
                .artifacts(artifacts.clone())
                .go()
                .await;
            assert!(dput.ok());

            let status = assert_ok!(
                context
                    .obs()
                    .client
                    .project(created_project.clone())
                    .package(TEST_PACKAGE_1.to_owned())
                    .status(TEST_REPO, TEST_ARCH_1)
                    .await
            );
            assert_eq!(status.code, obs::PackageCode::Broken);

            assert!(dput.log().contains("unchanged"));
        }
    }

    let results = context.fetch_artifacts(&dput.artifacts()).await;
    let build_info: ObsBuildInfo =
        serde_yaml::from_slice(results.get(DEFAULT_BUILD_INFO).unwrap()).unwrap();

    assert_eq!(build_info.project, created_project);
    assert_eq!(build_info.package, TEST_PACKAGE_1);
    assert_some!(build_info.rev.as_deref());
    assert_eq!(build_info.is_branched, test == DputTest::Branch);

    assert_eq!(
        build_info.enabled_repos.len(),
        if test == DputTest::Rebuild { 1 } else { 2 }
    );

    let arch_1 = build_info
        .enabled_repos
        .get(&RepoArch {
            repo: TEST_REPO.to_owned(),
            arch: TEST_ARCH_1.to_owned(),
        })
        .unwrap();

    if test == DputTest::Rebuild {
        assert_some!(arch_1.prev_endtime_for_commit);
    } else {
        assert_none!(arch_1.prev_endtime_for_commit);

        let arch_2 = build_info
            .enabled_repos
            .get(&RepoArch {
                repo: TEST_REPO.to_owned(),
                arch: TEST_ARCH_2.to_owned(),
            })
            .unwrap();
        assert_none!(arch_2.prev_endtime_for_commit);
    }

    let mut dir = assert_ok!(
        context
            .obs()
            .client
            .project(created_project.clone())
            .package(TEST_PACKAGE_1.to_owned())
            .list(None)
            .await
    );

    assert_eq!(dir.entries.len(), 3);
    dir.entries.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(dir.entries[0].name, "_meta");
    assert_eq!(dir.entries[1].name, test1_file);
    assert_eq!(dir.entries[1].size, test1_contents.len() as u64);
    assert_eq!(dir.entries[1].md5, test1_md5);
    assert_eq!(dir.entries[2].name, dsc1_file);
    assert_eq!(dir.entries[2].size, dsc1_contents.len() as u64);
    assert_eq!(dir.entries[2].md5, dsc1_md5);

    (dput.artifacts(), build_info)
}

pub const MONITOR_TEST_BUILD_RESULTS_DIR: &str = "results";
pub const MONITOR_TEST_BUILD_RESULT: &str = "test-build-result";
pub const MONITOR_TEST_BUILD_RESULT_CONTENTS: &[u8] = b"abcdef";
pub const MONITOR_TEST_LOG_TAIL: u64 = 50;
pub const MONITOR_TEST_OLD_STATUS_SLEEP_DURATION: Duration = Duration::from_millis(100);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MonitorLogTest {
    // Test that long logs are truncated.
    Long,
    // Test that short logs are fully shown.
    Short,
    // Test that revision mismatches result in unavailable logs.
    Unavailable,
}

#[allow(clippy::too_many_arguments)]
pub async fn test_monitoring<C: TestContext>(
    context: &mut C,
    dput: C::ArtifactsHandle,
    build_info: &ObsBuildInfo,
    repo: &RepoArch,
    script: &[String],
    success: bool,
    dput_test: DputTest,
    log_test: MonitorLogTest,
    download_binaries: bool,
) {
    let srcmd5_prefix = format!(
        "srcmd5 '{}' ",
        if log_test == MonitorLogTest::Unavailable {
            ZERO_REV_SRCMD5.to_owned()
        } else {
            let dir = assert_ok!(
                context
                    .obs()
                    .client
                    .project(build_info.project.to_owned())
                    .package(TEST_PACKAGE_1.to_owned())
                    .list(None)
                    .await
            );

            dir.srcmd5
        }
    );

    let (log_contents, log_vs_limit) = if log_test == MonitorLogTest::Short {
        (srcmd5_prefix + "short", Ordering::Less)
    } else {
        (
            srcmd5_prefix + "this is a long log that will need to be trimmed when printed",
            Ordering::Greater,
        )
    };

    assert_eq!(
        log_contents.len().cmp(&(MONITOR_TEST_LOG_TAIL as usize)),
        log_vs_limit
    );

    // Sanity check this, even though test_dput should have already checked it.
    assert_eq!(repo.repo, TEST_REPO);
    assert!(
        repo.arch == TEST_ARCH_1 || repo.arch == TEST_ARCH_2,
        "unexpected arch '{}'",
        repo.arch
    );

    context.obs().mock.set_package_build_status(
        &build_info.project,
        &repo.repo,
        &repo.arch,
        TEST_PACKAGE_1.to_owned(),
        MockBuildStatus::new(if success {
            MockPackageCode::Succeeded
        } else {
            MockPackageCode::Failed
        }),
    );
    context.obs().mock.add_completed_build_log(
        &build_info.project,
        TEST_REPO,
        &repo.arch,
        TEST_PACKAGE_1.to_owned(),
        MockBuildLog::new(log_contents.to_owned()),
        success,
    );
    context.obs().mock.set_package_binaries(
        &build_info.project,
        TEST_REPO,
        &repo.arch,
        TEST_PACKAGE_1.to_owned(),
        [(
            MONITOR_TEST_BUILD_RESULT.to_owned(),
            MockBinary {
                contents: MONITOR_TEST_BUILD_RESULT_CONTENTS.to_vec(),
                mtime: SystemTime::now(),
            },
        )]
        .into(),
    );

    if dput_test != DputTest::ReusePreviousBuild {
        // Update the endtime in the background, otherwise the monitor will hang
        // forever waiting.
        let mock = context.obs().mock.clone();
        let build_info_2 = build_info.clone();
        let repo_2 = repo.clone();
        tokio::spawn(async move {
            tokio::time::sleep(MONITOR_TEST_OLD_STATUS_SLEEP_DURATION * 10).await;
            mock.add_job_history(
                &build_info_2.project,
                &repo_2.repo,
                &repo_2.arch,
                MockJobHistoryEntry {
                    package: build_info_2.package,
                    endtime: SystemTime::UNIX_EPOCH + Duration::from_secs(999),
                    srcmd5: build_info_2.srcmd5.unwrap(),
                    ..Default::default()
                },
            );
        });
    }

    let monitor = context
        .run()
        .script(script)
        .artifacts(dput.clone())
        .timeout(MONITOR_TEST_OLD_STATUS_SLEEP_DURATION * 20)
        .go()
        .await;
    assert_eq!(
        monitor.ok(),
        success && log_test != MonitorLogTest::Unavailable
    );

    let log = monitor.log();

    assert_eq!(
        log.contains("unavailable"),
        log_test == MonitorLogTest::Unavailable
    );

    // If we reused a previous build, we're not waiting for a new build, so
    // don't check for an old build status.
    let build_actually_occurred = dput_test != DputTest::ReusePreviousBuild;
    assert_eq!(
        log.contains("Waiting for build status"),
        build_actually_occurred
    );

    assert_eq!(
        log.contains(&log_contents),
        !success && log_test == MonitorLogTest::Short
    );

    if !success && log_test == MonitorLogTest::Long {
        let log_bytes = log_contents.as_bytes();
        let truncated_log_bytes = &log_bytes[log_bytes.len() - (MONITOR_TEST_LOG_TAIL as usize)..];
        assert!(log.contains(String::from_utf8_lossy(truncated_log_bytes).as_ref()));
    }

    let results = context.fetch_artifacts(&monitor.artifacts()).await;
    let build_result_path = Utf8Path::new(MONITOR_TEST_BUILD_RESULTS_DIR)
        .join(MONITOR_TEST_BUILD_RESULT)
        .into_string();
    let mut has_built_result = false;

    if log_test != MonitorLogTest::Unavailable {
        let full_log = results.get(DEFAULT_BUILD_LOG).unwrap();
        assert_eq!(log_contents, String::from_utf8_lossy(full_log));

        if success && download_binaries {
            let build_result = results.get(&build_result_path).unwrap();
            assert_eq!(MONITOR_TEST_BUILD_RESULT_CONTENTS, &build_result[..]);

            has_built_result = true;
        }
    }

    if !has_built_result {
        assert_none!(results.get(&build_result_path));
    }
}

pub async fn test_prune_missing_build_info<C: TestContext>(context: &mut C) {
    let prune = context.run().command("prune").go().await;
    assert!(!prune.ok());

    let prune = context
        .run()
        .command("prune --ignore-missing-build-info")
        .go()
        .await;
    assert!(prune.ok());

    assert!(prune.log().contains("Skipping prune"));
}

pub async fn test_prune_deleted_package_1_if_branched<C: TestContext>(
    context: &C,
    build_info: &ObsBuildInfo,
    prune: &C::ExecutionResult,
) {
    if build_info.is_branched {
        assert_err!(
            context
                .obs()
                .client
                .project(build_info.project.clone())
                .package(TEST_PACKAGE_1.to_owned())
                .list(None)
                .await
        );
    } else {
        assert!(prune.log().contains("package was not branched"));

        assert_ok!(
            context
                .obs()
                .client
                .project(build_info.project.clone())
                .package(TEST_PACKAGE_1.to_owned())
                .list(None)
                .await
        );
    }
}
