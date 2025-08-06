use std::{
    collections::HashMap,
    path::Path,
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use camino::Utf8Path;
use claims::*;
use obo_cli::{DEFAULT_MONITOR_TABLE, MonitorTable};
use obo_core::actions::ObsBuildInfo;
use obo_test_support::*;
use obo_tests::*;
use rstest::rstest;
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};
use tokio_stream::{StreamExt, wrappers::LinesStream};

#[derive(Debug, Clone)]
struct CliArtifactsHandle(Arc<TempDir>);

impl ArtifactsHandle for CliArtifactsHandle {}

#[derive(Clone, Debug)]
struct CliExecutionResult {
    status: ExitStatus,
    out: String,
    artifacts: Arc<TempDir>,
}

impl ExecutionResult for CliExecutionResult {
    type Artifacts = CliArtifactsHandle;

    fn ok(&self) -> bool {
        self.status.success()
    }

    fn log(&self) -> String {
        self.out.clone()
    }

    fn artifacts(&self) -> Self::Artifacts {
        CliArtifactsHandle(self.artifacts.clone())
    }
}

struct CliRunBuilder {
    obs_server: String,
    script: Vec<String>,
    dependencies: Vec<Arc<TempDir>>,
    timeout: Duration,
}

#[async_trait]
impl RunBuilder<'_> for CliRunBuilder {
    type ArtifactsHandle = CliArtifactsHandle;
    type ExecutionResult = CliExecutionResult;

    fn script(mut self, script: &[String]) -> Self {
        for line in script {
            self.script
                .push(format!("{} {line}", env!("CARGO_BIN_EXE_obo")));
        }
        self
    }

    fn artifacts(mut self, artifacts: Self::ArtifactsHandle) -> Self {
        self.dependencies.push(artifacts.0);
        self
    }

    fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    // TODO: timeouts!!
    async fn go(self) -> Self::ExecutionResult {
        let temp = TempDir::new().unwrap();

        // Symlink all the dependency artifacts into the cwd, and clean them up
        // at the end.
        let mut dep_files = vec![];
        for dep in &self.dependencies {
            let mut reader = tokio::fs::read_dir(dep.path()).await.unwrap();
            while let Some(entry) = reader.next_entry().await.unwrap() {
                let dest = temp.path().join(entry.file_name());
                tokio::fs::symlink(entry.path(), &dest).await.unwrap();
                dep_files.push(dest);
            }
        }

        let mut child = Command::new("sh")
            .arg("-exc")
            .arg(self.script.join("\n"))
            .kill_on_drop(true)
            .env("OBS_SERVER", self.obs_server)
            .env("OBS_USER", TEST_USER)
            .env("OBS_PASSWORD", TEST_PASS)
            .env("OBO_TEST_LOG_TAIL", MONITOR_TEST_LOG_TAIL.to_string())
            .env("OBO_TEST_SLEEP_ON_BUILDING_MS", "0")
            .env(
                "OBO_TEST_SLEEP_ON_OLD_STATUS_MS",
                MONITOR_TEST_OLD_STATUS_SLEEP_DURATION
                    .as_millis()
                    .to_string(),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(temp.path())
            .spawn()
            .unwrap();

        let (status, lines) = tokio::time::timeout(self.timeout, async move {
            let stdout = BufReader::new(child.stdout.take().unwrap());
            let stderr = BufReader::new(child.stderr.take().unwrap());

            let mut output =
                LinesStream::new(stdout.lines()).merge(LinesStream::new(stderr.lines()));
            let mut lines = vec![];

            while let Some(line) = output.try_next().await.unwrap() {
                // Forward the lines back to the output.
                eprintln!("{line}");
                lines.push(line);
            }

            let status = child.wait().await.unwrap();
            (status, lines)
        })
        .await
        .unwrap();

        for file in dep_files {
            tokio::fs::remove_file(&file).await.unwrap();
        }

        Self::ExecutionResult {
            status,
            out: lines.join("\n"),
            artifacts: Arc::new(temp),
        }
    }
}

struct CliTestContext {
    obs: ObsContext,
}

fn collect_artifacts_from_dir(out: &mut HashMap<String, Vec<u8>>, root: &Path, subdir: &Utf8Path) {
    for entry in std::fs::read_dir(root.join(subdir.as_std_path())).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        let ft = entry.file_type().unwrap();
        if ft.is_dir() {
            collect_artifacts_from_dir(out, root, &subdir.join(name));
        } else if ft.is_file() {
            let contents = std::fs::read(entry.path())
                .unwrap_or_else(|err| panic!("Fetching {}: {err}", entry.path().display()));
            out.insert(subdir.join(name).into_string(), contents);
        }
    }
}

#[async_trait]
impl TestContext for CliTestContext {
    type ArtifactsHandle = CliArtifactsHandle;
    type ExecutionResult = CliExecutionResult;
    type RunBuilder<'context> = CliRunBuilder;

    fn obs(&self) -> &ObsContext {
        &self.obs
    }

    async fn inject_artifacts(
        &mut self,
        artifacts: HashMap<String, Vec<u8>>,
    ) -> Self::ArtifactsHandle {
        let temp = TempDir::new().unwrap();

        for (name, contents) in artifacts {
            tokio::fs::write(temp.path().join(name), contents)
                .await
                .unwrap();
        }

        CliArtifactsHandle(Arc::new(temp))
    }

    async fn fetch_artifacts(&self, handle: &Self::ArtifactsHandle) -> HashMap<String, Vec<u8>> {
        let mut ret = HashMap::new();
        collect_artifacts_from_dir(&mut ret, handle.0.path(), Utf8Path::new(""));
        ret
    }

    fn run(&mut self) -> Self::RunBuilder<'_> {
        CliRunBuilder {
            obs_server: self.obs.client.url().to_string(),
            script: vec![],
            dependencies: vec![],
            timeout: EXECUTION_DEFAULT_TIMEOUT,
        }
    }
}

async fn with_context<T>(func: impl AsyncFnOnce(CliTestContext) -> T) -> T {
    let obs_mock = create_default_mock().await;
    let obs_client = create_default_client(&obs_mock);

    let ctx = CliTestContext {
        obs: ObsContext {
            client: obs_client,
            mock: obs_mock,
        },
    };

    func(ctx).await
}

async fn test_monitor_table(
    context: &mut CliTestContext,
    dput: CliArtifactsHandle,
    build_info: &ObsBuildInfo,
    success: bool,
    dput_test: DputTest,
    log_test: MonitorLogTest,
    download_binaries: bool,
) {
    let mut generate_command = "generate-monitor".to_owned();
    if download_binaries {
        generate_command +=
            &format!(" --download-build-results-to {MONITOR_TEST_BUILD_RESULTS_DIR}");
    }

    let generate = context
        .run()
        .command(generate_command)
        .artifacts(dput.clone())
        .go()
        .await;
    assert!(generate.ok());

    let results = context.fetch_artifacts(&generate.artifacts()).await;
    let table: MonitorTable = assert_ok!(serde_json::from_slice(
        results.get(DEFAULT_MONITOR_TABLE).unwrap()
    ));

    for enabled in &build_info.enabled_repos {
        let entry = table
            .entries
            .iter()
            .find(|m| m.repo_arch == enabled.repo_arch)
            .unwrap();

        let mut script = vec![entry.commands.monitor.clone()];
        if download_binaries {
            script.push(entry.commands.download_binaries.clone().unwrap());
        } else {
            assert_none!(&entry.commands.download_binaries);
        }

        test_monitoring(
            context,
            dput.clone(),
            build_info,
            &enabled.repo_arch,
            &script,
            success,
            dput_test,
            log_test,
            download_binaries,
        )
        .await;
    }
}

async fn test_prune(
    context: &mut CliTestContext,
    dput: CliArtifactsHandle,
    build_info: &ObsBuildInfo,
) {
    test_prune_missing_build_info(context).await;

    let prune = context
        .run()
        .command("prune")
        .artifacts(dput.clone())
        .go()
        .await;
    test_prune_deleted_package_1_if_branched(context, build_info, &prune).await;
}

#[rstest]
#[tokio::test]
async fn test_cli_flow(
    #[values(
        DputTest::Basic,
        DputTest::Rebuild,
        DputTest::ReusePreviousBuild,
        DputTest::Branch
    )]
    dput_test: DputTest,
    #[values(true, false)] build_success: bool,
    #[values(
        MonitorLogTest::Long,
        MonitorLogTest::Short,
        MonitorLogTest::Unavailable
    )]
    log_test: MonitorLogTest,
    #[values(true, false)] download_binaries: bool,
) {
    with_context(async |mut context| {
        let (dput, build_info) = test_dput(&mut context, dput_test).await;

        test_monitor_table(
            &mut context,
            dput.clone(),
            &build_info,
            build_success,
            dput_test,
            log_test,
            download_binaries,
        )
        .await;

        test_prune(&mut context, dput.clone(), &build_info).await;
    })
    .await;
}
