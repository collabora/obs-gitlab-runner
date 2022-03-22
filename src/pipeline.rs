use std::{collections::HashMap, fs::File};

use color_eyre::eyre::{Context, Result};
use serde::Serialize;
use tracing::instrument;

use crate::build_meta::{CommitBuildInfo, RepoArch};

#[derive(Clone, Debug)]
pub struct GeneratePipelineOptions {
    pub tags: Vec<String>,
    pub artifact_expiration: String,
    pub build_results_dir: String,
    pub build_log_out: String,
    pub mixin: Option<String>,
    pub prefix: String,
}

#[derive(Serialize)]
struct ArtifactsSpec {
    paths: Vec<String>,
    when: String,
    expire_in: String,
}

#[derive(Serialize)]
struct JobSpec {
    tags: Vec<String>,
    variables: HashMap<String, String>,
    script: Vec<String>,
    artifacts: ArtifactsSpec,
    #[serde(flatten)]
    mixin: serde_yaml::Mapping,
}

#[instrument]
pub fn generate_monitor_pipeline(
    project: &str,
    package: &str,
    rev: &str,
    srcmd5: &str,
    enabled_repos: &HashMap<RepoArch, CommitBuildInfo>,
    options: GeneratePipelineOptions,
) -> Result<File> {
    const PROJECT_VAR: &str = "_OBS_RUNNER_PROJECT";
    const PACKAGE_VAR: &str = "_OBS_RUNNER_PACKAGE";
    const REV_VAR: &str = "_OBS_RUNNER_REV";
    const SRCMD5_VAR: &str = "_OBS_RUNNER_SRCMD5";
    const REPO_VAR: &str = "_OBS_RUNNER_REPO";
    const ARCH_VAR: &str = "_OBS_RUNNER_ARCH";
    const BCNT_VAR: &str = "_OBS_RUNNER_BCNT";

    const BUILD_RESULTS_DIR: &str = "_OBS_BUILD_RESULTS_DIR";
    const BUILD_LOG_OUT: &str = "_OBS_BUILD_LOG_OUT";

    let mixin: serde_yaml::Mapping = options
        .mixin
        .as_deref()
        .map(serde_yaml::from_str)
        .transpose()
        .wrap_err("Failed to parse provided rules")?
        .unwrap_or_default();

    let mut jobs = HashMap::new();
    for (RepoArch { repo, arch }, info) in enabled_repos {
        let mut variables: HashMap<_, _> = [
            (PROJECT_VAR.to_owned(), project.to_owned()),
            (PACKAGE_VAR.to_owned(), package.to_owned()),
            (REV_VAR.to_owned(), rev.to_owned()),
            (SRCMD5_VAR.to_owned(), srcmd5.to_owned()),
            (REPO_VAR.to_owned(), repo.clone()),
            (ARCH_VAR.to_owned(), arch.clone()),
            (
                BUILD_RESULTS_DIR.to_owned(),
                options.build_results_dir.clone(),
            ),
            (BUILD_LOG_OUT.to_owned(), options.build_log_out.clone()),
        ]
        .into();

        let mut command = format!(
            "monitor ${} ${} ${} ${} ${} ${} \
--build-results-dir ${} --build-log-out ${}",
            PROJECT_VAR,
            PACKAGE_VAR,
            REV_VAR,
            SRCMD5_VAR,
            REPO_VAR,
            ARCH_VAR,
            BUILD_RESULTS_DIR,
            BUILD_LOG_OUT
        );

        if let Some(bcnt) = &info.prev_bcnt_for_commit {
            variables.insert(BCNT_VAR.to_owned(), bcnt.clone());
            command += &format!(" --prev-bcnt-for-commit ${}", BCNT_VAR);
        }

        jobs.insert(
            format!("{}-{}-{}", options.prefix, repo, arch),
            JobSpec {
                tags: options.tags.clone(),
                variables,
                script: vec![command],
                artifacts: ArtifactsSpec {
                    paths: vec![
                        options.build_results_dir.clone(),
                        options.build_log_out.clone(),
                    ],
                    when: "always".to_owned(),
                    expire_in: options.artifact_expiration.clone(),
                },
                mixin: mixin.clone(),
            },
        );
    }

    // TODO: check this blocking file write & any others
    let mut file = tempfile::tempfile().wrap_err("Failed to create temp file")?;
    serde_yaml::to_writer(&mut file, &jobs).wrap_err("Failed to serialize jobs")?;

    Ok(file)
}
