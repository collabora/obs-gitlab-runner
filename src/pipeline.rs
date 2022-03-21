use std::{collections::HashMap, fs::File};

use color_eyre::eyre::{Context, Result};
use serde::Serialize;
use tracing::instrument;

use crate::build_meta::{CommitBuildInfo, RepoArch};

#[derive(Clone, Debug)]
pub struct GeneratePipelineOptions {
    pub build_results_dir: String,
    pub mixin: Option<String>,
    pub prefix: String,
}

#[derive(Serialize)]
struct JobSpec {
    variables: HashMap<String, String>,
    script: Vec<String>,
    #[serde(flatten)]
    mixin: serde_yaml::Mapping,
}

#[instrument]
pub async fn generate_monitor_pipeline(
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
        ]
        .into();

        let mut command = format!(
            "monitor ${} ${} ${} ${} ${} ${}",
            PROJECT_VAR, PACKAGE_VAR, REV_VAR, SRCMD5_VAR, REPO_VAR, ARCH_VAR,
        );

        if let Some(bcnt) = &info.prev_bcnt_for_commit {
            variables.insert(BCNT_VAR.to_owned(), bcnt.clone());
            command += &format!(" --prev-bcnt-for-commit ${}", BCNT_VAR);
        }

        jobs.insert(
            format!("{}-{}-{}", options.prefix, repo, arch),
            JobSpec {
                variables,
                script: vec![command],
                mixin: mixin.clone(),
            },
        );
    }

    // TODO: check this blocking file write & any others
    let mut file = tempfile::tempfile().wrap_err("Failed to create temp file")?;
    serde_yaml::to_writer(&mut file, &jobs).wrap_err("Failed to serialize jobs")?;

    Ok(file)
}
