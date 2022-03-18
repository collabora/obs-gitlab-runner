use std::{collections::HashMap, fs::File};

use color_eyre::eyre::{Context, Result};
use serde::Serialize;
use tracing::instrument;

use crate::build_meta::RepoArch;

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
    enabled_repos: &[RepoArch],
    options: GeneratePipelineOptions,
) -> Result<File> {
    const PROJECT_VAR: &str = "_OBS_RUNNER_PROJECT";
    const PACKAGE_VAR: &str = "_OBS_RUNNER_PACKAGE";
    const REV_VAR: &str = "_OBS_RUNNER_REV";
    const REPO_VAR: &str = "_OBS_RUNNER_REPO";
    const ARCH_VAR: &str = "_OBS_RUNNER_ARCH";

    let mixin: serde_yaml::Mapping = options
        .mixin
        .as_deref()
        .map(serde_yaml::from_str)
        .transpose()
        .wrap_err("Failed to parse provided rules")?
        .unwrap_or_default();

    let mut jobs = HashMap::new();
    for RepoArch { repo, arch } in enabled_repos {
        jobs.insert(
            format!("{}-{}-{}", options.prefix, repo, arch),
            JobSpec {
                variables: [
                    (PROJECT_VAR.to_owned(), project.to_owned()),
                    (PACKAGE_VAR.to_owned(), package.to_owned()),
                    (REV_VAR.to_owned(), rev.to_owned()),
                    (REPO_VAR.to_owned(), repo.clone()),
                    (ARCH_VAR.to_owned(), arch.clone()),
                ]
                .into(),
                script: vec![format!(
                    "monitor ${} ${} ${} ${} ${}",
                    PROJECT_VAR, PACKAGE_VAR, REV_VAR, REPO_VAR, ARCH_VAR,
                )],
                mixin: mixin.clone(),
            },
        );
    }

    // TODO: check this blocking file write & any others
    let mut file = tempfile::tempfile().wrap_err("Failed to create temp file")?;
    serde_yaml::to_writer(&mut file, &jobs).wrap_err("Failed to serialize jobs")?;

    Ok(file)
}
