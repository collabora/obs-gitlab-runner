use std::{collections::HashMap, fs::File};

use color_eyre::eyre::{Context, Result};
use open_build_service_api as obs;
use serde::Serialize;
use tracing::instrument;

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

#[instrument(skip(client))]
pub async fn generate_monitor_pipeline(
    client: obs::Client,
    project: &str,
    package: &str,
    rev: &str,
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

    let meta = client
        .project(project.to_owned())
        .meta()
        .await
        .wrap_err("Failed to get project meta")?;

    let mut jobs = HashMap::new();
    for repo in meta.repositories {
        for arch in repo.arches {
            jobs.insert(
                format!("{}-{}-{}", options.prefix, repo.name, arch),
                JobSpec {
                    variables: [
                        (PROJECT_VAR.to_owned(), project.to_owned()),
                        (PACKAGE_VAR.to_owned(), package.to_owned()),
                        (REV_VAR.to_owned(), rev.to_owned()),
                        (REPO_VAR.to_owned(), repo.name.clone()),
                        (ARCH_VAR.to_owned(), arch),
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
    }

    // TODO: check this blocking file write & any others
    let mut file = tempfile::tempfile().wrap_err("Failed to create temp file")?;
    serde_yaml::to_writer(&mut file, &jobs).wrap_err("Failed to serialize jobs")?;

    Ok(file)
}
