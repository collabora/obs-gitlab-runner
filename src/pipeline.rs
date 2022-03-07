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
    build_info_file: &str,
    options: GeneratePipelineOptions,
) -> Result<File> {
    const REPO_VAR: &str = "_OBS_RUNNER_REPO";
    const ARCH_VAR: &str = "_OBS_RUNNER_ARCH";
    const BUILD_INFO_VAR: &str = "_OBS_RUNNER_BUILD_INFO";

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
                        (REPO_VAR.to_owned(), repo.name.clone()),
                        (ARCH_VAR.to_owned(), arch),
                        (BUILD_INFO_VAR.to_owned(), build_info_file.to_owned()),
                    ]
                    .into(),
                    script: vec![format!(
                        "monitor ${} ${} --build-info ${}",
                        REPO_VAR, ARCH_VAR, BUILD_INFO_VAR
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
