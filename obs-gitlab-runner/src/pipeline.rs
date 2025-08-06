use std::collections::HashMap;

use color_eyre::eyre::{Context, Result};
use obo_core::{
    actions::{DownloadBinariesAction, MonitorAction},
    build_meta::{EnabledRepo, RepoArch},
};
use serde::Serialize;
use tracing::instrument;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PipelineDownloadBinaries {
    OnSuccess { build_results_dir: String },
    Never,
}

#[derive(Clone, Debug)]
pub struct GeneratePipelineOptions {
    pub tags: Vec<String>,
    pub artifact_expiration: String,
    pub prefix: String,
    pub timeout: Option<String>,
    pub rules: Option<String>,
    pub build_log_out: String,
    pub download_binaries: PipelineDownloadBinaries,
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
    before_script: Vec<String>,
    script: Vec<String>,
    after_script: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout: Option<String>,
    artifacts: ArtifactsSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    rules: Option<serde_yaml::Sequence>,
}

#[instrument]
pub fn generate_monitor_pipeline(
    project: &str,
    package: &str,
    rev: &str,
    srcmd5: &str,
    enabled_repos: &[EnabledRepo],
    options: GeneratePipelineOptions,
) -> Result<String> {
    let rules: Option<serde_yaml::Sequence> = options
        .rules
        .as_deref()
        .map(serde_yaml::from_str)
        .transpose()
        .wrap_err("Failed to parse provided rules")?;

    let mut jobs = HashMap::new();
    for enabled in enabled_repos {
        let RepoArch { repo, arch } = &enabled.repo_arch;

        let mut script = vec![];
        let mut artifact_paths = vec![];

        script.push(
            MonitorAction {
                project: project.to_owned(),
                package: package.to_owned(),
                repository: repo.to_owned(),
                arch: arch.to_owned(),
                rev: rev.to_owned(),
                srcmd5: srcmd5.to_owned(),
                build_log_out: options.build_log_out.clone(),
                prev_endtime_for_commit: enabled.prev_endtime_for_commit,
            }
            .generate_command(),
        );
        artifact_paths.push(options.build_log_out.clone());

        if let PipelineDownloadBinaries::OnSuccess { build_results_dir } =
            &options.download_binaries
        {
            script.push(
                DownloadBinariesAction {
                    project: project.to_owned(),
                    package: package.to_owned(),
                    repository: repo.to_owned(),
                    arch: arch.to_owned(),
                    build_results_dir: build_results_dir.into(),
                }
                .generate_command(),
            );
            artifact_paths.push(build_results_dir.clone());
        }

        jobs.insert(
            format!("{}-{}-{}", options.prefix, repo, arch),
            JobSpec {
                tags: options.tags.clone(),
                script,
                // The caller's full pipeline file may have had set some
                // defaults for 'before_script' and 'after_script'. We
                // definitely never want those in the generated jobs (they're
                // likely not even valid commands for this runner anyway), so
                // ensure that they're set to be empty.
                before_script: vec![],
                after_script: vec![],
                timeout: options.timeout.clone(),
                artifacts: ArtifactsSpec {
                    paths: artifact_paths,
                    when: "always".to_owned(),
                    expire_in: options.artifact_expiration.clone(),
                },
                rules: rules.clone(),
            },
        );
    }

    serde_yaml::to_string(&jobs).wrap_err("Failed to serialize jobs")
}
