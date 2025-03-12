use std::collections::HashMap;

use color_eyre::eyre::{Context, Result};
use serde::Serialize;
use tracing::instrument;

use crate::build_meta::{CommitBuildInfo, RepoArch};

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

fn generate_command(command_name: String, args: &[(&str, String)]) -> String {
    let mut command = vec![command_name];

    for (arg, value) in args {
        command.extend_from_slice(&[format!("--{}", arg), shell_words::quote(value).into_owned()]);
    }

    command.join(" ")
}

#[instrument]
pub fn generate_monitor_pipeline(
    project: &str,
    package: &str,
    rev: &str,
    srcmd5: &str,
    enabled_repos: &HashMap<RepoArch, CommitBuildInfo>,
    options: GeneratePipelineOptions,
) -> Result<String> {
    let rules: Option<serde_yaml::Sequence> = options
        .rules
        .as_deref()
        .map(serde_yaml::from_str)
        .transpose()
        .wrap_err("Failed to parse provided rules")?;

    let mut jobs = HashMap::new();
    for (RepoArch { repo, arch }, info) in enabled_repos {
        let mut script = vec![];
        let mut artifact_paths = vec![];

        let common_args = vec![
            ("project", project.to_owned()),
            ("package", package.to_owned()),
            ("repository", repo.to_owned()),
            ("arch", arch.to_owned()),
        ];

        let mut monitor_args = vec![
            ("rev", rev.to_owned()),
            ("srcmd5", srcmd5.to_owned()),
            ("build-log-out", options.build_log_out.clone()),
        ];
        if let Some(endtime) = &info.prev_endtime_for_commit {
            monitor_args.push(("prev-endtime-for-commit", endtime.to_string()));
        }
        monitor_args.extend_from_slice(&common_args);
        script.push(generate_command("monitor".to_owned(), &monitor_args));
        artifact_paths.push(options.build_log_out.clone());

        if let PipelineDownloadBinaries::OnSuccess { build_results_dir } =
            &options.download_binaries
        {
            let mut download_args = vec![("build-results-dir", build_results_dir.clone())];
            download_args.extend_from_slice(&common_args);
            script.push(generate_command(
                "download-binaries".to_owned(),
                &download_args,
            ));
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

    let string = serde_yaml::to_string(&jobs).wrap_err("Failed to serialize jobs")?;

    Ok(string)
}
