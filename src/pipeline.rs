use std::{collections::HashMap, fs::File};

use color_eyre::eyre::{Context, Result};
use serde::Serialize;
use tracing::instrument;

use crate::build_meta::{CommitBuildInfo, RepoArch};

#[derive(Clone, Debug)]
pub struct GeneratePipelineOptions {
    pub tags: Vec<String>,
    pub artifact_expiration: String,
    pub prefix: String,
    pub rules: Option<String>,
    pub build_results_dir: String,
    pub build_log_out: String,
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
    before_script: Vec<String>,
    script: Vec<String>,
    after_script: Vec<String>,
    artifacts: ArtifactsSpec,
    rules: serde_yaml::Sequence,
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
    let rules: serde_yaml::Sequence = options
        .rules
        .as_deref()
        .map(serde_yaml::from_str)
        .transpose()
        .wrap_err("Failed to parse provided rules")?
        .unwrap_or_default();

    let mut jobs = HashMap::new();
    for (RepoArch { repo, arch }, info) in enabled_repos {
        let mut command = vec!["monitor".to_owned()];
        let mut variables = HashMap::new();

        let mut args = vec![
            ("project", project),
            ("package", package),
            ("rev", rev),
            ("srcmd5", srcmd5),
            ("repository", repo),
            ("arch", arch),
            ("build-results-dir", &options.build_results_dir),
            ("build-log-out", &options.build_log_out),
        ];

        if let Some(bcnt) = &info.prev_bcnt_for_commit {
            args.push(("prev-bcnt-for-commit", bcnt.as_str()));
        }

        for (arg, value) in args {
            let var = format!("_OBS_{}", arg.replace('-', "_").to_uppercase());
            command.extend_from_slice(&[format!("--{}", arg), format!("${}", var)]);
            variables.insert(var, value.to_owned());
        }

        jobs.insert(
            format!("{}-{}-{}", options.prefix, repo, arch),
            JobSpec {
                tags: options.tags.clone(),
                variables,
                script: vec![command.join(" ")],
                // The caller's full pipeline file may have had set some
                // defaults for 'before_script' and 'after_script'. We
                // definitely never want those in the generated jobs (they're
                // likely not even valid commands for this runner anyway), so
                // ensure that they're set to be empty.
                before_script: vec![],
                after_script: vec![],
                artifacts: ArtifactsSpec {
                    paths: vec![
                        options.build_results_dir.clone(),
                        options.build_log_out.clone(),
                    ],
                    when: "always".to_owned(),
                    expire_in: options.artifact_expiration.clone(),
                },
                rules: rules.clone(),
            },
        );
    }

    // TODO: check this blocking file write & any others
    let mut file = tempfile::tempfile().wrap_err("Failed to create temp file")?;
    serde_yaml::to_writer(&mut file, &jobs).wrap_err("Failed to serialize jobs")?;

    Ok(file)
}
