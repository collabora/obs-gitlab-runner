use std::{fmt, str::FromStr};

use clap::Parser;
use color_eyre::eyre::{eyre, Result};
use gitlab_runner::{job::Job, Runner};
use open_build_service_api as obs;
use oscrc::Oscrc;
use tracing::{error, info, instrument};
use tracing_subscriber::{filter::targets::Targets, prelude::*, util::SubscriberInitExt, Layer};
use url::Url;

use crate::handler::{HandlerOptions, ObsJobHandler};

mod artifacts;
mod binaries;
mod cleanup;
mod dsc;
mod handler;
mod monitor;
mod pipeline;
mod retry;
mod upload;

#[cfg(test)]
mod test_support;

struct TargetsArg {
    targets: Targets,
    parsed_from: String,
}

impl FromStr for TargetsArg {
    type Err = <Targets as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Targets::from_str(s).map(|targets| TargetsArg {
            targets,
            parsed_from: s.to_owned(),
        })
    }
}

impl Default for TargetsArg {
    fn default() -> Self {
        "obs_gitlab_runner=info".parse().unwrap()
    }
}

impl fmt::Display for TargetsArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.parsed_from)
    }
}

#[derive(Parser)]
struct Args {
    #[clap(env = "GITHUB_URL")]
    server: Url,
    #[clap(env = "GITHUB_TOKEN")]
    token: String,
    #[clap(long, env = "RUST_LOG", default_value_t = TargetsArg::default())]
    log: TargetsArg,
}

#[instrument(skip_all, fields(job = job.id()))]
fn create_obs_client(job: &Job) -> Result<obs::Client> {
    let osc_config = job
        .variable("OSC_CONFIG")
        .ok_or_else(|| eyre!("Missing OSC_CONFIG"))?;

    let oscrc = Oscrc::from_reader(osc_config.value().as_bytes())?;
    let url = oscrc.default_service().clone();
    let (user, pass) = oscrc.credentials(&url)?;

    Ok(obs::Client::new(url, user, pass))
}

#[tokio::main]
async fn main() {
    const MAX_JOBS: usize = 64;

    let args = Args::parse();
    let temp = tempfile::tempdir().expect("Failed to create temporary directory");
    let (mut runner, layer) =
        Runner::new_with_layer(args.server, args.token, temp.path().to_owned());

    tracing_subscriber::registry()
        .with(tracing_error::ErrorLayer::default())
        .with(layer)
        .with(
            tracing_subscriber::fmt::layer()
                .event_format(tracing_subscriber::fmt::format().pretty())
                .with_filter(args.log.targets),
        )
        .init();

    color_eyre::install().unwrap();

    info!("Starting runner...");
    runner
        .run(
            |job| async {
                match create_obs_client(&job) {
                    Ok(client) => Ok(ObsJobHandler::new(job, client, HandlerOptions::default())),
                    Err(err) => {
                        error!("Failed to create new client: {:?}", err);
                        Err(())
                    }
                }
            },
            MAX_JOBS,
        )
        .await
        .expect("Failed to pick up incoming jobs");
}
