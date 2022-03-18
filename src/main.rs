use std::{fmt, str::FromStr};

use clap::Parser;
use color_eyre::eyre::Result;
use gitlab_runner::Runner;
use tracing::{error, info};
use tracing_subscriber::{filter::targets::Targets, prelude::*, util::SubscriberInitExt, Layer};
use url::Url;

use crate::handler::{HandlerOptions, ObsJobHandler};

mod artifacts;
mod binaries;
mod build_meta;
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
    #[clap(env = "GITLAB_URL")]
    server: Url,
    #[clap(env = "GITLAB_TOKEN")]
    token: String,
    #[clap(long, env = "OBS_RUNNER_LOG", default_value_t = TargetsArg::default())]
    log: TargetsArg,
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
                ObsJobHandler::from_obs_config_in_job(job, HandlerOptions::default()).map_err(
                    |err| {
                        error!("Failed to create new client: {:?}", err);
                    },
                )
            },
            MAX_JOBS,
        )
        .await
        .expect("Failed to pick up incoming jobs");
}
