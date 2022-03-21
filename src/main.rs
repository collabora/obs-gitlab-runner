use std::{fmt, str::FromStr};

use clap::Parser;
use color_eyre::eyre::Result;
use gitlab_runner::Runner;
use strum::{Display, EnumString};
use tracing::{error, info, Subscriber};
use tracing_subscriber::{
    filter::targets::Targets,
    fmt::{format::DefaultFields, FormatEvent},
    prelude::*,
    registry::LookupSpan,
    util::SubscriberInitExt,
    Layer,
};
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

#[derive(Display, EnumString)]
#[strum(serialize_all = "lowercase")]
enum LogFormat {
    Pretty,
    Compact,
    Json,
}

#[derive(Parser)]
struct Args {
    #[clap(env = "GITLAB_URL")]
    server: Url,
    #[clap(env = "GITLAB_TOKEN")]
    token: String,
    #[clap(long, env = "OBS_RUNNER_LOG", default_value_t = TargetsArg::default())]
    log: TargetsArg,
    #[clap(long, env = "OBS_RUNNER_LOG_FORMAT", default_value_t = LogFormat::Pretty)]
    log_format: LogFormat,
}

fn formatter_layer<E, S>(format: E, targets: Targets) -> impl Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    E: FormatEvent<S, DefaultFields> + 'static,
{
    tracing_subscriber::fmt::layer()
        .event_format(format)
        .with_filter(targets)
}

#[tokio::main]
async fn main() {
    const MAX_JOBS: usize = 64;

    let args = Args::parse();
    let temp = tempfile::tempdir().expect("Failed to create temporary directory");
    let (mut runner, layer) =
        Runner::new_with_layer(args.server, args.token, temp.path().to_owned());

    let registry = tracing_subscriber::registry()
        .with(tracing_error::ErrorLayer::default())
        .with(layer);

    match args.log_format {
        LogFormat::Compact => registry
            .with(formatter_layer(
                tracing_subscriber::fmt::format().compact(),
                args.log.targets,
            ))
            .init(),
        LogFormat::Json => registry
            .with(formatter_layer(
                tracing_subscriber::fmt::format().json(),
                args.log.targets,
            ))
            .init(),
        LogFormat::Pretty => registry
            .with(formatter_layer(
                tracing_subscriber::fmt::format().pretty(),
                args.log.targets,
            ))
            .init(),
    }

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
