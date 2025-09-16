use std::{fmt, str::FromStr};

use camino::Utf8PathBuf;
use clap::Parser;
use color_eyre::eyre::Result;
use obs_commander_cli::{CliAction, Handler};
use open_build_service_api as obs;
use tracing_subscriber::{filter::Targets, prelude::*};
use url::Url;

#[derive(Debug, Clone)]
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
        "info".parse().unwrap()
    }
}

impl fmt::Display for TargetsArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.parsed_from)
    }
}

#[derive(Parser)]
struct Args {
    #[clap(long, env = "OBS_COMMANDER_LOG", default_value_t = TargetsArg::default())]
    log: TargetsArg,

    #[clap(long, env = "OBS_SERVER")]
    obs_server: Url,
    #[clap(long, env = "OBS_USER")]
    obs_user: String,
    #[clap(long, env = "OBS_PASSWORD")]
    obs_password: String,

    #[clap(subcommand)]
    action: CliAction,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(tracing_error::ErrorLayer::default())
        .with(tracing_subscriber::fmt::layer().with_filter(args.log.targets))
        .init();

    color_eyre::install().unwrap();

    let client = obs::Client::new(args.obs_server, args.obs_user, args.obs_password);
    Handler::new(client, Utf8PathBuf::new())
        .run(args.action)
        .await?;

    Ok(())
}
