use std::{fmt, str::FromStr};

use camino::Utf8PathBuf;
use clap::Parser;
use color_eyre::eyre::Result;
use obo_cli::{CliAction, Handler};
use obo_core::logging::{
    get_event_message, is_output_field_in_metadata, is_output_field_set_in_event,
};
use open_build_service_api as obs;
use tracing::{Event, Subscriber};
use tracing_subscriber::{
    filter::Targets,
    fmt::{FmtContext, FormatEvent, FormatFields, format},
    layer::{self, Filter},
    prelude::*,
    registry::LookupSpan,
};
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
        "".parse().unwrap()
    }
}

impl fmt::Display for TargetsArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.parsed_from)
    }
}

struct OutputFilter;

impl<S> Filter<S> for OutputFilter {
    fn enabled(&self, meta: &tracing::Metadata<'_>, _cx: &layer::Context<'_, S>) -> bool {
        is_output_field_in_metadata(meta)
    }

    fn event_enabled(&self, event: &Event<'_>, _cx: &layer::Context<'_, S>) -> bool {
        is_output_field_set_in_event(event)
    }
}

struct OutputFormatter;

impl<S, N> FormatEvent<S, N> for OutputFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let Some(message) = get_event_message(event) else {
            return Ok(());
        };
        writeln!(writer, "{message}")
    }
}

#[derive(Parser)]
struct Args {
    #[clap(long, env = "OBO_LOG", default_value_t = TargetsArg::default())]
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
        .with(
            tracing_subscriber::fmt::layer()
                .event_format(OutputFormatter)
                .with_filter(OutputFilter),
        )
        .init();

    color_eyre::install().unwrap();

    let client = obs::Client::new(args.obs_server, args.obs_user, args.obs_password);
    Handler::new(client, Utf8PathBuf::new())
        .run(args.action)
        .await?;

    Ok(())
}
