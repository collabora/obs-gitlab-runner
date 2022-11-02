use color_eyre::{
    config::EyreHook,
    eyre::{EyreHandler, InstallError},
    Report,
};
use tracing::error;
use tracing_error::{ExtractSpanTrace, SpanTrace};
use valuable::Valuable;

struct Handler {
    inner: Box<dyn EyreHandler>,
    location: Option<&'static std::panic::Location<'static>>,
}

impl Handler {
    fn new(inner: Box<dyn EyreHandler>) -> Self {
        assert!(inner.downcast_ref::<color_eyre::Handler>().is_some());

        Self {
            inner,
            location: None,
        }
    }

    fn span_trace(&self) -> Option<&SpanTrace> {
        self.inner
            .downcast_ref::<color_eyre::Handler>()
            .unwrap()
            .span_trace()
    }
}

impl EyreHandler for Handler {
    fn debug(
        &self,
        error: &(dyn std::error::Error + 'static),
        f: &mut core::fmt::Formatter<'_>,
    ) -> core::fmt::Result {
        self.inner.debug(error, f)
    }

    fn display(
        &self,
        error: &(dyn std::error::Error + 'static),
        f: &mut core::fmt::Formatter<'_>,
    ) -> core::fmt::Result {
        self.inner.display(error, f)
    }

    fn track_caller(&mut self, location: &'static std::panic::Location<'static>) {
        self.location = Some(location);
        self.inner.track_caller(location)
    }
}

#[derive(Valuable)]
struct LocationValue {
    // These field names mimic the ones in regular span traces.
    filename: &'static str,
    line_number: u32,
}

#[derive(Valuable)]
struct SpanFrameValue {
    name: String,
    location: Option<LocationValue>,
    fields: String,
}

#[derive(Valuable)]
struct ReportValue {
    location: Option<LocationValue>,
    errors: Vec<String>,
    spans: Vec<SpanFrameValue>,
}

fn get_span_trace_value(span_trace: &SpanTrace) -> Vec<SpanFrameValue> {
    let mut trace: Vec<SpanFrameValue> = vec![];
    span_trace.with_spans(|span, fields| {
        trace.push(SpanFrameValue {
            name: span.name().to_owned(),
            location: match (span.file(), span.line()) {
                (Some(filename), Some(line_number)) => Some(LocationValue {
                    filename,
                    line_number,
                }),
                (_, _) => None,
            },
            fields: fields.to_owned(),
        });
        true
    });
    trace
}

impl ReportValue {
    fn from_report(report: &Report) -> Option<Self> {
        report
            .handler()
            .downcast_ref::<Handler>()
            .map(|handler| Self {
                location: handler.location.map(|location| LocationValue {
                    filename: location.file(),
                    line_number: location.line(),
                }),
                errors: report
                    .chain()
                    .filter_map(|error| {
                        // The is_none() is something color-eyre does here too,
                        // but I'm honestly not sure why.
                        if error.span_trace().is_none() {
                            Some(format!("{}", error))
                        } else {
                            None
                        }
                    })
                    .collect(),
                spans: handler
                    .span_trace()
                    // If the handler has no span trace, get the one closest to
                    // the root cause.
                    .or_else(|| report.chain().rev().filter_map(|e| e.span_trace()).next())
                    .map(get_span_trace_value)
                    .unwrap_or_default(),
            })
    }
}

pub fn install_json_report_hook(hook: EyreHook) -> Result<(), InstallError> {
    let error_hook = hook.into_eyre_hook();
    color_eyre::eyre::set_hook(Box::new(move |error| {
        Box::new(Handler::new(error_hook(error)))
    }))
}

pub fn log_report(report: Report) {
    if let Some(report) = ReportValue::from_report(&report) {
        error!(report = &report.as_value());
    } else {
        error!(?report);
    }
}
