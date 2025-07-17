use tracing::{
    Event, Metadata,
    field::{self, Field},
};

pub const TRACING_FIELD: &str = "obo_core.output";

#[macro_export]
macro_rules! outputln {
    ($($args:tt)*) => {
        ::tracing::trace!(obo_core.output = true, $($args)*)
    };
}

struct OutputTester(bool);

impl field::Visit for OutputTester {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == TRACING_FIELD {
            self.0 = value;
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

pub fn is_output_field_set_in_event(event: &Event<'_>) -> bool {
    let mut visitor = OutputTester(false);
    event.record(&mut visitor);
    visitor.0
}

pub fn is_output_field_in_metadata(metadata: &Metadata<'_>) -> bool {
    metadata.fields().iter().any(|f| f.name() == TRACING_FIELD)
}

struct MessageExtractor(Option<String>);

impl field::Visit for MessageExtractor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0 = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

pub fn get_event_message(event: &Event) -> Option<String> {
    let mut visitor = MessageExtractor(None);
    event.record(&mut visitor);
    visitor.0
}
