use gitlab_runner::GitlabLayer;
use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{self, Field, FieldSet},
    span::{Attributes, Id},
    subscriber::Interest,
};
use tracing_subscriber::{
    Layer,
    filter::{Filtered, Targets},
    layer::{Context, Filter},
    registry::LookupSpan,
};

// Inserted as an extension to indicate that logs within should be forwarded to
// GitLab.
struct GitLabForwarding;

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

// This mostly wraps a standard GitlabLayer, but it bypasses the filter
// sometimes to make sure that anything in a span labeled as
// `obs_gitlab_runner.forward = true` will actually be forwarded to GitLab,
// rather than only *events* that have `gitlab.output = true`.
pub struct GitLabForwarder<S: Subscriber, F: Filter<S>>(Filtered<GitlabLayer, F, S>);

impl<S: Subscriber + Send + Sync + 'static + for<'span> LookupSpan<'span>, F: Filter<S> + 'static>
    GitLabForwarder<S, F>
{
    pub fn new(inner: Filtered<GitlabLayer, F, S>) -> Filtered<Self, Targets, S> {
        GitLabForwarder(inner).with_filter(Targets::new().with_targets([
            ("obs_commander", Level::INFO),
            ("obs_gitlab_runner", Level::INFO),
            // This target is used to inject the current job ID, which
            // gitlab-runner needs to actually send the logs out.
            ("gitlab_runner::gitlab::job", Level::ERROR),
        ]))
    }
}

impl<S: Subscriber + Send + Sync + 'static + for<'span> LookupSpan<'span>, F: Filter<S> + 'static>
    Layer<S> for GitLabForwarder<S, F>
{
    fn on_register_dispatch(&self, subscriber: &tracing::Dispatch) {
        self.0.on_register_dispatch(subscriber);
    }

    fn on_layer(&mut self, subscriber: &mut S) {
        self.0.on_layer(subscriber);
    }

    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        self.0.register_callsite(metadata)
    }

    fn enabled(&self, metadata: &Metadata<'_>, ctx: Context<'_, S>) -> bool {
        // This controls *global* event filtering, not local, so the inner filter
        // should always return `true`. But we need to call it anyway, because
        // `Filter` will *save internal state* that's needed for other API
        // calls, and thus otherwise the event will always be treated as
        // disabled. (Of course, events in the span we want to forward will
        // also be disabled by this, which is why bypassing the filter in
        // `on_event` is important.)
        let enabled = self.0.enabled(metadata, ctx.clone());
        assert!(enabled);
        true
    }

    fn event_enabled(&self, event: &Event<'_>, ctx: Context<'_, S>) -> bool {
        self.0.event_enabled(event, ctx)
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        // If the span metadata has our annotation, then inject an extension
        // that will be used in `on_event` to determine whether or not to
        // forward the logs.
        if attrs
            .metadata()
            .fields()
            .iter()
            .any(|f| f.name() == "obs_gitlab_runner.forward")
        {
            let span = ctx.span(id).unwrap();
            let mut extensions = span.extensions_mut();
            extensions.insert(GitLabForwarding);
        }

        self.0.on_new_span(attrs, id, ctx);
    }

    fn on_follows_from(&self, span: &Id, follows: &Id, ctx: Context<'_, S>) {
        self.0.on_follows_from(span, follows, ctx);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // If a parent span hasn't declared that it wants to forward these
        // events to gitlab, then there's no special behavior needed.
        if !ctx.event_scope(event).is_some_and(|scope| {
            scope
                .from_root()
                .any(|span| span.extensions().get::<GitLabForwarding>().is_some())
        }) {
            self.0.on_event(event, ctx);
            return;
        }

        let mut visitor = MessageExtractor(None);
        event.record(&mut visitor);
        let Some(message) = visitor.0 else {
            return;
        };

        // Create a new event that contains the fields needed for gitlab-runner.
        let fields = FieldSet::new(&["gitlab.output", "message"], event.metadata().callsite());
        let mut iter = fields.iter();
        let values = [
            // "gitlab.output = true"
            (&iter.next().unwrap(), Some(&true as &dyn tracing::Value)),
            // "message"
            (&iter.next().unwrap(), Some(&message as &dyn tracing::Value)),
        ];

        let value_set = fields.value_set(&values);

        let event = if event.is_contextual() {
            // This event's parent is None, but if that's given to new_child_of,
            // then this will be treated as an event at the *root*, i.e.
            // completely parentless. By using `Event::new`, another contextual
            // event will be created, which can still be tied to the correct
            // `event_span`.
            Event::new(event.metadata(), &value_set)
        } else {
            Event::new_child_of(event.parent().cloned(), event.metadata(), &value_set)
        };

        // Bypass the filter completely, because the event was almost certainly
        // filtered out in `enabled` due to lacking `gitlab.output`.
        self.0.inner().on_event(&event, ctx);
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        self.0.on_enter(id, ctx)
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        self.0.on_exit(id, ctx)
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        self.0.on_close(id, ctx)
    }

    fn on_id_change(&self, old: &Id, new: &Id, ctx: Context<'_, S>) {
        self.0.on_id_change(old, new, ctx);
    }
}
