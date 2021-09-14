use crate::storage_layer::JsonStorage;
use serde::ser::{SerializeMap, Serializer};
use serde_json::Value;
use std::io::Write;
use tracing::{Event, Subscriber};
use tracing_core::metadata::Level;
use tracing_log::AsLog;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

const RESERVED_FIELDS: [&str; 3] = ["msg", "level", "time"];

/// Convert from log levels to Bunyan's levels.
fn format_log_level(level: &Level) -> &'static str {
    match level.as_log() {
        log::Level::Error => "ERROR",
        log::Level::Warn => "WARN",
        log::Level::Info => "INFO",
        log::Level::Debug => "DEBUG",
        log::Level::Trace => "TRACE",
    }
}

/// This layer is exclusively concerned with formatting information using the [Bunyan format](https://github.com/trentm/node-bunyan).
/// It relies on the upstream `JsonStorageLayer` to get access to the fields attached to
/// each span.
pub struct BunyanFormattingLayer<W: MakeWriter + 'static> {
    make_writer: W,
}

impl<W: MakeWriter + 'static> BunyanFormattingLayer<W> {
    /// Create a new `BunyanFormattingLayer`.
    ///
    /// You have to specify:
    /// - a `name`, which will be attached to all formatted records according to the [Bunyan format](https://github.com/trentm/node-bunyan#log-record-fields);
    /// - a `make_writer`, which will be used to get a `Write` instance to write formatted records to.
    ///
    /// ## Using stdout
    /// ```rust
    /// use tracing_bunyan_formatter::BunyanFormattingLayer;
    ///
    /// let formatting_layer = BunyanFormattingLayer::new("tracing_example".into(), std::io::stdout);
    /// ```
    ///
    /// If you prefer, you can use closure syntax:
    /// ```rust
    /// use tracing_bunyan_formatter::BunyanFormattingLayer;
    ///
    /// let formatting_layer = BunyanFormattingLayer::new("tracing_example".into(), || std::io::stdout());
    /// ```
    pub fn new(make_writer: W) -> Self {
        Self { make_writer }
    }

    fn serialize_bunyan_core_fields(
        &self,
        map_serializer: &mut impl SerializeMap<Error = serde_json::Error>,
        message: &str,
        level: &Level,
    ) -> Result<(), std::io::Error> {
        map_serializer.serialize_entry("msg", &message)?;
        map_serializer.serialize_entry("level", &format_log_level(level))?;
        map_serializer.serialize_entry("time", &chrono::Utc::now().to_rfc3339())?;
        Ok(())
    }

    /// Given an in-memory buffer holding a complete serialised record, flush it to the writer
    /// returned by self.make_writer.
    ///
    /// We add a trailing new-line at the end of the serialised record.
    ///
    /// If we write directly to the writer returned by self.make_writer in more than one go
    /// we can end up with broken/incoherent bits and pieces of those records when
    /// running multi-threaded/concurrent programs.
    fn emit(&self, mut buffer: Vec<u8>) -> Result<(), std::io::Error> {
        buffer.write_all(b"\n")?;
        self.make_writer.make_writer().write_all(&buffer)
    }
}

/// Ensure consistent formatting of event message.
///
/// Examples:
/// - "[AN_INTERESTING_SPAN - EVENT] My event message" (for an event with a parent span)
/// - "My event message" (for an event without a parent span)
fn format_event_message(event: &Event, event_visitor: &JsonStorage<'_>) -> String {
    // Extract the "message" field, if provided. Fallback to the target, if missing.
    let message = event_visitor
        .values()
        .get("message")
        .map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .flatten()
        .unwrap_or_else(|| event.metadata().target())
        .to_owned();

    message
}

impl<S, W> Layer<S> for BunyanFormattingLayer<W>
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    W: MakeWriter + 'static,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Events do not necessarily happen in the context of a span, hence lookup_current
        // returns an `Option<SpanRef<_>>` instead of a `SpanRef<_>`.
        let current_span = ctx.lookup_current();

        let mut event_visitor = JsonStorage::default();
        event.record(&mut event_visitor);

        // Opting for a closure to use the ? operator and get more linear code.
        let format = || {
            let mut buffer = Vec::new();

            let mut serializer = serde_json::Serializer::new(&mut buffer);
            let mut map_serializer = serializer.serialize_map(None)?;

            let message = format_event_message(event, &event_visitor);
            self.serialize_bunyan_core_fields(
                &mut map_serializer,
                &message,
                event.metadata().level(),
            )?;

            if let Some(span) = &current_span {
                map_serializer.serialize_entry("event", span.metadata().name())?;
            }

            // Additional metadata useful for debugging
            // They should be nested under `src` (see https://github.com/trentm/node-bunyan#src )
            // but `tracing` does not support nested values yet
            map_serializer.serialize_entry("target", event.metadata().target())?;
            map_serializer.serialize_entry("line", &event.metadata().line())?;
            map_serializer.serialize_entry("file", &event.metadata().file())?;

            // Add all the other fields associated with the event, expect the message we already used.
            for (key, value) in event_visitor
                .values()
                .iter()
                .filter(|(&key, _)| key != "message" && !RESERVED_FIELDS.contains(&key))
            {
                map_serializer.serialize_entry(key, value)?;
            }

            // Add all the fields from the current span, if we have one.
            if let Some(span) = &current_span {
                let extensions = span.extensions();
                if let Some(visitor) = extensions.get::<JsonStorage>() {
                    for (key, value) in visitor.values() {
                        if !RESERVED_FIELDS.contains(key) {
                            map_serializer.serialize_entry(key, value)?;
                        } else {
                            tracing::debug!(
                                "{} is a reserved field in the bunyan log format. Skipping it.",
                                key
                            );
                        }
                    }
                }
            }
            map_serializer.end()?;
            Ok(buffer)
        };

        let result: std::io::Result<Vec<u8>> = format();
        if let Ok(formatted) = result {
            let _ = self.emit(formatted);
        }
    }
}
