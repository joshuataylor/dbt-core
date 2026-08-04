//! Dbt-specific tracing initialization built on top of the generic subscriber setup.

use dbt_error::{FsError, FsResult};

use super::{
    dbt_data_layer::{dbt_data_layer_config, dbt_process_span_attributes},
    tracing_feature_handles::TracingConfigProvider,
};
use dbt_tracing::{
    TelemetryAttributes,
    init::{BaseSubscriber, TelemetryHandle},
    layer::{ConsumerLayer, MiddlewareLayer},
    layers::data_layer::TelemetryDataLayer,
    shutdown::TelemetryShutdownItem,
};
#[cfg(test)]
use tracing::Subscriber;
use tracing::{level_filters::LevelFilter, span};
use tracing_subscriber::Layer;

const DBT_TRACING_FILTER_DIRECTIVES: &[&str] = &[
    "hyper=off",
    "h2=off",
    "reqwest=off",
    "ureq=off",
    "opentelemetry=off",
];

/// Maps dbt output verbosity to the base subscriber cap.
///
/// dbt keeps the subscriber open at DEBUG unless TRACE is requested. This lets
/// DEBUG spans/events enter the telemetry pipeline even when a user-facing sink
/// is configured for INFO or lower; individual consumer layers still apply the
/// actual stdout, file, telemetry, or other sink-specific filters. TRACE remains
/// opt-in because native trace spans can be high-volume developer diagnostics.
fn dbt_max_log_verbosity(max_log_verbosity: LevelFilter) -> LevelFilter {
    if matches!(max_log_verbosity, LevelFilter::TRACE) {
        LevelFilter::TRACE
    } else {
        LevelFilter::DEBUG
    }
}

/// Creates a tracing subscriber with dbt's default base filter behavior.
#[cfg(test)]
pub(crate) fn create_tracing_subcriber_with_layer<
    D: Layer<BaseSubscriber> + Send + Sync + 'static,
>(
    max_log_verbosity: LevelFilter,
    data_layer: D,
) -> impl Subscriber + Send + Sync + 'static {
    dbt_tracing::init::create_tracing_subcriber_with_layer(
        dbt_max_log_verbosity(max_log_verbosity),
        data_layer,
        DBT_TRACING_FILTER_DIRECTIVES,
    )
    .expect("dbt tracing filter directives must be valid")
}

/// Initializes tracing with dbt's default base filter behavior.
pub fn init_tracing_with_consumer_layer<D: Layer<BaseSubscriber> + Send + Sync + 'static>(
    max_log_verbosity: LevelFilter,
    process_attributes: TelemetryAttributes,
    data_layer: D,
) -> dbt_tracing::error::TracingResult<span::Span> {
    dbt_tracing::init::init_tracing_with_consumer_layer(
        dbt_max_log_verbosity(max_log_verbosity),
        process_attributes,
        data_layer,
        DBT_TRACING_FILTER_DIRECTIVES,
    )
}

/// Initializes tracing from an explicitly assembled dbt middleware and consumer stack.
pub fn init_tracing_with_layers(
    package: &'static str,
    fallback_trace_id: u128,
    fallback_parent_span_id: Option<u64>,
    max_log_verbosity: LevelFilter,
    middlewares: Vec<MiddlewareLayer>,
    consumer_layers: Vec<ConsumerLayer>,
    shutdown_items: Vec<TelemetryShutdownItem>,
    feature_handle: Box<dyn TracingConfigProvider>,
) -> FsResult<(TelemetryHandle, Box<dyn TracingConfigProvider>)> {
    // Strip code location in non-debug builds
    let strip_code_location = !cfg!(debug_assertions);

    let data_layer = TelemetryDataLayer::new(
        dbt_data_layer_config(fallback_trace_id, fallback_parent_span_id),
        strip_code_location,
        middlewares.into_iter(),
        consumer_layers.into_iter(),
    );

    let process_span = init_tracing_with_consumer_layer(
        max_log_verbosity,
        dbt_process_span_attributes(package),
        data_layer,
    )
    .map_err(FsError::from)?;

    Ok((
        TelemetryHandle::new(shutdown_items, process_span),
        feature_handle,
    ))
}
