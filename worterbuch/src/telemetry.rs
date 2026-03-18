/*
 *  Copyright (C) 2025 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use opentelemetry::{KeyValue, global, trace::TracerProvider};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_resource_detectors::{
    HostResourceDetector, OsResourceDetector, ProcessResourceDetector,
};
use opentelemetry_sdk::{
    Resource, logs::SdkLoggerProvider, propagation::TraceContextPropagator,
    trace::SdkTracerProvider,
};
use std::{env, io};
use supports_color::Stream;
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use worterbuch_common::error::ConfigResult;

pub struct TelemetryDropGuard {
    logger_provider: SdkLoggerProvider,
    tracer_provider: SdkTracerProvider,
}

impl Drop for TelemetryDropGuard {
    fn drop(&mut self) {
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("Error shutting down tracer provider: {e}");
        }
        if let Err(e) = self.logger_provider.shutdown() {
            eprintln!("Error shutting down logger provider: {e}");
        }
    }
}

pub async fn init(
    node_id: String,
    cluster_role: Option<String>,
) -> ConfigResult<Option<TelemetryDropGuard>> {
    let subscriber = tracing_subscriber::registry().with(
        fmt::Layer::new()
            .with_ansi(supports_color::on(Stream::Stderr).is_some())
            .with_writer(io::stderr)
            .with_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .with_env_var("WORTERBUCH_LOG")
                    .from_env_lossy(),
            ),
    );

    let endpoint = env::var("WORTERBUCH_OPENTELEMETRY_ENDPOINT").ok();

    let drop_guard = if let Some(endpoint) = endpoint {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let log_exporter = {
            let builder = opentelemetry_otlp::LogExporter::builder();
            builder.with_tonic().with_endpoint(&endpoint).build()?
        };

        let span_exporter = {
            let builder = opentelemetry_otlp::SpanExporter::builder();
            builder.with_tonic().with_endpoint(&endpoint).build()?
        };

        let mut resource_builder = Resource::builder()
            .with_service_name("worterbuch")
            .with_detectors(&[
                Box::new(HostResourceDetector::default()),
                Box::new(OsResourceDetector),
                Box::new(ProcessResourceDetector),
            ])
            .with_attributes(vec![KeyValue::new("instance.name", node_id.clone())]);

        if let Some(cluster_role) = cluster_role {
            resource_builder =
                resource_builder.with_attributes(vec![KeyValue::new("cluster.role", cluster_role)]);
        }

        let logger_provider = SdkLoggerProvider::builder()
            .with_batch_exporter(log_exporter)
            .build();

        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter)
            .with_resource(resource_builder.build())
            .build();

        let tracer = tracer_provider.tracer(node_id);

        global::set_tracer_provider(tracer_provider.clone());

        let env_filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .with_env_var("WORTERBUCH_TRACING")
            .from_env_lossy();

        let otel_log_layer =
            OpenTelemetryTracingBridge::new(&logger_provider).with_filter(env_filter.clone());
        let otel_trace_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(env_filter);

        subscriber
            .with(otel_trace_layer)
            .with(otel_log_layer)
            .init();
        info!("Telemetry enabled.");
        Some(TelemetryDropGuard {
            logger_provider,
            tracer_provider,
        })
    } else {
        subscriber.init();
        info!("Telemetry disabled.");
        None
    };

    Ok(drop_guard)
}
