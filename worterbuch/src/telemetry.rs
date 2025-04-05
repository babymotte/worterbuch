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
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_resource_detectors::{
    HostResourceDetector, OsResourceDetector, ProcessResourceDetector,
};
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};
use std::{env, io};
use supports_color::Stream;
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use worterbuch_common::error::ConfigResult;

pub async fn init(node_id: String, cluster_role: Option<String>) -> ConfigResult<()> {
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

    if let Some(endpoint) = endpoint {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let exporter = {
            let builder = opentelemetry_otlp::SpanExporter::builder();

            builder.with_tonic().with_endpoint(endpoint).build()?
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

        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource_builder.build())
            .build();

        let tracer = tracer_provider.tracer(node_id);

        global::set_tracer_provider(tracer_provider);

        let opentelemetry = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .with_env_var("WORTERBUCH_TRACING")
                    .from_env_lossy(),
            );

        subscriber.with(opentelemetry).init();
        info!("Telemetry enabled.");
    } else {
        subscriber.init();
        info!("Telemetry disabled.");
    }

    Ok(())
}
