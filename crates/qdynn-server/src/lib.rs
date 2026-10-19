pub mod bridge_html;
mod carriers;
pub mod client_ip;
pub mod config;
pub mod doctor;
pub mod handlers;

/// Build the production router; integration tests use this same policy stack.
pub fn app(state: std::sync::Arc<handlers::AppState>) -> axum::Router {
    use axum::routing::{get, post};
    use handlers::*;
    let routes = axum::Router::new()
        .route("/", get(handle_root))
        .route(
            "/api/v1/session",
            post(handle_create_session).delete(handle_delete_session),
        )
        .route("/api/v1/up", post(handle_up))
        .route("/api/v1/down", post(handle_down))
        .route("/api/v1/ws", get(handle_ws))
        .route("/api/v1/report", post(handle_client_report))
        .route("/api/v1/recover", post(handle_recover))
        .fallback(handle_fallback)
        .with_state(state.clone());
    // Authorize before method routing: otherwise Axum can append API-specific
    // Allow headers even when policy returns the ordinary public website.
    axum::Router::new()
        .fallback_service(routes)
        .layer(axum::extract::DefaultBodyLimit::max(2 * 1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            request_policy,
        ))
}

pub fn admin_app(state: std::sync::Arc<handlers::AppState>) -> axum::Router {
    use axum::{extract::State, http::StatusCode, routing::get};
    use handlers::AppState;
    use std::{sync::Arc, time::Duration};
    axum::Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(|State(state): State<Arc<AppState>>| async move {
            if matches!(tokio::time::timeout(Duration::from_secs(1), tokio::net::TcpStream::connect(state.config.backend)).await, Ok(Ok(_))) { StatusCode::NO_CONTENT } else { StatusCode::SERVICE_UNAVAILABLE }
        }))
        .route("/metrics", get(|State(state): State<Arc<AppState>>| async move {
            let (sessions, streams) = state.registry.counts();
            let counts = state.registry.carrier_request_counts();
            let (latency_sum_us, latency_buckets) = state.registry.carrier_latency();
            let rejections = state.registry.api_rejection_counts();
            let client_reports = state.registry.client_report_counts();
            let client_ip_sources = state.registry.client_ip_source_counts();
            let transport = state.registry.transport_metrics().snapshot();
            let endpoints = ["session", "up", "down", "ws", "recover"];
            let classes = ["1xx", "2xx", "3xx", "4xx", "5xx"];
            let mut body = format!("# TYPE qdynn_sessions gauge\nqdynn_sessions {sessions}\n# TYPE qdynn_streams gauge\nqdynn_streams {streams}\n# TYPE qdynn_carrier_requests_total counter\n");
            body.push_str("# TYPE qdynn_carrier_request_duration_seconds histogram\n");
            body.push_str("# TYPE qdynn_api_rejections_total counter\n");
            for (reason, value) in [
                "http_admission",
                "down_poll_admission",
                "authorization_or_origin",
                "payload_limit",
                "input_budget",
            ]
            .iter()
            .zip(rejections)
            {
                body.push_str(&format!("qdynn_api_rejections_total{{reason=\"{reason}\"}} {value}\n"));
            }
            body.push_str("# TYPE qdynn_client_reports_total counter\n");
            for (reason, counts) in [
                "timeout",
                "session_rejected",
                "websocket_failed",
                "body_failed",
                "protocol_error",
                "transport_error",
            ]
            .iter()
            .zip(client_reports)
            {
                for (phase, count) in ["startup", "runtime"].iter().zip(counts) {
                    body.push_str(&format!("qdynn_client_reports_total{{reason=\"{reason}\",phase=\"{phase}\"}} {count}\n"));
                }
            }
            body.push_str("# TYPE qdynn_client_ip_source_total counter\n");
            for (source, count) in [
                "direct_peer",
                "trusted_forwarded",
                "trusted_proxy_missing",
                "trusted_proxy_invalid",
                "no_peer",
            ]
            .iter()
            .zip(client_ip_sources)
            {
                body.push_str(&format!("qdynn_client_ip_source_total{{source=\"{source}\"}} {count}\n"));
            }
            for (endpoint, endpoint_name) in endpoints.iter().enumerate() {
                let request_count: u64 = counts[endpoint].iter().sum();
                for (class, class_name) in classes.iter().enumerate() {
                    body.push_str(&format!("qdynn_carrier_requests_total{{endpoint=\"{endpoint_name}\",status_class=\"{class_name}\"}} {}\n", counts[endpoint][class]));
                }
                for (bucket, bound_ms) in [1, 5, 10, 50, 100, 500, 1000].iter().enumerate() {
                    body.push_str(&format!("qdynn_carrier_request_duration_seconds_bucket{{endpoint=\"{endpoint_name}\",le=\"{bound_ms}e-3\"}} {}\n", latency_buckets[endpoint][bucket]));
                }
                body.push_str(&format!("qdynn_carrier_request_duration_seconds_bucket{{endpoint=\"{endpoint_name}\",le=\"+Inf\"}} {request_count}\nqdynn_carrier_request_duration_seconds_sum{{endpoint=\"{endpoint_name}\"}} {:.6}\nqdynn_carrier_request_duration_seconds_count{{endpoint=\"{endpoint_name}\"}} {request_count}\n", latency_sum_us[endpoint] as f64 / 1_000_000.0));
            }
            body.push_str(&format!("# TYPE qdynn_backend_dials_total counter\nqdynn_backend_dials_total {}\n# TYPE qdynn_backend_dial_failures_total counter\nqdynn_backend_dial_failures_total {}\n# TYPE qdynn_backend_dial_duration_seconds histogram\n", transport.backend_dials, transport.backend_dial_failures));
            append_histogram(
                &mut body,
                "qdynn_backend_dial_duration_seconds",
                &transport.backend_dial_buckets,
                transport.backend_dials,
                transport.backend_dial_sum_us,
            );
            body.push_str(&format!("# TYPE qdynn_first_backend_byte_total counter\nqdynn_first_backend_byte_total {}\n# TYPE qdynn_first_backend_byte_duration_seconds histogram\n", transport.first_backend_bytes));
            append_histogram(
                &mut body,
                "qdynn_first_backend_byte_duration_seconds",
                &transport.first_backend_byte_buckets,
                transport.first_backend_bytes,
                transport.first_backend_byte_sum_us,
            );
            body.push_str("# TYPE qdynn_recoveries_total counter\n");
            for (outcome, count) in ["success", "rejected", "capacity"].iter().zip(transport.recoveries) {
                body.push_str(&format!("qdynn_recoveries_total{{outcome=\"{outcome}\"}} {count}\n"));
            }
            body.push_str("# TYPE qdynn_recovery_duration_seconds histogram\n");
            append_histogram(
                &mut body,
                "qdynn_recovery_duration_seconds",
                &transport.recovery_buckets,
                transport.recoveries.iter().sum(),
                transport.recovery_sum_us,
            );
            ([ ("content-type", "text/plain; version=0.0.4; charset=utf-8") ], body)
        }))
        .with_state(state)
}

fn append_histogram(body: &mut String, name: &str, buckets: &[u64; 7], count: u64, sum_us: u64) {
    for (bound_ms, value) in [1, 5, 10, 50, 100, 500, 1000].iter().zip(buckets) {
        body.push_str(&format!("{name}_bucket{{le=\"{bound_ms}e-3\"}} {value}\n"));
    }
    body.push_str(&format!(
        "{name}_bucket{{le=\"+Inf\"}} {count}\n{name}_sum {:.6}\n{name}_count {count}\n",
        sum_us as f64 / 1_000_000.0
    ));
}
