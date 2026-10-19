use crate::bridge_html::{
    generate_nonce, render_bridge_page, render_camouflage_page, PERMISSIONS_POLICY,
};
use crate::config::Config;
use axum::{
    body::Bytes,
    extract::{ConnectInfo, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use qdynn_core::SessionRegistry;
use qdynn_proto::verify_bridge_capability;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub struct ResolvedClientIp {
    pub address: Option<IpAddr>,
    pub source: crate::client_ip::ClientIpSource,
}

pub struct AppState {
    pub config: Config,
    pub secret_bytes: Vec<u8>,
    pub registry: Arc<SessionRegistry>,
    pub api_admission: Arc<tokio::sync::Semaphore>,
    pub down_poll_admission: Arc<tokio::sync::Semaphore>,
    pub websocket_admission: Arc<tokio::sync::Semaphore>,
    pub trusted_proxy_cidrs: Vec<crate::client_ip::IpCidr>,
}
impl AppState {
    pub fn new(config: Config, secret_bytes: Vec<u8>, registry: Arc<SessionRegistry>) -> Self {
        let trusted_proxy_cidrs = config
            .trusted_proxy_cidrs
            .iter()
            .map(|cidr| {
                cidr.parse()
                    .expect("Config::validate must parse trusted CIDRs")
            })
            .collect();
        Self {
            api_admission: Arc::new(tokio::sync::Semaphore::new(config.max_api_requests)),
            down_poll_admission: Arc::new(tokio::sync::Semaphore::new(
                (config.max_api_requests * 3 / 4).max(1),
            )),
            websocket_admission: Arc::new(tokio::sync::Semaphore::new(config.max_websockets)),
            trusted_proxy_cidrs,
            config,
            secret_bytes,
            registry,
        }
    }
}

/// Bridge access is bound to the configured hostname; other requests serve the public site.
pub async fn handle_root(
    State(state): State<Arc<AppState>>,
    request: axum::extract::Request,
) -> Response {
    let allowed_host = request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|host| {
            host.eq_ignore_ascii_case(&state.config.hostname)
                || host.eq_ignore_ascii_case(&format!("{}:443", state.config.hostname))
        });

    if let Some(candidate) = request
        .uri()
        .query()
        .filter(|_| request.method() == axum::http::Method::GET)
        .and_then(|q| q.strip_prefix("bridge="))
        .filter(|v| v.len() == 43)
    {
        let is_valid = allowed_host
            && verify_bridge_capability(&state.config.hostname, &state.secret_bytes, candidate);

        if is_valid {
            let nonce = generate_nonce();
            let bootstrap_token = state
                .registry
                .create_bootstrap_token("default".to_string(), state.config.backend);

            if bootstrap_token.is_empty() {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            let html_content = render_bridge_page(crate::bridge_html::BridgePageSettings {
                hostname: &state.config.hostname,
                bridge_capability: candidate,
                bootstrap_token: &bootstrap_token,
                nonce: &nonce,
                carrier_mode: &state.config.carrier_mode,
                startup_timeout_ms: state.config.startup_timeout_ms,
                websocket_open_timeout_ms: state.config.websocket_open_timeout_ms,
                request_timeout_ms: state.config.request_timeout_ms,
            });

            let csp = format!(
                "default-src 'none'; base-uri 'none'; child-src 'none'; connect-src 'self' wss://{host}; font-src 'none'; form-action 'none'; frame-ancestors http://127.0.0.1:*; frame-src 'none'; img-src 'none'; manifest-src 'none'; media-src 'none'; object-src 'none'; script-src 'nonce-{nonce}'; style-src 'none'; worker-src 'none'; sandbox allow-same-origin allow-scripts",
                host = state.config.hostname,
                nonce = nonce
            );

            return (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                    (header::CACHE_CONTROL, "no-store"),
                    (header::REFERRER_POLICY, "no-referrer"),
                    (
                        header::HeaderName::from_static("x-content-type-options"),
                        "nosniff",
                    ),
                    (
                        header::HeaderName::from_static("permissions-policy"),
                        PERMISSIONS_POLICY,
                    ),
                    (
                        header::HeaderName::from_static("content-security-policy"),
                        &csp,
                    ),
                ],
                html_content,
            )
                .into_response();
        }
    }

    public_response(&state, request).await
}

pub async fn handle_fallback(
    State(state): State<Arc<AppState>>,
    request: axum::extract::Request,
) -> Response {
    public_response(&state, request).await
}

/// Static responses are served by Tower's path-safe file service. With no custom
/// directory, unknown paths use the same built-in public page.
pub async fn public_response(state: &AppState, request: axum::extract::Request) -> Response {
    if let Some(dir) = &state.config.public_dir {
        use tower::ServiceExt;
        return tower_http::services::ServeDir::new(dir)
            .oneshot(request)
            .await
            .unwrap()
            .into_response();
    }
    use axum::http::Method;
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            [(header::ALLOW, "GET,HEAD")],
        )
            .into_response();
    }
    let (kind, body) = if request.uri().path() == "/robots.txt" {
        (
            "text/plain; charset=utf-8",
            "User-agent: *\nAllow: /\n".to_owned(),
        )
    } else {
        (
            "text/html; charset=utf-8",
            render_camouflage_page(&state.config.hostname),
        )
    };
    let length = body.len().to_string();
    let body = if request.method() == Method::HEAD {
        String::new()
    } else {
        body
    };
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, kind),
            (header::CONTENT_LENGTH, &length),
        ],
        body,
    )
        .into_response()
}

pub async fn request_policy(
    State(state): State<Arc<AppState>>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let request_started = std::time::Instant::now();
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(address)| *address);
    let forwarded_for = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok());
    let (address, source) =
        crate::client_ip::resolve_client_ip(peer, forwarded_for, &state.trusted_proxy_cidrs);
    state.registry.record_client_ip_source(source.label());
    request
        .extensions_mut()
        .insert(ResolvedClientIp { address, source });
    let path = request.uri().path().to_owned();
    let known_api = matches!(
        path.as_str(),
        "/api/v1/session"
            | "/api/v1/up"
            | "/api/v1/down"
            | "/api/v1/ws"
            | "/api/v1/report"
            | "/api/v1/recover"
    );
    let Ok(_permit) = state.api_admission.clone().try_acquire_owned() else {
        if known_api {
            state.registry.record_api_rejection("http_admission");
        }
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1"), ("cache-control", "no-store")],
        )
            .into_response();
    };
    let _down_poll_permit;
    let mut _input_charge = None;
    if known_api {
        let token = extract_bearer_token(request.headers()).or_else(|| {
            request
                .headers()
                .get("sec-websocket-protocol")?
                .to_str()
                .ok()?
                .split('.')
                .nth(1)
        });
        let valid = if path == "/api/v1/recover" && request.method() == axum::http::Method::POST {
            let capability_valid = request
                .headers()
                .get("x-bridge-capability")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|capability| {
                    verify_bridge_capability(
                        &state.config.hostname,
                        &state.secret_bytes,
                        capability,
                    )
                });
            let previous_valid = if request.headers().contains_key(header::AUTHORIZATION) {
                // Capability authorizes recovery even after a process restart.
                // An optional, well-formed bearer identifies a session to retire.
                token.is_some()
            } else {
                true
            };
            capability_valid && previous_valid
        } else {
            token.is_some_and(|value| {
                if matches!(path.as_str(), "/api/v1/session" | "/api/v1/report")
                    && request.method() == axum::http::Method::POST
                {
                    state.registry.has_bootstrap(value)
                } else {
                    state.registry.get_session(value).is_some()
                }
            })
        };
        if !valid || check_origin(&state, request.headers()).is_err() {
            state
                .registry
                .record_api_rejection("authorization_or_origin");
            return public_response(&state, request).await;
        }
        _down_poll_permit = if path == "/api/v1/down" {
            match state.down_poll_admission.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    state.registry.record_api_rejection("down_poll_admission");
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        [("retry-after", "1"), ("cache-control", "no-store")],
                    )
                        .into_response();
                }
            }
        } else {
            None
        };
        use axum::body::HttpBody;
        let size = request
            .body()
            .size_hint()
            .upper()
            .unwrap_or(2 * 1024 * 1024);
        if path == "/api/v1/report" && size > 256 {
            state.registry.record_api_rejection("payload_limit");
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                [(header::CACHE_CONTROL, "no-store")],
            )
                .into_response();
        }
        if path == "/api/v1/recover" && size != 0 {
            return (
                StatusCode::BAD_REQUEST,
                [(header::CACHE_CONTROL, "no-store")],
            )
                .into_response();
        }
        if size > 2 * 1024 * 1024 {
            state.registry.record_api_rejection("payload_limit");
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                [(header::CACHE_CONTROL, "no-store")],
            )
                .into_response();
        }
        if size != 0 {
            _input_charge = match state.registry.reserve_input(size as usize) {
                Ok(charge) => Some(charge),
                Err(_) => {
                    state.registry.record_api_rejection("input_budget");
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        [("retry-after", "1"), ("cache-control", "no-store")],
                    )
                        .into_response();
                }
            };
        }
    } else if path != "/"
        || !matches!(
            *request.method(),
            axum::http::Method::GET | axum::http::Method::HEAD
        )
    {
        return public_response(&state, request).await;
    }
    let mut response = next.run(request).await;
    if known_api {
        state.registry.record_carrier_request(
            &path,
            response.status().as_u16(),
            request_started.elapsed(),
        );
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    }
    if response.status() == StatusCode::SERVICE_UNAVAILABLE {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, "1".parse().unwrap());
    }
    response
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientReport {
    phase: String,
    reason: String,
}

pub async fn handle_client_report(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let expected_origin = format!("https://{}", state.config.hostname);
    if headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        != Some(expected_origin.as_str())
    {
        return Err(StatusCode::NOT_FOUND);
    }
    if headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        != Some("application/json")
    {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    if body.len() > 256 {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let report: ClientReport =
        serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let token = extract_bearer_token(&headers).ok_or(StatusCode::NOT_FOUND)?;
    let accepted = state
        .registry
        .record_client_report(token, &report.phase, &report.reason);
    if accepted {
        Ok(StatusCode::NO_CONTENT.into_response())
    } else if matches!(report.phase.as_str(), "startup" | "runtime")
        && matches!(
            report.reason.as_str(),
            "timeout"
                | "session_rejected"
                | "websocket_failed"
                | "body_failed"
                | "protocol_error"
                | "transport_error"
        )
    {
        // One accepted report per bootstrap. A duplicate is harmless and does
        // not reveal whether a token was already used for telemetry.
        Ok(StatusCode::NO_CONTENT.into_response())
    } else {
        Err(StatusCode::BAD_REQUEST)
    }
}

pub async fn handle_recover(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let started = std::time::Instant::now();
    let result = handle_recover_inner(&state, &headers);
    let outcome = match &result {
        Ok(_) => qdynn_core::metrics::RecoveryOutcome::Success,
        Err(StatusCode::SERVICE_UNAVAILABLE) => qdynn_core::metrics::RecoveryOutcome::Capacity,
        Err(_) => qdynn_core::metrics::RecoveryOutcome::Rejected,
    };
    state
        .registry
        .transport_metrics()
        .record_recovery(started.elapsed(), outcome);
    result
}

fn handle_recover_inner(state: &AppState, headers: &HeaderMap) -> Result<Response, StatusCode> {
    let expected_origin = format!("https://{}", state.config.hostname);
    let expected_host_with_port = format!("{}:443", state.config.hostname);
    if headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        != Some(expected_origin.as_str())
        || !headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|host| {
                host.eq_ignore_ascii_case(&state.config.hostname)
                    || host.eq_ignore_ascii_case(&expected_host_with_port)
            })
    {
        return Err(StatusCode::NOT_FOUND);
    }
    let capability = headers
        .get("x-bridge-capability")
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() == 43)
        .ok_or(StatusCode::NOT_FOUND)?;
    if !verify_bridge_capability(&state.config.hostname, &state.secret_bytes, capability) {
        return Err(StatusCode::NOT_FOUND);
    }
    let previous_session = match headers.get(header::AUTHORIZATION) {
        Some(_) => Some(extract_bearer_token(headers).ok_or(StatusCode::NOT_FOUND)?),
        None => None,
    };
    let bootstrap = state
        .registry
        .create_recovery_bootstrap(previous_session, "default", state.config.backend)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let body = serde_json::json!({"bootstrap": bootstrap}).to_string();
    if body.len() > 1024 {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        body,
    )
        .into_response())
}

use crate::carriers::{check_origin, extract_bearer_token};
pub use crate::carriers::{
    http::{handle_create_session, handle_delete_session, handle_down, handle_up},
    ws::handle_ws,
};
