use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{HeaderMap, Request, StatusCode},
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use qdynn_core::{BackendConnector, BackendParts, Session, SessionRegistry, TcpBackendConnector};
use qdynn_proto::{derive_bridge_capability, encode_batch, parse_secret, Frame};
use qdynn_server::{
    config::Config,
    handlers::{self, AppState},
};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tower::ServiceExt;

fn state(backend: std::net::SocketAddr, mode: &str) -> Arc<AppState> {
    Arc::new(AppState::new(
        Config {
            hostname: "test.example".into(),
            backend,
            carrier_mode: mode.into(),
            secret: "000102030405060708090a0b0c0d0e0f".into(),
            ..Config::default()
        },
        parse_secret("000102030405060708090a0b0c0d0e0f").unwrap(),
        Arc::new(SessionRegistry::new()),
    ))
}
fn session(state: &AppState) -> Arc<Session> {
    let token = state
        .registry
        .create_bootstrap_token("test".into(), state.config.backend);
    state
        .registry
        .exchange_bootstrap_token_for_carrier(&token, &state.config.carrier_mode)
        .unwrap()
}

struct CountingBackendConnector {
    calls: AtomicUsize,
    tcp: TcpBackendConnector,
}
impl BackendConnector for CountingBackendConnector {
    fn connect<'a>(
        &'a self,
        address: std::net::SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<BackendParts>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.tcp.connect(address)
    }
}

#[tokio::test]
async fn registry_backend_connector_is_used_for_streams() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connector = Arc::new(CountingBackendConnector {
        calls: AtomicUsize::new(0),
        tcp: TcpBackendConnector,
    });
    let registry = SessionRegistry::new().with_backend_connector(connector.clone());
    let token = registry.create_bootstrap_token("test".into(), backend.local_addr().unwrap());
    let session = registry.exchange_bootstrap_token(&token).unwrap();

    session.handle_frame(Frame::open(1)).await.unwrap();
    let (_socket, _) = timeout(Duration::from_secs(2), backend.accept())
        .await
        .expect("backend adapter did not connect")
        .unwrap();
    assert_eq!(connector.calls.load(Ordering::Relaxed), 1);
    session.close_all();
}

#[test]
fn runtime_registry_limits_bound_bootstraps_and_sessions() {
    let backend = "127.0.0.1:2398".parse().unwrap();
    let bootstrap_limited = SessionRegistry::with_limits(1, 2);
    assert!(!bootstrap_limited
        .create_bootstrap_token("test".into(), backend)
        .is_empty());
    assert!(bootstrap_limited
        .create_bootstrap_token("test".into(), backend)
        .is_empty());

    let registry = SessionRegistry::with_limits(2, 1);
    let first = registry.create_bootstrap_token("test".into(), backend);
    assert!(!first.is_empty());
    assert!(registry.exchange_bootstrap_token(&first).is_ok());
    let second = registry.create_bootstrap_token("test".into(), backend);
    assert!(!second.is_empty());
    assert!(registry.exchange_bootstrap_token(&second).is_err());
}

#[test]
fn profile_caps_are_independent_for_bootstraps_and_sessions() {
    let backend = "127.0.0.1:2398".parse().unwrap();
    let bootstrap_limited =
        SessionRegistry::with_profile_limits(4, 4, 1, 4, qdynn_core::queue::QueueConfig::default());
    assert!(!bootstrap_limited
        .create_bootstrap_token("alpha".into(), backend)
        .is_empty());
    assert!(bootstrap_limited
        .create_bootstrap_token("alpha".into(), backend)
        .is_empty());
    assert!(!bootstrap_limited
        .create_bootstrap_token("beta".into(), backend)
        .is_empty());

    let session_limited =
        SessionRegistry::with_profile_limits(4, 4, 4, 1, qdynn_core::queue::QueueConfig::default());
    let alpha1 = session_limited.create_bootstrap_token("alpha".into(), backend);
    let alpha2 = session_limited.create_bootstrap_token("alpha".into(), backend);
    let beta = session_limited.create_bootstrap_token("beta".into(), backend);
    assert!(session_limited.exchange_bootstrap_token(&alpha1).is_ok());
    assert!(session_limited.exchange_bootstrap_token(&alpha2).is_err());
    assert!(session_limited.exchange_bootstrap_token(&beta).is_ok());
}

#[test]
fn runtime_queue_and_backend_limits_are_shared_and_enforced() {
    let limits = qdynn_core::queue::QueueConfig {
        max_streams_per_session: 1,
        max_lanes_per_session: 1,
        backend_connections: 1,
        ..qdynn_core::queue::QueueConfig::default()
    };
    let registry = SessionRegistry::with_runtime_config(4, 2, limits);
    let backend = "127.0.0.1:2398".parse().unwrap();
    let first_token = registry.create_bootstrap_token("test".into(), backend);
    let second_token = registry.create_bootstrap_token("test".into(), backend);
    let first = registry.exchange_bootstrap_token(&first_token).unwrap();
    let second = registry.exchange_bootstrap_token(&second_token).unwrap();
    assert!(first.validate_batch(&[qdynn_proto::Frame::open(1)]).is_ok());
    assert!(first
        .validate_batch(&[qdynn_proto::Frame::open(1), qdynn_proto::Frame::open(2),])
        .is_err());
    assert!(first.lane(1).is_err());
    let permit = first.downlink_tx.budget().try_backend_permit().unwrap();
    assert!(second.downlink_tx.budget().try_backend_permit().is_err());
    drop(permit);
    assert!(second.downlink_tx.budget().try_backend_permit().is_ok());
}

#[tokio::test]
async fn down_poll_admission_leaves_capacity_for_uplink_control() {
    let mut state = state("127.0.0.1:2398".parse().unwrap(), "https");
    let app_state = Arc::get_mut(&mut state).unwrap();
    app_state.config.max_api_requests = 8;
    app_state.api_admission = Arc::new(tokio::sync::Semaphore::new(8));
    app_state.down_poll_admission = Arc::new(tokio::sync::Semaphore::new(6));
    let sessions: Vec<_> = (0..6).map(|_| session(&state)).collect();
    let app = qdynn_server::app(state.clone());

    let down_request = |session: &Session| {
        Request::builder()
            .method("POST")
            .uri("/api/v1/down")
            .header("authorization", format!("Bearer {}", session.id))
            .header("origin", "https://test.example")
            .header("x-down-cursor", "0")
            .body(Body::empty())
            .unwrap()
    };
    let polls: Vec<_> = sessions
        .iter()
        .map(|session| tokio::spawn(app.clone().oneshot(down_request(session))))
        .collect();
    timeout(Duration::from_secs(1), async {
        while state.down_poll_admission.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(state.api_admission.available_permits(), 2);

    let saturated_poll = app
        .clone()
        .oneshot(down_request(&sessions[0]))
        .await
        .unwrap();
    assert_eq!(saturated_poll.status(), StatusCode::SERVICE_UNAVAILABLE);

    let uplink_request = |session: &Session| {
        let body = encode_batch(&[Frame::pong(Bytes::new())]);
        let mut uplink = Request::builder()
            .method("POST")
            .uri("/api/v1/up")
            .header("authorization", format!("Bearer {}", session.id))
            .header("origin", "https://test.example")
            .header("x-up-seq", "1")
            .body(Body::from(body))
            .unwrap();
        uplink
            .headers_mut()
            .insert("content-type", "application/octet-stream".parse().unwrap());
        uplink
    };
    let (first_uplink, second_uplink) = tokio::join!(
        app.clone().oneshot(uplink_request(&sessions[0])),
        app.clone().oneshot(uplink_request(&sessions[1]))
    );
    assert_eq!(first_uplink.unwrap().status(), StatusCode::NO_CONTENT);
    assert_eq!(second_uplink.unwrap().status(), StatusCode::NO_CONTENT);

    for poll in polls {
        poll.abort();
        let _ = poll.await;
    }
}

#[tokio::test]
async fn client_reports_accept_only_fixed_codes_once_per_bootstrap() {
    let state = state("127.0.0.1:2398".parse().unwrap(), "https");
    let token = state
        .registry
        .create_bootstrap_token("test".into(), state.config.backend);
    let app = qdynn_server::app(state.clone());
    let report_request = |reason: &str| {
        Request::builder()
            .method("POST")
            .uri("/api/v1/report")
            .header("authorization", format!("Bearer {token}"))
            .header("origin", "https://test.example")
            .header("content-type", "application/json")
            .body(Body::from(format!(
                "{{\"phase\":\"startup\",\"reason\":\"{reason}\"}}"
            )))
            .unwrap()
    };
    for _ in 0..2 {
        let response = app
            .clone()
            .oneshot(report_request("timeout"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    let invalid = app
        .clone()
        .oneshot(report_request("arbitrary client text"))
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let oversized = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/report")
                .header("authorization", format!("Bearer {token}"))
                .header("origin", "https://test.example")
                .header("content-type", "application/json")
                .body(Body::from(vec![b' '; 257]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let response = qdynn_server::admin_app(state)
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let metrics = String::from_utf8_lossy(&body);
    assert!(metrics.contains("qdynn_client_reports_total{reason=\"timeout\",phase=\"startup\"} 1"));
    assert!(!metrics.contains("arbitrary client text"));
}

#[tokio::test]
async fn forwarded_client_ip_is_used_only_for_configured_trusted_proxies() {
    let config = Config {
        hostname: "test.example".into(),
        secret: "000102030405060708090a0b0c0d0e0f".into(),
        trusted_proxy_cidrs: vec!["192.0.2.0/24".into()],
        ..Config::default()
    };
    let state = Arc::new(AppState::new(
        config,
        parse_secret("000102030405060708090a0b0c0d0e0f").unwrap(),
        Arc::new(SessionRegistry::new()),
    ));
    let app = qdynn_server::app(state.clone());
    let send = |peer: &str, forwarded: &str| {
        let mut request = Request::builder()
            .uri("/")
            .header("x-forwarded-for", forwarded)
            .body(Body::empty())
            .unwrap();
        request.extensions_mut().insert(axum::extract::ConnectInfo(
            peer.parse::<std::net::SocketAddr>().unwrap(),
        ));
        request
    };
    for request in [
        send("198.51.100.7:1234", "203.0.113.99"),
        send("192.0.2.7:1234", "203.0.113.99, 192.0.2.8"),
        send("192.0.2.7:1234", "attacker-supplied-invalid"),
    ] {
        app.clone().oneshot(request).await.unwrap();
    }
    let response = qdynn_server::admin_app(state)
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let metrics = String::from_utf8_lossy(&body);
    assert!(metrics.contains("qdynn_client_ip_source_total{source=\"direct_peer\"} 1"));
    assert!(metrics.contains("qdynn_client_ip_source_total{source=\"trusted_forwarded\"} 1"));
    assert!(metrics.contains("qdynn_client_ip_source_total{source=\"trusted_proxy_invalid\"} 1"));
    assert!(!metrics.contains("203.0.113.99"));
}

#[tokio::test]
async fn recovery_reserves_fresh_bootstrap_before_retiring_old_session() {
    let state = state("127.0.0.1:2398".parse().unwrap(), "https");
    let old_bootstrap = state
        .registry
        .create_bootstrap_token("default".into(), state.config.backend);
    let old_session = state
        .registry
        .exchange_bootstrap_token(&old_bootstrap)
        .unwrap();
    let capability = derive_bridge_capability(&state.config.hostname, &state.secret_bytes);
    let response = qdynn_server::app(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/recover")
                .header("host", "test.example")
                .header("origin", "https://test.example")
                .header("x-bridge-capability", capability)
                .header("authorization", format!("Bearer {}", old_session.id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let response_body = to_bytes(response.into_body(), 1024).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&response_body).unwrap();
    let new_bootstrap = payload["bootstrap"].as_str().unwrap();
    assert_ne!(new_bootstrap, old_bootstrap);
    assert!(state.registry.has_bootstrap(new_bootstrap));
    assert!(state.registry.get_session(&old_session.id).is_none());
    assert_eq!(state.registry.counts().0, 0);

    let metrics = qdynn_server::admin_app(state)
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let metrics = String::from_utf8(
        to_bytes(metrics.into_body(), 32 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(metrics.contains("qdynn_recoveries_total{outcome=\"success\"} 1"));
    assert!(metrics.contains("qdynn_recovery_duration_seconds_count 1"));
    assert!(!metrics.contains(&old_session.id));
    assert!(!metrics.contains(new_bootstrap));
}

#[tokio::test]
async fn recovery_rejects_bad_capability_and_wrong_host_without_allocating() {
    let state = state("127.0.0.1:2398".parse().unwrap(), "https");
    let capability = derive_bridge_capability(&state.config.hostname, &state.secret_bytes);
    let app = qdynn_server::app(state.clone());
    let forged = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/recover")
                .header("host", "test.example")
                .header("origin", "https://test.example")
                .header("x-bridge-capability", "invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forged.status(), StatusCode::METHOD_NOT_ALLOWED); // public POST policy

    let wrong_host = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/recover")
                .header("host", "other.example")
                .header("origin", "https://test.example")
                .header("x-bridge-capability", capability)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_host.status(), StatusCode::NOT_FOUND);
    assert_eq!(state.registry.counts().0, 0);
}

#[tokio::test]
async fn auto_carrier_is_server_allowlisted_and_bound_to_bootstrap() {
    let state = state("127.0.0.1:2398".parse().unwrap(), "auto");
    let app = qdynn_server::app(state.clone());
    let hello = encode_batch(&[Frame::hello()]);
    let create = |token: String, mode: &'static str| {
        Request::builder()
            .method("POST")
            .uri("/api/v1/session")
            .header("host", "test.example")
            .header("origin", "https://test.example")
            .header("authorization", format!("Bearer {token}"))
            .header("x-carrier-mode", mode)
            .body(Body::from(hello.clone()))
            .unwrap()
    };

    let websocket_bootstrap = state
        .registry
        .create_bootstrap_token("default".into(), state.config.backend);
    let websocket = app
        .clone()
        .oneshot(create(websocket_bootstrap.clone(), "websocket"))
        .await
        .unwrap();
    assert_eq!(websocket.status(), StatusCode::OK);
    assert_eq!(websocket.headers()["x-carrier-mode"], "websocket");
    let websocket_session = websocket.headers()["x-session-token"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        state
            .registry
            .get_session(&websocket_session)
            .unwrap()
            .carrier_mode,
        "websocket"
    );

    let changed_mode = app
        .clone()
        .oneshot(create(websocket_bootstrap, "https"))
        .await
        .unwrap();
    assert_eq!(
        changed_mode.status(),
        StatusCode::NOT_FOUND,
        "an idempotent bootstrap must not switch its committed carrier"
    );

    let https_bootstrap = state
        .registry
        .create_bootstrap_token("default".into(), state.config.backend);
    let https = app
        .clone()
        .oneshot(create(https_bootstrap, "https"))
        .await
        .unwrap();
    assert_eq!(https.status(), StatusCode::OK);
    assert_eq!(https.headers()["x-carrier-mode"], "https");
    let https_session = https.headers()["x-session-token"]
        .to_str()
        .unwrap()
        .to_owned();
    let up = |token: &str| {
        Request::builder()
            .method("POST")
            .uri("/api/v1/up")
            .header("host", "test.example")
            .header("origin", "https://test.example")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/octet-stream")
            .header("x-up-seq", "1")
            .body(Body::from(encode_batch(&[Frame::pong(Bytes::new())])))
            .unwrap()
    };
    assert_eq!(
        app.clone()
            .oneshot(up(&https_session))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT,
        "HTTPS requests must route by the selected session carrier"
    );
    assert_eq!(
        app.clone()
            .oneshot(up(&websocket_session))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND,
        "HTTP carrier routes must reject sessions pinned to WebSocket"
    );

    let forbidden_bootstrap = state
        .registry
        .create_bootstrap_token("default".into(), state.config.backend);
    let forbidden = app
        .clone()
        .oneshot(create(forbidden_bootstrap, "unknown"))
        .await
        .unwrap();
    assert_eq!(forbidden.status(), StatusCode::NOT_FOUND);

    let lane_bootstrap = state
        .registry
        .create_bootstrap_token("default".into(), state.config.backend);
    let lane_carrier = app
        .oneshot(create(lane_bootstrap, "websocket-lanes"))
        .await
        .unwrap();
    assert_eq!(
        lane_carrier.status(),
        StatusCode::NOT_FOUND,
        "auto mode must not accept carriers absent from its client attempt list"
    );
    assert_eq!(state.registry.counts().0, 2);
}

#[test]
fn recovery_can_replace_a_session_at_bootstrap_and_session_caps() {
    let registry =
        SessionRegistry::with_profile_limits(1, 1, 1, 1, qdynn_core::queue::QueueConfig::default());
    let backend = "127.0.0.1:2398".parse().unwrap();
    let old_bootstrap = registry.create_bootstrap_token("default".into(), backend);
    let old_session = registry.exchange_bootstrap_token(&old_bootstrap).unwrap();
    let new_bootstrap = registry
        .create_recovery_bootstrap(Some(&old_session.id), "default", backend)
        .unwrap();
    assert_ne!(new_bootstrap, old_bootstrap);
    assert!(registry.has_bootstrap(&new_bootstrap));
    assert!(!registry.has_bootstrap(&old_bootstrap));
    assert!(registry.get_session(&old_session.id).is_none());
}

fn auth(sess: &Session) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        "authorization",
        format!("Bearer {}", sess.id).parse().unwrap(),
    );
    h
}
async fn wait_empty(sess: &Session) {
    timeout(Duration::from_secs(2), async {
        while !sess.streams.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn backend_eof_and_local_close_release_500_connections() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sess = Session::new("test".into(), "test".into(), backend.local_addr().unwrap());
    for id in 1..=500 {
        sess.handle_frame(Frame::open(id)).await.unwrap();
        let (mut socket, _) = timeout(Duration::from_secs(2), backend.accept())
            .await
            .unwrap()
            .unwrap();
        if id % 2 == 0 {
            sess.close_stream(id);
        } else {
            socket.shutdown().await.unwrap();
        }
        let mut byte = [0];
        assert_eq!(
            timeout(Duration::from_secs(1), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0,
            "socket {id} leaked"
        );
        let frame = timeout(Duration::from_secs(1), sess.downlink_rx.lock().await.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame, Frame::close(id));
        wait_empty(&sess).await;
    }
    assert_eq!(sess.tombstones.len(), 500);
}

#[tokio::test]
async fn dropping_session_cancels_idle_tcp() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sess = Session::new("test".into(), "test".into(), backend.local_addr().unwrap());
    sess.handle_frame(Frame::open(1)).await.unwrap();
    let (mut socket, _) = backend.accept().await.unwrap();
    drop(sess);
    assert_eq!(
        timeout(Duration::from_secs(1), socket.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn invalid_later_frame_has_no_open_side_effect() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(backend.local_addr().unwrap(), "https");
    let sess = session(&state);
    let mut headers = auth(&sess);
    headers.insert("x-up-seq", "1".parse().unwrap());
    let body = encode_batch(&[
        Frame::open(1),
        Frame::data(2, Bytes::from_static(b"invalid")),
    ]);
    assert_eq!(
        handlers::handle_up(State(state), headers, body)
            .await
            .unwrap_err(),
        StatusCode::BAD_REQUEST
    );
    assert!(sess.streams.is_empty());
    assert!(timeout(Duration::from_millis(100), backend.accept())
        .await
        .is_err());
}

#[tokio::test]
async fn changed_retry_and_sequence_gap_are_rejected() {
    let state = state("127.0.0.1:1".parse().unwrap(), "https");
    let sess = session(&state);
    let mut headers = auth(&sess);
    headers.insert("x-up-seq", "1".parse().unwrap());
    let body = encode_batch(&[Frame::pong(Bytes::from_static(b"a"))]);
    handlers::handle_up(State(state.clone()), headers.clone(), body)
        .await
        .unwrap();
    let changed = encode_batch(&[Frame::pong(Bytes::from_static(b"b"))]);
    assert_eq!(
        handlers::handle_up(State(state.clone()), headers.clone(), changed.clone())
            .await
            .unwrap_err(),
        StatusCode::CONFLICT
    );
    headers.insert("x-up-seq", "3".parse().unwrap());
    assert_eq!(
        handlers::handle_up(State(state), headers, changed)
            .await
            .unwrap_err(),
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn newer_poll_supersedes_old_without_losing_replay() {
    let state = state("127.0.0.1:1".parse().unwrap(), "https");
    let sess = session(&state);
    let mut h = auth(&sess);
    h.insert("x-down-cursor", "0".parse().unwrap());
    let first_state = state.clone();
    let first_h = h.clone();
    let first = tokio::spawn(async move {
        handlers::handle_down(State(first_state), first_h)
            .await
            .unwrap()
    });
    let lane = sess.lane(0).unwrap();
    timeout(Duration::from_secs(1), async {
        while lane.down.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let second_state = state.clone();
    let second_h = h.clone();
    let second = tokio::spawn(async move {
        handlers::handle_down(State(second_state), second_h)
            .await
            .unwrap()
    });
    assert_eq!(
        timeout(Duration::from_secs(1), first)
            .await
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    let expected = encode_batch(&[Frame::data(1, Bytes::from_static(b"kept"))]);
    sess.downlink_tx
        .send(Frame::data(1, Bytes::from_static(b"kept")))
        .await
        .unwrap();
    let response = timeout(Duration::from_secs(1), second)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.headers()["x-down-cursor"], "1");
    assert_eq!(
        to_bytes(response.into_body(), 4096).await.unwrap(),
        expected
    );
    let replay = handlers::handle_down(State(state), h).await.unwrap();
    assert_eq!(to_bytes(replay.into_body(), 4096).await.unwrap(), expected);
}

#[tokio::test]
async fn https_lanes_have_independent_sequence_and_replay() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(backend.local_addr().unwrap(), "https-lanes");
    let sess = session(&state);
    let mut sockets = Vec::new();
    for id in 1..=2 {
        let mut h = auth(&sess);
        h.insert("x-lane-id", id.to_string().parse().unwrap());
        h.insert("x-up-seq", "1".parse().unwrap());
        let body = encode_batch(&[Frame::open(id)]);
        handlers::handle_up(State(state.clone()), h.clone(), body.clone())
            .await
            .unwrap();
        handlers::handle_up(State(state.clone()), h, body)
            .await
            .unwrap();
        let (mut socket, _) = backend.accept().await.unwrap();
        socket.write_all(&[id as u8]).await.unwrap();
        sockets.push(socket);
    }
    for id in 1..=2 {
        let mut h = auth(&sess);
        h.insert("x-lane-id", id.to_string().parse().unwrap());
        h.insert("x-down-cursor", "0".parse().unwrap());
        let response = handlers::handle_down(State(state.clone()), h.clone())
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(
            body,
            encode_batch(&[Frame::data(id, Bytes::from(vec![id as u8]))])
        );
        let replay = handlers::handle_down(State(state.clone()), h)
            .await
            .unwrap();
        assert_eq!(to_bytes(replay.into_body(), 4096).await.unwrap(), body);
    }
    sess.close_all();
}

#[tokio::test]
async fn websocket_disconnect_and_delete_cancel_backend_and_peer() {
    for delete in [false, true] {
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let state = state(backend.local_addr().unwrap(), "websocket");
        let sess = session(&state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = qdynn_server::app(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut req = format!("ws://{addr}/api/v1/ws")
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            "sec-websocket-protocol",
            format!("tproxy-v1.{}", sess.id).parse().unwrap(),
        );
        let (mut ws, _) = connect_async(req.clone()).await.unwrap();
        assert!(
            connect_async(req).await.is_err(),
            "second writer attached to same session"
        );
        ws.send(Message::Binary(encode_batch(&[Frame::open(1)])))
            .await
            .unwrap();
        let (mut socket, _) = timeout(Duration::from_secs(1), backend.accept())
            .await
            .unwrap()
            .unwrap();
        if delete {
            handlers::handle_delete_session(State(state), auth(&sess)).await;
            timeout(Duration::from_secs(1), ws.next()).await.unwrap();
        } else {
            ws.close(None).await.unwrap();
        }
        assert_eq!(
            timeout(Duration::from_secs(1), socket.read(&mut [0]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        timeout(Duration::from_secs(1), async {
            while !sess.is_expired() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        server.abort();
    }
}

#[tokio::test]
async fn origin_host_and_unauthorized_routes_use_public_policy() {
    let state = state("127.0.0.1:1".parse().unwrap(), "websocket");
    let cap = derive_bridge_capability(&state.config.hostname, &state.secret_bytes);
    for host in ["evil.example", "test.example:80", "test.example:443:evil"] {
        let response = qdynn_server::app(state.clone())
            .oneshot(
                Request::builder()
                    .uri(format!("/?bridge={cap}"))
                    .header("host", host)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), 100000).await.unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("bootstrap="));
    }
    for path in ["/api/v1/session", "/api/v1/ws", "/api/v1/up", "/unknown"] {
        let response = qdynn_server::app(state.clone())
            .oneshot(
                Request::builder()
                    .uri(path)
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers()["allow"], "GET,HEAD");
    }
    let sess = session(&state);
    let response = qdynn_server::app(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/ws")
                .header("authorization", format!("Bearer {}", sess.id))
                .header("origin", "https://evil.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn admin_metrics_are_separate_and_readiness_checks_backend() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(backend.local_addr().unwrap(), "websocket");
    let active_session = session(&state);
    let response = qdynn_server::admin_app(state.clone())
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    drop(backend);
    let response = qdynn_server::admin_app(state.clone())
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let response = qdynn_server::app(state.clone())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/ws")
                .header("authorization", format!("Bearer {}", active_session.id))
                .header("origin", "https://test.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = qdynn_server::admin_app(state)
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let metrics_body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let metrics = String::from_utf8_lossy(&metrics_body);
    assert!(metrics.contains("qdynn_sessions 1"));
    assert!(
        metrics.contains("qdynn_carrier_requests_total{endpoint=\"ws\",status_class=\"4xx\"} 1")
    );
    assert!(metrics
        .contains("qdynn_carrier_request_duration_seconds_bucket{endpoint=\"ws\",le=\"+Inf\"} 1"));
    assert!(metrics.contains("qdynn_backend_dial_duration_seconds_count 0"));
    assert!(metrics.contains("qdynn_first_backend_byte_duration_seconds_count 0"));
}

#[test]
fn configuration_rejects_unsafe_or_ambiguous_values() {
    let auto = Config::load_from([
        "qdynn",
        "--secret",
        "000102030405060708090a0b0c0d0e0f",
        "--carrier-mode",
        "auto",
    ])
    .unwrap();
    assert_eq!(auto.carrier_mode, "auto");

    for (flag, value) in [
        ("--secret", "dd"),
        ("--carrier-mode", "typo"),
        ("--backend", "1.2.3.4:1234"),
        ("--hostname", "example.com/path"),
        ("--admin-listen", "0.0.0.0:9090"),
        ("--startup-timeout-ms", "999"),
        ("--websocket-open-timeout-ms", "16000"),
        ("--request-timeout-ms", "301000"),
        ("--max-sessions", "0"),
        ("--max-sessions-per-profile", "0"),
        ("--max-api-requests", "100001"),
        ("--max-api-requests", "1"),
        ("--session-queue-data-bytes", "1024"),
        ("--global-queue-data-bytes", "4294967296"),
        ("--max-streams-per-session", "0"),
        ("--lane-data-items", "129"),
        ("--trusted-proxy-cidrs", "not-a-cidr"),
        ("--config-file", "/no/such/config.toml"),
    ] {
        assert!(
            Config::load_from([
                "qdynn",
                "--secret",
                "000102030405060708090a0b0c0d0e0f",
                flag,
                value
            ])
            .is_err(),
            "accepted {flag} {value}"
        );
    }
    assert!(
        Config::load_from(["qdynn"]).is_err(),
        "demo secret was accepted implicitly"
    );
}

#[tokio::test]
async fn no_hello_consumption_on_bad_create() {
    let state = state("127.0.0.1:1".parse().unwrap(), "websocket");
    let token = state
        .registry
        .create_bootstrap_token("test".into(), state.config.backend);
    let mut h = HeaderMap::new();
    h.insert("authorization", format!("Bearer {token}").parse().unwrap());
    for body in [
        Bytes::new(),
        encode_batch(&[Frame::hello(), Frame::open(1)]),
        encode_batch(&[Frame::data(1, Bytes::from_static(b"x"))]),
    ] {
        assert!(
            handlers::handle_create_session(State(state.clone()), h.clone(), body)
                .await
                .is_err()
        );
    }
    assert!(
        handlers::handle_create_session(State(state), h, encode_batch(&[Frame::hello()]))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn closing_one_websocket_lane_preserves_other_lanes() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(backend.local_addr().unwrap(), "websocket-lanes");
    let sess = session(&state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = qdynn_server::app(state);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut lanes = Vec::new();
    let mut sockets = Vec::new();
    for id in 1..=2 {
        let mut req = format!("ws://{addr}/api/v1/ws")
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            "sec-websocket-protocol",
            format!("tproxy-lane-v1.{}.{id}", sess.id).parse().unwrap(),
        );
        let (mut ws, _) = connect_async(req).await.unwrap();
        ws.send(Message::Binary(encode_batch(&[Frame::open(id)])))
            .await
            .unwrap();
        let (socket, _) = backend.accept().await.unwrap();
        lanes.push(ws);
        sockets.push(socket);
    }
    lanes[0].close(None).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), sockets[0].read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    // Let the first stream's asynchronous cleanup complete before using lane 2.
    timeout(Duration::from_secs(1), async {
        while sess.streams.contains_key(&1) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!sess.is_expired());
    sockets[1].write_all(b"still alive").await.unwrap();
    let packet = timeout(Duration::from_secs(1), lanes[1].next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        packet,
        Message::Binary(encode_batch(&[Frame::data(
            2,
            Bytes::from_static(b"still alive")
        )]))
    );
    server.abort();
    sess.close_all();
}

#[tokio::test]
async fn closed_https_lane_acknowledges_late_frames_and_retires() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(backend.local_addr().unwrap(), "https-lanes");
    let sess = session(&state);
    let mut up = auth(&sess);
    up.insert("x-lane-id", "1".parse().unwrap());
    up.insert("x-up-seq", "1".parse().unwrap());
    handlers::handle_up(
        State(state.clone()),
        up.clone(),
        encode_batch(&[Frame::open(1)]),
    )
    .await
    .unwrap();
    let (socket, _) = backend.accept().await.unwrap();
    drop(socket);
    wait_empty(&sess).await;
    let mut down = auth(&sess);
    down.insert("x-lane-id", "1".parse().unwrap());
    down.insert("x-down-cursor", "0".parse().unwrap());
    let response = handlers::handle_down(State(state.clone()), down.clone())
        .await
        .unwrap();
    assert_eq!(
        to_bytes(response.into_body(), 4096).await.unwrap(),
        encode_batch(&[Frame::close(1)])
    );
    down.insert("x-down-cursor", "1".parse().unwrap());
    for _ in 0..2 {
        let response = timeout(
            Duration::from_secs(1),
            handlers::handle_down(State(state.clone()), down.clone()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers()["x-lane-closed"], "1");
    }
    assert!(!sess.has_lane(1));
    up.insert("x-up-seq", "2".parse().unwrap());
    let body = encode_batch(&[Frame::data(1, Bytes::from_static(b"late")), Frame::close(1)]);
    for _ in 0..2 {
        let response = handlers::handle_up(State(state.clone()), up.clone(), body.clone())
            .await
            .unwrap();
        assert_eq!(response.headers()["x-up-ack"], "2");
    }
    assert!(!sess.has_lane(1));
}

#[tokio::test]
async fn custom_public_directory_and_file_config_are_used() {
    let temp = std::env::temp_dir().join(format!(
        "qdynn-config-{}",
        qdynn_core::generate_secure_token()
    ));
    std::fs::create_dir(&temp).unwrap();
    std::fs::write(temp.join("index.html"), "custom public index").unwrap();
    std::fs::write(temp.join("asset.txt"), "custom asset").unwrap();
    let config_file = temp.join("config.toml");
    std::fs::write(
        &config_file,
        format!(
            "hostname = 'test.example'\nsecret = '000102030405060708090a0b0c0d0e0f'\ncarrier_mode = 'https'\npublic_dir = '{}'\nstartup_timeout_ms = 30000\nwebsocket_open_timeout_ms = 5000\nrequest_timeout_ms = 120000\nmax_sessions = 64\nmax_sessions_per_profile = 32\nmax_bootstraps_per_profile = 256\nmax_api_requests = 96\nsession_queue_data_bytes = 8388608\nglobal_queue_data_bytes = 67108864\ninput_body_bytes = 33554432\nmax_backend_connections = 64\nmax_streams_per_session = 24\nlane_queue_items = 64\nlane_data_items = 48\nstream_queue_items = 1024\ntrusted_proxy_cidrs = ['192.0.2.0/24']\n",
            temp.display()
        ),
    )
    .unwrap();
    let config = Config::load_from([
        "qdynn",
        "--config-file",
        config_file.to_str().unwrap(),
        "--carrier-mode",
        "websocket",
        "--startup-timeout-ms",
        "45000",
    ])
    .unwrap();
    assert_eq!(config.carrier_mode, "websocket");
    assert_eq!(config.startup_timeout_ms, 45_000);
    assert_eq!(config.websocket_open_timeout_ms, 5_000);
    assert_eq!(config.request_timeout_ms, 120_000);
    assert_eq!(config.max_sessions, 64);
    assert_eq!(config.max_api_requests, 96);
    assert_eq!(config.max_sessions_per_profile, 32);
    assert_eq!(config.max_bootstraps_per_profile, 256);
    assert_eq!(config.session_queue_data_bytes, 8 * 1024 * 1024);
    assert_eq!(config.global_queue_data_bytes, 64 * 1024 * 1024);
    assert_eq!(config.input_body_bytes, 32 * 1024 * 1024);
    assert_eq!(config.max_backend_connections, 64);
    assert_eq!(config.max_streams_per_session, 24);
    assert_eq!(config.lane_queue_items, 64);
    assert_eq!(config.lane_data_items, 48);
    assert_eq!(config.stream_queue_items, 1024);
    let queue_config = config.queue_config();
    assert_eq!(queue_config.session_data_bytes, 8 * 1024 * 1024);
    assert_eq!(queue_config.global_data_bytes, 64 * 1024 * 1024);
    assert_eq!(queue_config.backend_connections, 64);
    assert_eq!(config.trusted_proxy_cidrs, ["192.0.2.0/24"]);
    assert_eq!(config.hostname, "test.example");
    let mut state = state("127.0.0.1:1".parse().unwrap(), "websocket");
    Arc::get_mut(&mut state).unwrap().config = config;
    for (path, expected) in [
        ("/", "custom public index"),
        ("/asset.txt", "custom asset"),
        ("/?bridge=bad&unexpected=1", "custom public index"),
    ] {
        let response = qdynn_server::app(state.clone())
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(response.into_body(), 4096).await.unwrap(),
            expected
        );
    }
    std::fs::remove_dir_all(temp).unwrap();
}

#[test]
fn registry_capacity_does_not_consume_bootstrap_and_shutdown_stops_admission() {
    let registry = SessionRegistry::new();
    let backend = "127.0.0.1:1".parse().unwrap();
    let mut first = None;
    for _ in 0..128 {
        let token = registry.create_bootstrap_token("test".into(), backend);
        let sess = registry.exchange_bootstrap_token(&token).unwrap();
        first.get_or_insert(sess);
    }
    let pending = registry.create_bootstrap_token("test".into(), backend);
    assert!(registry.exchange_bootstrap_token(&pending).is_err());
    registry.remove_session(&first.unwrap().id);
    assert!(registry.exchange_bootstrap_token(&pending).is_ok());
    registry.close_all();
    assert!(registry
        .create_bootstrap_token("test".into(), backend)
        .is_empty());
    assert!(registry.exchange_bootstrap_token(&pending).is_err());
    assert_eq!(registry.counts(), (0, 0));
}
