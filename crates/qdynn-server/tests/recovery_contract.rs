use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use qdynn_core::SessionRegistry;
use qdynn_proto::{derive_bridge_capability, encode_batch, Frame};
use qdynn_server::{config::Config, handlers::AppState};
use std::sync::Arc;
use tower::ServiceExt;

fn state() -> Arc<AppState> {
    Arc::new(AppState::new(
        Config {
            hostname: "recovery.example".into(),
            carrier_mode: "https".into(),
            ..Config::default()
        },
        vec![7; 16],
        Arc::new(SessionRegistry::with_limits(1, 1)),
    ))
}

fn recover_request(state: &AppState, bearer: Option<&str>, capability: &str) -> Request<Body> {
    let mut request = Request::builder()
        .method("POST")
        .uri("/api/v1/recover")
        .header("host", &state.config.hostname)
        .header("origin", format!("https://{}", state.config.hostname))
        .header("x-bridge-capability", capability);
    if let Some(bearer) = bearer {
        request = request.header("authorization", format!("Bearer {bearer}"));
    }
    request.body(Body::empty()).unwrap()
}

#[tokio::test]
async fn bridge_recovery_hello_is_accepted_by_real_router() {
    let state = state();
    let app = qdynn_server::app(state.clone());
    let capability = derive_bridge_capability(&state.config.hostname, &state.secret_bytes);
    let response = app
        .clone()
        .oneshot(recover_request(&state, None, &capability))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let token = json["bootstrap"].as_str().unwrap();
    // Use the bytes the shipped bridge actually constructs, not a separate
    // test fixture that can silently diverge from JavaScript.
    let source = include_str!("../src/bridge.js");
    let literal = source
        .split("const hello=new Uint8Array([")
        .nth(1)
        .unwrap()
        .split(']')
        .next()
        .unwrap();
    let hello: Vec<u8> = literal
        .split(',')
        .map(|byte| byte.parse().unwrap())
        .collect();
    assert_eq!(hello, encode_batch(&[Frame::hello()]));
    let mut session_token = None;
    for _ in 0..2 {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/session")
                    .header("origin", "https://recovery.example")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(hello.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let new_token = response.headers()["x-session-token"]
            .to_str()
            .unwrap()
            .to_owned();
        if let Some(previous) = &session_token {
            assert_eq!(previous, &new_token);
        }
        session_token = Some(new_token);
        assert_eq!(
            to_bytes(response.into_body(), 8).await.unwrap(),
            encode_batch(&[Frame::welcome()])
        );
    }
    assert_eq!(state.registry.counts().0, 1);
}

#[tokio::test]
async fn stale_bearer_after_registry_restart_recovers_only_with_valid_capability() {
    let old_state = state();
    let bootstrap = old_state
        .registry
        .create_bootstrap_token("default".into(), old_state.config.backend);
    let old_session = old_state
        .registry
        .exchange_bootstrap_token(&bootstrap)
        .unwrap();
    let state = state(); // Process restart: same configuration, empty registry.
    let app = qdynn_server::app(state.clone());
    let forged = app
        .clone()
        .oneshot(recover_request(&state, Some(&old_session.id), "invalid"))
        .await
        .unwrap();
    assert!(serde_json::from_slice::<serde_json::Value>(
        &to_bytes(forged.into_body(), 100000).await.unwrap()
    )
    .is_err());
    let capability = derive_bridge_capability(&state.config.hostname, &state.secret_bytes);
    let malformed = app
        .clone()
        .oneshot(recover_request(&state, Some("invalid"), &capability))
        .await
        .unwrap();
    assert!(serde_json::from_slice::<serde_json::Value>(
        &to_bytes(malformed.into_body(), 100000).await.unwrap()
    )
    .is_err());
    let response = app
        .clone()
        .oneshot(recover_request(&state, Some(&old_session.id), &capability))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let json: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1024).await.unwrap()).unwrap();
    let new_session = state
        .registry
        .exchange_bootstrap_token(json["bootstrap"].as_str().unwrap())
        .unwrap();
    assert_eq!(new_session.profile_name, "default");
    // Unknown old bearer must not bypass quotas or retire an unrelated session.
    let full = app
        .oneshot(recover_request(&state, Some(&old_session.id), &capability))
        .await
        .unwrap();
    assert_eq!(full.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(state.registry.get_session(&new_session.id).is_some());
}

#[test]
fn recovery_cannot_retire_another_profile() {
    let state = state();
    let bootstrap = state
        .registry
        .create_bootstrap_token("other".into(), state.config.backend);
    let session = state.registry.exchange_bootstrap_token(&bootstrap).unwrap();
    assert!(state
        .registry
        .create_recovery_bootstrap(Some(&session.id), "default", state.config.backend)
        .is_err());
    assert!(state.registry.get_session(&session.id).is_some());
}
