use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use qdynn_core::SessionRegistry;
use qdynn_server::{
    config::{Command, Config},
    handlers::AppState,
};
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn public_methods_and_unauthorized_api_are_indistinguishable() {
    let temp = std::env::temp_dir().join(format!("qdynn-public-{}", rand::random::<u64>()));
    std::fs::create_dir_all(&temp).unwrap();
    std::fs::write(temp.join("index.html"), "own site").unwrap();
    std::fs::write(
        temp.join("robots.txt"),
        "User-agent: *\nDisallow: /private\n",
    )
    .unwrap();
    for custom in [false, true] {
        let state = Arc::new(AppState::new(
            Config {
                public_dir: custom.then(|| temp.clone()),
                ..Config::default()
            },
            vec![7; 16],
            Arc::new(SessionRegistry::new()),
        ));
        // Saturated long polls must not make only unauthenticated /down stand out.
        let _down_permits = state
            .down_poll_admission
            .clone()
            .acquire_many_owned(state.down_poll_admission.available_permits() as u32)
            .await
            .unwrap();
        let app = qdynn_server::app(state.clone());
        for method in ["GET", "HEAD", "POST", "PUT", "OPTIONS", "DELETE"] {
            let mut baseline = None;
            for path in [
                "/missing",
                "/api/v1/session",
                "/api/v1/up",
                "/api/v1/down",
                "/api/v1/ws",
                "/api/v1/report",
                "/api/v1/recover",
            ] {
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method(method)
                            .uri(path)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let status = response.status();
                let headers = response.headers().clone();
                assert!(!headers.contains_key("x-session-token"));
                assert!(!headers.contains_key("x-carrier-mode"));
                let body = to_bytes(response.into_body(), 100000).await.unwrap();
                if method == "HEAD" {
                    assert!(body.is_empty());
                }
                let actual = (status, headers, body);
                if let Some(expected) = &baseline {
                    assert_eq!(&actual, expected, "{method} {path}");
                } else {
                    baseline = Some(actual);
                }
            }
        }
        let get = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let headers = get.headers().clone();
        let body = to_bytes(get.into_body(), 100000).await.unwrap();
        let head = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head.headers()["content-length"], body.len().to_string());
        assert!(to_bytes(head.into_body(), 100000).await.unwrap().is_empty());
        if custom {
            let robots = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/robots.txt")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                to_bytes(robots.into_body(), 1000).await.unwrap(),
                "User-agent: *\nDisallow: /private\n"
            );
            let conditional = app
                .oneshot(
                    Request::builder()
                        .uri("/")
                        .header("if-modified-since", &headers["last-modified"])
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(conditional.status(), StatusCode::NOT_MODIFIED);
        }
        assert_eq!(state.registry.counts(), (0, 0));
    }
    std::fs::remove_dir_all(temp).unwrap();
}

#[test]
fn diagnostic_commands_load_config_without_becoming_persistent_settings() {
    let temp = std::env::temp_dir().join(format!("qdynn-doctor-{}.toml", rand::random::<u64>()));
    std::fs::write(
        &temp,
        "hostname='test.example'\nsecret='000102030405060708090a0b0c0d0e0f'\n",
    )
    .unwrap();
    for (name, command) in [
        ("doctor", Command::Doctor),
        ("check-config", Command::CheckConfig),
    ] {
        let config =
            Config::load_from(["qdynn", name, "--config-file", temp.to_str().unwrap()]).unwrap();
        assert_eq!(config.command, Some(command));
        assert_eq!(config.hostname, "test.example");
        assert!(!toml::to_string(&config).unwrap().contains("command"));
    }
    assert!(Config::load_from([
        "qdynn",
        "doctor",
        "--print-link",
        "--config-file",
        temp.to_str().unwrap()
    ])
    .is_err());
    let secret = "000102030405060708090a0b0c0d0e0f";
    std::fs::write(&temp, format!("secret = '{secret}\n")).unwrap();
    let error = Config::load_from(["qdynn", "doctor", "--config-file", temp.to_str().unwrap()])
        .unwrap_err();
    assert!(!format!("{error:?}").contains(secret));
    assert!(error.to_string().contains("near line"));
    std::fs::remove_file(temp).unwrap();
}
