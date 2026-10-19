use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use qdynn_core::SessionRegistry;
use qdynn_proto::{decode_batch, derive_bridge_capability, parse_secret, Frame, FrameType};
use qdynn_server::{config::Config, handlers::AppState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

/// Starts a mock MTProxy backend TCP server for testing
async fn start_mock_mtproxy() -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                Ok((mut socket, _)) = listener.accept() => {
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 1024];
                        while let Ok(n) = socket.read(&mut buf).await {
                            if n == 0 { break; }
                            // Echo back with "echo:" prefix
                            let mut response = b"echo:".to_vec();
                            response.extend_from_slice(&buf[..n]);
                            if socket.write_all(&response).await.is_err() {
                                break;
                            }
                        }
                    });
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    (addr, shutdown_tx)
}

/// Spawns an instance of the Qdynn Web Proxy server
async fn spawn_test_server(backend_addr: SocketAddr) -> (SocketAddr, String, String) {
    let hostname = "proxy.test.example";
    let secret = "000102030405060708090a0b0c0d0e0f";
    let secret_bytes = parse_secret(secret).unwrap();
    let registry = Arc::new(SessionRegistry::new());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let config = Config {
        command: None,
        listen: local_addr,
        hostname: hostname.to_string(),
        secret: secret.to_string(),
        backend: backend_addr,
        carrier_mode: "websocket".to_string(),
        public_dir: None,
        config_file: None,
        print_link: false,
        admin_listen: None,
        startup_timeout_ms: 15_000,
        websocket_open_timeout_ms: 4_000,
        request_timeout_ms: 90_000,
        max_bootstraps: 1024,
        max_sessions: 128,
        max_bootstraps_per_profile: 1024,
        max_sessions_per_profile: 128,
        max_api_requests: 256,
        max_websockets: 256,
        session_queue_data_bytes: 16 * 1024 * 1024,
        session_queue_control_bytes: 128 * 1024,
        global_queue_data_bytes: 128 * 1024 * 1024,
        global_queue_control_bytes: 4 * 1024 * 1024,
        input_body_bytes: 64 * 1024 * 1024,
        max_backend_connections: 256,
        max_streams_per_session: 32,
        max_lanes_per_session: 129,
        lane_queue_items: 128,
        lane_data_items: 96,
        stream_queue_items: 4096,
        trusted_proxy_cidrs: Vec::new(),
    };

    let state = Arc::new(AppState::new(config, secret_bytes, registry));

    let app = qdynn_server::app(state);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (local_addr, hostname.to_string(), secret.to_string())
}

/// Helper to send raw HTTP request over TCP and get response string and headers
async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (String, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut req = format!(
        "{} {} HTTP/1.1\r\nHost: proxy.test.example\r\nConnection: close\r\n",
        method, path
    );
    for (k, v) in headers {
        req.push_str(&format!("{}: {}\r\n", k, v));
    }
    if !body.is_empty() {
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    req.push_str("\r\n");

    stream.write_all(req.as_bytes()).await.unwrap();
    if !body.is_empty() {
        stream.write_all(body).await.unwrap();
    }

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();

    let split_pos = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("Invalid HTTP response");

    let header_str = String::from_utf8_lossy(&resp[..split_pos]).to_string();
    let body_bytes = resp[split_pos + 4..].to_vec();

    (header_str, body_bytes)
}

#[tokio::test]
async fn test_full_web_proxy_lifecycle() {
    let (backend_addr, _backend_shutdown) = start_mock_mtproxy().await;
    let (server_addr, hostname, secret) = spawn_test_server(backend_addr).await;

    let secret_bytes = parse_secret(&secret).unwrap();
    let capability = derive_bridge_capability(&hostname, &secret_bytes);

    // 1. Test Camouflage: Root without bridge parameter returns 200 with camouflage HTML
    let (camo_head, camo_body) = http_request(server_addr, "GET", "/", &[], &[]).await;
    assert!(camo_head.contains("200 OK"));
    let camo_text = String::from_utf8_lossy(&camo_body);
    assert!(camo_text.contains("Next-Generation Cloud Gateway"));

    // Test arbitrary URL fallback (looks like full SPA website)
    let (about_head, about_body) = http_request(server_addr, "GET", "/about", &[], &[]).await;
    assert!(about_head.contains("200 OK"));
    let about_text = String::from_utf8_lossy(&about_body);
    assert!(about_text.contains("Next-Generation Cloud Gateway"));

    // 2. Test Bridge access with valid capability
    let bridge_path = format!("/?bridge={}", capability);
    let (bridge_head, bridge_body) = http_request(server_addr, "GET", &bridge_path, &[], &[]).await;
    assert!(bridge_head.contains("200 OK"));
    let bridge_html = String::from_utf8_lossy(&bridge_body);
    assert!(bridge_html.contains("tproxy-v1"));
    assert!(bridge_html.contains("history.replaceState"));
    assert!(bridge_html.contains("startupBudgetMs=15000"));
    assert!(bridge_html.contains("bridgeCapability=\""));
    assert!(bridge_html.contains("webSocketOpenTimeoutMs=4000"));
    assert!(bridge_html.contains("requestTimeoutMs=90000"));

    // Extract bootstrap token from bridge HTML
    let token_marker = "bootstrap=\"";
    let token_start = bridge_html.find(token_marker).unwrap() + token_marker.len();
    let token_end = bridge_html[token_start..].find('"').unwrap() + token_start;
    let bootstrap_token = &bridge_html[token_start..token_end];

    // 3. Create session via POST /api/v1/session
    let hello_frame = Frame::hello();
    let mut hello_buf = BytesMut::new();
    hello_frame.encode(&mut hello_buf);

    let auth_header = format!("Bearer {}", bootstrap_token);
    let (session_head, session_body) = http_request(
        server_addr,
        "POST",
        "/api/v1/session",
        &[
            ("Authorization", &auth_header),
            ("Content-Type", "application/octet-stream"),
        ],
        &hello_buf.freeze(),
    )
    .await;

    assert!(session_head.contains("200 OK"));

    // Extract X-Session-Token from header
    let session_token_marker = "x-session-token: ";
    let session_token_line = session_head
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with(session_token_marker))
        .expect("Missing x-session-token header");
    let session_token = session_token_line[session_token_marker.len()..].trim();

    let mut welcome_buf = BytesMut::from(&session_body[..]);
    let welcome_frame = Frame::decode(&mut welcome_buf).unwrap().unwrap();
    assert_eq!(welcome_frame.frame_type, FrameType::Welcome);

    // 4. Connect via WebSocket
    let ws_url = format!("ws://{}/api/v1/ws", server_addr);
    let mut request = ws_url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        format!("tproxy-v1.{}", session_token).parse().unwrap(),
    );

    let (ws_stream, _) = connect_async(request).await.unwrap();
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // 5. Send OPEN frame for stream_id = 1
    let open_frame = Frame::open(1);
    let mut open_buf = BytesMut::new();
    open_frame.encode(&mut open_buf);
    ws_tx
        .send(Message::Binary(open_buf.freeze()))
        .await
        .unwrap();

    // 6. Send DATA frame on stream_id = 1
    let data_payload = Bytes::from_static(b"hello mtproxy via web");
    let data_frame = Frame::data(1, data_payload.clone());
    let mut data_buf = BytesMut::new();
    data_frame.encode(&mut data_buf);
    ws_tx
        .send(Message::Binary(data_buf.freeze()))
        .await
        .unwrap();

    // 7. Receive Echo DATA from Mock MTProxy via WebSocket
    let mut received_echo = false;
    let timeout_deadline = tokio::time::Instant::now() + Duration::from_secs(3);

    while tokio::time::Instant::now() < timeout_deadline && !received_echo {
        if let Ok(Some(Ok(Message::Binary(data)))) =
            tokio::time::timeout(Duration::from_millis(500), ws_rx.next()).await
        {
            let mut resp_buf = BytesMut::from(&data[..]);
            if let Ok(frames) = decode_batch(&mut resp_buf) {
                for f in frames {
                    if f.frame_type == FrameType::Data
                        && f.stream_id == 1
                        && f.payload == Bytes::from_static(b"echo:hello mtproxy via web")
                    {
                        received_echo = true;
                        break;
                    }
                }
            }
        }
    }

    assert!(received_echo, "Expected echo DATA frame from backend");

    // 8. Send CLOSE frame for stream_id = 1
    let close_frame = Frame::close(1);
    let mut close_buf = BytesMut::new();
    close_frame.encode(&mut close_buf);
    ws_tx
        .send(Message::Binary(close_buf.freeze()))
        .await
        .unwrap();

    // 9. Close session via DELETE /api/v1/session
    let (del_head, _) = http_request(
        server_addr,
        "DELETE",
        "/api/v1/session",
        &[("Authorization", &format!("Bearer {}", session_token))],
        &[],
    )
    .await;
    assert!(del_head.contains("204 No Content"));
}
