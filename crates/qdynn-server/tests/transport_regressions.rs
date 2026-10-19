// Regression tests for the defects reproduced by the architecture audit.
// Only loopback listeners and synthetic credentials are used.
use axum::{
    body::to_bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::get,
    Router,
};
use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use qdynn_core::{Session, SessionRegistry};
use qdynn_proto::{
    decode_batch, encode_batch, parse_secret, Frame, FrameType, INITIAL_STREAM_WINDOW,
};
use qdynn_server::{
    config::Config,
    handlers::{self, AppState},
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};

fn state(backend: SocketAddr, mode: &str) -> Arc<AppState> {
    Arc::new(AppState::new(
        Config {
            command: None,
            listen: "127.0.0.1:0".parse().unwrap(),
            hostname: "audit.example".into(),
            secret: "000102030405060708090a0b0c0d0e0f".into(),
            backend,
            carrier_mode: mode.into(),
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
        },
        parse_secret("000102030405060708090a0b0c0d0e0f").unwrap(),
        Arc::new(SessionRegistry::new()),
    ))
}

fn auth(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
    headers.insert("content-type", "application/octet-stream".parse().unwrap());
    headers
}

async fn downlink_http_request(
    address: SocketAddr,
    token: &str,
    cursor: u64,
) -> (u16, u64, Vec<u8>) {
    let mut socket = TcpStream::connect(address).await.unwrap();
    let request = format!(
        "POST /api/v1/down HTTP/1.1\r\nHost: audit.example\r\nConnection: close\r\nauthorization: Bearer {token}\r\ncontent-type: application/octet-stream\r\nx-down-cursor: {cursor}\r\ncontent-length: 0\r\n\r\n"
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    socket.read_to_end(&mut response).await.unwrap();
    let separator = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP response headers missing");
    let header_text = String::from_utf8_lossy(&response[..separator]);
    let status = header_text
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let next_cursor = header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("x-down-cursor")
                .then(|| value.trim().parse().unwrap())
        })
        .unwrap_or(cursor);
    (status, next_cursor, response[separator + 4..].to_vec())
}

fn session(state: &AppState) -> Arc<Session> {
    let token = state
        .registry
        .create_bootstrap_token("audit".into(), state.config.backend);
    state
        .registry
        .exchange_bootstrap_token_for_carrier(&token, &state.config.carrier_mode)
        .unwrap()
}

#[tokio::test]
async fn bootstrap_retry_returns_same_session() {
    let state = state("127.0.0.1:1".parse().unwrap(), "websocket");
    let token = state
        .registry
        .create_bootstrap_token("audit".into(), state.config.backend);
    let body = encode_batch(&[Frame::hello()]);
    let first = handlers::handle_create_session(State(state.clone()), auth(&token), body.clone())
        .await
        .unwrap();
    let second = handlers::handle_create_session(State(state), auth(&token), body).await;
    assert!(
        second.is_ok(),
        "retry must succeed with the same token; got {:?}",
        second.as_ref().err()
    );
    assert_eq!(
        first.headers()["x-session-token"],
        second.unwrap().headers()["x-session-token"]
    );
}

#[tokio::test]
async fn empty_hello_is_rejected() {
    let state = state("127.0.0.1:1".parse().unwrap(), "websocket");
    let token = state
        .registry
        .create_bootstrap_token("audit".into(), state.config.backend);
    let response = handlers::handle_create_session(State(state), auth(&token), Bytes::new()).await;
    assert!(
        response.is_err(),
        "server returned WELCOME without any HELLO"
    );
}

#[tokio::test]
async fn uplink_retry_does_not_duplicate_backend_bytes() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(backend.local_addr().unwrap(), "https");
    let sess = session(&state);
    sess.handle_frame(Frame::open(1)).await.unwrap();
    let (mut connection, _) = backend.accept().await.unwrap();
    let mut headers = auth(&sess.id);
    headers.insert("x-up-seq", "1".parse().unwrap());
    let body = encode_batch(&[Frame::data(1, Bytes::from_static(b"X"))]);
    handlers::handle_up(State(state.clone()), headers.clone(), body.clone())
        .await
        .unwrap();
    let mut byte = [0u8; 1];
    timeout(Duration::from_secs(2), connection.read_exact(&mut byte))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(byte, *b"X");
    handlers::handle_up(State(state), headers, body)
        .await
        .unwrap();
    let duplicate = timeout(Duration::from_millis(300), connection.read_exact(&mut byte)).await;
    assert!(
        duplicate.is_err(),
        "same sequence was written to TCP twice: {:?}",
        byte
    );
}

#[tokio::test]
async fn downlink_old_cursor_replays_identical_batch() {
    let state = state("127.0.0.1:1".parse().unwrap(), "https");
    let sess = session(&state);
    let mut headers = auth(&sess.id);
    headers.insert("x-down-cursor", "0".parse().unwrap());
    sess.downlink_tx
        .send(Frame::data(1, Bytes::from_static(b"first")))
        .await
        .unwrap();
    let first = handlers::handle_down(State(state.clone()), headers.clone())
        .await
        .unwrap();
    let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
    sess.downlink_tx
        .send(Frame::data(1, Bytes::from_static(b"second")))
        .await
        .unwrap();
    let replay = handlers::handle_down(State(state), headers).await.unwrap();
    let replay_body = to_bytes(replay.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        first_body, replay_body,
        "retry consumed a different batch instead of replaying the first"
    );
}

#[tokio::test]
async fn idle_https_poll_returns_204() {
    let state = state("127.0.0.1:1".parse().unwrap(), "https");
    let sess = session(&state);
    let mut headers = auth(&sess.id);
    headers.insert("x-down-cursor", "0".parse().unwrap());
    let response = timeout(
        Duration::from_secs(28),
        handlers::handle_down(State(state), headers),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "empty 200 makes bridge poll() fail"
    );
}

#[tokio::test]
async fn backend_data_stops_at_initial_credit() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sess = Session::new(
        "synthetic".into(),
        "audit".into(),
        backend.local_addr().unwrap(),
    );
    sess.handle_frame(Frame::open(1)).await.unwrap();
    let (mut connection, _) = backend.accept().await.unwrap();
    let writer = tokio::spawn(async move {
        connection
            .write_all(&vec![7u8; INITIAL_STREAM_WINDOW as usize + 65536])
            .await
            .unwrap();
        // Keep backend open so EOF does not influence the flow-control check.
        let mut buf = [0u8; 1];
        let _ = connection.read(&mut buf).await;
    });
    let mut received = 0usize;
    let mut rx = sess.downlink_rx.lock().await;
    let exceeded = timeout(Duration::from_secs(2), async {
        while let Some(frame) = rx.recv().await {
            if frame.frame_type == FrameType::Data {
                received += frame.payload.len();
            }
            if received > INITIAL_STREAM_WINDOW as usize {
                return true;
            }
        }
        false
    })
    .await;
    assert!(
        !matches!(exceeded, Ok(true)),
        "received {received} bytes without WINDOW; credit is {}",
        INITIAL_STREAM_WINDOW
    );
    assert_eq!(
        received, INITIAL_STREAM_WINDOW as usize,
        "backend did not deliver the granted window"
    );
    drop(rx);
    sess.handle_frame(Frame::window(1, 65536)).await.unwrap();
    let mut rx = sess.downlink_rx.lock().await;
    let mut extra = 0;
    timeout(Duration::from_secs(2), async {
        while extra < 65536 {
            let frame = rx.recv().await.unwrap();
            assert_eq!(frame.frame_type, FrameType::Data);
            extra += frame.payload.len();
        }
    })
    .await
    .unwrap();
    assert_eq!(extra, 65536);
    writer.abort();
}

#[tokio::test]
async fn multiplexed_downlink_serves_short_backend_reply_during_bulk_load() {
    const BULK_STREAMS: u32 = 8;
    const FRAMES_PER_STREAM: usize = 8;
    const FRAME_BYTES: usize = 64 * 1024;

    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(backend.local_addr().unwrap(), "https");
    let session = session(&state);
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = http_listener.local_addr().unwrap();
    let app = qdynn_server::app(state.clone()).into_make_service_with_connect_info::<SocketAddr>();
    let server = tokio::spawn(async move {
        axum::serve(http_listener, app).await.unwrap();
    });
    let mut peers = Vec::new();
    for id in 1..=BULK_STREAMS + 1 {
        session.handle_frame(Frame::open(id)).await.unwrap();
        let (peer, _) = timeout(Duration::from_secs(2), backend.accept())
            .await
            .expect("relay did not open backend stream")
            .unwrap();
        peers.push(peer);
    }

    let bulk_chunk = vec![0x5a; FRAME_BYTES];
    for peer in peers.iter_mut().take(BULK_STREAMS as usize) {
        for _ in 0..FRAMES_PER_STREAM {
            peer.write_all(&bulk_chunk).await.unwrap();
        }
    }
    let bulk_stream_ids = timeout(Duration::from_secs(5), async {
        loop {
            let counts = session.downlink_rx.lock().await.queued_items_by_stream();
            let ready: Vec<_> = counts
                .iter()
                .filter(|(_, count)| **count >= FRAMES_PER_STREAM)
                .map(|(id, _)| *id)
                .collect();
            if ready.len() >= BULK_STREAMS as usize {
                break ready;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("all bulk streams did not reach the relay queue");

    let probe_started = std::time::Instant::now();
    peers[BULK_STREAMS as usize]
        .write_all(b"short-reply-probe")
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let counts = session.downlink_rx.lock().await.queued_items_by_stream();
            if counts
                .iter()
                .any(|(id, count)| !bulk_stream_ids.contains(id) && *count > 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("probe reply did not reach the relay queue");

    let mut cursor = 0u64;
    let mut bytes_ahead = 0usize;
    let mut probe_seen = false;
    while !probe_seen {
        let response = timeout(
            Duration::from_secs(5),
            downlink_http_request(http_address, &session.id, cursor),
        )
        .await
        .expect("downlink poll timed out");
        let (status, next_cursor, body) = response;
        cursor = next_cursor;
        if status == StatusCode::OK.as_u16() {
            let mut encoded = BytesMut::from(body.as_slice());
            for frame in decode_batch(&mut encoded).unwrap() {
                if frame.payload.as_ref() == b"short-reply-probe" {
                    probe_seen = true;
                    break;
                }
                bytes_ahead += frame.payload.len() + 8;
            }
        }
        assert!(
            probe_started.elapsed() < Duration::from_secs(5),
            "short reply was not serviced promptly; bytes_ahead={bytes_ahead}"
        );
    }
    let fifo_ahead = BULK_STREAMS as usize * FRAMES_PER_STREAM * (FRAME_BYTES + 8);
    let elapsed = probe_started.elapsed();
    assert!(
        bytes_ahead < fifo_ahead / 2,
        "DRR should service the probe before half the queued FIFO bulk; ahead={bytes_ahead}, fifo={fifo_ahead}, elapsed={elapsed:?}"
    );
    eprintln!(
        "relay probe: bytes_ahead={bytes_ahead}, fifo_baseline={fifo_ahead}, elapsed_us={}",
        elapsed.as_micros()
    );
    for id in 1..=BULK_STREAMS + 1 {
        session.close_stream(id);
    }
    server.abort();
}

#[tokio::test]
async fn close_releases_idle_backend_socket() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sess = Session::new(
        "synthetic".into(),
        "audit".into(),
        backend.local_addr().unwrap(),
    );
    sess.handle_frame(Frame::open(1)).await.unwrap();
    let (mut connection, _) = backend.accept().await.unwrap();
    // Allow both tasks to park in read()/recv() before closing the logical stream.
    tokio::time::sleep(Duration::from_millis(30)).await;
    sess.close_stream(1);
    let mut buf = [0u8; 1];
    let outcome = timeout(Duration::from_millis(500), connection.read(&mut buf)).await;
    assert!(
        matches!(outcome, Ok(Ok(0)) | Ok(Err(_))),
        "backend still open 500ms after CLOSE: {outcome:?}"
    );
}

#[tokio::test]
async fn websocket_lanes_never_deliver_other_stream_ids() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(backend.local_addr().unwrap(), "websocket-lanes");
    let sess = session(&state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/api/v1/ws", get(handlers::handle_ws))
        .with_state(state);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let request = |lane: u32| {
        let mut req = format!("ws://{addr}/api/v1/ws")
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            "sec-websocket-protocol",
            format!("tproxy-lane-v1.{}.{}", sess.id, lane)
                .parse()
                .unwrap(),
        );
        req.headers_mut()
            .insert("origin", "https://audit.example".parse().unwrap());
        req
    };
    let (mut lane1, _) = connect_async(request(1)).await.unwrap();
    // Each lane owns its own receiver; wait for that receiver to be attached.
    timeout(Duration::from_secs(2), async {
        while sess.lane(1).unwrap().rx.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    lane1
        .send(Message::Binary(encode_batch(&[Frame::open(1)])))
        .await
        .unwrap();
    let (_backend1, _) = timeout(Duration::from_secs(2), backend.accept())
        .await
        .unwrap()
        .unwrap();
    let (mut lane2, _) = connect_async(request(2)).await.unwrap();
    lane2
        .send(Message::Binary(encode_batch(&[Frame::open(2)])))
        .await
        .unwrap();
    let (mut backend2, _) = timeout(Duration::from_secs(2), backend.accept())
        .await
        .unwrap()
        .unwrap();
    backend2.write_all(b"reply for stream 2").await.unwrap();
    let (receiving_lane, packet) = timeout(Duration::from_secs(2), async {
        tokio::select! {
            packet = lane1.next() => (1, packet),
            packet = lane2.next() => (2, packet),
        }
    })
    .await
    .unwrap();
    let packet = packet.unwrap().unwrap();
    server.abort();
    match packet {
        Message::Binary(data) => {
            let frames = decode_batch(&mut BytesMut::from(data.as_ref())).unwrap();
            assert_eq!(
                receiving_lane,
                2,
                "lane {receiving_lane} received stream IDs {:?}",
                frames.iter().map(|f| f.stream_id).collect::<Vec<_>>()
            );
            assert!(frames.iter().all(|f| f.stream_id == 2));
        }
        value => panic!("unexpected packet: {value:?}"),
    }
}

#[tokio::test]
async fn truncated_transport_batch_is_not_acknowledged() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(backend.local_addr().unwrap(), "https");
    let sess = session(&state);
    let mut headers = auth(&sess.id);
    headers.insert("x-up-seq", "1".parse().unwrap());
    let mut bytes = BytesMut::from(encode_batch(&[Frame::open(1)]).as_ref());
    bytes.extend_from_slice(&[2, 0, 0]);
    let response = handlers::handle_up(State(state), headers, bytes.freeze()).await;
    assert!(
        response.is_err(),
        "transport acknowledged a complete OPEN followed by a truncated frame"
    );
}
