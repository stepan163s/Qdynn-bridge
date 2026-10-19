use super::{check_origin, client_frames, extract_bearer_token, http_lane, number};
use crate::handlers::AppState;
use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use qdynn_proto::{decode_transport_batch, encode_batch, Frame, FrameType};
use sha2::{Digest, Sha256};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::time::timeout;

pub async fn handle_create_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, StatusCode> {
    check_origin(&state, &headers)?;
    let token = extract_bearer_token(&headers).ok_or(StatusCode::NOT_FOUND)?;
    if decode_transport_batch(&body).ok().as_deref() != Some(&[Frame::hello()]) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let requested_mode = headers
        .get("x-carrier-mode")
        .and_then(|value| value.to_str().ok())
        .unwrap_or(if state.config.carrier_mode == "auto" {
            "websocket"
        } else {
            state.config.carrier_mode.as_str()
        });
    let supported = if state.config.carrier_mode == "auto" {
        &["websocket", "https"][..]
    } else {
        &[state.config.carrier_mode.as_str()][..]
    };
    if !supported.contains(&requested_mode) {
        return Err(StatusCode::NOT_FOUND);
    }
    let session = state
        .registry
        .exchange_bootstrap_token_for_carrier(token, requested_mode)
        .map_err(|e| match e {
            qdynn_core::CoreError::StreamLimitExceeded => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::NOT_FOUND,
        })?;
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::CACHE_CONTROL, "no-store"),
            (
                header::HeaderName::from_static("x-session-token"),
                session.id.as_str(),
            ),
            (
                header::HeaderName::from_static("x-carrier-mode"),
                session.carrier_mode.as_str(),
            ),
            (header::HeaderName::from_static("x-down-cursor"), "0"),
        ],
        encode_batch(&[Frame::welcome()]),
    )
        .into_response())
}
pub async fn handle_up(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, StatusCode> {
    check_origin(&state, &headers)?;
    let token = extract_bearer_token(&headers).ok_or(StatusCode::NOT_FOUND)?;
    let session = state
        .registry
        .get_session(token)
        .ok_or(StatusCode::NOT_FOUND)?;
    let lane_id = http_lane(&session.carrier_mode, &headers)?;
    let frames = client_frames(&body, lane_id)?;
    let seq = number(&headers, "x-up-seq")?;
    if seq == 0 {
        return Err(StatusCode::CONFLICT);
    }
    if lane_id.is_some_and(|id| id != 0 && !session.has_lane(id))
        && frames.iter().all(|f| {
            matches!(
                f.frame_type,
                FrameType::Data | FrameType::Window | FrameType::Close
            )
        })
    {
        session.touch();
        return Ok((
            StatusCode::NO_CONTENT,
            [
                ("x-up-ack", seq.to_string()),
                ("cache-control", "no-store".into()),
            ],
        )
            .into_response());
    }
    let lane = session
        .lane(lane_id.unwrap_or(0))
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let digest: [u8; 32] = Sha256::digest(&body).into();
    let mut up = lane.up.lock().await;
    if seq == 0
        || (seq == up.seq && digest != up.digest)
        || (seq != up.seq && up.seq.checked_add(1) != Some(seq))
    {
        return Err(StatusCode::CONFLICT);
    }
    if seq != up.seq {
        let _mutation = session.mutation.lock().await;
        if let Some(id) = lane_id {
            if id != 0
                && !session.streams.contains_key(&id)
                && !session.tombstones.contains_key(&id)
                && frames[0].frame_type != FrameType::Open
            {
                return Err(StatusCode::BAD_REQUEST);
            }
        }
        session
            .validate_batch(&frames)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        for frame in frames {
            if session.apply_frame(frame, lane_id).await.is_err() {
                session.close_all();
                return Err(StatusCode::BAD_REQUEST);
            }
        }
        up.seq = seq;
        up.digest = digest;
    }
    session.touch();
    Ok((
        StatusCode::NO_CONTENT,
        [
            ("x-up-ack", seq.to_string()),
            ("cache-control", "no-store".into()),
        ],
    )
        .into_response())
}

/// The previous response remains retained until its cursor is acknowledged.
/// A new poll supersedes an older poll for the same lane without stealing data.
pub async fn handle_down(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    check_origin(&state, &headers)?;
    let token = extract_bearer_token(&headers).ok_or(StatusCode::NOT_FOUND)?;
    let session = state
        .registry
        .get_session(token)
        .ok_or(StatusCode::NOT_FOUND)?;
    let lane_id = http_lane(&session.carrier_mode, &headers)?;
    let cursor = number(&headers, "x-down-cursor")?;
    if lane_id.is_some_and(|id| id != 0 && !session.has_lane(id)) {
        session.touch();
        return Ok((
            StatusCode::NO_CONTENT,
            [
                ("x-down-cursor", cursor.to_string()),
                ("x-lane-closed", "1".into()),
                ("cache-control", "no-store".into()),
            ],
        )
            .into_response());
    }

    let lane = session
        .lane(lane_id.unwrap_or(0))
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let generation = lane.poll_generation.fetch_add(1, Ordering::AcqRel) + 1;
    lane.poll_changed.notify_waiters();
    let result = async {
        let mut down = lane.down.lock().await;
        if cursor != down.cursor {
            if cursor.checked_add(1) == Some(down.cursor) && !down.body.is_empty() {
                return Ok((down.cursor, down.body.clone()));
            }
            return Err(StatusCode::CONFLICT);
        }
        down.body = Bytes::new();
        down.charge = None;
        let mut rx = lane.rx.lock().await;
        let first = if lane_id.is_some_and(|id| id != 0 && session.tombstones.contains_key(&id)) {
            rx.try_recv().ok().map(|f| vec![f])
        } else {
            timeout(Duration::from_secs(25), rx.recv_batch())
                .await
                .ok()
                .flatten()
        };
        if let Some(batch) = first {
            let size = batch.iter().map(|f| f.payload.len() + 8).sum();
            down.charge = Some(lane.tx.budget().charge(size).map_err(|_| {
                session.close_all();
                StatusCode::SERVICE_UNAVAILABLE
            })?);
            down.body = encode_batch(&batch);
            down.cursor = down.cursor.checked_add(1).ok_or(StatusCode::CONFLICT)?;
        }
        Ok((down.cursor, down.body.clone()))
    };
    // Register before checking generation: notification cannot be lost between them.
    let superseded = async {
        loop {
            let changed = lane.poll_changed.notified();
            if lane.poll_generation.load(Ordering::Acquire) != generation || session.is_expired() {
                break;
            }
            changed.await;
        }
    };
    let (out_cursor, body) = tokio::select! {
        value = result => value?,
        _ = superseded => { if session.is_expired() { session.close_all(); } return Ok(StatusCode::NO_CONTENT.into_response()); },
    };
    session.touch();
    let closed = lane_id.is_some_and(|id| id != 0 && session.tombstones.contains_key(&id))
        && body.is_empty();
    if closed {
        session.retire_lane(lane_id.unwrap());
    }
    Ok((
        if body.is_empty() {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::OK
        },
        [
            ("content-type", "application/octet-stream".to_string()),
            ("cache-control", "no-store".into()),
            ("x-down-cursor", out_cursor.to_string()),
            ("x-lane-closed", if closed { "1" } else { "0" }.into()),
        ],
        body,
    )
        .into_response())
}
pub async fn handle_delete_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> StatusCode {
    if check_origin(&state, &headers).is_err() {
        return StatusCode::NOT_FOUND;
    }
    if let Some(token) = extract_bearer_token(&headers) {
        state.registry.remove_session(token);
    }
    StatusCode::NO_CONTENT
}
