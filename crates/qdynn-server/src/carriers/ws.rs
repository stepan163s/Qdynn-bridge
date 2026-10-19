use super::{check_origin, client_frames};
use crate::handlers::AppState;
use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use qdynn_proto::{encode_batch, FrameType};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::time::timeout;

struct Attachment {
    _permit: tokio::sync::OwnedSemaphorePermit,
    lane: Arc<qdynn_core::session::Lane>,
    session: Arc<qdynn_core::Session>,
    id: Option<u32>,
}
impl Drop for Attachment {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.session.close_stream(id);
            self.session.retire_lane(id);
        } else {
            self.session.close_all();
        }
        self.lane.attached.store(false, Ordering::Release);
    }
}
pub async fn handle_ws(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    check_origin(&state, &headers)?;
    let protocol = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::NOT_FOUND)?;
    let (requested_mode, token, lane_id) = if let Some(token) = protocol.strip_prefix("tproxy-v1.")
    {
        ("websocket", token, None)
    } else if let Some(rest) = protocol.strip_prefix("tproxy-lane-v1.") {
        let (token, id) = rest.split_once('.').ok_or(StatusCode::NOT_FOUND)?;
        let lane = id.parse::<u32>().map_err(|_| StatusCode::NOT_FOUND)?;
        if lane == 0 || lane > 0xffffff || lane.to_string() != id {
            return Err(StatusCode::NOT_FOUND);
        }
        ("websocket-lanes", token, Some(lane))
    } else {
        return Err(StatusCode::NOT_FOUND);
    };
    let session = state
        .registry
        .get_session(token)
        .ok_or(StatusCode::NOT_FOUND)?;
    if session.carrier_mode != requested_mode {
        return Err(StatusCode::NOT_FOUND);
    }
    let lane_id = match requested_mode {
        "websocket" => None,
        "websocket-lanes" => lane_id,
        _ => return Err(StatusCode::NOT_FOUND),
    };
    if lane_id.is_some_and(|id| session.tombstones.contains_key(&id)) {
        return Err(StatusCode::NOT_FOUND);
    }
    let lane = session
        .lane(lane_id.unwrap_or(0))
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    if lane.attached.swap(true, Ordering::AcqRel) {
        return Err(StatusCode::CONFLICT);
    }
    let permit = match state.websocket_admission.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            lane.attached.store(false, Ordering::Release);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };
    tracing::debug!(connection = %session.trace_id, lane = ?lane_id, "WebSocket attached");
    let attachment = Attachment {
        _permit: permit,
        lane,
        session,
        id: lane_id,
    };
    let registry = state.registry.clone();
    Ok(ws
        .max_message_size(2 * 1024 * 1024)
        .max_frame_size(2 * 1024 * 1024)
        .protocols([protocol.to_owned()])
        .on_upgrade(move |socket| handle_websocket_connection(socket, attachment, registry)))
}
async fn handle_websocket_connection(
    socket: WebSocket,
    attachment: Attachment,
    registry: Arc<qdynn_core::SessionRegistry>,
) {
    let (mut sender, mut receiver) = socket.split();
    let session = &attachment.session;
    let lane = &attachment.lane;
    let down = async {
        let mut rx = lane.rx.lock().await;
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(25),
            Duration::from_secs(25),
        );
        loop {
            let (message, _charge) = match tokio::select! { batch = rx.recv_batch() => Some(batch), _ = heartbeat.tick() => None }
            {
                Some(Some(batch)) => {
                    let size = batch.iter().map(|f| f.payload.len() + 8).sum();
                    let Ok(charge) = lane.tx.budget().charge(size) else {
                        break;
                    };
                    (Message::Binary(encode_batch(&batch)), Some(charge))
                }
                Some(None) => break,
                None => (Message::Ping(Bytes::new()), None),
            };
            if !matches!(
                timeout(Duration::from_secs(10), sender.send(message)).await,
                Ok(Ok(()))
            ) {
                break;
            }
        }
    };
    let up = async {
        let mut first = true;
        while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(50), receiver.next()).await {
            session.touch();
            match msg {
                Message::Binary(body) => {
                    let Ok(_input_charge) = registry.reserve_input(body.len()) else {
                        return;
                    };
                    let Ok(frames) = client_frames(&body, attachment.id) else {
                        break;
                    };
                    if first && attachment.id.is_some() && frames[0].frame_type != FrameType::Open {
                        break;
                    }
                    first = false;
                    let _guard = session.mutation.lock().await;
                    if session.validate_batch(&frames).is_err() {
                        return;
                    }
                    for frame in frames {
                        if session.apply_frame(frame, attachment.id).await.is_err() {
                            return;
                        }
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => break,
            }
        }
    };
    let ended = async {
        loop {
            let notified = session.ended.notified();
            if session.is_expired() {
                break;
            }
            notified.await;
        }
    };
    // These futures are scoped, not detached tasks. Dropping the losing future
    // releases the lane receiver and both halves of the WebSocket immediately.
    tokio::select! { _ = down => {}, _ = up => {}, _ = ended => {} }
}
