use crate::handlers::AppState;
use axum::http::{header, HeaderMap, StatusCode};
use qdynn_proto::{decode_transport_batch, Frame, FrameType};
pub(crate) mod http;
pub(crate) mod ws;

pub(crate) fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    let token = headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")?;
    (token.len() == 43
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
    .then_some(token)
}
pub(crate) fn check_origin(state: &AppState, headers: &HeaderMap) -> Result<(), StatusCode> {
    if let Some(origin) = headers.get(header::ORIGIN) {
        if origin.to_str().ok() != Some(format!("https://{}", state.config.hostname).as_str()) {
            return Err(StatusCode::NOT_FOUND);
        }
    }
    Ok(())
}
pub(super) fn number(headers: &HeaderMap, name: &str) -> Result<u64, StatusCode> {
    let text = headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let value = text.parse::<u64>().map_err(|_| StatusCode::BAD_REQUEST)?;
    if value.to_string() != text {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(value)
}
pub(super) fn http_lane(
    carrier_mode: &str,
    headers: &HeaderMap,
) -> Result<Option<u32>, StatusCode> {
    match carrier_mode {
        "https" if !headers.contains_key("x-lane-id") => Ok(None),
        "https-lanes" => {
            let id = number(headers, "x-lane-id")?;
            if id > 0xffffff {
                return Err(StatusCode::BAD_REQUEST);
            }
            Ok(Some(id as u32))
        }
        _ => Err(StatusCode::NOT_FOUND),
    }
}
pub(super) fn client_frames(body: &[u8], lane: Option<u32>) -> Result<Vec<Frame>, StatusCode> {
    let frames = decode_transport_batch(body).map_err(|_| StatusCode::BAD_REQUEST)?;
    if frames.is_empty()
        || frames.iter().any(|f| {
            !matches!(
                f.frame_type,
                FrameType::Open
                    | FrameType::Data
                    | FrameType::Close
                    | FrameType::Window
                    | FrameType::Pong
            ) || lane.is_some_and(|id| f.stream_id != id)
        })
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(frames)
}
