use crate::error::ProtoError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const HEADER_LEN: usize = 8;
pub const MAX_PAYLOAD_LEN: usize = 1024 * 1024; // 1 MiB max payload
pub const MAX_RELAY_CHUNK_LEN: usize = 64 * 1024; // 64 KiB chunk target
pub const INITIAL_STREAM_WINDOW: u32 = 4 * 1024 * 1024; // 4 MiB initial window

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    Open = 0x01,
    Data = 0x02,
    Close = 0x03,
    Window = 0x04,
    Ping = 0x05,
    Pong = 0x06,
    Hello = 0x10,
    Welcome = 0x11,
    Bye = 0x1f,
}

impl FrameType {
    pub fn from_u8(value: u8) -> Result<Self, ProtoError> {
        match value {
            0x01 => Ok(FrameType::Open),
            0x02 => Ok(FrameType::Data),
            0x03 => Ok(FrameType::Close),
            0x04 => Ok(FrameType::Window),
            0x05 => Ok(FrameType::Ping),
            0x06 => Ok(FrameType::Pong),
            0x10 => Ok(FrameType::Hello),
            0x11 => Ok(FrameType::Welcome),
            0x1f => Ok(FrameType::Bye),
            unknown => Err(ProtoError::UnknownFrameType(unknown)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub frame_type: FrameType,
    pub stream_id: u32, // 24-bit unsigned integer
    pub payload: Bytes,
}

impl Frame {
    pub fn new(frame_type: FrameType, stream_id: u32, payload: Bytes) -> Self {
        assert!(stream_id <= 0x00FF_FFFF, "stream_id must fit in 24 bits");
        Self {
            frame_type,
            stream_id,
            payload,
        }
    }

    pub fn hello() -> Self {
        Self::new(FrameType::Hello, 0, Bytes::from_static(&[0x01]))
    }

    pub fn welcome() -> Self {
        Self::new(FrameType::Welcome, 0, Bytes::new())
    }

    pub fn open(stream_id: u32) -> Self {
        assert!(stream_id > 0, "OPEN frame must have nonzero stream_id");
        Self::new(FrameType::Open, stream_id, Bytes::new())
    }

    pub fn data(stream_id: u32, payload: Bytes) -> Self {
        assert!(stream_id > 0, "DATA frame must have nonzero stream_id");
        Self::new(FrameType::Data, stream_id, payload)
    }

    pub fn close(stream_id: u32) -> Self {
        assert!(stream_id > 0, "CLOSE frame must have nonzero stream_id");
        Self::new(FrameType::Close, stream_id, Bytes::new())
    }

    pub fn window(stream_id: u32, delta: u32) -> Self {
        assert!(stream_id > 0, "WINDOW frame must have nonzero stream_id");
        assert!(delta > 0, "WINDOW delta must be > 0");
        let mut buf = BytesMut::with_capacity(4);
        buf.put_u32(delta);
        Self::new(FrameType::Window, stream_id, buf.freeze())
    }

    pub fn ping(token: Bytes) -> Self {
        Self::new(FrameType::Ping, 0, token)
    }

    pub fn pong(token: Bytes) -> Self {
        Self::new(FrameType::Pong, 0, token)
    }

    pub fn bye(reason: Option<Bytes>) -> Self {
        Self::new(FrameType::Bye, 0, reason.unwrap_or_default())
    }

    /// Returns WINDOW delta if this is a valid WINDOW frame.
    pub fn parse_window_delta(&self) -> Result<u32, ProtoError> {
        if self.frame_type != FrameType::Window {
            return Err(ProtoError::InvalidStreamId {
                frame_type: "WINDOW",
                expected: "Type == 0x04",
                actual: self.frame_type as u8 as u32,
            });
        }
        if self.payload.len() != 4 {
            return Err(ProtoError::PayloadLengthMismatch {
                header_len: 4,
                available: self.payload.len(),
            });
        }
        let delta = (&self.payload[..]).get_u32();
        if delta == 0 {
            return Err(ProtoError::InvalidWindowDelta);
        }
        Ok(delta)
    }

    pub fn validate_shape(&self) -> Result<(), ProtoError> {
        let session_control = matches!(
            self.frame_type,
            FrameType::Hello
                | FrameType::Welcome
                | FrameType::Ping
                | FrameType::Pong
                | FrameType::Bye
        );
        if self.stream_id > 0xffffff
            || (self.stream_id == 0) != session_control
            || self.payload.len() > MAX_PAYLOAD_LEN
        {
            return Err(ProtoError::InvalidFrame("stream id or payload size"));
        }
        let valid = match self.frame_type {
            FrameType::Open | FrameType::Close | FrameType::Welcome => self.payload.is_empty(),
            FrameType::Hello => self.payload.as_ref() == [1],
            FrameType::Data => !self.payload.is_empty(),
            FrameType::Window => self.parse_window_delta().is_ok(),
            _ => self.payload.len() <= 64,
        };
        if valid {
            Ok(())
        } else {
            Err(ProtoError::InvalidFrame("payload shape"))
        }
    }

    /// Encodes this single frame into the given BytesMut buffer.
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.reserve(HEADER_LEN + self.payload.len());
        // Type: u8
        dst.put_u8(self.frame_type as u8);
        // Stream ID: u24 (big-endian 3 bytes)
        dst.put_u8(((self.stream_id >> 16) & 0xFF) as u8);
        dst.put_u8(((self.stream_id >> 8) & 0xFF) as u8);
        dst.put_u8((self.stream_id & 0xFF) as u8);
        // Payload length: u32
        dst.put_u32(self.payload.len() as u32);
        // Payload
        dst.put_slice(&self.payload);
    }

    /// Attempts to decode one Frame from the front of the buffer.
    /// If there is not enough data, returns Ok(None) and leaves the buffer untouched.
    pub fn decode(src: &mut BytesMut) -> Result<Option<Self>, ProtoError> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }

        let type_byte = src[0];
        let stream_id = ((src[1] as u32) << 16) | ((src[2] as u32) << 8) | (src[3] as u32);
        let payload_len = ((src[4] as u32) << 24)
            | ((src[5] as u32) << 16)
            | ((src[6] as u32) << 8)
            | (src[7] as u32);

        if payload_len as usize > MAX_PAYLOAD_LEN {
            return Err(ProtoError::PayloadTooLarge {
                actual: payload_len as usize,
                max: MAX_PAYLOAD_LEN,
            });
        }

        let total_frame_len = HEADER_LEN + payload_len as usize;
        if src.len() < total_frame_len {
            // Wait for remaining bytes
            return Ok(None);
        }

        let frame_type = FrameType::from_u8(type_byte)?;

        // Advance past header
        src.advance(HEADER_LEN);
        // Extract exact payload without copying
        let payload = src.split_to(payload_len as usize).freeze();

        // Validate stream_id rules
        match frame_type {
            FrameType::Hello
            | FrameType::Welcome
            | FrameType::Ping
            | FrameType::Pong
            | FrameType::Bye => {
                if stream_id != 0 {
                    return Err(ProtoError::InvalidStreamId {
                        frame_type: "SessionControl",
                        expected: "0",
                        actual: stream_id,
                    });
                }
            }
            FrameType::Open | FrameType::Data | FrameType::Close | FrameType::Window => {
                if stream_id == 0 {
                    return Err(ProtoError::InvalidStreamId {
                        frame_type: "DataStream",
                        expected: "nonzero",
                        actual: 0,
                    });
                }
            }
        }

        Ok(Some(Frame {
            frame_type,
            stream_id,
            payload,
        }))
    }
}

/// Decodes all available frames from a batch buffer.
pub fn decode_batch(src: &mut BytesMut) -> Result<Vec<Frame>, ProtoError> {
    let mut frames = Vec::new();
    while let Some(frame) = Frame::decode(src)? {
        frames.push(frame);
    }
    Ok(frames)
}

/// A transport message must contain complete, well-formed frames only.
pub fn decode_transport_batch(data: &[u8]) -> Result<Vec<Frame>, ProtoError> {
    if data.is_empty() {
        return Err(ProtoError::InvalidFrame("empty batch"));
    }
    if data.len() > 2 * 1024 * 1024 {
        return Err(ProtoError::PayloadTooLarge {
            actual: data.len(),
            max: 2 * 1024 * 1024,
        });
    }
    let mut src = BytesMut::from(data);
    let mut frames = Vec::new();
    while !src.is_empty() {
        let frame = Frame::decode(&mut src)?.ok_or(ProtoError::BufferTooShort {
            expected: HEADER_LEN,
            actual: src.len(),
        })?;
        frame.validate_shape()?;
        if frames.len() >= 4096 {
            return Err(ProtoError::InvalidFrame("batch frame count"));
        }
        frames.push(frame);
    }
    Ok(frames)
}

/// Encodes a slice of frames into a single continuous Bytes buffer.
pub fn encode_batch(frames: &[Frame]) -> Bytes {
    let total_len: usize = frames.iter().map(|f| HEADER_LEN + f.payload.len()).sum();
    let mut buf = BytesMut::with_capacity(total_len);
    for frame in frames {
        frame.encode(&mut buf);
    }
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_rejects_truncation_and_malformed_shapes() {
        let body = encode_batch(&[Frame::data(1, Bytes::from_static(b"payload"))]);
        for end in 1..body.len() {
            assert!(decode_transport_batch(&body[..end]).is_err());
        }
        for frame in [
            Frame::new(FrameType::Open, 1, Bytes::from_static(b"x")),
            Frame::data(1, Bytes::new()),
            Frame::pong(Bytes::from(vec![0; 65])),
            Frame::new(FrameType::Window, 1, Bytes::from_static(&[0, 0, 0, 0])),
            Frame::new(FrameType::Hello, 0, Bytes::from_static(&[2])),
        ] {
            assert!(decode_transport_batch(&encode_batch(&[frame])).is_err());
        }
    }

    #[test]
    fn test_encode_decode_hello() {
        let frame = Frame::hello();
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        assert_eq!(buf.len(), 8 + 1);
        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_encode_decode_data() {
        let payload = Bytes::from_static(b"hello telegram web proxy");
        let frame = Frame::data(42, payload);
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_encode_decode_window() {
        let frame = Frame::window(101, 65536);
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.parse_window_delta().unwrap(), 65536);
    }

    #[test]
    fn test_batch_codec() {
        let frames = vec![
            Frame::open(1),
            Frame::data(1, Bytes::from_static(b"chunk1")),
            Frame::data(1, Bytes::from_static(b"chunk2")),
            Frame::window(1, 4096),
            Frame::close(1),
        ];

        let encoded = encode_batch(&frames);
        let mut buf = BytesMut::from(&encoded[..]);
        let decoded = decode_batch(&mut buf).unwrap();

        assert_eq!(decoded, frames);
        assert_eq!(buf.len(), 0);
    }
}
