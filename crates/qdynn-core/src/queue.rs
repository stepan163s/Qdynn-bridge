//! Queue charges include a conservative per-item cost. Data cannot consume the
//! permits reserved for WINDOW/CLOSE. Admission never waits while holding state.
use qdynn_proto::{Frame, FrameType};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, OnceLock},
};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct Budget {
    data: Arc<Semaphore>,
    control: Arc<Semaphore>,
    shared: Arc<SharedBudgets>,
    lane_queue_items: usize,
    lane_data_items: usize,
    stream_queue_items: usize,
}
#[derive(Clone)]
pub struct QueueConfig {
    pub session_data_bytes: usize,
    pub session_control_bytes: usize,
    pub global_data_bytes: usize,
    pub global_control_bytes: usize,
    pub input_bytes: usize,
    pub lane_queue_items: usize,
    pub lane_data_items: usize,
    pub stream_queue_items: usize,
    pub max_streams_per_session: usize,
    pub max_lanes_per_session: usize,
    pub backend_connections: usize,
}
impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            session_data_bytes: 16 * 1024 * 1024,
            session_control_bytes: 128 * 1024,
            global_data_bytes: 128 * 1024 * 1024,
            global_control_bytes: 4 * 1024 * 1024,
            input_bytes: 64 * 1024 * 1024,
            lane_queue_items: 128,
            lane_data_items: 96,
            stream_queue_items: 4096,
            max_streams_per_session: 32,
            max_lanes_per_session: 129,
            backend_connections: 256,
        }
    }
}
pub struct SharedBudgets {
    data: Arc<Semaphore>,
    control: Arc<Semaphore>,
    input: Arc<Semaphore>,
    backend_connections: Arc<Semaphore>,
}
impl SharedBudgets {
    pub fn new(config: &QueueConfig) -> Arc<Self> {
        Arc::new(Self {
            data: Arc::new(Semaphore::new(config.global_data_bytes)),
            control: Arc::new(Semaphore::new(config.global_control_bytes)),
            input: Arc::new(Semaphore::new(config.input_bytes)),
            backend_connections: Arc::new(Semaphore::new(config.backend_connections)),
        })
    }
}
impl Default for Budget {
    fn default() -> Self {
        let config = QueueConfig::default();
        Self::with_config(&config, default_shared())
    }
}
impl Budget {
    pub fn with_config(config: &QueueConfig, shared: Arc<SharedBudgets>) -> Self {
        Self {
            data: Arc::new(Semaphore::new(config.session_data_bytes)),
            control: Arc::new(Semaphore::new(config.session_control_bytes)),
            shared,
            lane_queue_items: config.lane_queue_items,
            lane_data_items: config.lane_data_items.min(config.lane_queue_items),
            stream_queue_items: config.stream_queue_items,
        }
    }
    pub fn lane_queue_items(&self) -> usize {
        self.lane_queue_items
    }
    pub fn lane_data_items(&self) -> usize {
        self.lane_data_items
    }
    pub fn stream_queue_items(&self) -> usize {
        self.stream_queue_items
    }
    pub fn try_backend_permit(&self) -> Result<OwnedSemaphorePermit, &'static str> {
        self.shared
            .backend_connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| "backend connection limit")
    }
}
pub fn default_shared() -> Arc<SharedBudgets> {
    static DEFAULT: OnceLock<Arc<SharedBudgets>> = OnceLock::new();
    DEFAULT
        .get_or_init(|| SharedBudgets::new(&QueueConfig::default()))
        .clone()
}
pub struct Charge {
    _local: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}
impl Budget {
    pub fn charge(&self, bytes: usize) -> Result<Charge, &'static str> {
        let cost = u32::try_from(bytes.checked_add(256).ok_or("budget overflow")?)
            .map_err(|_| "budget overflow")?;
        Ok(Charge {
            _local: self
                .data
                .clone()
                .try_acquire_many_owned(cost)
                .map_err(|_| "session byte budget")?,
            _global: self
                .shared
                .data
                .clone()
                .try_acquire_many_owned(cost)
                .map_err(|_| "global byte budget")?,
        })
    }
}
/// Separate admission for incoming HTTP/WS bodies: a full outbound queue must
/// not prevent a small WINDOW request from being read and releasing credit.
pub fn reserve_input(bytes: usize) -> Result<OwnedSemaphorePermit, &'static str> {
    reserve_input_with(default_shared(), bytes)
}
pub fn reserve_input_with(
    shared: Arc<SharedBudgets>,
    bytes: usize,
) -> Result<OwnedSemaphorePermit, &'static str> {
    let cost = u32::try_from(bytes.checked_add(256).ok_or("input overflow")?)
        .map_err(|_| "input overflow")?;
    shared
        .input
        .clone()
        .try_acquire_many_owned(cost)
        .map_err(|_| "input budget")
}
struct Charged {
    frame: Frame,
    _local: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
    _slot: Option<OwnedSemaphorePermit>,
}
#[derive(Default)]
struct ScheduledStream {
    frames: VecDeque<Charged>,
    deficit: usize,
}
#[derive(Clone)]
pub struct FrameSender {
    tx: mpsc::Sender<Charged>,
    budget: Budget,
    data_slots: Arc<Semaphore>,
}
pub struct FrameReceiver {
    rx: mpsc::Receiver<Charged>,
    pending: Option<Charged>,
    streams: HashMap<u32, ScheduledStream>,
    active_streams: VecDeque<u32>,
    active_controls: VecDeque<u32>,
    current_stream: Option<u32>,
    control_burst: usize,
}
pub fn channel(budget: Budget) -> (FrameSender, FrameReceiver) {
    let queue_items = budget.lane_queue_items();
    let data_items = budget.lane_data_items();
    let (tx, rx) = mpsc::channel(queue_items);
    (
        FrameSender {
            tx,
            budget,
            data_slots: Arc::new(Semaphore::new(data_items)),
        },
        FrameReceiver {
            rx,
            pending: None,
            streams: HashMap::new(),
            active_streams: VecDeque::new(),
            active_controls: VecDeque::new(),
            current_stream: None,
            control_burst: 0,
        },
    )
}
impl FrameSender {
    pub fn budget(&self) -> Budget {
        self.budget.clone()
    }
    pub fn try_send(&self, frame: Frame) -> Result<(), &'static str> {
        if frame.payload.len() > qdynn_proto::MAX_PAYLOAD_LEN {
            return Err("oversized frame");
        }
        let control = frame.frame_type != FrameType::Data;
        let cost = (frame.payload.len() + 256) as u32;
        let local = if control {
            &self.budget.control
        } else {
            &self.budget.data
        };
        let global = if control {
            &self.budget.shared.control
        } else {
            &self.budget.shared.data
        };
        let charged = Charged {
            frame,
            _local: local
                .clone()
                .try_acquire_many_owned(cost)
                .map_err(|_| "session queue budget")?,
            _global: global
                .clone()
                .try_acquire_many_owned(cost)
                .map_err(|_| "global queue budget")?,
            _slot: if control {
                None
            } else {
                Some(
                    self.data_slots
                        .clone()
                        .try_acquire_owned()
                        .map_err(|_| "lane queue budget")?,
                )
            },
        };
        self.tx.try_send(charged).map_err(|_| "lane queue full")
    }
    pub async fn send(&self, frame: Frame) -> Result<(), &'static str> {
        if frame.payload.len() > qdynn_proto::MAX_PAYLOAD_LEN {
            return Err("oversized frame");
        }
        let control = frame.frame_type != FrameType::Data;
        let cost = (frame.payload.len() + 256) as u32;
        let local = if control {
            &self.budget.control
        } else {
            &self.budget.data
        };
        let global = if control {
            &self.budget.shared.control
        } else {
            &self.budget.shared.data
        };
        let charged = Charged {
            frame,
            _local: local
                .clone()
                .acquire_many_owned(cost)
                .await
                .map_err(|_| "session closed")?,
            _global: global
                .clone()
                .acquire_many_owned(cost)
                .await
                .map_err(|_| "server closed")?,
            _slot: if control {
                None
            } else {
                Some(
                    self.data_slots
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| "lane closed")?,
                )
            },
        };
        self.tx.send(charged).await.map_err(|_| "lane closed")
    }
}
impl FrameReceiver {
    const SCHEDULER_QUANTUM: usize = qdynn_proto::MAX_RELAY_CHUNK_LEN + qdynn_proto::HEADER_LEN;
    const MAX_CONTROL_BURST: usize = 8;

    fn schedule(&mut self, value: Charged) {
        let stream_id = value.frame.stream_id;
        let is_data = value.frame.frame_type == FrameType::Data;
        let queue = self.streams.entry(stream_id).or_default();
        if queue.frames.is_empty() {
            if is_data {
                self.active_streams.push_back(stream_id);
            } else {
                self.active_controls.push_back(stream_id);
            }
        }
        queue.frames.push_back(value);
    }

    fn next_scheduled(&mut self) -> Option<Charged> {
        loop {
            let data_ready = self.current_stream.is_some() || !self.active_streams.is_empty();
            if !self.active_controls.is_empty()
                && (self.control_burst < Self::MAX_CONTROL_BURST || !data_ready)
            {
                let stream_id = self.active_controls.pop_front()?;
                let Some(state) = self.streams.get_mut(&stream_id) else {
                    continue;
                };
                if state
                    .frames
                    .front()
                    .is_some_and(|frame| frame.frame.frame_type == FrameType::Data)
                {
                    self.active_streams.push_back(stream_id);
                    continue;
                }
                let result = state.frames.pop_front();
                let empty = state.frames.is_empty();
                let next_is_data = state
                    .frames
                    .front()
                    .is_some_and(|frame| frame.frame.frame_type == FrameType::Data);
                if empty {
                    self.streams.remove(&stream_id);
                } else if next_is_data {
                    self.active_streams.push_back(stream_id);
                } else {
                    self.active_controls.push_back(stream_id);
                }
                self.control_burst += 1;
                return result;
            }

            let continuing_turn = self.current_stream.is_some();
            let stream_id = if continuing_turn {
                self.current_stream.take().unwrap()
            } else {
                let Some(stream_id) = self.active_streams.pop_front() else {
                    if self.active_controls.is_empty() {
                        return None;
                    }
                    continue;
                };
                stream_id
            };
            let Some(state) = self.streams.get_mut(&stream_id) else {
                continue;
            };
            if state
                .frames
                .front()
                .is_some_and(|frame| frame.frame.frame_type != FrameType::Data)
            {
                self.active_controls.push_back(stream_id);
                continue;
            }
            let mut deferred = false;
            let mut empty = false;
            let mut result = None;
            let mut can_continue = false;
            let mut next_is_data = false;
            if !continuing_turn {
                state.deficit = state.deficit.saturating_add(Self::SCHEDULER_QUANTUM);
            }
            if let Some(front) = state.frames.front() {
                let cost = front.frame.payload.len() + 8;
                if cost > state.deficit {
                    deferred = true;
                } else {
                    state.deficit -= cost;
                    result = state.frames.pop_front();
                    if let Some(next) = state.frames.front() {
                        next_is_data = next.frame.frame_type == FrameType::Data;
                        can_continue =
                            next_is_data && next.frame.payload.len() + 8 <= state.deficit;
                    } else {
                        empty = true;
                    }
                }
            } else {
                empty = true;
            }
            if deferred {
                self.active_streams.push_back(stream_id);
                continue;
            }
            if empty {
                self.streams.remove(&stream_id);
            } else if !next_is_data {
                self.active_controls.push_back(stream_id);
            } else {
                if can_continue {
                    self.current_stream = Some(stream_id);
                } else {
                    self.active_streams.push_back(stream_id);
                }
            }
            self.control_burst = 0;
            return result;
        }
    }

    pub fn close(&mut self) {
        self.pending = None;
        self.streams.clear();
        self.active_streams.clear();
        self.active_controls.clear();
        self.current_stream = None;
        self.control_burst = 0;
        self.rx.close();
    }
    /// Drain immediately available frames into the scheduler and return per-stream counts.
    pub fn queued_items_by_stream(&mut self) -> HashMap<u32, usize> {
        while let Ok(value) = self.rx.try_recv() {
            self.schedule(value);
        }
        let mut counts: HashMap<_, _> = self
            .streams
            .iter()
            .map(|(stream_id, stream)| (*stream_id, stream.frames.len()))
            .collect();
        if let Some(pending) = &self.pending {
            *counts.entry(pending.frame.stream_id).or_default() += 1;
        }
        counts
    }
    pub async fn recv(&mut self) -> Option<Frame> {
        if let Some(value) = self.pending.take() {
            return Some(value.frame);
        }
        if let Some(value) = self.next_scheduled() {
            return Some(value.frame);
        }
        let value = self.rx.recv().await?;
        self.schedule(value);
        self.next_scheduled().map(|value| value.frame)
    }
    pub fn try_recv(&mut self) -> Result<Frame, mpsc::error::TryRecvError> {
        if let Some(value) = self.pending.take() {
            return Ok(value.frame);
        }
        if let Some(value) = self.next_scheduled() {
            return Ok(value.frame);
        }
        self.rx.try_recv().map(|value| {
            self.schedule(value);
            self.next_scheduled().expect("scheduled frame").frame
        })
    }
    pub async fn recv_batch(&mut self) -> Option<Vec<Frame>> {
        let first = self.recv().await?;
        let mut bytes = first.payload.len() + 8;
        let mut frames = vec![first];
        while frames.len() < 4096 {
            while let Ok(value) = self.rx.try_recv() {
                self.schedule(value);
            }
            let Some(value) = self.next_scheduled() else {
                break;
            };
            let next = value.frame.payload.len() + 8;
            if bytes + next > 2 * 1024 * 1024 {
                self.pending = Some(value);
                break;
            }
            bytes += next;
            frames.push(value.frame);
        }
        Some(frames)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn data_backpressure_preserves_control_capacity_and_releases_charges() {
        let budget = Budget::default();
        let (tx, rx) = channel(budget.clone());
        for _ in 0..96 {
            tx.try_send(Frame::data(1, Bytes::from(vec![0; 65536])))
                .unwrap();
        }
        assert!(tx
            .try_send(Frame::data(1, Bytes::from_static(b"over quota")))
            .is_err());
        tx.try_send(Frame::close(1)).unwrap();
        drop(rx);
        assert!(budget.charge(16 * 1024 * 1024 - 256).is_ok());
    }

    #[tokio::test]
    async fn batches_respect_wire_size_without_dropping_lookahead() {
        let (tx, mut rx) = channel(Budget::default());
        let frame = Frame::data(1, Bytes::from(vec![0; 1024 * 1024]));
        tx.send(frame.clone()).await.unwrap();
        tx.send(frame.clone()).await.unwrap();
        assert_eq!(rx.recv_batch().await.unwrap(), std::slice::from_ref(&frame));
        assert_eq!(rx.recv_batch().await.unwrap(), [frame]);
    }

    #[tokio::test]
    async fn multiplexed_batch_round_robins_busy_streams_without_reordering_them() {
        let (tx, mut rx) = channel(Budget::default());
        for sequence in 0..17u8 {
            let mut payload = vec![0; 32 * 1024];
            payload[0] = sequence;
            tx.send(Frame::data(1, Bytes::from(payload))).await.unwrap();
        }
        tx.send(Frame::data(2, Bytes::from_static(b"other")))
            .await
            .unwrap();

        let batch = rx.recv_batch().await.unwrap();
        let other = batch.iter().position(|frame| frame.stream_id == 2).unwrap();
        assert!(
            other < 17,
            "busy stream must yield to a waiting short stream"
        );
        let stream_one: Vec<_> = batch
            .iter()
            .filter(|frame| frame.stream_id == 1)
            .map(|frame| frame.payload[0])
            .collect();
        assert_eq!(stream_one, (0..17u8).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn control_priority_is_bounded_and_preserves_stream_dependencies() {
        let (tx, mut rx) = channel(Budget::default());
        for _ in 0..12 {
            tx.send(Frame::window(2, 1024)).await.unwrap();
        }
        tx.send(Frame::open(3)).await.unwrap();
        for sequence in 0..12u8 {
            let mut payload = vec![0; 32 * 1024];
            payload[0] = sequence;
            tx.send(Frame::data(1, Bytes::from(payload))).await.unwrap();
        }
        tx.send(Frame::data(3, Bytes::from_static(b"ordered-data")))
            .await
            .unwrap();
        tx.send(Frame::close(3)).await.unwrap();

        let frames = rx.recv_batch().await.unwrap();
        let first_data = frames
            .iter()
            .position(|frame| frame.frame_type == FrameType::Data)
            .unwrap();
        assert!(
            first_data <= FrameReceiver::MAX_CONTROL_BURST,
            "control traffic must yield to DATA after a bounded burst"
        );
        let stream_three: Vec<_> = frames
            .iter()
            .filter(|frame| frame.stream_id == 3)
            .map(|frame| frame.frame_type)
            .collect();
        assert_eq!(
            stream_three,
            [FrameType::Open, FrameType::Data, FrameType::Close],
            "control scheduling must preserve OPEN/DATA/CLOSE order"
        );
        let bulk_sequence: Vec<_> = frames
            .iter()
            .filter(|frame| frame.stream_id == 1)
            .map(|frame| frame.payload[0])
            .collect();
        assert_eq!(bulk_sequence, (0..12u8).collect::<Vec<_>>());
    }

    #[test]
    fn configured_global_and_input_budgets_are_shared_and_released() {
        let config = QueueConfig {
            session_data_bytes: 8192,
            session_control_bytes: 4096,
            global_data_bytes: 8192,
            global_control_bytes: 4096,
            input_bytes: 8192,
            ..QueueConfig::default()
        };
        let shared = SharedBudgets::new(&config);
        let first = Budget::with_config(&config, shared.clone());
        let second = Budget::with_config(&config, shared.clone());
        let charge = first.charge(4000).unwrap();
        assert!(second.charge(4000).is_err());
        drop(charge);
        assert!(second.charge(4000).is_ok());

        let input = reserve_input_with(shared.clone(), 5000).unwrap();
        assert!(reserve_input_with(shared.clone(), 3000).is_err());
        drop(input);
        assert!(reserve_input_with(shared, 3000).is_ok());
    }
}
