use crate::queue::{self, Budget, FrameReceiver, FrameSender};
use crate::{
    error::CoreError,
    stream::{
        spawn_stream_bridge_with_cleanup_and_connector, BackendConnector, Stream,
        TcpBackendConnector,
    },
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use qdynn_proto::{Frame, FrameType};
use rand::RngCore;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Notify};
use tracing::Instrument;

pub const BOOTSTRAP_TTL: Duration = Duration::from_secs(120);
pub const SESSION_IDLE_TTL: Duration = Duration::from_secs(300);
pub fn generate_secure_token() -> String {
    let mut bytes = [0; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

#[derive(Default)]
pub struct UpState {
    pub seq: u64,
    pub digest: [u8; 32],
}
#[derive(Default)]
pub struct DownState {
    pub cursor: u64,
    pub body: Bytes,
    pub charge: Option<queue::Charge>,
}
pub struct Lane {
    pub tx: FrameSender,
    pub rx: Arc<Mutex<FrameReceiver>>,
    pub up: Mutex<UpState>,
    pub down: Mutex<DownState>,
    pub poll_generation: AtomicU64,
    pub poll_changed: Notify,
    pub attached: AtomicBool,
}
impl Lane {
    fn new(tx: FrameSender, rx: Arc<Mutex<FrameReceiver>>) -> Arc<Self> {
        Arc::new(Self {
            tx,
            rx,
            up: Mutex::new(UpState::default()),
            down: Mutex::new(DownState::default()),
            poll_generation: AtomicU64::new(0),
            poll_changed: Notify::new(),
            attached: AtomicBool::new(false),
        })
    }
}

pub struct Session {
    pub id: String,
    pub trace_id: String,
    pub profile_name: String,
    pub carrier_mode: String,
    pub backend_addr: SocketAddr,
    backend_connector: Arc<dyn BackendConnector>,
    pub transport_metrics: Arc<crate::metrics::TransportMetrics>,
    pub downlink_tx: FrameSender,
    pub downlink_rx: Arc<Mutex<FrameReceiver>>,
    pub streams: DashMap<u32, Arc<Stream>>,
    pub tombstones: DashMap<u32, Instant>,
    pub created_at: Instant,
    pub last_active: RwLock<Instant>,
    pub closed: AtomicBool,
    pub ended: Notify,
    pub mutation: Mutex<()>,
    budget: Budget,
    max_streams: usize,
    max_lanes: usize,
    stream_queue_items: usize,
    lanes: DashMap<u32, Arc<Lane>>,
    lane_admission: parking_lot::Mutex<()>,
    closed_order: parking_lot::Mutex<std::collections::VecDeque<u32>>,
}
struct SessionDependencies {
    transport_metrics: Arc<crate::metrics::TransportMetrics>,
    backend_connector: Arc<dyn BackendConnector>,
}
impl Session {
    pub fn new(id: String, profile_name: String, backend_addr: SocketAddr) -> Arc<Self> {
        Self::new_with_metrics(
            id,
            profile_name,
            backend_addr,
            "https",
            SessionDependencies {
                transport_metrics: Arc::new(crate::metrics::TransportMetrics::default()),
                backend_connector: Arc::new(TcpBackendConnector),
            },
            &queue::QueueConfig::default(),
            queue::default_shared(),
        )
    }
    fn new_with_metrics(
        id: String,
        profile_name: String,
        backend_addr: SocketAddr,
        carrier_mode: &str,
        dependencies: SessionDependencies,
        queue_config: &queue::QueueConfig,
        shared_budgets: Arc<queue::SharedBudgets>,
    ) -> Arc<Self> {
        let budget = Budget::with_config(queue_config, shared_budgets);
        let (tx, rx) = queue::channel(budget.clone());
        let rx = Arc::new(Mutex::new(rx));
        let lanes = DashMap::new();
        lanes.insert(0, Lane::new(tx.clone(), rx.clone()));
        Arc::new(Self {
            trace_id: generate_secure_token()[..12].to_owned(),
            id,
            profile_name,
            carrier_mode: carrier_mode.to_owned(),
            backend_addr,
            backend_connector: dependencies.backend_connector,
            transport_metrics: dependencies.transport_metrics,
            downlink_tx: tx,
            downlink_rx: rx,
            streams: DashMap::new(),
            tombstones: DashMap::new(),
            created_at: Instant::now(),
            last_active: RwLock::new(Instant::now()),
            closed: AtomicBool::new(false),
            ended: Notify::new(),
            mutation: Mutex::new(()),
            budget,
            max_streams: queue_config.max_streams_per_session,
            max_lanes: queue_config.max_lanes_per_session,
            stream_queue_items: queue_config.stream_queue_items,
            lanes,
            lane_admission: parking_lot::Mutex::new(()),
            closed_order: parking_lot::Mutex::new(std::collections::VecDeque::new()),
        })
    }
    pub fn touch(&self) {
        *self.last_active.write() = Instant::now();
    }
    pub fn is_expired(&self) -> bool {
        self.closed.load(Ordering::Acquire) || self.last_active.read().elapsed() > SESSION_IDLE_TTL
    }
    pub fn has_lane(&self, id: u32) -> bool {
        self.lanes.contains_key(&id)
    }
    pub fn lane(&self, id: u32) -> Result<Arc<Lane>, CoreError> {
        let _guard = self.lane_admission.lock();
        if self.closed.load(Ordering::Acquire) {
            return Err(CoreError::SessionNotFound(String::new()));
        }
        if let Some(lane) = self.lanes.get(&id) {
            return Ok(lane.clone());
        }
        if self.lanes.len() >= self.max_lanes {
            return Err(CoreError::StreamLimitExceeded);
        }
        Ok(self
            .lanes
            .entry(id)
            .or_insert_with(|| {
                let (tx, rx) = queue::channel(self.budget.clone());
                Lane::new(tx, Arc::new(Mutex::new(rx)))
            })
            .clone())
    }
    pub async fn handle_frame(self: &Arc<Self>, frame: Frame) -> Result<(), CoreError> {
        let _guard = self.mutation.lock().await;
        self.validate_batch(std::slice::from_ref(&frame))?;
        self.apply_frame(frame, None).await
    }
    /// Validate the entire batch against a snapshot before performing any I/O.
    /// The transport holds mutation across validation and application.
    pub fn validate_batch(&self, frames: &[Frame]) -> Result<(), CoreError> {
        use std::collections::{HashMap, HashSet};
        let mut live: HashMap<u32, (u32, usize)> = self
            .streams
            .iter()
            .filter(|s| !s.is_closed())
            .map(|s| (*s.key(), (s.receive_credit(), s.queue_capacity())))
            .collect();
        let mut closed: HashSet<u32> = self
            .streams
            .iter()
            .filter(|s| s.is_closed())
            .map(|s| *s.key())
            .collect();
        for frame in frames {
            frame.validate_shape()?;
            let id = frame.stream_id;
            let was_closed = closed.contains(&id) || self.tombstones.contains_key(&id);
            if frame.frame_type == FrameType::Pong && id == 0 {
                continue;
            }
            match frame.frame_type {
                FrameType::Open => {
                    if id == 0
                        || live.contains_key(&id)
                        || was_closed
                        || live.len() >= self.max_streams
                    {
                        return Err(CoreError::StreamLimitExceeded);
                    }
                    live.insert(
                        id,
                        (qdynn_proto::INITIAL_STREAM_WINDOW, self.stream_queue_items),
                    );
                }
                FrameType::Data | FrameType::Window | FrameType::Close => {
                    if was_closed {
                        continue;
                    }
                    let state = live.get_mut(&id).ok_or(CoreError::StreamNotFound(id))?;
                    match frame.frame_type {
                        FrameType::Data => {
                            state.0 = state
                                .0
                                .checked_sub(frame.payload.len() as u32)
                                .ok_or(CoreError::StreamLimitExceeded)?;
                            state.1 = state
                                .1
                                .checked_sub(1)
                                .ok_or(CoreError::StreamLimitExceeded)?;
                        }
                        FrameType::Window => {
                            frame.parse_window_delta()?;
                        }
                        FrameType::Close => {
                            live.remove(&id);
                            closed.insert(id);
                        }
                        _ => unreachable!(),
                    }
                }
                _ => return Err(CoreError::StreamNotFound(id)),
            }
        }
        Ok(())
    }
    /// Called with mutation held by the transport for the whole batch.
    pub async fn apply_frame(
        self: &Arc<Self>,
        frame: Frame,
        lane: Option<u32>,
    ) -> Result<(), CoreError> {
        if self.is_expired() {
            return Err(CoreError::SessionNotFound(String::new()));
        }
        self.touch();
        match frame.frame_type {
            FrameType::Open => {
                let id = frame.stream_id;
                self.streams.retain(|id, stream| {
                    if stream.is_closed() {
                        self.remember_closed(*id);
                        false
                    } else {
                        true
                    }
                });
                if self.tombstones.contains_key(&id) || self.streams.contains_key(&id) {
                    return Ok(());
                }
                if self.streams.len() >= self.max_streams {
                    return Err(CoreError::StreamLimitExceeded);
                }
                let tx = self.lane(lane.unwrap_or(0))?.tx.clone();
                let weak = Arc::downgrade(self);
                let cleanup = Box::new(move |delivered: bool| {
                    tokio::spawn(async move {
                        if let Some(session) = weak.upgrade() {
                            let _guard = session.mutation.lock().await;
                            session.close_stream(id);
                            if !delivered {
                                session.close_all();
                            }
                        }
                    });
                });
                match spawn_stream_bridge_with_cleanup_and_connector(
                    id,
                    self.backend_addr,
                    tx.clone(),
                    self.transport_metrics.clone(),
                    self.backend_connector.clone(),
                    cleanup,
                )
                .instrument(
                    tracing::info_span!("backend", connection = %self.trace_id, stream_id = id),
                )
                .await
                {
                    Ok(stream) => {
                        self.streams.insert(id, stream.clone());
                        if self.closed.load(Ordering::Acquire) {
                            stream.mark_closed();
                        }
                    }
                    Err(_) => {
                        self.remember_closed(id);
                        tx.try_send(Frame::close(id))
                            .map_err(|_| CoreError::StreamLimitExceeded)?;
                    }
                }
            }
            FrameType::Data => {
                let stream = self.streams.get(&frame.stream_id).map(|s| s.clone());
                if let Some(stream) = stream {
                    stream.enqueue(frame.payload).await?;
                }
            }
            FrameType::Window => {
                let delta = frame.parse_window_delta()?;
                if let Some(stream) = self.streams.get(&frame.stream_id) {
                    stream.add_credit(delta);
                }
            }
            FrameType::Close => self.close_stream(frame.stream_id),
            FrameType::Pong => {}
            _ => return Err(CoreError::StreamNotFound(frame.stream_id)),
        }
        Ok(())
    }
    fn remember_closed(&self, id: u32) {
        let mut order = self.closed_order.lock();
        if self.tombstones.contains_key(&id) {
            return;
        }
        if order.len() >= 4096 {
            if let Some(oldest) = order.pop_front() {
                self.tombstones.remove(&oldest);
            }
        }
        order.push_back(id);
        self.tombstones.insert(id, Instant::now());
    }
    pub fn retire_lane(&self, id: u32) {
        if id != 0 {
            self.lanes.remove(&id);
        }
    }
    pub fn close_stream(&self, id: u32) {
        if let Some((_, stream)) = self.streams.remove(&id) {
            stream.mark_closed();
        }
        self.remember_closed(id);
    }
    pub fn close_all(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            tracing::info!(connection = %self.trace_id, "Session closed");
        }
        let _lanes = self.lane_admission.lock();
        self.ended.notify_waiters();
        for stream in self.streams.iter() {
            stream.mark_closed();
        }
        self.streams.clear();
        for lane in self.lanes.iter() {
            lane.poll_changed.notify_waiters();
            if let Ok(mut rx) = lane.rx.try_lock() {
                rx.close();
                while rx.try_recv().is_ok() {}
            }
            if let Ok(mut down) = lane.down.try_lock() {
                down.body = Bytes::new();
                down.charge = None;
            }
        }
        self.lanes.clear();
        if let Ok(mut rx) = self.downlink_rx.try_lock() {
            rx.close();
            while rx.try_recv().is_ok() {}
        }
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.close_all();
    }
}

struct BootstrapInfo {
    profile: String,
    backend: SocketAddr,
    created: Instant,
    session: Option<std::sync::Weak<Session>>,
    client_reported: bool,
}
pub struct SessionRegistry {
    sessions: DashMap<String, Arc<Session>>,
    bootstrap_tokens: DashMap<String, BootstrapInfo>,
    admission: parking_lot::Mutex<()>,
    stopping: AtomicBool,
    carrier_requests: [[AtomicU64; 5]; 5],
    carrier_latency_sum_us: [AtomicU64; 5],
    carrier_latency_buckets: [[AtomicU64; 7]; 5],
    api_rejections: [AtomicU64; 5],
    client_reports: [[AtomicU64; 2]; 6],
    client_ip_sources: [AtomicU64; 5],
    transport_metrics: Arc<crate::metrics::TransportMetrics>,
    queue_config: queue::QueueConfig,
    shared_budgets: Arc<queue::SharedBudgets>,
    backend_connector: Arc<dyn BackendConnector>,
    max_bootstraps: usize,
    max_sessions: usize,
    max_bootstraps_per_profile: usize,
    max_sessions_per_profile: usize,
}
impl SessionRegistry {
    pub fn new() -> Self {
        Self::with_limits(1024, 128)
    }
    pub fn with_limits(max_bootstraps: usize, max_sessions: usize) -> Self {
        Self::with_runtime_config(max_bootstraps, max_sessions, queue::QueueConfig::default())
    }
    pub fn with_runtime_config(
        max_bootstraps: usize,
        max_sessions: usize,
        queue_config: queue::QueueConfig,
    ) -> Self {
        Self::with_profile_limits(
            max_bootstraps,
            max_sessions,
            max_bootstraps,
            max_sessions,
            queue_config,
        )
    }
    pub fn with_profile_limits(
        max_bootstraps: usize,
        max_sessions: usize,
        max_bootstraps_per_profile: usize,
        max_sessions_per_profile: usize,
        queue_config: queue::QueueConfig,
    ) -> Self {
        let shared_budgets = queue::SharedBudgets::new(&queue_config);
        Self {
            sessions: DashMap::new(),
            bootstrap_tokens: DashMap::new(),
            stopping: AtomicBool::new(false),
            admission: parking_lot::Mutex::new(()),
            carrier_requests: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            carrier_latency_sum_us: std::array::from_fn(|_| AtomicU64::new(0)),
            carrier_latency_buckets: std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicU64::new(0))
            }),
            api_rejections: std::array::from_fn(|_| AtomicU64::new(0)),
            client_reports: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            client_ip_sources: std::array::from_fn(|_| AtomicU64::new(0)),
            transport_metrics: Arc::new(crate::metrics::TransportMetrics::default()),
            queue_config,
            shared_budgets,
            backend_connector: Arc::new(TcpBackendConnector),
            max_bootstraps: max_bootstraps.max(1),
            max_sessions: max_sessions.max(1),
            max_bootstraps_per_profile: max_bootstraps_per_profile.max(1),
            max_sessions_per_profile: max_sessions_per_profile.max(1),
        }
    }
    pub fn with_backend_connector(mut self, connector: Arc<dyn BackendConnector>) -> Self {
        self.backend_connector = connector;
        self
    }
    pub fn record_carrier_request(&self, path: &str, status: u16, elapsed: Duration) {
        let endpoint = match path {
            "/api/v1/session" => 0,
            "/api/v1/up" => 1,
            "/api/v1/down" => 2,
            "/api/v1/ws" => 3,
            "/api/v1/recover" => 4,
            _ => return,
        };
        let class = (status / 100).clamp(1, 5) as usize - 1;
        self.carrier_requests[endpoint][class].fetch_add(1, Ordering::Relaxed);
        self.carrier_latency_sum_us[endpoint].fetch_add(
            elapsed.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        const BUCKET_MS: [u64; 7] = [1, 5, 10, 50, 100, 500, 1000];
        for (bucket, bound) in BUCKET_MS.iter().enumerate() {
            if elapsed <= Duration::from_millis(*bound) {
                self.carrier_latency_buckets[endpoint][bucket].fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    pub fn carrier_request_counts(&self) -> [[u64; 5]; 5] {
        std::array::from_fn(|endpoint| {
            std::array::from_fn(|class| {
                self.carrier_requests[endpoint][class].load(Ordering::Relaxed)
            })
        })
    }
    pub fn carrier_latency(&self) -> ([u64; 5], [[u64; 7]; 5]) {
        (
            std::array::from_fn(|endpoint| {
                self.carrier_latency_sum_us[endpoint].load(Ordering::Relaxed)
            }),
            std::array::from_fn(|endpoint| {
                std::array::from_fn(|bucket| {
                    self.carrier_latency_buckets[endpoint][bucket].load(Ordering::Relaxed)
                })
            }),
        )
    }
    pub fn record_api_rejection(&self, reason: &str) {
        let index = match reason {
            "http_admission" => 0,
            "down_poll_admission" => 1,
            "authorization_or_origin" => 2,
            "payload_limit" => 3,
            "input_budget" => 4,
            _ => return,
        };
        self.api_rejections[index].fetch_add(1, Ordering::Relaxed);
    }
    pub fn api_rejection_counts(&self) -> [u64; 5] {
        std::array::from_fn(|index| self.api_rejections[index].load(Ordering::Relaxed))
    }
    pub fn record_client_report(&self, token: &str, phase: &str, reason: &str) -> bool {
        let phase_index = match phase {
            "startup" => 0,
            "runtime" => 1,
            _ => return false,
        };
        let reason_index = match reason {
            "timeout" => 0,
            "session_rejected" => 1,
            "websocket_failed" => 2,
            "body_failed" => 3,
            "protocol_error" => 4,
            "transport_error" => 5,
            _ => return false,
        };
        let Some(mut bootstrap) = self.bootstrap_tokens.get_mut(token) else {
            return false;
        };
        if bootstrap.created.elapsed() > BOOTSTRAP_TTL || bootstrap.client_reported {
            return false;
        }
        bootstrap.client_reported = true;
        self.client_reports[reason_index][phase_index].fetch_add(1, Ordering::Relaxed);
        true
    }
    pub fn client_report_counts(&self) -> [[u64; 2]; 6] {
        std::array::from_fn(|reason| {
            std::array::from_fn(|phase| self.client_reports[reason][phase].load(Ordering::Relaxed))
        })
    }
    pub fn record_client_ip_source(&self, source: &str) {
        let index = match source {
            "direct_peer" => 0,
            "trusted_forwarded" => 1,
            "trusted_proxy_missing" => 2,
            "trusted_proxy_invalid" => 3,
            "no_peer" => 4,
            _ => return,
        };
        self.client_ip_sources[index].fetch_add(1, Ordering::Relaxed);
    }
    pub fn client_ip_source_counts(&self) -> [u64; 5] {
        std::array::from_fn(|i| self.client_ip_sources[i].load(Ordering::Relaxed))
    }
    pub fn transport_metrics(&self) -> Arc<crate::metrics::TransportMetrics> {
        self.transport_metrics.clone()
    }
    pub fn reserve_input(
        &self,
        bytes: usize,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, &'static str> {
        queue::reserve_input_with(self.shared_budgets.clone(), bytes)
    }
    pub fn create_bootstrap_token(&self, profile_name: String, backend_addr: SocketAddr) -> String {
        let _guard = self.admission.lock();
        if self.stopping.load(Ordering::Acquire) {
            return String::new();
        }
        if self.bootstrap_tokens.len() >= self.max_bootstraps
            || self
                .bootstrap_tokens
                .iter()
                .filter(|item| item.profile == profile_name)
                .count()
                >= self.max_bootstraps_per_profile
        {
            self.bootstrap_tokens
                .retain(|_, info| info.created.elapsed() <= BOOTSTRAP_TTL);
        }
        if self.bootstrap_tokens.len() >= self.max_bootstraps
            || self
                .bootstrap_tokens
                .iter()
                .filter(|item| item.profile == profile_name)
                .count()
                >= self.max_bootstraps_per_profile
        {
            return String::new();
        }
        let token = generate_secure_token();
        self.bootstrap_tokens.insert(
            token.clone(),
            BootstrapInfo {
                profile: profile_name,
                backend: backend_addr,
                created: Instant::now(),
                session: None,
                client_reported: false,
            },
        );
        token
    }
    /// Reserves a fresh incarnation before retiring the previous session.
    pub fn create_recovery_bootstrap(
        &self,
        previous_session_id: Option<&str>,
        fallback_profile: &str,
        fallback_backend: SocketAddr,
    ) -> Result<String, CoreError> {
        let _guard = self.admission.lock();
        if self.stopping.load(Ordering::Acquire) {
            return Err(CoreError::StreamLimitExceeded);
        }
        // A restart/expiry can remove the old incarnation. The caller has
        // independently authenticated the capability; missing state is not an
        // authentication failure and does not grant any quota exemption.
        let previous =
            previous_session_id.and_then(|id| self.sessions.get(id).map(|session| session.clone()));
        if previous
            .as_ref()
            .is_some_and(|s| s.profile_name != fallback_profile)
        {
            return Err(CoreError::SessionNotFound(String::new()));
        }
        let (profile, backend) = previous
            .as_ref()
            .map(|session| (session.profile_name.clone(), session.backend_addr))
            .unwrap_or_else(|| (fallback_profile.to_owned(), fallback_backend));

        let replaced_bootstrap = previous.as_ref().and_then(|session| {
            self.bootstrap_tokens.iter().find_map(|entry| {
                let belongs_to_session = entry
                    .session
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade)
                    .is_some_and(|linked| Arc::ptr_eq(&linked, session));
                belongs_to_session.then(|| entry.key().clone())
            })
        });

        if self.bootstrap_tokens.len() >= self.max_bootstraps {
            self.bootstrap_tokens
                .retain(|_, info| info.created.elapsed() <= BOOTSTRAP_TTL);
        }
        let profile_bootstraps = self
            .bootstrap_tokens
            .iter()
            .filter(|item| item.profile == profile)
            .count();
        let replacing = usize::from(replaced_bootstrap.is_some());
        if self.bootstrap_tokens.len().saturating_sub(replacing) >= self.max_bootstraps
            || profile_bootstraps.saturating_sub(replacing) >= self.max_bootstraps_per_profile
        {
            return Err(CoreError::StreamLimitExceeded);
        }

        let token = generate_secure_token();
        self.bootstrap_tokens.insert(
            token.clone(),
            BootstrapInfo {
                profile,
                backend,
                created: Instant::now(),
                session: None,
                client_reported: false,
            },
        );
        if let Some(old_token) = replaced_bootstrap {
            self.bootstrap_tokens.remove(&old_token);
        }
        if let Some(session) = previous {
            self.sessions.remove(&session.id);
            session.close_all();
        }
        Ok(token)
    }
    pub fn has_bootstrap(&self, token: &str) -> bool {
        self.bootstrap_tokens
            .get(token)
            .is_some_and(|info| info.created.elapsed() <= BOOTSTRAP_TTL)
    }
    pub fn exchange_bootstrap_token(&self, token: &str) -> Result<Arc<Session>, CoreError> {
        self.exchange_bootstrap_token_for_carrier(token, "https")
    }
    pub fn exchange_bootstrap_token_for_carrier(
        &self,
        token: &str,
        carrier_mode: &str,
    ) -> Result<Arc<Session>, CoreError> {
        let _guard = self.admission.lock();
        if self.stopping.load(Ordering::Acquire) {
            return Err(CoreError::StreamLimitExceeded);
        }
        if self.sessions.len() >= self.max_sessions {
            self.sessions.retain(|_, s| {
                if s.is_expired() {
                    s.close_all();
                    false
                } else {
                    true
                }
            });
        }
        let mut info = self
            .bootstrap_tokens
            .get_mut(token)
            .ok_or_else(|| CoreError::InvalidBootstrapToken(String::new()))?;
        if info.created.elapsed() > BOOTSTRAP_TTL {
            return Err(CoreError::BootstrapTokenExpired);
        }
        if let Some(weak) = &info.session {
            let session = weak
                .upgrade()
                .ok_or_else(|| CoreError::SessionNotFound(String::new()))?;
            return if session.is_expired() {
                Err(CoreError::SessionNotFound(String::new()))
            } else if session.carrier_mode != carrier_mode {
                Err(CoreError::Proto(qdynn_proto::ProtoError::InvalidFrame(
                    "bootstrap carrier mode changed",
                )))
            } else {
                Ok(session.clone())
            };
        }
        if self
            .sessions
            .iter()
            .filter(|item| item.profile_name == info.profile)
            .count()
            >= self.max_sessions_per_profile
        {
            self.sessions.retain(|_, session| {
                if session.is_expired() {
                    session.close_all();
                    false
                } else {
                    true
                }
            });
        }
        if self.sessions.len() >= self.max_sessions
            || self
                .sessions
                .iter()
                .filter(|item| item.profile_name == info.profile)
                .count()
                >= self.max_sessions_per_profile
        {
            return Err(CoreError::StreamLimitExceeded);
        }
        let session = Session::new_with_metrics(
            generate_secure_token(),
            info.profile.clone(),
            info.backend,
            carrier_mode,
            SessionDependencies {
                transport_metrics: self.transport_metrics.clone(),
                backend_connector: self.backend_connector.clone(),
            },
            &self.queue_config,
            self.shared_budgets.clone(),
        );
        tracing::info!(connection = %session.trace_id, profile = %session.profile_name, carrier = %session.carrier_mode, "Session established");
        self.sessions.insert(session.id.clone(), session.clone());
        info.session = Some(Arc::downgrade(&session));
        Ok(session)
    }
    pub fn get_session(&self, id: &str) -> Option<Arc<Session>> {
        if self.stopping.load(Ordering::Acquire) {
            return None;
        }
        self.sessions
            .get(id)
            .filter(|s| !s.is_expired())
            .map(|s| s.clone())
    }
    pub fn remove_session(&self, id: &str) -> Option<Arc<Session>> {
        self.sessions.remove(id).map(|(_, s)| {
            s.close_all();
            s
        })
    }
    pub fn counts(&self) -> (usize, usize) {
        let sessions = self.sessions.iter().filter(|s| !s.is_expired()).count();
        let streams = self
            .sessions
            .iter()
            .map(|s| s.streams.iter().filter(|v| !v.is_closed()).count())
            .sum();
        (sessions, streams)
    }
    pub fn close_all(&self) {
        let _guard = self.admission.lock();
        self.stopping.store(true, Ordering::Release);
        for s in self.sessions.iter() {
            s.close_all();
        }
        self.sessions.clear();
        self.bootstrap_tokens.clear();
    }
    pub fn cleanup_stale(&self) {
        self.bootstrap_tokens
            .retain(|_, info| info.created.elapsed() <= BOOTSTRAP_TTL);
        self.sessions.retain(|_, s| {
            if s.is_expired() {
                s.close_all();
                false
            } else {
                true
            }
        });
    }
}
impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
