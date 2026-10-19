use crate::error::CoreError;
use bytes::Bytes;
use qdynn_proto::{Frame, INITIAL_STREAM_WINDOW, MAX_RELAY_CHUNK_LEN};
use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, watch, Notify},
    time::timeout,
};
use tracing::Instrument;

pub trait BackendReader: AsyncRead + Unpin + Send {}
impl<T: AsyncRead + Unpin + Send> BackendReader for T {}
pub trait BackendWriter: AsyncWrite + Unpin + Send {}
impl<T: AsyncWrite + Unpin + Send> BackendWriter for T {}

pub struct BackendParts {
    pub reader: Box<dyn BackendReader>,
    pub writer: Box<dyn BackendWriter>,
}

pub trait BackendConnector: Send + Sync {
    fn connect<'a>(
        &'a self,
        address: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<BackendParts>> + Send + 'a>>;
}

#[derive(Default)]
pub struct TcpBackendConnector;
impl BackendConnector for TcpBackendConnector {
    fn connect<'a>(
        &'a self,
        address: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<BackendParts>> + Send + 'a>> {
        Box::pin(async move {
            let stream = TcpStream::connect(address).await?;
            stream.set_nodelay(true)?;
            let (reader, writer) = stream.into_split();
            Ok(BackendParts {
                reader: Box::new(reader),
                writer: Box::new(writer),
            })
        })
    }
}

struct UplinkChunk {
    data: Bytes,
    _charge: crate::queue::Charge,
}

pub struct Stream {
    pub stream_id: u32,
    uplink_tx: mpsc::Sender<UplinkChunk>,
    pub send_credit: AtomicU32,
    receive_credit: AtomicU32,
    pub closed: AtomicBool,
    cancel: watch::Sender<bool>,
    credit_changed: Notify,
    budget: crate::queue::Budget,
}

impl Stream {
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
        self.cancel.send_replace(true);
    }
    pub fn add_credit(&self, delta: u32) {
        let _ = self
            .send_credit
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                Some(v.saturating_add(delta))
            });
        self.credit_changed.notify_one();
    }
    pub fn receive_credit(&self) -> u32 {
        self.receive_credit.load(Ordering::Acquire)
    }
    pub fn queue_capacity(&self) -> usize {
        self.uplink_tx.capacity()
    }
    pub async fn enqueue(&self, data: Bytes) -> Result<(), CoreError> {
        if self.is_closed() {
            return Ok(());
        }
        self.receive_credit
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                v.checked_sub(data.len() as u32)
            })
            .map_err(|_| CoreError::StreamLimitExceeded)?;
        // Credit bounds queued bytes; try_send also bounds the number of tiny frames.
        self.uplink_tx
            .try_send(UplinkChunk {
                _charge: self
                    .budget
                    .charge(data.len())
                    .map_err(|_| CoreError::StreamLimitExceeded)?,
                data,
            })
            .map_err(|_| CoreError::StreamLimitExceeded)
    }
}

/// One supervisor owns both socket halves. Cancelling any direction drops the
/// other future, including blocked reads, writes, credit waits and channel sends.
pub async fn spawn_stream_bridge(
    stream_id: u32,
    backend_addr: SocketAddr,
    downlink_tx: crate::queue::FrameSender,
) -> Result<Arc<Stream>, CoreError> {
    spawn_stream_bridge_with_cleanup(
        stream_id,
        backend_addr,
        downlink_tx,
        Arc::new(crate::metrics::TransportMetrics::default()),
        Box::new(|_| {}),
    )
    .await
}
pub async fn spawn_stream_bridge_with_cleanup(
    stream_id: u32,
    backend_addr: SocketAddr,
    downlink_tx: crate::queue::FrameSender,
    transport_metrics: Arc<crate::metrics::TransportMetrics>,
    on_close: Box<dyn FnOnce(bool) + Send>,
) -> Result<Arc<Stream>, CoreError> {
    spawn_stream_bridge_with_cleanup_and_connector(
        stream_id,
        backend_addr,
        downlink_tx,
        transport_metrics,
        Arc::new(TcpBackendConnector),
        on_close,
    )
    .await
}

pub async fn spawn_stream_bridge_with_cleanup_and_connector(
    stream_id: u32,
    backend_addr: SocketAddr,
    downlink_tx: crate::queue::FrameSender,
    transport_metrics: Arc<crate::metrics::TransportMetrics>,
    backend: Arc<dyn BackendConnector>,
    on_close: Box<dyn FnOnce(bool) + Send>,
) -> Result<Arc<Stream>, CoreError> {
    let budget = downlink_tx.budget();
    let permit = budget
        .try_backend_permit()
        .map_err(|_| CoreError::StreamLimitExceeded)?;
    let (uplink_tx, mut uplink_rx) = mpsc::channel::<UplinkChunk>(budget.stream_queue_items());
    let (cancel, mut cancelled) = watch::channel(false);
    let stream = Arc::new(Stream {
        stream_id,
        uplink_tx,
        cancel,
        budget,
        send_credit: AtomicU32::new(INITIAL_STREAM_WINDOW),
        receive_credit: AtomicU32::new(INITIAL_STREAM_WINDOW),
        closed: AtomicBool::new(false),
        credit_changed: Notify::new(),
    });
    let owner = stream.clone();
    tokio::spawn(async move {
        let relay = async {
            let started = std::time::Instant::now();
            let parts = match timeout(Duration::from_secs(5), backend.connect(backend_addr)).await {
                Ok(Ok(parts)) => {
                    transport_metrics.record_backend_dial(started.elapsed(), true);
                    parts
                }
                Ok(Err(error)) => {
                    transport_metrics.record_backend_dial(started.elapsed(), false);
                    return Err(error);
                }
                Err(_) => {
                    transport_metrics.record_backend_dial(started.elapsed(), false);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "backend connect timeout",
                    ));
                }
            };
            tracing::debug!(
                stream_id,
                elapsed_ms = started.elapsed().as_millis(),
                "Backend connected"
            );
            let (mut reader, mut writer) = (parts.reader, parts.writer);
            let first_byte_recorded = std::sync::atomic::AtomicBool::new(false);
            let read = async {
                let mut buf = vec![0; MAX_RELAY_CHUNK_LEN];
                loop {
                    let changed = owner.credit_changed.notified();
                    let credit = owner.send_credit.load(Ordering::Acquire) as usize;
                    if credit == 0 {
                        changed.await;
                        continue;
                    }
                    let limit = credit.min(buf.len());
                    let n = reader.read(&mut buf[..limit]).await?;
                    if n == 0 {
                        return Ok::<(), std::io::Error>(());
                    }
                    if !first_byte_recorded.swap(true, Ordering::AcqRel) {
                        transport_metrics.record_first_backend_byte(started.elapsed());
                    }
                    owner.send_credit.fetch_sub(n as u32, Ordering::AcqRel);
                    if downlink_tx
                        .send(Frame::data(stream_id, Bytes::copy_from_slice(&buf[..n])))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            };
            let write = async {
                while let Some(chunk) = uplink_rx.recv().await {
                    timeout(Duration::from_secs(30), writer.write_all(&chunk.data))
                        .await
                        .map_err(|_| {
                            std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "backend write timeout",
                            )
                        })??;
                    let n = chunk.data.len() as u32;
                    owner.receive_credit.fetch_add(n, Ordering::AcqRel);
                    if downlink_tx.send(Frame::window(stream_id, n)).await.is_err() {
                        break;
                    }
                }
                Ok::<(), std::io::Error>(())
            };
            tokio::select! { result = read => result, result = write => result }
        };
        tokio::select! {
            result = relay => { if let Err(error) = result { tracing::warn!(stream_id, %error, "Backend relay failed"); } },
            _ = cancelled.changed() => {},
        }
        // All I/O futures (and both TCP halves) have been dropped here.
        let locally_closed = owner.is_closed();
        owner.mark_closed();
        let delivered = downlink_tx.try_send(Frame::close(stream_id)).is_ok();
        drop(permit);
        on_close(delivered || locally_closed);
    }.instrument(tracing::Span::current()));
    Ok(stream)
}
