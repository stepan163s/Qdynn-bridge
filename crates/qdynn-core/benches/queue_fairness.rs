use bytes::Bytes;
use qdynn_core::queue::{self, Budget, QueueConfig, SharedBudgets};
use qdynn_proto::Frame;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

const BULK_STREAMS: u32 = 8;
const FRAMES_PER_STREAM: usize = 32;
const PAYLOAD_BYTES: usize = 32 * 1024;
const RUNS: usize = 31;
const REPETITIONS_PER_RUN: usize = 128;
const TARGET_STREAM: u32 = BULK_STREAMS + 1;

struct Sample {
    bytes_before_target: usize,
    elapsed: Duration,
}
struct FifoQueued {
    frame: Frame,
    _local: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
}

fn workload() -> (Vec<Frame>, usize) {
    let payload = Bytes::from(vec![7; PAYLOAD_BYTES]);
    let mut frames = Vec::with_capacity(BULK_STREAMS as usize * FRAMES_PER_STREAM + 1);
    for stream_id in 1..=BULK_STREAMS {
        for _ in 0..FRAMES_PER_STREAM {
            frames.push(Frame::data(stream_id, payload.clone()));
        }
    }
    frames.push(Frame::data(TARGET_STREAM, Bytes::from_static(b"probe")));
    let total_bytes = frames.iter().map(|frame| frame.payload.len() + 8).sum();
    (frames, total_bytes)
}

async fn run_fifo(frames: &[Frame], expected_bytes: usize) -> Sample {
    let (sender, mut receiver) = mpsc::channel(frames.len());
    let local = Arc::new(Semaphore::new(frames.len()));
    let global = Arc::new(Semaphore::new(frames.len()));
    let slots = Arc::new(Semaphore::new(frames.len()));
    for frame in frames {
        sender
            .send(FifoQueued {
                frame: frame.clone(),
                _local: local.clone().try_acquire_owned().unwrap(),
                _global: global.clone().try_acquire_owned().unwrap(),
                _slot: slots.clone().try_acquire_owned().unwrap(),
            })
            .await
            .unwrap();
    }
    drop(sender);
    let started = Instant::now();
    let mut bytes_before_target = 0;
    let mut bytes_processed = 0;
    let mut target_seen = false;
    let mut pending = None;
    loop {
        let Some(first) = (match pending.take() {
            Some(value) => Some(value),
            None => receiver.recv().await,
        }) else {
            break;
        };
        let mut batch_bytes = 0;
        let mut batch = Vec::with_capacity(64);
        batch.push(first);
        while batch.len() < 4096 {
            let item = match receiver.try_recv() {
                Ok(item) => item,
                Err(_) => break,
            };
            let size = item.frame.payload.len() + 8;
            if batch_bytes + size > 2 * 1024 * 1024 {
                pending = Some(item);
                break;
            }
            batch_bytes += size;
            batch.push(item);
        }
        for item in batch {
            let size = item.frame.payload.len() + 8;
            let frame = item.frame;
            if frame.stream_id == TARGET_STREAM {
                target_seen = true;
            } else if !target_seen {
                bytes_before_target += size;
            }
            bytes_processed += size;
        }
    }
    assert_eq!(bytes_processed, expected_bytes);
    Sample {
        bytes_before_target,
        elapsed: started.elapsed(),
    }
}

async fn run_drr(frames: &[Frame], expected_bytes: usize) -> Sample {
    let config = QueueConfig {
        lane_queue_items: 4096,
        lane_data_items: 4096,
        session_data_bytes: 16 * 1024 * 1024,
        global_data_bytes: 16 * 1024 * 1024,
        ..QueueConfig::default()
    };
    let budget = Budget::with_config(&config, SharedBudgets::new(&config));
    let (sender, mut receiver) = queue::channel(budget);
    for frame in frames {
        sender.send(frame.clone()).await.unwrap();
    }
    drop(sender);

    let started = Instant::now();
    let mut bytes_before_target = 0;
    let mut bytes_processed = 0;
    let mut target_seen = false;
    while let Some(batch) = receiver.recv_batch().await {
        for frame in batch {
            let size = frame.payload.len() + 8;
            if frame.stream_id == TARGET_STREAM {
                target_seen = true;
            } else if !target_seen {
                bytes_before_target += size;
            }
            bytes_processed += size;
        }
    }
    assert_eq!(bytes_processed, expected_bytes);
    Sample {
        bytes_before_target,
        elapsed: started.elapsed(),
    }
}

fn percentile(values: &mut [u128], percent: usize) -> u128 {
    values.sort_unstable();
    values[(values.len() - 1) * percent / 100]
}

#[tokio::main]
async fn main() {
    let (frames, expected_bytes) = workload();
    let mut fifo_ns = Vec::with_capacity(RUNS);
    let mut drr_ns = Vec::with_capacity(RUNS);
    let mut fifo_delay = 0;
    let mut drr_delay = 0;
    for _ in 0..RUNS {
        let mut fifo_elapsed = 0u128;
        let mut drr_elapsed = 0u128;
        for _ in 0..REPETITIONS_PER_RUN {
            let fifo = run_fifo(&frames, expected_bytes).await;
            let drr = run_drr(&frames, expected_bytes).await;
            fifo_delay = fifo.bytes_before_target;
            drr_delay = drr.bytes_before_target;
            fifo_elapsed += fifo.elapsed.as_nanos();
            drr_elapsed += drr.elapsed.as_nanos();
        }
        fifo_ns.push(fifo_elapsed / REPETITIONS_PER_RUN as u128);
        drr_ns.push(drr_elapsed / REPETITIONS_PER_RUN as u128);
    }

    let fifo_p50 = percentile(&mut fifo_ns, 50);
    let fifo_p95 = percentile(&mut fifo_ns, 95);
    let drr_p50 = percentile(&mut drr_ns, 50);
    let drr_p95 = percentile(&mut drr_ns, 95);
    let fifo_mbps =
        (expected_bytes as f64 * 8.0) / (fifo_p50 as f64 / 1_000_000_000.0) / 1_000_000.0;
    let drr_mbps = (expected_bytes as f64 * 8.0) / (drr_p50 as f64 / 1_000_000_000.0) / 1_000_000.0;

    println!("workload: bulk_streams={BULK_STREAMS}, frames_per_stream={FRAMES_PER_STREAM}, payload_bytes={PAYLOAD_BYTES}, total_bytes={expected_bytes}, runs={RUNS}, repetitions_per_run={REPETITIONS_PER_RUN}");
    println!(
        "short_stream_bytes_ahead: fifo={fifo_delay}, drr={drr_delay}, reduction={:.1}%",
        (1.0 - drr_delay as f64 / fifo_delay as f64) * 100.0
    );
    println!(
        "modeled_serialization_delay_us: at_100_mbps_fifo={:.1}, drr={:.1}; at_1_gbps_fifo={:.1}, drr={:.1}",
        fifo_delay as f64 * 8.0 / 100.0,
        drr_delay as f64 * 8.0 / 100.0,
        fifo_delay as f64 * 8.0 / 1000.0,
        drr_delay as f64 * 8.0 / 1000.0
    );
    println!("queue_drain_time_ns: fifo_p50={fifo_p50}, fifo_p95={fifo_p95}, drr_p50={drr_p50}, drr_p95={drr_p95}");
    println!("queue_drain_throughput_mbps: fifo_p50={fifo_mbps:.1}, drr_p50={drr_mbps:.1}, relative={:.3}x", drr_mbps / fifo_mbps);
}
