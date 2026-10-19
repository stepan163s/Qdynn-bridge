use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const BUCKET_MS: [u64; 7] = [1, 5, 10, 50, 100, 500, 1000];

#[derive(Default)]
pub struct TransportMetrics {
    backend_dials: AtomicU64,
    backend_dial_failures: AtomicU64,
    backend_dial_sum_us: AtomicU64,
    backend_dial_buckets: [AtomicU64; 7],
    first_backend_bytes: AtomicU64,
    first_backend_byte_sum_us: AtomicU64,
    first_backend_byte_buckets: [AtomicU64; 7],
    recoveries: [AtomicU64; 3],
    recovery_sum_us: AtomicU64,
    recovery_buckets: [AtomicU64; 7],
}

#[derive(Clone, Copy)]
pub enum RecoveryOutcome {
    Success = 0,
    Rejected = 1,
    Capacity = 2,
}

impl TransportMetrics {
    pub fn record_backend_dial(&self, elapsed: Duration, success: bool) {
        self.backend_dials.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.backend_dial_failures.fetch_add(1, Ordering::Relaxed);
        }
        self.backend_dial_sum_us.fetch_add(
            elapsed.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        record_buckets(&self.backend_dial_buckets, elapsed);
    }

    pub fn record_first_backend_byte(&self, elapsed: Duration) {
        self.first_backend_bytes.fetch_add(1, Ordering::Relaxed);
        self.first_backend_byte_sum_us.fetch_add(
            elapsed.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        record_buckets(&self.first_backend_byte_buckets, elapsed);
    }

    pub fn record_recovery(&self, elapsed: Duration, outcome: RecoveryOutcome) {
        self.recoveries[outcome as usize].fetch_add(1, Ordering::Relaxed);
        self.recovery_sum_us.fetch_add(
            elapsed.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        record_buckets(&self.recovery_buckets, elapsed);
    }

    pub fn snapshot(&self) -> TransportMetricsSnapshot {
        TransportMetricsSnapshot {
            backend_dials: self.backend_dials.load(Ordering::Relaxed),
            backend_dial_failures: self.backend_dial_failures.load(Ordering::Relaxed),
            backend_dial_sum_us: self.backend_dial_sum_us.load(Ordering::Relaxed),
            backend_dial_buckets: std::array::from_fn(|i| {
                self.backend_dial_buckets[i].load(Ordering::Relaxed)
            }),
            first_backend_bytes: self.first_backend_bytes.load(Ordering::Relaxed),
            first_backend_byte_sum_us: self.first_backend_byte_sum_us.load(Ordering::Relaxed),
            first_backend_byte_buckets: std::array::from_fn(|i| {
                self.first_backend_byte_buckets[i].load(Ordering::Relaxed)
            }),
            recoveries: std::array::from_fn(|i| self.recoveries[i].load(Ordering::Relaxed)),
            recovery_sum_us: self.recovery_sum_us.load(Ordering::Relaxed),
            recovery_buckets: std::array::from_fn(|i| {
                self.recovery_buckets[i].load(Ordering::Relaxed)
            }),
        }
    }
}

fn record_buckets(buckets: &[AtomicU64; 7], elapsed: Duration) {
    for (i, bound_ms) in BUCKET_MS.iter().enumerate() {
        if elapsed <= Duration::from_millis(*bound_ms) {
            buckets[i].fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub struct TransportMetricsSnapshot {
    pub backend_dials: u64,
    pub backend_dial_failures: u64,
    pub backend_dial_sum_us: u64,
    pub backend_dial_buckets: [u64; 7],
    pub first_backend_bytes: u64,
    pub first_backend_byte_sum_us: u64,
    pub first_backend_byte_buckets: [u64; 7],
    pub recoveries: [u64; 3],
    pub recovery_sum_us: u64,
    pub recovery_buckets: [u64; 7],
}

#[cfg(test)]
mod tests {
    use super::TransportMetrics;
    use std::time::Duration;

    #[test]
    fn records_bounded_backend_phase_histograms() {
        let metrics = TransportMetrics::default();
        metrics.record_backend_dial(Duration::from_millis(7), true);
        metrics.record_backend_dial(Duration::from_millis(1500), false);
        metrics.record_first_backend_byte(Duration::from_millis(12));
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.backend_dials, 2);
        assert_eq!(snapshot.backend_dial_failures, 1);
        assert_eq!(snapshot.backend_dial_buckets[2], 1);
        assert_eq!(snapshot.backend_dial_buckets[6], 1);
        assert_eq!(snapshot.first_backend_bytes, 1);
        assert_eq!(snapshot.first_backend_byte_buckets[3], 1);
    }

    #[test]
    fn records_recovery_outcomes_and_bounded_latency() {
        use super::RecoveryOutcome;
        let metrics = TransportMetrics::default();
        metrics.record_recovery(Duration::from_millis(7), RecoveryOutcome::Success);
        metrics.record_recovery(Duration::from_millis(1500), RecoveryOutcome::Capacity);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.recoveries, [1, 0, 1]);
        assert_eq!(snapshot.recovery_sum_us, 1_507_000);
        assert_eq!(snapshot.recovery_buckets[2], 1);
        assert_eq!(snapshot.recovery_buckets[6], 1);
    }

    #[test]
    fn concurrent_metric_updates_preserve_counts_and_histogram_totals() {
        use super::RecoveryOutcome;
        use std::sync::Arc;

        const WORKERS: usize = 8;
        const ITERATIONS: usize = 4_998;
        const EVENTS: u64 = (WORKERS * ITERATIONS) as u64;
        let metrics = Arc::new(TransportMetrics::default());
        let workers: Vec<_> = (0..WORKERS)
            .map(|worker| {
                let metrics = Arc::clone(&metrics);
                std::thread::spawn(move || {
                    for iteration in 0..ITERATIONS {
                        metrics.record_backend_dial(Duration::from_millis(7), iteration % 2 == 0);
                        metrics.record_first_backend_byte(Duration::from_millis(7));
                        let outcome = match (worker + iteration) % 3 {
                            0 => RecoveryOutcome::Success,
                            1 => RecoveryOutcome::Rejected,
                            _ => RecoveryOutcome::Capacity,
                        };
                        metrics.record_recovery(Duration::from_millis(7), outcome);
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.backend_dials, EVENTS);
        assert_eq!(snapshot.backend_dial_failures, EVENTS / 2);
        assert_eq!(snapshot.backend_dial_sum_us, EVENTS * 7_000);
        assert_eq!(snapshot.backend_dial_buckets[1], 0);
        assert_eq!(snapshot.backend_dial_buckets[2], EVENTS);
        assert_eq!(snapshot.backend_dial_buckets[3], EVENTS);
        assert_eq!(snapshot.first_backend_bytes, EVENTS);
        assert_eq!(snapshot.first_backend_byte_sum_us, EVENTS * 7_000);
        assert_eq!(snapshot.first_backend_byte_buckets[3], EVENTS);
        assert_eq!(snapshot.recoveries.iter().sum::<u64>(), EVENTS);
        assert!(snapshot.recoveries.iter().all(|count| *count == EVENTS / 3));
        assert_eq!(snapshot.recovery_sum_us, EVENTS * 7_000);
        assert_eq!(snapshot.recovery_buckets[3], EVENTS);
    }
}
