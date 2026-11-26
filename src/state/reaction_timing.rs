//! How long the bot takes to react to an AH flip: from the moment the flip
//! message arrives on a websocket to the moment `/viewauction` is sent.
//!
//! The existing `[Timing]` log lines all start at `/viewauction`, so the part
//! of a buy the bot itself controls (parsing, filtering, queueing, waiting for
//! a running command) was never measured. This keeps the most recent samples
//! and serves a median / p90 / max summary to the web panel and the log.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use serde::Serialize;

/// How many recent buys the summary covers.
const MAX_SAMPLES: usize = 200;

static SAMPLES: Mutex<VecDeque<Duration>> = Mutex::new(VecDeque::new());

/// Summary of recent flip → `/viewauction` reaction times, in milliseconds.
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct ReactionSummary {
    pub samples: usize,
    pub last_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub max_ms: f64,
}

/// Record one reaction time.
pub fn record(elapsed: Duration) {
    if let Ok(mut samples) = SAMPLES.lock() {
        if samples.len() == MAX_SAMPLES {
            samples.pop_front();
        }
        samples.push_back(elapsed);
    }
}

/// Summary of the recorded samples, or `None` before the first buy.
pub fn summary() -> Option<ReactionSummary> {
    let samples = SAMPLES.lock().ok()?;
    summarize(samples.iter().copied())
}

fn summarize(samples: impl Iterator<Item = Duration>) -> Option<ReactionSummary> {
    let mut ordered: Vec<Duration> = samples.collect();
    let last = *ordered.last()?;
    ordered.sort_unstable();
    // Nearest-rank percentile.
    let pct = |p: f64| {
        let rank = ((p * ordered.len() as f64).ceil() as usize).clamp(1, ordered.len());
        ms(ordered[rank - 1])
    };
    Some(ReactionSummary {
        samples: ordered.len(),
        last_ms: ms(last),
        p50_ms: pct(0.50),
        p90_ms: pct(0.90),
        max_ms: ms(ordered[ordered.len() - 1]),
    })
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms_d(v: u64) -> Duration {
        Duration::from_millis(v)
    }

    #[test]
    fn empty_has_no_summary() {
        assert_eq!(summarize(std::iter::empty()), None);
    }

    #[test]
    fn percentiles_use_nearest_rank() {
        // 1..=10 ms, recorded in reverse so "last" is 1 ms.
        let s = summarize((1..=10).rev().map(ms_d)).unwrap();
        assert_eq!(s.samples, 10);
        assert_eq!(s.last_ms, 1.0);
        assert_eq!(s.p50_ms, 5.0);
        assert_eq!(s.p90_ms, 9.0);
        assert_eq!(s.max_ms, 10.0);
    }

    #[test]
    fn single_sample() {
        let s = summarize(std::iter::once(ms_d(7))).unwrap();
        assert_eq!(
            (s.p50_ms, s.p90_ms, s.max_ms, s.last_ms),
            (7.0, 7.0, 7.0, 7.0)
        );
    }
}
