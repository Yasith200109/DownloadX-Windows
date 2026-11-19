//! DownloadX's deterministic adaptive connection policy.
//!
//! This module is intentionally separate from the live download supervisor for the first
//! rollout. Callers can feed bounded-window measurements into it; the existing planner,
//! retry logic, host limits and disk backpressure remain authoritative until integration and
//! benchmark work is complete.

/// User-selected policy for adaptive HTTP connection counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SpeedMode {
    /// Gradually find useful parallelism while reacting promptly to congestion.
    #[default]
    Auto,
    /// Ramp up more quickly when the server and network remain healthy.
    Maximum,
    /// Prefer fewer workers and avoid frequent connection-count changes.
    Stable,
}

/// Measurements from one completed observation window.
///
/// Values are deliberately numeric and contain no URL, headers, cookies or other credentials.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpeedObservation {
    /// Current number of active connections.
    pub active_connections: u32,
    /// User-configured maximum connection count.
    pub configured_max: u32,
    /// A learned per-host ceiling, if one is known.
    pub host_limit: Option<u32>,
    /// Whether the server has confirmed byte-range support.
    pub ranges_supported: bool,
    /// Aggregate bytes received per second in this window.
    pub throughput_bps: u64,
    /// Median response-header latency (time to first byte, TTFB) in milliseconds for this window.
    pub rtt_ms: u64,
    /// Number of failed or retried requests in this window.
    pub request_errors: u32,
    /// Whether the server returned a throttling response (for example 429 or 503).
    pub throttled: bool,
    /// Whether writes are currently being held up by disk backpressure.
    pub disk_backpressure: bool,
    /// Whether a per-download or global speed limiter is active.
    pub limiter_active: bool,
}

/// Stateful, deterministic controller for connection-count decisions.
///
/// observe should be called on a regular, bounded measurement cadence, not on every UI
/// refresh. It returns a target count; it does not open or close connections itself.
#[derive(Clone, Debug)]
pub struct SpeedController {
    mode: SpeedMode,
    healthy_windows: u8,
    baseline_rtt_ms: Option<u64>,
    previous_throughput_bps: Option<u64>,
}

impl SpeedController {
    /// Create a controller for the selected mode.
    #[must_use]
    pub const fn new(mode: SpeedMode) -> Self {
        Self {
            mode,
            healthy_windows: 0,
            baseline_rtt_ms: None,
            previous_throughput_bps: None,
        }
    }

    /// Change mode and reset its ramp-up history.
    pub fn set_mode(&mut self, mode: SpeedMode) {
        if self.mode != mode {
            self.mode = mode;
            self.healthy_windows = 0;
            self.baseline_rtt_ms = None;
            self.previous_throughput_bps = None;
        }
    }

    /// Return the next target connection count from one observation window.
    ///
    /// A server without confirmed range support is always kept at one connection. A known
    /// host limit and the user's configured ceiling are hard upper bounds in every mode.
    pub fn observe(&mut self, sample: SpeedObservation) -> u32 {
        let configured_max = sample.configured_max.clamp(1, 128);
        let ceiling = sample
            .host_limit
            .filter(|limit| *limit > 0)
            .map_or(configured_max, |limit| configured_max.min(limit))
            .max(1);
        let current = sample.active_connections.clamp(1, ceiling);

        if !sample.ranges_supported {
            self.reset_window_history();
            return 1;
        }

        let disk_or_limiter_limited = sample.disk_backpressure || sample.limiter_active;
        let rtt_congested = self
            .baseline_rtt_ms
            .is_some_and(|baseline| baseline > 0 && sample.rtt_ms > baseline.saturating_mul(3) / 2);
        let throughput_falling = self.previous_throughput_bps.is_some_and(|previous| {
            previous > 0 && sample.throughput_bps.saturating_mul(100) < previous.saturating_mul(85)
        });
        let congested = sample.throttled
            || sample.request_errors >= 2
            || (!disk_or_limiter_limited && rtt_congested && throughput_falling);

        if congested {
            self.healthy_windows = 0;
            self.previous_throughput_bps = Some(sample.throughput_bps);
            return match self.mode {
                SpeedMode::Stable => current.saturating_sub(1).max(1),
                SpeedMode::Auto | SpeedMode::Maximum => current.div_ceil(2).max(1),
            };
        }

        let healthy = sample.request_errors == 0
            && !sample.throttled
            && sample.throughput_bps > 0
            && !disk_or_limiter_limited;

        if healthy {
            self.healthy_windows = self.healthy_windows.saturating_add(1);
            // A zero RTT means the caller has no usable latency sample for this window.
            // Seeding the baseline with zero would make the first real RTT look congested.
            if sample.rtt_ms > 0 {
                self.baseline_rtt_ms = Some(match self.baseline_rtt_ms {
                    Some(baseline) => baseline.saturating_mul(7).saturating_add(sample.rtt_ms) / 8,
                    None => sample.rtt_ms,
                });
            }
        } else {
            self.healthy_windows = 0;
        }

        let previous_throughput = self.previous_throughput_bps;
        self.previous_throughput_bps = Some(sample.throughput_bps);

        if current >= ceiling || !healthy {
            return current;
        }

        let ramp_interval = match self.mode {
            SpeedMode::Maximum => 2,
            SpeedMode::Auto => 3,
            SpeedMode::Stable => 6,
        };
        if self.healthy_windows >= ramp_interval {
            let throughput_improved =
                previous_throughput.is_none_or(|previous| sample.throughput_bps >= previous);
            if self.mode != SpeedMode::Stable || throughput_improved {
                self.healthy_windows = 0;
                return current.saturating_add(1).min(ceiling);
            }
        }

        current
    }

    fn reset_window_history(&mut self) {
        self.healthy_windows = 0;
        self.baseline_rtt_ms = None;
        self.previous_throughput_bps = None;
    }
}

#[cfg(test)]
mod tests {
    use super::{SpeedController, SpeedMode, SpeedObservation};

    fn sample(active_connections: u32) -> SpeedObservation {
        SpeedObservation {
            active_connections,
            configured_max: 16,
            host_limit: None,
            ranges_supported: true,
            throughput_bps: 10_000_000,
            rtt_ms: 40,
            request_errors: 0,
            throttled: false,
            disk_backpressure: false,
            limiter_active: false,
        }
    }

    #[test]
    fn unsupported_ranges_force_single_connection() {
        let mut controller = SpeedController::new(SpeedMode::Maximum);
        let mut observation = sample(8);
        observation.ranges_supported = false;
        assert_eq!(controller.observe(observation), 1);
    }

    #[test]
    fn user_and_host_limits_are_hard_ceilings() {
        let mut controller = SpeedController::new(SpeedMode::Maximum);
        let mut observation = sample(12);
        observation.configured_max = 10;
        observation.host_limit = Some(4);
        assert_eq!(controller.observe(observation), 4);
    }

    #[test]
    fn auto_ramps_up_only_after_three_healthy_windows() {
        let mut controller = SpeedController::new(SpeedMode::Auto);
        assert_eq!(controller.observe(sample(4)), 4);
        assert_eq!(controller.observe(sample(4)), 4);
        assert_eq!(controller.observe(sample(4)), 5);
    }

    #[test]
    fn maximum_ramps_up_faster_than_auto() {
        let mut controller = SpeedController::new(SpeedMode::Maximum);
        assert_eq!(controller.observe(sample(4)), 4);
        assert_eq!(controller.observe(sample(4)), 5);
    }

    #[test]
    fn throttling_reduces_parallelism_quickly() {
        let mut controller = SpeedController::new(SpeedMode::Auto);
        let mut observation = sample(8);
        observation.throttled = true;
        assert_eq!(controller.observe(observation), 4);
    }

    #[test]
    fn disk_backpressure_does_not_trigger_a_congestion_cut() {
        let mut controller = SpeedController::new(SpeedMode::Auto);
        let mut observation = sample(8);
        observation.disk_backpressure = true;
        observation.rtt_ms = 500;
        observation.throughput_bps = 1_000_000;
        assert_eq!(controller.observe(observation), 8);
    }

    #[test]
    fn stable_mode_uses_small_decreases_and_slow_ramp_up() {
        let mut controller = SpeedController::new(SpeedMode::Stable);
        let mut observation = sample(8);
        observation.throttled = true;
        assert_eq!(controller.observe(observation), 7);
        observation = sample(4);
        assert_eq!(controller.observe(observation), 4);
        for _ in 0..4 {
            assert_eq!(controller.observe(sample(4)), 4);
        }
        assert_eq!(controller.observe(sample(4)), 5);
    }

    #[test]
    fn changing_mode_resets_ramp_history() {
        let mut controller = SpeedController::new(SpeedMode::Auto);
        controller.observe(sample(4));
        controller.observe(sample(4));
        controller.set_mode(SpeedMode::Maximum);
        assert_eq!(controller.observe(sample(4)), 4);
    }

    #[test]
    fn adaptive_ramp_never_exceeds_configured_or_host_ceiling() {
        for mode in [SpeedMode::Auto, SpeedMode::Maximum, SpeedMode::Stable] {
            let mut controller = SpeedController::new(mode);
            let mut observation = sample(1);
            observation.configured_max = 7;
            observation.host_limit = Some(5);
            for _ in 0..100 {
                let target = controller.observe(observation);
                assert!((1..=5).contains(&target));
                observation.active_connections = target;
            }
        }
    }

    #[test]
    fn zero_configured_limit_is_safely_clamped_to_one() {
        let mut controller = SpeedController::new(SpeedMode::Maximum);
        let mut observation = sample(1);
        observation.configured_max = 0;
        assert_eq!(controller.observe(observation), 1);
    }

    #[test]
    fn zero_host_limit_is_ignored_as_unknown() {
        let mut controller = SpeedController::new(SpeedMode::Maximum);
        let mut observation = sample(3);
        observation.host_limit = Some(0);
        assert_eq!(controller.observe(observation), 3);
    }

    #[test]
    fn missing_rtt_samples_do_not_poison_the_latency_baseline() {
        let mut controller = SpeedController::new(SpeedMode::Auto);
        let mut missing = sample(8);
        missing.rtt_ms = 0;
        assert_eq!(controller.observe(missing), 8);
        assert_eq!(controller.observe(missing), 8);
        // Throughput can still justify the normal ramp-up without a latency sample.
        assert_eq!(controller.observe(missing), 9);

        // The first real measurement seeds the baseline instead of being compared with zero.
        let mut first_real = sample(9);
        first_real.rtt_ms = 40;
        assert_eq!(controller.observe(first_real), 9);
        let mut normal = sample(9);
        normal.rtt_ms = 45;
        assert_eq!(controller.observe(normal), 9);
    }

    #[test]
    fn rising_rtt_and_falling_throughput_reduce_parallelism() {
        let mut controller = SpeedController::new(SpeedMode::Auto);
        let baseline = sample(8);
        for _ in 0..3 {
            controller.observe(baseline);
        }
        let mut congested = sample(8);
        congested.rtt_ms = 100;
        congested.throughput_bps = 8_000_000;
        assert_eq!(controller.observe(congested), 4);
    }

    #[test]
    fn disk_and_limiter_windows_do_not_count_toward_ramp_up() {
        let mut controller = SpeedController::new(SpeedMode::Auto);
        let mut disk_limited = sample(4);
        disk_limited.disk_backpressure = true;
        for _ in 0..4 {
            assert_eq!(controller.observe(disk_limited), 4);
        }

        let mut limited = sample(4);
        limited.limiter_active = true;
        for _ in 0..4 {
            assert_eq!(controller.observe(limited), 4);
        }

        // Only fresh, unconstrained windows count toward the three-window ramp.
        assert_eq!(controller.observe(sample(4)), 4);
        assert_eq!(controller.observe(sample(4)), 4);
        assert_eq!(controller.observe(sample(4)), 5);
    }

    #[test]
    fn a_host_limit_change_is_applied_on_the_next_observation() {
        let mut controller = SpeedController::new(SpeedMode::Maximum);
        let mut observation = sample(8);
        observation.host_limit = Some(3);
        observation.disk_backpressure = true;
        assert_eq!(controller.observe(observation), 3);

        observation.active_connections = 3;
        observation.host_limit = Some(6);
        observation.disk_backpressure = false;
        // A newly raised host ceiling does not cause an instant jump; Maximum still
        // requires its normal healthy windows before adding another connection.
        assert_eq!(controller.observe(observation), 3);
        assert_eq!(controller.observe(observation), 4);
    }
}
