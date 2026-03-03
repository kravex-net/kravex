// ai
//! 🎛️📡🧮 PidBytesToDocCount — the adaptive feedback loop that turns response bytes into doc counts.
//!
//! 🎬 COLD OPEN — INT. CONTROL THEORY CLASSROOM — 1922
//!
//! Nicolas Minorsky stared at the ship's steering mechanism. "What if," he muttered,
//! "we could correct the rudder based on the error, the accumulated error, AND the
//! rate of change of the error?" His colleagues blinked. "That's three things," one said.
//! "Proportional, Integral, Derivative," Minorsky whispered. "PID."
//! Minorsky would be... confused, but not disappointed.
//!
//! ## What This Does 🧠
//!
//! Generates an output of **number of documents to fetch**, based on **measured size
//! of total documents received** (in bytes). It's cruise control, but for batch sizing:
//!
//! - **Set-point**: desired response size in bytes (the "72°F" we're targeting)
//! - **Measured variable**: actual response size in bytes (what we observe)
//! - **Output**: doc count to request next time (the "throttle position")
//!
//! ## PID Control Theory (the 3am version) 📐
//!
//! ```text
//!   Error = Desired - Measured(EMA-smoothed)
//!
//!   P (Proportional) = Kp × Error          ← "react to current error"
//!   I (Integral)     = Ki × ΣError          ← "remember past errors"
//!   D (Derivative)   = Kd × ΔError          ← "predict future errors"
//!
//!   Adjustment = (P + I + D) / AvgResponseSize
//!   Output = EMA(current_output + clamp(Adjustment, -100, 100))
//!   Output = clamp(Output, min_doc_count, max_doc_count)
//! ```
//!
//! ## Knowledge Graph 🧠
//! - Gains auto-calculated from `desired_response_size_bytes` and `max_doc_count`
//! - EMA smoothing on both measurements (α=0.25) and doc count adjustments (α=0.75)
//! - Anti-windup: integral error accumulation clamped to `[-desired, +desired]`
//! - Doc count adjustment clamped to `[-100, +100]` per step (no wild swings)
//! - Output clamped to `[min_doc_count, max_doc_count]`
//! - Trace-level logging on every measurement cycle for observability
//!
//! ⚠️ By the time the singularity arrives, this PID will have converged. Or not. PID makes no promises.
//! 🦆 The duck has no opinion on control theory. It just quacks at the branch predictor.

use super::Controller;
use tracing::trace;

/// 🎛️ PID Controller: measures response bytes, outputs doc count.
///
/// Everyday examples of PID: cruise control, thermostats, this thing.
/// It calculates an error value between the desired response size (set-point)
/// and the observed response size (measured variable), then adjusts the
/// doc count output to minimize that error over time.
///
/// ## Gains 📐
/// The three gain parameters are auto-calculated from `desired_response_size_bytes`
/// and `max_doc_count`:
/// - **Proportional (Kp)**: `max(max_docs, desired) / min(max_docs, desired)`
///   Immediate response to error. The primary driver of correction.
/// - **Integral (Ki)**: `Kp / 50`
///   Accounts for past errors. Addresses steady-state error (the lingering gap
///   between desired and actual). Set conservatively to avoid oscillation.
/// - **Derivative (Kd)**: `sqrt(Kp × Ki)`
///   Predicts future errors from rate of change. The geometric mean of P and I gains.
///   Dampens oscillation. May introduce noise. Use with care and crossed fingers.
///
/// ## EMA Smoothing 📊
/// Two exponential moving averages prevent the controller from overreacting:
/// - Response size EMA (α=0.25): smooths wild measurement fluctuations
/// - Doc count adjustment EMA (α=0.75): ensures gradual, stable output changes
#[derive(Debug)]
pub(crate) struct PidBytesToDocCount {
    // 🎯 The set-point: "I want responses THIS big."
    // Like setting a thermostat to 72°F — this is our target response size in bytes.
    the_desired_response_size_bytes: f64,

    // 📊 Current output doc count — the number we return from output().
    // This is what the source uses as its batch size hint.
    the_output_doc_count: f64,

    // 📉📈 Floor and ceiling for doc count output.
    // Below min: you're doing grep. Above max: you're doing OOM.
    the_min_doc_count: f64,
    the_max_doc_count: f64,

    // 🔙 Previous error for derivative calculation.
    // "Those who do not remember the past error are condemned to overshoot." — Santayana, if he did PID
    the_previous_error_response_size_bytes: f64,

    // ∫ Integral error accumulation — the running sum of all past errors.
    // Clamped to prevent "integral windup" (where the accumulator grows
    // so large it takes forever to unwind, like my credit card debt in grad school).
    the_integral_error_accumulation: f64,

    // 📐 PID gains — auto-calculated at construction, immutable thereafter.
    // These are the three knobs of PID control. Too high = oscillation.
    // Too low = sluggish response. Just right = the myth we chase.
    the_proportional_gain: f64,
    the_integral_gain: f64,
    the_derivative_gain: f64,

    // 📏 Anti-windup clamps for integral error accumulation.
    // Prevents the integral term from accumulating unbounded error,
    // which would cause the output to swing wildly after a large disturbance.
    the_integral_error_accumulation_min: f64,
    the_integral_error_accumulation_max: f64,

    // 📊 EMA-smoothed average response size — used for error calculation.
    // Raw measurements are noisy. EMA smooths them. Like Instagram filters, but for data.
    the_average_response_size_bytes: f64,

    // 📊 EMA alpha for response size smoothing (0 < α ≤ 1).
    // Lower = more smoothing (slower to react). Higher = less smoothing (faster to react).
    // 0.25 means "I trust 25% of the new measurement and 75% of history." Commitment issues.
    the_exponential_moving_average_alpha: f64,

    // 📊 EMA alpha for doc count adjustment smoothing.
    // 0.75 means "I trust 75% of the new adjustment and 25% of history."
    // More responsive than measurement smoothing because we WANT to adjust quickly.
    the_exponential_moving_doc_count_adjustment_alpha: f64,

    // 📏 Last measurement fed to the controller — for external observability.
    the_last_measured_value: f64,
}

impl PidBytesToDocCount {
    /// 🏗️ Creates a new PID controller with auto-calculated gains.
    ///
    /// The gains are derived from `desired_response_size_bytes` and `max_doc_count`:
    /// - Kp = max(max_docs, desired) / min(max_docs, desired)
    /// - Ki = Kp / 50
    /// - Kd = sqrt(Kp × Ki)
    ///
    /// 💀 Panics: never. But returns nonsensical results if you pass 0 for desired_response_size_bytes.
    /// Don't do that. Please. I'm begging you.
    pub(crate) fn new(
        desired_response_size_bytes: f64,
        initial_doc_count: usize,
        min_doc_count: usize,
        max_doc_count: usize,
    ) -> Self {
        let the_max_doc_count_f64 = max_doc_count as f64;
        let the_min_doc_count_f64 = min_doc_count as f64;

        // 📐 Auto-calculate gains from set-point and bounds.
        // Proportional: ratio of the larger to the smaller of (max_docs, desired_bytes).
        // This scales the correction proportionally to the problem space.
        let the_proportional_gain = f64::max(the_max_doc_count_f64, desired_response_size_bytes)
            / f64::min(the_max_doc_count_f64, desired_response_size_bytes);

        // Integral: P/50 — conservative to avoid oscillation.
        // If Ki is too high, the system oscillates like a pendulum. If too low,
        // steady-state error persists like that one bug nobody assigns.
        let the_integral_gain = the_proportional_gain / 50.0;

        // Derivative: geometric mean of P and I — dampens oscillation.
        // Typically smaller than P, larger than I. The Goldilocks gain.
        let the_derivative_gain = (the_proportional_gain * the_integral_gain).sqrt();

        // 📏 Anti-windup clamps: [-desired, +desired].
        // If the integral accumulator exceeds these bounds, we clamp it.
        // This prevents windup after sustained errors in one direction.
        let the_integral_error_accumulation_min = -desired_response_size_bytes;
        let the_integral_error_accumulation_max = desired_response_size_bytes;

        trace!(
            "🎛️ PID initialized: desired_bytes={}, initial_docs={}, bounds=[{}, {}], \
             gains=(Kp={:.4}, Ki={:.6}, Kd={:.5})",
            desired_response_size_bytes,
            initial_doc_count,
            min_doc_count,
            max_doc_count,
            the_proportional_gain,
            the_integral_gain,
            the_derivative_gain
        );

        Self {
            the_desired_response_size_bytes: desired_response_size_bytes,
            the_output_doc_count: initial_doc_count as f64,
            the_min_doc_count: the_min_doc_count_f64,
            the_max_doc_count: the_max_doc_count_f64,
            the_previous_error_response_size_bytes: 0.0,
            the_integral_error_accumulation: 0.0,
            the_proportional_gain,
            the_integral_gain,
            the_derivative_gain,
            the_integral_error_accumulation_min,
            the_integral_error_accumulation_max,
            the_average_response_size_bytes: desired_response_size_bytes,
            the_exponential_moving_average_alpha: 0.25,
            the_exponential_moving_doc_count_adjustment_alpha: 0.75,
            the_last_measured_value: desired_response_size_bytes,
        }
    }
}

impl Controller for PidBytesToDocCount {
    /// 🎯 Returns the current doc count recommendation, rounded to nearest integer.
    ///
    /// This is the "cruise control speed" — the number of documents the source
    /// should try to fetch in its next `next_page()` call. Updated after each
    /// `measure()` call. Between measurements, it returns the last computed value.
    ///
    /// Always returns at least 1 (you can't fetch zero documents and call it progress).
    #[inline]
    fn output(&self) -> usize {
        // 🧮 Round away from zero
        let the_rounded = self.the_output_doc_count.round() as usize;
        // 🛡️ Safety: ensure we never return 0, even if the PID math goes haywire
        the_rounded.max(1)
    }

    /// 📏 Feed a measured response size (in bytes) into the PID controller.
    ///
    ///
    /// 1. EMA-smooth the measurement
    /// 2. Calculate error = desired - smoothed_measurement
    /// 3. Accumulate integral error (with anti-windup clamping)
    /// 4. Calculate derivative error (rate of change)
    /// 5. Compute PID adjustment = (P + I + D) / avg_response_size
    /// 6. Clamp adjustment to [-100, +100] (no wild swings)
    /// 7. EMA-smooth the doc count adjustment
    /// 8. Clamp final output to [min_doc_count, max_doc_count]
    fn measure(&mut self, measured_response_size_bytes: f64) {
        // 📏 Record the raw measurement for observability
        self.the_last_measured_value = measured_response_size_bytes;

        // ══════════════════════════════════════════════════════════
        //  Step 1: EMA-smooth the measured response size
        //  Reduces impact of sudden spikes/dips in response size.
        //  Formula: EMA = α × current + (1 - α) × previous_ema
        //  With α=0.25: "I'm 25% new data, 75% trust issues with volatility"
        // ══════════════════════════════════════════════════════════
        self.the_average_response_size_bytes = self.the_exponential_moving_average_alpha
            * measured_response_size_bytes
            + (1.0 - self.the_exponential_moving_average_alpha)
                * self.the_average_response_size_bytes;

        // ══════════════════════════════════════════════════════════
        //  Step 2: Calculate error between desired and smoothed actual
        //  Positive error = response too small (need more docs)
        //  Negative error = response too large (need fewer docs)
        // ══════════════════════════════════════════════════════════
        let the_error_response_size_bytes =
            self.the_desired_response_size_bytes - self.the_average_response_size_bytes;

        // ══════════════════════════════════════════════════════════
        //  Step 3: Accumulate integral error (with anti-windup)
        //  The integral term addresses steady-state error — the
        //  persistent gap between where we are and where we want to be.
        //  Clamped to prevent runaway accumulation after sustained error.
        // ══════════════════════════════════════════════════════════
        self.the_integral_error_accumulation += the_error_response_size_bytes;
        self.the_integral_error_accumulation = self.the_integral_error_accumulation.clamp(
            self.the_integral_error_accumulation_min,
            self.the_integral_error_accumulation_max,
        );

        // ══════════════════════════════════════════════════════════
        //  Step 4: Calculate derivative error (rate of change)
        //  How quickly is the error changing? If it's growing fast,
        //  we need to react harder. If it's shrinking, ease off.
        // ══════════════════════════════════════════════════════════
        let the_derivative_error =
            the_error_response_size_bytes - self.the_previous_error_response_size_bytes;
        self.the_previous_error_response_size_bytes = the_error_response_size_bytes;

        // ══════════════════════════════════════════════════════════
        //  Step 5: PID calculation — the three musketeers of control
        //  P: "The error is THIS big right now"
        //  I: "The error has been THIS big over time"
        //  D: "The error is changing THIS fast"
        // ══════════════════════════════════════════════════════════
        let the_proportional_term = self.the_proportional_gain * the_error_response_size_bytes;
        let the_integral_term = self.the_integral_gain * self.the_integral_error_accumulation;
        let the_derivative_term = self.the_derivative_gain * the_derivative_error;
        let the_adjustment = the_proportional_term + the_integral_term + the_derivative_term;

        // ══════════════════════════════════════════════════════════
        //  Step 6: Convert PID adjustment to doc count delta
        //  Divide by avg response size to normalize: "how many docs
        //  worth of adjustment does this byte-level error represent?"
        //  Clamp to [-100, +100] to prevent extreme single-step changes.
        // ══════════════════════════════════════════════════════════
        let the_output_doc_count_adjustment = the_adjustment / self.the_average_response_size_bytes;
        let the_clamped_adjustment = the_output_doc_count_adjustment.clamp(-100.0, 100.0);

        // ══════════════════════════════════════════════════════════
        //  Step 7: EMA-smooth the doc count adjustment
        //  With α=0.75: "I'm 75% new adjustment, 25% previous output"
        //  More responsive than measurement EMA because we WANT to
        //  adjust the doc count relatively quickly.
        //
        //  Formula: weighted = α × (current + adjustment) + (1 - α) × previous_output
        // ══════════════════════════════════════════════════════════
        let the_weighted_output = self.the_exponential_moving_doc_count_adjustment_alpha
            * (self.the_output_doc_count + the_clamped_adjustment)
            + (1.0 - self.the_exponential_moving_doc_count_adjustment_alpha)
                * self.the_output_doc_count.round();

        // ══════════════════════════════════════════════════════════
        //  Step 8: Update and clamp the output doc count
        //  The final answer: how many docs should the next request fetch?
        //  Clamped to [min, max] because RAM is finite and dignity is fragile.
        // ══════════════════════════════════════════════════════════
        self.the_output_doc_count =
            the_weighted_output.clamp(self.the_min_doc_count, self.the_max_doc_count);

        trace!(
            "🎛️ PID cycle: measured={:.0}B, avg={:.0}B, error={:.0}, \
             P={:.2}, I={:.2}, D={:.2}, adj={:.2}, clamped_adj={:.2}, \
             output_docs={:.0} | \"The numbers, Mason, what do they mean?\"",
            measured_response_size_bytes,
            self.the_average_response_size_bytes,
            the_error_response_size_bytes,
            the_proportional_term,
            the_integral_term,
            the_derivative_term,
            the_output_doc_count_adjustment,
            the_clamped_adjustment,
            self.the_output_doc_count,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 🧠 Test constants — reused across tests for consistency.
    // These represent a "typical" PID configuration for doc count control.
    const THE_DESIRED_BYTES: f64 = 5_000_000.0; // 5MB target response
    const THE_INITIAL_DOCS: usize = 1000;
    const THE_MIN_DOCS: usize = 10;
    const THE_MAX_DOCS: usize = 10_000;

    fn make_a_standard_pid() -> PidBytesToDocCount {
        PidBytesToDocCount::new(
            THE_DESIRED_BYTES,
            THE_INITIAL_DOCS,
            THE_MIN_DOCS,
            THE_MAX_DOCS,
        )
    }

    /// 🧪 Verify initial output matches the configured initial_doc_count.
    /// Before any measurements, the PID should return what it was told. Like a new employee.
    #[test]
    fn the_one_where_initial_output_matches_config() {
        let the_pid = make_a_standard_pid();
        assert_eq!(
            the_pid.output(),
            THE_INITIAL_DOCS,
            "Initial output should be the configured starting doc count"
        );
    }

    /// 🧪 Verify PID stays within [min, max] bounds under all conditions.
    /// The clamps are the guardrails. If they fail, we're off the cliff.
    #[test]
    fn the_one_where_pid_respects_boundaries_like_a_gentleman() {
        let mut the_pid = make_a_standard_pid();

        // 📏 Feed absurdly large responses — should drive doc count DOWN toward min
        for _ in 0..100 {
            the_pid.measure(100_000_000.0); // 100MB — ludicrous
        }
        let the_output = the_pid.output();
        assert!(
            the_output >= THE_MIN_DOCS,
            "Output {} fell below min {} — the floor has a hole in it 💀",
            the_output,
            THE_MIN_DOCS
        );

        // 📏 Feed absurdly small responses — should drive doc count UP toward max
        for _ in 0..100 {
            the_pid.measure(1.0); // 1 byte — what even is this
        }
        let the_output = the_pid.output();
        assert!(
            the_output <= THE_MAX_DOCS,
            "Output {} exceeded max {} — the ceiling has been breached 💀",
            the_output,
            THE_MAX_DOCS
        );
    }

    /// 🧪 Verify PID converges when fed the desired response size repeatedly.
    /// If the measurement matches the set-point, the output should stabilize.
    /// Like a thermostat that's actually working — rare in real life, common in tests.
    #[test]
    fn the_one_where_pid_converges_to_steady_state() {
        let mut the_pid = make_a_standard_pid();

        // 📏 Feed exactly the desired response size, repeatedly
        for _ in 0..50 {
            the_pid.measure(THE_DESIRED_BYTES);
        }

        // 🎯 After many iterations at set-point, output should be stable
        let the_output_a = the_pid.output();
        the_pid.measure(THE_DESIRED_BYTES);
        let the_output_b = the_pid.output();

        // 📐 The difference between consecutive outputs should be tiny
        let the_diff = (the_output_a as i64 - the_output_b as i64).unsigned_abs();
        assert!(
            the_diff <= 1,
            "After 50 iterations at set-point, output should be stable. \
             Got {} then {} (diff={}). The PID has commitment issues.",
            the_output_a,
            the_output_b,
            the_diff
        );
    }

    /// 🧪 Verify PID increases doc count when responses are smaller than desired.
    /// Small responses = "we're not getting enough data" = request more docs.
    /// Like asking for a bigger slice when the first one was just a crumb.
    #[test]
    fn the_one_where_small_responses_trigger_more_docs() {
        let mut the_pid = make_a_standard_pid();
        let the_initial_output = the_pid.output();

        // 📏 Feed responses that are half the desired size
        for _ in 0..10 {
            the_pid.measure(THE_DESIRED_BYTES / 2.0);
        }

        let the_adjusted_output = the_pid.output();
        assert!(
            the_adjusted_output > the_initial_output,
            "Half-sized responses should increase doc count. \
             Initial={}, After={} — the PID should want MORE, not less.",
            the_initial_output,
            the_adjusted_output
        );
    }

    /// 🧪 Verify PID decreases doc count when responses are larger than desired.
    /// Large responses = "we're getting too much data" = request fewer docs.
    /// Like the buffet of regret: you took too much, now scale back.
    #[test]
    fn the_one_where_large_responses_trigger_fewer_docs() {
        let mut the_pid = make_a_standard_pid();
        let the_initial_output = the_pid.output();

        // 📏 Feed responses that are double the desired size
        for _ in 0..10 {
            the_pid.measure(THE_DESIRED_BYTES * 2.0);
        }

        let the_adjusted_output = the_pid.output();
        assert!(
            the_adjusted_output < the_initial_output,
            "Double-sized responses should decrease doc count. \
             Initial={}, After={} — the PID should want LESS, not more.",
            the_initial_output,
            the_adjusted_output
        );
    }

    /// 🧪 Verify EMA smoothing prevents wild oscillations.
    /// Feed alternating extreme measurements — output should change gradually,
    /// not ping-pong between extremes. The EMA is the adult in the room.
    #[test]
    fn the_one_where_ema_smooths_the_chaos() {
        let mut the_pid = make_a_standard_pid();
        let mut the_outputs: Vec<usize> = Vec::with_capacity(20);

        // 📏 Alternate between tiny and huge responses
        for i in 0..20 {
            if i % 2 == 0 {
                the_pid.measure(100.0); // tiny
            } else {
                the_pid.measure(THE_DESIRED_BYTES * 10.0); // massive
            }
            the_outputs.push(the_pid.output());
        }

        // 📊 Check that outputs don't oscillate wildly between extremes.
        // With EMA, consecutive outputs should be relatively close.
        for window in the_outputs.windows(2) {
            let the_diff = (window[0] as f64 - window[1] as f64).abs();
            let the_max_expected_swing = THE_MAX_DOCS as f64 * 0.5; // no more than 50% swing
            assert!(
                the_diff < the_max_expected_swing,
                "Consecutive outputs {} and {} differ by {} — EMA should prevent swings > {}. \
                 The smoothing filter called in sick today.",
                window[0],
                window[1],
                the_diff,
                the_max_expected_swing
            );
        }
    }

    /// 🧪 Verify anti-windup: integral accumulation doesn't cause permanent lock-up.
    /// After oversized responses drive output to min, switching to small responses
    /// (below desired) should eventually push output back UP. The PID must recover —
    /// it just takes time because EMA smoothing and clamped adjustments are conservative by design.
    #[test]
    fn the_one_where_anti_windup_prevents_integral_hangover() {
        let mut the_pid = make_a_standard_pid();

        // 📏 Drive output down to minimum with oversized responses
        for _ in 0..50 {
            the_pid.measure(THE_DESIRED_BYTES * 3.0);
        }
        let the_post_oversize_output = the_pid.output();
        assert_eq!(
            the_post_oversize_output, THE_MIN_DOCS,
            "After sustained oversizing, output should hit min floor"
        );

        // 📏 Now feed UNDERSIZED responses — this creates positive error
        // (desired > measured), which should drive doc count back UP.
        // We need many iterations because:
        //   - EMA smoothing (α=0.25) takes ~16 iterations to forget old values
        //   - Doc count adjustment is clamped to [-100, +100] per step
        //   - Doc count EMA (α=0.75) further dampens the recovery
        for _ in 0..100 {
            the_pid.measure(THE_DESIRED_BYTES * 0.5);
        }
        let the_recovered_output = the_pid.output();

        // 🎯 After 100 undersized measurements, output should have recovered above the floor.
        // The anti-windup clamp prevents the integral from locking us at min indefinitely.
        assert!(
            the_recovered_output > the_post_oversize_output,
            "After 100 undersized measurements, output ({}) should be above the oversized floor ({}). \
             The integral is permanently hungover. Anti-windup may be broken.",
            the_recovered_output,
            the_post_oversize_output
        );
    }

    /// 🧪 Verify PID handles edge case: measurement of exactly 0 bytes.
    /// Zero-byte response is pathological but shouldn't crash the controller.
    #[test]
    fn the_one_where_zero_byte_response_doesnt_crash_the_universe() {
        let mut the_pid = make_a_standard_pid();

        // 📏 Zero bytes — pathological, but the PID should survive
        the_pid.measure(0.0);
        let the_output = the_pid.output();

        assert!(
            the_output >= THE_MIN_DOCS,
            "Zero-byte response shouldn't crash — got output {}",
            the_output
        );
        assert!(
            the_output <= THE_MAX_DOCS,
            "Zero-byte response output {} exceeds max {}",
            the_output,
            THE_MAX_DOCS
        );
    }

    /// 🧪 Verify gains are calculated correctly from constructor params.
    #[test]
    fn the_one_where_gains_follow_the_sacred_formulas() {
        let the_pid = PidBytesToDocCount::new(1_000_000.0, 500, 10, 5000);

        // Kp = max(5000, 1_000_000) / min(5000, 1_000_000) = 1_000_000 / 5000 = 200
        let the_expected_kp = 1_000_000.0_f64 / 5000.0;
        assert!(
            (the_pid.the_proportional_gain - the_expected_kp).abs() < f64::EPSILON,
            "Kp should be {} but got {}",
            the_expected_kp,
            the_pid.the_proportional_gain
        );

        // Ki = Kp / 50
        let the_expected_ki = the_expected_kp / 50.0;
        assert!(
            (the_pid.the_integral_gain - the_expected_ki).abs() < f64::EPSILON,
            "Ki should be {} but got {}",
            the_expected_ki,
            the_pid.the_integral_gain
        );

        // Kd = sqrt(Kp * Ki)
        let the_expected_kd = (the_expected_kp * the_expected_ki).sqrt();
        assert!(
            (the_pid.the_derivative_gain - the_expected_kd).abs() < 1e-10,
            "Kd should be {} but got {}",
            the_expected_kd,
            the_pid.the_derivative_gain
        );
    }

    /// 🧪 Verify output never returns 0, even under extreme conditions.
    /// Zero is not a valid batch size. It's an existential crisis.
    #[test]
    fn the_one_where_output_is_always_at_least_one() {
        let mut the_pid = PidBytesToDocCount::new(1.0, 1, 1, 10);

        // 📏 Feed extreme measurements to try to break it
        for _ in 0..100 {
            the_pid.measure(f64::MAX / 2.0);
        }
        assert!(
            the_pid.output() >= 1,
            "Output must never be 0 — minimum viable batch is 1"
        );

        let mut the_pid2 = PidBytesToDocCount::new(1_000_000.0, 1, 1, 10);
        for _ in 0..100 {
            the_pid2.measure(0.001);
        }
        assert!(
            the_pid2.output() >= 1,
            "Output must never be 0, even with tiny measurements"
        );
    }

    /// 🧪 Verify the PID works with equal desired and max_doc_count.
    /// This makes Kp = 1, which is a valid (if boring) configuration.
    #[test]
    fn the_one_where_desired_equals_max_and_everything_is_fine() {
        let the_pid = PidBytesToDocCount::new(1000.0, 500, 10, 1000);
        assert!(
            (the_pid.the_proportional_gain - 1.0).abs() < f64::EPSILON,
            "When desired == max, Kp should be 1.0"
        );
        assert_eq!(the_pid.output(), 500);
    }

    // 🦆 The duck has reviewed these tests and finds them... adequate.
    // The duck has no formal training in control theory.
    // Neither did the developer. But the tests pass. And that's what matters.
    // "If you're reading this, the code review went poorly." — Breaking the 4th wall 🎬
}
