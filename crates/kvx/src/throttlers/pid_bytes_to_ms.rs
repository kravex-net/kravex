// ai
// Copyright (c) 2026 Kravex Contributors. All rights reserved.
// Licensed under the Business Source License 1.1 — see LICENSE-EE in the repository root.
// This file is part of the Enterprise Edition (EE) and is governed by the Business Source License.
// Use of this file in production requires a valid commercial license from Kravex.
// "The secret sauce. The Colonel's 11 herbs and spices. The Krabby Patty formula." 🔒
//
//! 🎬 *[INT. MISSION CONTROL — 3:47 AM]*
//! *[A bulk request screams across the wire. Too large. The server chokes. 429. 429. 429.]*
//! *[The engineer's coffee goes cold. Their eyes are hollow.]*
//!
//! *["If only," they whisper, "there were a control loop mechanism employing feedback..."]*
//!
//! *[DRAMATIC ZOOM on a whiteboard. Three letters. P. I. D.]*
//! *[The crowd goes wild. The server stops 429-ing. The coffee reheats itself.]*
//!
//! 📦 **PidControllerBytesToMs** — a PID controller that outputs recommended byte sizes
//! based on measured request durations. The crown jewel of adaptive throttling. 🧠
//!
//! 🧠 Knowledge graph — PID Control Theory 101:
//! - **PID**: Proportional-Integral-Derivative. Everyday examples: cruise control, thermostats.
//! - **Set point**: the target we're aiming for (e.g., 8000ms = "ideal" bulk request duration).
//!   Think thermostat set to 72°F. We want each bulk request to take ~set_point ms.
//! - **Error**: difference between set point and measured value. Positive = too fast, negative = too slow.
//! - **Proportional (P)**: immediate response to error. "We're off by X, correct by K_p * X."
//!   Primary driver of correction. Fraction of estimated best size to prevent overcorrection.
//! - **Integral (I)**: accumulated past errors. Addresses steady-state error — the lingering
//!   gap between desired and actual that P alone can't fix. Too high → oscillation. 🎢
//! - **Derivative (D)**: rate of change of error. Predicts future errors. Smooths out P's
//!   aggressive corrections. May introduce noise. Typically smaller than P, larger than I.
//!
//! ⚠️ Anti-windup: integral accumulation is clamped to prevent runaway correction
//! when the system is saturated (e.g., during extended 429 storms).
//!
//! 🦆 "He who tunes PID gains at 3am, finds enlightenment at 4am. Or just more coffee."

use super::ThrottleController;

/// 🧠 PID Controller: Bytes to Milliseconds.
///
/// Generates an output of recommended bytes, based on measured time spans.
/// The feedback loop: measure duration → compute error → adjust output → repeat.
///
/// 🧠 Gain derivation rationale
/// - `proportional_gain`: ratio of output to set_point (or vice versa, whichever is larger).
///   This scales correction proportionally to the initial "gap" between our guess and target.
///   A 10MB initial output with an 8s set point gives K_p ≈ 1.25 — gentle corrections.
/// - `integral_gain`: K_p / 10. Slow accumulation. We don't want the integral term to
///   dominate — it should only fix the steady-state bias that P misses.
/// - `derivative_gain`: geometric mean of K_p and K_i, i.e. sqrt(K_p * K_i).
///   Balances between P's aggression and I's patience. Smooths oscillations.
///
/// 📡 Exponential Moving Average (EMA) with α=0.25 smooths noisy measurements.
/// Raw latencies can spike (GC pauses, network hiccups). EMA provides a stable signal
/// to the PID loop, preventing knee-jerk overcorrections. 🔄
///
/// Licensed under LICENSE-EE. This is the Krabby Patty formula of kravex. 🔒
#[derive(Debug, Clone)]
pub(crate) struct PidControllerBytesToMs {
    // 📐 PID output — the recommended byte size for the next request.
    // Stored as f64 for precision during intermediate calculations,
    // rounded to usize on output. Like a Swiss watch, but for bytes.
    cruise_control_output: f64,

    // 🎯 The target duration in milliseconds — our "72°F thermostat setting".
    // All error calculations are relative to this. The North Star. The vibe we're chasing.
    goldilocks_duration_ms: f64,

    // 📏 Output bounds — the guardrails preventing the controller from going full send
    // (or full stop). Even PID needs boundaries. We all do.
    floor_bytes: f64,
    ceiling_bytes: f64,

    // 🧠 PID gains — the three amigos of control theory.
    // Tuned at construction based on the ratio of initial output to set point.
    proportional_gain: f64,
    integral_gain: f64,
    derivative_gain: f64,

    // 📊 Error tracking — the controller's emotional baggage.
    // Previous error: for computing derivative (rate of change).
    // Integral accumulation: for computing integral (historical bias).
    emotional_baggage_previous_error: f64,
    therapy_backlog_integral_accumulation: f64,

    // ⚠️ Anti-windup bounds on integral accumulation.
    // Without these, extended saturation (e.g., server constantly 429-ing) causes
    // the integral term to grow unboundedly, leading to massive overcorrection
    // when the system recovers. Clamped to ±5× set point.
    integral_floor: f64,
    integral_ceiling: f64,

    // 📡 Exponential Moving Average state.
    // Alpha = 0.25 means "75% old average, 25% new measurement".
    // Smooths out latency spikes. The emotional support animal of the feedback loop.
    ema_alpha: f64,
    running_average: f64,

    // 📊 Last raw measurement — stored for observability / debugging.
    last_measured_ms: f64,
}

impl PidControllerBytesToMs {
    /// 🏗️ Construct a new PID controller.
    ///
    /// # Arguments
    /// - `set_point_ms`: Target request duration in milliseconds (the "thermostat setting").
    ///   For Elasticsearch bulk, 8000ms is a reasonable starting point.
    /// - `initial_output_bytes`: Starting byte size estimate. The PID loop will adjust from here.
    ///   Think of it as your first guess at the restaurant: "I'll have... 10MB?"
    /// - `min_bytes`: Floor for output. Even if the server is choking, we won't go below this.
    /// - `max_bytes`: Ceiling for output. Even if the server is blazing fast, we cap here.
    ///
    /// 🧠 Gain auto-tuning rationale:
    /// The proportional gain is derived from the ratio of initial output to set point.
    /// This means if your initial guess is far from the target, corrections are more aggressive.
    /// If your guess is close, corrections are gentler. Self-calibrating. Like a cat. 🐱
    pub(crate) fn new(
        set_point_ms: f64,
        initial_output_bytes: usize,
        min_bytes: usize,
        max_bytes: usize,
    ) -> Self {
        let initial_output = initial_output_bytes as f64;
        let min = min_bytes as f64;
        let max = max_bytes as f64;

        // 🧠 Gain calculation: ratio of the larger to the smaller of output vs set point.
        // This auto-scales the proportional response based on how "far off" our initial guess is.
        // If output >> set_point (or vice versa), K_p is larger → more aggressive initial correction.
        // If they're close, K_p ≈ 1 → gentle nudges. Elegant, really. Like math always is.
        let proportional_gain =
            f64::max(initial_output, set_point_ms) / f64::min(initial_output, set_point_ms);

        // 🧠 Integral gain = K_p / 10. The "slow and steady" correction.
        // Addresses persistent bias without causing oscillation.
        // If this were any smaller, it'd need a magnifying glass.
        let integral_gain = proportional_gain / 10.0;

        // 🧠 Derivative gain = geometric mean of K_p and K_i.
        // sqrt(K_p * K_i) sits neatly between the two — not too aggressive, not too sleepy.
        // Predicts future error from rate-of-change. The fortune teller of the PID world. 🔮
        let derivative_gain = f64::sqrt(proportional_gain * integral_gain);

        // ⚠️ Anti-windup: clamp integral accumulation to ±5× set point.
        // Prevents the integral term from building up a massive "debt" during sustained
        // saturation (e.g., 429 storm). When the storm passes, recovery is proportional,
        // not catastrophic. Like limiting your credit card, but for error accumulation.
        let integral_floor = set_point_ms * -5.0;
        let integral_ceiling = set_point_ms * 5.0;

        // 🧠 Initialize EMA to the set point, not zero.
        // If we start at 0, the first N measurements see a massive positive error
        // (set_point - near_zero = "hey we're super fast, send ALL the bytes!")
        // and the output rockets to ceiling before any real data arrives.
        // Starting at set_point means: "assume we're on target until proven otherwise."
        Self {
            cruise_control_output: initial_output,
            goldilocks_duration_ms: set_point_ms,
            floor_bytes: min,
            ceiling_bytes: max,
            proportional_gain,
            integral_gain,
            derivative_gain,
            emotional_baggage_previous_error: 0.0,
            therapy_backlog_integral_accumulation: 0.0,
            integral_floor,
            integral_ceiling,
            ema_alpha: 0.25,
            running_average: set_point_ms,
            last_measured_ms: 0.0,
        }
    }
}

impl ThrottleController for PidControllerBytesToMs {
    /// 📡 Feed a measured request duration into the PID feedback loop.
    ///
    /// This is where the magic happens. The alchemy. The "I can't believe it works" moment.
    ///
    /// 🧠 Algorithm walkthrough (because future-you will thank present-you):
    /// 1. Update EMA: smooth the raw measurement to dampen noise.
    /// 2. Compute error: `set_point - smoothed_average`. Positive = "too fast" = "send more bytes".
    ///    Negative = "too slow" = "send fewer bytes". The universe in one subtraction.
    /// 3. Accumulate integral error (with anti-windup clamping).
    /// 4. Compute derivative error: rate of change since last measurement.
    /// 5. PID adjustment: `K_p * error + K_i * integral + K_d * derivative`.
    /// 6. Apply adjustment to output. Clamp to [min, max].
    /// 7. Store current error for next derivative calculation.
    ///
    /// ⚠️ Thread safety: currently relies on SinkWorker owning the controller exclusively.
    /// If we ever share controllers across workers, we'll need a Mutex here.
    /// "He who shares mutable state, debugs race conditions in production." — Ancient proverb 🔒
    fn measure(&mut self, duration_ms: f64) {
        self.last_measured_ms = duration_ms;

        // 📡 Step 1: Exponential Moving Average — smooth the signal.
        // α * new_value + (1 - α) * old_average
        // With α=0.25, we're 75% history, 25% new data. Conservative. Cautious. Like me with sushi.
        self.running_average =
            self.ema_alpha * duration_ms + (1.0 - self.ema_alpha) * self.running_average;

        // 🎯 Step 2: Error = set_point - smoothed_measurement.
        // Positive error → request was faster than target → we can afford to send more bytes.
        // Negative error → request was slower than target → back off, send fewer bytes.
        let error = self.goldilocks_duration_ms - self.running_average;

        // 🔄 Step 3: Integral accumulation — the long memory of the controller.
        // Adds up all past errors. Addresses steady-state bias (consistent under/over target).
        // Clamped to prevent windup during sustained saturation events (429 storms, server outages).
        self.therapy_backlog_integral_accumulation += error;
        self.therapy_backlog_integral_accumulation = self
            .therapy_backlog_integral_accumulation
            .clamp(self.integral_floor, self.integral_ceiling);

        // 📈 Step 4: Derivative — rate of change of error.
        // If error is growing → things are getting worse → apply stronger correction.
        // If error is shrinking → things are improving → ease up. Momentum-based correction.
        let derivative_error = error - self.emotional_baggage_previous_error;

        // 🧠 Step 5: The PID equation. Three terms, one destiny.
        //   P = K_p × error            (proportional: "how far off are we?")
        //   I = K_i × integral_sum     (integral: "how long have we been off?")
        //   D = K_d × derivative       (derivative: "how fast is the error changing?")
        let p_term = self.proportional_gain * error;
        let i_term = self.integral_gain * self.therapy_backlog_integral_accumulation;
        let d_term = self.derivative_gain * derivative_error;

        let adjustment = p_term + i_term + d_term;

        // 📏 Step 6: Apply adjustment and clamp to output bounds.
        // The controller has opinions. But the bounds have final say. Like a code review.
        self.cruise_control_output += adjustment;
        self.cruise_control_output = self
            .cruise_control_output
            .clamp(self.floor_bytes, self.ceiling_bytes);

        // 📊 Step 7: Remember this error for next time's derivative calculation.
        self.emotional_baggage_previous_error = error;
    }

    /// 📏 Get the current recommended byte size for the next bulk request.
    ///
    /// Rounded to nearest usize. The PID has spoken. Respect its wisdom. 🎯
    fn output(&self) -> usize {
        self.cruise_control_output.round() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 The one where the PID controller is born and immediately has an existential crisis
    #[test]
    fn the_one_where_pid_is_born_with_reasonable_defaults() {
        // -- 🧪 Birth of a controller. It has gains. It has dreams. It has no measurements yet.
        let controller = PidControllerBytesToMs::new(
            8000.0,      // 🎯 Target: 8 seconds per request
            10_485_760,  // 📦 Initial guess: 10MB
            1_048_576,   // 📏 Floor: 1MB (we won't go smaller than this)
            104_857_600, // 📏 Ceiling: 100MB (we won't go bigger than this, we're not animals)
        );

        assert_eq!(
            controller.output(),
            10_485_760,
            "💀 Initial output should be 10MB. We haven't measured anything yet. Patience."
        );

        // 🧠 Verify gain auto-tuning: K_p = max(10485760, 8000) / min(10485760, 8000)
        let expected_kp = 10_485_760.0_f64 / 8000.0;
        assert!(
            (controller.proportional_gain - expected_kp).abs() < 0.001,
            "💀 Proportional gain should be ratio of output to set point. Math is non-negotiable."
        );
    }

    /// 🧪 The one where requests are too slow and the controller wisely backs off
    #[test]
    fn the_one_where_slow_requests_cause_byte_reduction() {
        let mut controller = PidControllerBytesToMs::new(
            8000.0,      // 🎯 We want 8s
            10_485_760,  // 📦 Starting at 10MB
            1_048_576,   // 📏 Floor: 1MB
            104_857_600, // 📏 Ceiling: 100MB
        );

        // -- 🧪 Server is struggling. Each request takes 16 seconds. Way over our 8s target.
        // -- The PID should reduce byte output because negative error = "slow down chief"
        for _ in 0..20 {
            controller.measure(16000.0);
        }

        assert!(
            controller.output() < 10_485_760,
            "💀 After consistently slow responses (16s vs 8s target), output should decrease. \
             The controller should be saying 'send fewer bytes'. Got: {} bytes",
            controller.output()
        );
    }

    /// 🧪 The one where requests are blazingly fast and the controller says "give me more"
    #[test]
    fn the_one_where_fast_requests_cause_byte_increase() {
        let mut controller = PidControllerBytesToMs::new(
            8000.0,      // 🎯 We want 8s
            10_485_760,  // 📦 Starting at 10MB
            1_048_576,   // 📏 Floor: 1MB
            104_857_600, // 📏 Ceiling: 100MB
        );

        // -- 🧪 Server is zooming. Each request completes in 2 seconds. Way under 8s target.
        // -- The PID should increase byte output because positive error = "send more bytes fam"
        for _ in 0..20 {
            controller.measure(2000.0);
        }

        assert!(
            controller.output() > 10_485_760,
            "💀 After consistently fast responses (2s vs 8s target), output should increase. \
             The controller should be saying 'send more bytes'. Got: {} bytes",
            controller.output()
        );
    }

    /// 🧪 The one where the controller respects the floor like a gentleman
    #[test]
    fn the_one_where_output_never_goes_below_floor() {
        let mut controller = PidControllerBytesToMs::new(
            8000.0,     // 🎯 Target: 8s
            10_485_760, // 📦 Start: 10MB
            1_048_576,  // 📏 Floor: 1MB — the line in the sand
            104_857_600,
        );

        // -- 🧪 Simulate catastrophically slow responses — 60 seconds each. The server
        // -- is basically a fax machine at this point 📠
        for _ in 0..100 {
            controller.measure(60000.0);
        }

        assert!(
            controller.output() >= 1_048_576,
            "💀 Output breached the floor! The controller went subterranean. Got: {} bytes",
            controller.output()
        );
    }

    /// 🧪 The one where the controller respects the ceiling like it pays rent
    #[test]
    fn the_one_where_output_never_exceeds_ceiling() {
        let mut controller = PidControllerBytesToMs::new(
            8000.0,      // 🎯 Target: 8s
            10_485_760,  // 📦 Start: 10MB
            1_048_576,   // 📏 Floor: 1MB
            104_857_600, // 📏 Ceiling: 100MB — the glass ceiling (but for bytes)
        );

        // -- 🧪 Server is a rocket ship — everything completes in 100ms.
        // -- Even so, we must not exceed 100MB. There are rules. There are limits. There is 100MB.
        for _ in 0..100 {
            controller.measure(100.0);
        }

        assert!(
            controller.output() <= 104_857_600,
            "💀 Output breached the ceiling! The controller went to space without permission. Got: {} bytes",
            controller.output()
        );
    }

    /// 🧪 The one where the PID stabilizes around the set point like a zen master
    #[test]
    fn the_one_where_pid_converges_toward_stability() {
        let mut controller = PidControllerBytesToMs::new(
            8000.0,      // 🎯 Target: 8s
            10_485_760,  // 📦 Start: 10MB
            1_048_576,   // 📏 Floor: 1MB
            104_857_600, // 📏 Ceiling: 100MB
        );

        // -- 🧪 Feed it exactly the set point, repeatedly.
        // -- A well-tuned PID should barely move. Like a cat in a sunbeam. 🐱
        let initial = controller.output();
        for _ in 0..50 {
            controller.measure(8000.0);
        }

        // When measured == set_point, error is ~0 after EMA converges.
        // Output should not have drifted far from initial.
        let final_output = controller.output();
        let drift = (final_output as f64 - initial as f64).abs();
        let drift_percent = drift / initial as f64 * 100.0;

        assert!(
            drift_percent < 15.0,
            "💀 PID drifted {:.1}% from initial when fed exactly the set point. \
             It should be stable, not having an identity crisis. Initial: {}, Final: {}",
            drift_percent,
            initial,
            final_output
        );
    }

    /// 🧪 The one where the EMA smooths oscillations and output variance decreases over time
    #[test]
    fn the_one_where_ema_smooths_noisy_measurements() {
        let mut controller =
            PidControllerBytesToMs::new(8000.0, 10_485_760, 1_048_576, 104_857_600);

        // -- 🧪 Alternate between very fast and very slow.
        // -- EMA smoothing creates an asymmetric lag: the "fast" half (2000ms) briefly pulls EMA
        // -- below set_point → positive error → increase output. The "slow" half (14000ms) pulls
        // -- EMA above set_point → negative error → decrease output. But the EMA's lag means
        // -- the magnitudes aren't symmetric — K_p is large (10MB/8s = 1310), so even small
        // -- net bias accumulates. This is correct PID behavior for an oscillating signal!
        // --
        // -- What we're really testing: output variance stabilizes over time as EMA converges
        // -- to the true mean. The output should NOT be stuck at the floor or ceiling.
        let mut outputs = Vec::new();
        for i in 0..40 {
            if i % 2 == 0 {
                controller.measure(2000.0); // 🚀 Speedy
            } else {
                controller.measure(14000.0); // 🐌 Sluggish
            }
            outputs.push(controller.output());
        }

        // 🧠 Verify the output stabilizes — variance in the last 10 should be less than first 10.
        // This proves the EMA is converging and the PID corrections are dampening over time.
        let first_10: Vec<f64> = outputs[..10].iter().map(|&o| o as f64).collect();
        let last_10: Vec<f64> = outputs[30..].iter().map(|&o| o as f64).collect();

        let variance = |slice: &[f64]| -> f64 {
            let mean = slice.iter().sum::<f64>() / slice.len() as f64;
            slice.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / slice.len() as f64
        };

        let first_variance = variance(&first_10);
        let last_variance = variance(&last_10);

        assert!(
            last_variance <= first_variance,
            "💀 Output variance should decrease over time as EMA converges. \
             First 10 variance: {:.0}, Last 10 variance: {:.0}. \
             The controller is getting MORE erratic, not less. Send help.",
            first_variance,
            last_variance
        );

        // 🎯 Also verify output stays within bounds — not stuck at floor or ceiling
        let final_output = controller.output();
        assert!(
            final_output > 1_048_576 && final_output < 104_857_600,
            "💀 Output should be between floor and ceiling, not pinned to an extreme. Got: {}",
            final_output
        );
    }

    /// 🧪 The one where anti-windup prevents integral catastrophe during a 429 storm
    #[test]
    fn the_one_where_anti_windup_prevents_integral_catastrophe() {
        let mut controller = PidControllerBytesToMs::new(
            8000.0,      // 🎯 8s target
            10_485_760,  // 📦 10MB start
            1_048_576,   // 📏 1MB floor
            104_857_600, // 📏 100MB ceiling
        );

        // -- 🧪 Simulate a prolonged 429 storm — server responds in 30s for 50 cycles.
        // -- Without anti-windup, integral would accumulate massive negative "debt".
        // -- When the storm clears, recovery would be catastrophically slow.
        for _ in 0..50 {
            controller.measure(30000.0);
        }
        let output_after_storm = controller.output();

        // -- 🧪 Now the storm clears — responses are fast again (2s).
        // -- With anti-windup, the controller should recover within ~20 cycles,
        // -- not stay floored for 200 cycles.
        for _ in 0..20 {
            controller.measure(2000.0);
        }
        let output_after_recovery = controller.output();

        assert!(
            output_after_recovery > output_after_storm,
            "💀 After recovery from a 429 storm, output should increase. \
             Storm: {} bytes, Recovery: {} bytes. The integral term is holding a grudge.",
            output_after_storm,
            output_after_recovery
        );
    }
}
