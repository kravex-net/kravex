// ai
//! 🎬 *[INT. CONTROL ROOM — ALARMS BLARING]*
//! *[A single dial labeled "CPU %" creeps past 80.]*
//! *[The PID controller wakes. It has been training for this moment its entire lifecycle.]*
//! *["Reduce the flow," it whispers. "Before the JVM GC eats us all."]*  🧵🔧🦆
//!
//! 📦 CpuPressure — a PID controller that regulates payload size based on CPU pressure.
//!
//! 🧠 Knowledge graph:
//! - Input: CPU usage percent (0.0–100.0), smoothed via EMA (α=0.25)
//! - Output: effective max request size in bytes, clamped to [min, max]
//! - PID error = setpoint - smoothed_reading (positive = headroom, negative = overloaded)
//! - Gains auto-tuned from output range ratio — no manual knob-twiddling required
//! - Integral term anti-windup via clamping to [-setpoint*5, +setpoint*5]
//! - Additive PID: output += kp*error + ki*integral + kd*derivative
//!
//! ⚠️ The singularity will PID-control itself. Until then, we have floats and prayer.

use std::time::Duration;

use crate::{GaugeReading, regulators::Regulate};

/// 🎛️ EMA smoothing factor — how much we trust the latest reading vs history.
/// 0.25 = "I believe you, but I also believed the last three readings." 📊
const EMA_ALPHA: f64 = 0.25;

/// 🧮 PID controller that converts CPU pressure into a flow rate (bytes).
///
/// Like a thermostat, but instead of temperature it controls how much JSON
/// we hurl at Elasticsearch before the JVM starts having Vietnam flashbacks
/// about garbage collection. 🗑️💀
///
/// 🧠 Tribal knowledge: the gains are auto-tuned from the output range ratio.
/// This means you don't need a PhD in control theory to use this.
/// You just need a PhD in Rust lifetimes. Which is arguably harder.
#[derive(Debug, Clone)]
pub struct CpuPressure {
    /// 🎯 Target CPU percent — the promised land we're trying to reach (default: 75.0)
    setpoint: f64,
    /// 📏 Minimum output bytes — floor so we don't throttle to zero and stall the pipeline
    min_output: f64,
    /// 📏 Maximum output bytes — ceiling so we don't YOLO the entire index in one request
    max_output: f64,
    /// 📊 Current output value — the flow rate in bytes, adjusted each regulate() call
    output: f64,
    /// 🔧 Proportional gain — "how much do I care about the current error?"
    kp: f64,
    /// 🔧 Integral gain — "how much do I care about accumulated past errors?"
    ki: f64,
    /// 🔧 Derivative gain — "how much do I care about the rate of change?"
    kd: f64,
    /// 📈 Accumulated integral term — the controller's grudge against past errors
    integral_accumulation: f64,
    /// 🔒 Integral anti-windup clamp — prevents the integral from going full send
    integral_clamp: f64,
    /// 📉 Previous error — for computing derivative (rate of change)
    prev_error: f64,
    /// 📊 EMA-smoothed CPU reading — because raw readings jitter like a chihuahua on espresso
    ema_average: f64,
}

impl CpuPressure {
    /// 🏗️ Create a new PID controller with auto-tuned gains.
    ///
    /// "Give me a setpoint and a range, and I shall move the world."
    /// — Archimedes, if he wrote control systems instead of taking baths 🛁
    ///
    /// 🧠 Gain auto-tuning rationale:
    /// - `kp` = max_output / min_output — scales proportional response to the full operating range.
    ///   When the range is wide (64MiB / 128KiB = 512), the controller responds aggressively.
    ///   When the range is narrow, it responds gently. This is the key insight. 🔑
    /// - `ki` = kp/10 — gentle integral, enough to eliminate steady-state error without overshoot
    /// - `kd` = sqrt(kp * ki) — geometric mean, balances damping between P and I
    /// - `integral_clamp` = setpoint * 5 — prevents windup during sustained error periods
    ///
    /// ⚠️ Panics if `min_output > max_output` or either is non-positive.
    /// "He who inverts the bounds, debugs in production." — Ancient proverb 💀
    pub fn new(setpoint: f64, min_output: f64, max_output: f64, initial_output: f64) -> Self {
        assert!(
            min_output > 0.0 && max_output > 0.0,
            "💀 PID output bounds must be positive — got min={}, max={}. \
             Negative bytes are not a thing. Yet.",
            min_output, max_output
        );
        assert!(
            min_output <= max_output,
            "💀 PID min_output ({}) > max_output ({}) — the floor is above the ceiling. \
             This is not an Escher painting, it's a config error.",
            min_output, max_output
        );

        // 🧮 Auto-tune gains from the full operating range ratio
        // 📊 max/min gives us scale sensitivity — wide range = aggressive, narrow = gentle
        let kp = max_output / min_output;
        let ki = kp / 10.0;
        let kd = (kp * ki).sqrt();
        let integral_clamp = setpoint * 5.0;

        Self {
            setpoint,
            min_output,
            max_output,
            output: initial_output,
            kp,
            ki,
            kd,
            integral_accumulation: 0.0,
            integral_clamp,
            prev_error: 0.0,
            ema_average: setpoint, // 📊 Assume we start at setpoint — optimistic but fair 🦆
        }
    }
}

impl Regulate for CpuPressure {
    /// 🔄 Feed a CPU pressure reading, get back an adjusted flow rate in bytes.
    ///
    /// The PID loop in all its glory:
    /// 1. EMA smooth the raw reading (tame the jitter demon)
    /// 2. Compute error = setpoint - smoothed (positive = headroom, negative = too hot 🔥)
    /// 3. Accumulate integral (with anti-windup clamping because we're not animals)
    /// 4. Compute derivative (how fast are things changing?)
    /// 5. Additive output adjustment: output += kp*e + ki*I + kd*D
    /// 6. Clamp to [min, max] — no matter what the math says, physics has limits
    ///
    /// "He who regulates without smoothing, oscillates in production." — Ancient proverb 📜
    fn regulate(&mut self, reading: GaugeReading, since_last_checked_ms: Duration) -> f64 {
        // 📊 Step 1: EMA smooth — trust the new reading 25%, trust history 75%
        self.ema_average = EMA_ALPHA * reading + (1.0 - EMA_ALPHA) * self.ema_average;

        // 🎯 Step 2: Error = how far from the promised land
        // Positive error = CPU below setpoint = we can send MORE bytes 🚀
        // Negative error = CPU above setpoint = SLOW DOWN BUDDY 🛑
        let error = self.setpoint - self.ema_average;

        // 📈 Step 3: Integral accumulation with anti-windup
        let dt_seconds = since_last_checked_ms / 1000.0;
        self.integral_accumulation += error * dt_seconds;
        self.integral_accumulation = self
            .integral_accumulation
            .clamp(-self.integral_clamp, self.integral_clamp);

        // 📉 Step 4: Derivative (rate of error change)
        let derivative = if dt_seconds > 0.0 {
            (error - self.prev_error) / dt_seconds
        } else {
            0.0
        };
        self.prev_error = error;

        // 🧮 Step 5: PID output adjustment — the moment of truth
        let the_pid_says = self.kp * error
            + self.ki * self.integral_accumulation
            + self.kd * derivative;

        // 📏 Step 6: Additive adjustment + clamp — the guardrails of sanity
        self.output = (self.output + the_pid_says).clamp(self.min_output, self.max_output);

        self.output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 The one where the PID controller is born with reasonable defaults.
    /// Like a newborn, it doesn't know what it's doing yet, but the gains look promising. 👶
    #[test]
    fn the_one_where_pid_initializes_without_existential_crisis() {
        // 🏗️ 75% CPU target, 128KiB min, 64MiB max, start at 4MiB
        let the_controller = CpuPressure::new(75.0, 131_072.0, 67_108_864.0, 4_194_304.0);

        assert_eq!(the_controller.setpoint, 75.0, "🎯 Setpoint should be 75%");
        assert_eq!(the_controller.output, 4_194_304.0, "📊 Initial output should be 4 MiB");
        assert!(the_controller.kp > 0.0, "🔧 kp should be positive — we need proportional response");
        assert!(the_controller.ki > 0.0, "🔧 ki should be positive — we need integral correction");
        assert!(the_controller.kd > 0.0, "🔧 kd should be positive — we need derivative damping");
        assert_eq!(the_controller.ema_average, 75.0, "📊 EMA starts at setpoint — hopeful 🦆");
    }

    /// 🧪 The one where CPU is below setpoint so PID increases flow.
    /// "The CPU is bored. Feed it more data." — every optimization engineer ever 🚀
    #[test]
    fn the_one_where_low_cpu_means_more_bytes() {
        let mut the_controller = CpuPressure::new(75.0, 131_072.0, 67_108_864.0, 4_194_304.0);
        let the_starting_output = the_controller.output;

        // 📊 Feed it 50% CPU — well below 75% setpoint — should increase output
        let the_new_flow = the_controller.regulate(50.0, 3000.0);

        assert!(
            the_new_flow > the_starting_output,
            "🎯 Output should increase when CPU ({}) is below setpoint (75%) — got {} vs {}",
            50.0, the_new_flow, the_starting_output
        );
    }

    /// 🧪 The one where CPU is above setpoint so PID decreases flow.
    /// "The CPU is on fire. Stop feeding it. STOP." — the PID controller, calmly 🔥
    #[test]
    fn the_one_where_high_cpu_means_fewer_bytes() {
        let mut the_controller = CpuPressure::new(75.0, 131_072.0, 67_108_864.0, 4_194_304.0);

        // 📊 First, pump output up by feeding low CPU readings — give it room to decrease
        for _ in 0..10 {
            the_controller.regulate(50.0, 3000.0);
        }
        let the_elevated_output = the_controller.output;

        // 📊 Now slam it with 95% CPU — should decrease output.
        // ⚠️ EMA needs several readings to overcome the low-CPU momentum,
        // so we pump 20 readings at 95% to let the PID fully respond. 🔥
        for _ in 0..20 {
            the_controller.regulate(95.0, 3000.0);
        }

        assert!(
            the_controller.output < the_elevated_output,
            "🎯 Output should decrease when CPU is sustained above setpoint — got {} vs {}",
            the_controller.output, the_elevated_output
        );
    }

    /// 🧪 The one where the output never exceeds the clamp range.
    /// "I don't care what the math says, we have physical limits." — every engineer 📏
    #[test]
    fn the_one_where_output_respects_the_guardrails() {
        let the_min = 131_072.0_f64;
        let the_max = 67_108_864.0_f64;
        let mut the_controller = CpuPressure::new(75.0, the_min, the_max, 4_194_304.0);

        // 🚀 Feed it super low CPU — should try to max out but respect ceiling
        for _ in 0..100 {
            the_controller.regulate(10.0, 3000.0);
        }
        assert!(
            the_controller.output <= the_max,
            "🎯 Output {} should not exceed max {}",
            the_controller.output, the_max
        );

        // 💀 Feed it super high CPU — should try to min out but respect floor
        for _ in 0..100 {
            the_controller.regulate(99.0, 3000.0);
        }
        assert!(
            the_controller.output >= the_min,
            "🎯 Output {} should not go below min {}",
            the_controller.output, the_min
        );
    }

    /// 🧪 The one where EMA smoothing tames a jittery reading.
    /// The reading says 100%, then 0%, then 100% — the EMA says "chill." 📊🦆
    #[test]
    fn the_one_where_ema_smoothing_prevents_whiplash() {
        let mut the_controller = CpuPressure::new(75.0, 131_072.0, 67_108_864.0, 4_194_304.0);

        // 📊 Alternate between extremes — EMA should dampen the oscillation
        the_controller.regulate(100.0, 3000.0);
        let after_high = the_controller.ema_average;

        the_controller.regulate(0.0, 3000.0);
        let after_low = the_controller.ema_average;

        // 📊 EMA should NOT be at 0 — it should still remember the 100 reading
        assert!(
            after_low > 10.0,
            "🎯 EMA after 0% reading should still be well above 0 due to smoothing — got {}",
            after_low
        );
        // 📊 EMA after high reading should NOT be at 100 — it was blended with the starting 75
        assert!(
            after_high < 90.0,
            "🎯 EMA after 100% reading should be dampened below 90 — got {}",
            after_high
        );
    }

    /// 🧪 The one where zero dt doesn't cause division by zero.
    /// Because even PID controllers deserve NaN protection. 🛡️
    #[test]
    fn the_one_where_zero_dt_doesnt_explode() {
        let mut the_controller = CpuPressure::new(75.0, 131_072.0, 67_108_864.0, 4_194_304.0);

        // 📊 Zero dt — derivative should be 0, no NaN, no panic
        let the_result = the_controller.regulate(80.0, 0.0);
        assert!(
            the_result.is_finite(),
            "🎯 Output should be finite even with zero dt — got {}",
            the_result
        );
    }

    /// 🧪 The one where the PID converges to steady state at setpoint.
    /// Feed it exactly 75% CPU for 100 iterations — output should stabilize near initial value.
    /// If it oscillates wildly, the gains are drunk. 📊🦆
    #[test]
    fn the_one_where_pid_finds_inner_peace_at_setpoint() {
        let mut the_controller = CpuPressure::new(75.0, 131_072.0, 67_108_864.0, 4_194_304.0);

        // 📊 Feed exactly the setpoint for many iterations — should converge
        for _ in 0..100 {
            the_controller.regulate(75.0, 3000.0);
        }

        let the_settled_output = the_controller.output;

        // 📊 Run 20 more — output should barely change (steady state)
        for _ in 0..20 {
            the_controller.regulate(75.0, 3000.0);
        }

        let the_drift = (the_controller.output - the_settled_output).abs();
        let the_drift_percent = the_drift / the_settled_output * 100.0;

        assert!(
            the_drift_percent < 1.0,
            "🎯 Output should be stable at setpoint — drifted {:.4}% ({} → {}). \
             The PID is restless. It needs therapy.",
            the_drift_percent, the_settled_output, the_controller.output
        );
    }

    /// 🧪 The one where inverted bounds panic like they should.
    /// min > max is a config error, not a feature. 💀
    #[test]
    #[should_panic(expected = "min_output")]
    fn the_one_where_inverted_bounds_trigger_existential_panic() {
        // 📏 min=1M, max=100 — floor above ceiling, like an Escher painting
        CpuPressure::new(75.0, 1_000_000.0, 100.0, 500.0);
    }
}
