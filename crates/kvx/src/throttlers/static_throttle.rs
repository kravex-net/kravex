// ai
//! 🎬 *[INT. A BEIGE OFFICE — DAY]*
//! *[A thermostat sits on the wall. It reads 72°F. It has read 72°F since 1987.]*
//! *[Nobody has ever touched it. Nobody will ever touch it.]*
//! *[It is... the static throttle controller.]*
//!
//! 📦 **StaticThrottleController** — for when you know exactly how many bytes you want
//! and refuse to let a feedback loop tell you otherwise. 🧊
//!
//! 🧠 Knowledge graph:
//! - Wraps the original behavior: `max_request_size_bytes` is a fixed constant.
//! - `measure()` is a no-op — measurements go in, nothing comes out. Like therapy.
//! - `output()` always returns the same value. Immutable. Stubborn. Reliable.
//! - This exists so SinkWorker can use the ThrottleController trait uniformly,
//!   regardless of whether the user configured static or PID throttling.
//! - Default strategy when no throttle config is specified. Backwards compatible.
//!
//! 🦆 "I used to be a dynamic controller like you. Then I took a const to the knee."

use super::ThrottleController;

/// 🧊 A throttle controller that outputs a fixed byte size. Forever. Unwaveringly.
///
/// Like that one friend who always orders the same thing at every restaurant.
/// "I'll have the 10MB, please." Every time. Without looking at the menu. 🍔
///
/// 🧠 Tribal knowledge: this is the backwards-compatible default. Before PID was added,
/// `max_request_size_bytes` was a plain `usize` on SinkWorker. This struct preserves
/// that exact behavior while conforming to the ThrottleController trait interface.
#[derive(Debug, Clone)]
pub(crate) struct StaticThrottleController {
    /// 📏 The immovable byte target — will not change no matter what you measure.
    /// It has the emotional range of a brick. And roughly the same adaptability.
    the_number_that_shall_not_change: usize,
}

impl StaticThrottleController {
    /// 🏗️ Create a new static controller with a fixed output.
    ///
    /// The `fixed_bytes` value becomes the eternal, unchanging output.
    /// Like a tattoo. But for throughput. And slightly less regrettable. 🚀
    pub(crate) fn new(fixed_bytes: usize) -> Self {
        // -- "You want dynamic? Wrong controller, pal." — StaticThrottleController, probably
        Self {
            the_number_that_shall_not_change: fixed_bytes,
        }
    }
}

impl ThrottleController for StaticThrottleController {
    /// 📡 Receive a measurement and do absolutely nothing with it.
    ///
    /// This function exists purely to satisfy the trait contract.
    /// It's the "mark all as read" of feedback loops.
    /// Your latency data enters a void from which no correction emerges. 🕳️
    fn measure(&mut self, _duration_ms: f64) {
        // -- 🧘 *Om.* The measurement has been received. And released.
        // -- Like a leaf on a stream. Like a PR comment you chose to ignore.
        // -- Nothing will change. This is the way.
    }

    /// 📏 Returns the fixed byte size. Surprised? You shouldn't be.
    ///
    /// This value was set at construction and will be returned until
    /// the heat death of the universe, whichever comes first. 🎯
    fn output(&self) -> usize {
        self.the_number_that_shall_not_change
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 The one where static means static and nothing changes, ever, like my stance on tabs vs spaces
    #[test]
    fn the_one_where_static_means_static_and_nothing_ever_changes() {
        // -- 🧪 If this test fails, physics is broken
        let mut controller = StaticThrottleController::new(42_000_000);
        assert_eq!(
            controller.output(),
            42_000_000,
            "💀 The static controller changed its mind. This shouldn't happen unless we're in a parallel universe."
        );

        // -- 🧪 Feed it measurements. Watch it not care.
        controller.measure(100.0);
        controller.measure(99999.0);
        controller.measure(0.001);
        controller.measure(f64::MAX);

        assert_eq!(
            controller.output(),
            42_000_000,
            "💀 The static controller was swayed by measurements. It has betrayed its core identity."
        );
    }

    /// 🧪 The one where zero bytes is a valid but questionable life choice
    #[test]
    fn the_one_where_zero_bytes_is_technically_valid() {
        // -- 🧪 Edge case: "I want to send nothing, forever"
        let controller = StaticThrottleController::new(0);
        assert_eq!(
            controller.output(),
            0,
            "💀 Zero should mean zero. This isn't a Zen koan."
        );
    }

    /// 🧪 The pilot episode where we establish the controller's personality
    #[test]
    fn pilot_episode_the_controller_is_born() {
        // -- 🧪 A controller is born. It has one purpose. One number. One truth.
        let controller = StaticThrottleController::new(10_485_760);
        assert_eq!(
            controller.output(),
            10_485_760,
            "💀 10MB should be 10MB. Math is math."
        );
    }
}
