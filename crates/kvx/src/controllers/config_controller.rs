// ai
//! 🧊📦🔧 ConfigController — the "set it and forget it" of batch sizing.
//!
//! 🎬 COLD OPEN — INT. KITCHEN — SUNDAY MORNING
//!
//! "Honey, did you adjust the thermostat?" asked nobody, because the
//! ConfigController doesn't adjust anything. It was set to 72°F in 2019.
//! It will be 72°F when the heat death of the universe arrives.
//! Some call it stubborn. We call it *configuration-driven*.
//!
//! This is the simplest possible controller: it returns a fixed value
//! and ignores all measurements. It's the `Passthrough` of controllers.
//! The `::<Identity>` of feedback loops. The `/dev/null` of PID theory.
//!
//! ## Knowledge Graph 🧠
//! - Implements `Controller` trait from parent module
//! - `output()` returns `page_size` (set at construction, never changes)
//! - `measure()` is a no-op (measurements vanish into the void)
//! - Used when `ControllerConfig::Static` is selected (the default)
//! - Preserves pre-controller behavior: source uses whatever `max_batch_size_docs` says
//! - Zero overhead: no allocations, no state updates, no math, no regrets
//!
//! 🦆 The duck approves of this controller's commitment to doing absolutely nothing.
//! ⚠️ The singularity will not improve upon this design. It is already perfect in its nothingness.

use super::Controller;

/// 🧊 ConfigController — returns the configured page size, ignores measurements.
///
/// Like a thermostat that's been taped over by a passive-aggressive office manager:
/// the temperature is set, the dial is decorative, and your feedback is noted but not actioned.
///
/// 🧠 Knowledge graph:
/// - Constructed with `page_size` from `CommonSourceConfig.max_batch_size_docs`
/// - `output()` always returns `page_size` — consistent as a metronome, exciting as a spreadsheet
/// - `measure()` accepts the measurement gracefully, then throws it into the void
/// - This is the default controller when no `[controller]` config section exists
///
/// 📜 "He who measures but does not act, has a ConfigController." — Ancient PID proverb
#[derive(Debug)]
pub(crate) struct ConfigController {
    // 🎯 The one number this controller knows. The one number it will ever know.
    // Like a dog with one trick — but the trick is "return usize" and it's REALLY good at it.
    the_immutable_page_size: usize,
}

impl ConfigController {
    /// 🏗️ Creates a new ConfigController with the given static page size.
    ///
    /// This is the only time the page size is set. After this, it's locked in.
    /// Like choosing a tattoo, except less painful and more deterministic.
    pub(crate) fn new(page_size: usize) -> Self {
        Self {
            the_immutable_page_size: page_size,
        }
    }
}

impl Controller for ConfigController {
    /// 🎯 Returns the configured page size. Always. Forever. No exceptions.
    ///
    /// This function is pure in the mathematical sense: no side effects,
    /// no state changes, no surprises. The functional programming crowd
    /// would be proud if they weren't busy arguing about monads.
    #[inline]
    fn output(&self) -> usize {
        self.the_immutable_page_size
    }

    /// 📏 Accepts a measurement and does absolutely nothing with it.
    ///
    /// "Timeout exceeded: We waited. And waited. Like a dog at the window.
    /// But the measurement never changed anything. It never does."
    #[inline]
    fn measure(&mut self, _measurement: f64) {
        // 🧘 Achievement unlocked: zen-level acceptance of irrelevant input.
        // The measurement arrives. The measurement departs. Nothing changes.
        // This is not a bug. This is enlightenment.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 Verify output returns the configured page size.
    /// If this fails, the one thing ConfigController does is broken.
    /// That's... actually impressive in a way.
    #[test]
    fn the_one_where_config_controller_returns_static_value() {
        let the_controller = ConfigController::new(42);
        assert_eq!(
            the_controller.output(),
            42,
            "ConfigController should return 42 — the answer to life, the universe, and batch sizing"
        );
    }

    /// 🧪 Verify measurements are ignored — output never changes.
    #[test]
    fn the_one_where_measurements_enter_the_void() {
        let mut the_controller = ConfigController::new(1000);
        assert_eq!(the_controller.output(), 1000);

        // 📏 Feed measurements of various absurdity levels
        the_controller.measure(0.0);
        assert_eq!(the_controller.output(), 1000, "Zero bytes — still 1000");

        the_controller.measure(999_999_999.0);
        assert_eq!(the_controller.output(), 1000, "Nearly a gigabyte — still 1000");

        the_controller.measure(-42.0);
        assert_eq!(the_controller.output(), 1000, "Negative bytes (?) — still 1000");

        the_controller.measure(f64::INFINITY);
        assert_eq!(the_controller.output(), 1000, "Infinity — still 1000. Zen. 🧘");

        the_controller.measure(f64::NAN);
        assert_eq!(the_controller.output(), 1000, "NaN — still 1000. Nothing phases us.");
    }

    /// 🧪 Verify multiple calls to output() are consistent.
    /// Because consistency is a feature, not a personality trait.
    #[test]
    fn the_one_where_consistency_is_verified_a_thousand_times() {
        let the_controller = ConfigController::new(7777);
        for _ in 0..1000 {
            assert_eq!(
                the_controller.output(),
                7777,
                "ConfigController changed its mind. This should be impossible. Check the laws of physics."
            );
        }
    }

    /// 🧪 Verify edge case: page_size of 1.
    /// The absolute minimum. Like ordering "just water" at a restaurant.
    #[test]
    fn the_one_where_the_minimum_viable_batch_is_one() {
        let the_controller = ConfigController::new(1);
        assert_eq!(the_controller.output(), 1);
    }

    /// 🧪 Verify edge case: large page_size.
    /// Because someone will configure this to usize::MAX and we should not panic.
    #[test]
    fn the_one_where_someone_configures_an_absurd_batch_size() {
        let the_controller = ConfigController::new(usize::MAX);
        assert_eq!(the_controller.output(), usize::MAX);
    }

    // 🦆 The duck notes that these tests are more comprehensive than the controller itself.
    // This is by design. The controller does one thing. The tests verify it a hundred ways.
}
