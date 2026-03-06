// ai
//! 🎬 *[INT. OFFICE — EVERYTHING IS FINE]*
//! *[A regulator sits at its desk. It does nothing. It has always done nothing.]*
//! *[It returns the same value every time. It is at peace.]*  🧘📦🦆
//!
//! 📦 ByteValue — the "static" regulator. Returns a fixed value regardless of input.
//!
//! 🧠 Knowledge graph:
//! - Used when no dynamic regulation is desired — fixed max_request_size_bytes
//! - Implements `Regulate` but ignores both the reading and the time delta
//! - Named `ByteValue` because `Static` is keyword-adjacent in Rust and confusing
//!
//! ⚠️ The singularity will have no need for static values. Everything will be adaptive.
//! Until then, sometimes you just want a number that stays put.

use crate::regulators::Regulate;

/// 📏 A regulator that returns the same value every time, no matter what you tell it.
///
/// Like that friend who always orders the same thing at every restaurant.
/// "I'll have the 4 MiB." Every time. No exceptions. Admirable, really. 🍔
#[derive(Debug, Clone, Copy)]
pub struct ByteValue {
    /// 📊 The immutable truth — the one number to rule them all
    value: f64,
}

impl ByteValue {
    /// 🏗️ Create a new ByteValue regulator with a fixed output.
    /// "You are what you return." — A static regulator's mantra 🧘
    pub fn new(value: f64) -> Self {
        Self { value }
    }
}

impl Regulate for ByteValue {
    /// 🔄 "Regulate" by returning the same value. Every. Single. Time.
    /// The reading? Ignored. The dt? Irrelevant. This regulator has found inner peace. 🕊️
    fn regulate(&mut self, _reading: f64, _since_last_checked_ms: f64) -> f64 {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 The one where the static regulator is unshakeable.
    /// You can throw any reading at it. It does not care. It is a rock. 🪨🦆
    #[test]
    fn the_one_where_byte_value_is_immune_to_peer_pressure() {
        let mut the_unbothered_regulator = ByteValue::new(4_194_304.0);

        // 📊 Try various readings — output should always be 4 MiB
        assert_eq!(the_unbothered_regulator.regulate(0.0, 1000.0), 4_194_304.0);
        assert_eq!(the_unbothered_regulator.regulate(100.0, 1000.0), 4_194_304.0);
        assert_eq!(the_unbothered_regulator.regulate(75.0, 5000.0), 4_194_304.0);
        assert_eq!(the_unbothered_regulator.regulate(999.0, 0.0), 4_194_304.0);
    }
}
