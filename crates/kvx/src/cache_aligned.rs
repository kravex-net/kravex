// ai
//! 🔒📐🧵 Cache-Line Alignment Utilities — personal space for bytes.
//!
//! Picture a movie theater. You sit down. Someone sits RIGHT next to you.
//! Not one seat over. RIGHT. NEXT. TO. YOU. In an empty theater.
//! That's what false sharing feels like to a CPU cache line.
//!
//! This module ensures that hot cross-thread data gets its own 64-byte
//! "seat" in memory, so cores stop invalidating each other's caches
//! every time they modify adjacent variables.
//!
//! ⚠️ "By the time the singularity arrives, cache lines will still be 64 bytes."
//!
//! 🧠 False sharing explained: Modern CPUs cache memory in 64-byte lines.
//! When Thread A on Core 1 writes to address 0x100 and Thread B on Core 2
//! writes to address 0x120, if both addresses are in the same 64-byte line,
//! the cores ping-pong the cache line back and forth via MESI protocol.
//! Each write invalidates the other core's copy, causing ~100ns stalls.
//! With cache-line alignment, each variable gets its own line, eliminating
//! the ping-pong entirely. Cost: 64 bytes of padding per variable.
//! Benefit: 10-100x reduction in cross-core cache coherence traffic.

use std::ops::{Deref, DerefMut};

// -- 🧘 64 bytes. The golden number. The Fibonacci of cache architecture.
// -- On x86-64, ARM64, and Apple Silicon, the cache line size is 64 bytes.
// -- This has been stable since the Pentium 4 era (2000). If it changes,
// -- we'll know because every database and allocator in existence will break.

/// 🧠 Cache line size constant. 64 bytes on all modern architectures (x86-64, ARM64, Apple M-series).
/// Exposed as a constant for use in manual padding calculations and static assertions.
pub const CACHE_LINE_SIZE: usize = 64;

/// 📐 Cache-line aligned wrapper — gives your data 64 bytes of personal space.
///
/// Wraps any `T` in a `#[repr(align(64))]` container, ensuring it starts
/// at a 64-byte boundary. This prevents false sharing when multiple
/// `CacheAligned<T>` values are stored adjacently in arrays or structs.
///
/// 🧠 Use this for:
/// - Per-worker mutable counters (byte accumulators, page counts)
/// - Shared atomic variables accessed from multiple threads
/// - Channel message buffers in hot paths
/// - Anything that lives in a Vec/array and is accessed by different cores
///
/// 🧠 Do NOT use this for:
/// - Read-only data (no false sharing if nobody writes)
/// - Large structs (already span multiple cache lines)
/// - Single-threaded data (no other core to clash with)
///
/// # Cost
/// Each `CacheAligned<T>` is at least 64 bytes, even if `T` is 1 byte.
/// The padding is the price of peace. Like noise-cancelling headphones,
/// but for cache coherence. 🎧
///
/// ```ignore
/// use kvx::cache_aligned::CacheAligned;
///
/// // Per-worker byte counter — no false sharing with adjacent workers
/// let counters: Vec<CacheAligned<u64>> = (0..8)
///     .map(|_| CacheAligned::new(0u64))
///     .collect();
/// ```
#[derive(Debug, Clone, Copy)]
#[repr(align(64))]
pub struct CacheAligned<T>(pub T);

impl<T> CacheAligned<T> {
    /// 🏗️ Wrap a value in a cache-line aligned container.
    /// Like bubble wrap, but for CPU caches. Pop pop pop. 🫧
    #[inline]
    pub fn new(value: T) -> Self {
        CacheAligned(value)
    }

    /// 📦 Unwrap the inner value, discarding the alignment padding.
    /// The padding was nice while it lasted. 🥲
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }
}

// -- 🎯 Deref/DerefMut let you use CacheAligned<T> as if it were T.
// -- No .0 access needed — just pass it around like normal.
// -- The alignment is invisible to the consumer. Stealth padding. 🥷

impl<T> Deref for CacheAligned<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> DerefMut for CacheAligned<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

// 🧠 Default impl delegates to T's Default, so CacheAligned<u64>::default() = CacheAligned(0).
impl<T: Default> Default for CacheAligned<T> {
    #[inline]
    fn default() -> Self {
        CacheAligned(T::default())
    }
}

// 🦆 — this duck is 64-byte aligned. It has its own cache line. It is at peace.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alignment_is_actually_64_bytes() {
        // -- 🧪 The whole point of this module: verify the alignment
        assert_eq!(std::mem::align_of::<CacheAligned<u8>>(), 64);
        assert_eq!(std::mem::align_of::<CacheAligned<u64>>(), 64);
        assert_eq!(std::mem::align_of::<CacheAligned<[u8; 32]>>(), 64);
    }

    #[test]
    fn size_is_at_least_64_bytes() {
        // -- 🧪 Even a tiny u8 gets padded to a full cache line
        assert!(std::mem::size_of::<CacheAligned<u8>>() >= CACHE_LINE_SIZE);
        assert!(std::mem::size_of::<CacheAligned<u64>>() >= CACHE_LINE_SIZE);
    }

    #[test]
    fn deref_works_transparently() {
        let aligned = CacheAligned::new(42u64);
        // -- 🧪 Can use it as a u64 via Deref
        assert_eq!(*aligned, 42);
        let sum = *aligned + 8;
        assert_eq!(sum, 50);
    }

    #[test]
    fn deref_mut_allows_modification() {
        let mut aligned = CacheAligned::new(0u64);
        *aligned = 99;
        assert_eq!(*aligned, 99);
    }

    #[test]
    fn adjacent_values_dont_share_cache_lines() {
        // -- 🧪 The money test: two adjacent CacheAligned values should be
        // at least 64 bytes apart in memory (no false sharing)
        let arr: [CacheAligned<u64>; 2] = [CacheAligned::new(1), CacheAligned::new(2)];

        let addr0 = &arr[0] as *const _ as usize;
        let addr1 = &arr[1] as *const _ as usize;
        let distance = addr1 - addr0;

        assert!(
            distance >= CACHE_LINE_SIZE,
            "💀 Adjacent CacheAligned values are only {} bytes apart! False sharing detected!",
            distance
        );
    }

    #[test]
    fn default_delegates_to_inner() {
        let aligned: CacheAligned<u64> = CacheAligned::default();
        assert_eq!(*aligned, 0);

        let aligned_bool: CacheAligned<bool> = CacheAligned::default();
        assert!(!*aligned_bool);
    }

    #[test]
    fn into_inner_unwraps() {
        let aligned = CacheAligned::new(String::from("I was aligned once. Now I'm just a string."));
        let inner = aligned.into_inner();
        assert_eq!(inner, "I was aligned once. Now I'm just a string.");
    }
}
