// ai
//! 🏦📦🚀 The Buffer Pool — a lending library for bytes.
//!
//! It was 3 AM. The heap allocator was sweating. "Another Vec<u8>?" it whispered.
//! "I just freed the last one." But the source workers kept coming, relentless,
//! each one demanding fresh memory like toddlers demanding juice boxes.
//! And then... the BufferPool arrived. A hero. A recycler. A byte whisperer.
//!
//! ⚠️ "The singularity will arrive before we stop recycling buffers."
//!
//! 🧠 Architecture: C# ArrayPool-inspired 3-tier rental system:
//!   1. Thread-Local Storage (TLS) — zero-contention fast path, per-thread cache
//!   2. Shared Pool (Mutex) — brief lock, cross-thread fallback
//!   3. Heap Allocation — last resort, brand new Vec<u8>
//!
//! 🧠 Why this design: In steady-state, source workers rent from TLS and sink workers
//! return to TLS on the same thread. The shared pool handles cross-thread returns
//! (source thread rents, sink thread returns). New allocations only happen during
//! ramp-up or when pool is exhausted.
//!
//! 🧠 Bucket strategy: Power-of-2 sizes from 4 KiB to 1 MiB (10 buckets).
//! This matches typical Elasticsearch scroll page sizes (4KB-512KB).
//! Rounding up to next power-of-2 wastes at most 50% capacity but eliminates
//! fragmentation and makes bucket lookup O(1) via leading_zeros().

use std::cell::RefCell;
use std::io::{self, Write};
use std::sync::Mutex;

// -- 🐛 TODO: win the lottery, retire, delete this crate
// -- no cap this buffer pool slaps fr fr

// 🧠 Bucket sizes: 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB, 512KB, 1MB
// These correspond to bucket indices 0-8, selected by rounding up to next power-of-2.
// The minimum bucket is 4KB (2^12) because anything smaller isn't worth pooling —
// the overhead of pool management would exceed the allocation cost.
const MIN_BUCKET_SHIFT: u32 = 12; // 2^12 = 4096 = 4 KiB
const MAX_BUCKET_SHIFT: u32 = 20; // 2^20 = 1,048,576 = 1 MiB
const NUM_BUCKETS: usize = (MAX_BUCKET_SHIFT - MIN_BUCKET_SHIFT + 1) as usize; // 9 buckets

// 🧠 TLS capacity: 8 buffers per bucket per thread is the sweet spot.
// Too few = frequent fallback to shared pool (contention). Too many = wasted memory
// sitting idle in thread-local caches. 8 matches typical pipeline depth
// (source fetches ~8 pages ahead of sink draining them).
const TLS_CAPACITY_PER_BUCKET: usize = 8;

// 🧠 Shared pool max: 256 total buffers across all buckets globally.
// This caps memory usage at ~256 MB worst case (256 × 1MB max bucket).
// In practice most buffers are 64KB-256KB so actual usage is ~16-64 MB.
const SHARED_MAX_PER_BUCKET: usize = 32;

/// 📦 PoolBuffer — a reusable byte buffer that remembers where it came from.
///
/// Like a library book with a barcode — it knows which shelf (bucket) it belongs on.
/// When dropped, it returns itself to the pool instead of being freed. The heap
/// allocator sends a thank-you card every time. 🎉
///
/// 🧠 The bucket_index field is set at rent-time and never changes. This ensures
/// the buffer always returns to the correct bucket, even if its logical length varies.
/// The underlying Vec capacity is always >= the bucket's minimum size.
#[derive(Debug)]
pub struct PoolBuffer {
    // 🧠 The actual byte storage. We manipulate len (via clear/extend/write) but
    // never shrink capacity. The whole point is to reuse the allocation.
    data: Vec<u8>,
    // 🧠 Which bucket this buffer belongs to (0 = 4KB, 1 = 8KB, ..., 8 = 1MB).
    // Used by return_buffer() to put it back in the right bucket. None if the buffer
    // was too large for any bucket (>1MB) — these just get dropped normally.
    bucket_index: Option<usize>,
}

impl PoolBuffer {
    /// 🏗️ Create a new PoolBuffer with a specific capacity and bucket assignment.
    fn new(capacity: usize, bucket_index: Option<usize>) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bucket_index,
        }
    }

    /// 📐 Current number of valid bytes in the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// 🤔 Is this buffer empty? Like my soul after a production incident.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// 📐 Total allocated capacity (never shrinks after creation).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// 👀 Borrow the buffer contents as a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// 👀 Borrow the buffer contents as a mutable byte slice.
    #[inline]
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// 📝 Try to view buffer contents as a UTF-8 string slice.
    ///
    /// 🧠 Reqwest .text() guarantees UTF-8, but .copy_to() gives raw bytes.
    /// Source implementations that use raw byte reads MUST validate UTF-8 before
    /// calling this. For reqwest sources, the response is already decoded to UTF-8
    /// by the time we get it, so this is safe.
    #[inline]
    pub fn as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.data)
    }

    /// 📝 View buffer as UTF-8 without checking.
    ///
    /// # Safety
    /// Caller must guarantee the buffer contains valid UTF-8. Use this only when
    /// the data source guarantees UTF-8 (e.g., reqwest text responses, JSON payloads).
    #[inline]
    pub unsafe fn as_str_unchecked(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.data) }
    }

    /// 🗑️ Reset length to 0 without deallocating. The buffer is ready for reuse.
    /// Like erasing a whiteboard — the surface area (capacity) stays the same.
    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// 📦 Consume the PoolBuffer and return the underlying Vec<u8>.
    /// The buffer will NOT be returned to the pool after this — it's a one-way trip
    /// to reqwest body territory. Like checking out of Hotel California, except you can leave.
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        // 🧠 We use ManuallyDrop-like semantics via std::mem::forget pattern.
        // But actually we just consume self, so Drop doesn't run.
        // The caller owns the Vec now. Goodbye, sweet buffer. 🥲
        let mut this = std::mem::ManuallyDrop::new(self);
        // Safety: we're taking the Vec out of the struct, then forgetting the struct
        // so Drop doesn't try to return a now-empty PoolBuffer to the pool.
        std::mem::take(&mut this.data)
    }

    /// 📝 Extend the buffer with bytes from a slice.
    #[inline]
    pub fn extend_from_slice(&mut self, src: &[u8]) {
        self.data.extend_from_slice(src);
    }

    /// 📝 Extend the buffer with bytes from a str (convenience for UTF-8 data).
    #[inline]
    pub fn push_str(&mut self, s: &str) {
        self.data.extend_from_slice(s.as_bytes());
    }

    /// 📝 Push a single byte. Like push_str but for the commitment-phobic. 🎯
    #[inline]
    pub fn push_byte(&mut self, b: u8) {
        self.data.push(b);
    }

    /// 🔧 Access the underlying Vec mutably for advanced operations.
    /// Use with care — don't shrink_to_fit() or you'll defeat the whole purpose. 💀
    #[inline]
    pub fn as_vec_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
}

// 🧠 impl Write lets reqwest's copy_to() write response bytes directly into a PoolBuffer.
// This avoids the intermediate String allocation that .text() creates.
// The Write trait is also useful for serde_json::to_writer() and similar byte-oriented APIs.
impl Write for PoolBuffer {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.data.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        // -- Nothing to flush. We're a Vec. We're always flushed. We're very composed. 🧘
        Ok(())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.data.extend_from_slice(buf);
        Ok(())
    }
}

// 🧠 impl fmt::Write lets write!() / writeln!() format strings directly into a PoolBuffer.
// This is the key enabler for eliminating per-doc String allocations in transforms —
// action lines like {"index":{"_id":"abc"}} get write!()'d straight into the output buffer
// instead of allocating a temporary String, formatting into it, then copying to PoolBuffer.
// std::io::Write is for raw bytes (serde_json::to_writer). std::fmt::Write is for formatted text.
// Together they let PoolBuffer be the universal write target. Like a good therapist: accepts everything. 🦆
impl std::fmt::Write for PoolBuffer {
    #[inline]
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.data.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

// 🧠 From<String> convenience: lets existing test code and simple sources wrap a String
// into a PoolBuffer without going through the pool. The bucket_index is None because
// this buffer wasn't rented — it'll just be freed normally on Drop.
impl From<String> for PoolBuffer {
    fn from(s: String) -> Self {
        Self {
            data: s.into_bytes(),
            bucket_index: None,
        }
    }
}

impl From<Vec<u8>> for PoolBuffer {
    fn from(v: Vec<u8>) -> Self {
        Self {
            data: v,
            bucket_index: None,
        }
    }
}

// 🧠 Drop returns the buffer to the pool automatically. This is the key ergonomic win —
// callers don't need to manually call return_buffer(). When a PoolBuffer goes out of
// scope anywhere in the pipeline, it finds its way home. Like a homing pigeon. 🐦
impl Drop for PoolBuffer {
    fn drop(&mut self) {
        // -- "You can check out any time you like, but you can never leave" — Hotel Buffer Pool 🎸
        if let Some(bucket) = self.bucket_index {
            // 🧠 Take the data Vec out of self (replacing with empty Vec — zero alloc).
            // Then attempt to return it to the pool. If the pool is full, it just gets dropped
            // normally. Either way, no leak.
            let data = std::mem::take(&mut self.data);
            if !data.is_empty() || data.capacity() > 0 {
                BUFFER_POOL.return_vec(data, bucket);
            }
        }
        // 🧠 If bucket_index is None, this buffer wasn't from the pool (e.g., From<String>).
        // Just let the Vec drop normally. The heap allocator does its thing. 🗑️
    }
}

// 🎯 PartialEq implementations — because tests deserve nice things.
// Comparing PoolBuffer to &str makes assertions clean: `assert_eq!(payload, "expected")`
// without forcing callers to `.as_str().unwrap()` everywhere. The borrow checker appreciates
// our thoughtfulness. For once. 🦆
impl PartialEq<str> for PoolBuffer {
    fn eq(&self, other: &str) -> bool {
        self.data == other.as_bytes()
    }
}

impl PartialEq<&str> for PoolBuffer {
    fn eq(&self, other: &&str) -> bool {
        self.data == other.as_bytes()
    }
}

impl PartialEq<String> for PoolBuffer {
    fn eq(&self, other: &String) -> bool {
        self.data == other.as_bytes()
    }
}

/// 🏦 The Global Buffer Pool — one pool to rule them all, one pool to bind them.
///
/// 🧠 This is a singleton accessed via the static BUFFER_POOL. It contains:
/// - Thread-local caches (zero contention, per-thread)
/// - A shared Mutex-protected pool (brief contention, cross-thread fallback)
///
/// The design follows C#'s ArrayPool<T>: TLS fast path → shared fallback → allocate new.
/// In steady-state pipeline operation, most rent/return pairs hit TLS (same thread).
/// Cross-thread returns (source thread → sink thread) go through the shared pool.
struct BufferPool {
    // 🧠 Shared pool: Mutex<Vec<Vec<Vec<u8>>>>
    // Outer Vec: indexed by bucket (0..NUM_BUCKETS)
    // Inner Vec: stack of available Vec<u8> buffers for that bucket
    // Mutex contention is brief — just push/pop from a Vec.
    shared: Mutex<Vec<Vec<Vec<u8>>>>,
}

// 🧠 Thread-local cache: each thread gets its own stack of buffers per bucket.
// This eliminates all synchronization on the fast path. The RefCell is safe because
// TLS is single-threaded by definition. The outer Vec is indexed by bucket,
// inner Vec is the stack of available buffers.
thread_local! {
    static TLS_CACHE: RefCell<Vec<Vec<Vec<u8>>>> = RefCell::new(
        (0..NUM_BUCKETS).map(|_| Vec::with_capacity(TLS_CAPACITY_PER_BUCKET)).collect()
    );
}

// 🧠 Global singleton. Constructed at program start via const initialization.
// No lazy_static or OnceLock needed — the Mutex and Vec are const-constructible.
static BUFFER_POOL: BufferPool = BufferPool {
    shared: Mutex::new(Vec::new()),
};

impl BufferPool {
    /// 🧠 Initialize the shared pool buckets. Must be called once at startup.
    /// We can't use const Vec::new() for the inner vecs in a static context,
    /// so we lazily initialize on first access.
    fn ensure_initialized(&self) {
        let mut shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        if shared.is_empty() {
            *shared = (0..NUM_BUCKETS)
                .map(|_| Vec::with_capacity(SHARED_MAX_PER_BUCKET))
                .collect();
        }
    }

    /// 🚀 Rent a buffer with at least `min_capacity` bytes of space.
    ///
    /// 🧠 Three-tier lookup:
    /// 1. TLS cache (no lock, no atomic, just a RefCell::borrow_mut + pop)
    /// 2. Shared pool (Mutex lock, pop from bucket)
    /// 3. Allocate new (Vec::with_capacity at bucket size)
    ///
    /// Returns a PoolBuffer that auto-returns to pool on Drop.
    fn rent(&self, min_capacity: usize) -> PoolBuffer {
        let (bucket_index, bucket_capacity) = bucket_for_capacity(min_capacity);

        match bucket_index {
            Some(idx) => {
                // -- 🔍 Phase 1: Check the TLS cache (like checking your pockets for keys)
                let tls_hit = TLS_CACHE.with(|cache| {
                    let mut buckets = cache.borrow_mut();
                    buckets[idx].pop()
                });

                if let Some(mut data) = tls_hit {
                    data.clear();
                    return PoolBuffer {
                        data,
                        bucket_index: Some(idx),
                    };
                }

                // -- 🔍 Phase 2: Check the shared pool (like checking the junk drawer)
                let shared_hit = {
                    let mut shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
                    if !shared.is_empty() {
                        shared[idx].pop()
                    } else {
                        None
                    }
                };

                if let Some(mut data) = shared_hit {
                    data.clear();
                    return PoolBuffer {
                        data,
                        bucket_index: Some(idx),
                    };
                }

                // -- 🔍 Phase 3: Allocate new (like buying new socks because you can't find any)
                PoolBuffer::new(bucket_capacity, Some(idx))
            }
            None => {
                // 🧠 Requested capacity exceeds max bucket (>1MB). Allocate without pooling.
                // These are rare — only happens if a source returns unusually large pages.
                // They'll be freed normally on Drop (bucket_index = None).
                PoolBuffer::new(min_capacity, None)
            }
        }
    }

    /// 🔄 Return a Vec<u8> to the pool for reuse.
    ///
    /// 🧠 Two-tier return:
    /// 1. TLS cache (if not full) — fast, no contention
    /// 2. Shared pool (if not full) — brief lock
    /// 3. If both full, just drop it — the pool self-regulates memory usage
    fn return_vec(&self, data: Vec<u8>, bucket_index: usize) {
        if bucket_index >= NUM_BUCKETS {
            return; // -- 💀 Invalid bucket. This buffer has lost its way, like a GPS in a tunnel.
        }

        // -- 🔄 Phase 1: Try to return to TLS (like putting the remote back on the couch)
        // 🧠 The closure takes ownership of `data` and returns Some(data) if TLS is full,
        // or None if successfully stored. This avoids the moved-value-after-closure problem.
        let overflow = TLS_CACHE.with(|cache| {
            let mut buckets = cache.borrow_mut();
            if buckets[bucket_index].len() < TLS_CAPACITY_PER_BUCKET {
                buckets[bucket_index].push(data);
                None
            } else {
                Some(data)
            }
        });

        let Some(data) = overflow else {
            return;
        };

        // -- 🔄 Phase 2: TLS full, try shared pool (like putting overflow in the garage)
        let mut shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        if !shared.is_empty() && shared[bucket_index].len() < SHARED_MAX_PER_BUCKET {
            shared[bucket_index].push(data);
        }
        // 🧠 If shared pool is also full, `data` drops here. That's fine — the pool
        // naturally sheds excess capacity. No leak, no problem. The heap allocator
        // accepts returns without a receipt. 🧾
    }
}

/// 🧮 Determine which bucket a requested capacity maps to.
///
/// 🧠 Algorithm: round up to next power-of-2, then clamp to bucket range.
/// Uses bit manipulation (leading_zeros) for O(1) bucket selection.
/// Returns (Some(bucket_index), bucket_capacity) for poolable sizes,
/// or (None, min_capacity) for oversized requests.
///
/// Examples:
///   1000 bytes → bucket 0 (4 KiB) — small JSON docs
///   5000 bytes → bucket 1 (8 KiB) — typical ES scroll page
///   100_000 bytes → bucket 5 (128 KiB) — large bulk response
///   2_000_000 bytes → (None, 2MB) — too big, allocate directly
#[inline]
fn bucket_for_capacity(min_capacity: usize) -> (Option<usize>, usize) {
    if min_capacity == 0 {
        return (Some(0), 1 << MIN_BUCKET_SHIFT);
    }

    // 🧠 next_power_of_two gives us the smallest power-of-2 >= min_capacity.
    // Then we compute the shift (log2) to get the bucket index.
    let rounded = min_capacity.next_power_of_two();
    let shift = rounded.trailing_zeros();

    if shift < MIN_BUCKET_SHIFT {
        // -- 📦 Requested less than 4KB — just use the smallest bucket. Waste not, want not.
        (Some(0), 1 << MIN_BUCKET_SHIFT)
    } else if shift > MAX_BUCKET_SHIFT {
        // -- 🐋 Whale of a request. Too big for the pool. Swim free, noble buffer.
        (None, min_capacity)
    } else {
        let index = (shift - MIN_BUCKET_SHIFT) as usize;
        (Some(index), rounded)
    }
}

// ═══════════════════════════════════════════════════════════════
//  🌐 Public API — the doors to the lending library
// ═══════════════════════════════════════════════════════════════

/// 🔧 Initialize the buffer pool. Call once at startup before any workers run.
/// Idempotent — safe to call multiple times, only initializes once.
pub fn init_pool() {
    BUFFER_POOL.ensure_initialized();
}

/// 🚀 Rent a buffer from the pool with at least `min_capacity` bytes of space.
///
/// The returned PoolBuffer automatically returns to the pool when dropped.
/// No manual return needed — just let it go out of scope, like your hopes
/// of zero-copy parsing in a UTF-8 world.
///
/// # Example
/// ```ignore
/// let mut buf = kvx::buffer_pool::rent(65_536); // 64 KiB buffer
/// buf.push_str("{\"hello\":\"world\"}");
/// // buf auto-returns to pool when dropped
/// ```
#[inline]
pub fn rent(min_capacity: usize) -> PoolBuffer {
    BUFFER_POOL.rent(min_capacity)
}

/// 🚀 Rent a buffer pre-initialized from a String.
///
/// Takes ownership of the String's backing allocation and assigns it to a bucket.
/// On Drop, the buffer returns to the pool for reuse. The String gives up its
/// identity and becomes Just Another Buffer. Like a witness protection program for bytes.
pub fn rent_from_string(s: String) -> PoolBuffer {
    let bytes = s.into_bytes();
    let cap = bytes.capacity();
    let (bucket_index, _) = bucket_for_capacity(cap);
    PoolBuffer {
        data: bytes,
        bucket_index,
    }
}

// 🦆 — a duck, because every file needs one, and this one lives by the pool

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_one_where_buckets_make_sense() {
        // -- 🧪 Verify bucket selection rounds up to power-of-2 correctly
        assert_eq!(bucket_for_capacity(0), (Some(0), 4096));
        assert_eq!(bucket_for_capacity(1), (Some(0), 4096));
        assert_eq!(bucket_for_capacity(4096), (Some(0), 4096));
        assert_eq!(bucket_for_capacity(4097), (Some(1), 8192));
        assert_eq!(bucket_for_capacity(8192), (Some(1), 8192));
        assert_eq!(bucket_for_capacity(65536), (Some(4), 65536));
        assert_eq!(bucket_for_capacity(1_048_576), (Some(8), 1_048_576));
        // -- 🐋 Too big for the pool
        assert_eq!(bucket_for_capacity(1_048_577), (None, 1_048_577));
        assert_eq!(bucket_for_capacity(2_000_000), (None, 2_000_000));
    }

    #[test]
    fn rent_return_cycle_doesnt_leak_or_explode() {
        init_pool();

        // -- 🧪 Rent, write, drop — the buffer should return to the pool
        let mut buf = rent(4096);
        buf.push_str("hello world");
        assert_eq!(buf.len(), 11);
        assert!(buf.capacity() >= 4096);
        assert_eq!(buf.as_str().unwrap(), "hello world");
        drop(buf);

        // -- 🧪 Rent again — should get a recycled buffer (same capacity, len=0)
        let buf2 = rent(4096);
        assert!(buf2.is_empty());
        assert!(buf2.capacity() >= 4096);
    }

    #[test]
    fn tls_fast_path_reuses_buffers() {
        init_pool();

        // -- 🧪 Rent and return several buffers on the same thread
        // They should accumulate in TLS and be reused
        for _ in 0..5 {
            let mut buf = rent(8192);
            buf.push_str("recycling is good for the environment");
            drop(buf);
        }

        // -- 🧪 The next rent should hit TLS (we can't directly observe this,
        // but if it works without crashing, that's a win 🎉)
        let buf = rent(8192);
        assert!(buf.capacity() >= 8192);
    }

    #[test]
    fn write_trait_works_for_reqwest_compat() {
        init_pool();

        let mut buf = rent(4096);
        // -- 🧪 Use the Write trait like reqwest's copy_to() would
        write!(buf, "{{\"index\":{{\"_id\":\"{}\"}} }}", "doc-123").unwrap();
        let s = buf.as_str().unwrap();
        assert!(s.contains("doc-123"));
    }

    #[test]
    fn from_string_preserves_content() {
        let original = String::from("I was a String once. Now I'm a buffer. Life comes at you fast.");
        let buf = PoolBuffer::from(original);
        assert_eq!(buf.as_str().unwrap(), "I was a String once. Now I'm a buffer. Life comes at you fast.");
    }

    #[test]
    fn into_vec_consumes_without_pool_return() {
        init_pool();

        let mut buf = rent(4096);
        buf.push_str("goodbye cruel pool");
        let vec = buf.into_vec();
        assert_eq!(std::str::from_utf8(&vec).unwrap(), "goodbye cruel pool");
        // -- 🧪 If Drop ran after into_vec(), it would try to return an empty buffer.
        // ManuallyDrop ensures that doesn't happen. No double-free, no panic. ✅
    }

    #[test]
    fn rent_from_string_assigns_bucket() {
        init_pool();

        let big_string = "x".repeat(10_000);
        let buf = rent_from_string(big_string);
        // -- 🧪 10,000 bytes rounds up to 16 KiB bucket (index 2)
        assert!(buf.capacity() >= 10_000);
        assert_eq!(buf.as_str().unwrap().len(), 10_000);
        // -- On drop, this should go to the 16 KiB bucket
    }

    #[test]
    fn oversized_buffer_doesnt_panic() {
        init_pool();

        // -- 🧪 Request more than max bucket (1 MiB). Should allocate without pooling.
        let mut buf = rent(2_000_000);
        buf.push_str("I'm too big for this pool. Like an elephant in a kiddie pool. 🐘");
        assert!(buf.capacity() >= 2_000_000);
        // -- Dropping this should not try to return to pool (bucket_index = None)
    }

    #[test]
    fn multithreaded_rent_return_survives() {
        init_pool();

        let handles: Vec<_> = (0..8)
            .map(|i| {
                std::thread::spawn(move || {
                    // -- 🧵 Each thread rents and returns 100 buffers
                    for j in 0..100 {
                        let mut buf = rent(4096 * (1 + (j % 4)));
                        write!(buf, "thread {} iteration {}", i, j).unwrap();
                        // -- Drop returns to pool (TLS for this thread)
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("💀 Thread panicked during buffer pool stress test");
        }
    }
}
