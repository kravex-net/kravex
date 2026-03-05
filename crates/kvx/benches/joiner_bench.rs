// ai
//! 🧵📊🚀 Joiner Benchmark Suite — "The Thread Redemption"
//!
//! It was a quiet Tuesday. The joiners were just sitting there, buffering feeds,
//! casting documents, joining payloads. Nobody knew how fast they really were.
//! Nobody *asked*. Until now.
//!
//! This benchmark answers the question: "How many MB/s and docs/s can a single
//! Joiner thread push through ch1 → buffer → manifold.join → ch2?"
//!
//! Auto-discovers all `ManifoldBackend` variants via `all_variants()` — add a new
//! manifold and it gets benched for free. Like a gym membership you actually use.
//!
//! 🦆 The duck wonders if we're benchmarking the joiner or the channel. Yes.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kvx::casts::passthrough::Passthrough;
use kvx::casts::DocumentCaster;
use kvx::manifolds::ManifoldBackend;
use kvx::workers::Joiner;
use std::hint::black_box;

// -- 📏 Doc counts to sweep — enough range to see if throughput scales linearly
// -- 1K = warm-up vibes, 10K = realistic batch, 50K = stress test energy
const DOC_COUNTS: &[usize] = &[1_000, 10_000, 50_000];

// -- 📐 Channel capacity — big enough that the sender doesn't block on the joiner
// -- but not so big that we're benchmarking malloc instead of join logic
const CHANNEL_CAPACITY: usize = 1024;

// -- 📏 Max request size for the joiner — 10 MiB, large enough that we flush on close
// -- not mid-stream, so we measure join throughput without flush chatter
const MAX_REQUEST_SIZE_BYTES: usize = 10 * 1024 * 1024;

/// 🧱 Generate N synthetic JSON feeds — pre-allocated outside the hot path.
///
/// Each doc: `{"id":42,"name":"bench_doc_42","data":"aaaa..."}` ≈ 100 bytes
/// The padding ensures we're not just benchmarking `format!("{}")` on tiny strings.
///
/// "He who generates test data inline, benchmarks allocation, not logic." — Ancient proverb 📜
fn generate_feeds(count: usize) -> Vec<String> {
    // -- 🚀 pre-size the vec because reallocation mid-generation is for amateurs
    let mut feeds = Vec::with_capacity(count);
    for i in 0..count {
        feeds.push(format!(
            r#"{{"id":{i},"name":"bench_doc_{i}","data":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}"#
        ));
    }
    feeds
}

/// 📏 Total byte size of all feeds — for Throughput::Bytes reporting.
/// Counts raw feed bytes, not post-join payload bytes, because we want to know
/// how fast the joiner *processes input*, not how big the output is. 🧮
fn total_feed_bytes(feeds: &[String]) -> u64 {
    feeds.iter().map(|f| f.len() as u64).sum()
}

/// 🚀📡 Throughput in MB/s — how fast does the joiner chew through raw feed bytes?
///
/// Iterates all ManifoldBackend variants × doc counts. Criterion plots MB/s curves.
/// If your manifold is slow, this benchmark will publicly shame it. No pressure.
fn joiner_throughput_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("joiner_throughput_bytes");

    for manifold in ManifoldBackend::all_variants() {
        for &doc_count in DOC_COUNTS {
            // -- 📦 Pre-generate feeds OUTSIDE the measured section
            let feeds = generate_feeds(doc_count);
            let total_bytes = total_feed_bytes(&feeds);

            group.throughput(Throughput::Bytes(total_bytes));
            group.bench_with_input(
                BenchmarkId::new(format!("{:?}", manifold), doc_count),
                &doc_count,
                |b, &_n| {
                    b.iter(|| {
                        // -- 🧵 Fresh channels + joiner per iteration — no stale state leaking between runs
                        let (tx1, rx1) = async_channel::bounded::<String>(CHANNEL_CAPACITY);
                        let (tx2, rx2) = async_channel::bounded::<String>(CHANNEL_CAPACITY);

                        let joiner = Joiner::new(
                            rx1,
                            tx2,
                            DocumentCaster::Passthrough(Passthrough),
                            manifold.clone(),
                            MAX_REQUEST_SIZE_BYTES,
                        );

                        // -- 🚀 Launch the joiner thread — it blocks on recv_blocking until feeds arrive
                        let the_joiner_handle = joiner.start();

                        // -- 📤 Sender thread: shove all feeds into ch1, then close
                        let feeds_clone = feeds.clone();
                        let sender_handle = std::thread::spawn(move || {
                            for feed in feeds_clone {
                                tx1.send_blocking(feed).unwrap();
                            }
                            // -- 🏁 Close ch1 — triggers joiner's final flush
                            drop(tx1);
                        });

                        // -- 📥 Main thread: drain ch2 until closed, black_box each payload
                        while let Ok(payload) = rx2.recv_blocking() {
                            black_box(payload);
                        }

                        // -- 🧹 Wait for threads to finish — clean exits only, no zombies 🧟
                        sender_handle.join().expect("💀 Sender thread panicked");
                        the_joiner_handle
                            .join()
                            .expect("💀 Joiner thread panicked")
                            .expect("💀 Joiner returned an error");
                    });
                },
            );
        }
    }
    group.finish();
}

/// 🚀🔢 Throughput in docs/s — how many feeds can the joiner process per second?
///
/// Same setup as bytes bench but with `Throughput::Elements`. Because sometimes you
/// want to know "how many docs" not "how many bytes." Both are valid life questions.
fn joiner_throughput_docs(c: &mut Criterion) {
    let mut group = c.benchmark_group("joiner_throughput_docs");

    for manifold in ManifoldBackend::all_variants() {
        for &doc_count in DOC_COUNTS {
            // -- 📦 Pre-generate feeds OUTSIDE the measured section
            let feeds = generate_feeds(doc_count);

            group.throughput(Throughput::Elements(doc_count as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("{:?}", manifold), doc_count),
                &doc_count,
                |b, &_n| {
                    b.iter(|| {
                        // -- 🧵 Fresh channels + joiner per iteration
                        let (tx1, rx1) = async_channel::bounded::<String>(CHANNEL_CAPACITY);
                        let (tx2, rx2) = async_channel::bounded::<String>(CHANNEL_CAPACITY);

                        let joiner = Joiner::new(
                            rx1,
                            tx2,
                            DocumentCaster::Passthrough(Passthrough),
                            manifold.clone(),
                            MAX_REQUEST_SIZE_BYTES,
                        );

                        let the_joiner_handle = joiner.start();

                        // -- 📤 Sender thread: feed the beast
                        let feeds_clone = feeds.clone();
                        let sender_handle = std::thread::spawn(move || {
                            for feed in feeds_clone {
                                tx1.send_blocking(feed).unwrap();
                            }
                            drop(tx1);
                        });

                        // -- 📥 Drain ch2 — every payload gets black_box'd so the optimizer
                        // -- doesn't get clever and optimize away our entire benchmark 🧠
                        while let Ok(payload) = rx2.recv_blocking() {
                            black_box(payload);
                        }

                        sender_handle.join().expect("💀 Sender thread panicked");
                        the_joiner_handle
                            .join()
                            .expect("💀 Joiner thread panicked")
                            .expect("💀 Joiner returned an error");
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, joiner_throughput_bytes, joiner_throughput_docs);
criterion_main!(benches);
