// ai
//! 🧪📊🚀 FileSource Benchmark Suite — "The Fast and the Curious: Tokyo Drift"
//!
//! It was 3am. The CI pipeline was green. The metrics dashboard was suspiciously calm.
//! And then someone asked: "but how fast is it, *really*?"
//!
//! This benchmark answers that question by pitting our buffered chunk reader (128 KiB
//! chunks + memchr SIMD scanning) against the old-school BufReader + read_line approach.
//! May the fastest I/O strategy win. Spoiler: the chunks win. They always win.
//!
//! Reports both MB/s (throughput) and docs/s (elements) for each approach.
//!
//! 🦆 The singularity will arrive before we stop benchmarking this.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kvx::backends::file::FileSource;
use kvx::backends::{CommonSourceConfig, FileSourceConfig, Source};
use std::io::Write;
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufReadExt, BufReader};

// -- 📏 benchmark parameters — 100k docs at ~100 bytes each ≈ 10 MB
// -- large enough to amortize startup cost, small enough to not bore the CI server
const NUM_DOCS: usize = 100_000;

/// 📁 Generates a temp NDJSON file with NUM_DOCS lines of fake JSON.
/// Returns (temp_file, file_size_bytes, doc_count).
///
/// Each doc looks like: `{"id":42,"name":"benchmark_doc_42","payload":"aaaa..."}` (~100 bytes)
///
/// "I generate test data, therefore I am." — René Descartes, probably 🦆
fn generate_test_file() -> (NamedTempFile, u64, usize) {
    // -- 📝 pre-compute all docs into a single buffer to avoid slow per-line writes
    let mut buf = Vec::with_capacity(NUM_DOCS * 110);
    for i in 0..NUM_DOCS {
        // -- 🧱 pad each doc to ~100 bytes with a payload field
        // -- because real NDJSON docs aren't just {"id":1}
        let _ = writeln!(
            buf,
            r#"{{"id":{i},"name":"benchmark_doc_{i}","payload":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}"#
        );
    }

    let mut tmp = NamedTempFile::new().expect("💀 Failed to create temp file for benchmark");
    tmp.write_all(&buf)
        .expect("💀 Failed to write benchmark data. The disk has opinions.");
    tmp.flush().expect("💀 Flush failed. The bytes are staging a sit-in.");

    let file_size = tmp.as_file().metadata().unwrap().len();
    (tmp, file_size, NUM_DOCS)
}

/// 🚀 Benchmark Group 1: Buffered chunk reading (the new hotness)
///
/// Uses FileSource with 128 KiB chunks + memchr SIMD scanning.
/// This is the implementation we're betting our mortgage on. 🏠
fn bench_buffered_chunk_reading(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (tmp, file_size, doc_count) = generate_test_file();
    let path = tmp.path().to_str().unwrap().to_string();

    // -- 📊 MB/s measurement — how fast can we shovel bytes?
    {
        let mut group = c.benchmark_group("file_source_throughput");
        group.throughput(Throughput::Bytes(file_size));
        // -- 🧪 20 samples to keep bench time reasonable for a 10 MB file
        group.sample_size(20);

        group.bench_with_input(
            BenchmarkId::new("buffered_128k_chunks", file_size),
            &path,
            |b, path| {
                b.to_async(&rt).iter(|| async {
                    let config = FileSourceConfig {
                        file_name: path.clone(),
                        common_config: CommonSourceConfig {
                            max_batch_size_docs: 10_000,
                            max_batch_size_bytes: 10 * 1024 * 1024,
                        },
                    };
                    let mut source = FileSource::new(config).await.unwrap();
                    // -- 🔄 drain every page until EOF. This is the full pipeline.
                    let mut pages = 0u64;
                    while let Some(_feed) = source.next_page().await.unwrap() {
                        pages += 1;
                    }
                    criterion::black_box(pages)
                });
            },
        );

        group.finish();
    }

    // -- 📊 docs/s measurement — how fast can we count lines?
    {
        let mut group = c.benchmark_group("file_source_docs_per_sec");
        group.throughput(Throughput::Elements(doc_count as u64));
        group.sample_size(20);

        group.bench_with_input(
            BenchmarkId::new("buffered_128k_chunks", doc_count),
            &path,
            |b, path| {
                b.to_async(&rt).iter(|| async {
                    let config = FileSourceConfig {
                        file_name: path.clone(),
                        common_config: CommonSourceConfig {
                            max_batch_size_docs: 10_000,
                            max_batch_size_bytes: 10 * 1024 * 1024,
                        },
                    };
                    let mut source = FileSource::new(config).await.unwrap();
                    let mut total_docs = 0usize;
                    while let Some(feed) = source.next_page().await.unwrap() {
                        // -- 🎯 count docs by counting newlines + 1 (feed has no trailing \n)
                        total_docs += feed.as_str().matches('\n').count() + 1;
                    }
                    criterion::black_box(total_docs)
                });
            },
        );

        group.finish();
    }
}

/// 🐌 Benchmark Group 2: Line-by-line BufReader baseline (the old busted)
///
/// This replicates the pre-buffered approach: tokio BufReader (8 KiB default)
/// with read_line() per line. The baseline against which our new impl flexes.
///
/// "In a world where BufReader was king... one chunk dared to be 128 KiB." 🎬
fn bench_line_by_line_reading(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (tmp, file_size, doc_count) = generate_test_file();
    let path = tmp.path().to_str().unwrap().to_string();

    // -- 📊 MB/s measurement — the old way
    {
        let mut group = c.benchmark_group("file_source_throughput");
        group.throughput(Throughput::Bytes(file_size));
        group.sample_size(20);

        group.bench_with_input(
            BenchmarkId::new("bufreader_read_line", file_size),
            &path,
            |b, path| {
                b.to_async(&rt).iter(|| async {
                    let file = tokio::fs::File::open(path).await.unwrap();
                    let reader = BufReader::new(file);
                    let mut lines = reader.lines();
                    // -- 🐌 one line at a time, like reading a novel one word at a time
                    let mut count = 0usize;
                    let mut bytes = 0usize;
                    while let Some(line) = lines.next_line().await.unwrap() {
                        bytes += line.len();
                        count += 1;
                    }
                    criterion::black_box((count, bytes))
                });
            },
        );

        group.finish();
    }

    // -- 📊 docs/s measurement — the old way
    {
        let mut group = c.benchmark_group("file_source_docs_per_sec");
        group.throughput(Throughput::Elements(doc_count as u64));
        group.sample_size(20);

        group.bench_with_input(
            BenchmarkId::new("bufreader_read_line", doc_count),
            &path,
            |b, path| {
                b.to_async(&rt).iter(|| async {
                    let file = tokio::fs::File::open(path).await.unwrap();
                    let reader = BufReader::new(file);
                    let mut lines = reader.lines();
                    let mut count = 0usize;
                    while let Some(_line) = lines.next_line().await.unwrap() {
                        count += 1;
                    }
                    criterion::black_box(count)
                });
            },
        );

        group.finish();
    }
}

// -- 🏁 "And they're off!" — every horse race announcer and every benchmark suite
criterion_group!(benches, bench_buffered_chunk_reading, bench_line_by_line_reading);
criterion_main!(benches);
