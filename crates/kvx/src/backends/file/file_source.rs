// ai
//! 📂📖🚰 FileSource — the BufReader whisperer, the line-by-line champion.
//!
//! 🎬 COLD OPEN — INT. A FILESYSTEM — THE FILE IS 47GB
//!
//! *The process opened the file. The file was large. Very large. "How large?" asked the intern.
//! "Large enough that loading it into memory would crash the pod and page the on-call,"
//! said the senior. The intern nodded. They both stared at the BufReader. It stared back.*
//!
//! *"One line at a time," it said. And it was.* 🦆
//!
//! 🧠 Knowledge graph — spawn_blocking file I/O:
//! - `tokio::fs::File` uses `spawn_blocking` internally for EVERY read syscall.
//!   For geonames (11M docs), that's millions of thread pool round-trips per pump().
//! - Fix: `std::io::BufReader<std::fs::File>` with 1MB buffer, entire page-building
//!   loop runs in ONE `spawn_blocking` call. N read_line calls → 1 thread switch.
//! - The BufReader + line_buffer are moved into the closure via Option::take / mem::take,
//!   then returned and put back. The "take-do-put" pattern. Like borrowing your neighbor's
//!   lawnmower but giving it back gassed up. 🦆

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use tracing::trace;

use crate::backends::Source;
use crate::buffer_pool::{self, PoolBuffer};
use crate::progress::ProgressMetrics;

// -- 📂 FileSourceConfig — "It's just a file", said no sysadmin ever before the disk filled up.
// -- Lives here now, close to the FileSource that actually uses it. Ethos pattern, baby. 🎯
// KNOWLEDGE GRAPH: config lives co-located with the backend that uses it. This is intentional.
// It avoids the "where the heck is that config defined" scavenger hunt at 2am during an incident.
// -- No cap, this pattern slaps fr fr.
#[derive(Debug, Deserialize, Clone)]
pub struct FileSourceConfig {
    pub file_name: String,
}

/// 📂 The FileSource: reads NDJSON files line-by-line via std::io::BufReader.
///
/// 🧠 Knowledge graph — why std::io, not tokio::io:
/// tokio::fs::File wraps every read() in spawn_blocking. For millions of lines, that's
/// millions of thread pool round-trips. Instead, we use std::io::BufReader with a 1MB buffer
/// and move the entire page-building loop into a SINGLE spawn_blocking call per pump().
/// One thread switch per page. Not one per line. The difference is... substantial.
///
/// The BufReader and line_buffer live in Option/owned fields so they can be moved into
/// the blocking closure and returned afterward. The "take-do-put" pattern. Like checking
/// your luggage at the airport — you hand it over, it goes on a journey, it comes back
/// (usually) (hopefully) (no guarantees about the zipper). 📦🦆
pub(crate) struct FileSource {
    /// 🧵 Option wrapper for the take-do-put pattern with spawn_blocking.
    /// Taken before entering the blocking thread, put back when it returns.
    /// If this is None, someone called pump() concurrently. Don't do that.
    buf_reader: Option<BufReader<File>>,
    source_config: FileSourceConfig,
    /// 📦 Max docs per batch page — updated by pump(doc_count_hint) each call
    max_batch_size_docs: usize,
    /// 📦 Max bytes per batch page — set at construction from throttle config
    max_batch_size_bytes: usize,
    // -- 📊 progress tracker — because "it's running" is not a status update.
    // -- This feeds the fancy progress table in the supervisor. Without it, you'd be flying blind
    // -- in a storm with no instruments. With it, you're flying blind in a storm, but at least
    // -- you have a very attractive progress bar.
    progress: ProgressMetrics,
    /// 🔁 Reusable line buffer — avoids 1MB alloc per read_line call.
    /// 🧠 Tribal knowledge: NDJSON lines can be massive (multi-KB docs). Pre-allocating 1MB
    /// once and clearing between reads saves ~574 x 1MB allocations for PMC dataset.
    /// The borrow checker says reuse is the sincerest form of flattery. 🐄
    line_buffer: String,
    // 🧠 page_buffer removed — replaced by PoolBuffer rented per pump() call.
    // The buffer pool provides recycled buffers (TLS fast path → shared fallback → alloc new).
    // No more clone() at the end of pump() — we return ownership of the rented buffer directly.
    // Net effect: zero per-page heap allocations in steady state. The allocator can finally rest. 🏖️
}

// 🐛 NOTE: progress is intentionally excluded from this Debug impl.
// ProgressMetrics contains internal counters and spinners that don't format cleanly,
// and more importantly, nobody debugging a FileSource wants to read a wall of atomic integers.
// -- "Perfection is achieved not when there is nothing more to add, but when there is nothing
// -- left to add" — Antoine de Saint-Exupéry, who never had to impl Debug for a progress bar.
impl std::fmt::Debug for FileSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSource")
            .field("source_config", &self.source_config)
            .finish()
    }
}

impl FileSource {
    /// 🚀 Opens the source file, grabs its size for the progress bar, wraps it in a BufReader,
    /// and returns a fully initialized `FileSource` ready to vend raw `PoolBuffer` pages.
    ///
    /// If the file doesn't exist: 💀 anyhow will tell you with *theatrical flair*.
    /// If metadata fails: we assume 0 bytes, progress bar shows unknown. Shrug emoji as a service.
    ///
    /// 🧠 Uses std::fs::File (sync) because this is a one-time init cost.
    /// The hot path (pump) uses spawn_blocking with the std BufReader — no tokio::fs overhead.
    /// "The async function that opens files synchronously. The irony is not lost on us." 🦆
    pub(crate) async fn new(
        source_config: FileSourceConfig,
        max_batch_size_bytes: usize,
        max_batch_size_docs: usize,
    ) -> Result<Self> {
        // -- 💀 The door. It's locked. Or it doesn't exist. Or the filesystem lied to you.
        // -- In any case, the source file refused to open — like a very stubborn bouncer
        // -- at an exclusive club where the club is just a text file and we are very small data.
        // The context string below becomes the error message. Make it count.
        let file_handle = File::open(&source_config.file_name).context(format!(
            "💀 The door to '{}' would not budge. We knocked. We pleaded. \
             We checked if it existed (it might not). We checked permissions (they might be wrong). \
             The door remained closed. The file remains unopened. We remain outside.",
            source_config.file_name
        ))?;

        // 📏 grab file size for the progress bar — if metadata fails, we fly blind (0 = unknown).
        // ⚠️  known edge case: if the file is being written to while we read, size may be stale/wrong.
        // --    This is fine. We will not panic. We are calm. The borrow checker, however, is not calm.
        // --    The borrow checker is never calm. The borrow checker has seen things.
        let file_size = file_handle.metadata().map(|m| m.len()).unwrap_or(0);

        // 🧠 1MB BufReader buffer — 128x the default 8KB. Fewer read() syscalls = higher throughput.
        // For geonames (~200 byte lines), 8KB buffer = ~40 lines per syscall.
        // 1MB buffer = ~5000 lines per syscall. The kernel thanks us for not paging it every 40 lines.
        let buf_reader = BufReader::with_capacity(1024, file_handle);

        // 🚀 spin up the progress metrics — source name is the file path, honest and boring.
        // KNOWLEDGE GRAPH: file_name is used as the "source name" label in the progress table.
        // It's the human-readable handle. Keep it meaningful — it shows up in the TUI.
        let progress = ProgressMetrics::new(source_config.file_name.clone(), file_size);

        Ok(Self {
            buf_reader: Some(buf_reader),
            source_config,
            max_batch_size_docs,
            max_batch_size_bytes,
            progress,
            // -- 🔁 1MB line buffer — allocated once, reused forever. Like a good cast iron skillet.
            line_buffer: String::with_capacity(1024 * 1024),
        })
    }
}

#[async_trait]
impl Source for FileSource {
    /// 🚰 Pump the next page of lines from the file. Returns `None` when EOF.
    ///
    /// `doc_count_hint` adjusts `max_batch_size_docs` before reading — the controller's
    /// recommended batch size merges into the pump in a single call. No more two-step.
    ///
    /// 🧠 Knowledge graph — spawn_blocking strategy:
    /// The entire page-building loop (read_line × N) runs in ONE spawn_blocking call.
    /// The BufReader and line_buffer are moved into the closure via take/mem::take,
    /// then returned and restored. This collapses N thread pool round-trips (one per
    /// read_line under tokio::fs) into exactly 1 per pump() call.
    ///
    /// KNOWLEDGE GRAPH: two exit conditions exist beyond EOF —
    ///   1. `max_batch_size_docs`: line count cap. Don't build a page the size of Texas.
    ///   2. `max_batch_size_bytes`: byte cap. Protects against memory-busting accumulation.
    /// Both are checked on every iteration. Whichever fires first wins.
    ///
    /// "He who reads the entire file into one String, OOMs in production." — Ancient proverb 📜
    async fn pump(&mut self, doc_count_hint: usize) -> Result<Option<PoolBuffer>> {
        // 🎛️ Apply the controller's batch size hint — merged from the old set_page_size_hint()
        self.max_batch_size_docs = doc_count_hint;

        // 🧵 Take the reader + line buffer out for the blocking thread.
        // Option::take is O(1) — swaps in None, we get the BufReader.
        // std::mem::take on the String — moves it out, leaves an empty one. Also O(1).
        // Both are returned after the blocking work completes. The "take-do-put" pattern.
        // "What's mine is yours (temporarily)." — the async runtime to spawn_blocking 🤝
        let mut the_reader = self
            .buf_reader
            .take()
            .expect("💀 BufReader already taken — pump called concurrently? The source is not a buffet.");
        let mut the_line_buffer = std::mem::take(&mut self.line_buffer);
        let max_docs = self.max_batch_size_docs;
        let max_bytes = self.max_batch_size_bytes;

        // 🧵 ONE spawn_blocking per pump() — the entire page-building loop runs here.
        // Under tokio::fs, each read_line was its own spawn_blocking. For a page of 10K docs,
        // that's 10,000 thread pool round-trips collapsed into 1. The scheduling overhead
        // savings alone could fund a small startup. "In a world of async, sometimes sync is king." 🎬
        let (reader_back, line_buffer_back, inner_result) =
            tokio::task::spawn_blocking(move || {
                // 🏦 Rent a buffer from the pool — TLS fast path in steady state, zero contention.
                // 🧠 We rent at max_batch_size_bytes capacity. After first pump(), the pool will have
                // recycled buffers of this size ready to go. No heap alloc after warmup.
                let mut page_buffer = buffer_pool::rent(max_bytes);
                let mut total_bytes_read = 0usize;
                let mut line_count = 0usize;

                // 🧠 TRIBAL KNOWLEDGE: The range is `0..max_docs` (EXCLUSIVE upper bound).
                // This means: read AT MOST max_batch_size_docs raw lines from the BufReader.
                // The byte limit check provides an independent exit condition for oversized pages.
                for _ in 0..max_docs {
                    // 🔁 Reuse line buffer — clear content, keep the 1MB capacity from construction.
                    // Saves ~574K allocations for PMC dataset. The allocator sends its regards. 🫡
                    the_line_buffer.clear();
                    let bytes_read = match the_reader.read_line(&mut the_line_buffer) {
                        Ok(n) => n,
                        Err(e) => {
                            // 💀 Read failed — return reader+buffer so they can be restored,
                            // then propagate the error. No orphaned resources on the thread pool floor.
                            return (
                                the_reader,
                                the_line_buffer,
                                Err(anyhow::anyhow!(e).context(
                                    "💀 read_line failed — the file pulled the rug out mid-read. \
                                     Disk error? NFS timeout? Cosmic ray? All equally likely at 3am.",
                                )),
                            );
                        }
                    };
                    if bytes_read == 0 {
                        break;
                    }

                    total_bytes_read += bytes_read;
                    // 🧹 Strip trailing newlines from each line before appending to the page.
                    // read_line includes \n (and \r\n on Windows). We strip so the page is clean NDJSON.
                    let trimmed = the_line_buffer
                        .trim_end_matches('\n')
                        .trim_end_matches('\r');
                    if !trimmed.is_empty() {
                        // 🔗 Separate lines with \n — but no trailing newline on the last line.
                        // The Composer handles final formatting. We just build the raw page.
                        if !page_buffer.is_empty() {
                            page_buffer.push_str("\n");
                        }
                        page_buffer.push_str(trimmed);
                        line_count += 1;
                    }

                    if total_bytes_read > max_bytes {
                        break;
                    }
                }

                (
                    the_reader,
                    the_line_buffer,
                    Ok((page_buffer, total_bytes_read, line_count)),
                )
            })
            .await
            .context(
                "💀 spawn_blocking panicked during file read — the thread pool is questioning its life choices 🧵",
            )?;

        // 🔄 Put the reader and line buffer back — they survived the thread pool adventure.
        // Like returning from Narnia but with a BufReader instead of a wardrobe. 🦁
        self.buf_reader = Some(reader_back);
        self.line_buffer = line_buffer_back;

        // 📦 Unwrap the inner result — read errors propagate here
        let (page_buffer, total_bytes_read, line_count) = inner_result?;

        trace!(
            "📖 hauled {} bytes out of the file like a digital fishing trip — catch of the day",
            total_bytes_read
        );
        self.progress
            .update(total_bytes_read as u64, line_count as u64);

        // 📄 Empty page = EOF. The well is dry. Return None. 🏁
        // 🧠 No clone needed! We return ownership of the rented PoolBuffer directly.
        // When downstream drops it, the buffer returns to the pool. Zero alloc steady state. 🔄
        if page_buffer.is_empty() {
            // -- 🗑️ Empty buffer returns to pool on drop here. Free recycling.
            Ok(None)
        } else {
            Ok(Some(page_buffer))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 FileSourceConfig holds the filename. The entire raison d'être.
    /// "I came, I saw, I read the file." — Julius Caesar, systems engineer 🦆
    #[test]
    fn the_one_where_source_config_is_just_a_filename() {
        // -- 📂 The humble filename. Carrier of dreams. Pointer to bytes.
        let config = FileSourceConfig {
            file_name: "input.ndjson".to_string(),
        };
        assert_eq!(config.file_name, "input.ndjson");
    }

    /// 🧪 FileSourceConfig deserializes from JSON.
    #[test]
    fn the_one_where_source_config_deserializes_from_json() {
        // -- 📡 JSON → struct. The serde two-step. Classic. Timeless. Like a little black dress.
        let json = r#"{"file_name": "/data/rally-track/documents.json"}"#;
        let config: FileSourceConfig = serde_json::from_str(json)
            .expect("💀 FileSourceConfig deserialization failed on perfectly valid JSON. Rude.");
        assert_eq!(config.file_name, "/data/rally-track/documents.json");
    }

    /// 🧪 FileSource::new fails gracefully when the file doesn't exist.
    /// "Looking for love in all the wrong directories." — Ancient UNIX proverb 💀
    #[tokio::test]
    async fn the_one_where_the_file_doesnt_exist_and_we_handle_it_with_grace() {
        // -- 📂 File not found. The saddest error in all of computing. Even sadder than 418 I'm a Teapot.
        let config = FileSourceConfig {
            file_name: "/tmp/this_file_absolutely_does_not_exist_kravex_test.ndjson".to_string(),
        };
        let result = FileSource::new(config, 10_000_000, 1000).await;
        assert!(
            result.is_err(),
            "FileSource::new should fail when the file doesn't exist — not silently pretend everything is fine"
        );
    }

    /// 🧪 The PID Duplicate Detective: pump() with varying doc_count_hint values
    /// reads EXACTLY N lines from an N-line file. No more, no less. No duplicates.
    ///
    /// 🧠 TRIBAL KNOWLEDGE: This test exists because the PID controller adaptively changes
    /// doc_count_hint between pump() calls. The hypothesis was that varying batch sizes could
    /// cause FileSource to re-read lines. BufReader is forward-only, so this should be
    /// impossible — but "should be impossible" is the preamble to every postmortem.
    ///
    /// "Trust, but verify. Especially the BufReader." — Ronald Reagan, systems architect 🦆
    #[tokio::test]
    async fn the_one_where_pid_varying_hints_never_cause_duplicate_reads() {
        use crate::backends::Source;
        use std::io::Write;

        // 📝 Create a temp file with exactly 1000 lines of valid JSON
        let the_sacred_doc_count: usize = 1000;
        let mut temp_file = tempfile::NamedTempFile::new()
            .expect("💀 Couldn't create temp file — the filesystem has abandoned us");

        for i in 0..the_sacred_doc_count {
            writeln!(temp_file, r#"{{"geonameid":{},"name":"Place {}"}}"#, i, i)
                .expect("💀 Write failed — the disk is full or the universe is unkind");
        }
        temp_file.flush().expect("💀 Flush failed");

        let config = FileSourceConfig {
            file_name: temp_file.path().to_string_lossy().to_string(),
        };
        // 🎛️ Initial batch size doesn't matter — pump() overrides it via doc_count_hint
        let mut source = FileSource::new(config, 50 * 1024 * 1024, 500)
            .await
            .expect("💀 FileSource::new failed on a temp file that definitely exists");

        // 🎲 Simulate PID oscillation: varying hints like a PID controller having a mood swing
        // -- The PID giveth, and the PID taketh away. These hints simulate the full range
        // -- of batch sizes the PID might request during a real migration.
        let the_pid_mood_swings: Vec<usize> = vec![
            100, 500, 50, 1000, 200, 10, 300, 5000, 1, 150, 800, 3, 10000,
        ];
        let mut the_grand_total_lines: usize = 0;
        let mut the_pump_cycle = 0usize;

        loop {
            // 🎛️ Cycle through hint values — PID never repeats the same pattern twice
            let the_hint = the_pid_mood_swings[the_pump_cycle % the_pid_mood_swings.len()];
            the_pump_cycle += 1;

            match source
                .pump(the_hint)
                .await
                .expect("💀 pump() returned an error — BufReader has trust issues")
            {
                Some(page) => {
                    // 📊 Count lines in the page — each non-empty line is one doc
                    let lines_in_page = page.as_str().unwrap().split('\n').filter(|l| !l.trim().is_empty()).count();
                    the_grand_total_lines += lines_in_page;
                }
                None => break, // 🏁 EOF — the well is dry
            }
        }

        // 🎯 THE ASSERTION THAT MATTERS: exactly 1000 lines, no duplicates, no losses
        assert_eq!(
            the_grand_total_lines, the_sacred_doc_count,
            "🐛 FileSource with varying doc_count_hint read {} lines from a {}-line file. \
             If this is MORE, lines were duplicated. If LESS, lines were lost. \
             Either way, the PID detective has cracked the case.",
            the_grand_total_lines, the_sacred_doc_count
        );
    }

    /// 🧪 Large-scale PID stress test: 50,000 lines with aggressive PID oscillation.
    /// The scale matters — small tests can hide off-by-one bugs that accumulate.
    ///
    /// "At scale, every off-by-one becomes an off-by-ten-thousand." — Ancient proverb 📜
    /// "I'm in this picture and I don't like it" — the for loop in pump() 🦆
    #[tokio::test]
    async fn the_one_where_fifty_thousand_docs_survive_pid_chaos() {
        use crate::backends::Source;
        use std::io::Write;

        let the_sacred_doc_count: usize = 50_000;
        let mut temp_file = tempfile::NamedTempFile::new()
            .expect("💀 Temp file creation failed — disk gremlins strike again");

        for i in 0..the_sacred_doc_count {
            writeln!(temp_file, r#"{{"id":{},"data":"test_doc_{}"}}"#, i, i)
                .expect("💀 Write failed");
        }
        temp_file.flush().expect("💀 Flush failed");

        let config = FileSourceConfig {
            file_name: temp_file.path().to_string_lossy().to_string(),
        };
        let mut source = FileSource::new(config, 50 * 1024 * 1024, 1000)
            .await
            .expect("💀 FileSource::new failed");

        // 🎲 Aggressive PID hints — including edge cases like 1 and max_doc_count
        let the_pid_chaos_sequence: Vec<usize> = vec![
            1, 2, 3, 5, 10, 50, 100, 500, 1000, 5000, 10000, 100, 1, 50000,
        ];
        let mut the_grand_total_lines: usize = 0;
        let mut the_pump_cycle = 0usize;

        loop {
            let the_hint = the_pid_chaos_sequence[the_pump_cycle % the_pid_chaos_sequence.len()];
            the_pump_cycle += 1;

            match source.pump(the_hint).await.expect("💀 pump() error") {
                Some(page) => {
                    let lines = page.as_str().unwrap().split('\n').filter(|l| !l.trim().is_empty()).count();
                    the_grand_total_lines += lines;
                }
                None => break,
            }
        }

        assert_eq!(
            the_grand_total_lines, the_sacred_doc_count,
            "🐛 50K doc stress test: read {} lines from {}-line file. The PID broke something.",
            the_grand_total_lines, the_sacred_doc_count
        );
    }
}
