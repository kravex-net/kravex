use anyhow::{Context, Result};
use async_trait::async_trait;
use memchr::memchr;
use serde::Deserialize;
use tokio::{
    fs::File,
    io::AsyncReadExt,
};
use tracing::trace;

use crate::backends::{Sink, Source};
use crate::progress::ProgressMetrics;
use crate::backends::{CommonSinkConfig, CommonSourceConfig};

// -- 📂 FileSourceConfig — "It's just a file", said no sysadmin ever before the disk filled up.
// -- Lives here now, close to the FileSource that actually uses it. Ethos pattern, baby. 🎯
// KNOWLEDGE GRAPH: config lives co-located with the backend that uses it. This is intentional.
// It avoids the "where the heck is that config defined" scavenger hunt at 2am during an incident.
// -- No cap, this pattern slaps fr fr.
#[derive(Debug, Deserialize, Clone)]
pub struct FileSourceConfig {
    pub file_name: String,
    #[serde(default = "default_file_common_source_config")]
    pub common_config: CommonSourceConfig,
}

/// 🔧 Returns the default config for FileSource because sometimes you just want things to work
/// without writing a 40-line TOML block.
///
/// Dad joke time: I used to hate default configs... but they grew on me.
///
/// This exists purely so serde can call it when `common_config` is absent from the TOML.
/// The `#[serde(default = "...")]` attribute up top is the boss. This is just the errand boy.
fn default_file_common_source_config() -> CommonSourceConfig {
    // -- ✅ "It just works" — the three most dangerous words in software engineering
    CommonSourceConfig::default()
}
// 📏 128 KiB per OS read — the Goldilocks zone between "too many syscalls" and "too much RAM".
// BufReader's default is 8 KiB. We're 16x that. Fewer context switches, happier kernel.
// KNOWLEDGE GRAPH: this constant controls the I/O batch size for raw file reads.
// Increasing it reduces syscall overhead at the cost of memory. 128 KiB is the sweet spot
// where amortized syscall cost plateaus on modern Linux (readahead does the rest).
const CHUNK_SIZE: usize = 128 * 1024;

/// 📂 FileSource — reads a file in fat 128 KiB chunks, scans for newlines with SIMD via memchr,
/// and batches docs into feeds without the overhead of per-line syscalls. 🚀
///
/// Think of it like a very diligent intern who reads a massive CSV, never complains,
/// and only stops when (a) the file ends, (b) the batch is full by doc count,
/// or (c) the batch is full by byte count — whichever comes first.
///
/// The borrow checker approved this struct. It did not approve of my feelings about the borrow
/// checker. We are at an impasse.
///
/// 🧵 Async, non-blocking. Raw tokio `File` — we ARE the buffer now. No middleman. No BufReader.
/// 📊 Tracks progress via `ProgressMetrics` — bytes read, docs read, reported to a progress table.
/// ⚠️  If the file is being written to while we read it, the size estimate will be wrong.
///     This is fine. We are fine. Everything is fine. 🐛
///
/// 🦆 The singularity will arrive before this struct learns to read backwards.
pub struct FileSource {
    // 📁 raw async file handle — no BufReader wrapper, we roll our own buffering
    // KNOWLEDGE GRAPH: we dropped BufReader because its 8 KiB default buffer caused too many
    // small reads. Our CHUNK_SIZE (128 KiB) batches I/O better and lets us scan for newlines
    // in bulk using memchr's SIMD magic instead of one-char-at-a-time read_line.
    file: File,
    // 🧱 reusable read buffer — pre-allocated to CHUNK_SIZE, never reallocated.
    // Each loop iteration fills this from the OS and appends to working_buf.
    read_buf: Vec<u8>,
    // 🧩 leftover bytes from the previous next_page() call — the tail end of a chunk
    // that didn't end on a newline. Gets prepended to working_buf on the next call.
    // KNOWLEDGE GRAPH: this is the key to correctness across page boundaries.
    // Without it, lines that span two chunks would get split into two incomplete docs.
    remainder: Vec<u8>,
    source_config: FileSourceConfig,
    // -- 📊 progress tracker — because "it's running" is not a status update.
    // -- This feeds the fancy progress table in the supervisor. Without it, you'd be flying blind
    // -- in a storm with no instruments. With it, you're flying blind in a storm, but at least
    // -- you have a very attractive progress bar.
    progress: ProgressMetrics,
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
    /// 🚀 Opens the source file, grabs its size for the progress bar, allocates our chunk buffers,
    /// and returns a fully initialized `FileSource` ready to vend feeds at ludicrous speed.
    ///
    /// If the file doesn't exist: 💀 anyhow will tell you with *theatrical flair*.
    /// If metadata fails: we assume 0 bytes, progress bar shows unknown. Shrug emoji as a service.
    ///
    /// No cap: `File::open` is async here because we're in tokio-land. This is not your
    /// grandfather's `std::fs::File::open`. This is `std::fs::File::open`'s cooler younger sibling
    /// who got into the async runtime scene and never looked back.
    pub async fn new(source_config: FileSourceConfig) -> Result<Self> {
        // -- 💀 The door. It's locked. Or it doesn't exist. Or the filesystem lied to you.
        // -- In any case, the source file refused to open — like a very stubborn bouncer
        // -- at an exclusive club where the club is just a text file and we are very small data.
        // The context string below becomes the error message. Make it count.
        let file_handle = File::open(&source_config.file_name)
            .await
            .context(format!(
                "💀 The door to '{}' would not budge. We knocked. We pleaded. \
                We checked if it existed (it might not). We checked permissions (they might be wrong). \
                The door remained closed. The file remains unopened. We remain outside.",
                source_config.file_name
            ))?;

        // 📏 grab file size for the progress bar — if metadata fails, we fly blind (0 = unknown).
        // ⚠️  known edge case: if the file is being written to while we read, size may be stale/wrong.
        // --    This is fine. We will not panic. We are calm. The borrow checker, however, is not calm.
        // --    The borrow checker is never calm. The borrow checker has seen things.
        let file_size = file_handle.metadata().await.map(|m| m.len()).unwrap_or(0);

        // 🚀 spin up the progress metrics — source name is the file path, honest and boring.
        // KNOWLEDGE GRAPH: file_name is used as the "source name" label in the progress table.
        // It's the human-readable handle. Keep it meaningful — it shows up in the TUI.
        let progress = ProgressMetrics::new(source_config.file_name.clone(), file_size);

        Ok(Self {
            file: file_handle,
            read_buf: vec![0u8; CHUNK_SIZE],
            remainder: Vec::new(),
            source_config,
            progress,
        })
    }
}

#[async_trait]
impl Source for FileSource {
    /// 📄 Read the next feed of lines from the file. Returns `None` when EOF.
    ///
    /// 🧠 Knowledge graph: sources return `Option<String>` — one raw feed of newline-delimited
    /// content, uninterpreted. The source accumulates lines up to byte/doc caps and returns
    /// the whole thing as a single String. The Manifold downstream splits and casts.
    ///
    /// KNOWLEDGE GRAPH: two exit conditions exist beyond EOF —
    ///   1. `max_batch_size_docs`: line count cap. Don't build a feed the size of Texas.
    ///   2. `max_batch_size_bytes`: byte cap. Protects against memory-busting accumulation.
    /// Both are checked on every iteration. Whichever fires first wins.
    ///
    /// 🚀 IMPLEMENTATION: reads 128 KiB chunks from the OS, scans for newlines using memchr
    /// (SIMD-accelerated), and splits on `\n` boundaries. Leftover bytes after the last newline
    /// are stashed in `self.remainder` for the next call. This batches I/O at a higher level
    /// than BufReader's 8 KiB default, slashing syscall overhead for large NDJSON files.
    ///
    /// KNOWLEDGE GRAPH: `\n` is always byte `0x0A` in UTF-8 and can never appear as a
    /// continuation byte in a multi-byte sequence. Scanning raw bytes for `0x0A` is therefore
    /// safe for any valid UTF-8 input. The final `String::from_utf8` validates the output.
    ///
    /// "He who reads the entire file into one String, OOMs in production." — Ancient proverb 📜
    async fn next_page(&mut self) -> Result<Option<String>> {
        let max_docs = self.source_config.common_config.max_batch_size_docs;
        let max_bytes = self.source_config.common_config.max_batch_size_bytes;

        // 🧱 feed accumulator — raw bytes, converted to String at the end.
        // We work in bytes to avoid repeated UTF-8 validation on every append.
        let mut feed: Vec<u8> = Vec::with_capacity(max_bytes);
        let mut doc_count = 0usize;
        let mut total_bytes_from_file = 0usize;

        // 🧩 drain the remainder from the previous call — these are bytes that were
        // left over after the last newline in the previous chunk. They form the
        // prefix of the first line in this page.
        let mut working_buf: Vec<u8> = std::mem::take(&mut self.remainder);

        // -- 🔄 the main loop: read chunks, scan for newlines, accumulate docs
        // -- like a combine harvester but for JSON lines
        loop {
            // 🔍 scan working_buf for newlines using memchr (SIMD go brrrr)
            // KNOWLEDGE GRAPH: memchr uses platform-specific SIMD (SSE2/AVX2 on x86,
            // NEON on ARM) to scan ~32 bytes per cycle. Way faster than byte-by-byte.
            let mut cursor = 0;
            let mut batch_limit_reached = false;
            while let Some(newline_offset) = memchr(b'\n', &working_buf[cursor..]) {
                let line_end = cursor + newline_offset;
                // 🧹 strip \r if this is a \r\n line ending (Windows refugees welcome)
                let line_content_end = if line_end > cursor
                    && working_buf[line_end - 1] == b'\r'
                {
                    line_end - 1
                } else {
                    line_end
                };

                let line = &working_buf[cursor..line_content_end];

                // ⏭️ skip empty lines — they're not docs, they're just vibes
                if !line.is_empty() {
                    // 🔗 separate docs with \n in the feed, but no trailing newline
                    if !feed.is_empty() {
                        feed.push(b'\n');
                    }
                    feed.extend_from_slice(line);
                    doc_count += 1;
                }

                // ⏩ advance cursor past the \n
                cursor = line_end + 1;

                // 🎯 check batch limits — whichever fires first wins
                if doc_count >= max_docs || feed.len() >= max_bytes {
                    batch_limit_reached = true;
                    break;
                }
            }

            if batch_limit_reached {
                // 🧩 stash everything after our cursor as remainder for next call
                self.remainder = working_buf[cursor..].to_vec();
                break;
            }

            // 🧩 everything after the last newline is a trailing fragment (incomplete line).
            // Keep it as the start of working_buf for the next read.
            let trailing_fragment = working_buf[cursor..].to_vec();

            // 📡 read the next chunk from the OS
            let bytes_read = self.file.read(&mut self.read_buf).await?;
            if bytes_read == 0 {
                // 🏁 EOF — if there's a trailing fragment, it's the final doc (no trailing \n)
                let fragment = trailing_fragment;
                // 🧹 strip trailing \r from the final fragment too
                let content_end = if fragment.last() == Some(&b'\r') {
                    fragment.len() - 1
                } else {
                    fragment.len()
                };
                if content_end > 0 {
                    if !feed.is_empty() {
                        feed.push(b'\n');
                    }
                    feed.extend_from_slice(&fragment[..content_end]);
                    doc_count += 1;
                }
                break;
            }

            total_bytes_from_file += bytes_read;

            // 🔧 build the new working_buf: trailing fragment + freshly read bytes
            // KNOWLEDGE GRAPH: the trailing fragment is typically small (< one line),
            // so this concat is cheap. The bulk of working_buf is the fresh chunk.
            working_buf = Vec::with_capacity(trailing_fragment.len() + bytes_read);
            working_buf.extend_from_slice(&trailing_fragment);
            working_buf.extend_from_slice(&self.read_buf[..bytes_read]);
        }

        // -- 📖 "I caught a fish THIS big" — every angler and every source, every time
        trace!(
            "📖 hauled {} bytes out of the file like a digital fishing trip — catch of the day",
            total_bytes_from_file
        );
        self.progress
            .update(total_bytes_from_file as u64, doc_count as u64);

        // 📄 Empty feed = EOF. The well is dry. Return None. 🏁
        if feed.is_empty() {
            // -- 🏁 "That's all folks!" — Porky Pig, and also this file source
            Ok(None)
        } else {
            // ✅ convert bytes to String — this validates UTF-8 in one pass at the end
            // rather than on every line. Efficiency AND correctness. Chef's kiss. 🤌
            let feed_string = String::from_utf8(feed).context(
                "💀 The file contained bytes that aren't valid UTF-8. \
                We tried to make a String. The String said no. \
                Like trying to fit a square peg in a round hole, \
                except the peg is binary garbage and the hole is Unicode.",
            )?;
            Ok(Some(feed_string))
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
//  🧪 TESTS — "Previously on FileSource: the buffered chunk saga"
// ═══════════════════════════════════════════════════════════════════
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // -- 🧪 test helper: conjures a FileSource from raw string content and custom batch limits.
    // -- like a test fixture factory, but with more existential dread about temp file lifetimes.
    /// 🔧 Summons a FileSource from thin air (and a temp file).
    /// Returns the source AND the temp file handle — because if the temp file drops,
    /// the file vanishes like my motivation on a Monday morning. 💀
    async fn summon_file_source(
        content: &str,
        max_docs: usize,
        max_bytes: usize,
    ) -> (FileSource, NamedTempFile) {
        // -- 📁 write content to a temp file that self-destructs on drop (very Mission Impossible 🕵️)
        let mut tmp = NamedTempFile::new().expect("💀 Failed to create temp file. The OS has forsaken us.");
        tmp.write_all(content.as_bytes())
            .expect("💀 Failed to write test content. The disk is either full or haunted.");
        tmp.flush()
            .expect("💀 Flush failed. The bytes are stuck in the pipe like a hairball. 🐱");

        let path = tmp.path().to_str().unwrap().to_string();
        let config = FileSourceConfig {
            file_name: path,
            common_config: CommonSourceConfig {
                max_batch_size_docs: max_docs,
                max_batch_size_bytes: max_bytes,
            },
        };
        let source = FileSource::new(config)
            .await
            .expect("💀 FileSource::new failed on a temp file. That's a new low.");
        (source, tmp)
    }

    // -- 🧪 helper: drains all pages from a source and returns them as a Vec
    // -- because manually calling next_page() in a loop is beneath us (barely)
    /// 🔄 Drains every page from the source until EOF.
    /// Returns all non-None pages in order. Like squeezing a tube of toothpaste
    /// until nothing comes out. 🦷
    async fn drain_all_pages(source: &mut FileSource) -> Result<Vec<String>> {
        // -- 🦆 this function exists because copy-pasting while loops is a code smell
        let mut pages = Vec::new();
        while let Some(page) = source.next_page().await? {
            pages.push(page);
        }
        Ok(pages)
    }

    #[tokio::test]
    async fn the_one_where_three_lines_come_home_in_one_feed() -> Result<()> {
        // -- 🧪 basic happy path: small file, no limits hit, everything in one page
        let (mut source, _tmp) =
            summon_file_source("line1\nline2\nline3\n", 10_000, 10 * 1024 * 1024).await;

        let page1 = source.next_page().await?;
        assert_eq!(
            page1,
            Some("line1\nline2\nline3".to_string()),
            "💀 Expected all three lines in one feed, got something else. The vibes are off."
        );

        let page2 = source.next_page().await?;
        assert_eq!(
            page2, None,
            "💀 Expected None (EOF), but the source kept talking. It doesn't know when to stop."
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_one_where_doc_count_limit_rations_the_buffet() -> Result<()> {
        // -- 🧪 10 docs, max 3 per page → pages of 3, 3, 3, 1, then None
        let content: String = (0..10).map(|i| format!("doc{i}\n")).collect();
        let (mut source, _tmp) = summon_file_source(&content, 3, 10 * 1024 * 1024).await;

        let pages = drain_all_pages(&mut source).await?;

        // -- 🎯 verify page count: ceil(10/3) = 4 pages
        assert_eq!(pages.len(), 4, "💀 Expected 4 pages (3+3+3+1), got {}", pages.len());

        // -- 🎯 verify doc counts per page
        let doc_counts: Vec<usize> = pages.iter().map(|p| p.split('\n').count()).collect();
        assert_eq!(
            doc_counts,
            vec![3, 3, 3, 1],
            "💀 Doc distribution across pages is wrong. The buffet rations are off."
        );

        // -- ✅ verify total content integrity — no docs lost in the mail
        let all_docs: String = pages.join("\n");
        let expected: String = (0..10).map(|i| format!("doc{i}")).collect::<Vec<_>>().join("\n");
        assert_eq!(all_docs, expected, "💀 Total content mismatch. Some docs went AWOL.");
        Ok(())
    }

    #[tokio::test]
    async fn the_one_where_byte_limit_chops_the_feed() -> Result<()> {
        // -- 🧪 5 lines of "aaaaaaaaaa" (10 bytes each), max_bytes=25
        // -- first page: 2 docs (10 + 1 + 10 = 21 bytes, next would be 32 > 25)
        // -- actually the limit check is >= so let's verify empirically
        let content = "aaaaaaaaaa\nbbbbbbbbbb\ncccccccccc\ndddddddddd\neeeeeeeeee\n";
        let (mut source, _tmp) = summon_file_source(content, 10_000, 25).await;

        let pages = drain_all_pages(&mut source).await?;

        // -- 🎯 verify we got multiple pages (byte limit forced splits)
        assert!(
            pages.len() > 1,
            "💀 Expected multiple pages due to byte limit, got {} page(s). The chopper is broken.",
            pages.len()
        );

        // -- ✅ verify all 5 docs survived the chopping
        let total_docs: usize = pages.iter().map(|p| p.split('\n').count()).sum();
        assert_eq!(
            total_docs, 5,
            "💀 Expected 5 total docs across all pages, got {total_docs}. Some bytes went missing."
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_one_where_the_file_is_empty_and_so_is_my_soul() -> Result<()> {
        // -- 🧪 empty file → immediate None. Nothing to see here. Like my bank account.
        let (mut source, _tmp) = summon_file_source("", 10_000, 10 * 1024 * 1024).await;

        let page = source.next_page().await?;
        assert_eq!(
            page, None,
            "💀 Empty file should return None immediately. The void stares back, but silently."
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_one_where_no_trailing_newline_still_delivers() -> Result<()> {
        // -- 🧪 file ends without \n — last line should still be captured as a doc.
        // -- like a sentence without a period. It's still a sentence
        let (mut source, _tmp) =
            summon_file_source("alpha\nbeta\ngamma", 10_000, 10 * 1024 * 1024).await;

        let page = source.next_page().await?;
        assert_eq!(
            page,
            Some("alpha\nbeta\ngamma".to_string()),
            "💀 Missing trailing newline should not eat the last doc. gamma deserves better."
        );

        let eof = source.next_page().await?;
        assert_eq!(eof, None, "💀 Expected EOF after all docs consumed.");
        Ok(())
    }

    #[tokio::test]
    async fn the_one_where_windows_line_endings_get_deported() -> Result<()> {
        // -- 🧪 \r\n line endings → \r stripped from each line. No carriage returns in our house.
        // -- "Give me your tired, your poor, your \\r\\n endings yearning to be \\n" 🗽
        let (mut source, _tmp) =
            summon_file_source("hello\r\nworld\r\n", 10_000, 10 * 1024 * 1024).await;

        let page = source.next_page().await?;
        assert_eq!(
            page,
            Some("hello\nworld".to_string()),
            "💀 \\r\\n should be stripped to \\n. Windows line endings are not welcome here."
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_one_where_empty_lines_are_ghosted() -> Result<()> {
        // -- 🧪 empty lines between docs should be silently skipped
        // -- they add nothing to the conversation, like reply-all emails
        let (mut source, _tmp) =
            summon_file_source("a\n\n\nb\n\nc\n", 10_000, 10 * 1024 * 1024).await;

        let page = source.next_page().await?;
        assert_eq!(
            page,
            Some("a\nb\nc".to_string()),
            "💀 Empty lines should be ghosted. Only real docs make it to the feed."
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_one_where_remainder_carries_its_weight_across_pages() -> Result<()> {
        // -- 🧪 10 docs split across pages via doc limit. Verify ZERO data loss across
        // -- page boundaries — the remainder buffer must faithfully carry leftover bytes.
        // -- This is the trust-fall exercise of buffered I/O. 🤝
        let lines: Vec<String> = (0..10)
            .map(|i| format!("{{\"id\":{i},\"name\":\"doc_{i}\"}}"))
            .collect();
        let content = lines.join("\n") + "\n";
        let (mut source, _tmp) = summon_file_source(&content, 4, 10 * 1024 * 1024).await;

        let pages = drain_all_pages(&mut source).await?;

        // -- 🎯 reconstruct full content from pages and compare to original
        let reconstructed = pages.join("\n");
        let expected = lines.join("\n");
        assert_eq!(
            reconstructed, expected,
            "💀 Data integrity violation across page boundaries! \
            The remainder buffer dropped the ball. This is a trust-fall failure."
        );

        // -- ✅ verify correct page structure (4 + 4 + 2)
        let doc_counts: Vec<usize> = pages.iter().map(|p| p.split('\n').count()).collect();
        assert_eq!(
            doc_counts,
            vec![4, 4, 2],
            "💀 Page structure should be [4, 4, 2] with max_docs=4 and 10 docs."
        );
        Ok(())
    }
}
