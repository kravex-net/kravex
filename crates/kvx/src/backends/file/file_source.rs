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
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use tokio::{
    fs::File,
    io::{self, AsyncBufReadExt},
};
use tracing::trace;

use crate::backends::Source;
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
/// 📂 FileSource — reads a file line by line, assembles raw `String` pages, and moves on.
///
/// Think of it like a very diligent intern who reads a massive CSV, never complains,
/// and only stops when (a) the file ends, (b) the batch is full by doc count,
/// or (c) the batch is full by byte count — whichever comes first.
///
/// The borrow checker approved this struct. It did not approve of my feelings about the borrow
/// checker. We are at an impasse.
///
/// 🧵 Async, non-blocking. The BufReader wraps a tokio `File`, so we're doing real async I/O.
/// 📊 Tracks progress via `ProgressMetrics` — bytes read, docs read, reported to a progress table.
/// ⚠️  If the file is being written to while we read it, the size estimate will be wrong.
///     This is fine. We are fine. Everything is fine. 🐛
pub(crate) struct FileSource {
    buf_reader: io::BufReader<File>,
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
    /// and returns a fully initialized `FileSource` ready to vend raw `String` pages.
    ///
    /// If the file doesn't exist: 💀 anyhow will tell you with *theatrical flair*.
    /// If metadata fails: we assume 0 bytes, progress bar shows unknown. Shrug emoji as a service.
    ///
    /// No cap: `File::open` is async here because we're in tokio-land. This is not your
    /// grandfather's `std::fs::File::open`. This is `std::fs::File::open`'s cooler younger sibling
    /// who got into the async runtime scene and never looked back.
    pub(crate) async fn new(
        source_config: FileSourceConfig,
        max_batch_size_bytes: usize,
        max_batch_size_docs: usize,
    ) -> Result<Self> {
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

        let buf_reader = io::BufReader::new(file_handle);

        // 🚀 spin up the progress metrics — source name is the file path, honest and boring.
        // KNOWLEDGE GRAPH: file_name is used as the "source name" label in the progress table.
        // It's the human-readable handle. Keep it meaningful — it shows up in the TUI.
        let progress = ProgressMetrics::new(source_config.file_name.clone(), file_size);

        Ok(Self {
            buf_reader,
            source_config,
            max_batch_size_docs,
            max_batch_size_bytes,
            progress,
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
    /// 🧠 Knowledge graph: sources return `Option<String>` — one raw page of newline-delimited
    /// content, uninterpreted. The source accumulates lines up to byte/doc caps and returns
    /// the whole thing as a single String. The Composer downstream splits and transforms.
    ///
    /// KNOWLEDGE GRAPH: two exit conditions exist beyond EOF —
    ///   1. `max_batch_size_docs`: line count cap. Don't build a page the size of Texas.
    ///   2. `max_batch_size_bytes`: byte cap. Protects against memory-busting accumulation.
    /// Both are checked on every iteration. Whichever fires first wins.
    ///
    /// "He who reads the entire file into one String, OOMs in production." — Ancient proverb 📜
    async fn pump(&mut self, doc_count_hint: usize) -> Result<Option<String>> {
        // 🎛️ Apply the controller's batch size hint — merged from the old set_page_size_hint()
        self.max_batch_size_docs = doc_count_hint;
        let mut page = String::with_capacity(self.max_batch_size_bytes);
        let mut total_bytes_read = 0usize;
        let mut line_count = 0usize;
        // ⚠️ 1MB initial capacity per line — because NDJSON documents can be chunky.
        let mut line = String::with_capacity(1024 * 1024);

        for _ in 0..=self.max_batch_size_docs {
            let bytes_read = self.buf_reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                break;
            }

            total_bytes_read += bytes_read;
            // 🧹 Strip trailing newlines from each line before appending to the page.
            // read_line includes \n (and \r\n on Windows). We strip so the page is clean NDJSON.
            let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
            if !trimmed.is_empty() {
                // 🔗 Separate lines with \n — but no trailing newline on the last line.
                // The Composer handles final formatting. We just build the raw page.
                if !page.is_empty() {
                    page.push('\n');
                }
                page.push_str(trimmed);
                line_count += 1;
            }
            line.clear();

            if total_bytes_read > self.max_batch_size_bytes {
                break;
            }

            if line_count >= self.max_batch_size_docs {
                break;
            }
        }

        trace!(
            "📖 hauled {} bytes out of the file like a digital fishing trip — catch of the day",
            total_bytes_read
        );
        self.progress
            .update(total_bytes_read as u64, line_count as u64);

        // 📄 Empty page = EOF. The well is dry. Return None. 🏁
        if page.is_empty() {
            Ok(None)
        } else {
            Ok(Some(page))
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
}
