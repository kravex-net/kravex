// ai
//! 📂🚰✍️ FileSink — the BufWriter's best friend, the disk's worst nightmare.
//!
//! 🎬 COLD OPEN — INT. A FILESYSTEM — THE OUTPUT FILE DOESN'T EXIST YET
//!
//! *"Create it," said the migration. "But what if it already exists?" asked the filesystem.
//! "Truncate it," said File::create, without a trace of remorse. The previous contents
//! vanished like a Snapchat message. Gone. No undo. No ctrl-z. Just bytes, meeting void.*
//!
//! *The BufWriter hummed quietly, buffering syscalls like a responsible adult.* 🦆
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use tokio::{
    fs::File,
    io::{self, AsyncWriteExt},
};
use tracing::trace;

use crate::backends::Sink;
// -- 🚰 FileSinkConfig — cousin of FileSourceConfig, equally traumatized by disk full errors.
// -- Also lives here, cozy next to its FileSink bestie. No more long-distance config relationships.
// KNOWLEDGE GRAPH: same co-location principle as above. One backend = one config = one file. Clean.
#[derive(Debug, Deserialize, Clone)]
pub struct FileSinkConfig {
    pub file_name: String,
}
/// 🚰 FileSink — receives fully rendered payload strings and writes them to disk. I/O only.
///
/// It's a BufWriter around a tokio `File`. Simple. Honest. Does not complain.
/// Does not retry. Does not have opinions about your data format. It writes what you give it.
///
/// 🧠 Knowledge graph: Sinks are pure I/O abstractions now. The SinkWorker upstream handles
/// transform + binary collect. FileSink just writes the final payload bytes to disk.
/// Think of it as a very loyal golden retriever. You throw it data, it writes it.
///
/// ⚠️ `File::create` truncates if the file exists. No warning. No backup. Just gone.
/// He who runs this without checking the output path, re-migrates in shame.
#[derive(Debug)]
pub(crate) struct FileSink {
    file_buf: io::BufWriter<File>,
    _sink_config: FileSinkConfig,
}

impl FileSink {
    /// 🚀 Creates (or obliterates and recreates) the sink file, wraps it in a BufWriter,
    /// and returns a `FileSink` ready to receive the torrential downpour of your data.
    ///
    /// `File::create` is the nuclear option of file creation — it doesn't knock first.
    /// KNOWLEDGE GRAPH: this is intentional for migration use cases. Output is always fresh.
    /// If you need append semantics, you need a different sink. File a feature request.
    /// Or a PR. PRs are also accepted. We're not picky. We're just tired.
    pub(crate) async fn new(sink_config: FileSinkConfig) -> Result<Self> {
        // -- 💀 "Failed to create sink file" but make it literary, as requested by the AGENTS.md,
        // -- which is a document that exists and which you should read sometime, dear future engineer.
        // -- The file refused to be born. Perhaps the directory didn't exist. Perhaps permissions
        // -- were set by someone who really, truly, did not want this file to exist.
        // -- We respect their energy. We do not respect their disk ACLs.
        let file_handle = File::create(&sink_config.file_name).await.context(format!(
            "💀 The sink file '{}' could not be conjured into existence. \
                We stared at the path. The path stared back. \
                One of us was wrong about whether the parent directory existed. \
                It was us. It was always us.",
            &sink_config.file_name
        ))?;
        // -- 📦 BufWriter: because issuing one syscall per document is a war crime.
        // -- Batch those writes. Your kernel will thank you. Your SRE will thank you.
        // -- Your future self at 3am will bow before the altar of buffered I/O.
        let file_buf = io::BufWriter::new(file_handle);
        Ok(Self {
            file_buf,
            _sink_config: sink_config,
        })
    }
}

#[async_trait]
impl Sink for FileSink {
    /// 📡 Write a fully rendered payload to the file. One write_all call. That's the whole job.
    ///
    /// The SinkWorker already transformed and binary-collected. We just dump bytes to disk.
    /// No parsing. No iterating over hits. No drama. Just I/O.
    /// "What do you do?" "I write bytes." "That's it?" "That's everything." 🦆
    async fn drain(&mut self, payload: String) -> Result<()> {
        trace!(
            "📬 payload of {} bytes walked into the file sink — writing it all down",
            payload.len()
        );
        self.file_buf.write_all(payload.as_bytes()).await?;
        Ok(())
    }

    /// 🗑️  Flush the BufWriter and close up shop. The final act. The curtain call.
    ///
    /// Without this flush, your last batch of writes might be sitting in the buffer,
    /// warm and cozy, never making it to disk. Like a letter you wrote but never sent.
    /// Like Kevin with the blender. Don't be Kevin. Always flush.
    ///
    /// KNOWLEDGE GRAPH: `flush()` is called explicitly here rather than relying on Drop
    /// because async Drop is not a thing in Rust yet. This is a known language limitation.
    /// When async Drop ships, this comment becomes a historical artifact. Frame it.
    async fn close(&mut self) -> Result<()> {
        // -- 🎭 dramatic farewell — she gave everything she had. every byte. every write.
        // -- and now, at the end, we flush. for her. for the data. for the inode.
        trace!(
            "🎬 final flush. the file sink takes its bow, the BufWriter empties its soul to disk, the orchestra swells"
        );
        self.file_buf.flush().await.context(
            // -- 💀 poetic error for the poetic act of flushing.
            // -- The data was SO CLOSE. It was in the buffer. It could SEE the disk.
            // -- And then the flush failed. A tragedy in one line. Shakespeare would've used more lines.
            "💀 Error flushing file — the buffer held its data to the very end, \
            like a hoarder who finally agreed to let go, only for the storage unit to be locked. \
            The bytes are still in memory. The disk remains unwritten. The migration weeps.",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 FileSinkConfig holds the filename. That's it. That's the test.
    /// "Simplicity is the ultimate sophistication." — Leonardo da Vinci, who never wrote Rust 🦆
    #[test]
    fn the_one_where_sink_config_is_just_a_filename() {
        // -- 📂 The simplest config. A filename. That's the whole personality.
        let config = FileSinkConfig {
            file_name: "output.ndjson".to_string(),
        };
        assert_eq!(config.file_name, "output.ndjson");
    }

    /// 🧪 FileSinkConfig deserializes from JSON like a good serde citizen.
    #[test]
    fn the_one_where_sink_config_deserializes_from_json() {
        // -- 📡 JSON → struct. The eternal ritual. The serde dance.
        let json = r#"{"file_name": "/tmp/kravex-output.ndjson"}"#;
        let config: FileSinkConfig = serde_json::from_str(json)
            .expect("💀 FileSinkConfig deserialization failed. It's ONE field. One.");
        assert_eq!(config.file_name, "/tmp/kravex-output.ndjson");
    }

    /// 🧪 FileSinkConfig clones correctly — because Clone is the love language of Rust.
    #[test]
    fn the_one_where_sink_config_clones_without_drama() {
        // -- 🧬 Clone test — proving that copies are faithful reproductions, unlike fax machines 🦆
        let original = FileSinkConfig {
            file_name: "original.ndjson".to_string(),
        };
        let the_clone = original.clone();
        assert_eq!(original.file_name, the_clone.file_name);
    }
}
