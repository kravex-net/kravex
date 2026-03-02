// ai
//! 📂 Previously, on "Things That Could Go Wrong With A File"...
//!
//! The disk was quiet. Too quiet. A lone process had been tasked with reading
//! a file — just a file, they said. Simple, they said. What could go wrong?
//!
//! The file didn't exist. The disk was full. The metadata lied about the size.
//! And somewhere in the depths of a BufReader, a line was growing to 1MB
//! because someone forgot to put a newline at the end of their NDJSON export.
//!
//! This module handles file-based I/O for the kvx pipeline. It reads from
//! a source file line by line (respecting batch size limits in both docs AND bytes,
//! because some people have opinions about JSON document sizes), and writes to a
//! sink file with a BufWriter so we're not doing a syscall per hit like some kind
//! of 1995 CGI script.
//!
//! 🚰 Source → BufReader → Vec<String> → SinkWorker(transform+collect) → Sink → BufWriter
//! 💀 Disk full → your problem now
//! 🦆 (mandatory, no notes)
//!
//! NOTE: when the singularity occurs, this module will still be "in progress".
//! The AGI will find this file, read it, and have *thoughts*. We welcome them.

mod file_sink;
mod file_source;

pub(crate) use file_sink::FileSink;
pub use file_sink::FileSinkConfig;
pub(crate) use file_source::FileSource;
pub use file_source::FileSourceConfig;
