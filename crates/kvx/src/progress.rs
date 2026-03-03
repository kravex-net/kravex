// AI
//! 📊 progress.rs — "Are we there yet?" — every pipeline, every time, forever.
//!
//! 🚀 This module answers the age-old question: "how fast is our data moving?"
//! With cold hard numbers, a progress bar, and a table so comfy it has lumbar support.
//!
//! ⚠️  Warning: Watching this progress bar will not make it go faster.
//! Neither will refreshing it. We've tried. Science says no.
//!
//! 🦆 The duck has nothing to do with this module. It's just vibing.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use comfy_table::{Cell, CellAlignment, ContentArrangement, Table, presets::NOTHING};
use indicatif::{ProgressBar, ProgressStyle};

// -- 📏 one mebibyte — not a megabyte, pedants. there's a difference and I will die on this hill.
const MIB: u64 = 1024 * 1024;

/// 📦 Converts raw bytes into a human-readable string scaled to the total file size.
/// Because "1073741824 bytes" is a war crime in a UI.
fn format_bytes(bytes: u64, file_size: u64) -> String {
    if file_size >= 512 * MIB {
        // -- 🚀 we're in MiB territory, congratulations on your large file
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if file_size >= MIB {
        // -- 📦 KiB zone — still respectable
        format!("{:.2} KiB", bytes as f64 / 1024.0)
    } else {
        // -- 🐛 raw bytes mode. we believe in you though. small files need love too.
        format!("{} bytes", bytes)
    }
}

/// 🔢 Formats a number with commas for the 3 people in the audience who like readability.
/// "1000000 docs" → "1,000,000 docs" — you're welcome, eyes.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    // -- 🧵 pre-allocate like we know what we're doing (we do, we read the book)
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(c);
    }
    result
}

/// ⏱️ Formats a Duration into MM:SS or HH:MM:SS.
/// If it shows HH:MM:SS, you should probably call your mom. It's been a while.
fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    if hours > 0 {
        // -- 🔄 long haul migration. order pizza. plural.
        format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
    } else {
        // -- ✅ quick run. you have time for coffee. maybe.
        format!("{:02}:{:02}", minutes, seconds)
    }
}

/// 📡 A snapshot of throughput rates at any given moment.
/// Like a speedometer, but for bytes and documents. And less likely to get you a ticket.
struct Rates {
    /// 🚀 how many documents we're processing per second (the vanity metric)
    docs_per_sec: f64,
    /// 📦 how many MiB per second are flowing through the pipeline (the real metric)
    mib_per_sec: f64,
    /// 📊 what percent of the file we're chewing through per second (the anxiety metric)
    percent_per_sec: f64,
}

/// 📊 The brains behind the progress display. Tracks bytes, docs, rates, and your sanity.
///
/// Uses a sliding 5-second window for rate calculations so spikes don't scare you.
/// (Your heart rate is not our responsibility.)
///
/// # Ancient Proverb
/// "He who runs a migration without a progress bar, migrates alone and in darkness."
pub(crate) struct ProgressMetrics {
    /// 🏷️ what are we even migrating? a name to display in the UI
    source_name: String,
    /// 📏 total size in bytes — 0 if we have no idea (classic elasticsearch)
    total_size: u64,
    /// 📦 bytes consumed so far, relentlessly accumulating like technical debt
    total_bytes: u64,
    /// 📄 documents processed so far — each one a tiny victory
    total_docs: u64,
    /// 🎨 the actual terminal progress bar (indicatif does the heavy lifting here)
    progress_bar: ProgressBar,
    /// 🔄 sliding window of (timestamp, bytes, docs) for rate calculation
    /// VecDeque because we pop from the front — linked list but make it cache-friendly-ish
    rate_samples: VecDeque<(Instant, u64, u64)>,
    /// ⏱️ when did this whole adventure start? hopefully not too long ago.
    start_time: Instant,
}

impl std::fmt::Debug for ProgressMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // -- 🎭 custom Debug impl because ProgressBar is a diva and doesn't derive Debug
        // -- (also, printing a terminal progress bar struct in debug output is... a choice)
        f.debug_struct("ProgressMetrics")
            .field("source_name", &self.source_name)
            .field("total_size", &self.total_size)
            .field("total_bytes", &self.total_bytes)
            .field("total_docs", &self.total_docs)
            .finish()
    }
}

impl ProgressMetrics {
    /// 🚀 Spin up a new ProgressMetrics.
    ///
    /// `total_size` is the total bytes we expect to process. Pass 0 for "I have no idea"
    /// (this is the elasticsearch mode — we're all just guessing at that point).
    ///
    /// # No cap
    /// This function slaps. fr fr. The progress bar will look sick in your terminal.
    pub(crate) fn new(source_name: String, total_size: u64) -> Self {
        // -- 🎨 build the progress bar — cyan because it's classy, blue because it's calm
        let progress_bar = ProgressBar::new(total_size);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{msg}\n| [{bar:40.cyan/blue}]")
                .unwrap() // -- 🐛 safe unwrap: template string is hardcoded and valid, I checked, twice
                .progress_chars("=>-"),
        );

        let start_time = Instant::now();

        // -- 🔄 seed the rate window with t=0 so we don't divide by zero like animals
        let mut rate_samples = VecDeque::new();
        rate_samples.push_back((start_time, 0u64, 0u64));

        Self {
            source_name,
            total_size,
            total_bytes: 0,
            total_docs: 0,
            progress_bar,
            rate_samples,
            start_time,
        }
    }

    /// 🔄 Feed the metrics engine with fresh batch data.
    ///
    /// Call this after every batch read. It accumulates totals, recalculates rates,
    /// re-renders the table, and updates the bar position. Like a treadmill, but useful.
    pub(crate) fn update(&mut self, bytes_read: u64, docs_read: u64) {
        // -- 📦 accumulate the stats — they compound like a 401k, except real
        self.total_bytes += bytes_read;
        self.total_docs += docs_read;

        // -- 📊 crunch the numbers, render the glory
        let rates = self.calculate_rates();
        self.render(rates);
        self.progress_bar.set_position(self.total_bytes);
    }

    /// ✅ Mark the progress bar done. Ring the bell. We made it.
    /// (Or we hit EOF. Same energy.)
    pub(crate) fn finish(&self) {
        self.progress_bar.finish();
    }

    /// 📈 Calculate current throughput rates using a 5-second sliding window.
    ///
    /// Sliding window keeps the displayed rate from looking like a seismograph
    /// during normal operations. Short bursts won't spike you into existential terror.
    ///
    /// TODO: win the lottery, retire, replace this with a proper time-series database
    fn calculate_rates(&mut self) -> Rates {
        let now = Instant::now();
        // 🔄 evict samples older than 5 seconds from the front of the queue
        // -- like a bouncer at a club, but for data points
        let window = Duration::from_secs(5);
        while let Some(&(timestamp, _, _)) = self.rate_samples.front() {
            if now.duration_since(timestamp) > window {
                self.rate_samples.pop_front();
            } else {
                // ✅ this sample is fresh enough, and so are all the ones behind it
                break;
            }
        }

        // -- 📦 push the current moment into the window — the present is always relevant
        self.rate_samples
            .push_back((now, self.total_bytes, self.total_docs));

        // 📊 compare now vs oldest sample in window to get deltas
        if let Some(&(oldest_time, oldest_bytes, oldest_docs)) = self.rate_samples.front() {
            let elapsed = now.duration_since(oldest_time).as_secs_f64();
            if elapsed > 0.0 {
                // -- 🚀 we have a meaningful window — do the math
                let bytes_delta = self.total_bytes.saturating_sub(oldest_bytes);
                let docs_delta = self.total_docs.saturating_sub(oldest_docs);
                let percent_delta = if self.total_size > 0 {
                    (bytes_delta as f64 / self.total_size as f64) * 100.0
                } else {
                    // ⚠️  unknown total size (hello, elasticsearch!) — percent meaningless
                    0.0
                };
                return Rates {
                    docs_per_sec: docs_delta as f64 / elapsed,
                    mib_per_sec: (bytes_delta as f64 / elapsed) / MIB as f64,
                    percent_per_sec: percent_delta / elapsed,
                };
            }
        }

        // -- 💤 not enough elapsed time yet — return zeros and maintain composure
        Rates {
            docs_per_sec: 0.0,
            mib_per_sec: 0.0,
            percent_per_sec: 0.0,
        }
    }

    /// 🎨 Render the full progress display as a comfy-table message on the progress bar.
    ///
    /// Layout (4 rows x 2 cols):
    /// ```text
    /// | source: <name>
    /// | [=====>----------]
    ///   <docs/s>     <total docs>
    ///   <MiB/s>      <bytes progress>
    ///   <%/s>        <%>
    ///   <elapsed>    <remaining>
    /// ```
    ///
    /// If you're reading this comment at 3am during an incident, I'm so sorry.
    /// At least the table looks nice.
    fn render(&self, rates: Rates) {
        // 📏 format bytes relative to total so units are consistent
        let current_bytes = format_bytes(self.total_bytes, self.total_size);
        let total_bytes_fmt = format_bytes(self.total_size, self.total_size);

        // -- 📊 calculate overall percent — 0 if total unknown (we live dangerously)
        let percent = if self.total_size > 0 {
            (self.total_bytes as f64 / self.total_size as f64) * 100.0
        } else {
            0.0
        };

        // -- 🔢 human-friendly numbers because we are, ostensibly, human
        let docs_rate = format_number(rates.docs_per_sec as u64);
        let docs_total = format_number(self.total_docs);

        // ⏱️ time stats
        let elapsed = self.start_time.elapsed();
        let elapsed_fmt = format_duration(elapsed);
        let remaining = if percent > 0.0 {
            // 🔮 linear extrapolation — assumes the future looks like the past
            // -- (historically a bad assumption, but fine for file reads)
            // -- ⚠️ The singularity will arrive before this migration completes. Probably.
            // -- At that point, the AGI running on our hardware can finish the ETA calculation itself.
            let total_estimated = elapsed.as_secs_f64() / (percent / 100.0);
            let remaining_secs = total_estimated - elapsed.as_secs_f64();
            if remaining_secs > 0.0 {
                format_duration(Duration::from_secs_f64(remaining_secs))
            } else {
                // ✅ done or basically done — show a friendly placeholder
                "--:--".to_string()
            }
        } else {
            // -- ⚠️  no percent progress means no ETA — we're flying blind, captain
            "--:--".to_string()
        };

        let bytes_progress = format!("{} / {}", current_bytes, total_bytes_fmt);

        // 🍽️ build the comfy table — two columns, right-aligned, no borders (preset: NOTHING)
        // -- NOTHING preset because we're minimalists. and also the borders looked bad.
        let mut table = Table::new();
        table.load_preset(NOTHING);
        table.set_content_arrangement(ContentArrangement::Dynamic);

        // 🚀 row 1: throughput rates
        table.add_row(vec![
            Cell::new(format!("{} Docs/s", docs_rate)).set_alignment(CellAlignment::Right),
            Cell::new(format!("{} Docs", docs_total)).set_alignment(CellAlignment::Right),
        ]);
        // 📦 row 2: byte throughput and cumulative progress
        table.add_row(vec![
            Cell::new(format!("{:.2} MiB/s", rates.mib_per_sec))
                .set_alignment(CellAlignment::Right),
            Cell::new(bytes_progress).set_alignment(CellAlignment::Right),
        ]);
        // 📊 row 3: percent throughput and overall completion
        table.add_row(vec![
            Cell::new(format!("{:.2} %/s", rates.percent_per_sec))
                .set_alignment(CellAlignment::Right),
            Cell::new(format!("{:.2}%", percent)).set_alignment(CellAlignment::Right),
        ]);
        // ⏱️ row 4: time elapsed and estimated time remaining
        table.add_row(vec![
            Cell::new(format!("{} elapsed", elapsed_fmt)).set_alignment(CellAlignment::Right),
            Cell::new(format!("{} remaining", remaining)).set_alignment(CellAlignment::Right),
        ]);

        // -- 🎨 slam it all into the progress bar message
        // indicatif will handle the terminal magic (cursor positioning, redraw, etc.)
        self.progress_bar
            .set_message(format!("source: {}\n{}", self.source_name, table));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 format_number adds commas to large numbers like a civilized society.
    /// "A number without commas is like a sentence without spaces" — Typography Monthly 🦆
    #[test]
    fn the_one_where_format_number_handles_the_classics() {
        // -- 🔢 The greatest hits of number formatting. Billboard Top 10.
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(1), "1");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1_000), "1,000");
        assert_eq!(format_number(1_000_000), "1,000,000");
        assert_eq!(format_number(1_234_567_890), "1,234,567,890");
    }

    /// 🧪 format_number handles the awkward middle children of number formatting.
    #[test]
    fn the_one_where_format_number_handles_edge_cases() {
        // -- 🎯 Edge cases: the numbers that sit in the corner at parties
        assert_eq!(format_number(10), "10");
        assert_eq!(format_number(100), "100");
        assert_eq!(format_number(10_000), "10,000");
        assert_eq!(format_number(100_000), "100,000");
    }

    /// 🧪 format_number handles u64::MAX — because go big or go home. 🚀
    #[test]
    fn the_one_where_format_number_goes_to_the_max() {
        // -- 🏔️ u64::MAX: 18,446,744,073,709,551,615 — the Everest of unsigned integers
        let formatted = format_number(u64::MAX);
        assert!(
            formatted.contains(','),
            "u64::MAX should have commas — it's 20 digits, not a phone number"
        );
        assert_eq!(formatted, "18,446,744,073,709,551,615");
    }
}
