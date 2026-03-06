// ai
//! 🎬 *[a payload arrives on ch2. the drainer doesn't flinch.]*
//! *[it sends. if rejected, it waits. it tries again. patience of a saint with a retry budget.]*
//! *["I used to give up on the first failure," it whispers.]*
//! *["Now I have exponential backoff. And honestly? Therapy helped too."]* 🗑️🚀🦆
//!
//! 📦 The Drainer — async I/O worker that receives assembled payloads from ch2
//! and sends them to the sink. Now with retry logic, because even data deserves second chances.
//!
//! ```text
//! Joiner(s) (std::thread) → ch2 → Drainer(s) (tokio::spawn) → Sink (HTTP/file/memory)
//!                                                                  ↻ retry with backoff
//! ```
//!
//! 🧠 Knowledge graph: the Drainer was once a complex beast that buffered raw feeds,
//! cast them via DocumentCaster, joined them via Manifold, AND sent them to the sink.
//! That CPU-bound work now lives in the Joiner (on std::thread). The Drainer has been
//! liberated. It is now a thin async relay with retry armor: recv payload → send to sink
//! → if rejected, back off exponentially → retry → repeat. Like a polite debt collector. 📬
//!
//! ⚠️ The singularity will drain data at the speed of light. We drain at the speed of HTTP,
//! plus occasional exponential naps.

use super::Worker;
use super::DrainerConfig;
use crate::Payload;
use crate::backends::{Sink, SinkBackend};
use anyhow::{Context, Result};
use async_channel::Receiver;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// 🗑️ The Drainer: async relay from ch2 to sink, now with retry superpowers.
///
/// Receives pre-assembled payload Strings from joiners via ch2,
/// sends them to the sink with exponential backoff on failure.
/// Like a postman who delivers, gets the door slammed in his face,
/// waits politely, and tries again. 📬
///
/// 📜 Lifecycle:
/// 1. **Recv**: assembled payload String from ch2 (async)
/// 2. **Send**: payload → Sink::send (HTTP POST, file write, memory push)
///    - On failure: exponential backoff → retry up to max_retries
///    - On exhaustion: propagate error (pipeline dies with dignity) 💀
/// 3. **Repeat** until ch2 closes (all joiners done)
/// 4. **Close**: Sink::close — flush and finalize 🦆
#[derive(Debug)]
pub struct Drainer {
    /// 📥 ch2 receiver — assembled payloads from the joiner thread pool
    rx: Receiver<Payload>,
    /// 🚰 The final destination — where payloads go to live their best life (or die trying)
    sink: SinkBackend,
    /// 🔄 Retry configuration — how persistent are we when the sink says "nah"?
    retry_config: DrainerConfig,
}

impl Drainer {
    /// 🏗️ Construct a Drainer — a receiver, a sink, and the patience of a retry config. 🚰
    ///
    /// "Give a drainer a payload, it sends for a millisecond.
    ///  Give a drainer retries, it sends until the heat death of the universe." — Ancient proverb 🦆
    pub fn new(rx: Receiver<Payload>, sink: SinkBackend, retry_config: DrainerConfig) -> Self {
        Self { rx, sink, retry_config }
    }
}

/// 🔄 Send a payload to the sink with exponential backoff retries.
///
/// Clones the payload before each attempt because sink.send() consumes it —
/// like handing someone your only copy of a document and hoping they don't
/// shred it. We make photocopies. We're not animals. 📋
///
/// Backoff formula: min(initial_ms * multiplier^attempt, max_ms)
/// Attempt 0: initial_ms. Attempt 1: initial_ms * mult. Attempt 2: initial_ms * mult².
/// It's like compound interest, but for suffering. 📈🦆
async fn drain_with_retry(
    sink: &mut (impl Sink + ?Sized),
    the_payload: Payload,
    config: &DrainerConfig,
) -> Result<()> {
    // 🎯 Total attempts = 1 initial + max_retries
    let the_total_attempts = config.max_retries + 1;
    let mut the_last_error = None;

    for my_therapist_says_move_on in 0..the_total_attempts {
        // 📋 Clone the payload for this attempt — send() consumes it like a black hole eats light
        let the_payload_clone = the_payload.clone();

        match sink.send(the_payload_clone).await {
            Ok(()) => return Ok(()),
            Err(the_rejection) => {
                // 💀 The sink said no. Like my college applications all over again.
                the_last_error = Some(the_rejection);

                // 🏁 If this was our last attempt, don't bother sleeping — just accept fate
                if my_therapist_says_move_on + 1 >= the_total_attempts {
                    break;
                }

                // 📈 Calculate backoff: initial_ms * multiplier^attempt, capped at max_ms
                let the_exponential_dread = (config.initial_backoff_ms as f64)
                    * config.backoff_multiplier.powi(my_therapist_says_move_on as i32);
                let the_actual_nap_ms = (the_exponential_dread as u64).min(config.max_backoff_ms);

                warn!(
                    "⚠️ Drainer send failed (attempt {}/{}), backing off {}ms before retry — {}",
                    my_therapist_says_move_on + 1,
                    the_total_attempts,
                    the_actual_nap_ms,
                    the_last_error.as_ref().unwrap(),
                );

                // 💤 Sleep it off. Like a failed deployment, sometimes you just need time.
                tokio::time::sleep(std::time::Duration::from_millis(the_actual_nap_ms)).await;
            }
        }
    }

    // 💀 All attempts exhausted. The sink has rejected us completely.
    // Like sending 4 texts and getting no reply. Time to accept it.
    Err(the_last_error.unwrap()).context(format!(
        "💀 Drainer exhausted all {} retry attempts — the sink said 'no' {} times. \
         The payload was assembled with care by a joiner thread. The sink was unmoved. \
         Like writing a heartfelt cover letter and getting an automated rejection.",
        config.max_retries + 1,
        config.max_retries + 1,
    ))
}

impl Worker for Drainer {
    fn start(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            debug!("📥 Drainer started — recv from ch2 → send to sink (with retry armor)");

            loop {
                match self.rx.recv().await {
                    Ok(the_payload) => {
                        debug!("📄 Drainer received {} byte payload from ch2", the_payload.len());

                        // 📡 Send the assembled payload to the sink, with retries.
                        // Skip empty payloads — the joiner should filter these, but belt AND suspenders 🩳
                        if !the_payload.is_empty() && *the_payload != "[]" {
                            drain_with_retry(&mut self.sink, the_payload, &self.retry_config)
                                .await
                                .context(
                                    "💀 Drainer gave up on payload after all retries — the I/O layer \
                                     said 'nah' repeatedly. Like asking someone out multiple times. \
                                     At some point you have to take the hint.",
                                )?;
                        }
                    }
                    Err(_) => {
                        // 🏁 ch2 closed — all joiners are done. Close the sink and exit.
                        debug!("🏁 Drainer: ch2 closed. All joiners done. Closing sink. Goodnight. 💤");
                        self.sink
                            .close()
                            .await
                            .context("💀 Drainer failed to close sink — the farewell was awkward")?;
                        return Ok(());
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use async_trait::async_trait;

    /// 🧪 A sink that fails N times then succeeds — like a vending machine
    /// that needs exactly 3 kicks before dispensing your snack. 🦆
    #[derive(Debug)]
    struct FlakyTestSink {
        /// 💀 How many times to fail before finally cooperating
        the_failures_remaining: Arc<AtomicUsize>,
        /// ✅ Payloads that actually made it through the gauntlet
        the_survivors: Vec<String>,
    }

    impl FlakyTestSink {
        fn new(fail_count: usize) -> Self {
            Self {
                the_failures_remaining: Arc::new(AtomicUsize::new(fail_count)),
                the_survivors: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl Sink for FlakyTestSink {
        async fn send(&mut self, payload: Payload) -> Result<()> {
            // 💀 Fail if we still have failures to give
            let the_remaining = self.the_failures_remaining.load(Ordering::SeqCst);
            if the_remaining > 0 {
                self.the_failures_remaining.fetch_sub(1, Ordering::SeqCst);
                anyhow::bail!(
                    "💀 FlakyTestSink: {the_remaining} failure(s) left — the vending machine demands more kicks"
                );
            }
            // ✅ Finally cooperating
            self.the_survivors.push(payload.0);
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// 🧪 A sink that ALWAYS fails — like applying to FAANG with a 2-week bootcamp cert. 🦆
    #[derive(Debug)]
    struct AlwaysFailSink;

    #[async_trait]
    impl Sink for AlwaysFailSink {
        async fn send(&mut self, _payload: Payload) -> Result<()> {
            anyhow::bail!("💀 AlwaysFailSink: I reject all payloads on principle. Nothing personal.")
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// 🧪 Fast config for tests — no one wants to wait for real backoff in CI 🏎️
    fn test_config(max_retries: usize) -> DrainerConfig {
        DrainerConfig {
            max_retries,
            initial_backoff_ms: 1,
            backoff_multiplier: 1.0,
            max_backoff_ms: 1,
        }
    }

    #[tokio::test]
    async fn the_one_where_the_drainer_succeeds_on_first_try() {
        // 🧪 No failures — drain_with_retry should succeed immediately, like ordering pizza online
        let mut the_sink = FlakyTestSink::new(0);
        let the_payload = Payload::from("test payload".to_string());
        let the_config = test_config(3);

        let honestly_who_knows = drain_with_retry(&mut the_sink, the_payload, &the_config).await;
        assert!(honestly_who_knows.is_ok(), "🎯 First-try success should just work");
        assert_eq!(the_sink.the_survivors.len(), 1);
        assert_eq!(the_sink.the_survivors[0], "test payload");
    }

    #[tokio::test]
    async fn the_one_where_the_drainer_retries_and_eventually_wins() {
        // 🧪 Fail twice, succeed on third attempt — like parallel parking
        let mut the_sink = FlakyTestSink::new(2);
        let the_payload = Payload::from("persistent payload".to_string());
        let the_config = test_config(3);

        let honestly_who_knows = drain_with_retry(&mut the_sink, the_payload, &the_config).await;
        assert!(honestly_who_knows.is_ok(), "🎯 Should succeed after retries");
        assert_eq!(the_sink.the_survivors.len(), 1);
        assert_eq!(the_sink.the_survivors[0], "persistent payload");
    }

    #[tokio::test]
    async fn the_one_where_the_drainer_exhausts_all_retries_and_gives_up() {
        // 🧪 More failures than retries — the payload was doomed from the start
        let mut the_sink = AlwaysFailSink;
        let the_payload = Payload::from("doomed payload".to_string());
        let the_config = test_config(2);

        let honestly_who_knows = drain_with_retry(&mut the_sink, the_payload, &the_config).await;
        assert!(honestly_who_knows.is_err(), "💀 Should fail after exhausting retries");
        let the_error_msg = format!("{}", honestly_who_knows.unwrap_err());
        assert!(the_error_msg.contains("exhausted"), "🎯 Error should mention exhaustion");
    }

    #[tokio::test]
    async fn the_one_where_zero_retries_means_one_shot() {
        // 🧪 max_retries=0 means exactly 1 attempt, no retries. YOLO mode.
        let mut the_sink = FlakyTestSink::new(1);
        let the_payload = Payload::from("one shot payload".to_string());
        let the_config = test_config(0);

        let honestly_who_knows = drain_with_retry(&mut the_sink, the_payload, &the_config).await;
        assert!(honestly_who_knows.is_err(), "💀 Zero retries = one attempt, one failure, one sadness");
    }

    #[tokio::test]
    async fn the_one_where_the_drainer_succeeds_on_the_last_possible_attempt() {
        // 🧪 Fail exactly max_retries times, succeed on the final attempt — peak drama
        let mut the_sink = FlakyTestSink::new(3);
        let the_payload = Payload::from("clutch payload".to_string());
        let the_config = test_config(3);

        let honestly_who_knows = drain_with_retry(&mut the_sink, the_payload, &the_config).await;
        assert!(honestly_who_knows.is_ok(), "🎯 Should succeed on the last attempt — main character energy");
        assert_eq!(the_sink.the_survivors[0], "clutch payload");
    }

    #[tokio::test]
    async fn the_one_where_empty_payloads_are_skipped_not_retried() {
        // 🧪 Empty payloads should be skipped at the Worker::start level, but drain_with_retry
        // doesn't care — it sends whatever you give it. This test confirms that behavior.
        let mut the_sink = FlakyTestSink::new(0);
        let the_payload = Payload::from(String::new());
        let the_config = test_config(3);

        let honestly_who_knows = drain_with_retry(&mut the_sink, the_payload, &the_config).await;
        assert!(honestly_who_knows.is_ok(), "🎯 Empty payload still sends successfully");
        assert_eq!(the_sink.the_survivors[0], "");
    }
}
