//! 🎬 *[camera pans across a dimly lit server room]*
//! 🎬 *[dramatic orchestral music swells]*
//! 🎬 "In a world where workers toil endlessly..."
//! 🎬 "One supervisor dared to manage them all."
//! 🎬 *[record scratch]* 🦆
//!
//! 📦 The Supervisor module — part middle manager, part helicopter parent,
//! part that one project manager who schedules a meeting to plan the next meeting.
//!
//! ⚠️ DO NOT MAKE THIS PUB EVER
//! ⚠️ YOU HAVE BEEN WARNED
//! 💀 WORKERS ARE SUPERVISORS PRIVATE LITTLE MINIONS WHOM THE WORLD FORGOT ABOUT
//! 🔒 Like Fight Club, but for async tasks. First rule: you don't pub the workers.

use crate::app_config::AppConfig;
use crate::composers::ComposerBackend;
use crate::workers::Worker;
use crate::workers;
use crate::transforms::DocumentTransformer;
use anyhow::{Context, Result};

/// 📦 The Supervisor: because even async tasks need someone hovering over them
/// asking "is it done yet?" every 5 milliseconds.
///
/// 🏗️ Built with the same care and attention as IKEA furniture —
/// looks good in the docs, wobbly in production.
pub struct Supervisor {
    /// 🔧 The sacred scrolls of configuration, passed down from main()
    /// through the ancient ritual of .clone()
    app_config: AppConfig,
}

impl Supervisor {
    /// 🚀 Birth of a Supervisor. It's like a baby, but less crying.
    /// Actually no, there's plenty of crying. Mostly from the developer.
    pub fn new(app_config: AppConfig) -> Self {
        // -- 🐛 "My therapist says I should let go of control"
        // -- — said no supervisor ever
        Self { app_config }
    }
}

impl Supervisor {
    /// 🧵 Unleash the workers! Now with Composer powers and page buffering.
    ///
    /// 🧠 Knowledge graph: the pipeline flow is now:
    /// ```text
    /// Source.next_page() → channel(String) → SinkWorker(buffer pages → composer.compose → sink.send) → Sink(I/O)
    /// ```
    /// Each SinkWorker gets its own clone of the `DocumentTransformer` and `ComposerBackend`.
    /// Since transforms and composers are zero-sized structs, cloning is free.
    /// The Composer handles both transformation AND assembly — the Cow lives there. 🐄
    pub async fn start_workers(
        &self,
        source_backend: crate::backends::SourceBackend,
        sink_backends: Vec<crate::backends::SinkBackend>,
        transformer: DocumentTransformer,
        composer: ComposerBackend,
        max_request_size_bytes: usize,
    ) -> Result<()> {
        // 📬 Channel carries String — raw pages from source to sink workers.
        let (tx, rx) = async_channel::bounded(self.app_config.runtime.queue_capacity);

        let mut worker_handles = Vec::with_capacity(sink_backends.len() + 1);

        // 🗑️ Spawn N sink workers, each with its own transformer + composer clones.
        for sink_backend in sink_backends {
            let sink_worker = workers::SinkWorker::new(
                rx.clone(),
                sink_backend,
                transformer.clone(),
                composer.clone(),
                max_request_size_bytes,
            );
            worker_handles.push(sink_worker.start());
        }

        // 🚰 Spawn the source worker — it pumps raw pages into the channel.
        let source_worker = workers::SourceWorker::new(tx.clone(), source_backend);
        worker_handles.push(source_worker.start());

        let results = futures::future::join_all(worker_handles).await;
        for result in results {
            // 🤯 result?? — the outer `?` unwraps the JoinHandle, the inner `?` unwraps the work.
            result??;
        }

        Ok(())
    }
}
