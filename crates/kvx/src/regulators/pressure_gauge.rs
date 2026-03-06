// ai
//! 🎬 *[INT. ENGINE ROOM — THE GAUGES FLICKER]*
//! *[A tokio task awakens in the background. Its mission: read the pressure.]*
//! *[Every 3 seconds it pings the node. Every 3 seconds it knows the truth.]*
//! *["75%," it reports. "We're in the zone."]*  📡🔧🦆
//!
//! 📦 PressureGauge — background tokio task that reads ES/OS node CPU stats,
//! feeds them through a Regulator, and adjusts the FlowKnob that Joiners read.
//!
//! 🧠 Knowledge graph:
//! - Hits `_nodes/stats/os` on the sink cluster every N seconds
//! - Extracts CPU percent from node stats response
//! - Feeds reading to `Regulators::regulate()` → new flow rate in bytes
//! - Writes new value to `FlowKnob` (Arc<AtomicUsize>) via `store(Relaxed)`
//! - Joiners read `FlowKnob` via `load(Relaxed)` on every flush check
//! - Relaxed ordering is fine — we don't need sequential consistency for a throttle knob,
//!   just eventual visibility. Like a thermostat, not a mutex.
//!
//! ⚠️ The singularity will regulate itself. We're just building the training data.

use crate::regulators::{Regulate, CpuRegulatorConfig, Regulators};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// 🔧 The FlowKnob — a shared atomic valve that controls payload size.
///
/// The pressure gauge writes it. The joiners read it. Nobody else touches it.
/// Like the office thermostat, except this one actually works. 🌡️
pub type FlowKnob = Arc<AtomicUsize>;

/// 🚰 Authentication for accessing sink node stats — reuse the drain credentials.
///
/// Because the pressure gauge needs to talk to the same cluster the drainers talk to,
/// and we're not about to ask for a second set of credentials like some kind of bureaucrat. 🏛️
#[derive(Debug, Clone)]
pub enum SinkAuth {
    /// 🔑 Basic auth — username:password, encoded with base64, served with anxiety
    Basic { username: String, password: String },
    /// 🔓 No auth — living dangerously, like running ES on port 9200 with no password
    None,
}

/// 🔬 Parsed node stats — the bits we actually care about from the firehose of stats.
///
/// ES/OS return approximately 47 billion fields per node. We want exactly one: CPU percent.
/// This struct is the bouncer at the door. "CPU? You're on the list. Everything else? Nah." 🚪
#[derive(Debug, serde::Deserialize)]
struct NodeStatsResponse {
    nodes: std::collections::HashMap<String, NodeStats>,
}

#[derive(Debug, serde::Deserialize)]
struct NodeStats {
    os: Option<NodeOsStats>,
}

#[derive(Debug, serde::Deserialize)]
struct NodeOsStats {
    cpu: Option<NodeCpuStats>,
}

#[derive(Debug, serde::Deserialize)]
struct NodeCpuStats {
    percent: u64,
}

/// 📡 Read CPU pressure from the sink cluster's `_nodes/stats/os` endpoint.
///
/// Returns the average CPU percent across all nodes in the cluster.
/// If a node doesn't report CPU stats (it's being coy), we skip it.
///
/// 🧠 Extracted as a standalone function for testability — you can mock the HTTP call
/// and test the parsing logic independently. Future you will thank present you. 🎁
pub async fn read_node_pressure(
    client: &reqwest::Client,
    base_url: &str,
    auth: &SinkAuth,
) -> anyhow::Result<f64> {
    let the_stats_url = format!("{}/_nodes/stats/os", base_url.trim_end_matches('/'));

    let mut the_request_builder = client.get(&the_stats_url);
    if let SinkAuth::Basic { username, password } = auth {
        the_request_builder = the_request_builder.basic_auth(username, Some(password));
    }

    let the_response = the_request_builder
        .send()
        .await
        .map_err(|e| anyhow::anyhow!(
            "💀 Failed to reach node stats endpoint at {} — the cluster ghosted us. \
             Like texting your ex. Error: {}",
            the_stats_url, e
        ))?;

    let the_response_body = the_response
        .text()
        .await
        .map_err(|e| anyhow::anyhow!(
            "💀 Got a response from node stats but couldn't read the body — \
             it's like receiving a letter in a language you don't speak. Error: {}",
            e
        ))?;

    let the_stats: NodeStatsResponse = serde_json::from_str(&the_response_body)
        .map_err(|e| anyhow::anyhow!(
            "💀 Failed to parse node stats JSON — the cluster is speaking in tongues. \
             Expected _nodes/stats/os format, got something else entirely. Error: {}",
            e
        ))?;

    // 📊 Average CPU across all nodes that report it
    let mut the_cpu_sum = 0.0_f64;
    let mut the_node_count = 0_u64;

    for (_node_id, node) in &the_stats.nodes {
        if let Some(os) = &node.os {
            if let Some(cpu) = &os.cpu {
                the_cpu_sum += cpu.percent as f64;
                the_node_count += 1;
            }
        }
    }

    if the_node_count == 0 {
        // ⚠️ No nodes reported CPU — return setpoint as a safe fallback
        warn!("⚠️ No nodes reported CPU stats — returning 50.0 as a neutral fallback. \
               The cluster is being mysterious. Like a cat. 🐱");
        return Ok(50.0);
    }

    Ok(the_cpu_sum / the_node_count as f64)
}

/// 🚀 Spawn the pressure gauge background task.
///
/// This tokio task runs forever (until the returned JoinHandle is aborted),
/// reading node stats, feeding the regulator, and adjusting the flow knob.
///
/// 📜 Lifecycle:
/// 1. Sleep for poll_interval
/// 2. Read node CPU pressure via `_nodes/stats/os`
/// 3. Feed to `regulator.regulate(pressure, dt_ms)` → new flow rate
/// 4. Store new flow rate in `flow_knob` (AtomicUsize, Relaxed)
/// 5. Repeat until aborted
///
/// "In a world where JVM GC pauses threaten everything we hold dear...
///  one background task dared to poll every 3 seconds." 🎬🦆
pub fn spawn_pressure_gauge(
    config: CpuRegulatorConfig,
    base_url: String,
    auth: SinkAuth,
    flow_knob: FlowKnob,
    sink_max_request_size_bytes: usize,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let the_poll_interval = std::time::Duration::from_secs(config.poll_interval_secs);
        let mut the_regulator = Regulators::from_config(&config, sink_max_request_size_bytes);

        let the_http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("💀 Failed to build HTTP client for pressure gauge — reqwest said no. \
                     This is like the thermostat refusing to read the thermometer.");

        info!(
            "🔬 Pressure gauge online — polling {} every {}s, target CPU: {}%",
            base_url, config.poll_interval_secs, config.target_cpu
        );

        loop {
            tokio::time::sleep(the_poll_interval).await;

            match read_node_pressure(&the_http_client, &base_url, &auth).await {
                Ok(the_cpu_reading) => {
                    let the_dt_ms = the_poll_interval.as_millis() as f64;
                    let the_new_flow = the_regulator.regulate(the_cpu_reading, the_dt_ms);
                    let the_new_flow_usize = the_new_flow as usize;

                    let the_old_flow = flow_knob.swap(the_new_flow_usize, Ordering::Relaxed);

                    debug!(
                        "🔬 Gauge: CPU={:.1}% → regulator → flow: {} → {} bytes (Δ{})",
                        the_cpu_reading,
                        the_old_flow,
                        the_new_flow_usize,
                        the_new_flow_usize as i64 - the_old_flow as i64
                    );
                }
                Err(the_gauge_malfunction) => {
                    warn!(
                        "⚠️ Pressure gauge failed to read node stats — keeping current flow knob value. \
                         Error: {}. The gauge will try again in {}s. Like a persistent telemarketer. 📞",
                        the_gauge_malfunction, config.poll_interval_secs
                    );
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 The one where node stats JSON parses correctly.
    /// If this fails, the cluster changed its API and didn't tell us. Classic. 📡
    #[test]
    fn the_one_where_node_stats_json_parses_like_a_champ() {
        let the_json = r#"{
            "nodes": {
                "node1": {
                    "os": {
                        "cpu": {
                            "percent": 72
                        }
                    }
                },
                "node2": {
                    "os": {
                        "cpu": {
                            "percent": 68
                        }
                    }
                }
            }
        }"#;

        let the_stats: NodeStatsResponse = serde_json::from_str(the_json)
            .expect("💀 Node stats JSON should parse — we literally wrote it ourselves");

        assert_eq!(the_stats.nodes.len(), 2, "🎯 Should have 2 nodes");

        // 📊 Average should be (72 + 68) / 2 = 70
        let the_total: u64 = the_stats.nodes.values()
            .filter_map(|n| n.os.as_ref()?.cpu.as_ref())
            .map(|c| c.percent)
            .sum();
        let the_avg = the_total as f64 / 2.0;
        assert!((the_avg - 70.0).abs() < f64::EPSILON, "🎯 Average CPU should be 70% — got {}", the_avg);
    }

    /// 🧪 The one where missing CPU stats don't crash the parser.
    /// Some nodes just don't want to share. And that's okay. 🤐🦆
    #[test]
    fn the_one_where_missing_cpu_stats_are_handled_gracefully() {
        let the_json = r#"{
            "nodes": {
                "shy_node": {
                    "os": null
                },
                "chatty_node": {
                    "os": {
                        "cpu": {
                            "percent": 80
                        }
                    }
                }
            }
        }"#;

        let the_stats: NodeStatsResponse = serde_json::from_str(the_json)
            .expect("💀 Should handle null os gracefully");

        let the_reporting_nodes: Vec<_> = the_stats.nodes.values()
            .filter_map(|n| n.os.as_ref()?.cpu.as_ref())
            .collect();

        assert_eq!(the_reporting_nodes.len(), 1, "🎯 Only chatty_node should report");
        assert_eq!(the_reporting_nodes[0].percent, 80, "🎯 Chatty node says 80%");
    }

    /// 🧪 The one where FlowKnob works as an atomic valve.
    /// Relaxed ordering because eventual consistency is good enough for a throttle. 🔧
    #[test]
    fn the_one_where_flow_knob_atomically_adjusts() {
        let the_knob: FlowKnob = Arc::new(AtomicUsize::new(4_194_304));

        // 📏 Read the initial value
        assert_eq!(the_knob.load(Ordering::Relaxed), 4_194_304);

        // 🔧 Adjust the knob
        the_knob.store(2_097_152, Ordering::Relaxed);
        assert_eq!(the_knob.load(Ordering::Relaxed), 2_097_152);

        // 🔧 Swap returns old value
        let the_old = the_knob.swap(8_388_608, Ordering::Relaxed);
        assert_eq!(the_old, 2_097_152);
        assert_eq!(the_knob.load(Ordering::Relaxed), 8_388_608);
    }
}
