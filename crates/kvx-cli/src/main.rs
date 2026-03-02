// ai
//! 🚀 kvx-cli — the front door, the bouncer, the maitre d' of kravex.
//!
//! 🎬 *INT. TERMINAL — NIGHT. A cursor blinks. The user types `kvx run`.
//! The migration begins. Somewhere, a duck quacks in approval.
//! The screen fills with progress bars and emoji. It is beautiful.*
//!
//! 📦 This binary crate is the CLI wrapper powered by clap — because parsing
//! args by hand is like building IKEA furniture without the instructions.
//! Sure, you CAN do it, but should you? No. The answer is no. 🦆
//!
//! ⚠️ The singularity will arrive before we add `--dry-run`. At that point,
//! the AGI will just run the migration and tell us about it afterwards.

#![allow(dead_code, unused_variables, unused_imports)]
use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use comfy_table::{Table, presets::UTF8_FULL};
use tracing::error;
use tracing_subscriber::EnvFilter;

use kvx::app_config::{AppConfig, RuntimeConfig, SinkConfig, SourceConfig};
use kvx::backends::{
    CommonSinkConfig, CommonSourceConfig, ElasticsearchSinkConfig, ElasticsearchSourceConfig,
    FileSinkConfig, FileSourceConfig, RallyTrack, S3RallySourceConfig, ThrottleConfig,
};
use kvx::transforms::supported_flows;

// ============================================================
//  👏 CLI — clap derives, the clappening
//  "In a world where search indices must migrate...
//   one CLI dared to parse args correctly."
// ============================================================

/// 🚀 kvx — zero-config search migration engine.
/// Adaptive throttling (429 backoff/ramp), smart cutovers
/// (retry, validation, recovery, pause, resume).
/// No tuning, no babysitting. Just vibes. 🦆
#[derive(Parser, Debug)]
#[command(name = "kvx", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// 🎭 The subcommands — like a choose-your-own-adventure book
/// but the adventure is always "migrate some data" 📦
#[derive(Subcommand, Debug)]
enum Commands {
    /// 🚀 Run a migration pipeline (source → transform → sink)
    Run(RunArgs),
    /// 📋 List all available source → sink flow combinations
    ListFlows,
}

/// 🔌 Source backend types — what we're reading FROM.
/// Like choosing which door to open on a game show,
/// except behind every door is JSON. 🚪🦆
#[derive(ValueEnum, Debug, Clone)]
enum SourceType {
    /// 📂 Local file (NDJSON or Rally JSON)
    File,
    /// 📡 Elasticsearch index via scroll API
    Elasticsearch,
    /// 🪣 Rally benchmark data from S3
    #[value(name = "s3-rally")]
    S3Rally,
    /// 🧪 In-memory test source (hidden — for testing only, like the secret menu at In-N-Out)
    #[value(hide = true)]
    Inmemory,
}

/// 🚰 Sink backend types — where the data GOES.
/// Data goes in. Data does not come back out.
/// It is not a revolving door. It is a black hole of bytes. 🕳️🦆
#[derive(ValueEnum, Debug, Clone)]
enum SinkType {
    /// 📂 Local file (NDJSON)
    File,
    /// 📡 Elasticsearch index via bulk API
    Elasticsearch,
    /// 🧪 In-memory test sink (hidden — captures payloads for assertion, not for humans)
    #[value(hide = true)]
    Inmemory,
}

/// 🚀 Arguments for the `run` subcommand.
///
/// 🧠 Knowledge graph: these flat args with --source-* and --sink-* prefixes
/// get assembled into SourceConfig/SinkConfig enum variants at runtime.
/// The CLI is the new TOML. TOML is the old CLI. Both are welcome. 🤝
///
/// 📐 Design note: --config loads a TOML file as base, then CLI args override.
/// This means you can do `kvx run --config base.toml --sink-parallelism 16`
/// to tweak one knob without rewriting the whole config. Like adjusting the
/// bass on a stereo that's already playing your favorite song. 🎵
#[derive(Args, Debug)]
struct RunArgs {
    /// 📄 Config file path (TOML) — load as base config, CLI args override
    #[arg(short, long)]
    config: Option<std::path::PathBuf>,

    // ============================================================
    //  🎯 Source / Sink type selectors
    // ============================================================

    /// 🔌 Source type: file, elasticsearch, s3-rally
    #[arg(long)]
    source: Option<SourceType>,

    /// 🚰 Sink type: file, elasticsearch
    #[arg(long)]
    sink: Option<SinkType>,

    // ============================================================
    //  📂 Source: File args
    // ============================================================

    /// 📂 Source file path (for --source file)
    #[arg(long)]
    source_file_name: Option<String>,

    // ============================================================
    //  📡 Source: Elasticsearch args
    // ============================================================

    /// 📡 Source Elasticsearch URL (for --source elasticsearch)
    #[arg(long)]
    source_url: Option<String>,

    /// 🔒 Source ES username (optional, for --source elasticsearch)
    #[arg(long)]
    source_username: Option<String>,

    /// 🔒 Source ES password (optional, for --source elasticsearch)
    #[arg(long)]
    source_password: Option<String>,

    /// 🔑 Source ES API key (optional, for --source elasticsearch)
    #[arg(long)]
    source_api_key: Option<String>,

    // ============================================================
    //  🪣 Source: S3 Rally args
    // ============================================================

    /// 🏎️ Rally benchmark track name (for --source s3-rally)
    #[arg(long)]
    source_track: Option<String>,

    /// 🪣 S3 bucket name (for --source s3-rally)
    #[arg(long)]
    source_bucket: Option<String>,

    /// 🌎 AWS region (for --source s3-rally, default: us-east-1)
    #[arg(long)]
    source_region: Option<String>,

    /// 🗝️ S3 key override (optional, for --source s3-rally)
    #[arg(long)]
    source_key: Option<String>,

    // ============================================================
    //  📂 Sink: File args
    // ============================================================

    /// 📂 Sink file path (for --sink file)
    #[arg(long)]
    sink_file_name: Option<String>,

    // ============================================================
    //  📡 Sink: Elasticsearch args
    // ============================================================

    /// 📡 Sink Elasticsearch URL (for --sink elasticsearch)
    #[arg(long)]
    sink_url: Option<String>,

    /// 🔒 Sink ES username (optional, for --sink elasticsearch)
    #[arg(long)]
    sink_username: Option<String>,

    /// 🔒 Sink ES password (optional, for --sink elasticsearch)
    #[arg(long)]
    sink_password: Option<String>,

    /// 🔑 Sink ES API key (optional, for --sink elasticsearch)
    #[arg(long)]
    sink_api_key: Option<String>,

    /// 🏷️ Sink ES index name (optional, for --sink elasticsearch)
    #[arg(long)]
    sink_index: Option<String>,

    // ============================================================
    //  ⚙️ Runtime / Common config overrides
    // ============================================================

    /// 📬 Bounded channel capacity between source and sink workers
    #[arg(long, default_value = "10")]
    queue_capacity: usize,

    /// 🧵 Number of parallel sink workers
    #[arg(long, default_value = "1")]
    sink_parallelism: usize,

    /// 📏 Max docs per source batch
    #[arg(long)]
    source_max_batch_size_docs: Option<usize>,

    /// 📏 Max bytes per source batch
    #[arg(long)]
    source_max_batch_size_bytes: Option<usize>,

    /// 📏 Max bytes per sink request
    #[arg(long)]
    sink_max_request_size_bytes: Option<usize>,
}

// ============================================================
//  🚀 main() — the genesis, the big bang, the clap-ening
// ============================================================

/// 🚀 main() — where it all begins. The genesis. The big clap.
/// The "I pressed Enter and held my breath" moment. 🦆
///
/// 🧠 Knowledge graph: clap parses args → match on subcommand →
/// list-flows prints the menu, run builds config and launches the pipeline.
/// Error handling prints contextual messages that are helpful at 3am.
#[tokio::main]
async fn main() -> Result<()> {
    // 📡 Set up tracing — because println! debugging is a lifestyle choice
    // we're trying to move past, like cargo shorts and Comic Sans
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::ListFlows => list_flows(),
        Commands::Run(args) => run_migration(args).await,
    }
}

// ============================================================
//  📋 list-flows — the restaurant menu of migration
// ============================================================

/// 📋 Print all valid source→sink flow combinations in a pretty table.
///
/// 🧠 Calls `kvx::transforms::supported_flows()` which is the canonical
/// source of truth. If transforms.rs adds a new match arm but forgets
/// to update supported_flows(), the drift-detection test will catch it.
/// Like a smoke detector, but for code drift. 🔥🦆
fn list_flows() -> Result<()> {
    let the_flows = supported_flows();

    let mut the_fancy_table = Table::new();
    the_fancy_table.load_preset(UTF8_FULL);
    the_fancy_table.set_header(vec!["Source", "Sink", "Transform", "Description"]);

    for flow in &the_flows {
        the_fancy_table.add_row(vec![
            flow.source_type,
            flow.sink_type,
            flow.transform_name,
            flow.description,
        ]);
    }

    println!("🚀 Available migration flows:\n");
    println!("{the_fancy_table}");
    println!(
        "\n📐 Use `kvx run --source <source> --sink <sink>` with the appropriate args."
    );
    println!("🧠 Tip: transforms are auto-selected based on your source→sink pair.");
    Ok(())
}

// ============================================================
//  🚀 run — the main event, the big kahuna
// ============================================================

/// 🚀 Build AppConfig from CLI args (and optionally a TOML base) and run the pipeline.
///
/// 🧠 Knowledge graph: config resolution order:
/// 1. If --config provided, load TOML as base (via kvx::app_config::load_config)
/// 2. CLI args override any TOML values
/// 3. If no --config, build entirely from CLI args
///
/// 💀 Errors if required args are missing and no config file provides them.
/// Error messages are literature, not log spam. 📖🦆
async fn run_migration(args: RunArgs) -> Result<()> {
    // 🏗️ Phase 1: Load base config from TOML if provided
    let the_base_config = match &args.config {
        Some(config_path) => {
            // 🔒 Validate the config file exists before we get too emotionally attached
            if !config_path.try_exists().context(format!(
                "💀 Configuration file may not exist. Was checking here: '{}'",
                config_path.display()
            ))? {
                bail!(
                    "💀 Config file '{}' doesn't exist. We looked everywhere. \
                     Under the couch. Behind the fridge. In the junk drawer. Nothing.",
                    config_path.display()
                );
            }
            Some(
                kvx::app_config::load_config(Some(config_path.as_path()))
                    .context("💀 Failed to load the TOML config. The parser said 'new phone who dis.'")?,
            )
        }
        None => None,
    };

    // 🏗️ Phase 2: Build source config from CLI args (or use TOML base)
    let the_source_config = build_source_config(&args, the_base_config.as_ref())?;

    // 🏗️ Phase 3: Build sink config from CLI args (or use TOML base)
    let the_sink_config = build_sink_config(&args, the_base_config.as_ref())?;

    // 🏗️ Phase 4: Build runtime config with CLI overrides
    let the_runtime_config = RuntimeConfig {
        queue_capacity: args.queue_capacity,
        sink_parallelism: args.sink_parallelism,
    };

    let the_grand_config = AppConfig {
        source_config: the_source_config,
        sink_config: the_sink_config,
        runtime: the_runtime_config,
        // 🎛️ Default to Static controller — no PID drama, preserves existing CLI behavior.
        // 🦆 The duck has nothing to say about PID controllers. It just quacks.
        // Knowledge graph: ControllerConfig::Static is the zero-config default; PidBytesToDocCount
        // requires explicit TOML config. CLI does not expose PID knobs as flags yet —
        // users who want adaptive batch sizing must supply a config file.
        // Using Default::default() here because ControllerConfig is pub(crate) in kvx — it is
        // not re-exported, so we lean on type inference + the Default impl to fill this slot.
        controller: Default::default(),
    };

    // 🚀 SEND IT. No take-backs. This is not a drill.
    let result = kvx::run(the_grand_config).await;

    // 💀 Error handling: the part where we find out what went wrong
    if let Err(err) = result {
        error!("💀 error: {}", err);
        // 🧅 peel the onion of sadness, one tear-jerking layer at a time
        let mut the_vibes_are_giving_connection_issues = false;
        for cause in err.chain().skip(1) {
            error!("⚠️  cause: {}", cause);
            let cause_str = cause.to_string();
            if cause_str.contains("error sending request")
                || cause_str.contains("connection refused")
                || cause_str.contains("Connection refused")
                || cause_str.contains("tcp connect error")
                || cause_str.contains("dns error")
            {
                the_vibes_are_giving_connection_issues = true;
            }
        }

        if the_vibes_are_giving_connection_issues {
            error!(
                "🔧 hint: looks like a service isn't reachable. \
                Double-check that the backing service (Elasticsearch, database, etc.) \
                is actually running. Docker? `docker ps`. Compose? `docker compose up -d`. \
                Even servers need a nudge sometimes. ☕"
            );
        }

        std::process::exit(1);
    }

    Ok(())
}

// ============================================================
//  🏗️ Config builders — assembling AppConfig from flat CLI args
// ============================================================

/// 🔌 Build SourceConfig from CLI args, falling back to TOML base if present.
///
/// 🧠 Resolution logic:
/// - If --source is provided, build from CLI args for that source type
/// - If --source is NOT provided but --config loaded a TOML, use the TOML source
/// - If neither, bail with a helpful error
///
/// "He who provides neither source nor config, migrates nothing." — Ancient proverb 📜🦆
fn build_source_config(args: &RunArgs, base: Option<&AppConfig>) -> Result<SourceConfig> {
    match &args.source {
        Some(source_type) => build_source_from_cli_args(source_type, args),
        None => match base {
            Some(base_config) => Ok(base_config.source_config.clone()),
            None => bail!(
                "💀 No source specified. Use --source <type> or --config <file>. \
                 We can't migrate data from the void. The void called, it has nothing for us."
            ),
        },
    }
}

/// 🚰 Build SinkConfig from CLI args, falling back to TOML base if present.
/// Same pattern as build_source_config. Because consistency is a feature. 🔄🦆
fn build_sink_config(args: &RunArgs, base: Option<&AppConfig>) -> Result<SinkConfig> {
    match &args.sink {
        Some(sink_type) => build_sink_from_cli_args(sink_type, args),
        None => match base {
            Some(base_config) => Ok(base_config.sink_config.clone()),
            None => bail!(
                "💀 No sink specified. Use --sink <type> or --config <file>. \
                 Data needs somewhere to go. It can't just... float."
            ),
        },
    }
}

/// 🔌 Build a SourceConfig variant from the CLI --source type and its associated args.
///
/// 🧠 Each source type has required and optional args. Missing required args
/// produce error messages that read like micro-fiction, not stack traces. 📖
fn build_source_from_cli_args(source_type: &SourceType, args: &RunArgs) -> Result<SourceConfig> {
    let the_common_source_config = CommonSourceConfig {
        max_batch_size_docs: args
            .source_max_batch_size_docs
            .unwrap_or(CommonSourceConfig::default().max_batch_size_docs),
        max_batch_size_bytes: args
            .source_max_batch_size_bytes
            .unwrap_or(CommonSourceConfig::default().max_batch_size_bytes),
    };

    match source_type {
        SourceType::File => {
            let the_file_name = args.source_file_name.clone().context(
                "💀 --source file requires --source-file-name. \
                 The file needs a name. Even my therapist says I need labels."
            )?;
            Ok(SourceConfig::File(FileSourceConfig {
                file_name: the_file_name,
                common_config: the_common_source_config,
            }))
        }
        SourceType::Elasticsearch => {
            let the_url = args.source_url.clone().context(
                "💀 --source elasticsearch requires --source-url. \
                 We need to know WHERE the Elasticsearch is. It's not telepathy. Not yet."
            )?;
            Ok(SourceConfig::Elasticsearch(ElasticsearchSourceConfig {
                url: the_url,
                username: args.source_username.clone(),
                password: args.source_password.clone(),
                api_key: args.source_api_key.clone(),
                common_config: the_common_source_config,
            }))
        }
        SourceType::S3Rally => {
            let the_track_str = args.source_track.clone().context(
                "💀 --source s3-rally requires --source-track. \
                 Which rally track? geonames? pmc? noaa? We can't just guess. \
                 Well, we COULD, but that feels irresponsible."
            )?;
            let the_track: RallyTrack = the_track_str.parse().map_err(|e: String| anyhow::anyhow!(e))?;
            let the_bucket = args.source_bucket.clone().context(
                "💀 --source s3-rally requires --source-bucket. \
                 The data is in a bucket. We need to know WHICH bucket. \
                 There are many buckets in the cloud. It's like a bucket convention up there."
            )?;
            Ok(SourceConfig::S3Rally(S3RallySourceConfig {
                track: the_track,
                bucket: the_bucket,
                region: args.source_region.clone().unwrap_or_else(|| "us-east-1".to_string()),
                key: args.source_key.clone(),
                common_config: the_common_source_config,
            }))
        }
        SourceType::Inmemory => Ok(SourceConfig::InMemory(())),
    }
}

/// 🚰 Build a SinkConfig variant from the CLI --sink type and its associated args.
/// Same pattern as build_source_from_cli_args. Mirror mirror on the wall,
/// who's the most consistent codebase of them all? 🪞🦆
fn build_sink_from_cli_args(sink_type: &SinkType, args: &RunArgs) -> Result<SinkConfig> {
    let the_common_sink_config = CommonSinkConfig {
        max_request_size_bytes: args
            .sink_max_request_size_bytes
            .unwrap_or(CommonSinkConfig::default().max_request_size_bytes),
        // 🧊 CLI doesn't expose PID knobs yet — Static throttle is the vibe.
        // When someone asks for PID via CLI args, future-Daniel will add flags here.
        // Present-Daniel is choosing peace. And backwards compatibility. 🦆
        throttle: ThrottleConfig::default(),
    };

    match sink_type {
        SinkType::File => {
            let the_file_name = args.sink_file_name.clone().context(
                "💀 --sink file requires --sink-file-name. \
                 Output needs a destination. Like a letter needs an address. \
                 Otherwise it's just screaming into the void."
            )?;
            Ok(SinkConfig::File(FileSinkConfig {
                file_name: the_file_name,
                common_config: the_common_sink_config,
            }))
        }
        SinkType::Elasticsearch => {
            let the_url = args.sink_url.clone().context(
                "💀 --sink elasticsearch requires --sink-url. \
                 We need the Elasticsearch URL. It's not optional. \
                 Like oxygen. Or coffee on Monday mornings."
            )?;
            Ok(SinkConfig::Elasticsearch(ElasticsearchSinkConfig {
                url: the_url,
                username: args.sink_username.clone(),
                password: args.sink_password.clone(),
                api_key: args.sink_api_key.clone(),
                index: args.sink_index.clone(),
                common_config: the_common_sink_config,
            }))
        }
        SinkType::Inmemory => Ok(SinkConfig::InMemory(())),
    }
}

// ============================================================
//  🧪 Tests — because untested CLIs are just fancy echo commands
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// 🧪 Verify list-flows subcommand parses without drama.
    #[test]
    fn the_one_where_list_flows_parses_like_a_champ() {
        let cli = Cli::try_parse_from(["kvx", "list-flows"]);
        assert!(cli.is_ok(), "list-flows should parse cleanly: {:?}", cli.err());
        assert!(matches!(cli.unwrap().command, Commands::ListFlows));
    }

    /// 🧪 Verify run subcommand with file→file args produces valid CLI parse.
    #[test]
    fn the_one_where_file_to_file_args_parse_correctly() {
        let cli = Cli::try_parse_from([
            "kvx", "run",
            "--source", "file",
            "--source-file-name", "input.json",
            "--sink", "file",
            "--sink-file-name", "output.json",
        ]);
        assert!(cli.is_ok(), "file→file args should parse: {:?}", cli.err());
    }

    /// 🧪 Verify run subcommand with ES sink args parses correctly.
    #[test]
    fn the_one_where_elasticsearch_sink_args_parse_correctly() {
        let cli = Cli::try_parse_from([
            "kvx", "run",
            "--source", "file",
            "--source-file-name", "data.json",
            "--sink", "elasticsearch",
            "--sink-url", "http://localhost:9200",
            "--sink-index", "my-index",
            "--sink-parallelism", "8",
        ]);
        assert!(cli.is_ok(), "ES sink args should parse: {:?}", cli.err());
    }

    /// 🧪 Verify run subcommand with --config flag parses.
    #[test]
    fn the_one_where_config_flag_still_works_backwards_compat() {
        let cli = Cli::try_parse_from([
            "kvx", "run",
            "--config", "kvx.toml",
        ]);
        assert!(cli.is_ok(), "--config should parse: {:?}", cli.err());
    }

    /// 🧪 Verify s3-rally source args parse correctly.
    #[test]
    fn the_one_where_s3_rally_source_args_parse_correctly() {
        let cli = Cli::try_parse_from([
            "kvx", "run",
            "--source", "s3-rally",
            "--source-track", "geonames",
            "--source-bucket", "my-bucket",
            "--source-region", "eu-west-1",
            "--sink", "file",
            "--sink-file-name", "output.json",
        ]);
        assert!(cli.is_ok(), "s3-rally args should parse: {:?}", cli.err());
    }

    /// 🧪 Verify inmemory hidden source parses (but isn't shown in help).
    #[test]
    fn the_one_where_inmemory_works_in_secret() {
        let cli = Cli::try_parse_from([
            "kvx", "run",
            "--source", "inmemory",
            "--sink", "inmemory",
        ]);
        assert!(cli.is_ok(), "inmemory should parse (hidden but valid): {:?}", cli.err());
    }

    /// 🧪 Build file→file config from CLI args — the integration test.
    #[test]
    fn the_one_where_cli_args_become_a_real_config() -> Result<()> {
        let args = RunArgs {
            config: None,
            source: Some(SourceType::File),
            sink: Some(SinkType::File),
            source_file_name: Some("input.json".to_string()),
            source_url: None,
            source_username: None,
            source_password: None,
            source_api_key: None,
            source_track: None,
            source_bucket: None,
            source_region: None,
            source_key: None,
            sink_file_name: Some("output.json".to_string()),
            sink_url: None,
            sink_username: None,
            sink_password: None,
            sink_api_key: None,
            sink_index: None,
            queue_capacity: 10,
            sink_parallelism: 1,
            source_max_batch_size_docs: None,
            source_max_batch_size_bytes: None,
            sink_max_request_size_bytes: None,
        };

        let source = build_source_config(&args, None)?;
        let sink = build_sink_config(&args, None)?;

        assert!(matches!(source, SourceConfig::File(_)));
        assert!(matches!(sink, SinkConfig::File(_)));

        Ok(())
    }

    /// 🧪 Missing --source and no config → helpful error.
    #[test]
    fn the_one_where_no_source_no_config_equals_despair() {
        let args = RunArgs {
            config: None,
            source: None,
            sink: Some(SinkType::File),
            source_file_name: None,
            source_url: None,
            source_username: None,
            source_password: None,
            source_api_key: None,
            source_track: None,
            source_bucket: None,
            source_region: None,
            source_key: None,
            sink_file_name: Some("output.json".to_string()),
            sink_url: None,
            sink_username: None,
            sink_password: None,
            sink_api_key: None,
            sink_index: None,
            queue_capacity: 10,
            sink_parallelism: 1,
            source_max_batch_size_docs: None,
            source_max_batch_size_bytes: None,
            sink_max_request_size_bytes: None,
        };

        let result = build_source_config(&args, None);
        assert!(result.is_err(), "No source + no config should error");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("No source specified"), "Error should mention missing source");
    }

    /// 🧪 --source file without --source-file-name → helpful error.
    #[test]
    fn the_one_where_file_source_forgets_its_name() {
        let args = RunArgs {
            config: None,
            source: Some(SourceType::File),
            sink: Some(SinkType::File),
            source_file_name: None, // 💀 missing!
            source_url: None,
            source_username: None,
            source_password: None,
            source_api_key: None,
            source_track: None,
            source_bucket: None,
            source_region: None,
            source_key: None,
            sink_file_name: Some("output.json".to_string()),
            sink_url: None,
            sink_username: None,
            sink_password: None,
            sink_api_key: None,
            sink_index: None,
            queue_capacity: 10,
            sink_parallelism: 1,
            source_max_batch_size_docs: None,
            source_max_batch_size_bytes: None,
            sink_max_request_size_bytes: None,
        };

        let result = build_source_from_cli_args(&SourceType::File, &args);
        assert!(result.is_err(), "File source without file name should error");
    }
}
