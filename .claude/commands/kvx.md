# kvx Migration Skill

You are helping the user run a kvx data migration. Follow this workflow precisely.

## Step 1: Discover Available Flows

Run the following command to get the list of valid source → sink combinations:

```bash
cargo run -p kvx-cli -- list-flows
```

Present the table output to the user and ask them to select a source → sink pair.

## Step 2: Collect Configuration

Based on the user's selected flow, collect the required configuration values. Use `AskUserQuestion` to prompt for each value, providing the defaults shown below.

### Source: `file`
| Arg | Required | Default | Description |
|-----|----------|---------|-------------|
| `--source-file-name` | Yes | — | Path to the input file (NDJSON or Rally JSON) |

### Source: `elasticsearch`
| Arg | Required | Default | Description |
|-----|----------|---------|-------------|
| `--source-url` | Yes | `http://localhost:9200` | Elasticsearch URL |
| `--source-username` | No | — | Basic auth username |
| `--source-password` | No | — | Basic auth password |
| `--source-api-key` | No | — | API key auth |

### Source: `s3-rally`
| Arg | Required | Default | Description |
|-----|----------|---------|-------------|
| `--source-track` | Yes | — | Rally track: big5, clickbench, eventdata, geonames, geopoint, geopointshape, geoshape, http_logs, nested, neural_search, noaa, noaa_semantic_search, nyc_taxis, percolator, pmc, so, treccovid_semantic_search, vectorsearch |
| `--source-bucket` | Yes | — | S3 bucket name |
| `--source-region` | No | `us-east-1` | AWS region |
| `--source-key` | No | `{track}/documents.json` | S3 object key override |

### Sink: `file`
| Arg | Required | Default | Description |
|-----|----------|---------|-------------|
| `--sink-file-name` | Yes | — | Path to the output file |

### Sink: `elasticsearch`
| Arg | Required | Default | Description |
|-----|----------|---------|-------------|
| `--sink-url` | Yes | `http://localhost:9200` | Elasticsearch URL |
| `--sink-username` | No | — | Basic auth username |
| `--sink-password` | No | — | Basic auth password |
| `--sink-api-key` | No | — | API key auth |
| `--sink-index` | No | — | Target index name |

### Runtime (all flows)
| Arg | Default | Description |
|-----|---------|-------------|
| `--queue-capacity` | `10` | Bounded channel capacity |
| `--sink-parallelism` | `1` | Number of parallel sink workers |
| `--source-max-batch-size-docs` | `10000` | Max docs per source batch |
| `--source-max-batch-size-bytes` | `10485760` | Max bytes per source batch (10MB) |
| `--sink-max-request-size-bytes` | `10485760` | Max bytes per sink request (10MB) |

## Step 3: Construct and Confirm

Build the full CLI command from the user's inputs. Show it for confirmation before executing.

Example commands:

```bash
# File → Elasticsearch
cargo run -p kvx-cli -- run \
  --source file --source-file-name input.json \
  --sink elasticsearch --sink-url http://localhost:9200 --sink-index my-index \
  --sink-parallelism 8

# S3 Rally → File (download benchmark data)
cargo run -p kvx-cli -- run \
  --source s3-rally --source-track geonames --source-bucket rally-tracks-bench \
  --sink file --sink-file-name geonames.json

# File → File (passthrough)
cargo run -p kvx-cli -- run \
  --source file --source-file-name input.json \
  --sink file --sink-file-name output.json

# Using a TOML config as base with CLI overrides
cargo run -p kvx-cli -- run --config kvx.toml --sink-parallelism 16
```

## Step 4: Execute

Run the confirmed command.

## Step 5: Chain Flows (Optional)

After completion, ask the user if they want to run another flow. Common multi-step patterns:

1. **S3 → File → Elasticsearch**: Download benchmark data locally first, then index it
   - Step A: `s3-rally → file` (download)
   - Step B: `file → elasticsearch` (index)

2. **Elasticsearch → File → Elasticsearch**: Migrate between clusters via file
   - Step A: `elasticsearch → file` (dump source)
   - Step B: `file → elasticsearch` (load target)

Repeat from Step 1 if chaining.
