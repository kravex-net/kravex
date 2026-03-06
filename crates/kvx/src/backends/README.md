

# Backends

I/O abstraction layer for sources (data readers) and sinks (data writers).

## Traits

| Trait | Method | Returns | Purpose |
|---|---|---|---|
| `Source` | `pump()` | `Option<String>` | Read next raw feed; `None` = EOF |
| `Sink` | `drain(payload)` | `Result<()>` | Write a rendered payload |

## Dispatcher Enums

| Enum | Variants | Purpose |
|---|---|---|
| `SourceBackend` | Elasticsearch, File, InMemory | Route to concrete Source impl |
| `SinkBackend` | Elasticsearch, File, InMemory | Route to concrete Sink impl |

## Backend Implementations

| Backend | Source | Sink | Config |
|---|---|---|---|
| **Elasticsearch** | PIT + search_after pagination | `_bulk` HTTP POST | `config.rs` |
| **File** | NDJSON line reader | NDJSON file writer | `config.rs` |
| **InMemory** | Vec-backed test source | Vec-backed test sink | Inline |

## Shared Config

`CommonSourceConfig` and `CommonSinkConfig` provide backend-agnostic configuration fields shared across all implementations.

## Pattern

All backends follow: **trait → concrete impl → enum dispatcher → from_config resolver**

## Knowledge Graph

```
backends.rs → re-exports Source, Sink, SourceBackend, SinkBackend
backends/source.rs → Source trait + SourceBackend enum
backends/sink.rs → Sink trait + SinkBackend enum
backends/config.rs → CommonSourceConfig, CommonSinkConfig
backends/elasticsearch/ → ES-specific source, sink, config
backends/file/ → File-specific source, sink, config
backends/in_mem/ → In-memory source, sink (testing)
```
