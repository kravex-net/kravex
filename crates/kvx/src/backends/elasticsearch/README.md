

# Elasticsearch Backend

Elasticsearch-specific Source and Sink implementations.

## Source

Reads documents from Elasticsearch using **PIT (Point In Time) + search_after** pagination. Zero-copy deserialization via `serde_json::value::RawValue` — borrows `_id`, `_source`, `_index` directly from the response buffer.

## Sink

Writes documents to Elasticsearch via the **`_bulk` API**. Pre-computes the bulk URL and auth header at construction time for zero-allocation-per-request hot path.

## Config

`ElasticsearchSourceConfig` and `ElasticsearchSinkConfig` — pure connection configuration (host, index, auth credentials). No batch/request sizing fields.

## Key Concepts

- **PIT**: Consistent snapshot for pagination, avoids deep-pagination overhead
- **search_after**: Cursor-based pagination using sort values from previous response
- **`_bulk` API**: Batch document indexing via NDJSON action/document pairs
- **Pre-computed auth**: Basic auth header encoded once at construction

## Knowledge Graph

```
ElasticsearchSource → Source trait → SourceBackend::Elasticsearch
ElasticsearchSink → Sink trait → SinkBackend::Elasticsearch
ElasticsearchSourceConfig → CommonSourceConfig (embedded)
ElasticsearchSinkConfig → CommonSinkConfig (embedded)
PIT + search_after → feeds (raw JSON pages)
_bulk API ← payloads (NDJSON action+doc pairs)
```
