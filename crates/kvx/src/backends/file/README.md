

# File Backend

File-based Source and Sink implementations for NDJSON data.

## Source

Reads NDJSON files using chunked I/O with SIMD-accelerated newline scanning via `memchr`. Returns one page of newline-delimited records per `pump()` call.

## Sink

Writes NDJSON payloads to a file. Appends rendered payloads directly.

## Config

`FileSourceConfig` and `FileSinkConfig` — file path configuration.

## Key Concepts

- **Chunked I/O**: Raw byte reads, not line-by-line — high throughput
- **memchr**: SIMD-accelerated byte scanning for newline boundaries
- **Remainder stashing**: Partial lines carried between pump calls
- **NDJSON**: Newline-Delimited JSON — one JSON object per line

## Knowledge Graph

```
FileSource → Source trait → SourceBackend::File
FileSink → Sink trait → SinkBackend::File
FileSourceConfig → CommonSourceConfig (embedded)
FileSinkConfig → CommonSinkConfig (embedded)
```
