

# In-Memory Backend

In-memory Source and Sink implementations for testing.

## Source

Vec-backed source that yields pre-loaded feeds. Used in unit tests to supply controlled input without I/O.

## Sink

Vec-backed sink that collects drained payloads. Used in unit tests to capture output for assertion.

## Key Concepts

- **Test-only**: Not intended for production use
- **Deterministic**: No I/O, no network, no filesystem
- **Paired with JsonArrayManifold**: InMemory sinks resolve to JsonArrayManifold for payload assembly

## Knowledge Graph

```
InMemorySource → Source trait → SourceBackend::InMemory
InMemorySink → Sink trait → SinkBackend::InMemory
InMemory backends → used by unit tests across the crate
```
