# Breeze logs

Process-level `tracing` output for Breeze services.

The default configuration writes to `../logs` without installing a
stdout or stderr writer:

- `TRACE`, `DEBUG`, and `INFO` events go to `info.log`.
- `WARN` events go to `warn.log`.
- `ERROR` events go to `error.log`.

Every line uses a fixed UTC+8 wall-clock timestamp without a timezone suffix:

```text
2026-08-30 15:10:17 [INFO] ListStorage get new version. listId:6296, version:1684410575955
```

Initialize the global subscriber once in the final binary and retain the guard
until shutdown:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let logs = logs::init_default()?;
    tracing::info!("service started");

    logs.flush()?;
    Ok(())
}
```

`LogsConfig` controls the directory, `RUST_LOG`-compatible filter, bounded
queue, two-chunk ephemeral line arena, maximum line size, overflow behavior,
and flush policy. Each arena chunk defaults to 16 MiB, so the arena owns one
32 MiB backing buffer. `with_arena_chunk_bytes` can change the per-chunk
capacity during initialization. `LogsConfig::from_env` additionally reads
`BREEZE_LOG_DIR` and `RUST_LOG`.

Each formatted line starts with an arena segment whose capacity adapts between
512 bytes, 1 KiB, and 2 KiB. Overflow appends geometrically growing segments
capped at 2 KiB without moving bytes already formatted. After 512 consecutive
low-usage lines the initial segment shrinks by one step.

One worker drains the shared queue into a bounded batch, groups its entries by
destination, and writes each file with vectored I/O. A single `write_vectored`
call carries at most 1024 arena segments; a larger file batch is continued by
subsequent calls. There is no additional per-file userspace byte buffer.

The default queue is lossy under sustained overload so logging cannot block a
service hot path. `LogsGuard::dropped_lines` and `LogsGuard::last_error` expose
runtime health, while `LogsGuard::flush` provides an explicit flush barrier.

Enable the optional `metrics` feature to register the count-only
`type=LOG,name=queue_dropped` profile metric. Its `total_count` is the number of
new lines rejected by a full queue during that profile interval. The disabled
build has no dependency on `brz-metrics`; `LogsGuard::dropped_lines` remains
available in both builds and is cumulative for the process lifetime.
