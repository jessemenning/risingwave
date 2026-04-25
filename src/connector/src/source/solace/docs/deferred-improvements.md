# Solace Connector — Deferred Improvements

Three medium-severity issues were identified during the April 2026 code review.
All three have since been implemented.  This document is retained as a reference
for the decisions made and the approaches taken.

---

## 1. ~~Fire-and-forget Publish — Persistent Delivery Not Confirmed~~ — **DONE**

**Implemented** in `src/connector/src/sink/solace.rs` and `jessemenning/solace-rs`
as of April 2026.  See `src/connector/src/sink/docs/publish-with-ack-implementation.md`
for the full implementation reference.

`write_chunk` now branches on `delivery_mode`:

- `"direct"` — fire-and-forget (best-effort by definition; no change).
- `"persistent"` / `"non_persistent"` — uses `session.publish_with_ack(msg)` to
  obtain a broker-ACK future, then registers it with
  `add_future.add_future_may_await(future)`.  RisingWave holds its checkpoint
  until all registered futures resolve, guaranteeing at-least-once delivery to
  the Solace broker.

`publish_with_ack` was added to `solace-rs` (`async_support.rs`) using a
correlation-tag map and the `SessionEvent::Acknowledgement` trampoline.

**Original problem (for context):** `write_chunk` ignored the
`DeliveryFutureManagerAddFuture` parameter (`_add_future`), making all publishes
fire-and-forget regardless of `delivery_mode`.  Silent data loss was possible on
broker back-pressure or restart when using persistent mode.

---

## 2. ~~Hardcoded JSON Encoder Precision~~ — **DONE**

**Implemented** in `src/connector/src/sink/solace.rs` as of April 2026.

`SolaceConfig` exposes two user-configurable fields:

- `solace.date_handling_mode` — `"from_ce"` (default), `"from_epoch"`, `"string"`
- `solace.timestamp_handling_mode` — `"milli"` (default), `"string"`

`TimestamptzHandlingMode` (for TIMESTAMPTZ columns) is read from `format_desc.options`
via the standard `"timestamptz.handling.mode"` key, consistent with other RisingWave sinks.

Parse helpers `parse_date_handling_mode` and `parse_timestamp_handling_mode` exist.
`SolaceSink::validate()` calls `build_json_encoder` to catch invalid values at creation time.

---

## 3. ~~No Connection Pooling for Status Table Writes~~ — **DONE**

**Implemented** in `src/connector/src/source/solace/source/reader.rs` as of
April 2026 (Option A).

The reader now maintains:

- `STATUS_POOL` — a `tokio::sync::OnceCell<bb8::Pool<PostgresConnectionManager>>`
  (max 5 connections) initialised lazily in `SolaceSplitReader::new()` from the
  configured DSN.  All status writes reuse pooled connections.
- `STATUS_SCHEMA_INITIALISED` — a `std::sync::OnceLock<()>` that ensures the
  `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ADD COLUMN IF NOT EXISTS` DDL block
  runs at most once per process lifetime via the `ensure_status_schema` helper.
- `barrier_flush_and_status` keeps its own dedicated connection for the long-lived
  `WAIT` statement so it never holds a pool slot during checkpointing.

**Original problem (for context):** each sentinel event opened a fresh
`tokio_postgres` connection and re-ran the full schema DDL.  At ≥10 concurrent
Solace sources restarting simultaneously, this could saturate RisingWave's
`max_connections` and cause silent status-write failures.

---

## Summary Table

| # | Issue | Status |
|---|-------|--------|
| 1 | ~~Fire-and-forget persistent publish~~ | **Done** — `publish_with_ack` + delivery future wiring |
| 2 | ~~Hardcoded encoder precision~~ | **Done** — `solace.date_handling_mode` / `timestamp_handling_mode` |
| 3 | ~~No connection pooling~~ | **Done** — `STATUS_POOL` + `STATUS_SCHEMA_INITIALISED` |

All three issues have been resolved.  The connector is production-ready for
typical deployments.
