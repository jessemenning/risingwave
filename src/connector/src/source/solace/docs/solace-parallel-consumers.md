# Solace Parallel Consumers — Sentinel Backfill Detection

## How it works

`solace.num_consumers = N` causes the enumerator to return N splits (IDs `"0"`
through `"N-1"`). Each split gets its own `SolaceSplitReader` and its own Solace
flow bound to the same queue. The broker load-balances messages across all bound
flows.

Because each flow receives a different subset of messages, the sentinel message
(`x-solace-sentinel: backfill-complete`) is delivered to exactly one flow. The
remaining N-1 readers never see it directly.

---

## Cross-reader coordination via the status table

**Implemented April 2026.**

The detecting reader runs the normal sentinel sequence:

1. ACKs the sentinel on Solace.
2. Spawns `barrier_flush_and_status` — issues `WAIT`, then writes `is_ready = TRUE`
   to `rw_solace_connector_status`.
3. Publishes the readiness event to `solace.sentinel_readiness_topic` (if configured).

All other readers learn that backfill is complete via the status table:

- **At startup** (`SolaceSplitReader::new()`), if `sentinel_detected = false` in the
  checkpoint, the reader queries `rw_solace_connector_status` for its queue name.
  If `is_ready = TRUE`, it sets `sentinel_detected = true` in memory and boots
  directly into live mode — no barrier re-run, no duplicate readiness event.

- **On restart after the original detection**, the detecting reader's checkpoint
  already carries `sentinel_detected = true`; it re-publishes the readiness event
  only if `readiness_published_at` is `None` in the split state.

---

## Readiness event suppression (`readiness_published_at`)

`SolaceSplit` carries a `readiness_published_at: Option<String>` field. After the
readiness event is published, this field is set to the current ISO 8601 timestamp and
persisted in the checkpoint. On the next restart, the reader skips re-publishing if
`readiness_published_at` is already set.

This is **best-effort**: the field is only updated through the checkpoint path when
`SplitMetaData::update_offset` is called, which does not occur on sentinel publication.
In practice multiple readers may all publish the readiness event on the first restart
after sentinel detection; all publishes are idempotent for consumers.

---

## Duplicate sentinel resilience

If two messages with `x-solace-sentinel: backfill-complete` are published, the second
sentinel will arrive at `SolaceSplitReader::into_data_stream` when `self.sentinel_detected`
is already `true`. The reader logs a `WARN` and discards it.

---

## Acceptance criteria

- `num_consumers = 4`, sentinel published once → exactly one `WAIT` issues →
  `rw_solace_connector_status.is_ready = TRUE` → readiness event published.
- On restart with N readers and `is_ready = TRUE` in the status table, all readers
  boot into live mode without re-running `WAIT`.
- A duplicate sentinel is discarded with `WARN` log.
