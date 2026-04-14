# Solace Parallel Consumers — Sentinel Backfill Detection Roadmap

## Current state: Option D (shipped)

`solace.num_consumers = N` causes the enumerator to return N splits (IDs `"0"`
through `"N-1"`).  Each split gets its own `SolaceSplitReader` and its own
Solace flow bound to the same queue.  The broker load-balances messages across
all bound flows.

**Limitation:** sentinel-based backfill detection is **automatically disabled**
whenever `num_consumers > 1`.

The sentinel mechanism works by having the operator publish a single message
with user property `x-solace-sentinel: backfill-complete` to the queue after
all historical data has been replayed.  That message is delivered to exactly
one of the N consumers; the remaining N-1 readers never see it and can never
know that backfill is complete.

When `num_consumers > 1`, a received sentinel is acked and discarded with a
`WARN`-level log line.  No backfill state is set; no readiness event is
published.

---

## Option A: Shared sentinel state via checkpoint coordination

**Goal:** restore sentinel-based backfill detection for `num_consumers > 1` by
propagating the detecting reader's sentinel state to all other readers through
the normal checkpoint/split mechanism.

### How it works

1. **Detection:** one reader (call it reader R) receives the sentinel in its
   flow.  R sets `split.sentinel_detected = true` in the in-memory split state
   as it does today, then calls `barrier_flush_and_status` in a spawned task.

2. **Checkpoint propagation:** on the next RisingWave checkpoint, the framework
   calls `SplitMetaData::encode_to_json` on each split to persist state.  R's
   split now has `sentinel_detected: true`.  RisingWave persists this to the
   meta store.

3. **Re-enumeration:** after each checkpoint, the `SplitEnumerator::list_splits`
   is called.  Currently it returns fresh `SolaceSplit::new(...)` objects that
   reset all state.  **The enumerator must instead preserve in-flight sentinel
   state from the previously assigned splits.**

4. **State injection:** when `SplitReader::new` is called with a split that
   already carries `sentinel_detected: true`, the reader skips re-running the
   barrier flush (it already ran) and re-publishes the readiness event as it
   already does on restart.

### Required changes

#### `SolaceSplitEnumerator`

The enumerator currently holds only `queue_name` and `num_consumers`; it has
no memory of prior split state.

New design:

```rust
pub struct SolaceSplitEnumerator {
    queue_name: String,
    num_consumers: usize,
    /// Last-seen split state, keyed by split_id.
    /// Populated by the framework calling assign_splits() / update with
    /// the checkpointed splits after each barrier.
    known_splits: HashMap<SplitId, SolaceSplit>,
}
```

`list_splits` merges: if a split ID already exists in `known_splits`, return
the persisted split (preserving `sentinel_detected` and `sentinel_detected_at`);
otherwise return a fresh split.

To receive updated split state, the enumerator needs to implement the
`SplitEnumerator::update_with_splits` hook (or equivalent framework callback
— check the `SplitEnumerator` trait for the exact method name in the current
RisingWave version).

#### Sentinel guard in `reader.rs`

Remove the `num_consumers > 1` early-return guards added by Option D.  The
normal detection path (`self.sentinel_detected` check, barrier flush spawn,
etc.) handles all cases once the enumerator correctly forwards state.

#### Readiness event coordination (optional hardening)

With Option A, all N readers independently re-publish the readiness event on
restart (because all splits carry `sentinel_detected: true` after propagation).
This is idempotent for downstream consumers, but noisy.  To suppress redundant
publishes, add a `readiness_published_at` field to `SolaceSplit` and only
publish if that field is absent.

### Why this is non-trivial

- The `SplitEnumerator` trait in RisingWave does not currently have a
  standardized "receive last checkpointed split states" callback in all
  connector configurations.  You may need to thread the state through
  the properties or through a side-channel (e.g. a shared `Arc<Mutex<...>>`
  between the enumerator and reader factory).
- The barrier flush task is idempotent (WAIT + upsert) so it is safe for
  reader R to spawn it while the other N-1 readers detect `sentinel_detected =
  true` from the next checkpoint and skip it.  The ordering guarantee is that
  the status table is written *after* WAIT completes, regardless of which
  reader did the detection.
- Testing requires a real Solace broker with N flows bound; the unit-test
  approach used for Option D is not sufficient.

### Acceptance criteria

- `num_consumers = 4`, sentinel published once → exactly one `WAIT` issues →
  `rw_solace_connector_status.is_ready = TRUE` → readiness event published on
  the configured topic.
- On connector restart with N readers and checkpointed `sentinel_detected =
  true`, all readers boot into "live" mode without re-running WAIT.
- Sentinel discarded with `WARN` log when `sentinel_detected` is already `true`
  (duplicate sentinel resilience, same as today).
