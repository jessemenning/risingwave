# Persistent Delivery ACK — Implementation Reference

> **Status: COMPLETE.** Both changes described here are already in the codebase.
> This document records what was implemented and where to find it.

---

## Background

The Solace sink's `write_chunk` originally ignored the
`DeliveryFutureManagerAddFuture` parameter (named `_add_future`), making all
publishes fire-and-forget regardless of `delivery_mode`. For `"persistent"` and
`"non_persistent"` delivery modes this was misleading: users expected RisingWave
to hold its checkpoint until the broker confirmed durability, but it did not.

Two changes were needed:

1. Add `publish_with_ack` to `solace-rs` (the Rust wrapper around the Solace C SDK)
2. Wire `add_future` in the RisingWave sink to use it

Both are done.

---

## Change 1 — `publish_with_ack` in solace-rs

**File:** `solace-rs/src/async_support.rs` (also at `risingwave/solace-rs/src/async_support.rs`)

### How it works

`publish_with_ack` assigns a monotonically-incrementing `u64` correlation ID to the
outbound message via `message.set_correlation_tag(&corr_id.to_ne_bytes())`, then
stores a `oneshot::Sender` in a shared `Arc<Mutex<HashMap<u64, oneshot::Sender>>>`:

```rust
pub fn publish_with_ack(
    &self,
    message: OutboundMessage,
) -> Result<oneshot::Receiver<Result<(), SessionError>>, SessionError> {
    let corr_id = self.next_corr_id.fetch_add(1, Ordering::Relaxed);
    message.set_correlation_tag(&corr_id.to_ne_bytes());

    let (tx, rx) = oneshot::channel();
    self.ack_senders.lock().unwrap().insert(corr_id, tx);

    match self.publish(message) {
        Ok(()) => Ok(rx),
        Err(e) => {
            self.ack_senders.lock().unwrap().remove(&corr_id);
            Err(e)
        }
    }
}
```

The session's event handler intercepts `SessionEvent::Acknowledgement` and
`SessionEvent::RejectedMsgError`, reads the correlation pointer from the
`CURRENT_EVENT_CORR_PTR` thread-local (set by the C SDK callback trampoline),
converts it to a `u64`, and sends `Ok(())` or `Err(SessionError::AcknowledgementRejected)`
through the stored sender.

The returned `oneshot::Receiver` resolves as soon as the broker responds.

### Important note on `delivery_mode = "direct"`

Direct messages are best-effort — the broker never sends acknowledgements. Calling
`publish_with_ack` on a direct message will return a receiver that never resolves.
**Do not use `publish_with_ack` for direct delivery.** The sink branches on
`delivery_mode` to handle this correctly (see Change 2 below).

---

## Change 2 — Sink wiring in risingwave

**File:** `src/connector/src/sink/solace.rs`  
**Location:** `SolaceSinkWriter::write_chunk` (~line 662)

`write_chunk` now branches on `self.delivery_mode`:

```rust
match self.delivery_mode {
    SolaceDeliveryMode::Direct => {
        self.session
            .publish(msg)
            .map_err(|e| SinkError::Solace(anyhow!("Solace publish error: {}", e)))?;
        // No ACK tracking for direct — intentional.
    }
    SolaceDeliveryMode::Persistent | SolaceDeliveryMode::NonPersistent => {
        let ack_rx = self
            .session
            .publish_with_ack(msg)
            .map_err(|e| SinkError::Solace(anyhow!("Solace publish error: {}", e)))?;
        let future = ack_rx.map(|r| match r {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                Err(SinkError::Solace(anyhow!("Solace broker rejected message: {}", e)))
            }
            Err(_) => Err(SinkError::Solace(anyhow!(
                "Solace ACK receiver dropped before broker confirmed delivery"
            ))),
        });
        add_future.add_future_may_await(future).await?;
    }
}
```

`add_future.add_future_may_await(future)` registers the broker-ACK future with
RisingWave's delivery tracking system. RisingWave will not advance its checkpoint
for this sink until all registered futures for the current batch have resolved —
guaranteeing at-least-once delivery to the Solace broker for persistent and
non-persistent modes.

The `SolaceSinkDeliveryFuture` opaque type is defined at the top of the file (~line 49)
and declared via `#[define_opaque(SolaceSinkDeliveryFuture)]` on `write_chunk`.

---

## Delivery guarantee summary

| `delivery_mode` | Broker ACK required before checkpoint? | Notes |
|---|---|---|
| `direct` | No | Best-effort by definition; fire-and-forget |
| `persistent` | Yes | Broker persists to disk before ACKing |
| `non_persistent` | Yes | Broker ACKs after accepting (not persisted) |

---

## Testing

The sink unit tests in `src/connector/src/sink/solace.rs` cover delivery mode
parsing and message construction. End-to-end ACK verification requires a live
Solace broker — the integration test suite in `tests/` should be used for that.
