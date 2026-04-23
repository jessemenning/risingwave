# Solace Sink Connector

The Solace sink connector publishes rows from a RisingWave materialized view or table to a Solace Platform broker using the SMF protocol.

## Source files

| File | Purpose |
|------|---------|
| `src/connector/src/sink/solace.rs` | Main sink implementation (733 lines) |
| `src/connector/src/connector_common/solace_common.rs` | Shared connection config struct |
| `src/connector/src/sink/mod.rs` | Registration via `for_all_sinks!` macro |

Feature-gated under the `"solace"` Cargo feature.

---

## CREATE SINK syntax

```sql
CREATE SINK <sink_name>
FROM <source_or_mv>
WITH (
    connector = 'solace',
    type = 'append-only',

    -- Required
    "solace.url"            = 'tcp://broker.example.com:55555',
    "solace.topic"          = 'acme/orders/{region}/{status}/v1',

    -- Auth (optional)
    "solace.vpn_name"       = 'default',
    "solace.username"       = 'admin',
    "solace.password"       = 'secret',

    -- Connection tuning (optional)
    "solace.client_name"              = 'rw-sink-001',
    "solace.ssl_trust_store_dir"      = '/etc/ssl/certs',
    "solace.reconnect_retries"        = '-1',
    "solace.reconnect_retry_wait_ms"  = '3000',
    "solace.connect_timeout_ms"       = '30000',

    -- Delivery (optional)
    "solace.delivery_mode"  = 'direct',   -- 'direct' | 'persistent' | 'non_persistent'

    -- Deduplication (optional; both required together)
    "solace.dedup_key"      = 'order_id,region',
    "solace.dedup_window"   = '5m'        -- '300', '300s', '5m', '1h'
) FORMAT PLAIN ENCODE JSON;
```

**Constraints:**
- `type = 'append-only'` is the only supported mode.
- `FORMAT PLAIN ENCODE JSON` is the only supported format.
- `solace.dedup_key` and `solace.dedup_window` must both be set or both omitted.

---

## Configuration reference

### Connection options (shared with source connector)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `solace.url` | String | **required** | Broker URL, e.g. `tcp://host:55555` or `tcps://host:55443` |
| `solace.vpn_name` | String | `"default"` | Message VPN name |
| `solace.username` | String | — | Auth username |
| `solace.password` | String | — | Auth password |
| `solace.client_name` | String | auto | SMF client name (auto-generated if omitted) |
| `solace.ssl_trust_store_dir` | String | — | Path to TLS trust store directory |
| `solace.reconnect_retries` | i64 | `-1` | Reconnect retry count; `-1` = infinite |
| `solace.reconnect_retry_wait_ms` | u64 | `3000` | Wait between reconnects (ms) |
| `solace.connect_timeout_ms` | u64 | `30000` | Connection timeout (ms) |

### Sink-specific options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `solace.topic` | String | **required** | Parameterized topic template (see below) |
| `solace.delivery_mode` | String | `"direct"` | `direct`, `persistent`, or `non_persistent` |
| `solace.dedup_key` | String | — | Comma-separated column names for dedup key |
| `solace.dedup_window` | String | — | Suppression window; required when `dedup_key` is set |

---

## Topic templating

`solace.topic` is a parameterized template. Each `{column_name}` placeholder is replaced at write time with that column's value from the current row.

**Rules:**
- Every placeholder must reference a column in the sink's schema.
- No `{...}` segment may contain `/` (would produce an illegal topic hierarchy).
- Supported column types: `Utf8`, `Int16`, `Int32`, `Int64`, `Float32`, `Float64`, `Bool`, `Decimal`, `Serial`. Other types produce a `NotSupported` error.

**Examples:**

```
acme/orders/{region}/{status}/v1
sensors/{device_id}/telemetry
events/{event_type}/v2
```

---

## Delivery modes

| Value | SMF mode | Notes |
|-------|----------|-------|
| `direct` (default) | Direct | Lowest latency; no broker persistence |
| `persistent` | Persistent | Guaranteed delivery; requires a queue |
| `non_persistent` | Non-Persistent | Broker buffers but does not persist |

---

## Deduplication

When `solace.dedup_key` is set, the sink maintains an in-memory map of recently emitted key values. A row is published only if its dedup key has **not** been seen within the `solace.dedup_window` interval.

- **`solace.dedup_key`**: comma-separated column names (e.g. `"order_id"` or `"order_id,region"`)
- **`solace.dedup_window`**: duration string — `"300"` (plain seconds), `"300s"`, `"5m"`, `"1h"`
- State is **in-memory only** — lost on restart. Suitable for short-window suppression of duplicate events.

---

## Message format

Each published message corresponds to one row. The payload is a JSON object containing all columns of the row.

The encoder uses:
- `TimestampHandlingMode::Milli` — timestamps serialized as milliseconds since epoch
- Optional timezone from `FORMAT ... ENCODE JSON (...)` options

---

## Architecture

```
SolaceSink  (implements Sink trait)
  ├── validate()         — checks config, template, delivery mode, dedup, encoder, and broker connectivity
  └── new_log_sinker()  → SolaceSinkWriter

SolaceSinkWriter  (implements AsyncTruncateSinkWriter trait)
  ├── _context: Context          — keeps C-level solace context alive
  ├── session: AsyncSession      — solace-rs async SMF session
  ├── topic_template: TopicTemplate
  ├── delivery_mode: SolaceDeliveryMode
  ├── encoder: JsonEncoder
  └── dedup_filter: Option<DedupFilter>

write_chunk() for each row:
  1. dedup_filter.should_emit(row)  →  skip if suppressed
  2. topic = topic_template.render(row)
  3. payload = encoder.encode(row)  →  JSON bytes
  4. session.publish(topic, payload, delivery_mode)
```

---

## Validation at CREATE SINK time

`SolaceSink::validate()` checks these in order:

1. Config deserialization succeeds.
2. Topic template parses — all `{col}` references resolve to schema columns.
3. Delivery mode string is valid.
4. Dedup config is consistent (both set or both absent).
5. JSON encoder builds without error.
6. A real TCP connection to the broker succeeds (early connectivity check).

If the broker is unreachable, `CREATE SINK` fails immediately.

---

## Dependency notes

The connector depends on `jessemenning/solace-rs` (fork of upstream), not the published crate. The fork adds:

- `InboundMessage::is_redelivered()`
- `InboundMessage::get_replication_group_message_id()`
- `InboundMessage::get_payload_as_string()`
- `InboundMessage::get_rcv_timestamp()`
- `AsyncSessionBuilder::client_name()`

Solace C SDK version: **7.33.2.3**. OpenSSL 3.0.8 is statically embedded in `libsolclient.a` — no separate ssl/crypto link targets are needed.
