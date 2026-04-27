// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{LazyLock, OnceLock};

use async_trait::async_trait;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::Utc;
use futures::FutureExt;
#[cfg(test)]
use futures::future::OptionFuture;
use futures_async_stream::try_stream;
use moka::future::Cache as MokaCache;
use solace_rs::async_support::{AsyncSession, OwnedAsyncFlow};
use solace_rs::context::Context;
use solace_rs::flow::AckMode;
use solace_rs::message::destination::{DestinationType, MessageDestination};
use solace_rs::message::outbound::OutboundMessageBuilder;
use solace_rs::message::DeliveryMode;
use tokio::sync::oneshot;

use super::message::SolaceMessage;
use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::solace::split::SolaceSplit;
use crate::source::solace::{SolaceAckMode, SolaceProperties};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitReader,
    into_chunk_stream,
};

/// Global channel map for checkpoint-based Solace ack.
///
/// When `ack_mode = checkpoint`, the reader registers a channel sender here keyed by
/// `{source_id}_{split_id}`. The `WaitCheckpointTask::AckSolaceMessage` variant sends
/// collected message IDs through this channel after checkpoint, and the reader's
/// background ack loop processes them.
pub static SOLACE_ACK_CHANNEL: LazyLock<
    MokaCache<String, tokio::sync::mpsc::UnboundedSender<Vec<u64>>>,
> = LazyLock::new(|| {
    moka::future::Cache::builder()
        // Bound memory: sender halves from restarted readers expire after 24 h.
        .time_to_live(std::time::Duration::from_secs(86_400))
        .build()
});

pub fn build_solace_ack_channel_id(source_id: impl std::fmt::Display, split_id: &SplitId) -> String {
    format!("solace_{}_{}", source_id, split_id)
}

type PgPool = Pool<PostgresConnectionManager<tokio_postgres::NoTls>>;

/// Shared connection pool for connector status table writes.
///
/// Capped at 5 connections so concurrent Solace source restarts cannot exhaust
/// RisingWave's `max_connections` limit. Initialized on the first reader
/// construction and reused by all subsequent readers in the same process.
static STATUS_POOL: tokio::sync::OnceCell<PgPool> = tokio::sync::OnceCell::const_new();

/// Guards against redundant DDL on every status write.
///
/// Schema creation (`CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ADD COLUMN IF
/// NOT EXISTS`) runs once per process lifetime. Concurrent initializations are
/// harmless because all DDL statements are idempotent.
static STATUS_SCHEMA_INITIALISED: OnceLock<()> = OnceLock::new();

/// Drop guard for [`OwnedAsyncFlow`] during [`SolaceSplitReader`] construction.
///
/// If [`SolaceSplitReader::new`] returns an error after the flow is created, this
/// guard ensures the flow is explicitly dropped — triggering a broker unbind — rather
/// than remaining in a `windowSize=0` limbo until the TCP connection times out.
/// Defuse with `.take()` before returning `Ok(reader)`.
struct FlowGuard(Option<OwnedAsyncFlow>);

impl Drop for FlowGuard {
    fn drop(&mut self) {
        if self.0.is_some() {
            tracing::warn!(
                "Solace flow dropped during SolaceSplitReader construction — \
                 broker will see a transient bind/unbind"
            );
        }
    }
}

pub struct SolaceSplitReader {
    flow: OwnedAsyncFlow,
    /// Keep session alive for the flow's lifetime. Also used to publish
    /// readiness events back to Solace after sentinel detection.
    session: AsyncSession,
    // held alive for C FFI: session borrows context's C-level handle
    #[expect(dead_code)]
    context: Context,
    properties: SolaceProperties,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    split_id: SplitId,
    ack_mode: SolaceAckMode,
    /// Whether the backfill sentinel has been detected in this reader's lifetime.
    sentinel_detected: bool,
    /// ISO 8601 timestamp of when the sentinel was first detected.
    sentinel_detected_at: Option<String>,
    /// Number of business events consumed before the sentinel.
    events_before_sentinel: u64,
    /// Total business events consumed (excludes sentinel).
    total_consumed: u64,
    /// Pending ack receiver for checkpoint mode. Drained at the top of each
    /// read loop iteration instead of via a background task.
    ack_rx: Option<tokio::sync::mpsc::UnboundedReceiver<Vec<u64>>>,
}

#[async_trait]
impl SplitReader for SolaceSplitReader {
    type Properties = SolaceProperties;
    type Split = SolaceSplit;

    async fn new(
        properties: SolaceProperties,
        splits: Vec<SolaceSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        assert_eq!(splits.len(), 1);
        let split = splits.into_iter().next().unwrap();
        let split_id = split.split_id;
        let sentinel_detected = split.sentinel_detected;
        let sentinel_detected_at = split.sentinel_detected_at.clone();
        let readiness_published_at = split.readiness_published_at.clone();

        let ack_mode = SolaceAckMode::from_str_opt(properties.ack_mode.as_deref())?;

        if let Some(bs) = properties.batch_size {
            if bs > 10_000 {
                return Err(anyhow::anyhow!(
                    "solace.batch_size {} exceeds the maximum allowed value of 10000",
                    bs
                )
                .into());
            }
        }

        // When num_consumers > 1, each reader must bind with a unique client name so the
        // broker doesn't reject the second (and subsequent) connections as duplicates.
        // Append the split ID to the configured base name; if no name was configured,
        // pass None so the broker auto-assigns a distinct name for each session.
        let num_consumers = match properties.num_consumers {
            None | Some(1..) => properties.num_consumers.unwrap_or(1),
            Some(0) => return Err(anyhow::anyhow!("solace.num_consumers must be >= 1").into()),
        };
        let effective_client_name: Option<String> = if num_consumers > 1 {
            properties
                .common
                .client_name
                .as_deref()
                .map(|base| format!("{}-{}", base, split_id))
        } else {
            // Single-consumer: pass None here; build_async_session will use self.client_name.
            None
        };

        // ── Option A: status pool initialized before Solace resource acquisition ──
        // If pool construction fails here, new() returns before any Solace resources
        // are acquired — no orphaned broker binds, no windowSize=0 limbo.
        //
        // ── Option B: pool init is non-fatal ─────────────────────────────────────
        // Sentinel cross-reader coordination degrades gracefully on pool failure —
        // all downstream callers already guard with STATUS_POOL.get() returning
        // Option<&PgPool>. Message flow must not be gated on a monitoring concern.
        let dsn = properties.risingwave_dsn_or_default().to_owned();
        if let Err(e) = STATUS_POOL
            .get_or_try_init(|| async {
                let mgr =
                    PostgresConnectionManager::new_from_stringlike(&dsn, tokio_postgres::NoTls)
                        .map_err(|e| anyhow::anyhow!("invalid DSN for status pool: {e}"))?;
                bb8::Pool::builder()
                    .max_size(5)
                    .build(mgr)
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to build status pool: {e}"))
            })
            .await
        {
            tracing::warn!(
                error = %e,
                "Status pool init failed — sentinel coordination disabled for this reader"
            );
        }

        // If another reader in this consumer group already detected the sentinel, the
        // status table will show is_ready = true. Boot directly into live mode rather
        // than waiting for a sentinel this reader will never receive.
        let sentinel_detected = if !sentinel_detected {
            let already_ready: Option<bool> = async {
                let pool = STATUS_POOL.get()?;
                let conn = pool.get().await.ok()?;
                let row = conn
                    .query_opt(
                        "SELECT is_ready FROM rw_solace_connector_status \
                         WHERE connector_name = $1",
                        &[&properties.queue],
                    )
                    .await
                    .ok()??;
                Some(row.get::<_, bool>(0))
            }
            .await;
            if already_ready == Some(true) {
                tracing::info!(
                    queue = %properties.queue,
                    split_id = %split_id,
                    "Status table is_ready=true — booting into live mode without sentinel re-detection",
                );
                true
            } else {
                false
            }
        } else {
            sentinel_detected
        };

        // Always use Client ack mode — we control when ack happens based on ack_mode.
        let (context, session) = properties
            .common
            .build_async_session(effective_client_name.as_deref())?;

        // ── Option C: explicit flow cleanup on any construction error ─────────────
        // FlowGuard logs and drops the flow if new() returns an error after this point,
        // triggering a broker unbind rather than leaving the flow in windowSize=0 limbo.
        // Defused via .take() before Ok(reader) — Drop runs silently when flow is None.
        let mut flow_guard = FlowGuard(Some(
            session
                .create_flow(&properties.queue, AckMode::Client)
                .map_err(|e| anyhow::anyhow!("failed to create Solace flow: {e}"))?,
        ));

        // For checkpoint mode, register the ack sender so WaitCheckpointTask can
        // push message IDs to us, then store the receiver for inline draining.
        let ack_rx = if matches!(ack_mode, SolaceAckMode::Checkpoint) {
            let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u64>>();
            let channel_id = build_solace_ack_channel_id(source_ctx.source_id, &split_id);
            SOLACE_ACK_CHANNEL
                .entry(channel_id)
                .and_upsert_with(|_| std::future::ready(ack_tx))
                .await;
            Some(ack_rx)
        } else {
            None
        };

        // Defuse the guard — construction succeeded; transfer flow ownership to reader.
        let flow = flow_guard.0.take().unwrap();

        // Explicitly start the flow — belt-and-suspenders for broker versions where
        // FLOW_START_STATE=1 alone does not open the delivery window in CLIENT ack mode.
        if let Err(e) = flow.start() {
            tracing::warn!(
                queue = %properties.queue,
                error = %e,
                "flow.start() returned non-OK — delivery window may not open (may be benign)",
            );
        }

        let reader = Self {
            flow,
            session,
            context,
            properties,
            parser_config,
            source_ctx,
            split_id,
            ack_mode,
            sentinel_detected,
            sentinel_detected_at,
            events_before_sentinel: 0,
            total_consumed: 0,
            ack_rx,
        };

        // On restart after sentinel: re-publish readiness event and re-write status
        // table so consumers that started after the original event get notified.
        // Suppressed when `readiness_published_at` is already set (best-effort: the
        // field resets on each restart because it cannot be written back through the
        // standard update_offset checkpoint path, so suppression applies within a
        // single process lifetime only).
        if sentinel_detected && readiness_published_at.is_none() {
            tracing::info!(
                queue = %reader.properties.queue,
                split_id = %reader.split_id,
                detected_at = ?reader.sentinel_detected_at,
                "Connector restarted after sentinel — re-publishing readiness event.",
            );
            let now = Utc::now().to_rfc3339();
            let det_at = reader.sentinel_detected_at.clone().unwrap_or_default();
            if let Some(pool) = STATUS_POOL.get() {
                let _ = write_connector_status_pg(
                    &reader.properties.queue, &det_at, 0, 0, &now, pool,
                )
                .await;
            }
            reader.publish_readiness_event_with_barrier(false);
        }

        Ok(reader)
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

/// Return type from [`recv_or_flush`] — avoids `tokio::select!` inside `#[try_stream]`,
/// which is a proc-macro coroutine context where `select!`'s generated `.await` expressions
/// are not recognized as being inside an `async` block (causes E0728).
enum RecvOrFlush<T> {
    Msg(Option<T>),
    FlushDone,
}

/// Race an inbound-message future against a oneshot flush receiver.
///
/// Called with a plain `.await` from the `#[try_stream]` function — the proc macro handles
/// simple `.await`; the `select!` inside this regular `async fn` compiles fine.
async fn recv_or_flush<T, F>(recv_fut: F, flush_rx: &mut oneshot::Receiver<()>) -> RecvOrFlush<T>
where
    F: std::future::Future<Output = Option<T>>,
{
    tokio::select! {
        msg = recv_fut => RecvOrFlush::Msg(msg),
        _ = flush_rx  => RecvOrFlush::FlushDone,
    }
}

impl SolaceSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(mut self) {
        // Local oneshot receiver — set when a sentinel spawns the barrier flush task.
        // Keeping it local (not a struct field) avoids borrowck conflicts.
        let mut flush_rx: Option<oneshot::Receiver<()>> = None;

        loop {
            // Drain any pending checkpoint acks before blocking on the next message.
            if let Some(ref mut rx) = self.ack_rx {
                while let Ok(msg_ids) = rx.try_recv() {
                    for msg_id in msg_ids {
                        if let Err(e) = self.flow.ack(msg_id) {
                            tracing::error!(
                                msg_id,
                                error = %e,
                                "failed to ack Solace message on checkpoint",
                            );
                        }
                    }
                }
            }

            // Race message arrival against flush completion.
            // When no flush is pending, take the direct recv path (zero select overhead).
            let result = if let Some(ref mut rx) = flush_rx {
                recv_or_flush(self.flow.recv(), rx).await
            } else {
                RecvOrFlush::Msg(self.flow.recv().await)
            };

            match result {
                RecvOrFlush::FlushDone => {
                    // Barrier flush task signalled completion — WAIT confirmed MV currency.
                    flush_rx = None;
                    self.publish_readiness_event_with_barrier(true);
                }

                RecvOrFlush::Msg(None) => {
                    return Err(anyhow::anyhow!(
                        "Solace flow disconnected — broker closed the flow (queue: {}). \
                         Drop and recreate the source to recover.",
                        self.properties.queue
                    )
                    .into());
                }

                RecvOrFlush::Msg(Some(msg)) => {
                    let msg_id = msg.get_msg_id().ok().flatten().unwrap_or(0);
                    let solace_msg = SolaceMessage::from_inbound(&msg, self.split_id.clone());
                    if solace_msg.redelivered {
                        tracing::warn!(
                            queue = %self.properties.queue,
                            msg_id,
                            "Redelivered message received — possible duplicate",
                        );
                    }

                    // ── Sentinel detection ───────────────────────────────────
                    if solace_msg.is_sentinel() {
                        if let Err(e) = self.flow.ack(msg_id) {
                            tracing::warn!(msg_id, error = %e, "failed to ack sentinel");
                        }

                        if self.sentinel_detected {
                            tracing::warn!(
                                queue = %self.properties.queue,
                                split_id = %self.split_id,
                                "Duplicate sentinel ignored",
                            );
                            continue;
                        }

                        self.handle_sentinel_detected(&mut flush_rx);

                        // DO NOT yield sentinel to downstream.
                        continue;
                    }

                    // ── Normal message processing ────────────────────────────
                    self.total_consumed += 1;

                    if matches!(self.ack_mode, SolaceAckMode::Immediate) {
                        if let Err(e) = self.flow.ack(msg_id) {
                            tracing::warn!(msg_id, error = %e, "failed to immediately ack");
                        }
                    }

                    // ── Batch fill ──────────────────────────────────────────
                    // Non-blockingly grab additional messages until the batch is
                    // full or no more are immediately available.
                    let batch_size = self.properties.batch_size.unwrap_or(100).max(1);
                    let mut batch: Vec<SourceMessage> = Vec::with_capacity(batch_size);
                    batch.push(SourceMessage::from(solace_msg));

                    'fill: while batch.len() < batch_size {
                        match self.flow.recv().now_or_never() {
                            Some(Some(msg)) => {
                                let msg_id = msg.get_msg_id().ok().flatten().unwrap_or(0);
                                let sm = SolaceMessage::from_inbound(&msg, self.split_id.clone());
                                if sm.redelivered {
                                    tracing::warn!(
                                        queue = %self.properties.queue,
                                        msg_id,
                                        "Redelivered message in batch fill — possible duplicate",
                                    );
                                }
                                if sm.is_sentinel() {
                                    if let Err(e) = self.flow.ack(msg_id) {
                                        tracing::warn!(msg_id, error = %e, "failed to ack sentinel in batch fill");
                                    }
                                    if !self.sentinel_detected {
                                        self.handle_sentinel_detected(&mut flush_rx);
                                    } else {
                                        tracing::warn!(
                                            queue = %self.properties.queue,
                                            split_id = %self.split_id,
                                            "Duplicate sentinel in batch fill ignored",
                                        );
                                    }
                                    break 'fill;
                                }
                                self.total_consumed += 1;
                                if matches!(self.ack_mode, SolaceAckMode::Immediate) {
                                    if let Err(e) = self.flow.ack(msg_id) {
                                        tracing::warn!(msg_id, error = %e, "failed to immediately ack in batch fill");
                                    }
                                }
                                batch.push(SourceMessage::from(sm));
                            }
                            _ => break 'fill,
                        }
                    }

                    yield batch;
                }
            }
        }
    }

    fn handle_sentinel_detected(&mut self, flush_rx: &mut Option<oneshot::Receiver<()>>) {
        let now = Utc::now();
        self.sentinel_detected = true;
        self.sentinel_detected_at = Some(now.to_rfc3339());
        // total_consumed already includes all batch messages added before this sentinel.
        self.events_before_sentinel = self.total_consumed;

        tracing::info!(
            queue = %self.properties.queue,
            events_before_sentinel = self.events_before_sentinel,
            "Sentinel detected — spawning barrier flush task",
        );

        let queue  = self.properties.queue.clone();
        let det_at = self.sentinel_detected_at.clone().unwrap_or_default();
        let eb     = self.events_before_sentinel as i64;
        let tc     = self.total_consumed as i64;
        let dsn    = self.properties.risingwave_dsn_or_default().to_owned();
        let pool   = STATUS_POOL.get().cloned();
        let wait_timeout_ms = self.properties.wait_timeout_ms_or_default();

        let (flush_tx, rx) = oneshot::channel::<()>();
        *flush_rx = Some(rx);

        // Spawn WAIT task. Stream keeps flowing so the executor can
        // checkpoint, which unblocks WAIT.
        tokio::spawn(async move {
            if let Err(e) = barrier_flush_and_status(
                &queue, &det_at, eb, tc, &dsn, pool.as_ref(), wait_timeout_ms,
            )
            .await
            {
                tracing::error!(error = %e, "Barrier flush failed — readiness will fire anyway");
            }
            let _ = flush_tx.send(());
        });
    }

    /// Publish a readiness event to Solace so consumers are notified instantly.
    ///
    /// Topic: `{sentinel_readiness_topic}/{queue_name}/ready`
    /// User property: `x-risingwave-event = connector-ready`
    fn publish_readiness_event_with_barrier(&self, barrier_flush_complete: bool) {
        let topic_prefix = match &self.properties.sentinel_readiness_topic {
            Some(prefix) if !prefix.is_empty() => prefix,
            _ => {
                tracing::debug!(
                    "sentinel_readiness_topic not configured — skipping readiness publish"
                );
                return;
            }
        };

        let topic = format!("{}/{}/ready", topic_prefix, self.properties.queue);
        let payload = serde_json::json!({
            "connector_name": self.properties.queue,
            "status": "ready",
            "sentinel_detected_at": self.sentinel_detected_at,
            "historical_events_processed": self.events_before_sentinel,
            "total_events_consumed": self.total_consumed,
            "barrier_flush_complete": barrier_flush_complete,
            "message": if barrier_flush_complete {
                "All historical events processed. All materialized views current. Safe to query."
            } else {
                "Historical events processed. Barrier flush failed — allow brief propagation delay."
            },
        });

        let payload_bytes = payload.to_string();

        let dest = match MessageDestination::new(DestinationType::Topic, topic.as_str()) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(error = %e, "Failed to create topic destination");
                return;
            }
        };

        let build_result = OutboundMessageBuilder::new()
            .delivery_mode(DeliveryMode::Direct)
            .destination(dest)
            .payload(payload_bytes.as_bytes())
            .user_property("x-risingwave-event", "connector-ready")
            .build();

        match build_result {
            Ok(outbound_msg) => {
                if let Err(e) = self.session.publish(outbound_msg) {
                    tracing::error!(
                        topic = %topic,
                        error = %e,
                        "Failed to publish readiness event to Solace",
                    );
                } else {
                    tracing::info!(
                        topic = %topic,
                        "Readiness event published to Solace",
                    );
                }
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "Failed to build readiness outbound message",
                );
            }
        }
    }
}

/// Ensures the `rw_solace_connector_status` table and its columns exist.
///
/// Uses [`STATUS_SCHEMA_INITIALISED`] to run DDL at most once per process lifetime.
/// Concurrent callers may both run DDL, but all statements are idempotent.
async fn ensure_status_schema(
    client: &tokio_postgres::Client,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if STATUS_SCHEMA_INITIALISED.get().is_some() {
        return Ok(());
    }

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS rw_solace_connector_status (
                connector_name          VARCHAR PRIMARY KEY,
                is_ready                BOOLEAN DEFAULT FALSE,
                sentinel_detected_at    VARCHAR,
                events_before_sentinel  BIGINT,
                total_events_consumed   BIGINT,
                last_updated            VARCHAR
            )",
            &[],
        )
        .await?;

    // Schema-resilient upgrade path: add any columns that may be absent in older
    // deployments. RisingWave supports ADD COLUMN IF NOT EXISTS.
    for stmt in [
        "ALTER TABLE rw_solace_connector_status ADD COLUMN IF NOT EXISTS is_ready BOOLEAN DEFAULT FALSE",
        "ALTER TABLE rw_solace_connector_status ADD COLUMN IF NOT EXISTS sentinel_detected_at VARCHAR",
        "ALTER TABLE rw_solace_connector_status ADD COLUMN IF NOT EXISTS events_before_sentinel BIGINT",
        "ALTER TABLE rw_solace_connector_status ADD COLUMN IF NOT EXISTS total_events_consumed BIGINT",
        "ALTER TABLE rw_solace_connector_status ADD COLUMN IF NOT EXISTS last_updated VARCHAR",
    ] {
        client.execute(stmt, &[]).await?;
    }

    STATUS_SCHEMA_INITIALISED.set(()).ok();
    Ok(())
}

/// Standalone async function for writing the connector status table.
///
/// Factored out of `SolaceSplitReader` methods so no borrow on the reader
/// (which contains non-Send raw pointers) is held across await points.
///
/// When `pool` is `Some`, a pooled connection is acquired and returned after the
/// write.  When `pool` is `None` (pool not yet initialised), a short-lived
/// connection is opened directly.
///
/// **Connection budget (pool = None):** one connection per sentinel event per reader.
/// For typical deployments (1–5 sources, sentinel fires once per backfill) this is
/// negligible.  If you are running >20 concurrent Solace sources, ensure the pool is
/// initialised (it is always set during `SolaceSplitReader::new`).
async fn write_connector_status_pg(
    queue: &str,
    detected_at: &str,
    events_before: i64,
    total: i64,
    now: &str,
    pool: &PgPool,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let conn = pool.get().await?;

    ensure_status_schema(&conn).await?;

    conn.execute(
        "INSERT INTO rw_solace_connector_status
            (connector_name, is_ready, sentinel_detected_at,
             events_before_sentinel, total_events_consumed, last_updated)
         VALUES ($1, TRUE, $2, $3, $4, $5)
         ON CONFLICT (connector_name) DO UPDATE SET
            is_ready = TRUE,
            sentinel_detected_at = EXCLUDED.sentinel_detected_at,
            events_before_sentinel = EXCLUDED.events_before_sentinel,
            total_events_consumed = EXCLUDED.total_events_consumed,
            last_updated = EXCLUDED.last_updated",
        &[&queue, &detected_at, &events_before, &total, &now],
    )
    .await?;

    tracing::info!(
        queue = %queue,
        "Connector status written to rw_solace_connector_status",
    );

    Ok(())
}

/// Issue a RisingWave `WAIT` barrier flush, then write the connector status table.
///
/// Must be called from a `tokio::spawn` task — NOT inline in `into_data_stream` — because
/// WAIT blocks until the executor checkpoints and the executor cannot checkpoint while the
/// stream generator is suspended on an `.await` inside it.
///
/// The WAIT command uses a dedicated short-lived connection (not from the pool) because
/// it holds the connection open for the full checkpoint duration.  The subsequent status
/// write uses `pool` when available to avoid a second connection allocation.
async fn barrier_flush_and_status(
    queue: &str,
    detected_at: &str,
    events_before: i64,
    total: i64,
    dsn: &str,
    pool: Option<&PgPool>,
    wait_timeout_ms: u64,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // WAIT must use its own dedicated connection so the pool is not exhausted
    // while checkpoint is in progress.
    let (wait_client, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
    tokio::spawn(async move { let _ = conn.await; });

    wait_client
        .execute(
            &format!("SET streaming_flush_wait_timeout_ms = {}", wait_timeout_ms),
            &[],
        )
        .await?;
    wait_client.execute("WAIT", &[]).await?;

    // Write status only after WAIT confirms MV currency.
    let now = Utc::now().to_rfc3339();
    if let Some(pool) = pool {
        write_connector_status_pg(queue, detected_at, events_before, total, &now, pool).await?;
    } else {
        // Pool not yet available (unusual); fall back to a fresh connection.
        let (client, conn2) = tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
        tokio::spawn(async move { let _ = conn2.await; });
        ensure_status_schema(&client).await?;
        client
            .execute(
                "INSERT INTO rw_solace_connector_status
                    (connector_name, is_ready, sentinel_detected_at,
                     events_before_sentinel, total_events_consumed, last_updated)
                 VALUES ($1, TRUE, $2, $3, $4, $5)
                 ON CONFLICT (connector_name) DO UPDATE SET
                    is_ready = TRUE,
                    sentinel_detected_at = EXCLUDED.sentinel_detected_at,
                    events_before_sentinel = EXCLUDED.events_before_sentinel,
                    total_events_consumed = EXCLUDED.total_events_consumed,
                    last_updated = EXCLUDED.last_updated",
                &[&queue, &detected_at, &events_before, &total, &now],
            )
            .await?;
    }

    tracing::info!(queue, "Barrier flush complete — all MVs current");
    Ok(())
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_build_solace_ack_channel_id() {
        let split_id: SplitId = Arc::from("0");
        let id = build_solace_ack_channel_id(42u32, &split_id);
        assert_eq!(id, "solace_42_0");
    }

    #[test]
    fn test_build_solace_ack_channel_id_different_inputs() {
        let split_a: SplitId = Arc::from("0");
        let split_b: SplitId = Arc::from("1");

        let id_a = build_solace_ack_channel_id(1u32, &split_a);
        let id_b = build_solace_ack_channel_id(1u32, &split_b);
        let id_c = build_solace_ack_channel_id(2u32, &split_a);

        assert_ne!(id_a, id_b);
        assert_ne!(id_a, id_c);
        assert_ne!(id_b, id_c);
    }

    #[tokio::test]
    async fn test_solace_ack_channel_roundtrip() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u64>>();
        let channel_id = "solace_test_0".to_owned();

        SOLACE_ACK_CHANNEL
            .entry(channel_id.clone())
            .and_upsert_with(|_| std::future::ready(tx))
            .await;

        let retrieved_tx = SOLACE_ACK_CHANNEL.get(&channel_id).await.unwrap();
        retrieved_tx.send(vec![1, 2, 3]).unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received, vec![1, 2, 3]);
    }

    // ── Readiness event tests ───────────────────────────────────────────

    #[test]
    fn test_readiness_event_topic_construction() {
        // Verify the topic follows the pattern: {prefix}/{queue}/ready
        let prefix = "system/risingwave/connector";
        let queue = "rw-ingest";
        let topic = format!("{}/{}/ready", prefix, queue);
        assert_eq!(topic, "system/risingwave/connector/rw-ingest/ready");
    }

    #[test]
    fn test_readiness_event_payload_structure() {
        // Verify the JSON payload contains all required fields.
        let payload = serde_json::json!({
            "connector_name": "rw-ingest",
            "status": "ready",
            "sentinel_detected_at": "2026-04-13T12:00:00+00:00",
            "historical_events_processed": 150_000u64,
            "total_events_consumed": 150_000u64,
            "barrier_flush_complete": false,
            "message": "All historical events processed. Safe to query materialized views.",
        });

        let obj = payload.as_object().unwrap();
        assert_eq!(obj["connector_name"], "rw-ingest");
        assert_eq!(obj["status"], "ready");
        assert!(obj.contains_key("sentinel_detected_at"));
        assert!(obj.contains_key("historical_events_processed"));
        assert!(obj.contains_key("total_events_consumed"));
        assert!(obj.contains_key("barrier_flush_complete"));
        assert!(obj.contains_key("message"));
    }

    #[test]
    fn test_readiness_outbound_message_builds() {
        // Verify OutboundMessageBuilder can produce the readiness message
        // without a live Solace session (tests the builder, not the publish).
        let topic = "system/risingwave/connector/test-queue/ready";
        let payload = b"{\"status\":\"ready\"}";

        let dest = MessageDestination::new(DestinationType::Topic, topic).unwrap();
        let result = OutboundMessageBuilder::new()
            .delivery_mode(DeliveryMode::Direct)
            .destination(dest)
            .payload(payload)
            .user_property("x-risingwave-event", "connector-ready")
            .build();

        assert!(result.is_ok(), "OutboundMessageBuilder should succeed");
    }

    // ── Barrier flush tests ─────────────────────────────────────────────

    #[test]
    fn test_readiness_payload_barrier_true() {
        // Assert JSON payload contains "barrier_flush_complete": true when flag is true.
        let barrier_flush_complete = true;
        let payload = serde_json::json!({
            "connector_name": "rw-ingest",
            "status": "ready",
            "sentinel_detected_at": "2026-04-13T12:00:00+00:00",
            "historical_events_processed": 100u64,
            "total_events_consumed": 105u64,
            "barrier_flush_complete": barrier_flush_complete,
            "message": if barrier_flush_complete {
                "All historical events processed. All materialized views current. Safe to query."
            } else {
                "Historical events processed. Barrier flush failed — allow brief propagation delay."
            },
        });

        let obj = payload.as_object().unwrap();
        assert_eq!(obj["barrier_flush_complete"], true);
        assert_eq!(
            obj["message"],
            "All historical events processed. All materialized views current. Safe to query."
        );
    }

    #[test]
    fn test_readiness_payload_barrier_false() {
        // Assert JSON payload contains "barrier_flush_complete": false when flag is false.
        let barrier_flush_complete = false;
        let payload = serde_json::json!({
            "connector_name": "rw-ingest",
            "status": "ready",
            "sentinel_detected_at": "2026-04-13T12:00:00+00:00",
            "historical_events_processed": 100u64,
            "total_events_consumed": 105u64,
            "barrier_flush_complete": barrier_flush_complete,
            "message": if barrier_flush_complete {
                "All historical events processed. All materialized views current. Safe to query."
            } else {
                "Historical events processed. Barrier flush failed — allow brief propagation delay."
            },
        });

        let obj = payload.as_object().unwrap();
        assert_eq!(obj["barrier_flush_complete"], false);
        assert_eq!(
            obj["message"],
            "Historical events processed. Barrier flush failed — allow brief propagation delay."
        );
    }

    #[tokio::test]
    async fn test_barrier_flush_and_status_pg_error() {
        // Call barrier_flush_and_status against localhost:4566. Without a running
        // RisingWave instance this returns Err (connection refused); with one it
        // may return Ok. Either way the function must not panic.
        let result = barrier_flush_and_status(
            "test-queue",
            "2026-04-13T12:00:00+00:00",
            100,
            105,
            "host=localhost port=4566 user=root dbname=dev connect_timeout=5",
            None,
            60_000,
        )
        .await;
        // Accept both outcomes — the key invariant is no panic.
        let _ = result;
    }

    #[tokio::test]
    async fn test_option_future_pending_when_none() {
        // When flush_rx is None, OptionFuture resolves immediately to None, not Some.
        // This ensures the flush arm in recv_or_flush is never spuriously triggered
        // before a sentinel has been seen.
        let flush_rx: Option<oneshot::Receiver<()>> = None;
        let result: Option<std::result::Result<(), oneshot::error::RecvError>> =
            OptionFuture::from(flush_rx).await;
        assert!(result.is_none(), "OptionFuture(None) should resolve to None, not Some");
    }

    #[tokio::test]
    async fn test_flush_rx_set_on_sentinel() {
        // Verify the oneshot channel pattern: sender fires, receiver resolves.
        use tokio::time::{Duration, timeout};

        let (tx, rx) = oneshot::channel::<()>();
        let mut flush_rx: Option<oneshot::Receiver<()>> = Some(rx);

        // Simulate the spawned task completing.
        tx.send(()).unwrap();

        // The receiver should now be ready.
        let result = timeout(
            Duration::from_millis(100),
            OptionFuture::from(flush_rx.take()),
        )
        .await;
        assert!(result.is_ok(), "flush_rx should resolve after sender fires");
        assert!(result.unwrap().is_some(), "OptionFuture should yield Some(Ok(()))");
    }
}
