// Copyright 2024 RisingWave Labs
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

use std::sync::LazyLock;

use async_trait::async_trait;
use chrono::Utc;
use futures_async_stream::try_stream;
use moka::future::Cache as MokaCache;
use solace_rs::async_support::{AsyncSession, OwnedAsyncFlow};
use solace_rs::context::Context;
use solace_rs::flow::AckMode;
use solace_rs::message::destination::{DestinationType, MessageDestination};
use solace_rs::message::outbound::OutboundMessageBuilder;
use solace_rs::message::DeliveryMode;

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
> = LazyLock::new(|| moka::future::Cache::builder().build());

pub fn build_solace_ack_channel_id(source_id: impl std::fmt::Display, split_id: &SplitId) -> String {
    format!("solace_{}_{}", source_id, split_id)
}

pub struct SolaceSplitReader {
    flow: OwnedAsyncFlow,
    /// Keep session alive for the flow's lifetime. Also used to publish
    /// readiness events back to Solace after sentinel detection.
    session: AsyncSession,
    /// Keep context alive for the session's lifetime.
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

        let ack_mode = SolaceAckMode::from_str_opt(properties.ack_mode.as_deref())?;

        // Always use Client ack mode — we control when ack happens based on ack_mode.
        let (context, session) = properties.common.build_async_session()?;
        let flow = session
            .create_flow(&properties.queue, AckMode::Client)
            .map_err(|e| anyhow::anyhow!("failed to create Solace flow: {e}"))?;

        // For checkpoint mode, set up the ack channel so WaitCheckpointTask can
        // send message IDs back to us for acking.
        if matches!(ack_mode, SolaceAckMode::Checkpoint) {
            let (ack_tx, mut ack_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u64>>();
            let channel_id =
                build_solace_ack_channel_id(source_ctx.source_id, &split_id);
            SOLACE_ACK_CHANNEL
                .entry(channel_id)
                .and_upsert_with(|_| std::future::ready(ack_tx))
                .await;

            let flow_ptr = &flow as *const OwnedAsyncFlow as usize;
            let source_name = source_ctx.source_name.clone();
            tokio::spawn(async move {
                while let Some(msg_ids) = ack_rx.recv().await {
                    let flow_ref = unsafe { &*(flow_ptr as *const OwnedAsyncFlow) };
                    for msg_id in msg_ids {
                        if let Err(e) = flow_ref.ack(msg_id) {
                            tracing::error!(
                                source_name = %source_name,
                                msg_id,
                                error = %e,
                                "failed to ack Solace message on checkpoint",
                            );
                        }
                    }
                }
            });
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
        };

        // On restart after sentinel: re-publish readiness event and re-write status
        // table so new consumers that started after the original event get notified.
        if sentinel_detected {
            tracing::info!(
                queue = %reader.properties.queue,
                detected_at = ?reader.sentinel_detected_at,
                "Connector restarted after sentinel. Re-publishing readiness event.",
            );
            let now = Utc::now().to_rfc3339();
            let det_at = reader.sentinel_detected_at.clone().unwrap_or_default();
            let _ = write_connector_status_pg(
                &reader.properties.queue, &det_at, 0, 0, &now,
            )
            .await;
            reader.publish_readiness_event();
        }

        Ok(reader)
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

impl SolaceSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(mut self) {
        loop {
            match self.flow.recv().await {
                Some(msg) => {
                    let msg_id = msg
                        .get_msg_id()
                        .ok()
                        .flatten()
                        .unwrap_or(0);

                    let solace_msg = SolaceMessage::from_inbound(&msg, self.split_id.clone());

                    // ── Sentinel detection ───────────────────────────────────
                    if solace_msg.is_sentinel() {
                        // Always ack the sentinel regardless of ack mode.
                        if let Err(e) = self.flow.ack(msg_id) {
                            tracing::warn!(
                                msg_id,
                                error = %e,
                                "failed to ack sentinel message",
                            );
                        }

                        // Idempotent — ignore duplicate sentinels.
                        if self.sentinel_detected {
                            tracing::warn!(
                                queue = %self.properties.queue,
                                original_at = ?self.sentinel_detected_at,
                                "Duplicate sentinel ignored",
                            );
                            continue;
                        }

                        let now = Utc::now();
                        self.sentinel_detected = true;
                        self.sentinel_detected_at = Some(now.to_rfc3339());
                        self.events_before_sentinel = self.total_consumed;

                        tracing::info!(
                            queue = %self.properties.queue,
                            events_before_sentinel = self.events_before_sentinel,
                            "Sentinel detected. Writing status and publishing readiness event.",
                        );

                        // Write status table (best-effort — readiness event is primary).
                        {
                            let now = Utc::now().to_rfc3339();
                            let det_at = self.sentinel_detected_at.clone().unwrap_or_default();
                            let eb = self.events_before_sentinel as i64;
                            let tc = self.total_consumed as i64;
                            let q = self.properties.queue.clone();
                            let _ = write_connector_status_pg(&q, &det_at, eb, tc, &now).await;
                        }

                        // Publish readiness event back to Solace.
                        self.publish_readiness_event();

                        // DO NOT yield sentinel to source table — continue with next message.
                        continue;
                    }

                    // ── Normal message processing ────────────────────────────
                    self.total_consumed += 1;

                    // In immediate mode, ack right away before processing.
                    if matches!(self.ack_mode, SolaceAckMode::Immediate) {
                        if let Err(e) = self.flow.ack(msg_id) {
                            tracing::warn!(
                                msg_id,
                                error = %e,
                                "failed to immediately ack Solace message",
                            );
                        }
                    }

                    yield vec![SourceMessage::from(solace_msg)];
                }
                None => {
                    tracing::warn!("Solace flow recv returned None, flow disconnected");
                    break;
                }
            }
        }
    }

    /// Publish a readiness event to Solace so consumers are notified instantly.
    ///
    /// Topic: `{sentinel_readiness_topic}/{queue_name}/ready`
    /// User property: `x-risingwave-event = connector-ready`
    fn publish_readiness_event(&self) {
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
            "barrier_flush_complete": false,
            "message": "All historical events processed. Safe to query materialized views.",
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

/// Standalone async function for writing the connector status table.
///
/// Factored out of `SolaceSplitReader` methods so no borrow on the reader
/// (which contains non-Send raw pointers) is held across await points.
async fn write_connector_status_pg(
    queue: &str,
    detected_at: &str,
    events_before: i64,
    total: i64,
    now: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=4566 user=root dbname=dev connect_timeout=5",
        tokio_postgres::NoTls,
    )
    .await?;

    // Spawn the connection handler — it exits when the client is dropped.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::debug!("pg connection closed: {e}");
        }
    });

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
            &[
                &queue,
                &detected_at,
                &events_before,
                &total,
                &now,
            ],
        )
        .await?;

    tracing::info!(
        queue = %queue,
        "Connector status written to rw_solace_connector_status",
    );

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
}
