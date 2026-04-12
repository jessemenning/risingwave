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
use futures_async_stream::try_stream;
use moka::future::Cache as MokaCache;
use solace_rs::async_support::{AsyncSession, OwnedAsyncFlow};
use solace_rs::context::Context;
use solace_rs::flow::AckMode;

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
    /// Keep session alive for the flow's lifetime.
    #[expect(dead_code)]
    session: AsyncSession,
    /// Keep context alive for the session's lifetime.
    #[expect(dead_code)]
    context: Context,
    #[expect(dead_code)]
    properties: SolaceProperties,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    split_id: SplitId,
    ack_mode: SolaceAckMode,
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

            // Spawn a background task that receives batches of message IDs from the
            // checkpoint task and acks them on the flow.
            //
            // This is safe because `OwnedAsyncFlow::ack(&self)` takes a shared reference
            // and the C API's sendAck is thread-safe.
            //
            // We leak a raw flow pointer here for the ack task. The pointer is valid as
            // long as the flow is alive. The flow is dropped when this reader is dropped,
            // at which point the channel closes and the task exits.
            let flow_ptr = &flow as *const OwnedAsyncFlow as usize;
            let source_name = source_ctx.source_name.clone();
            tokio::spawn(async move {
                while let Some(msg_ids) = ack_rx.recv().await {
                    // Safety: the flow pointer is valid as long as the reader is alive.
                    // The channel closes when the reader drops, exiting this loop.
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

        Ok(Self {
            flow,
            session,
            context,
            properties,
            parser_config,
            source_ctx,
            split_id,
            ack_mode,
        })
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

                    let solace_msg = SolaceMessage::from_inbound(&msg, self.split_id.clone());
                    yield vec![SourceMessage::from(solace_msg)];
                }
                None => {
                    // Flow disconnected — the session was dropped or the broker closed
                    // the flow. Break out of the loop; the executor will handle reconnection.
                    tracing::warn!("Solace flow recv returned None, flow disconnected");
                    break;
                }
            }
        }
    }
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
        // Verify the global ack channel can register and retrieve senders.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u64>>();
        let channel_id = "solace_test_0".to_owned();

        SOLACE_ACK_CHANNEL
            .entry(channel_id.clone())
            .and_upsert_with(|_| std::future::ready(tx))
            .await;

        // Retrieve and send through the channel
        let retrieved_tx = SOLACE_ACK_CHANNEL.get(&channel_id).await.unwrap();
        retrieved_tx.send(vec![1, 2, 3]).unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received, vec![1, 2, 3]);
    }
}
