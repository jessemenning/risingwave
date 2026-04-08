// Copyright 2026 RisingWave Labs
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

use std::collections::VecDeque;

use anyhow::anyhow;
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::store::{
    CHECK_BYTES_EQUAL, InitOptions, LocalStateStore, NewLocalOptions, OpConsistencyLevel,
    StateStoreWriteEpochControl,
};

use crate::common::log_store_impl::kv_log_store::LogStoreVnodeProgress;
use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store_impl::kv_log_store::state::{
    LogStoreReadState, LogStoreWriteState, new_log_store_state,
};
use crate::executor::prelude::*;
use crate::task::FragmentId;

pub struct ChangeBufferExecutor<S: StateStore> {
    // Todo(ying): add metrics
    actor_context: ActorContextRef,
    input: Executor,
    state_store: S,
    table_id: TableId,
    buffer_max_size: usize,
}

struct ChangeBuffer {
    buffer: VecDeque<StreamChunk>,
    current_size: usize,
    max_size: usize,
}

impl ChangeBuffer {
    fn add_or_flush_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> Option<StreamChunk> {
        let current_size = self.current_size;
        let chunk_size = chunk.cardinality();

        let should_flush_chunk = current_size + chunk_size > self.max_size;
        
        if should_flush_chunk {
            Some(chunk)
        } else {
            self.buffer.push_back(chunk);
            None
        }
    }
}

impl<S: StateStore> ChangeBufferExecutor<S> {
    #[allow(dead_code)]
    pub(crate) fn new(
        actor_context: ActorContextRef,
        input: Executor,
        state_store: S,
        table_id: TableId,
        buffer_max_size: usize,
    ) -> Self {
        Self {
            actor_context,
            input,
            state_store,
            table_id,
            buffer_max_size,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let Self {
            actor_context,
            input,
            state_store,
            table_id,
            buffer_max_size,
        } = self;

        let mut input = input.execute();

        let first_barrier = expect_first_barrier(&mut input).await?;
        let first_write_epoch = first_barrier.epoch;
        yield Message::Barrier(first_barrier.clone());

        let local_state_store = self
        .state_store
        .new_local(NewLocalOptions { 
                table_id: self.table_id,
                fragment_id: self.actor_context.fragment_id,
                op_consistency_level: OpConsistencyLevel::Inconsistent,
                table_option: TableOption {
                    retention_seconds: None,
                },
                is_replicated: false,
                vnodes: self.serde.vnodes().clone(),
                upload_on_flush: false,
            })
        .await;

        let buffer = ChangeBuffer {
            buffer: VecDeque::new(),
            current_size: 0,
            max_size: buffer_max_size,
        };
        let curr_epoch = first_write_epoch.curr;

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    if chunk.cardinality() == 0 {
                        tracing::warn!(
                            epoch = curr_epoch,
                            "received empty chunk (cardinality=0), skipping"
                        );
                    } else {
                        if let Some(chunk) = buffer.add_or_flush_chunk(chunk) {
                            local_state_store.insert()
                        }
                    }
                }
                Message::Barrier(barrier) => {
                    if has_spilled {
                        for chunk in
                            Self::emit_persisted_epoch_changes(&mut read_state, barrier.epoch.prev)
                                .await?
                        {
                            yield Message::Chunk(chunk);
                        }
                    }

                    for chunk in buffer.drain() {
                        yield Message::Chunk(chunk);
                    }

                    let new_vnodes = barrier.as_update_vnode_bitmap(actor_context.id);
                    let post_seal = write_state
                        .seal_current_epoch(barrier.epoch.curr, LogStoreVnodeProgress::None);
                    current_epoch = barrier.epoch.curr;
                    has_spilled = false;
                    yield Message::Barrier(barrier);
                    if let Some(vnodes) = new_vnodes {
                        read_state.update_vnode_bitmap(vnodes.clone());
                        let _ = post_seal.post_yield_barrier(Some(vnodes)).await?;
                    } else {
                        let _ = post_seal.post_yield_barrier(None).await?;
                    }
                }
                Message::Watermark(watermark) => {
                    // TODO: decide whether watermarks should also be buffered to preserve ordering
                    // with delayed chunks.
                    yield Message::Watermark(watermark);
                }
            }
        }
    }
}

impl<S: StateStore> Execute for ChangeBufferExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunkTestExt;

    use super::EpochChunkBuffer;

    #[test]
    fn test_epoch_chunk_buffer_tracks_rows() {
        let mut buffer = EpochChunkBuffer::default();
        buffer.push_chunk(StreamChunk::from_pretty(" I\n + 1"));
        buffer.push_chunk(StreamChunk::from_pretty(" I\n + 2\n + 3"));

        assert!(buffer.exceeds(3));
        assert!(!buffer.is_empty());

        let chunks = buffer.drain().collect::<Vec<_>>();
        assert_eq!(chunks.len(), 2);
        assert!(!buffer.exceeds(1));
        assert!(buffer.is_empty());
    }
}
