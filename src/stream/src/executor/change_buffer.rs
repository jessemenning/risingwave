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

#[derive(Default)]
struct EpochChunkBuffer {
    chunks: VecDeque<StreamChunk>,
    row_count: usize,
}

impl EpochChunkBuffer {
    fn push_chunk(&mut self, chunk: StreamChunk) {
        self.row_count += chunk.cardinality();
        self.chunks.push_back(chunk);
    }

    fn exceeds(&self, max_rows: usize) -> bool {
        self.row_count >= max_rows
    }

    fn drain(&mut self) -> impl Iterator<Item = StreamChunk> + '_ {
        self.row_count = 0;
        self.chunks.drain(..)
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

/// Buffers incoming chunks within an epoch and spills to local hummock when the buffer exceeds the
/// threshold.
///
/// This first version intentionally focuses on the outer control flow:
/// - buffer incoming chunks in a `VecDeque`
/// - spill the buffered chunks to local hummock when the threshold is exceeded
/// - on barrier, drain persisted data first, then drain the in-memory buffer
///
/// The actual row encoding into local hummock and the uncommitted changelog reader are left as
/// follow-up work. Once spill happens, the executor will return a clear error instead of silently
/// dropping data.
pub struct ChangeBufferExecutor<S: StateStore> {
    actor_context: ActorContextRef,
    input: Executor,
    state_store: S,
    table_id: TableId,
    serde: LogStoreRowSerde,
    vnodes: Arc<Bitmap>,
    buffer_max_rows: usize,
    chunk_size: usize,
}

impl<S: StateStore> ChangeBufferExecutor<S> {
    #[allow(dead_code)]
    pub(crate) fn new(
        actor_context: ActorContextRef,
        input: Executor,
        state_store: S,
        table_id: TableId,
        serde: LogStoreRowSerde,
        vnodes: Arc<Bitmap>,
        buffer_max_rows: usize,
        chunk_size: usize,
    ) -> Self {
        Self {
            actor_context,
            input,
            state_store,
            table_id,
            serde,
            vnodes,
            buffer_max_rows,
            chunk_size,
        }
    }

    async fn init_local_store(
        state_store: &S,
        table_id: TableId,
        fragment_id: FragmentId,
        serde: LogStoreRowSerde,
        vnodes: Arc<Bitmap>,
        epoch: EpochPair,
        chunk_size: usize,
    ) -> StreamExecutorResult<(
        LogStoreReadState<<S::Local as LocalStateStore>::FlushedSnapshotReader>,
        LogStoreWriteState<S::Local>,
    )> {
        let mut local_store = state_store
            .new_local(NewLocalOptions {
                table_id,
                fragment_id,
                op_consistency_level: OpConsistencyLevel::ConsistentOldValue {
                    check_old_value: CHECK_BYTES_EQUAL.clone(),
                    is_log_store: true,
                },
                table_option: TableOption {
                    retention_seconds: None,
                },
                is_replicated: false,
                vnodes,
                upload_on_flush: false,
            })
            .await;
        local_store.init(InitOptions::new(epoch)).await?;
        Ok(new_log_store_state(
            table_id,
            local_store,
            serde,
            chunk_size,
        ))
    }

    async fn spill_buffer_to_local_hummock(
        write_state: &mut LogStoreWriteState<S::Local>,
        buffer: &mut EpochChunkBuffer,
        epoch: u64,
    ) -> StreamExecutorResult<()> {
        let _ = (write_state, buffer, epoch);
        Err(anyhow!("ChangeBufferExecutor spill-to-hummock is not implemented yet").into())
    }

    async fn emit_persisted_epoch_changes(
        read_state: &mut LogStoreReadState<<S::Local as LocalStateStore>::FlushedSnapshotReader>,
        epoch: u64,
    ) -> StreamExecutorResult<Vec<StreamChunk>> {
        let _ = (read_state, epoch);
        Err(
            anyhow!("ChangeBufferExecutor uncommitted changelog iterator is not implemented yet")
                .into(),
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let Self {
            actor_context,
            input,
            state_store,
            table_id,
            serde,
            vnodes,
            buffer_max_rows,
            chunk_size,
        } = self;

        let mut input = input.execute();

        let first_barrier = expect_first_barrier(&mut input).await?;
        let (mut read_state, mut write_state) = Self::init_local_store(
            &state_store,
            table_id,
            actor_context.fragment_id,
            serde,
            vnodes.clone(),
            first_barrier.epoch,
            chunk_size,
        )
        .await?;
        let mut current_epoch = first_barrier.epoch.curr;
        let mut buffer = EpochChunkBuffer::default();
        let mut has_spilled = false;

        yield Message::Barrier(first_barrier);

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    buffer.push_chunk(chunk);
                    if buffer.exceeds(buffer_max_rows) {
                        Self::spill_buffer_to_local_hummock(
                            &mut write_state,
                            &mut buffer,
                            current_epoch,
                        )
                        .await?;
                        has_spilled = true;
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
