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

//! Lightweight epoch tracking for batch refresh jobs.
//!
//! The actual barrier injection, collection, completing and ack are handled by
//! [`PartialGraphManager`]. This module only tracks the maximum committed epoch
//! so the owning [`super::BatchRefreshJobCheckpointControl`] can query progress
//! without going through the partial graph manager.

use risingwave_common::id::JobId;

use crate::barrier::partial_graph::CollectedBarrier;

/// Tracks epoch progress for a single batch-refresh job.
///
/// Barrier lifecycle is managed by [`PartialGraphManager`]; this struct only
/// records the committed-epoch watermark used by the batch refresh control to
/// decide when to idle and when to refresh.
#[derive(Debug)]
pub(super) struct BatchRefreshBarrierControl {
    #[allow(dead_code)]
    job_id: JobId,
    max_committed_epoch: Option<u64>,
}

impl BatchRefreshBarrierControl {
    pub(super) fn new(
        job_id: JobId,
        _snapshot_epoch: u64,
        max_committed_epoch: Option<u64>,
    ) -> Self {
        Self {
            job_id,
            max_committed_epoch,
        }
    }

    /// Called after a barrier has been injected into the partial graph.
    /// Epoch bookkeeping is handled by [`PartialGraphManager`]; this is a
    /// no-op kept for call-site symmetry.
    pub(super) fn enqueue_epoch(&mut self, _prev_epoch: u64) {}

    /// Called when a barrier has been collected from all workers for this
    /// partial graph.  Epoch bookkeeping is handled by [`PartialGraphManager`];
    /// this is a no-op kept for call-site symmetry.
    pub(super) fn collect(&mut self, _collected_barrier: CollectedBarrier<'_>) {}

    /// Returns the highest epoch that has been fully committed, or `None` if
    /// no epoch has been committed yet.
    pub(super) fn max_committed_epoch(&self) -> Option<u64> {
        self.max_committed_epoch
    }

    /// Record that `epoch` has been committed.  Panics if the epoch does not
    /// advance monotonically.
    pub(super) fn update_committed_epoch(&mut self, epoch: u64) {
        if let Some(prev) = self.max_committed_epoch {
            assert!(epoch > prev, "epoch must advance monotonically");
        }
        self.max_committed_epoch = Some(epoch);
    }
}
