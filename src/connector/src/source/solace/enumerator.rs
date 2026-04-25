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

use std::sync::Arc;

use async_trait::async_trait;

use super::SolaceProperties;
use super::split::SolaceSplit;
use crate::error::ConnectorResult;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

/// Solace queues are not partitioned; each split maps to one consumer flow.
///
/// When `solace.num_consumers = N` (N > 1), the enumerator returns N splits
/// (IDs "0" through "N-1") so RisingWave assigns one `SolaceSplitReader` per
/// split. The Solace broker load-balances messages across all bound flows.
///
/// Exactly one reader will receive the sentinel message; it triggers backfill
/// detection and writes `is_ready = true` to the `rw_solace_connector_status`
/// table. On restart, all readers query this table at startup and boot directly
/// into live mode if `is_ready = true`, without waiting for a sentinel they
/// will never receive.
#[derive(Debug)]
pub struct SolaceSplitEnumerator {
    queue_name: String,
    num_consumers: usize,
}

#[async_trait]
impl SplitEnumerator for SolaceSplitEnumerator {
    type Properties = SolaceProperties;
    type Split = SolaceSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        let num_consumers = match properties.num_consumers {
            None | Some(1..) => properties.num_consumers.unwrap_or(1),
            Some(0) => return Err(anyhow::anyhow!("solace.num_consumers must be >= 1").into()),
        };
        Ok(Self {
            queue_name: properties.queue.clone(),
            num_consumers,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<SolaceSplit>> {
        Ok((0..self.num_consumers)
            .map(|i| SolaceSplit::new(self.queue_name.clone(), Arc::from(i.to_string().as_str())))
            .collect())
    }
}
