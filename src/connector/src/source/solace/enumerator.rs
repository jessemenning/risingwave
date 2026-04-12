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

use std::sync::Arc;

use async_trait::async_trait;

use super::SolaceProperties;
use super::split::SolaceSplit;
use crate::error::ConnectorResult;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

/// Solace queues are not partitioned, so we return a single split per source.
#[derive(Debug)]
pub struct SolaceSplitEnumerator {
    queue_name: String,
}

#[async_trait]
impl SplitEnumerator for SolaceSplitEnumerator {
    type Properties = SolaceProperties;
    type Split = SolaceSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        Ok(Self {
            queue_name: properties.queue.clone(),
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<SolaceSplit>> {
        // Single split per queue — Solace queues are not partitioned.
        Ok(vec![SolaceSplit::new(
            self.queue_name.clone(),
            Arc::from("0"),
        )])
    }
}
