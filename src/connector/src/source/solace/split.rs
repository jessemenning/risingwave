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

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::{SplitId, SplitMetaData};

/// The states of a Solace split, which will be persisted to checkpoint.
///
/// Solace queues are not partitioned, so we use a single split per queue.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct SolaceSplit {
    pub(crate) queue_name: String,
    pub(crate) split_id: SplitId,
    /// Last seen message ID, used for offset tracking.
    pub(crate) start_offset: Option<String>,
}

impl SplitMetaData for SolaceSplit {
    fn id(&self) -> SplitId {
        self.split_id.clone()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        self.start_offset = Some(last_seen_offset);
        Ok(())
    }
}

impl SolaceSplit {
    pub fn new(queue_name: String, split_id: SplitId) -> Self {
        Self {
            queue_name,
            split_id,
            start_offset: None,
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_solace_split_id() {
        let split = SolaceSplit::new("test-queue".to_owned(), Arc::from("solace-0"));
        assert_eq!(split.id().as_ref(), "solace-0");
    }

    #[test]
    fn test_solace_split_encode_decode_json() {
        let split = SolaceSplit::new("my-queue".to_owned(), Arc::from("0"));
        let encoded = split.encode_to_json();
        let decoded = SolaceSplit::restore_from_json(encoded).unwrap();
        assert_eq!(split, decoded);
    }

    #[test]
    fn test_solace_split_encode_decode_with_offset() {
        let mut split = SolaceSplit::new("orders-queue".to_owned(), Arc::from("0"));
        split.start_offset = Some("12345".to_owned());

        let encoded = split.encode_to_json();
        let decoded = SolaceSplit::restore_from_json(encoded).unwrap();
        assert_eq!(split, decoded);
        assert_eq!(decoded.start_offset, Some("12345".to_owned()));
    }

    #[test]
    fn test_solace_split_update_offset() {
        let mut split = SolaceSplit::new("q".to_owned(), Arc::from("0"));
        assert_eq!(split.start_offset, None);

        split.update_offset("99999".to_owned()).unwrap();
        assert_eq!(split.start_offset, Some("99999".to_owned()));

        split.update_offset("100000".to_owned()).unwrap();
        assert_eq!(split.start_offset, Some("100000".to_owned()));
    }
}
