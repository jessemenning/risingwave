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
    /// Last-seen Solace message ID (stored as a decimal string).
    ///
    /// This field is updated by the framework via `update_offset` after each message is
    /// processed. It is NOT used to seek the queue position on reconnect — queue-backed
    /// connectors rely on broker-side acking: the Solace broker tracks which messages have
    /// been acked per client flow and automatically re-delivers unacked messages after a
    /// reconnect, regardless of where the consumer left off.
    ///
    /// The field IS used by the checkpoint-ack path: the framework propagates `start_offset`
    /// into `WaitCheckpointTask::AckSolaceMessage` as the offset column, and at checkpoint
    /// the task parses it back to `u64` and sends the IDs through `SOLACE_ACK_CHANNEL` so
    /// the reader can call `flow.ack()` after the checkpoint barrier clears.
    pub(crate) start_offset: Option<String>,
    /// Whether the backfill sentinel was detected. When `true`, the connector
    /// is in live mode and will re-publish the readiness event on restart.
    #[serde(default)]
    pub(crate) sentinel_detected: bool,
    /// ISO 8601 timestamp of when the sentinel was first detected.
    #[serde(default)]
    pub(crate) sentinel_detected_at: Option<String>,
    /// ISO 8601 timestamp of when this reader last published the readiness
    /// event. `None` means the readiness event has not yet been published by
    /// this split's reader. Used to suppress redundant re-publishes on restart
    /// when multiple readers all boot into live mode simultaneously.
    #[serde(default)]
    pub(crate) readiness_published_at: Option<String>,
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
            sentinel_detected: false,
            sentinel_detected_at: None,
            readiness_published_at: None,
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

    #[test]
    fn test_split_sentinel_false_by_default() {
        let split = SolaceSplit::new("q".to_owned(), Arc::from("0"));
        assert!(!split.sentinel_detected);
        assert_eq!(split.sentinel_detected_at, None);
    }

    #[test]
    fn test_split_encode_decode_with_sentinel() {
        let split = SolaceSplit {
            queue_name: "rw-ingest".to_owned(),
            split_id: Arc::from("0"),
            start_offset: Some("42".to_owned()),
            sentinel_detected: true,
            sentinel_detected_at: Some("2026-04-13T12:00:00Z".to_owned()),
            readiness_published_at: None,
        };

        let encoded = split.encode_to_json();
        let decoded = SolaceSplit::restore_from_json(encoded).unwrap();
        assert_eq!(split, decoded);
        assert!(decoded.sentinel_detected);
        assert_eq!(
            decoded.sentinel_detected_at,
            Some("2026-04-13T12:00:00Z".to_owned())
        );
    }

    #[test]
    fn test_split_backward_compat_no_sentinel_fields() {
        // Simulate an OLD checkpoint that was serialized before sentinel fields existed.
        let old_json = serde_json::json!({
            "queue_name": "legacy-queue",
            "split_id": "0",
            "start_offset": "999"
        });
        let split: SolaceSplit = serde_json::from_value(old_json).unwrap();
        assert_eq!(split.queue_name, "legacy-queue");
        assert_eq!(split.start_offset, Some("999".to_owned()));
        // All optional fields default via #[serde(default)]
        assert!(!split.sentinel_detected);
        assert_eq!(split.sentinel_detected_at, None);
        assert_eq!(split.readiness_published_at, None);
    }

    #[test]
    fn test_split_encode_decode_with_readiness_published_at() {
        let split = SolaceSplit {
            queue_name: "rw-ingest".to_owned(),
            split_id: Arc::from("0"),
            start_offset: Some("42".to_owned()),
            sentinel_detected: true,
            sentinel_detected_at: Some("2026-04-25T10:00:00Z".to_owned()),
            readiness_published_at: Some("2026-04-25T10:00:05Z".to_owned()),
        };
        let encoded = split.encode_to_json();
        let decoded = SolaceSplit::restore_from_json(encoded).unwrap();
        assert_eq!(split, decoded);
        assert_eq!(
            decoded.readiness_published_at,
            Some("2026-04-25T10:00:05Z".to_owned())
        );
    }
}
