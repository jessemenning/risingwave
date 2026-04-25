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

use std::collections::HashMap;

use risingwave_common::types::{ScalarRefImpl, Timestamptz};
use solace_rs::message::{InboundMessage, Message};

use crate::source::{SourceMessage, SourceMeta, SplitId};

#[derive(Debug, Clone)]
pub struct SolaceMeta {
    /// The destination (topic or queue) the message arrived on.
    pub destination: Option<String>,
    /// Broker-assigned sender timestamp in milliseconds since Unix epoch.
    /// `None` if the publisher did not set a timestamp.
    pub sender_timestamp: Option<Timestamptz>,
    /// Broker-assigned replication group message ID (hex string).
    /// Enables exactly-once dedup across replicated brokers.
    pub replication_group_message_id: Option<String>,
    /// Application-set correlation ID for request/reply correlation.
    pub correlation_id: Option<String>,
    /// Publisher-set monotonic sequence number.
    pub sequence_number: Option<i64>,
    /// Message delivery priority (0–255, higher = higher priority).
    pub priority: Option<i32>,
    /// True if the broker redelivered this message after a failed ack.
    pub redelivered: bool,
    /// Application-set message ID for application-level dedup.
    pub application_message_id: Option<String>,
    /// Message expiration time. `None` if the message does not expire.
    pub expiration: Option<Timestamptz>,
    /// Reply-to destination for request/reply patterns.
    pub reply_to: Option<String>,
}

impl SolaceMeta {
    pub fn extract_destination(&self) -> Option<ScalarRefImpl<'_>> {
        self.destination.as_deref().map(ScalarRefImpl::Utf8)
    }

    pub fn extract_sender_timestamp(&self) -> Option<ScalarRefImpl<'_>> {
        self.sender_timestamp
            .map(|ts| ScalarRefImpl::Timestamptz(ts))
    }

    pub fn extract_replication_group_message_id(&self) -> Option<ScalarRefImpl<'_>> {
        self.replication_group_message_id
            .as_deref()
            .map(ScalarRefImpl::Utf8)
    }

    pub fn extract_correlation_id(&self) -> Option<ScalarRefImpl<'_>> {
        self.correlation_id.as_deref().map(ScalarRefImpl::Utf8)
    }

    pub fn extract_sequence_number(&self) -> Option<ScalarRefImpl<'_>> {
        self.sequence_number
            .map(|n| ScalarRefImpl::Int64(n))
    }

    pub fn extract_priority(&self) -> Option<ScalarRefImpl<'_>> {
        self.priority.map(|p| ScalarRefImpl::Int32(p))
    }

    pub fn extract_redelivered(&self) -> ScalarRefImpl<'_> {
        ScalarRefImpl::Bool(self.redelivered)
    }

    pub fn extract_application_message_id(&self) -> Option<ScalarRefImpl<'_>> {
        self.application_message_id
            .as_deref()
            .map(ScalarRefImpl::Utf8)
    }

    pub fn extract_expiration(&self) -> Option<ScalarRefImpl<'_>> {
        self.expiration.map(|ts| ScalarRefImpl::Timestamptz(ts))
    }

    pub fn extract_reply_to(&self) -> Option<ScalarRefImpl<'_>> {
        self.reply_to.as_deref().map(ScalarRefImpl::Utf8)
    }
}

#[derive(Clone, Debug)]
pub struct SolaceMessage {
    pub split_id: SplitId,
    /// Message ID used for acknowledgment. Stored as string in offset field.
    pub msg_id: String,
    pub payload: Vec<u8>,
    pub destination: Option<String>,
    pub sender_timestamp: Option<Timestamptz>,
    pub replication_group_message_id: Option<String>,
    pub correlation_id: Option<String>,
    pub sequence_number: Option<i64>,
    pub priority: Option<i32>,
    pub redelivered: bool,
    pub application_message_id: Option<String>,
    pub expiration: Option<Timestamptz>,
    pub reply_to: Option<String>,
    /// User properties extracted from the Solace message.
    /// Used for sentinel detection (key: `x-solace-sentinel`).
    pub user_properties: HashMap<String, String>,
}

impl SolaceMessage {
    /// Returns `true` if this message carries the backfill sentinel user property
    /// (`x-solace-sentinel = backfill-complete`). Sentinel messages should be
    /// intercepted by the reader and never yielded to the source table.
    pub fn is_sentinel(&self) -> bool {
        self.user_properties
            .get("x-solace-sentinel")
            .map(|v| v == "backfill-complete")
            .unwrap_or(false)
    }
}

impl SolaceMessage {
    pub fn from_inbound(msg: &InboundMessage, split_id: SplitId) -> Self {
        let msg_id = msg
            .get_msg_id()
            .ok()
            .flatten()
            .map(|id| id.to_string())
            .unwrap_or_default();

        // Try the string variant first so the Solace C SDK strips the SDT
        // container header that the Python Messaging API adds to string
        // payloads.  Fall back to the raw binary getter for non-string
        // attachments (e.g. Bytes payloads published from other clients).
        let payload = msg
            .get_payload_as_string()
            .ok()
            .flatten()
            .map(|s| s.as_bytes().to_vec())
            .or_else(|| {
                msg.get_payload()
                    .ok()
                    .flatten()
                    .map(|p| p.to_vec())
            })
            .unwrap_or_else(|| {
                tracing::warn!(
                    "Failed to extract payload from Solace message — \
                     both string and binary extractors returned None; treating as empty payload"
                );
                vec![]
            });

        let destination = msg
            .get_destination()
            .ok()
            .flatten()
            .map(|d| d.dest.to_string_lossy().into_owned());

        // Use sender timestamp if present; fall back to the broker receive
        // timestamp; finally use SystemTime::now() so the column is never NULL.
        // (The Python Messaging API does not expose a sender-timestamp setter,
        // and solClient_msg_getRcvTimestamp returns NotFound for guaranteed
        // queue messages on some broker versions.)
        let sender_timestamp = msg
            .get_sender_timestamp()
            .ok()
            .flatten()
            .or_else(|| msg.get_rcv_timestamp().ok().flatten())
            .unwrap_or_else(std::time::SystemTime::now)
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_millis() as i64)
            .and_then(Timestamptz::from_millis);

        let replication_group_message_id =
            msg.get_replication_group_message_id().ok().flatten();

        let correlation_id = msg
            .get_correlation_id()
            .ok()
            .flatten()
            .map(|s| s.to_owned());

        let sequence_number = msg.get_sequence_number().ok().flatten();

        let priority = msg
            .get_priority()
            .ok()
            .flatten()
            .map(|p| p as i32);

        let redelivered = msg.is_redelivered();

        let application_message_id = msg
            .get_application_message_id()
            .map(|s| s.to_owned());

        // get_expiration() returns 0 if not set; treat 0 as None.
        let expiration = {
            let exp_ms = msg.get_expiration();
            if exp_ms == 0 {
                None
            } else {
                Timestamptz::from_millis(exp_ms)
            }
        };

        let reply_to = msg
            .get_reply_to()
            .ok()
            .flatten()
            .map(|d| d.dest.to_string_lossy().into_owned());

        let user_properties = msg
            .get_user_properties()
            .unwrap_or_default();

        Self {
            split_id,
            msg_id,
            payload,
            destination,
            sender_timestamp,
            replication_group_message_id,
            correlation_id,
            sequence_number,
            priority,
            redelivered,
            application_message_id,
            expiration,
            reply_to,
            user_properties,
        }
    }
}

impl From<SolaceMessage> for SourceMessage {
    fn from(message: SolaceMessage) -> Self {
        SourceMessage {
            key: None,
            payload: Some(message.payload),
            // offset stores the message ID for checkpoint-based ack
            offset: message.msg_id,
            split_id: message.split_id,
            meta: SourceMeta::Solace(SolaceMeta {
                destination: message.destination,
                sender_timestamp: message.sender_timestamp,
                replication_group_message_id: message.replication_group_message_id,
                correlation_id: message.correlation_id,
                sequence_number: message.sequence_number,
                priority: message.priority,
                redelivered: message.redelivered,
                application_message_id: message.application_message_id,
                expiration: message.expiration,
                reply_to: message.reply_to,
            }),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use risingwave_common::types::{ScalarRefImpl, Timestamptz};

    use super::*;

    fn make_msg(destination: Option<&str>) -> SolaceMessage {
        SolaceMessage {
            split_id: Arc::from("0"),
            msg_id: "42".to_owned(),
            payload: b"hello world".to_vec(),
            destination: destination.map(|s| s.to_owned()),
            sender_timestamp: None,
            replication_group_message_id: None,
            correlation_id: None,
            sequence_number: None,
            priority: None,
            redelivered: false,
            application_message_id: None,
            expiration: None,
            reply_to: None,
            user_properties: HashMap::new(),
        }
    }

    #[test]
    fn test_solace_message_to_source_message() {
        let msg = make_msg(Some("topic/orders/new"));
        let source_msg = SourceMessage::from(msg);

        assert_eq!(source_msg.key, None);
        assert_eq!(source_msg.payload, Some(b"hello world".to_vec()));
        assert_eq!(source_msg.offset, "42");
        assert_eq!(source_msg.split_id.as_ref(), "0");
        match &source_msg.meta {
            SourceMeta::Solace(meta) => {
                assert_eq!(meta.destination, Some("topic/orders/new".to_owned()));
                assert!(meta.sender_timestamp.is_none());
                assert!(!meta.redelivered);
            }
            other => panic!("expected SourceMeta::Solace, got {:?}", other),
        }
    }

    #[test]
    fn test_solace_message_to_source_message_no_destination() {
        let mut msg = make_msg(None);
        msg.payload = vec![];
        msg.msg_id = String::new();
        let source_msg = SourceMessage::from(msg);

        assert_eq!(source_msg.payload, Some(vec![]));
        assert_eq!(source_msg.offset, "");
        match &source_msg.meta {
            SourceMeta::Solace(meta) => {
                assert_eq!(meta.destination, None);
            }
            other => panic!("expected SourceMeta::Solace, got {:?}", other),
        }
    }

    #[test]
    fn test_solace_message_preserves_offset_for_ack() {
        let msg = SolaceMessage {
            split_id: Arc::from("split-0"),
            msg_id: "9876543210".to_owned(),
            payload: b"{}".to_vec(),
            destination: None,
            sender_timestamp: None,
            replication_group_message_id: None,
            correlation_id: None,
            sequence_number: None,
            priority: None,
            redelivered: false,
            application_message_id: None,
            expiration: None,
            reply_to: None,
            user_properties: HashMap::new(),
        };

        let source_msg = SourceMessage::from(msg);

        // The offset field is critical — it carries the message ID through the
        // checkpoint pipeline so that WaitCheckpointTask can parse it back to u64
        // and call flow.ack(msg_id).
        assert_eq!(source_msg.offset, "9876543210");
        // Verify it parses back to u64 cleanly
        let parsed: u64 = source_msg.offset.parse().unwrap();
        assert_eq!(parsed, 9876543210);
    }

    #[test]
    fn test_solace_meta_with_all_fields() {
        let ts = Timestamptz::from_millis(1_700_000_000_000).unwrap();
        let msg = SolaceMessage {
            split_id: Arc::from("0"),
            msg_id: "1".to_owned(),
            payload: vec![],
            destination: Some("topic/test".to_owned()),
            sender_timestamp: Some(ts),
            replication_group_message_id: Some("RGMID-ABC123".to_owned()),
            correlation_id: Some("corr-1".to_owned()),
            sequence_number: Some(99),
            priority: Some(5),
            redelivered: true,
            application_message_id: Some("app-msg-1".to_owned()),
            expiration: Some(ts),
            reply_to: Some("reply/topic".to_owned()),
            user_properties: HashMap::new(),
        };

        let source_msg = SourceMessage::from(msg);
        match &source_msg.meta {
            SourceMeta::Solace(meta) => {
                assert_eq!(meta.destination, Some("topic/test".to_owned()));
                assert_eq!(meta.sender_timestamp, Some(ts));
                assert_eq!(
                    meta.replication_group_message_id,
                    Some("RGMID-ABC123".to_owned())
                );
                assert_eq!(meta.correlation_id, Some("corr-1".to_owned()));
                assert_eq!(meta.sequence_number, Some(99));
                assert_eq!(meta.priority, Some(5));
                assert!(meta.redelivered);
                assert_eq!(
                    meta.application_message_id,
                    Some("app-msg-1".to_owned())
                );
                assert_eq!(meta.expiration, Some(ts));
                assert_eq!(meta.reply_to, Some("reply/topic".to_owned()));
            }
            other => panic!("expected SourceMeta::Solace, got {:?}", other),
        }
    }

    // ── Sentinel detection tests ────────────────────────────────────────

    #[test]
    fn test_is_sentinel_with_valid_property() {
        let mut msg = make_msg(Some("fleet/system/sentinel"));
        msg.user_properties.insert(
            "x-solace-sentinel".to_owned(),
            "backfill-complete".to_owned(),
        );
        assert!(msg.is_sentinel());
    }

    #[test]
    fn test_is_sentinel_with_wrong_value() {
        let mut msg = make_msg(Some("fleet/system/sentinel"));
        msg.user_properties.insert(
            "x-solace-sentinel".to_owned(),
            "something-else".to_owned(),
        );
        assert!(!msg.is_sentinel());
    }

    #[test]
    fn test_is_sentinel_with_no_properties() {
        let msg = make_msg(Some("fleet/telemetry/vehicle_001/metrics"));
        assert!(msg.user_properties.is_empty());
        assert!(!msg.is_sentinel());
    }

    #[test]
    fn test_is_sentinel_with_other_properties() {
        let mut msg = make_msg(Some("fleet/telemetry/vehicle_001/metrics"));
        msg.user_properties.insert(
            "x-custom-header".to_owned(),
            "some-value".to_owned(),
        );
        assert!(!msg.is_sentinel());
    }

    #[test]
    fn test_sentinel_has_payload_but_is_still_sentinel() {
        // Sentinel messages carry a payload (informational), but the sentinel
        // identity comes from the user property, not the payload content.
        let mut msg = make_msg(Some("fleet/system/sentinel"));
        msg.payload = b"{\"message\":\"backfill boundary marker\"}".to_vec();
        msg.user_properties.insert(
            "x-solace-sentinel".to_owned(),
            "backfill-complete".to_owned(),
        );
        assert!(msg.is_sentinel());
        assert!(!msg.payload.is_empty());
    }

    // -- SolaceMeta extract_* method tests --

    fn make_meta_full() -> SolaceMeta {
        let ts = Timestamptz::from_millis(1_700_000_000_000).unwrap();
        SolaceMeta {
            destination: Some("topic/orders".to_owned()),
            sender_timestamp: Some(ts),
            replication_group_message_id: Some("RGMID-XYZ".to_owned()),
            correlation_id: Some("corr-42".to_owned()),
            sequence_number: Some(7),
            priority: Some(3),
            redelivered: true,
            application_message_id: Some("app-id-1".to_owned()),
            expiration: Some(ts),
            reply_to: Some("reply/topic".to_owned()),
        }
    }

    #[test]
    fn test_solace_meta_extract_all_methods() {
        let meta = make_meta_full();

        assert!(
            matches!(meta.extract_destination(), Some(ScalarRefImpl::Utf8("topic/orders"))),
            "destination extractor"
        );
        assert!(
            matches!(meta.extract_sender_timestamp(), Some(ScalarRefImpl::Timestamptz(_))),
            "sender_timestamp extractor"
        );
        assert!(
            matches!(
                meta.extract_replication_group_message_id(),
                Some(ScalarRefImpl::Utf8("RGMID-XYZ"))
            ),
            "replication_group_message_id extractor"
        );
        assert!(
            matches!(meta.extract_correlation_id(), Some(ScalarRefImpl::Utf8("corr-42"))),
            "correlation_id extractor"
        );
        assert!(
            matches!(meta.extract_sequence_number(), Some(ScalarRefImpl::Int64(7))),
            "sequence_number extractor"
        );
        assert!(
            matches!(meta.extract_priority(), Some(ScalarRefImpl::Int32(3))),
            "priority extractor"
        );
        assert!(
            matches!(meta.extract_redelivered(), ScalarRefImpl::Bool(true)),
            "redelivered extractor"
        );
        assert!(
            matches!(
                meta.extract_application_message_id(),
                Some(ScalarRefImpl::Utf8("app-id-1"))
            ),
            "application_message_id extractor"
        );
        assert!(
            matches!(meta.extract_expiration(), Some(ScalarRefImpl::Timestamptz(_))),
            "expiration extractor"
        );
        assert!(
            matches!(meta.extract_reply_to(), Some(ScalarRefImpl::Utf8("reply/topic"))),
            "reply_to extractor"
        );
    }

    #[test]
    fn test_solace_meta_extract_none_fields() {
        let meta = SolaceMeta {
            destination: None,
            sender_timestamp: None,
            replication_group_message_id: None,
            correlation_id: None,
            sequence_number: None,
            priority: None,
            redelivered: false,
            application_message_id: None,
            expiration: None,
            reply_to: None,
        };

        assert!(meta.extract_destination().is_none(), "destination should be None");
        assert!(
            meta.extract_sender_timestamp().is_none(),
            "sender_timestamp should be None"
        );
        assert!(
            meta.extract_replication_group_message_id().is_none(),
            "replication_group_message_id should be None"
        );
        assert!(meta.extract_correlation_id().is_none(), "correlation_id should be None");
        assert!(meta.extract_sequence_number().is_none(), "sequence_number should be None");
        assert!(meta.extract_priority().is_none(), "priority should be None");
        // redelivered is a plain bool — always returns a value (false when not redelivered).
        assert!(
            matches!(meta.extract_redelivered(), ScalarRefImpl::Bool(false)),
            "redelivered should be false"
        );
        assert!(
            meta.extract_application_message_id().is_none(),
            "application_message_id should be None"
        );
        assert!(meta.extract_expiration().is_none(), "expiration should be None");
        assert!(meta.extract_reply_to().is_none(), "reply_to should be None");
    }
}
