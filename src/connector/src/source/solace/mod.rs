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

pub mod enumerator;
pub use enumerator::SolaceSplitEnumerator;
pub mod source;
pub mod split;
pub use split::SolaceSplit;

use std::collections::HashMap;

use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use with_options::WithOptions;

use crate::connector_common::SolaceCommon;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::source::SourceProperties;
use crate::source::solace::source::reader::SolaceSplitReader;

pub const SOLACE_CONNECTOR: &str = "solace";

/// Acknowledgment mode for Solace guaranteed messages.
///
/// - `checkpoint`: Messages are ACKed on the Solace queue only after RisingWave has
///   durably checkpointed them. Provides exactly-once semantics but higher latency.
/// - `immediate`: Messages are ACKed immediately upon read. Lower latency but
///   messages may be redelivered on failure (at-least-once).
#[derive(Debug, Clone, Default)]
pub enum SolaceAckMode {
    #[default]
    Checkpoint,
    Immediate,
}

impl SolaceAckMode {
    pub fn from_str_opt(s: Option<&str>) -> ConnectorResult<Self> {
        match s {
            None | Some("checkpoint") => Ok(Self::Checkpoint),
            Some("immediate") => Ok(Self::Immediate),
            Some(other) => Err(anyhow::anyhow!(
                "invalid solace.ack_mode '{}', expected 'checkpoint' or 'immediate'",
                other
            )
            .into()),
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct SolaceProperties {
    #[serde(flatten)]
    pub common: SolaceCommon,

    /// Solace queue name to bind to for guaranteed message consumption.
    #[serde(rename = "solace.queue")]
    pub queue: String,

    /// Acknowledgment mode: "checkpoint" (default, slower, exactly-once) or
    /// "immediate" (faster, at-least-once).
    #[serde(rename = "solace.ack_mode")]
    pub ack_mode: Option<String>,

    /// Topic prefix for the readiness event published after sentinel detection.
    /// The connector appends `/{queue_name}/ready` to form the full topic.
    /// When unset, no readiness event is published (sentinel is still detected
    /// and intercepted, but no outbound notification is sent).
    #[serde(rename = "solace.sentinel_readiness_topic")]
    pub sentinel_readiness_topic: Option<String>,

    /// RisingWave Postgres DSN used for connector-status writes and barrier flush.
    /// Defaults to `host=localhost port=4566 user=root dbname=dev connect_timeout=5`.
    #[serde(rename = "solace.risingwave_dsn")]
    pub risingwave_dsn: Option<String>,

    /// Maximum number of messages to batch per yield.
    /// Defaults to 100. Higher values increase throughput at the cost of latency.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "solace.batch_size")]
    pub batch_size: Option<usize>,

    /// Number of parallel consumers (RisingWave splits) binding to the queue.
    /// Each consumer gets its own Solace flow and is assigned work by the broker's
    /// built-in load balancer. Defaults to 1.
    ///
    /// **Limitation:** sentinel-based backfill detection is automatically disabled
    /// when this value is greater than 1, because a single sentinel message is
    /// consumed by only one of the N readers. See PARALLEL_CONSUMERS.md for
    /// the Option A roadmap to lift this restriction.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "solace.num_consumers")]
    pub num_consumers: Option<usize>,

    /// Timeout in milliseconds for the RisingWave `WAIT` barrier flush command issued
    /// after sentinel detection. Defaults to 60000 (60 seconds). Increase for large
    /// historical backfills where materialized view catch-up may take longer.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "solace.wait_timeout_ms")]
    pub wait_timeout_ms: Option<u64>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl SolaceProperties {
    /// Returns the configured RisingWave DSN, or the local-dev default.
    pub fn risingwave_dsn_or_default(&self) -> &str {
        self.risingwave_dsn
            .as_deref()
            .unwrap_or("host=localhost port=4566 user=root dbname=dev connect_timeout=5")
    }

    /// Returns the configured WAIT timeout in milliseconds, or the default of 60 000 ms.
    pub fn wait_timeout_ms_or_default(&self) -> u64 {
        self.wait_timeout_ms.unwrap_or(60_000)
    }
}

impl EnforceSecret for SolaceProperties {
    fn enforce_secret<'a>(prop_iter: impl Iterator<Item = &'a str>) -> ConnectorResult<()> {
        for prop in prop_iter {
            SolaceCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl SourceProperties for SolaceProperties {
    type Split = SolaceSplit;
    type SplitEnumerator = SolaceSplitEnumerator;
    type SplitReader = SolaceSplitReader;

    const SOURCE_NAME: &'static str = SOLACE_CONNECTOR;
}

impl crate::source::UnknownFields for SolaceProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use maplit::btreemap;

    use super::*;

    #[test]
    fn test_parse_solace_properties() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://broker.example.com:55555".to_owned(),
            "solace.vpn_name".to_owned() => "my_vpn".to_owned(),
            "solace.username".to_owned() => "admin".to_owned(),
            "solace.password".to_owned() => "secret".to_owned(),
            "solace.client_name".to_owned() => "rw-source-1".to_owned(),
            "solace.reconnect_retries".to_owned() => "5".to_owned(),
            "solace.reconnect_retry_wait_ms".to_owned() => "2000".to_owned(),
            "solace.connect_timeout_ms".to_owned() => "10000".to_owned(),
            "solace.queue".to_owned() => "orders-queue".to_owned(),
            "solace.ack_mode".to_owned() => "immediate".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();

        assert_eq!(props.common.url, "tcp://broker.example.com:55555");
        assert_eq!(props.common.vpn_name, Some("my_vpn".to_owned()));
        assert_eq!(props.common.username, Some("admin".to_owned()));
        assert_eq!(props.common.password, Some("secret".to_owned()));
        assert_eq!(props.common.client_name, Some("rw-source-1".to_owned()));
        assert_eq!(props.common.reconnect_retries, Some(5));
        assert_eq!(props.common.reconnect_retry_wait_ms, Some(2000));
        assert_eq!(props.common.connect_timeout_ms, Some(10000));
        assert_eq!(props.queue, "orders-queue");
        assert_eq!(props.ack_mode, Some("immediate".to_owned()));
    }

    #[test]
    fn test_parse_solace_properties_minimal() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "test-queue".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();

        assert_eq!(props.common.url, "tcp://localhost:55555");
        assert_eq!(props.common.vpn_name, None);
        assert_eq!(props.common.username, None);
        assert_eq!(props.common.password, None);
        assert_eq!(props.common.client_name, None);
        assert_eq!(props.common.ssl_trust_store_dir, None);
        assert_eq!(props.common.reconnect_retries, None);
        assert_eq!(props.common.reconnect_retry_wait_ms, None);
        assert_eq!(props.common.connect_timeout_ms, None);
        assert_eq!(props.queue, "test-queue");
        assert_eq!(props.ack_mode, None);
    }

    #[test]
    fn test_parse_solace_ack_mode_checkpoint() {
        let mode = SolaceAckMode::from_str_opt(Some("checkpoint")).unwrap();
        assert!(matches!(mode, SolaceAckMode::Checkpoint));
    }

    #[test]
    fn test_parse_solace_ack_mode_immediate() {
        let mode = SolaceAckMode::from_str_opt(Some("immediate")).unwrap();
        assert!(matches!(mode, SolaceAckMode::Immediate));
    }

    #[test]
    fn test_parse_solace_ack_mode_default() {
        let mode = SolaceAckMode::from_str_opt(None).unwrap();
        assert!(matches!(mode, SolaceAckMode::Checkpoint));
    }

    #[test]
    fn test_parse_solace_ack_mode_invalid() {
        let result = SolaceAckMode::from_str_opt(Some("bogus"));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sentinel_readiness_topic() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "test-queue".to_owned(),
            "solace.sentinel_readiness_topic".to_owned() => "system/risingwave/connector".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert_eq!(
            props.sentinel_readiness_topic,
            Some("system/risingwave/connector".to_owned())
        );
    }

    #[test]
    fn test_parse_risingwave_dsn() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "test-queue".to_owned(),
            "solace.risingwave_dsn".to_owned() => "host=rw-host port=4566 user=root dbname=prod".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert_eq!(
            props.risingwave_dsn_or_default(),
            "host=rw-host port=4566 user=root dbname=prod"
        );
    }

    #[test]
    fn test_risingwave_dsn_default() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "test-queue".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert_eq!(
            props.risingwave_dsn_or_default(),
            "host=localhost port=4566 user=root dbname=dev connect_timeout=5"
        );
    }

    #[test]
    fn test_parse_batch_size() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "test-queue".to_owned(),
            "solace.batch_size".to_owned() => "250".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert_eq!(props.batch_size, Some(250));
    }

    #[test]
    fn test_batch_size_default_is_none() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "test-queue".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert_eq!(props.batch_size, None);
    }

    #[test]
    fn test_parse_no_sentinel_readiness_topic() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "test-queue".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert_eq!(props.sentinel_readiness_topic, None);
    }

    #[test]
    fn test_parse_num_consumers() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "test-queue".to_owned(),
            "solace.num_consumers".to_owned() => "4".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert_eq!(props.num_consumers, Some(4));
    }

    #[test]
    fn test_num_consumers_default_is_none() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "test-queue".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert_eq!(props.num_consumers, None);
    }

    #[test]
    fn test_parse_wait_timeout_ms() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "test-queue".to_owned(),
            "solace.wait_timeout_ms".to_owned() => "120000".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert_eq!(props.wait_timeout_ms, Some(120_000));
        assert_eq!(props.wait_timeout_ms_or_default(), 120_000);
    }

    #[test]
    fn test_wait_timeout_ms_default() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "test-queue".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
        assert_eq!(props.wait_timeout_ms, None);
        assert_eq!(props.wait_timeout_ms_or_default(), 60_000);
    }

    #[test]
    fn test_unknown_fields_collected() {
        let config: BTreeMap<String, String> = btreemap! {
            "solace.url".to_owned() => "tcp://localhost:55555".to_owned(),
            "solace.queue".to_owned() => "q".to_owned(),
            "some.unknown.field".to_owned() => "value".to_owned(),
            "another.field".to_owned() => "123".to_owned(),
        };

        let props: SolaceProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();

        let unknown = crate::source::UnknownFields::unknown_fields(&props);
        assert_eq!(unknown.get("some.unknown.field"), Some(&"value".to_owned()));
        assert_eq!(unknown.get("another.field"), Some(&"123".to_owned()));
        // Known fields should NOT appear in unknown_fields
        assert!(!unknown.contains_key("solace.url"));
        assert!(!unknown.contains_key("solace.queue"));
    }
}
