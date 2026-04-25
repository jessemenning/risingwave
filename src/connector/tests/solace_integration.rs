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

//! Integration tests for the Solace connector against a real broker.
//!
//! All tests are `#[ignore]` and require:
//!   1. A running Solace broker at `tcp://localhost:55554` (the docker-compose
//!      in `solace-rs/docker-compose.yaml` uses host port 55554 → broker 55555).
//!   2. A pre-provisioned durable queue named `rw-integration-test-queue`
//!      (run `./tests/scripts/setup-solace-queues.sh` once to create it).
//!
//! Run with:
//!   ```
//!   cargo test -p risingwave_connector \
//!       --features source-solace,sink-solace \
//!       --test solace_integration \
//!       -- --include-ignored --test-threads=1
//!   ```
//!
//! **`--test-threads=1` is required.** Queue-based tests share a single exclusive
//! queue; concurrent flows would steal each other's messages.
//!
//! Or use the helper script:
//!   ```
//!   ./src/connector/tests/run-solace-integration-tests.sh --setup
//!   ```
//!
//! These tests bypass the `SolaceSplitReader` framework layer and operate
//! directly against `AsyncSession` + `OwnedAsyncFlow` to avoid hard framework
//! dependencies (`SourceContextRef`, `ParserConfig`, `DeliveryFutureManager`).

#![cfg(all(feature = "source-solace", feature = "sink-solace"))]

use std::sync::Arc;
use std::time::Duration;

use risingwave_connector::connector_common::SolaceCommon;
use risingwave_connector::source::solace::source::message::SolaceMessage;
use solace_rs::flow::AckMode;
use solace_rs::message::{DeliveryMode, DestinationType, Message, MessageDestination, OutboundMessageBuilder};
use solace_rs::SolaceLogLevel;
use solace_rs::context::Context;
use solace_rs::async_support::AsyncSessionBuilder;

/// Host-side broker URL (docker-compose maps host 55554 → broker 55555).
const BROKER_URL: &str = "tcp://localhost:55554";
const BROKER_VPN: &str = "default";
const BROKER_USER: &str = "default";
const BROKER_PASS: &str = "";

/// Pre-provisioned durable queue (created by setup-solace-queues.sh).
const TEST_QUEUE: &str = "rw-integration-test-queue";

/// Per-message receive timeout.
static RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// Brief pause to let subscriptions register before publishing.
static SLEEP_TIME: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// Helper — build a SolaceCommon with local docker defaults
// ---------------------------------------------------------------------------

fn test_common() -> SolaceCommon {
    serde_json::from_value(serde_json::json!({
        "solace.url": BROKER_URL,
        "solace.vpn_name": BROKER_VPN,
        "solace.username": BROKER_USER,
        "solace.password": BROKER_PASS,
    }))
    .expect("SolaceCommon deserialization failed")
}

// ---------------------------------------------------------------------------
// 1. Connection — SolaceCommon::build_async_session connects successfully
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_connect() {
    let common = test_common();
    let (_ctx, session) = common
        .build_async_session(None)
        .expect("build_async_session must not fail with a running broker");

    // If we reach here the TCP handshake + SMRP login succeeded.
    session.disconnect().expect("clean disconnect");
}

// ---------------------------------------------------------------------------
// 2. Publish and receive a persistent message via OwnedAsyncFlow
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_source_receive_message() {
    let ctx = Context::new(SolaceLogLevel::Warning).unwrap();
    let session = AsyncSessionBuilder::new(&ctx)
        .host_name(BROKER_URL)
        .vpn_name(BROKER_VPN)
        .username(BROKER_USER)
        .password(BROKER_PASS)
        .build()
        .expect("session");

    let mut flow = session
        .create_flow(TEST_QUEUE, AckMode::Auto)
        .expect("create_flow");

    let dest = MessageDestination::new(DestinationType::Queue, TEST_QUEUE).unwrap();
    let msg = OutboundMessageBuilder::new()
        .destination(dest)
        .delivery_mode(DeliveryMode::Persistent)
        .payload(b"hello-from-integration-test" as &[u8])
        .build()
        .unwrap();
    session.publish(msg).expect("publish");

    let received = tokio::time::timeout(RECV_TIMEOUT, flow.recv())
        .await
        .expect("timed out waiting for message")
        .expect("channel closed");

    assert_eq!(
        received.get_payload().unwrap().unwrap(),
        b"hello-from-integration-test"
    );
}

// ---------------------------------------------------------------------------
// 3. SolaceMessage::from_inbound — wraps an InboundMessage correctly
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_solace_message_extraction() {
    let ctx = Context::new(SolaceLogLevel::Warning).unwrap();
    let session = AsyncSessionBuilder::new(&ctx)
        .host_name(BROKER_URL)
        .vpn_name(BROKER_VPN)
        .username(BROKER_USER)
        .password(BROKER_PASS)
        .build()
        .expect("session");

    let mut flow = session
        .create_flow(TEST_QUEUE, AckMode::Auto)
        .expect("create_flow");

    let payload = b"extraction-test-payload";
    let dest = MessageDestination::new(DestinationType::Queue, TEST_QUEUE).unwrap();
    let msg = OutboundMessageBuilder::new()
        .destination(dest)
        .delivery_mode(DeliveryMode::Persistent)
        .payload(payload as &[u8])
        .build()
        .unwrap();
    session.publish(msg).expect("publish");

    let inbound = tokio::time::timeout(RECV_TIMEOUT, flow.recv())
        .await
        .expect("timed out")
        .expect("channel closed");

    let split_id: Arc<str> = Arc::from("test-split-0");
    let solace_msg = SolaceMessage::from_inbound(&inbound, split_id.clone());

    assert_eq!(solace_msg.payload, payload);
    assert_eq!(solace_msg.split_id, split_id);
    // Persistent messages always carry a broker-assigned msg_id (non-empty).
    assert!(
        !solace_msg.msg_id.is_empty(),
        "persistent message must have a msg_id"
    );
    assert!(
        solace_msg.msg_id.parse::<u64>().is_ok(),
        "msg_id must be parseable as u64 for checkpoint-ack: got '{}'",
        solace_msg.msg_id
    );
    assert!(!solace_msg.is_sentinel());
}

// ---------------------------------------------------------------------------
// 4. Sentinel detection — x-solace-sentinel: backfill-complete user property
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_sentinel_detection() {
    let ctx = Context::new(SolaceLogLevel::Warning).unwrap();
    let session = AsyncSessionBuilder::new(&ctx)
        .host_name(BROKER_URL)
        .vpn_name(BROKER_VPN)
        .username(BROKER_USER)
        .password(BROKER_PASS)
        .build()
        .expect("session");

    let mut flow = session
        .create_flow(TEST_QUEUE, AckMode::Auto)
        .expect("create_flow");

    // Publish a regular message followed by a sentinel.
    let regular = OutboundMessageBuilder::new()
        .destination(MessageDestination::new(DestinationType::Queue, TEST_QUEUE).unwrap())
        .delivery_mode(DeliveryMode::Persistent)
        .payload(b"regular-payload" as &[u8])
        .build()
        .unwrap();
    session.publish(regular).expect("publish regular");

    let sentinel = OutboundMessageBuilder::new()
        .destination(MessageDestination::new(DestinationType::Queue, TEST_QUEUE).unwrap())
        .delivery_mode(DeliveryMode::Persistent)
        .payload(b"sentinel-payload" as &[u8])
        .user_property("x-solace-sentinel", "backfill-complete")
        .build()
        .unwrap();
    session.publish(sentinel).expect("publish sentinel");

    let split_id: Arc<str> = Arc::from("test-split-0");

    // First message: not a sentinel.
    let inbound1 = tokio::time::timeout(RECV_TIMEOUT, flow.recv())
        .await
        .expect("timed out waiting for regular message")
        .expect("channel closed");
    let msg1 = SolaceMessage::from_inbound(&inbound1, split_id.clone());
    assert!(!msg1.is_sentinel(), "regular message must not be a sentinel");
    assert_eq!(msg1.payload, b"regular-payload");

    // Second message: sentinel.
    let inbound2 = tokio::time::timeout(RECV_TIMEOUT, flow.recv())
        .await
        .expect("timed out waiting for sentinel message")
        .expect("channel closed");
    let msg2 = SolaceMessage::from_inbound(&inbound2, split_id);
    assert!(msg2.is_sentinel(), "message with sentinel user property must be detected");
    assert_eq!(msg2.payload, b"sentinel-payload");
}

// ---------------------------------------------------------------------------
// 5. Explicit client ack — acked message is not redelivered after reconnect
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_client_ack_no_redeliver() {
    let ctx = Context::new(SolaceLogLevel::Warning).unwrap();
    let session = AsyncSessionBuilder::new(&ctx)
        .host_name(BROKER_URL)
        .vpn_name(BROKER_VPN)
        .username(BROKER_USER)
        .password(BROKER_PASS)
        .build()
        .expect("session");

    let mut flow = session
        .create_flow(TEST_QUEUE, AckMode::Client)
        .expect("create_flow");

    let dest = MessageDestination::new(DestinationType::Queue, TEST_QUEUE).unwrap();
    let msg = OutboundMessageBuilder::new()
        .destination(dest)
        .delivery_mode(DeliveryMode::Persistent)
        .payload(b"ack-test-payload" as &[u8])
        .build()
        .unwrap();
    session.publish(msg).expect("publish");

    let received = tokio::time::timeout(RECV_TIMEOUT, flow.recv())
        .await
        .expect("timed out")
        .expect("channel closed");

    assert_eq!(received.get_payload().unwrap().unwrap(), b"ack-test-payload");

    let msg_id = received
        .get_msg_id()
        .expect("get_msg_id error")
        .expect("persistent message must carry a msg_id");

    // Ack before dropping the flow.
    flow.ack(msg_id).expect("ack failed");
    drop(flow);

    // Reconnect with a new flow — the acked message must not reappear.
    let flow2 = session
        .create_flow(TEST_QUEUE, AckMode::Client)
        .expect("create_flow 2");

    // Give the broker a moment, then assert the queue is empty.
    tokio::time::sleep(SLEEP_TIME).await;

    let mut flow2 = flow2;
    let result = flow2.try_recv();
    assert!(
        result.is_err(),
        "acked message must not be redelivered to a new flow; got a message instead"
    );
}

// ---------------------------------------------------------------------------
// 6. Direct message publish via AsyncSession
//    (mirrors the sink connector's Direct delivery path)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_sink_direct_publish() {
    let topic = "rw/integration/sink/direct";

    let ctx = Context::new(SolaceLogLevel::Warning).unwrap();

    // Subscriber session.
    let mut sub_session = AsyncSessionBuilder::new(&ctx)
        .host_name(BROKER_URL)
        .vpn_name(BROKER_VPN)
        .username(BROKER_USER)
        .password(BROKER_PASS)
        .build()
        .expect("subscriber session");
    sub_session.subscribe(topic).expect("subscribe");
    tokio::time::sleep(SLEEP_TIME).await;

    // Publisher session (separate client, mirrors the sink connector).
    let pub_session = AsyncSessionBuilder::new(&ctx)
        .host_name(BROKER_URL)
        .vpn_name(BROKER_VPN)
        .username(BROKER_USER)
        .password(BROKER_PASS)
        .build()
        .expect("publisher session");

    let dest = MessageDestination::new(DestinationType::Topic, topic).unwrap();
    let msg = OutboundMessageBuilder::new()
        .destination(dest)
        .delivery_mode(DeliveryMode::Direct)
        .payload(b"direct-sink-payload" as &[u8])
        .build()
        .unwrap();
    pub_session.publish(msg).expect("publish direct");

    let received = tokio::time::timeout(RECV_TIMEOUT, sub_session.recv())
        .await
        .expect("timed out waiting for direct message")
        .expect("channel closed");

    assert_eq!(
        received.get_payload().unwrap().unwrap(),
        b"direct-sink-payload"
    );
}
