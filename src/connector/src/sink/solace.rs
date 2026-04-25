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

use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use futures::FutureExt;
use futures::prelude::TryFuture;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::ScalarRefImpl;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::SinkWriterParam;
use super::catalog::{SinkEncode, SinkFormat, SinkFormatDesc};
use super::encoder::{
    DateHandlingMode, JsonEncoder, JsonbHandlingMode, RowEncoder, SerTo, TimeHandlingMode,
    TimestampHandlingMode, TimestamptzHandlingMode,
};
use super::writer::AsyncTruncateSinkWriterExt;
use crate::connector_common::SolaceCommon;
use crate::enforce_secret::EnforceSecret;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter};
use crate::sink::{Result, SINK_TYPE_APPEND_ONLY, Sink, SinkError, SinkParam};
use solace_rs::SessionError;
use solace_rs::context::Context;
use solace_rs::message::DeliveryMode;
use solace_rs::message::destination::{DestinationType, MessageDestination};
use solace_rs::message::outbound::OutboundMessageBuilder;

pub const SOLACE_SINK: &str = "solace";

pub type SolaceSinkDeliveryFuture =
    impl TryFuture<Ok = (), Error = SinkError> + Unpin + 'static;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct SolaceConfig {
    #[serde(flatten)]
    pub common: SolaceCommon,

    /// Parameterized topic string, e.g. `acme/orders/{region}/{status}/v1`.
    /// Each `{column_name}` placeholder is replaced with the column's value at write time.
    #[serde(rename = "solace.topic")]
    pub topic_template: String,

    /// SMF delivery mode: "direct" (default), "persistent", or "non_persistent".
    #[serde(rename = "solace.delivery_mode", default = "default_delivery_mode")]
    pub delivery_mode: String,

    /// Comma-separated list of column names used as a deduplication key.
    /// When set, messages with the same key value are suppressed until
    /// `solace.dedup_window` has elapsed since the last emission.
    /// Dedup state is in-memory only and resets on connector restart;
    /// a key suppressed before restart may re-emit once immediately after restart.
    #[serde(rename = "solace.dedup_key")]
    pub dedup_key: Option<String>,

    /// Duration during which repeat emissions for the same dedup key are suppressed.
    /// Accepts "Xs" (seconds), "Xm" (minutes), "Xh" (hours), or a plain integer
    /// number of seconds (e.g. "300", "5m", "1h"). Required when `solace.dedup_key` is set.
    #[serde(rename = "solace.dedup_window")]
    pub dedup_window: Option<String>,

    /// How to handle a NULL column value when rendering the topic template.
    /// - `"error"` (default): return an error and fail the row
    /// - `"skip"`: silently drop the row without publishing
    /// - `"replace:<value>"`: substitute a literal string for the NULL segment
    #[serde(rename = "solace.null_topic_behavior", default = "default_null_topic_behavior")]
    pub null_topic_behavior: String,

    /// Maximum payload size in bytes. Messages exceeding this limit are dropped
    /// with a warning instead of being sent. Useful for avoiding broker rejections
    /// when the Solace broker max message size is below the default 10 MB (10485760).
    /// When unset, no size limit is enforced.
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    #[serde(rename = "solace.max_message_bytes")]
    pub max_message_bytes: Option<usize>,

    /// How to encode DATE columns in the JSON payload.
    /// - `"from_ce"` (default): integer days since the Common Era epoch (0001-01-01)
    /// - `"from_epoch"`: integer days since the Unix epoch (1970-01-01)
    /// - `"string"`: ISO 8601 string (e.g. `"2024-04-25"`)
    #[serde(rename = "solace.date_handling_mode", default = "default_date_handling_mode")]
    pub date_handling_mode: String,

    /// How to encode TIMESTAMP columns in the JSON payload.
    /// - `"milli"` (default): integer milliseconds since Unix epoch
    /// - `"string"`: ISO 8601 string
    #[serde(rename = "solace.timestamp_handling_mode", default = "default_timestamp_handling_mode")]
    pub timestamp_handling_mode: String,

    // accept "append-only"
    pub r#type: String,
}

fn default_delivery_mode() -> String {
    "direct".to_owned()
}

fn default_null_topic_behavior() -> String {
    "error".to_owned()
}

fn default_date_handling_mode() -> String {
    "from_ce".to_owned()
}

fn default_timestamp_handling_mode() -> String {
    "milli".to_owned()
}

impl SolaceConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<SolaceConfig>(
            serde_json::to_value(values)
                .expect("BTreeMap<String, String> is always JSON-serializable"),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY {
            return Err(SinkError::Config(anyhow!(
                "Solace sink only supports append-only mode"
            )));
        }
        Ok(config)
    }
}

// ---------------------------------------------------------------------------
// Null topic behavior
// ---------------------------------------------------------------------------

/// Policy for handling NULL column values when rendering the topic template.
#[derive(Debug, Clone)]
enum NullTopicBehavior {
    /// Return an error — the write fails for the affected row (default).
    Error,
    /// Silently skip the row without publishing a message.
    Skip,
    /// Substitute a literal replacement string for the NULL topic segment.
    Replace(String),
}

fn parse_null_topic_behavior(s: &str) -> Result<NullTopicBehavior> {
    if s == "error" {
        return Ok(NullTopicBehavior::Error);
    }
    if s == "skip" {
        return Ok(NullTopicBehavior::Skip);
    }
    if let Some(val) = s.strip_prefix("replace:") {
        return Ok(NullTopicBehavior::Replace(val.to_owned()));
    }
    Err(SinkError::Config(anyhow!(
        "invalid solace.null_topic_behavior '{}', expected 'error', 'skip', or 'replace:<value>'",
        s
    )))
}

// ---------------------------------------------------------------------------
// Topic template
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum Segment {
    Literal(String),
    Column { index: usize },
}

#[derive(Debug)]
struct TopicTemplate {
    segments: Vec<Segment>,
    null_behavior: NullTopicBehavior,
}

impl TopicTemplate {
    fn parse(template: &str, schema: &Schema, null_behavior: NullTopicBehavior) -> Result<Self> {
        let mut segments = Vec::new();
        let mut remaining = template;

        while !remaining.is_empty() {
            if let Some(open) = remaining.find('{') {
                if open > 0 {
                    segments.push(Segment::Literal(remaining[..open].to_owned()));
                }
                remaining = &remaining[open + 1..];
                let close = remaining.find('}').ok_or_else(|| {
                    SinkError::Config(anyhow!(
                        "unclosed '{{' in solace.topic template '{}'",
                        template
                    ))
                })?;
                let col_name = &remaining[..close];
                let index = schema
                    .fields()
                    .iter()
                    .position(|f| f.name == col_name)
                    .ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "column '{}' in solace.topic template not found in schema",
                            col_name
                        ))
                    })?;
                segments.push(Segment::Column { index });
                remaining = &remaining[close + 1..];
            } else {
                segments.push(Segment::Literal(remaining.to_owned()));
                break;
            }
        }

        Ok(Self { segments, null_behavior })
    }

    /// Renders the topic string for `row`.
    ///
    /// Returns `Ok(None)` when a column is NULL and `null_behavior` is `Skip`.
    /// Returns `Ok(Some(topic))` on success.
    /// Returns `Err` when `null_behavior` is `Error` and a NULL is encountered, or on other errors.
    fn render(&self, row: impl Row) -> Result<Option<String>> {
        let mut out = String::new();
        for seg in &self.segments {
            match seg {
                Segment::Literal(s) => out.push_str(s),
                Segment::Column { index } => {
                    let datum = row.datum_at(*index);
                    let s = match datum {
                        Some(scalar) => scalar_to_topic_segment(scalar)?,
                        None => match &self.null_behavior {
                            NullTopicBehavior::Error => {
                                return Err(SinkError::Solace(anyhow!(
                                    "NULL value in topic template column index {}",
                                    index
                                )));
                            }
                            NullTopicBehavior::Skip => return Ok(None),
                            NullTopicBehavior::Replace(val) => val.clone(),
                        },
                    };
                    if s.contains('/') {
                        return Err(SinkError::Solace(anyhow!(
                            "topic segment value '{}' contains '/' which is not allowed",
                            s
                        )));
                    }
                    out.push_str(&s);
                }
            }
        }
        Ok(Some(out))
    }
}

fn scalar_to_topic_segment(scalar: ScalarRefImpl<'_>) -> Result<String> {
    match scalar {
        ScalarRefImpl::Utf8(s) => Ok(s.to_owned()),
        ScalarRefImpl::Int16(v) => Ok(v.to_string()),
        ScalarRefImpl::Int32(v) => Ok(v.to_string()),
        ScalarRefImpl::Int64(v) => Ok(v.to_string()),
        ScalarRefImpl::Float32(v) => Ok(v.0.to_string()),
        ScalarRefImpl::Float64(v) => Ok(v.0.to_string()),
        ScalarRefImpl::Bool(v) => Ok(v.to_string()),
        ScalarRefImpl::Decimal(v) => Ok(v.to_string()),
        ScalarRefImpl::Serial(v) => Ok(v.as_row_id().to_string()),
        other => Err(SinkError::Solace(anyhow!(
            "unsupported type for topic template column: {:?}",
            other
        ))),
    }
}

/// Converts a scalar to its string representation for use as a dedup key component.
/// Returns a consistent, locale-invariant string for all supported types.
fn scalar_for_dedup_key(scalar: ScalarRefImpl<'_>) -> String {
    match scalar {
        ScalarRefImpl::Utf8(s) => s.to_owned(),
        ScalarRefImpl::Int16(v) => v.to_string(),
        ScalarRefImpl::Int32(v) => v.to_string(),
        ScalarRefImpl::Int64(v) => v.to_string(),
        ScalarRefImpl::Float32(v) => v.0.to_string(),
        ScalarRefImpl::Float64(v) => v.0.to_string(),
        ScalarRefImpl::Bool(v) => v.to_string(),
        ScalarRefImpl::Decimal(v) => v.to_string(),
        ScalarRefImpl::Serial(v) => v.as_row_id().to_string(),
        other => format!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Dedup filter
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct DedupFilter {
    cur:             HashSet<String>,
    prev:            HashSet<String>,
    cur_start:       Instant,
    half_window:     Duration,
    key_col_indices: Vec<usize>,
}

impl DedupFilter {
    fn new(dedup_key: &str, window: Duration, schema: &Schema) -> Result<Self> {
        let col_names: Vec<&str> = dedup_key.split(',').map(|c| c.trim()).collect();

        // Reject duplicate column names up front.
        let mut seen = HashSet::new();
        for col in &col_names {
            if !seen.insert(*col) {
                return Err(SinkError::Config(anyhow!(
                    "solace.dedup_key contains duplicate column '{}'",
                    col
                )));
            }
        }

        let indices: Result<Vec<usize>> = col_names
            .iter()
            .map(|col| {
                schema
                    .fields()
                    .iter()
                    .position(|f| f.name == *col)
                    .ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "solace.dedup_key column '{}' not found in schema",
                            col
                        ))
                    })
            })
            .collect();
        Ok(Self {
            cur:         HashSet::new(),
            prev:        HashSet::new(),
            cur_start:   Instant::now(),
            half_window: window / 2,
            key_col_indices: indices?,
        })
    }

    fn build_key(&self, row: impl Row) -> String {
        // Serialize as a JSON array to prevent key collisions when column values contain
        // characters that would otherwise be confused with the key separator.
        // serde_json handles string escaping, making ("a|b","c") != ("a","b|c").
        let values: Vec<serde_json::Value> = self
            .key_col_indices
            .iter()
            .map(|&i| match row.datum_at(i) {
                None => serde_json::Value::Null,
                Some(s) => serde_json::Value::String(scalar_for_dedup_key(s)),
            })
            .collect();
        serde_json::to_string(&values).unwrap_or_default()
    }

    fn should_emit(&mut self, row: impl Row) -> bool {
        let key = self.build_key(row);
        let now = Instant::now();

        if now >= self.cur_start + self.half_window {
            self.prev = std::mem::take(&mut self.cur);
            self.cur_start = now;
        }

        if self.cur.contains(&key) || self.prev.contains(&key) {
            return false;
        }
        self.cur.insert(key);
        true
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if let Some(stripped) = s.strip_suffix('h') {
        let v: u64 = stripped
            .parse()
            .map_err(|_| SinkError::Config(anyhow!("invalid solace.dedup_window '{}'", s)))?;
        return Ok(Duration::from_secs(v * 3600));
    }
    if let Some(stripped) = s.strip_suffix('m') {
        let v: u64 = stripped
            .parse()
            .map_err(|_| SinkError::Config(anyhow!("invalid solace.dedup_window '{}'", s)))?;
        return Ok(Duration::from_secs(v * 60));
    }
    if let Some(stripped) = s.strip_suffix('s') {
        let v: u64 = stripped
            .parse()
            .map_err(|_| SinkError::Config(anyhow!("invalid solace.dedup_window '{}'", s)))?;
        return Ok(Duration::from_secs(v));
    }
    let v: u64 = s
        .parse()
        .map_err(|_| SinkError::Config(anyhow!("invalid solace.dedup_window '{}'", s)))?;
    Ok(Duration::from_secs(v))
}

/// Copy-safe proxy for `DeliveryMode` (the solace-rs type isn't `Copy` or `Clone`).
#[derive(Debug, Clone, Copy)]
enum SolaceDeliveryMode {
    Direct,
    Persistent,
    NonPersistent,
}

impl SolaceDeliveryMode {
    fn into_solace(self) -> DeliveryMode {
        match self {
            Self::Direct => DeliveryMode::Direct,
            Self::Persistent => DeliveryMode::Persistent,
            Self::NonPersistent => DeliveryMode::NonPersistent,
        }
    }
}

fn parse_delivery_mode(s: &str) -> Result<SolaceDeliveryMode> {
    match s {
        "direct" => Ok(SolaceDeliveryMode::Direct),
        "persistent" => Ok(SolaceDeliveryMode::Persistent),
        "non_persistent" => Ok(SolaceDeliveryMode::NonPersistent),
        other => Err(SinkError::Config(anyhow!(
            "invalid solace.delivery_mode '{}', expected 'direct', 'persistent', or 'non_persistent'",
            other
        ))),
    }
}

fn parse_date_handling_mode(s: &str) -> Result<DateHandlingMode> {
    match s {
        "from_ce" => Ok(DateHandlingMode::FromCe),
        "from_epoch" => Ok(DateHandlingMode::FromEpoch),
        "string" => Ok(DateHandlingMode::String),
        other => Err(SinkError::Config(anyhow!(
            "invalid solace.date_handling_mode '{}', expected 'from_ce', 'from_epoch', or 'string'",
            other
        ))),
    }
}

fn parse_timestamp_handling_mode(s: &str) -> Result<TimestampHandlingMode> {
    match s {
        "milli" => Ok(TimestampHandlingMode::Milli),
        "string" => Ok(TimestampHandlingMode::String),
        other => Err(SinkError::Config(anyhow!(
            "invalid solace.timestamp_handling_mode '{}', expected 'milli' or 'string'",
            other
        ))),
    }
}

fn build_json_encoder(
    schema: &Schema,
    format_desc: &SinkFormatDesc,
    config: &SolaceConfig,
) -> Result<JsonEncoder> {
    let timestamptz_mode = TimestamptzHandlingMode::from_options(&format_desc.options)?;
    let jsonb_handling_mode = JsonbHandlingMode::from_options(&format_desc.options)?;
    let date_mode = parse_date_handling_mode(&config.date_handling_mode)?;
    let ts_mode = parse_timestamp_handling_mode(&config.timestamp_handling_mode)?;
    match &format_desc.format {
        SinkFormat::AppendOnly => match &format_desc.encode {
            SinkEncode::Json => Ok(JsonEncoder::new(
                schema.clone(),
                None,
                date_mode,
                ts_mode,
                timestamptz_mode,
                TimeHandlingMode::Milli,
                jsonb_handling_mode,
            )),
            other => Err(SinkError::Config(anyhow!(
                "Solace sink encode unsupported: {:?}; only JSON is supported",
                other
            ))),
        },
        other => Err(SinkError::Config(anyhow!(
            "Solace sink format unsupported: {:?}; only append-only is supported",
            other
        ))),
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct SolaceSink {
    pub config: SolaceConfig,
    schema: Schema,
    format_desc: SinkFormatDesc,
    is_append_only: bool,
    name: String,
}

impl EnforceSecret for SolaceSink {
    fn enforce_secret<'a>(prop_iter: impl Iterator<Item = &'a str>) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            SolaceCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for SolaceSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = SolaceConfig::from_btreemap(param.properties)?;
        Ok(Self {
            config,
            schema,
            name: param.sink_name,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for SolaceSink {
    type LogSinker = AsyncTruncateLogSinkerOf<SolaceSinkWriter>;

    const SINK_NAME: &'static str = SOLACE_SINK;

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Config(anyhow!(
                "Solace sink only supports append-only mode"
            )));
        }

        // Validate null_topic_behavior before parsing the template (it's needed for parse).
        let null_behavior = parse_null_topic_behavior(&self.config.null_topic_behavior)?;

        // Validate topic template column references.
        TopicTemplate::parse(&self.config.topic_template, &self.schema, null_behavior)?;

        // Validate delivery mode string.
        parse_delivery_mode(&self.config.delivery_mode)?;

        // Validate dedup config if provided.
        if let Some(ref key) = self.config.dedup_key {
            let window_str = self.config.dedup_window.as_deref().ok_or_else(|| {
                SinkError::Config(anyhow!(
                    "solace.dedup_window is required when solace.dedup_key is set"
                ))
            })?;
            let window = parse_duration(window_str)?;
            DedupFilter::new(key, window, &self.schema)?;
        }

        // Validate encoder config.
        build_json_encoder(&self.schema, &self.format_desc, &self.config)?;

        // Broker smoke-test: verify we can connect.
        let (_ctx, _session) = self
            .config
            .common
            .build_async_session(Some(&format!("{}-validate", self.name)))
            .map_err(|e| SinkError::Solace(anyhow!(e)))?;

        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(SolaceSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            &self.format_desc,
            &self.name,
        )?
        .into_log_sinker(usize::MAX))
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

pub struct SolaceSinkWriter {
    // held alive for C FFI: session borrows context's C-level handle
    _context: Context,
    session: solace_rs::async_support::AsyncSession,
    topic_template: TopicTemplate,
    delivery_mode: SolaceDeliveryMode,
    encoder: JsonEncoder,
    dedup_filter: Option<DedupFilter>,
    max_message_bytes: Option<usize>,
}

impl SolaceSinkWriter {
    fn new(
        config: SolaceConfig,
        schema: Schema,
        format_desc: &SinkFormatDesc,
        name: &str,
    ) -> Result<Self> {
        let null_behavior = parse_null_topic_behavior(&config.null_topic_behavior)?;
        let topic_template = TopicTemplate::parse(&config.topic_template, &schema, null_behavior)?;
        let delivery_mode = parse_delivery_mode(&config.delivery_mode)?;
        let encoder = build_json_encoder(&schema, format_desc, &config)?;
        let max_message_bytes = config.max_message_bytes;

        let dedup_filter = if let Some(ref key) = config.dedup_key {
            let window_str = config.dedup_window.as_deref().ok_or_else(|| {
                SinkError::Config(anyhow!(
                    "solace.dedup_window is required when solace.dedup_key is set"
                ))
            })?;
            let window = parse_duration(window_str)?;
            Some(DedupFilter::new(key, window, &schema)?)
        } else {
            None
        };

        let (context, session) = config
            .common
            .build_async_session(Some(name))
            .map_err(|e| SinkError::Solace(anyhow!(e)))?;

        Ok(Self {
            _context: context,
            session,
            topic_template,
            delivery_mode,
            encoder,
            dedup_filter,
            max_message_bytes,
        })
    }
}

impl AsyncTruncateSinkWriter for SolaceSinkWriter {
    type DeliveryFuture = SolaceSinkDeliveryFuture;

    /// For `delivery_mode = "direct"` this is fire-and-forget — broker acknowledgement
    /// is never requested. For `"persistent"` and `"non_persistent"` modes, each message
    /// is published with a correlation tag and the resulting broker ACK future is registered
    /// with `add_future`, so RisingWave does not advance its checkpoint until the broker
    /// has confirmed delivery.
    #[define_opaque(SolaceSinkDeliveryFuture)]
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }

            if let Some(ref mut dedup) = self.dedup_filter {
                if !dedup.should_emit(row) {
                    continue;
                }
            }

            // render() returns Ok(None) when null_topic_behavior = "skip" and a NULL
            // column is encountered. Use let-else to drop those rows silently.
            let Some(topic) = self.topic_template.render(row)? else {
                continue;
            };

            let payload: Vec<u8> = self
                .encoder
                .encode(row)?
                .ser_to()
                .map_err(|e| SinkError::Solace(anyhow!(e)))?;

            if let Some(max_bytes) = self.max_message_bytes {
                if payload.len() > max_bytes {
                    tracing::warn!(
                        topic = %topic,
                        payload_bytes = payload.len(),
                        max_bytes,
                        "Message payload exceeds solace.max_message_bytes — dropping",
                    );
                    continue;
                }
            }

            let dest = MessageDestination::new(DestinationType::Topic, topic.as_str())
                .map_err(|e| SinkError::Solace(anyhow!("failed to build topic destination: {}", e)))?;

            let msg = OutboundMessageBuilder::new()
                .delivery_mode(self.delivery_mode.into_solace())
                .destination(dest)
                .payload(payload.as_slice())
                .build()
                .map_err(|e| SinkError::Solace(anyhow!("failed to build Solace message: {}", e)))?;

            match self.delivery_mode {
                SolaceDeliveryMode::Direct => {
                    self.session
                        .publish(msg)
                        .map_err(|e: SessionError| SinkError::Solace(anyhow!("Solace publish error: {}", e)))?;
                }
                SolaceDeliveryMode::Persistent | SolaceDeliveryMode::NonPersistent => {
                    let ack_rx = self
                        .session
                        .publish_with_ack(msg)
                        .map_err(|e: SessionError| SinkError::Solace(anyhow!("Solace publish error: {}", e)))?;
                    let future = ack_rx.map(|r| match r {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => {
                            Err(SinkError::Solace(anyhow!("Solace broker rejected message: {}", e)))
                        }
                        Err(_) => Err(SinkError::Solace(anyhow!(
                            "Solace ACK receiver dropped before broker confirmed delivery"
                        ))),
                    });
                    add_future.add_future_may_await(future).await?;
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, ScalarRefImpl};

    use super::*;

    fn schema_varchar_int() -> Schema {
        Schema::new(vec![
            Field::with_name(DataType::Varchar, "region"),
            Field::with_name(DataType::Int32, "status_code"),
        ])
    }

    fn schema_one_varchar() -> Schema {
        Schema::new(vec![Field::with_name(DataType::Varchar, "flight_id")])
    }

    fn schema_two_varchar() -> Schema {
        Schema::new(vec![
            Field::with_name(DataType::Varchar, "col_a"),
            Field::with_name(DataType::Varchar, "col_b"),
        ])
    }

    // -- TopicTemplate tests --

    #[test]
    fn test_topic_template_parse_valid() {
        let schema = schema_varchar_int();
        let tmpl = TopicTemplate::parse(
            "acme/{region}/code/{status_code}/v1",
            &schema,
            NullTopicBehavior::Error,
        );
        assert!(tmpl.is_ok());
    }

    #[test]
    fn test_topic_template_parse_unknown_column() {
        let schema = schema_varchar_int();
        let err = TopicTemplate::parse("acme/{unknown}/v1", &schema, NullTopicBehavior::Error)
            .unwrap_err();
        assert!(
            format!("{err}").contains("not found in schema"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_topic_template_parse_unclosed_brace() {
        let schema = schema_varchar_int();
        let err = TopicTemplate::parse("acme/{region/v1", &schema, NullTopicBehavior::Error)
            .unwrap_err();
        assert!(
            format!("{err}").contains("unclosed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_topic_template_render() {
        let schema = schema_varchar_int();
        let tmpl = TopicTemplate::parse(
            "acme/{region}/code/{status_code}/v1",
            &schema,
            NullTopicBehavior::Error,
        )
        .unwrap();

        let chunk = DataChunk::from_pretty(
            "T i
             US 200",
        );
        let row = chunk.row_at(0).0;
        let topic = tmpl.render(row).unwrap().unwrap();
        assert_eq!(topic, "acme/US/code/200/v1");
    }

    #[test]
    fn test_topic_template_render_slash_rejected() {
        let schema = Schema::new(vec![Field::with_name(DataType::Varchar, "path")]);
        let tmpl =
            TopicTemplate::parse("root/{path}/leaf", &schema, NullTopicBehavior::Error).unwrap();

        let chunk = DataChunk::from_pretty(
            "T
             a/b",
        );
        let row = chunk.row_at(0).0;
        let err = tmpl.render(row).unwrap_err();
        assert!(
            format!("{err}").contains("contains '/'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_null_topic_behavior_error() {
        let schema = Schema::new(vec![Field::with_name(DataType::Varchar, "region")]);
        let tmpl =
            TopicTemplate::parse("acme/{region}/v1", &schema, NullTopicBehavior::Error).unwrap();

        // DataChunk with a NULL varchar (use . for null in from_pretty)
        let chunk = DataChunk::from_pretty(
            "T
             .",
        );
        let row = chunk.row_at(0).0;
        let result = tmpl.render(row);
        assert!(result.is_err(), "NullTopicBehavior::Error should return Err on NULL");
    }

    #[test]
    fn test_null_topic_behavior_skip() {
        let schema = Schema::new(vec![Field::with_name(DataType::Varchar, "region")]);
        let tmpl =
            TopicTemplate::parse("acme/{region}/v1", &schema, NullTopicBehavior::Skip).unwrap();

        let chunk = DataChunk::from_pretty(
            "T
             .",
        );
        let row = chunk.row_at(0).0;
        let result = tmpl.render(row).unwrap();
        assert!(result.is_none(), "NullTopicBehavior::Skip should return Ok(None) on NULL");
    }

    #[test]
    fn test_null_topic_behavior_replace() {
        let schema = Schema::new(vec![Field::with_name(DataType::Varchar, "region")]);
        let tmpl = TopicTemplate::parse(
            "acme/{region}/v1",
            &schema,
            NullTopicBehavior::Replace("unknown".to_owned()),
        )
        .unwrap();

        let chunk = DataChunk::from_pretty(
            "T
             .",
        );
        let row = chunk.row_at(0).0;
        let topic = tmpl.render(row).unwrap().unwrap();
        assert_eq!(topic, "acme/unknown/v1");
    }

    // -- parse_null_topic_behavior tests --

    #[test]
    fn test_parse_null_topic_behavior_error() {
        assert!(matches!(
            parse_null_topic_behavior("error"),
            Ok(NullTopicBehavior::Error)
        ));
    }

    #[test]
    fn test_parse_null_topic_behavior_skip() {
        assert!(matches!(
            parse_null_topic_behavior("skip"),
            Ok(NullTopicBehavior::Skip)
        ));
    }

    #[test]
    fn test_parse_null_topic_behavior_replace() {
        let result = parse_null_topic_behavior("replace:unknown-region").unwrap();
        assert!(matches!(result, NullTopicBehavior::Replace(v) if v == "unknown-region"));
    }

    #[test]
    fn test_parse_null_topic_behavior_invalid() {
        assert!(parse_null_topic_behavior("bogus").is_err());
    }

    // -- parse_duration tests --

    #[test]
    fn test_parse_duration_plain_seconds() {
        assert_eq!(parse_duration("300").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn test_parse_duration_seconds_suffix() {
        assert_eq!(parse_duration("300s").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("bogus").is_err());
        assert!(parse_duration("5x").is_err());
    }

    // -- DedupFilter tests (time-independent subset) --

    #[test]
    fn test_dedup_filter_unknown_column_rejected() {
        let schema = schema_one_varchar();
        let err =
            DedupFilter::new("nonexistent", Duration::from_secs(300), &schema).unwrap_err();
        assert!(
            format!("{err}").contains("not found in schema"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_dedup_filter_duplicate_column_rejected() {
        let schema = schema_one_varchar();
        let err = DedupFilter::new("flight_id,flight_id", Duration::from_secs(300), &schema)
            .unwrap_err();
        assert!(
            format!("{err}").contains("duplicate column"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_dedup_filter_different_keys_independent() {
        let schema = schema_one_varchar();
        let mut filter = DedupFilter::new("flight_id", Duration::from_secs(300), &schema).unwrap();

        let chunk_a = DataChunk::from_pretty(
            "T
             AAL100",
        );
        let chunk_b = DataChunk::from_pretty(
            "T
             UAL200",
        );

        let row_a = chunk_a.row_at(0).0;
        let row_b = chunk_b.row_at(0).0;

        // Both distinct keys should emit on first encounter.
        assert!(filter.should_emit(row_a));
        assert!(filter.should_emit(row_b));

        // Both should be suppressed on repeat within window.
        assert!(!filter.should_emit(row_a));
        assert!(!filter.should_emit(row_b));
    }

    #[test]
    fn test_dedup_filter_suppresses_repeat() {
        let schema = schema_one_varchar();
        let mut filter = DedupFilter::new("flight_id", Duration::from_secs(300), &schema).unwrap();

        let chunk = DataChunk::from_pretty(
            "T
             AAL100",
        );
        let row = chunk.row_at(0).0;

        assert!(filter.should_emit(row), "first emission should be allowed");
        assert!(!filter.should_emit(row), "second emission within window should be suppressed");
    }

    #[test]
    fn test_dedup_key_no_collision_with_pipe_in_value() {
        // Verify that rows which previously produced the same |-joined key now have distinct keys.
        // ("a|b", "c") and ("a", "b|c") must hash differently.
        let schema = schema_two_varchar();
        let mut filter =
            DedupFilter::new("col_a,col_b", Duration::from_secs(300), &schema).unwrap();

        let chunk_ab = DataChunk::from_pretty(
            "T T
             a|b c",
        );
        let chunk_a_bc = DataChunk::from_pretty(
            "T T
             a b|c",
        );

        let row_ab = chunk_ab.row_at(0).0;
        let row_a_bc = chunk_a_bc.row_at(0).0;

        // Both should emit — they are distinct rows.
        assert!(filter.should_emit(row_ab), "('a|b','c') should emit");
        assert!(
            filter.should_emit(row_a_bc),
            "('a','b|c') should also emit — must not collide with ('a|b','c')"
        );
    }

    // -- parse_delivery_mode tests --

    #[test]
    fn test_parse_delivery_mode_direct() {
        assert!(matches!(parse_delivery_mode("direct"), Ok(SolaceDeliveryMode::Direct)));
    }

    #[test]
    fn test_parse_delivery_mode_persistent() {
        assert!(matches!(
            parse_delivery_mode("persistent"),
            Ok(SolaceDeliveryMode::Persistent)
        ));
    }

    #[test]
    fn test_parse_delivery_mode_non_persistent() {
        assert!(matches!(
            parse_delivery_mode("non_persistent"),
            Ok(SolaceDeliveryMode::NonPersistent)
        ));
    }

    #[test]
    fn test_parse_delivery_mode_invalid() {
        assert!(parse_delivery_mode("bogus").is_err());
    }

    // -- parse_date_handling_mode tests --

    #[test]
    fn test_parse_date_handling_mode_from_ce() {
        assert!(matches!(
            parse_date_handling_mode("from_ce"),
            Ok(DateHandlingMode::FromCe)
        ));
    }

    #[test]
    fn test_parse_date_handling_mode_from_epoch() {
        assert!(matches!(
            parse_date_handling_mode("from_epoch"),
            Ok(DateHandlingMode::FromEpoch)
        ));
    }

    #[test]
    fn test_parse_date_handling_mode_string() {
        assert!(matches!(
            parse_date_handling_mode("string"),
            Ok(DateHandlingMode::String)
        ));
    }

    #[test]
    fn test_parse_date_handling_mode_invalid() {
        assert!(parse_date_handling_mode("unix").is_err());
    }

    // -- parse_timestamp_handling_mode tests --

    #[test]
    fn test_parse_timestamp_handling_mode_milli() {
        assert!(matches!(
            parse_timestamp_handling_mode("milli"),
            Ok(TimestampHandlingMode::Milli)
        ));
    }

    #[test]
    fn test_parse_timestamp_handling_mode_string() {
        assert!(matches!(
            parse_timestamp_handling_mode("string"),
            Ok(TimestampHandlingMode::String)
        ));
    }

    #[test]
    fn test_parse_timestamp_handling_mode_invalid() {
        assert!(parse_timestamp_handling_mode("micro").is_err());
    }

    #[test]
    fn test_dedup_filter_window_expires() {
        let schema = schema_one_varchar();
        let mut filter =
            DedupFilter::new("flight_id", Duration::from_millis(10), &schema).unwrap();

        let chunk = DataChunk::from_pretty(
            "T
             AAL100",
        );
        let row = chunk.row_at(0).0;

        assert!(filter.should_emit(row), "first emission allowed");
        assert!(!filter.should_emit(row), "repeat within window suppressed");

        // Two calls each separated by > half_window advance through both buckets.
        std::thread::sleep(Duration::from_millis(6));
        filter.should_emit(row); // first rotation: key moves to prev
        std::thread::sleep(Duration::from_millis(6));
        assert!(filter.should_emit(row), "key should re-emit after full window expires");
    }

    #[test]
    fn test_topic_template_pure_literal() {
        let schema = schema_varchar_int();
        let tmpl =
            TopicTemplate::parse("just/a/literal/topic", &schema, NullTopicBehavior::Error)
                .unwrap();

        let chunk = DataChunk::from_pretty(
            "T i
             US 200",
        );
        let row = chunk.row_at(0).0;
        let topic = tmpl.render(row).unwrap().unwrap();
        assert_eq!(topic, "just/a/literal/topic");
    }

    #[test]
    fn test_topic_template_consecutive_placeholders() {
        let schema = schema_two_varchar();
        let tmpl =
            TopicTemplate::parse("{col_a}{col_b}", &schema, NullTopicBehavior::Error).unwrap();

        let chunk = DataChunk::from_pretty(
            "T T
             hello world",
        );
        let row = chunk.row_at(0).0;
        let topic = tmpl.render(row).unwrap().unwrap();
        assert_eq!(topic, "helloworld");
    }

    #[test]
    fn test_scalar_to_topic_segment_numeric_and_bool() {
        assert_eq!(
            scalar_to_topic_segment(ScalarRefImpl::Int32(42)).unwrap(),
            "42"
        );
        assert_eq!(
            scalar_to_topic_segment(ScalarRefImpl::Int64(9_876_543_210)).unwrap(),
            "9876543210"
        );
        assert_eq!(
            scalar_to_topic_segment(ScalarRefImpl::Int16(7)).unwrap(),
            "7"
        );
        assert_eq!(
            scalar_to_topic_segment(ScalarRefImpl::Bool(true)).unwrap(),
            "true"
        );
        assert_eq!(
            scalar_to_topic_segment(ScalarRefImpl::Bool(false)).unwrap(),
            "false"
        );
    }

    #[test]
    fn test_dedup_filter_multi_column_key() {
        let schema = schema_two_varchar();
        let mut filter =
            DedupFilter::new("col_a,col_b", Duration::from_secs(300), &schema).unwrap();

        let chunk1 = DataChunk::from_pretty(
            "T T
             foo bar",
        );
        let chunk2 = DataChunk::from_pretty(
            "T T
             foo baz",
        );

        let row1 = chunk1.row_at(0).0;
        let row2 = chunk2.row_at(0).0;

        // (foo, bar) and (foo, baz) differ only in second column — both should emit.
        assert!(filter.should_emit(row1), "(foo,bar) should emit");
        assert!(
            filter.should_emit(row2),
            "(foo,baz) should emit — different second column"
        );

        // Both are suppressed on repeat within the window.
        assert!(!filter.should_emit(row1));
        assert!(!filter.should_emit(row2));
    }

    #[test]
    fn test_dedup_filter_null_column_in_key() {
        let schema = schema_two_varchar();
        let mut filter =
            DedupFilter::new("col_a,col_b", Duration::from_secs(300), &schema).unwrap();

        // NULL second column — serialized as JSON null, must not collide with "bar".
        let chunk_null = DataChunk::from_pretty(
            "T T
             foo .",
        );
        let chunk_value = DataChunk::from_pretty(
            "T T
             foo bar",
        );

        let row_null = chunk_null.row_at(0).0;
        let row_value = chunk_value.row_at(0).0;

        assert!(filter.should_emit(row_null), "(foo, NULL) should emit first time");
        assert!(
            filter.should_emit(row_value),
            "(foo, bar) should emit — not same key as (foo, NULL)"
        );

        // Each is suppressed on repeat.
        assert!(!filter.should_emit(row_null));
        assert!(!filter.should_emit(row_value));
    }

    #[test]
    fn test_solace_config_non_append_only_rejected() {
        let mut map = BTreeMap::new();
        map.insert("solace.url".to_owned(), "tcp://localhost:55555".to_owned());
        map.insert("solace.topic".to_owned(), "test/topic".to_owned());
        map.insert("type".to_owned(), "upsert".to_owned());

        let err = SolaceConfig::from_btreemap(map).unwrap_err();
        assert!(
            format!("{err}").contains("append-only"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_parse_null_topic_behavior_replace_empty() {
        // "replace:" with nothing after the colon → Replace("") is valid.
        let result = parse_null_topic_behavior("replace:").unwrap();
        assert!(matches!(result, NullTopicBehavior::Replace(v) if v.is_empty()));
    }

    #[test]
    fn test_parse_duration_zero() {
        assert_eq!(parse_duration("0").unwrap(), Duration::from_secs(0));
        assert_eq!(parse_duration("0s").unwrap(), Duration::from_secs(0));
    }
}
