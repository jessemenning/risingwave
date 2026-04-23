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

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use anyhow::anyhow;
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
    #[serde(rename = "solace.dedup_key")]
    pub dedup_key: Option<String>,

    /// Duration during which repeat emissions for the same dedup key are suppressed.
    /// Accepts "Xs" (seconds), "Xm" (minutes), "Xh" (hours), or a plain integer
    /// number of seconds (e.g. "300", "5m", "1h"). Required when `solace.dedup_key` is set.
    #[serde(rename = "solace.dedup_window")]
    pub dedup_window: Option<String>,

    // accept "append-only"
    pub r#type: String,
}

fn default_delivery_mode() -> String {
    "direct".to_owned()
}

impl SolaceConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<SolaceConfig>(serde_json::to_value(values).unwrap())
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
}

impl TopicTemplate {
    fn parse(template: &str, schema: &Schema) -> Result<Self> {
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

        Ok(Self { segments })
    }

    fn render(&self, row: impl Row) -> Result<String> {
        let mut out = String::new();
        for seg in &self.segments {
            match seg {
                Segment::Literal(s) => out.push_str(s),
                Segment::Column { index } => {
                    let datum = row.datum_at(*index);
                    let scalar = datum.ok_or_else(|| {
                        SinkError::Solace(anyhow!(
                            "NULL value in topic template column index {}",
                            index
                        ))
                    })?;
                    let s = scalar_to_topic_segment(scalar)?;
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
        Ok(out)
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

// ---------------------------------------------------------------------------
// Dedup filter
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct DedupFilter {
    last_emitted: HashMap<String, Instant>,
    window: Duration,
    key_col_indices: Vec<usize>,
}

impl DedupFilter {
    fn new(dedup_key: &str, window: Duration, schema: &Schema) -> Result<Self> {
        let indices: Result<Vec<usize>> = dedup_key
            .split(',')
            .map(|col| {
                let col = col.trim();
                schema
                    .fields()
                    .iter()
                    .position(|f| f.name == col)
                    .ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "solace.dedup_key column '{}' not found in schema",
                            col
                        ))
                    })
            })
            .collect();
        Ok(Self {
            last_emitted: HashMap::new(),
            window,
            key_col_indices: indices?,
        })
    }

    fn build_key(&self, row: impl Row) -> String {
        self.key_col_indices
            .iter()
            .map(|&i| format!("{:?}", row.datum_at(i)))
            .collect::<Vec<_>>()
            .join("|")
    }

    fn should_emit(&mut self, row: impl Row) -> bool {
        let key = self.build_key(row);
        let now = Instant::now();

        // Prune entries that have fully expired to bound memory usage.
        self.last_emitted
            .retain(|_, ts| now.duration_since(*ts) < self.window);

        match self.last_emitted.get(&key) {
            Some(ts) if now.duration_since(*ts) < self.window => false,
            _ => {
                self.last_emitted.insert(key, now);
                true
            }
        }
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

fn build_json_encoder(schema: &Schema, format_desc: &SinkFormatDesc) -> Result<JsonEncoder> {
    let timestamptz_mode = TimestamptzHandlingMode::from_options(&format_desc.options)?;
    let jsonb_handling_mode = JsonbHandlingMode::from_options(&format_desc.options)?;
    match &format_desc.format {
        SinkFormat::AppendOnly => match &format_desc.encode {
            SinkEncode::Json => Ok(JsonEncoder::new(
                schema.clone(),
                None,
                DateHandlingMode::FromCe,
                TimestampHandlingMode::Milli,
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

        // Validate topic template column references.
        TopicTemplate::parse(&self.config.topic_template, &self.schema)?;

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
        build_json_encoder(&self.schema, &self.format_desc)?;

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
    // Context must be stored to keep the C-level context alive for the session's duration.
    _context: Context,
    session: solace_rs::async_support::AsyncSession,
    topic_template: TopicTemplate,
    delivery_mode: SolaceDeliveryMode,
    encoder: JsonEncoder,
    dedup_filter: Option<DedupFilter>,
}

impl SolaceSinkWriter {
    fn new(
        config: SolaceConfig,
        schema: Schema,
        format_desc: &SinkFormatDesc,
        name: &str,
    ) -> Result<Self> {
        let topic_template = TopicTemplate::parse(&config.topic_template, &schema)?;
        let delivery_mode = parse_delivery_mode(&config.delivery_mode)?;
        let encoder = build_json_encoder(&schema, format_desc)?;

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
        })
    }
}

impl AsyncTruncateSinkWriter for SolaceSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
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

            let topic = self
                .topic_template
                .render(row)
                .map_err(|e| SinkError::Solace(anyhow!(e)))?;

            let payload: Vec<u8> = self
                .encoder
                .encode(row)?
                .ser_to()
                .map_err(|e| SinkError::Solace(anyhow!(e)))?;

            let dest = MessageDestination::new(DestinationType::Topic, topic.as_str())
                .map_err(|e| SinkError::Solace(anyhow!("failed to build topic destination: {}", e)))?;

            let msg = OutboundMessageBuilder::new()
                .delivery_mode(self.delivery_mode.into_solace())
                .destination(dest)
                .payload(payload.as_slice())
                .build()
                .map_err(|e| SinkError::Solace(anyhow!("failed to build Solace message: {}", e)))?;

            self.session
                .publish(msg)
                .map_err(|e: SessionError| SinkError::Solace(anyhow!("Solace publish error: {}", e)))?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

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

    // -- TopicTemplate tests --

    #[test]
    fn test_topic_template_parse_valid() {
        let schema = schema_varchar_int();
        let tmpl = TopicTemplate::parse("acme/{region}/code/{status_code}/v1", &schema);
        assert!(tmpl.is_ok());
    }

    #[test]
    fn test_topic_template_parse_unknown_column() {
        let schema = schema_varchar_int();
        let err = TopicTemplate::parse("acme/{unknown}/v1", &schema).unwrap_err();
        assert!(
            format!("{err}").contains("not found in schema"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_topic_template_parse_unclosed_brace() {
        let schema = schema_varchar_int();
        let err = TopicTemplate::parse("acme/{region/v1", &schema).unwrap_err();
        assert!(
            format!("{err}").contains("unclosed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_topic_template_render() {
        let schema = schema_varchar_int();
        let tmpl = TopicTemplate::parse("acme/{region}/code/{status_code}/v1", &schema).unwrap();

        let chunk = DataChunk::from_pretty(
            "T i
             US 200",
        );
        let row = chunk.row_at(0).0;
        let topic = tmpl.render(row).unwrap();
        assert_eq!(topic, "acme/US/code/200/v1");
    }

    #[test]
    fn test_topic_template_render_slash_rejected() {
        let schema = Schema::new(vec![Field::with_name(DataType::Varchar, "path")]);
        let tmpl = TopicTemplate::parse("root/{path}/leaf", &schema).unwrap();

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
}
