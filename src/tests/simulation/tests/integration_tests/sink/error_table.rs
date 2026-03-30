// Copyright 2023 RisingWave Labs
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

use std::io::Write;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration};
use tempfile::NamedTempFile;
use tokio::time::sleep;

use crate::sink::utils::*;
use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

async fn start_error_table_test_cluster() -> Result<Cluster> {
    let config_path = {
        let mut file = NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(include_bytes!("../../../../../config/ci-sim.toml"))
            .expect("failed to write config file");
        file.write_all(b"\n[streaming.developer]\nmemory_controller_update_interval_ms = 600000\n")
            .expect("failed to write test config override");
        file.into_temp_path()
    };

    Cluster::start(Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 1,
        meta_nodes: 1,
        compactor_nodes: 1,
        compute_node_cores: 2,
        ..Default::default()
    })
    .await
}

#[tokio::test]
async fn test_sink_error_table() -> Result<()> {
    let mut cluster = start_error_table_test_cluster().await?;
    let test_sink = SimulationTestSink::register_new(TestSinkType::ReportError);
    let test_source = SimulationTestSource::register_new(2, 0..10, 1.0, 2);

    let mut session = cluster.start_session();
    session.run("set streaming_parallelism = 2").await?;
    session.run("set sink_decouple = false").await?;
    session.run(CREATE_SOURCE).await?;
    session
        .run(
            "create sink test_sink from test_source with (connector = 'test', type = 'upsert', skip_error = 'true')",
        )
        .await?;

    test_sink.wait_initial_parallelism(2).await?;
    test_sink
        .store
        .wait_for_count(test_source.id_list.len())
        .await?;
    session.run("flush").await?;

    let internal_tables = session.run("show internal tables").await?;
    let table_name_prefix = "public.__internal_test_sink_";
    let error_table_name = internal_tables
        .lines()
        .find(|line| {
            line.contains(table_name_prefix) && line.to_ascii_lowercase().contains("sinkerror")
        })
        .ok_or_else(|| anyhow::anyhow!("failed to find sink error internal table"))?
        .to_owned();

    let expected_count = test_source.id_list.len();
    let count_sql = format!("select count(*) from {}", error_table_name);
    let count_result = loop {
        let count_result = session.run(&count_sql).await?;
        if count_result.trim() == expected_count.to_string() {
            break count_result;
        }
        sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(count_result.trim(), expected_count.to_string());

    let rows = session
        .run(format!(
            "select sink_error_row_op, id, name from {} order by id",
            error_table_name
        ))
        .await?;
    let expected_rows = test_source
        .id_list
        .iter()
        .sorted()
        .map(|id| format!("1 {} {}", id, simple_name_of_id(*id)))
        .join("\n");
    assert_eq!(rows.trim(), expected_rows);

    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;
    assert_eq!(
        0,
        test_sink
            .parallelism_counter
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    Ok(())
}
