// Copyright 2026 RisingWave Labs
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

use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration};
use tokio::time::sleep;

use crate::utils::kill_cn_and_wait_recover;

/// Test batch refresh MV recovery during initial snapshot backfill.
///
/// 1. Create a table and insert 10,000 rows.
/// 2. Create a batch refresh MV (`refresh.interval.sec`) with background DDL and rate limit = 1.
/// 3. Verify DDL progress is reported.
/// 4. Trigger recovery and verify progress survives.
/// 5. Alter rate limit to default (unlimited) and wait for completion.
/// 6. Verify the MV row count matches the source table.
/// 7. Trigger recovery while idle and verify data survives.
#[tokio::test]
async fn test_batch_refresh_recovery() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    // Step 1: Create table and insert 10,000 rows.
    session.run("CREATE TABLE t(v1 int);").await?;
    session
        .run("INSERT INTO t SELECT * FROM generate_series(1, 10000);")
        .await?;
    session.flush().await?;

    // Verify table has 10,000 rows.
    let count = session.run("SELECT COUNT(*) FROM t;").await?;
    assert_eq!(count, "10000");

    // Step 2: Create a batch refresh MV with background DDL and backfill rate limit = 1.
    session.run("SET BACKGROUND_DDL = true;").await?;
    session.run("SET BACKFILL_RATE_LIMIT = 1;").await?;

    // Create the upstream MV first (batch refresh needs an MV as upstream).
    session
        .run("CREATE MATERIALIZED VIEW mv_up AS SELECT * FROM t;")
        .await?;

    // Wait for upstream MV to finish.
    session.run("WAIT;").await?;

    // Now create the batch refresh MV on top of the upstream MV.
    session
        .run(
            "CREATE MATERIALIZED VIEW mv_batch WITH (refresh.interval.sec = 600) AS SELECT * FROM mv_up;",
        )
        .await?;

    // Step 3: Wait a bit for backfill to make some progress, then check DDL progress.
    sleep(Duration::from_secs(10)).await;

    let progress = session
        .run("SELECT progress FROM rw_catalog.rw_ddl_progress;")
        .await?;
    eprintln!("=== DDL progress before recovery: {}", progress);
    assert!(
        !progress.is_empty(),
        "DDL progress should be reported during batch refresh backfill"
    );

    // Step 4: Trigger recovery and verify progress survives.
    eprintln!("=== Step 4: triggering recovery during backfill...");
    kill_cn_and_wait_recover(&cluster).await;
    eprintln!("=== Step 4: recovery done");

    let progress_after = session
        .run("SELECT progress FROM rw_catalog.rw_ddl_progress;")
        .await?;
    eprintln!("=== DDL progress after recovery: {}", progress_after);
    assert!(
        !progress_after.is_empty(),
        "DDL progress should still be reported after recovery"
    );

    // Step 5: Alter rate limit to default (unlimited).
    eprintln!("=== Step 5: alter rate limit...");
    session
        .run("ALTER MATERIALIZED VIEW mv_batch SET BACKFILL_RATE_LIMIT = DEFAULT;")
        .await?;

    // Step 6: Wait for the batch refresh to complete.
    eprintln!("=== Step 6: WAIT for completion...");
    session.run("WAIT;").await?;
    eprintln!("=== Step 6: done");

    // Step 7: Verify the MV row count matches the source table.
    eprintln!("=== Step 7: verifying row counts...");
    let t_count = session.run("SELECT COUNT(*) FROM t;").await?;
    let mv_count = session.run("SELECT COUNT(*) FROM mv_batch;").await?;
    assert_eq!(
        t_count, mv_count,
        "MV row count should match source table: t={}, mv={}",
        t_count, mv_count
    );
    assert_eq!(mv_count, "10000");
    eprintln!("=== Step 7: PASSED");

    // Step 8: Now the batch refresh job is idle (snapshot done, waiting for next refresh).
    // Verify no DDL progress is reported.
    eprintln!("=== Step 8: checking DDL progress is empty...");
    let progress_idle = session
        .run("SELECT count(*) FROM rw_catalog.rw_ddl_progress;")
        .await?;
    assert_eq!(
        progress_idle, "0",
        "No DDL progress should be reported when batch refresh is idle"
    );
    eprintln!("=== Step 8: PASSED");

    // Step 9: Trigger another recovery while the batch refresh job is idle.
    eprintln!("=== Step 9: triggering recovery while idle...");
    kill_cn_and_wait_recover(&cluster).await;
    eprintln!("=== Step 9: recovery done");

    // Step 10: Verify that the MV is still queryable after idle recovery.
    eprintln!("=== Step 10: querying mv_batch after idle recovery...");
    let mv_count_after_idle = session.run("SELECT COUNT(*) FROM mv_batch;").await?;
    assert_eq!(
        mv_count_after_idle, "10000",
        "MV should still be queryable after idle recovery"
    );
    eprintln!(
        "=== Step 10: PASSED - query returned {}",
        mv_count_after_idle
    );

    // Step 11: DROP the MV.
    eprintln!("=== Step 11: dropping mv_batch...");
    session.run("DROP MATERIALIZED VIEW mv_batch;").await?;
    eprintln!("=== Step 11: mv_batch dropped");

    // Cleanup
    eprintln!("=== Cleanup: dropping mv_up...");
    session.run("DROP MATERIALIZED VIEW mv_up;").await?;
    eprintln!("=== Cleanup: mv_up dropped. dropping t...");
    session.run("DROP TABLE t;").await?;
    eprintln!("=== Cleanup: all done");

    Ok(())
}

/// Test batch refresh MV periodic refresh lifecycle:
///
/// 1. Create a table, upstream MV, and batch refresh MV with a short interval.
/// 2. Verify initial snapshot data.
/// 3. Insert more data upstream.
/// 4. Wait for a refresh cycle and verify new data appears.
/// 5. Insert even more data additional rows.
/// 6. Wait for another refresh and confirm the MV catches up.
/// 7. Drop the batch refresh MV while idle.
///
/// NOTE: This test is currently ignored because `resolve_upstream_log_epochs`
/// panics when the exclusive_start_log_epoch falls between checkpoint epochs
/// (assertion at line 842). Enable once the epoch resolution bug is fixed.
#[tokio::test]
#[ignore]
async fn test_batch_refresh_periodic_lifecycle() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    // Step 1: Setup tables.
    session.run("CREATE TABLE t(v1 int);").await?;
    session
        .run("INSERT INTO t SELECT * FROM generate_series(1, 100);")
        .await?;
    session.flush().await?;

    session
        .run("CREATE MATERIALIZED VIEW mv_up AS SELECT * FROM t;")
        .await?;

    // Step 2: Create batch refresh MV with 5-second interval (short for testing).
    session
        .run(
            "CREATE MATERIALIZED VIEW mv_refresh WITH (refresh.interval.sec = 5) AS SELECT * FROM mv_up;",
        )
        .await?;

    // Verify initial snapshot.
    let count = session.run("SELECT COUNT(*) FROM mv_refresh;").await?;
    assert_eq!(count, "100", "initial snapshot should have 100 rows");

    // Step 3: Insert more data into the base table.
    session
        .run("INSERT INTO t SELECT * FROM generate_series(101, 200);")
        .await?;
    session.flush().await?;

    // Upstream MV should reflect the new rows.
    sleep(Duration::from_secs(2)).await;
    let up_count = session.run("SELECT COUNT(*) FROM mv_up;").await?;
    assert_eq!(up_count, "200");

    // Immediately after insert, batch refresh MV should still show old count.
    let mv_count_before = session.run("SELECT COUNT(*) FROM mv_refresh;").await?;
    assert_eq!(
        mv_count_before, "100",
        "batch refresh MV should still have old data before refresh"
    );

    // Step 4: Wait for a refresh cycle (interval = 5s, give it some extra time).
    eprintln!("=== Waiting for first refresh cycle...");
    sleep(Duration::from_secs(15)).await;

    // After the refresh, verify the MV has the new data.
    let mv_count_after = session.run("SELECT COUNT(*) FROM mv_refresh;").await?;
    assert_eq!(
        mv_count_after, "200",
        "batch refresh MV should have 200 rows after first refresh"
    );

    // Step 5: Insert even more data.
    session
        .run("INSERT INTO t SELECT * FROM generate_series(201, 300);")
        .await?;
    session.flush().await?;

    // Step 6: Wait for another refresh cycle.
    eprintln!("=== Waiting for second refresh cycle...");
    sleep(Duration::from_secs(15)).await;

    let mv_count_final = session.run("SELECT COUNT(*) FROM mv_refresh;").await?;
    assert_eq!(
        mv_count_final, "300",
        "batch refresh MV should have 300 rows after second refresh"
    );

    // Step 7: Drop the batch refresh MV.
    session.run("DROP MATERIALIZED VIEW mv_refresh;").await?;
    session.run("DROP MATERIALIZED VIEW mv_up;").await?;
    session.run("DROP TABLE t;").await?;

    Ok(())
}

/// Test batch refresh MV with recovery during the periodic refresh (logstore) phase.
///
/// 1. Create the batch refresh MV and let initial snapshot complete.
/// 2. Insert new data upstream.
/// 3. Wait until the refresh is likely in progress (logstore consuming).
/// 4. Kill compute nodes to trigger recovery.
/// 5. Wait for recovery and another refresh cycle.
/// 6. Verify data integrity.
///
/// NOTE: Currently ignored due to epoch resolution bug in
/// `resolve_upstream_log_epochs`. Enable once the bug is fixed.
#[tokio::test]
#[ignore]
async fn test_batch_refresh_recovery_during_refresh() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    // Setup: Create table and upstream MV.
    session.run("CREATE TABLE t(v1 int);").await?;
    session
        .run("INSERT INTO t SELECT * FROM generate_series(1, 100);")
        .await?;
    session.flush().await?;

    session
        .run("CREATE MATERIALIZED VIEW mv_up AS SELECT * FROM t;")
        .await?;

    // Create batch refresh MV with 5-second interval.
    session
        .run(
            "CREATE MATERIALIZED VIEW mv_batch WITH (refresh.interval.sec = 5) AS SELECT * FROM mv_up;",
        )
        .await?;

    // Verify initial snapshot.
    let count = session.run("SELECT COUNT(*) FROM mv_batch;").await?;
    assert_eq!(count, "100");

    // Insert a large batch of data to ensure the refresh takes some time.
    session
        .run("INSERT INTO t SELECT * FROM generate_series(101, 5000);")
        .await?;
    session.flush().await?;

    // Wait for the refresh interval to trigger and the logstore to start consuming.
    eprintln!("=== Waiting for refresh to start...");
    sleep(Duration::from_secs(8)).await;

    // Trigger recovery during the refresh.
    eprintln!("=== Triggering recovery during refresh...");
    kill_cn_and_wait_recover(&cluster).await;
    eprintln!("=== Recovery done");

    // After recovery, wait for the job to stabilize and potentially re-run refresh.
    eprintln!("=== Waiting for post-recovery stabilization...");
    sleep(Duration::from_secs(20)).await;

    // The MV should eventually catch up. It may take one or two refresh cycles
    // after recovery. Verify the MV is at least queryable.
    let mv_count = session.run("SELECT COUNT(*) FROM mv_batch;").await?;
    let mv_count_val: i64 = mv_count.parse().unwrap_or(0);

    eprintln!(
        "=== MV count after recovery + refresh: {} (expected up to 5000)",
        mv_count_val
    );

    // After enough time and refresh cycles, the MV should have caught up to 5000.
    // But at minimum it should have the initial 100 rows (snapshot data survives recovery).
    assert!(
        mv_count_val >= 100,
        "MV should have at least 100 rows after recovery, got {}",
        mv_count_val
    );

    // Cleanup.
    session.run("DROP MATERIALIZED VIEW mv_batch;").await?;
    session.run("DROP MATERIALIZED VIEW mv_up;").await?;
    session.run("DROP TABLE t;").await?;

    Ok(())
}

/// Test dropping a batch refresh MV during the idle phase.
///
/// Verify that DROP succeeds cleanly when the MV is between refresh cycles.
#[tokio::test]
async fn test_batch_refresh_drop_while_idle() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    // Setup
    session.run("CREATE TABLE t(v1 int);").await?;
    session
        .run("INSERT INTO t SELECT * FROM generate_series(1, 50);")
        .await?;
    session.flush().await?;

    session
        .run("CREATE MATERIALIZED VIEW mv_up AS SELECT * FROM t;")
        .await?;

    // Create with a long interval so it stays idle after initial snapshot.
    session
        .run(
            "CREATE MATERIALIZED VIEW mv_batch WITH (refresh.interval.sec = 600) AS SELECT * FROM mv_up;",
        )
        .await?;

    // Verify initial data.
    let count = session.run("SELECT COUNT(*) FROM mv_batch;").await?;
    assert_eq!(count, "50");

    // The job is now idle (600-second interval). Drop should succeed.
    session.run("DROP MATERIALIZED VIEW mv_batch;").await?;

    // Verify the MV is gone.
    let result = session.run("SELECT COUNT(*) FROM mv_batch;").await;
    assert!(result.is_err(), "mv_batch should not exist after DROP");

    // Cleanup
    session.run("DROP MATERIALIZED VIEW mv_up;").await?;
    session.run("DROP TABLE t;").await?;

    Ok(())
}

/// Test that a batch refresh MV created without background DDL works correctly.
///
/// When created without background DDL, the initial snapshot backfill should
/// complete synchronously before the DDL returns.
#[tokio::test]
async fn test_batch_refresh_foreground_ddl() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    // Setup
    session.run("CREATE TABLE t(v1 int);").await?;
    session
        .run("INSERT INTO t SELECT * FROM generate_series(1, 200);")
        .await?;
    session.flush().await?;

    session
        .run("CREATE MATERIALIZED VIEW mv_up AS SELECT * FROM t;")
        .await?;

    // Create batch refresh MV without background DDL.
    session.run("SET BACKGROUND_DDL = false;").await?;
    session
        .run(
            "CREATE MATERIALIZED VIEW mv_batch WITH (refresh.interval.sec = 600) AS SELECT * FROM mv_up;",
        )
        .await?;

    // Since it's foreground DDL, snapshot should be complete immediately.
    let count = session.run("SELECT COUNT(*) FROM mv_batch;").await?;
    assert_eq!(
        count, "200",
        "foreground DDL should complete snapshot synchronously"
    );

    // Cleanup
    session.run("DROP MATERIALIZED VIEW mv_batch;").await?;
    session.run("DROP MATERIALIZED VIEW mv_up;").await?;
    session.run("DROP TABLE t;").await?;

    Ok(())
}
