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

//! Batch refresh job checkpoint control for periodic materialized views.
//!
//! A batch refresh job is a long-lived job that periodically catches up with upstream
//! via snapshot backfill, then idles until the next refresh interval.
//!
//! It lives permanently in `DatabaseCheckpointControl.batch_refresh_job_controls` for
//! its entire lifetime. It is NEVER placed in `creating_streaming_job_controls`.
//!
//! Lifecycle for first run (snapshot only):
//!   DDL → `ConsumingSnapshot` → snapshot done → `Stopping` → stop actors committed → `Resetting` → `Idle`
//!
//! Lifecycle for subsequent runs (logstore):
//!   `Idle` → re-render actors → `ConsumingLogStore` → all barriers committed → `Resetting` → `Idle`

mod barrier_control;

use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::mem::{replace, take};
use std::ops::Bound::{Excluded, Unbounded};

use barrier_control::BatchRefreshBarrierControl;
use itertools::Itertools;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::id::JobId;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_meta_model::{DispatcherType, WorkerId};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::PbBackfillType;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::{ActorId, FragmentId, PartialGraphId};
use risingwave_pb::stream_plan::barrier::PbBarrierKind;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{AddMutation, StartFragmentBackfillMutation, StopMutation};
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::{debug, info};

use super::super::state::RenderResult;
use super::creating_job::{CreatingJobInfo, CreatingStreamingJobBarrierStats};
use crate::MetaResult;
use crate::barrier::backfill_order_control::get_nodes_with_backfill_dependencies;
use crate::barrier::checkpoint::state::render_actors;
use crate::barrier::command::PostCollectCommand;
use crate::barrier::context::CreateSnapshotBackfillJobCommandInfo;
use crate::barrier::edge_builder::FragmentEdgeBuildResult;
use crate::barrier::info::{BarrierInfo, InflightStreamingJobInfo};
use crate::barrier::notifier::Notifier;
use crate::barrier::partial_graph::{
    CollectedBarrier, PartialGraphBarrierInfo, PartialGraphManager,
};
use crate::barrier::progress::{CreateMviewProgressTracker, TrackingJob, collect_done_fragments};
use crate::barrier::rpc::to_partial_graph_id;
use crate::barrier::{
    BackfillOrderState, BackfillProgress, BarrierKind, Command, FragmentBackfillProgress,
    TracedEpoch,
};
use crate::controller::fragment::InflightFragmentInfo;
use crate::controller::scale::NoShuffleEnsemble;
use crate::model::{
    FragmentDownstreamRelation, StreamJobActorsToCreate, StreamJobFragmentsToCreate,
};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::source_manager::SplitAssignment;
use crate::stream::{ExtendedFragmentBackfillOrder, build_actor_connector_splits};

// ── Public types ──────────────────────────────────────────────────────────────

/// Information about a batch refresh job stored at creation time.
#[derive(Debug, Clone)]
pub(crate) struct BatchRefreshJobInfo {
    /// The interval in seconds between refreshes.
    pub batch_refresh_seconds: u64,
    /// The upstream MV table IDs that this job subscribes to.
    pub upstream_table_ids: HashSet<TableId>,
}

/// Static context cached at DDL creation time for re-rendering actors during
/// subsequent logstore runs. Contains all information that does NOT change
/// between runs.
#[derive(Debug, Clone)]
pub(crate) struct BatchRefreshJobCachedContext {
    /// Fragment definitions (stream nodes, state tables, etc.).
    pub(crate) stream_job_fragments: StreamJobFragmentsToCreate,
    /// Streaming job model for parallelism settings.
    pub(crate) streaming_job_model: risingwave_meta_model::streaming_job::Model,
    /// MV definition string (for `StreamActor.mview_definition`).
    pub(crate) definition: String,
    /// Database resource group (for worker selection during rendering).
    pub(crate) database_resource_group: String,
    /// `NoShuffle` ensembles for this job's fragments.
    pub(crate) ensembles: Vec<NoShuffleEnsemble>,
    /// Backfill ordering for fragment dependencies.
    pub(crate) fragment_backfill_ordering: ExtendedFragmentBackfillOrder,
    /// Locality mapping for state table locality.
    pub(crate) locality_fragment_state_table_mapping:
        HashMap<FragmentId, HashMap<FragmentId, HashSet<TableId>>>,
}

// ── Status ────────────────────────────────────────────────────────────────────

#[derive(Debug)]
enum BatchRefreshJobStatus {
    /// The job is consuming upstream snapshot.
    ConsumingSnapshot {
        prev_epoch_fake_physical_time: u64,
        pending_upstream_barriers: Vec<BarrierInfo>,
        version_stats: HummockVersionStats,
        create_mview_tracker: CreateMviewProgressTracker,
        snapshot_backfill_actors: HashSet<ActorId>,
        snapshot_epoch: u64,
        info: CreatingJobInfo,
        pending_non_checkpoint_barriers: Vec<u64>,
    },
    /// Snapshot is finished. On next upstream epoch we inject the final checkpoint at
    /// `snapshot_epoch` plus a `StopMutation` barrier. After both are committed,
    /// we transition to `Idle` (via `Resetting`).
    Stopping {
        info: CreatingJobInfo,
        /// Set to `Some(barriers)` initially; consumed on the first `on_new_upstream_epoch` call.
        barriers_to_inject: Option<Vec<BarrierInfo>>,
        /// Fake physical time counter (continues from `ConsumingSnapshot`).
        prev_epoch_fake_physical_time: u64,
        pending_non_checkpoint_barriers: Vec<u64>,
        /// The `prev_epoch` of the stop barrier. When this epoch is committed we are done.
        stop_at_epoch: Option<u64>,
    },
    /// The job is idle, waiting for the next trigger. No partial graph or actors.
    Idle { last_committed_epoch: u64 },
    /// Logstore cycle: processing pre-resolved log epoch barriers.
    /// All barriers are injected at once; the last one carries `StopMutation`.
    /// When the stop barrier's epoch is committed, we transition to `Resetting` → `Idle`.
    ConsumingLogStore {
        info: CreatingJobInfo,
        /// The `prev_epoch` of the stop barrier. When this is committed we are done.
        stop_at_epoch: u64,
    },
    /// The partial graph is being reset.
    Resetting {
        notifiers: Vec<Notifier>,
        next: ResetNextState,
    },
    /// Temporary variant for `mem::replace` transitions.
    PlaceHolder,
}

#[derive(Debug)]
enum ResetNextState {
    /// After reset, transition to Idle at this committed epoch.
    TransitionToIdle { last_committed_epoch: u64 },
    /// After reset, the job is being dropped — remove from the map.
    Dropped,
}

// ── Complete type ─────────────────────────────────────────────────────────────

/// The type of completion for a batch refresh barrier.
pub(crate) enum BatchRefreshCompleteType {
    /// The first barrier for this job (includes creation info for hummock).
    First(CreateSnapshotBackfillJobCommandInfo),
    /// A normal intermediate barrier.
    Normal,
    /// The stop barrier has been committed after snapshot consumption.
    SnapshotDone,
    /// The stop barrier has been committed after logstore consumption.
    LogStoreDone,
}

// ── Main checkpoint control ───────────────────────────────────────────────────

/// Self-contained checkpoint control for a batch refresh MV.
///
/// Unlike `CreatingStreamingJobControl`, this struct handles the full lifecycle
/// (snapshot → idle → re-run → idle → ...) and is never placed in
/// `creating_streaming_job_controls`.
#[derive(Debug)]
pub(crate) struct BatchRefreshJobCheckpointControl {
    job_id: JobId,
    partial_graph_id: PartialGraphId,
    snapshot_backfill_upstream_tables: HashSet<TableId>,
    snapshot_epoch: u64,
    batch_refresh_info: BatchRefreshJobInfo,

    node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    state_table_ids: HashSet<TableId>,

    barrier_control: BatchRefreshBarrierControl,
    status: BatchRefreshJobStatus,

    /// Cached context for re-rendering actors in subsequent logstore runs.
    /// Populated at creation time, `None` only for recovered idle jobs
    /// (which will be populated on the first re-run trigger).
    cached_context: Option<BatchRefreshJobCachedContext>,
    /// Used exactly once to notify catalog/frontends that the initial create finished.
    tracking_job: Option<TrackingJob>,

    upstream_lag: LabelGuardedIntGauge,
}

// ── Status helper methods ─────────────────────────────────────────────────────

impl BatchRefreshJobStatus {
    fn new_fake_barrier(
        prev_epoch_fake_physical_time: &mut u64,
        pending_non_checkpoint_barriers: &mut Vec<u64>,
        kind: PbBarrierKind,
    ) -> BarrierInfo {
        let prev_epoch =
            TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
        *prev_epoch_fake_physical_time += 1;
        let curr_epoch =
            TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
        let kind = match kind {
            PbBarrierKind::Unspecified => unreachable!(),
            PbBarrierKind::Initial => {
                assert!(pending_non_checkpoint_barriers.is_empty());
                BarrierKind::Initial
            }
            PbBarrierKind::Barrier => {
                pending_non_checkpoint_barriers.push(prev_epoch.value().0);
                BarrierKind::Barrier
            }
            PbBarrierKind::Checkpoint => {
                pending_non_checkpoint_barriers.push(prev_epoch.value().0);
                BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers))
            }
        };
        BarrierInfo {
            prev_epoch,
            curr_epoch,
            kind,
        }
    }

    /// Returns `true` if snapshot finished and status transitioned to Stopping.
    fn update_progress(
        &mut self,
        create_mview_progress: impl IntoIterator<
            Item = &risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress,
        >,
    ) -> bool {
        match self {
            BatchRefreshJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                version_stats,
                prev_epoch_fake_physical_time,
                pending_non_checkpoint_barriers,
                snapshot_epoch,
                ..
            } => {
                for progress in create_mview_progress {
                    create_mview_tracker.apply_progress(progress, version_stats);
                }
                if create_mview_tracker.is_finished() {
                    // Generate the final checkpoint barrier at snapshot_epoch.
                    pending_non_checkpoint_barriers.push(*snapshot_epoch);
                    let prev_epoch = Epoch::from_physical_time(*prev_epoch_fake_physical_time);
                    let final_checkpoint = BarrierInfo {
                        curr_epoch: TracedEpoch::new(Epoch(*snapshot_epoch)),
                        prev_epoch: TracedEpoch::new(prev_epoch),
                        kind: BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers)),
                    };

                    let BatchRefreshJobStatus::ConsumingSnapshot {
                        info,
                        prev_epoch_fake_physical_time,
                        ..
                    } = replace(self, BatchRefreshJobStatus::PlaceHolder)
                    else {
                        unreachable!()
                    };

                    *self = BatchRefreshJobStatus::Stopping {
                        info,
                        barriers_to_inject: Some(vec![final_checkpoint]),
                        prev_epoch_fake_physical_time,
                        pending_non_checkpoint_barriers: vec![],
                        stop_at_epoch: None,
                    };
                    return true;
                }
                false
            }
            _ => false,
        }
    }

    fn on_new_upstream_epoch(
        &mut self,
        barrier_info: &BarrierInfo,
        mutation: Option<Mutation>,
    ) -> Vec<(BarrierInfo, Option<Mutation>)> {
        match self {
            BatchRefreshJobStatus::ConsumingSnapshot {
                pending_upstream_barriers,
                prev_epoch_fake_physical_time,
                pending_non_checkpoint_barriers,
                create_mview_tracker,
                ..
            } => {
                let mutation = mutation.or_else(|| {
                    let pending_backfill_nodes = create_mview_tracker
                        .take_pending_backfill_nodes()
                        .collect_vec();
                    if pending_backfill_nodes.is_empty() {
                        None
                    } else {
                        Some(Mutation::StartFragmentBackfill(
                            StartFragmentBackfillMutation {
                                fragment_ids: pending_backfill_nodes,
                            },
                        ))
                    }
                });
                pending_upstream_barriers.push(barrier_info.clone());
                vec![(
                    Self::new_fake_barrier(
                        prev_epoch_fake_physical_time,
                        pending_non_checkpoint_barriers,
                        match barrier_info.kind {
                            BarrierKind::Barrier => PbBarrierKind::Barrier,
                            BarrierKind::Checkpoint(_) => PbBarrierKind::Checkpoint,
                            BarrierKind::Initial => {
                                unreachable!("upstream new epoch should not be initial")
                            }
                        },
                    ),
                    mutation,
                )]
            }
            BatchRefreshJobStatus::Stopping {
                barriers_to_inject,
                info,
                prev_epoch_fake_physical_time,
                pending_non_checkpoint_barriers,
                stop_at_epoch,
            } => {
                if let Some(barriers) = barriers_to_inject.take() {
                    let mut result: Vec<_> = barriers.into_iter().map(|b| (b, None)).collect();

                    // Inject a StopMutation barrier after the final checkpoint.
                    let stop_actors: Vec<ActorId> = info
                        .fragment_infos
                        .values()
                        .flat_map(|f| f.actors.keys().copied())
                        .collect();

                    // The stop barrier must start from the final checkpoint's curr_epoch so the
                    // partial-graph barrier sequence remains contiguous.
                    let snapshot_epoch_val =
                        result.last().map(|(b, _)| b.curr_epoch.value().0).unwrap();
                    *prev_epoch_fake_physical_time = max(
                        *prev_epoch_fake_physical_time,
                        Epoch(snapshot_epoch_val).physical_time(),
                    );

                    let stop_barrier = Self::new_fake_barrier(
                        prev_epoch_fake_physical_time,
                        pending_non_checkpoint_barriers,
                        PbBarrierKind::Checkpoint,
                    );

                    *stop_at_epoch = Some(stop_barrier.prev_epoch());

                    result.push((
                        stop_barrier,
                        Some(Mutation::Stop(StopMutation {
                            actors: stop_actors,
                            dropped_sink_fragments: vec![],
                        })),
                    ));

                    result
                } else {
                    // Already injected stop barrier, no more barriers.
                    vec![]
                }
            }
            BatchRefreshJobStatus::Idle { .. }
            | BatchRefreshJobStatus::Resetting { .. }
            | BatchRefreshJobStatus::ConsumingLogStore { .. } => vec![],
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        }
    }

    fn fragment_infos(&self) -> Option<&HashMap<FragmentId, InflightFragmentInfo>> {
        match self {
            BatchRefreshJobStatus::ConsumingSnapshot { info, .. }
            | BatchRefreshJobStatus::Stopping { info, .. }
            | BatchRefreshJobStatus::ConsumingLogStore { info, .. } => Some(&info.fragment_infos),
            BatchRefreshJobStatus::Idle { .. } | BatchRefreshJobStatus::Resetting { .. } => None,
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        }
    }

    fn creating_job_info(&self) -> Option<&CreatingJobInfo> {
        match self {
            BatchRefreshJobStatus::ConsumingSnapshot { info, .. }
            | BatchRefreshJobStatus::Stopping { info, .. }
            | BatchRefreshJobStatus::ConsumingLogStore { info, .. } => Some(info),
            BatchRefreshJobStatus::Idle { .. } | BatchRefreshJobStatus::Resetting { .. } => None,
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        }
    }
}

// ── Construction ──────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    /// Create from DDL command. Starts in `ConsumingSnapshot`.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        create_info: CreateSnapshotBackfillJobCommandInfo,
        batch_refresh_info: BatchRefreshJobInfo,
        notifiers: Vec<Notifier>,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        snapshot_epoch: u64,
        version_stat: &HummockVersionStats,
        partial_graph_manager: &mut PartialGraphManager,
        edges: &mut FragmentEdgeBuildResult,
        split_assignment: &SplitAssignment,
        actors: &RenderResult,
        ensembles: Vec<NoShuffleEnsemble>,
    ) -> MetaResult<Self> {
        let info = create_info.info.clone();
        let job_id = info.stream_job_fragments.stream_job_id();
        let database_id = info.streaming_job.database_id();

        // Cache static context for re-rendering in subsequent logstore runs.
        let cached_context = BatchRefreshJobCachedContext {
            stream_job_fragments: info.stream_job_fragments.clone(),
            streaming_job_model: info.streaming_job_model.clone(),
            definition: info.definition.clone(),
            database_resource_group: info.database_resource_group.clone(),
            ensembles,
            fragment_backfill_ordering: info.fragment_backfill_ordering.clone(),
            locality_fragment_state_table_mapping: info
                .locality_fragment_state_table_mapping
                .iter()
                .map(|(frag_id, table_ids)| {
                    (
                        *frag_id,
                        std::iter::once((*frag_id, table_ids.iter().cloned().collect())).collect(),
                    )
                })
                .collect(),
        };

        debug!(
            %job_id,
            definition = info.definition,
            "new batch refresh job"
        );

        let fragment_infos: HashMap<FragmentId, InflightFragmentInfo> = info
            .stream_job_fragments
            .new_fragment_info(
                &actors.stream_actors,
                &actors.actor_location,
                split_assignment,
            )
            .collect();
        let snapshot_backfill_actors =
            InflightStreamingJobInfo::snapshot_backfill_actor_ids(&fragment_infos).collect();
        let backfill_nodes_to_pause =
            get_nodes_with_backfill_dependencies(&info.fragment_backfill_ordering)
                .into_iter()
                .collect();
        let backfill_order_state = BackfillOrderState::new(
            &info.fragment_backfill_ordering,
            &fragment_infos,
            info.locality_fragment_state_table_mapping.clone(),
        );
        let create_mview_tracker = CreateMviewProgressTracker::recover(
            job_id,
            &fragment_infos,
            backfill_order_state,
            version_stat,
        );

        let actors_to_create = Command::create_streaming_job_actors_to_create(
            &info,
            edges,
            &actors.stream_actors,
            &actors.actor_location,
        );

        let barrier_control = BatchRefreshBarrierControl::new(job_id, snapshot_epoch, None);

        let mut prev_epoch_fake_physical_time = 0;
        let mut pending_non_checkpoint_barriers = vec![];

        let initial_barrier_info = BatchRefreshJobStatus::new_fake_barrier(
            &mut prev_epoch_fake_physical_time,
            &mut pending_non_checkpoint_barriers,
            PbBarrierKind::Checkpoint,
        );

        let added_actors: Vec<ActorId> = actors
            .stream_actors
            .values()
            .flatten()
            .map(|actor| actor.actor_id)
            .collect();
        let actor_splits = split_assignment
            .values()
            .flat_map(build_actor_connector_splits)
            .collect();

        let initial_mutation = Mutation::Add(AddMutation {
            actor_dispatchers: Default::default(),
            added_actors,
            actor_splits,
            pause: false,
            subscriptions_to_add: Default::default(),
            backfill_nodes_to_pause,
            actor_cdc_table_snapshot_splits: None,
            new_upstream_sinks: Default::default(),
        });

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
        let state_table_ids =
            InflightFragmentInfo::existing_table_ids(fragment_infos.values()).collect();

        let partial_graph_id = to_partial_graph_id(database_id, Some(job_id));
        let tracking_job = TrackingJob::new(&info.stream_job_fragments);

        let job_info = CreatingJobInfo {
            fragment_infos,
            upstream_fragment_downstreams: Default::default(),
            downstreams: info.stream_job_fragments.downstreams,
            snapshot_backfill_upstream_tables: snapshot_backfill_upstream_tables.clone(),
            stream_actors: actors
                .stream_actors
                .values()
                .flatten()
                .map(|actor| (actor.actor_id, actor.clone()))
                .collect(),
        };

        let mut job = Self {
            partial_graph_id,
            job_id,
            snapshot_backfill_upstream_tables,
            snapshot_epoch,
            batch_refresh_info,
            barrier_control,
            status: BatchRefreshJobStatus::PlaceHolder,
            cached_context: Some(cached_context),
            tracking_job: Some(tracking_job),
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
            node_actors,
            state_table_ids,
        };

        let mut graph_adder = partial_graph_manager.add_partial_graph(
            partial_graph_id,
            CreatingStreamingJobBarrierStats::new(job_id, snapshot_epoch),
        );

        if let Err(e) = Self::inject_barrier(
            partial_graph_id,
            graph_adder.manager(),
            &mut job.barrier_control,
            &job.node_actors,
            &job.state_table_ids,
            initial_barrier_info,
            Some(actors_to_create),
            Some(initial_mutation),
            notifiers,
            Some(create_info),
        ) {
            graph_adder.failed();
            job.status = BatchRefreshJobStatus::Resetting {
                notifiers: vec![],
                next: ResetNextState::Dropped,
            };
            return Err(e);
        }

        graph_adder.added();
        assert!(pending_non_checkpoint_barriers.is_empty());
        job.status = BatchRefreshJobStatus::ConsumingSnapshot {
            prev_epoch_fake_physical_time,
            pending_upstream_barriers: vec![],
            version_stats: version_stat.clone(),
            create_mview_tracker,
            snapshot_backfill_actors,
            snapshot_epoch,
            info: job_info,
            pending_non_checkpoint_barriers,
        };

        Ok(job)
    }

    /// Recover from a persistent state during recovery.
    ///
    /// - If `committed_epoch >= snapshot_epoch` → Idle (snapshot completed before crash).
    /// - If `committed_epoch < snapshot_epoch` → `ConsumingSnapshot` (re-render actors, restart).
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn recover(
        database_id: DatabaseId,
        job_id: JobId,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        upstream_table_log_epochs: &HashMap<TableId, Vec<(Vec<u64>, u64)>>,
        snapshot_epoch: u64,
        committed_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
        fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
        backfill_order: ExtendedFragmentBackfillOrder,
        fragment_relations: &FragmentDownstreamRelation,
        version_stat: &HummockVersionStats,
        new_actors: StreamJobActorsToCreate,
        initial_mutation: Mutation,
        batch_refresh_info: BatchRefreshJobInfo,
        partial_graph_recoverer: &mut crate::barrier::partial_graph::PartialGraphRecoverer<'_>,
    ) -> MetaResult<Self> {
        if committed_epoch >= snapshot_epoch {
            // Snapshot completed; recover to Idle (no partial graph needed).
            info!(
                %job_id,
                committed_epoch,
                snapshot_epoch,
                "recovered batch refresh job to idle"
            );
            return Ok(Self {
                job_id,
                partial_graph_id: to_partial_graph_id(database_id, Some(job_id)),
                snapshot_backfill_upstream_tables: batch_refresh_info.upstream_table_ids.clone(),
                snapshot_epoch,
                batch_refresh_info,
                node_actors: Default::default(),
                state_table_ids: Default::default(),
                barrier_control: BatchRefreshBarrierControl::new(
                    job_id,
                    snapshot_epoch,
                    Some(committed_epoch),
                ),
                status: BatchRefreshJobStatus::Idle {
                    last_committed_epoch: committed_epoch,
                },
                cached_context: None,
                tracking_job: None,
                upstream_lag: GLOBAL_META_METRICS
                    .snapshot_backfill_lag
                    .with_guarded_label_values(&[&format!("{}", job_id)]),
            });
        }

        // Snapshot still in-progress; recover to ConsumingSnapshot.
        info!(
            %job_id,
            committed_epoch,
            snapshot_epoch,
            "recovered batch refresh job to consuming snapshot"
        );

        let barrier_control =
            BatchRefreshBarrierControl::new(job_id, snapshot_epoch, Some(committed_epoch));

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
        let state_table_ids: HashSet<_> =
            InflightFragmentInfo::existing_table_ids(fragment_infos.values()).collect();

        let downstreams = fragment_infos
            .keys()
            .filter_map(|fragment_id| {
                fragment_relations
                    .get(fragment_id)
                    .map(|relation| (*fragment_id, relation.clone()))
            })
            .collect();

        let info = CreatingJobInfo {
            fragment_infos,
            upstream_fragment_downstreams: Default::default(),
            downstreams,
            snapshot_backfill_upstream_tables: snapshot_backfill_upstream_tables.clone(),
            stream_actors: new_actors
                .values()
                .flat_map(|fragments| {
                    fragments.values().flat_map(|(_, actors, _)| {
                        actors
                            .iter()
                            .map(|(actor, _, _)| (actor.actor_id, actor.clone()))
                    })
                })
                .collect(),
        };

        let mut prev_epoch_fake_physical_time = Epoch(committed_epoch).physical_time();
        let mut pending_non_checkpoint_barriers = vec![];

        let locality_fragment_state_table_mapping =
            crate::barrier::rpc::build_locality_fragment_state_table_mapping(&info.fragment_infos);
        let backfill_order_state = BackfillOrderState::recover_from_fragment_infos(
            &backfill_order,
            &info.fragment_infos,
            locality_fragment_state_table_mapping,
        );

        let create_mview_tracker = CreateMviewProgressTracker::recover(
            job_id,
            &info.fragment_infos,
            backfill_order_state,
            version_stat,
        );

        let first_barrier_info = BatchRefreshJobStatus::new_fake_barrier(
            &mut prev_epoch_fake_physical_time,
            &mut pending_non_checkpoint_barriers,
            PbBarrierKind::Initial,
        );

        let upstream_log_epochs = Self::resolve_upstream_log_epochs(
            &snapshot_backfill_upstream_tables,
            upstream_table_log_epochs,
            snapshot_epoch,
            upstream_barrier_info,
        )?;

        let partial_graph_id = to_partial_graph_id(database_id, Some(job_id));
        let tracking_job = TrackingJob::recovered(job_id, &info.fragment_infos);

        partial_graph_recoverer.recover_graph(
            partial_graph_id,
            initial_mutation,
            &first_barrier_info,
            &node_actors,
            state_table_ids.iter().copied(),
            new_actors,
            CreatingStreamingJobBarrierStats::new(job_id, snapshot_epoch),
        )?;

        Ok(Self {
            job_id,
            partial_graph_id,
            snapshot_backfill_upstream_tables: batch_refresh_info.upstream_table_ids.clone(),
            snapshot_epoch,
            batch_refresh_info,
            node_actors,
            state_table_ids,
            barrier_control,
            status: BatchRefreshJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time,
                pending_upstream_barriers: upstream_log_epochs,
                version_stats: version_stat.clone(),
                create_mview_tracker,
                snapshot_backfill_actors: InflightStreamingJobInfo::snapshot_backfill_actor_ids(
                    &info.fragment_infos,
                )
                .collect(),
                info,
                snapshot_epoch,
                pending_non_checkpoint_barriers,
            },
            cached_context: None,
            tracking_job: Some(tracking_job),
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
        })
    }
}

// ── Barrier injection ─────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    fn inject_barrier(
        partial_graph_id: PartialGraphId,
        partial_graph_manager: &mut PartialGraphManager,
        barrier_control: &mut BatchRefreshBarrierControl,
        node_actors: &HashMap<WorkerId, HashSet<ActorId>>,
        state_table_ids: &HashSet<TableId>,
        barrier_info: BarrierInfo,
        new_actors: Option<StreamJobActorsToCreate>,
        mutation: Option<Mutation>,
        notifiers: Vec<Notifier>,
        first_create_info: Option<CreateSnapshotBackfillJobCommandInfo>,
    ) -> MetaResult<()> {
        let prev_epoch = barrier_info.prev_epoch();
        partial_graph_manager.inject_barrier(
            partial_graph_id,
            mutation,
            node_actors,
            state_table_ids.iter().copied(),
            node_actors.keys().copied(),
            new_actors,
            PartialGraphBarrierInfo::new(
                first_create_info.map_or_else(
                    PostCollectCommand::barrier,
                    CreateSnapshotBackfillJobCommandInfo::into_post_collect,
                ),
                barrier_info,
                notifiers,
                state_table_ids.clone(),
            ),
        )?;
        barrier_control.enqueue_epoch(prev_epoch);
        Ok(())
    }

    fn resolve_upstream_log_epochs(
        snapshot_backfill_upstream_tables: &HashSet<TableId>,
        upstream_table_log_epochs: &HashMap<TableId, Vec<(Vec<u64>, u64)>>,
        exclusive_start_log_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
    ) -> MetaResult<Vec<BarrierInfo>> {
        let table_id = snapshot_backfill_upstream_tables
            .iter()
            .next()
            .expect("snapshot backfill job should have upstream");

        // Flatten log entries into individual (epoch, is_checkpoint_start) pairs,
        // skipping everything up to and including `exclusive_start_log_epoch`.
        //
        // `exclusive_start_log_epoch` may be a non-checkpoint epoch that falls
        // *within* a log entry's `non_checkpoint_epochs`, not only at a
        // `checkpoint_epoch` boundary. We therefore iterate all epochs in each
        // entry until we pass the start epoch.
        let mut flattened: Vec<(u64, bool)> = Vec::new(); // (epoch, is_first_in_group)

        if let Some(epochs) = upstream_table_log_epochs.get(table_id) {
            let mut found_start = false;
            for (non_checkpoint_epochs, checkpoint_epoch) in epochs {
                // Fast-skip entire entries whose checkpoint is before the start.
                if !found_start && *checkpoint_epoch < exclusive_start_log_epoch {
                    continue;
                }

                for (i, epoch) in non_checkpoint_epochs
                    .iter()
                    .chain(std::iter::once(checkpoint_epoch))
                    .enumerate()
                {
                    if !found_start {
                        if *epoch <= exclusive_start_log_epoch {
                            if *epoch == exclusive_start_log_epoch {
                                found_start = true;
                            }
                            continue;
                        }
                        // epoch > exclusive_start_log_epoch but we haven't
                        // found the exact match. This can happen when the
                        // start epoch was truncated from the log. Treat
                        // everything from here onward as new.
                        found_start = true;
                    }
                    flattened.push((*epoch, i == 0));
                }
            }
        } else {
            // No change log entries found for the upstream table. This can
            // happen when the upstream's change log has been truncated or is
            // not yet available. In this case the refresh has nothing to
            // consume — return early with an empty barrier list.
            if upstream_barrier_info.prev_epoch() != exclusive_start_log_epoch {
                tracing::warn!(
                    upstream_prev_epoch = upstream_barrier_info.prev_epoch(),
                    exclusive_start_log_epoch,
                    "batch refresh: upstream advanced but change log not found; \
                     skipping this refresh cycle"
                );
                return Ok(vec![]);
            }
        };

        let mut ret = vec![];
        let mut prev_epoch = exclusive_start_log_epoch;
        let mut pending_non_checkpoint_barriers = vec![];
        for (epoch, is_checkpoint_start) in &flattened {
            assert!(
                *epoch > prev_epoch,
                "epoch {} should be > prev_epoch {}",
                epoch,
                prev_epoch
            );
            pending_non_checkpoint_barriers.push(prev_epoch);
            ret.push(BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                curr_epoch: TracedEpoch::new(Epoch(*epoch)),
                kind: if *is_checkpoint_start {
                    BarrierKind::Checkpoint(take(&mut pending_non_checkpoint_barriers))
                } else {
                    BarrierKind::Barrier
                },
            });
            prev_epoch = *epoch;
        }
        Ok(ret)
    }
}

// ── Barrier forwarding and collection ─────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    pub(crate) fn on_new_upstream_barrier(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        barrier_info: &BarrierInfo,
        mutation: Option<(Mutation, Vec<Notifier>)>,
    ) -> MetaResult<()> {
        if matches!(
            self.status,
            BatchRefreshJobStatus::Idle { .. }
                | BatchRefreshJobStatus::Resetting { .. }
                | BatchRefreshJobStatus::ConsumingLogStore { .. }
        ) {
            return Ok(());
        }

        let progress_epoch =
            if let Some(max_committed_epoch) = self.barrier_control.max_committed_epoch() {
                max(max_committed_epoch, self.snapshot_epoch)
            } else {
                self.snapshot_epoch
            };
        self.upstream_lag.set(
            barrier_info
                .prev_epoch
                .value()
                .0
                .saturating_sub(progress_epoch) as _,
        );
        let (mut mutation, mut notifiers) = match mutation {
            Some((mutation, notifiers)) => (Some(mutation), notifiers),
            None => (None, vec![]),
        };
        for (barrier_to_inject, mutation) in self
            .status
            .on_new_upstream_epoch(barrier_info, mutation.take())
        {
            Self::inject_barrier(
                self.partial_graph_id,
                partial_graph_manager,
                &mut self.barrier_control,
                &self.node_actors,
                &self.state_table_ids,
                barrier_to_inject,
                None,
                mutation,
                take(&mut notifiers),
                None,
            )?;
        }
        assert!(mutation.is_none(), "must have consumed mutation");
        assert!(notifiers.is_empty(), "must consumed notifiers");
        Ok(())
    }

    pub(crate) fn collect(&mut self, collected_barrier: CollectedBarrier<'_>) {
        self.barrier_control.collect(collected_barrier);
    }
}

// ── Completing ────────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    pub(crate) fn start_completing(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        min_upstream_inflight_epoch: Option<u64>,
        upstream_committed_epoch: u64,
    ) -> Option<(
        u64,
        Vec<BarrierCompleteResponse>,
        PartialGraphBarrierInfo,
        BatchRefreshCompleteType,
    )> {
        if upstream_committed_epoch < self.snapshot_epoch {
            return None;
        }

        let (stop_at_epoch, epoch_end_bound, is_logstore) = match &self.status {
            BatchRefreshJobStatus::Stopping { stop_at_epoch, .. } => {
                let epoch_end_bound = if let Some(stop_epoch) = stop_at_epoch {
                    min_upstream_inflight_epoch
                        .map(|upstream_epoch| {
                            if upstream_epoch < *stop_epoch {
                                Excluded(upstream_epoch)
                            } else {
                                Unbounded
                            }
                        })
                        .unwrap_or(Unbounded)
                } else {
                    min_upstream_inflight_epoch
                        .map(Excluded)
                        .unwrap_or(Unbounded)
                };
                (*stop_at_epoch, epoch_end_bound, false)
            }
            BatchRefreshJobStatus::ConsumingLogStore { stop_at_epoch, .. } => {
                (Some(*stop_at_epoch), Unbounded, true)
            }
            BatchRefreshJobStatus::ConsumingSnapshot { .. } => (
                None,
                min_upstream_inflight_epoch
                    .map(Excluded)
                    .unwrap_or(Unbounded),
                false,
            ),
            BatchRefreshJobStatus::Idle { .. } | BatchRefreshJobStatus::Resetting { .. } => {
                return None;
            }
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        };

        partial_graph_manager
            .start_completing(
                self.partial_graph_id,
                epoch_end_bound,
                |_non_checkpoint_epoch, _resps, _| {
                    // Progress already applied in `collect()`.
                },
            )
            .map(|(epoch, resps, info)| {
                // Update progress from responses.
                self.status
                    .update_progress(resps.values().flat_map(|resp| &resp.create_mview_progress));
                let resps: Vec<_> = resps.into_values().collect();

                let complete_type = if let Some(stop_epoch) = stop_at_epoch
                    && epoch == stop_epoch
                {
                    // The stop barrier has been completed. Ack it and signal done.
                    partial_graph_manager.ack_completed(self.partial_graph_id, epoch);
                    self.barrier_control.update_committed_epoch(epoch);
                    if is_logstore {
                        BatchRefreshCompleteType::LogStoreDone
                    } else {
                        BatchRefreshCompleteType::SnapshotDone
                    }
                } else {
                    BatchRefreshCompleteType::Normal
                };
                (epoch, resps, info, complete_type)
            })
    }

    pub(crate) fn ack_completed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        completed_epoch: u64,
    ) {
        partial_graph_manager.ack_completed(self.partial_graph_id, completed_epoch);
        self.barrier_control.update_committed_epoch(completed_epoch);
    }

    /// Request transition to idle by resetting the partial graph.
    ///
    /// Returns the fragment IDs from the pre-idle status (if any) so the caller
    /// can remove them from `shared_actor_infos`.
    pub(crate) fn start_idle_transition(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> Vec<FragmentId> {
        let last_committed_epoch = self
            .barrier_control
            .max_committed_epoch()
            .unwrap_or(self.snapshot_epoch);
        info!(
            job_id = %self.job_id,
            last_committed_epoch,
            "batch refresh job: resetting partial graph for idle transition"
        );

        // Extract fragment infos before replacing the status, so the caller
        // can clean them up from shared_actor_infos.
        let old_status = replace(&mut self.status, BatchRefreshJobStatus::PlaceHolder);
        let retired_fragment_ids: Vec<FragmentId> = match &old_status {
            BatchRefreshJobStatus::ConsumingSnapshot { info, .. }
            | BatchRefreshJobStatus::Stopping { info, .. }
            | BatchRefreshJobStatus::ConsumingLogStore { info, .. } => {
                info.fragment_infos.keys().copied().collect()
            }
            _ => vec![],
        };
        drop(old_status);

        partial_graph_manager.reset_partial_graphs([self.partial_graph_id]);
        self.status = BatchRefreshJobStatus::Resetting {
            notifiers: vec![],
            next: ResetNextState::TransitionToIdle {
                last_committed_epoch,
            },
        };

        retired_fragment_ids
    }

    /// Called when the partial graph reset is confirmed.
    ///
    /// Returns `true` if the job should remain in the map (transitioned to Idle).
    /// Returns `false` if the job was being dropped and should be removed.
    pub(crate) fn on_partial_graph_reset(&mut self) -> bool {
        match &mut self.status {
            BatchRefreshJobStatus::Resetting { notifiers, next } => {
                for notifier in notifiers.drain(..) {
                    notifier.notify_collected();
                }
                match next {
                    ResetNextState::TransitionToIdle {
                        last_committed_epoch,
                    } => {
                        let epoch = *last_committed_epoch;
                        info!(
                            job_id = %self.job_id,
                            last_committed_epoch = epoch,
                            "batch refresh job: transitioned to idle"
                        );
                        self.node_actors.clear();
                        self.state_table_ids.clear();
                        self.status = BatchRefreshJobStatus::Idle {
                            last_committed_epoch: epoch,
                        };
                        true
                    }
                    ResetNextState::Dropped => false,
                }
            }
            _ => {
                panic!(
                    "batch refresh job {}: on_partial_graph_reset in unexpected state {:?}",
                    self.job_id, self.status
                );
            }
        }
    }
}

// ── Logstore refresh run ──────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    /// Start a new logstore refresh run for an idle batch refresh job.
    ///
    /// This re-renders actors, resolves log epochs, creates a partial graph, and
    /// injects all barriers at once (the last carrying `StopMutation`).
    /// Transitions status from `Idle` to `ConsumingLogStore`.
    ///
    /// Returns the fragment infos for the new run (caller should update `shared_actor_infos`).
    pub(crate) fn start_refresh_run(
        &mut self,
        database_info: &crate::barrier::info::InflightDatabaseInfo,
        upstream_table_log_epochs: &HashMap<TableId, Vec<(Vec<u64>, u64)>>,
        target_upstream_epoch: u64,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
        actor_id_counter: &std::sync::atomic::AtomicU32,
        adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> MetaResult<()> {
        let BatchRefreshJobStatus::Idle {
            last_committed_epoch,
        } = &self.status
        else {
            panic!(
                "batch refresh job {}: start_refresh_run called in non-idle state {:?}",
                self.job_id, self.status
            );
        };
        let last_committed_epoch = *last_committed_epoch;

        let cached = self.cached_context.as_ref().expect(
            "cached_context must be populated before start_refresh_run; \
             for recovered idle jobs it should have been loaded first",
        );

        info!(
            job_id = %self.job_id,
            last_committed_epoch,
            target_upstream_epoch,
            "batch refresh job: starting logstore refresh run"
        );

        // Step 1: Resolve log epochs from last_committed_epoch to target_upstream_epoch.
        let target_barrier_info = BarrierInfo {
            prev_epoch: TracedEpoch::new(Epoch(target_upstream_epoch)),
            curr_epoch: TracedEpoch::new(Epoch(target_upstream_epoch)),
            kind: BarrierKind::Checkpoint(vec![]),
        };
        let log_epoch_barriers = Self::resolve_upstream_log_epochs(
            &self.snapshot_backfill_upstream_tables,
            upstream_table_log_epochs,
            last_committed_epoch,
            &target_barrier_info,
        )?;

        if log_epoch_barriers.is_empty() {
            info!(
                job_id = %self.job_id,
                "batch refresh job: no log epochs to consume, staying idle"
            );
            return Ok(());
        }

        // Step 2: Re-render actors.
        let actors = render_actors(
            &cached.stream_job_fragments,
            database_info,
            &cached.definition,
            &cached.stream_job_fragments.inner.ctx,
            &cached.streaming_job_model,
            actor_id_counter,
            worker_nodes,
            adaptive_parallelism_strategy,
            &cached.ensembles,
            &cached.database_resource_group,
        )?;

        // Step 3: Build fragment infos from rendered actors.
        let empty_split_assignment: SplitAssignment = HashMap::new();
        let fragment_infos: HashMap<FragmentId, InflightFragmentInfo> = cached
            .stream_job_fragments
            .inner
            .new_fragment_info(
                &actors.stream_actors,
                &actors.actor_location,
                &empty_split_assignment,
            )
            .collect();

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
        let state_table_ids: HashSet<TableId> =
            InflightFragmentInfo::existing_table_ids(fragment_infos.values()).collect();

        // Step 4: Build edges for the partial graph.
        // Batch refresh jobs don't connect to upstream — only internal edges matter.
        let mut edges = database_info.build_edge_for_batch_refresh_rerun(
            &cached.stream_job_fragments.downstreams,
            self.partial_graph_id,
            cached.stream_job_fragments.inner.fragments.values(),
            partial_graph_manager.control_stream_manager(),
            &actors.stream_actors,
            &actors.actor_location,
        );

        let downstreams = fragment_infos
            .keys()
            .filter_map(|fragment_id| {
                cached
                    .stream_job_fragments
                    .downstreams
                    .get(fragment_id)
                    .map(|relation| (*fragment_id, relation.clone()))
            })
            .collect();

        let creating_job_info = CreatingJobInfo {
            fragment_infos,
            upstream_fragment_downstreams: Default::default(),
            downstreams,
            snapshot_backfill_upstream_tables: self.snapshot_backfill_upstream_tables.clone(),
            stream_actors: actors
                .stream_actors
                .values()
                .flatten()
                .map(|actor| (actor.actor_id, actor.clone()))
                .collect(),
        };

        // Step 5: Build actors to create with edge info.
        let actors_to_create = edges.collect_actors_to_create(
            cached
                .stream_job_fragments
                .inner
                .fragments
                .values()
                .map(|fragment| {
                    let frag_actors = actors
                        .stream_actors
                        .get(&fragment.fragment_id)
                        .into_iter()
                        .flatten()
                        .map(|actor| (actor, actors.actor_location[&actor.actor_id]));
                    (
                        fragment.fragment_id,
                        &fragment.nodes,
                        frag_actors,
                        [], // no subscriber for batch refresh rerun
                    )
                }),
        );

        // Step 6: Create partial graph and inject barriers.
        let mut graph_adder = partial_graph_manager.add_partial_graph(
            self.partial_graph_id,
            CreatingStreamingJobBarrierStats::new(self.job_id, self.snapshot_epoch),
        );

        let added_actors: Vec<ActorId> = actors
            .stream_actors
            .values()
            .flatten()
            .map(|actor| actor.actor_id)
            .collect();

        let initial_mutation = Mutation::Add(AddMutation {
            actor_dispatchers: Default::default(),
            added_actors,
            actor_splits: Default::default(),
            pause: false,
            subscriptions_to_add: Default::default(),
            backfill_nodes_to_pause: vec![],
            actor_cdc_table_snapshot_splits: None,
            new_upstream_sinks: Default::default(),
        });

        // Reset barrier control for the new run.
        self.barrier_control = BatchRefreshBarrierControl::new(
            self.job_id,
            self.snapshot_epoch,
            Some(last_committed_epoch),
        );

        // Inject the first real log barrier with AddMutation so the barrier epochs remain
        // contiguous within the partial graph.
        let num_barriers = log_epoch_barriers.len();
        let mut barriers = log_epoch_barriers.into_iter();
        let first_barrier = barriers
            .next()
            .expect("log_epoch_barriers should be non-empty after early return");
        if let Err(e) = Self::inject_barrier(
            self.partial_graph_id,
            graph_adder.manager(),
            &mut self.barrier_control,
            &node_actors,
            &state_table_ids,
            first_barrier.clone(),
            Some(actors_to_create),
            Some(initial_mutation),
            vec![],
            None,
        ) {
            graph_adder.failed();
            self.status = BatchRefreshJobStatus::Resetting {
                notifiers: vec![],
                next: ResetNextState::Dropped,
            };
            return Err(e);
        }
        graph_adder.added();

        let mut last_real_barrier = first_barrier;
        for barrier_info in barriers {
            last_real_barrier = barrier_info.clone();
            Self::inject_barrier(
                self.partial_graph_id,
                partial_graph_manager,
                &mut self.barrier_control,
                &node_actors,
                &state_table_ids,
                barrier_info,
                None,
                None,
                vec![],
                None,
            )?;
        }

        // Inject a dedicated stop barrier after all real log barriers.
        let stop_actors: Vec<ActorId> = creating_job_info
            .fragment_infos
            .values()
            .flat_map(|f| f.actors.keys().copied())
            .collect();
        let mut prev_epoch_fake_physical_time =
            Epoch(last_real_barrier.curr_epoch.value().0).physical_time();
        let mut pending_non_checkpoint_barriers = vec![];
        let stop_barrier = BatchRefreshJobStatus::new_fake_barrier(
            &mut prev_epoch_fake_physical_time,
            &mut pending_non_checkpoint_barriers,
            PbBarrierKind::Checkpoint,
        );
        let stop_at_epoch = stop_barrier.prev_epoch();
        Self::inject_barrier(
            self.partial_graph_id,
            partial_graph_manager,
            &mut self.barrier_control,
            &node_actors,
            &state_table_ids,
            stop_barrier,
            None,
            Some(Mutation::Stop(StopMutation {
                actors: stop_actors,
                dropped_sink_fragments: vec![],
            })),
            vec![],
            None,
        )?;

        // Step 7: Transition to ConsumingLogStore.
        self.node_actors = node_actors;
        self.state_table_ids = state_table_ids;
        self.status = BatchRefreshJobStatus::ConsumingLogStore {
            info: creating_job_info,
            stop_at_epoch,
        };

        info!(
            job_id = %self.job_id,
            stop_at_epoch,
            num_barriers,
            "batch refresh job: entered ConsumingLogStore"
        );

        Ok(())
    }

    pub(crate) fn database_id(&self) -> DatabaseId {
        let (database_id, _) = crate::barrier::rpc::from_partial_graph_id(self.partial_graph_id);
        database_id
    }
}

// ── Query methods ─────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    pub(crate) fn is_valid_after_worker_err(&self, worker_id: WorkerId) -> bool {
        self.status
            .fragment_infos()
            .map(|fragment_infos| {
                !InflightFragmentInfo::contains_worker(fragment_infos.values(), worker_id)
            })
            .unwrap_or(true)
    }

    pub(crate) fn gen_backfill_progress(&self) -> BackfillProgress {
        let progress = match &self.status {
            BatchRefreshJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                ..
            } => {
                if create_mview_tracker.is_finished() {
                    "Snapshot finished".to_owned()
                } else {
                    let progress = create_mview_tracker.gen_backfill_progress();
                    format!("BatchRefresh Snapshot [{}]", progress)
                }
            }
            BatchRefreshJobStatus::Stopping { .. } => "BatchRefresh Stopping".to_owned(),
            BatchRefreshJobStatus::ConsumingLogStore { .. } => {
                "BatchRefresh ConsumingLogStore".to_owned()
            }
            BatchRefreshJobStatus::Idle { .. } => "BatchRefresh Idle".to_owned(),
            BatchRefreshJobStatus::Resetting { .. } => "BatchRefresh Resetting".to_owned(),
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        };
        BackfillProgress {
            progress,
            backfill_type: PbBackfillType::SnapshotBackfill,
        }
    }

    pub(crate) fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        match &self.status {
            BatchRefreshJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                info,
                ..
            } => create_mview_tracker.collect_fragment_progress(&info.fragment_infos, true),
            BatchRefreshJobStatus::Stopping { info, .. }
            | BatchRefreshJobStatus::ConsumingLogStore { info, .. } => {
                collect_done_fragments(self.job_id, &info.fragment_infos)
            }
            BatchRefreshJobStatus::Idle { .. } | BatchRefreshJobStatus::Resetting { .. } => vec![],
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        }
    }

    /// Returns the pinned upstream log epoch and upstream table IDs.
    pub(crate) fn pinned_upstream_log_epoch(&self) -> (u64, HashSet<TableId>) {
        let epoch = match &self.status {
            BatchRefreshJobStatus::Idle {
                last_committed_epoch,
            } => *last_committed_epoch,
            _ => max(
                self.barrier_control.max_committed_epoch().unwrap_or(0),
                self.snapshot_epoch,
            ),
        };
        (epoch, self.snapshot_backfill_upstream_tables.clone())
    }

    /// Check if this idle job should start a new refresh run based on epoch gap.
    pub(crate) fn should_start_refresh(&self, upstream_committed_epoch: u64) -> bool {
        if let BatchRefreshJobStatus::Idle {
            last_committed_epoch,
        } = &self.status
        {
            let upstream_time = Epoch(upstream_committed_epoch).physical_time();
            let job_time = Epoch(*last_committed_epoch).physical_time();
            let diff_ms = upstream_time.saturating_sub(job_time);
            diff_ms >= self.batch_refresh_info.batch_refresh_seconds * 1000
        } else {
            false
        }
    }

    pub fn state_table_ids(&self) -> &HashSet<TableId> {
        &self.state_table_ids
    }

    /// Returns `true` if the cached context for re-rendering is populated.
    pub(crate) fn has_cached_context(&self) -> bool {
        self.cached_context.is_some()
    }

    /// Set the cached context for re-rendering actors (used after recovery).
    pub(crate) fn set_cached_context(&mut self, ctx: BatchRefreshJobCachedContext) {
        self.cached_context = Some(ctx);
    }

    pub(crate) fn take_tracking_job(&mut self) -> Option<TrackingJob> {
        self.tracking_job.take()
    }

    pub(crate) fn fragment_infos(&self) -> Option<&HashMap<FragmentId, InflightFragmentInfo>> {
        self.status.fragment_infos()
    }

    pub(crate) fn is_snapshot_backfilling(&self) -> bool {
        matches!(
            self.status,
            BatchRefreshJobStatus::ConsumingSnapshot { .. }
                | BatchRefreshJobStatus::Stopping { .. }
        )
    }

    /// Clean up the job during database recovery. Returns `true` if the partial
    /// graph was already resetting (drop in progress).
    pub(crate) fn reset(self) -> bool {
        match self.status {
            BatchRefreshJobStatus::Resetting { .. } => true,
            _ => false,
        }
    }

    pub fn fragment_infos_with_job_id(
        &self,
    ) -> impl Iterator<Item = (&InflightFragmentInfo, JobId)> + '_ {
        self.status
            .fragment_infos()
            .into_iter()
            .flat_map(|fragments| fragments.values().map(|fragment| (fragment, self.job_id)))
    }

    pub(crate) fn collect_reschedule_blocked_fragment_ids(
        &self,
        blocked_fragment_ids: &mut HashSet<FragmentId>,
    ) {
        let Some(info) = self.status.creating_job_info() else {
            return;
        };
        for (fragment_id, fragment) in &info.fragment_infos {
            if fragment_has_online_unreschedulable_scan(fragment) {
                blocked_fragment_ids.insert(*fragment_id);
                collect_fragment_upstream_fragment_ids(fragment, blocked_fragment_ids);
            }
        }
    }

    pub(crate) fn collect_no_shuffle_fragment_relations(
        &self,
        no_shuffle_relations: &mut Vec<(FragmentId, FragmentId)>,
    ) {
        let Some(info) = self.status.creating_job_info() else {
            return;
        };
        for (upstream_fragment_id, downstreams) in &info.upstream_fragment_downstreams {
            no_shuffle_relations.extend(
                downstreams
                    .iter()
                    .filter(|d| d.dispatcher_type == DispatcherType::NoShuffle)
                    .map(|d| (*upstream_fragment_id, d.downstream_fragment_id)),
            );
        }
        for (fragment_id, downstreams) in &info.downstreams {
            no_shuffle_relations.extend(
                downstreams
                    .iter()
                    .filter(|d| d.dispatcher_type == DispatcherType::NoShuffle)
                    .map(|d| (*fragment_id, d.downstream_fragment_id)),
            );
        }
    }
}

// ── Drop handling ─────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    /// Drop this batch refresh job.
    pub(crate) fn drop(
        &mut self,
        notifiers: &mut Vec<Notifier>,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> bool {
        match &mut self.status {
            BatchRefreshJobStatus::Resetting {
                notifiers: existing_notifiers,
                next,
            } => {
                for notifier in &mut *notifiers {
                    notifier.notify_started();
                }
                existing_notifiers.append(notifiers);
                *next = ResetNextState::Dropped;
                true
            }
            BatchRefreshJobStatus::ConsumingSnapshot { .. }
            | BatchRefreshJobStatus::Stopping { .. }
            | BatchRefreshJobStatus::ConsumingLogStore { .. } => {
                for notifier in &mut *notifiers {
                    notifier.notify_started();
                }
                partial_graph_manager.reset_partial_graphs([self.partial_graph_id]);
                self.status = BatchRefreshJobStatus::Resetting {
                    notifiers: take(notifiers),
                    next: ResetNextState::Dropped,
                };
                true
            }
            BatchRefreshJobStatus::Idle { .. } => {
                // No partial graph to reset; notify and allow removal.
                for mut notifier in notifiers.drain(..) {
                    notifier.notify_started();
                    notifier.notify_collected();
                }
                true
            }
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn fragment_has_online_unreschedulable_scan(fragment: &InflightFragmentInfo) -> bool {
    let mut has_unreschedulable_scan = false;
    visit_stream_node_cont(&fragment.nodes, |node| {
        if let Some(NodeBody::StreamScan(stream_scan)) = node.node_body.as_ref() {
            let scan_type = stream_scan.stream_scan_type();
            if !scan_type.is_reschedulable(true) {
                has_unreschedulable_scan = true;
                return false;
            }
        }
        true
    });
    has_unreschedulable_scan
}

fn collect_fragment_upstream_fragment_ids(
    fragment: &InflightFragmentInfo,
    upstream_fragment_ids: &mut HashSet<FragmentId>,
) {
    visit_stream_node_cont(&fragment.nodes, |node| {
        if let Some(NodeBody::Merge(merge)) = node.node_body.as_ref() {
            upstream_fragment_ids.insert(merge.upstream_fragment_id);
        }
        true
    });
}
