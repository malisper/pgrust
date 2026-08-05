//! Runtime stats/trace counters (M0 deliverable 6). All relaxed atomics,
//! instance-owned (no statics — loom rule), snapshot-readable for tests and
//! the PGRUST_RUNTIME_TRACE=1 eprintln trace.

use crate::sync::atomic::{AtomicU64, Ordering};

macro_rules! counters {
    ($struct_name:ident, $snap_name:ident; $($(#[$doc:meta])* $field:ident),+ $(,)?) => {
        #[derive(Default)]
        pub struct $struct_name {
            $($(#[$doc])* pub $field: AtomicU64,)+
        }

        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
        pub struct $snap_name {
            $(pub $field: u64,)+
        }

        impl $struct_name {
            pub fn snapshot(&self) -> $snap_name {
                $snap_name {
                    $($field: self.$field.load(Ordering::Relaxed),)+
                }
            }
        }
    };
}

counters!(RuntimeStats, RuntimeStatsSnapshot;
    /// Resource groups submitted.
    rgs_submitted,
    /// Resource groups completed (any outcome).
    rgs_completed,
    /// Resource groups completed aborted.
    rgs_aborted,
    /// Task sets published into a slot.
    tasksets_published,
    /// Task-set slot invalidations (coordinator elections won).
    tasksets_invalidated,
    /// Finalization events (last-worker-out finalize ran).
    finalize_events,
    /// Finalization-marker swaps installed by coordinators.
    finalize_marks,
    /// Times a finalization counter was observed transiently negative.
    finalize_negative_observed,
    /// Tasks claimed (worker began a task on a task set).
    tasks_claimed,
    /// Tasks completed (budget spent or set exhausted).
    tasks_completed,
    /// Morsels claimed from shared cursors.
    morsels_claimed,
    /// Granules executed.
    granules_executed,
    /// Sizing decisions by rule.
    sizing_ramp,
    sizing_default,
    sizing_shutdown,
    /// Tasks refused by a dead generation (aborted-query cleanup path).
    generation_refusals,
    /// Idle worker parks.
    worker_parks,
    /// Aborted still-QUEUED RGs reaped from the wait queue at abort time
    /// (M5-4 slot-reclamation fix) instead of waiting for a slot to free.
    queued_aborts_reaped,
    /// Stride picks resolved by the session-affinity equal-pass tiebreak
    /// (M5-4: the pick preferred a slot whose leader session the worker is
    /// sticky-bound to over an equal-pass lower-index slot).
    affinity_tiebreaks,
    /// M5-5 decaying priorities: priority updates applied (one per RG per
    /// crossed decay quantum until the RG reaches the p_min floor).
    priority_decays,
    /// POOL-QOS (GL-POOLDB-1 mitigation): interactive demand published
    /// (unmet-width units added at fresh bound publishes).
    qos_demand_published,
    /// POOL-QOS: demoted serves that yielded their thread at a morsel
    /// boundary toward waiting interactive width.
    qos_yields,
    /// POOL-QOS: priority-lane permit acquires by interactive drives.
    qos_priority_acquires,
    /// POOL-QOS: demoted holders that released/re-acquired their permit at
    /// a step boundary because priority waiters were parked.
    qos_permit_defers,
    /// POOL-QOS memory governor (GL-CONCMEM-1): step boundaries where the
    /// QoS moves (yield/defer) were HELD because the process was over the
    /// memory bar — the serve ran on toward completion instead of
    /// interleaving.
    qos_mem_holds,
    /// M5+1 pipeline-DAG dispatch: dependency-satisfied pipelines published
    /// into ADDITIONAL slots (beyond the RG's first/reused slot) — the
    /// independent-subtree overlap the increment buys.
    dag_fanout_publishes,
    /// M5+1: ready pipelines left unpublished for lack of a free slot (they
    /// re-publish when one of the query's own pipelines finishes — the RG
    /// always retains a slot while work remains, so nothing strands).
    dag_ready_deferred,
    /// M5+1: stride picks resolved by the within-query dependency-depth
    /// refinement (equal pass, same query — deeper pipeline preferred).
    dag_depth_picks,
    /// M2 inc-2 bound engagements served by pool workers (one per
    /// bind→drive→unbind span dispatched through a BoundDescriptor).
    bound_serves,
    /// M2 inc-2 bound-slot skips recorded (a worker's serve returned
    /// Refused/Closed and the slot's publication was skip-cached).
    bound_skips,
);

counters!(RgStats, RgStatsSnapshot;
    /// Tasks claimed within this resource group.
    tasks_claimed,
    /// Tasks completed within this resource group.
    tasks_completed,
    /// Morsels claimed within this resource group.
    morsels_claimed,
    /// Granules executed within this resource group.
    granules_executed,
);

impl RuntimeStats {
    pub fn tick(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}
