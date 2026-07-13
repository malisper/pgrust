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
