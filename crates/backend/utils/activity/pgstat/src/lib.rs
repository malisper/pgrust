// backend-utils-activity-pgstat — pgstat.c's per-backend half: the pending-entry
// model, pgstat_report_stat batching, the counting layers for every kind, the
// shared store the flush paths apply into plus its fetch/snapshot readers and
// reset machinery, and the statsfile persistence half (write on clean
// shutdown, restore/discard at startup).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use core::cell::Cell;

pub mod archiver;
pub mod backend;
pub mod bgwriter;
pub mod checkpointer;
pub mod database;
pub mod file;
pub mod function;
pub mod io;
pub mod pending;
pub mod relation;
pub mod replslot;
pub mod shmem;
pub mod slru;
pub mod subscription;
pub mod wal;
pub mod xact;

pub use database::{
    pgstat_fetch_stat_dbentry, pgstat_prepare_report_checksum_failure, pgstat_report_autovac,
    pgstat_report_checksum_failures_in_db,
};
pub use function::{find_funcstat_entry, pgstat_fetch_stat_funcentry};
pub use pending::pgstat_clear_snapshot;
pub use relation::{
    pgstat_fetch_stat_tabentry, pgstat_fetch_stat_tabentry_ext, pgstat_report_analyze,
    pgstat_report_vacuum,
};
pub use replslot::pgstat_fetch_replslot;
pub use shmem::{
    pgstat_get_stat_snapshot_timestamp, pgstat_have_entry, pgstat_reset, pgstat_reset_counters,
    pgstat_reset_of_kind, PgStat_StatTabEntry,
};
pub use subscription::pgstat_fetch_stat_subscription;

pub type PgStat_Counter = i64;

// INSTR_TIME_SET_CURRENT: monotonic ns; only same-thread diffs are used.
// +1 keeps 0 free as the "timing disabled" sentinel.
pub(crate) fn now_ns() -> i64 {
    use std::sync::OnceLock;
    use std::time::Instant;
    static ANCHOR: OnceLock<Instant> = OnceLock::new();
    let anchor = *ANCHOR.get_or_init(Instant::now);
    anchor.elapsed().as_nanos() as i64 + 1
}

pub fn pgstat_get_kind_from_str(kind_str: &str) -> types_error::PgResult<pending::PgStat_Kind> {
    use pending::*;
    const NAMES: [(&str, PgStat_Kind); 12] = [
        ("database", PGSTAT_KIND_DATABASE),
        ("relation", PGSTAT_KIND_RELATION),
        ("function", PGSTAT_KIND_FUNCTION),
        ("replslot", PGSTAT_KIND_REPLSLOT),
        ("subscription", PGSTAT_KIND_SUBSCRIPTION),
        ("backend", PGSTAT_KIND_BACKEND),
        ("archiver", PGSTAT_KIND_ARCHIVER),
        ("bgwriter", PGSTAT_KIND_BGWRITER),
        ("checkpointer", PGSTAT_KIND_CHECKPOINTER),
        ("io", PGSTAT_KIND_IO),
        ("slru", PGSTAT_KIND_SLRU),
        ("wal", PGSTAT_KIND_WAL),
    ];
    for (name, kind) in NAMES {
        if kind_str.eq_ignore_ascii_case(name) {
            return Ok(kind);
        }
    }
    Err(Box::new(
        types_error::PgError::error(format!("invalid statistics kind: \"{kind_str}\""))
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
    ))
}

pub const PGSTAT_FETCH_CONSISTENCY_NONE: i32 = 0;
pub const PGSTAT_FETCH_CONSISTENCY_CACHE: i32 = 1;
pub const PGSTAT_FETCH_CONSISTENCY_SNAPSHOT: i32 = 2;

thread_local! {
    static TRACK_COUNTS: Cell<bool> = const { Cell::new(false) };
    static FETCH_CONSISTENCY: Cell<i32> = const { Cell::new(PGSTAT_FETCH_CONSISTENCY_CACHE) };
}

pub fn pgstat_track_counts() -> bool {
    TRACK_COUNTS.with(|c| c.get())
}

pub fn set_pgstat_track_counts(v: bool) {
    TRACK_COUNTS.with(|c| c.set(v));
}

pub fn pgstat_fetch_consistency() -> i32 {
    FETCH_CONSISTENCY.with(|c| c.get())
}

// assign_stats_fetch_consistency (pgstat.c): a mode change forces a snapshot
// clear on the next access.
pub fn set_pgstat_fetch_consistency(v: i32) {
    FETCH_CONSISTENCY.with(|c| {
        if c.get() != v {
            shmem::force_snapshot_clear_on_next_access();
        }
        c.set(v);
    });
}

thread_local! {
    static IS_INITIALIZED: Cell<bool> = const { Cell::new(false) };
}

pub fn pgstat_initialize() -> types_error::PgResult<()> {
    debug_assert!(!IS_INITIALIZED.with(|c| c.get()));
    wal::pgstat_wal_init_backend_cb();
    ipc_seams::before_shmem_exit::call(pgstat_shutdown_hook, datum::Datum::from_usize(0))?;
    IS_INITIALIZED.with(|c| c.set(true));
    Ok(())
}

/// Retention claim (wretain): pgstat TLS survived the park (the shutdown
/// hook flushed pending stats and dropped the per-backend entry); re-arm the
/// hook and re-baseline the WAL usage deltas for the new task.
pub fn pgstat_reattach_retained_backend() -> types_error::PgResult<()> {
    debug_assert!(IS_INITIALIZED.with(|c| c.get()));
    wal::pgstat_wal_init_backend_cb();
    ipc_seams::before_shmem_exit::call(pgstat_shutdown_hook, datum::Datum::from_usize(0))
}

fn pgstat_shutdown_hook(_code: i32, _arg: datum::Datum) -> types_error::PgResult<()> {
    debug_assert!(IS_INITIALIZED.with(|c| c.get()));
    if init_small::globals::MyDatabaseId() != types_core::InvalidOid {
        database::pgstat_report_disconnect(init_small::globals::MyDatabaseId());
    }
    pending::pgstat_report_stat(true);
    backend::pgstat_backend_shutdown();
    Ok(())
}

pub fn init_seams() {
    pgstat_seams::pgstat_initialize::set(pgstat_initialize);
    pgstat_seams::pgstat_reattach_retained_backend::set(pgstat_reattach_retained_backend);
    pgstat_seams::pgstat_set_session_end_cause_fatal::set(
        database::pgstat_set_session_end_cause_fatal,
    );
    pgstat_seams::pgstat_report_tempfile::set(database::pgstat_report_tempfile);
    pgstat_seams::pgstat_init_relation::set(relation::pgstat_init_relation);
    pgstat_seams::pgstat_before_server_shutdown::set(file::pgstat_before_server_shutdown);
    pgstat_seams::pgstat_restore_stats::set(file::pgstat_restore_stats);
    pgstat_seams::pgstat_discard_stats::set(file::pgstat_discard_stats);

    pgstat_seams::pgstat_get_slru_index::set(slru::pgstat_get_slru_index);
    pgstat_seams::pgstat_count_slru_page_zeroed::set(slru::pgstat_count_slru_page_zeroed);
    pgstat_seams::pgstat_count_slru_page_hit::set(slru::pgstat_count_slru_page_hit);
    pgstat_seams::pgstat_count_slru_page_read::set(slru::pgstat_count_slru_page_read);
    pgstat_seams::pgstat_count_slru_page_written::set(slru::pgstat_count_slru_page_written);
    pgstat_seams::pgstat_count_slru_page_exists::set(slru::pgstat_count_slru_page_exists);
    pgstat_seams::pgstat_count_slru_flush::set(slru::pgstat_count_slru_flush);
    pgstat_seams::pgstat_count_slru_truncate::set(slru::pgstat_count_slru_truncate);
    pgstat_seams::pgstat_count_checkpointer_slru_written::set(
        checkpointer::pgstat_count_checkpointer_slru_written,
    );
    pgstat_seams::pgstat_count_checkpointer_num_timed::set(|| {
        checkpointer::with_pending_checkpointer_stats(|s| s.num_timed += 1)
    });
    pgstat_seams::pgstat_count_checkpointer_num_requested::set(|| {
        checkpointer::with_pending_checkpointer_stats(|s| s.num_requested += 1)
    });
    pgstat_seams::pgstat_count_checkpointer_num_performed::set(|| {
        checkpointer::with_pending_checkpointer_stats(|s| s.num_performed += 1)
    });
    pgstat_seams::pgstat_count_checkpointer_restartpoints_timed::set(|| {
        checkpointer::with_pending_checkpointer_stats(|s| s.restartpoints_timed += 1)
    });
    pgstat_seams::pgstat_count_checkpointer_restartpoints_requested::set(|| {
        checkpointer::with_pending_checkpointer_stats(|s| s.restartpoints_requested += 1)
    });
    pgstat_seams::pgstat_count_checkpointer_restartpoints_performed::set(|| {
        checkpointer::with_pending_checkpointer_stats(|s| s.restartpoints_performed += 1)
    });
    pgstat_seams::pgstat_count_checkpointer_write_time::set(|msecs| {
        checkpointer::with_pending_checkpointer_stats(|s| s.write_time += msecs)
    });
    pgstat_seams::pgstat_count_checkpointer_sync_time::set(|msecs| {
        checkpointer::with_pending_checkpointer_stats(|s| s.sync_time += msecs)
    });

    pgstat_seams::pgstat_report_checkpointer::set(checkpointer::pgstat_report_checkpointer);
    pgstat_seams::pgstat_report_bgwriter::set(bgwriter::pgstat_report_bgwriter);
    pgstat_seams::pgstat_report_wal::set(wal::pgstat_report_wal);
    pgstat_seams::pgstat_report_fixed_set::set(pending::pgstat_report_fixed_set);
    pgstat_seams::pgstat_count_io_op::set(|obj, ctx, op, cnt, bytes| {
        io::pgstat_count_io_op(
            io::io_object_from_u32(obj),
            io::io_context_from_index(ctx as usize),
            io::io_op_from_u32(op),
            cnt,
            bytes,
        )
    });
    pgstat_seams::pgstat_prepare_io_time::set(io::pgstat_prepare_io_time);
    pgstat_seams::pgstat_count_io_op_time::set(|obj, ctx, op, start_ns, cnt, bytes| {
        io::pgstat_count_io_op_time(
            io::io_object_from_u32(obj),
            io::io_context_from_index(ctx as usize),
            io::io_op_from_u32(op),
            start_ns,
            cnt,
            bytes,
        )
    });


    // pgstat.c owns these GUC variables' backing storage (pgstat.c:204-205).
    use guc_tables::{vars, GucVarAccessors};
    vars::pgstat_track_counts.install(GucVarAccessors {
        get: pgstat_track_counts,
        set: set_pgstat_track_counts,
    });
    vars::pgstat_fetch_consistency.install(GucVarAccessors {
        get: pgstat_fetch_consistency,
        set: set_pgstat_fetch_consistency,
    });
    // pgstat_function.c:30 owns pgstat_track_functions.
    vars::pgstat_track_functions.install(GucVarAccessors {
        get: function::pgstat_track_functions,
        set: function::set_pgstat_track_functions,
    });
}

#[cfg(test)]
mod tests;
