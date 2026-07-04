// backend-utils-activity-pgstat — pgstat.c's per-backend half: the pending-entry
// model, pgstat_report_stat batching, the relation/xact/database/slru/
// checkpointer counting layers, and the shared store the flush paths apply
// into plus its fetch/snapshot readers and variable-kind reset. Still
// unported: fixed-kind reset, 2PC record registration, and connstat session
// times (needs MyBackendType).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use core::cell::Cell;

pub mod checkpointer;
pub mod database;
pub mod pending;
pub mod relation;
pub mod shmem;
pub mod slru;
pub mod xact;

pub use database::{pgstat_fetch_stat_dbentry, pgstat_report_autovac};
pub use pending::pgstat_clear_snapshot;
pub use relation::{
    pgstat_fetch_stat_tabentry, pgstat_fetch_stat_tabentry_ext, pgstat_report_analyze,
    pgstat_report_vacuum,
};
pub use shmem::{
    pgstat_get_stat_snapshot_timestamp, pgstat_have_entry, pgstat_reset, pgstat_reset_counters,
    pgstat_reset_of_kind, PgStat_StatTabEntry,
};

pub type PgStat_Counter = i64;

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

pub fn set_pgstat_fetch_consistency(v: i32) {
    FETCH_CONSISTENCY.with(|c| c.set(v));
}

thread_local! {
    static IS_INITIALIZED: Cell<bool> = const { Cell::new(false) };
}

pub fn pgstat_initialize() -> types_error::PgResult<()> {
    debug_assert!(!IS_INITIALIZED.with(|c| c.get()));
    ipc_seams::before_shmem_exit::call(pgstat_shutdown_hook, datum::Datum::from_usize(0))?;
    IS_INITIALIZED.with(|c| c.set(true));
    Ok(())
}

fn pgstat_shutdown_hook(_code: i32, _arg: datum::Datum) -> types_error::PgResult<()> {
    debug_assert!(IS_INITIALIZED.with(|c| c.get()));
    if init_small::globals::MyDatabaseId() != types_core::InvalidOid {
        database::pgstat_report_disconnect(init_small::globals::MyDatabaseId());
    }
    pending::pgstat_report_stat(true);
    Ok(())
}

pub fn init_seams() {
    pgstat_seams::pgstat_initialize::set(pgstat_initialize);
    pgstat_seams::pgstat_set_session_end_cause_fatal::set(
        database::pgstat_set_session_end_cause_fatal,
    );
    pgstat_seams::pgstat_report_tempfile::set(database::pgstat_report_tempfile);
    pgstat_seams::pgstat_init_relation::set(relation::pgstat_init_relation);

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
}

#[cfg(test)]
mod tests;
