//! Port of `src/backend/utils/activity/pgstat_relation.c` (PostgreSQL 18.3).
//!
//! Implementation of relation statistics (`PGSTAT_KIND_RELATION`, a
//! variable-numbered stats kind). Kept separate from `pgstat.c` to enforce the
//! line between the statistics access/storage implementation and the details
//! about individual kinds of statistics.
//!
//! ## Model reconciliation (the dual-intrusive-pointer collapse)
//!
//! In C the per-table pending block is `PgStat_TableStatus`, reached directly
//! through `rel->pgstat_info` (a raw pointer cached in the relcache entry) and
//! through `PgStat_TableXactStatus.parent`. Its `trans` field is the head of a
//! per-table chain of `PgStat_TableXactStatus` nodes (innermost subxact first,
//! walking outward through `upper`); each such node *also* lives in its
//! (sub)transaction level's `PgStat_SubXactStatus.first` intrusive `next`-list.
//! One node thus lives in two intrusive lists at once via raw pointers.
//!
//! This model cannot reproduce dual raw-pointer membership with a single Rust
//! owner, so (matching the carriers in `types_pgstat`):
//!
//! * The `PgStat_TableStatus` pending block lives in `pgstat.c`'s owner-private
//!   entry-ref hash (`PgStat_EntryRef::pending`, a `Box<dyn Any>`), reached *by
//!   key* via [`pgstat_with_pending_mut`](pgstat_core::pgstat_with_pending_mut) —
//!   the documented reconciliation of C's `rel->pgstat_info` / `trans` /
//!   `parent` pointer chases. There is no `pgstat_info` back-pointer in the
//!   relcache; the entry is keyed by `(dboid, relid)`.
//!
//! * The per-table `trans`/`upper` chain is *owned* by the table's pending block
//!   (`PgStat_TableStatus::trans: Option<Box<PgStat_TableXactStatus>>`).
//!
//! * The per-level `PgStat_SubXactStatus::first` carries only the *keys* of the
//!   tables that have a node at that level (one node per table per level, so the
//!   key plus the level's `nest_level` uniquely names the node). The level node
//!   lives in `pgstat_xact.c`'s thread-local stack
//!   ([`pgstat_get_xact_stack_level`](xact::pgstat_get_xact_stack_level)).
//!
//! * `PgStat_TableStatus::relation` (C's `Relation` back-pointer) is dropped
//!   from the model, so `pgstat_relation_delete_pending_cb`'s
//!   `pgstat_unlink_relation(pending->relation)` collapses to a no-op (there is
//!   no cached relcache pointer to clear; the relcache reaches its pending entry
//!   by key, not via a stored pointer).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::boxed::Box;

use backend_access_transam_twophase_rmgr::TWOPHASE_RM_PGSTAT_ID;
use backend_utils_activity_pgstat::entry_ref::PgStat_EntryRef;
use backend_utils_activity_pgstat::kind_info::KindInfoBuilder;
use backend_utils_activity_pgstat::pgstat_core;
use backend_utils_activity_pgstat::registry;
use backend_utils_activity_pgstat::shmem;
use backend_utils_activity_pgstat_backend::{pgstat_flush_backend, PGSTAT_BACKEND_FLUSH_IO};
use backend_utils_activity_pgstat_database::pgstat_prep_database_pending;
use backend_utils_activity_xact as xact;
use backend_utils_init_small_seams::{my_backend_type, my_database_id};
use types_core::init::BackendType;
use types_core::primitive::{InvalidOid, Oid};
use types_core::TimestampTz;
use types_error::PgResult;
use types_pgstat::activity_pgstat::{
    PgStat_Counter, PgStat_StatTabEntry, PgStat_TableStatus, PgStat_TableXactStatus,
    PGSTAT_KIND_RELATION,
};
use types_pgstat::pgstat_internal::{PgStat_HashKey, PgStat_KindInfo, PgStatShared_Relation};

// ---------------------------------------------------------------------------
// Small helpers reproducing C macros / globals.
// ---------------------------------------------------------------------------

/// `pgstat_track_counts` GUC.
fn pgstat_track_counts() -> bool {
    backend_utils_misc_guc_tables::vars::pgstat_track_counts.read()
}

/// `AmAutoVacuumWorkerProcess()` (`miscadmin.h`): `MyBackendType ==
/// B_AUTOVAC_WORKER`.
fn am_auto_vacuum_worker_process() -> bool {
    my_backend_type::call() == BackendType::AutovacWorker
}

/// `GetCurrentTimestamp()`.
fn get_current_timestamp() -> TimestampTz {
    backend_utils_adt_timestamp_seams::get_current_timestamp::call()
}

/// `TimestampDifferenceMilliseconds(start, stop)`.
fn timestamp_difference_milliseconds(start: TimestampTz, stop: TimestampTz) -> PgStat_Counter {
    backend_utils_adt_timestamp_seams::timestamp_difference_milliseconds::call(start, stop)
}

/// `GetCurrentTransactionStopTimestamp()`.
fn get_current_transaction_stop_timestamp() -> TimestampTz {
    backend_access_transam_xact_seams::get_current_transaction_stop_timestamp::call()
}

/// `GetCurrentTransactionNestLevel()`.
fn get_current_transaction_nest_level() -> i32 {
    backend_access_transam_xact_seams::get_current_transaction_nest_level::call()
}

/// `IsSharedRelation(relid)` (`catalog/catalog.c`).
fn is_shared_relation(relid: Oid) -> bool {
    backend_catalog_catalog_seams::is_shared_relation::call(relid)
}

/// The pgstat hash key for a relation: `dboid = relisshared ? InvalidOid :
/// MyDatabaseId` (exactly C's `rel->rd_rel->relisshared ? InvalidOid :
/// MyDatabaseId`).
fn relation_key(relid: Oid, relisshared: bool) -> PgStat_HashKey {
    PgStat_HashKey {
        kind: PGSTAT_KIND_RELATION,
        dboid: if relisshared {
            InvalidOid
        } else {
            my_database_id::call()
        },
        objid: relid as u64,
    }
}

/// The backend-local pending block for a relation entry: C allocates a zeroed
/// `PgStat_TableStatus` (`pending_size`).
fn new_pending_relation() -> Box<dyn core::any::Any> {
    Box::new(PgStat_TableStatus::default())
}

// ---------------------------------------------------------------------------
// Copy / init / assoc / unlink.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_copy_relation_stats(Relation dst, Relation src)` — copy
/// stats between relations (e.g. REINDEX CONCURRENTLY).
///
/// `src` / `dst` are reduced to `(relid, relisshared)` the way the count seams
/// are: the relcache `Relation` pointer is not carried; the entry is keyed by
/// `(dboid, relid)`.
pub fn pgstat_copy_relation_stats(
    dst_relid: Oid,
    dst_relisshared: bool,
    src_relid: Oid,
    src_relisshared: bool,
) -> PgResult<()> {
    // srcstats = pgstat_fetch_stat_tabentry_ext(src->rd_rel->relisshared,
    //                                           RelationGetRelid(src));
    let srcstats = pgstat_fetch_stat_tabentry_ext(src_relisshared, src_relid)?;
    let Some(srcstats) = srcstats else {
        return Ok(());
    };

    let dst_ref = shmem::pgstat_get_entry_ref_locked(
        PGSTAT_KIND_RELATION,
        if dst_relisshared {
            InvalidOid
        } else {
            my_database_id::call()
        },
        dst_relid as u64,
        false,
    )?
    .expect("pgstat_copy_relation_stats: get_entry_ref_locked returned None");

    // dstshstats = (PgStatShared_Relation *) dst_ref->shared_stats;
    // dstshstats->stats = *srcstats;
    // SAFETY: just-resolved, content-locked live reference.
    let er = unsafe { dst_ref.get() };
    let dstshstats = unsafe { &mut *(er.shared_stats as *mut PgStatShared_Relation) };
    dstshstats.stats = srcstats;

    shmem::pgstat_unlock_entry(er)?;
    Ok(())
}

// `pgstat_init_relation` lives in the pgstat owner crate
// (`backend_utils_activity_pgstat::pgstat_relation`) and is installed from
// there (it is the relation-open gate, on the inward boot path, with no
// dependency on this crate). Re-export it for completeness.
pub use backend_utils_activity_pgstat::pgstat_relation::pgstat_init_relation;

/// Port of `void pgstat_assoc_relation(Relation rel)` — prepare for statistics
/// for this relation to be collected (ensure a reference to the stats entry
/// before stats can be generated).
///
/// C sets `rel->pgstat_info = pgstat_prep_relation_pending(...)` and
/// `rel->pgstat_info->relation = rel`. In this model there is no relcache
/// back-pointer and the `relation` field is dropped, so this reduces to
/// ensuring the pending entry exists (its identity is the key).
pub fn pgstat_assoc_relation(relid: Oid, relisshared: bool) -> PgResult<()> {
    // Assert(rel->pgstat_enabled);
    // Assert(rel->pgstat_info == NULL);
    pgstat_prep_relation_pending(relid, relisshared)?;
    // rel->pgstat_info->relation = rel;  (dropped: no relcache back-pointer)
    Ok(())
}

/// Port of `void pgstat_unlink_relation(Relation rel)` — break the mutual link
/// between a relcache entry and its pending stats entry.
///
/// The model has no relcache back-pointer (`PgStat_TableStatus::relation` is
/// dropped), so there is nothing to unlink: the relcache reaches its pending
/// entry by key, not via a cached pointer. Kept as a no-op for call-site
/// parity.
pub fn pgstat_unlink_relation(_relid: Oid, _relisshared: bool) {
    // rel->pgstat_info->relation = NULL; rel->pgstat_info = NULL; (both no-ops)
}

// ---------------------------------------------------------------------------
// Transactional create / drop.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_create_relation(Relation rel)` — ensure stats are
/// dropped if the transaction aborts.
pub fn pgstat_create_relation(relid: Oid, relisshared: bool) -> PgResult<()> {
    xact::pgstat_create_transactional(
        PGSTAT_KIND_RELATION,
        if relisshared {
            InvalidOid
        } else {
            my_database_id::call()
        },
        relid as u64,
    )
}

/// Port of `void pgstat_drop_relation(Relation rel)` — ensure stats are dropped
/// if the transaction commits.
pub fn pgstat_drop_relation(relid: Oid, relisshared: bool) -> PgResult<()> {
    let nest_level = get_current_transaction_nest_level();

    xact::pgstat_drop_transactional(
        PGSTAT_KIND_RELATION,
        if relisshared {
            InvalidOid
        } else {
            my_database_id::call()
        },
        relid as u64,
    )?;

    // if (!pgstat_should_count_relation(rel)) return;
    //
    // C gates on `rel->pgstat_info != NULL`: a pending entry must already
    // exist. The drop path never lazily creates one (it has no pgstat_enabled
    // bit here), so if there is no pending entry, return.
    let key = relation_key(relid, relisshared);
    if !pgstat_core::pgstat_have_pending(key) {
        return Ok(());
    }

    // Transactionally set counters to 0 so accesses to pg_stat_xact_all_tables
    // inside the transaction show 0.
    pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |pgstat_info| {
        if let Some(trans) = pgstat_info.trans.as_mut() {
            if trans.nest_level == nest_level {
                save_truncdrop_counters(trans, true);
                trans.tuples_inserted = 0;
                trans.tuples_updated = 0;
                trans.tuples_deleted = 0;
            }
        }
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Vacuum / analyze reporting.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_report_vacuum(Oid tableoid, bool shared,
/// PgStat_Counter livetuples, PgStat_Counter deadtuples, TimestampTz
/// starttime)` — report that the table was just vacuumed and flush IO stats.
pub fn pgstat_report_vacuum(
    tableoid: Oid,
    shared: bool,
    livetuples: PgStat_Counter,
    deadtuples: PgStat_Counter,
    starttime: TimestampTz,
) -> PgResult<()> {
    if !pgstat_track_counts() {
        return Ok(());
    }

    let dboid = if shared { InvalidOid } else { my_database_id::call() };

    // ts = GetCurrentTimestamp();
    // elapsedtime = TimestampDifferenceMilliseconds(starttime, ts);
    let ts = get_current_timestamp();
    let elapsedtime = timestamp_difference_milliseconds(starttime, ts);

    let entry_ref =
        shmem::pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION, dboid, tableoid as u64, false)?
            .expect("pgstat_report_vacuum: get_entry_ref_locked returned None");

    // SAFETY: just-resolved, content-locked live reference.
    let er = unsafe { entry_ref.get() };
    let shtabentry = unsafe { &mut *(er.shared_stats as *mut PgStatShared_Relation) };
    let tabentry = &mut shtabentry.stats;

    tabentry.live_tuples = livetuples;
    tabentry.dead_tuples = deadtuples;

    // See C comment: zero the insert counter regardless of skipped pages.
    tabentry.ins_since_vacuum = 0;

    if am_auto_vacuum_worker_process() {
        tabentry.last_autovacuum_time = ts;
        tabentry.autovacuum_count += 1;
        tabentry.total_autovacuum_time += elapsedtime;
    } else {
        tabentry.last_vacuum_time = ts;
        tabentry.vacuum_count += 1;
        tabentry.total_vacuum_time += elapsedtime;
    }

    shmem::pgstat_unlock_entry(er)?;

    // Flush IO statistics now (see C comment).
    backend_utils_activity_stat_seams::pgstat_flush_io::call(false)?;
    let _ = pgstat_flush_backend(false, PGSTAT_BACKEND_FLUSH_IO)?;

    Ok(())
}

/// Port of `void pgstat_report_analyze(Relation rel, PgStat_Counter livetuples,
/// PgStat_Counter deadtuples, bool resetcounter, TimestampTz starttime)` —
/// report that the table was just analyzed and flush IO statistics.
///
/// `rel` is reduced to `(relid, relisshared, relkind, pgstat_enabled)`. C reads
/// `rel->rd_rel->relisshared`, `rel->rd_rel->relkind`, and (through
/// `pgstat_should_count_relation`) `rel->pgstat_info` / `rel->pgstat_enabled`.
pub fn pgstat_report_analyze(
    relid: Oid,
    relisshared: bool,
    relkind: u8,
    pgstat_enabled: bool,
    mut livetuples: PgStat_Counter,
    mut deadtuples: PgStat_Counter,
    resetcounter: bool,
    starttime: TimestampTz,
) -> PgResult<()> {
    use types_tuple::access::RELKIND_PARTITIONED_TABLE;

    if !pgstat_track_counts() {
        return Ok(());
    }

    let dboid = if relisshared {
        InvalidOid
    } else {
        my_database_id::call()
    };
    let key = relation_key(relid, relisshared);

    // Unlike VACUUM, ANALYZE might be running inside a transaction that has
    // already inserted/deleted rows. Subtract off transactional counts so they
    // are not double-counted after commit. Waste no time on partitioned tables.
    if let Some(count_key) = should_count_relation(relid, relisshared, pgstat_enabled)? {
        if relkind != RELKIND_PARTITIONED_TABLE {
            pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(
                count_key,
                |pgstat_info| {
                    let mut trans = pgstat_info.trans.as_deref();
                    while let Some(t) = trans {
                        livetuples -= t.tuples_inserted - t.tuples_deleted;
                        deadtuples -= t.tuples_updated + t.tuples_deleted;
                        trans = t.upper.as_deref();
                    }
                    // count stuff inserted by already-aborted subxacts, too
                    deadtuples -= pgstat_info.counts.delta_dead_tuples;
                },
            );
            // Since ANALYZE's counts are estimates, we could have underflowed.
            livetuples = livetuples.max(0);
            deadtuples = deadtuples.max(0);
        }
    }

    let ts = get_current_timestamp();
    let elapsedtime = timestamp_difference_milliseconds(starttime, ts);

    let entry_ref =
        shmem::pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION, dboid, relid as u64, false)?
            .expect("pgstat_report_analyze: get_entry_ref_locked returned None");

    // can't get dropped while accessed
    // Assert(entry_ref != NULL && entry_ref->shared_stats != NULL);
    let _ = key; // dboid/relid form the key; kept for parity with C's lookup

    // SAFETY: just-resolved, content-locked live reference.
    let er = unsafe { entry_ref.get() };
    assert!(
        !er.shared_stats.is_null(),
        "pgstat_report_analyze: shared_stats NULL"
    );
    let shtabentry = unsafe { &mut *(er.shared_stats as *mut PgStatShared_Relation) };
    let tabentry = &mut shtabentry.stats;

    tabentry.live_tuples = livetuples;
    tabentry.dead_tuples = deadtuples;

    // If commanded, reset mod_since_analyze to zero.
    if resetcounter {
        tabentry.mod_since_analyze = 0;
    }

    if am_auto_vacuum_worker_process() {
        tabentry.last_autoanalyze_time = ts;
        tabentry.autoanalyze_count += 1;
        tabentry.total_autoanalyze_time += elapsedtime;
    } else {
        tabentry.last_analyze_time = ts;
        tabentry.analyze_count += 1;
        tabentry.total_analyze_time += elapsedtime;
    }

    shmem::pgstat_unlock_entry(er)?;

    // see pgstat_report_vacuum()
    backend_utils_activity_stat_seams::pgstat_flush_io::call(false)?;
    let _ = pgstat_flush_backend(false, PGSTAT_BACKEND_FLUSH_IO)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Count macros (pgstat.h / pgstat_relation.c).
// ---------------------------------------------------------------------------

/// `pgstat_should_count_relation(rel)` (`pgstat.h` macro):
///
/// ```c
/// (likely(rel->pgstat_info != NULL) ? true :
///  (rel->pgstat_enabled ? pgstat_assoc_relation(rel), true : false))
/// ```
///
/// Returns the relation's pending-entry key when stats should be counted
/// (lazily prepping the entry the way C's `pgstat_assoc_relation` does), else
/// `None`. `pgstat_info != NULL` is modeled as "a pending entry exists for the
/// key".
fn should_count_relation(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
) -> PgResult<Option<PgStat_HashKey>> {
    let key = relation_key(relid, relisshared);
    if pgstat_core::pgstat_have_pending(key) {
        return Ok(Some(key));
    }
    if pgstat_enabled {
        pgstat_assoc_relation(relid, relisshared)?;
        return Ok(Some(key));
    }
    Ok(None)
}

/// Port of `void pgstat_count_heap_insert(Relation rel, PgStat_Counter n)`.
pub fn pgstat_count_heap_insert(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
    n: PgStat_Counter,
) -> PgResult<()> {
    if let Some(key) = should_count_relation(relid, relisshared, pgstat_enabled)? {
        ensure_tabstat_xact_level(key, relid, relisshared)?;
        pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |pgstat_info| {
            pgstat_info
                .trans
                .as_mut()
                .expect("ensure_tabstat_xact_level guarantees trans")
                .tuples_inserted += n;
        });
    }
    Ok(())
}

/// Port of `void pgstat_count_heap_update(Relation rel, bool hot, bool
/// newpage)`.
pub fn pgstat_count_heap_update(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
    hot: bool,
    newpage: bool,
) -> PgResult<()> {
    debug_assert!(!(hot && newpage));

    if let Some(key) = should_count_relation(relid, relisshared, pgstat_enabled)? {
        ensure_tabstat_xact_level(key, relid, relisshared)?;
        pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |pgstat_info| {
            pgstat_info
                .trans
                .as_mut()
                .expect("ensure_tabstat_xact_level guarantees trans")
                .tuples_updated += 1;

            // tuples_hot_updated / tuples_newpage_updated are nontransactional.
            if hot {
                pgstat_info.counts.tuples_hot_updated += 1;
            } else if newpage {
                pgstat_info.counts.tuples_newpage_updated += 1;
            }
        });
    }
    Ok(())
}

/// Port of `void pgstat_count_heap_delete(Relation rel)`.
pub fn pgstat_count_heap_delete(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
) -> PgResult<()> {
    if let Some(key) = should_count_relation(relid, relisshared, pgstat_enabled)? {
        ensure_tabstat_xact_level(key, relid, relisshared)?;
        pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |pgstat_info| {
            pgstat_info
                .trans
                .as_mut()
                .expect("ensure_tabstat_xact_level guarantees trans")
                .tuples_deleted += 1;
        });
    }
    Ok(())
}

/// Port of `void pgstat_count_truncate(Relation rel)` — update tuple counters
/// due to truncate.
pub fn pgstat_count_truncate(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
) -> PgResult<()> {
    if let Some(key) = should_count_relation(relid, relisshared, pgstat_enabled)? {
        ensure_tabstat_xact_level(key, relid, relisshared)?;
        pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |pgstat_info| {
            let trans = pgstat_info
                .trans
                .as_mut()
                .expect("ensure_tabstat_xact_level guarantees trans");
            save_truncdrop_counters(trans, false);
            trans.tuples_inserted = 0;
            trans.tuples_updated = 0;
            trans.tuples_deleted = 0;
        });
    }
    Ok(())
}

/// Port of `void pgstat_update_heap_dead_tuples(Relation rel, int delta)`.
pub fn pgstat_update_heap_dead_tuples(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
    delta: i32,
) -> PgResult<()> {
    if let Some(key) = should_count_relation(relid, relisshared, pgstat_enabled)? {
        pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |pgstat_info| {
            pgstat_info.counts.delta_dead_tuples -= delta as PgStat_Counter;
        });
    }
    Ok(())
}

/// Port of the `pgstat_count_heap_scan(rel)` / `pgstat_count_index_scan(rel)`
/// macros: increment `counts.numscans`.
pub fn pgstat_count_heap_scan(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
) -> PgResult<()> {
    count_field(relid, relisshared, pgstat_enabled, |c| c.numscans += 1)
}

/// Port of `pgstat_count_index_scan(rel)`: increment `counts.numscans`.
pub fn pgstat_count_index_scan(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
) -> PgResult<()> {
    count_field(relid, relisshared, pgstat_enabled, |c| c.numscans += 1)
}

/// Port of `pgstat_count_heap_getnext(rel)`: increment `counts.tuples_returned`.
pub fn pgstat_count_heap_getnext(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
) -> PgResult<()> {
    count_field(relid, relisshared, pgstat_enabled, |c| {
        c.tuples_returned += 1
    })
}

/// Port of `pgstat_count_heap_fetch(rel)`: increment `counts.tuples_fetched`.
pub fn pgstat_count_heap_fetch(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
) -> PgResult<()> {
    count_field(relid, relisshared, pgstat_enabled, |c| c.tuples_fetched += 1)
}

/// Port of `pgstat_count_index_tuples(rel, n)`: add `n` to
/// `counts.tuples_returned`.
pub fn pgstat_count_index_tuples(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
    n: PgStat_Counter,
) -> PgResult<()> {
    count_field(relid, relisshared, pgstat_enabled, |c| {
        c.tuples_returned += n
    })
}

/// Shared body for the nontransactional `counts` macros.
fn count_field(
    relid: Oid,
    relisshared: bool,
    pgstat_enabled: bool,
    f: impl FnOnce(&mut types_pgstat::activity_pgstat::PgStat_TableCounts),
) -> PgResult<()> {
    if let Some(key) = should_count_relation(relid, relisshared, pgstat_enabled)? {
        pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |pgstat_info| {
            f(&mut pgstat_info.counts);
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// SQL-callable fetch + find.
// ---------------------------------------------------------------------------

/// Port of `PgStat_StatTabEntry *pgstat_fetch_stat_tabentry(Oid relid)`.
pub fn pgstat_fetch_stat_tabentry(relid: Oid) -> PgResult<Option<PgStat_StatTabEntry>> {
    pgstat_fetch_stat_tabentry_ext(is_shared_relation(relid), relid)
}

/// Port of `PgStat_StatTabEntry *pgstat_fetch_stat_tabentry_ext(bool shared, Oid
/// reloid)`.
pub fn pgstat_fetch_stat_tabentry_ext(
    shared: bool,
    reloid: Oid,
) -> PgResult<Option<PgStat_StatTabEntry>> {
    let dboid = if shared { InvalidOid } else { my_database_id::call() };
    let bytes = pgstat_core::pgstat_fetch_entry(PGSTAT_KIND_RELATION, dboid, reloid as u64)?;
    Ok(bytes.map(|b| decode_tab_entry(&b)))
}

/// Decode the `shared_data_len` bytes `pgstat_fetch_entry` copies out into the
/// typed `PgStat_StatTabEntry` (C's `(PgStat_StatTabEntry *) ...`).
fn decode_tab_entry(bytes: &[u8]) -> PgStat_StatTabEntry {
    assert_eq!(
        bytes.len(),
        core::mem::size_of::<PgStat_StatTabEntry>(),
        "pgstat_fetch_stat_tabentry: unexpected stats blob size"
    );
    // SAFETY: the blob is exactly a `PgStat_StatTabEntry` (a Copy, pointer-free
    // POD), copied byte-for-byte by pgstat_fetch_entry.
    unsafe { core::ptr::read_unaligned(bytes.as_ptr() as *const PgStat_StatTabEntry) }
}

/// Port of `PgStat_TableStatus *find_tabstat_entry(Oid rel_id)` — find any
/// existing pending entry for `rel_id` in the current database, then shared.
///
/// Returns a copy with live subtransaction counts reconciled into the copy's
/// `counts`, and `trans` cleared. `None` if no entry (does not create).
pub fn find_tabstat_entry(rel_id: Oid) -> Option<PgStat_TableStatus> {
    // Try MyDatabaseId first, then shared (InvalidOid).
    let local_key = PgStat_HashKey {
        kind: PGSTAT_KIND_RELATION,
        dboid: my_database_id::call(),
        objid: rel_id as u64,
    };
    let shared_key = PgStat_HashKey {
        kind: PGSTAT_KIND_RELATION,
        dboid: InvalidOid,
        objid: rel_id as u64,
    };

    let key = if pgstat_core::pgstat_have_pending(local_key) {
        local_key
    } else if pgstat_core::pgstat_have_pending(shared_key) {
        shared_key
    } else {
        return None;
    };

    pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, PgStat_TableStatus>(key, |tabentry| {
        // tablestatus = palloc(...); *tablestatus = *tabentry;
        let mut tablestatus = tabentry.clone();
        // Reset tablestatus->trans in the copy (may point to shared memory).
        tablestatus.trans = None;

        // Reconcile live subtransaction counts into the copy.
        let mut trans = tabentry.trans.as_deref();
        while let Some(t) = trans {
            tablestatus.counts.tuples_inserted += t.tuples_inserted;
            tablestatus.counts.tuples_updated += t.tuples_updated;
            tablestatus.counts.tuples_deleted += t.tuples_deleted;
            trans = t.upper.as_deref();
        }
        tablestatus
    })
}

// ---------------------------------------------------------------------------
// End-of-(sub)transaction work (helpers for AtEOXact_PgStat).
// ---------------------------------------------------------------------------

/// Port of `void AtEOXact_PgStat_Relations(PgStat_SubXactStatus *xact_state,
/// bool isCommit)` — transfer transactional insert/update counts into the base
/// tabstat entries.
///
/// `xact_state->first` is the per-level chain of table keys; in this model each
/// table's level-1 `PgStat_TableXactStatus` node is owned by its pending block's
/// `trans` head (with `nest_level == 1` and `upper == None`, the C asserts).
pub fn AtEOXact_PgStat_Relations(
    xact_state: &mut types_pgstat::activity_pgstat::PgStat_SubXactStatus,
    isCommit: bool,
) {
    for &key in &xact_state.first {
        pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |tabstat| {
            // trans = tabstat->trans (the head, == the level node)
            let Some(mut trans) = tabstat.trans.take() else {
                // Defensive: C asserts tabstat->trans == trans (always set).
                return;
            };
            debug_assert_eq!(trans.nest_level, 1);
            debug_assert!(trans.upper.is_none());

            // restore pre-truncate/drop stats (if any) in case of aborted xact
            if !isCommit {
                restore_truncdrop_counters(&mut trans);
            }
            // count attempted actions regardless of commit/abort
            tabstat.counts.tuples_inserted += trans.tuples_inserted;
            tabstat.counts.tuples_updated += trans.tuples_updated;
            tabstat.counts.tuples_deleted += trans.tuples_deleted;
            if isCommit {
                tabstat.counts.truncdropped = trans.truncdropped;
                if trans.truncdropped {
                    // forget live/dead stats seen by backend thus far
                    tabstat.counts.delta_live_tuples = 0;
                    tabstat.counts.delta_dead_tuples = 0;
                }
                // insert adds a live tuple, delete removes one
                tabstat.counts.delta_live_tuples +=
                    trans.tuples_inserted - trans.tuples_deleted;
                // update and delete each create a dead tuple
                tabstat.counts.delta_dead_tuples +=
                    trans.tuples_updated + trans.tuples_deleted;
                // insert, update, delete each count as one change event
                tabstat.counts.changed_tuples +=
                    trans.tuples_inserted + trans.tuples_updated + trans.tuples_deleted;
            } else {
                // inserted tuples are dead, deleted tuples are unaffected
                tabstat.counts.delta_dead_tuples +=
                    trans.tuples_inserted + trans.tuples_updated;
                // an aborted xact generates no changed_tuple events
            }
            // tabstat->trans = NULL (already taken above)
        });
    }
}

/// Port of `void AtEOSubXact_PgStat_Relations(PgStat_SubXactStatus *xact_state,
/// bool isCommit, int nestDepth)` — transfer the subtransaction's
/// transactional insert/update counts into the next higher subtransaction
/// state (commit) or fold them into the tables' pending stats (abort).
///
/// The level node has already been popped from the stack by the caller, so
/// `pgstat_get_xact_stack_level(nestDepth - 1)` reaches/creates the parent.
pub fn AtEOSubXact_PgStat_Relations(
    xact_state: &mut types_pgstat::activity_pgstat::PgStat_SubXactStatus,
    isCommit: bool,
    nestDepth: i32,
) -> PgResult<()> {
    // Drain xact_state->first (the C `for (trans = first; trans; trans =
    // next_trans)` over this level's nodes), processing each table's level node.
    for &key in &xact_state.first {
        // C: tabstat = trans->parent; Assert(tabstat->trans == trans);
        // The table's level node is its pending block's `trans` head.
        let mut push_to_parent = false;

        pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |tabstat| {
            let Some(mut trans) = tabstat.trans.take() else {
                return;
            };
            debug_assert_eq!(trans.nest_level, nestDepth);

            if isCommit {
                let upper_is_immediate_parent = trans
                    .upper
                    .as_ref()
                    .is_some_and(|u| u.nest_level == nestDepth - 1);

                if upper_is_immediate_parent {
                    // Merge into the immediate parent node and free `trans`.
                    let mut upper = trans.upper.take().expect("checked is_some above");
                    if trans.truncdropped {
                        // propagate the truncate/drop status one level up
                        save_truncdrop_counters(&mut upper, false);
                        // replace upper xact stats with ours
                        upper.tuples_inserted = trans.tuples_inserted;
                        upper.tuples_updated = trans.tuples_updated;
                        upper.tuples_deleted = trans.tuples_deleted;
                    } else {
                        upper.tuples_inserted += trans.tuples_inserted;
                        upper.tuples_updated += trans.tuples_updated;
                        upper.tuples_deleted += trans.tuples_deleted;
                    }
                    // tabstat->trans = trans->upper; pfree(trans);
                    tabstat.trans = Some(upper);
                    // (the parent level node already lists this key, so no
                    // change to any level's key list is needed)
                } else {
                    // No immediate parent state: reuse the record by re-linking
                    // it into the parent level. C just re-points list links;
                    // here we re-stamp the node's nest_level and re-insert it as
                    // the table's `trans` head, and add the key to the parent
                    // level node's `first` list (done after this closure, since
                    // it touches the xact stack thread-local).
                    trans.nest_level = nestDepth - 1;
                    tabstat.trans = Some(trans);
                    push_to_parent = true;
                }
            } else {
                // On abort: update top-level tabstat counts, then forget the
                // subtransaction.

                // first restore values obliterated by truncate/drop
                restore_truncdrop_counters(&mut trans);
                // count attempted actions regardless of commit/abort
                tabstat.counts.tuples_inserted += trans.tuples_inserted;
                tabstat.counts.tuples_updated += trans.tuples_updated;
                tabstat.counts.tuples_deleted += trans.tuples_deleted;
                // inserted tuples are dead, deleted tuples are unaffected
                tabstat.counts.delta_dead_tuples +=
                    trans.tuples_inserted + trans.tuples_updated;
                // tabstat->trans = trans->upper; pfree(trans);
                tabstat.trans = trans.upper.take();
            }
        });

        if push_to_parent {
            // upper_xact_state = pgstat_get_xact_stack_level(nestDepth - 1);
            // trans->next = upper_xact_state->first; upper_xact_state->first =
            // trans;
            xact::pgstat_get_xact_stack_level(nestDepth - 1, |upper_xact_state| {
                upper_xact_state.first.push(key);
            })?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// 2PC prepare / post-commit / post-abort.
// ---------------------------------------------------------------------------

/// `TwoPhasePgStatRecord` (`pgstat_relation.c`) — record written to the 2PC
/// state file when pgstat state is persisted. `#[repr(C)]` so the byte image
/// matches C's struct exactly for `RegisterTwoPhaseRecord` / record decode.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct TwoPhasePgStatRecord {
    /// tuples inserted in xact
    tuples_inserted: PgStat_Counter,
    /// tuples updated in xact
    tuples_updated: PgStat_Counter,
    /// tuples deleted in xact
    tuples_deleted: PgStat_Counter,
    /// tuples i/u/d prior to truncate/drop
    inserted_pre_truncdrop: PgStat_Counter,
    updated_pre_truncdrop: PgStat_Counter,
    deleted_pre_truncdrop: PgStat_Counter,
    /// table's OID
    id: Oid,
    /// is it a shared catalog?
    shared: bool,
    /// was the relation truncated/dropped?
    truncdropped: bool,
}

impl TwoPhasePgStatRecord {
    /// Byte image of the record, for `RegisterTwoPhaseRecord(..., &record,
    /// sizeof(TwoPhasePgStatRecord))`.
    fn as_bytes(&self) -> [u8; core::mem::size_of::<TwoPhasePgStatRecord>()] {
        // SAFETY: a `#[repr(C)]` POD (no pointers); read its raw bytes.
        unsafe {
            core::ptr::read_unaligned(
                self as *const TwoPhasePgStatRecord
                    as *const [u8; core::mem::size_of::<TwoPhasePgStatRecord>()],
            )
        }
    }

    /// Decode from the 2PC state-file bytes (`(TwoPhasePgStatRecord *)
    /// recdata`).
    fn from_bytes(bytes: &[u8]) -> TwoPhasePgStatRecord {
        assert_eq!(
            bytes.len(),
            core::mem::size_of::<TwoPhasePgStatRecord>(),
            "pgstat 2PC record: unexpected size"
        );
        // SAFETY: the bytes are exactly a TwoPhasePgStatRecord POD.
        unsafe { core::ptr::read_unaligned(bytes.as_ptr() as *const TwoPhasePgStatRecord) }
    }
}

/// Port of `void AtPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state)` —
/// generate 2PC records for all the pending transaction-dependent relation
/// stats.
pub fn AtPrepare_PgStat_Relations(
    xact_state: &mut types_pgstat::activity_pgstat::PgStat_SubXactStatus,
) -> PgResult<()> {
    for &key in &xact_state.first {
        // Build the record from the table's level-1 node + identity.
        let record = pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, TwoPhasePgStatRecord>(
            key,
            |tabstat| {
                let trans = tabstat
                    .trans
                    .as_ref()
                    .expect("AtPrepare_PgStat_Relations: tabstat->trans must be set");
                debug_assert_eq!(trans.nest_level, 1);
                debug_assert!(trans.upper.is_none());
                TwoPhasePgStatRecord {
                    tuples_inserted: trans.tuples_inserted,
                    tuples_updated: trans.tuples_updated,
                    tuples_deleted: trans.tuples_deleted,
                    inserted_pre_truncdrop: trans.inserted_pre_truncdrop,
                    updated_pre_truncdrop: trans.updated_pre_truncdrop,
                    deleted_pre_truncdrop: trans.deleted_pre_truncdrop,
                    id: tabstat.id,
                    shared: tabstat.shared,
                    truncdropped: trans.truncdropped,
                }
            },
        );

        let Some(record) = record else { continue };

        backend_access_transam_twophase_seams::register_two_phase_record::call(
            TWOPHASE_RM_PGSTAT_ID,
            0,
            &record.as_bytes(),
        )?;
    }
    Ok(())
}

/// Port of `void PostPrepare_PgStat_Relations(PgStat_SubXactStatus
/// *xact_state)` — unlink the transaction stats state from the nontransactional
/// state.
pub fn PostPrepare_PgStat_Relations(
    xact_state: &mut types_pgstat::activity_pgstat::PgStat_SubXactStatus,
) {
    for &key in &xact_state.first {
        // tabstat = trans->parent; tabstat->trans = NULL;
        pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |tabstat| {
            tabstat.trans = None;
        });
    }
}

/// Port of `void pgstat_twophase_postcommit(TransactionId xid, uint16 info,
/// void *recdata, uint32 len)` — load the saved counts into local pgstats state
/// (COMMIT PREPARED).
pub fn pgstat_twophase_postcommit(
    _xid: types_core::primitive::TransactionId,
    _info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    let rec = TwoPhasePgStatRecord::from_bytes(recdata);

    // Find or create a tabstat entry for the rel.
    let key = pgstat_prep_relation_pending(rec.id, rec.shared)?;

    pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |pgstat_info| {
        // Same math as in AtEOXact_PgStat, commit case.
        pgstat_info.counts.tuples_inserted += rec.tuples_inserted;
        pgstat_info.counts.tuples_updated += rec.tuples_updated;
        pgstat_info.counts.tuples_deleted += rec.tuples_deleted;
        pgstat_info.counts.truncdropped = rec.truncdropped;
        if rec.truncdropped {
            // forget live/dead stats seen by backend thus far
            pgstat_info.counts.delta_live_tuples = 0;
            pgstat_info.counts.delta_dead_tuples = 0;
        }
        pgstat_info.counts.delta_live_tuples += rec.tuples_inserted - rec.tuples_deleted;
        pgstat_info.counts.delta_dead_tuples += rec.tuples_updated + rec.tuples_deleted;
        pgstat_info.counts.changed_tuples +=
            rec.tuples_inserted + rec.tuples_updated + rec.tuples_deleted;
    });
    Ok(())
}

/// Port of `void pgstat_twophase_postabort(TransactionId xid, uint16 info, void
/// *recdata, uint32 len)` — load the saved counts as aborted (ROLLBACK
/// PREPARED).
pub fn pgstat_twophase_postabort(
    _xid: types_core::primitive::TransactionId,
    _info: u16,
    recdata: &[u8],
) -> PgResult<()> {
    let mut rec = TwoPhasePgStatRecord::from_bytes(recdata);

    let key = pgstat_prep_relation_pending(rec.id, rec.shared)?;

    pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |pgstat_info| {
        // Same math as in AtEOXact_PgStat, abort case.
        if rec.truncdropped {
            rec.tuples_inserted = rec.inserted_pre_truncdrop;
            rec.tuples_updated = rec.updated_pre_truncdrop;
            rec.tuples_deleted = rec.deleted_pre_truncdrop;
        }
        pgstat_info.counts.tuples_inserted += rec.tuples_inserted;
        pgstat_info.counts.tuples_updated += rec.tuples_updated;
        pgstat_info.counts.tuples_deleted += rec.tuples_deleted;
        pgstat_info.counts.delta_dead_tuples += rec.tuples_inserted + rec.tuples_updated;
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Flush + delete callbacks.
// ---------------------------------------------------------------------------

/// Port of `bool pgstat_relation_flush_cb(PgStat_EntryRef *entry_ref, bool
/// nowait)` — flush out pending stats for the entry.
pub fn pgstat_relation_flush_cb(entry_ref: &mut PgStat_EntryRef, nowait: bool) -> PgResult<bool> {
    // dboid = entry_ref->shared_entry->key.dboid;
    // SAFETY: shared_entry points at a live PgStatShared_HashEntry.
    let dboid = unsafe { (*entry_ref.shared_entry).key.dboid };

    // lstats = (PgStat_TableStatus *) entry_ref->pending; — copied out so its
    // borrow does not collide with lock/unlock's &entry_ref borrow.
    let lstats: PgStat_TableStatus = entry_ref
        .pending
        .as_ref()
        .expect("pgstat_relation_flush_cb: entry has no pending block")
        .downcast_ref::<PgStat_TableStatus>()
        .expect("pgstat_relation_flush_cb: pending is not a PgStat_TableStatus")
        .clone();

    // Ignore entries that didn't accumulate any actual counts (e.g. indexes
    // opened by the planner but not used).
    if lstats.counts.is_all_zeros() {
        return Ok(true);
    }

    if !shmem::pgstat_lock_entry(entry_ref, nowait)? {
        return Ok(false);
    }

    // tabentry = &shtabstats->stats;
    // SAFETY: shared body live + locked.
    let shtabstats = entry_ref.shared_stats as *mut PgStatShared_Relation;
    let tabentry = unsafe { &mut (*shtabstats).stats };

    tabentry.numscans += lstats.counts.numscans;
    if lstats.counts.numscans != 0 {
        let t = get_current_transaction_stop_timestamp();
        if t > tabentry.lastscan {
            tabentry.lastscan = t;
        }
    }
    tabentry.tuples_returned += lstats.counts.tuples_returned;
    tabentry.tuples_fetched += lstats.counts.tuples_fetched;
    tabentry.tuples_inserted += lstats.counts.tuples_inserted;
    tabentry.tuples_updated += lstats.counts.tuples_updated;
    tabentry.tuples_deleted += lstats.counts.tuples_deleted;
    tabentry.tuples_hot_updated += lstats.counts.tuples_hot_updated;
    tabentry.tuples_newpage_updated += lstats.counts.tuples_newpage_updated;

    // If table was truncated/dropped, first reset the live/dead counters.
    if lstats.counts.truncdropped {
        tabentry.live_tuples = 0;
        tabentry.dead_tuples = 0;
        tabentry.ins_since_vacuum = 0;
    }

    tabentry.live_tuples += lstats.counts.delta_live_tuples;
    tabentry.dead_tuples += lstats.counts.delta_dead_tuples;
    tabentry.mod_since_analyze += lstats.counts.changed_tuples;

    // (see C comment about tracking aborted inserts)
    tabentry.ins_since_vacuum += lstats.counts.tuples_inserted;

    tabentry.blocks_fetched += lstats.counts.blocks_fetched;
    tabentry.blocks_hit += lstats.counts.blocks_hit;

    // Clamp live/dead in case of negative deltas.
    tabentry.live_tuples = tabentry.live_tuples.max(0);
    tabentry.dead_tuples = tabentry.dead_tuples.max(0);

    shmem::pgstat_unlock_entry(entry_ref)?;

    // The entry was successfully flushed, add the same to database stats.
    let dbref = pgstat_prep_database_pending(dboid)?;
    // SAFETY: just-prepped live reference whose pending was ensured present.
    let dber = unsafe { dbref.get() };
    let dbentry = dber
        .pending
        .as_mut()
        .expect("pgstat_relation_flush_cb: database pending missing after prep")
        .downcast_mut::<types_pgstat::activity_pgstat::PgStat_StatDBEntry>()
        .expect("pgstat_relation_flush_cb: database pending is not a PgStat_StatDBEntry");
    dbentry.tuples_returned += lstats.counts.tuples_returned;
    dbentry.tuples_fetched += lstats.counts.tuples_fetched;
    dbentry.tuples_inserted += lstats.counts.tuples_inserted;
    dbentry.tuples_updated += lstats.counts.tuples_updated;
    dbentry.tuples_deleted += lstats.counts.tuples_deleted;
    dbentry.blocks_fetched += lstats.counts.blocks_fetched;
    dbentry.blocks_hit += lstats.counts.blocks_hit;

    Ok(true)
}

/// Port of `void pgstat_relation_delete_pending_cb(PgStat_EntryRef
/// *entry_ref)`.
///
/// C: `if (pending->relation) pgstat_unlink_relation(pending->relation);`. The
/// model has no `relation` back-pointer, so this is a no-op (see module docs).
pub fn pgstat_relation_delete_pending_cb(_entry_ref: &mut PgStat_EntryRef) {
    // pending->relation is not modeled; nothing to unlink.
}

// ---------------------------------------------------------------------------
// Static helpers (pending prep + xact-level bookkeeping).
// ---------------------------------------------------------------------------

/// Port of `static PgStat_TableStatus *pgstat_prep_relation_pending(Oid rel_id,
/// bool isshared)` — find or create a `PgStat_TableStatus` entry for `rel`.
///
/// Returns the entry's key (C returns the pending pointer; the pending block is
/// reached/mutated by key in this model).
fn pgstat_prep_relation_pending(rel_id: Oid, isshared: bool) -> PgResult<PgStat_HashKey> {
    let dboid = if isshared {
        InvalidOid
    } else {
        my_database_id::call()
    };
    let entry_ref =
        pgstat_core::pgstat_prep_pending_entry(PGSTAT_KIND_RELATION, dboid, rel_id as u64, new_pending_relation)?;

    // pending->id = rel_id; pending->shared = isshared;
    // SAFETY: just-prepped live reference whose pending was ensured present.
    let er = unsafe { entry_ref.get() };
    let pending = er
        .pending
        .as_mut()
        .expect("pgstat_prep_relation_pending: pending missing after prep")
        .downcast_mut::<PgStat_TableStatus>()
        .expect("pgstat_prep_relation_pending: pending is not a PgStat_TableStatus");
    pending.id = rel_id;
    pending.shared = isshared;

    Ok(PgStat_HashKey {
        kind: PGSTAT_KIND_RELATION,
        dboid,
        objid: rel_id as u64,
    })
}

/// Port of `static void add_tabstat_xact_level(PgStat_TableStatus *pgstat_info,
/// int nest_level)` — add a new (sub)transaction state record.
///
/// C pushes a transaction-stack entry, allocates a new
/// `PgStat_TableXactStatus`, links it as the table's new `trans` head (the old
/// head becoming `upper`), and links it into the level node's `first` list and
/// the table's `trans`. In this model the node is owned by the table's pending
/// block; the level node's `first` carries the table's key.
fn add_tabstat_xact_level(
    key: PgStat_HashKey,
    relid: Oid,
    relisshared: bool,
    nest_level: i32,
) -> PgResult<()> {
    // If this is the first rel modified at the current nest level, push a
    // transaction stack entry, and record this table's key at the level.
    xact::pgstat_get_xact_stack_level(nest_level, |xact_state| {
        xact_state.first.push(key);
    })?;

    // Make a per-table stack entry: trans->upper = pgstat_info->trans; the new
    // node becomes the table's `trans` head.
    pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, ()>(key, |pgstat_info| {
        let upper = pgstat_info.trans.take();
        pgstat_info.trans = Some(Box::new(PgStat_TableXactStatus {
            nest_level,
            upper,
            parent: key,
            ..PgStat_TableXactStatus::default()
        }));
    });
    let _ = (relid, relisshared);
    Ok(())
}

/// Port of `static void ensure_tabstat_xact_level(PgStat_TableStatus
/// *pgstat_info)` — add a new (sub)transaction record if needed.
fn ensure_tabstat_xact_level(
    key: PgStat_HashKey,
    relid: Oid,
    relisshared: bool,
) -> PgResult<()> {
    let nest_level = get_current_transaction_nest_level();

    let need_new = pgstat_core::pgstat_with_pending_mut::<PgStat_TableStatus, bool>(key, |info| {
        match info.trans.as_ref() {
            None => true,
            Some(t) => t.nest_level != nest_level,
        }
    })
    .expect("ensure_tabstat_xact_level: pending entry must exist (should_count prepped it)");

    if need_new {
        add_tabstat_xact_level(key, relid, relisshared, nest_level)?;
    }
    Ok(())
}

/// Port of `static void save_truncdrop_counters(PgStat_TableXactStatus *trans,
/// bool is_drop)`.
fn save_truncdrop_counters(trans: &mut PgStat_TableXactStatus, is_drop: bool) {
    if !trans.truncdropped || is_drop {
        trans.inserted_pre_truncdrop = trans.tuples_inserted;
        trans.updated_pre_truncdrop = trans.tuples_updated;
        trans.deleted_pre_truncdrop = trans.tuples_deleted;
        trans.truncdropped = true;
    }
}

/// Port of `static void restore_truncdrop_counters(PgStat_TableXactStatus
/// *trans)`.
fn restore_truncdrop_counters(trans: &mut PgStat_TableXactStatus) {
    if trans.truncdropped {
        trans.tuples_inserted = trans.inserted_pre_truncdrop;
        trans.tuples_updated = trans.updated_pre_truncdrop;
        trans.tuples_deleted = trans.deleted_pre_truncdrop;
    }
}

// ---------------------------------------------------------------------------
// Kind registration + seam installation.
// ---------------------------------------------------------------------------

/// The `PgStat_KindInfo` metadata for `PGSTAT_KIND_RELATION`
/// (`pgstat.c:pgstat_kind_builtin_infos[PGSTAT_KIND_RELATION]`).
fn relation_kind_info() -> PgStat_KindInfo {
    PgStat_KindInfo {
        fixed_amount: false,
        accessed_across_databases: false,
        write_to_file: true,
        shared_size: core::mem::size_of::<PgStatShared_Relation>() as u32,
        snapshot_ctl_off: 0,
        shared_ctl_off: 0,
        // offsetof(PgStatShared_Relation, stats)
        shared_data_off: core::mem::offset_of!(PgStatShared_Relation, stats) as u32,
        shared_data_len: core::mem::size_of::<PgStat_StatTabEntry>() as u32,
        pending_size: core::mem::size_of::<PgStat_TableStatus>() as u32,
        name: "relation",
    }
}

/// Register `PGSTAT_KIND_RELATION` and install the `pgstat_relation.c` outward
/// seams.
///
/// Must run before `backend_utils_activity_pgstat::init_seams()` seals the
/// per-kind table.
pub fn init_seams() {
    registry::register(
        KindInfoBuilder::new(PGSTAT_KIND_RELATION, relation_kind_info())
            .flush_pending_cb(pgstat_relation_flush_cb)
            .delete_pending_cb(pgstat_relation_delete_pending_cb)
            // On-disk stats-file (de)serialization of the `PgStat_StatTabEntry`
            // body: a real initdb cluster's pgstat.stat has hundreds of relation
            // entries, decoded at startup by pgstat_read_statsfile.
            .read_var_cb(|header, bytes| {
                // SAFETY: header points at a live PgStatShared_Relation body.
                let sh = unsafe { &mut *(header as *mut PgStatShared_Relation) };
                sh.stats = backend_utils_activity_pgstat::kind_info::pgstat_deserialize_pod::<
                    PgStat_StatTabEntry,
                >(bytes);
                Ok(())
            })
            .write_var_cb(|header| {
                // SAFETY: header points at a live PgStatShared_Relation body.
                let sh = unsafe { &*(header as *const PgStatShared_Relation) };
                backend_utils_activity_pgstat::kind_info::pgstat_serialize_pod(&sh.stats)
            }),
    );

    // --- canonical pgstat-relation count / create / drop seams
    //     (backend-utils-activity-pgstat-seams) ---
    use backend_utils_activity_pgstat_seams as pgseam;
    pgseam::pgstat_create_relation::set(pgstat_create_relation);
    pgseam::pgstat_drop_relation::set(pgstat_drop_relation);

    // The canonical count seams are infallible (`-> ()`), mirroring C's void
    // count macros; the only inner failure surface is the lazy
    // `pgstat_assoc_relation` pending-entry allocation (C longjmps on OOM).
    // Install via `.expect`-wrapping closures, the established pattern for an
    // infallible seam over a `PgResult` body (see pgstat_database.c's
    // report_deadlock / report_tempfile installs).
    pgseam::pgstat_count_index_tuples::set(|relid, relisshared, en, n| {
        pgstat_count_index_tuples(relid, relisshared, en, n)
            .expect("pgstat_count_index_tuples failed");
    });
    pgseam::pgstat_count_heap_fetch::set(|relid, relisshared, en| {
        pgstat_count_heap_fetch(relid, relisshared, en).expect("pgstat_count_heap_fetch failed");
    });
    pgseam::pgstat_count_index_scan::set(|relid, relisshared, en| {
        pgstat_count_index_scan(relid, relisshared, en).expect("pgstat_count_index_scan failed");
    });
    pgseam::pgstat_count_heap_scan::set(|relid, relisshared, en| {
        pgstat_count_heap_scan(relid, relisshared, en).expect("pgstat_count_heap_scan failed");
    });
    pgseam::pgstat_count_heap_getnext::set(|relid, relisshared, en| {
        pgstat_count_heap_getnext(relid, relisshared, en)
            .expect("pgstat_count_heap_getnext failed");
    });
    pgseam::pgstat_count_heap_insert::set(|relid, relisshared, en, n| {
        pgstat_count_heap_insert(relid, relisshared, en, n)
            .expect("pgstat_count_heap_insert failed");
    });
    pgseam::pgstat_count_heap_delete::set(|relid, relisshared, en| {
        pgstat_count_heap_delete(relid, relisshared, en).expect("pgstat_count_heap_delete failed");
    });
    pgseam::pgstat_count_heap_update::set(|relid, relisshared, en, hot, newpage| {
        pgstat_count_heap_update(relid, relisshared, en, hot, newpage)
            .expect("pgstat_count_heap_update failed");
    });
    pgseam::pgstat_update_heap_dead_tuples::set(|relid, relisshared, en, delta| {
        pgstat_update_heap_dead_tuples(relid, relisshared, en, delta)
            .expect("pgstat_update_heap_dead_tuples failed");
    });

    // --- transactional / 2PC seams (backend-utils-activity-stat-seams,
    //     consumed by backend-utils-activity-xact + twophase-rmgr) ---
    use backend_utils_activity_stat_seams as statseam;
    statseam::at_eoxact_pgstat_relations::set(AtEOXact_PgStat_Relations);
    statseam::at_eosubxact_pgstat_relations::set(AtEOSubXact_PgStat_Relations);
    statseam::at_prepare_pgstat_relations::set(AtPrepare_PgStat_Relations);
    statseam::post_prepare_pgstat_relations::set(PostPrepare_PgStat_Relations);
    statseam::pgstat_twophase_postcommit::set(pgstat_twophase_postcommit);
    statseam::pgstat_twophase_postabort::set(pgstat_twophase_postabort);
}
