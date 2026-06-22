//! `pg_replication_origin` catalog mutation/lookup machinery that `origin.c`
//! reaches through [`backend_replication_logical_origin_extern_seams`]. The
//! heap/genam/syscache/indexing/xact substrate is ported, so this crate owns
//! these seam bodies (mirroring the `checkpoint_write`/`checkpoint_read`
//! self-install in `lib.rs`).
//!
//! Faithful to PostgreSQL 18.3 `replication/logical/origin.c`:
//! `replorigin_create` (the dirty-snapshot free-id scan + insert),
//! `replorigin_by_name` / `replorigin_drop_by_name`'s syscache lookups, and
//! the drop-leg catalog tuple delete.

use alloc::string::String;

use backend_access_common_heaptuple::{heap_form_tuple, heap_getattr};
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam;
use backend_access_table_table::{table_close, table_open};
use backend_catalog_indexing::keystone::{CatalogTupleDelete, CatalogTupleInsert};
use backend_utils_cache_syscache::SearchSysCache1;
use mcx::MemoryContext;
use types_cache::syscache::SysCacheKey;
use types_catalog::catalog::{
    ANUM_PG_REPLICATION_ORIGIN_RONAME, ANUM_PG_REPLICATION_ORIGIN_ROIDENT,
    NATTS_PG_REPLICATION_ORIGIN, REPLICATION_ORIGIN_IDENT_INDEX, REPLICATION_ORIGIN_RELATION_ID,
};
use types_core::fmgr::F_OIDEQ;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessShareLock, ExclusiveLock, NoLock, RowExclusiveLock};
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::core::PG_UINT16_MAX;

use backend_utils_cache_syscache::cacheinfo::{REPLORIGIDENT, REPLORIGNAME};

/// `replorigin_create`'s catalog work (origin.c lines 282-361): open
/// `pg_replication_origin` under `ExclusiveLock`, scan with a dirty snapshot
/// for the first unused `roident` in `[InvalidOid+1, PG_UINT16_MAX)`, form +
/// insert the new row, `CommandCounterIncrement`, and close (keeping the lock
/// to commit). Returns the chosen `roident`, or `None` when every id collided.
pub fn create_catalog_insert(roname: &str) -> PgResult<Option<Oid>> {
    let scratch = MemoryContext::new("replorigin_create scan");
    let mcx = scratch.mcx();

    // InitDirtySnapshot(SnapshotDirty);
    let dirty = types_snapshot::SnapshotData::sentinel(types_snapshot::SnapshotType::SNAPSHOT_DIRTY);

    // rel = table_open(ReplicationOriginRelationId, ExclusiveLock);
    let rel = table_open(mcx, REPLICATION_ORIGIN_RELATION_ID, ExclusiveLock)?;

    let mut chosen: Option<Oid> = None;

    let mut roident: Oid = InvalidOid + 1;
    while roident < PG_UINT16_MAX as Oid {
        // CHECK_FOR_INTERRUPTS();
        backend_commands_vacuum_seams::check_for_interrupts::call()?;

        // ScanKeyInit(&key, Anum_pg_replication_origin_roident,
        //             BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roident));
        let mut key = ScanKeyData::empty();
        ScanKeyInit(
            &mut key,
            ANUM_PG_REPLICATION_ORIGIN_ROIDENT,
            BTEqualStrategyNumber,
            F_OIDEQ,
            Datum::from_oid(roident),
        )?;

        // scan = systable_beginscan(rel, ReplicationOriginIdentIndex, true,
        //                           &SnapshotDirty, 1, &key);
        let mut scan = genam::systable_beginscan::call(
            &rel,
            REPLICATION_ORIGIN_IDENT_INDEX,
            true,
            Some(&dirty),
            core::slice::from_ref(&key),
        )?;

        // collides = HeapTupleIsValid(systable_getnext(scan));
        let collides = genam::systable_getnext::call(mcx, scan.desc_mut())?.is_some();

        // systable_endscan(scan);
        scan.end()?;

        if !collides {
            // memset(&nulls, 0, sizeof(nulls));
            let nulls = [false; NATTS_PG_REPLICATION_ORIGIN];
            let mut values: [Datum; NATTS_PG_REPLICATION_ORIGIN] =
                core::array::from_fn(|_| Datum::null());

            // values[Anum_..._roident - 1] = ObjectIdGetDatum(roident);
            values[(ANUM_PG_REPLICATION_ORIGIN_ROIDENT - 1) as usize] = Datum::from_oid(roident);
            // values[Anum_..._roname - 1] = roname_d; (CStringGetTextDatum)
            values[(ANUM_PG_REPLICATION_ORIGIN_RONAME - 1) as usize] =
                backend_commands_comment_seams::cstring_get_text_datum::call(mcx, roname)?;

            // tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
            let mut tuple = heap_form_tuple(mcx, &rel.rd_att, &values, &nulls)
                .map_err(|e| PgError::error(format!("heap_form_tuple failed: {e:?}")))?;

            // CatalogTupleInsert(rel, tuple);
            CatalogTupleInsert(mcx, &rel, &mut tuple)?;

            // CommandCounterIncrement();
            backend_access_transam_xact_seams::command_counter_increment::call()?;

            chosen = Some(roident);
            break;
        }

        roident += 1;
    }

    // table_close(rel, ExclusiveLock);  (C keeps no lock note here; the
    // ExclusiveLock is held to commit so concurrent creators serialize.)
    table_close(rel, ExclusiveLock)?;

    Ok(chosen)
}

/// `SearchSysCache1(REPLORIGNAME, CStringGetTextDatum(roname))` ->
/// `ident->roident` (origin.c lines 230-249's `replorigin_by_name` core), or
/// `None` when no tuple is cached.
pub fn syscache_roident_by_name(roname: &str) -> PgResult<Option<Oid>> {
    let scratch = MemoryContext::new("replorigin_by_name lookup");
    let mcx = scratch.mcx();

    // tuple = SearchSysCache1(REPLORIGNAME, CStringGetTextDatum(roname));
    let result: Option<Oid> = match SearchSysCache1(mcx, REPLORIGNAME, SysCacheKey::Str(roname))? {
        // ident = GETSTRUCT(tuple); roident = ident->roident;
        Some(tuple) => {
            let rel = table_open(mcx, REPLICATION_ORIGIN_RELATION_ID, AccessShareLock)?;
            let (val, isnull) = heap_getattr(
                mcx,
                &tuple,
                ANUM_PG_REPLICATION_ORIGIN_ROIDENT as i32,
                &rel.rd_att,
            )?;
            table_close(rel, AccessShareLock)?;
            if isnull {
                return Err(PgError::error("pg_replication_origin.roident is null"));
            }
            Some(val.as_oid())
        }
        None => None,
    };
    Ok(result)
}

/// `SearchSysCache1(REPLORIGIDENT, ObjectIdGetDatum(roident))` ->
/// `text_to_cstring(&ric->roname)` (origin.c `replorigin_by_oid`'s name copy),
/// or `None` when no tuple is cached.
pub fn syscache_roname_by_oid(roident: Oid) -> PgResult<Option<String>> {
    let scratch = MemoryContext::new("replorigin_by_oid name");
    let mcx = scratch.mcx();

    let result: Option<String> =
        match SearchSysCache1(mcx, REPLORIGIDENT, SysCacheKey::Value(scalar_oid(roident)))? {
            Some(tuple) => {
                let rel = table_open(mcx, REPLICATION_ORIGIN_RELATION_ID, AccessShareLock)?;
                let (val, isnull) = heap_getattr(
                    mcx,
                    &tuple,
                    ANUM_PG_REPLICATION_ORIGIN_RONAME as i32,
                    &rel.rd_att,
                )?;
                table_close(rel, AccessShareLock)?;
                if isnull {
                    return Err(PgError::error("pg_replication_origin.roname is null"));
                }
                // text_to_cstring(&ric->roname)
                let s = backend_utils_adt_varlena_seams::text_to_cstring_v::call(mcx, &val)?;
                Some(String::from(s.as_str()))
            }
            None => None,
        };
    Ok(result)
}

/// `table_open(ReplicationOriginRelationId, RowExclusiveLock)` — the drop-leg
/// open (origin.c line 446). The RowExclusiveLock it acquires is held for the
/// transaction by the lock manager, so the per-seam open in
/// [`drop_delete_tuple`] re-pins the relcache entry under the lock already
/// held.
pub fn drop_open_relation() -> PgResult<()> {
    let scratch = MemoryContext::new("replorigin_drop open");
    let mcx = scratch.mcx();
    let rel = table_open(mcx, REPLICATION_ORIGIN_RELATION_ID, RowExclusiveLock)?;
    // Keep the lock (NoLock close), as the surrounding drop holds it to commit.
    table_close(rel, NoLock)?;
    Ok(())
}

/// Whether the `SearchSysCache1(REPLORIGIDENT, roident)` tuple exists
/// (origin.c line 454's `HeapTupleIsValid(tuple)`).
pub fn drop_tuple_exists(roident: Oid) -> PgResult<bool> {
    let scratch = MemoryContext::new("replorigin_drop exists");
    let mcx = scratch.mcx();
    let exists =
        SearchSysCache1(mcx, REPLORIGIDENT, SysCacheKey::Value(scalar_oid(roident)))?.is_some();
    Ok(exists)
}

/// `CatalogTupleDelete(rel, &tuple->t_self)` + `CommandCounterIncrement()` for
/// the located REPLORIGIDENT tuple (origin.c lines 475-479). The C code reads
/// `tuple->t_self` from the syscache tuple; the owned syscache tuple does not
/// carry the on-disk `t_self`, so we relocate the live heap tuple by a
/// dirty-snapshot index scan on `roident` (under the held RowExclusiveLock,
/// the row is stable) to obtain its `t_self`.
pub fn drop_delete_tuple(roident: Oid) -> PgResult<()> {
    let scratch = MemoryContext::new("replorigin_drop delete");
    let mcx = scratch.mcx();

    let rel = table_open(mcx, REPLICATION_ORIGIN_RELATION_ID, RowExclusiveLock)?;

    let dirty = types_snapshot::SnapshotData::sentinel(types_snapshot::SnapshotType::SNAPSHOT_DIRTY);

    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        ANUM_PG_REPLICATION_ORIGIN_ROIDENT,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(roident),
    )?;

    let mut scan = genam::systable_beginscan::call(
        &rel,
        REPLICATION_ORIGIN_IDENT_INDEX,
        true,
        Some(&dirty),
        core::slice::from_ref(&key),
    )?;

    let tid = match genam::systable_getnext::call(mcx, scan.desc_mut())? {
        Some(tuple) => tuple.tuple.t_self,
        None => {
            scan.end()?;
            table_close(rel, NoLock)?;
            return Err(PgError::error(format!(
                "cache lookup failed for replication origin with ID {roident}"
            )));
        }
    };
    scan.end()?;

    // CatalogTupleDelete(rel, &tuple->t_self);
    CatalogTupleDelete(mcx, &rel, tid)?;

    // CommandCounterIncrement();
    backend_access_transam_xact_seams::command_counter_increment::call()?;

    // Keep the lock until commit (the success-path close is NoLock).
    table_close(rel, NoLock)?;
    Ok(())
}

/// `table_close(rel, RowExclusiveLock)` — the already-dropped early-return
/// close (origin.c line 467), releasing the lock since nothing was changed.
pub fn drop_close_relation_keep_unlocked() -> PgResult<()> {
    let scratch = MemoryContext::new("replorigin_drop early close");
    let mcx = scratch.mcx();
    // The relation was opened by drop_open_relation under RowExclusiveLock; the
    // lock is still held by the lock manager. Re-pin and release it.
    let rel = table_open(mcx, REPLICATION_ORIGIN_RELATION_ID, NoLock)?;
    table_close(rel, RowExclusiveLock)?;
    Ok(())
}

/// `table_close(rel, NoLock)` — success-path close, keeping the lock to commit
/// (origin.c line 482). The delete already closed the relation in
/// [`drop_delete_tuple`]; this is a no-op for the owned per-seam model.
pub fn drop_close_relation_nolock() -> PgResult<()> {
    Ok(())
}

/// `ObjectIdGetDatum(roident)` as the syscache key word (`types_datum::Datum`).
fn scalar_oid(roident: Oid) -> types_datum::Datum {
    types_datum::Datum::from_oid(roident)
}

/// `RecoveryInProgress()` (xlog.c) — delegate to the ported xlog owner.
pub fn recovery_in_progress() -> PgResult<bool> {
    Ok(backend_access_transam_xlog_seams::recovery_in_progress::call())
}

/// `IsTransactionState()` (xact.c) — delegate to the ported xact owner.
pub fn is_transaction_state() -> PgResult<bool> {
    Ok(backend_access_transam_xact_seams::is_transaction_state::call())
}

// ---------------------------------------------------------------------------
// lock manager (lmgr) — object & relation locks on ReplicationOriginRelationId.
// origin.c locks the pg_replication_origin catalog around create/drop so the
// roident slot is not reused concurrently.
// ---------------------------------------------------------------------------

/// `LockSharedObject(ReplicationOriginRelationId, roident, 0,
/// AccessExclusiveLock)` (origin.c).
pub fn lock_shared_object_origin(roident: Oid) -> PgResult<()> {
    backend_storage_lmgr_lmgr::LockSharedObject(
        REPLICATION_ORIGIN_RELATION_ID,
        roident,
        0,
        types_storage::lock::AccessExclusiveLock,
    )
}

/// `UnlockSharedObject(ReplicationOriginRelationId, roident, 0,
/// AccessExclusiveLock)` (origin.c).
pub fn unlock_shared_object_origin(roident: Oid) -> PgResult<()> {
    backend_storage_lmgr_lmgr::UnlockSharedObject(
        REPLICATION_ORIGIN_RELATION_ID,
        roident,
        0,
        types_storage::lock::AccessExclusiveLock,
    )
}

/// `LockRelationOid(ReplicationOriginRelationId, RowExclusiveLock)` (origin.c).
pub fn lock_relation_oid_origin() -> PgResult<()> {
    backend_storage_lmgr_lmgr::LockRelationOid(REPLICATION_ORIGIN_RELATION_ID, RowExclusiveLock)
}

/// `UnlockRelationOid(ReplicationOriginRelationId, RowExclusiveLock)` (origin.c).
pub fn unlock_relation_oid_origin() -> PgResult<()> {
    backend_storage_lmgr_lmgr::UnlockRelationOid(REPLICATION_ORIGIN_RELATION_ID, RowExclusiveLock)
}

/// `ReplicationOriginLock` — built-in individual LWLock #40 (lwlocklist.h
/// `PG_LWLOCK(40, ReplicationOrigin)`; runtime `MainLWLockArray` offset 40).
const REPLICATION_ORIGIN_LOCK: usize = 40;

/// `LWLockAcquire(ReplicationOriginLock, mode)` (origin.c). The lock stays held
/// in HELD_LWLOCKS until the matching `LWLockReleaseReplicationOriginLock`
/// (origin.c pairs acquire/release C-style within a function). The seam returns
/// a RAII guard whose `Drop` would release the lock immediately, so it is
/// `forget`-ed here, leaving the lock held (the abort-path `LWLockReleaseAll`
/// is the leak backstop, exactly as in C).
pub fn lwlock_acquire_replication_origin_lock(
    mode: types_storage::LWLockMode,
) -> PgResult<()> {
    let guard = backend_storage_lmgr_lwlock_seams::lwlock_acquire_main::call(
        REPLICATION_ORIGIN_LOCK,
        mode,
    )?;
    core::mem::forget(guard);
    Ok(())
}

/// `LWLockRelease(ReplicationOriginLock)` (origin.c).
pub fn lwlock_release_replication_origin_lock() -> PgResult<()> {
    backend_storage_lmgr_lwlock_seams::lwlock_release_main::call(REPLICATION_ORIGIN_LOCK)
}
