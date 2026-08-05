use datum::Datum;
use elog::ereport;
use mcx::Mcx;
use pg_database::{
    Anum_pg_database_datconnlimit, Anum_pg_database_datname, DatabaseNameIndexId,
    DATCONNLIMIT_INVALID_DB,
};
use types_core::catalog::{C_COLLATION_OID, DATABASE_RELATION_ID};
use types_core::fmgr::F_NAMEEQ;
use types_core::{AttrNumber, InvalidOid, Oid};
use types_error::{
    PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_OBJECT_IN_USE, ERRCODE_UNDEFINED_DATABASE,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE,
};
use types_rel::Relation;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessExclusiveLock, RowExclusiveLock};
use types_storage::storage::ProcSignalBarrierType;
use types_tuple::NameData;

use crate::{errdetail_busy_db, get_db_info, loc};

const SubscriptionRelationId: Oid = 6100;
const Anum_pg_subscription_subdbid: i32 = 2;
const SharedDescriptionRelationId: Oid = 2396;
const SharedDescriptionObjIndexId: Oid = 2397;
const SharedSecLabelRelationId: Oid = 3592;
const SharedSecLabelObjectIndexId: Oid = 3593;
const DbRoleSettingRelationId: Oid = 2964;
const DbRoleSettingDatidRolidIndexId: Oid = 2965;

fn oid_key(attno: i32, arg: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(oideq) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(arg);
    key
}

fn delete_where(mcx: Mcx<'_>, relid: Oid, index_id: Oid, keys: &[ScanKeyData]) -> PgResult<()> {
    let rel = table::table_open(mcx, relid, RowExclusiveLock)?;
    let mut scan = genam::systable_beginscan(mcx, &rel, index_id, true, None, keys)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        catalog_indexing::CatalogTupleDelete(&rel, &tup.t_self)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

// DeleteSharedComments (comment.c) / DeleteSharedSecurityLabel (seclabel.c):
// delete rows keyed (objoid, classoid).
fn delete_shared_object_rows(
    mcx: Mcx<'_>,
    relid: Oid,
    index_id: Oid,
    oid: Oid,
    classoid: Oid,
) -> PgResult<()> {
    let keys = [oid_key(1, oid), oid_key(2, classoid)];
    delete_where(mcx, relid, index_id, &keys)
}

// DropSetting (pg_db_role_setting.c) with databaseid valid, roleid invalid.
fn drop_setting(mcx: Mcx<'_>, databaseid: Oid) -> PgResult<()> {
    let keys = [oid_key(1, databaseid)];
    delete_where(
        mcx,
        DbRoleSettingRelationId,
        DbRoleSettingDatidRolidIndexId,
        &keys,
    )
}

// CountDBSubscriptions (pg_subscription.c): RowExclusiveLock held to commit
// so a concurrent CREATE SUBSCRIPTION can't slip in behind the count.
fn count_db_subscriptions(mcx: Mcx<'_>, dbid: Oid) -> PgResult<i32> {
    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;
    let mut n = 0;
    let key = [oid_key(Anum_pg_subscription_subdbid, dbid)];
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &key)?;
    while genam::systable_getnext(mcx, &mut scan)?.is_some() {
        n += 1;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_storage::lock::NoLock)?;
    Ok(n)
}

pub(crate) fn name_key<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
) -> PgResult<(mcx::PgBox<'mcx, NameData>, ScanKeyData)> {
    let mut nd = NameData::default();
    nd.namestrcpy(name);
    let boxed = mcx::PgBox::new_in(nd, mcx);
    let mut key = ScanKeyData::empty();
    key.sk_attno = Anum_pg_database_datname as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(F_NAMEEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(nameeq) failed: {e:?}"));
    key.sk_argument = Datum::from_usize(boxed.as_ref() as *const NameData as usize);
    Ok((boxed, key))
}

fn set_database_invalid(
    mcx: Mcx<'_>,
    pgdbrel: &Relation<'_>,
    dbname: &str,
    db_id: Oid,
) -> PgResult<()> {
    let (_nd, key) = name_key(mcx, dbname)?;
    let Some((tup, state)) =
        genam::systable_inplace_update_begin(mcx, pgdbrel, DatabaseNameIndexId, true, &[key])?
    else {
        return Err(ereport(ERROR)
            .errmsg(format!("cache lookup failed for database {db_id}"))
            .into_error()
            .into());
    };
    let desc = pgdbrel.descr();
    let natts = desc.natts as usize;
    let mut values = vec![Datum::null(); natts];
    let mut isnull = vec![false; natts];
    let mut replace = vec![false; natts];
    values[Anum_pg_database_datconnlimit as usize - 1] = Datum::from_i32(DATCONNLIMIT_INVALID_DB);
    replace[Anum_pg_database_datconnlimit as usize - 1] = true;
    let newtup =
        heaptuple::heap_modify_tuple(mcx, tup.as_tuple(), desc, &values, &isnull, &replace)?;
    genam::systable_inplace_update_finish(mcx, state, newtup.as_tuple())?;
    transam_xlog::write::XLogFlush(transam_xlog::XactLastRecEnd())
}

pub fn dropdb(mcx: Mcx<'_>, dbname: &str, missing_ok: bool, force: bool) -> PgResult<()> {
    dropdb_guts(mcx, dbname, missing_ok, force, true, None).map(|_| ())
}

/// pgrust-only additive entry (docs/design/test-views.md D1 item 2, the one
/// sanctioned extension of this C-parity crate): `dropdb` minus the per-drop
/// `RequestCheckpoint`, for the ephemeral-db janitor's batched reaps — the
/// janitor issues ONE immediate checkpoint per cycle after the batch, and
/// its cycle does not complete until that checkpoint returns.
///
/// Why deferring the checkpoint is safe — C's rationale for the request
/// (dbcommands.c dropdb: "Force a checkpoint to make sure the checkpointer
/// has received the message sent by ForgetDatabaseSyncRequests. On Windows,
/// this also ensures that background procs don't hold any open files, which
/// would cause rmdir() to fail.") discharges as follows:
///
/// 1. Fsync-forget absorption: `ForgetDatabaseSyncRequests` (kept per-drop,
///    above) queues a SYNC_FILTER_REQUEST into the checkpointer's request
///    queue BEFORE `remove_dbtablespaces` unlinks anything, and the queue
///    is absorbed in FIFO order (sync.rs RememberSyncRequest). If the
///    checkpointer fsyncs between a drop and the batch checkpoint, the
///    fsyncgate-hardened loop (sync.rs ProcessSyncRequests) absorbs on
///    ENOENT and finds the cancel — the exact machinery C relies on when a
///    forget races an in-progress checkpoint. Deferral widens the window
///    from ~zero to one janitor cycle; it does not create a new state.
/// 2. Pending-unlink reuse ("or worse, it will delete files that belong to
///    a newly created database with the same OID", the C comment on
///    ForgetDatabaseSyncRequests): stale unlinks are cancelled by the same
///    FIFO filter, and a same-OID recreation inside the one-cycle window
///    additionally requires a pg_database OID-counter wrap — the batch
///    checkpoint closes the window each cycle, exactly as C's per-drop
///    checkpoint closes it per drop.
/// 3. The Windows open-handle/rmdir clause does not apply to this port's
///    targets; the cross-platform half of that concern is the SMGRRELEASE
///    proc-signal barrier, which stays per-drop (below).
/// 4. Crash-replay against already-deleted files needs no checkpoint in
///    either variant: replay records missing pages in the invalid-pages
///    table and XLOG_DBASE_DROP replay clears them — the per-drop WAL
///    sequence (drop record + ForceSyncCommit'd commit) is unchanged here.
///
/// Every other safety step of `dropdb` — occupancy ERROR, template refusal,
/// buffer drop, sync-request forget, SMGRRELEASE barrier, ForceSyncCommit —
/// is preserved verbatim; the C-shaped `dropdb` entry above keeps its exact
/// behavior. Callers of this variant MUST issue
/// `RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE |
/// CHECKPOINT_WAIT)` after their batch.
///
/// `expected_oid`: the janitor selects victims by enumerated oid but drops
/// by name in a later transaction. A same-named database re-minted in that
/// window (drop + re-CREATE under a reused name — the normal test-harness
/// pattern) must NOT inherit the old oid's reap eligibility: when the name
/// resolves (under the AccessExclusiveLock) to a different oid than the
/// caller enumerated, the drop is skipped as a contained no-op — the same
/// shape as C's OID-recheck on the connect path. `None` = drop whatever the
/// name resolves to (the C-shaped behavior).
///
/// Returns true if the database was dropped, false if the drop was skipped
/// (name gone with `missing_ok`, or `expected_oid` mismatch).
pub fn dropdb_skip_checkpoint(
    mcx: Mcx<'_>,
    dbname: &str,
    missing_ok: bool,
    force: bool,
    expected_oid: Option<Oid>,
) -> PgResult<bool> {
    dropdb_guts(mcx, dbname, missing_ok, force, false, expected_oid)
}

/// The `expected_oid` guard's decision, extracted to a pure predicate so the
/// oid-mismatch skip is unit-testable without a booted catalog (the storm
/// gate's HONESTY NOTE, scripts/testmode/janitor-storm.sh: the e2e rig can
/// exercise the re-mint choreography but cannot falsify this comparison).
/// `dropdb_guts` calls it on the oid the name JUST resolved to, under the
/// AccessExclusiveLock that resolution took — behavior-preserving extraction,
/// no lock or catalog state is consulted here.
///
/// Returns true when the drop must be SKIPPED: the caller enumerated a
/// specific victim oid and the name now resolves to a DIFFERENT database
/// (re-minted since enumeration — it earned none of the old oid's reap
/// eligibility). `None` means no expectation: drop whatever the name
/// resolves to (the C-shaped `dropdb` path), never skip.
fn victim_was_reminted(resolved_oid: Oid, expected_oid: Option<Oid>) -> bool {
    match expected_oid {
        Some(expected) => resolved_oid != expected,
        None => false,
    }
}

fn dropdb_guts(
    mcx: Mcx<'_>,
    dbname: &str,
    missing_ok: bool,
    force: bool,
    request_checkpoint: bool,
    expected_oid: Option<Oid>,
) -> PgResult<bool> {
    let pgdbrel = table::table_open(mcx, DATABASE_RELATION_ID, RowExclusiveLock)?;

    let Some(db) = get_db_info(mcx, dbname, AccessExclusiveLock)? else {
        pgdbrel.close(RowExclusiveLock)?;
        if !missing_ok {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!("database \"{dbname}\" does not exist"))
                .into_error()
                .into());
        }
        ereport(NOTICE)
            .errmsg(format!("database \"{dbname}\" does not exist, skipping"))
            .finish(loc("dropdb"))?;
        return Ok(false);
    };
    let db_id = db.oid;

    if victim_was_reminted(db_id, expected_oid) {
        // The name was re-minted since the caller enumerated it: the
        // brand-new database earned none of the caller's eligibility.
        // Release the lock we just took on the innocent database and
        // skip (see the doc comment on `dropdb_skip_checkpoint`).
        lmgr::UnlockSharedObject(DATABASE_RELATION_ID, db_id, 0, AccessExclusiveLock)?;
        pgdbrel.close(RowExclusiveLock)?;
        return Ok(false);
    }

    if !adt_acl::has_privs_of_role(miscinit::GetUserId(), db.datdba)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("must be owner of database {dbname}"))
            .into_error()
            .into());
    }

    if db.datistemplate {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot drop a template database".to_string())
            .into_error()
            .into());
    }

    if db_id == init_small::globals::MyDatabaseId() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg("cannot drop the currently open database".to_string())
            .into_error()
            .into());
    }

    // Replication-slot checks ride the replication lane (no slot subsystem
    // exists, so zero slots is the true count).

    let nsubscriptions = count_db_subscriptions(mcx, db_id)?;
    if nsubscriptions > 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!(
                "database \"{dbname}\" is being used by logical replication subscription"
            ))
            .errdetail(if nsubscriptions == 1 {
                "There is 1 subscription.".to_string()
            } else {
                format!("There are {nsubscriptions} subscriptions.")
            })
            .into_error()
            .into());
    }

    if force {
        procarray::TerminateOtherDBBackends(db_id)?;
    }

    if let Some((notherbackends, npreparedxacts)) = procarray::CountOtherDBBackends(db_id)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!(
                "database \"{dbname}\" is being accessed by other users"
            ))
            .errdetail(errdetail_busy_db(notherbackends, npreparedxacts))
            .into_error()
            .into());
    }

    delete_shared_object_rows(
        mcx,
        SharedDescriptionRelationId,
        SharedDescriptionObjIndexId,
        db_id,
        DATABASE_RELATION_ID,
    )?;
    delete_shared_object_rows(
        mcx,
        SharedSecLabelRelationId,
        SharedSecLabelObjectIndexId,
        db_id,
        DATABASE_RELATION_ID,
    )?;
    drop_setting(mcx, db_id)?;
    pg_shdepend::dropDatabaseDependencies(mcx, db_id)?;

    // Tell the cumulative stats system to forget it immediately, too
    // (dbcommands.c:1816). Registered transactionally: the commit record's
    // stats item is what drops the entry (and, via the dboid cascade, every
    // entry of the database) on standbys (030_stats_cleanup_replica).
    pgstat::database::pgstat_drop_database(db_id);

    set_database_invalid(mcx, &pgdbrel, dbname, db_id)?;

    let (_nd, key) = name_key(mcx, dbname)?;
    let mut scan =
        genam::systable_beginscan(mcx, &pgdbrel, DatabaseNameIndexId, true, None, &[key])?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        panic!("cache lookup failed for database {db_id}");
    };
    catalog_indexing::CatalogTupleDelete(&pgdbrel, &tup.t_self)?;
    genam::systable_endscan(mcx, scan)?;

    // Drop db-specific replication slots (dbcommands.c:1852).
    if slot_seams::replication_slots_drop_db_slots::is_installed() {
        slot_seams::replication_slots_drop_db_slots::call(db_id)?;
    }

    bufmgr::DropDatabaseBuffers(db_id)?;
    smgr::ForgetDatabaseSyncRequests(db_id)?;

    if request_checkpoint {
        checkpointer::RequestCheckpoint(
            transam_xlog::CHECKPOINT_IMMEDIATE
                | transam_xlog::CHECKPOINT_FORCE
                | transam_xlog::CHECKPOINT_WAIT,
        )?;
    }

    let gen =
        procsignal::EmitProcSignalBarrier(ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE);
    procsignal::WaitForProcSignalBarrier(gen)?;

    crate::remove_dbtablespaces(mcx, db_id)?;

    pgdbrel.close(types_storage::lock::NoLock)?;

    // Parked parallel-pool standbys pinned to this database are permanently
    // unclaimable once it is gone; retire them so their PGPROCs return to the
    // bgworker freelist and the pool replenishes fresh standbys.
    if postmaster_seams::parallel_pool_retire_db::is_installed() {
        postmaster_seams::parallel_pool_retire_db::call(db_id);
    }

    xact::ForceSyncCommit();
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Discrimination of the expected_oid skip (owed by the janitor-storm
    /// gate's HONESTY NOTE; RELEASE-effective — plain asserts, no
    /// debug_assert). Kills the live mutations of the guard: comparison
    /// flipped or dropped (same-oid case), mismatch ignored (re-mint case),
    /// and `None` treated as a mismatch (the C-shaped path must never skip).
    #[test]
    fn expected_oid_guard_discriminates() {
        let enumerated: Oid = 90101;
        let reminted: Oid = 90102;

        // The name still resolves to the enumerated victim: proceed.
        assert!(
            !victim_was_reminted(enumerated, Some(enumerated)),
            "guard must let the enumerated victim drop"
        );
        // Re-minted under the same name (different oid): SKIP — the new
        // database inherited none of the old oid's eligibility.
        assert!(
            victim_was_reminted(reminted, Some(enumerated)),
            "guard must skip a re-minted (oid-mismatched) database"
        );
        // No expectation = the C-shaped drop-by-name entry: never skip.
        assert!(
            !victim_was_reminted(reminted, None),
            "C-shaped dropdb (expected_oid=None) must never skip on oid"
        );
    }
}
