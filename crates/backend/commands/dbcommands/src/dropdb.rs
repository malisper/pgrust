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
    delete_where(mcx, DbRoleSettingRelationId, DbRoleSettingDatidRolidIndexId, &keys)
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

pub(crate) fn name_key<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<(mcx::PgBox<'mcx, NameData>, ScanKeyData)> {
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
    let newtup = heaptuple::heap_modify_tuple(mcx, tup.as_tuple(), desc, &values, &isnull, &replace)?;
    genam::systable_inplace_update_finish(mcx, state, newtup.as_tuple())?;
    transam_xlog::write::XLogFlush(transam_xlog::XactLastRecEnd())
}

pub fn dropdb(mcx: Mcx<'_>, dbname: &str, missing_ok: bool, force: bool) -> PgResult<()> {
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
        return Ok(());
    };
    let db_id = db.oid;

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
            .errmsg(format!("database \"{dbname}\" is being accessed by other users"))
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
    let mut scan = genam::systable_beginscan(mcx, &pgdbrel, DatabaseNameIndexId, true, None, &[key])?;
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

    checkpointer::RequestCheckpoint(
        transam_xlog::CHECKPOINT_IMMEDIATE
            | transam_xlog::CHECKPOINT_FORCE
            | transam_xlog::CHECKPOINT_WAIT,
    )?;

    let gen = procsignal::EmitProcSignalBarrier(ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE);
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
    Ok(())
}
