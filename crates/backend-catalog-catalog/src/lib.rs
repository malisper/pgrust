#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// Every fallible function returns the shared `PgResult`, whose `PgError`
// variant is large; boxing it would diverge from the rest of the workspace's
// vocabulary.
#![allow(clippy::result_large_err)]

//! Idiomatic port of `backend/catalog/catalog.c` — routines concerned with
//! catalog naming conventions and other bits of hard-wired knowledge.
//!
//! The pure classification predicates (the fixed lists of catalog, shared, and
//! pinned OIDs, plus the `FirstUnpinnedObjectId` cutoff) are ported verbatim.
//! The `Relation`-taking predicates read the owned `RelationData` fields
//! directly — the C comment that "this function does not perform any catalog
//! accesses" makes that the faithful shape.
//!
//! The OID-generation paths (`GetNewOidWithIndex`, `GetNewRelFileNumber`) and
//! the SQL-callable `pg_nextoid` / `pg_stop_making_pinned_objects` cross to
//! their owners through each owner's seam crate (genam systable scan, varsup
//! OID generator, table/index open, relpath + `access()`, miscadmin/xact-mode
//! globals); each caller's own control flow is ported 1:1.

use backend_utils_error::ereport;
use types_error::{
    ErrorLocation, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_UNDEFINED_COLUMN, ERROR, LOG,
};
use types_core::catalog::AUTH_ID_OID_INDEX_ID;
use types_catalog::catalog::{
    ANUM_PG_CLASS_OID, AUTH_ID_RELATION_ID, AUTH_ID_ROLNAME_INDEX_ID,
    AUTH_MEM_GRANTOR_INDEX_ID, AUTH_MEM_MEM_ROLE_INDEX_ID, AUTH_MEM_OID_INDEX_ID,
    AUTH_MEM_RELATION_ID, AUTH_MEM_ROLE_MEM_INDEX_ID, CLASS_OID_INDEX_ID, DATABASE_NAME_INDEX_ID,
    DATABASE_OID_INDEX_ID, DATABASE_RELATION_ID, DB_ROLE_SETTING_DATID_ROLID_INDEX_ID,
    DB_ROLE_SETTING_RELATION_ID, FIRST_UNPINNED_OBJECT_ID, GLOBALTABLESPACE_OID,
    LARGE_OBJECT_RELATION_ID, NAMESPACE_RELATION_ID, PARAMETER_ACL_OID_INDEX_ID,
    PARAMETER_ACL_PARNAME_INDEX_ID, PARAMETER_ACL_RELATION_ID, PG_CATALOG_NAMESPACE,
    PG_DATABASE_TOAST_INDEX, PG_DATABASE_TOAST_TABLE, PG_DB_ROLE_SETTING_TOAST_INDEX,
    PG_DB_ROLE_SETTING_TOAST_TABLE, PG_PARAMETER_ACL_TOAST_INDEX, PG_PARAMETER_ACL_TOAST_TABLE,
    PG_PUBLIC_NAMESPACE, PG_SHDESCRIPTION_TOAST_INDEX, PG_SHDESCRIPTION_TOAST_TABLE,
    PG_SHSECLABEL_TOAST_INDEX, PG_SHSECLABEL_TOAST_TABLE, PG_SUBSCRIPTION_TOAST_INDEX,
    PG_SUBSCRIPTION_TOAST_TABLE, PG_TABLESPACE_TOAST_INDEX, PG_TABLESPACE_TOAST_TABLE,
    RELATION_RELATION_ID, REPLICATION_ORIGIN_IDENT_INDEX, REPLICATION_ORIGIN_NAME_INDEX,
    REPLICATION_ORIGIN_RELATION_ID, SEC_LABEL_OBJECT_INDEX_ID, SHARED_DEPEND_DEPENDER_INDEX_ID,
    SHARED_DEPEND_REFERENCE_INDEX_ID, SHARED_DESCRIPTION_OBJ_INDEX_ID, SHARED_DESCRIPTION_RELATION_ID,
    SHARED_SEC_LABEL_OBJECT_INDEX_ID, SHARED_SEC_LABEL_RELATION_ID, SUBSCRIPTION_NAME_INDEX_ID,
    SUBSCRIPTION_OBJECT_INDEX_ID, SUBSCRIPTION_RELATION_ID, TABLESPACE_NAME_INDEX_ID,
    TABLESPACE_OID_INDEX_ID, TABLE_SPACE_RELATION_ID, TS_DICTIONARY_RELATION_ID,
};
use types_catalog::catalog_dependency::DEPEND_RELATION_ID;
use types_catalog::catalog_shdepend::SHARED_DEPEND_RELATION_ID;
use types_cluster::PgClassForm;
use types_core::fmgr::F_OIDEQ;
use types_core::primitive::{
    AttrNumber, InvalidOid, Oid, ProcNumber, RelFileNumber, INVALID_PROC_NUMBER, MAIN_FORKNUM,
};
use types_core::catalog::{
    OIDOID, PG_TOAST_NAMESPACE, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP,
    RELPERSISTENCE_UNLOGGED,
};
// `ScanKeyData::sk_argument` is the canonical unified `Datum<'mcx>` (the
// Datum-unification keystone flipped this edge); scan-key construction carries
// the canonical value.
use types_tuple::backend_access_common_heaptuple::Datum;
use types_rel::{FormData_pg_class, Relation, RelationData};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_snapshot::snapshot::{SnapshotData, SnapshotType};
use types_storage::lock::RowExclusiveLock;

use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_access_index_indexam_seams as indexam_seams;
use backend_access_table_table_seams as table_seams;
use backend_access_transam_parallel_rt_seams as parallel_rt_seams;
use backend_access_transam_varsup_seams as varsup_seams;
use backend_catalog_namespace::isTempToastNamespace;
use backend_common_relpath_seams as relpath_seams;
use backend_storage_file_fd_seams as fd_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_init_miscinit_seams as miscinit_seams;
use backend_utils_init_small_seams as init_small_seams;
use backend_utils_misc_superuser_seams as superuser_seams;
use mcx::Mcx;

/// Parameters to determine when to emit a log message in [`GetNewOidWithIndex`].
const GETNEWOID_LOG_THRESHOLD: u64 = 1_000_000;
const GETNEWOID_LOG_MAX_INTERVAL: u64 = 128_000_000;

/// `_PG_init`-class detail dropped: catalog.c is plain backend code. The static
/// catalog OID set keeps `TS_DICTIONARY_RELATION_ID` out of scope, so silence
/// the unused warning the import would otherwise raise.
const _: Oid = TS_DICTIONARY_RELATION_ID;

/// Mirrors C's `__FILE__`/`__LINE__`/`__func__` triple supplied to `errfinish`.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("catalog.c", 0, funcname)
}

/*
 * IsSystemRelation
 *		True iff the relation is either a system catalog or a toast table.
 *		See IsCatalogRelation for the exact definition of a system catalog.
 *
 *		We treat toast tables of user relations as "system relations" for
 *		protection purposes, e.g. you can't change their schemas without
 *		special permissions.  Therefore, most uses of this function are
 *		checking whether allow_system_table_mods restrictions apply.
 *		For other purposes, consider whether you shouldn't be using
 *		IsCatalogRelation instead.
 *
 *		This function does not perform any catalog accesses.
 *		Some callers rely on that!
 */
pub fn IsSystemRelation(relation: &RelationData) -> bool {
    IsSystemClassForm(relation.rd_id, &relation.rd_rel)
}

/*
 * IsSystemClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 *
 * The standalone-tuple ([`PgClassForm`]) and relcache-`rd_rel`
 * ([`FormData_pg_class`]) faces of `Form_pg_class` both reach the same logic;
 * [`IsSystemClass`] is the cross-crate face, [`IsSystemClassForm`] the in-crate
 * `rd_rel` reader.
 */
pub fn IsSystemClass(relid: Oid, reltuple: &PgClassForm) -> bool {
    /* IsCatalogRelationOid is a bit faster, so test that first */
    IsCatalogRelationOid(relid) || IsToastNamespace(reltuple.relnamespace)
}

/// `IsSystemClass(relid, reltuple)` keyed by `relnamespace` only (the aclchk
/// `pg_class_aclmask_ext` face): `IsCatalogRelationOid(relid) ||
/// IsToastNamespace(relnamespace)`.
pub fn IsSystemClassByNamespace(relid: Oid, relnamespace: Oid) -> bool {
    IsCatalogRelationOid(relid) || IsToastNamespace(relnamespace)
}

fn IsSystemClassForm(relid: Oid, reltuple: &FormData_pg_class) -> bool {
    IsCatalogRelationOid(relid) || IsToastClass(reltuple)
}

/*
 * IsCatalogRelation
 *		True iff the relation is a system catalog.
 *
 *		By a system catalog, we mean one that is created during the bootstrap
 *		phase of initdb.  That includes not just the catalogs per se, but
 *		also their indexes, and TOAST tables and indexes if any.
 *
 *		This function does not perform any catalog accesses.
 *		Some callers rely on that!
 */
pub fn IsCatalogRelation(relation: &RelationData) -> bool {
    IsCatalogRelationOid(relation.rd_id)
}

/*
 * IsCatalogRelationOid
 *		True iff the relation identified by this OID is a system catalog.
 *
 *		By a system catalog, we mean one that is created during the bootstrap
 *		phase of initdb.  That includes not just the catalogs per se, but
 *		also their indexes, and their TOAST tables and indexes.
 *
 *		This rule excludes the relations in information_schema; this test is
 *		reliable since an OID wraparound will skip this range of OIDs.
 */
pub fn IsCatalogRelationOid(relid: Oid) -> bool {
    relid < FIRST_UNPINNED_OBJECT_ID
}

/*
 * IsCatalogTextUniqueIndexOid
 *		True iff the relation identified by this OID is a catalog UNIQUE index
 *		having a column of type "text".
 *
 *		The relcache must not use these indexes.  To avoid being itself the
 *		cause of self-deadlock, this doesn't read catalogs; it uses a
 *		hard-coded list with a supporting regression test.
 */
pub fn IsCatalogTextUniqueIndexOid(relid: Oid) -> bool {
    matches!(
        relid,
        x if x == PARAMETER_ACL_PARNAME_INDEX_ID
            || x == REPLICATION_ORIGIN_NAME_INDEX
            || x == SEC_LABEL_OBJECT_INDEX_ID
            || x == SHARED_SEC_LABEL_OBJECT_INDEX_ID
    )
}

/*
 * IsInplaceUpdateRelation
 *		True iff core code performs inplace updates on the relation.
 */
pub fn IsInplaceUpdateRelation(relation: &RelationData) -> bool {
    IsInplaceUpdateOid(relation.rd_id)
}

/*
 * IsInplaceUpdateOid
 *		Like the above, but takes an OID as argument.
 */
pub fn IsInplaceUpdateOid(relid: Oid) -> bool {
    relid == RELATION_RELATION_ID || relid == DATABASE_RELATION_ID
}

/*
 * IsToastRelation
 *		True iff relation is a TOAST support relation (or index).
 *
 *		Does not perform any catalog accesses.
 */
pub fn IsToastRelation(relation: &RelationData) -> bool {
    /*
     * What we actually check is whether the relation belongs to a pg_toast
     * namespace.  Notice this will not say "true" for toast tables belonging
     * to other sessions' temp tables; we expect that other mechanisms will
     * prevent access to those.
     */
    IsToastNamespace(relation.rd_rel.relnamespace)
}

/*
 * IsToastClass
 *		Like the above, but takes a Form_pg_class as argument.
 */
pub fn IsToastClass(reltuple: &FormData_pg_class) -> bool {
    IsToastNamespace(reltuple.relnamespace)
}

/*
 * IsCatalogNamespace
 *		True iff namespace is pg_catalog.
 *
 *		Does not perform any catalog accesses.
 */
pub fn IsCatalogNamespace(namespaceId: Oid) -> bool {
    namespaceId == PG_CATALOG_NAMESPACE
}

/*
 * IsToastNamespace
 *		True iff namespace is pg_toast or my temporary-toast-table namespace.
 *
 *		Does not perform any catalog accesses.
 *
 * Note: this will return false for temporary-toast-table namespaces belonging
 * to other backends.
 */
pub fn IsToastNamespace(namespaceId: Oid) -> bool {
    (namespaceId == PG_TOAST_NAMESPACE) || isTempToastNamespace(namespaceId)
}

/*
 * IsReservedName
 *		True iff name starts with the pg_ prefix.
 */
pub fn IsReservedName(name: &str) -> bool {
    /* ugly coding for speed */
    let b = name.as_bytes();
    b.len() >= 3 && b[0] == b'p' && b[1] == b'g' && b[2] == b'_'
}

/*
 * IsSharedRelation
 *		Given the OID of a relation, determine whether it's supposed to be
 *		shared across an entire database cluster.
 *
 * The set of shared relations is fairly static, so a hand-maintained list of
 * their OIDs (look for BKI_SHARED_RELATION) is used here.
 */
pub fn IsSharedRelation(relationId: Oid) -> bool {
    /* These are the shared catalogs (look for BKI_SHARED_RELATION) */
    if relationId == AUTH_ID_RELATION_ID
        || relationId == AUTH_MEM_RELATION_ID
        || relationId == DATABASE_RELATION_ID
        || relationId == DB_ROLE_SETTING_RELATION_ID
        || relationId == PARAMETER_ACL_RELATION_ID
        || relationId == REPLICATION_ORIGIN_RELATION_ID
        || relationId == SHARED_DEPEND_RELATION_ID
        || relationId == SHARED_DESCRIPTION_RELATION_ID
        || relationId == SHARED_SEC_LABEL_RELATION_ID
        || relationId == SUBSCRIPTION_RELATION_ID
        || relationId == TABLE_SPACE_RELATION_ID
    {
        return true;
    }
    /* These are their indexes */
    if relationId == AUTH_ID_OID_INDEX_ID
        || relationId == AUTH_ID_ROLNAME_INDEX_ID
        || relationId == AUTH_MEM_MEM_ROLE_INDEX_ID
        || relationId == AUTH_MEM_ROLE_MEM_INDEX_ID
        || relationId == AUTH_MEM_OID_INDEX_ID
        || relationId == AUTH_MEM_GRANTOR_INDEX_ID
        || relationId == DATABASE_NAME_INDEX_ID
        || relationId == DATABASE_OID_INDEX_ID
        || relationId == DB_ROLE_SETTING_DATID_ROLID_INDEX_ID
        || relationId == PARAMETER_ACL_OID_INDEX_ID
        || relationId == PARAMETER_ACL_PARNAME_INDEX_ID
        || relationId == REPLICATION_ORIGIN_IDENT_INDEX
        || relationId == REPLICATION_ORIGIN_NAME_INDEX
        || relationId == SHARED_DEPEND_DEPENDER_INDEX_ID
        || relationId == SHARED_DEPEND_REFERENCE_INDEX_ID
        || relationId == SHARED_DESCRIPTION_OBJ_INDEX_ID
        || relationId == SHARED_SEC_LABEL_OBJECT_INDEX_ID
        || relationId == SUBSCRIPTION_NAME_INDEX_ID
        || relationId == SUBSCRIPTION_OBJECT_INDEX_ID
        || relationId == TABLESPACE_NAME_INDEX_ID
        || relationId == TABLESPACE_OID_INDEX_ID
    {
        return true;
    }
    /* These are their toast tables and toast indexes */
    if relationId == PG_DATABASE_TOAST_TABLE
        || relationId == PG_DATABASE_TOAST_INDEX
        || relationId == PG_DB_ROLE_SETTING_TOAST_TABLE
        || relationId == PG_DB_ROLE_SETTING_TOAST_INDEX
        || relationId == PG_PARAMETER_ACL_TOAST_TABLE
        || relationId == PG_PARAMETER_ACL_TOAST_INDEX
        || relationId == PG_SHDESCRIPTION_TOAST_TABLE
        || relationId == PG_SHDESCRIPTION_TOAST_INDEX
        || relationId == PG_SHSECLABEL_TOAST_TABLE
        || relationId == PG_SHSECLABEL_TOAST_INDEX
        || relationId == PG_SUBSCRIPTION_TOAST_TABLE
        || relationId == PG_SUBSCRIPTION_TOAST_INDEX
        || relationId == PG_TABLESPACE_TOAST_TABLE
        || relationId == PG_TABLESPACE_TOAST_INDEX
    {
        return true;
    }
    false
}

/*
 * IsPinnedObject
 *		Given the class + OID identity of a database object, report whether
 *		it is "pinned", that is not droppable because the system requires it.
 *
 * We rely on an OID range test rather than explicit pg_depend rows.
 */
pub fn IsPinnedObject(classId: Oid, objectId: Oid) -> bool {
    /*
     * Objects with OIDs above FirstUnpinnedObjectId are never pinned.  Since
     * the OID generator skips this range when wrapping around, this check
     * guarantees that user-defined objects are never considered pinned.
     */
    if objectId >= FIRST_UNPINNED_OBJECT_ID {
        return false;
    }

    /*
     * Large objects are never pinned.  We need this special case because
     * their OIDs can be user-assigned.
     */
    if classId == LARGE_OBJECT_RELATION_ID {
        return false;
    }

    /* the public namespace is not pinned */
    if classId == NAMESPACE_RELATION_ID && objectId == PG_PUBLIC_NAMESPACE {
        return false;
    }

    /*
     * Databases are never pinned.  We do this intentionally so that template0
     * and template1 can be rebuilt from each other.
     */
    if classId == DATABASE_RELATION_ID {
        return false;
    }

    /*
     * All other initdb-created objects are pinned.  This is overkill but
     * generating only the minimum required set of dependencies seems hard.
     */
    true
}

/*
 * GetNewOidWithIndex
 *		Generate a new OID that is unique within the system relation.
 *
 * Since the OID is not immediately inserted into the table, there is a race
 * condition here; we use SnapshotAny in the test so we see uncommitted rows.
 *
 * Caller must have a suitable lock on the relation.
 */
pub fn GetNewOidWithIndex(
    relation: &RelationData,
    indexId: Oid,
    oidcolumn: AttrNumber,
) -> PgResult<Oid> {
    let mut newOid: Oid;
    let mut collides: bool;
    let mut retries: u64 = 0;
    let mut retries_before_log: u64 = GETNEWOID_LOG_THRESHOLD;

    /* Only system relations are supported */
    debug_assert!(IsSystemRelation(relation));

    /* In bootstrap mode, we don't have any indexes to use */
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return varsup_seams::get_new_object_id::call();
    }

    /*
     * We should never be asked to generate a new pg_type OID during
     * pg_upgrade; doing so would risk collisions with the OIDs it wants to
     * assign.
     */
    debug_assert!(
        !init_small_seams::is_binary_upgrade::call()
            || relation.rd_id != types_catalog::catalog::TYPE_RELATION_ID
    );

    /* SnapshotAny — see notes above */
    let snapshot_any = SnapshotData::sentinel(SnapshotType::SNAPSHOT_ANY);

    /* Generate new OIDs until we find one not in the table */
    loop {
        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        newOid = varsup_seams::get_new_object_id::call()?;

        /*
         * ScanKeyInit(&key, oidcolumn, BTEqualStrategyNumber, F_OIDEQ,
         *             ObjectIdGetDatum(newOid));
         */
        let mut key = ScanKeyData::empty();
        ScanKeyInit(
            &mut key,
            oidcolumn,
            BTEqualStrategyNumber,
            F_OIDEQ,
            Datum::from_oid(newOid),
        )?;
        let keys = [key];

        /*
         * scan = systable_beginscan(relation, indexId, true, SnapshotAny, 1, &key);
         * collides = HeapTupleIsValid(systable_getnext(scan));
         * systable_endscan(scan);
         */
        let mut scan = genam_seams::systable_beginscan::call(
            relation,
            indexId,
            true,
            Some(&snapshot_any),
            &keys,
        )?;
        let scratch = mcx::MemoryContext::new("GetNewOidWithIndex probe");
        collides = genam_seams::systable_getnext::call(scratch.mcx(), scan.desc_mut())?.is_some();
        drop(scratch);
        scan.end()?;

        /*
         * Log that we iterate more than GETNEWOID_LOG_THRESHOLD but have not
         * yet found an OID unused in the relation, with exponentially
         * increasing intervals up to GETNEWOID_LOG_MAX_INTERVAL.
         */
        if retries >= retries_before_log {
            let relname = relation.name();
            ereport(LOG)
                .errmsg(format!(
                    "still searching for an unused OID in relation \"{relname}\""
                ))
                .errdetail_plural(
                    format!(
                        "OID candidates have been checked {retries} time, but no unused OID has been found yet."
                    ),
                    format!(
                        "OID candidates have been checked {retries} times, but no unused OID has been found yet."
                    ),
                    retries,
                )
                .finish(here("GetNewOidWithIndex"))?;

            if retries_before_log * 2 <= GETNEWOID_LOG_MAX_INTERVAL {
                retries_before_log *= 2;
            } else {
                retries_before_log += GETNEWOID_LOG_MAX_INTERVAL;
            }
        }

        retries += 1;

        if !collides {
            break;
        }
    }

    /*
     * If at least one log message is emitted, also log the completion of OID
     * assignment.
     */
    if retries > GETNEWOID_LOG_THRESHOLD {
        let relname = relation.name();
        ereport(LOG)
            .errmsg_plural(
                format!("new OID has been assigned in relation \"{relname}\" after {retries} retry"),
                format!(
                    "new OID has been assigned in relation \"{relname}\" after {retries} retries"
                ),
                retries,
            )
            .finish(here("GetNewOidWithIndex"))?;
    }

    Ok(newOid)
}

/*
 * GetNewRelFileNumber
 *		Generate a new relfilenumber that is unique within the
 *		database of the given tablespace.
 *
 * If the relfilenumber will also be used as the relation's OID, pass the
 * opened pg_class catalog, and this routine guarantees an unused OID within
 * pg_class.  If the result is only a relfilenumber for an existing relation,
 * pass None for pg_class.
 *
 * Note: we don't support using this in bootstrap mode.
 */
pub fn GetNewRelFileNumber(
    mcx: Mcx<'_>,
    reltablespace: Oid,
    pg_class: Option<&RelationData>,
    relpersistence: u8,
) -> PgResult<RelFileNumber> {
    let mut collides: bool;
    let procNumber: ProcNumber;

    /*
     * If we ever get here during pg_upgrade, there's something wrong; all
     * relfilenumber assignments during a binary-upgrade run should be
     * determined by commands in the dump script.
     */
    debug_assert!(!init_small_seams::is_binary_upgrade::call());

    if relpersistence == RELPERSISTENCE_TEMP {
        procNumber = ProcNumberForTempRelations();
    } else if relpersistence == RELPERSISTENCE_UNLOGGED
        || relpersistence == RELPERSISTENCE_PERMANENT
    {
        procNumber = INVALID_PROC_NUMBER;
    } else {
        /* elog(ERROR, "invalid relpersistence: %c", relpersistence); */
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "invalid relpersistence: {}",
                relpersistence as char
            ))
            .into_error());
    }

    /* This logic should match RelationInitPhysicalAddr */
    let spc_oid = if reltablespace != InvalidOid {
        reltablespace
    } else {
        init_small_seams::my_database_table_space::call()
    };
    let db_oid = if spc_oid == GLOBALTABLESPACE_OID {
        InvalidOid
    } else {
        init_small_seams::my_database_id::call()
    };

    /*
     * The relpath will vary based on the backend number, so we must
     * initialize that properly here to make sure that any collisions based on
     * filename are properly detected.
     */
    let backend = procNumber;

    let mut rel_number: RelFileNumber;

    loop {
        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        /* Generate the OID */
        rel_number = match pg_class {
            Some(pg_class) => GetNewOidWithIndex(pg_class, CLASS_OID_INDEX_ID, ANUM_PG_CLASS_OID)?,
            None => varsup_seams::get_new_object_id::call()?,
        };

        /*
         * Check for existing file of same name:
         *   rpath = relpath(rlocator, MAIN_FORKNUM);
         *   if (access(rpath.str, F_OK) == 0) collides = true;
         */
        let rpath = relpath_seams::relpath_backend::call(
            mcx,
            db_oid,
            spc_oid,
            rel_number,
            backend,
            MAIN_FORKNUM,
        )?;

        if fd_seams::access_f_ok::call(rpath.as_str())? == fd_seams::AccessResult::Ok {
            /* definite collision */
            collides = true;
        } else {
            /*
             * If errno is something other than ENOENT, go ahead regardless: if
             * there is a colliding file we will get an smgr failure when we
             * attempt to create the new relation file.
             */
            collides = false;
        }

        if !collides {
            break;
        }
    }

    Ok(rel_number)
}

/// `ProcNumberForTempRelations()` (`storage/procnumber.h`): our own proc number
/// normally, but parallel workers use their leader's.
fn ProcNumberForTempRelations() -> ProcNumber {
    let leader = parallel_rt_seams::parallel_leader_proc_number::call();
    if leader == INVALID_PROC_NUMBER {
        init_small_seams::my_proc_number::call()
    } else {
        leader
    }
}

/*
 * SQL callable interface for GetNewOidWithIndex().
 *
 * The C signature is `Datum pg_nextoid(PG_FUNCTION_ARGS)`; the Datum/fmgr
 * value-layer is the accepted project-wide deferral, so this exposes the
 * already-unwrapped arguments and returns the resulting OID directly.
 */
pub fn pg_nextoid(reloid: Oid, attname: &str, idxoid: Oid) -> PgResult<Oid> {
    /*
     * As this function is not intended to be used during normal running, and
     * only supports system catalogs (which require superuser permissions to
     * modify), just checking for superuser ought to not obstruct valid
     * usecases.
     */
    if !superuser_seams::superuser::call()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("must be superuser to call {}()", "pg_nextoid"))
            .into_error());
    }

    let scratch = mcx::MemoryContext::new("pg_nextoid");
    let mcx = scratch.mcx();

    /*
     * rel = table_open(reloid, RowExclusiveLock);
     * idx = index_open(idxoid, RowExclusiveLock);
     *
     * Both handles release on Drop (the C abort path closes locks at xact
     * abort); on the success path the explicit close below mirrors C.
     */
    let rel: Relation = table_seams::table_open::call(mcx, reloid, RowExclusiveLock)?;
    let idx: Relation = indexam_seams::index_open::call(mcx, idxoid, RowExclusiveLock)?;

    let newoid = pg_nextoid_inner(&rel, &idx, attname)?;

    /* table_close(rel, RowExclusiveLock); index_close(idx, RowExclusiveLock); */
    rel.close(RowExclusiveLock)?;
    idx.close(RowExclusiveLock)?;

    Ok(newoid)
}

fn pg_nextoid_inner(rel: &RelationData, idx: &RelationData, attname: &str) -> PgResult<Oid> {
    if !IsSystemRelation(rel) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("pg_nextoid() can only be used on system catalogs")
            .into_error());
    }

    /* idx->rd_index is always populated for an opened index relation. */
    let rd_index = idx.rd_index.ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("index relation has no rd_index (Form_pg_index)")
            .into_error()
    })?;

    /* if (idx->rd_index->indrelid != RelationGetRelid(rel)) */
    if rd_index.indrelid != rel.rd_id {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "index \"{}\" does not belong to table \"{}\"",
                idx.name(),
                rel.name()
            ))
            .into_error());
    }

    /*
     * atttuple = SearchSysCacheAttName(reloid, NameStr(*attname));
     * if (!HeapTupleIsValid(atttuple)) ereport(ERROR, ...);
     */
    let (attno, atttypid) = match syscache_seams::search_syscache_attname::call(rel.rd_id, attname)?
    {
        Some(column) => column,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{attname}\" of relation \"{}\" does not exist",
                    rel.name()
                ))
                .into_error());
        }
    };

    /* if (attform->atttypid != OIDOID) */
    if atttypid != OIDOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("column \"{attname}\" is not of type oid"))
            .into_error());
    }

    /*
     * if (IndexRelationGetNumberOfKeyAttributes(idx) != 1 ||
     *     idx->rd_index->indkey.values[0] != attno)
     */
    if idx.indnkeyatts() != 1 || rd_index.indkey0 != attno {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "index \"{}\" is not the index for column \"{attname}\"",
                idx.name()
            ))
            .into_error());
    }

    /* newoid = GetNewOidWithIndex(rel, idxoid, attno); */
    GetNewOidWithIndex(rel, idx.rd_id, attno)
}

/*
 * SQL callable interface for StopGeneratingPinnedObjectIds().
 *
 * This is only to be used by initdb. The C signature is
 * `Datum pg_stop_making_pinned_objects(PG_FUNCTION_ARGS)`; this takes no fmgr
 * arguments and returns `()` (the C `PG_RETURN_VOID`).
 */
pub fn pg_stop_making_pinned_objects() -> PgResult<()> {
    /*
     * Belt-and-suspenders check, since StopGeneratingPinnedObjectIds will fail
     * anyway in non-single-user mode.
     */
    if !superuser_seams::superuser::call()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "must be superuser to call {}()",
                "pg_stop_making_pinned_objects"
            ))
            .into_error());
    }

    varsup_seams::stop_generating_pinned_object_ids::call()?;

    Ok(())
}

/// `RelationInvalidatesSnapshotsOnly(relid)` (`utils/cache/syscache.c`,
/// declared via the catalog classification-predicate seam).
///
/// Certain relations that do not have system caches send snapshot
/// invalidation messages in lieu of catcache messages. This is for the benefit
/// of `GetCatalogSnapshot()`, which can then reuse its existing MVCC snapshot
/// for scanning one of those catalogs, rather than taking a new one, if no
/// invalidation has been received.
///
/// Relations that have syscaches need not (and must not) be listed here. The
/// catcache invalidation messages will also flush the snapshot. If you add a
/// syscache for one of these relations, remove it from this list.
///
/// The OID switch is a self-contained, infallible classification predicate
/// over fixed catalog relation OIDs, so it lives with the catalog naming
/// predicates here (the seam was declared on this owner). `inval.c`
/// (`RegisterCatcacheInvalidation` / `PrepareInvalidationState`) calls it
/// across the catalog<->cache cycle through this seam.
fn RelationInvalidatesSnapshotsOnly(relid: Oid) -> bool {
    /// `DescriptionRelationId` (`catalog/pg_description_d.h`).
    const DESCRIPTION_RELATION_ID: Oid = 2609;
    /// `SecLabelRelationId` (`catalog/pg_seclabel_d.h`).
    const SEC_LABEL_RELATION_ID: Oid = 3596;

    matches!(
        relid,
        DB_ROLE_SETTING_RELATION_ID
            | DEPEND_RELATION_ID
            | SHARED_DEPEND_RELATION_ID
            | DESCRIPTION_RELATION_ID
            | SHARED_DESCRIPTION_RELATION_ID
            | SEC_LABEL_RELATION_ID
            | SHARED_SEC_LABEL_RELATION_ID
    )
}

/// Install this crate's seams (`backend-catalog-catalog-seams`) — the catalog.c
/// classification predicates other catalog/storage ports call across a cycle.
pub fn init_seams() {
    backend_catalog_catalog_seams::is_pinned_object::set(IsPinnedObject);
    backend_catalog_catalog_seams::is_catalog_relation_oid::set(IsCatalogRelationOid);
    backend_catalog_catalog_seams::is_catalog_relation::set(IsCatalogRelation);
    backend_catalog_catalog_seams::is_toast_relation::set(IsToastRelation);
    backend_catalog_catalog_seams::is_shared_relation::set(IsSharedRelation);
    backend_catalog_catalog_seams::is_catalog_namespace::set(IsCatalogNamespace);
    backend_catalog_catalog_seams::is_reserved_name::set(|name| Ok(IsReservedName(&name)));
    backend_catalog_catalog_seams::is_system_relation::set(is_system_relation_seam);
    backend_catalog_catalog_seams::is_system_class::set(is_system_class_seam);
    backend_catalog_catalog_seams::is_system_class_by_namespace::set(IsSystemClassByNamespace);
    backend_catalog_catalog_seams::get_new_relfilenumber::set(get_new_relfilenumber_seam);
    backend_catalog_catalog_seams::relation_invalidates_snapshots_only::set(
        RelationInvalidatesSnapshotsOnly,
    );
}

/// Seam adapter for `GetNewRelFileNumber`: the relcache caller
/// (`RelationSetNewRelfilenumber`) passes only the tablespace and persistence
/// (it calls C's `GetNewRelFileNumber(reltablespace, NULL, persistence)`), so
/// `pg_class` is `None`. The transient `relpath` allocation is done in a
/// short-lived context that is dropped before returning the scalar result.
fn get_new_relfilenumber_seam(reltablespace: Oid, relpersistence: i8) -> PgResult<RelFileNumber> {
    let cx = mcx::MemoryContext::new("GetNewRelFileNumber");
    GetNewRelFileNumber(cx.mcx(), reltablespace, None, relpersistence as u8)
}

/// Seam adapter: the cross-crate `is_system_relation` carrier hands a
/// [`Relation`] handle and returns `PgResult` (the frozen contract for callers
/// that propagate with `?`). `IsSystemRelation` reads the underlying
/// [`RelationData`] and is infallible, as in C.
fn is_system_relation_seam(rel: &Relation<'_>) -> PgResult<bool> {
    Ok(IsSystemRelation(rel))
}

/// Seam adapter for `is_system_class`: the frozen contract returns `PgResult`;
/// `IsSystemClass` is infallible.
fn is_system_class_seam(relid: Oid, form: &PgClassForm) -> PgResult<bool> {
    Ok(IsSystemClass(relid, form))
}

#[cfg(test)]
mod tests;
