// toasting.c; bootstrap arms are unreached here.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use execindexing::IndexInfo;
use mcx::Mcx;
use types_core::{
    AttrNumber, InvalidOid, Oid, INDEX_MAX_KEYS, PG_TOAST_NAMESPACE, RELATION_RELATION_ID,
};
use types_error::PgResult;
use types_rel::{
    AccessExclusiveLock, NoLock, Relation, RowExclusiveLock, ShareLock, RELKIND_PARTITIONED_TABLE,
    RELKIND_TOASTVALUE,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::TYPSTORAGE_PLAIN;

const OIDOID: Oid = 26;
const INT4OID: Oid = 23;
const BYTEAOID: Oid = 17;
const OID_BTREE_OPS_OID: Oid = 1981;
const INT4_BTREE_OPS_OID: Oid = 1978;
const Anum_pg_class_reltoastrelid: usize = 14;
const InvalidCompressionMethod: i8 = 0;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: toasting.c {what}")
}

// toasting.c:342/:362 elog(ERROR, "cache lookup failed for relation %u") --
// catchable, SQLSTATE XX000 (elog's default for ERROR), not a backend abort.
#[cold]
#[inline(never)]
fn relation_lookup_failed(relOid: types_core::Oid) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::error(format!("cache lookup failed for relation {relOid}")))
}

pub fn NewRelationCreateToastTable<'mcx>(
    mcx: Mcx<'mcx>,
    relOid: Oid,
    reloptions: Option<&[u8]>,
) -> PgResult<()> {
    CheckAndCreateToastTable(mcx, relOid, reloptions, AccessExclusiveLock, false, InvalidOid)
}

pub fn AlterTableCreateToastTable<'mcx>(
    mcx: Mcx<'mcx>,
    relOid: Oid,
    reloptions: Option<&[u8]>,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<()> {
    CheckAndCreateToastTable(mcx, relOid, reloptions, lockmode, true, InvalidOid)
}

// toasting.c:64-68: the transient heap of a table rewrite (CLUSTER, VACUUM
// FULL, ALTER TABLE rewrites) gets its toast table with relrewrite =
// OIDOldToast, the old heap's toast table.
pub fn NewHeapCreateToastTable<'mcx>(
    mcx: Mcx<'mcx>,
    relOid: Oid,
    reloptions: Option<&[u8]>,
    lockmode: types_rel::LOCKMODE,
    OIDOldToast: Oid,
) -> PgResult<()> {
    CheckAndCreateToastTable(mcx, relOid, reloptions, lockmode, false, OIDOldToast)
}

fn CheckAndCreateToastTable<'mcx>(
    mcx: Mcx<'mcx>,
    relOid: Oid,
    reloptions: Option<&[u8]>,
    lockmode: types_rel::LOCKMODE,
    check: bool,
    OIDOldToast: Oid,
) -> PgResult<()> {
    let rel = table::table_open(mcx, relOid, lockmode)?;
    create_toast_table(mcx, &rel, reloptions, lockmode, check, OIDOldToast)?;
    rel.close(NoLock)
}

fn create_toast_table<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    reloptions: Option<&[u8]>,
    lockmode: types_rel::LOCKMODE,
    check: bool,
    OIDOldToast: Oid,
) -> PgResult<bool> {
    let relOid = rel.rd_id;

    if rel.rd_rel.reltoastrelid != InvalidOid {
        return Ok(false);
    }
    if !init_small::globals::IsBinaryUpgrade() {
        if !needs_toast_table(rel) {
            return Ok(false);
        }
    } else if !catalog_heap::NextToastPgClassOidIsSet() {
        // In binary-upgrade mode, create a TOAST table iff pg_upgrade
        // provided a TOAST table OID (the old cluster had one).
        return Ok(false);
    }
    // toasting.c:192-193 elog(ERROR, ...): a catchable cross-check error
    // (SQLSTATE XX000), not a backend abort.  Reached with a preset toast
    // OID in binary-upgrade mode by ALTER TABLE ... SET (reloptions)
    // (ShareUpdateExclusiveLock).
    if check && lockmode != AccessExclusiveLock {
        return Err(Box::new(types_error::PgError::error(
            "AccessExclusiveLock required to add toast table.",
        )));
    }

    let toast_relname = format!("pg_toast_{relOid}");
    let toast_idxname = format!("pg_toast_{relOid}_index");

    let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, 3)?;
    tupdesc::TupleDescInitEntry(&mut tupdesc, 1 as AttrNumber, Some("chunk_id"), OIDOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut tupdesc, 2 as AttrNumber, Some("chunk_seq"), INT4OID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut tupdesc, 3 as AttrNumber, Some("chunk_data"), BYTEAOID, -1, 0)?;
    for i in 0..3 {
        let att = tupdesc.attr_mut(i);
        att.attstorage = TYPSTORAGE_PLAIN;
        att.attcompression = InvalidCompressionMethod;
        tupdesc::populate_compact_attribute(&mut tupdesc, i);
    }

    let namespaceid = if catalog_namespace::isTempOrTempToastNamespace(rel.rd_rel.relnamespace) {
        catalog_namespace::GetTempToastNamespace()
    } else {
        PG_TOAST_NAMESPACE
    };

    let toast_relid = catalog_heap::heap_create_with_catalog(
        mcx,
        &catalog_heap::HeapCreateParams {
            relname: &toast_relname,
            relnamespace: namespaceid,
            reltablespace: rel.rd_rel.reltablespace,
            ownerid: rel.rd_rel.relowner,
            accessmtd: tableam::table_relation_toast_am(rel),
            relkind: RELKIND_TOASTVALUE,
            relpersistence: rel.rd_rel.relpersistence,
            reloftype: InvalidOid,
            // "Toast table is shared if and only if its parent is" (toasting.c:241);
            // after initdb heap_create refuses it exactly as C's relcache does.
            shared: rel.rd_rel.relisshared,
            // "It's mapped if and only if its parent is, too" (toasting.c:244).
            mapped: rel.is_mapped(),
            allow_system_table_mods: true,
            reloptions,
            // toasting.c:266 relrewrite = OIDOldToast (heap.c:1348).
            relrewrite: OIDOldToast,
            use_user_acl: false,
            is_internal: true,
        },
        &tupdesc,
    )?;
    debug_assert!(toast_relid != InvalidOid);

    xact::CommandCounterIncrement()?;

    let toast_rel = table::table_open(mcx, toast_relid, ShareLock)?;

    let mut attnums = [0 as AttrNumber; INDEX_MAX_KEYS as usize];
    attnums[0] = 1;
    attnums[1] = 2;
    let mut indexInfo = IndexInfo {
        ii_NumIndexAttrs: 2,
        ii_AmCache: None,
        ii_NumIndexKeyAttrs: 2,
        ii_IndexAttrNumbers: attnums,
        ii_Expressions: types_nodes::NodeList::nil(),
        ii_ExpressionsState: mcx::PgVec::new_in(mcx),
        ii_Predicate: types_nodes::NodeList::nil(),
        ii_PredicateState: None,
        ii_Unique: true,
        ii_NullsNotDistinct: false,
        ii_ReadyForInserts: true,
        ii_Summarizing: false,
        ii_Concurrent: false,
        ii_BrokenHotChain: false,
        ii_UniqueOps: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueProcs: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueStrats: [0; INDEX_MAX_KEYS as usize],
        ii_HasExclusion: false,
        ii_ExclusionOps: [0; INDEX_MAX_KEYS as usize],
        ii_ExclusionProcs: [0; INDEX_MAX_KEYS as usize],
        ii_ExclusionStrats: [0; INDEX_MAX_KEYS as usize],
        ii_WithoutOverlaps: false,
        ii_CheckedUnchanged: false,
        ii_IndexUnchanged: false,
    };
    let collationIds = [InvalidOid, InvalidOid];
    let opclassIds = [OID_BTREE_OPS_OID, INT4_BTREE_OPS_OID];
    let coloptions = [0i16, 0i16];

    catalog_index::index_create(
        mcx,
        &toast_rel,
        &toast_idxname,
        InvalidOid,
        &mut indexInfo,
        &["chunk_id", "chunk_seq"],
        catalog_index::BTREE_AM_OID,
        rel.rd_rel.reltablespace,
        &collationIds,
        &opclassIds,
        &coloptions,
        &catalog_index::IndexCreateExtra {
            flags: catalog_index::INDEX_CREATE_IS_PRIMARY,
            constr_flags: 0,
            allow_system_table_mods: true,
            is_internal: true,
            parent_index_relid: InvalidOid,
            parent_constraint_id: InvalidOid,
            reloptions: None,
            opclass_options: None,
            stattargets: None,
            old_number: types_core::InvalidRelFileNumber,
        },
    )?;

    toast_rel.close(NoLock)?;

    // Store the toast table's OID in the parent's pg_class row.
    let class_rel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        unported("create_toast_table: bootstrap inplace reltoastrelid update");
    }
    {
        let mut key = ScanKeyData::empty();
        key.sk_attno = 1;
        key.sk_strategy = BTEqualStrategyNumber;
        key.sk_collation = 0;
        key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
            .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
        key.sk_argument = Datum::from_oid(relOid);
        let mut scan = genam::systable_beginscan(
            mcx,
            &class_rel,
            catalog::ClassOidIndexId,
            true,
            None,
            core::slice::from_ref(&key),
        )?;
        let Some(reltup) = genam::systable_getnext(mcx, &mut scan)? else {
            return Err(relation_lookup_failed(relOid));
        };
        let natts = class_rel.descr().natts as usize;
        let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, Datum::null());
        isnull.resize(natts, false);
        replace.resize(natts, false);
        values[Anum_pg_class_reltoastrelid - 1] = Datum::from_oid(toast_relid);
        replace[Anum_pg_class_reltoastrelid - 1] = true;
        let mut newtup = heaptuple::heap_modify_tuple(
            mcx,
            reltup,
            class_rel.descr(),
            &values,
            &isnull,
            &replace,
        )?;
        let otid = reltup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &class_rel, &otid, &mut newtup)?;
    }
    class_rel.close(RowExclusiveLock)?;

    let baseobject = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, relOid);
    let toastobject = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, toast_relid);
    pg_depend::recordDependencyOn(
        mcx,
        &toastobject,
        &baseobject,
        pg_depend::DependencyType::Internal,
    )?;

    xact::CommandCounterIncrement()?;
    Ok(true)
}

fn needs_toast_table(rel: &Relation<'_>) -> bool {
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return false;
    }
    if rel.rd_rel.relisshared && !miscinit_seams::is_bootstrap_processing_mode::call() {
        return false;
    }
    if catalog::IsCatalogRelation(rel) && !miscinit_seams::is_bootstrap_processing_mode::call() {
        return false;
    }
    tableam::table_relation_needs_toast_table(rel)
}
