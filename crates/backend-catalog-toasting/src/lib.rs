//! Port of `src/backend/catalog/toasting.c` (PostgreSQL 18.3) — routines to
//! support creation of TOAST tables.
//!
//! Exported (non-static) C functions: [`AlterTableCreateToastTable`],
//! [`NewHeapCreateToastTable`], [`NewRelationCreateToastTable`],
//! [`BootstrapToastTable`]. Static helpers: `CheckAndCreateToastTable`,
//! `create_toast_table`, `needs_toast_table`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::Mcx;
use types_cluster::RelOptionsToken;
use types_core::primitive::{AttrNumber, Oid};
use types_core::{InvalidOid, OidIsValid, PG_TOAST_NAMESPACE, RELATION_RELATION_ID};
// The toast-table `reloptions` parameter travels as the opaque
// [`RelOptionsToken`] forwarded into the `heap_create_with_catalog` seam.
// The separate `index_create` seam still takes the canonical unified
// `Datum<'mcx>` for its own `reloptions` field, so this crate constructs
// `Datum::null()` for the toast index.
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::{PgError, PgResult};
use types_nodes::execnodes::IndexInfo;
use types_nodes::primnodes::OnCommitAction;
use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_INTERNAL};
use types_rel::Relation;
use types_storage::lock::{
    LOCKMODE, AccessExclusiveLock, NoLock, RowExclusiveLock, ShareLock,
};
use types_tuple::access::{
    RangeVar, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_TOASTVALUE,
};
use types_tuple::heaptuple::{INT4OID, OIDOID};

use backend_access_table_table as table;
use backend_access_common_toastdesc_seams as toastdesc_seams;
use backend_access_table_tableam_seams as tableam_seams;
use backend_access_transam_xact_seams as xact_seams;
use backend_catalog_catalog_seams as catalog_seams;
use backend_catalog_heap_seams as heap_seams;
use backend_catalog_index_seams as index_seams;
use backend_catalog_indexing_seams as indexing_seams;
use backend_utils_init_miscinit_seams as miscinit_seams;

/// `NAMEDATALEN` (pg_config_manual.h).
const NAMEDATALEN: usize = 64;

/// `BYTEAOID` (`catalog/pg_type_d.h`) — the OID of the `bytea` type.
const BYTEAOID: Oid = 17;
/// `BTREE_AM_OID` (`catalog/pg_am_d.h`) — the OID of the btree access method.
const BTREE_AM_OID: Oid = 403;

/// `OID_BTREE_OPS_OID` (`catalog/pg_opclass_d.h`).
const OID_BTREE_OPS_OID: Oid = 1981;
/// `INT4_BTREE_OPS_OID` (`catalog/pg_opclass_d.h`).
const INT4_BTREE_OPS_OID: Oid = 1978;

/// `INDEX_CREATE_IS_PRIMARY` `(1 << 0)` (catalog/index.h).
const INDEX_CREATE_IS_PRIMARY: u16 = 1 << 0;

/// `TYPSTORAGE_PLAIN` `'p'` (catalog/pg_type.h) — type not prepared for
/// toasting.
const TYPSTORAGE_PLAIN: i8 = b'p' as i8;

/// `InvalidCompressionMethod` `'\0'` (access/toast_compression.h).
const InvalidCompressionMethod: i8 = b'\0' as i8;

/// `CreateToastTable` variants. If the table needs a toast table, and doesn't
/// already have one, then create a toast table for it. `reloptions` for the
/// toast table can be passed, too; pass `Datum::null()` (`(Datum) 0`) for
/// default reloptions. The caller is expected to have verified the relation is
/// a table and done any necessary permission checks; this function ends with
/// `CommandCounterIncrement` if it makes any changes. (toasting.c:57)
pub fn AlterTableCreateToastTable<'mcx>(
    mcx: Mcx<'mcx>,
    relOid: Oid,
    reloptions: RelOptionsToken,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    CheckAndCreateToastTable(mcx, relOid, reloptions, lockmode, true, InvalidOid)
}

/// (toasting.c:63)
pub fn NewHeapCreateToastTable<'mcx>(
    mcx: Mcx<'mcx>,
    relOid: Oid,
    reloptions: RelOptionsToken,
    lockmode: LOCKMODE,
    OIDOldToast: Oid,
) -> PgResult<()> {
    CheckAndCreateToastTable(mcx, relOid, reloptions, lockmode, false, OIDOldToast)
}

/// (toasting.c:70)
pub fn NewRelationCreateToastTable(
    mcx: Mcx<'_>,
    relOid: Oid,
    reloptions: RelOptionsToken,
) -> PgResult<()> {
    CheckAndCreateToastTable(mcx, relOid, reloptions, AccessExclusiveLock, false, InvalidOid)
}

/// (toasting.c:77)
fn CheckAndCreateToastTable<'mcx>(
    mcx: Mcx<'mcx>,
    relOid: Oid,
    reloptions: RelOptionsToken,
    lockmode: LOCKMODE,
    check: bool,
    OIDOldToast: Oid,
) -> PgResult<()> {
    let rel = table::table_open(mcx, relOid, lockmode)?;

    // create_toast_table does all the work
    let _ = create_toast_table(
        mcx,
        &rel,
        InvalidOid,
        InvalidOid,
        reloptions,
        lockmode,
        check,
        OIDOldToast,
    )?;

    table::table_close(rel, NoLock)?;

    Ok(())
}

/// Create a toast table during bootstrap. Here we need to prespecify the OIDs
/// of the toast table and its index. (toasting.c:97)
pub fn BootstrapToastTable(
    mcx: Mcx<'_>,
    relName: &str,
    toastOid: Oid,
    toastIndexOid: Oid,
) -> PgResult<()> {
    // rel = table_openrv(makeRangeVar(NULL, relName, -1), AccessExclusiveLock);
    let range_var = makeRangeVar(relName, -1);
    let rel = table::table_openrv(mcx, &range_var, AccessExclusiveLock)?;

    if rel.rd_rel.relkind != RELKIND_RELATION && rel.rd_rel.relkind != RELKIND_MATVIEW {
        // elog(ERROR, "\"%s\" is not a table or materialized view", relName);
        return Err(PgError::error(format!(
            "\"{relName}\" is not a table or materialized view"
        )));
    }

    // create_toast_table does all the work
    if !create_toast_table(
        mcx,
        &rel,
        toastOid,
        toastIndexOid,
        // (Datum) 0 — NULL reloptions
        RelOptionsToken {
            is_null: true,
            bytes: Vec::new(),
        },
        AccessExclusiveLock,
        false,
        InvalidOid,
    )? {
        // elog(ERROR, "\"%s\" does not require a toast table", relName);
        return Err(PgError::error(format!(
            "\"{relName}\" does not require a toast table"
        )));
    }

    table::table_close(rel, NoLock)?;

    Ok(())
}

/// `create_toast_table` --- internal workhorse. `rel` is already opened and
/// locked. `toastOid`/`toastIndexOid` are normally `InvalidOid`, but during
/// bootstrap they can be nonzero to specify hand-assigned OIDs. (toasting.c:126)
#[allow(clippy::too_many_arguments)]
fn create_toast_table<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'_>,
    toastOid: Oid,
    toastIndexOid: Oid,
    reloptions: RelOptionsToken,
    lockmode: LOCKMODE,
    check: bool,
    OIDOldToast: Oid,
) -> PgResult<bool> {
    let relOid = rel.rd_id; // RelationGetRelid(rel)

    // Is it already toasted?
    if rel.rd_rel.reltoastrelid != InvalidOid {
        return Ok(false);
    }

    // Check to see whether the table actually needs a TOAST table.
    if !backend_utils_init_small::globals::IsBinaryUpgrade() {
        // Normal mode, normal check
        if !needs_toast_table(rel)? {
            return Ok(false);
        }
    } else {
        // In binary-upgrade mode, create a TOAST table iff pg_upgrade told us
        // to (ie, a TOAST table OID has been provided).
        if !OidIsValid(backend_utils_init_small::globals::binary_upgrade_next_toast_pg_class_oid()) {
            return Ok(false);
        }
    }

    // If requested check lockmode is sufficient. This is a cross check in case
    // of errors or conflicting decisions in earlier code.
    if check && lockmode != AccessExclusiveLock {
        return Err(PgError::error(
            "AccessExclusiveLock required to add toast table.",
        ));
    }

    // Create the toast table and its index
    let toast_relname = snprintf_name(format!("pg_toast_{relOid}"));
    let toast_idxname = snprintf_name(format!("pg_toast_{relOid}_index"));

    // this is pretty painful...  need a tuple descriptor
    let mut tupdesc = toastdesc_seams::create_template_tuple_desc::call(mcx, 3)?;
    toastdesc_seams::tuple_desc_init_entry::call(
        &mut tupdesc,
        1 as AttrNumber,
        "chunk_id",
        OIDOID,
        -1,
        0,
    )?;
    toastdesc_seams::tuple_desc_init_entry::call(
        &mut tupdesc,
        2 as AttrNumber,
        "chunk_seq",
        INT4OID,
        -1,
        0,
    )?;
    toastdesc_seams::tuple_desc_init_entry::call(
        &mut tupdesc,
        3 as AttrNumber,
        "chunk_data",
        BYTEAOID,
        -1,
        0,
    )?;

    // Ensure that the toast table doesn't itself get toasted, or we'll be
    // toast :-(.  This is essential for chunk_data because type bytea is
    // toastable; hit the other two just to be sure.
    tupdesc.attr_mut(0).attstorage = TYPSTORAGE_PLAIN;
    tupdesc.attr_mut(1).attstorage = TYPSTORAGE_PLAIN;
    tupdesc.attr_mut(2).attstorage = TYPSTORAGE_PLAIN;

    // Toast field should not be compressed
    tupdesc.attr_mut(0).attcompression = InvalidCompressionMethod;
    tupdesc.attr_mut(1).attcompression = InvalidCompressionMethod;
    tupdesc.attr_mut(2).attcompression = InvalidCompressionMethod;

    // Toast tables for regular relations go in pg_toast; those for temp
    // relations go into the per-backend temp-toast-table namespace.
    let namespaceid: Oid =
        if backend_catalog_namespace::isTempOrTempToastNamespace(rel.rd_rel.relnamespace)? {
            backend_catalog_namespace::GetTempToastNamespace()
        } else {
            PG_TOAST_NAMESPACE
        };

    // Toast table is shared if and only if its parent is.
    let shared_relation = rel.rd_rel.relisshared;

    // It's mapped if and only if its parent is, too
    let mapped_relation = rel.is_mapped();

    let toast_am = tableam_seams::table_relation_toast_am::call(rel);

    let toast_relid = heap_seams::heap_create_with_catalog::call(heap_seams::HeapCreateWithCatalogArgs {
        relname: toast_relname,
        relnamespace: namespaceid,
        reltablespace: rel.rd_rel.reltablespace,
        relid: toastOid,
        reltypeid: InvalidOid,
        reloftypeid: InvalidOid,
        ownerid: rel.rd_rel.relowner,
        accessmtd: toast_am,
        tupdesc,
        relkind: RELKIND_TOASTVALUE,
        relpersistence: rel.rd_rel.relpersistence,
        shared_relation,
        mapped_relation,
        oncommit: OnCommitAction::ONCOMMIT_NOOP,
        reloptions,
        use_user_acl: false,
        allow_system_table_mods: true,
        is_internal: true,
        relrewrite: OIDOldToast,
    })?;
    assert!(toast_relid != InvalidOid); // Assert(toast_relid != InvalidOid);

    // make the toast relation visible, else table_open will fail
    xact_seams::command_counter_increment::call()?;

    // ShareLock is not really needed here, but take it anyway
    let toast_rel = table::table_open(mcx, toast_relid, ShareLock)?;

    // Create unique index on chunk_id, chunk_seq.  The slice access routines
    // use both columns; we want it unique to guard against duplicate TOAST
    // chunk OIDs.
    let mut ii_index_attr_numbers = [0 as AttrNumber; types_core::fmgr::INDEX_MAX_KEYS as usize];
    ii_index_attr_numbers[0] = 1;
    ii_index_attr_numbers[1] = 2;
    let indexInfo = IndexInfo {
        ii_NumIndexAttrs: 2,
        ii_NumIndexKeyAttrs: 2,
        ii_IndexAttrNumbers: ii_index_attr_numbers,
        ii_Unique: true,
        ii_NullsNotDistinct: false,
        ii_ReadyForInserts: true,
        ii_CheckedUnchanged: false,
        ii_IndexUnchanged: false,
        ii_Concurrent: false,
        ii_BrokenHotChain: false,
        ii_ParallelWorkers: 0,
        ii_Am: BTREE_AM_OID,
    };

    // collationIds[0] = collationIds[1] = InvalidOid;
    let collationIds: [Oid; 2] = [InvalidOid, InvalidOid];
    // opclassIds[0] = OID_BTREE_OPS_OID; opclassIds[1] = INT4_BTREE_OPS_OID;
    let opclassIds: [Oid; 2] = [OID_BTREE_OPS_OID, INT4_BTREE_OPS_OID];
    // coloptions[0] = coloptions[1] = 0;
    let coloptions: [i16; 2] = [0, 0];

    index_seams::index_create::call(
        &toast_rel,
        index_seams::IndexCreateArgs {
            index_relation_name: toast_idxname,
            index_relation_id: toastIndexOid,
            parent_index_relid: InvalidOid,
            parent_constraint_id: InvalidOid,
            rel_file_number: InvalidOid,
            index_info: indexInfo,
            // list_make2("chunk_id", "chunk_seq")
            index_col_names: vec![String::from("chunk_id"), String::from("chunk_seq")],
            access_method_id: BTREE_AM_OID,
            table_space_id: rel.rd_rel.reltablespace,
            collation_ids: collationIds.to_vec(),
            opclass_ids: opclassIds.to_vec(),
            coloptions: coloptions.to_vec(),
            reloptions: Datum::null(), // (Datum) 0
            flags: INDEX_CREATE_IS_PRIMARY,
            constr_flags: 0,
            allow_system_table_mods: true,
            is_internal: true,
        },
    )?;

    table::table_close(toast_rel, NoLock)?;

    // Store the toast table's OID in the parent relation's pg_class row.
    let class_rel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;

    if !miscinit_seams::is_bootstrap_processing_mode::call() {
        // normal case, use a transactional update
        //
        //   reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relOid));
        //   if (!HeapTupleIsValid(reltup))
        //       elog(ERROR, "cache lookup failed for relation %u", relOid);
        //   ((Form_pg_class) GETSTRUCT(reltup))->reltoastrelid = toast_relid;
        //   CatalogTupleUpdate(class_rel, &reltup->t_self, reltup);
        //   ... heap_freetuple(reltup);
        //
        // The seam performs the syscache copy, the GETSTRUCT field write, and
        // the CatalogTupleUpdate, returning HeapTupleIsValid(reltup); the
        // `cache lookup failed` elog(ERROR) for the !HeapTupleIsValid case stays
        // here.
        if !indexing_seams::set_pg_class_reltoastrelid::call(&class_rel, relOid, toast_relid)? {
            // elog(ERROR, "cache lookup failed for relation %u", relOid);
            return Err(PgError::error(format!(
                "cache lookup failed for relation {relOid}"
            )));
        }
    } else {
        // While bootstrapping, we cannot UPDATE, so overwrite in-place
        //
        //   systable_inplace_update_begin(class_rel, ClassOidIndexId, true,
        //                                 NULL, 1, key, &reltup, &state);
        //   if (!HeapTupleIsValid(reltup))
        //       elog(ERROR, "cache lookup failed for relation %u", relOid);
        //   ((Form_pg_class) GETSTRUCT(reltup))->reltoastrelid = toast_relid;
        //   systable_inplace_update_finish(state, reltup);
        //
        // The seam performs the inplace begin/finish and the GETSTRUCT field
        // write, returning HeapTupleIsValid(reltup); the `cache lookup failed`
        // elog(ERROR) for the !HeapTupleIsValid case stays here.
        if !indexing_seams::set_pg_class_reltoastrelid_inplace::call(&class_rel, relOid, toast_relid)?
        {
            // elog(ERROR, "cache lookup failed for relation %u", relOid);
            return Err(PgError::error(format!(
                "cache lookup failed for relation {relOid}"
            )));
        }
    }

    table::table_close(class_rel, RowExclusiveLock)?;

    // Register dependency from the toast table to the main, so that the toast
    // table will be deleted if the main is.  Skip this in bootstrap mode.
    if !miscinit_seams::is_bootstrap_processing_mode::call() {
        let baseobject = ObjectAddress {
            classId: RELATION_RELATION_ID,
            objectId: relOid,
            objectSubId: 0,
        };
        let toastobject = ObjectAddress {
            classId: RELATION_RELATION_ID,
            objectId: toast_relid,
            objectSubId: 0,
        };

        backend_catalog_pg_depend::recordDependencyOn(
            mcx,
            &toastobject,
            &baseobject,
            DEPENDENCY_INTERNAL,
        )?;
    }

    // Make changes visible
    xact_seams::command_counter_increment::call()?;

    Ok(true)
}

/// Check to see whether the table needs a TOAST table. (toasting.c:400)
fn needs_toast_table(rel: &Relation<'_>) -> PgResult<bool> {
    // No need to create a TOAST table for partitioned tables.
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Ok(false);
    }

    // We cannot allow toasting a shared relation after initdb (because there's
    // no way to mark it toasted in other databases' pg_class).
    if rel.rd_rel.relisshared && !miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(false);
    }

    // Ignore attempts to create toast tables on catalog tables after initdb.
    // Which catalogs get toast tables is explicitly chosen in catalog/pg_*.h.
    if IsCatalogRelation(rel) && !miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(false);
    }

    // Otherwise, let the AM decide.
    Ok(tableam_seams::table_relation_needs_toast_table::call(rel))
}

/// `IsCatalogRelation(relation)` (catalog/catalog.c):
/// `IsCatalogRelationOid(RelationGetRelid(relation))`.
fn IsCatalogRelation(rel: &Relation<'_>) -> bool {
    catalog_seams::is_catalog_relation_oid::call(rel.rd_id)
}

/// `makeRangeVar(NULL, relName, location)` (nodes/makefuncs.c) — a permanent,
/// inheritance-enabled `RangeVar` with no catalog/schema qualification.
fn makeRangeVar(relname: &str, location: i32) -> RangeVar {
    RangeVar {
        catalogname: None,
        schemaname: None,
        relname: String::from(relname),
        inh: true,
        relpersistence: types_core::RELPERSISTENCE_PERMANENT,
        location,
    }
}

/// Build a `NAMEDATALEN`-truncated owned name, mirroring `snprintf(buf,
/// NAMEDATALEN, ...)` into `char buf[NAMEDATALEN]`: at most `NAMEDATALEN - 1`
/// chars plus the terminating NUL (the owned `String` carries no terminator).
fn snprintf_name(mut s: String) -> String {
    if s.len() > NAMEDATALEN - 1 {
        s.truncate(NAMEDATALEN - 1);
    }
    s
}

/// Install this crate's inward seams. `backend-commands-cluster` calls
/// `new_heap_create_toast_table` across the cluster/toasting cycle; wire its
/// real implementation here. The seam decl signature
/// (`mcx, rel_oid, reloptions: RelOptionsToken, lockmode, toast_oid`) matches
/// [`NewHeapCreateToastTable`] exactly, so install it directly.
pub fn init_seams() {
    backend_catalog_toasting_seams::new_heap_create_toast_table::set(NewHeapCreateToastTable);
}
