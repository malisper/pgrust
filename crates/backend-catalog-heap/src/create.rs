//! The relation-creation core of `catalog/heap.c`: `heap_create_with_catalog`
//! and its `AddNewRelationType` / `AddNewRelationTuple` / `InsertPgClassTuple` /
//! `AddNewAttributeTuples` / `InsertPgAttributeTuples` helpers, ported against
//! the K1/K2/K3 catalog-write carrier keystone.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use backend_utils_error::{elog, ereport};
use mcx::Mcx;
use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_NORMAL};
use types_catalog::pg_attribute::PgAttributeInsertRow;
use types_catalog::pg_class::PgClassInsertRow;
use types_core::primitive::{InvalidOid, Oid, OidIsValid, RelFileNumber, TransactionId};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_DUPLICATE_TABLE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERROR,
};
use types_nodes::parsenodes::ObjectType;
use types_nodes::primnodes::OnCommitAction;
use types_rel::Relation;
use types_storage::lock::{AccessExclusiveLock, NoLock, RowExclusiveLock};
use backend_utils_cache_lsyscache::relation::get_relname_relid;
use types_tuple::access::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE, RELKIND_VIEW,
};
use types_tuple::heaptuple::{TupleDescData, DEFAULT_COLLATION_OID, RECORDOID};

use crate::{
    heap_create, namestrcpy, sys_att, AccessMethodRelationId, AttributeRelationId,
    CollationRelationId, NamespaceRelationId, RelationRelationId, TypeRelationId,
    CHKATYPE_ANYARRAY, DEFAULT_TYPDELIM, F_ARRAY_IN, F_ARRAY_OUT, F_ARRAY_RECV, F_ARRAY_SEND,
    F_ARRAY_SUBSCRIPT_HANDLER, F_ARRAY_TYPANALYZE, F_RECORD_IN, F_RECORD_OUT, F_RECORD_RECV,
    F_RECORD_SEND, GLOBALTABLESPACE_OID, RELKIND_HAS_STORAGE, RELKIND_HAS_TABLE_AM,
    RelFileNumberIsValid, TYPALIGN_DOUBLE, TYPCATEGORY_ARRAY, TYPCATEGORY_COMPOSITE, TYPSTORAGE_EXTENDED,
    TYPTYPE_BASE_C, TYPTYPE_COMPOSITE_C, CheckAttributeNamesTypes,
};

/// The on-disk varlena header bytes of an `ArrayType` (the repo's `ArrayType`
/// is exactly the 16-byte `{vl_len_, ndim, dataoffset, elemtype}` header; the
/// catalog producers store array-typed columns at header fidelity — mirroring
/// `backend-catalog-indexing`'s `arraytype_header_bytes`).
fn arraytype_header_bytes(arr: &types_array::ArrayType) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&arr.vl_len_.to_ne_bytes());
    out[4..8].copy_from_slice(&arr.ndim.to_ne_bytes());
    out[8..12].copy_from_slice(&arr.dataoffset.to_ne_bytes());
    out[12..16].copy_from_slice(&arr.elemtype.to_ne_bytes());
    out
}

/// `ObjectAddressSet(object, classId, objectId)` (objectaddress.h).
fn object_address(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// `ObjectAddressSubSet(object, classId, objectId, subId)` (objectaddress.h).
fn object_subaddress(class_id: Oid, object_id: Oid, sub_id: i32) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: sub_id,
    }
}

/* --------------------------------
 *		InsertPgClassTuple
 *
 *		Construct and insert a new tuple in pg_class.
 *
 * Caller has already opened and locked pg_class.  Tuple data is taken from
 * new_rel_desc->rd_rel, except for the variable-width fields which are not
 * present in a cached reldesc.  relacl and reloptions are passed in Datum form;
 * pass `None` to set them to NULL.
 * --------------------------------
 */
///
/// In the owned model the K1 producer `catalog_tuple_insert_pg_class` forms the
/// pg_class heap tuple from a typed [`PgClassInsertRow`]. This routine builds
/// that row from the live relcache `rd_rel` (the C reads `new_rel_desc->rd_rel`)
/// plus the AddNewRelationTuple-supplied `reltype`/`reloftype`/`relowner`/
/// `relfrozenxid`/`relminmxid`/`relpages`/etc. The trimmed `FormData_pg_class`
/// the relcache carries does not hold the write-only columns
/// (`reltype`/`reloftype`/`relchecks`/`relhasrules`/`relhastriggers`/
/// `relforcerowsecurity`/`relrewrite`/`relminmxid`), so they are passed
/// explicitly by [`AddNewRelationTuple`] (the values the C scribbles on `rd_rel`
/// just before `InsertPgClassTuple`); a freshly-created relation has the
/// constant defaults (`relchecks = 0`, `relhasrules/relhastriggers/
/// relforcerowsecurity = false`).
pub fn InsertPgClassTuple<'mcx>(
    mcx: Mcx<'mcx>,
    pg_class_desc: &Relation<'mcx>,
    new_rel_desc: &Relation<'mcx>,
    new_rel_oid: Oid,
    write: &PgClassWriteFields,
    relacl: Option<types_array::ArrayType>,
    reloptions: Option<Vec<u8>>,
) -> PgResult<()> {
    let rd_rel = &new_rel_desc.rd_rel;

    // C: `if (relacl != (Datum) 0) values[relacl] = relacl`. The repo's
    // `ArrayType` is the 16-byte on-disk varlena header (the element payload is
    // modeled at header fidelity by the catalog producers — same level as
    // pg_namespace.nspacl / pg_type.typacl). `None` ⇒ SQL NULL.
    let relacl: Option<Vec<u8>> = relacl.map(|arr| arraytype_header_bytes(&arr).to_vec());

    let row = PgClassInsertRow {
        oid: new_rel_oid,
        relname: namestrcpy(rd_rel.relname.as_str()),
        relnamespace: rd_rel.relnamespace,
        reltype: write.reltype,
        reloftype: write.reloftype,
        relowner: write.relowner,
        relam: rd_rel.relam,
        relfilenode: rd_rel.relfilenode,
        reltablespace: rd_rel.reltablespace,
        relpages: write.relpages,
        reltuples: write.reltuples,
        relallvisible: write.relallvisible,
        relallfrozen: write.relallfrozen,
        reltoastrelid: rd_rel.reltoastrelid,
        relhasindex: rd_rel.relhasindex,
        relisshared: rd_rel.relisshared,
        relpersistence: rd_rel.relpersistence as i8,
        relkind: rd_rel.relkind as i8,
        relnatts: new_rel_desc.rd_att.natts as i16,
        relchecks: 0,
        relhasrules: false,
        relhastriggers: false,
        relrowsecurity: rd_rel.relrowsecurity,
        relforcerowsecurity: false,
        relhassubclass: rd_rel.relhassubclass,
        relispopulated: rd_rel.relispopulated,
        relreplident: rd_rel.relreplident as i8,
        relispartition: write.relispartition,
        relrewrite: write.relrewrite,
        relfrozenxid: write.relfrozenxid,
        relminmxid: write.relminmxid,
        relacl,
        reloptions,
    };

    // tup = heap_form_tuple(RelationGetDescr(pg_class_desc), values, nulls);
    // CatalogTupleInsert(pg_class_desc, tup); heap_freetuple(tup);
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_class::call(mcx, pg_class_desc, &row)
}

/// The pg_class columns that `AddNewRelationTuple` writes onto `rd_rel` just
/// before `InsertPgClassTuple` (the ones the trimmed relcache `FormData_pg_class`
/// does not carry, or that AddNewRelationTuple overrides).
pub struct PgClassWriteFields {
    pub relpages: i32,
    pub reltuples: f32,
    pub relallvisible: i32,
    pub relallfrozen: i32,
    pub relfrozenxid: TransactionId,
    pub relminmxid: u32,
    pub relowner: Oid,
    pub reltype: Oid,
    pub reloftype: Oid,
    pub relispartition: bool,
    pub relrewrite: Oid,
}

/* --------------------------------
 *		AddNewRelationTuple
 *
 *		this registers the new relation in the catalogs by adding a tuple to
 *		pg_class.
 * --------------------------------
 */
pub fn AddNewRelationTuple<'mcx>(
    mcx: Mcx<'mcx>,
    pg_class_desc: &Relation<'mcx>,
    new_rel_desc: &Relation<'mcx>,
    new_rel_oid: Oid,
    new_type_oid: Oid,
    reloftype: Oid,
    relowner: Oid,
    relkind: u8,
    relfrozenxid: TransactionId,
    relminmxid: u32,
    relrewrite: Oid,
    relacl: Option<types_array::ArrayType>,
    reloptions: Option<Vec<u8>>,
) -> PgResult<()> {
    /*
     * first we update some of the information in our uncataloged relation's
     * relation descriptor.
     */

    /* The relation is empty */
    let mut relpages = 0;
    let mut reltuples = -1.0f32;
    let relallvisible = 0;
    let relallfrozen = 0;

    /* Sequences always have a known size */
    if relkind == RELKIND_SEQUENCE {
        relpages = 1;
        reltuples = 1.0;
    }

    /*
     * fill rd_att's type ID with something sane even if reltype is zero
     * (C: new_rel_desc->rd_att->tdtypeid = new_type_oid ? new_type_oid :
     * RECORDOID; tdtypmod = -1).
     *
     * Behaviour-preserving omission: in this owned model the pg_class heap
     * tuple is formed by the K1 producer from the typed `PgClassInsertRow`
     * (reltype = new_type_oid), NOT off the live relcache `rd_att`, and the
     * immediately-following `AddNewAttributeTuples` reads only the per-column
     * `Form_pg_attribute`s of the tuple descriptor, never `tdtypeid`/`tdtypmod`.
     * The `rd_att` write is purely relcache-entry bookkeeping that the next
     * relcache rebuild (after CommandCounterIncrement) reloads from the
     * catalog (`reltype` + pg_attribute) anyway, so there is no observable
     * difference. The trimmed relcache entry exposes no `tdtypeid` setter; were
     * one needed it would be a relcache seam. (`new_type_oid` is recorded as
     * `reltype` in the pg_class row below; an absent reltype maps to RECORDOID
     * when the descriptor is rematerialized.)
     */
    let _ = RECORDOID; // (documents the C `new_type_oid ? new_type_oid : RECORDOID`)

    let write = PgClassWriteFields {
        relpages,
        reltuples,
        relallvisible,
        relallfrozen,
        relfrozenxid,
        relminmxid,
        relowner,
        reltype: new_type_oid,
        reloftype,
        relispartition: false,
        relrewrite,
    };

    /* Now build and insert the tuple */
    InsertPgClassTuple(
        mcx,
        pg_class_desc,
        new_rel_desc,
        new_rel_oid,
        &write,
        relacl,
        reloptions,
    )
}

/* --------------------------------
 *		AddNewRelationType -
 *
 *		define a composite type corresponding to the new relation
 * --------------------------------
 */
pub fn AddNewRelationType(
    type_name: &str,
    type_namespace: Oid,
    new_rel_oid: Oid,
    new_rel_kind: u8,
    ownerid: Oid,
    new_row_type: Oid,
    new_array_type: Oid,
) -> PgResult<ObjectAddress> {
    use backend_catalog_pg_type::TypeCreate;
    use types_catalog::pg_type::TypeCreateParams;

    TypeCreate(TypeCreateParams {
        new_type_oid: new_row_type, /* optional predetermined OID */
        type_name: String::from(type_name),
        type_namespace,
        relation_oid: new_rel_oid,
        relation_kind: new_rel_kind as i8,
        owner_id: ownerid,
        internal_size: -1, /* internal size (varlena) */
        type_type: TYPTYPE_COMPOSITE_C,
        type_category: TYPCATEGORY_COMPOSITE,
        type_preferred: false, /* composite types are never preferred */
        type_delim: DEFAULT_TYPDELIM,
        input_procedure: F_RECORD_IN,
        output_procedure: F_RECORD_OUT,
        receive_procedure: F_RECORD_RECV,
        send_procedure: F_RECORD_SEND,
        typmodin_procedure: InvalidOid,
        typmodout_procedure: InvalidOid,
        analyze_procedure: InvalidOid,
        subscript_procedure: InvalidOid,
        element_type: InvalidOid, /* array element type - irrelevant */
        is_implicit_array: false,
        array_type: new_array_type,
        base_type: InvalidOid,
        default_type_value: None,
        default_type_bin: None,
        passed_by_value: false,
        alignment: TYPALIGN_DOUBLE, /* alignment - must be the largest! */
        storage: TYPSTORAGE_EXTENDED,
        type_mod: -1,
        typ_ndims: 0,
        type_not_null: false,
        type_collation: InvalidOid, /* rowtypes never have a collation */
    })
}

/* --------------------------------
 *		InsertPgAttributeTuples
 *
 *		Construct and insert a set of tuples in pg_attribute.
 * --------------------------------
 */
///
/// `tupdesc` contains the attributes to insert. (`tupdesc_extra` is NULL at
/// both heap.c call sites, so it is omitted; `attstattarget` / `attoptions` are
/// inserted SQL NULL.) The K2 producer `catalog_insert_pg_attribute_tuples`
/// batches the multi-insert with index maintenance; the per-row `attrelid` is
/// pre-resolved here (the C `new_rel_oid != InvalidOid` selection).
pub fn InsertPgAttributeTuples<'mcx>(
    mcx: Mcx<'mcx>,
    pg_attribute_rel: &Relation<'mcx>,
    tupdesc: &TupleDescData<'_>,
    new_rel_oid: Oid,
) -> PgResult<()> {
    let mut rows: Vec<PgAttributeInsertRow> = Vec::with_capacity(tupdesc.natts.max(0) as usize);
    for i in 0..tupdesc.natts as usize {
        let attrs = tupdesc.attr(i);

        let attrelid = if OidIsValid(new_rel_oid) {
            new_rel_oid
        } else {
            attrs.attrelid
        };

        rows.push(PgAttributeInsertRow {
            attrelid,
            attname: pad_name(attrs.attname.name_str()),
            atttypid: attrs.atttypid,
            attlen: attrs.attlen,
            attnum: attrs.attnum,
            atttypmod: attrs.atttypmod,
            attndims: attrs.attndims,
            attbyval: attrs.attbyval,
            attalign: attrs.attalign,
            attstorage: attrs.attstorage,
            attcompression: attrs.attcompression,
            attnotnull: attrs.attnotnull,
            atthasdef: attrs.atthasdef,
            atthasmissing: attrs.atthasmissing,
            attidentity: attrs.attidentity,
            attgenerated: attrs.attgenerated,
            attisdropped: attrs.attisdropped,
            attislocal: attrs.attislocal,
            attinhcount: attrs.attinhcount,
            attcollation: attrs.attcollation,
            // tupdesc_extra is NULL at both call sites: attstattarget /
            // attoptions inserted SQL NULL.
            attstattarget: None,
            attoptions: None,
        });
    }

    backend_catalog_indexing_seams::catalog_insert_pg_attribute_tuples::call(
        mcx,
        pg_attribute_rel,
        &rows,
    )
}

/// `NameGetDatum(&attname)` image: a NUL-padded 64-byte copy of `name`.
fn pad_name(name: &[u8]) -> [u8; 64] {
    let mut data = [0u8; 64];
    let take = name.len().min(64);
    data[..take].copy_from_slice(&name[..take]);
    data
}

/* --------------------------------
 *		AddNewAttributeTuples
 *
 *		this registers the new relation's schema by adding tuples to
 *		pg_attribute.
 * --------------------------------
 */
pub fn AddNewAttributeTuples<'mcx>(
    mcx: Mcx<'mcx>,
    new_rel_oid: Oid,
    tupdesc: &TupleDescData<'_>,
    relkind: u8,
) -> PgResult<()> {
    let natts = tupdesc.natts;

    /*
     * open pg_attribute and its indexes.
     */
    let rel = backend_access_table_table::table_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    InsertPgAttributeTuples(mcx, &rel, tupdesc, new_rel_oid)?;

    /* add dependencies on their datatypes and collations */
    for i in 0..natts as usize {
        let attr = tupdesc.attr(i);

        /* Add dependency info */
        let myself = object_subaddress(RelationRelationId, new_rel_oid, (i + 1) as i32);
        let referenced = object_address(TypeRelationId, attr.atttypid);
        backend_catalog_pg_depend::recordDependencyOn(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;

        /* The default collation is pinned, so don't bother recording it */
        if OidIsValid(attr.attcollation) && attr.attcollation != DEFAULT_COLLATION_OID {
            let referenced = object_address(CollationRelationId, attr.attcollation);
            backend_catalog_pg_depend::recordDependencyOn(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;
        }
    }

    /*
     * Next we add the system attributes.  Skip all for a view or type
     * relation.  We don't bother with making datatype dependencies here,
     * since presumably all these types are pinned.
     */
    if relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE {
        // td = CreateTupleDesc(lengthof(SysAtt), &SysAtt);
        let sysatts = sys_att();
        let td = backend_access_common_tupdesc::CreateTupleDesc(mcx, &sysatts)?;
        InsertPgAttributeTuples(mcx, &rel, &td, new_rel_oid)?;
    }

    /*
     * clean up
     */
    rel.close(RowExclusiveLock)?;
    Ok(())
}

/* --------------------------------
 *		heap_create_with_catalog
 *
 *		creates a new cataloged relation.  see comments above.
 * --------------------------------
 */
pub fn heap_create_with_catalog<'mcx>(
    mcx: Mcx<'mcx>,
    relname: &str,
    relnamespace: Oid,
    reltablespace: Oid,
    mut relid: Oid,
    reltypeid: Oid,
    reloftypeid: Oid,
    ownerid: Oid,
    accessmtd: Oid,
    tupdesc: &TupleDescData<'_>,
    relkind: u8,
    relpersistence: u8,
    shared_relation: bool,
    mapped_relation: bool,
    oncommit: OnCommitAction,
    reloptions: Option<Vec<u8>>,
    use_user_acl: bool,
    allow_system_table_mods: bool,
    is_internal: bool,
    relrewrite: Oid,
) -> PgResult<Oid> {
    /* By default set to InvalidOid unless overridden by binary-upgrade */
    let mut relfilenumber: RelFileNumber = InvalidOid;

    let pg_class_desc =
        backend_access_table_table::table_open(mcx, RelationRelationId, RowExclusiveLock)?;

    /*
     * sanity checks
     */
    debug_assert!(
        backend_utils_init_miscinit::IsNormalProcessingMode()
            || backend_utils_init_miscinit::IsBootstrapProcessingMode()
    );

    /*
     * Validate proposed tupdesc for the desired relkind.  If
     * allow_system_table_mods is on, allow ANYARRAY to be used.
     */
    let attrs: Vec<_> = (0..tupdesc.natts as usize).map(|i| *tupdesc.attr(i)).collect();
    CheckAttributeNamesTypes(
        mcx,
        &attrs,
        relkind,
        if allow_system_table_mods {
            CHKATYPE_ANYARRAY
        } else {
            0
        },
    )?;

    /*
     * This would fail later on anyway, if the relation already exists.  But by
     * catching it here we can emit a nicer error message.
     */
    let existing_relid = get_relname_relid(relname, relnamespace)?;
    if existing_relid != InvalidOid {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_TABLE)
            .errmsg(format!("relation \"{relname}\" already exists"))
            .into_error());
    }

    /*
     * Since we are going to create a rowtype as well, also check for collision
     * with an existing type name.
     */
    let old_type_oid =
        backend_utils_cache_syscache_seams::get_type_oid::call(relname, relnamespace)?;
    if OidIsValid(old_type_oid)
        && !backend_catalog_pg_type::moveArrayTypeName(old_type_oid, relname, relnamespace)?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("type \"{relname}\" already exists"))
            .errhint(
                "A relation has an associated type of the same name, so you must use a name that doesn't conflict with any existing type.",
            )
            .into_error());
    }

    /*
     * Shared relations must be in pg_global (last-ditch check)
     */
    if shared_relation && reltablespace != GLOBALTABLESPACE_OID {
        elog(ERROR, "shared relations must be placed in pg_global tablespace")?;
    }

    /*
     * Allocate an OID for the relation, unless we were told what to use.
     */
    if !OidIsValid(relid) {
        /* Use binary-upgrade override for pg_class.oid and relfilenumber */
        if backend_catalog_binary_upgrade_seams::is_binary_upgrade::call() {
            /* Indexes are not supported here. */
            debug_assert!(relkind != RELKIND_INDEX);
            debug_assert!(relkind != RELKIND_PARTITIONED_INDEX);

            if relkind == RELKIND_TOASTVALUE {
                /* There might be no TOAST table, so we have to test for it. */
                let next =
                    backend_catalog_binary_upgrade_seams::consume_next_pg_class_oid::call(true);
                if OidIsValid(next) {
                    relid = next;

                    let next_rfn =
                        backend_catalog_binary_upgrade_seams::consume_next_pg_class_relfilenumber::call(true);
                    if !RelFileNumberIsValid(next_rfn) {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg("toast relfilenumber value not set when in binary upgrade mode")
                            .into_error());
                    }
                    relfilenumber = next_rfn;
                }
            } else {
                let next =
                    backend_catalog_binary_upgrade_seams::consume_next_pg_class_oid::call(false);
                if !OidIsValid(next) {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg("pg_class heap OID value not set when in binary upgrade mode")
                        .into_error());
                }
                relid = next;

                if RELKIND_HAS_STORAGE(relkind) {
                    let next_rfn =
                        backend_catalog_binary_upgrade_seams::consume_next_pg_class_relfilenumber::call(false);
                    if !RelFileNumberIsValid(next_rfn) {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg("relfilenumber value not set when in binary upgrade mode")
                            .into_error());
                    }
                    relfilenumber = next_rfn;
                }
            }
        }

        if !OidIsValid(relid) {
            relid = backend_catalog_catalog::GetNewRelFileNumber(
                mcx,
                reltablespace,
                Some(&pg_class_desc),
                relpersistence,
            )?;
        }
    }

    /*
     * Other sessions' catalog scans can't find this until we commit.  Hence, it
     * doesn't hurt to hold AccessExclusiveLock.
     */
    backend_storage_lmgr_lmgr::LockRelationOid(relid, AccessExclusiveLock)?;

    /*
     * Determine the relation's initial permissions.
     */
    let relacl: Option<types_array::ArrayType> = if use_user_acl {
        match relkind {
            x if x == RELKIND_RELATION
                || x == RELKIND_VIEW
                || x == RELKIND_MATVIEW
                || x == RELKIND_FOREIGN_TABLE
                || x == RELKIND_PARTITIONED_TABLE =>
            {
                backend_catalog_aclchk_seams::get_user_default_acl::call(
                    ObjectType::Table,
                    ownerid,
                    relnamespace,
                )?
            }
            x if x == RELKIND_SEQUENCE => backend_catalog_aclchk_seams::get_user_default_acl::call(
                ObjectType::Sequence,
                ownerid,
                relnamespace,
            )?,
            _ => None,
        }
    } else {
        None
    };

    /*
     * Create the relcache entry (mostly dummy at this point) and the physical
     * disk file.
     */
    let created = heap_create(
        mcx,
        relname,
        relnamespace,
        reltablespace,
        relid,
        relfilenumber,
        accessmtd,
        tupdesc,
        relkind,
        relpersistence,
        shared_relation,
        mapped_relation,
        allow_system_table_mods,
        true,
    )?;
    let new_rel_oid_built = created.rel;
    let relfrozenxid = created.xids.relfrozenxid;
    let relminmxid = created.xids.relminmxid;

    // Assert(relid == RelationGetRelid(new_rel_desc)).
    debug_assert!(relid == new_rel_oid_built);

    // Open the freshly-built relcache entry to read its rd_rel/rd_att for the
    // catalog inserts (C: works directly on `new_rel_desc`). NoLock — the
    // AccessExclusiveLock above already protects it.
    let new_rel_desc = backend_access_common_relation::relation_open(mcx, relid, NoLock)?;

    /*
     * Decide whether to create a pg_type entry for the relation's rowtype.
     */
    let new_type_oid;
    if !(relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_PARTITIONED_INDEX)
    {
        /*
         * We'll make an array over the composite type, too.  For largely
         * historical reasons, the array type's OID is assigned first.
         */
        let new_array_oid = backend_commands_typecmds::AssignTypeArrayOid()?;

        /*
         * Make the pg_type entry for the composite type.
         */
        let new_type_addr = AddNewRelationType(
            relname,
            relnamespace,
            relid,
            relkind,
            ownerid,
            reltypeid,
            new_array_oid,
        )?;
        new_type_oid = new_type_addr.objectId;

        /* Now create the array type. */
        let relarrayname = backend_catalog_pg_type::makeArrayTypeName(relname, relnamespace)?;

        use backend_catalog_pg_type::TypeCreate;
        use types_catalog::pg_type::TypeCreateParams;
        TypeCreate(TypeCreateParams {
            new_type_oid: new_array_oid, /* force the type's OID to this */
            type_name: relarrayname,
            type_namespace: relnamespace,
            relation_oid: InvalidOid,
            relation_kind: 0,
            owner_id: ownerid,
            internal_size: -1,
            type_type: TYPTYPE_BASE_C,
            type_category: TYPCATEGORY_ARRAY,
            type_preferred: false,
            type_delim: DEFAULT_TYPDELIM,
            input_procedure: F_ARRAY_IN,
            output_procedure: F_ARRAY_OUT,
            receive_procedure: F_ARRAY_RECV,
            send_procedure: F_ARRAY_SEND,
            typmodin_procedure: InvalidOid,
            typmodout_procedure: InvalidOid,
            analyze_procedure: F_ARRAY_TYPANALYZE,
            subscript_procedure: F_ARRAY_SUBSCRIPT_HANDLER,
            element_type: new_type_oid, /* array element type - the rowtype */
            is_implicit_array: true,
            array_type: InvalidOid,
            base_type: InvalidOid,
            default_type_value: None,
            default_type_bin: None,
            passed_by_value: false,
            alignment: TYPALIGN_DOUBLE,
            storage: TYPSTORAGE_EXTENDED,
            type_mod: -1,
            typ_ndims: 0,
            type_not_null: false,
            type_collation: InvalidOid,
        })?;
    } else {
        /* Caller should not be expecting a type to be created. */
        debug_assert!(reltypeid == InvalidOid);
        new_type_oid = InvalidOid;
    }

    /*
     * now create an entry in pg_class for the relation.
     */
    AddNewRelationTuple(
        mcx,
        &pg_class_desc,
        &new_rel_desc,
        relid,
        new_type_oid,
        reloftypeid,
        ownerid,
        relkind,
        relfrozenxid,
        relminmxid,
        relrewrite,
        relacl,
        reloptions,
    )?;

    /*
     * now add tuples to pg_attribute for the attributes in our new relation.
     */
    let rd_att = new_rel_desc.rd_att_clone_in(mcx)?;
    AddNewAttributeTuples(mcx, relid, &rd_att, relkind)?;

    /*
     * Make a dependency link to force the relation to be deleted if its
     * namespace is.  Also make a dependency link to its owner, etc.
     */
    if relkind != RELKIND_COMPOSITE_TYPE
        && relkind != RELKIND_TOASTVALUE
        && !backend_utils_init_miscinit::IsBootstrapProcessingMode()
    {
        let myself = object_address(RelationRelationId, relid);

        backend_catalog_pg_shdepend_seams::recordDependencyOnOwner::call(
            RelationRelationId,
            relid,
            ownerid,
        )?;

        backend_catalog_aclchk_seams::record_dependency_on_new_acl::call(
            RelationRelationId,
            relid,
            0,
            ownerid,
            relacl,
        )?;

        backend_catalog_pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;

        let mut addrs = backend_catalog_dependency::new_object_addresses();

        let referenced = object_address(NamespaceRelationId, relnamespace);
        backend_catalog_dependency::add_exact_object_address(&referenced, &mut addrs);

        if OidIsValid(reloftypeid) {
            let referenced = object_address(TypeRelationId, reloftypeid);
            backend_catalog_dependency::add_exact_object_address(&referenced, &mut addrs);
        }

        /*
         * Make a dependency link to force the relation to be deleted if its
         * access method is.
         */
        if (RELKIND_HAS_TABLE_AM(relkind) && relkind != RELKIND_TOASTVALUE)
            || (relkind == RELKIND_PARTITIONED_TABLE && OidIsValid(accessmtd))
        {
            let referenced = object_address(AccessMethodRelationId, accessmtd);
            backend_catalog_dependency::add_exact_object_address(&referenced, &mut addrs);
        }

        backend_catalog_dependency::record_object_address_dependencies(
            &myself,
            &mut addrs,
            DEPENDENCY_NORMAL,
        )?;
    }

    /* Post creation hook for new relation */
    backend_catalog_objectaccess::invoke_object_post_create_hook(
        RelationRelationId,
        relid,
        0,
        is_internal,
    )?;

    /*
     * Store any supplied CHECK constraints and defaults.
     *
     * The inward `heap_create_with_catalog` call surface carries no cooked
     * constraints (the C `cooked_constraints == NIL` branch), so StoreConstraints
     * is a no-op (nothing to do). The constraint-cooker family is deeper-keystone
     * blocked (see the crate-level STOP note).
     */
    // StoreConstraints(new_rel_desc, NIL, is_internal) — returns immediately.

    /*
     * If there's a special on-commit action, remember it.
     */
    if oncommit != OnCommitAction::ONCOMMIT_NOOP {
        backend_commands_tablecmds_seams::register_on_commit_action::call(relid, oncommit)?;
    }

    /*
     * ok, the relation has been cataloged, so close our relations and return
     * the OID of the newly created relation.
     */
    new_rel_desc.close(NoLock)?; /* do not unlock till end of xact */
    pg_class_desc.close(RowExclusiveLock)?;

    Ok(relid)
}
