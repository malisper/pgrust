//! `src/backend/catalog/pg_cast.c` (PostgreSQL 18.3) — routines to support
//! manipulation of the `pg_cast` relation.
//!
//! Ported 1:1 against the C, name-for-name. Catalog access mirrors the
//! `backend-catalog-pg-constraint` precedent: `table_open`/`close` guard the
//! relation, the duplicate check is a `systable` scan on the
//! `CastSourceTargetIndexId` unique index (the C uses `SearchSysCache2(
//! CASTSOURCETARGET, …)` purely for a friendlier error — the unique index
//! catches it either way, per the C comment), the tuple build + insert crosses
//! the `catalog_tuple_insert_pg_cast` heapam seam, and dependency recording +
//! the post-create hook go through the dependency / objectaccess seams.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

use mcx::{Mcx, MemoryContext};

use types_catalog::catalog::{LANGUAGE_RELATION_ID, PROCEDURE_RELATION_ID, TYPE_RELATION_ID};
use types_catalog::catalog_dependency::{DependencyType, ObjectAddress, DEPENDENCY_NORMAL};
use types_catalog::pg_cast::{
    Anum_pg_cast_castsource, Anum_pg_cast_casttarget, CastRelationId, CastSourceTargetIndexId,
    PgCastInsertRow,
};
use types_catalog::pg_transform::{
    Anum_pg_transform_oid, Anum_pg_transform_trflang, Anum_pg_transform_trftype,
    PgTransformInsertRow, TransformRelationId, TransformTypeLangIndexId,
};
use types_core::fmgr::F_OIDEQ;
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERROR};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use heaptuple::heap_deform_tuple;
use scankey::ScanKeyInit;
use genam_seams as genam_seams;
use table as table;
use dependency_seams as dependency_seams;
use indexing_seams as indexing_seams;
use objectaccess_seams as objectaccess_seams;
use pg_depend_seams as pg_depend_seams;
use format_type_seams as format_type_seams;

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: i16, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(value),
    )?;
    Ok(key)
}

/// `ObjectAddressSet(addr, class, object)` (`catalog/objectaddress.h`) — set the
/// classId/objectId, with objectSubId = 0.
#[inline]
fn ObjectAddressSet(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/* ===========================================================================
 * CastCreate (pg_cast.c:48-138)
 * ========================================================================= */

/// CastCreate
///
/// Forms and inserts catalog tuples for a new cast being created. Caller must
/// have already checked privileges, and done consistency checks on the given
/// datatypes and cast function (if applicable).
///
/// Since we allow binary coercibility of the datatypes to the cast function's
/// input and result, there could be one or two `WITHOUT FUNCTION` casts that
/// this one depends on. We don't record that explicitly in pg_cast, but we
/// still need to make dependencies on those casts.
///
/// `behavior` indicates the types of the dependencies that the new cast will
/// have on its input and output types, the cast function, and the other casts
/// if any.
pub fn CastCreate(
    mcx: Mcx<'_>,
    sourcetypeid: Oid,
    targettypeid: Oid,
    funcid: Oid,
    incastid: Oid,
    outcastid: Oid,
    castcontext: i8,
    castmethod: i8,
    behavior: DependencyType,
) -> PgResult<ObjectAddress> {
    let cast_ctx = MemoryContext::new("pg_cast");
    let relation = table::table_open(cast_ctx.mcx(), CastRelationId, RowExclusiveLock)?;

    /*
     * Check for duplicate.  This is just to give a friendly error message, the
     * unique index would catch it anyway (so no need to sweat about race
     * conditions).
     *
     *   tuple = SearchSysCache2(CASTSOURCETARGET,
     *                           ObjectIdGetDatum(sourcetypeid),
     *                           ObjectIdGetDatum(targettypeid));
     *   if (HeapTupleIsValid(tuple)) ereport(ERROR, ...);
     */
    let skey = [
        oid_key(Anum_pg_cast_castsource, sourcetypeid)?,
        oid_key(Anum_pg_cast_casttarget, targettypeid)?,
    ];
    let mut found = false;
    {
        let mut scan = genam_seams::systable_beginscan::call(
            &relation,
            CastSourceTargetIndexId,
            true,
            None,
            &skey,
        )?;
        let scratch = MemoryContext::new("pg_cast dup scan");
        if genam_seams::systable_getnext::call(scratch.mcx(), scan.desc_mut())?.is_some() {
            found = true;
        }
        scan.end()?;
    }
    if found {
        return Err(PgError::new(
            ERROR,
            format!(
                "cast from type {} to type {} already exists",
                format_type_seams::format_type_be::call(mcx, sourcetypeid)?.as_str(),
                format_type_seams::format_type_be::call(mcx, targettypeid)?.as_str(),
            ),
        )
        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
    }

    /* ready to go */
    // castid = GetNewOidWithIndex(relation, CastOidIndexId, Anum_pg_cast_oid);
    // values[...] = ...;  tuple = heap_form_tuple(...);  CatalogTupleInsert(...);
    let row = PgCastInsertRow {
        castsource: sourcetypeid,
        casttarget: targettypeid,
        castfunc: funcid,
        castcontext,
        castmethod,
    };
    let castid: Oid = indexing_seams::catalog_tuple_insert_pg_cast::call(cast_ctx.mcx(), &relation, &row)?;

    // addrs = new_object_addresses();
    let mut addrs = dependency_seams::new_object_addresses::call()?;

    /* make dependency entries */
    // ObjectAddressSet(myself, CastRelationId, castid);
    let myself = ObjectAddressSet(CastRelationId, castid);

    /* dependency on source type */
    dependency_seams::add_exact_object_address::call(
        ObjectAddressSet(TYPE_RELATION_ID, sourcetypeid),
        &mut addrs,
    )?;

    /* dependency on target type */
    dependency_seams::add_exact_object_address::call(
        ObjectAddressSet(TYPE_RELATION_ID, targettypeid),
        &mut addrs,
    )?;

    /* dependency on function */
    if OidIsValid(funcid) {
        dependency_seams::add_exact_object_address::call(
            ObjectAddressSet(PROCEDURE_RELATION_ID, funcid),
            &mut addrs,
        )?;
    }

    /* dependencies on casts required for function */
    if OidIsValid(incastid) {
        dependency_seams::add_exact_object_address::call(
            ObjectAddressSet(CastRelationId, incastid),
            &mut addrs,
        )?;
    }
    if OidIsValid(outcastid) {
        dependency_seams::add_exact_object_address::call(
            ObjectAddressSet(CastRelationId, outcastid),
            &mut addrs,
        )?;
    }

    // record_object_address_dependencies(&myself, addrs, behavior);
    dependency_seams::record_object_address_dependencies::call(myself, &mut addrs, behavior)?;
    // free_object_addresses(addrs);
    dependency_seams::free_object_addresses::call(addrs)?;

    /* dependency on extension */
    // recordDependencyOnCurrentExtension(&myself, false);
    pg_depend_seams::recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;

    /* Post creation hook for new cast */
    // InvokeObjectPostCreateHook(CastRelationId, castid, 0);
    objectaccess_seams::invoke_object_post_create_hook::call(CastRelationId, castid, 0)?;

    // heap_freetuple(tuple);  (the formed tuple is owned by the insert seam)
    // table_close(relation, RowExclusiveLock);
    relation.close(RowExclusiveLock)?;

    Ok(myself)
}

/* ===========================================================================
 * CreateTransform catalog block (functioncmds.c:1955-2035) — the pg_transform
 * insert/update + dependency rebuild + extension dependency + post-create hook.
 * The privilege/signature checks stay in functioncmds; this is the raw catalog
 * tuple I/O reached via the `create_transform_tuple` seam.
 * ========================================================================= */

/// The `pg_transform` insert/update of `CreateTransform`. `typeid`/`langid`/
/// `fromsqlfuncid`/`tosqlfuncid` are the resolved arguments; `replace` selects
/// `CREATE OR REPLACE` semantics; `lang_name` supplies the error text.
#[allow(clippy::too_many_arguments)]
pub fn CreateTransformTuple(
    mcx: Mcx<'_>,
    typeid: Oid,
    langid: Oid,
    fromsqlfuncid: Oid,
    tosqlfuncid: Oid,
    replace: bool,
    lang_name: &str,
) -> PgResult<ObjectAddress> {
    let trf_ctx = MemoryContext::new("pg_transform");
    let relation = table::table_open(trf_ctx.mcx(), TransformRelationId, RowExclusiveLock)?;

    /*
     * tuple = SearchSysCache2(TRFTYPELANG, typeid, langid);
     * (There is no TRFTYPELANG-by-oid scan need; a systable scan over the
     * unique type/lang index is the by-(type,lang) lookup the syscache backs.)
     */
    let skey = [
        oid_key(Anum_pg_transform_trftype, typeid)?,
        oid_key(Anum_pg_transform_trflang, langid)?,
    ];
    let mut existing: Option<(Oid, ItemPointerData)> = None;
    {
        let mut scan = genam_seams::systable_beginscan::call(
            &relation,
            TransformTypeLangIndexId,
            true,
            None,
            &skey,
        )?;
        let scratch = MemoryContext::new("pg_transform dup scan");
        if let Some(tup) = genam_seams::systable_getnext::call(scratch.mcx(), scan.desc_mut())? {
            let cols = heap_deform_tuple(scratch.mcx(), &tup.tuple, &relation.rd_att, &tup.data)?;
            let oid = cols[Anum_pg_transform_oid as usize - 1].0.as_oid();
            existing = Some((oid, tup.tuple.t_self));
        }
        scan.end()?;
    }

    let row = PgTransformInsertRow {
        trftype: typeid,
        trflang: langid,
        trffromsql: fromsqlfuncid,
        trftosql: tosqlfuncid,
    };

    let (transformid, is_replace) = match existing {
        Some((oid, tid)) => {
            if !replace {
                return Err(PgError::new(
                    ERROR,
                    format!(
                        "transform for type {} language \"{}\" already exists",
                        format_type_seams::format_type_be::call(mcx, typeid)?.as_str(),
                        lang_name,
                    ),
                )
                .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
            }
            // replaces[trffromsql] = replaces[trftosql] = true;
            // newtuple = heap_modify_tuple(...); CatalogTupleUpdate(...);
            indexing_seams::catalog_tuple_insert_pg_transform::call(
                trf_ctx.mcx(),
                &relation,
                &row,
                oid,
                tid,
            )?;
            (oid, true)
        }
        None => {
            let oid = indexing_seams::catalog_tuple_insert_pg_transform::call(
                trf_ctx.mcx(),
                &relation,
                &row,
                InvalidOid,
                ItemPointerData::default(),
            )?;
            (oid, false)
        }
    };

    if is_replace {
        // deleteDependencyRecordsFor(TransformRelationId, transformid, true);
        pg_depend_seams::deleteDependencyRecordsFor::call(TransformRelationId, transformid, true)?;
    }

    // addrs = new_object_addresses();
    let mut addrs = dependency_seams::new_object_addresses::call()?;

    /* make dependency entries */
    let myself = ObjectAddressSet(TransformRelationId, transformid);

    /* dependency on language */
    dependency_seams::add_exact_object_address::call(
        ObjectAddressSet(LANGUAGE_RELATION_ID, langid),
        &mut addrs,
    )?;

    /* dependency on type */
    dependency_seams::add_exact_object_address::call(
        ObjectAddressSet(TYPE_RELATION_ID, typeid),
        &mut addrs,
    )?;

    /* dependencies on functions */
    if OidIsValid(fromsqlfuncid) {
        dependency_seams::add_exact_object_address::call(
            ObjectAddressSet(PROCEDURE_RELATION_ID, fromsqlfuncid),
            &mut addrs,
        )?;
    }
    if OidIsValid(tosqlfuncid) {
        dependency_seams::add_exact_object_address::call(
            ObjectAddressSet(PROCEDURE_RELATION_ID, tosqlfuncid),
            &mut addrs,
        )?;
    }

    // record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    dependency_seams::record_object_address_dependencies::call(
        myself,
        &mut addrs,
        DEPENDENCY_NORMAL,
    )?;
    dependency_seams::free_object_addresses::call(addrs)?;

    /* dependency on extension */
    pg_depend_seams::recordDependencyOnCurrentExtension::call(mcx, &myself, is_replace)?;

    /* Post creation hook for new transform */
    objectaccess_seams::invoke_object_post_create_hook::call(TransformRelationId, transformid, 0)?;

    relation.close(RowExclusiveLock)?;

    Ok(myself)
}

/// `pg_cast.c` — install the `cast_create` outward seam that functioncmds.c's
/// `CreateCast` reaches across the command→catalog cycle. The seam carries the
/// seven `CastCreate` arguments; the `DEPENDENCY_NORMAL` behavior CreateCast
/// always passes is supplied here, and the owner runs behind a scratch context.
pub fn init_seams() {
    functioncmds_seams::create_transform_tuple::set(
        |type_id, lang_id, fromsql_func, tosql_func, replace, lang_name| {
            let ctx = MemoryContext::new("functioncmds create_transform_tuple");
            CreateTransformTuple(
                ctx.mcx(),
                type_id,
                lang_id,
                fromsql_func,
                tosql_func,
                replace,
                &lang_name,
            )
        },
    );

    functioncmds_seams::cast_create::set(
        |source_type, target_type, func_id, in_cast_id, out_cast_id, cast_context, cast_method| {
            let ctx = MemoryContext::new("functioncmds cast_create");
            CastCreate(
                ctx.mcx(),
                source_type,
                target_type,
                func_id,
                in_cast_id,
                out_cast_id,
                cast_context,
                cast_method,
                DEPENDENCY_NORMAL,
            )
        },
    );
}
