//! `src/backend/catalog/pg_conversion.c` (PostgreSQL 18.3) — routines to
//! support manipulation of the `pg_conversion` relation.
//!
//! Ported 1:1 against the C, name-for-name. Catalog access mirrors the
//! `backend-catalog-pg-constraint` precedent: `table_open`/`close` guard the
//! relation; the duplicate-name check (`SearchSysCacheExists2(CONNAMENSP, …)`)
//! and `FindDefaultConversion` (`SearchSysCacheList3(CONDEFAULT, …)`) are
//! `systable` scans on the corresponding unique indexes; the tuple build +
//! insert crosses the `catalog_tuple_insert_pg_conversion` heapam seam; and
//! dependency recording + the post-create hook go through the dependency /
//! shdepend / objectaccess seams.
//!
//! This crate OWNS the inward seams `conversion_create` (consumed by
//! conversioncmds.c) and `find_default_conversion` (consumed by namespace.c),
//! installed in `init_seams()`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use mcx::{Mcx, MemoryContext, PgVec};

use types_catalog::catalog::{
    NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID,
};
use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_NORMAL};
use types_catalog::pg_conversion::{
    Anum_pg_conversion_conname, Anum_pg_conversion_condefault, Anum_pg_conversion_conforencoding,
    Anum_pg_conversion_connamespace, Anum_pg_conversion_conproc, Anum_pg_conversion_contoencoding,
    ConversionDefaultIndexId, ConversionNameNspIndexId, ConversionRelationId, PgConversionInsertRow,
};
use types_core::fmgr::{F_INT4EQ, F_NAMEEQ, F_OIDEQ};
use types_core::primitive::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERROR};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::heaptuple::Datum;

use heaptuple::heap_deform_tuple;
use scankey::ScanKeyInit;
use genam_seams as genam_seams;
use table as table;
use indexing_seams as indexing_seams;
use objectaccess_seams as objectaccess_seams;
use pg_depend_seams as pg_depend_seams;
use pg_shdepend_seams as pg_shdepend_seams;
use encnames_seams as encnames_seams;

/// `NAMEDATALEN`.
const NAMEDATALEN: usize = 64;

/// `namestrcpy(&name, src)` — copy `src` into a zero-filled `NameData`,
/// truncated to `NAMEDATALEN`, force-terminated at the last slot.
fn namestrcpy(src: &str) -> [u8; NAMEDATALEN] {
    let mut name = [0u8; NAMEDATALEN];
    for (i, &byte) in src.as_bytes().iter().take(NAMEDATALEN).enumerate() {
        name[i] = byte;
    }
    name[NAMEDATALEN - 1] = 0;
    name
}

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

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_INT4EQ,
/// Int32GetDatum(value))`.
fn int4_key<'mcx>(attno: i16, value: i32) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Datum::from_i32(value),
    )?;
    Ok(key)
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_NAMEEQ,
/// CStringGetDatum(value))`. The name crosses as a NUL-terminated byte image
/// (the genam owner's `nameeq` comparator interprets it).
fn name_key<'mcx>(mcx: Mcx<'mcx>, attno: i16, value: &str) -> PgResult<ScanKeyData<'mcx>> {
    let mut bytes: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, value.len() + 1)?;
    for &b in value.as_bytes() {
        bytes.push(b);
    }
    bytes.push(0);
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        Datum::ByRef(bytes),
    )?;
    Ok(key)
}

/* ===========================================================================
 * ConversionCreate (pg_conversion.c:35-160)
 * ========================================================================= */

/// ConversionCreate — add a new tuple to pg_conversion.
pub fn ConversionCreate(
    conname: &str,
    connamespace: Oid,
    conowner: Oid,
    conforencoding: i32,
    contoencoding: i32,
    conproc: Oid,
    def: bool,
) -> PgResult<ObjectAddress> {
    /* sanity checks */
    // if (!conname) elog(ERROR, "no conversion name supplied");
    if conname.is_empty() {
        return Err(PgError::error("no conversion name supplied".to_string()));
    }

    let conv_ctx = MemoryContext::new("pg_conversion");
    let mcx = conv_ctx.mcx();

    /* make sure there is no existing conversion of same name */
    // if (SearchSysCacheExists2(CONNAMENSP, conname, connamespace)) ereport(...);
    if conversion_name_exists(mcx, conname, connamespace)? {
        return Err(PgError::new(
            ERROR,
            format!("conversion \"{conname}\" already exists"),
        )
        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
    }

    if def {
        /*
         * make sure there is no existing default <for encoding><to encoding>
         * pair in this name space
         */
        if find_default_conversion_impl(mcx, connamespace, conforencoding, contoencoding)?
            != InvalidOid
        {
            return Err(PgError::new(
                ERROR,
                format!(
                    "default conversion for {} to {} already exists",
                    encnames_seams::pg_encoding_to_char::call(conforencoding),
                    encnames_seams::pg_encoding_to_char::call(contoencoding),
                ),
            )
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
        }
    }

    /* open pg_conversion */
    let rel = table::table_open(mcx, ConversionRelationId, RowExclusiveLock)?;

    /* form a tuple (namestrcpy + GetNewOidWithIndex + heap_form_tuple) */
    let row = PgConversionInsertRow {
        conname: namestrcpy(conname),
        connamespace,
        conowner,
        conforencoding,
        contoencoding,
        conproc,
        condefault: def,
    };

    /* insert a new tuple */
    let oid = indexing_seams::catalog_tuple_insert_pg_conversion::call(mcx, &rel, &row)?;

    // myself.classId = ConversionRelationId; myself.objectId = oid;
    let myself = ObjectAddress {
        classId: ConversionRelationId,
        objectId: oid,
        objectSubId: 0,
    };

    /* create dependency on conversion procedure */
    let referenced = ObjectAddress {
        classId: PROCEDURE_RELATION_ID,
        objectId: conproc,
        objectSubId: 0,
    };
    pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;

    /* create dependency on namespace */
    let referenced = ObjectAddress {
        classId: NAMESPACE_RELATION_ID,
        objectId: connamespace,
        objectSubId: 0,
    };
    pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;

    /* create dependency on owner */
    pg_shdepend_seams::recordDependencyOnOwner::call(ConversionRelationId, oid, conowner)?;

    /* dependency on extension */
    pg_depend_seams::recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;

    /* Post creation hook for new conversion */
    objectaccess_seams::invoke_object_post_create_hook::call(ConversionRelationId, oid, 0)?;

    // heap_freetuple(tup);  (the formed tuple is owned by the insert seam)
    rel.close(RowExclusiveLock)?;

    Ok(myself)
}

/// `SearchSysCacheExists2(CONNAMENSP, conname, connamespace)` — does a
/// conversion of this name already exist in the namespace? Scans the
/// `ConversionNameNspIndexId` unique index.
fn conversion_name_exists(mcx: Mcx<'_>, conname: &str, connamespace: Oid) -> PgResult<bool> {
    let rel = table::table_open(mcx, ConversionRelationId, RowExclusiveLock)?;
    let skey = [
        name_key(mcx, Anum_pg_conversion_conname, conname)?,
        oid_key(Anum_pg_conversion_connamespace, connamespace)?,
    ];
    let mut found = false;
    {
        let mut scan = genam_seams::systable_beginscan::call(
            &rel,
            ConversionNameNspIndexId,
            true,
            None,
            &skey,
        )?;
        let scratch = MemoryContext::new("pg_conversion name scan");
        if genam_seams::systable_getnext::call(scratch.mcx(), scan.desc_mut())?.is_some() {
            found = true;
        }
        scan.end()?;
    }
    rel.close(RowExclusiveLock)?;
    Ok(found)
}

/* ===========================================================================
 * FindDefaultConversion (pg_conversion.c:162-200)
 * ========================================================================= */

/// FindDefaultConversion — find "default" conversion proc by `for_encoding` and
/// `to_encoding` in the given namespace. Returns the procedure's OID (not the
/// conversion's!), or `InvalidOid`.
pub fn FindDefaultConversion(
    mcx: Mcx<'_>,
    name_space: Oid,
    for_encoding: i32,
    to_encoding: i32,
) -> PgResult<Oid> {
    find_default_conversion_impl(mcx, name_space, for_encoding, to_encoding)
}

/// Shared body for `FindDefaultConversion` and the `def` self-check in
/// `ConversionCreate`. The C `SearchSysCacheList3(CONDEFAULT, namespace,
/// for_encoding, to_encoding)` is a scan on the `ConversionDefaultIndexId`
/// unique index `(connamespace, conforencoding, contoencoding, oid)`; we take
/// the first row whose `condefault` is set (mirroring the C loop + `break`).
fn find_default_conversion_impl(
    mcx: Mcx<'_>,
    name_space: Oid,
    for_encoding: i32,
    to_encoding: i32,
) -> PgResult<Oid> {
    let rel = table::table_open(mcx, ConversionRelationId, RowExclusiveLock)?;
    let skey = [
        oid_key(Anum_pg_conversion_connamespace, name_space)?,
        int4_key(Anum_pg_conversion_conforencoding, for_encoding)?,
        int4_key(Anum_pg_conversion_contoencoding, to_encoding)?,
    ];

    let mut proc = InvalidOid;
    {
        let mut scan = genam_seams::systable_beginscan::call(
            &rel,
            ConversionDefaultIndexId,
            true,
            None,
            &skey,
        )?;
        loop {
            let scratch = MemoryContext::new("pg_conversion default scan row");
            let smcx = scratch.mcx();
            let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
                break;
            };
            let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
            // body = (Form_pg_conversion) GETSTRUCT(tuple);
            let condefault = cols[Anum_pg_conversion_condefault as usize - 1].0.as_bool();
            if condefault {
                proc = cols[Anum_pg_conversion_conproc as usize - 1].0.as_oid();
                break;
            }
        }
        scan.end()?;
    }
    rel.close(RowExclusiveLock)?;
    Ok(proc)
}

/* ===========================================================================
 * Inward-seam adapters + install
 * ========================================================================= */

/// `conversion_create` seam: the conversion-seams contract takes no `mcx`
/// (`ConversionCreate` opens/closes `pg_conversion` itself over a private
/// context), so the adapter just forwards.
fn conversion_create_seam(
    conname: &str,
    connamespace: Oid,
    conowner: Oid,
    conforencoding: i32,
    contoencoding: i32,
    conproc: Oid,
    def: bool,
) -> PgResult<ObjectAddress> {
    ConversionCreate(
        conname,
        connamespace,
        conowner,
        conforencoding,
        contoencoding,
        conproc,
        def,
    )
}

/// `find_default_conversion` seam: opens a private context for the scan.
fn find_default_conversion_seam(
    connamespace: Oid,
    for_encoding: i32,
    to_encoding: i32,
) -> PgResult<Oid> {
    let ctx = MemoryContext::new("FindDefaultConversion");
    find_default_conversion_impl(ctx.mcx(), connamespace, for_encoding, to_encoding)
}

/// Install this crate's implementations into
/// `backend-catalog-pg-conversion-seams`.
pub fn init_seams() {
    use pg_conversion_seams as seams;

    seams::conversion_create::set(conversion_create_seam);
    seams::find_default_conversion::set(find_default_conversion_seam);
}
