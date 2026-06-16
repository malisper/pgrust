//! The relcache catalog scan-and-decode primitives (`relcache.c`'s
//! `ScanPgRelation` / `RelationBuildTupleDesc` / `RelationGetIndexList` /
//! `RelationBuildRuleLock` / `RelationGetStatExtList` / `RelationGetFKeyList` /
//! `RelationGetExclusionInfo` / `AttrDefaultFetch` / `CheckNNConstraintFetch`
//! scan loops), bodied here because the whole `table_open` +
//! `systable_beginscan`/`getnext` + `GETSTRUCT`/`heap_getattr` deform is
//! genam-owned catalog vocabulary.
//!
//! Each function mirrors the C scan loop exactly: build the `ScanKeyData`
//! (`ScanKeyInit`), `table_open(<catalog>, AccessShareLock)`,
//! `systable_beginscan(<index>, ...)`, then `systable_getnext` + per-row
//! `heap_deform_tuple` (`GETSTRUCT`) decode into the owner-vocabulary DTO the
//! relcache consumes. The relcache caller never deforms catalog tuples; it
//! marshals these decoded rows into its owned entry fields.

extern crate alloc;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::{Mcx, MemoryContext};
use types_core::fmgr::{FmgrInfo, F_INT2GT, F_OIDEQ};
use types_core::primitive::{AttrNumber, Oid};
use types_error::{PgError, PgResult};
use types_scan::scankey::{
    BTEqualStrategyNumber, BTGreaterStrategyNumber, ScanKeyData,
};
use types_storage::lock::AccessShareLock;
use types_tuple::backend_access_common_heaptuple::{Datum, DeformedColumn};

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_index_genam_seams as seam;
use backend_access_table_table::{table_close, table_open};
use backend_utils_fmgr_fmgr_seams as fmgr_seams;

use types_catalog::catalog::CONSTRAINT_RELATION_ID;

use types_catalog::pg_attrdef::{
    AttrDefaultIndexId, AttrDefaultRelationId, Anum_pg_attrdef_adbin,
    Anum_pg_attrdef_adnum,
};
use types_catalog::pg_attribute::{
    AttributeRelationId, AttributeRelidNumIndexId, Anum_pg_attribute_attalign,
    Anum_pg_attribute_attbyval, Anum_pg_attribute_attcollation,
    Anum_pg_attribute_attcompression, Anum_pg_attribute_attgenerated,
    Anum_pg_attribute_atthasdef, Anum_pg_attribute_atthasmissing,
    Anum_pg_attribute_attidentity, Anum_pg_attribute_attinhcount,
    Anum_pg_attribute_attmissingval,
    Anum_pg_attribute_attisdropped, Anum_pg_attribute_attislocal,
    Anum_pg_attribute_attlen, Anum_pg_attribute_attname,
    Anum_pg_attribute_attndims, Anum_pg_attribute_attnotnull,
    Anum_pg_attribute_attnum, Anum_pg_attribute_attrelid,
    Anum_pg_attribute_attstorage, Anum_pg_attribute_atttypid,
    Anum_pg_attribute_atttypmod,
};
use types_catalog::pg_class::{
    ClassOidIndexId, RelationRelationId, Anum_pg_class_oid,
    Anum_pg_class_relallvisible, Anum_pg_class_relam, Anum_pg_class_relchecks,
    Anum_pg_class_relfilenode, Anum_pg_class_relforcerowsecurity,
    Anum_pg_class_relfrozenxid, Anum_pg_class_relhasindex,
    Anum_pg_class_relhasrules, Anum_pg_class_relhassubclass,
    Anum_pg_class_relhastriggers, Anum_pg_class_relispartition,
    Anum_pg_class_relispopulated, Anum_pg_class_relisshared,
    Anum_pg_class_relkind, Anum_pg_class_relminmxid, Anum_pg_class_relname,
    Anum_pg_class_relnamespace, Anum_pg_class_relnatts, Anum_pg_class_reloftype,
    Anum_pg_class_relowner, Anum_pg_class_relpages, Anum_pg_class_relpersistence,
    Anum_pg_class_relreplident, Anum_pg_class_reloptions, Anum_pg_class_relrewrite,
    Anum_pg_class_relrowsecurity, Anum_pg_class_reltablespace,
    Anum_pg_class_reltoastrelid, Anum_pg_class_reltuples, Anum_pg_class_reltype,
};
use types_catalog::pg_constraint::{
    ConstraintRelidTypidNameIndexId, Anum_pg_constraint_conbin,
    Anum_pg_constraint_conenforced, Anum_pg_constraint_conexclop,
    Anum_pg_constraint_conindid, Anum_pg_constraint_conname,
    Anum_pg_constraint_connoinherit, Anum_pg_constraint_conrelid,
    Anum_pg_constraint_contype, Anum_pg_constraint_convalidated,
    Anum_pg_constraint_oid, CONSTRAINT_CHECK, CONSTRAINT_EXCLUSION,
    CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL,
};
use types_catalog::pg_index::{
    IndexIndrelidIndexId, IndexRelationId, Anum_pg_index_indcheckxmin,
    Anum_pg_index_indexrelid, Anum_pg_index_indimmediate,
    Anum_pg_index_indisclustered, Anum_pg_index_indisexclusion,
    Anum_pg_index_indislive, Anum_pg_index_indisprimary,
    Anum_pg_index_indisready, Anum_pg_index_indisreplident,
    Anum_pg_index_indisunique, Anum_pg_index_indisvalid, Anum_pg_index_indkey,
    Anum_pg_index_indnatts, Anum_pg_index_indnkeyatts,
    Anum_pg_index_indnullsnotdistinct, Anum_pg_index_indpred, Anum_pg_index_indrelid,
};
use types_catalog::pg_rewrite::{
    RewriteRelationId, Anum_pg_rewrite_ev_action, Anum_pg_rewrite_ev_class,
    Anum_pg_rewrite_ev_enabled, Anum_pg_rewrite_ev_qual,
    Anum_pg_rewrite_ev_type, Anum_pg_rewrite_is_instead, Anum_pg_rewrite_oid,
};
use types_catalog::pg_statistic_ext::{
    StatisticExtRelationId, StatisticExtRelidIndexId,
    Anum_pg_statistic_ext_oid, Anum_pg_statistic_ext_stxrelid,
};

use crate::{systable_beginscan, systable_getnext};

const CONSTRAINT_CONRELID_INDEX_OK: bool = true;

/// `RewriteRelRulesIndexId` — `pg_rewrite_rel_rulename_index` (OID 2693). Not
/// yet a `types_catalog` constant; declared here (the only consumer) until the
/// catalog header lands it. `RelationBuildRuleLock` orders its scan by this
/// index (`rulename`).
const REWRITE_REL_RULES_INDEX_ID: Oid = 2693;

// ===========================================================================
// ScanKeyInit helper
// ===========================================================================

/// `ScanKeyInit(entry, attno, strategy, procOid, argument)` (access/skey.h):
/// build one equality/comparison scan key. The eager fmgr resolution
/// (`fmgr_info_cxt(procOid, ...)`) crosses the fmgr seam, exactly where C does
/// the lookup; the trimmed [`FmgrInfo`] records the resolved procedure OID.
fn scan_key_init<'mcx>(
    attno: AttrNumber,
    strategy: types_scan::scankey::StrategyNumber,
    proc_oid: Oid,
    argument: Datum<'mcx>,
) -> PgResult<ScanKeyData<'mcx>> {
    fmgr_seams::fmgr_info_check::call(proc_oid)?;
    let mut key = ScanKeyData::empty();
    key.sk_flags = 0;
    key.sk_attno = attno;
    key.sk_strategy = strategy;
    key.sk_subtype = types_core::InvalidOid;
    key.sk_collation = types_core::InvalidOid;
    key.sk_func = FmgrInfo {
        fn_oid: proc_oid,
        ..Default::default()
    };
    key.sk_argument = argument;
    Ok(key)
}

// ===========================================================================
// per-column deform readers
// ===========================================================================

/// `GETSTRUCT(tup)->field` for a by-value column: read the deformed column as a
/// bare word. Errors if the catalog row is unexpectedly short (column index out
/// of range) or the column is NULL/by-reference where a scalar is required —
/// matching the C contract that fixed-width `Form_*` columns are never NULL.
fn col<'a, 'mcx>(
    row: &'a [DeformedColumn<'mcx>],
    anum: i16,
    name: &str,
) -> PgResult<&'a Datum<'mcx>> {
    let idx = (anum - 1) as usize;
    let (datum, isnull) = row
        .get(idx)
        .ok_or_else(|| PgError::error(alloc::format!("catalog row too short for {name}")))?;
    if *isnull {
        return Err(PgError::error(alloc::format!(
            "unexpected NULL in catalog column {name}"
        )));
    }
    Ok(datum)
}

/// `NameStr(form->field)` — a `NameData` by-reference column read up to its
/// first NUL.
fn name_col(row: &[DeformedColumn<'_>], anum: i16, name: &str) -> PgResult<String> {
    let datum = col(row, anum, name)?;
    let bytes = datum.as_ref_bytes();
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    Ok(String::from_utf8_lossy(&bytes[..end]).into_owned())
}

/// A variable-length (by-reference) column read as its verbatim on-disk varlena
/// bytes, or `None` for the C `isnull` (the `fastgetattr` result tested with
/// `isnull`). Unlike [`col`], a NULL is not an error — variable-length tail
/// columns (`reloptions`, `attmissingval`) are legitimately NULL. A column
/// index past the deformed row's end is also treated as NULL (the C
/// `heap_getattr` returns NULL for attributes beyond the stored tuple's
/// natts).
fn bytea_col_opt(row: &[DeformedColumn<'_>], anum: i16) -> Option<Vec<u8>> {
    let idx = (anum - 1) as usize;
    match row.get(idx) {
        None => None,
        Some((_, true)) => None,
        Some((datum, false)) => Some(datum.as_ref_bytes().to_vec()),
    }
}

// ===========================================================================
// scan_pg_class — ScanPgRelation
// ===========================================================================

/// `ScanPgRelation(targetRelId, indexOK, force_non_historic)` (relcache.c).
fn scan_pg_class(reloid: Oid, index_ok: bool) -> PgResult<Option<seam::ScannedPgClass>> {
    let scratch = MemoryContext::new("ScanPgRelation scan");
    let smcx = scratch.mcx();

    // ScanKeyInit(&key[0], Anum_pg_class_oid, BTEqualStrategyNumber, F_OIDEQ,
    //             ObjectIdGetDatum(targetRelId));
    let skey = [scan_key_init(
        Anum_pg_class_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(reloid),
    )?];

    let relation = table_open(smcx, RelationRelationId, AccessShareLock)?;
    let mut scandesc =
        systable_beginscan(&relation, ClassOidIndexId, index_ok, None, &skey)?;

    // pg_class_tuple = systable_getnext(pg_class_scan); if (!HeapTupleIsValid)
    // return NULL.
    let out = match systable_getnext(smcx, scandesc.desc_mut())? {
        None => None,
        Some(ntp) => {
            let row = heap_deform_tuple(smcx, &ntp.tuple, &relation.rd_att, &ntp.data)?;
            Some(decode_pg_class(&row)?)
        }
    };

    scandesc.end()?;
    table_close(relation, AccessShareLock)?;
    drop(scratch);
    Ok(out)
}

/// `GETSTRUCT(pg_class_tuple)` field-for-field into [`seam::ScannedPgClass`].
fn decode_pg_class(row: &[DeformedColumn<'_>]) -> PgResult<seam::ScannedPgClass> {
    Ok(seam::ScannedPgClass {
        oid: col(row, Anum_pg_class_oid, "pg_class.oid")?.as_oid(),
        relname: name_col(row, Anum_pg_class_relname, "pg_class.relname")?,
        relnamespace: col(row, Anum_pg_class_relnamespace, "relnamespace")?.as_oid(),
        reltype: col(row, Anum_pg_class_reltype, "reltype")?.as_oid(),
        reloftype: col(row, Anum_pg_class_reloftype, "reloftype")?.as_oid(),
        relowner: col(row, Anum_pg_class_relowner, "relowner")?.as_oid(),
        relam: col(row, Anum_pg_class_relam, "relam")?.as_oid(),
        relfilenode: col(row, Anum_pg_class_relfilenode, "relfilenode")?.as_oid(),
        reltablespace: col(row, Anum_pg_class_reltablespace, "reltablespace")?.as_oid(),
        relpages: col(row, Anum_pg_class_relpages, "relpages")?.as_i32(),
        reltuples: col(row, Anum_pg_class_reltuples, "reltuples")?.as_f32(),
        relallvisible: col(row, Anum_pg_class_relallvisible, "relallvisible")?.as_i32(),
        reltoastrelid: col(row, Anum_pg_class_reltoastrelid, "reltoastrelid")?.as_oid(),
        relhasindex: col(row, Anum_pg_class_relhasindex, "relhasindex")?.as_bool(),
        relisshared: col(row, Anum_pg_class_relisshared, "relisshared")?.as_bool(),
        relpersistence: col(row, Anum_pg_class_relpersistence, "relpersistence")?.as_char(),
        relkind: col(row, Anum_pg_class_relkind, "relkind")?.as_char(),
        relnatts: col(row, Anum_pg_class_relnatts, "relnatts")?.as_i16(),
        relchecks: col(row, Anum_pg_class_relchecks, "relchecks")?.as_i16(),
        relhasrules: col(row, Anum_pg_class_relhasrules, "relhasrules")?.as_bool(),
        relhastriggers: col(row, Anum_pg_class_relhastriggers, "relhastriggers")?.as_bool(),
        relhassubclass: col(row, Anum_pg_class_relhassubclass, "relhassubclass")?.as_bool(),
        relrowsecurity: col(row, Anum_pg_class_relrowsecurity, "relrowsecurity")?.as_bool(),
        relforcerowsecurity: col(row, Anum_pg_class_relforcerowsecurity, "relforcerowsecurity")?
            .as_bool(),
        relispopulated: col(row, Anum_pg_class_relispopulated, "relispopulated")?.as_bool(),
        relreplident: col(row, Anum_pg_class_relreplident, "relreplident")?.as_char(),
        relispartition: col(row, Anum_pg_class_relispartition, "relispartition")?.as_bool(),
        relrewrite: col(row, Anum_pg_class_relrewrite, "relrewrite")?.as_oid(),
        relfrozenxid: col(row, Anum_pg_class_relfrozenxid, "relfrozenxid")?.as_u32(),
        relminmxid: col(row, Anum_pg_class_relminmxid, "relminmxid")?.as_u32(),
        // The variable-length reloptions tail column (text[]): its verbatim
        // varlena bytes, or None for the C isnull. RelationParseRelOptions feeds
        // these to extractRelOptions.
        reloptions: bytea_col_opt(row, Anum_pg_class_reloptions),
    })
}

// ===========================================================================
// scan_pg_attribute — RelationBuildTupleDesc
// ===========================================================================

/// `RelationBuildTupleDesc(relation)`'s `pg_attribute` scan (relcache.c).
fn scan_pg_attribute(reloid: Oid, _natts: i16) -> PgResult<Vec<seam::ScannedPgAttribute>> {
    let scratch = MemoryContext::new("RelationBuildTupleDesc scan");
    let smcx = scratch.mcx();

    // ScanKeyInit(&skey[0], Anum_pg_attribute_attrelid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));
    // ScanKeyInit(&skey[1], Anum_pg_attribute_attnum, BTGreaterStrategyNumber,
    //             F_INT2GT, Int16GetDatum(0));
    let skey = [
        scan_key_init(
            Anum_pg_attribute_attrelid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            Datum::from_oid(reloid),
        )?,
        scan_key_init(
            Anum_pg_attribute_attnum,
            BTGreaterStrategyNumber,
            F_INT2GT,
            Datum::from_i16(0),
        )?,
    ];

    let relation = table_open(smcx, AttributeRelationId, AccessShareLock)?;
    let mut scandesc =
        systable_beginscan(&relation, AttributeRelidNumIndexId, true, None, &skey)?;

    let mut out = Vec::new();
    while let Some(ntp) = systable_getnext(smcx, scandesc.desc_mut())? {
        let row = heap_deform_tuple(smcx, &ntp.tuple, &relation.rd_att, &ntp.data)?;
        out.push(decode_pg_attribute(smcx, &row)?);
    }

    scandesc.end()?;
    table_close(relation, AccessShareLock)?;
    drop(scratch);
    Ok(out)
}

/// `RelationBuildTupleDesc`'s `attmissingval` fetch (relcache.c): when the
/// column has a missing value (`atthasmissing`), `heap_getattr(
/// Anum_pg_attribute_attmissingval)` then `array_get_element(missingval, 1,
/// &one, -1, attlen, attbyval, attalign)` extracts the single element. The
/// `attmissingval` array is a 1-element 1-D array of the column's own type, so
/// the element-1 fetch is element index 0 of the deconstructed array. Returns
/// the element's value image (lifetime-free), or `None` for the C `missingNull`
/// (no missing value for this column) — including the `atthasmissing`-false
/// short-circuit.
fn extract_attmissingval(
    mcx: Mcx<'_>,
    row: &[DeformedColumn<'_>],
    atthasmissing: bool,
    atttypid: Oid,
    attlen: i16,
    attbyval: bool,
    attalign: i8,
) -> PgResult<Option<types_tuple::heaptuple::MissingValueImage>> {
    if !atthasmissing {
        return Ok(None);
    }
    // missingval = heap_getattr(pg_attribute_tuple, Anum_pg_attribute_attmissingval,
    //                           pg_attribute_desc->rd_att, &missingNull);
    let bytes = match bytea_col_opt(row, Anum_pg_attribute_attmissingval) {
        // if (missingNull) -> no missing value.
        None => return Ok(None),
        Some(b) => b,
    };
    // missval = array_get_element(missingval, 1, &one, -1, attlen, attbyval,
    //                             attalign, &is_null);  Assert(!is_null);
    // The single-element array is deconstructed; element 0 is the C "element 1".
    let elems = backend_utils_adt_arrayfuncs_seams::deconstruct_array_values_bytes::call(
        mcx, &bytes, atttypid, attlen, attbyval, attalign,
    )?;
    let (datum, is_null) = elems
        .first()
        .ok_or_else(|| PgError::error("attmissingval array has no element"))?;
    // Assert(!is_null);
    if *is_null {
        return Err(PgError::error("attmissingval element is unexpectedly NULL"));
    }
    Ok(Some(types_tuple::heaptuple::MissingValueImage::from_datum(
        datum,
    )))
}

/// `GETSTRUCT(pg_attribute_tuple)` field-for-field into
/// [`seam::ScannedPgAttribute`]. The `attrelid` column is read for the C-side
/// sanity assert (`Assert(attp->attrelid == ...)`) but not carried (the
/// relation owns its OID); reading it validates the row shape.
fn decode_pg_attribute<'mcx>(
    mcx: Mcx<'mcx>,
    row: &[DeformedColumn<'_>],
) -> PgResult<seam::ScannedPgAttribute> {
    // Assert(attp->attrelid == RelationGetRelid(relation)) — validate presence.
    let _attrelid = col(row, Anum_pg_attribute_attrelid, "attrelid")?.as_oid();
    let atttypid = col(row, Anum_pg_attribute_atttypid, "atttypid")?.as_oid();
    let attlen = col(row, Anum_pg_attribute_attlen, "attlen")?.as_i16();
    let attbyval = col(row, Anum_pg_attribute_attbyval, "attbyval")?.as_bool();
    let attalign = col(row, Anum_pg_attribute_attalign, "attalign")?.as_char();
    let atthasmissing =
        col(row, Anum_pg_attribute_atthasmissing, "atthasmissing")?.as_bool();
    Ok(seam::ScannedPgAttribute {
        attname: name_col(row, Anum_pg_attribute_attname, "attname")?,
        atttypid,
        attlen,
        attnum: col(row, Anum_pg_attribute_attnum, "attnum")?.as_i16(),
        atttypmod: col(row, Anum_pg_attribute_atttypmod, "atttypmod")?.as_i32(),
        attndims: col(row, Anum_pg_attribute_attndims, "attndims")?.as_i16(),
        attbyval,
        attalign,
        attstorage: col(row, Anum_pg_attribute_attstorage, "attstorage")?.as_char(),
        attcompression: col(row, Anum_pg_attribute_attcompression, "attcompression")?.as_char(),
        attnotnull: col(row, Anum_pg_attribute_attnotnull, "attnotnull")?.as_bool(),
        atthasdef: col(row, Anum_pg_attribute_atthasdef, "atthasdef")?.as_bool(),
        atthasmissing,
        attidentity: col(row, Anum_pg_attribute_attidentity, "attidentity")?.as_char(),
        attgenerated: col(row, Anum_pg_attribute_attgenerated, "attgenerated")?.as_char(),
        attisdropped: col(row, Anum_pg_attribute_attisdropped, "attisdropped")?.as_bool(),
        attislocal: col(row, Anum_pg_attribute_attislocal, "attislocal")?.as_bool(),
        attinhcount: col(row, Anum_pg_attribute_attinhcount, "attinhcount")?.as_i16(),
        attcollation: col(row, Anum_pg_attribute_attcollation, "attcollation")?.as_oid(),
        // If the column has a "missing" value, fetch + extract the single
        // array element (relcache.c's attmissingval branch).
        attmissingval: extract_attmissingval(
            mcx,
            row,
            atthasmissing,
            atttypid,
            attlen,
            attbyval,
            attalign,
        )?,
    })
}

// ===========================================================================
// relcache_scan_pg_index — RelationGetIndexList
// ===========================================================================

/// `int2vector` C struct image → the table column numbers. Layout mirrors
/// `buildint2vector`'s output: a varlena header (4) + ndim(4) + dataoffset(4)
/// + elemtype(4) + dim1(4) + lbound1(4) = 24-byte header, then `dim1` × `int16`
/// elements. (`pg_index.indkey` is always a 1-D `int2vector`.)
fn int2vector_elems(bytes: &[u8]) -> PgResult<Vec<AttrNumber>> {
    const HEADER: usize = 24;
    if bytes.len() < HEADER {
        return Err(PgError::error("int2vector image too short"));
    }
    let nelems = i32::from_ne_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    if nelems < 0 {
        return Err(PgError::error("int2vector has negative dim1"));
    }
    let nelems = nelems as usize;
    let need = HEADER + nelems * 2;
    if bytes.len() < need {
        return Err(PgError::error("int2vector image shorter than dim1 implies"));
    }
    let mut out = Vec::with_capacity(nelems);
    for i in 0..nelems {
        let off = HEADER + i * 2;
        out.push(i16::from_ne_bytes([bytes[off], bytes[off + 1]]));
    }
    Ok(out)
}

/// `RelationGetIndexList(relation)`'s `pg_index` scan (relcache.c).
fn relcache_scan_pg_index(relid: Oid) -> PgResult<Vec<seam::ScannedPgIndex>> {
    let scratch = MemoryContext::new("RelationGetIndexList scan");
    let smcx = scratch.mcx();

    // ScanKeyInit(&skey, Anum_pg_index_indrelid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(relid));
    let skey = [scan_key_init(
        Anum_pg_index_indrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?];

    let relation = table_open(smcx, IndexRelationId, AccessShareLock)?;
    let mut scandesc =
        systable_beginscan(&relation, IndexIndrelidIndexId, true, None, &skey)?;

    let mut out = Vec::new();
    while let Some(ntp) = systable_getnext(smcx, scandesc.desc_mut())? {
        let row = heap_deform_tuple(smcx, &ntp.tuple, &relation.rd_att, &ntp.data)?;
        let indkey = int2vector_elems(col(&row, Anum_pg_index_indkey, "indkey")?.as_ref_bytes())?;
        // heap_attisnull(htup, Anum_pg_index_indpred, NULL).
        let indpred_isnull = row
            .get((Anum_pg_index_indpred - 1) as usize)
            .map(|(_, isnull)| *isnull)
            .unwrap_or(true);
        out.push(seam::ScannedPgIndex {
            indexrelid: col(&row, Anum_pg_index_indexrelid, "indexrelid")?.as_oid(),
            indnatts: col(&row, Anum_pg_index_indnatts, "indnatts")?.as_i16(),
            indnkeyatts: col(&row, Anum_pg_index_indnkeyatts, "indnkeyatts")?.as_i16(),
            indisunique: col(&row, Anum_pg_index_indisunique, "indisunique")?.as_bool(),
            indnullsnotdistinct: col(&row, Anum_pg_index_indnullsnotdistinct, "indnullsnotdistinct")?
                .as_bool(),
            indisprimary: col(&row, Anum_pg_index_indisprimary, "indisprimary")?.as_bool(),
            indisexclusion: col(&row, Anum_pg_index_indisexclusion, "indisexclusion")?.as_bool(),
            indimmediate: col(&row, Anum_pg_index_indimmediate, "indimmediate")?.as_bool(),
            indisclustered: col(&row, Anum_pg_index_indisclustered, "indisclustered")?.as_bool(),
            indisvalid: col(&row, Anum_pg_index_indisvalid, "indisvalid")?.as_bool(),
            indcheckxmin: col(&row, Anum_pg_index_indcheckxmin, "indcheckxmin")?.as_bool(),
            indisready: col(&row, Anum_pg_index_indisready, "indisready")?.as_bool(),
            indislive: col(&row, Anum_pg_index_indislive, "indislive")?.as_bool(),
            indisreplident: col(&row, Anum_pg_index_indisreplident, "indisreplident")?.as_bool(),
            indkey,
            indpred_isnull,
        });
    }

    scandesc.end()?;
    table_close(relation, AccessShareLock)?;
    drop(scratch);
    Ok(out)
}

// ===========================================================================
// relcache_scan_pg_rewrite — RelationBuildRuleLock
// ===========================================================================

/// `text` node-string column → its `TextDatumGetCString`, or `None` when NULL.
/// The `ev_qual`/`ev_action`/`conbin`/`adbin` columns are stored `text` images;
/// `text_to_cstring` detoasts and copies the payload as a NUL-free `String`.
fn text_col_opt(
    mcx: Mcx<'_>,
    row: &[DeformedColumn<'_>],
    anum: i16,
) -> PgResult<Option<String>> {
    let idx = (anum - 1) as usize;
    let (datum, isnull) = match row.get(idx) {
        None => return Ok(None),
        Some(c) => c,
    };
    if *isnull {
        return Ok(None);
    }
    let s = backend_utils_adt_varlena_seams::text_to_cstring_v::call(mcx, datum)?;
    Ok(Some(s.as_str().to_string()))
}

/// `RelationBuildRuleLock(relation)`'s `pg_rewrite` scan (relcache.c).
fn relcache_scan_pg_rewrite(relid: Oid) -> PgResult<Vec<seam::ScannedPgRewrite>> {
    let scratch = MemoryContext::new("RelationBuildRuleLock scan");
    let smcx = scratch.mcx();

    // ScanKeyInit(&skey, Anum_pg_rewrite_ev_class, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));
    let skey = [scan_key_init(
        Anum_pg_rewrite_ev_class,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?];

    let relation = table_open(smcx, RewriteRelationId, AccessShareLock)?;
    // rewrite_tupdesc = RelationGetDescr(rewrite_desc);
    // rewrite_scan = systable_beginscan(rewrite_desc, RewriteRelRulesIndexId,
    //                                   true, NULL, 1, &key);
    let mut scandesc =
        systable_beginscan(&relation, REWRITE_REL_RULES_INDEX_ID, true, None, &skey)?;

    let mut out = Vec::new();
    while let Some(ntp) = systable_getnext(smcx, scandesc.desc_mut())? {
        let row = heap_deform_tuple(smcx, &ntp.tuple, &relation.rd_att, &ntp.data)?;
        // rule->ruleId = rewrite_form->oid; rule->event = ev_type - '0';
        // rule->enabled = ev_enabled; rule->isInstead = is_instead;
        out.push(seam::ScannedPgRewrite {
            ruleid: col(&row, Anum_pg_rewrite_oid, "pg_rewrite.oid")?.as_oid(),
            ev_type: col(&row, Anum_pg_rewrite_ev_type, "ev_type")?.as_char() as u8,
            ev_enabled: col(&row, Anum_pg_rewrite_ev_enabled, "ev_enabled")?.as_char() as u8,
            is_instead: col(&row, Anum_pg_rewrite_is_instead, "is_instead")?.as_bool(),
            // ev_qual = TextDatumGetCString(fastgetattr(... ev_qual ...)).
            ev_qual: text_col_opt(smcx, &row, Anum_pg_rewrite_ev_qual)?,
            // ev_action = TextDatumGetCString(fastgetattr(... ev_action ...)).
            ev_action: text_col_opt(smcx, &row, Anum_pg_rewrite_ev_action)?,
        });
    }

    scandesc.end()?;
    table_close(relation, AccessShareLock)?;
    drop(scratch);
    Ok(out)
}

// ===========================================================================
// relcache_scan_pg_statistic_ext — RelationGetStatExtList
// ===========================================================================

/// `RelationGetStatExtList(relation)`'s `pg_statistic_ext` scan (relcache.c).
fn relcache_scan_pg_statistic_ext(relid: Oid) -> PgResult<Vec<Oid>> {
    let scratch = MemoryContext::new("RelationGetStatExtList scan");
    let smcx = scratch.mcx();

    // ScanKeyInit(&skey, Anum_pg_statistic_ext_stxrelid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));
    let skey = [scan_key_init(
        Anum_pg_statistic_ext_stxrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?];

    let relation = table_open(smcx, StatisticExtRelationId, AccessShareLock)?;
    let mut scandesc =
        systable_beginscan(&relation, StatisticExtRelidIndexId, true, None, &skey)?;

    let mut out = Vec::new();
    while let Some(ntp) = systable_getnext(smcx, scandesc.desc_mut())? {
        let row = heap_deform_tuple(smcx, &ntp.tuple, &relation.rd_att, &ntp.data)?;
        // oid = ((Form_pg_statistic_ext) GETSTRUCT(htup))->oid;
        out.push(col(&row, Anum_pg_statistic_ext_oid, "pg_statistic_ext.oid")?.as_oid());
    }

    scandesc.end()?;
    table_close(relation, AccessShareLock)?;
    drop(scratch);
    Ok(out)
}

// ===========================================================================
// relcache_scan_pg_constraint_fkeys — RelationGetFKeyList
// ===========================================================================

/// `RelationGetFKeyList(relation)`'s `pg_constraint` scan (relcache.c): keep the
/// rows whose `contype == CONSTRAINT_FOREIGN` and record their OIDs (the
/// relcache only caches the list + the presence flag; the full
/// `ForeignKeyCacheInfo` deconstruct is done lazily elsewhere).
fn relcache_scan_pg_constraint_fkeys(relid: Oid) -> PgResult<Vec<seam::ScannedFkInfo>> {
    let scratch = MemoryContext::new("RelationGetFKeyList scan");
    let smcx = scratch.mcx();

    // ScanKeyInit(&skey, Anum_pg_constraint_conrelid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));
    let skey = [scan_key_init(
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?];

    let relation = table_open(smcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let mut scandesc = systable_beginscan(
        &relation,
        ConstraintRelidTypidNameIndexId,
        CONSTRAINT_CONRELID_INDEX_OK,
        None,
        &skey,
    )?;

    let mut out = Vec::new();
    while let Some(ntp) = systable_getnext(smcx, scandesc.desc_mut())? {
        let row = heap_deform_tuple(smcx, &ntp.tuple, &relation.rd_att, &ntp.data)?;
        // if (constraint->contype != CONSTRAINT_FOREIGN) continue;
        if col(&row, Anum_pg_constraint_contype, "contype")?.as_char() != CONSTRAINT_FOREIGN {
            continue;
        }
        out.push(seam::ScannedFkInfo {
            conoid: col(&row, Anum_pg_constraint_oid, "pg_constraint.oid")?.as_oid(),
        });
    }

    scandesc.end()?;
    table_close(relation, AccessShareLock)?;
    drop(scratch);
    Ok(out)
}

// ===========================================================================
// relcache_exclusion_info — RelationGetExclusionInfo
// ===========================================================================

/// 1-D `Oid[]` `ArrayType` C image → its elements. Mirrors `ARR_DIMS(arr)[0]`
/// + `memcpy(values, ARR_DATA_PTR(arr), sizeof(Oid) * nelem)` for a non-NULL,
/// 1-D array. Header: vl_len(4) + ndim(4) + dataoffset(4) + elemtype(4) +
/// dim0(4) + lbound0(4) = 24 bytes, then `dim0` × 4-byte Oids. (`conexclop`
/// is always a 1-D OID array with `indnkeyatts` elements, no NULLs.)
fn oid_array_elems(bytes: &[u8]) -> PgResult<Vec<Oid>> {
    const HEADER: usize = 24;
    if bytes.len() < HEADER {
        return Err(PgError::error("conexclop array image too short"));
    }
    let ndim = i32::from_ne_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if ndim != 1 {
        return Err(PgError::error("conexclop is not a 1-D array"));
    }
    let dataoffset = i32::from_ne_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    if dataoffset != 0 {
        // A non-zero dataoffset means the array carries a null bitmap; conexclop
        // never does.
        return Err(PgError::error("conexclop array unexpectedly has nulls"));
    }
    let nelems = i32::from_ne_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    if nelems < 0 {
        return Err(PgError::error("conexclop array has negative dim0"));
    }
    let nelems = nelems as usize;
    let need = HEADER + nelems * 4;
    if bytes.len() < need {
        return Err(PgError::error("conexclop array shorter than dim0 implies"));
    }
    let mut out = Vec::with_capacity(nelems);
    for i in 0..nelems {
        let off = HEADER + i * 4;
        out.push(u32::from_ne_bytes([
            bytes[off],
            bytes[off + 1],
            bytes[off + 2],
            bytes[off + 3],
        ]));
    }
    Ok(out)
}

/// `RelationGetExclusionInfo(indexRelation, ...)` (relcache.c).
fn relcache_exclusion_info(
    index_relid: Oid,
    indrelid: Oid,
    indnkeyatts: usize,
) -> PgResult<Vec<seam::ExclusionKeyInfo>> {
    let scratch = MemoryContext::new("RelationGetExclusionInfo scan");
    let smcx = scratch.mcx();

    // The index relation supplies rd_opfamily[i] for the per-column strategy
    // lookup; re-acquire it as a real cache-carrying handle (the index is
    // already open + locked by the relcache caller, so NoLock would suffice,
    // but AccessShareLock matches C's never-locked rd_opfamily read since the
    // entry is cached). We open it to read rd_opfamily.
    let index_rel =
        crate::indexam_index_open(smcx, index_relid)?;
    let rd_opfamily: Vec<Oid> = index_rel.rd_opfamily.iter().copied().collect();

    // ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(indrelid));
    let skey = [scan_key_init(
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(indrelid),
    )?];

    let conrel = table_open(smcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let mut scandesc = systable_beginscan(
        &conrel,
        ConstraintRelidTypidNameIndexId,
        true,
        None,
        &skey,
    )?;

    // Walk the matching constraints; keep the exclusion constraint whose
    // conindid is this index, decode its conexclop, then stop.
    let mut found_ops: Option<Vec<Oid>> = None;
    while let Some(ntp) = systable_getnext(smcx, scandesc.desc_mut())? {
        let row = heap_deform_tuple(smcx, &ntp.tuple, &conrel.rd_att, &ntp.data)?;
        // We're only interested in exclusion constraints.
        if col(&row, Anum_pg_constraint_contype, "contype")?.as_char() != CONSTRAINT_EXCLUSION {
            continue;
        }
        // The constraint should have the same index OID.
        if col(&row, Anum_pg_constraint_conindid, "conindid")?.as_oid() != index_relid {
            continue;
        }
        // Extract the conexclop array.
        let val = col(&row, Anum_pg_constraint_conexclop, "conexclop")?;
        found_ops = Some(oid_array_elems(val.as_ref_bytes())?);
        break;
    }

    scandesc.end()?;
    table_close(conrel, AccessShareLock)?;

    let ops = found_ops
        .ok_or_else(|| PgError::error("missing exclusion constraint for index"))?;
    if ops.len() < indnkeyatts {
        return Err(PgError::error("conexclop array shorter than index key columns"));
    }

    // For each column, resolve op → procedure (get_opcode) and op → strategy
    // within the index's opfamily (get_op_opfamily_strategy).
    let mut out = Vec::with_capacity(indnkeyatts);
    for i in 0..indnkeyatts {
        let op = ops[i];
        let proc = backend_utils_cache_lsyscache_seams::get_opcode::call(op)?;
        let opfamily = rd_opfamily
            .get(i)
            .copied()
            .ok_or_else(|| PgError::error("index rd_opfamily shorter than key columns"))?;
        let strat =
            backend_utils_cache_lsyscache_seams::get_op_opfamily_strategy::call(op, opfamily)?;
        out.push(seam::ExclusionKeyInfo {
            op,
            proc,
            strat: strat as u16,
        });
    }

    // index_close(indexRelation, AccessShareLock) — drop the handle we acquired.
    crate::indexam_index_close(index_rel)?;
    drop(scratch);
    Ok(out)
}

// ===========================================================================
// scan_pg_attrdef — AttrDefaultFetch
// ===========================================================================

/// `AttrDefaultFetch(relation, ndef)`'s `pg_attrdef` scan (relcache.c).
fn scan_pg_attrdef(relid: Oid) -> PgResult<Vec<seam::PgAttrdefRow>> {
    let scratch = MemoryContext::new("AttrDefaultFetch scan");
    let smcx = scratch.mcx();

    // ScanKeyInit(&skey, Anum_pg_attrdef_adrelid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));
    let skey = [scan_key_init(
        types_catalog::pg_attrdef::Anum_pg_attrdef_adrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?];

    let relation = table_open(smcx, AttrDefaultRelationId, AccessShareLock)?;
    let mut scandesc =
        systable_beginscan(&relation, AttrDefaultIndexId, true, None, &skey)?;

    let mut out = Vec::new();
    while let Some(ntp) = systable_getnext(smcx, scandesc.desc_mut())? {
        let row = heap_deform_tuple(smcx, &ntp.tuple, &relation.rd_att, &ntp.data)?;
        out.push(seam::PgAttrdefRow {
            adnum: col(&row, Anum_pg_attrdef_adnum, "adnum")?.as_i16(),
            // val = fastgetattr(htup, Anum_pg_attrdef_adbin, ...);
            // TextDatumGetCString(val).
            adbin: text_col_opt(smcx, &row, Anum_pg_attrdef_adbin)?,
        });
    }

    scandesc.end()?;
    table_close(relation, AccessShareLock)?;
    drop(scratch);
    Ok(out)
}

// ===========================================================================
// scan_pg_constraint_nncheck — CheckNNConstraintFetch
// ===========================================================================

/// `extractNotNullColumn(htup)` (pg_constraint.c) inline: a not-null
/// constraint's `conkey` is a 1-D smallint array with a single element. The
/// `conkey` column comes back as the array image; decode its sole element.
fn extract_not_null_column(row: &[DeformedColumn<'_>]) -> PgResult<AttrNumber> {
    let val = col(row, types_catalog::pg_constraint::Anum_pg_constraint_conkey, "conkey")?;
    let bytes = val.as_ref_bytes();
    // 1-D int2 array: header(24) then one int16.
    const HEADER: usize = 24;
    let ndim = if bytes.len() >= 8 {
        i32::from_ne_bytes([bytes[4], bytes[5], bytes[6], bytes[7]])
    } else {
        return Err(PgError::error("conkey is not a 1-D smallint array"));
    };
    let dim0 = if bytes.len() >= 20 {
        i32::from_ne_bytes([bytes[16], bytes[17], bytes[18], bytes[19]])
    } else {
        return Err(PgError::error("conkey is not a 1-D smallint array"));
    };
    if ndim != 1 || dim0 != 1 || bytes.len() < HEADER + 2 {
        return Err(PgError::error("conkey is not a 1-D smallint array"));
    }
    Ok(i16::from_ne_bytes([bytes[HEADER], bytes[HEADER + 1]]))
}

/// `CheckNNConstraintFetch(relation)`'s `pg_constraint` scan (relcache.c).
fn scan_pg_constraint_nncheck(relid: Oid) -> PgResult<Vec<seam::PgConstraintNnCheckRow>> {
    let scratch = MemoryContext::new("CheckNNConstraintFetch scan");
    let smcx = scratch.mcx();

    // ScanKeyInit(&skey, Anum_pg_constraint_conrelid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));
    let skey = [scan_key_init(
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?];

    let relation = table_open(smcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let mut scandesc = systable_beginscan(
        &relation,
        ConstraintRelidTypidNameIndexId,
        true,
        None,
        &skey,
    )?;

    let mut out = Vec::new();
    while let Some(ntp) = systable_getnext(smcx, scandesc.desc_mut())? {
        let row = heap_deform_tuple(smcx, &ntp.tuple, &relation.rd_att, &ntp.data)?;
        let contype = col(&row, Anum_pg_constraint_contype, "contype")?.as_char();

        let mut dto = seam::PgConstraintNnCheckRow {
            contype,
            notnull_invalid: false,
            notnull_attnum: 0,
            ccenforced: false,
            ccvalid: false,
            ccnoinherit: false,
            ccname: String::new(),
            ccbin: None,
        };

        if contype == CONSTRAINT_NOTNULL {
            // if (!conform->convalidated) { mark invalid; attnum =
            // extractNotNullColumn(htup); }
            let convalidated =
                col(&row, Anum_pg_constraint_convalidated, "convalidated")?.as_bool();
            dto.notnull_invalid = !convalidated;
            if dto.notnull_invalid {
                dto.notnull_attnum = extract_not_null_column(&row)?;
            }
        } else if contype == CONSTRAINT_CHECK {
            dto.ccenforced = col(&row, Anum_pg_constraint_conenforced, "conenforced")?.as_bool();
            dto.ccvalid = col(&row, Anum_pg_constraint_convalidated, "convalidated")?.as_bool();
            dto.ccnoinherit = col(&row, Anum_pg_constraint_connoinherit, "connoinherit")?.as_bool();
            dto.ccname = name_col(&row, Anum_pg_constraint_conname, "conname")?;
            // conbin = TextDatumGetCString(fastgetattr(... conbin ...)).
            dto.ccbin = text_col_opt(smcx, &row, Anum_pg_constraint_conbin)?;
        }
        // Other constraint kinds are returned with only `contype` set; the
        // relcache `CheckNNConstraintFetch` accounting skips them.

        out.push(dto);
    }

    scandesc.end()?;
    table_close(relation, AccessShareLock)?;
    drop(scratch);
    Ok(out)
}

// ===========================================================================
// install
// ===========================================================================

/// Install the relcache catalog scan-and-decode seams.
pub fn init_decode_seams() {
    seam::scan_pg_class::set(scan_pg_class);
    seam::scan_pg_attribute::set(scan_pg_attribute);
    seam::relcache_scan_pg_index::set(relcache_scan_pg_index);
    seam::relcache_scan_pg_rewrite::set(relcache_scan_pg_rewrite);
    seam::relcache_scan_pg_statistic_ext::set(relcache_scan_pg_statistic_ext);
    seam::relcache_scan_pg_constraint_fkeys::set(relcache_scan_pg_constraint_fkeys);
    seam::relcache_exclusion_info::set(relcache_exclusion_info);
    seam::scan_pg_attrdef::set(scan_pg_attrdef);
    seam::scan_pg_constraint_nncheck::set(scan_pg_constraint_nncheck);
}
