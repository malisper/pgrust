//! `backend-catalog-pg-database` — the `pg_database` catalog read + mutate
//! owner.
//!
//! This unit owns the marshaling between the on-disk `pg_database` tuple and the
//! decoded [`FormPgDatabase`] / [`NewDbRecord`] carriers in `types-catalog`, so
//! that consumers (postinit.c's startup checks, and — when it lands —
//! dbcommands.c's `CREATE`/`DROP`/`ALTER DATABASE`) never touch the datum
//! layout. It is the keystone that unblocks `backend-commands-dbcommands`.
//!
//! READ side (3 seams consumed by postinit.c): scan `pg_database` by name / by
//! OID (`table_open` is the caller's; this crate runs `systable_beginscan` +
//! `systable_getnext` + `heap_deform_tuple` + the varlena decode) and the
//! syscache read (`SearchSysCache1(DATABASEOID)` + decode).
//!
//! MUTATE side (the read-modify-write surface dbcommands.c needs): form one row
//! from a `NewDbRecord` and `CatalogTupleInsert` it (createdb); the locked
//! scan + transactional `CatalogTupleUpdate` + `UnlockTuple` pair (the `ALTER
//! DATABASE` family); the in-place `datconnlimit = DATCONNLIMIT_INVALID_DB`
//! mark via genam's in-place update (dropdb); and the transactional
//! `CatalogTupleDelete` (dropdb).
//!
//! The catalog-mutation engine (`CatalogTupleInsert`/`Update`/`Delete`,
//! `catalog/indexing.c`) is consumed as `pub` functions from
//! `backend-catalog-indexing`, exactly as that crate's F1 per-catalog family
//! does (no dependency cycle).

#![allow(non_snake_case)]

use mcx::{Mcx, PgString, PgVec};
use types_catalog::pg_database as cat;
use types_catalog::pg_database::{FormPgDatabase, NewDbRecord};
use types_core::primitive::{AttrNumber, Oid};
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ, NAMEDATALEN};
use types_error::{PgError, PgResult};
use types_rel::Relation;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessShareLock, InplaceUpdateTupleLock};
use types_tuple::backend_access_common_heaptuple::{Datum, DeformedColumn, FormedTuple};
use types_tuple::heaptuple::{ItemPointerData, TupleDescData};

use backend_access_common_heaptuple::{heap_deform_tuple, heap_form_tuple};
use backend_access_common_scankey::ScanKeyInit;
use backend_catalog_indexing::keystone::{
    CatalogTupleDelete, CatalogTupleInsert, CatalogTupleUpdate,
};

use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table_seams as table_seams;
use backend_utils_cache_relcache_seams as relcache_seams;
use backend_storage_lmgr_lmgr_seams as lmgr_seams;
use backend_utils_adt_varlena_seams as varlena_seams;
use backend_utils_cache_syscache::SearchSysCache1;
use types_cache::SysCacheKey;
use types_datum::Datum as KeyDatum;
use types_syscache::DATABASEOID;

/* ==========================================================================
 * Scan-key builders.
 * ========================================================================== */

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: AttrNumber, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(&mut key, attno, BTEqualStrategyNumber, F_OIDEQ, Datum::from_oid(value))?;
    Ok(key)
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_NAMEEQ,
/// CStringGetDatum(value))`. The name crosses as a NUL-terminated byte image
/// (the genam `nameeq` comparator interprets it).
fn name_key<'mcx>(mcx: Mcx<'mcx>, attno: AttrNumber, value: &str) -> PgResult<ScanKeyData<'mcx>> {
    let mut bytes: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, value.len() + 1)?;
    for &b in value.as_bytes() {
        bytes.push(b);
    }
    bytes.push(0);
    let mut key = ScanKeyData::empty();
    ScanKeyInit(&mut key, attno, BTEqualStrategyNumber, F_NAMEEQ, Datum::ByRef(bytes))?;
    Ok(key)
}

/* ==========================================================================
 * Decode: one scanned/cached pg_database tuple -> FormPgDatabase.
 * ========================================================================== */

/// Read a `NameData` (64-byte, NUL-padded) by-value image out of a deformed
/// column, as a `PgString` (the bytes up to the first NUL). The C reads it via
/// `NameStr(datform->datname)`.
fn name_to_string<'mcx>(mcx: Mcx<'mcx>, col: &DeformedColumn<'mcx>) -> PgResult<PgString<'mcx>> {
    let bytes: &[u8] = match &col.0 {
        Datum::ByRef(b) => b,
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => &[],
    };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let s = core::str::from_utf8(&bytes[..end]).map_err(|_| {
        PgError::error("pg_database name column is not valid UTF-8")
    })?;
    PgString::from_str_in(s, mcx)
}

/// `TextDatumGetCString(datum)` for one (not-null) `text` column — detoast +
/// copy the payload out as a `PgString` (`text_to_cstring`, varlena.c).
fn text_to_string<'mcx>(mcx: Mcx<'mcx>, col: &DeformedColumn<'mcx>) -> PgResult<PgString<'mcx>> {
    varlena_seams::text_to_cstring_v::call(mcx, &col.0)
}

/// Decode the `Natts_pg_database` deformed columns into an owned
/// [`FormPgDatabase`] (the `heap_copytuple` + `GETSTRUCT`/`SysCacheGetAttr`
/// analog). `cols[i]` is the `(value, isnull)` for `Anum_pg_database_* = i+1`.
fn decode_form<'mcx>(
    mcx: Mcx<'mcx>,
    cols: &PgVec<'mcx, DeformedColumn<'mcx>>,
) -> PgResult<FormPgDatabase<'mcx>> {
    let col = |attno: i32| &cols[(attno - 1) as usize];

    // BKI_FORCE_NOT_NULL text columns.
    let datcollate = text_to_string(mcx, col(cat::Anum_pg_database_datcollate))?;
    let datctype = text_to_string(mcx, col(cat::Anum_pg_database_datctype))?;

    // Nullable text columns.
    let opt_text = |mcx: Mcx<'mcx>, attno: i32| -> PgResult<Option<PgString<'mcx>>> {
        let c = col(attno);
        if c.1 {
            Ok(None)
        } else {
            Ok(Some(text_to_string(mcx, c)?))
        }
    };
    let datlocale = opt_text(mcx, cat::Anum_pg_database_datlocale)?;
    let daticurules = opt_text(mcx, cat::Anum_pg_database_daticurules)?;
    let datcollversion = opt_text(mcx, cat::Anum_pg_database_datcollversion)?;

    // datacl: keep the raw detoasted varlena bytes (or None for NULL).
    let datacl = {
        let c = col(cat::Anum_pg_database_datacl);
        if c.1 {
            None
        } else {
            match &c.0 {
                Datum::ByRef(b) => Some(mcx::slice_in(mcx, b)?),
                // An aclitem[] array is always pass-by-reference; a ByVal here
                // is impossible for this column, but keep the bytes empty
                // rather than fabricate.
                Datum::ByVal(_)
                | Datum::Cstring(_)
                | Datum::Composite(_)
                | Datum::Expanded(_)
                | Datum::Internal(_) => Some(mcx::vec_with_capacity_in(mcx, 0)?),
            }
        }
    };

    Ok(FormPgDatabase {
        oid: col(cat::Anum_pg_database_oid).0.as_oid(),
        datname: name_to_string(mcx, col(cat::Anum_pg_database_datname))?,
        datdba: col(cat::Anum_pg_database_datdba).0.as_oid(),
        encoding: col(cat::Anum_pg_database_encoding).0.as_i32(),
        datlocprovider: col(cat::Anum_pg_database_datlocprovider).0.as_i8(),
        datistemplate: col(cat::Anum_pg_database_datistemplate).0.as_bool(),
        datallowconn: col(cat::Anum_pg_database_datallowconn).0.as_bool(),
        dathasloginevt: col(cat::Anum_pg_database_dathasloginevt).0.as_bool(),
        datconnlimit: col(cat::Anum_pg_database_datconnlimit).0.as_i32(),
        datfrozenxid: col(cat::Anum_pg_database_datfrozenxid).0.as_transaction_id(),
        datminmxid: col(cat::Anum_pg_database_datminmxid).0.as_transaction_id(),
        dattablespace: col(cat::Anum_pg_database_dattablespace).0.as_oid(),
        datcollate,
        datctype,
        datlocale,
        daticurules,
        datcollversion,
        datacl,
    })
}

/// Deform a freshly scanned/cached `pg_database` [`FormedTuple`] into the
/// `Natts_pg_database` `(value, isnull)` columns, then [`decode_form`] it.
fn deform_and_decode<'mcx>(
    mcx: Mcx<'mcx>,
    rel_desc: &TupleDescData<'_>,
    tup: &FormedTuple<'mcx>,
) -> PgResult<FormPgDatabase<'mcx>> {
    let cols = heap_deform_tuple(mcx, &tup.tuple, rel_desc, &tup.data)?;
    decode_form(mcx, &cols)
}

/* ==========================================================================
 * Encode: NewDbRecord / FormPgDatabase -> values[] + nulls[] for forming.
 * ========================================================================== */

/// `CStringGetTextDatum(s)` — build a `text` varlena `Datum` (varlena.c).
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `namein(s)` — a 64-byte NUL-padded `NameData` by-reference Datum image.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    let mut image: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, NAMEDATALEN as usize)?;
    let src = s.as_bytes();
    let take = core::cmp::min(src.len(), (NAMEDATALEN as usize) - 1);
    for &b in &src[..take] {
        image.push(b);
    }
    while image.len() < NAMEDATALEN as usize {
        image.push(0);
    }
    Ok(Datum::ByRef(image))
}

/// Build the `values[]`/`nulls[]` arrays for a `pg_database` row from a fully
/// specified set of columns. `datacl` is always NULL here (createdb sets it
/// to default; the `ALTER` re-form path that carries an existing `datacl`
/// supplies its bytes separately via [`form_from_existing`]).
struct DbColumns<'mcx> {
    values: [Datum<'mcx>; cat::Natts_pg_database],
    nulls: [bool; cat::Natts_pg_database],
}

/// Form the column arrays from a [`NewDbRecord`] (createdb).
fn columns_from_new_record<'mcx>(
    mcx: Mcx<'mcx>,
    r: &NewDbRecord<'mcx>,
) -> PgResult<DbColumns<'mcx>> {
    let mut nulls = [false; cat::Natts_pg_database];
    let idx = |attno: i32| (attno - 1) as usize;

    let mut values: [Datum<'mcx>; cat::Natts_pg_database] = core::array::from_fn(|_| Datum::null());
    values[idx(cat::Anum_pg_database_oid)] = Datum::from_oid(r.oid);
    values[idx(cat::Anum_pg_database_datname)] = name_datum(mcx, r.datname.as_str())?;
    values[idx(cat::Anum_pg_database_datdba)] = Datum::from_oid(r.datdba);
    values[idx(cat::Anum_pg_database_encoding)] = Datum::from_i32(r.encoding);
    values[idx(cat::Anum_pg_database_datlocprovider)] = Datum::from_i8(r.datlocprovider);
    values[idx(cat::Anum_pg_database_datistemplate)] = Datum::from_bool(r.datistemplate);
    values[idx(cat::Anum_pg_database_datallowconn)] = Datum::from_bool(r.datallowconn);
    values[idx(cat::Anum_pg_database_dathasloginevt)] = Datum::from_bool(r.dathasloginevt);
    values[idx(cat::Anum_pg_database_datconnlimit)] = Datum::from_i32(r.datconnlimit);
    values[idx(cat::Anum_pg_database_datfrozenxid)] = Datum::from_transaction_id(r.datfrozenxid);
    values[idx(cat::Anum_pg_database_datminmxid)] = Datum::from_transaction_id(r.datminmxid);
    values[idx(cat::Anum_pg_database_dattablespace)] = Datum::from_oid(r.dattablespace);
    values[idx(cat::Anum_pg_database_datcollate)] = text_datum(mcx, r.datcollate.as_str())?;
    values[idx(cat::Anum_pg_database_datctype)] = text_datum(mcx, r.datctype.as_str())?;

    match &r.datlocale {
        Some(s) => values[idx(cat::Anum_pg_database_datlocale)] = text_datum(mcx, s.as_str())?,
        None => nulls[idx(cat::Anum_pg_database_datlocale)] = true,
    }
    match &r.daticurules {
        Some(s) => values[idx(cat::Anum_pg_database_daticurules)] = text_datum(mcx, s.as_str())?,
        None => nulls[idx(cat::Anum_pg_database_daticurules)] = true,
    }
    match &r.datcollversion {
        Some(s) => values[idx(cat::Anum_pg_database_datcollversion)] = text_datum(mcx, s.as_str())?,
        None => nulls[idx(cat::Anum_pg_database_datcollversion)] = true,
    }

    // datacl deliberately default (NULL) at create time.
    nulls[idx(cat::Anum_pg_database_datacl)] = true;

    Ok(DbColumns { values, nulls })
}

/// Form the column arrays from an existing (modified) [`FormPgDatabase`] for a
/// re-form + `CatalogTupleUpdate` (the `ALTER DATABASE` family). Carries the
/// existing `datacl` bytes through unchanged.
fn columns_from_existing<'mcx>(
    mcx: Mcx<'mcx>,
    f: &FormPgDatabase<'mcx>,
) -> PgResult<DbColumns<'mcx>> {
    let mut nulls = [false; cat::Natts_pg_database];
    let idx = |attno: i32| (attno - 1) as usize;

    let mut values: [Datum<'mcx>; cat::Natts_pg_database] = core::array::from_fn(|_| Datum::null());
    values[idx(cat::Anum_pg_database_oid)] = Datum::from_oid(f.oid);
    values[idx(cat::Anum_pg_database_datname)] = name_datum(mcx, f.datname.as_str())?;
    values[idx(cat::Anum_pg_database_datdba)] = Datum::from_oid(f.datdba);
    values[idx(cat::Anum_pg_database_encoding)] = Datum::from_i32(f.encoding);
    values[idx(cat::Anum_pg_database_datlocprovider)] = Datum::from_i8(f.datlocprovider);
    values[idx(cat::Anum_pg_database_datistemplate)] = Datum::from_bool(f.datistemplate);
    values[idx(cat::Anum_pg_database_datallowconn)] = Datum::from_bool(f.datallowconn);
    values[idx(cat::Anum_pg_database_dathasloginevt)] = Datum::from_bool(f.dathasloginevt);
    values[idx(cat::Anum_pg_database_datconnlimit)] = Datum::from_i32(f.datconnlimit);
    values[idx(cat::Anum_pg_database_datfrozenxid)] = Datum::from_transaction_id(f.datfrozenxid);
    values[idx(cat::Anum_pg_database_datminmxid)] = Datum::from_transaction_id(f.datminmxid);
    values[idx(cat::Anum_pg_database_dattablespace)] = Datum::from_oid(f.dattablespace);
    values[idx(cat::Anum_pg_database_datcollate)] = text_datum(mcx, f.datcollate.as_str())?;
    values[idx(cat::Anum_pg_database_datctype)] = text_datum(mcx, f.datctype.as_str())?;

    match &f.datlocale {
        Some(s) => values[idx(cat::Anum_pg_database_datlocale)] = text_datum(mcx, s.as_str())?,
        None => nulls[idx(cat::Anum_pg_database_datlocale)] = true,
    }
    match &f.daticurules {
        Some(s) => values[idx(cat::Anum_pg_database_daticurules)] = text_datum(mcx, s.as_str())?,
        None => nulls[idx(cat::Anum_pg_database_daticurules)] = true,
    }
    match &f.datcollversion {
        Some(s) => values[idx(cat::Anum_pg_database_datcollversion)] = text_datum(mcx, s.as_str())?,
        None => nulls[idx(cat::Anum_pg_database_datcollversion)] = true,
    }
    match &f.datacl {
        Some(b) => values[idx(cat::Anum_pg_database_datacl)] = Datum::ByRef(mcx::slice_in(mcx, b)?),
        None => nulls[idx(cat::Anum_pg_database_datacl)] = true,
    }

    Ok(DbColumns { values, nulls })
}

/* ==========================================================================
 * Scan helper: by name / by oid, first matching live tuple.
 * ========================================================================== */

/// `systable_beginscan(rel, indexId, indexOK, NULL, 1, &key)` +
/// `systable_getnext(scan)` (the first row) + `systable_endscan(scan)` — the
/// shared scan that `GetDatabaseTuple`/`GetDatabaseTupleByOid` and the locked
/// ALTER read perform. Returns the first matching tuple (copied into `mcx`) and
/// the `DatabaseNameIndexId`/`DatabaseOidIndexId` it used. `None` if no match.
///
/// `index_ok` mirrors the C `indexOK` arg: `GetDatabaseTuple[ByOid]` pass
/// `criticalSharedRelcachesBuilt` so that during early `InitPostgres` startup
/// (before the critical shared relcache entries are nailed) the scan falls back
/// to a heap scan — opening `DatabaseNameIndexId` would recursively try to build
/// the still-absent `pg_class` relcache entry. The ALTER/DROP read paths run
/// well after startup and always pass `true` (matching dbcommands.c).
fn scan_first<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    index_id: Oid,
    index_ok: bool,
    key: ScanKeyData<'mcx>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let keys = [key];
    let mut scan =
        genam_seams::systable_beginscan::call(rel, index_id, index_ok, None, &keys)?;
    let tup = genam_seams::systable_getnext::call(mcx, scan.desc_mut())?;
    scan.end()?;
    Ok(tup)
}

/* ==========================================================================
 * READ seams (consumed by postinit.c).
 * ========================================================================== */

/// `GetDatabaseTuple(dbname)` (postinit.c).
fn get_database_tuple_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    dbname: &str,
) -> PgResult<Option<FormPgDatabase<'mcx>>> {
    // The C opens pg_database AccessShareLock for this scan; the read seam owns
    // the open/close (postinit only asks for the decoded row).
    let rel = table_seams::table_open::call(mcx, cat::DatabaseRelationId, AccessShareLock)?;
    let key = name_key(mcx, cat::Anum_pg_database_datname as AttrNumber, dbname)?;
    // C: systable_beginscan(rel, DatabaseNameIndexId, criticalSharedRelcachesBuilt, ...)
    let index_ok = relcache_seams::critical_shared_relcaches_built::call();
    let result = scan_first(mcx, &rel, cat::DatabaseNameIndexId, index_ok, key)?;
    let decoded = match &result {
        Some(tup) => {
            let desc = rel.rd_att_clone_in(mcx)?;
            Some(deform_and_decode(mcx, &desc, tup)?)
        }
        None => None,
    };
    rel.close(AccessShareLock)?;
    Ok(decoded)
}

/// `GetDatabaseTupleByOid(dboid)` (postinit.c).
fn get_database_tuple_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    dboid: Oid,
) -> PgResult<Option<FormPgDatabase<'mcx>>> {
    let rel = table_seams::table_open::call(mcx, cat::DatabaseRelationId, AccessShareLock)?;
    let key = oid_key(cat::Anum_pg_database_oid as AttrNumber, dboid)?;
    // C: systable_beginscan(rel, DatabaseOidIndexId, criticalSharedRelcachesBuilt, ...)
    let index_ok = relcache_seams::critical_shared_relcaches_built::call();
    let result = scan_first(mcx, &rel, cat::DatabaseOidIndexId, index_ok, key)?;
    let decoded = match &result {
        Some(tup) => {
            let desc = rel.rd_att_clone_in(mcx)?;
            Some(deform_and_decode(mcx, &desc, tup)?)
        }
        None => None,
    };
    rel.close(AccessShareLock)?;
    Ok(decoded)
}

/// `SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dboid))` + decode
/// (`CheckMyDatabase`, postinit.c). The decode needs the relation descriptor;
/// the syscache tuple carries its own descriptor reference, but the deform here
/// opens pg_database AccessShareLock for the descriptor (the C reads the columns
/// via the cached tuple's `tupleDescriptor`). `None` on a cache miss.
fn search_database_syscache<'mcx>(
    mcx: Mcx<'mcx>,
    dboid: Oid,
) -> PgResult<Option<FormPgDatabase<'mcx>>> {
    let tuple = SearchSysCache1(mcx, DATABASEOID, SysCacheKey::Value(KeyDatum::from_oid(dboid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let rel = table_seams::table_open::call(mcx, cat::DatabaseRelationId, AccessShareLock)?;
    let desc = rel.rd_att_clone_in(mcx)?;
    let decoded = deform_and_decode(mcx, &desc, &tup)?;
    rel.close(AccessShareLock)?;
    Ok(Some(decoded))
}

/* ==========================================================================
 * MUTATE seams.
 * ========================================================================== */

/// `heap_form_tuple(RelationGetDescr(rel), values, nulls)` +
/// `CatalogTupleInsert(rel, tuple)` (createdb).
fn insert_pg_database<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    record: &NewDbRecord<'mcx>,
) -> PgResult<()> {
    let DbColumns { values, nulls } = columns_from_new_record(mcx, record)?;
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    CatalogTupleInsert(mcx, rel, &mut tup)?;
    Ok(())
}

/// `CatalogTupleDelete(rel, &tup->t_self)` (dropdb transactional delete).
fn delete_pg_database<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: ItemPointerData,
) -> PgResult<()> {
    CatalogTupleDelete(mcx, rel, tid)
}

/// Re-form the row at `otid` from the modified [`FormPgDatabase`],
/// `CatalogTupleUpdate(rel, otid, newtuple)`, then `UnlockTuple(rel, &otid,
/// InplaceUpdateTupleLock)` (the `ALTER DATABASE` family).
fn update_pg_database<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    otid: ItemPointerData,
    form: &FormPgDatabase<'mcx>,
) -> PgResult<()> {
    let DbColumns { values, nulls } = columns_from_existing(mcx, form)?;
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    // newtup->t_self is set by simple_heap_update to the new TID; the C passes
    // &otid (the old TID) to CatalogTupleUpdate / UnlockTuple.
    CatalogTupleUpdate(mcx, rel, otid, &mut tup)?;
    lmgr_seams::unlock_tuple::call(rel.rd_id, otid, InplaceUpdateTupleLock)?;
    Ok(())
}

/// Locked scan by OID / by name + `LockTuple(rel, &tup->t_self,
/// InplaceUpdateTupleLock)` + decode (the `ALTER DATABASE` read).
fn scan_pg_database_locked_for_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    _my_database_id: Oid,
    by_oid: bool,
    dboid: Oid,
    dbname: &str,
) -> PgResult<Option<(ItemPointerData, FormPgDatabase<'mcx>)>> {
    let (index_id, key) = if by_oid {
        (
            cat::DatabaseOidIndexId,
            oid_key(cat::Anum_pg_database_oid as AttrNumber, dboid)?,
        )
    } else {
        (
            cat::DatabaseNameIndexId,
            name_key(mcx, cat::Anum_pg_database_datname as AttrNumber, dbname)?,
        )
    };

    // dbcommands.c: ALTER/DROP DATABASE always passes indexOK=true (runs well
    // after the critical shared relcache entries are nailed).
    let result = scan_first(mcx, rel, index_id, true, key)?;
    let Some(tup) = result else {
        return Ok(None);
    };
    let tid = tup.tuple.t_self;

    // LockTuple(rel, &oldtuple->t_self, InplaceUpdateTupleLock).
    lmgr_seams::lock_tuple::call(rel.rd_id, tid, InplaceUpdateTupleLock)?;

    let desc = rel.rd_att_clone_in(mcx)?;
    let form = deform_and_decode(mcx, &desc, &tup)?;
    Ok(Some((tid, form)))
}

/// The dropdb in-place invalidate: scan by name + in-place set of
/// `datconnlimit = DATCONNLIMIT_INVALID_DB` + `XLogFlush`, returning the row's
/// `t_self`. The in-place flow (buffer lock + WAL) runs entirely in genam's
/// `systable_inplace_update` seam.
fn set_pg_database_invalid_inplace<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    dbname: &str,
) -> PgResult<Option<ItemPointerData>> {
    let key = name_key(mcx, cat::Anum_pg_database_datname as AttrNumber, dbname)?;
    let keys = [key];

    // The mutate callback overwrites the fixed-width datconnlimit column in the
    // tuple's user-data area. The column is a 4-byte int4 at the fixed offset
    // the descriptor places it (preceded by the fixed-width oid/datname/datdba/
    // encoding/datlocprovider/datistemplate/datallowconn/dathasloginevt
    // columns); rather than recompute the offset here, the owner of the in-place
    // flow (genam) re-deforms against the descriptor and re-forms the one column
    // — see the seam contract. The callback writes the little-endian int4 value
    // DATCONNLIMIT_INVALID_DB into the column the owner addresses.
    let mut mutate = |datconnlimit_bytes: &mut [u8]| -> PgResult<bool> {
        let v = (cat::DATCONNLIMIT_INVALID_DB as i32).to_ne_bytes();
        if datconnlimit_bytes.len() != v.len() {
            return Err(PgError::error(
                "set_pg_database_invalid_inplace: datconnlimit column is not 4 bytes",
            ));
        }
        datconnlimit_bytes.copy_from_slice(&v);
        // dropdb always overwrites datconnlimit → always dirty (always _finish).
        Ok(true)
    };

    let tid = genam_seams::systable_inplace_update::call(
        mcx,
        rel,
        cat::DatabaseNameIndexId,
        true,
        &keys,
        &mut mutate,
    )?;

    // XLogFlush(XactLastRecEnd) — guarantee the in-place mark is durable before
    // the caller performs the irreversible filesystem operations of DROP.
    if tid.is_some() {
        let lsn = backend_access_transam_xlog_seams::xact_last_rec_end::call();
        backend_access_transam_xlog_seams::xlog_flush::call(lsn)?;
    }

    Ok(tid)
}

/// Install every inward seam this unit owns. Wired into `seams-init::init_all`.
pub fn init_seams() {
    use backend_catalog_pg_database_seams as s;
    // READ seams (consumed by postinit.c).
    s::get_database_tuple_by_name::set(get_database_tuple_by_name);
    s::get_database_tuple_by_oid::set(get_database_tuple_by_oid);
    s::search_database_syscache::set(search_database_syscache);
    // MUTATE seams (the read-modify-write surface dbcommands.c needs).
    s::insert_pg_database::set(insert_pg_database);
    s::delete_pg_database::set(delete_pg_database);
    s::update_pg_database::set(update_pg_database);
    s::scan_pg_database_locked_for_update::set(scan_pg_database_locked_for_update);
    s::set_pg_database_invalid_inplace::set(set_pg_database_invalid_inplace);
}
