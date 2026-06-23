#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `pg_ts_dict` / `pg_ts_config` / `pg_ts_config_map` catalog read+write value
//! layer for `commands/tsearchcmds.c`'s `DefineTSDictionary` /
//! `DefineTSConfiguration` / `RemoveTSConfigurationById` (and the COPY
//! map-copy path).
//!
//! The `tsearchcmds.c` orchestration (in `backend-commands-tsearchcmds`) lives
//! over the owned `'mcx` node tree and does not model the C's opened
//! `Relation`/`HeapTuple` handles. Each catalog read/write crosses a
//! self-contained seam declared in [`tsearchcmds_seams`]; this
//! crate installs those seams. Every body opens the catalog by OID in a private
//! `MemoryContext`, allocates the row OID with `GetNewOidWithIndex`, forms the
//! heap tuple against the relation's descriptor, and runs `CatalogTupleInsert`
//! (index maintenance included) — the same precedent as
//! `backend-catalog-indexing`'s `family_opclass` / `family_authid` no-`mcx`
//! inserts. The map scans/deletes mirror `backend-catalog-pg-depend`'s
//! `systable_beginscan` + `CatalogTupleDelete` loop.

extern crate alloc;

use alloc::vec;
use alloc::vec::Vec;

use mcx::{Mcx, MemoryContext, PgVec};
use ::cache::SysCacheKey;
use ::types_core::primitive::{AttrNumber, Oid};
use ::datum::Datum as ScalarWord;
use types_error::{PgError, PgResult};
use rel::{Relation, RelationData};
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use ::types_storage::lock::RowExclusiveLock;
use ::types_tuple::heaptuple::Datum;
use ::types_tuple::heaptuple::ItemPointerData;

use heaptuple::{heap_deform_tuple, heap_form_tuple};
use ::scankey::ScanKeyInit;
use genam_seams as genam_seams;
use table::{table_close, table_open};
use ::catalog_catalog::GetNewOidWithIndex;
use ::indexing::keystone::{CatalogTupleInsert, CatalogTupleUpdate};
use indexing_seams as indexing_seams;
use cache_syscache as syscache;

use tsearchcmds_seams::{
    ConfigMapEntry, NewTSParser, NewTSTemplate, TSConfigForm, TSDictForm, TSParserForm,
    TSTemplateForm,
};

use ::types_core::fmgr::{F_INT4EQ, F_OIDEQ};

/* ===========================================================================
 * Catalog OIDs + attribute numbers (catalog/pg_ts_*_d.h).
 * ========================================================================= */

const TSDictionaryRelationId: Oid = 3600;
const TSDictionaryOidIndexId: Oid = 3605;
const Natts_pg_ts_dict: usize = 6;
const Anum_pg_ts_dict_oid: AttrNumber = 1;
const Anum_pg_ts_dict_dictname: AttrNumber = 2;
const Anum_pg_ts_dict_dictnamespace: AttrNumber = 3;
const Anum_pg_ts_dict_dictowner: AttrNumber = 4;
const Anum_pg_ts_dict_dicttemplate: AttrNumber = 5;
const Anum_pg_ts_dict_dictinitoption: AttrNumber = 6;

const TSConfigRelationId: Oid = 3602;
const TSConfigOidIndexId: Oid = 3712;
const Natts_pg_ts_config: usize = 5;
const Anum_pg_ts_config_oid: AttrNumber = 1;
const Anum_pg_ts_config_cfgname: AttrNumber = 2;
const Anum_pg_ts_config_cfgnamespace: AttrNumber = 3;
const Anum_pg_ts_config_cfgowner: AttrNumber = 4;
const Anum_pg_ts_config_cfgparser: AttrNumber = 5;

const TSConfigMapRelationId: Oid = 3603;
const TSConfigMapIndexId: Oid = 3609;
const Natts_pg_ts_config_map: usize = 4;
const Anum_pg_ts_config_map_mapcfg: AttrNumber = 1;
const Anum_pg_ts_config_map_maptokentype: AttrNumber = 2;
const Anum_pg_ts_config_map_mapseqno: AttrNumber = 3;
const Anum_pg_ts_config_map_mapdict: AttrNumber = 4;

const Anum_pg_ts_template_tmplname: AttrNumber = 2;
const Anum_pg_ts_template_tmplinit: AttrNumber = 4;

const TSParserRelationId: Oid = 3601;
const TSParserOidIndexId: Oid = 3607;
const Natts_pg_ts_parser: usize = 8;
const Anum_pg_ts_parser_oid: AttrNumber = 1;
const Anum_pg_ts_parser_prsname: AttrNumber = 2;
const Anum_pg_ts_parser_prsnamespace: AttrNumber = 3;
const Anum_pg_ts_parser_prsstart: AttrNumber = 4;
const Anum_pg_ts_parser_prstoken: AttrNumber = 5;
const Anum_pg_ts_parser_prsend: AttrNumber = 6;
const Anum_pg_ts_parser_prsheadline: AttrNumber = 7;
const Anum_pg_ts_parser_prslextype: AttrNumber = 8;

const TSTemplateRelationId: Oid = 3764;
const TSTemplateOidIndexId: Oid = 3767;
const Natts_pg_ts_template: usize = 5;
const Anum_pg_ts_template_oid: AttrNumber = 1;
const Anum_pg_ts_template_tmplnamespace: AttrNumber = 3;
const Anum_pg_ts_template_tmpllexize: AttrNumber = 5;

const TSCONFIGOID: i32 = syscache::TSCONFIGOID;
const TSTEMPLATEOID: i32 = syscache::TSTEMPLATEOID;
const TSDICTOID: i32 = syscache::TSDICTOID;

/* ===========================================================================
 * Shared helpers.
 * ========================================================================= */

/// `namestrcpy(&name, src)` — a zero-filled 64-byte `NameData` image.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, src: &str) -> PgResult<Datum<'mcx>> {
    let mut name = [0u8; 64];
    for (i, &b) in src.as_bytes().iter().take(63).enumerate() {
        name[i] = b;
    }
    Ok(Datum::ByRef(::mcx::slice_in(mcx, &name[..])?))
}

/// `CStringGetTextDatum(src)` — pack a string into a `text` varlena image.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, src: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, src)
}

/// `heap_form_tuple(RelationGetDescr(rel), values, nulls)` +
/// `CatalogTupleInsert(rel, tup)`.
fn form_and_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
) -> PgResult<()> {
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, values, nulls)?;
    CatalogTupleInsert(mcx, rel, &mut tup)
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: AttrNumber, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
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
fn i32_key<'mcx>(attno: AttrNumber, value: i32) -> PgResult<ScanKeyData<'mcx>> {
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

/// One scanned `pg_ts_config_map` row: the heap TID (`tup->t_self`) plus the
/// `heap_deform_tuple` projection of the whole row.
struct SysScanRow<'a, 'mcx> {
    tid: ItemPointerData,
    values: &'a [Datum<'mcx>],
}

/// `systable_beginscan(rel, indexId, true, NULL, nkeys, key)` + the
/// `while ((tup = systable_getnext(scan)))` loop + `systable_endscan(scan)`.
/// `body` returning `Ok(true)` continues; an `Err` propagates after the scan is
/// ended.
fn systable_scan_foreach(
    rel: &RelationData<'_>,
    index_id: Oid,
    keys: &[ScanKeyData],
    mut body: impl FnMut(&SysScanRow<'_, '_>) -> PgResult<bool>,
) -> PgResult<()> {
    let mut scan = genam_seams::systable_beginscan::call(rel, index_id, true, None, keys)?;
    loop {
        let scratch = MemoryContext::new("ts map scan row");
        let smcx = scratch.mcx();
        let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
            break;
        };
        let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let mut values: PgVec<'_, Datum<'_>> = ::mcx::vec_with_capacity_in(smcx, cols.len())?;
        for (value, _null) in cols.iter() {
            values.push(value.clone());
        }
        let row = SysScanRow {
            tid: tup.tuple.t_self,
            values: &values,
        };
        let keep_going = body(&row)?;
        if !keep_going {
            break;
        }
    }
    scan.end()
}

/* ===========================================================================
 * `insert_ts_dict` — CREATE TEXT SEARCH DICTIONARY's pg_ts_dict insert.
 * ========================================================================= */

fn insert_ts_dict(
    name: &str,
    namespaceoid: Oid,
    owner: Oid,
    templ_id: Oid,
    dictoptions: Option<&str>,
) -> PgResult<(Oid, TSDictForm)> {
    let ctx = MemoryContext::new("insert_ts_dict");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TSDictionaryRelationId, RowExclusiveLock)?;

    /* memset(values, 0); memset(nulls, false). */
    let mut values: Vec<Datum> = vec![Datum::null(); Natts_pg_ts_dict];
    let mut nulls: Vec<bool> = vec![false; Natts_pg_ts_dict];

    let dict_oid = GetNewOidWithIndex(&rel, TSDictionaryOidIndexId, Anum_pg_ts_dict_oid)?;
    values[Anum_pg_ts_dict_oid as usize - 1] = Datum::from_oid(dict_oid);
    values[Anum_pg_ts_dict_dictname as usize - 1] = name_datum(mcx, name)?;
    values[Anum_pg_ts_dict_dictnamespace as usize - 1] = Datum::from_oid(namespaceoid);
    values[Anum_pg_ts_dict_dictowner as usize - 1] = Datum::from_oid(owner);
    values[Anum_pg_ts_dict_dicttemplate as usize - 1] = Datum::from_oid(templ_id);
    match dictoptions {
        Some(opt) => {
            values[Anum_pg_ts_dict_dictinitoption as usize - 1] = text_datum(mcx, opt)?;
        }
        None => {
            nulls[Anum_pg_ts_dict_dictinitoption as usize - 1] = true;
        }
    }

    form_and_insert(mcx, &rel, &values, &nulls)?;

    table_close(rel, RowExclusiveLock)?;

    let form = TSDictForm {
        oid: dict_oid,
        dictnamespace: namespaceoid,
        dictowner: owner,
        dicttemplate: templ_id,
    };
    Ok((dict_oid, form))
}

/* ===========================================================================
 * `insert_ts_config` — CREATE TEXT SEARCH CONFIGURATION's pg_ts_config insert.
 * ========================================================================= */

fn insert_ts_config(
    name: &str,
    namespaceoid: Oid,
    owner: Oid,
    prs_oid: Oid,
) -> PgResult<(Oid, TSConfigForm)> {
    let ctx = MemoryContext::new("insert_ts_config");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TSConfigRelationId, RowExclusiveLock)?;

    let mut values: Vec<Datum> = vec![Datum::null(); Natts_pg_ts_config];
    let nulls: Vec<bool> = vec![false; Natts_pg_ts_config];

    let cfg_oid = GetNewOidWithIndex(&rel, TSConfigOidIndexId, Anum_pg_ts_config_oid)?;
    values[Anum_pg_ts_config_oid as usize - 1] = Datum::from_oid(cfg_oid);
    values[Anum_pg_ts_config_cfgname as usize - 1] = name_datum(mcx, name)?;
    values[Anum_pg_ts_config_cfgnamespace as usize - 1] = Datum::from_oid(namespaceoid);
    values[Anum_pg_ts_config_cfgowner as usize - 1] = Datum::from_oid(owner);
    values[Anum_pg_ts_config_cfgparser as usize - 1] = Datum::from_oid(prs_oid);

    form_and_insert(mcx, &rel, &values, &nulls)?;

    table_close(rel, RowExclusiveLock)?;

    let form = TSConfigForm {
        oid: cfg_oid,
        cfgnamespace: namespaceoid,
        cfgowner: owner,
        cfgparser: prs_oid,
    };
    Ok((cfg_oid, form))
}

/* ===========================================================================
 * `insert_ts_parser` — CREATE TEXT SEARCH PARSER's pg_ts_parser insert.
 * ========================================================================= */

fn insert_ts_parser(row: &NewTSParser) -> PgResult<(Oid, TSParserForm)> {
    let ctx = MemoryContext::new("insert_ts_parser");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TSParserRelationId, RowExclusiveLock)?;

    /* memset(values, 0); memset(nulls, false). */
    let mut values: Vec<Datum> = vec![Datum::null(); Natts_pg_ts_parser];
    let nulls: Vec<bool> = vec![false; Natts_pg_ts_parser];

    let prs_oid = GetNewOidWithIndex(&rel, TSParserOidIndexId, Anum_pg_ts_parser_oid)?;
    values[Anum_pg_ts_parser_oid as usize - 1] = Datum::from_oid(prs_oid);
    values[Anum_pg_ts_parser_prsname as usize - 1] = name_datum(mcx, &row.prsname)?;
    values[Anum_pg_ts_parser_prsnamespace as usize - 1] = Datum::from_oid(row.prsnamespace);
    values[Anum_pg_ts_parser_prsstart as usize - 1] = Datum::from_oid(row.prsstart);
    values[Anum_pg_ts_parser_prstoken as usize - 1] = Datum::from_oid(row.prstoken);
    values[Anum_pg_ts_parser_prsend as usize - 1] = Datum::from_oid(row.prsend);
    values[Anum_pg_ts_parser_prsheadline as usize - 1] = Datum::from_oid(row.prsheadline);
    values[Anum_pg_ts_parser_prslextype as usize - 1] = Datum::from_oid(row.prslextype);

    form_and_insert(mcx, &rel, &values, &nulls)?;

    table_close(rel, RowExclusiveLock)?;

    let form = TSParserForm {
        oid: prs_oid,
        prsnamespace: row.prsnamespace,
        prsstart: row.prsstart,
        prstoken: row.prstoken,
        prsend: row.prsend,
        prsheadline: row.prsheadline,
        prslextype: row.prslextype,
    };
    Ok((prs_oid, form))
}

/* ===========================================================================
 * `insert_ts_template` — CREATE TEXT SEARCH TEMPLATE's pg_ts_template insert.
 * ========================================================================= */

fn insert_ts_template(row: &NewTSTemplate) -> PgResult<(Oid, TSTemplateForm)> {
    let ctx = MemoryContext::new("insert_ts_template");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TSTemplateRelationId, RowExclusiveLock)?;

    /* memset(values, 0); memset(nulls, false). */
    let mut values: Vec<Datum> = vec![Datum::null(); Natts_pg_ts_template];
    let nulls: Vec<bool> = vec![false; Natts_pg_ts_template];

    let tmpl_oid = GetNewOidWithIndex(&rel, TSTemplateOidIndexId, Anum_pg_ts_template_oid)?;
    values[Anum_pg_ts_template_oid as usize - 1] = Datum::from_oid(tmpl_oid);
    values[Anum_pg_ts_template_tmplname as usize - 1] = name_datum(mcx, &row.tmplname)?;
    values[Anum_pg_ts_template_tmplnamespace as usize - 1] = Datum::from_oid(row.tmplnamespace);
    values[Anum_pg_ts_template_tmplinit as usize - 1] = Datum::from_oid(row.tmplinit);
    values[Anum_pg_ts_template_tmpllexize as usize - 1] = Datum::from_oid(row.tmpllexize);

    form_and_insert(mcx, &rel, &values, &nulls)?;

    table_close(rel, RowExclusiveLock)?;

    let form = TSTemplateForm {
        oid: tmpl_oid,
        tmplnamespace: row.tmplnamespace,
        tmplinit: row.tmplinit,
        tmpllexize: row.tmpllexize,
    };
    Ok((tmpl_oid, form))
}

/* ===========================================================================
 * `config_form_by_oid` — SearchSysCache1(TSCONFIGOID, sourceOid) + GETSTRUCT.
 * ========================================================================= */

fn config_form_by_oid(source_oid: Oid) -> PgResult<TSConfigForm> {
    let ctx = MemoryContext::new("config_form_by_oid");
    let mcx = ctx.mcx();

    let tp = syscache::SearchSysCache1(
        mcx,
        TSCONFIGOID,
        SysCacheKey::Value(ScalarWord::from_oid(source_oid)),
    )?;
    let Some(tup) = tp else {
        return Err(PgError::error(alloc::format!(
            "cache lookup failed for text search configuration {source_oid}"
        )));
    };

    let cfgnamespace =
        syscache::SysCacheGetAttrNotNull(mcx, TSCONFIGOID, &tup, Anum_pg_ts_config_cfgnamespace as i32)?
            .as_oid();
    let cfgowner =
        syscache::SysCacheGetAttrNotNull(mcx, TSCONFIGOID, &tup, Anum_pg_ts_config_cfgowner as i32)?
            .as_oid();
    let cfgparser =
        syscache::SysCacheGetAttrNotNull(mcx, TSCONFIGOID, &tup, Anum_pg_ts_config_cfgparser as i32)?
            .as_oid();

    let form = TSConfigForm {
        oid: source_oid,
        cfgnamespace,
        cfgowner,
        cfgparser,
    };
    syscache::ReleaseSysCache(tup);
    Ok(form)
}

/* ===========================================================================
 * `ts_template_init_method` — verify_dictoptions's pg_ts_template read:
 * `SearchSysCache1(TSTEMPLATEOID, tmpl_id)` then `(tmplname, tmplinit)`.
 * `Ok(None)` on cache miss (the C "cache lookup failed" elog at the caller).
 * ========================================================================= */

fn ts_template_init_method(tmpl_id: Oid) -> PgResult<Option<(alloc::string::String, Oid)>> {
    let ctx = MemoryContext::new("ts_template_init_method");
    let mcx = ctx.mcx();

    let tp = syscache::SearchSysCache1(
        mcx,
        TSTEMPLATEOID,
        SysCacheKey::Value(ScalarWord::from_oid(tmpl_id)),
    )?;
    let Some(tup) = tp else {
        return Ok(None);
    };

    /* tmplname: NameData (by-ref, NUL-padded 64-byte buffer). */
    let name_datum = syscache::SysCacheGetAttrNotNull(
        mcx,
        TSTEMPLATEOID,
        &tup,
        Anum_pg_ts_template_tmplname as i32,
    )?;
    let raw = name_datum.as_ref_bytes();
    let nul = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    let tmplname = alloc::string::String::from_utf8_lossy(&raw[..nul]).into_owned();

    /* tmplinit: regproc (Oid; InvalidOid when '-'). */
    let tmplinit = syscache::SysCacheGetAttrNotNull(
        mcx,
        TSTEMPLATEOID,
        &tup,
        Anum_pg_ts_template_tmplinit as i32,
    )?
    .as_oid();

    syscache::ReleaseSysCache(tup);
    Ok(Some((tmplname, tmplinit)))
}

/* ===========================================================================
 * `config_map_entries` — pg_ts_config_map rows for cfg_id (COPY/dependencies).
 * ========================================================================= */

fn config_map_entries<'mcx>(
    mcx: Mcx<'mcx>,
    cfg_id: Oid,
) -> PgResult<PgVec<'mcx, ConfigMapEntry>> {
    let ctx = MemoryContext::new("config_map_entries");
    let scan_mcx = ctx.mcx();
    let rel = table_open(scan_mcx, TSConfigMapRelationId, RowExclusiveLock)?;

    let key = [oid_key(Anum_pg_ts_config_map_mapcfg, cfg_id)?];

    let mut out: PgVec<'mcx, ConfigMapEntry> = PgVec::new_in(mcx);
    systable_scan_foreach(&rel, TSConfigMapIndexId, &key, |row| {
        let entry = ConfigMapEntry {
            maptokentype: row.values[Anum_pg_ts_config_map_maptokentype as usize - 1].as_i32(),
            mapseqno: row.values[Anum_pg_ts_config_map_mapseqno as usize - 1].as_i32(),
            mapdict: row.values[Anum_pg_ts_config_map_mapdict as usize - 1].as_oid(),
        };
        out.push(entry);
        Ok(true)
    })?;

    table_close(rel, RowExclusiveLock)?;
    Ok(out)
}

/* ===========================================================================
 * `insert_config_map_entries` — insert new pg_ts_config_map rows for cfg_id.
 * ========================================================================= */

fn insert_config_map_entries(cfg_id: Oid, entries: &[ConfigMapEntry]) -> PgResult<()> {
    let ctx = MemoryContext::new("insert_config_map_entries");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TSConfigMapRelationId, RowExclusiveLock)?;

    for e in entries {
        let mut values: Vec<Datum> = vec![Datum::null(); Natts_pg_ts_config_map];
        let nulls: Vec<bool> = vec![false; Natts_pg_ts_config_map];

        values[Anum_pg_ts_config_map_mapcfg as usize - 1] = Datum::from_oid(cfg_id);
        values[Anum_pg_ts_config_map_maptokentype as usize - 1] = Datum::from_i32(e.maptokentype);
        values[Anum_pg_ts_config_map_mapseqno as usize - 1] = Datum::from_i32(e.mapseqno);
        values[Anum_pg_ts_config_map_mapdict as usize - 1] = Datum::from_oid(e.mapdict);

        form_and_insert(mcx, &rel, &values, &nulls)?;
    }

    table_close(rel, RowExclusiveLock)?;
    Ok(())
}

/* ===========================================================================
 * `delete_ts_config_row` — RemoveTSConfigurationById's pg_ts_config delete.
 * ========================================================================= */

fn delete_ts_config_row(cfg_id: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("delete_ts_config_row");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TSConfigRelationId, RowExclusiveLock)?;

    let tp = syscache::SearchSysCache1(
        mcx,
        TSCONFIGOID,
        SysCacheKey::Value(ScalarWord::from_oid(cfg_id)),
    )?;
    let Some(tup) = tp else {
        return Err(PgError::error(alloc::format!(
            "cache lookup failed for text search dictionary {cfg_id}"
        )));
    };

    indexing_seams::catalog_tuple_delete::call(&rel, tup.tuple.t_self)?;

    syscache::ReleaseSysCache(tup);
    table_close(rel, RowExclusiveLock)?;
    Ok(())
}

/* ===========================================================================
 * `delete_config_map_for_cfg` — RemoveTSConfigurationById's map clear.
 * ========================================================================= */

fn delete_config_map_for_cfg(cfg_id: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("delete_config_map_for_cfg");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TSConfigMapRelationId, RowExclusiveLock)?;

    let key = [oid_key(Anum_pg_ts_config_map_mapcfg, cfg_id)?];

    let mut tids: Vec<ItemPointerData> = Vec::new();
    systable_scan_foreach(&rel, TSConfigMapIndexId, &key, |row| {
        tids.push(row.tid);
        Ok(true)
    })?;
    for tid in tids {
        indexing_seams::catalog_tuple_delete::call(&rel, tid)?;
    }

    table_close(rel, RowExclusiveLock)?;
    Ok(())
}

/* ===========================================================================
 * `delete_config_map_for_token` — MakeConfigurationMapping / DropConfigurationMapping
 * per-token delete: CatalogTupleDelete every pg_ts_config_map row with
 * (mapcfg = cfg_id, maptokentype = token_num); returns the deleted count.
 * ========================================================================= */

fn delete_config_map_for_token(cfg_id: Oid, token_num: i32) -> PgResult<i64> {
    let ctx = MemoryContext::new("delete_config_map_for_token");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TSConfigMapRelationId, RowExclusiveLock)?;

    let key = [
        oid_key(Anum_pg_ts_config_map_mapcfg, cfg_id)?,
        i32_key(Anum_pg_ts_config_map_maptokentype, token_num)?,
    ];

    let mut tids: Vec<ItemPointerData> = Vec::new();
    systable_scan_foreach(&rel, TSConfigMapIndexId, &key, |row| {
        tids.push(row.tid);
        Ok(true)
    })?;
    let count = tids.len() as i64;
    for tid in tids {
        indexing_seams::catalog_tuple_delete::call(&rel, tid)?;
    }

    table_close(rel, RowExclusiveLock)?;
    Ok(count)
}

/* ===========================================================================
 * `replace_config_map_dict` — MakeConfigurationMapping REPLACE path: for
 * pg_ts_config_map rows of cfg_id whose maptokentype is in token_nums (or all
 * rows when token_nums is empty) and whose mapdict == dict_old, set mapdict to
 * dict_new (heap_modify_tuple-equivalent re-form + CatalogTupleUpdate).
 * ========================================================================= */

fn replace_config_map_dict(
    cfg_id: Oid,
    token_nums: &[i32],
    dict_old: Oid,
    dict_new: Oid,
) -> PgResult<()> {
    let ctx = MemoryContext::new("replace_config_map_dict");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TSConfigMapRelationId, RowExclusiveLock)?;

    let key = [oid_key(Anum_pg_ts_config_map_mapcfg, cfg_id)?];

    /* Collect the (tid, maptokentype, mapseqno) of every row to rewrite. The
     * C scans pg_ts_config_map for mapcfg = cfg_id, restricts to rows whose
     * maptokentype is in `tokens` (when nonempty), and replaces mapdict when
     * it equals dictOld. */
    struct Hit {
        tid: ItemPointerData,
        maptokentype: i32,
        mapseqno: i32,
    }
    let mut hits: Vec<Hit> = Vec::new();
    systable_scan_foreach(&rel, TSConfigMapIndexId, &key, |row| {
        let maptokentype = row.values[Anum_pg_ts_config_map_maptokentype as usize - 1].as_i32();
        let mapseqno = row.values[Anum_pg_ts_config_map_mapseqno as usize - 1].as_i32();
        let mapdict = row.values[Anum_pg_ts_config_map_mapdict as usize - 1].as_oid();

        /* if tokens is nonempty, only rows whose maptokentype matches one */
        let token_match = token_nums.is_empty() || token_nums.contains(&maptokentype);
        if token_match && mapdict == dict_old {
            hits.push(Hit {
                tid: row.tid,
                maptokentype,
                mapseqno,
            });
        }
        Ok(true)
    })?;

    for hit in hits {
        let mut values: Vec<Datum> = vec![Datum::null(); Natts_pg_ts_config_map];
        let nulls: Vec<bool> = vec![false; Natts_pg_ts_config_map];
        values[Anum_pg_ts_config_map_mapcfg as usize - 1] = Datum::from_oid(cfg_id);
        values[Anum_pg_ts_config_map_maptokentype as usize - 1] = Datum::from_i32(hit.maptokentype);
        values[Anum_pg_ts_config_map_mapseqno as usize - 1] = Datum::from_i32(hit.mapseqno);
        values[Anum_pg_ts_config_map_mapdict as usize - 1] = Datum::from_oid(dict_new);

        let tupdesc = rel.rd_att_clone_in(mcx)?;
        let mut newtup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
        CatalogTupleUpdate(mcx, &rel, hit.tid, &mut newtup)?;
    }

    table_close(rel, RowExclusiveLock)?;
    Ok(())
}

/* ===========================================================================
 * `dict_options_and_template` — AlterTSDictionary syscache read:
 * SearchSysCache1(TSDICTOID, dict_id) then (dicttemplate OID, raw dictinitoption
 * text). `None` for the option when the attribute is SQL NULL.
 * ========================================================================= */

fn dict_options_and_template(
    dict_id: Oid,
) -> PgResult<(Oid, Option<alloc::string::String>)> {
    let ctx = MemoryContext::new("dict_options_and_template");
    let mcx = ctx.mcx();

    let tp = syscache::SearchSysCache1(
        mcx,
        TSDICTOID,
        SysCacheKey::Value(ScalarWord::from_oid(dict_id)),
    )?;
    let Some(tup) = tp else {
        return Err(PgError::error(alloc::format!(
            "cache lookup failed for text search dictionary {dict_id}"
        )));
    };

    /* dicttemplate: regdictionary/oid (NOT NULL). */
    let dicttemplate = syscache::SysCacheGetAttrNotNull(
        mcx,
        TSDICTOID,
        &tup,
        Anum_pg_ts_dict_dicttemplate as i32,
    )?
    .as_oid();

    /* dictinitoption: text, nullable. */
    let (opt_datum, opt_isnull) = syscache::SysCacheGetAttr(
        mcx,
        TSDICTOID,
        &tup,
        Anum_pg_ts_dict_dictinitoption as i32,
    )?;
    let existing_opt = if opt_isnull {
        None
    } else {
        let s = varlena_seams::text_to_cstring_v::call(mcx, &opt_datum)?;
        Some(s.as_str().to_string())
    };

    syscache::ReleaseSysCache(tup);
    Ok((dicttemplate, existing_opt))
}

/* ===========================================================================
 * `update_dict_options` — AlterTSDictionary update: set the pg_ts_dict row's
 * dictinitoption to opttext (None => SQL NULL) + CatalogTupleUpdate.
 * ========================================================================= */

fn update_dict_options(dict_id: Oid, opttext: Option<&str>) -> PgResult<()> {
    let ctx = MemoryContext::new("update_dict_options");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TSDictionaryRelationId, RowExclusiveLock)?;

    let tp = syscache::SearchSysCache1(
        mcx,
        TSDICTOID,
        SysCacheKey::Value(ScalarWord::from_oid(dict_id)),
    )?;
    let Some(tup) = tp else {
        return Err(PgError::error(alloc::format!(
            "cache lookup failed for text search dictionary {dict_id}"
        )));
    };

    /* The C uses heap_modify_tuple over repl_repl[dictinitoption]; rebuild the
     * full row from the cached tuple's deformed columns with the one attribute
     * replaced. */
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let cols = ::heaptuple::heap_deform_tuple(
        mcx,
        &tup.tuple,
        &rel.rd_att,
        &tup.data,
    )?;
    let mut values: Vec<Datum> = Vec::with_capacity(cols.len());
    let mut nulls: Vec<bool> = Vec::with_capacity(cols.len());
    for (value, isnull) in cols.iter() {
        values.push(value.clone());
        nulls.push(*isnull);
    }

    match opttext {
        Some(opt) => {
            values[Anum_pg_ts_dict_dictinitoption as usize - 1] = text_datum(mcx, opt)?;
            nulls[Anum_pg_ts_dict_dictinitoption as usize - 1] = false;
        }
        None => {
            values[Anum_pg_ts_dict_dictinitoption as usize - 1] = Datum::null();
            nulls[Anum_pg_ts_dict_dictinitoption as usize - 1] = true;
        }
    }

    let otid = tup.tuple.t_self;
    let mut newtup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    CatalogTupleUpdate(mcx, &rel, otid, &mut newtup)?;

    syscache::ReleaseSysCache(tup);
    table_close(rel, RowExclusiveLock)?;
    Ok(())
}

/// Install the `pg_ts_dict` / `pg_ts_config` / `pg_ts_config_map` catalog
/// read+write seams `commands/tsearchcmds.c` calls.
pub fn init_seams() {
    use tsearchcmds_seams as s;
    s::insert_ts_parser::set(insert_ts_parser);
    s::insert_ts_template::set(insert_ts_template);
    s::insert_ts_dict::set(insert_ts_dict);
    s::insert_ts_config::set(insert_ts_config);
    s::config_form_by_oid::set(config_form_by_oid);
    s::ts_template_init_method::set(ts_template_init_method);
    s::config_map_entries::set(config_map_entries);
    s::insert_config_map_entries::set(insert_config_map_entries);
    s::delete_ts_config_row::set(delete_ts_config_row);
    s::delete_config_map_for_cfg::set(delete_config_map_for_cfg);
    s::delete_config_map_for_token::set(delete_config_map_for_token);
    s::replace_config_map_dict::set(replace_config_map_dict);
    s::dict_options_and_template::set(dict_options_and_template);
    s::update_dict_options::set(update_dict_options);
}
