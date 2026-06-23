//! The catalog SCAN + inplace-WRITE bodies that `vac_update_relstats` and
//! `vac_update_datfrozenxid` (and `get_all_vacuum_rels`) drive — vacuum.c's own
//! `table_open(...)` + `systable_beginscan`/`heap_getnext` seqscan loops and the
//! `systable_inplace_update_begin/finish` pg_class/pg_database writes.
//!
//! These are vacuum.c functions (the loops were seamed out of `lib.rs` and never
//! ported). They live here and call the already-installed substrate: `table_open`
//! (table.c), the genam `systable_*` scan + `systable_inplace_update` family
//! (genam.c), `heap_deform_tuple` (heaptuple.c), and the per-relation
//! xid-horizon reads (varsup.c / multixact.c) the future-value guard needs.
//!
//! Faithfulness note: vac_update_relstats/vac_update_datfrozenxid use
//! `systable_inplace_update_begin/finish` (a non-transactional overwrite), NOT a
//! transactional CatalogTupleUpdate — leaving no dead tuple behind. The genam
//! owner exposes that exact begin→mutate→finish flow as the
//! `systable_inplace_update` seam (the same one `index_update_stats` and
//! `dropdb`'s datconnlimit-invalidate use); the mutate callback edits the
//! fixed-size `Form_pg_class`/`Form_pg_database` user-data area in place at the
//! columns' descriptor offsets.

use alloc::string::String;
use alloc::vec::Vec;

use mcx::{Mcx, MemoryContext};

use types_core::primitive::{MultiXactId, Oid, TransactionId};
use types_error::{PgError, PgResult};
use types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock};

use types_tuple::heaptuple::Datum;

use heaptuple::heap_deform_tuple;
use genam_seams as genam;
use table::{table_close, table_open};

use vacuum_seams::{
    DatFrozenApplyResult, PgClassFrozenRow, PgClassScanRow, PgDatabaseFrozenRow,
    RelStatsApplyResult,
};
use types_vacuum::vacuumlazy::UpdateRelStatsArgs;

use crate::{multixact_seam, varsup_seam};

// ---------------------------------------------------------------------------
// Catalog constants (verified against PostgreSQL 18.3 headers).
// ---------------------------------------------------------------------------

use types_catalog::pg_class::{
    Anum_pg_class_oid, Anum_pg_class_relallfrozen, Anum_pg_class_relallvisible,
    Anum_pg_class_relfrozenxid, Anum_pg_class_relhasindex, Anum_pg_class_relisshared,
    Anum_pg_class_relkind, Anum_pg_class_relminmxid, Anum_pg_class_relpages,
    Anum_pg_class_relreplident, Anum_pg_class_reltuples, ClassOidIndexId, RelationRelationId,
};
use types_catalog::pg_class::{
    Anum_pg_class_relam, Anum_pg_class_relfilenode, Anum_pg_class_relhassubclass,
    Anum_pg_class_relispartition, Anum_pg_class_relispopulated, Anum_pg_class_relname,
    Anum_pg_class_relnamespace, Anum_pg_class_relowner, Anum_pg_class_relpersistence,
    Anum_pg_class_relrowsecurity, Anum_pg_class_reltablespace, Anum_pg_class_reltoastrelid,
    Anum_pg_class_reltype,
};
use types_catalog::pg_database::{
    Anum_pg_database_datconnlimit, Anum_pg_database_datfrozenxid, Anum_pg_database_datminmxid,
    Anum_pg_database_datname, Anum_pg_database_oid, DatabaseOidIndexId, DatabaseRelationId,
    DATCONNLIMIT_INVALID_DB,
};

// ---------------------------------------------------------------------------
// small deform helpers (mirror the GETSTRUCT field reads).
// ---------------------------------------------------------------------------

/// `NameStr` of a `NameData` (`name`) attribute: read up to the first NUL of the
/// fixed-width 64-byte field.
fn name_str(col: &(Datum<'_>, bool)) -> String {
    match &col.0 {
        Datum::ByRef(b) => {
            let len = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            String::from_utf8_lossy(&b[..len]).into_owned()
        }
        // A NameData column is always by-reference; anything else is a corrupt
        // catalog row, but fall back to the empty name rather than panicking on
        // the by-value word.
        _ => String::new(),
    }
}

// ===========================================================================
// scan_pg_class_frozenids — vac_update_datfrozenxid's pg_class seqscan.
// ===========================================================================

/// `vac_update_datfrozenxid`'s `table_open(RelationRelationId, AccessShareLock)`
/// + `systable_beginscan(InvalidOid, indexOK=false)` seqscan loop (vacuum.c):
/// read `relkind` / `relfrozenxid` / `relminmxid` off each `pg_class` row. The
/// caller does the horizon min/future-value accounting.
fn scan_pg_class_frozenids() -> PgResult<Vec<PgClassFrozenRow>> {
    let ctx = MemoryContext::new("scan_pg_class_frozenids");
    let mcx = ctx.mcx();

    // relation = table_open(RelationRelationId, AccessShareLock);
    let relation = table_open(mcx, RelationRelationId, AccessShareLock)?;

    // scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);
    let mut scan = genam::systable_beginscan::call(&relation, Oid::default(), false, None, &[])?;

    let mut rows: Vec<PgClassFrozenRow> = Vec::new();
    while let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? {
        // Form_pg_class classForm = (Form_pg_class) GETSTRUCT(classTup);
        let cols = heap_deform_tuple(mcx, &tup.tuple, &relation.rd_att, &tup.data)?;
        rows.push(PgClassFrozenRow {
            relkind: cols[(Anum_pg_class_relkind - 1) as usize].0.as_u8(),
            relfrozenxid: cols[(Anum_pg_class_relfrozenxid - 1) as usize]
                .0
                .as_transaction_id(),
            relminmxid: cols[(Anum_pg_class_relminmxid - 1) as usize].0.as_u32(),
        });
    }

    scan.end()?;
    table_close(relation, AccessShareLock)?;
    Ok(rows)
}

// ===========================================================================
// scan_pg_database_frozenids — vac_truncate_clog's pg_database seqscan.
// ===========================================================================

/// `vac_truncate_clog`'s `table_open(DatabaseRelationId, AccessShareLock)` +
/// catalog seqscan loop (vacuum.c): read `oid` / `datname` / `datfrozenxid` /
/// `datminmxid` and the `database_is_invalid_form` flag (`datconnlimit ==
/// DATCONNLIMIT_INVALID_DB`) off each `pg_database` row. The caller computes the
/// cluster-wide min datfrozenxid/datminmxid and the wrap/future guards.
fn scan_pg_database_frozenids() -> PgResult<Vec<PgDatabaseFrozenRow>> {
    let ctx = MemoryContext::new("scan_pg_database_frozenids");
    let mcx = ctx.mcx();

    // relation = table_open(DatabaseRelationId, AccessShareLock);
    let relation = table_open(mcx, DatabaseRelationId, AccessShareLock)?;

    // scan = table_beginscan_catalog(relation, 0, NULL); a full unkeyed heap
    // scan — systable_beginscan with indexOK=false is the same catalog seqscan.
    let mut scan = genam::systable_beginscan::call(&relation, Oid::default(), false, None, &[])?;

    let mut rows: Vec<PgDatabaseFrozenRow> = Vec::new();
    while let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? {
        // Form_pg_database dbform = (Form_pg_database) GETSTRUCT(tuple);
        let cols = heap_deform_tuple(mcx, &tup.tuple, &relation.rd_att, &tup.data)?;
        let datconnlimit = cols[(Anum_pg_database_datconnlimit - 1) as usize].0.as_i32();
        rows.push(PgDatabaseFrozenRow {
            oid: cols[(Anum_pg_database_oid - 1) as usize].0.as_oid(),
            datname: name_str(&cols[(Anum_pg_database_datname - 1) as usize]),
            datfrozenxid: cols[(Anum_pg_database_datfrozenxid - 1) as usize]
                .0
                .as_transaction_id(),
            datminmxid: cols[(Anum_pg_database_datminmxid - 1) as usize].0.as_u32(),
            // database_is_invalid_form(dbform): datconnlimit == DATCONNLIMIT_INVALID_DB.
            is_invalid: datconnlimit == DATCONNLIMIT_INVALID_DB,
        });
    }

    scan.end()?;
    table_close(relation, AccessShareLock)?;
    Ok(rows)
}

// ===========================================================================
// scan_all_pg_class — get_all_vacuum_rels's pg_class seqscan.
// ===========================================================================

/// `get_all_vacuum_rels`'s `table_open(RelationRelationId, AccessShareLock)` +
/// catalog seqscan loop (vacuum.c:1078): deform each `pg_class` row into the
/// trimmed `FormData_pg_class` projection (`oid`, `relkind`, and the
/// permission/identity fields `vacuum_is_permitted_for_relation` reads). The
/// caller filters by relkind and permission and builds the `VacuumRelation`s.
fn scan_all_pg_class<'caller>(caller_mcx: Mcx<'caller>) -> PgResult<Vec<PgClassScanRow<'caller>>> {
    let ctx = MemoryContext::new("scan_all_pg_class");
    let mcx = ctx.mcx();

    let relation = table_open(mcx, RelationRelationId, AccessShareLock)?;
    let mut scan = genam::systable_beginscan::call(&relation, Oid::default(), false, None, &[])?;

    let mut rows: Vec<PgClassScanRow<'caller>> = Vec::new();
    while let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? {
        let cols = heap_deform_tuple(mcx, &tup.tuple, &relation.rd_att, &tup.data)?;
        let oid = cols[(Anum_pg_class_oid - 1) as usize].0.as_oid();
        // Project into the cross-unit FormData_pg_class in the CALLER's arena (it
        // outlives this scratch scan context). Only the fields downstream
        // consumers read are filled from the tuple; the rest take faithful
        // deformed values too so the projection is a true GETSTRUCT image.
        let class_form = rel::FormData_pg_class {
            relname: mcx::PgString::from_str_in(
                &name_str(&cols[(Anum_pg_class_relname - 1) as usize]),
                caller_mcx,
            )?,
            relnamespace: cols[(Anum_pg_class_relnamespace - 1) as usize].0.as_oid(),
            relowner: cols[(Anum_pg_class_relowner - 1) as usize].0.as_oid(),
            relrowsecurity: cols[(Anum_pg_class_relrowsecurity - 1) as usize].0.as_bool(),
            relpages: cols[(Anum_pg_class_relpages - 1) as usize].0.as_i32(),
            reltuples: cols[(Anum_pg_class_reltuples - 1) as usize].0.as_f32(),
            relallvisible: cols[(Anum_pg_class_relallvisible - 1) as usize].0.as_i32(),
            reltoastrelid: cols[(Anum_pg_class_reltoastrelid - 1) as usize].0.as_oid(),
            reltablespace: cols[(Anum_pg_class_reltablespace - 1) as usize].0.as_oid(),
            relfilenode: cols[(Anum_pg_class_relfilenode - 1) as usize].0.as_oid(),
            relisshared: cols[(Anum_pg_class_relisshared - 1) as usize].0.as_bool(),
            relhasindex: cols[(Anum_pg_class_relhasindex - 1) as usize].0.as_bool(),
            relhassubclass: cols[(Anum_pg_class_relhassubclass - 1) as usize].0.as_bool(),
            relpersistence: cols[(Anum_pg_class_relpersistence - 1) as usize].0.as_u8(),
            relkind: cols[(Anum_pg_class_relkind - 1) as usize].0.as_u8(),
            reltype: cols[(Anum_pg_class_reltype - 1) as usize].0.as_oid(),
            relam: cols[(Anum_pg_class_relam - 1) as usize].0.as_oid(),
            relispopulated: cols[(Anum_pg_class_relispopulated - 1) as usize].0.as_bool(),
            relreplident: cols[(Anum_pg_class_relreplident - 1) as usize].0.as_u8(),
            relispartition: cols[(Anum_pg_class_relispartition - 1) as usize].0.as_bool(),
            relfrozenxid: cols[(Anum_pg_class_relfrozenxid - 1) as usize]
                .0
                .as_transaction_id(),
            relminmxid: cols[(Anum_pg_class_relminmxid - 1) as usize].0.as_u32(),
        };
        rows.push(PgClassScanRow {
            oid,
            relkind: class_form.relkind,
            class_form,
        });
    }

    scan.end()?;
    table_close(relation, AccessShareLock)?;
    Ok(rows)
}

// ===========================================================================
// fixed-width column byte offset (tupdesc walk; tupmacs.h att_align_nominal).
// ===========================================================================

/// The fixed byte offset, within a heap tuple's user-data area, of the 1-based
/// fixed-width column `anum`, assuming every preceding column is fixed-width and
/// non-null. pg_class's leading columns (through `relminmxid`@31, before the
/// variable-length `relacl`@32) and pg_database's leading columns (through
/// `datminmxid`@11, before the variable-length `datcollate`@13) are all
/// fixed-width non-null, so each sits at a constant data-area offset. Returns
/// `None` if a preceding column is variable-length.
fn fixed_attr_offset(tupdesc: &types_tuple::heaptuple::TupleDescData<'_>, anum: i16) -> Option<usize> {
    use arrayfuncs::foundation::att_align_nominal;
    let mut off: usize = 0;
    for i in 0..(anum as usize - 1) {
        let att = tupdesc.attr(i);
        if att.attlen < 0 {
            return None;
        }
        off = att_align_nominal(off, att.attalign as u8);
        off += att.attlen as usize;
    }
    let att = tupdesc.attr(anum as usize - 1);
    off = att_align_nominal(off, att.attalign as u8);
    Some(off)
}

/// `oid = OID` scan key on `attno` (`ScanKeyInit(BTEqualStrategyNumber,
/// F_OIDEQ)`).
fn oid_key<'mcx>(
    attno: types_core::AttrNumber,
    value: Oid,
) -> PgResult<types_scan::scankey::ScanKeyData<'mcx>> {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    scankey::ScanKeyInit(
        &mut key,
        attno,
        types_scan::scankey::BTEqualStrategyNumber,
        types_core::fmgr::F_OIDEQ,
        Datum::from_oid(value),
    )?;
    Ok(key)
}

// ===========================================================================
// vac_update_relstats_apply — pg_class inplace update (vacuum.c:1463-1588).
// ===========================================================================

/// The `table_open(RelationRelationId, RowExclusiveLock)` +
/// `systable_inplace_update_begin/finish/cancel` worker of `vac_update_relstats`
/// (vacuum.c:1442): poke `relpages` / `reltuples` / `relallvisible` /
/// `relallfrozen` and (when `!in_outer_xact`) `relhasindex` / `relhasrules` /
/// `relhastriggers`, then apply the `relfrozenxid` / `relminmxid` backward-guard
/// (don't go backward unless the stored value is "in the future" per
/// `ReadNextTransactionId()` / `ReadNextMultiXactId()`). Returns the
/// `*_updated` out-flags + the `futurexid`/`futuremxid` corruption flags and the
/// old values the caller uses to emit the data-corruption WARNINGs.
fn vac_update_relstats_apply(relation: Oid, args: UpdateRelStatsArgs) -> PgResult<RelStatsApplyResult> {
    let ctx = MemoryContext::new("vac_update_relstats_apply");
    let mcx = ctx.mcx();

    let relid = relation;

    // The "in the future" cutoffs read outside the buffer lock (C reads them
    // inside the dirty test; the values are stable for this transaction).
    let next_xid: TransactionId = varsup_seam::read_next_transaction_id::call();
    let next_multi: MultiXactId = multixact_seam::read_next_multixact_id::call()?;

    // The !in_outer_xact relhasrules/relhastriggers clearing reads whether the
    // relation currently has any rules (rd_rules == NULL) or triggers
    // (trigdesc == NULL). Both come off the open relcache entry: rd_rules is
    // exposed via the relation_rules projection (None == NULL), trigdesc via
    // the relation handle's rd_trigdesc (None == NULL).
    let has_rules = !args.in_outer_xact && {
        relcache_seams::relation_rules::call(mcx, relid)?.is_some()
    };
    let has_triggers = !args.in_outer_xact && {
        let r = common_relation_seams::relation_open::call(mcx, relid, NoLock)?;
        r.rd_trigdesc.is_some()
    };

    // rd = table_open(RelationRelationId, RowExclusiveLock);
    let rd = table_open(mcx, RelationRelationId, RowExclusiveLock)?;

    // Fixed data-area byte offsets of every column we touch.
    let tupdesc = rd.rd_att_clone_in(mcx)?;
    let off_relpages = fixed_attr_offset(&tupdesc, Anum_pg_class_relpages)
        .ok_or_else(|| PgError::error("pg_class relpages not at a fixed offset"))?;
    let off_reltuples = fixed_attr_offset(&tupdesc, Anum_pg_class_reltuples)
        .ok_or_else(|| PgError::error("pg_class reltuples not at a fixed offset"))?;
    let off_relallvisible = fixed_attr_offset(&tupdesc, Anum_pg_class_relallvisible)
        .ok_or_else(|| PgError::error("pg_class relallvisible not at a fixed offset"))?;
    let off_relallfrozen = fixed_attr_offset(&tupdesc, Anum_pg_class_relallfrozen)
        .ok_or_else(|| PgError::error("pg_class relallfrozen not at a fixed offset"))?;
    let off_relhasindex = fixed_attr_offset(&tupdesc, Anum_pg_class_relhasindex)
        .ok_or_else(|| PgError::error("pg_class relhasindex not at a fixed offset"))?;
    let off_relhasrules = fixed_attr_offset(&tupdesc, types_catalog::pg_class::Anum_pg_class_relhasrules)
        .ok_or_else(|| PgError::error("pg_class relhasrules not at a fixed offset"))?;
    let off_relhastriggers =
        fixed_attr_offset(&tupdesc, types_catalog::pg_class::Anum_pg_class_relhastriggers)
            .ok_or_else(|| PgError::error("pg_class relhastriggers not at a fixed offset"))?;
    let off_relfrozenxid = fixed_attr_offset(&tupdesc, Anum_pg_class_relfrozenxid)
        .ok_or_else(|| PgError::error("pg_class relfrozenxid not at a fixed offset"))?;
    let off_relminmxid = fixed_attr_offset(&tupdesc, Anum_pg_class_relminmxid)
        .ok_or_else(|| PgError::error("pg_class relminmxid not at a fixed offset"))?;

    let keys = [oid_key(Anum_pg_class_oid, relid)?];

    // Outputs computed inside the locked-buffer callback (mirror the C
    // `*frozenxid_updated`/`futurexid` writes).
    let mut result = RelStatsApplyResult::default();
    let result_ref = &mut result;

    let new_pages = args.num_pages as i32;
    let new_tuples = args.num_tuples as f32;
    let new_allvis = args.num_all_visible_pages as i32;
    let new_allfrozen = args.num_all_frozen_pages as i32;
    let hasindex = args.hasindex;
    let in_outer_xact = args.in_outer_xact;
    let frozenxid = args.frozenxid;
    let minmulti = args.minmulti;

    let mut mutate = |data: &mut [u8]| -> PgResult<bool> {
        if off_relminmxid + 4 > data.len() {
            return Err(PgError::error("pg_class column offset out of range"));
        }
        let mut dirty = false;

        // relpages / reltuples / relallvisible / relallfrozen.
        let cur = i32::from_ne_bytes(data[off_relpages..off_relpages + 4].try_into().unwrap());
        if cur != new_pages {
            data[off_relpages..off_relpages + 4].copy_from_slice(&new_pages.to_ne_bytes());
            dirty = true;
        }
        let cur = f32::from_ne_bytes(data[off_reltuples..off_reltuples + 4].try_into().unwrap());
        if cur != new_tuples {
            data[off_reltuples..off_reltuples + 4].copy_from_slice(&new_tuples.to_ne_bytes());
            dirty = true;
        }
        let cur =
            i32::from_ne_bytes(data[off_relallvisible..off_relallvisible + 4].try_into().unwrap());
        if cur != new_allvis {
            data[off_relallvisible..off_relallvisible + 4].copy_from_slice(&new_allvis.to_ne_bytes());
            dirty = true;
        }
        let cur =
            i32::from_ne_bytes(data[off_relallfrozen..off_relallfrozen + 4].try_into().unwrap());
        if cur != new_allfrozen {
            data[off_relallfrozen..off_relallfrozen + 4].copy_from_slice(&new_allfrozen.to_ne_bytes());
            dirty = true;
        }

        // Apply DDL updates, but not inside an outer transaction.
        if !in_outer_xact {
            // If we didn't find any indexes, reset relhasindex.
            if data[off_relhasindex] != 0 && !hasindex {
                data[off_relhasindex] = 0;
                dirty = true;
            }
            // We also clear relhasrules and relhastriggers if needed.
            if data[off_relhasrules] != 0 && !has_rules {
                data[off_relhasrules] = 0;
                dirty = true;
            }
            if data[off_relhastriggers] != 0 && !has_triggers {
                data[off_relhastriggers] = 0;
                dirty = true;
            }
        }

        // relfrozenxid backward-guard.
        let oldfrozenxid =
            u32::from_ne_bytes(data[off_relfrozenxid..off_relfrozenxid + 4].try_into().unwrap());
        result_ref.old_frozenxid = oldfrozenxid;
        if transaction_id_is_normal(frozenxid) && oldfrozenxid != frozenxid {
            let mut update = false;
            if transaction_id_precedes(oldfrozenxid, frozenxid) {
                update = true;
            } else if transaction_id_precedes(next_xid, oldfrozenxid) {
                result_ref.futurexid = true;
                update = true;
            }
            if update {
                data[off_relfrozenxid..off_relfrozenxid + 4].copy_from_slice(&frozenxid.to_ne_bytes());
                dirty = true;
                result_ref.frozenxid_updated = true;
            }
        }

        // relminmxid backward-guard.
        let oldminmulti =
            u32::from_ne_bytes(data[off_relminmxid..off_relminmxid + 4].try_into().unwrap());
        result_ref.old_minmulti = oldminmulti;
        if multixact_id_is_valid(minmulti) && oldminmulti != minmulti {
            let mut update = false;
            if multixact_id_precedes(oldminmulti, minmulti) {
                update = true;
            } else if multixact_id_precedes(next_multi, oldminmulti) {
                result_ref.futuremxid = true;
                update = true;
            }
            if update {
                data[off_relminmxid..off_relminmxid + 4].copy_from_slice(&minmulti.to_ne_bytes());
                dirty = true;
                result_ref.minmulti_updated = true;
            }
        }

        Ok(dirty)
    };

    let tid = genam::systable_inplace_update::call(
        mcx,
        &rd,
        ClassOidIndexId,
        true,
        &keys,
        &mut mutate,
    )?;

    // if (!HeapTupleIsValid(ctup)) elog(ERROR, "pg_class entry for relid %u
    // vanished during vacuuming", relid);
    if tid.is_none() {
        return Err(PgError::error(alloc::format!(
            "pg_class entry for relid {relid} vanished during vacuuming"
        )));
    }

    // table_close(rd, RowExclusiveLock);
    table_close(rd, RowExclusiveLock)?;
    Ok(result)
}

// ===========================================================================
// vac_update_datfrozenxid_apply — pg_database inplace update (vacuum.c:1759-1812).
// ===========================================================================

/// The `table_open(DatabaseRelationId, RowExclusiveLock)` +
/// `systable_inplace_update_begin/finish/cancel` worker of
/// `vac_update_datfrozenxid` (vacuum.c:1759): fetch our `pg_database` row
/// (`oid = MyDatabaseId`), apply the (possibly advanced) `datfrozenxid` /
/// `datminmxid` under the same don't-go-backward-unless-corrupt guard the caller
/// computed `last_sane_*` for, and report the effective values + whether
/// anything was dirtied.
fn vac_update_datfrozenxid_apply(
    new_frozen_xid: TransactionId,
    new_min_multi: MultiXactId,
    last_sane_frozen_xid: TransactionId,
    last_sane_min_multi: MultiXactId,
) -> PgResult<DatFrozenApplyResult> {
    let ctx = MemoryContext::new("vac_update_datfrozenxid_apply");
    let mcx = ctx.mcx();

    let my_database_id = crate::init_small_seam::my_database_id::call();

    // relation = table_open(DatabaseRelationId, RowExclusiveLock);
    let relation = table_open(mcx, DatabaseRelationId, RowExclusiveLock)?;

    let tupdesc = relation.rd_att_clone_in(mcx)?;
    let off_datfrozenxid = fixed_attr_offset(&tupdesc, Anum_pg_database_datfrozenxid as i16)
        .ok_or_else(|| PgError::error("pg_database datfrozenxid not at a fixed offset"))?;
    let off_datminmxid = fixed_attr_offset(&tupdesc, Anum_pg_database_datminmxid as i16)
        .ok_or_else(|| PgError::error("pg_database datminmxid not at a fixed offset"))?;

    let keys = [oid_key(Anum_pg_database_oid as i16, my_database_id)?];

    // The effective values default to the proposed new ones; the callback
    // lowers them back to the stored value when it declines to advance (the C
    // `newFrozenXid = dbform->datfrozenxid` else-branches).
    let mut result = DatFrozenApplyResult {
        eff_frozen_xid: new_frozen_xid,
        eff_min_multi: new_min_multi,
        dirty: false,
    };
    let result_ref = &mut result;

    let mut mutate = |data: &mut [u8]| -> PgResult<bool> {
        if off_datfrozenxid + 4 > data.len() || off_datminmxid + 4 > data.len() {
            return Err(PgError::error("pg_database column offset out of range"));
        }
        let mut dirty = false;

        let cur_frozen =
            u32::from_ne_bytes(data[off_datfrozenxid..off_datfrozenxid + 4].try_into().unwrap());
        // if (dbform->datfrozenxid != newFrozenXid &&
        //     (TransactionIdPrecedes(dbform->datfrozenxid, newFrozenXid) ||
        //      TransactionIdPrecedes(lastSaneFrozenXid, dbform->datfrozenxid)))
        if cur_frozen != new_frozen_xid
            && (transaction_id_precedes(cur_frozen, new_frozen_xid)
                || transaction_id_precedes(last_sane_frozen_xid, cur_frozen))
        {
            data[off_datfrozenxid..off_datfrozenxid + 4].copy_from_slice(&new_frozen_xid.to_ne_bytes());
            dirty = true;
        } else {
            result_ref.eff_frozen_xid = cur_frozen;
        }

        let cur_multi =
            u32::from_ne_bytes(data[off_datminmxid..off_datminmxid + 4].try_into().unwrap());
        if cur_multi != new_min_multi
            && (multixact_id_precedes(cur_multi, new_min_multi)
                || multixact_id_precedes(last_sane_min_multi, cur_multi))
        {
            data[off_datminmxid..off_datminmxid + 4].copy_from_slice(&new_min_multi.to_ne_bytes());
            dirty = true;
        } else {
            result_ref.eff_min_multi = cur_multi;
        }

        result_ref.dirty = dirty;
        Ok(dirty)
    };

    let tid = genam::systable_inplace_update::call(
        mcx,
        &relation,
        DatabaseOidIndexId,
        true,
        &keys,
        &mut mutate,
    )?;

    // if (!HeapTupleIsValid(tuple)) elog(ERROR, "could not find tuple for
    // database %u", MyDatabaseId);
    if tid.is_none() {
        return Err(PgError::error(alloc::format!(
            "could not find tuple for database {my_database_id}"
        )));
    }

    // table_close(relation, RowExclusiveLock);
    table_close(relation, RowExclusiveLock)?;
    Ok(result)
}

// ---------------------------------------------------------------------------
// xid / multixact comparison helpers (transam.h / multixact.h inlines).
// ---------------------------------------------------------------------------

/// `TransactionIdIsNormal(xid)` — xid >= FirstNormalTransactionId (3).
fn transaction_id_is_normal(xid: TransactionId) -> bool {
    xid >= 3
}

/// `TransactionIdPrecedes(id1, id2)` — modulo-2^32 "id1 < id2".
fn transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool {
    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

/// `MultiXactIdIsValid(multi)` — multi != InvalidMultiXactId (0).
fn multixact_id_is_valid(multi: MultiXactId) -> bool {
    multi != 0
}

/// `MultiXactIdPrecedes(multi1, multi2)` — modulo-2^32 "multi1 < multi2".
fn multixact_id_precedes(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    let diff = multi1.wrapping_sub(multi2) as i32;
    diff < 0
}

// ---------------------------------------------------------------------------
// install — wire the five seams from this unit's init_seams().
// ---------------------------------------------------------------------------

/// Install the catalog SCAN + inplace-WRITE seams this unit owns (the five
/// vacuum.c loops/writers that were seamed out of `lib.rs`).
pub(crate) fn install() {
    vacuum_seams::scan_pg_class_frozenids::set(scan_pg_class_frozenids);
    vacuum_seams::scan_pg_database_frozenids::set(scan_pg_database_frozenids);
    vacuum_seams::scan_all_pg_class::set(scan_all_pg_class);
    vacuum_seams::vac_update_relstats_apply::set(vac_update_relstats_apply);
    vacuum_seams::vac_update_datfrozenxid_apply::set(vac_update_datfrozenxid_apply);
}
