//! Port of `src/backend/access/nbtree/nbtpreprocesskeys.c` (PostgreSQL 18.3) —
//! preprocessing of the scan keys for a btree index scan.
//!
//! All twenty top-level functions of `nbtpreprocesskeys.c` are ported here
//! (C names preserved), operating on the repo's owned runtime structs:
//! [`BTScanOpaqueData`] / [`BTArrayKeyInfo`] / [`ScanKeyData`] over `'mcx`, and
//! a real [`Relation`].
//!
//! # Chosen signatures (no `IndexScanDesc` in this repo)
//!
//! The C entry points take `IndexScanDesc scan` and reach `scan->opaque`,
//! `scan->indexRelation`, `scan->keyData[]`, `scan->numberOfKeys`,
//! `scan->parallel_scan`.  Here those are passed as explicit arguments,
//! mirroring how the rest of the nbtree-core crate is shaped (real
//! `Relation<'mcx>` + `&mut BTScanOpaqueData<'mcx>`):
//!
//! - On entry, `so.keyData` holds the AM-caller's *input* scan keys (exactly as
//!   `scan->keyData[]`); on return it holds the *output* preprocessed keys
//!   (`so->keyData[]`). `_bt_preprocess_keys` clones the input before mutating,
//!   faithful to C never overwriting the source data.
//! - `mcx` is the scan memory context, used where C `palloc`s into
//!   `so->arrayContext` (the array workspace). OOM paths return `PgResult`.
//! - `parallel_scan` is the boolean `scan->parallel_scan != NULL`.
//!
//! Public entry points used by `search.rs` (`_bt_first`):
//!
//! ```ignore
//! pub fn _bt_preprocess_keys<'mcx>(
//!     mcx: Mcx<'mcx>, rel: &Relation<'mcx>, so: &mut BTScanOpaqueData<'mcx>,
//!     parallel_scan: bool,
//! ) -> PgResult<()>;
//!
//! pub fn _bt_preprocess_array_keys<'mcx>(
//!     mcx: Mcx<'mcx>, rel: &Relation<'mcx>, so: &mut BTScanOpaqueData<'mcx>,
//!     input_keys: &[ScanKeyData<'mcx>], new_numberOfKeys: &mut i32,
//! ) -> PgResult<Option<PgVec<'mcx, ScanKeyData<'mcx>>>>;
//! ```
//!
//! # In-crate vs. seam
//!
//! Faithfully ported in-crate: the entire control flow of every function, the
//! row-comparison member loops (the repo's `ScanKeyData::sk_subkeys` carries the
//! real subkey vector, so no seam is needed for them, unlike the src-idiomatic
//! port), array merge/sort/dedup, skip-array high/low compare bookkeeping.
//!
//! Wired to existing real seams:
//!   * `FunctionCall2Coll`/`OidFunctionCall2Coll` → `function_call2_coll`
//!     (`backend-utils-fmgr-fmgr-seams`, by-OID dispatch).
//!   * `get_opfamily_member`/`get_opcode`/`get_opfamily_proc` →
//!     `backend-utils-cache-lsyscache-seams`.
//!   * `deconstruct_array` + `get_typlenbyvalalign` →
//!     `backend-utils-adt-arrayfuncs-seams` / `backend-utils-cache-lsyscache-seams`.
//!
//! Genuinely-unported callees (no producer exists in this repo yet) are reached
//! through `seamish` honest panics (NOT `todo!`/`unimplemented!`):
//!   * per-column index metadata `rd_indoption[]` / `rd_opfamily[]` /
//!     `rd_indcollation[]` — the trimmed `RelationData` does not model these
//!     arrays (only `rd_opcintype[]` is carried), and no relcache producer
//!     populates them.
//!   * `index_getprocinfo` / `fmgr_info` materialisation into the `u64` ORDER
//!     proc handles `so->orderProcs[]` carries — no handle producer exists.
//!   * `_bt_binsrch_array_skey` (owned by nbtutils, not yet in this crate).
//!   * `PrepareSkipSupportFromOpclass` and the opclass skip-support
//!     increment/decrement (sortsupport owner, unported).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::primitive::{AttrNumber, Oid};
use types_error::{PgError, PgResult};
use types_rel::Relation;
use types_tuple::backend_access_common_heaptuple::Datum;

use types_nbtree::{BTArrayKeyInfo, BTScanOpaqueData, BTORDER_PROC, BTMaxStrategyNumber};
use types_scan::scankey::{
    ScanKeyData, StrategyNumber, BTEqualStrategyNumber, BTGreaterEqualStrategyNumber,
    BTGreaterStrategyNumber, BTLessEqualStrategyNumber, BTLessStrategyNumber, InvalidStrategy,
    SK_ISNULL, SK_ROW_HEADER, SK_ROW_MEMBER, SK_SEARCHARRAY, SK_SEARCHNOTNULL, SK_SEARCHNULL,
};

use backend_utils_fmgr_fmgr_seams::function_call2_coll;
// `deconstruct_array` lives in the arrayfuncs seams crate; `get_typlenbyvalalign`
// and the opfamily lookups live in the lsyscache seams crate.
use backend_utils_adt_arrayfuncs_seams::deconstruct_array;
use backend_utils_cache_lsyscache_seams::{
    get_opcode, get_opfamily_member, get_opfamily_proc, get_typlenbyvalalign,
};

extern crate alloc;

// ---------------------------------------------------------------------------
// Constants the C uses (access/skey.h, access/nbtree.h, pg_index.h) that are
// not yet centralised in the repo's type crates.
// ---------------------------------------------------------------------------

/// `INDEX_MAX_KEYS` (`pg_config_manual.h`).
const INDEX_MAX_KEYS: i32 = 32;
const INDEX_MAX_KEYS_USIZE: usize = INDEX_MAX_KEYS as usize;
/// `InvalidOid`.
const InvalidOid: Oid = 0;
/// `InvalidAttrNumber` (`access/attnum.h`).
const InvalidAttrNumber: AttrNumber = 0;

/// `SK_BT_INDOPTION_SHIFT` (`access/nbtree.h`) — leave room for system flag bits.
const SK_BT_INDOPTION_SHIFT: i32 = 24;
/// `SK_BT_DESC` — `indoption[]` DESC, shifted into `sk_flags`.
const SK_BT_DESC: i32 = 0x01 << SK_BT_INDOPTION_SHIFT;
/// `SK_BT_NULLS_FIRST` — `indoption[]` NULLS FIRST, shifted into `sk_flags`.
const SK_BT_NULLS_FIRST: i32 = 0x02 << SK_BT_INDOPTION_SHIFT;
/// `SK_BT_REQFWD` — required to continue a forward scan.
const SK_BT_REQFWD: i32 = 0x00010000;
/// `SK_BT_REQBKWD` — required to continue a backward scan.
const SK_BT_REQBKWD: i32 = 0x00020000;
/// `SK_BT_SKIP` — re-exported from types-scan (skip array marker).
const SK_BT_SKIP: i32 = types_scan::scankey::SK_BT_SKIP;

/// `INDOPTION_DESC` (`catalog/pg_index.h`): index column is in DESC order.
const INDOPTION_DESC: i16 = 0x0001;
/// `INDOPTION_NULLS_FIRST` (`catalog/pg_index.h`). The DESC bit is what
/// preprocessing reads from `indoption[]`; NULLS FIRST reaches the scankey via
/// the `SK_BT_NULLS_FIRST` flag bit instead, so the raw constant is referenced
/// only by the flag-shift invariant test.
#[allow(dead_code)]
const INDOPTION_NULLS_FIRST: i16 = 0x0002;

// ---------------------------------------------------------------------------
// Small inline helpers (c.h / common/int.h / access/nbtree.h macros).
// ---------------------------------------------------------------------------

/// `OidIsValid(oid)`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `RegProcedureIsValid(p)`.
#[inline]
fn reg_procedure_is_valid(p: Oid) -> bool {
    p != InvalidOid
}

/// `DatumGetBool(d)` — low bit of the `Datum` word.
#[inline]
fn datum_get_bool(d: types_datum::Datum) -> bool {
    (d.as_usize() & 1) != 0
}

/// `DatumGetInt32(d)` — a btree comparison support function returns an `int32`
/// in the low 32 bits of its `Datum` result.
#[inline]
fn datum_get_int32(d: types_datum::Datum) -> i32 {
    d.as_i32()
}

/// `INVERT_COMPARE_RESULT(var)` (c.h): `var = (var < 0) ? 1 : -(var)`.
#[inline]
fn invert_compare_result(var: i32) -> i32 {
    if var < 0 {
        1
    } else {
        -var
    }
}

/// `pg_cmp_s32(a, b)` (common/int.h).
#[inline]
fn pg_cmp_s32(a: i32, b: i32) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

/// `BTCommuteStrategyNumber(strat)` (`access/nbtree.h`):
/// `(BTMaxStrategyNumber + 1 - strat)`.
#[inline]
fn bt_commute_strategy_number(strat: StrategyNumber) -> StrategyNumber {
    BTMaxStrategyNumber + 1 - strat
}

/// `elog(ERROR, msg)` — raise a btree-internal error.
fn elog_error(msg: String) -> PgError {
    PgError::error(msg)
}

/// Convert a canonical `Datum<'mcx>` argument into the bare-word
/// `types_datum::Datum` the fmgr seam dispatches on (by-value path; by-ref args
/// carry their word, the same convention the repo uses everywhere fmgr dispatch
/// crosses a seam).
#[inline]
fn to_word(d: &Datum) -> types_datum::Datum {
    types_datum::Datum::from_usize(d.as_usize())
}

/// `FunctionCall2Coll(flinfo, collation, arg1, arg2)` by the cached proc's OID.
#[inline]
fn function_call2_coll_oid(
    proc_oid: Oid,
    collation: Oid,
    arg1: &Datum,
    arg2: &Datum,
) -> PgResult<types_datum::Datum> {
    function_call2_coll::call(proc_oid, collation, to_word(arg1), to_word(arg2))
}

// ---------------------------------------------------------------------------
// Relcache index-metadata reads that the trimmed `RelationData` does not model.
// Genuinely-unported: the per-column `rd_indoption[]` / `rd_opfamily[]` /
// `rd_indcollation[]` arrays are not carried by this repo's `RelationData`
// (only `rd_opcintype[]` is). No relcache producer fills them, so these are
// honest seam-and-panic boundaries (NOT stubs wired to nothing).
// ---------------------------------------------------------------------------

/// `rel->rd_indoption[attno - 1]` (`utils/rel.h`) — the per-column index option
/// flag word (DESC / NULLS FIRST bits).
fn rd_indoption(_rel: &Relation, _attno: AttrNumber) -> i16 {
    panic!("_bt_preprocess_keys: rel->rd_indoption[] (relcache index options) not yet ported")
}

/// `rel->rd_opfamily[attno - 1]` (`utils/rel.h`) — the per-column operator
/// family OID.
fn rd_opfamily(_rel: &Relation, _attno: AttrNumber) -> Oid {
    panic!("_bt_preprocess_keys: rel->rd_opfamily[] (relcache index opfamilies) not yet ported")
}

/// `rel->rd_opcintype[attno - 1]` (`utils/rel.h`) — the per-column opclass input
/// type OID. This one IS modelled by the trimmed `RelationData`.
fn rd_opcintype(rel: &Relation, attno: AttrNumber) -> Oid {
    let idx = (attno - 1) as usize;
    rel.rd_opcintype[idx]
}

/// `rel->rd_indcollation[attno - 1]` (`utils/rel.h`) — the per-column index
/// collation OID.
fn rd_indcollation(_rel: &Relation, _attno: AttrNumber) -> Oid {
    panic!(
        "_bt_preprocess_keys: rel->rd_indcollation[] (relcache index collations) not yet ported"
    )
}

/// `IndexRelationGetNumberOfKeyAttributes(rel)` (`utils/rel.h`).
fn index_nkeyatts(rel: &Relation) -> i32 {
    rel.indnkeyatts()
}

/// `RelationGetRelationName(rel)`.
fn relation_name<'a>(rel: &'a Relation<'a>) -> &'a str {
    rel.name()
}

/// `TupleDescCompactAttr(RelationGetDescr(rel), attno - 1)` projected to the
/// `(attlen, attbyval)` the skip-array bookkeeping reads.
fn compact_attr(rel: &Relation, attoff: i32) -> (i16, bool) {
    let att = rel.rd_att.attr(attoff as usize);
    (att.attlen, att.attbyval)
}

// ---------------------------------------------------------------------------
// Genuinely-unported support-proc / catalog callees (no producer in repo yet).
// ---------------------------------------------------------------------------

/// `index_getprocinfo(rel, attno, BTORDER_PROC)` (access/genam.c) — fetch the
/// cached same-type ORDER support proc as an fmgr handle. No `u64`-handle
/// producer exists for `so->orderProcs[]`.
fn index_getprocinfo(_rel: &Relation, _attno: AttrNumber, _procnum: i16) -> u64 {
    panic!("_bt_setup_array_cmp: index_getprocinfo (ORDER support proc handle) not yet ported")
}

/// `fmgr_info(proc, &flinfo)` / `fmgr_info_cxt(...)` (fmgr.c) — materialise an
/// fmgr handle for a freshly-looked-up cross-type proc. No `u64`-handle
/// producer exists.
fn fmgr_info(_cmp_proc: Oid) -> u64 {
    panic!("_bt_setup_array_cmp: fmgr_info (ORDER support proc handle) not yet ported")
}

/// `_bt_binsrch_array_skey(orderproc, false, NoMovementScanDirection, tupdatum,
/// false, array, arraysk, &cmpresult)` (nbtutils.c, owned by a sibling unit not
/// yet ported into this crate). Returns `(matchelem, cmpresult)`.
fn binsrch_array_skey(
    _cur_elem: i32,
    _tupdatum: &Datum,
    _elem_values: &[Datum],
    _orderproc_handle: u64,
    _collation: Oid,
) -> (i32, i32) {
    panic!("_bt_saoparray_shrink: _bt_binsrch_array_skey (nbtutils) not yet ported")
}

/// `array->sksup->decrement(rel, sk_argument, &underflow)` — opclass skip-support
/// decrement. The repo carries `sksup` as a `u64` handle with no producer.
fn skip_decrement(_rel: &Relation, _attno: AttrNumber, _arg: &Datum) -> (Datum<'static>, bool) {
    panic!("_bt_skiparray_strat_decrement: opclass skip-support decrement not yet ported")
}

/// `array->sksup->increment(rel, sk_argument, &overflow)` — opclass skip-support
/// increment.
fn skip_increment(_rel: &Relation, _attno: AttrNumber, _arg: &Datum) -> (Datum<'static>, bool) {
    panic!("_bt_skiparray_strat_increment: opclass skip-support increment not yet ported")
}

/// `PrepareSkipSupportFromOpclass(opfamily, opcintype, reverse)` (sortsupport,
/// unported). Returns the `(handle, BTSkipSupport)` pair, or `None`.
fn prepare_skip_support(
    _rel: &Relation,
    _attno: AttrNumber,
    _opfamily: Oid,
    _opcintype: Oid,
    _reverse: bool,
) -> Option<(u64, types_nbtree::BTSkipSupport<'static>)> {
    panic!("_bt_preprocess_array_keys: PrepareSkipSupportFromOpclass (sortsupport) not yet ported")
}

/// The `fn_oid` carried by an ORDER-proc fmgr handle (low 32 bits), used only
/// by `OidIsValid(orderproc->fn_oid)` debug asserts.
#[inline]
fn handle_fn_oid(handle: u64) -> Oid {
    handle as u32
}

// ---------------------------------------------------------------------------
// BTSortArrayContext / BTScanKeyPreproc analogues.
// ---------------------------------------------------------------------------

/// `BTSortArrayContext` — the qsort_arg comparison context used while sorting
/// and merging array elements. `sortproc` is the same-type ORDER proc handle.
struct BtSortArrayContext {
    sortproc: u64,
    collation: Oid,
    reverse: bool,
}

/// `BTScanKeyPreproc` — the per-strategy "best key so far" tracker used by
/// `_bt_preprocess_keys`. In C `inkey` is a `ScanKey` pointer into the input
/// keys array; here it is an index into the local `inkeys` slice (None == NULL).
#[derive(Clone, Copy, Default)]
struct BtScanKeyPreproc {
    inkey: Option<usize>,
    inkeyi: i32,
    arrayidx: i32,
}

// ---------------------------------------------------------------------------
// _bt_preprocess_keys
// ---------------------------------------------------------------------------

/// `_bt_preprocess_keys()` — Preprocess scan keys.
///
/// The given search-type keys (held in `so.keyData` on entry, mirroring
/// `scan->keyData[]`) are copied to `so.keyData` on exit (mirroring
/// `so->keyData[]`) with possible transformation. Calling here a second or
/// subsequent time during the same btrescan is a no-op.
pub fn _bt_preprocess_keys<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    parallel_scan: bool,
) -> PgResult<()> {
    let numberOfKeys_in = so.keyData.len() as i32;
    let mut numberOfKeys = numberOfKeys_in;
    let new_numberOfKeys: i32;
    let mut numberOfEqualCols: i32;
    let mut xform: [BtScanKeyPreproc; INDEX_MAX_KEYS_USIZE] =
        [BtScanKeyPreproc::default(); INDEX_MAX_KEYS_USIZE];
    let mut redundant_key_kept = false;
    let mut attno: AttrNumber;
    let mut keyDataMap: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let mut arrayidx: i32 = 0;

    if so.numberOfKeys > 0 {
        // Only need to do preprocessing once per btrescan, at most.
        return Ok(());
    }

    // initialize result variables
    so.qual_ok = true;
    so.numberOfKeys = 0;

    if numberOfKeys < 1 {
        return Ok(()); // done if qual-less scan
    }

    // Snapshot the AM-caller's input keys (so.keyData on entry).
    let input_keys: PgVec<'mcx, ScanKeyData<'mcx>> = clone_keys_in(mcx, &so.keyData)?;

    // If any keys are SK_SEARCHARRAY type, set up array-key info
    let arrayKeyData = _bt_preprocess_array_keys(mcx, rel, so, &input_keys, &mut numberOfKeys)?;
    if !so.qual_ok {
        // unmatchable array, so give up
        return Ok(());
    }

    // Treat arrayKeyData[] (a partially preprocessed copy of scan->keyData[]) as
    // our input if _bt_preprocess_array_keys just allocated it, else just use the
    // original input keys.
    let arrayKeyData_some = arrayKeyData.is_some();
    let mut inkeys: PgVec<'mcx, ScanKeyData<'mcx>> = if let Some(akd) = arrayKeyData {
        // Also maintain keyDataMap for remapping so->orderProcs[] later
        keyDataMap = vec_of_in(mcx, 0i32, numberOfKeys as usize)?;
        akd
    } else {
        clone_keys_in(mcx, &input_keys)?
    };

    // Output array (so.keyData). Start from a clean copy of the input keys and
    // grow as needed (C repallocs so->keyData; here we rebuild it).
    so.keyData = clone_keys_in(mcx, &inkeys)?;
    if numberOfKeys > numberOfKeys_in {
        while (so.keyData.len() as i32) < numberOfKeys {
            so.keyData.push(ScanKeyData::empty());
        }
    }

    // we check that input keys are correctly ordered
    if inkeys[0].sk_attno < 1 {
        return Err(elog_error(
            "btree index keys must be ordered by attribute".into(),
        ));
    }

    // We can short-circuit most of the work if there's just one key
    if numberOfKeys == 1 {
        // Apply indoption to scankey (might change sk_strategy!)
        if !_bt_fix_scankey_strategy(rel, &mut inkeys[0])? {
            so.qual_ok = false;
        }
        so.keyData[0] = inkeys[0].clone();
        so.numberOfKeys = 1;
        // We can mark the qual as required if it's for first index col
        if inkeys[0].sk_attno == 1 {
            _bt_mark_scankey_required(&mut so.keyData[0])?;
        }
        if arrayKeyData_some {
            // Don't call _bt_preprocess_array_keys_final in this fast path.
            debug_assert!((so.keyData[0].sk_flags & SK_SEARCHARRAY) != 0);
            debug_assert!(
                so.keyData[0].sk_strategy != BTEqualStrategyNumber
                    || (so.arrayKeys[0].scan_key == 0
                        && (so.keyData[0].sk_flags & SK_BT_SKIP) == 0
                        && oid_is_valid(handle_fn_oid(so.orderProcs[0])))
            );
        }
        // truncate output array to one key
        so.keyData.truncate(1);
        return Ok(());
    }

    // Otherwise, do the full set of pushups.
    let mut new_numberOfKeys_local = 0i32;
    numberOfEqualCols = 0;

    // Initialize for processing of keys for attr 1.
    attno = 1;

    let mut i: i32 = 0;
    loop {
        if i < numberOfKeys {
            // Apply indoption to scankey (might change sk_strategy!)
            if !_bt_fix_scankey_strategy(rel, &mut inkeys[i as usize])? {
                // NULL can't be matched, so give up
                so.qual_ok = false;
                return Ok(());
            }
        }

        let inkey_attno = if i < numberOfKeys {
            inkeys[i as usize].sk_attno
        } else {
            0
        };

        // If we are at the end of the keys for a particular attr, finish up
        // processing and emit the cleaned-up keys.
        if i == numberOfKeys || inkey_attno != attno {
            let priorNumberOfEqualCols = numberOfEqualCols;

            // check input keys are correctly ordered
            if i < numberOfKeys && inkey_attno < attno {
                return Err(elog_error(
                    "btree index keys must be ordered by attribute".into(),
                ));
            }

            // If = has been specified, all other keys can be eliminated as
            // redundant.
            if let Some(eq_idx) = xform[(BTEqualStrategyNumber - 1) as usize].inkey {
                let mut array: Option<usize> = None;
                let mut orderproc_idx: Option<usize> = None;

                if arrayKeyData_some && (inkeys[eq_idx].sk_flags & SK_SEARCHARRAY) != 0 {
                    let eq_in_ikey = xform[(BTEqualStrategyNumber - 1) as usize].inkeyi;
                    let eq_arrayidx = xform[(BTEqualStrategyNumber - 1) as usize].arrayidx;
                    array = Some((eq_arrayidx - 1) as usize);
                    orderproc_idx = Some(eq_in_ikey as usize);

                    debug_assert!(so.arrayKeys[(eq_arrayidx - 1) as usize].scan_key == eq_in_ikey);
                    debug_assert!(oid_is_valid(handle_fn_oid(so.orderProcs[eq_in_ikey as usize])));
                }

                let mut j = BTMaxStrategyNumber as i32;
                loop {
                    j -= 1;
                    if j < 0 {
                        break;
                    }

                    let chk_idx = match xform[j as usize].inkey {
                        Some(idx) => idx,
                        None => continue,
                    };
                    if j == (BTEqualStrategyNumber - 1) as i32 {
                        continue;
                    }

                    if (inkeys[eq_idx].sk_flags & SK_SEARCHNULL) != 0 {
                        // IS NULL is contradictory to anything else
                        so.qual_ok = false;
                        return Ok(());
                    }

                    // C: _bt_compare_scankey_args(scan, chk, eq, chk, array,
                    //    orderproc, &test_result) => op=chk, leftarg=eq, rightarg=chk.
                    let cmp = _bt_compare_scankey_args(
                        rel, so, &mut inkeys, chk_idx, eq_idx, chk_idx, array, orderproc_idx,
                    )?;
                    match cmp {
                        Some(res) => {
                            if !res {
                                // keys proven mutually contradictory
                                so.qual_ok = false;
                                return Ok(());
                            }
                            // else discard the redundant non-equality key
                            xform[j as usize].inkey = None;
                            xform[j as usize].inkeyi = -1;
                        }
                        None => {
                            redundant_key_kept = true;
                        }
                    }
                }
                // track number of attrs for which we have "=" keys
                numberOfEqualCols += 1;
            }

            // try to keep only one of <, <=
            if xform[(BTLessStrategyNumber - 1) as usize].inkey.is_some()
                && xform[(BTLessEqualStrategyNumber - 1) as usize].inkey.is_some()
            {
                let lt = xform[(BTLessStrategyNumber - 1) as usize].inkey.unwrap();
                let le = xform[(BTLessEqualStrategyNumber - 1) as usize].inkey.unwrap();
                match _bt_compare_scankey_args(rel, so, &mut inkeys, le, lt, le, None, None)? {
                    Some(res) => {
                        if res {
                            xform[(BTLessEqualStrategyNumber - 1) as usize].inkey = None;
                        } else {
                            xform[(BTLessStrategyNumber - 1) as usize].inkey = None;
                        }
                    }
                    None => redundant_key_kept = true,
                }
            }

            // try to keep only one of >, >=
            if xform[(BTGreaterStrategyNumber - 1) as usize].inkey.is_some()
                && xform[(BTGreaterEqualStrategyNumber - 1) as usize].inkey.is_some()
            {
                let gt = xform[(BTGreaterStrategyNumber - 1) as usize].inkey.unwrap();
                let ge = xform[(BTGreaterEqualStrategyNumber - 1) as usize].inkey.unwrap();
                match _bt_compare_scankey_args(rel, so, &mut inkeys, ge, gt, ge, None, None)? {
                    Some(res) => {
                        if res {
                            xform[(BTGreaterEqualStrategyNumber - 1) as usize].inkey = None;
                        } else {
                            xform[(BTGreaterStrategyNumber - 1) as usize].inkey = None;
                        }
                    }
                    None => redundant_key_kept = true,
                }
            }

            // Emit the cleaned-up keys into the so->keyData[] array, and mark
            // them required if applicable.
            let mut j = BTMaxStrategyNumber as i32;
            loop {
                j -= 1;
                if j < 0 {
                    break;
                }
                if let Some(src_idx) = xform[j as usize].inkey {
                    let outidx = new_numberOfKeys_local as usize;
                    new_numberOfKeys_local += 1;
                    ensure_keydata_len(so, new_numberOfKeys_local);
                    so.keyData[outidx] = inkeys[src_idx].clone();
                    if arrayKeyData_some {
                        keyDataMap[outidx] = xform[j as usize].inkeyi;
                    }
                    if priorNumberOfEqualCols == attno as i32 - 1 {
                        _bt_mark_scankey_required(&mut so.keyData[outidx])?;
                    }
                }
            }

            // Exit loop here if done.
            if i == numberOfKeys {
                break;
            }

            // Re-initialize for new attno
            attno = inkey_attno;
            xform = [BtScanKeyPreproc::default(); INDEX_MAX_KEYS_USIZE];
        }

        // check strategy this key's operator corresponds to
        let j = (inkeys[i as usize].sk_strategy - 1) as i32;

        if inkeys[i as usize].sk_strategy == BTEqualStrategyNumber
            && (inkeys[i as usize].sk_flags & SK_SEARCHARRAY) != 0
        {
            debug_assert!(arrayKeyData_some);
            arrayidx += 1;
        }

        // have we seen a scan key for this same attribute and strategy before?
        if xform[j as usize].inkey.is_none() {
            xform[j as usize].inkey = Some(i as usize);
            xform[j as usize].inkeyi = i;
            xform[j as usize].arrayidx = arrayidx;
        } else {
            let mut orderproc_idx: Option<usize> = None;
            let mut array: Option<usize> = None;

            // Seen one of these before, so keep only the more restrictive key.
            if j == (BTEqualStrategyNumber - 1) as i32 && arrayKeyData_some {
                if (inkeys[i as usize].sk_flags & SK_SEARCHARRAY) != 0 {
                    array = Some((arrayidx - 1) as usize);
                    orderproc_idx = Some(i as usize);

                    debug_assert!(so.arrayKeys[(arrayidx - 1) as usize].scan_key == i);
                    debug_assert!(oid_is_valid(handle_fn_oid(so.orderProcs[i as usize])));
                    debug_assert!((inkeys[i as usize].sk_flags & SK_BT_SKIP) == 0);
                } else {
                    let prior = xform[j as usize].inkey.unwrap();
                    if (inkeys[prior].sk_flags & SK_SEARCHARRAY) != 0 {
                        array = Some((xform[j as usize].arrayidx - 1) as usize);
                        orderproc_idx = Some(xform[j as usize].inkeyi as usize);

                        debug_assert!(
                            so.arrayKeys[(xform[j as usize].arrayidx - 1) as usize].scan_key
                                == xform[j as usize].inkeyi
                        );
                        debug_assert!(oid_is_valid(handle_fn_oid(
                            so.orderProcs[xform[j as usize].inkeyi as usize]
                        )));
                        debug_assert!((inkeys[prior].sk_flags & SK_BT_SKIP) == 0);
                    }
                }
                // Both scan keys might have arrays; we'll arbitrarily pass one.
            }

            let prior_idx = xform[j as usize].inkey.unwrap();
            match _bt_compare_scankey_args(
                rel,
                so,
                &mut inkeys,
                i as usize,
                i as usize,
                prior_idx,
                array,
                orderproc_idx,
            )? {
                Some(res) => {
                    if res {
                        // New key is more restrictive, and so replaces old key...
                        let prior = xform[j as usize].inkey.unwrap();
                        if j != (BTEqualStrategyNumber - 1) as i32
                            || (inkeys[prior].sk_flags & SK_SEARCHARRAY) == 0
                        {
                            xform[j as usize].inkey = Some(i as usize);
                            xform[j as usize].inkeyi = i;
                            xform[j as usize].arrayidx = arrayidx;
                        } else {
                            // ...unless we keep the old (array) key.
                            debug_assert!((inkeys[i as usize].sk_flags & SK_SEARCHARRAY) == 0);
                        }
                    } else if j == (BTEqualStrategyNumber - 1) as i32 {
                        // key == a && key == b, but a != b
                        so.qual_ok = false;
                        return Ok(());
                    }
                    // else old key is more restrictive, keep it
                }
                None => {
                    // Can't determine which key is more restrictive.  Push
                    // xform[j] directly to the output array, then set xform[j].
                    let outidx = new_numberOfKeys_local as usize;
                    new_numberOfKeys_local += 1;
                    ensure_keydata_len(so, new_numberOfKeys_local);
                    so.keyData[outidx] = inkeys[prior_idx].clone();
                    if arrayKeyData_some {
                        keyDataMap[outidx] = xform[j as usize].inkeyi;
                    }
                    if numberOfEqualCols == attno as i32 - 1 {
                        _bt_mark_scankey_required(&mut so.keyData[outidx])?;
                    }
                    xform[j as usize].inkey = Some(i as usize);
                    xform[j as usize].inkeyi = i;
                    xform[j as usize].arrayidx = arrayidx;
                    redundant_key_kept = true;
                }
            }
        }

        i += 1;
    }

    new_numberOfKeys = new_numberOfKeys_local;
    so.numberOfKeys = new_numberOfKeys;
    // shrink keyData[] to the number of output keys
    so.keyData.truncate(new_numberOfKeys as usize);

    // Fix array->scan_key references / consolidate so->orderProcs[].
    if arrayKeyData_some {
        _bt_preprocess_array_keys_final(rel, so, &keyDataMap, parallel_scan)?;
    }

    // If there are remaining redundant inequality keys, make sure each index
    // attribute has no more than one required >/>= key and one required </<= key.
    if redundant_key_kept && so.qual_ok {
        _bt_unmark_keys(mcx, so, &mut keyDataMap)?;
    }

    Ok(())
}

/// Clone a key slice into `mcx`.
fn clone_keys_in<'mcx>(
    mcx: Mcx<'mcx>,
    keys: &[ScanKeyData<'mcx>],
) -> PgResult<PgVec<'mcx, ScanKeyData<'mcx>>> {
    let mut v = vec_with_capacity_in(mcx, keys.len())?;
    for k in keys {
        v.push(k.clone());
    }
    Ok(v)
}

/// `vec![value; n]` over `mcx`.
fn vec_of_in<'mcx, T: Clone>(mcx: Mcx<'mcx>, value: T, n: usize) -> PgResult<PgVec<'mcx, T>> {
    let mut v = vec_with_capacity_in(mcx, n)?;
    for _ in 0..n {
        v.push(value.clone());
    }
    Ok(v)
}

/// Helper to grow `so->keyData[]` to at least `len` entries.
#[inline]
fn ensure_keydata_len(so: &mut BTScanOpaqueData, len: i32) {
    while (so.keyData.len() as i32) < len {
        so.keyData.push(ScanKeyData::empty());
    }
}

// ---------------------------------------------------------------------------
// _bt_fix_scankey_strategy
// ---------------------------------------------------------------------------

/// `_bt_fix_scankey_strategy()` — adjust a scankey's strategy and flags for
/// indoptions. Returns `Ok(true)` if the comparison value isn't NULL,
/// `Ok(false)` if the scan should be abandoned.
fn _bt_fix_scankey_strategy<'mcx>(
    rel: &Relation<'mcx>,
    skey: &mut ScanKeyData<'mcx>,
) -> PgResult<bool> {
    let mut addflags = (rd_indoption(rel, skey.sk_attno) as i32) << SK_BT_INDOPTION_SHIFT;

    // We treat all btree operators as strict.
    if (skey.sk_flags & SK_ISNULL) != 0 {
        debug_assert!((skey.sk_flags & SK_ROW_HEADER) == 0);

        skey.sk_flags |= addflags;

        if (skey.sk_flags & SK_SEARCHNULL) != 0 {
            skey.sk_strategy = BTEqualStrategyNumber;
            skey.sk_subtype = InvalidOid;
            skey.sk_collation = InvalidOid;
        } else if (skey.sk_flags & SK_SEARCHNOTNULL) != 0 {
            if (skey.sk_flags & SK_BT_NULLS_FIRST) != 0 {
                skey.sk_strategy = BTGreaterStrategyNumber;
            } else {
                skey.sk_strategy = BTLessStrategyNumber;
            }
            skey.sk_subtype = InvalidOid;
            skey.sk_collation = InvalidOid;
        } else {
            // regular qual, so it cannot be satisfied
            return Ok(false);
        }

        return Ok(true);
    }

    // Adjust strategy for DESC, if we didn't already
    if (addflags & SK_BT_DESC) != 0 && (skey.sk_flags & SK_BT_DESC) == 0 {
        skey.sk_strategy = bt_commute_strategy_number(skey.sk_strategy);
    }
    skey.sk_flags |= addflags;

    // If it's a row header, fix row member flags and strategies similarly.
    // The subsidiary keys live in `sk_subkeys` (the owned model of C's
    // `DatumGetPointer(sk_argument)` chain), so the loop is ported in-crate.
    if (skey.sk_flags & SK_ROW_HEADER) != 0 {
        let subkeys = skey
            .sk_subkeys
            .as_mut()
            .ok_or_else(|| elog_error("_bt_fix_scankey_strategy: row header lacks subkeys".into()))?;

        if (subkeys[0].sk_flags & SK_ISNULL) != 0 {
            // First row member is NULL, so RowCompare is unsatisfiable
            debug_assert!((subkeys[0].sk_flags & SK_ROW_MEMBER) != 0);
            return Ok(false);
        }

        for subkey in subkeys.iter_mut() {
            debug_assert!((subkey.sk_flags & SK_ROW_MEMBER) != 0);
            addflags = (rd_indoption(rel, subkey.sk_attno) as i32) << SK_BT_INDOPTION_SHIFT;
            if (addflags & SK_BT_DESC) != 0 && (subkey.sk_flags & SK_BT_DESC) == 0 {
                subkey.sk_strategy = bt_commute_strategy_number(subkey.sk_strategy);
            }
            subkey.sk_flags |= addflags;
            if (subkey.sk_flags & types_scan::scankey::SK_ROW_END) != 0 {
                break;
            }
        }
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// _bt_mark_scankey_required
// ---------------------------------------------------------------------------

/// `_bt_mark_scankey_required()` — mark a scankey as "required to continue the
/// scan", propagating into row-comparison subkeys as the C does.
fn _bt_mark_scankey_required(skey: &mut ScanKeyData) -> PgResult<()> {
    let addflags = match skey.sk_strategy {
        s if s == BTLessStrategyNumber || s == BTLessEqualStrategyNumber => SK_BT_REQFWD,
        s if s == BTEqualStrategyNumber => SK_BT_REQFWD | SK_BT_REQBKWD,
        s if s == BTGreaterEqualStrategyNumber || s == BTGreaterStrategyNumber => SK_BT_REQBKWD,
        other => {
            return Err(elog_error(format!(
                "unrecognized StrategyNumber: {}",
                other as i32
            )));
        }
    };

    skey.sk_flags |= addflags;

    if (skey.sk_flags & SK_ROW_HEADER) != 0 {
        let attno0 = skey.sk_attno;
        let strat0 = skey.sk_strategy;
        let subkeys = skey
            .sk_subkeys
            .as_mut()
            .ok_or_else(|| elog_error("_bt_mark_scankey_required: row header lacks subkeys".into()))?;

        // First subkey should be same column/operator as the header
        debug_assert!(subkeys[0].sk_attno == attno0);
        debug_assert!(subkeys[0].sk_strategy == strat0);

        let mut attno = attno0;
        for subkey in subkeys.iter_mut() {
            debug_assert!((subkey.sk_flags & SK_ROW_MEMBER) != 0);
            if subkey.sk_attno != attno {
                break; // non-adjacent key, so not required
            }
            if subkey.sk_strategy != strat0 {
                break; // wrong direction, so not required
            }
            subkey.sk_flags |= addflags;
            if (subkey.sk_flags & types_scan::scankey::SK_ROW_END) != 0 {
                break;
            }
            attno += 1;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// _bt_compare_scankey_args
// ---------------------------------------------------------------------------

/// `_bt_compare_scankey_args()` — compare two scankey values using a specified
/// operator. Indices index into `inkeys`. `array` indexes `so->arrayKeys[]`,
/// `orderproc_idx` indexes `so->orderProcs[]` (None == NULL).
///
/// Returns `Ok(Some(result))` if the comparison could be made, `Ok(None)` if
/// not.
fn _bt_compare_scankey_args<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    inkeys: &mut [ScanKeyData<'mcx>],
    op: usize,
    leftarg: usize,
    rightarg: usize,
    array: Option<usize>,
    orderproc_idx: Option<usize>,
) -> PgResult<Option<bool>> {
    let leftarg_flags = inkeys[leftarg].sk_flags;
    let rightarg_flags = inkeys[rightarg].sk_flags;

    debug_assert!(((leftarg_flags | rightarg_flags) & SK_ROW_MEMBER) == 0);

    // First, deal with cases where one or both args are NULL.
    if ((leftarg_flags | rightarg_flags) & SK_ISNULL) != 0 {
        let leftnull;
        let rightnull;

        // Handle skip array comparison with IS NOT NULL scan key
        if ((leftarg_flags | rightarg_flags) & SK_BT_SKIP) != 0 {
            debug_assert!(((leftarg_flags | rightarg_flags) & SK_SEARCHNULL) == 0);
            debug_assert!(((leftarg_flags | rightarg_flags) & SK_SEARCHNOTNULL) != 0);

            let arr = &mut so.arrayKeys[array.ok_or_else(|| {
                elog_error("_bt_compare_scankey_args: array index is NULL for SK_BT_SKIP key".into())
            })?];
            debug_assert!(arr.num_elems == -1);
            arr.null_elem = false;

            return Ok(Some(true));
        }

        if (leftarg_flags & SK_ISNULL) != 0 {
            debug_assert!((leftarg_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)) != 0);
            leftnull = true;
        } else {
            leftnull = false;
        }
        if (rightarg_flags & SK_ISNULL) != 0 {
            debug_assert!((rightarg_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)) != 0);
            rightnull = true;
        } else {
            rightnull = false;
        }

        let mut strat = inkeys[op].sk_strategy;
        if (inkeys[op].sk_flags & SK_BT_NULLS_FIRST) != 0 {
            strat = bt_commute_strategy_number(strat);
        }

        let result = match strat {
            s if s == BTLessStrategyNumber => (leftnull as i32) < (rightnull as i32),
            s if s == BTLessEqualStrategyNumber => (leftnull as i32) <= (rightnull as i32),
            s if s == BTEqualStrategyNumber => leftnull == rightnull,
            s if s == BTGreaterEqualStrategyNumber => (leftnull as i32) >= (rightnull as i32),
            s if s == BTGreaterStrategyNumber => (leftnull as i32) > (rightnull as i32),
            other => {
                return Err(elog_error(format!(
                    "unrecognized StrategyNumber: {}",
                    other as i32
                )));
            }
        };
        return Ok(Some(result));
    }

    // Row compare key: barring IS NULL/IS NOT NULL we can't determine redundancy.
    if ((leftarg_flags | rightarg_flags) & SK_ROW_HEADER) != 0 {
        debug_assert!(((leftarg_flags | rightarg_flags) & SK_BT_SKIP) == 0);
        return Ok(None);
    }

    // If either leftarg or rightarg are equality-type array scankeys, dispatch.
    if let Some(arr_idx) = array {
        let leftarray = (leftarg_flags & SK_SEARCHARRAY) != 0
            && inkeys[leftarg].sk_strategy == BTEqualStrategyNumber;
        let rightarray = (rightarg_flags & SK_SEARCHARRAY) != 0
            && inkeys[rightarg].sk_strategy == BTEqualStrategyNumber;

        if leftarray && rightarray {
            debug_assert!(((leftarg_flags | rightarg_flags) & SK_BT_SKIP) == 0);
            return Ok(None);
        }

        if leftarray {
            return _bt_compare_array_scankey_args(
                rel, so, inkeys, leftarg, rightarg, orderproc_idx, arr_idx,
            );
        } else if rightarray {
            return _bt_compare_array_scankey_args(
                rel, so, inkeys, rightarg, leftarg, orderproc_idx, arr_idx,
            );
        }
        // FALL THRU
    }

    // The opfamily we need is identified by the index column.
    debug_assert!(inkeys[leftarg].sk_attno == inkeys[rightarg].sk_attno);

    let opcintype = rd_opcintype(rel, inkeys[leftarg].sk_attno);

    let mut lefttype = inkeys[leftarg].sk_subtype;
    if lefttype == InvalidOid {
        lefttype = opcintype;
    }
    let mut righttype = inkeys[rightarg].sk_subtype;
    if righttype == InvalidOid {
        righttype = opcintype;
    }
    let mut optype = inkeys[op].sk_subtype;
    if optype == InvalidOid {
        optype = opcintype;
    }

    // If leftarg and rightarg match the types expected for the "op" scankey, we
    // can use its already-looked-up comparison function.
    if lefttype == opcintype && righttype == optype {
        let result = datum_get_bool(function_call2_coll_oid(
            inkeys[op].sk_func.fn_oid,
            inkeys[op].sk_collation,
            &inkeys[leftarg].sk_argument,
            &inkeys[rightarg].sk_argument,
        )?);
        return Ok(Some(result));
    }

    // Otherwise, go to the syscache for the appropriate operator. Un-flip the
    // strategy if _bt_fix_scankey_strategy commuted it.
    let mut strat = inkeys[op].sk_strategy;
    if (inkeys[op].sk_flags & SK_BT_DESC) != 0 {
        strat = bt_commute_strategy_number(strat);
    }

    let cmp_op = get_opfamily_member::call(
        rd_opfamily(rel, inkeys[leftarg].sk_attno),
        lefttype,
        righttype,
        strat as i16,
    )?;
    if oid_is_valid(cmp_op) {
        let cmp_proc = get_opcode::call(cmp_op)?;
        if reg_procedure_is_valid(cmp_proc) {
            let result = datum_get_bool(function_call2_coll_oid(
                cmp_proc,
                inkeys[op].sk_collation,
                &inkeys[leftarg].sk_argument,
                &inkeys[rightarg].sk_argument,
            )?);
            return Ok(Some(result));
        }
    }

    // Can't make the comparison
    Ok(None)
}

// ---------------------------------------------------------------------------
// _bt_compare_array_scankey_args
// ---------------------------------------------------------------------------

/// `_bt_compare_array_scankey_args()` — compare an array scan key to a scalar
/// scan key, eliminating contradictory array elements.
fn _bt_compare_array_scankey_args<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    inkeys: &mut [ScanKeyData<'mcx>],
    arraysk: usize,
    skey: usize,
    orderproc_idx: Option<usize>,
    array: usize,
) -> PgResult<Option<bool>> {
    debug_assert!(inkeys[arraysk].sk_attno == inkeys[skey].sk_attno);
    debug_assert!((inkeys[arraysk].sk_flags & (SK_ISNULL | SK_ROW_HEADER | SK_ROW_MEMBER)) == 0);
    debug_assert!(
        (inkeys[arraysk].sk_flags & SK_SEARCHARRAY) != 0
            && inkeys[arraysk].sk_strategy == BTEqualStrategyNumber
    );
    debug_assert!((inkeys[skey].sk_flags & (SK_ISNULL | SK_ROW_HEADER | SK_ROW_MEMBER)) == 0);
    debug_assert!(
        (inkeys[skey].sk_flags & SK_SEARCHARRAY) == 0
            || inkeys[skey].sk_strategy != BTEqualStrategyNumber
    );

    if so.arrayKeys[array].num_elems != -1 {
        _bt_saoparray_shrink(rel, so, inkeys, arraysk, skey, orderproc_idx, array)
    } else {
        _bt_skiparray_shrink(rel, so, inkeys, skey, array)
    }
}

// ---------------------------------------------------------------------------
// _bt_saoparray_shrink
// ---------------------------------------------------------------------------

/// `_bt_saoparray_shrink()` — preprocessing of a SAOP array scan key.
fn _bt_saoparray_shrink<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    inkeys: &[ScanKeyData<'mcx>],
    arraysk: usize,
    skey: usize,
    orderproc_idx: Option<usize>,
    array: usize,
) -> PgResult<Option<bool>> {
    let opcintype = rd_opcintype(rel, inkeys[arraysk].sk_attno);
    let cmpresult: i32;
    let mut cmpexact: i32 = 0;
    let mut matchelem: i32;
    let new_nelems: i32;

    debug_assert!(so.arrayKeys[array].num_elems > 0);
    debug_assert!((inkeys[arraysk].sk_flags & SK_BT_SKIP) == 0);

    // Reuse the array's ORDER proc when the scalar key's type matches the
    // attribute's opclass input type; otherwise do another ORDER proc lookup.
    let orderproc_handle: u64;
    if inkeys[skey].sk_subtype != opcintype && inkeys[skey].sk_subtype != InvalidOid {
        let mut arraysk_elemtype = inkeys[arraysk].sk_subtype;
        if arraysk_elemtype == InvalidOid {
            arraysk_elemtype = rd_opcintype(rel, inkeys[arraysk].sk_attno);
        }
        let cmp_proc = get_opfamily_proc::call(
            rd_opfamily(rel, inkeys[arraysk].sk_attno),
            inkeys[skey].sk_subtype,
            arraysk_elemtype,
            BTORDER_PROC,
        )?;
        if !reg_procedure_is_valid(cmp_proc) {
            return Ok(None); // Can't make the comparison
        }
        orderproc_handle = fmgr_info(cmp_proc);
    } else {
        orderproc_handle = so.orderProcs[orderproc_idx.ok_or_else(|| {
            elog_error("_bt_saoparray_shrink: orderproc must be set for same-type".into())
        })?];
    }

    let skey_argument = inkeys[skey].sk_argument.clone();
    let skey_strategy = inkeys[skey].sk_strategy;
    let arraysk_collation = inkeys[arraysk].sk_collation;

    let cur_elem = so.arrayKeys[array].cur_elem;
    let (m, c) = {
        let arr = &so.arrayKeys[array];
        binsrch_array_skey(
            cur_elem,
            &skey_argument,
            arr.elem_values.as_slice(),
            orderproc_handle,
            arraysk_collation,
        )
    };
    matchelem = m;
    cmpresult = c;

    let arr = &mut so.arrayKeys[array];
    match skey_strategy {
        s if s == BTLessStrategyNumber => {
            cmpexact = 1; // exclude exact match, if any -- FALL THRU
            if cmpresult >= cmpexact {
                matchelem += 1;
            }
            new_nelems = matchelem;
        }
        s if s == BTLessEqualStrategyNumber => {
            if cmpresult >= cmpexact {
                matchelem += 1;
            }
            new_nelems = matchelem;
        }
        s if s == BTEqualStrategyNumber => {
            if cmpresult != 0 {
                new_nelems = 0; // qual is unsatisfiable
            } else {
                arr.elem_values[0] = arr.elem_values[matchelem as usize].clone();
                new_nelems = 1;
            }
        }
        s if s == BTGreaterEqualStrategyNumber => {
            cmpexact = 1; // include exact match, if any -- FALL THRU
            if cmpresult >= cmpexact {
                matchelem += 1;
            }
            new_nelems = arr.num_elems - matchelem;
            // memmove(elem_values, elem_values + matchelem, sizeof(Datum)*new_nelems)
            datum_memmove_to_start(&mut arr.elem_values, matchelem as usize, new_nelems as usize);
        }
        s if s == BTGreaterStrategyNumber => {
            if cmpresult >= cmpexact {
                matchelem += 1;
            }
            new_nelems = arr.num_elems - matchelem;
            datum_memmove_to_start(&mut arr.elem_values, matchelem as usize, new_nelems as usize);
        }
        other => {
            return Err(elog_error(format!(
                "unrecognized StrategyNumber: {}",
                other as i32
            )));
        }
    }

    debug_assert!(new_nelems >= 0);
    debug_assert!(new_nelems <= arr.num_elems);

    arr.num_elems = new_nelems;
    Ok(Some(new_nelems > 0))
}

// ---------------------------------------------------------------------------
// _bt_skiparray_shrink
// ---------------------------------------------------------------------------

/// `_bt_skiparray_shrink()` — preprocessing of a skip array scan key against a
/// non-array scalar inequality.
fn _bt_skiparray_shrink<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    inkeys: &[ScanKeyData<'mcx>],
    skey: usize,
    array: usize,
) -> PgResult<Option<bool>> {
    debug_assert!(so.arrayKeys[array].num_elems == -1);

    so.arrayKeys[array].null_elem = false;

    let skey_strategy = inkeys[skey].sk_strategy;
    let skey_copy = inkeys[skey].clone();

    match skey_strategy {
        s if s == BTLessStrategyNumber || s == BTLessEqualStrategyNumber => {
            if so.arrayKeys[array].high_compare.is_some() {
                let high = (**so.arrayKeys[array].high_compare.as_ref().unwrap()).clone();
                // op=high_compare, leftarg=skey, rightarg=high_compare
                let mut pair = [high, skey_copy.clone()];
                match _bt_compare_scankey_args(rel, so, &mut pair, 0, 1, 0, None, None)? {
                    None => return Ok(None),
                    Some(false) => return Ok(Some(true)),
                    Some(true) => {}
                }
            }
            so.arrayKeys[array].high_compare = Some(Box::new(skey_copy));
        }
        s if s == BTGreaterEqualStrategyNumber || s == BTGreaterStrategyNumber => {
            if so.arrayKeys[array].low_compare.is_some() {
                let low = (**so.arrayKeys[array].low_compare.as_ref().unwrap()).clone();
                let mut pair = [low, skey_copy.clone()];
                match _bt_compare_scankey_args(rel, so, &mut pair, 0, 1, 0, None, None)? {
                    None => return Ok(None),
                    Some(false) => return Ok(Some(true)),
                    Some(true) => {}
                }
            }
            so.arrayKeys[array].low_compare = Some(Box::new(skey_copy));
        }
        _ => {
            return Err(elog_error(format!(
                "unrecognized StrategyNumber: {}",
                skey_strategy as i32
            )));
        }
    }

    Ok(Some(true))
}

// ---------------------------------------------------------------------------
// _bt_skiparray_strat_adjust / decrement / increment
// ---------------------------------------------------------------------------

/// `_bt_skiparray_strat_adjust()` — convert a `>` low_compare into `>=`, and a
/// `<` high_compare into `<=`, via the opclass skip support routine.
fn _bt_skiparray_strat_adjust<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    arraysk: &ScanKeyData<'mcx>,
    array: usize,
) -> PgResult<()> {
    debug_assert!((arraysk.sk_flags & SK_BT_SKIP) != 0);
    debug_assert!(
        so.arrayKeys[array].num_elems == -1
            && !so.arrayKeys[array].null_elem
            && so.arrayKeys[array].sksup.is_some()
    );

    let high_is_lt = so.arrayKeys[array]
        .high_compare
        .as_deref()
        .map(|k| k.sk_strategy == BTLessStrategyNumber)
        .unwrap_or(false);
    if high_is_lt {
        _bt_skiparray_strat_decrement(rel, so, arraysk, array)?;
    }

    let low_is_gt = so.arrayKeys[array]
        .low_compare
        .as_deref()
        .map(|k| k.sk_strategy == BTGreaterStrategyNumber)
        .unwrap_or(false);
    if low_is_gt {
        _bt_skiparray_strat_increment(rel, so, arraysk, array)?;
    }

    Ok(())
}

/// `_bt_skiparray_strat_decrement()` — convert skip array's `<` high_compare key
/// into a `<=` key.
fn _bt_skiparray_strat_decrement<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    arraysk: &ScanKeyData<'mcx>,
    array: usize,
) -> PgResult<()> {
    let opfamily = rd_opfamily(rel, arraysk.sk_attno);
    let opcintype = rd_opcintype(rel, arraysk.sk_attno);

    let high_compare = (**so.arrayKeys[array].high_compare.as_ref().unwrap()).clone();
    let orig_sk_argument = high_compare.sk_argument.clone();

    debug_assert!(high_compare.sk_strategy == BTLessStrategyNumber);

    if high_compare.sk_subtype != opcintype && high_compare.sk_subtype != InvalidOid {
        return Ok(());
    }

    // Decrement, handling underflow by marking the qual unsatisfiable
    let (new_sk_argument, uflow) = skip_decrement(rel, arraysk.sk_attno, &orig_sk_argument);
    if uflow {
        so.qual_ok = false;
        return Ok(());
    }

    let mut lookupstrat = BTLessEqualStrategyNumber;
    if (high_compare.sk_flags & SK_BT_DESC) != 0 {
        lookupstrat = BTGreaterEqualStrategyNumber; // commute this too
    }
    let leop = get_opfamily_member::call(opfamily, opcintype, opcintype, lookupstrat as i16)?;
    if !oid_is_valid(leop) {
        return Ok(());
    }
    let cmp_proc = get_opcode::call(leop)?;
    if reg_procedure_is_valid(cmp_proc) {
        let hc = so.arrayKeys[array].high_compare.as_deref_mut().unwrap();
        hc.sk_func.fn_oid = cmp_proc;
        hc.sk_argument = relocate_datum(new_sk_argument);
        hc.sk_strategy = BTLessEqualStrategyNumber;
    }

    Ok(())
}

/// `_bt_skiparray_strat_increment()` — convert skip array's `>` low_compare key
/// into a `>=` key.
fn _bt_skiparray_strat_increment<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    arraysk: &ScanKeyData<'mcx>,
    array: usize,
) -> PgResult<()> {
    let opfamily = rd_opfamily(rel, arraysk.sk_attno);
    let opcintype = rd_opcintype(rel, arraysk.sk_attno);

    let low_compare = (**so.arrayKeys[array].low_compare.as_ref().unwrap()).clone();
    let orig_sk_argument = low_compare.sk_argument.clone();

    debug_assert!(low_compare.sk_strategy == BTGreaterStrategyNumber);

    if low_compare.sk_subtype != opcintype && low_compare.sk_subtype != InvalidOid {
        return Ok(());
    }

    let (new_sk_argument, oflow) = skip_increment(rel, arraysk.sk_attno, &orig_sk_argument);
    if oflow {
        so.qual_ok = false;
        return Ok(());
    }

    let mut lookupstrat = BTGreaterEqualStrategyNumber;
    if (low_compare.sk_flags & SK_BT_DESC) != 0 {
        lookupstrat = BTLessEqualStrategyNumber;
    }
    let geop = get_opfamily_member::call(opfamily, opcintype, opcintype, lookupstrat as i16)?;
    if !oid_is_valid(geop) {
        return Ok(());
    }
    let cmp_proc = get_opcode::call(geop)?;
    if reg_procedure_is_valid(cmp_proc) {
        let lc = so.arrayKeys[array].low_compare.as_deref_mut().unwrap();
        lc.sk_func.fn_oid = cmp_proc;
        lc.sk_argument = relocate_datum(new_sk_argument);
        lc.sk_strategy = BTGreaterEqualStrategyNumber;
    }

    Ok(())
}

/// Re-bind a `'static` Datum produced by the (unported) skip-support routine to
/// the scan's `'mcx` lifetime. Only the by-value arm is ever produced by the
/// skip-support increment/decrement on a by-value attribute (its only call
/// site); a by-ref result would carry owned bytes into `'mcx`.
fn relocate_datum<'mcx>(d: Datum<'static>) -> Datum<'mcx> {
    match d {
        Datum::ByVal(w) => Datum::ByVal(w),
        Datum::ByRef(_) => {
            panic!("_bt_skiparray_strat_adjust: by-ref skip-support result relocation not yet ported")
        }
        Datum::Cstring(_) | Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => {
            panic!("_bt_skiparray_strat_adjust: non-ByVal/ByRef skip-support result not yet produced — wave 2")
        }
    }
}

// ---------------------------------------------------------------------------
// _bt_unmark_keys
// ---------------------------------------------------------------------------

/// `_bt_unmark_keys()` — make superfluous required keys nonrequired after all.
fn _bt_unmark_keys<'mcx>(
    mcx: Mcx<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    keyDataMap: &mut [i32],
) -> PgResult<()> {
    let numberOfKeys = so.numberOfKeys;
    let mut attno: AttrNumber;
    let mut nunmark: i32;
    let mut nunmarked: i32;
    let mut nkept: i32;
    let mut firsti: i32;
    let mut haveReqEquals: bool;
    let mut haveReqForward: bool;
    let mut haveReqBackward: bool;

    let mut unmarkikey = vec_of_in(mcx, false, numberOfKeys as usize)?;
    nunmark = 0;

    attno = so.keyData[0].sk_attno;
    firsti = 0;
    haveReqEquals = false;
    haveReqForward = false;
    haveReqBackward = false;
    for i in 0..numberOfKeys {
        let origkey_flags = so.keyData[i as usize].sk_flags;
        let origkey_attno = so.keyData[i as usize].sk_attno;
        let origkey_strategy = so.keyData[i as usize].sk_strategy;

        if origkey_attno != attno {
            attno = origkey_attno;
            firsti = i;
            haveReqEquals = false;
            haveReqForward = false;
            haveReqBackward = false;
        }

        // Equalities get priority over inequalities
        if haveReqEquals {
            debug_assert!((origkey_flags & SK_SEARCHNULL) == 0);
            unmarkikey[i as usize] = true;
            nunmark += 1;
            continue;
        } else if (origkey_flags & SK_BT_REQFWD) != 0 && (origkey_flags & SK_BT_REQBKWD) != 0 {
            debug_assert!(origkey_strategy == BTEqualStrategyNumber);
            haveReqEquals = true;
            for jj in firsti..i {
                if !unmarkikey[jj as usize] {
                    unmarkikey[jj as usize] = true;
                    nunmark += 1;
                }
            }
            continue;
        }

        // Deal with inequalities next
        if (origkey_flags & SK_BT_REQFWD) != 0 && !haveReqForward {
            haveReqForward = true;
            continue;
        } else if (origkey_flags & SK_BT_REQBKWD) != 0 && !haveReqBackward {
            haveReqBackward = true;
            continue;
        }

        unmarkikey[i as usize] = true;
        nunmark += 1;
    }

    // Should only be called when _bt_compare_scankey_args reported failure
    debug_assert!(nunmark > 0);

    let mut unmarkKeys: PgVec<'mcx, ScanKeyData<'mcx>> =
        vec_of_in(mcx, ScanKeyData::empty(), nunmark as usize)?;
    let mut keepKeys: PgVec<'mcx, ScanKeyData<'mcx>> =
        vec_of_in(mcx, ScanKeyData::empty(), (numberOfKeys - nunmark) as usize)?;
    nunmarked = 0;
    nkept = 0;
    let has_arrays = so.numArrayKeys != 0;
    let mut unmarkOrderProcs: PgVec<'mcx, u64> = PgVec::new_in(mcx);
    let mut keepOrderProcs: PgVec<'mcx, u64> = PgVec::new_in(mcx);
    if has_arrays {
        unmarkOrderProcs = vec_of_in(mcx, 0u64, nunmark as usize)?;
        keepOrderProcs = vec_of_in(mcx, 0u64, (numberOfKeys - nunmark) as usize)?;
    }

    for i in 0..numberOfKeys {
        if !unmarkikey[i as usize] {
            keepKeys[nkept as usize] = so.keyData[i as usize].clone();
            if has_arrays {
                keyDataMap[i as usize] = nkept;
                keepOrderProcs[nkept as usize] = so.orderProcs[i as usize];
            }
            nkept += 1;
            continue;
        }

        let mut unmark = so.keyData[i as usize].clone();

        if has_arrays {
            keyDataMap[i as usize] = (numberOfKeys - nunmark) + nunmarked;
            unmarkOrderProcs[nunmarked as usize] = so.orderProcs[i as usize];
        }

        debug_assert!((unmark.sk_flags & SK_BT_SKIP) == 0);
        debug_assert!(
            (unmark.sk_flags & SK_ISNULL) == 0
                || (unmark.sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) == 0
        );

        // Clear requiredness flags on redundant key (and on any subkeys)
        unmark.sk_flags &= !(SK_BT_REQFWD | SK_BT_REQBKWD);
        if (unmark.sk_flags & SK_ROW_HEADER) != 0 {
            if let Some(subkeys) = unmark.sk_subkeys.as_mut() {
                debug_assert!(subkeys[0].sk_strategy == unmark.sk_strategy);
                for subkey in subkeys.iter_mut() {
                    debug_assert!((subkey.sk_flags & SK_ROW_MEMBER) != 0);
                    subkey.sk_flags &= !(SK_BT_REQFWD | SK_BT_REQBKWD);
                    if (subkey.sk_flags & types_scan::scankey::SK_ROW_END) != 0 {
                        break;
                    }
                }
            }
        }

        unmarkKeys[nunmarked as usize] = unmark;
        nunmarked += 1;
    }

    debug_assert!(nkept == numberOfKeys - nunmark);
    debug_assert!(nunmarked == nunmark);
    for (k, kk) in keepKeys.into_iter().enumerate() {
        so.keyData[k] = kk;
    }
    for (k, uk) in unmarkKeys.into_iter().enumerate() {
        so.keyData[nkept as usize + k] = uk;
    }

    if has_arrays {
        for (k, op) in keepOrderProcs.iter().enumerate() {
            so.orderProcs[k] = *op;
        }
        for (k, op) in unmarkOrderProcs.iter().enumerate() {
            so.orderProcs[nkept as usize + k] = *op;
        }

        // Also fix-up array->scan_key references
        for arridx in 0..so.numArrayKeys as usize {
            let sk = so.arrayKeys[arridx].scan_key;
            so.arrayKeys[arridx].scan_key = keyDataMap[sk as usize];
        }

        // Sort so->arrayKeys[] based on its new scan_key offsets.
        so.arrayKeys
            .sort_by(|a, b| _bt_reorder_array_cmp(a, b).cmp(&0));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// _bt_reorder_array_cmp
// ---------------------------------------------------------------------------

/// `_bt_reorder_array_cmp()` — qsort comparator for reordering so->arrayKeys[].
fn _bt_reorder_array_cmp(arraya: &BTArrayKeyInfo, arrayb: &BTArrayKeyInfo) -> i32 {
    pg_cmp_s32(arraya.scan_key, arrayb.scan_key)
}

// ---------------------------------------------------------------------------
// _bt_preprocess_array_keys
// ---------------------------------------------------------------------------

/// `_bt_preprocess_array_keys()` — preprocess `SK_SEARCHARRAY` scan keys.
///
/// Deconstructs array(s) and sets up `BTArrayKeyInfo` for each equality key;
/// generates skip arrays. Returns the modified copy of the input keys (or
/// `None` when there's nothing to do).
pub fn _bt_preprocess_array_keys<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    input_keys: &[ScanKeyData<'mcx>],
    new_numberOfKeys: &mut i32,
) -> PgResult<Option<PgVec<'mcx, ScanKeyData<'mcx>>>> {
    let scan_numberOfKeys = input_keys.len() as i32;
    let mut numArrayKeys: i32;
    let numSkipArrayKeys: i32;
    let mut numSkipArrayKeys_left: i32;
    let numArrayKeyData: i32;
    let mut attno_skip: AttrNumber = 1;
    let mut origarrayatt: AttrNumber = InvalidAttrNumber;
    let mut origarraykey: i32 = -1;
    let mut origelemtype: Oid = InvalidOid;

    let mut skip_eq_ops = [InvalidOid; INDEX_MAX_KEYS_USIZE];

    // Count input array keys (also decides extra skip arrays).
    let mut nsk: i32 = 0;
    numArrayKeys = _bt_num_array_keys(rel, input_keys, &mut skip_eq_ops, &mut nsk)?;
    numSkipArrayKeys = nsk;
    so.skipScan = numSkipArrayKeys > 0;

    if numArrayKeys == 0 {
        return Ok(None);
    }

    numArrayKeyData = scan_numberOfKeys + numSkipArrayKeys;

    let mut arrayKeyData: PgVec<'mcx, ScanKeyData<'mcx>> =
        vec_of_in(mcx, ScanKeyData::empty(), numArrayKeyData as usize)?;

    // Per-array data.
    so.arrayKeys = vec_with_capacity_in(mcx, numArrayKeys as usize)?;
    for _ in 0..numArrayKeys {
        so.arrayKeys.push(BTArrayKeyInfo::new_in(mcx));
    }
    // ORDER procs.
    so.orderProcs = vec_of_in(mcx, 0u64, numArrayKeyData as usize)?;

    numArrayKeys = 0;
    let mut numArrayKeyDataPos: i32 = 0;
    numSkipArrayKeys_left = numSkipArrayKeys;

    let mut input_ikey: i32 = 0;
    'outer: while input_ikey < scan_numberOfKeys {
        let inkey = input_keys[input_ikey as usize].clone();

        // Backfill skip arrays for attrs < or <= input key's attr?
        while numSkipArrayKeys_left != 0 && attno_skip <= inkey.sk_attno {
            let opfamily = rd_opfamily(rel, attno_skip);
            let opcintype = rd_opcintype(rel, attno_skip);
            let collation = rd_indcollation(rel, attno_skip);
            let eq_op = skip_eq_ops[(attno_skip - 1) as usize];

            if !oid_is_valid(eq_op) {
                debug_assert!(attno_skip == inkey.sk_attno);
                debug_assert!(input_ikey < scan_numberOfKeys - 1);
                attno_skip += 1;
                break;
            }

            let cmp_proc = get_opcode::call(eq_op)?;
            if !reg_procedure_is_valid(cmp_proc) {
                return Err(elog_error(format!(
                    "missing oprcode for skipping equals operator {}",
                    eq_op
                )));
            }

            // ScanKeyEntryInitialize(cur, SK_SEARCHARRAY | SK_BT_SKIP, attno_skip,
            //   BTEqualStrategyNumber, InvalidOid, collation, cmp_proc, (Datum) 0)
            {
                let cur = &mut arrayKeyData[numArrayKeyDataPos as usize];
                cur.sk_flags = SK_SEARCHARRAY | SK_BT_SKIP;
                cur.sk_attno = attno_skip;
                cur.sk_strategy = BTEqualStrategyNumber;
                cur.sk_subtype = InvalidOid;
                cur.sk_collation = collation;
                cur.sk_func = types_core::fmgr::FmgrInfo::empty();
                cur.sk_func.fn_oid = cmp_proc;
                cur.sk_argument = Datum::null();
            }

            so.arrayKeys[numArrayKeys as usize].scan_key = numArrayKeyDataPos;
            so.arrayKeys[numArrayKeys as usize].num_elems = -1;

            let (attlen, attbyval) = compact_attr(rel, (attno_skip - 1) as i32);
            let reverse = (rd_indoption(rel, attno_skip) & INDOPTION_DESC) != 0;
            so.arrayKeys[numArrayKeys as usize].attlen = attlen;
            so.arrayKeys[numArrayKeys as usize].attbyval = attbyval;
            so.arrayKeys[numArrayKeys as usize].null_elem = true; // for now
            match prepare_skip_support(rel, attno_skip, opfamily, opcintype, reverse) {
                Some((handle, data)) => {
                    so.arrayKeys[numArrayKeys as usize].sksup = Some(handle);
                    so.arrayKeys[numArrayKeys as usize].sksup_data = Some(relocate_sksup(data));
                }
                None => {
                    so.arrayKeys[numArrayKeys as usize].sksup = None;
                    so.arrayKeys[numArrayKeys as usize].sksup_data = None;
                }
            }
            so.arrayKeys[numArrayKeys as usize].low_compare = None; // for now
            so.arrayKeys[numArrayKeys as usize].high_compare = None; // for now

            // We'll need a 3-way ORDER proc.  Set that up now.
            let cur_copy = arrayKeyData[numArrayKeyDataPos as usize].clone();
            let mut orderproc: u64 = 0;
            _bt_setup_array_cmp(rel, so, &cur_copy, opcintype, &mut orderproc, None)?;
            so.orderProcs[numArrayKeyDataPos as usize] = orderproc;

            numArrayKeys += 1;
            numArrayKeyDataPos += 1; // keep this scan key/array

            numSkipArrayKeys_left -= 1;
            attno_skip += 1;
        }

        // Provisionally copy scan key into arrayKeyData[]
        arrayKeyData[numArrayKeyDataPos as usize] = inkey.clone();
        let cur_flags = arrayKeyData[numArrayKeyDataPos as usize].sk_flags;

        if (cur_flags & SK_SEARCHARRAY) == 0 {
            numArrayKeyDataPos += 1; // keep this non-array scan key
            input_ikey += 1;
            continue;
        }

        // Process SAOP array scan key
        debug_assert!(
            (arrayKeyData[numArrayKeyDataPos as usize].sk_flags
                & (SK_ROW_HEADER | SK_SEARCHNULL | SK_SEARCHNOTNULL))
                == 0
        );

        // If array is null as a whole, the scan qual is unsatisfiable
        if (arrayKeyData[numArrayKeyDataPos as usize].sk_flags & SK_ISNULL) != 0 {
            so.qual_ok = false;
            break;
        }

        // Determine the nominal datatype of the array elements.
        let mut elemtype = arrayKeyData[numArrayKeyDataPos as usize].sk_subtype;
        if elemtype == InvalidOid {
            elemtype = rd_opcintype(rel, arrayKeyData[numArrayKeyDataPos as usize].sk_attno);
        }

        // Deconstruct the array into elements (compress out NULLs).
        let tlbva = get_typlenbyvalalign::call(elemtype)?;
        let arr_word = to_word(&arrayKeyData[numArrayKeyDataPos as usize].sk_argument);
        let pairs = deconstruct_array::call(
            mcx,
            arr_word,
            elemtype,
            tlbva.typlen,
            tlbva.typbyval,
            tlbva.typalign as core::ffi::c_char,
        )?;

        let mut elem_values: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, pairs.len())?;
        for (val, isnull) in pairs.iter() {
            if !*isnull {
                elem_values.push(word_to_datum(*val));
            }
        }
        let num_nonnulls = elem_values.len() as i32;

        if num_nonnulls == 0 {
            so.qual_ok = false;
            break;
        }

        // Inequality array degenerates to a comparison vs the extreme element.
        let cur_strategy = arrayKeyData[numArrayKeyDataPos as usize].sk_strategy;
        match cur_strategy {
            s if s == BTLessStrategyNumber || s == BTLessEqualStrategyNumber => {
                let cur_copy = arrayKeyData[numArrayKeyDataPos as usize].clone();
                let extreme = _bt_find_extreme_element(
                    rel,
                    &cur_copy,
                    elemtype,
                    BTGreaterStrategyNumber,
                    elem_values.as_slice(),
                    num_nonnulls,
                )?;
                arrayKeyData[numArrayKeyDataPos as usize].sk_argument = extreme;
                numArrayKeyDataPos += 1;
                input_ikey += 1;
                continue;
            }
            s if s == BTEqualStrategyNumber => { /* proceed with rest of loop */ }
            s if s == BTGreaterEqualStrategyNumber || s == BTGreaterStrategyNumber => {
                let cur_copy = arrayKeyData[numArrayKeyDataPos as usize].clone();
                let extreme = _bt_find_extreme_element(
                    rel,
                    &cur_copy,
                    elemtype,
                    BTLessStrategyNumber,
                    elem_values.as_slice(),
                    num_nonnulls,
                )?;
                arrayKeyData[numArrayKeyDataPos as usize].sk_argument = extreme;
                numArrayKeyDataPos += 1;
                input_ikey += 1;
                continue;
            }
            other => {
                return Err(elog_error(format!(
                    "unrecognized StrategyNumber: {}",
                    other as i32
                )));
            }
        }

        // Set up the 3-way ORDER proc (and the same-type sort proc).
        let cur_copy = arrayKeyData[numArrayKeyDataPos as usize].clone();
        let mut orderproc: u64 = 0;
        let mut sortproc: u64 = 0;
        let mut sortproc_is_orderproc = false;
        _bt_setup_array_cmp(
            rel,
            so,
            &cur_copy,
            elemtype,
            &mut orderproc,
            Some((&mut sortproc, &mut sortproc_is_orderproc)),
        )?;
        so.orderProcs[numArrayKeyDataPos as usize] = orderproc;
        let sortproc_val = if sortproc_is_orderproc {
            orderproc
        } else {
            sortproc
        };

        // Sort the non-null elements and eliminate duplicates.
        let reverse =
            (rd_indoption(rel, arrayKeyData[numArrayKeyDataPos as usize].sk_attno) & INDOPTION_DESC)
                != 0;
        let cur_copy = arrayKeyData[numArrayKeyDataPos as usize].clone();
        let num_elems =
            _bt_sort_array_elements(&cur_copy, sortproc_val, reverse, elem_values.as_mut_slice())?;
        elem_values.truncate(num_elems as usize);

        if origarrayatt == arrayKeyData[numArrayKeyDataPos as usize].sk_attno {
            let orig_idx = origarraykey as usize;

            debug_assert!(
                arrayKeyData[so.arrayKeys[orig_idx].scan_key as usize].sk_attno
                    == arrayKeyData[numArrayKeyDataPos as usize].sk_attno
            );
            debug_assert!(
                arrayKeyData[so.arrayKeys[orig_idx].scan_key as usize].sk_collation
                    == arrayKeyData[numArrayKeyDataPos as usize].sk_collation
            );

            // Borrow the original array's elements out for the merge.
            let mut orig_elems = core::mem::replace(
                &mut so.arrayKeys[orig_idx].elem_values,
                PgVec::new_in(mcx),
            );
            let mut orig_nelems = so.arrayKeys[orig_idx].num_elems;
            let cur_copy = arrayKeyData[numArrayKeyDataPos as usize].clone();
            let merged = _bt_merge_arrays(
                rel,
                so,
                &cur_copy,
                sortproc_val,
                reverse,
                origelemtype,
                elemtype,
                orig_elems.as_mut_slice(),
                &mut orig_nelems,
                elem_values.as_slice(),
                num_elems,
            )?;
            so.arrayKeys[orig_idx].elem_values = orig_elems;
            so.arrayKeys[orig_idx].num_elems = orig_nelems;

            if merged {
                // Successfully eliminated this array
                if so.arrayKeys[orig_idx].num_elems == 0 {
                    so.qual_ok = false;
                    break;
                }
                // Throw away this scan key/array
                input_ikey += 1;
                continue;
            }
            // Unable to merge; keep both scan keys/arrays.
        } else {
            origarrayatt = arrayKeyData[numArrayKeyDataPos as usize].sk_attno;
            origarraykey = numArrayKeys;
            origelemtype = elemtype;
        }

        so.arrayKeys[numArrayKeys as usize].scan_key = numArrayKeyDataPos;
        so.arrayKeys[numArrayKeys as usize].num_elems = num_elems;
        so.arrayKeys[numArrayKeys as usize].elem_values = elem_values;
        so.arrayKeys[numArrayKeys as usize].cur_elem = -1;

        numArrayKeys += 1;
        numArrayKeyDataPos += 1; // keep this scan key/array
        input_ikey += 1;
        continue 'outer;
    }

    debug_assert!(numSkipArrayKeys_left == 0 || !so.qual_ok);

    so.numArrayKeys = numArrayKeys;
    // truncate to the actual number of array keys produced
    so.arrayKeys.truncate(numArrayKeys as usize);
    *new_numberOfKeys = numArrayKeyDataPos;

    Ok(Some(arrayKeyData))
}

/// Re-bind a `'static` `BTSkipSupport` (from the unported skip-support owner) to
/// `'mcx`. Both sentinels are by-value on the only call path; a by-ref sentinel
/// would carry owned bytes into `'mcx`.
fn relocate_sksup<'mcx>(s: types_nbtree::BTSkipSupport<'static>) -> types_nbtree::BTSkipSupport<'mcx> {
    types_nbtree::BTSkipSupport {
        low_elem: relocate_datum(s.low_elem),
        high_elem: relocate_datum(s.high_elem),
        attno: s.attno,
    }
}

/// Convert a bare-word fmgr-seam `Datum` into the canonical by-value `Datum`.
#[inline]
fn word_to_datum<'mcx>(w: types_datum::Datum) -> Datum<'mcx> {
    Datum::ByVal(w.as_usize())
}

/// `memmove(elems, elems + from, sizeof(Datum) * n)` — left-shift `n` elements
/// starting at `from` down to offset 0. The canonical `Datum` is not `Copy`
/// (it has a by-reference arm), so the move is done element-by-element via
/// `clone` (forward order is safe for a strict left shift where dest < src).
fn datum_memmove_to_start<'mcx>(elems: &mut [Datum<'mcx>], from: usize, n: usize) {
    for k in 0..n {
        elems[k] = elems[from + k].clone();
    }
}

// ---------------------------------------------------------------------------
// _bt_preprocess_array_keys_final
// ---------------------------------------------------------------------------

/// `_bt_preprocess_array_keys_final()` — fix up array scan key references.
fn _bt_preprocess_array_keys_final<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut BTScanOpaqueData<'mcx>,
    keyDataMap: &[i32],
    parallel_scan: bool,
) -> PgResult<()> {
    let mut arrayidx: i32 = 0;
    let mut last_equal_output_ikey: i32 = -1;

    debug_assert!(so.qual_ok);

    if so.numArrayKeys == 0 {
        return Ok(());
    }

    let mut output_ikey: i32 = 0;
    while output_ikey < so.numberOfKeys {
        let outkey_flags = so.keyData[output_ikey as usize].sk_flags;
        let outkey_strategy = so.keyData[output_ikey as usize].sk_strategy;
        let outkey_attno = so.keyData[output_ikey as usize].sk_attno;
        let outkey_subtype = so.keyData[output_ikey as usize].sk_subtype;

        debug_assert!(outkey_strategy != InvalidStrategy);

        if outkey_strategy != BTEqualStrategyNumber {
            output_ikey += 1;
            continue;
        }

        let input_ikey = keyDataMap[output_ikey as usize];

        debug_assert!(last_equal_output_ikey < output_ikey);
        debug_assert!(last_equal_output_ikey < input_ikey);
        last_equal_output_ikey = output_ikey;

        if (outkey_flags & SK_SEARCHARRAY) == 0 {
            // No ORDER proc needed for IS NULL
            if (outkey_flags & SK_SEARCHNULL) != 0 {
                output_ikey += 1;
                continue;
            }
            // A non-required scan key doesn't need an ORDER proc either
            if (outkey_flags & SK_BT_REQFWD) == 0 {
                output_ikey += 1;
                continue;
            }

            let mut elemtype = outkey_subtype;
            if elemtype == InvalidOid {
                elemtype = rd_opcintype(rel, outkey_attno);
            }

            let outkey_copy = so.keyData[output_ikey as usize].clone();
            let mut orderproc: u64 = 0;
            _bt_setup_array_cmp(rel, so, &outkey_copy, elemtype, &mut orderproc, None)?;
            so.orderProcs[output_ikey as usize] = orderproc;
            output_ikey += 1;
            continue;
        }

        // Reorder existing array scan key so->orderProcs[] entries.
        so.orderProcs[output_ikey as usize] = so.orderProcs[input_ikey as usize];

        // Fix-up array->scan_key references for arrays
        while arrayidx < so.numArrayKeys {
            let array_num_elems = so.arrayKeys[arrayidx as usize].num_elems;
            let array_scan_key = so.arrayKeys[arrayidx as usize].scan_key;

            debug_assert!(array_num_elems > 0 || array_num_elems == -1);
            debug_assert!(array_num_elems != -1 || (outkey_flags & SK_BT_REQFWD) != 0);
            debug_assert!(
                array_num_elems != -1 || outkey_attno < index_nkeyatts(rel) as AttrNumber
            );

            if array_scan_key == input_ikey {
                so.arrayKeys[arrayidx as usize].scan_key = output_ikey;

                if so.arrayKeys[arrayidx as usize].num_elems == 1 {
                    let val = so.arrayKeys[arrayidx as usize].elem_values[0].clone();
                    so.keyData[output_ikey as usize].sk_flags &= !SK_SEARCHARRAY;
                    so.keyData[output_ikey as usize].sk_argument = val;
                    so.numArrayKeys -= 1;

                    if so.numArrayKeys == 0 {
                        return Ok(());
                    }

                    // Shift other arrays forward
                    so.arrayKeys.remove(arrayidx as usize);
                    // Don't increment arrayidx.
                } else {
                    if so.arrayKeys[arrayidx as usize].num_elems == -1
                        && so.arrayKeys[arrayidx as usize].sksup.is_some()
                        && !so.arrayKeys[arrayidx as usize].null_elem
                    {
                        let outkey_copy = so.keyData[output_ikey as usize].clone();
                        _bt_skiparray_strat_adjust(rel, so, &outkey_copy, arrayidx as usize)?;
                    }
                    arrayidx += 1;
                }
                break;
            }
            arrayidx += 1;
        }

        output_ikey += 1;
    }

    // Parallel index scans defensively limit the array count.
    if parallel_scan && so.numArrayKeys > INDEX_MAX_KEYS {
        return Err(elog_error(format!(
            "number of array scan keys left by preprocessing ({}) exceeds the maximum allowed by parallel btree index scans ({})",
            so.numArrayKeys, INDEX_MAX_KEYS
        )));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// _bt_num_array_keys
// ---------------------------------------------------------------------------

/// `_bt_num_array_keys()` — determine # of BTArrayKeyInfo entries.
fn _bt_num_array_keys<'mcx>(
    rel: &Relation<'mcx>,
    input_keys: &[ScanKeyData<'mcx>],
    skip_eq_ops_out: &mut [Oid],
    numSkipArrayKeys_out: &mut i32,
) -> PgResult<i32> {
    let scan_numberOfKeys = input_keys.len() as i32;
    let mut attno_skip: AttrNumber = 1;
    let mut attno_inkey: AttrNumber = 1;
    let mut attno_has_equal = false;
    let mut attno_has_rowcompare = false;
    let mut numSAOPArrayKeys: i32;
    let mut numSkipArrayKeys: i32;
    let mut prev_numSkipArrayKeys: i32;

    debug_assert!(scan_numberOfKeys != 0);

    numSAOPArrayKeys = 0;
    *numSkipArrayKeys_out = 0;
    prev_numSkipArrayKeys = 0;
    numSkipArrayKeys = 0;
    for inkey in input_keys.iter() {
        if (inkey.sk_flags & SK_SEARCHARRAY) != 0 {
            numSAOPArrayKeys += 1;
        }
    }

    // (DEBUG_DISABLE_SKIP_SCAN is not defined.)

    let mut i: i32 = 0;
    loop {
        // Backfill skip arrays for any wholly omitted attributes prior to attno_inkey
        while attno_skip < attno_inkey {
            let opfamily = rd_opfamily(rel, attno_skip);
            let opcintype = rd_opcintype(rel, attno_skip);

            skip_eq_ops_out[(attno_skip - 1) as usize] = get_opfamily_member::call(
                opfamily,
                opcintype,
                opcintype,
                BTEqualStrategyNumber as i16,
            )?;
            if !oid_is_valid(skip_eq_ops_out[(attno_skip - 1) as usize]) {
                *numSkipArrayKeys_out = prev_numSkipArrayKeys;
                return Ok(numSAOPArrayKeys + prev_numSkipArrayKeys);
            }

            numSkipArrayKeys += 1;
            attno_skip += 1;
        }

        prev_numSkipArrayKeys = numSkipArrayKeys;

        // Stop once past the final input scan key.
        if i == scan_numberOfKeys {
            break;
        }

        let inkey = input_keys[i as usize].clone();

        // Stop adding skip arrays once we see a RowCompare.
        if attno_has_rowcompare {
            break;
        }

        if attno_inkey < inkey.sk_attno {
            if attno_has_equal {
                skip_eq_ops_out[(attno_skip - 1) as usize] = InvalidOid;
            } else {
                let opfamily = rd_opfamily(rel, attno_skip);
                let opcintype = rd_opcintype(rel, attno_skip);
                skip_eq_ops_out[(attno_skip - 1) as usize] = get_opfamily_member::call(
                    opfamily,
                    opcintype,
                    opcintype,
                    BTEqualStrategyNumber as i16,
                )?;
                if !oid_is_valid(skip_eq_ops_out[(attno_skip - 1) as usize]) {
                    break;
                }
                numSkipArrayKeys += 1;
            }

            attno_skip += 1;
            attno_inkey = inkey.sk_attno;
            attno_has_equal = false;
        }

        if inkey.sk_strategy == BTEqualStrategyNumber || (inkey.sk_flags & SK_SEARCHNULL) != 0 {
            attno_has_equal = true;
        }
        if (inkey.sk_flags & SK_ROW_HEADER) != 0 {
            attno_has_rowcompare = true;
        }

        i += 1;
    }

    *numSkipArrayKeys_out = numSkipArrayKeys;
    Ok(numSAOPArrayKeys + numSkipArrayKeys)
}

// ---------------------------------------------------------------------------
// _bt_find_extreme_element
// ---------------------------------------------------------------------------

/// `_bt_find_extreme_element()` — get least or greatest array element.
fn _bt_find_extreme_element<'mcx>(
    rel: &Relation<'mcx>,
    skey: &ScanKeyData<'mcx>,
    elemtype: Oid,
    strat: StrategyNumber,
    elems: &[Datum<'mcx>],
    nelems: i32,
) -> PgResult<Datum<'mcx>> {
    debug_assert!(skey.sk_strategy != BTEqualStrategyNumber);
    debug_assert!(oid_is_valid(elemtype));

    let opfamily = rd_opfamily(rel, skey.sk_attno);
    let cmp_op = get_opfamily_member::call(opfamily, elemtype, elemtype, strat as i16)?;
    if !oid_is_valid(cmp_op) {
        return Err(elog_error(format!(
            "missing operator {}({},{}) in opfamily {}",
            strat, elemtype, elemtype, opfamily
        )));
    }
    let cmp_proc = get_opcode::call(cmp_op)?;
    if !reg_procedure_is_valid(cmp_proc) {
        return Err(elog_error(format!("missing oprcode for operator {}", cmp_op)));
    }

    debug_assert!(nelems > 0);
    let mut result = elems[0].clone();
    for i in 1..nelems as usize {
        if datum_get_bool(function_call2_coll_oid(
            cmp_proc,
            skey.sk_collation,
            &elems[i],
            &result,
        )?) {
            result = elems[i].clone();
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// _bt_setup_array_cmp
// ---------------------------------------------------------------------------

/// `_bt_setup_array_cmp()` — set up array comparison functions.
///
/// Sets the ORDER proc handle in `orderproc`. When `sortprocp` is
/// `Some((out, is_order))`, also sets the same-type ORDER proc: `*is_order =
/// true` means "sortproc points to the same proc as orderproc".
fn _bt_setup_array_cmp<'mcx>(
    rel: &Relation<'mcx>,
    _so: &mut BTScanOpaqueData<'mcx>,
    skey: &ScanKeyData<'mcx>,
    elemtype: Oid,
    orderproc: &mut u64,
    sortprocp: Option<(&mut u64, &mut bool)>,
) -> PgResult<()> {
    let opcintype = rd_opcintype(rel, skey.sk_attno);

    debug_assert!(skey.sk_strategy == BTEqualStrategyNumber);
    debug_assert!(oid_is_valid(elemtype));

    // Non-cross-type: use the cached comparison function.
    if elemtype == opcintype {
        *orderproc = index_getprocinfo(rel, skey.sk_attno, BTORDER_PROC);
        if let Some((_out, is_order)) = sortprocp {
            *is_order = true; // *sortprocp = orderproc
        }
        return Ok(());
    }

    // Cross-type ORDER proc lookup.
    let opfamily = rd_opfamily(rel, skey.sk_attno);
    let cmp_proc = get_opfamily_proc::call(opfamily, opcintype, elemtype, BTORDER_PROC)?;
    if !reg_procedure_is_valid(cmp_proc) {
        return Err(elog_error(format!(
            "missing support function {}({},{}) for attribute {} of index \"{}\"",
            BTORDER_PROC,
            opcintype,
            elemtype,
            skey.sk_attno,
            relation_name(rel)
        )));
    }
    *orderproc = fmgr_info(cmp_proc);

    let (out, is_order) = match sortprocp {
        Some(p) => p,
        None => return Ok(()),
    };

    // Same-type sort proc lookup.
    let cmp_proc = get_opfamily_proc::call(opfamily, elemtype, elemtype, BTORDER_PROC)?;
    if !reg_procedure_is_valid(cmp_proc) {
        return Err(elog_error(format!(
            "missing support function {}({},{}) for attribute {} of index \"{}\"",
            BTORDER_PROC,
            elemtype,
            elemtype,
            skey.sk_attno,
            relation_name(rel)
        )));
    }
    *out = fmgr_info(cmp_proc);
    *is_order = false;

    Ok(())
}

// ---------------------------------------------------------------------------
// _bt_sort_array_elements
// ---------------------------------------------------------------------------

/// `_bt_sort_array_elements()` — sort and de-dup array elements in place;
/// returns the new element count.
fn _bt_sort_array_elements<'mcx>(
    skey: &ScanKeyData<'mcx>,
    sortproc: u64,
    reverse: bool,
    elems: &mut [Datum<'mcx>],
) -> PgResult<i32> {
    let nelems = elems.len() as i32;
    if nelems <= 1 {
        return Ok(nelems);
    }

    let cxt = BtSortArrayContext {
        sortproc,
        collation: skey.sk_collation,
        reverse,
    };
    // qsort_arg: a fallible comparator can ereport(ERROR); capture it.
    let mut sort_err: Option<PgError> = None;
    elems.sort_by(|a, b| {
        if sort_err.is_some() {
            return core::cmp::Ordering::Equal;
        }
        match _bt_compare_array_elements(a, b, &cxt) {
            Ok(c) => c.cmp(&0),
            Err(e) => {
                sort_err = Some(e);
                core::cmp::Ordering::Equal
            }
        }
    });
    if let Some(e) = sort_err {
        return Err(e);
    }

    // qunique_arg: remove adjacent duplicates.
    qunique_arg(elems, &cxt)
}

/// `qunique_arg(array, nitems, sizeof(Datum), cmp, arg)` (lib/qunique.h).
fn qunique_arg<'mcx>(elems: &mut [Datum<'mcx>], cxt: &BtSortArrayContext) -> PgResult<i32> {
    let nitems = elems.len();
    if nitems <= 1 {
        return Ok(nitems as i32);
    }
    let mut dst = 0usize;
    for src in 1..nitems {
        let differ = {
            let a = elems[dst].clone();
            let b = elems[src].clone();
            _bt_compare_array_elements(&a, &b, cxt)? != 0
        };
        if differ {
            dst += 1;
            if dst != src {
                elems[dst] = elems[src].clone();
            }
        }
    }
    Ok((dst + 1) as i32)
}

// ---------------------------------------------------------------------------
// _bt_merge_arrays
// ---------------------------------------------------------------------------

/// `_bt_merge_arrays()` — merge next array's elements into an original array.
fn _bt_merge_arrays<'mcx>(
    rel: &Relation<'mcx>,
    _so: &mut BTScanOpaqueData<'mcx>,
    skey: &ScanKeyData<'mcx>,
    sortproc: u64,
    reverse: bool,
    origelemtype: Oid,
    nextelemtype: Oid,
    elems_orig: &mut [Datum<'mcx>],
    nelems_orig: &mut i32,
    elems_next: &[Datum<'mcx>],
    nelems_next: i32,
) -> PgResult<bool> {
    let nelems_orig_start = *nelems_orig;
    let mut nelems_orig_merged = 0i32;
    let mut mergeproc = sortproc;

    debug_assert!(skey.sk_strategy == BTEqualStrategyNumber);
    debug_assert!(oid_is_valid(origelemtype) && oid_is_valid(nextelemtype));

    if origelemtype != nextelemtype {
        let opfamily = rd_opfamily(rel, skey.sk_attno);
        let cmp_proc = get_opfamily_proc::call(opfamily, origelemtype, nextelemtype, BTORDER_PROC)?;
        if !reg_procedure_is_valid(cmp_proc) {
            return Ok(false);
        }
        mergeproc = fmgr_info(cmp_proc);
    }

    let cxt = BtSortArrayContext {
        sortproc: mergeproc,
        collation: skey.sk_collation,
        reverse,
    };

    let mut i = 0i32;
    let mut j = 0i32;
    while i < nelems_orig_start && j < nelems_next {
        let oelem = elems_orig[i as usize].clone();
        let nelem = elems_next[j as usize].clone();
        let res = _bt_compare_array_elements(&oelem, &nelem, &cxt)?;

        if res == 0 {
            elems_orig[nelems_orig_merged as usize] = oelem;
            nelems_orig_merged += 1;
            i += 1;
            j += 1;
        } else if res < 0 {
            i += 1;
        } else {
            j += 1;
        }
    }

    *nelems_orig = nelems_orig_merged;
    Ok(true)
}

// ---------------------------------------------------------------------------
// _bt_compare_array_elements
// ---------------------------------------------------------------------------

/// `_bt_compare_array_elements()` — qsort_arg comparator for sorting array
/// elements.
fn _bt_compare_array_elements<'mcx>(
    da: &Datum<'mcx>,
    db: &Datum<'mcx>,
    cxt: &BtSortArrayContext,
) -> PgResult<i32> {
    let mut compare = datum_get_int32(function_call2_coll_oid(
        cxt.sortproc as u32, // sortproc handle's low 32 bits carry the proc OID
        cxt.collation,
        da,
        db,
    )?);
    if cxt.reverse {
        compare = invert_compare_result(compare);
    }
    Ok(compare)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_cmp_s32_orders() {
        assert_eq!(pg_cmp_s32(1, 2), -1);
        assert_eq!(pg_cmp_s32(2, 2), 0);
        assert_eq!(pg_cmp_s32(3, 2), 1);
    }

    #[test]
    fn invert_compare_result_matches_c() {
        // C: var = (var < 0) ? 1 : -(var)
        assert_eq!(invert_compare_result(-5), 1);
        assert_eq!(invert_compare_result(0), 0);
        assert_eq!(invert_compare_result(7), -7);
    }

    #[test]
    fn commute_strategy_flips() {
        assert_eq!(bt_commute_strategy_number(BTLessStrategyNumber), BTGreaterStrategyNumber);
        assert_eq!(
            bt_commute_strategy_number(BTLessEqualStrategyNumber),
            BTGreaterEqualStrategyNumber
        );
        assert_eq!(bt_commute_strategy_number(BTEqualStrategyNumber), BTEqualStrategyNumber);
        assert_eq!(
            bt_commute_strategy_number(BTGreaterEqualStrategyNumber),
            BTLessEqualStrategyNumber
        );
        assert_eq!(
            bt_commute_strategy_number(BTGreaterStrategyNumber),
            BTLessStrategyNumber
        );
    }

    #[test]
    fn indoption_flag_shift() {
        // DESC bit shifts to SK_BT_DESC, NULLS FIRST to SK_BT_NULLS_FIRST.
        let desc = (INDOPTION_DESC as i32) << SK_BT_INDOPTION_SHIFT;
        assert_eq!(desc, SK_BT_DESC);
        let nf = (INDOPTION_NULLS_FIRST as i32) << SK_BT_INDOPTION_SHIFT;
        assert_eq!(nf, SK_BT_NULLS_FIRST);
    }

    #[test]
    fn mark_scankey_required_directions() {
        let mut k = ScanKeyData::empty();
        k.sk_strategy = BTLessStrategyNumber;
        _bt_mark_scankey_required(&mut k).unwrap();
        assert_eq!(k.sk_flags & SK_BT_REQFWD, SK_BT_REQFWD);
        assert_eq!(k.sk_flags & SK_BT_REQBKWD, 0);

        let mut k = ScanKeyData::empty();
        k.sk_strategy = BTEqualStrategyNumber;
        _bt_mark_scankey_required(&mut k).unwrap();
        assert_eq!(k.sk_flags & SK_BT_REQFWD, SK_BT_REQFWD);
        assert_eq!(k.sk_flags & SK_BT_REQBKWD, SK_BT_REQBKWD);

        let mut k = ScanKeyData::empty();
        k.sk_strategy = BTGreaterStrategyNumber;
        _bt_mark_scankey_required(&mut k).unwrap();
        assert_eq!(k.sk_flags & SK_BT_REQBKWD, SK_BT_REQBKWD);
        assert_eq!(k.sk_flags & SK_BT_REQFWD, 0);
    }

    #[test]
    fn reorder_array_cmp_by_scan_key() {
        let root = mcx::MemoryContext::new("nbtpp-test");
        let mcx = root.mcx();
        let mut a = BTArrayKeyInfo::new_in(mcx);
        let mut b = BTArrayKeyInfo::new_in(mcx);
        a.scan_key = 1;
        b.scan_key = 3;
        assert!(_bt_reorder_array_cmp(&a, &b) < 0);
        assert!(_bt_reorder_array_cmp(&b, &a) > 0);
        a.scan_key = 3;
        assert_eq!(_bt_reorder_array_cmp(&a, &b), 0);
    }
}
