//! Port of `src/backend/utils/adt/tsginidx.c` (PostgreSQL 18.3) — the GIN
//! support functions for the `tsvector_ops` opclass.
//!
//! Entry points: `gin_cmp_tslexeme` / `gin_cmp_prefix` (GIN `compare` /
//! `comparePartial`), `gin_extract_tsvector` (`extractValue`),
//! `gin_extract_tsquery` (`extractQuery`), `gin_tsquery_consistent` /
//! `gin_tsquery_triconsistent` (`consistent` / `triConsistent`), plus the five
//! backward-compatibility stubs.
//!
//! The detoasted `tsvector` / `tsquery` datums are walked as raw byte slices
//! via the `ts_type.h` header-macro accessors (the offset arithmetic the C
//! macros compute). The three `tsvector_op.c` functions (`tsCompareString`,
//! `tsquery_requires_match`, `TS_execute_ternary`) are owned by
//! `backend-utils-adt-tsvector-core` and called through its seam crate; they
//! panic until that owner lands.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use tsvector_core_seams as tsop;
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::{PgError, PgResult};
use tsearch::gin::{
    GinTernaryValue, GIN_FALSE, GIN_MAYBE, GIN_SEARCH_MODE_ALL, GIN_SEARCH_MODE_DEFAULT, GIN_TRUE,
};
use tsearch::tsearch::{
    ExecPhraseData, QueryItem, QueryOperand, QueryOperator, TSTernaryValue, QI_OPR, QI_VAL,
    TS_EXEC_PHRASE_NO_POS,
};

pub mod fmgr_builtins;

#[cfg(test)]
mod tests;

/// Install this crate's fmgr builtin rows into the fmgr-core table. Invoked by
/// `seams-init::init_all` (registers `gin_cmp_tslexeme`, oid 3724).
pub fn init_seams() {
    fmgr_builtins::register_tsginidx_builtins();
}

// ---------------------------------------------------------------------------
// ts_type.h header-macro accessors over the raw (detoasted) datum byte slices.
//
// A `tsvector` datum: `int32 vl_len_; int32 size;` (DATAHDRSIZE = 8) then
// `WordEntry entries[size]` then the lexeme storage.  A `tsquery` datum:
// `int32 vl_len_; int32 size;` (HDRSIZETQ = 8) then `QueryItem items[size]`
// then the operand C-strings.  Native endianness.
// ---------------------------------------------------------------------------

const VARHDRSZ: usize = 4;
const DATAHDRSIZE: usize = 8;
const HDRSIZETQ: usize = 8;
const WORDENTRY_SIZE: usize = 4;
const QUERYITEM_SIZE: usize = 12;

/// `vector->size` — number of `WordEntry` entries in a tsvector datum.
#[inline]
fn tsv_size(t: &[u8]) -> i32 {
    i32::from_ne_bytes([t[4], t[5], t[6], t[7]])
}

#[inline]
fn tsv_entry_word(t: &[u8], i: usize) -> u32 {
    let off = DATAHDRSIZE + i * WORDENTRY_SIZE;
    u32::from_ne_bytes([t[off], t[off + 1], t[off + 2], t[off + 3]])
}

/// `WordEntry.len:11` for entry `i`.
#[inline]
fn we_len(t: &[u8], i: usize) -> u32 {
    (tsv_entry_word(t, i) >> 1) & 0x7FF
}

/// `WordEntry.pos:20` for entry `i`.
#[inline]
fn we_pos(t: &[u8], i: usize) -> u32 {
    (tsv_entry_word(t, i) >> 12) & 0xFFFFF
}

/// `STRPTR(vector)` — byte offset of the lexeme storage.
#[inline]
fn tsv_strptr_off(t: &[u8]) -> usize {
    DATAHDRSIZE + (tsv_size(t) as usize) * WORDENTRY_SIZE
}

/// `STRPTR(vector) + we->pos` for `we->len` bytes — the lexeme string of `i`.
#[inline]
fn tsv_lexeme(t: &[u8], i: usize) -> &[u8] {
    let base = tsv_strptr_off(t) + we_pos(t, i) as usize;
    &t[base..base + we_len(t, i) as usize]
}

/// `query->size` — number of `QueryItem`s in a tsquery datum.
#[inline]
fn tsq_size(q: &[u8]) -> i32 {
    i32::from_ne_bytes([q[4], q[5], q[6], q[7]])
}

/// `GETQUERY(query)` reified — decode the `QueryItem` array, allocated in `mcx`.
#[inline]
fn getquery<'mcx>(mcx: Mcx<'mcx>, q: &[u8]) -> PgResult<PgVec<'mcx, QueryItem>> {
    let n = tsq_size(q) as usize;
    let mut items = vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        items.push(read_query_item(q, i));
    }
    Ok(items)
}

/// Decode the `QueryItem` at array index `i`.
#[inline]
fn read_query_item(q: &[u8], i: usize) -> QueryItem {
    let off = HDRSIZETQ + i * QUERYITEM_SIZE;
    let type_ = q[off] as i8;
    if type_ == QI_OPR {
        let oper = q[off + 1] as i8;
        let distance = i16::from_ne_bytes([q[off + 2], q[off + 3]]);
        let left = u32::from_ne_bytes([q[off + 4], q[off + 5], q[off + 6], q[off + 7]]);
        QueryItem::Qoperator(QueryOperator {
            type_,
            oper,
            distance,
            left,
        })
    } else {
        QueryItem::Qoperand(read_query_operand(q, i))
    }
}

/// Decode the `QueryOperand` at `QueryItem` array index `i`.
#[inline]
fn read_query_operand(q: &[u8], i: usize) -> QueryOperand {
    let off = HDRSIZETQ + i * QUERYITEM_SIZE;
    // `valcrc` is an int32 at byte offset 4 (offset 3 is padding after `prefix`).
    QueryOperand {
        type_: q[off] as i8,
        weight: q[off + 1],
        prefix: q[off + 2] != 0,
        valcrc: i32::from_ne_bytes([q[off + 4], q[off + 5], q[off + 6], q[off + 7]]),
        len_dist: u32::from_ne_bytes([q[off + 8], q[off + 9], q[off + 10], q[off + 11]]),
    }
}

/// `GETOPERAND(query)` — byte offset of the operand C-string storage.
#[inline]
fn tsq_operand_off(q: &[u8]) -> usize {
    HDRSIZETQ + (tsq_size(q) as usize) * QUERYITEM_SIZE
}

/// `GETOPERAND(query) + val->distance` for `val->length` bytes.
#[inline]
fn tsq_operand_str<'a>(q: &'a [u8], val: &QueryOperand) -> &'a [u8] {
    let base = tsq_operand_off(q) + val.distance() as usize;
    &q[base..base + val.length() as usize]
}

/// C `ereport(ERROR, "unrecognized operator: %d", oper)` analog, raised only for
/// a corrupt datum carrying a non-operand item where one was expected.
fn unrecognized_item() -> PgError {
    PgError::error("unrecognized tsquery item")
}

// ---------------------------------------------------------------------------
// cstring_to_text_with_len — build a 4-byte-header varlena `text` Datum image.
// ---------------------------------------------------------------------------

/// C: `SET_VARSIZE(item, size)` — store `(size << 2)` on little-endian; on
/// big-endian the high bit is the 4B-uncompressed flag (0).
#[inline]
fn set_varsize(item: &mut [u8], size: u32) {
    let w = if cfg!(target_endian = "big") {
        size.to_ne_bytes()
    } else {
        (size << 2).to_ne_bytes()
    };
    item[0..4].copy_from_slice(&w);
}

/// `cstring_to_text_with_len(s, len)` — a `text` varlena (`VARHDRSZ + len`
/// bytes) whose payload is `s`, allocated in `mcx`.
#[inline]
fn cstring_to_text_with_len<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let len = s.len();
    let total = VARHDRSZ + len;
    let mut item: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, total)?;
    item.resize(total, 0u8);
    set_varsize(&mut item, total as u32);
    item[VARHDRSZ..total].copy_from_slice(s);
    Ok(item)
}

// ---------------------------------------------------------------------------
// Entry points (tsginidx.c)
// ---------------------------------------------------------------------------

/// `gin_cmp_tslexeme` (tsginidx.c:23) — GIN `compare`: compare two lexeme
/// `text` payloads (header-stripped). C:
/// `tsCompareString(VARDATA_ANY(a), .., VARDATA_ANY(b), .., false)`.
pub fn gin_cmp_tslexeme(a: &[u8], b: &[u8]) -> i32 {
    tsop::ts_compare_string::call(a, b, false)
}

/// `gin_cmp_prefix` (tsginidx.c:39) — GIN `comparePartial`: prefix compare with
/// the "prevent continue scan" fix-up. The `strategy`/`extra_data` args are
/// `#ifdef NOT_USED` in C and omitted.
pub fn gin_cmp_prefix(a: &[u8], b: &[u8]) -> i32 {
    let mut cmp = tsop::ts_compare_string::call(a, b, true);
    if cmp < 0 {
        cmp = 1; // prevent continue scan
    }
    cmp
}

/// `gin_extract_tsvector` (tsginidx.c:63) — GIN `extractValue`: one `text` key
/// per lexeme of the indexed tsvector `vector`. Returns the entry `text`
/// varlenas (`*nentries == entries.len() == vector->size`), allocated in `mcx`.
pub fn gin_extract_tsvector<'mcx>(
    mcx: Mcx<'mcx>,
    vector: &[u8],
) -> PgResult<PgVec<'mcx, PgVec<'mcx, u8>>> {
    let size = tsv_size(vector);
    let mut entries: PgVec<'mcx, PgVec<'mcx, u8>> =
        vec_with_capacity_in(mcx, size.max(0) as usize)?;
    if size > 0 {
        for i in 0..size as usize {
            let txt = cstring_to_text_with_len(mcx, tsv_lexeme(vector, i))?;
            entries.push(txt);
        }
    }
    Ok(entries)
}

/// The result of `gin_extract_tsquery` (the C `Datum *entries` return plus the
/// `*nentries`, `*ptr_partialmatch`, `*extra_data`, and `*searchMode`
/// out-parameters), all allocated in `mcx`.
pub struct GinTsqueryExtraction<'mcx> {
    /// the operand `text` keys (length is `*nentries`, the # of `QI_VAL` items)
    pub entries: PgVec<'mcx, PgVec<'mcx, u8>>,
    /// `*ptr_partialmatch` — per-entry prefix-search flags (`val->prefix`)
    pub partialmatch: PgVec<'mcx, bool>,
    /// `map_item_operand` — `query->size`-long map from each `QueryItem` index
    /// to its operand (entry) number (the C `extra_data[0]`)
    pub map_item_operand: PgVec<'mcx, i32>,
    /// `*searchMode` — `GIN_SEARCH_MODE_DEFAULT`/`_ALL`; `None` when
    /// `query->size == 0` (C leaves `*searchMode` untouched)
    pub search_mode: Option<i32>,
}

/// `gin_extract_tsquery` (tsginidx.c:93) — GIN `extractQuery`: extract the
/// operand `text` keys (one per `QI_VAL`), partial-match flags, the
/// item->operand map, and the search mode.
pub fn gin_extract_tsquery<'mcx>(
    mcx: Mcx<'mcx>,
    query: &[u8],
) -> PgResult<GinTsqueryExtraction<'mcx>> {
    let mut out = GinTsqueryExtraction {
        entries: vec_with_capacity_in(mcx, 0)?,
        partialmatch: vec_with_capacity_in(mcx, 0)?,
        map_item_operand: vec_with_capacity_in(mcx, 0)?,
        // *nentries = 0; *searchMode untouched until query->size > 0
        search_mode: None,
    };

    if tsq_size(query) > 0 {
        let item = getquery(mcx, query)?;

        // If the query doesn't have any required positive matches (e.g. '! foo')
        // we have to do a full index scan.
        if tsop::tsquery_requires_match::call(&item)? {
            out.search_mode = Some(GIN_SEARCH_MODE_DEFAULT);
        } else {
            out.search_mode = Some(GIN_SEARCH_MODE_ALL);
        }

        // count number of VAL items
        let mut j: usize = 0;
        for it in item.iter() {
            if it.item_type() == QI_VAL {
                j += 1;
            }
        }
        // *nentries = j;

        let mut entries: PgVec<'mcx, PgVec<'mcx, u8>> = vec_with_capacity_in(mcx, j)?;
        let mut partialmatch: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, j)?;

        // Make map to convert item's number to operand's (entry's) number.
        // map_item_operand = palloc0(sizeof(int) * query->size);
        let qsize = tsq_size(query) as usize;
        let mut map_item_operand: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, qsize)?;
        map_item_operand.resize(qsize, 0i32);

        // Now rescan the VAL items and fill in the arrays.
        let mut j2: usize = 0;
        for (i, it) in item.iter().enumerate() {
            if it.item_type() == QI_VAL {
                let val = match it {
                    QueryItem::Qoperand(o) => *o,
                    _ => return Err(unrecognized_item()),
                };
                let txt = cstring_to_text_with_len(mcx, tsq_operand_str(query, &val))?;
                entries.push(txt);
                partialmatch.push(val.prefix);
                map_item_operand[i] = j2 as i32;
                j2 += 1;
            }
        }

        out.entries = entries;
        out.partialmatch = partialmatch;
        out.map_item_operand = map_item_operand;
    }

    Ok(out)
}

/// `GinChkVal` (tsginidx.c:172) — `checkcondition_gin` closure state: the GIN
/// per-entry `check` array and the `map_item_operand` map (the `first_item`
/// base is the implicit item-index argument to the callback).
struct GinChkVal<'a> {
    /// C: `GinTernaryValue *check` — one entry per operand.
    check: &'a [GinTernaryValue],
    /// C: `int *map_item_operand` — item index -> operand (entry) number.
    map_item_operand: &'a [i32],
}

/// `checkcondition_gin` (tsginidx.c:182) — `TS_execute` callback matching a
/// tsquery operand to GIN index data. `item_index` is the operand's position in
/// the query's `QueryItem` array (C: `(QueryItem *) val - gcv->first_item`).
fn checkcondition_gin(
    gcv: &GinChkVal<'_>,
    item_index: usize,
    val: &QueryOperand,
    data: Option<&mut ExecPhraseData>,
) -> TSTernaryValue {
    // convert item's number to corresponding entry's (operand's) number
    let j = gcv.map_item_operand[item_index] as usize;

    // determine presence of current entry in indexed value
    let mut result: GinTernaryValue = gcv.check[j];

    // If any val requiring a weight is used or caller needs position information
    // then we must recheck, so replace TRUE with MAYBE.
    if result == GIN_TRUE && (val.weight != 0 || data.is_some()) {
        result = GIN_MAYBE;
    }

    // GinTernaryValue and TSTernaryValue use equivalent value assignments
    // (GIN_FALSE/TRUE/MAYBE == TS_NO/YES/MAYBE == 0/1/2).
    match result {
        GIN_FALSE => TSTernaryValue::TS_NO,
        GIN_TRUE => TSTernaryValue::TS_YES,
        _ => TSTernaryValue::TS_MAYBE,
    }
}

/// `gin_tsquery_consistent` (tsginidx.c:213) — GIN `consistent`: evaluate the
/// query tree against the boolean GIN check array `check`. `extra_data0` is the
/// `map_item_operand` array (C: `extra_data[0]`). Returns `(res, recheck)`.
///
/// C's `gcv.check = (GinTernaryValue *) check` reinterprets the `bool[]` as
/// `0`/`1` (`GIN_FALSE`/`GIN_TRUE`); reproduced by mapping each bool.
pub fn gin_tsquery_consistent(
    mcx: Mcx<'_>,
    check: &[bool],
    query: &[u8],
    extra_data0: &[i32],
) -> PgResult<(bool, bool)> {
    let mut res = false;
    // Initially assume query doesn't require recheck
    let mut recheck = false;

    if tsq_size(query) > 0 {
        // gcv.check = (GinTernaryValue *) check;
        let mut check_tri: PgVec<'_, GinTernaryValue> = vec_with_capacity_in(mcx, check.len())?;
        for &b in check {
            check_tri.push(if b { GIN_TRUE } else { GIN_FALSE });
        }

        let items = getquery(mcx, query)?;

        let outcome = {
            let gcv = GinChkVal {
                check: &check_tri[..],
                map_item_operand: extra_data0,
            };
            let mut cb = |item_index: usize,
                          val: &QueryOperand,
                          data: Option<&mut ExecPhraseData>|
             -> TSTernaryValue { checkcondition_gin(&gcv, item_index, val, data) };

            tsop::ts_execute_ternary::call(mcx, &items, TS_EXEC_PHRASE_NO_POS, &mut cb)?
        };

        match outcome {
            TSTernaryValue::TS_NO => res = false,
            TSTernaryValue::TS_YES => res = true,
            TSTernaryValue::TS_MAYBE => {
                res = true;
                recheck = true;
            }
        }
    }

    Ok((res, recheck))
}

/// `gin_tsquery_triconsistent` (tsginidx.c:262) — GIN `triConsistent`: evaluate
/// the query tree against the ternary GIN check array `check`. `extra_data0` is
/// the `map_item_operand` array. Returns the [`GinTernaryValue`] result.
pub fn gin_tsquery_triconsistent(
    mcx: Mcx<'_>,
    check: &[GinTernaryValue],
    query: &[u8],
    extra_data0: &[i32],
) -> PgResult<GinTernaryValue> {
    let mut res: GinTernaryValue = GIN_FALSE;

    if tsq_size(query) > 0 {
        let items = getquery(mcx, query)?;

        let outcome = {
            let gcv = GinChkVal {
                check,
                map_item_operand: extra_data0,
            };
            let mut cb = |item_index: usize,
                          val: &QueryOperand,
                          data: Option<&mut ExecPhraseData>|
             -> TSTernaryValue { checkcondition_gin(&gcv, item_index, val, data) };

            // GinTernaryValue and TSTernaryValue use equivalent value assignments.
            tsop::ts_execute_ternary::call(mcx, &items, TS_EXEC_PHRASE_NO_POS, &mut cb)?
        };

        res = match outcome {
            TSTernaryValue::TS_NO => GIN_FALSE,
            TSTernaryValue::TS_YES => GIN_TRUE,
            TSTernaryValue::TS_MAYBE => GIN_MAYBE,
        };
    }

    Ok(res)
}

// ---------------------------------------------------------------------------
// Backward-compatibility stubs (tsginidx.c:295-353)
//
// In C each stub asserts a minimum PG_NARGS() ("should not happen") and then
// tail-calls its primary function with the same fcinfo. At this layer there is
// no fcinfo to forward, so each delegates to the corresponding primary.
// ---------------------------------------------------------------------------

/// `gin_extract_tsvector_2args` (tsginidx.c:303).
pub fn gin_extract_tsvector_2args<'mcx>(
    mcx: Mcx<'mcx>,
    vector: &[u8],
) -> PgResult<PgVec<'mcx, PgVec<'mcx, u8>>> {
    gin_extract_tsvector(mcx, vector)
}

/// `gin_extract_tsquery_5args` (tsginidx.c:315).
pub fn gin_extract_tsquery_5args<'mcx>(
    mcx: Mcx<'mcx>,
    query: &[u8],
) -> PgResult<GinTsqueryExtraction<'mcx>> {
    gin_extract_tsquery(mcx, query)
}

/// `gin_tsquery_consistent_6args` (tsginidx.c:327).
pub fn gin_tsquery_consistent_6args(
    mcx: Mcx<'_>,
    check: &[bool],
    query: &[u8],
    extra_data0: &[i32],
) -> PgResult<(bool, bool)> {
    gin_tsquery_consistent(mcx, check, query, extra_data0)
}

/// `gin_extract_tsquery_oldsig` (tsginidx.c:339).
pub fn gin_extract_tsquery_oldsig<'mcx>(
    mcx: Mcx<'mcx>,
    query: &[u8],
) -> PgResult<GinTsqueryExtraction<'mcx>> {
    gin_extract_tsquery(mcx, query)
}

/// `gin_tsquery_consistent_oldsig` (tsginidx.c:349).
pub fn gin_tsquery_consistent_oldsig(
    mcx: Mcx<'_>,
    check: &[bool],
    query: &[u8],
    extra_data0: &[i32],
) -> PgResult<(bool, bool)> {
    gin_tsquery_consistent(mcx, check, query, extra_data0)
}

/// Surface the C `elog(ERROR, "%s requires %d arguments")` arg-count guard for
/// callers that model the fmgr `PG_NARGS()` checks (the stubs above model the
/// post-dispatch tail-call, so this is not invoked by them).
pub fn elog_requires_args(name: &str, n: usize) -> PgError {
    PgError::error(format!("{name} requires {n} arguments"))
}
