//! Port of `src/backend/utils/adt/tsvector_op.c` — the operations over
//! `tsvector`: the comparison family, the manipulation functions
//! (strip/setweight/delete/filter/concat/unnest), the array conversions, the
//! `TS_execute` query-evaluation engine, the `@@` match operators, the
//! `ts_stat` statistics aggregator and the `tsvector_update_trigger`.
//!
//! The on-disk layouts are the `tsvector` / `tsquery` varlena images: this
//! module works directly on the detoasted datum bytes through the [`crate::access`]
//! helpers. The deterministic portions — the comparison / manipulation cores,
//! the entire `TS_execute` / `TS_phrase_execute` engine, the `@@` operators and
//! the `ts_stat` tree — are ported 1:1. Only the genuinely-external boundaries
//! (interrupt/stack guard, array element I/O, SRF emission, SPI cursor, config
//! resolution, the trigger pipeline, `pg_mblen`) cross a seam.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::Mcx;
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_ZERO_LENGTH_CHARACTER_STRING, ERROR,
};
use tsearch::tsearch::{
    CheckCondition, ExecPhraseData, QueryItem, QueryOperand, QueryOperator, TSTernaryValue,
    WordEntry, WordEntryPos, LIMITPOS, MAXENTRYPOS, MAXNUMPOS, MAXSTRPOS, OP_AND, OP_NOT, OP_OR,
    OP_PHRASE, QI_VAL, TS_EXEC_EMPTY, TS_EXEC_PHRASE_NO_POS, TS_EXEC_SKIP_NOT, WEP_GETPOS,
    WEP_GETWEIGHT, WEP_SETPOS, WEP_SETWEIGHT,
};

use utils_error::ereport;

use postgres_seams as tcop;
use array_more_seams as arr;
use tsvector_ext_seams as ext;
use mbutils_seams as mb;

use ext::{ArrayElem, SrfRow, TupleSource};

use types_ri_triggers::TriggerDataRef;

use spi::{SPI_fnumber, SPI_getbinval, SPI_gettypeid, SPI_ERROR_NOATTRIBUTE};
use coerce::IsBinaryCoercible;
use parse::ts_parse::{parsetext, ParsedText};
use detoast::pg_detoast_datum_packed;

use crate::access::{
    arrptr, lexeme, posdatalen, posdataptr, posvecptr_off, set_arrptr, set_tsv_size, set_varsize,
    shortalign, strptr_off, tsv_size, varsize, SIZEOF_NPOS, SIZEOF_WEP, SIZEOF_WORDENTRY, VARHDRSZ,
};

extern crate alloc;

use TSTernaryValue::{TS_MAYBE, TS_NO, TS_YES};

/// `DATAHDRSIZE` (`offsetof(TSVectorData, entries)`).
const DATAHDRSIZE: usize = tsearch::tsearch::DATAHDRSIZE;

/// `TSVECTOROID` (pg_type.dat).
const TSVECTOROID: u32 = 3614;
/// `REGCONFIGOID` (pg_type.dat).
const REGCONFIGOID: u32 = 3734;
/// `TEXTOID` (pg_type.dat).
const TEXTOID: u32 = 25;

// ===========================================================================
// crate-local carrier / scratch types (not shared)
// ===========================================================================

/// `CHKVAL` (tsvector_op.c) — the closure context for the `@@`-match lexeme
/// check: a borrowed `tsvector` datum plus the query's operand storage.
#[derive(Clone, Copy, Debug)]
pub struct ChkVal<'a> {
    /// the whole tsvector datum bytes.
    pub tsv: &'a [u8],
    /// `tsv->size` (cached).
    pub size: i32,
    /// the query operand C-string storage (`GETOPERAND(query)`).
    pub operand: &'a [u8],
}

/// `StatEntry` (tsvector_op.c) — a node of the `ts_stat` binary tree. Links are
/// arena indices into [`TSVectorStat::nodes`]; [`NO_NODE`] is the null link.
#[derive(Clone, Debug, Default)]
pub struct StatEntry {
    /// zero indicates we were already here while walking the tree
    pub ndoc: u32,
    /// total occurrences across all documents
    pub nentry: u32,
    pub left: usize,
    pub right: usize,
    pub lexeme: Vec<u8>,
}

/// The arena null link.
pub const NO_NODE: usize = usize::MAX;

/// `TSVectorStat` (tsvector_op.c) — the `ts_stat` accumulator state.
#[derive(Clone, Debug, Default)]
pub struct TSVectorStat {
    pub weight: i32,
    pub maxdepth: u32,
    pub stack: Vec<usize>,
    pub stackpos: u32,
    pub root: usize,
    pub nodes: Vec<StatEntry>,
}

/// One row of `tsvector_unnest`: `(lexeme, positions, weights)`.
pub struct UnnestRow {
    pub lexeme: Vec<u8>,
    /// `(positions, weights)`; `None` when the lexeme has no position data.
    pub posweights: Option<(Vec<i16>, Vec<u8>)>,
}

// ===========================================================================
// alloc-safety helpers
// ===========================================================================

fn out_of_memory() -> PgError {
    ereport(ERROR).errmsg("out of memory").into_error()
}

#[inline]
fn try_reserve<T>(v: &mut Vec<T>, n: usize) -> PgResult<()> {
    v.try_reserve(n).map_err(|_| out_of_memory())
}

/// A fresh zero-filled owned buffer of `len` bytes (palloc0 analog).
fn try_zeroed(len: usize) -> PgResult<Vec<u8>> {
    let mut v: Vec<u8> = Vec::new();
    v.try_reserve(len).map_err(|_| out_of_memory())?;
    v.resize(len, 0u8);
    Ok(v)
}

// ===========================================================================
// small ts_type.h helpers
// ===========================================================================

/// `CALCDATASIZE(nentries, lenstr)` (ts_type.h).
#[inline]
fn calcdatasize(nentries: i32, lenstr: usize) -> usize {
    DATAHDRSIZE + (nentries as usize) * SIZEOF_WORDENTRY + lenstr
}

/// `pg_cmp_s32` (common/int.h).
#[inline]
fn pg_cmp_s32(a: i32, b: i32) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

/// `compareWordEntryPos` (tsvector.c:36).
#[inline]
fn compareWordEntryPos(a: WordEntryPos, b: WordEntryPos) -> i32 {
    pg_cmp_s32(WEP_GETPOS(a) as i32, WEP_GETPOS(b) as i32)
}

/// `qunique` over a `WordEntryPos` slice.
fn qunique_wep(arr: &mut [WordEntryPos]) -> usize {
    let elements = arr.len();
    if elements <= 1 {
        return elements;
    }
    let mut j = 0usize;
    for i in 1..elements {
        if compareWordEntryPos(arr[i], arr[j]) != 0 {
            j += 1;
            if j != i {
                arr[j] = arr[i];
            }
        }
    }
    j + 1
}

/// `qunique` over an `i32` slice.
fn qunique_i32(arr: &mut [i32]) -> usize {
    let elements = arr.len();
    if elements <= 1 {
        return elements;
    }
    let mut j = 0usize;
    for i in 1..elements {
        if pg_cmp_s32(arr[i], arr[j]) != 0 {
            j += 1;
            if j != i {
                arr[j] = arr[i];
            }
        }
    }
    j + 1
}

// ===========================================================================
// comparison / manipulation cores
// ===========================================================================

/// `silly_cmp_tsvector` (tsvector_op.c:85) — raw byte comparison of two
/// tsvectors.
pub fn silly_cmp_tsvector(a: &[u8], b: &[u8]) -> i32 {
    let va = varsize(a);
    let vb = varsize(b);
    if va < vb {
        return -1;
    } else if va > vb {
        return 1;
    }
    let sa = tsv_size(a);
    let sb = tsv_size(b);
    if sa < sb {
        return -1;
    } else if sa > sb {
        return 1;
    }

    for i in 0..sa {
        let aptr = arrptr(a, i as usize);
        let bptr = arrptr(b, i as usize);

        if aptr.haspos() != bptr.haspos() {
            return if aptr.haspos() > bptr.haspos() { -1 } else { 1 };
        }
        let res = tsCompareString(lexeme(a, sa, aptr), lexeme(b, sb, bptr), false);
        if res != 0 {
            return res;
        } else if aptr.haspos() != 0 {
            let la = posdatalen(a, sa, aptr);
            let lb = posdatalen(b, sb, bptr);
            if la != lb {
                return if la > lb { -1 } else { 1 };
            }
            for j in 0..la as usize {
                let ap = posdataptr(a, sa, aptr, j);
                let bp = posdataptr(b, sb, bptr, j);
                if WEP_GETPOS(ap) != WEP_GETPOS(bp) {
                    return if WEP_GETPOS(ap) > WEP_GETPOS(bp) { -1 } else { 1 };
                } else if WEP_GETWEIGHT(ap) != WEP_GETWEIGHT(bp) {
                    return if WEP_GETWEIGHT(ap) > WEP_GETWEIGHT(bp) { -1 } else { 1 };
                }
            }
        }
    }

    0
}

/// `tsvector_lt` (`TSVECTORCMPFUNC(lt, <, BOOL)`).
pub fn tsvector_lt(a: &[u8], b: &[u8]) -> bool {
    silly_cmp_tsvector(a, b) < 0
}
/// `tsvector_le`.
pub fn tsvector_le(a: &[u8], b: &[u8]) -> bool {
    silly_cmp_tsvector(a, b) <= 0
}
/// `tsvector_eq`.
pub fn tsvector_eq(a: &[u8], b: &[u8]) -> bool {
    silly_cmp_tsvector(a, b) == 0
}
/// `tsvector_ge`.
pub fn tsvector_ge(a: &[u8], b: &[u8]) -> bool {
    silly_cmp_tsvector(a, b) >= 0
}
/// `tsvector_gt`.
pub fn tsvector_gt(a: &[u8], b: &[u8]) -> bool {
    silly_cmp_tsvector(a, b) > 0
}
/// `tsvector_ne`.
pub fn tsvector_ne(a: &[u8], b: &[u8]) -> bool {
    silly_cmp_tsvector(a, b) != 0
}
/// `tsvector_cmp`.
pub fn tsvector_cmp(a: &[u8], b: &[u8]) -> i32 {
    silly_cmp_tsvector(a, b)
}

/// `tsvector_strip` (tsvector_op.c:167) — drop all position/weight data.
pub fn tsvector_strip(input: &[u8]) -> PgResult<Vec<u8>> {
    let size = tsv_size(input);
    let mut datalen = 0usize;
    for i in 0..size {
        datalen += arrptr(input, i as usize).len() as usize;
    }

    let len = calcdatasize(size, datalen);
    let mut out = try_zeroed(len)?;
    set_varsize(&mut out, len);
    set_tsv_size(&mut out, size);

    let in_str = strptr_off(size);
    let out_str = strptr_off(size);
    let mut cur = 0usize;
    for i in 0..size {
        let ein = arrptr(input, i as usize);
        let lex = &input[in_str + ein.pos() as usize..in_str + ein.pos() as usize + ein.len() as usize];
        out[out_str + cur..out_str + cur + ein.len() as usize].copy_from_slice(lex);
        let mut eout = WordEntry::default();
        eout.set_haspos(0);
        eout.set_len(ein.len());
        eout.set_pos(cur as u32);
        set_arrptr(&mut out, i as usize, eout);
        cur += ein.len() as usize;
    }

    Ok(out)
}

/// `tsvector_length` (tsvector_op.c:200) — number of lexemes.
pub fn tsvector_length(input: &[u8]) -> i32 {
    tsv_size(input)
}

/// Map a weight character to its 0..3 weight code.
fn weight_char_to_code(cw: i8) -> Option<i32> {
    match cw as u8 {
        b'A' | b'a' => Some(3),
        b'B' | b'b' => Some(2),
        b'C' | b'c' => Some(1),
        b'D' | b'd' => Some(0),
        _ => None,
    }
}

/// C: `elog(ERROR, "unrecognized weight: %d", cw)`.
fn unrecognized_weight_internal(cw: i8) -> PgError {
    ereport(ERROR)
        .errmsg(format!("unrecognized weight: {}", cw as i32))
        .into_error()
}

/// `tsvector_setweight` (tsvector_op.c:210) — set every position's weight.
pub fn tsvector_setweight(input: &[u8], cw: i8) -> PgResult<Vec<u8>> {
    let w = match weight_char_to_code(cw) {
        Some(w) => w as u16,
        None => return Err(unrecognized_weight_internal(cw)),
    };

    let mut out: Vec<u8> = Vec::new();
    out.try_reserve(input.len()).map_err(|_| out_of_memory())?;
    out.extend_from_slice(input);
    let size = tsv_size(&out);
    for idx in 0..size {
        let entry = arrptr(&out, idx as usize);
        let n = posdatalen(&out, size, entry);
        if n != 0 {
            let base = posvecptr_off(size, entry) + SIZEOF_NPOS;
            for j in 0..n as usize {
                let off = base + j * SIZEOF_WEP;
                let mut p = u16::from_ne_bytes([out[off], out[off + 1]]);
                WEP_SETWEIGHT(&mut p, w);
                out[off..off + 2].copy_from_slice(&p.to_ne_bytes());
            }
        }
    }
    Ok(out)
}

/// `tsvector_setweight_by_filter` (tsvector_op.c:272) — set weight only for the
/// lexemes named in a `text[]` (passed pre-deconstructed).
pub fn tsvector_setweight_by_filter(
    tsin: &[u8],
    char_weight: i8,
    lexemes: &[ArrayElem],
) -> PgResult<Vec<u8>> {
    let weight = match weight_char_to_code(char_weight) {
        Some(w) => w as u16,
        None => return Err(unrecognized_weight_internal(char_weight)),
    };

    let mut out: Vec<u8> = Vec::new();
    out.try_reserve(tsin.len()).map_err(|_| out_of_memory())?;
    out.extend_from_slice(tsin);
    let size = tsv_size(&out);

    for elem in lexemes {
        if elem.is_null {
            continue;
        }
        let lex_pos = tsvector_bsearch(&out, &elem.value);
        if lex_pos < 0 {
            continue;
        }
        let entry = arrptr(&out, lex_pos as usize);
        let n = posdatalen(&out, size, entry);
        if n != 0 {
            let base = posvecptr_off(size, entry) + SIZEOF_NPOS;
            for j in 0..n as usize {
                let off = base + j * SIZEOF_WEP;
                let mut p = u16::from_ne_bytes([out[off], out[off + 1]]);
                WEP_SETWEIGHT(&mut p, weight);
                out[off..off + 2].copy_from_slice(&p.to_ne_bytes());
            }
        }
    }
    Ok(out)
}

/// `tsvector_setweight_by_filter` datum entry (tsvector_op.c:315).
pub fn tsvector_setweight_by_filter_datum(
    tsin: &[u8],
    char_weight: i8,
    lexemes: &[u8],
) -> PgResult<Vec<u8>> {
    let elems = arr::deconstruct_text_array::call(lexemes)?;
    tsvector_setweight_by_filter(tsin, char_weight, &elems)
}

/// `compareEntry(pa, a, pb, b)` macro (tsvector_op.c:354).
#[inline]
fn compare_entry(pa: &[u8], a: WordEntry, pb: &[u8], b: WordEntry) -> i32 {
    tsCompareString(
        &pa[a.pos() as usize..a.pos() as usize + a.len() as usize],
        &pb[b.pos() as usize..b.pos() as usize + b.len() as usize],
        false,
    )
}

/// `add_pos` (tsvector_op.c:363) — append a source lexeme's positions to a
/// destination position vector, offsetting by `maxpos` and deduplicating
/// against `MAXNUMPOS`/`MAXENTRYPOS`. Returns the number added.
fn add_pos(
    src: &[u8],
    src_size: i32,
    srcptr: WordEntry,
    dest: &mut [u8],
    dest_posvec_off: usize,
    dest_haspos_before: bool,
    maxpos: i32,
) -> i32 {
    let read_clen =
        |dest: &[u8]| -> u16 { u16::from_ne_bytes([dest[dest_posvec_off], dest[dest_posvec_off + 1]]) };
    let write_clen = |dest: &mut [u8], v: u16| {
        dest[dest_posvec_off..dest_posvec_off + 2].copy_from_slice(&v.to_ne_bytes());
    };

    let slen = posdatalen(src, src_size, srcptr);
    let spos_base = posvecptr_off(src_size, srcptr) + SIZEOF_NPOS;
    let dpos_base = dest_posvec_off + SIZEOF_NPOS;

    if !dest_haspos_before {
        write_clen(dest, 0);
    }

    let startlen = read_clen(dest);
    let mut clen = startlen;
    let mut i = 0u16;
    while (i as i32) < slen as i32
        && (clen as i32) < MAXNUMPOS
        && (clen == 0 || {
            let last_off = dpos_base + (clen as usize - 1) * SIZEOF_WEP;
            let last = u16::from_ne_bytes([dest[last_off], dest[last_off + 1]]);
            WEP_GETPOS(last) != (MAXENTRYPOS - 1)
        })
    {
        let sp_off = spos_base + i as usize * SIZEOF_WEP;
        let sp = u16::from_ne_bytes([src[sp_off], src[sp_off + 1]]);

        let dp_off = dpos_base + clen as usize * SIZEOF_WEP;
        let mut dp = u16::from_ne_bytes([dest[dp_off], dest[dp_off + 1]]);
        WEP_SETWEIGHT(&mut dp, WEP_GETWEIGHT(sp));
        WEP_SETPOS(&mut dp, LIMITPOS(WEP_GETPOS(sp) as i32 + maxpos) as u16);
        dest[dp_off..dp_off + 2].copy_from_slice(&dp.to_ne_bytes());

        clen += 1;
        i += 1;
    }

    write_clen(dest, clen);
    (clen - startlen) as i32
}

/// `tsvector_bsearch` (tsvector_op.c:399) — binary search for a lexeme; returns
/// the entry index or -1.
pub fn tsvector_bsearch(tsv: &[u8], lexeme_bytes: &[u8]) -> i32 {
    let size = tsv_size(tsv);
    let str_off = strptr_off(size);
    let mut stop_low = 0i32;
    let mut stop_high = size;

    while stop_low < stop_high {
        let stop_middle = (stop_low + stop_high) / 2;
        let e = arrptr(tsv, stop_middle as usize);
        let elex = &tsv[str_off + e.pos() as usize..str_off + e.pos() as usize + e.len() as usize];
        let cmp = tsCompareString(lexeme_bytes, elex, false);

        if cmp < 0 {
            stop_high = stop_middle;
        } else if cmp > 0 {
            stop_low = stop_middle + 1;
        } else {
            return stop_middle;
        }
    }
    -1
}

/// `tsvector_delete_by_indices` (tsvector_op.c:463).
pub fn tsvector_delete_by_indices(tsv: &[u8], indices_to_delete: &mut Vec<i32>) -> PgResult<Vec<u8>> {
    let in_size = tsv_size(tsv);
    let in_str = strptr_off(in_size);

    let mut indices_count = indices_to_delete.len();
    if indices_count > 1 {
        indices_to_delete.sort_unstable();
        indices_count = qunique_i32(indices_to_delete);
        indices_to_delete.truncate(indices_count);
    }

    // Overestimate tsout size; exact VARSIZE set at the end.
    let mut tsout = try_zeroed(varsize(tsv))?;
    let out_size = in_size - indices_count as i32;
    set_tsv_size(&mut tsout, out_size);

    let out_str = strptr_off(out_size);
    let mut curoff = 0usize;
    let mut j = 0usize;
    let mut k = 0usize;
    for i in 0..in_size {
        if k < indices_count && i == indices_to_delete[k] {
            k += 1;
            continue;
        }

        let ein = arrptr(tsv, i as usize);
        let lex_start = in_str + ein.pos() as usize;
        tsout[out_str + curoff..out_str + curoff + ein.len() as usize]
            .copy_from_slice(&tsv[lex_start..lex_start + ein.len() as usize]);

        let mut eout = WordEntry::default();
        eout.set_haspos(ein.haspos());
        eout.set_len(ein.len());
        eout.set_pos(curoff as u32);
        set_arrptr(&mut tsout, j, eout);
        curoff += ein.len() as usize;

        if ein.haspos() != 0 {
            let len = posdatalen(tsv, in_size, ein) as usize * SIZEOF_WEP + SIZEOF_NPOS;
            curoff = shortalign(curoff);
            let src_pos = in_str + shortalign(ein.pos() as usize + ein.len() as usize);
            tsout[out_str + curoff..out_str + curoff + len]
                .copy_from_slice(&tsv[src_pos..src_pos + len]);
            curoff += len;
        }

        j += 1;
    }
    debug_assert_eq!(k, indices_count);

    let new_size = calcdatasize(out_size, curoff);
    set_varsize(&mut tsout, new_size);
    tsout.truncate(new_size);
    Ok(tsout)
}

/// `tsvector_delete_str` (tsvector_op.c:553) — delete one lexeme by string.
pub fn tsvector_delete_str(tsin: &[u8], lexeme_bytes: &[u8]) -> PgResult<Vec<u8>> {
    let skip_index = tsvector_bsearch(tsin, lexeme_bytes);
    if skip_index == -1 {
        return Ok(tsin.to_vec());
    }
    let mut indices = Vec::new();
    try_reserve(&mut indices, 1)?;
    indices.push(skip_index);
    tsvector_delete_by_indices(tsin, &mut indices)
}

/// `tsvector_delete_arr` (tsvector_op.c:577).
pub fn tsvector_delete_arr(tsin: &[u8], lexemes: &[ArrayElem]) -> PgResult<Vec<u8>> {
    let mut skip_indices: Vec<i32> = Vec::new();
    try_reserve(&mut skip_indices, lexemes.len())?;
    for elem in lexemes {
        if elem.is_null {
            continue;
        }
        let lex_pos = tsvector_bsearch(tsin, &elem.value);
        if lex_pos >= 0 {
            skip_indices.push(lex_pos);
        }
    }
    tsvector_delete_by_indices(tsin, &mut skip_indices)
}

/// `tsvector_delete_arr` datum entry (tsvector_op.c:590).
pub fn tsvector_delete_arr_datum(tsin: &[u8], lexemes: &[u8]) -> PgResult<Vec<u8>> {
    let elems = arr::deconstruct_text_array::call(lexemes)?;
    tsvector_delete_arr(tsin, &elems)
}

/// `tsvector_unnest` (tsvector_op.c:631) — expand into
/// `(lexeme, positions int2[], weights "char"[])` rows.
pub fn tsvector_unnest(tsin: &[u8]) -> PgResult<Vec<UnnestRow>> {
    let size = tsv_size(tsin);
    let str_off = strptr_off(size);
    let mut rows = Vec::new();
    try_reserve(&mut rows, size.max(0) as usize)?;

    for i in 0..size {
        let e = arrptr(tsin, i as usize);
        let lex = tsin[str_off + e.pos() as usize..str_off + e.pos() as usize + e.len() as usize]
            .to_vec();

        if e.haspos() != 0 {
            let npos = posdatalen(tsin, size, e);
            let mut positions = Vec::new();
            try_reserve(&mut positions, npos as usize)?;
            let mut weights = Vec::new();
            try_reserve(&mut weights, npos as usize)?;
            for j in 0..npos as usize {
                let p = posdataptr(tsin, size, e, j);
                positions.push(WEP_GETPOS(p) as i16);
                let weight = b'D' - WEP_GETWEIGHT(p) as u8;
                weights.push(weight);
            }
            rows.push(UnnestRow {
                lexeme: lex,
                posweights: Some((positions, weights)),
            });
        } else {
            rows.push(UnnestRow {
                lexeme: lex,
                posweights: None,
            });
        }
    }

    Ok(rows)
}

/// Emit a [`tsvector_unnest`] result through the SRF seam.
pub fn tsvector_unnest_srf(tsin: &[u8]) -> PgResult<()> {
    for row in tsvector_unnest(tsin)? {
        let (positions, weights) = match &row.posweights {
            Some((p, w)) => {
                let mut wcols: Vec<Vec<u8>> = Vec::new();
                try_reserve(&mut wcols, w.len())?;
                for c in w {
                    let mut one = Vec::new();
                    try_reserve(&mut one, 1)?;
                    one.push(*c);
                    wcols.push(one);
                }
                (
                    Some(arr::construct_int2_array::call(p)?),
                    Some(arr::construct_text_array::call(&wcols)?),
                )
            }
            None => (None, None),
        };
        ext::srf_return_next::call(SrfRow {
            col0: row.lexeme,
            col1: positions,
            col2: weights,
        })?;
    }
    Ok(())
}

/// `tsvector_to_array` (tsvector_op.c:719) — the lexemes as a `text[]`.
pub fn tsvector_to_array(tsin: &[u8]) -> PgResult<Vec<u8>> {
    let size = tsv_size(tsin);
    let str_off = strptr_off(size);
    let mut elements: Vec<Vec<u8>> = Vec::new();
    try_reserve(&mut elements, size.max(0) as usize)?;
    for i in 0..size {
        let e = arrptr(tsin, i as usize);
        elements.push(
            tsin[str_off + e.pos() as usize..str_off + e.pos() as usize + e.len() as usize]
                .to_vec(),
        );
    }
    arr::construct_text_array::call(&elements)
}

/// `compare_text_lexemes` (tsvector_op.c:441).
fn compare_text_lexemes(a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    match tsCompareString(a, b, false) {
        x if x < 0 => core::cmp::Ordering::Less,
        0 => core::cmp::Ordering::Equal,
        _ => core::cmp::Ordering::Greater,
    }
}

/// `array_to_tsvector` (tsvector_op.c:746).
pub fn array_to_tsvector(elems: &[ArrayElem]) -> PgResult<Vec<u8>> {
    for e in elems {
        if e.is_null {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .errmsg("lexeme array may not contain nulls")
                .into_error());
        }
        if e.value.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_ZERO_LENGTH_CHARACTER_STRING)
                .errmsg("lexeme array may not contain empty strings")
                .into_error());
        }
    }

    let mut lexemes: Vec<&[u8]> = Vec::new();
    try_reserve(&mut lexemes, elems.len())?;
    for e in elems {
        lexemes.push(e.value.as_slice());
    }
    let nitems_in = lexemes.len();
    if nitems_in > 1 {
        lexemes.sort_by(|a, b| compare_text_lexemes(a, b));
        lexemes.dedup_by(|a, b| tsCompareString(a, b, false) == 0);
    }
    let nitems = lexemes.len() as i32;

    let mut datalen = 0usize;
    for lex in &lexemes {
        datalen += lex.len();
    }
    let tslen = calcdatasize(nitems, datalen);

    let mut tsout = try_zeroed(tslen)?;
    set_varsize(&mut tsout, tslen);
    set_tsv_size(&mut tsout, nitems);

    let str_off = strptr_off(nitems);
    let mut cur = 0usize;
    for (i, lex) in lexemes.iter().enumerate() {
        tsout[str_off + cur..str_off + cur + lex.len()].copy_from_slice(lex);
        let mut e = WordEntry::default();
        e.set_haspos(0);
        e.set_len(lex.len() as u32);
        e.set_pos(cur as u32);
        set_arrptr(&mut tsout, i, e);
        cur += lex.len();
    }

    Ok(tsout)
}

/// `array_to_tsvector` datum entry (tsvector_op.c:760).
pub fn array_to_tsvector_datum(v: &[u8]) -> PgResult<Vec<u8>> {
    let elems = arr::deconstruct_text_array::call(v)?;
    array_to_tsvector(&elems)
}

/// `tsvector_filter` (tsvector_op.c:818) — keep only lexemes having one of the
/// given weights (`weights` pre-deconstructed `"char"[]`).
pub fn tsvector_filter(tsin: &[u8], weights: &[ArrayElem]) -> PgResult<Vec<u8>> {
    let mut mask: u8 = 0;
    for elem in weights {
        if elem.is_null {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .errmsg("weight array may not contain nulls")
                .into_error());
        }
        let char_weight = *elem.value.first().unwrap_or(&0);
        match char_weight {
            b'A' | b'a' => mask |= 8,
            b'B' | b'b' => mask |= 4,
            b'C' | b'c' => mask |= 2,
            b'D' | b'd' => mask |= 1,
            _ => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("unrecognized weight: \"{}\"", char_weight as char))
                    .into_error());
            }
        }
    }

    let in_size = tsv_size(tsin);
    let in_str = strptr_off(in_size);

    let mut tsout = try_zeroed(varsize(tsin))?;
    set_tsv_size(&mut tsout, in_size);
    let out_str = strptr_off(in_size);

    let mut j = 0usize;
    let mut cur_pos = 0usize;
    for i in 0..in_size {
        let ein = arrptr(tsin, i as usize);
        if ein.haspos() == 0 {
            continue;
        }

        let posvin_npos = posdatalen(tsin, in_size, ein) as usize;
        let posvout_off = out_str + shortalign(cur_pos + ein.len() as usize);

        let mut npos = 0usize;
        for k in 0..posvin_npos {
            let p = posdataptr(tsin, in_size, ein, k);
            if mask & (1 << WEP_GETWEIGHT(p)) != 0 {
                let dst = posvout_off + SIZEOF_NPOS + npos * SIZEOF_WEP;
                tsout[dst..dst + 2].copy_from_slice(&p.to_ne_bytes());
                npos += 1;
            }
        }

        if npos == 0 {
            continue;
        }

        let mut eout = WordEntry::default();
        eout.set_haspos(1);
        eout.set_len(ein.len());
        eout.set_pos(cur_pos as u32);
        set_arrptr(&mut tsout, j, eout);

        let lex_start = in_str + ein.pos() as usize;
        tsout[out_str + cur_pos..out_str + cur_pos + ein.len() as usize]
            .copy_from_slice(&tsin[lex_start..lex_start + ein.len() as usize]);
        tsout[posvout_off..posvout_off + 2].copy_from_slice(&(npos as u16).to_ne_bytes());

        cur_pos += shortalign(ein.len() as usize);
        cur_pos += npos * SIZEOF_WEP + SIZEOF_NPOS;
        j += 1;
    }

    set_tsv_size(&mut tsout, j as i32);
    let new_str = strptr_off(j as i32);
    if new_str != out_str {
        tsout.copy_within(out_str..out_str + cur_pos, new_str);
    }
    let new_size = calcdatasize(j as i32, cur_pos);
    set_varsize(&mut tsout, new_size);
    tsout.truncate(new_size);

    Ok(tsout)
}

/// `tsvector_filter` datum entry (tsvector_op.c:836).
pub fn tsvector_filter_datum(tsin: &[u8], weights: &[u8]) -> PgResult<Vec<u8>> {
    let elems = arr::deconstruct_char_array::call(weights)?;
    tsvector_filter(tsin, &elems)
}

/// `tsvector_concat` (tsvector_op.c:924) — concatenate two tsvectors, merging
/// duplicate lexemes and shifting `in2`'s positions past `in1`'s max position.
pub fn tsvector_concat(in1: &[u8], in2: &[u8]) -> PgResult<Vec<u8>> {
    let size1 = tsv_size(in1);
    let size2 = tsv_size(in2);
    let str1 = strptr_off(size1);
    let str2 = strptr_off(size2);

    let mut maxpos = 0i32;
    for i in 0..size1 {
        let e = arrptr(in1, i as usize);
        let n = posdatalen(in1, size1, e);
        for j in 0..n as usize {
            let p = posdataptr(in1, size1, e, j);
            if WEP_GETPOS(p) as i32 > maxpos {
                maxpos = WEP_GETPOS(p) as i32;
            }
        }
    }

    let output_bytes = varsize(in1) + varsize(in2) + size1 as usize + size2 as usize;
    let mut out = try_zeroed(output_bytes)?;
    set_varsize(&mut out, output_bytes);

    let out_total = size1 + size2;
    set_tsv_size(&mut out, out_total);
    let out_str = strptr_off(out_total);

    let mut ptr = 0usize;
    let mut i1 = 0i32;
    let mut i2 = 0i32;
    let mut dataoff = 0usize;

    let copy_posvec =
        |out: &mut [u8], dataoff: &mut usize, src: &[u8], src_size: i32, e: WordEntry| {
            let n = posdatalen(src, src_size, e) as usize;
            let bytes = n * SIZEOF_WEP + SIZEOF_NPOS;
            *dataoff = shortalign(*dataoff);
            let src_off = posvecptr_off(src_size, e);
            out[out_str + *dataoff..out_str + *dataoff + bytes]
                .copy_from_slice(&src[src_off..src_off + bytes]);
            *dataoff += bytes;
        };

    while i1 < size1 && i2 < size2 {
        let p1 = arrptr(in1, i1 as usize);
        let p2 = arrptr(in2, i2 as usize);
        let cmp = compare_entry(&in1[str1..], p1, &in2[str2..], p2);

        if cmp < 0 {
            let mut e = WordEntry::default();
            e.set_haspos(p1.haspos());
            e.set_len(p1.len());
            let lex_off = str1 + p1.pos() as usize;
            out[out_str + dataoff..out_str + dataoff + p1.len() as usize]
                .copy_from_slice(&in1[lex_off..lex_off + p1.len() as usize]);
            e.set_pos(dataoff as u32);
            dataoff += p1.len() as usize;
            if e.haspos() != 0 {
                copy_posvec(&mut out, &mut dataoff, in1, size1, p1);
            }
            set_arrptr(&mut out, ptr, e);
            ptr += 1;
            i1 += 1;
        } else if cmp > 0 {
            let mut e = WordEntry::default();
            e.set_haspos(p2.haspos());
            e.set_len(p2.len());
            let lex_off = str2 + p2.pos() as usize;
            out[out_str + dataoff..out_str + dataoff + p2.len() as usize]
                .copy_from_slice(&in2[lex_off..lex_off + p2.len() as usize]);
            e.set_pos(dataoff as u32);
            dataoff += p2.len() as usize;
            if e.haspos() != 0 {
                let posvec_off = out_str + shortalign(dataoff);
                let addlen = add_pos(in2, size2, p2, &mut out, posvec_off, false, maxpos);
                if addlen == 0 {
                    e.set_haspos(0);
                } else {
                    dataoff = shortalign(dataoff);
                    dataoff += addlen as usize * SIZEOF_WEP + SIZEOF_NPOS;
                }
            }
            set_arrptr(&mut out, ptr, e);
            ptr += 1;
            i2 += 1;
        } else {
            let mut e = WordEntry::default();
            e.set_haspos(p1.haspos() | p2.haspos());
            e.set_len(p1.len());
            let lex_off = str1 + p1.pos() as usize;
            out[out_str + dataoff..out_str + dataoff + p1.len() as usize]
                .copy_from_slice(&in1[lex_off..lex_off + p1.len() as usize]);
            e.set_pos(dataoff as u32);
            dataoff += p1.len() as usize;
            if e.haspos() != 0 {
                if p1.haspos() != 0 {
                    let posvec_off = out_str + shortalign(dataoff);
                    copy_posvec(&mut out, &mut dataoff, in1, size1, p1);
                    if p2.haspos() != 0 {
                        let addlen = add_pos(in2, size2, p2, &mut out, posvec_off, true, maxpos);
                        dataoff += addlen as usize * SIZEOF_WEP;
                    }
                } else {
                    let posvec_off = out_str + shortalign(dataoff);
                    let addlen = add_pos(in2, size2, p2, &mut out, posvec_off, false, maxpos);
                    if addlen == 0 {
                        e.set_haspos(0);
                    } else {
                        dataoff = shortalign(dataoff);
                        dataoff += addlen as usize * SIZEOF_WEP + SIZEOF_NPOS;
                    }
                }
            }
            set_arrptr(&mut out, ptr, e);
            ptr += 1;
            i1 += 1;
            i2 += 1;
        }
    }

    while i1 < size1 {
        let p1 = arrptr(in1, i1 as usize);
        let mut e = WordEntry::default();
        e.set_haspos(p1.haspos());
        e.set_len(p1.len());
        let lex_off = str1 + p1.pos() as usize;
        out[out_str + dataoff..out_str + dataoff + p1.len() as usize]
            .copy_from_slice(&in1[lex_off..lex_off + p1.len() as usize]);
        e.set_pos(dataoff as u32);
        dataoff += p1.len() as usize;
        if e.haspos() != 0 {
            copy_posvec(&mut out, &mut dataoff, in1, size1, p1);
        }
        set_arrptr(&mut out, ptr, e);
        ptr += 1;
        i1 += 1;
    }

    while i2 < size2 {
        let p2 = arrptr(in2, i2 as usize);
        let mut e = WordEntry::default();
        e.set_haspos(p2.haspos());
        e.set_len(p2.len());
        let lex_off = str2 + p2.pos() as usize;
        out[out_str + dataoff..out_str + dataoff + p2.len() as usize]
            .copy_from_slice(&in2[lex_off..lex_off + p2.len() as usize]);
        e.set_pos(dataoff as u32);
        dataoff += p2.len() as usize;
        if e.haspos() != 0 {
            let posvec_off = out_str + shortalign(dataoff);
            let addlen = add_pos(in2, size2, p2, &mut out, posvec_off, false, maxpos);
            if addlen == 0 {
                e.set_haspos(0);
            } else {
                dataoff = shortalign(dataoff);
                dataoff += addlen as usize * SIZEOF_WEP + SIZEOF_NPOS;
            }
        }
        set_arrptr(&mut out, ptr, e);
        ptr += 1;
        i2 += 1;
    }

    if dataoff > MAXSTRPOS as usize {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "string is too long for tsvector ({} bytes, max {} bytes)",
                dataoff, MAXSTRPOS
            ))
            .into_error());
    }

    let output_size = ptr as i32;
    debug_assert!(output_size <= out_total);
    set_tsv_size(&mut out, output_size);
    let new_str = strptr_off(output_size);
    if new_str != out_str {
        out.copy_within(out_str..out_str + dataoff, new_str);
    }
    let output_bytes2 = calcdatasize(output_size, dataoff);
    debug_assert!(output_bytes2 <= varsize(&out));
    set_varsize(&mut out, output_bytes2);
    out.truncate(output_bytes2);

    Ok(out)
}

// ===========================================================================
// query execution engine
// ===========================================================================

/// `tsCompareString` (tsvector_op.c:1151) — compare two lexeme strings, with
/// optional prefix matching (returns 0 iff `b` has prefix `a`).
pub fn tsCompareString(a: &[u8], b: &[u8], prefix: bool) -> i32 {
    let lena = a.len();
    let lenb = b.len();
    let cmp;

    if lena == 0 {
        if prefix {
            cmp = 0;
        } else {
            cmp = if lenb > 0 { -1 } else { 0 };
        }
    } else if lenb == 0 {
        cmp = if lena > 0 { 1 } else { 0 };
    } else {
        let n = lena.min(lenb);
        let mut c = memcmp(&a[..n], &b[..n]);
        if prefix {
            if c == 0 && lena > lenb {
                c = 1;
            }
        } else if c == 0 && lena != lenb {
            c = if lena < lenb { -1 } else { 1 };
        }
        cmp = c;
    }

    cmp
}

#[inline]
fn memcmp(a: &[u8], b: &[u8]) -> i32 {
    match a.cmp(b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// Owned position storage produced by a match (the C `ExecPhraseData`, always
/// owning the positions in this port).
#[derive(Clone, Debug, Default)]
struct PhraseData {
    npos: i32,
    negate: bool,
    width: i32,
    pos: Vec<WordEntryPos>,
}

impl PhraseData {
    fn into_exec(self) -> ExecPhraseData {
        ExecPhraseData {
            npos: self.npos,
            allocated: false,
            negate: self.negate,
            pos: self.pos,
            width: self.width,
        }
    }
}

/// The `TSExecuteCallback` abstraction: check whether a primitive operand is
/// present, optionally returning its position list. `opidx` is the operand's
/// index in the `QueryItem` array (the repo's `CheckCondition` callback
/// identifies the operand by index rather than by the C `void *arg`).
trait TSExecuteCallback {
    fn chkcond(
        &mut self,
        opidx: usize,
        val: QueryOperand,
        data: Option<&mut PhraseData>,
    ) -> TSTernaryValue;
}

/// `checkclass_str` (tsvector_op.c:1188) — weight-class filter for a matched
/// entry's positions.
fn checkclass_str(
    chkval: &ChkVal,
    entry: WordEntry,
    val: QueryOperand,
    data: Option<&mut PhraseData>,
) -> TSTernaryValue {
    let mut result = TS_NO;
    debug_assert!(data.as_ref().map(|d| d.npos == 0).unwrap_or(true));

    if entry.haspos() != 0 {
        let npos = posdatalen(chkval.tsv, chkval.size, entry);
        let read_pos = |j: usize| -> WordEntryPos { posdataptr(chkval.tsv, chkval.size, entry, j) };

        match (val.weight != 0, data) {
            (true, Some(data)) => {
                data.pos.clear();
                for j in 0..npos as usize {
                    let p = read_pos(j);
                    if val.weight & (1 << WEP_GETWEIGHT(p)) != 0 {
                        data.pos.push(WEP_GETPOS(p));
                    }
                }
                data.npos = data.pos.len() as i32;
                if data.npos > 0 {
                    result = TS_YES;
                } else {
                    data.pos.clear();
                }
            }
            (true, None) => {
                for j in 0..npos as usize {
                    let p = read_pos(j);
                    if val.weight & (1 << WEP_GETWEIGHT(p)) != 0 {
                        result = TS_YES;
                        break;
                    }
                }
            }
            (false, Some(data)) => {
                data.pos.clear();
                for j in 0..npos as usize {
                    data.pos.push(read_pos(j));
                }
                data.npos = npos as i32;
                result = TS_YES;
            }
            (false, None) => {
                result = TS_YES;
            }
        }
    } else if data.is_some() {
        result = TS_MAYBE;
    } else {
        result = TS_YES;
    }

    result
}

/// `checkcondition_str` (tsvector_op.c:1294) — the `TSExecuteCallback` for
/// matching a query operand against plain tsvector data (`@@`).
fn checkcondition_str(
    chkval: &ChkVal,
    val: QueryOperand,
    mut data: Option<&mut PhraseData>,
) -> TSTernaryValue {
    let str_off = strptr_off(chkval.size);
    let mut stop_low = 0i32;
    let mut stop_high = chkval.size;
    let mut stop_middle = stop_high;
    let mut res = TS_NO;

    let opnd = &chkval.operand
        [val.distance() as usize..val.distance() as usize + val.length() as usize];

    while stop_low < stop_high {
        stop_middle = stop_low + (stop_high - stop_low) / 2;
        let e = arrptr(chkval.tsv, stop_middle as usize);
        let elex =
            &chkval.tsv[str_off + e.pos() as usize..str_off + e.pos() as usize + e.len() as usize];
        let difference = tsCompareString(opnd, elex, false);

        if difference == 0 {
            res = checkclass_str(chkval, e, val, data.as_deref_mut());
            break;
        } else if difference > 0 {
            stop_low = stop_middle + 1;
        } else {
            stop_high = stop_middle;
        }
    }

    if val.prefix && (res != TS_YES || data.is_some()) {
        let mut allpos: Vec<WordEntryPos> = Vec::new();

        if stop_low >= stop_high {
            stop_middle = stop_high;
        }

        if let Some(d) = data.as_deref_mut() {
            d.pos.clear();
            d.npos = 0;
        }
        res = TS_NO;

        while (res != TS_YES || data.is_some()) && stop_middle < chkval.size && {
            let e = arrptr(chkval.tsv, stop_middle as usize);
            let elex = &chkval.tsv
                [str_off + e.pos() as usize..str_off + e.pos() as usize + e.len() as usize];
            tsCompareString(opnd, elex, true) == 0
        } {
            let e = arrptr(chkval.tsv, stop_middle as usize);
            let subres = checkclass_str(chkval, e, val, data.as_deref_mut());

            if subres != TS_NO {
                if let Some(d) = data.as_deref_mut() {
                    if subres == TS_MAYBE {
                        res = TS_MAYBE;
                        allpos.clear();
                        break;
                    }
                    allpos.extend_from_slice(&d.pos);
                    d.pos.clear();
                    d.npos = 0;
                } else if subres == TS_YES || res == TS_NO {
                    res = subres;
                }
            }

            stop_middle += 1;
        }

        if let Some(d) = data {
            if !allpos.is_empty() {
                allpos.sort_unstable_by(|a, b| compareWordEntryPos(*a, *b).cmp(&0));
                let newlen = qunique_wep(&mut allpos);
                allpos.truncate(newlen);
                d.npos = allpos.len() as i32;
                d.pos = allpos;
                res = TS_YES;
            }
        }
    }

    res
}

/// `TSPO_L_ONLY` (tsvector_op.c:1463).
const TSPO_L_ONLY: i32 = 0x01;
/// `TSPO_R_ONLY`.
const TSPO_R_ONLY: i32 = 0x02;
/// `TSPO_BOTH`.
const TSPO_BOTH: i32 = 0x04;

/// `TS_phrase_output` (tsvector_op.c:1467) — merge two operands' position lists
/// under a phrase / boolean operator, per the `emit` bitmask.
fn TS_phrase_output(
    data: Option<&mut PhraseData>,
    ldata: &PhraseData,
    rdata: &PhraseData,
    emit: i32,
    loffset: i32,
    roffset: i32,
    _max_npos: i32,
) -> TSTernaryValue {
    let mut lindex = 0i32;
    let mut rindex = 0i32;
    let mut out: Vec<WordEntryPos> = Vec::new();
    let want_data = data.is_some();

    while lindex < ldata.npos || rindex < rdata.npos {
        let mut output_pos = 0i32;

        let lpos = if lindex < ldata.npos {
            WEP_GETPOS(ldata.pos[lindex as usize]) as i32 + loffset
        } else {
            if emit & TSPO_R_ONLY == 0 {
                break;
            }
            i32::MAX
        };
        let rpos = if rindex < rdata.npos {
            WEP_GETPOS(rdata.pos[rindex as usize]) as i32 + roffset
        } else {
            if emit & TSPO_L_ONLY == 0 {
                break;
            }
            i32::MAX
        };

        if lpos < rpos {
            if emit & TSPO_L_ONLY != 0 {
                output_pos = lpos;
            }
            lindex += 1;
        } else if lpos == rpos {
            if emit & TSPO_BOTH != 0 {
                output_pos = rpos;
            }
            lindex += 1;
            rindex += 1;
        } else {
            if emit & TSPO_R_ONLY != 0 {
                output_pos = rpos;
            }
            rindex += 1;
        }

        if output_pos > 0 {
            if want_data {
                out.push(output_pos as WordEntryPos);
            } else {
                return TS_YES;
            }
        }
    }

    if want_data && !out.is_empty() {
        let d = data.unwrap();
        d.npos = out.len() as i32;
        d.pos = out;
        return TS_YES;
    }
    TS_NO
}

/// `TS_phrase_execute` (tsvector_op.c:1608).
fn TS_phrase_execute(
    items: &[QueryItem],
    cur: usize,
    chkcond: &mut dyn TSExecuteCallback,
    flags: u32,
    mut data: Option<&mut PhraseData>,
) -> PgResult<TSTernaryValue> {
    tcop::check_stack_depth::call()?;
    tcop::check_for_interrupts::call()?;

    let item = &items[cur];
    if item.item_type() == QI_VAL {
        let val = query_operand(item);
        return Ok(chkcond.chkcond(cur, val, data));
    }

    let qop = query_operator(item);
    Ok(match qop.oper {
        OP_NOT => {
            if flags & TS_EXEC_SKIP_NOT != 0 {
                let d = data
                    .ok_or_else(|| PgError::error("TS_phrase_execute: data is NULL (OP_NOT)"))?;
                debug_assert!(d.npos == 0 && !d.negate);
                d.negate = true;
                return Ok(TS_YES);
            }
            match TS_phrase_execute(items, cur + 1, chkcond, flags, data.as_deref_mut())? {
                TS_NO => {
                    let d = data.ok_or_else(|| {
                        PgError::error("TS_phrase_execute: data is NULL (OP_NOT)")
                    })?;
                    debug_assert!(d.npos == 0 && !d.negate);
                    d.negate = true;
                    TS_YES
                }
                TS_YES => {
                    let d = data.ok_or_else(|| {
                        PgError::error("TS_phrase_execute: data is NULL (OP_NOT)")
                    })?;
                    if d.npos > 0 {
                        d.negate = !d.negate;
                        TS_YES
                    } else if d.negate {
                        d.negate = false;
                        TS_NO
                    } else {
                        debug_assert!(false);
                        TS_NO
                    }
                }
                TS_MAYBE => TS_MAYBE,
            }
        }

        OP_PHRASE | OP_AND => {
            let mut ldata = PhraseData::default();
            let mut rdata = PhraseData::default();

            let lmatch = TS_phrase_execute(
                items,
                (cur as i64 + qop.left as i64) as usize,
                chkcond,
                flags,
                Some(&mut ldata),
            )?;
            if lmatch == TS_NO {
                return Ok(TS_NO);
            }
            let rmatch = TS_phrase_execute(items, cur + 1, chkcond, flags, Some(&mut rdata))?;
            if rmatch == TS_NO {
                return Ok(TS_NO);
            }

            if lmatch == TS_MAYBE || rmatch == TS_MAYBE {
                return Ok(TS_MAYBE);
            }

            let loffset;
            let roffset;
            if qop.oper == OP_PHRASE {
                loffset = qop.distance as i32 + rdata.width;
                roffset = 0;
                if let Some(d) = data.as_deref_mut() {
                    d.width = qop.distance as i32 + ldata.width + rdata.width;
                }
            } else {
                let maxwidth = ldata.width.max(rdata.width);
                loffset = maxwidth - ldata.width;
                roffset = maxwidth - rdata.width;
                if let Some(d) = data.as_deref_mut() {
                    d.width = maxwidth;
                }
            }

            if ldata.negate && rdata.negate {
                let _ = TS_phrase_output(
                    data.as_deref_mut(),
                    &ldata,
                    &rdata,
                    TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                    loffset,
                    roffset,
                    ldata.npos + rdata.npos,
                );
                if let Some(d) = data {
                    d.negate = true;
                }
                TS_YES
            } else if ldata.negate {
                TS_phrase_output(data, &ldata, &rdata, TSPO_R_ONLY, loffset, roffset, rdata.npos)
            } else if rdata.negate {
                TS_phrase_output(data, &ldata, &rdata, TSPO_L_ONLY, loffset, roffset, ldata.npos)
            } else {
                TS_phrase_output(
                    data,
                    &ldata,
                    &rdata,
                    TSPO_BOTH,
                    loffset,
                    roffset,
                    ldata.npos.min(rdata.npos),
                )
            }
        }

        OP_OR => {
            let mut ldata = PhraseData::default();
            let mut rdata = PhraseData::default();

            let lmatch = TS_phrase_execute(
                items,
                (cur as i64 + qop.left as i64) as usize,
                chkcond,
                flags,
                Some(&mut ldata),
            )?;
            let rmatch = TS_phrase_execute(items, cur + 1, chkcond, flags, Some(&mut rdata))?;

            if lmatch == TS_NO && rmatch == TS_NO {
                return Ok(TS_NO);
            }
            if lmatch == TS_MAYBE || rmatch == TS_MAYBE {
                return Ok(TS_MAYBE);
            }

            if lmatch == TS_NO {
                ldata.width = 0;
            }
            if rmatch == TS_NO {
                rdata.width = 0;
            }

            let maxwidth = ldata.width.max(rdata.width);
            let loffset = maxwidth - ldata.width;
            let roffset = maxwidth - rdata.width;
            let data =
                data.ok_or_else(|| PgError::error("TS_phrase_execute: data is NULL (OP_OR)"))?;
            data.width = maxwidth;

            if ldata.negate && rdata.negate {
                let _ = TS_phrase_output(
                    Some(data),
                    &ldata,
                    &rdata,
                    TSPO_BOTH,
                    loffset,
                    roffset,
                    ldata.npos.min(rdata.npos),
                );
                data.negate = true;
                TS_YES
            } else if ldata.negate {
                let _ = TS_phrase_output(
                    Some(data),
                    &ldata,
                    &rdata,
                    TSPO_L_ONLY,
                    loffset,
                    roffset,
                    ldata.npos,
                );
                data.negate = true;
                TS_YES
            } else if rdata.negate {
                let _ = TS_phrase_output(
                    Some(data),
                    &ldata,
                    &rdata,
                    TSPO_R_ONLY,
                    loffset,
                    roffset,
                    rdata.npos,
                );
                data.negate = true;
                TS_YES
            } else {
                TS_phrase_output(
                    Some(data),
                    &ldata,
                    &rdata,
                    TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                    loffset,
                    roffset,
                    ldata.npos + rdata.npos,
                )
            }
        }

        other => return unreachable_operator(other),
    })
}

/// Read the `QueryOperand` of a `QI_VAL` [`QueryItem`].
#[inline]
fn query_operand(item: &QueryItem) -> QueryOperand {
    match item {
        QueryItem::Qoperand(v) => *v,
        QueryItem::Type_(t) => QueryOperand {
            type_: *t,
            ..QueryOperand::default()
        },
        QueryItem::Qoperator(o) => QueryOperand {
            type_: o.type_,
            ..QueryOperand::default()
        },
    }
}

/// Read the `QueryOperator` of a non-`QI_VAL` [`QueryItem`].
#[inline]
fn query_operator(item: &QueryItem) -> QueryOperator {
    match item {
        QueryItem::Qoperator(o) => *o,
        QueryItem::Type_(t) => QueryOperator {
            type_: *t,
            ..QueryOperator::default()
        },
        QueryItem::Qoperand(o) => QueryOperator {
            type_: o.type_,
            ..QueryOperator::default()
        },
    }
}

/// C `elog(ERROR, "unrecognized operator: %d", oper)`.
fn unreachable_operator<T>(oper: i8) -> PgResult<T> {
    Err(PgError::error(format!(
        "unrecognized operator: {}",
        oper as i32
    )))
}

/// `TS_execute_recurse` (tsvector_op.c:1882).
fn TS_execute_recurse(
    items: &[QueryItem],
    cur: usize,
    chkcond: &mut dyn TSExecuteCallback,
    flags: u32,
) -> PgResult<TSTernaryValue> {
    tcop::check_stack_depth::call()?;
    tcop::check_for_interrupts::call()?;

    let item = &items[cur];
    if item.item_type() == QI_VAL {
        let val = query_operand(item);
        return Ok(chkcond.chkcond(cur, val, None));
    }

    let qop = query_operator(item);
    Ok(match qop.oper {
        OP_NOT => {
            if flags & TS_EXEC_SKIP_NOT != 0 {
                return Ok(TS_YES);
            }
            match TS_execute_recurse(items, cur + 1, chkcond, flags)? {
                TS_NO => TS_YES,
                TS_YES => TS_NO,
                TS_MAYBE => TS_MAYBE,
            }
        }
        OP_AND => {
            let lmatch =
                TS_execute_recurse(items, (cur as i64 + qop.left as i64) as usize, chkcond, flags)?;
            if lmatch == TS_NO {
                return Ok(TS_NO);
            }
            match TS_execute_recurse(items, cur + 1, chkcond, flags)? {
                TS_NO => TS_NO,
                TS_YES => lmatch,
                TS_MAYBE => TS_MAYBE,
            }
        }
        OP_OR => {
            let lmatch =
                TS_execute_recurse(items, (cur as i64 + qop.left as i64) as usize, chkcond, flags)?;
            if lmatch == TS_YES {
                return Ok(TS_YES);
            }
            match TS_execute_recurse(items, cur + 1, chkcond, flags)? {
                TS_NO => lmatch,
                TS_YES => TS_YES,
                TS_MAYBE => TS_MAYBE,
            }
        }
        OP_PHRASE => match TS_phrase_execute(items, cur, chkcond, flags, None)? {
            TS_NO => TS_NO,
            TS_YES => TS_YES,
            TS_MAYBE => {
                if flags & TS_EXEC_PHRASE_NO_POS != 0 {
                    TS_MAYBE
                } else {
                    TS_NO
                }
            }
        },
        other => return unreachable_operator(other),
    })
}

/// Adapter over the public [`CheckCondition`] callback type.
struct CallbackAdapter<'a, 'b> {
    chkcond: &'a mut CheckCondition<'b>,
}
impl TSExecuteCallback for CallbackAdapter<'_, '_> {
    fn chkcond(
        &mut self,
        opidx: usize,
        val: QueryOperand,
        data: Option<&mut PhraseData>,
    ) -> TSTernaryValue {
        // The public callback type takes an &mut ExecPhraseData; bridge the
        // local PhraseData over it. `opidx` is the operand's QueryItem index,
        // by which the public callback identifies the operand.
        match data {
            Some(d) => {
                let mut exec = ExecPhraseData {
                    npos: d.npos,
                    allocated: false,
                    negate: d.negate,
                    pos: core::mem::take(&mut d.pos),
                    width: d.width,
                };
                let r = (self.chkcond)(opidx, &val, Some(&mut exec));
                d.npos = exec.npos;
                d.negate = exec.negate;
                d.width = exec.width;
                d.pos = exec.pos;
                r
            }
            None => (self.chkcond)(opidx, &val, None),
        }
    }
}

/// `TS_execute` (tsvector_op.c:1853) — boolean result over the `chkcond`
/// callback. Installed into the `ts_execute` seam.
pub fn ts_execute_seam<'mcx>(
    _mcx: Mcx<'mcx>,
    query_items: &[QueryItem],
    flags: u32,
    chkcond: &mut CheckCondition<'_>,
) -> PgResult<bool> {
    let mut cb = CallbackAdapter { chkcond };
    Ok(TS_execute_recurse(query_items, 0, &mut cb, flags)? != TS_NO)
}

/// `TS_execute_ternary` (tsvector_op.c:1870). Installed into the
/// `ts_execute_ternary` seam.
pub fn ts_execute_ternary_seam<'mcx>(
    _mcx: Mcx<'mcx>,
    query_items: &[QueryItem],
    flags: u32,
    chkcond: &mut CheckCondition<'_>,
) -> PgResult<TSTernaryValue> {
    let mut cb = CallbackAdapter { chkcond };
    TS_execute_recurse(query_items, 0, &mut cb, flags)
}

/// `TS_execute` (tsvector_op.c:1853) — public entry: evaluate `query` against
/// `vec`'s data (the `@@` semantics) via the plain-tsvector callback.
fn ts_execute_chkval(items: &[QueryItem], chkval: &ChkVal, flags: u32) -> PgResult<bool> {
    let mut cb = StrCallback { chkval };
    Ok(TS_execute_recurse(items, 0, &mut cb, flags)? != TS_NO)
}

struct StrCallback<'a, 'b> {
    chkval: &'b ChkVal<'a>,
}
impl TSExecuteCallback for StrCallback<'_, '_> {
    fn chkcond(
        &mut self,
        _opidx: usize,
        val: QueryOperand,
        data: Option<&mut PhraseData>,
    ) -> TSTernaryValue {
        checkcondition_str(self.chkval, val, data)
    }
}

/// `TS_execute_locations` (tsvector_op.c:2006).
pub fn TS_execute_locations(items: &[QueryItem], chkval: &ChkVal) -> PgResult<Vec<ExecPhraseData>> {
    let mut cb = StrCallback { chkval };
    debug_assert_eq!(0u32, TS_EXEC_EMPTY);
    let mut result = Vec::new();
    if TS_execute_locations_recurse(items, 0, &mut cb, &mut result)? {
        let mut out = Vec::new();
        try_reserve(&mut out, result.len())?;
        for d in result {
            out.push(d.into_exec());
        }
        Ok(out)
    } else {
        Ok(Vec::new())
    }
}

/// `TS_execute_locations_recurse` (tsvector_op.c:2024).
fn TS_execute_locations_recurse(
    items: &[QueryItem],
    cur: usize,
    chkcond: &mut dyn TSExecuteCallback,
    locations: &mut Vec<PhraseData>,
) -> PgResult<bool> {
    tcop::check_stack_depth::call()?;
    tcop::check_for_interrupts::call()?;

    locations.clear();

    let item = &items[cur];
    if item.item_type() == QI_VAL {
        let val = query_operand(item);
        let mut data = PhraseData::default();
        if chkcond.chkcond(cur, val, Some(&mut data)) == TS_YES {
            *locations = alloc_one(data)?;
            return Ok(true);
        }
        return Ok(false);
    }

    let qop = query_operator(item);
    Ok(match qop.oper {
        OP_NOT => {
            let mut llocations = Vec::new();
            if !TS_execute_locations_recurse(items, cur + 1, chkcond, &mut llocations)? {
                return Ok(true);
            }
            false
        }
        OP_AND => {
            let mut llocations = Vec::new();
            if !TS_execute_locations_recurse(
                items,
                (cur as i64 + qop.left as i64) as usize,
                chkcond,
                &mut llocations,
            )? {
                return Ok(false);
            }
            let mut rlocations = Vec::new();
            if !TS_execute_locations_recurse(items, cur + 1, chkcond, &mut rlocations)? {
                return Ok(false);
            }
            llocations.append(&mut rlocations);
            *locations = llocations;
            true
        }
        OP_OR => {
            let mut llocations = Vec::new();
            let lmatch = TS_execute_locations_recurse(
                items,
                (cur as i64 + qop.left as i64) as usize,
                chkcond,
                &mut llocations,
            )?;
            let mut rlocations = Vec::new();
            let rmatch = TS_execute_locations_recurse(items, cur + 1, chkcond, &mut rlocations)?;
            if lmatch || rmatch {
                if llocations.is_empty() {
                    *locations = rlocations;
                } else if rlocations.is_empty() {
                    *locations = llocations;
                } else {
                    let mut out = Vec::new();
                    for ldata in &llocations {
                        for rdata in &rlocations {
                            let mut data = PhraseData::default();
                            let _ = TS_phrase_output(
                                Some(&mut data),
                                ldata,
                                rdata,
                                TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                                0,
                                0,
                                ldata.npos + rdata.npos,
                            );
                            data.width = ldata.width.max(rdata.width);
                            out.push(data);
                        }
                    }
                    *locations = out;
                }
                return Ok(true);
            }
            false
        }
        OP_PHRASE => {
            let mut data = PhraseData::default();
            if TS_phrase_execute(items, cur, chkcond, TS_EXEC_EMPTY, Some(&mut data))? == TS_YES {
                if !data.negate {
                    *locations = alloc_one(data)?;
                }
                return Ok(true);
            }
            false
        }
        other => return unreachable_operator(other),
    })
}

#[inline]
fn alloc_one(data: PhraseData) -> PgResult<Vec<PhraseData>> {
    let mut v = Vec::new();
    try_reserve(&mut v, 1)?;
    v.push(data);
    Ok(v)
}

/// `tsquery_requires_match` (tsvector_op.c:2155). Installed into the seam.
pub fn tsquery_requires_match(items: &[QueryItem]) -> PgResult<bool> {
    tsquery_requires_match_at(items, 0)
}

fn tsquery_requires_match_at(items: &[QueryItem], cur: usize) -> PgResult<bool> {
    tcop::check_stack_depth::call()?;

    let item = &items[cur];
    if item.item_type() == QI_VAL {
        return Ok(true);
    }
    let qop = query_operator(item);
    Ok(match qop.oper {
        OP_NOT => false,
        OP_PHRASE | OP_AND => {
            if tsquery_requires_match_at(items, (cur as i64 + qop.left as i64) as usize)? {
                true
            } else {
                tsquery_requires_match_at(items, cur + 1)?
            }
        }
        OP_OR => {
            if tsquery_requires_match_at(items, (cur as i64 + qop.left as i64) as usize)? {
                tsquery_requires_match_at(items, cur + 1)?
            } else {
                false
            }
        }
        other => return unreachable_operator(other),
    })
}

// ===========================================================================
// @@ match operators
// ===========================================================================

/// `ts_match_vq` (tsvector_op.c:2213) — `tsvector @@ tsquery`.
pub fn ts_match_vq(vec: &[u8], query: &[u8]) -> PgResult<bool> {
    use ts_small::util::{get_operand, get_query, tsq_size};

    if tsq_size(query) == 0 {
        return Ok(false);
    }

    let chkval = ChkVal {
        tsv: vec,
        size: tsv_size(vec),
        operand: get_operand(query),
    };
    let items = get_query(query)?;
    ts_execute_chkval(&items, &chkval, TS_EXEC_EMPTY)
}

/// `ts_match_qv` (tsvector_op.c:2205) — `tsquery @@ tsvector` (swaps args).
pub fn ts_match_qv(query: &[u8], vec: &[u8]) -> PgResult<bool> {
    ts_match_vq(vec, query)
}

/// `ts_match_tt` (tsvector_op.c:2243) — `text @@ text`. The text->tsvector and
/// text->tsquery conversions live in the tsearch dictionary subsystem; the
/// caller supplies the already-converted datums and this is the post-conversion
/// body.
pub fn ts_match_tt(vector: &[u8], query: &[u8]) -> PgResult<bool> {
    ts_match_vq(vector, query)
}

/// `ts_match_tq` (tsvector_op.c:2265) — `text @@ tsquery`.
pub fn ts_match_tq(vector: &[u8], query: &[u8]) -> PgResult<bool> {
    ts_match_vq(vector, query)
}

// ===========================================================================
// ts_stat aggregator
// ===========================================================================

/// `check_weight` (tsvector_op.c:2294).
pub fn check_weight(txt: &[u8], txt_size: i32, wptr: WordEntry, weight: i8) -> i32 {
    let len = posdatalen(txt, txt_size, wptr);
    let mut num = 0i32;
    for j in 0..len as usize {
        let p = posdataptr(txt, txt_size, wptr, j);
        if (weight as i32) & (1 << WEP_GETWEIGHT(p)) != 0 {
            num += 1;
        }
    }
    num
}

/// `compareStatWord(a,e,t)` macro (tsvector_op.c:2310).
fn compare_stat_word(node_lexeme: &[u8], txt: &[u8], txt_size: i32, e: WordEntry) -> i32 {
    let str_off = strptr_off(txt_size);
    let elex = &txt[str_off + e.pos() as usize..str_off + e.pos() as usize + e.len() as usize];
    tsCompareString(node_lexeme, elex, false)
}

/// `insertStatEntry` (tsvector_op.c:2315).
pub fn insertStatEntry(stat: &mut TSVectorStat, txt: &[u8], txt_size: i32, off: u32) -> PgResult<()> {
    let we = arrptr(txt, off as usize);

    let n = if stat.weight == 0 {
        if we.haspos() != 0 {
            posdatalen(txt, txt_size, we) as i32
        } else {
            1
        }
    } else if we.haspos() != 0 {
        check_weight(txt, txt_size, we, stat.weight as i8)
    } else {
        0
    };

    if n == 0 {
        return Ok(());
    }

    let str_off = strptr_off(txt_size);
    let lex =
        txt[str_off + we.pos() as usize..str_off + we.pos() as usize + we.len() as usize].to_vec();

    let mut depth = 1u32;
    let mut link = stat.root;
    enum Slot {
        Root,
        Left(usize),
        Right(usize),
    }
    let mut parent_slot = Slot::Root;

    loop {
        if link == NO_NODE {
            if depth > stat.maxdepth {
                stat.maxdepth = depth;
            }
            stat.nodes.try_reserve(1).map_err(|_| out_of_memory())?;
            let new_idx = stat.nodes.len();
            stat.nodes.push(StatEntry {
                ndoc: 1,
                nentry: n as u32,
                left: NO_NODE,
                right: NO_NODE,
                lexeme: lex,
            });
            match parent_slot {
                Slot::Root => stat.root = new_idx,
                Slot::Left(p) => stat.nodes[p].left = new_idx,
                Slot::Right(p) => stat.nodes[p].right = new_idx,
            }
            return Ok(());
        }

        let res = compare_stat_word(&stat.nodes[link].lexeme, txt, txt_size, we);
        if res == 0 {
            if depth > stat.maxdepth {
                stat.maxdepth = depth;
            }
            stat.nodes[link].ndoc += 1;
            stat.nodes[link].nentry += n as u32;
            return Ok(());
        }
        depth += 1;
        if res < 0 {
            parent_slot = Slot::Left(link);
            link = stat.nodes[link].left;
        } else {
            parent_slot = Slot::Right(link);
            link = stat.nodes[link].right;
        }
    }
}

/// `chooseNextStatEntry` (tsvector_op.c:2380).
pub fn chooseNextStatEntry(
    stat: &mut TSVectorStat,
    txt: &[u8],
    txt_size: i32,
    low: u32,
    high: u32,
    offset: u32,
) -> PgResult<()> {
    let middle = (low + high) >> 1;

    let mut pos = (low + middle) >> 1;
    if low != middle && pos >= offset && (pos - offset) < txt_size as u32 {
        insertStatEntry(stat, txt, txt_size, pos - offset)?;
    }
    pos = (high + middle + 1) >> 1;
    if middle + 1 != high && pos >= offset && (pos - offset) < txt_size as u32 {
        insertStatEntry(stat, txt, txt_size, pos - offset)?;
    }

    if low != middle {
        chooseNextStatEntry(stat, txt, txt_size, low, middle, offset)?;
    }
    if high != middle + 1 {
        chooseNextStatEntry(stat, txt, txt_size, middle + 1, high, offset)?;
    }
    Ok(())
}

/// `ts_accum` (tsvector_op.c:2412).
pub fn ts_accum(stat: &mut TSVectorStat, data: &[u8]) -> PgResult<()> {
    let txt_size = tsv_size(data);
    if txt_size == 0 {
        return Ok(());
    }

    let mut nbit = 0u32;
    let mut i = (txt_size - 1) as u32;
    while i > 0 {
        nbit += 1;
        i >>= 1;
    }
    let nbit = 1u32 << nbit;
    let offset = (nbit - txt_size as u32) / 2;

    insertStatEntry(stat, data, txt_size, (nbit >> 1) - offset)?;
    chooseNextStatEntry(stat, data, txt_size, 0, nbit, offset)?;
    Ok(())
}

/// `ts_setup_firstcall` (tsvector_op.c:2447).
pub fn ts_setup_firstcall(stat: &mut TSVectorStat) -> PgResult<()> {
    let mut stack = Vec::new();
    try_reserve(&mut stack, (stat.maxdepth + 1) as usize)?;
    stack.resize((stat.maxdepth + 1) as usize, NO_NODE);
    stat.stack = stack;
    stat.stackpos = 0;

    let mut node = stat.root;
    if node == NO_NODE {
        stat.stack[0] = NO_NODE;
        return Ok(());
    }
    loop {
        stat.stack[stat.stackpos as usize] = node;
        let left = stat.nodes[node].left;
        if left != NO_NODE {
            stat.stackpos += 1;
            node = left;
        } else {
            break;
        }
    }
    debug_assert!(stat.stackpos <= stat.maxdepth);
    Ok(())
}

/// `walkStatEntryTree` (tsvector_op.c:2488).
pub fn walkStatEntryTree(stat: &mut TSVectorStat) -> usize {
    let node = stat.stack[stat.stackpos as usize];
    if node == NO_NODE {
        return NO_NODE;
    }

    if stat.nodes[node].ndoc != 0 {
        return node;
    }

    let right = stat.nodes[node].right;

    if right != NO_NODE && right != stat.stack[(stat.stackpos + 1) as usize] {
        stat.stackpos += 1;
        let mut cur = right;
        loop {
            stat.stack[stat.stackpos as usize] = cur;
            let left = stat.nodes[cur].left;
            if left != NO_NODE {
                stat.stackpos += 1;
                cur = left;
            } else {
                break;
            }
        }
        debug_assert!(stat.stackpos <= stat.maxdepth);
        cur
    } else {
        if stat.stackpos == 0 {
            return NO_NODE;
        }
        stat.stackpos -= 1;
        walkStatEntryTree(stat)
    }
}

/// `ts_process_call` (tsvector_op.c:2534).
pub fn ts_process_call(stat: &mut TSVectorStat) -> Option<SrfRow> {
    let entry = walkStatEntryTree(stat);
    if entry == NO_NODE {
        return None;
    }
    let row = SrfRow {
        col0: stat.nodes[entry].lexeme.clone(),
        col1: Some(stat.nodes[entry].ndoc.to_string().into_bytes()),
        col2: Some(stat.nodes[entry].nentry.to_string().into_bytes()),
    };
    stat.nodes[entry].ndoc = 0;
    Some(row)
}

/// Parse a weight string into a `stat.weight` bitmask, multibyte-aware.
fn parse_weight_string(ws: &[u8]) -> PgResult<i32> {
    let mut weight = 0i32;
    let mut pos = 0usize;
    while pos < ws.len() {
        let len = mb::pg_mblen_range::call(&ws[pos..])? as usize;
        if len == 1 {
            match ws[pos] {
                b'A' | b'a' => weight |= 1 << 3,
                b'B' | b'b' => weight |= 1 << 2,
                b'C' | b'c' => weight |= 1 << 1,
                b'D' | b'd' => weight |= 1,
                _ => weight |= 0,
            }
        }
        pos += len;
    }
    Ok(weight)
}

/// `ts_stat_sql` (tsvector_op.c:2574) — run the user SQL (via the SPI seam) and
/// build the stat tree.
pub fn ts_stat_sql(query: &[u8], ws: Option<&[u8]>) -> PgResult<TSVectorStat> {
    let rows = ext::exec_stat_query::call(query)?;

    let mut stat = TSVectorStat {
        weight: 0,
        maxdepth: 1,
        stack: Vec::new(),
        stackpos: 0,
        root: NO_NODE,
        nodes: Vec::new(),
    };

    if let Some(ws) = ws {
        stat.weight = parse_weight_string(ws)?;
    }

    for data in &rows {
        ts_accum(&mut stat, data)?;
    }

    Ok(stat)
}

/// `ts_stat1` (tsvector_op.c:2667) — `ts_stat(query)` SRF.
pub fn ts_stat1(query: &[u8]) -> PgResult<()> {
    let mut stat = ts_stat_sql(query, None)?;
    ts_setup_firstcall(&mut stat)?;
    while let Some(row) = ts_process_call(&mut stat) {
        ext::srf_return_next::call(row)?;
    }
    Ok(())
}

/// `ts_stat2` (tsvector_op.c:2692) — `ts_stat(query, weights)` SRF.
pub fn ts_stat2(query: &[u8], ws: &[u8]) -> PgResult<()> {
    let mut stat = ts_stat_sql(query, Some(ws))?;
    ts_setup_firstcall(&mut stat)?;
    while let Some(row) = ts_process_call(&mut stat) {
        ext::srf_return_next::call(row)?;
    }
    Ok(())
}

// ===========================================================================
// trigger
// ===========================================================================

/// `elog(ERROR, "%s", msg)` — internal-error path for the trigger checks.
fn trigger_internal_error(msg: &str) -> PgError {
    ereport(ERROR).errmsg(msg.to_string()).into_error()
}

/// `tsvector_update_trigger_byid` (tsvector_op.c:2730).
pub fn tsvector_update_trigger_byid<'mcx>(
    mcx: Mcx<'mcx>,
    trigdata: TriggerDataRef,
) -> PgResult<()> {
    tsvector_update_trigger(mcx, trigdata, false)
}

/// `tsvector_update_trigger_bycolumn` (tsvector_op.c:2736).
pub fn tsvector_update_trigger_bycolumn<'mcx>(
    mcx: Mcx<'mcx>,
    trigdata: TriggerDataRef,
) -> PgResult<()> {
    tsvector_update_trigger(mcx, trigdata, true)
}

/// `tsvector_update_trigger` (tsvector_op.c:2742) — shared trigger body. Every
/// branch decision, error check, message text and SQLSTATE lives here.
///
/// `trigdata` is the `TriggerData *` handle (`fcinfo->context`), carried
/// explicitly rather than read from ambient thread-local state. The relation's
/// `rd_att` tuple descriptor and the working row tuple are fetched once through
/// the trigger-manager carrier seams; the column lookups (`SPI_fnumber` /
/// `SPI_gettypeid` / `SPI_getbinval`), the `IsBinaryCoercible` checks and the
/// `parsetext` dictionary calls then run against those explicit carriers via the
/// real owner crates.
pub fn tsvector_update_trigger<'mcx>(
    mcx: Mcx<'mcx>,
    trigdata: TriggerDataRef,
    config_column: bool,
) -> PgResult<()> {
    // Check call context (CALLED_AS_TRIGGER + TRIGGER_FIRED_* over tg_event).
    let ev = ext::trigger_event::call(trigdata);
    if !ev.called_as_trigger {
        return Err(trigger_internal_error(
            "tsvector_update_trigger: not fired by trigger manager",
        ));
    }
    if !ev.fired_for_row {
        return Err(trigger_internal_error(
            "tsvector_update_trigger: must be fired for row",
        ));
    }
    if !ev.fired_before {
        return Err(trigger_internal_error(
            "tsvector_update_trigger: must be fired BEFORE event",
        ));
    }

    let rettuple: TupleSource;
    let mut update_needed: bool;
    if ev.fired_by_insert {
        // rettuple = trigdata->tg_trigtuple;
        rettuple = TupleSource::TrigTuple;
        update_needed = true;
    } else if ev.fired_by_update {
        // rettuple = trigdata->tg_newtuple;
        rettuple = TupleSource::NewTuple;
        update_needed = false; // computed below
    } else {
        return Err(trigger_internal_error(
            "tsvector_update_trigger: must be fired for INSERT or UPDATE",
        ));
    }

    let tgnargs = ext::tgnargs::call(trigdata);
    if tgnargs < 3 {
        return Err(trigger_internal_error(
            "tsvector_update_trigger: arguments must be tsvector_field, ts_config, text_field1, ...)",
        ));
    }

    // rel = trigdata->tg_relation;  the column reads all key off rel->rd_att.
    let tupdesc = ext::tg_relation_tupdesc::call(mcx, trigdata)?;
    let row = ext::tg_rettuple::call(mcx, trigdata, rettuple)?;

    // Find the target tsvector column.
    let arg0 = ext::tgarg::call(trigdata, 0);
    let tsvector_attr_num = SPI_fnumber(&tupdesc, &arg0);
    if tsvector_attr_num == SPI_ERROR_NOATTRIBUTE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "tsvector column \"{}\" does not exist",
                cstr_display(&arg0)
            ))
            .into_error());
    }
    // This will effectively reject system columns, so no separate test.
    if !IsBinaryCoercible(SPI_gettypeid(&tupdesc, tsvector_attr_num)?, TSVECTOROID)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "column \"{}\" is not of tsvector type",
                cstr_display(&arg0)
            ))
            .into_error());
    }

    // Find the configuration to use.
    let cfg_id: u32;
    if config_column {
        let arg1 = ext::tgarg::call(trigdata, 1);
        let config_attr_num = SPI_fnumber(&tupdesc, &arg1);
        if config_attr_num == SPI_ERROR_NOATTRIBUTE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "configuration column \"{}\" does not exist",
                    cstr_display(&arg1)
                ))
                .into_error());
        }
        if !IsBinaryCoercible(SPI_gettypeid(&tupdesc, config_attr_num)?, REGCONFIGOID)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "column \"{}\" is not of regconfig type",
                    cstr_display(&arg1)
                ))
                .into_error());
        }

        // datum = SPI_getbinval(rettuple, rel->rd_att, config_attr_num, &isnull);
        let (datum, isnull) = SPI_getbinval(mcx, &row, &tupdesc, config_attr_num)?;
        if isnull {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .errmsg(format!(
                    "configuration column \"{}\" must not be null",
                    cstr_display(&arg1)
                ))
                .into_error());
        }
        // cfgId = DatumGetObjectId(datum);
        cfg_id = datum.as_oid();
    } else {
        // names = stringToQualifiedNameList(trigger->tgargs[1], NULL);
        // require a schema so that results are not search path dependent.
        let arg1 = ext::tgarg::call(trigdata, 1);
        if qualified_name_length(&arg1) < 2 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "text search configuration name \"{}\" must be schema-qualified",
                    cstr_display(&arg1)
                ))
                .into_error());
        }
        // cfgId = get_ts_config_oid(names, false);
        cfg_id = ext::lookup_ts_config::call(&arg1)?;
    }

    // initialize parse state (C: prs.lenwords = 32; ... palloc(32)).
    let mut prs = ParsedText::with_lenwords(32);

    // find all words in indexable column(s).
    let mut i = 2;
    while i < tgnargs {
        let argi = ext::tgarg::call(trigdata, i);
        let numattr = SPI_fnumber(&tupdesc, &argi);
        if numattr == SPI_ERROR_NOATTRIBUTE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!("column \"{}\" does not exist", cstr_display(&argi)))
                .into_error());
        }
        if !IsBinaryCoercible(SPI_gettypeid(&tupdesc, numattr)?, TEXTOID)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "column \"{}\" is not of a character type",
                    cstr_display(&argi)
                ))
                .into_error());
        }

        // if (bms_is_member(numattr - FirstLowInvalidHeapAttributeNumber,
        //                   trigdata->tg_updatedcols)) update_needed = true;
        if ext::updated_col::call(trigdata, numattr) {
            update_needed = true;
        }

        // datum = SPI_getbinval(rettuple, rel->rd_att, numattr, &isnull);
        let (datum, isnull) = SPI_getbinval(mcx, &row, &tupdesc, numattr)?;
        if isnull {
            i += 1;
            continue;
        }

        // txt = DatumGetTextPP(datum);
        // parsetext(cfgId, &prs, VARDATA_ANY(txt), VARSIZE_ANY_EXHDR(txt));
        let detoasted = pg_detoast_datum_packed(mcx, datum.as_ref_bytes())?;
        let payload = text_payload(&detoasted);
        parsetext(cfg_id, &mut prs, payload)?;

        i += 1;
    }

    if update_needed {
        // datum = TSVectorGetDatum(make_tsvector(&prs));
        // rettuple = heap_modify_tuple_by_cols(rettuple, rel->rd_att, 1,
        //                                      &tsvector_attr_num, &datum, &false);
        ext::make_and_install_tsvector::call(trigdata, rettuple, tsvector_attr_num, prs)?;
    }

    Ok(())
}

/// `VARDATA_ANY(t)` over a detoasted varlena image: strip the varlena header
/// (1-byte short header when `(image[0] & 0x01)`, else the 4-byte long header)
/// and return the payload `parsetext` consumes. The image is already detoasted
/// (`pg_detoast_datum_packed`), so only the two in-line header forms occur.
fn text_payload(image: &[u8]) -> &[u8] {
    // VARATT_IS_1B(image): (image[0] & 0x01) == 0x01  -> short (1-byte) header.
    if !image.is_empty() && (image[0] & 0x01) != 0 {
        &image[1..]
    } else {
        &image[VARHDRSZ..]
    }
}

/// Render a trigger-arg C-string for an error message.
fn cstr_display(s: &[u8]) -> String {
    let end = s.iter().position(|&b| b == 0).unwrap_or(s.len());
    String::from_utf8_lossy(&s[..end]).into_owned()
}

/// `list_length(stringToQualifiedNameList(name, NULL))` — the dotted-name
/// component count used to enforce schema qualification.
fn qualified_name_length(name: &[u8]) -> usize {
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    let s = &name[..end];
    let mut count = 1usize;
    let mut in_quotes = false;
    let mut any = false;
    for &b in s {
        any = true;
        if b == b'"' {
            in_quotes = !in_quotes;
        } else if b == b'.' && !in_quotes {
            count += 1;
        }
    }
    if !any {
        return 0;
    }
    count
}

// ===========================================================================
// Headline tsquery execution (checkcondition_HL) — wparser_def.c
//
// The headline selector (`prsd_headline` in backend-tsearch-parse) runs the
// generic TS_execute engine against the parsed headline word list rather than
// a tsvector. The per-operand match callback is `checkcondition_HL`: a query
// operand "matches" a headline word when that word's `item` index equals the
// operand's query-item index, reporting the word's position.
//
// These two functions back the `ts_execute_hl` / `ts_execute_locations_hl`
// seams declared in `backend-tsearch-parse-seams`; tsvector-core owns the
// TS_execute engine and installs them. The seam carries the parse-side
// idiomatic `QueryItem`/`ExecPhraseData` types, so the bodies bridge to the
// ABI `types-tsearch` shapes the engine consumes.
// ===========================================================================

use parse_seams::{
    ExecPhraseData as HlExecPhraseData, QueryItem as HlQueryItem, QueryOperand as HlQueryOperand,
    QueryOperator as HlQueryOperator,
};

/// Bridge one parse-side `QueryItem` to the ABI `types-tsearch` `QueryItem`
/// the engine reads.
fn hl_query_item_to_core(it: &HlQueryItem) -> QueryItem {
    match it {
        HlQueryItem::Operand(o) => QueryItem::Qoperand(hl_operand_to_core(o)),
        HlQueryItem::Operator(o) => QueryItem::Qoperator(hl_operator_to_core(o)),
    }
}

/// Bridge a parse-side `QueryOperator` to the ABI `QueryOperator`.
fn hl_operator_to_core(o: &HlQueryOperator) -> QueryOperator {
    QueryOperator {
        type_: o.type_,
        oper: o.oper,
        distance: o.distance,
        left: o.left,
    }
}

/// Bridge a parse-side `QueryOperand` to the ABI `QueryOperand`. The engine
/// only reads `type_`/`weight`/`prefix` for the HL callback (operand identity
/// is by index), so the CRC/len/distance fields are reconstructed faithfully.
fn hl_operand_to_core(o: &HlQueryOperand) -> QueryOperand {
    let mut core = QueryOperand {
        type_: o.type_,
        weight: o.weight,
        prefix: o.prefix,
        valcrc: o.valcrc,
        len_dist: 0,
    };
    core.set_length(o.length);
    core.set_distance(o.distance);
    core
}

/// `checkcondition_HL` (wparser_def.c:1981): the `TSExecuteCallback` over the
/// headline word match-table. `match_table[i] == (item, pos)` for headline
/// word `i`; operand `opidx` matches every word whose `item == Some(opidx)`,
/// reporting those words' positions (ordered, kept strictly increasing).
struct HlCallback<'a> {
    match_table: &'a [(Option<usize>, u16)],
}

impl TSExecuteCallback for HlCallback<'_> {
    fn chkcond(
        &mut self,
        opidx: usize,
        _val: QueryOperand,
        data: Option<&mut PhraseData>,
    ) -> TSTernaryValue {
        match data {
            None => {
                // if (!data) return any-match ? TS_YES : TS_NO.
                for (item, _pos) in self.match_table {
                    if *item == Some(opidx) {
                        return TS_YES;
                    }
                }
                TS_NO
            }
            Some(data) => {
                for (item, pos) in self.match_table {
                    if *item == Some(opidx) {
                        let p = *pos;
                        // data->pos[npos-1] < pos: keep strictly increasing.
                        if data.pos.last().map(|&last| last < p).unwrap_or(true) {
                            data.pos.push(p);
                            data.npos = data.pos.len() as i32;
                        }
                    }
                }
                if data.npos > 0 {
                    TS_YES
                } else {
                    TS_NO
                }
            }
        }
    }
}

/// Bridge the seam's `QueryItem` list into the ABI form the engine consumes.
fn hl_items_to_core(items: &[HlQueryItem]) -> alloc::vec::Vec<QueryItem> {
    items.iter().map(hl_query_item_to_core).collect()
}

/// `ts_execute_hl` seam body: `TS_execute(GETQUERY(query), &ch, flags,
/// checkcondition_HL)` — the boolean form used by `hlCover`.
pub fn ts_execute_hl_seam(
    items: alloc::vec::Vec<HlQueryItem>,
    match_table: alloc::vec::Vec<(Option<usize>, u16)>,
    flags: u32,
) -> PgResult<bool> {
    let core_items = hl_items_to_core(&items);
    let mut cb = HlCallback {
        match_table: &match_table,
    };
    Ok(TS_execute_recurse(&core_items, 0, &mut cb, flags)? != TS_NO)
}

/// `ts_execute_locations_hl` seam body: `TS_execute_locations(GETQUERY(query),
/// &ch, flags, checkcondition_HL)` — the per-AND'ed-term position lists
/// `hlCover` walks to find covers.
pub fn ts_execute_locations_hl_seam(
    items: alloc::vec::Vec<HlQueryItem>,
    match_table: alloc::vec::Vec<(Option<usize>, u16)>,
    _flags: u32,
) -> PgResult<alloc::vec::Vec<HlExecPhraseData>> {
    let core_items = hl_items_to_core(&items);
    let mut cb = HlCallback {
        match_table: &match_table,
    };
    let mut result: alloc::vec::Vec<PhraseData> = alloc::vec::Vec::new();
    if TS_execute_locations_recurse(&core_items, 0, &mut cb, &mut result)? {
        let mut out = alloc::vec::Vec::new();
        try_reserve(&mut out, result.len())?;
        for d in result {
            out.push(HlExecPhraseData {
                npos: d.npos,
                // ExecPhraseData.pos on the parse side is `Vec<i32>`; the engine
                // tracks `Vec<WordEntryPos>` (u16). HL positions never carry a
                // weight bit, so the value is the position directly.
                pos: d.pos.iter().map(|&p| p as i32).collect(),
                width: d.width,
            });
        }
        Ok(out)
    } else {
        Ok(alloc::vec::Vec::new())
    }
}
