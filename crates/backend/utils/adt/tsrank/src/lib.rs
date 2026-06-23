//! Port of `src/backend/utils/adt/tsrank.c` (PostgreSQL 18.3) — ranking a
//! `tsvector` against a `tsquery`.
//!
//! The two ranking families: `ts_rank` (`calc_rank` / `calc_rank_and` /
//! `calc_rank_or`) and `ts_rank_cd` (`calc_rank_cd` + the cover-density
//! machinery `Cover` / `get_docrep` / `fillQueryRepresentationData`), plus the
//! SQL entry points `ts_rank_*` / `ts_rankcd_*` and the weight-array reader
//! `getWeights`.
//!
//! The `tsvector`/`tsquery` operands are walked as raw (detoasted) varlena byte
//! slices via the `ts_type.h` header-macro accessors. `tsCompareString` and the
//! `TS_execute` engine are owned by `backend-utils-adt-tsvector-core` and called
//! through its seam crate; the `float4[]` weight-array deconstruction is owned
//! by the array subsystem (`arrayfuncs.c`) and seamed; `check_stack_depth` /
//! `CHECK_FOR_INTERRUPTS` are owned by `tcop/postgres.c` and seamed.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::too_many_arguments)]

pub mod fmgr_builtins;

use postgres_seams as tcop;
use array_more_seams as array_seams;
use tsvector_core_seams as tsop;
use mcx::{vec_with_capacity_in, Mcx, PgVec};

/// Register the `ts_rank` / `ts_rank_cd` fmgr builtins. This unit declares no
/// inward seam crate (nothing calls back across a cycle), so the only install
/// work is the builtin registration.
pub fn init_seams() {
    fmgr_builtins::register_tsrank_builtins();
}
use types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
};
use ::tsearch::tsearch::{
    ExecPhraseData, QueryItem, QueryOperand, QueryOperator, TSTernaryValue, MAXENTRYPOS, OP_AND,
    OP_PHRASE, QI_OPR, QI_VAL, TS_EXEC_EMPTY, WEP_GETPOS, WEP_GETWEIGHT, WEP_SETPOS, WordEntryPos,
};

/// `NUM_WEIGHTS` (tsrank.c:24).
pub const NUM_WEIGHTS: usize = 4;
/// `default_weights` (tsrank.c:25).
pub const default_weights: [f32; NUM_WEIGHTS] = [0.1, 0.2, 0.4, 1.0];

/// `RANK_NO_NORM` (tsrank.c:29).
pub const RANK_NO_NORM: i32 = 0x00;
/// `RANK_NORM_LOGLENGTH` (tsrank.c:30).
pub const RANK_NORM_LOGLENGTH: i32 = 0x01;
/// `RANK_NORM_LENGTH` (tsrank.c:31).
pub const RANK_NORM_LENGTH: i32 = 0x02;
/// `RANK_NORM_EXTDIST` (tsrank.c:32).
pub const RANK_NORM_EXTDIST: i32 = 0x04;
/// `RANK_NORM_UNIQ` (tsrank.c:33).
pub const RANK_NORM_UNIQ: i32 = 0x08;
/// `RANK_NORM_LOGUNIQ` (tsrank.c:34).
pub const RANK_NORM_LOGUNIQ: i32 = 0x10;
/// `RANK_NORM_RDIVRPLUS1` (tsrank.c:35).
pub const RANK_NORM_RDIVRPLUS1: i32 = 0x20;
/// `DEF_NORM_METHOD` (tsrank.c:36).
pub const DEF_NORM_METHOD: i32 = RANK_NO_NORM;

/// `MAXQROPOS` (tsrank.c:545) — `MAXENTRYPOS`.
pub const MAXQROPOS: usize = MAXENTRYPOS as usize;

// ---------------------------------------------------------------------------
// ts_type.h header-macro accessors over the raw (detoasted) datum byte slices.
// ---------------------------------------------------------------------------

const DATAHDRSIZE: usize = 8; // offsetof(TSVectorData, entries)
const HDRSIZETQ: usize = 8; // VARHDRSZ + sizeof(int32)
const WORDENTRY_SIZE: usize = 4;
const QUERYITEM_SIZE: usize = 12;
const WORDENTRYPOS_SIZE: usize = 2;

#[inline]
fn tsv_size(t: &[u8]) -> i32 {
    i32::from_ne_bytes([t[4], t[5], t[6], t[7]])
}

#[inline]
fn tsv_entry_word(t: &[u8], i: usize) -> u32 {
    let off = DATAHDRSIZE + i * WORDENTRY_SIZE;
    u32::from_ne_bytes([t[off], t[off + 1], t[off + 2], t[off + 3]])
}

#[inline]
fn we_haspos(t: &[u8], i: usize) -> bool {
    (tsv_entry_word(t, i) & 0x1) != 0
}
#[inline]
fn we_len(t: &[u8], i: usize) -> u32 {
    (tsv_entry_word(t, i) >> 1) & 0x7FF
}
#[inline]
fn we_pos(t: &[u8], i: usize) -> u32 {
    (tsv_entry_word(t, i) >> 12) & 0xFFFFF
}

#[inline]
fn tsv_strptr_off(t: &[u8]) -> usize {
    DATAHDRSIZE + (tsv_size(t) as usize) * WORDENTRY_SIZE
}

#[inline]
fn tsv_lexeme(t: &[u8], i: usize) -> &[u8] {
    let base = tsv_strptr_off(t) + we_pos(t, i) as usize;
    &t[base..base + we_len(t, i) as usize]
}

/// `SHORTALIGN(LEN)` — align up to 2 bytes.
#[inline]
fn shortalign(len: usize) -> usize {
    (len + 1) & !1
}

/// `_POSVECPTR(t, e)` — byte offset of entry `i`'s `WordEntryPosVector`.
#[inline]
fn tsv_posvec_off(t: &[u8], i: usize) -> usize {
    tsv_strptr_off(t) + shortalign((we_pos(t, i) + we_len(t, i)) as usize)
}

/// `_POSVECPTR(t, e)->npos`.
#[inline]
fn tsv_posvec_npos(t: &[u8], i: usize) -> u16 {
    let off = tsv_posvec_off(t, i);
    u16::from_ne_bytes([t[off], t[off + 1]])
}

/// `POSDATALEN(t, e)` — position count, or 0 when `!haspos`.
#[inline]
fn posdatalen(t: &[u8], i: usize) -> i32 {
    if we_haspos(t, i) {
        tsv_posvec_npos(t, i) as i32
    } else {
        0
    }
}

/// `POSDATAPTR(t, e)[j]`.
#[inline]
fn posdataptr(t: &[u8], i: usize, j: usize) -> WordEntryPos {
    let off = tsv_posvec_off(t, i) + WORDENTRYPOS_SIZE /* npos */ + j * WORDENTRYPOS_SIZE;
    u16::from_ne_bytes([t[off], t[off + 1]])
}

#[inline]
fn tsq_size(q: &[u8]) -> i32 {
    i32::from_ne_bytes([q[4], q[5], q[6], q[7]])
}

/// `GETQUERY(q)` reified — decode the `QueryItem` array into `mcx`.
#[inline]
fn getquery<'mcx>(mcx: Mcx<'mcx>, q: &[u8]) -> PgResult<PgVec<'mcx, QueryItem>> {
    let n = tsq_size(q) as usize;
    let mut items = vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        items.push(read_query_item(q, i));
    }
    Ok(items)
}

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

#[inline]
fn read_query_operand(q: &[u8], i: usize) -> QueryOperand {
    let off = HDRSIZETQ + i * QUERYITEM_SIZE;
    QueryOperand {
        type_: q[off] as i8,
        weight: q[off + 1],
        prefix: q[off + 2] != 0,
        valcrc: i32::from_ne_bytes([q[off + 4], q[off + 5], q[off + 6], q[off + 7]]),
        len_dist: u32::from_ne_bytes([q[off + 8], q[off + 9], q[off + 10], q[off + 11]]),
    }
}

#[inline]
fn tsq_operand_off(q: &[u8]) -> usize {
    HDRSIZETQ + (tsq_size(q) as usize) * QUERYITEM_SIZE
}

#[inline]
fn tsq_operand_str<'a>(q: &'a [u8], item: &QueryOperand) -> &'a [u8] {
    let base = tsq_operand_off(q) + item.distance() as usize;
    &q[base..base + item.length() as usize]
}

/// Read the `QueryOperand` of a `QI_VAL` item (the C `curitem->qoperand`).
#[inline]
fn expect_operand(item: &QueryItem) -> QueryOperand {
    match item {
        QueryItem::Qoperand(o) => *o,
        _ => unreachable!("expected QI_VAL operand variant"),
    }
}

/// Read the `QueryOperator` of a non-`QI_VAL` item (the C `curitem->qoperator`).
#[inline]
fn expect_operator(item: &QueryItem) -> QueryOperator {
    match item {
        QueryItem::Qoperator(o) => *o,
        _ => unreachable!("expected QI_OPR operator variant"),
    }
}

// ---------------------------------------------------------------------------
// ts_rank
// ---------------------------------------------------------------------------

/// `word_distance` (tsrank.c:45) — weight of a word collocation given a gap.
pub fn word_distance(w: i32) -> f32 {
    if w > 100 {
        return 1e-30f32;
    }
    (1.0_f64 / (1.005_f64 + 0.05_f64 * ((w as f32 as f64) / 1.5_f64 - 2.0_f64).exp())) as f32
}

/// `cnt_length` (tsrank.c:54) — count of position-bearing occurrences.
pub fn cnt_length(t: &[u8]) -> i32 {
    let size = tsv_size(t) as usize;
    let mut len = 0;
    for i in 0..size {
        let clen = posdatalen(t, i);
        if clen == 0 {
            len += 1;
        } else {
            len += clen;
        }
    }
    len
}

/// `find_wordentry` (tsrank.c:87) — locate the `WordEntry`(s) matching a query
/// operand; returns the index of the first matching entry and sets `*nitem`, or
/// `None` if not found.
pub fn find_wordentry(t: &[u8], q: &[u8], item: &QueryOperand, nitem: &mut i32) -> Option<usize> {
    let size = tsv_size(t) as usize;
    let mut stop_low = 0usize;
    let mut stop_high = size;
    let mut stop_middle = stop_high;

    *nitem = 0;

    let qstr = tsq_operand_str(q, item);

    // Loop invariant: StopLow <= item < StopHigh
    while stop_low < stop_high {
        stop_middle = stop_low + (stop_high - stop_low) / 2;
        let difference = tsop::ts_compare_string::call(qstr, tsv_lexeme(t, stop_middle), false);
        if difference == 0 {
            stop_high = stop_middle;
            *nitem = 1;
            break;
        } else if difference > 0 {
            stop_low = stop_middle + 1;
        } else {
            stop_high = stop_middle;
        }
    }

    if item.prefix {
        if stop_low >= stop_high {
            stop_middle = stop_high;
        }

        *nitem = 0;

        while stop_middle < size
            && tsop::ts_compare_string::call(qstr, tsv_lexeme(t, stop_middle), true) == 0
        {
            *nitem += 1;
            stop_middle += 1;
        }
    }

    if *nitem > 0 {
        Some(stop_high)
    } else {
        None
    }
}

/// `compareQueryOperand` (tsrank.c:136) — `qsort_arg` comparator on operand
/// lexeme strings.
pub fn compareQueryOperand(qa: &QueryOperand, qb: &QueryOperand, operand_off: usize, q: &[u8]) -> i32 {
    let a = {
        let base = operand_off + qa.distance() as usize;
        &q[base..base + qa.length() as usize]
    };
    let b = {
        let base = operand_off + qb.distance() as usize;
        &q[base..base + qb.length() as usize]
    };
    tsop::ts_compare_string::call(a, b, false)
}

/// `SortAndUniqItems` (tsrank.c:155) — collect, sort, and de-duplicate the
/// query's operands; returns each as a `(QueryItem index, QueryOperand)` pair.
/// `*size` receives the count.
pub fn SortAndUniqItems<'mcx>(
    mcx: Mcx<'mcx>,
    q: &[u8],
    size: &mut i32,
) -> PgResult<PgVec<'mcx, (usize, QueryOperand)>> {
    let operand_off = tsq_operand_off(q);
    let nitems = *size as usize;

    let mut res: PgVec<'mcx, (usize, QueryOperand)> = vec_with_capacity_in(mcx, nitems)?;
    for i in 0..nitems {
        let item = read_query_item(q, i);
        if item.item_type() == QI_VAL {
            res.push((i, expect_operand(&item)));
        }
    }

    *size = res.len() as i32;
    if *size < 2 {
        return Ok(res);
    }

    // qsort_arg(res, ..., compareQueryOperand, operand)
    res.sort_by(|a, b| compareQueryOperand(&a.1, &b.1, operand_off, q).cmp(&0));

    // remove duplicates
    let mut prevptr = 0usize;
    let mut ptr = 1usize;
    while ptr < *size as usize {
        if compareQueryOperand(&res[ptr].1, &res[prevptr].1, operand_off, q) != 0 {
            prevptr += 1;
            res[prevptr] = res[ptr];
        }
        ptr += 1;
    }

    *size = (prevptr + 1) as i32;
    res.truncate(*size as usize);
    Ok(res)
}

/// `wpos(wep)` (tsrank.c:27) — `w[WEP_GETWEIGHT(wep)]`.
#[inline]
fn wpos(w: &[f32], wep: WordEntryPos) -> f32 {
    w[WEP_GETWEIGHT(wep) as usize]
}

/// `pos[i]` models the C `WordEntryPosVector **pos`: a NULL pointer, the shared
/// POSNULL sentinel, or a real per-lexeme position vector.
enum PosRef<'mcx> {
    Null,
    PosNull,
    Real(PgVec<'mcx, WordEntryPos>),
}

impl<'mcx> PosRef<'mcx> {
    /// A read-only view of this slot's positions (the `Real` buffer, or
    /// `posnull` for the `PosNull` sentinel). Callers must not pass `Null`.
    #[inline]
    fn view<'a>(&'a self, posnull: &'a [WordEntryPos]) -> &'a [WordEntryPos] {
        match self {
            PosRef::Real(v) => v,
            PosRef::PosNull => posnull,
            PosRef::Null => &[],
        }
    }
}

/// `calc_rank_and` (tsrank.c:201) — term-frequency rank for an AND-dominated
/// query.
pub fn calc_rank_and(mcx: Mcx<'_>, w: &[f32], t: &[u8], q: &[u8]) -> PgResult<f32> {
    // POSNULL: a dummy WordEntryPos array used when haspos is false.
    let mut posnull_pos: WordEntryPos = 0;
    WEP_SETPOS(&mut posnull_pos, MAXENTRYPOS - 1);
    let posnull: [WordEntryPos; 1] = [posnull_pos];

    let mut size = tsq_size(q);
    let item = SortAndUniqItems(mcx, q, &mut size)?;
    if size < 2 {
        return calc_rank_or(mcx, w, t, q);
    }

    // pos = palloc0(sizeof(WordEntryPosVector *) * q->size);
    let qsz = tsq_size(q) as usize;
    let mut pos: PgVec<'_, PosRef<'_>> = vec_with_capacity_in(mcx, qsz)?;
    for _ in 0..qsz {
        pos.push(PosRef::Null);
    }

    let mut res: f32 = -1.0;

    for i in 0..size as usize {
        let mut nitem = 0i32;
        let firstentry = match find_wordentry(t, q, &item[i].1, &mut nitem) {
            Some(e) => e,
            None => continue,
        };

        let mut entry = firstentry;
        while (entry - firstentry) < nitem as usize {
            let dimt: i32 = if we_haspos(t, entry) {
                let npos = posdatalen(t, entry);
                let mut v: PgVec<'_, WordEntryPos> = vec_with_capacity_in(mcx, npos as usize)?;
                for j in 0..npos as usize {
                    v.push(posdataptr(t, entry, j));
                }
                pos[i] = PosRef::Real(v);
                npos
            } else {
                pos[i] = PosRef::PosNull;
                1
            };

            let i_is_posnull = matches!(pos[i], PosRef::PosNull);

            for k in 0..i {
                let k_is_posnull = matches!(pos[k], PosRef::PosNull);
                if matches!(pos[k], PosRef::Null) {
                    continue;
                }
                // `i != k`, so two disjoint shared borrows are fine.
                let post = pos[i].view(&posnull);
                let ct = pos[k].view(&posnull);
                let lenct = ct.len() as i32;
                for l in 0..dimt as usize {
                    for p in 0..lenct as usize {
                        let mut dist =
                            (WEP_GETPOS(post[l]) as i32 - WEP_GETPOS(ct[p]) as i32).abs();
                        if dist != 0 || (dist == 0 && (i_is_posnull || k_is_posnull)) {
                            if dist == 0 {
                                dist = MAXENTRYPOS as i32;
                            }
                            // C: curw = (float) sqrt((double)(w*w*word_distance)).
                            let curw = ((wpos(w, post[l]) * wpos(w, ct[p]) * word_distance(dist))
                                as f64)
                                .sqrt() as f32;
                            // C: res = (res < 0) ? curw
                            //          : (float)(1.0 - (1.0 - res)*(1.0 - curw)).
                            res = if res < 0.0 {
                                curw
                            } else {
                                (1.0_f64 - (1.0_f64 - res as f64) * (1.0_f64 - curw as f64)) as f32
                            };
                        }
                    }
                }
            }

            entry += 1;
        }
    }

    Ok(res)
}

/// `calc_rank_or` (tsrank.c:284) — term-frequency rank for an OR-dominated
/// query.
pub fn calc_rank_or(mcx: Mcx<'_>, w: &[f32], t: &[u8], q: &[u8]) -> PgResult<f32> {
    // A dummy WordEntryPos array to use when haspos is false.
    let posnull: [WordEntryPos; 1] = [0];

    let mut size = tsq_size(q);
    let item = SortAndUniqItems(mcx, q, &mut size)?;

    let mut res: f32 = 0.0;

    for i in 0..size as usize {
        let mut nitem = 0i32;
        let firstentry = match find_wordentry(t, q, &item[i].1, &mut nitem) {
            Some(e) => e,
            None => continue,
        };

        let mut entry = firstentry;
        while (entry - firstentry) < nitem as usize {
            let mut post: PgVec<'_, WordEntryPos> = vec_with_capacity_in(mcx, 0)?;
            let dimt: i32;
            if we_haspos(t, entry) {
                let npos = posdatalen(t, entry);
                post = vec_with_capacity_in(mcx, npos as usize)?;
                for j in 0..npos as usize {
                    post.push(posdataptr(t, entry, j));
                }
                dimt = npos;
            } else {
                dimt = 1;
            }
            let post_slice: &[WordEntryPos] = if we_haspos(t, entry) { &post } else { &posnull };

            let mut resj: f32 = 0.0;
            let mut wjm: f32 = -1.0;
            let mut jm: i32 = 0;
            for j in 0..dimt {
                resj += wpos(w, post_slice[j as usize]) / (((j + 1) * (j + 1)) as f32);
                if wpos(w, post_slice[j as usize]) > wjm {
                    wjm = wpos(w, post_slice[j as usize]);
                    jm = j;
                }
            }
            // C: res = (double)res + (double)(float_expr) / 1.64493406685.
            res = ((res as f64)
                + (wjm + resj - wjm / (((jm + 1) * (jm + 1)) as f32)) as f64 / 1.64493406685_f64)
                as f32;

            entry += 1;
        }
    }
    if size > 0 {
        res /= size as f32;
    }
    Ok(res)
}

/// `calc_rank` (tsrank.c:358) — dispatch and apply normalization `method`.
pub fn calc_rank(mcx: Mcx<'_>, w: &[f32], t: &[u8], q: &[u8], method: i32) -> PgResult<f32> {
    if tsv_size(t) == 0 || tsq_size(q) == 0 {
        return Ok(0.0);
    }

    let item0 = read_query_item(q, 0);
    // XXX: What about NOT?
    let is_and = if item0.item_type() == QI_OPR {
        let op = expect_operator(&item0);
        op.oper == OP_AND || op.oper == OP_PHRASE
    } else {
        false
    };

    let mut res = if is_and {
        calc_rank_and(mcx, w, t, q)?
    } else {
        calc_rank_or(mcx, w, t, q)?
    };

    if res < 0.0 {
        res = 1e-20f32;
    }

    if (method & RANK_NORM_LOGLENGTH) != 0 && tsv_size(t) > 0 {
        res = ((res as f64) / (((cnt_length(t) + 1) as f64).ln() / 2.0_f64.ln())) as f32;
    }

    if method & RANK_NORM_LENGTH != 0 {
        let len = cnt_length(t);
        if len > 0 {
            res /= len as f32;
        }
    }

    // RANK_NORM_EXTDIST not applicable

    if (method & RANK_NORM_UNIQ) != 0 && tsv_size(t) > 0 {
        res /= tsv_size(t) as f32;
    }

    if (method & RANK_NORM_LOGUNIQ) != 0 && tsv_size(t) > 0 {
        res = ((res as f64) / (((tsv_size(t) + 1) as f64).ln() / 2.0_f64.ln())) as f32;
    }

    if method & RANK_NORM_RDIVRPLUS1 != 0 {
        res /= res + 1.0;
    }

    Ok(res)
}

/// `getWeights` (tsrank.c:405) — read a 4-element `float4[]` weight array into
/// `ws`, validating range; negative entries fall back to [`default_weights`].
/// The array deconstruction (`utils/array.h`) is seamed.
pub fn getWeights(mcx: Mcx<'_>, win: &[u8], ws: &mut [f32; NUM_WEIGHTS]) -> PgResult<()> {
    // The seam mirrors `ARR_NDIM != 1` and `array_contains_nulls` (with the
    // exact SQLSTATE / message text) and yields the element values.
    let arrdata = array_seams::deconstruct_float4_array::call(mcx, win)?;

    if arrdata.len() < NUM_WEIGHTS {
        return Err(PgError::error("array of weight is too short")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    for i in 0..NUM_WEIGHTS {
        ws[i] = if arrdata[i] >= 0.0 {
            arrdata[i]
        } else {
            default_weights[i]
        };
        if ws[i] > 1.0 {
            return Err(PgError::error("weight out of range")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
    }
    Ok(())
}

/// `ts_rank_wttf` (tsrank.c:439) — `ts_rank(weights, vec, query, method)`.
pub fn ts_rank_wttf(mcx: Mcx<'_>, win: &[u8], txt: &[u8], query: &[u8], method: i32) -> PgResult<f32> {
    let mut weights = [0.0f32; NUM_WEIGHTS];
    getWeights(mcx, win, &mut weights)?;
    calc_rank(mcx, &weights, txt, query, method)
}

/// `ts_rank_wtt` (tsrank.c:458) — `ts_rank(weights, vec, query)`.
pub fn ts_rank_wtt(mcx: Mcx<'_>, win: &[u8], txt: &[u8], query: &[u8]) -> PgResult<f32> {
    let mut weights = [0.0f32; NUM_WEIGHTS];
    getWeights(mcx, win, &mut weights)?;
    calc_rank(mcx, &weights, txt, query, DEF_NORM_METHOD)
}

/// `ts_rank_ttf` (tsrank.c:476) — `ts_rank(vec, query, method)`.
pub fn ts_rank_ttf(mcx: Mcx<'_>, txt: &[u8], query: &[u8], method: i32) -> PgResult<f32> {
    calc_rank(mcx, &default_weights, txt, query, method)
}

/// `ts_rank_tt` (tsrank.c:491) — `ts_rank(vec, query)`.
pub fn ts_rank_tt(mcx: Mcx<'_>, txt: &[u8], query: &[u8]) -> PgResult<f32> {
    calc_rank(mcx, &default_weights, txt, query, DEF_NORM_METHOD)
}

// ---------------------------------------------------------------------------
// ts_rank_cd (cover density)
// ---------------------------------------------------------------------------

/// `DocRepresentation` (tsrank.c:504) — one entry of the document position
/// stream. The C `data` union (`query` vs `map`) is a Rust enum.
#[derive(Clone)]
pub struct DocRepresentation {
    pub data: DocRepData,
    pub pos: WordEntryPos,
}

/// The `DocRepresentation.data` union (tsrank.c:506). `items`/`item` store the
/// operand's `QueryItem` array index; `entry` the tsvector `WordEntry` index.
#[derive(Clone)]
pub enum DocRepData {
    /// compiled doc representation (`items`, `nitem`)
    Query { items: Vec<usize>, nitem: i16 },
    /// preparing doc representation (`item`, `entry`)
    Map { item: usize, entry: usize },
}

impl DocRepData {
    #[inline]
    fn map_entry(&self) -> usize {
        match self {
            DocRepData::Map { entry, .. } => *entry,
            DocRepData::Query { .. } => unreachable!("map_entry on Query variant"),
        }
    }
    #[inline]
    fn map_item(&self) -> usize {
        match self {
            DocRepData::Map { item, .. } => *item,
            DocRepData::Query { .. } => unreachable!("map_item on Query variant"),
        }
    }
}

/// `QueryRepresentationOperand` (tsrank.c:546) — per-operand match state.
#[derive(Clone)]
pub struct QueryRepresentationOperand {
    pub operandexists: bool,
    /// true means descending insert order
    pub reverseinsert: bool,
    pub npos: u32,
    pub pos: [WordEntryPos; MAXQROPOS],
}

impl Default for QueryRepresentationOperand {
    fn default() -> Self {
        QueryRepresentationOperand {
            operandexists: false,
            reverseinsert: false,
            npos: 0,
            pos: [0; MAXQROPOS],
        }
    }
}

/// `QueryRepresentation` (tsrank.c:555) — the query + per-operand data.
pub struct QueryRepresentation {
    /// number of `QueryItem`s in the query (`query->size`)
    pub query_size: i32,
    pub operandData: Vec<QueryRepresentationOperand>,
}

/// `CoverExt` (tsrank.c:588) — the extent state threaded through [`Cover`].
#[derive(Clone, Copy, Default)]
pub struct CoverExt {
    pub pos: i32,
    pub p: i32,
    pub q: i32,
    pub begin: i32,
    pub end: i32,
}

/// `compareDocR` (tsrank.c:524) — qsort comparator (pos, weight, entry).
pub fn compareDocR(a: &DocRepresentation, b: &DocRepresentation) -> i32 {
    if WEP_GETPOS(a.pos) == WEP_GETPOS(b.pos) {
        if WEP_GETWEIGHT(a.pos) == WEP_GETWEIGHT(b.pos) {
            let ae = a.data.map_entry();
            let be = b.data.map_entry();
            if ae == be {
                return 0;
            }
            return if ae > be { 1 } else { -1 };
        }
        return if WEP_GETWEIGHT(a.pos) > WEP_GETWEIGHT(b.pos) {
            1
        } else {
            -1
        };
    }
    if WEP_GETPOS(a.pos) > WEP_GETPOS(b.pos) {
        1
    } else {
        -1
    }
}

/// `checkcondition_QueryOperand` (tsrank.c:568) — the query-operand check
/// callback matching against a [`QueryRepresentation`]. `opidx` is the
/// operand's `QueryItem` array index (the C `QR_GET_OPERAND_DATA` offset).
pub fn checkcondition_QueryOperand(
    qr: &QueryRepresentation,
    opidx: usize,
    data: Option<&mut ExecPhraseData>,
) -> TSTernaryValue {
    let op_data = &qr.operandData[opidx];

    if !op_data.operandexists {
        return TSTernaryValue::TS_NO;
    }

    if let Some(data) = data {
        data.npos = op_data.npos as i32;
        // In C, data->pos points into opData->pos (+ offset for reverse insert).
        let off = if op_data.reverseinsert {
            MAXQROPOS - op_data.npos as usize
        } else {
            0
        };
        data.pos = op_data.pos[off..off + op_data.npos as usize].to_vec();
        data.allocated = false;
    }

    TSTernaryValue::TS_YES
}

/// `resetQueryRepresentation` (tsrank.c:598).
pub fn resetQueryRepresentation(qr: &mut QueryRepresentation, reverseinsert: bool) {
    for i in 0..qr.query_size as usize {
        qr.operandData[i].operandexists = false;
        qr.operandData[i].reverseinsert = reverseinsert;
        qr.operandData[i].npos = 0;
    }
}

/// `fillQueryRepresentationData` (tsrank.c:611).
pub fn fillQueryRepresentationData(
    qr: &mut QueryRepresentation,
    items: &[QueryItem],
    entry: &DocRepresentation,
) {
    let (qitems, nitem) = match &entry.data {
        DocRepData::Query { items, nitem } => (items, *nitem),
        DocRepData::Map { .. } => unreachable!("fillQueryRepresentationData on Map variant"),
    };

    for i in 0..nitem as usize {
        let opidx = qitems[i];
        if items[opidx].item_type() != QI_VAL {
            continue;
        }

        let op_data = &mut qr.operandData[opidx];

        op_data.operandexists = true;

        if op_data.npos == 0 {
            let last_pos = if op_data.reverseinsert { MAXQROPOS - 1 } else { 0 };
            op_data.pos[last_pos] = entry.pos;
            op_data.npos += 1;
            continue;
        }

        let last_pos = if op_data.reverseinsert {
            MAXQROPOS - op_data.npos as usize
        } else {
            (op_data.npos - 1) as usize
        };

        if WEP_GETPOS(op_data.pos[last_pos]) != WEP_GETPOS(entry.pos) {
            let last_pos = if op_data.reverseinsert {
                MAXQROPOS - 1 - op_data.npos as usize
            } else {
                op_data.npos as usize
            };

            op_data.pos[last_pos] = entry.pos;
            op_data.npos += 1;
        }
    }
}

/// `Cover` (tsrank.c:651) — find the next minimal cover in the doc stream.
pub fn Cover(
    mcx: Mcx<'_>,
    doc: &[DocRepresentation],
    len: i32,
    qr: &mut QueryRepresentation,
    items: &[QueryItem],
    ext: &mut CoverExt,
) -> PgResult<bool> {
    // this function recurses, so it could be driven to stack overflow.
    tcop::check_stack_depth::call()?;

    let mut lastpos = ext.pos;
    let mut found = false;

    resetQueryRepresentation(qr, false);

    ext.p = i32::MAX;
    ext.q = 0;
    let mut ptr = ext.pos;

    // find upper bound of cover from current position, move up
    while ptr < len {
        fillQueryRepresentationData(qr, items, &doc[ptr as usize]);

        if ts_execute_cover(mcx, qr, items)? {
            if (WEP_GETPOS(doc[ptr as usize].pos) as i32) > ext.q {
                ext.q = WEP_GETPOS(doc[ptr as usize].pos) as i32;
                ext.end = ptr;
                lastpos = ptr;
                found = true;
            }
            break;
        }
        ptr += 1;
    }

    if !found {
        return Ok(false);
    }

    resetQueryRepresentation(qr, true);

    ptr = lastpos;

    // find lower bound of cover from found upper bound, move down
    while ptr >= ext.pos {
        // we scan doc from right to left, so pos info in reverse order!
        fillQueryRepresentationData(qr, items, &doc[ptr as usize]);

        if ts_execute_cover(mcx, qr, items)? {
            if (WEP_GETPOS(doc[ptr as usize].pos) as i32) < ext.p {
                ext.begin = ptr;
                ext.p = WEP_GETPOS(doc[ptr as usize].pos) as i32;
            }
            break;
        }
        ptr -= 1;
    }

    if ext.p <= ext.q {
        // set position for next try to next lexeme after beginning of found cover
        ext.pos = ptr + 1;
        return Ok(true);
    }

    ext.pos += 1;
    Cover(mcx, doc, len, qr, items, ext)
}

/// `TS_execute(GETQUERY(query), qr, TS_EXEC_EMPTY, checkcondition_QueryOperand)`,
/// wiring the [`checkcondition_QueryOperand`] callback to `qr`.
fn ts_execute_cover(mcx: Mcx<'_>, qr: &QueryRepresentation, items: &[QueryItem]) -> PgResult<bool> {
    let mut chk = |opidx: usize, _val: &QueryOperand, data: Option<&mut ExecPhraseData>| {
        checkcondition_QueryOperand(qr, opidx, data)
    };
    tsop::ts_execute::call(mcx, items, TS_EXEC_EMPTY, &mut chk)
}

/// `get_docrep` (tsrank.c:732) — build the [`DocRepresentation`] position stream
/// for a tsvector under a query; `*doclen` receives its length. Returns `None`
/// (C NULL) when no positions match.
pub fn get_docrep(
    mcx: Mcx<'_>,
    txt: &[u8],
    qr: &QueryRepresentation,
    query: &[u8],
    doclen: &mut i32,
) -> PgResult<Option<Vec<DocRepresentation>>> {
    let qsize = qr.query_size as usize;
    // C palloc's `doc` at `query->size * 4` and repalloc-doubles; an mcx-charged
    // `PgVec` grows the same way.
    let mut doc: PgVec<'_, DocRepresentation> = vec_with_capacity_in(mcx, qsize * 4)?;

    // Iterate through query to make DocRepresentation for words satisfied by it.
    for i in 0..qsize {
        let item_i = read_query_item(query, i);
        if item_i.item_type() != QI_VAL {
            continue;
        }
        let curoperand = expect_operand(&item_i);

        let mut nitem = 0i32;
        let firstentry = match find_wordentry(txt, query, &curoperand, &mut nitem) {
            Some(e) => e,
            None => continue,
        };

        let mut entry = firstentry;
        while (entry - firstentry) < nitem as usize {
            if !we_haspos(txt, entry) {
                // ignore words without positions
                entry += 1;
                continue;
            }
            let dimt = posdatalen(txt, entry);

            for j in 0..dimt as usize {
                let p = posdataptr(txt, entry, j);
                if curoperand.weight == 0 || (curoperand.weight & (1 << WEP_GETWEIGHT(p))) != 0 {
                    doc.push(DocRepresentation {
                        pos: p,
                        data: DocRepData::Map { item: i, entry },
                    });
                }
            }

            entry += 1;
        }
    }

    let cur = doc.len();
    if cur > 0 {
        // Sort representation in ascending order by pos and entry.
        doc.sort_by(|a, b| compareDocR(a, b).cmp(&0));

        // Join QueryItem per WordEntry and its position. `result` escapes to the
        // caller; it is a plain owned Vec.
        let mut result: Vec<DocRepresentation> = Vec::new();
        result.try_reserve(cur).map_err(|_| mcx.oom(cur))?;

        let mut storage_pos = doc[0].pos;
        let mut storage_items: Vec<usize> = Vec::new();
        storage_items.try_reserve(qsize).map_err(|_| mcx.oom(qsize))?;
        storage_items.push(doc[0].data.map_item());
        let mut storage_nitem: i16 = 1;

        let mut rptr = 1usize;
        while rptr < cur {
            if doc[rptr].pos == doc[rptr - 1].pos
                && doc[rptr].data.map_entry() == doc[rptr - 1].data.map_entry()
            {
                storage_items.push(doc[rptr].data.map_item());
                storage_nitem += 1;
            } else {
                result.push(DocRepresentation {
                    pos: storage_pos,
                    data: DocRepData::Query {
                        items: storage_items,
                        nitem: storage_nitem,
                    },
                });
                storage_pos = doc[rptr].pos;
                storage_items = Vec::new();
                storage_items.try_reserve(qsize).map_err(|_| mcx.oom(qsize))?;
                storage_items.push(doc[rptr].data.map_item());
                storage_nitem = 1;
            }
            rptr += 1;
        }

        result.push(DocRepresentation {
            pos: storage_pos,
            data: DocRepData::Query {
                items: storage_items,
                nitem: storage_nitem,
            },
        });

        *doclen = result.len() as i32;
        return Ok(Some(result));
    }

    Ok(None)
}

/// `calc_rank_cd` (tsrank.c:855) — cover-density rank with normalization.
pub fn calc_rank_cd(mcx: Mcx<'_>, arrdata: &[f32], txt: &[u8], query: &[u8], method: i32) -> PgResult<f32> {
    let mut invws = [0.0f64; NUM_WEIGHTS];
    let mut wdoc: f64 = 0.0;
    let mut sum_dist: f64 = 0.0;
    let mut prev_ext_pos: f64 = 0.0;
    let mut n_extent: i32 = 0;

    for i in 0..NUM_WEIGHTS {
        invws[i] = (if arrdata[i] >= 0.0 {
            arrdata[i]
        } else {
            default_weights[i]
        }) as f64;
        if invws[i] > 1.0 {
            return Err(PgError::error("weight out of range")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
        invws[i] = 1.0 / invws[i];
    }

    let qsize = tsq_size(query) as usize;
    let mut operand_data: Vec<QueryRepresentationOperand> = Vec::new();
    operand_data
        .try_reserve(qsize)
        .map_err(|_| mcx.oom(qsize))?;
    operand_data.resize(qsize, QueryRepresentationOperand::default());
    let mut qr = QueryRepresentation {
        query_size: tsq_size(query),
        operandData: operand_data,
    };
    let items = getquery(mcx, query)?;

    let mut doclen = 0i32;
    let doc = match get_docrep(mcx, txt, &qr, query, &mut doclen)? {
        Some(d) => d,
        None => return Ok(0.0),
    };

    let mut ext = CoverExt::default();
    while Cover(mcx, &doc, doclen, &mut qr, &items, &mut ext)? {
        let mut inv_sum: f64 = 0.0;
        let mut ptr = ext.begin;
        while ptr <= ext.end {
            inv_sum += invws[WEP_GETWEIGHT(doc[ptr as usize].pos) as usize];
            ptr += 1;
        }

        let cpos: f64 = ((ext.end - ext.begin + 1) as f64) / inv_sum;

        // if doc are big enough then ext.q may be equal to ext.p due to limit of
        // positional information. Approximate noise words as half cover's length.
        let mut n_noise = (ext.q - ext.p) - (ext.end - ext.begin);
        if n_noise < 0 {
            n_noise = (ext.end - ext.begin) / 2;
        }
        wdoc += cpos / ((1 + n_noise) as f64);

        let cur_ext_pos: f64 = ((ext.q + ext.p) as f64) / 2.0;
        if n_extent > 0 && cur_ext_pos > prev_ext_pos
        /* prevent division by zero in a case of multiple lexize */
        {
            sum_dist += 1.0 / (cur_ext_pos - prev_ext_pos);
        }

        prev_ext_pos = cur_ext_pos;
        n_extent += 1;
    }

    if (method & RANK_NORM_LOGLENGTH) != 0 && tsv_size(txt) > 0 {
        wdoc /= ((cnt_length(txt) + 1) as f64).ln();
    }

    if method & RANK_NORM_LENGTH != 0 {
        let len = cnt_length(txt);
        if len > 0 {
            wdoc /= len as f64;
        }
    }

    if (method & RANK_NORM_EXTDIST) != 0 && n_extent > 0 && sum_dist > 0.0 {
        wdoc /= (n_extent as f64) / sum_dist;
    }

    if (method & RANK_NORM_UNIQ) != 0 && tsv_size(txt) > 0 {
        wdoc /= tsv_size(txt) as f64;
    }

    if (method & RANK_NORM_LOGUNIQ) != 0 && tsv_size(txt) > 0 {
        wdoc /= ((tsv_size(txt) + 1) as f64).ln() / 2.0_f64.ln();
    }

    if method & RANK_NORM_RDIVRPLUS1 != 0 {
        wdoc /= wdoc + 1.0;
    }

    Ok(wdoc as f32)
}

/// `ts_rankcd_wttf` (tsrank.c:958) — `ts_rank_cd(weights, vec, query, method)`.
pub fn ts_rankcd_wttf(mcx: Mcx<'_>, win: &[u8], txt: &[u8], query: &[u8], method: i32) -> PgResult<f32> {
    let mut weights = [0.0f32; NUM_WEIGHTS];
    getWeights(mcx, win, &mut weights)?;
    calc_rank_cd(mcx, &weights, txt, query, method)
}

/// `ts_rankcd_wtt` (tsrank.c:977) — `ts_rank_cd(weights, vec, query)`.
pub fn ts_rankcd_wtt(mcx: Mcx<'_>, win: &[u8], txt: &[u8], query: &[u8]) -> PgResult<f32> {
    let mut weights = [0.0f32; NUM_WEIGHTS];
    getWeights(mcx, win, &mut weights)?;
    calc_rank_cd(mcx, &weights, txt, query, DEF_NORM_METHOD)
}

/// `ts_rankcd_ttf` (tsrank.c:995) — `ts_rank_cd(vec, query, method)`.
pub fn ts_rankcd_ttf(mcx: Mcx<'_>, txt: &[u8], query: &[u8], method: i32) -> PgResult<f32> {
    calc_rank_cd(mcx, &default_weights, txt, query, method)
}

/// `ts_rankcd_tt` (tsrank.c:1010) — `ts_rank_cd(vec, query)`.
pub fn ts_rankcd_tt(mcx: Mcx<'_>, txt: &[u8], query: &[u8]) -> PgResult<f32> {
    calc_rank_cd(mcx, &default_weights, txt, query, DEF_NORM_METHOD)
}
