use ::adt_tsvector_core::layout::*;
use ::adt_tsvector_core::query::{Item, Operand, TsQueryRef, OP_AND, OP_PHRASE};
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::PgResult;

pub const NUM_WEIGHTS: usize = 4;
pub const DEFAULT_WEIGHTS: [f32; NUM_WEIGHTS] = [0.1, 0.2, 0.4, 1.0];

pub const RANK_NORM_LOGLENGTH: i32 = 0x01;
pub const RANK_NORM_LENGTH: i32 = 0x02;
pub const RANK_NORM_EXTDIST: i32 = 0x04;
pub const RANK_NORM_UNIQ: i32 = 0x08;
pub const RANK_NORM_LOGUNIQ: i32 = 0x10;
pub const RANK_NORM_RDIVRPLUS1: i32 = 0x20;
pub const DEF_NORM_METHOD: i32 = 0;

fn word_distance(w: i32) -> f32 {
    if w > 100 {
        return 1e-30f32;
    }
    (1.0f64 / (1.005 + 0.05 * (w as f32 as f64 / 1.5 - 2.0).exp())) as f32
}

pub fn cnt_length(t: TsVec<'_>) -> i32 {
    let mut len = 0i32;
    for i in 0..t.size() {
        let clen = t.positions(t.entry(i)).len() as i32;
        len += if clen == 0 { 1 } else { clen };
    }
    len
}

// find_wordentry: (first_index, nitem) of entries matching `item`.
pub fn find_wordentry(
    t: TsVec<'_>,
    q: TsQueryRef<'_>,
    item: &Operand,
) -> Option<(usize, usize)> {
    let operand = q.operand_str(item);
    let mut lo = 0usize;
    let mut hi = t.size();
    let mut mid = hi;
    let mut nitem = 0usize;
    while lo < hi {
        mid = lo + (hi - lo) / 2;
        let difference = ts_compare_string(operand, t.lexeme(t.entry(mid)), false);
        if difference == 0 {
            hi = mid;
            nitem = 1;
            break;
        } else if difference > 0 {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if item.prefix {
        if lo >= hi {
            mid = hi;
        }
        nitem = 0;
        let mut m = mid;
        while m < t.size() && ts_compare_string(operand, t.lexeme(t.entry(m)), true) == 0 {
            nitem += 1;
            m += 1;
        }
        if nitem > 0 {
            return Some((mid, nitem));
        }
        return None;
    }
    if nitem > 0 {
        Some((hi, nitem))
    } else {
        None
    }
}

fn wpos(w: &[f32; NUM_WEIGHTS], p: WordEntryPos) -> f32 {
    w[wep_getweight(p) as usize]
}

// SortAndUniqItems: sorted, de-duplicated QueryOperands.
fn sort_and_uniq_items<'mcx>(
    mcx: Mcx<'mcx>,
    q: TsQueryRef<'_>,
) -> PgResult<PgVec<'mcx, Operand>> {
    let mut res: PgVec<Operand> = vec_with_capacity_in(mcx, q.size())?;
    for i in 0..q.size() {
        if let Item::Val(op) = q.item(i) {
            res.push(op);
        }
    }
    if res.len() < 2 {
        return Ok(res);
    }
    let pool = q.operand_pool();
    res.sort_by(|a, b| {
        match ts_compare_string(
            &pool[a.distance..a.distance + a.length],
            &pool[b.distance..b.distance + b.length],
            false,
        ) {
            n if n < 0 => core::cmp::Ordering::Less,
            0 => core::cmp::Ordering::Equal,
            _ => core::cmp::Ordering::Greater,
        }
    });
    res.dedup_by(|a, b| {
        ts_compare_string(
            &pool[a.distance..a.distance + a.length],
            &pool[b.distance..b.distance + b.length],
            false,
        ) == 0
    });
    Ok(res)
}

const POSNULL_POS: WordEntryPos = (MAXENTRYPOS - 1) as WordEntryPos;

fn calc_rank_and(
    mcx: Mcx<'_>,
    w: &[f32; NUM_WEIGHTS],
    t: TsVec<'_>,
    q: TsQueryRef<'_>,
) -> PgResult<f32> {
    let item = sort_and_uniq_items(mcx, q)?;
    if item.len() < 2 {
        return calc_rank_or(mcx, w, t, q);
    }
    let posnull: [WordEntryPos; 1] = [POSNULL_POS];
    // pos[i]: last-seen (positions, is_posnull_dummy) for deduped item i.
    let mut pos: PgVec<Option<(&[WordEntryPos], bool)>> = vec_with_capacity_in(mcx, q.size())?;
    for _ in 0..q.size() {
        pos.push(None);
    }
    let mut res = -1.0f32;

    for (i, it) in item.iter().enumerate() {
        let Some((first, nitem)) = find_wordentry(t, q, it) else {
            continue;
        };
        for entry in first..first + nitem {
            let e = t.entry(entry);
            let this: &[WordEntryPos] = if e.haspos() { t.positions(e) } else { &posnull };
            let this_is_null = !e.haspos();
            pos[i] = Some((this, this_is_null));
            for k in 0..i {
                let Some((ct, ct_is_null)) = pos[k] else { continue };
                for &lp in this {
                    for &cp in ct {
                        let dist =
                            (wep_getpos(lp) as i32 - wep_getpos(cp) as i32).abs();
                        if dist != 0 || (dist == 0 && (this_is_null || ct_is_null)) {
                            let d = if dist == 0 { MAXENTRYPOS as i32 } else { dist };
                            let curw = ((wpos(w, lp) * wpos(w, cp) * word_distance(d))
                                as f64)
                                .sqrt() as f32;
                            res = if res < 0.0 {
                                curw
                            } else {
                                (1.0 - (1.0 - res as f64) * (1.0 - curw as f64)) as f32
                            };
                        }
                    }
                }
            }
        }
    }
    Ok(res)
}

fn calc_rank_or(
    mcx: Mcx<'_>,
    w: &[f32; NUM_WEIGHTS],
    t: TsVec<'_>,
    q: TsQueryRef<'_>,
) -> PgResult<f32> {
    let item = sort_and_uniq_items(mcx, q)?;
    let posnull: [WordEntryPos; 1] = [0];
    let mut res = 0.0f32;

    for it in item.iter() {
        let Some((first, nitem)) = find_wordentry(t, q, it) else {
            continue;
        };
        for entry in first..first + nitem {
            let e = t.entry(entry);
            let post: &[WordEntryPos] = if e.haspos() { t.positions(e) } else { &posnull };
            let mut resj = 0.0f32;
            let mut wjm = -1.0f32;
            let mut jm = 0i32;
            for (j, &p) in post.iter().enumerate() {
                resj += wpos(w, p) / ((j + 1) * (j + 1)) as f32;
                if wpos(w, p) > wjm {
                    wjm = wpos(w, p);
                    jm = j as i32;
                }
            }
            res = (res as f64
                + (wjm + resj - wjm / ((jm + 1) * (jm + 1)) as f32) as f64 / 1.64493406685)
                as f32;
        }
    }
    if !item.is_empty() {
        res /= item.len() as f32;
    }
    Ok(res)
}

pub fn calc_rank(
    mcx: Mcx<'_>,
    w: &[f32; NUM_WEIGHTS],
    t: TsVec<'_>,
    q: TsQueryRef<'_>,
    method: i32,
) -> PgResult<f32> {
    if t.size() == 0 || q.size() == 0 {
        return Ok(0.0);
    }
    let is_and = matches!(q.item(0), Item::Opr(o) if o.oper == OP_AND || o.oper == OP_PHRASE);
    let mut res = if is_and {
        calc_rank_and(mcx, w, t, q)?
    } else {
        calc_rank_or(mcx, w, t, q)?
    };
    if res < 0.0 {
        res = 1e-20f32;
    }
    if method & RANK_NORM_LOGLENGTH != 0 && t.size() > 0 {
        res = (res as f64 / (((cnt_length(t) + 1) as f64).ln() / 2.0f64.ln())) as f32;
    }
    if method & RANK_NORM_LENGTH != 0 {
        let len = cnt_length(t);
        if len > 0 {
            res /= len as f32;
        }
    }
    if method & RANK_NORM_UNIQ != 0 && t.size() > 0 {
        res /= t.size() as f32;
    }
    if method & RANK_NORM_LOGUNIQ != 0 && t.size() > 0 {
        res = (res as f64 / (((t.size() + 1) as f64).ln() / 2.0f64.ln())) as f32;
    }
    if method & RANK_NORM_RDIVRPLUS1 != 0 {
        res /= res + 1.0;
    }
    Ok(res)
}
