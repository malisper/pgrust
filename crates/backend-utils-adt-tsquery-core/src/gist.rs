//! Port of `src/backend/utils/adt/tsquery_gist.c` — GiST index support for the
//! `gtsquery` opclass.
//!
//! The GiST key is a [`TSQuerySign`] (`uint64` bit signature). The fmgr
//! `GISTENTRY` / `GistEntryVector` / `GIST_SPLITVEC` framing is the GiST access
//! method's boundary (the unported `access/gist.c`); these functions take the
//! already-decoded keys and return the bare results, exactly as the sibling
//! `backend-utils-adt-tsgistidx` GiST port does.

extern crate alloc;

use alloc::vec::Vec;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::PgResult;

use crate::op::makeTSQuerySign;
use types_tsearch::tsearch::{TSQuerySign, TSQS_SIGLEN};

/// `RTContainsStrategyNumber` (`access/stratnum.h`).
pub const RT_CONTAINS_STRATEGY_NUMBER: u16 = 7;
/// `RTContainedByStrategyNumber` (`access/stratnum.h`).
pub const RT_CONTAINED_BY_STRATEGY_NUMBER: u16 = 8;

/// `gtsquery_compress` (tsquery_gist.c:26) — the leaf branch (`entry->leafkey`):
/// turn the `tsquery` leaf datum into its [`TSQuerySign`] key. The non-leaf
/// branch is the identity (`retval = entry`) and lives in the GiST AM boundary.
pub fn gtsquery_compress_leaf(query: &[u8]) -> PgResult<TSQuerySign> {
    makeTSQuerySign(query)
}

/// `gtsquery_consistent` (tsquery_gist.c:52) — GiST `consistent`. `key` is the
/// stored signature, `query` the search `tsquery` image, `strategy` the
/// `RT*StrategyNumber`, `is_leaf` whether the entry is on a leaf page. Returns
/// `(matched, recheck)`; `recheck` is always `true` (all cases inexact).
pub fn gtsquery_consistent(
    key: TSQuerySign,
    query: &[u8],
    strategy: u16,
    is_leaf: bool,
) -> PgResult<(bool, bool)> {
    // All cases served by this function are inexact
    let recheck = true;
    let sq = makeTSQuerySign(query)?;

    let retval = match strategy {
        RT_CONTAINS_STRATEGY_NUMBER => {
            if is_leaf {
                (key & sq) == sq
            } else {
                (key & sq) != 0
            }
        }
        RT_CONTAINED_BY_STRATEGY_NUMBER => {
            if is_leaf {
                (key & sq) == key
            } else {
                (key & sq) != 0
            }
        }
        _ => false,
    };
    Ok((retval, recheck))
}

/// `gtsquery_union` (tsquery_gist.c:88) — OR together the signatures of the
/// entry vector. `*size = sizeof(TSQuerySign)` is the AM's concern (fixed).
pub fn gtsquery_union(entries: &[TSQuerySign]) -> TSQuerySign {
    let mut sign: TSQuerySign = 0;
    for &e in entries {
        sign |= e;
    }
    sign
}

/// `gtsquery_same(a, b, *result)` (tsquery_gist.c:106) — GiST `same`.
pub fn gtsquery_same(a: TSQuerySign, b: TSQuerySign) -> bool {
    a == b
}

/// `sizebitvec(sign)` (tsquery_gist.c:118) — popcount over `TSQS_SIGLEN` bits.
fn sizebitvec(sign: TSQuerySign) -> i32 {
    let mut size = 0;
    for i in 0..TSQS_SIGLEN {
        size += (0x01 & (sign >> i)) as i32;
    }
    size
}

/// `hemdist(a, b)` (tsquery_gist.c:130) — Hamming distance of the signatures.
fn hemdist(a: TSQuerySign, b: TSQuerySign) -> i32 {
    sizebitvec(a ^ b)
}

/// `gtsquery_penalty(origval, newval, *penalty)` (tsquery_gist.c:138) — the
/// `hemdist` of the two keys, as an `f32`.
pub fn gtsquery_penalty(origval: TSQuerySign, newval: TSQuerySign) -> f32 {
    hemdist(origval, newval) as f32
}

/// `SPLITCOST` (tsquery_gist.c:151).
struct SplitCost {
    pos: u16,
    cost: i32,
}

/// `comparecost` (tsquery_gist.c:157) — order by `cost` ascending
/// (`pg_cmp_s32`).
fn comparecost(a: &SplitCost, b: &SplitCost) -> core::cmp::Ordering {
    a.cost.cmp(&b.cost)
}

/// `WISH_F(a, b, c)` (tsquery_gist.c:164).
#[inline]
fn wish_f(a: i32, b: i32, c: f64) -> f64 {
    let d = (a - b) as f64;
    -(d * d * d) * c
}

/// The owned result of [`gtsquery_picksplit`], replacing the C `GIST_SPLITVEC`'s
/// caller-allocated `spl_left`/`spl_right` arrays + union datums.
pub struct PickSplit {
    /// `v->spl_left` — the 1-based offsets assigned to the left page.
    pub spl_left: Vec<u16>,
    /// `v->spl_right` — the 1-based offsets assigned to the right page.
    pub spl_right: Vec<u16>,
    /// `v->spl_ldatum` — the left union signature.
    pub spl_ldatum: TSQuerySign,
    /// `v->spl_rdatum` — the right union signature.
    pub spl_rdatum: TSQuerySign,
}

/// `gtsquery_picksplit` (tsquery_gist.c:166) — GiST `picksplit`. `entrysign`
/// holds the signature of every entry in the `GistEntryVector`, indexed exactly
/// as the C `GETENTRY(entryvec, pos)` (`entryvec->vector[pos].key`), so
/// `entrysign.len() == entryvec->n`. The C `OffsetNumber`s are 1-based.
pub fn gtsquery_picksplit(mcx: Mcx<'_>, entrysign: &[TSQuerySign]) -> PgResult<PickSplit> {
    // OffsetNumber maxoff = entryvec->n - 2;
    let n = entrysign.len() as i32;
    let mut maxoff: i32 = n - 2;

    // GETENTRY(vec, pos) is entryvec->vector[pos].key — 0-based in C; the loops
    // run over 1-based OffsetNumbers, so we read entrysign[k] for offset k.
    let getentry = |pos: i32| -> TSQuerySign { entrysign[pos as usize] };

    let nbytes = (maxoff + 2) as usize;
    let mut left: Vec<u16> = Vec::new();
    let mut right: Vec<u16> = Vec::new();
    left.try_reserve(nbytes).map_err(|_| oom())?;
    right.try_reserve(nbytes).map_err(|_| oom())?;
    let mut spl_nleft = 0i32;
    let mut spl_nright = 0i32;

    let mut waste: i32 = -1;
    let mut seed_1: i32 = 0;
    let mut seed_2: i32 = 0;

    // for (k = FirstOffsetNumber; k < maxoff; k++)
    //   for (j = k+1; j <= maxoff; j++)
    let mut k = 1;
    while k < maxoff {
        let mut j = k + 1;
        while j <= maxoff {
            let size_waste = hemdist(getentry(j), getentry(k));
            if size_waste > waste {
                waste = size_waste;
                seed_1 = k;
                seed_2 = j;
            }
            j += 1;
        }
        k += 1;
    }

    if seed_1 == 0 || seed_2 == 0 {
        seed_1 = 1;
        seed_2 = 2;
    }

    let mut datum_l = getentry(seed_1);
    let mut datum_r = getentry(seed_2);

    // maxoff = OffsetNumberNext(maxoff);
    maxoff += 1;
    let mut costvector: PgVec<'_, SplitCost> =
        vec_with_capacity_in(mcx, maxoff as usize).map_err(|_| oom())?;
    // for (j = FirstOffsetNumber; j <= maxoff; j++)
    let mut j = 1;
    while j <= maxoff {
        let size_alpha = hemdist(getentry(seed_1), getentry(j));
        let size_beta = hemdist(getentry(seed_2), getentry(j));
        costvector.push(SplitCost {
            pos: j as u16,
            cost: (size_alpha - size_beta).abs(),
        });
        j += 1;
    }
    costvector.sort_by(comparecost);

    // for (k = 0; k < maxoff; k++)
    for k in 0..maxoff as usize {
        let jj = costvector[k].pos as i32;
        if jj == seed_1 {
            left.push(jj as u16);
            spl_nleft += 1;
            continue;
        } else if jj == seed_2 {
            right.push(jj as u16);
            spl_nright += 1;
            continue;
        }
        let size_alpha = hemdist(datum_l, getentry(jj));
        let size_beta = hemdist(datum_r, getentry(jj));

        if (size_alpha as f64) < size_beta as f64 + wish_f(spl_nleft, spl_nright, 0.05) {
            datum_l |= getentry(jj);
            left.push(jj as u16);
            spl_nleft += 1;
        } else {
            datum_r |= getentry(jj);
            right.push(jj as u16);
            spl_nright += 1;
        }
    }

    // C writes the final sentinel `*right = *left = FirstOffsetNumber;` into the
    // slot past the assigned offsets; the owned-Vec model carries exactly the
    // assigned offsets (spl_nleft/spl_nright), so that scratch write is inert.

    Ok(PickSplit {
        spl_left: left,
        spl_right: right,
        spl_ldatum: datum_l,
        spl_rdatum: datum_r,
    })
}

/// `gtsquery_consistent_oldsig` (tsquery_gist.c:272) — the pre-9.6 signature
/// compatibility shim; tail-calls [`gtsquery_consistent`] with the same args.
pub fn gtsquery_consistent_oldsig(
    key: TSQuerySign,
    query: &[u8],
    strategy: u16,
    is_leaf: bool,
) -> PgResult<(bool, bool)> {
    gtsquery_consistent(key, query, strategy, is_leaf)
}

/// Out-of-memory error for a guarded allocation (mirrors the shared helper).
fn oom() -> types_error::PgError {
    backend_utils_adt_ts_small::util::oom()
}
