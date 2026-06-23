//! Multi-column GiST page-split decision algorithm (`access/gist/gistsplit.c`).
//!
//! The opclass-specific PickSplit methods only split on a single column. We run
//! PickSplit for column 1, then (for multi-column indexes) look for tuples that
//! are "don't cares" for the column-1 split — they could go either side for zero
//! penalty — and redistribute them on the basis of the next column, recursively.
//!
//! [`gistSplitByKey`] is the entry point.
//!
//! Model notes (owned tree vs. C):
//!   * `IndexTuple *itup` becomes `&[&[u8]]` — a slice of on-disk index-tuple
//!     byte images (what `index_form_tuple` / `gistextractpage` produce).
//!   * The user PickSplit method is reached through the typed dispatch seam
//!     `gist_picksplit` keyed on `giststate.picksplitFn[attno].fn_oid`.
//!   * `entryvec->vector[0]` goes unused (the C code is 1-based); we keep a
//!     placeholder entry at index 0 so the offsets line up with C.
//!   * `Datum` union values are `Option<Datum>`; `None` is C's never-read datum
//!     for an all-null column (`spl_*isnull` is true then).

use alloc::vec::Vec;
use utils_error::{ereport, PgResult};
use ::mcx::Mcx;
use ::types_core::primitive::{InvalidBlockNumber, InvalidOid, OffsetNumber};
use ::types_error::error::{DEBUG1, ERRCODE_INTERNAL_ERROR};
use gist::{
    GistEntryVector, GistSplitVector, GISTENTRY, GISTSTATE, GIST_SPLITVEC,
};
use ::rel::Relation;
use ::types_tuple::heaptuple::Datum;
use ::types_tuple::heaptuple::FIRST_OFFSET_NUMBER;

use crate::gistutil::{
    gistKeyIsEQ, gistMakeUnionItVec, gistMakeUnionKey, gistdentryinit, gistDeCompressAtt,
    gistpenalty, index_getattr_pub,
};

/// `GistSplitUnion` (gistsplit.c) — subroutine context for `gistunionsubkey`.
/// `entries`/`len`/`attr`/`isnull` reference into the `GistSplitVector` being
/// recomputed; `dontcare` is the optional don't-care mask.
struct GistSplitUnion<'a, 'mcx> {
    entries: &'a [OffsetNumber],
    attr: &'a mut Vec<Option<Datum<'mcx>>>,
    isnull: &'a mut Vec<bool>,
    dontcare: Option<&'a [bool]>,
}

/// `gistunionsubkeyvec(giststate, itvec, gsvp)` (gistsplit.c:46): form unions of
/// the subkeys in the `itvec` entries listed in `gsvp.entries`, ignoring any
/// tuples marked in `gsvp.dontcare`.
fn gistunionsubkeyvec<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    itvec: &[&[u8]],
    gsvp: &mut GistSplitUnion<'_, 'mcx>,
) -> PgResult<()> {
    let mut cleaned: Vec<&[u8]> = Vec::with_capacity(gsvp.entries.len());

    for &e in gsvp.entries.iter() {
        if let Some(dc) = gsvp.dontcare {
            if dc[e as usize] {
                continue;
            }
        }
        // cleanedItVec[cleanedLen++] = itvec[entries[i] - 1];
        cleaned.push(itvec[(e - 1) as usize]);
    }

    let (attr, isnull) = gistMakeUnionItVec(mcx, giststate, &cleaned)?;
    *gsvp.attr = attr.into_iter().map(Some).collect();
    *gsvp.isnull = isnull;
    Ok(())
}

/// `gistunionsubkey(giststate, itvec, spl)` (gistsplit.c:79): recompute the
/// unions of the left- and right-side subkeys after a page split, ignoring any
/// tuples marked in `spl.spl_dontcare`. Always recomputes all index columns.
fn gistunionsubkey<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    itvec: &[&[u8]],
    spl: &mut GistSplitVector<'mcx>,
) -> PgResult<()> {
    let dontcare: Option<Vec<bool>> = if spl.spl_dontcare.is_empty() {
        None
    } else {
        Some(spl.spl_dontcare.clone())
    };

    // left
    {
        let entries = spl.splitVector.spl_left.clone();
        let mut gsvp = GistSplitUnion {
            entries: &entries,
            attr: &mut spl.spl_lattr,
            isnull: &mut spl.spl_lisnull,
            dontcare: dontcare.as_deref(),
        };
        gistunionsubkeyvec(mcx, giststate, itvec, &mut gsvp)?;
    }
    // right
    {
        let entries = spl.splitVector.spl_right.clone();
        let mut gsvp = GistSplitUnion {
            entries: &entries,
            attr: &mut spl.spl_rattr,
            isnull: &mut spl.spl_risnull,
            dontcare: dontcare.as_deref(),
        };
        gistunionsubkeyvec(mcx, giststate, itvec, &mut gsvp)?;
    }
    Ok(())
}

/// `findDontCares(r, giststate, valvec, spl, attno)` (gistsplit.c:112): find the
/// tuples that could move to the other side of the split with zero penalty so
/// far as the `attno` column is concerned, marking them in `spl.spl_dontcare`.
/// Returns the number found. The `attno` column is known all-not-null here.
fn find_dont_cares<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    valvec: &[GISTENTRY<'mcx>],
    spl: &mut GistSplitVector<'mcx>,
    attno: usize,
) -> PgResult<i32> {
    let mut num_dont_care = 0;

    // First, left-side tuples vs. the right-side union key.
    let entry = GISTENTRY {
        key: spl.splitVector.spl_rdatum.clone().unwrap_or(Datum::ByVal(0)),
        rel: InvalidOid,
        page: InvalidBlockNumber,
        offset: 0,
        leafkey: false,
    };
    for i in 0..spl.splitVector.spl_left.len() {
        let j = spl.splitVector.spl_left[i] as usize;
        let penalty = gistpenalty(mcx, giststate, attno, &entry, false, &valvec[j], false)?;
        if penalty == 0.0 {
            spl.spl_dontcare[j] = true;
            num_dont_care += 1;
        }
    }

    // And conversely for the right-side tuples vs. the left-side union key.
    let entry = GISTENTRY {
        key: spl.splitVector.spl_ldatum.clone().unwrap_or(Datum::ByVal(0)),
        rel: InvalidOid,
        page: InvalidBlockNumber,
        offset: 0,
        leafkey: false,
    };
    for i in 0..spl.splitVector.spl_right.len() {
        let j = spl.splitVector.spl_right[i] as usize;
        let penalty = gistpenalty(mcx, giststate, attno, &entry, false, &valvec[j], false)?;
        if penalty == 0.0 {
            spl.spl_dontcare[j] = true;
            num_dont_care += 1;
        }
    }

    Ok(num_dont_care)
}

/// `removeDontCares(a, len, dontcare)` (gistsplit.c:166): remove the tuples
/// marked don't-care from a tuple-index array (`spl_left` or `spl_right`).
fn remove_dont_cares(a: &mut Vec<OffsetNumber>, dontcare: &[bool]) {
    a.retain(|&ai| !dontcare[ai as usize]);
}

/// `placeOne(r, giststate, v, itup, off, attno)` (gistsplit.c:199): place a
/// single don't-care tuple into either side of the split, according to which has
/// the least penalty for merging the tuple into the previously-computed union
/// keys. Only columns from `attno` onward are considered.
fn place_one<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    v: &mut GistSplitVector<'mcx>,
    itup: &[u8],
    off: OffsetNumber,
    mut attno: usize,
) -> PgResult<()> {
    let (identry, isnull) = gistDeCompressAtt(mcx, giststate, r, itup, InvalidBlockNumber, 0)?;
    let mut to_left = true;

    let ncols = giststate
        .nonLeafTupdesc
        .as_ref()
        .expect("placeOne: nonLeafTupdesc")
        .as_ref()
        .natts as usize;

    while attno < ncols {
        let lentry = GISTENTRY {
            key: v.spl_lattr[attno].clone().unwrap_or(Datum::ByVal(0)),
            rel: InvalidOid,
            page: InvalidBlockNumber,
            offset: 0,
            leafkey: false,
        };
        let lpenalty = gistpenalty(
            mcx,
            giststate,
            attno,
            &lentry,
            v.spl_lisnull[attno],
            &identry[attno],
            isnull[attno],
        )?;
        let rentry = GISTENTRY {
            key: v.spl_rattr[attno].clone().unwrap_or(Datum::ByVal(0)),
            rel: InvalidOid,
            page: InvalidBlockNumber,
            offset: 0,
            leafkey: false,
        };
        let rpenalty = gistpenalty(
            mcx,
            giststate,
            attno,
            &rentry,
            v.spl_risnull[attno],
            &identry[attno],
            isnull[attno],
        )?;

        if lpenalty != rpenalty {
            if lpenalty > rpenalty {
                to_left = false;
            }
            break;
        }
        attno += 1;
    }

    if to_left {
        v.splitVector.spl_left.push(off);
    } else {
        v.splitVector.spl_right.push(off);
    }
    Ok(())
}

/// `supportSecondarySplit(r, giststate, attno, sv, oldL, oldR)` (gistsplit.c:257):
/// clean up after a secondary split where the user PickSplit method didn't
/// support it (leaving `spl_ldatum_exists`/`spl_rdatum_exists` true). May swap
/// left/right outputs, and updates the union datums by adding the previous union
/// keys (`oldL`/`oldR`).
fn support_secondary_split<'mcx>(
    mcx: Mcx<'mcx>,
    _r: &Relation<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    attno: usize,
    sv: &mut GIST_SPLITVEC<'mcx>,
    old_l: Option<Datum<'mcx>>,
    old_r: Option<Datum<'mcx>>,
) -> PgResult<()> {
    let mut leave_on_left = true;

    let entry_l = GISTENTRY {
        key: old_l.clone().unwrap_or(Datum::ByVal(0)),
        rel: InvalidOid,
        page: InvalidBlockNumber,
        offset: 0,
        leafkey: false,
    };
    let entry_r = GISTENTRY {
        key: old_r.clone().unwrap_or(Datum::ByVal(0)),
        rel: InvalidOid,
        page: InvalidBlockNumber,
        offset: 0,
        leafkey: false,
    };
    let entry_sl = GISTENTRY {
        key: sv.spl_ldatum.clone().unwrap_or(Datum::ByVal(0)),
        rel: InvalidOid,
        page: InvalidBlockNumber,
        offset: 0,
        leafkey: false,
    };
    let entry_sr = GISTENTRY {
        key: sv.spl_rdatum.clone().unwrap_or(Datum::ByVal(0)),
        rel: InvalidOid,
        page: InvalidBlockNumber,
        offset: 0,
        leafkey: false,
    };

    if sv.spl_ldatum_exists && sv.spl_rdatum_exists {
        let penalty1 = gistpenalty(mcx, giststate, attno, &entry_l, false, &entry_sl, false)?
            + gistpenalty(mcx, giststate, attno, &entry_r, false, &entry_sr, false)?;
        let penalty2 = gistpenalty(mcx, giststate, attno, &entry_l, false, &entry_sr, false)?
            + gistpenalty(mcx, giststate, attno, &entry_r, false, &entry_sl, false)?;

        if penalty1 > penalty2 {
            leave_on_left = false;
        }
    } else {
        // Only one previously-defined union; choose swap-or-not by lowest
        // penalty for that side.
        let entry1 = if sv.spl_ldatum_exists { &entry_l } else { &entry_r };
        let penalty1 = gistpenalty(mcx, giststate, attno, entry1, false, &entry_sl, false)?;
        let penalty2 = gistpenalty(mcx, giststate, attno, entry1, false, &entry_sr, false)?;

        if penalty1 < penalty2 {
            leave_on_left = sv.spl_ldatum_exists;
        } else {
            leave_on_left = sv.spl_rdatum_exists;
        }
    }

    // entry_sl / entry_sr are recomputed after a swap below.
    let mut entry_sl = entry_sl;
    let mut entry_sr = entry_sr;

    if !leave_on_left {
        // swap left and right
        core::mem::swap(&mut sv.spl_left, &mut sv.spl_right);
        core::mem::swap(&mut sv.spl_ldatum, &mut sv.spl_rdatum);
        entry_sl = GISTENTRY {
            key: sv.spl_ldatum.clone().unwrap_or(Datum::ByVal(0)),
            rel: InvalidOid,
            page: InvalidBlockNumber,
            offset: 0,
            leafkey: false,
        };
        entry_sr = GISTENTRY {
            key: sv.spl_rdatum.clone().unwrap_or(Datum::ByVal(0)),
            rel: InvalidOid,
            page: InvalidBlockNumber,
            offset: 0,
            leafkey: false,
        };
    }

    if sv.spl_ldatum_exists {
        let (d, _) =
            gistMakeUnionKey(mcx, giststate, attno, &entry_l, false, &entry_sl, false)?;
        sv.spl_ldatum = Some(d);
    }
    if sv.spl_rdatum_exists {
        let (d, _) =
            gistMakeUnionKey(mcx, giststate, attno, &entry_r, false, &entry_sr, false)?;
        sv.spl_rdatum = Some(d);
    }

    sv.spl_ldatum_exists = false;
    sv.spl_rdatum_exists = false;
    Ok(())
}

/// `genericPickSplit(giststate, entryvec, v, attno)` (gistsplit.c:343): trivial
/// PickSplit (split in half + form union datums). Called only when a user-
/// defined PickSplit put all keys on the same side (a bug we don't want to fail
/// on).
fn generic_pick_split<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    entryvec: &GistEntryVector<'mcx>,
    v: &mut GIST_SPLITVEC<'mcx>,
    attno: usize,
) -> PgResult<()> {
    let maxoff = (entryvec.n - 1) as OffsetNumber;

    v.spl_left.clear();
    v.spl_right.clear();

    let mut i = FIRST_OFFSET_NUMBER;
    while i <= maxoff {
        if i <= (maxoff - FIRST_OFFSET_NUMBER + 1) / 2 {
            v.spl_left.push(i);
        } else {
            v.spl_right.push(i);
        }
        i += 1;
    }

    // Form union datums for each side. entryvec->vector + FirstOffsetNumber is
    // index 1 (vector[0] unused).
    let left_n = v.spl_left.len();
    let right_n = v.spl_right.len();

    let mut evec = GistEntryVector {
        n: left_n as i32,
        vector: entryvec.vector[FIRST_OFFSET_NUMBER as usize
            ..FIRST_OFFSET_NUMBER as usize + left_n]
            .to_vec(),
    };
    let proc_oid = giststate.unionFn[attno].fn_oid;
    v.spl_ldatum = Some(dispatch_seams::gist_union::call(
        mcx,
        proc_oid,
        giststate.supportCollation[attno],
        &evec,
    )?);

    evec.n = right_n as i32;
    evec.vector = entryvec.vector[FIRST_OFFSET_NUMBER as usize + left_n
        ..FIRST_OFFSET_NUMBER as usize + left_n + right_n]
        .to_vec();
    v.spl_rdatum = Some(dispatch_seams::gist_union::call(
        mcx,
        proc_oid,
        giststate.supportCollation[attno],
        &evec,
    )?);

    Ok(())
}

/// `gistUserPicksplit(r, entryvec, attno, v, itup, len, giststate)`
/// (gistsplit.c:414): call the user PickSplit method for `attno` to split tuples
/// into two vectors. See the C comment for the meaning of the boolean result and
/// `v.spl_dontcare` (empty here means C's NULL).
#[allow(clippy::too_many_arguments)]
fn gist_user_picksplit<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    entryvec: &GistEntryVector<'mcx>,
    attno: usize,
    v: &mut GistSplitVector<'mcx>,
    itup: &[&[u8]],
    giststate: &GISTSTATE<'mcx>,
) -> PgResult<bool> {
    // Prepare spl_ldatum/spl_rdatum/exists in case of a secondary split.
    v.splitVector.spl_ldatum_exists = !v.spl_lisnull[attno];
    v.splitVector.spl_rdatum_exists = !v.spl_risnull[attno];
    v.splitVector.spl_ldatum = v.spl_lattr[attno].clone();
    v.splitVector.spl_rdatum = v.spl_rattr[attno].clone();

    // Let the opclass-specific PickSplit do its thing. No null keys in entryvec.
    let proc_oid = giststate.picksplitFn[attno].fn_oid;
    dispatch_seams::gist_picksplit::call(
        mcx,
        proc_oid,
        giststate.supportCollation[attno],
        entryvec,
        &mut v.splitVector,
    )?;

    if v.splitVector.spl_left.is_empty() || v.splitVector.spl_right.is_empty() {
        // User PickSplit put everything on the same side. Complain but cope.
        // ereport(DEBUG1, ...) is a log, not a throw; logging is not modeled.
        let _ = ereport(DEBUG1)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(alloc::format!(
                "picksplit method for column {} of index \"{}\" failed",
                attno + 1,
                r.name()
            ))
            .errhint(
                "The index is not optimal. To optimize it, contact a developer, \
                 or try to use the column as the second one in the CREATE INDEX command.",
            );

        // Reinit GIST_SPLITVEC for further processing.
        v.splitVector.spl_ldatum_exists = !v.spl_lisnull[attno];
        v.splitVector.spl_rdatum_exists = !v.spl_risnull[attno];
        v.splitVector.spl_ldatum = v.spl_lattr[attno].clone();
        v.splitVector.spl_rdatum = v.spl_rattr[attno].clone();

        generic_pick_split(mcx, giststate, entryvec, &mut v.splitVector, attno)?;
    } else {
        // Hack for compatibility with old picksplit API: a trailing
        // InvalidOffsetNumber means "the last entry".
        let last_l = v.splitVector.spl_left.len() - 1;
        if v.splitVector.spl_left[last_l] == ::types_tuple::heaptuple::INVALID_OFFSET_NUMBER {
            v.splitVector.spl_left[last_l] = (entryvec.n - 1) as OffsetNumber;
        }
        let last_r = v.splitVector.spl_right.len() - 1;
        if v.splitVector.spl_right[last_r] == ::types_tuple::heaptuple::INVALID_OFFSET_NUMBER {
            v.splitVector.spl_right[last_r] = (entryvec.n - 1) as OffsetNumber;
        }
    }

    // Clean up if PickSplit didn't take care of a secondary split.
    if v.splitVector.spl_ldatum_exists || v.splitVector.spl_rdatum_exists {
        let old_l = v.spl_lattr[attno].clone();
        let old_r = v.spl_rattr[attno].clone();
        support_secondary_split(mcx, r, giststate, attno, &mut v.splitVector, old_l, old_r)?;
    }

    // Emit union datums computed by PickSplit back to v arrays.
    v.spl_lattr[attno] = v.splitVector.spl_ldatum.clone();
    v.spl_rattr[attno] = v.splitVector.spl_rdatum.clone();
    v.spl_lisnull[attno] = false;
    v.spl_risnull[attno] = false;

    // If index columns remain, consider whether we can improve the split.
    v.spl_dontcare = Vec::new();

    let ncols = giststate
        .nonLeafTupdesc
        .as_ref()
        .expect("gistUserPicksplit: nonLeafTupdesc")
        .as_ref()
        .natts as usize;

    if attno + 1 < ncols {
        // Quick check: if left and right union keys are equal, the split is
        // certainly degenerate.
        let l = v.splitVector.spl_ldatum.clone().unwrap_or(Datum::ByVal(0));
        let rr = v.splitVector.spl_rdatum.clone().unwrap_or(Datum::ByVal(0));
        if gistKeyIsEQ(mcx, giststate, attno, &l, &rr)? {
            return Ok(true);
        }

        // Locate don't-care tuples, if any.
        v.spl_dontcare = alloc::vec![false; (entryvec.n + 1) as usize];

        let num_dont_care = find_dont_cares(mcx, giststate, &entryvec.vector, v, attno)?;

        if num_dont_care > 0 {
            let dontcare = v.spl_dontcare.clone();
            remove_dont_cares(&mut v.splitVector.spl_left, &dontcare);
            remove_dont_cares(&mut v.splitVector.spl_right, &dontcare);

            // If all tuples on either side were don't-cares, the split is
            // degenerate; ignore it and split on the next column.
            if v.splitVector.spl_left.is_empty() || v.splitVector.spl_right.is_empty() {
                v.spl_dontcare = Vec::new();
                return Ok(true);
            }

            // Recompute union keys, considering only non-don't-care tuples.
            gistunionsubkey(mcx, giststate, itup, v)?;

            if num_dont_care == 1 {
                // Only one don't-care tuple — can't PickSplit it, just choose a
                // side by comparing penalties.
                let mut to_move = FIRST_OFFSET_NUMBER;
                while (to_move as i32) < entryvec.n {
                    if v.spl_dontcare[to_move as usize] {
                        break;
                    }
                    to_move += 1;
                }
                debug_assert!((to_move as i32) < entryvec.n);

                place_one(
                    mcx,
                    r,
                    giststate,
                    v,
                    itup[(to_move - 1) as usize],
                    to_move,
                    attno + 1,
                )?;
                // Union keys are wrong now, but we're done splitting; the
                // outermost gistSplitByKey level fixes things before returning.
            } else {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// `gistSplitHalf(v, len)` (gistsplit.c:584): simply split the page in half.
/// Caller computes union keys.
fn gist_split_half(v: &mut GIST_SPLITVEC<'_>, len: i32) {
    v.spl_right.clear();
    v.spl_left.clear();
    for i in 1..=len {
        if i < len / 2 {
            v.spl_right.push(i as OffsetNumber);
        } else {
            v.spl_left.push(i as OffsetNumber);
        }
    }
}

/// `gistSplitByKey(r, page, itup, len, giststate, v, attno)` (gistsplit.c:622):
/// main entry point for the page-splitting algorithm.
///
/// `page` is only used to stamp the per-entry `GISTENTRY.page` (block number);
/// `page_blkno` carries it in the owned model. Outside callers pass `attno == 0`
/// and must initialize `v.spl_lisnull`/`v.spl_risnull` all-true.
#[allow(clippy::too_many_arguments)]
pub fn gistSplitByKey<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    page_blkno: ::types_core::primitive::BlockNumber,
    itup: &[&[u8]],
    len: i32,
    giststate: &GISTSTATE<'mcx>,
    v: &mut GistSplitVector<'mcx>,
    attno: usize,
) -> PgResult<()> {
    // generate the item array, identify tuples with null keys.
    // entryvec->vector[0] goes unused; we keep a placeholder there.
    let placeholder = GISTENTRY {
        key: Datum::ByVal(0),
        rel: InvalidOid,
        page: InvalidBlockNumber,
        offset: 0,
        leafkey: false,
    };
    let mut entryvec = GistEntryVector {
        n: len + 1,
        vector: Vec::with_capacity((len + 1) as usize),
    };
    entryvec.vector.push(placeholder);

    let mut off_null_tuples: Vec<OffsetNumber> = Vec::with_capacity(len as usize);

    for i in 1..=len {
        let (datum, is_null) =
            index_getattr_pub(mcx, itup[(i - 1) as usize], (attno + 1) as i32, giststate)?;
        let entry = gistdentryinit(
            mcx,
            giststate,
            attno,
            datum,
            r.rd_id,
            page_blkno,
            i as OffsetNumber,
            false,
            is_null,
        )?;
        entryvec.vector.push(entry);
        if is_null {
            off_null_tuples.push(i as OffsetNumber);
        }
    }

    let ncols = giststate
        .nonLeafTupdesc
        .as_ref()
        .expect("gistSplitByKey: nonLeafTupdesc")
        .as_ref()
        .natts as usize;

    if off_null_tuples.len() == len as usize {
        // Corner case: all keys in attno column are null; transfer attention to
        // the next column, or split in half if no next column.
        v.spl_risnull[attno] = true;
        v.spl_lisnull[attno] = true;

        if attno + 1 < ncols {
            gistSplitByKey(mcx, r, page_blkno, itup, len, giststate, v, attno + 1)?;
        } else {
            gist_split_half(&mut v.splitVector, len);
        }
    } else if !off_null_tuples.is_empty() {
        // Don't mix NULL and not-NULL keys on one page: nulls to right, not-nulls
        // to left.
        v.splitVector.spl_right = off_null_tuples.clone();
        v.spl_risnull[attno] = true;

        v.splitVector.spl_left = Vec::with_capacity(len as usize);
        let mut j = 0usize;
        for i in 1..=len {
            if j < v.splitVector.spl_right.len()
                && off_null_tuples[j] == i as OffsetNumber
            {
                j += 1;
            } else {
                v.splitVector.spl_left.push(i as OffsetNumber);
            }
        }

        // Compute union keys unless an outer recursion level will handle it.
        if attno == 0 && ncols == 1 {
            v.spl_dontcare = Vec::new();
            gistunionsubkey(mcx, giststate, itup, v)?;
        }
    } else {
        // All keys not-null, so apply user PickSplit.
        if gist_user_picksplit(mcx, r, &entryvec, attno, v, itup, giststate)? {
            // Splitting on attno is not optimal; redistribute don't-cares
            // according to the next column.
            debug_assert!(attno + 1 < ncols);

            if v.spl_dontcare.is_empty() {
                // Degenerate; ignore and split according to the next column.
                gistSplitByKey(mcx, r, page_blkno, itup, len, giststate, v, attno + 1)?;
            } else {
                // Form an array of just the don't-care tuples for a recursive
                // call on the next column.
                let mut newitup: Vec<&[u8]> = Vec::with_capacity(len as usize);
                let mut map: Vec<OffsetNumber> = Vec::with_capacity(len as usize);
                for i in 0..len as usize {
                    if v.spl_dontcare[i + 1] {
                        newitup.push(itup[i]);
                        map.push((i + 1) as OffsetNumber);
                    }
                }
                let newlen = newitup.len();
                debug_assert!(newlen > 0);

                // Backup copy of v.splitVector; the recursive call overwrites it.
                let mut backup_left = v.splitVector.spl_left.clone();
                let mut backup_right = v.splitVector.spl_right.clone();

                gistSplitByKey(
                    mcx,
                    r,
                    page_blkno,
                    &newitup,
                    newlen as i32,
                    giststate,
                    v,
                    attno + 1,
                )?;

                // Merge result of subsplit with non-don't-care tuples.
                for i in 0..v.splitVector.spl_left.len() {
                    backup_left.push(map[(v.splitVector.spl_left[i] - 1) as usize]);
                }
                for i in 0..v.splitVector.spl_right.len() {
                    backup_right.push(map[(v.splitVector.spl_right[i] - 1) as usize]);
                }

                v.splitVector.spl_left = backup_left;
                v.splitVector.spl_right = backup_right;
            }
        }
    }

    // At the end of the outermost recursion in a multicolumn index, recompute the
    // left and right union datums for all index columns.
    if attno == 0 && ncols > 1 {
        v.spl_dontcare = Vec::new();
        gistunionsubkey(mcx, giststate, itup, v)?;
    }

    Ok(())
}
