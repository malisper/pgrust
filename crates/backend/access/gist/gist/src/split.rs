//! gistsplit.c: multi-column page-split decisions around the opclass
//! picksplit method.

use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::fmgr::INDEX_MAX_KEYS;
use ::types_core::OffsetNumber;
use ::types_error::PgResult;
use ::types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};
use ::types_rel::Relation;

use crate::state::GistState;
use crate::util::{
    gistDeCompressAtt, gistKeyIsEQ, gistMakeUnionItVec, gistMakeUnionKey, gist_index_getattr,
    gistdentryinit, gistpenalty, FirstOffsetNumber, ITup,
};

const K: usize = INDEX_MAX_KEYS as usize;
const InvalidOffsetNumber: OffsetNumber = 0;

pub struct GistSplitVector {
    pub splitVector: GistSplitVec,
    pub spl_lattr: [Datum; K],
    pub spl_lisnull: [bool; K],
    pub spl_rattr: [Datum; K],
    pub spl_risnull: [bool; K],
    pub spl_dontcare: Option<Vec<bool>>,
}

impl GistSplitVector {
    pub fn new() -> Self {
        GistSplitVector {
            splitVector: GistSplitVec::default(),
            spl_lattr: [Datum::null(); K],
            spl_lisnull: [true; K],
            spl_rattr: [Datum::null(); K],
            spl_risnull: [true; K],
            spl_dontcare: None,
        }
    }
}

// gistunionsubkeyvec.
fn gistunionsubkeyvec(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    itvec: &[ITup],
    entries: &[OffsetNumber],
    dontcare: Option<&[bool]>,
    attr: &mut [Datum],
    isnull: &mut [bool],
) -> PgResult<()> {
    let mut cleaned: Vec<ITup> = Vec::with_capacity(entries.len());
    for &e in entries {
        if let Some(dc) = dontcare {
            if dc[e as usize] {
                continue;
            }
        }
        cleaned.push(itvec[e as usize - 1]);
    }
    gistMakeUnionItVec(mcx, giststate, &cleaned, attr, isnull)
}

// gistunionsubkey.
fn gistunionsubkey(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    itvec: &[ITup],
    spl: &mut GistSplitVector,
) -> PgResult<()> {
    let dontcare = spl.spl_dontcare.take();
    {
        let entries = core::mem::take(&mut spl.splitVector.spl_left);
        gistunionsubkeyvec(
            mcx,
            giststate,
            itvec,
            &entries,
            dontcare.as_deref(),
            &mut spl.spl_lattr,
            &mut spl.spl_lisnull,
        )?;
        spl.splitVector.spl_left = entries;
    }
    {
        let entries = core::mem::take(&mut spl.splitVector.spl_right);
        gistunionsubkeyvec(
            mcx,
            giststate,
            itvec,
            &entries,
            dontcare.as_deref(),
            &mut spl.spl_rattr,
            &mut spl.spl_risnull,
        )?;
        spl.splitVector.spl_right = entries;
    }
    spl.spl_dontcare = dontcare;
    Ok(())
}

// findDontCares; attno column is known all-not-null.
fn findDontCares(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    valvec: &[GISTENTRY],
    spl: &mut GistSplitVector,
    attno: usize,
) -> PgResult<usize> {
    let mut num_dont_care = 0usize;
    let dc = spl.spl_dontcare.as_mut().expect("spl_dontcare allocated");

    let entry = GISTENTRY::init(spl.splitVector.spl_rdatum, 0, false, false);
    for i in 0..spl.splitVector.spl_left.len() {
        let j = spl.splitVector.spl_left[i] as usize;
        let penalty = gistpenalty(mcx, giststate, attno, &entry, false, &valvec[j], false)?;
        if penalty == 0.0 {
            dc[j] = true;
            num_dont_care += 1;
        }
    }

    let entry = GISTENTRY::init(spl.splitVector.spl_ldatum, 0, false, false);
    for i in 0..spl.splitVector.spl_right.len() {
        let j = spl.splitVector.spl_right[i] as usize;
        let penalty = gistpenalty(mcx, giststate, attno, &entry, false, &valvec[j], false)?;
        if penalty == 0.0 {
            dc[j] = true;
            num_dont_care += 1;
        }
    }

    Ok(num_dont_care)
}

// removeDontCares.
fn removeDontCares(a: &mut Vec<OffsetNumber>, dontcare: &[bool]) {
    a.retain(|&ai| !dontcare[ai as usize]);
}

// placeOne.
fn placeOne(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    r: &Relation<'_>,
    v: &mut GistSplitVector,
    itup: ITup,
    off: OffsetNumber,
    attno_start: usize,
) -> PgResult<()> {
    let mut identry = [GISTENTRY::default(); K];
    let mut isnull = [false; K];
    gistDeCompressAtt(mcx, giststate, r, itup, 0, &mut identry, &mut isnull)?;

    let natts = giststate.nonLeafTupdesc.natts as usize;
    let mut to_left = true;
    for attno in attno_start..natts {
        let entry = GISTENTRY::init(v.spl_lattr[attno], 0, false, false);
        let lpenalty = gistpenalty(
            mcx,
            giststate,
            attno,
            &entry,
            v.spl_lisnull[attno],
            &identry[attno],
            isnull[attno],
        )?;
        let entry = GISTENTRY::init(v.spl_rattr[attno], 0, false, false);
        let rpenalty = gistpenalty(
            mcx,
            giststate,
            attno,
            &entry,
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
    }

    if to_left {
        v.splitVector.spl_left.push(off);
    } else {
        v.splitVector.spl_right.push(off);
    }
    Ok(())
}

// supportSecondarySplit.
fn supportSecondarySplit(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    attno: usize,
    sv: &mut GistSplitVec,
    old_l: Datum,
    old_r: Datum,
) -> PgResult<()> {
    let mut leave_on_left = true;

    let entry_l = GISTENTRY::init(old_l, 0, false, false);
    let entry_r = GISTENTRY::init(old_r, 0, false, false);
    let mut entry_sl = GISTENTRY::init(sv.spl_ldatum, 0, false, false);
    let mut entry_sr = GISTENTRY::init(sv.spl_rdatum, 0, false, false);

    if sv.spl_ldatum_exists && sv.spl_rdatum_exists {
        let penalty1 = gistpenalty(mcx, giststate, attno, &entry_l, false, &entry_sl, false)?
            + gistpenalty(mcx, giststate, attno, &entry_r, false, &entry_sr, false)?;
        let penalty2 = gistpenalty(mcx, giststate, attno, &entry_l, false, &entry_sr, false)?
            + gistpenalty(mcx, giststate, attno, &entry_r, false, &entry_sl, false)?;
        if penalty1 > penalty2 {
            leave_on_left = false;
        }
    } else {
        let entry1 = if sv.spl_ldatum_exists { &entry_l } else { &entry_r };
        let entry1 = *entry1;
        let penalty1 = gistpenalty(mcx, giststate, attno, &entry1, false, &entry_sl, false)?;
        let penalty2 = gistpenalty(mcx, giststate, attno, &entry1, false, &entry_sr, false)?;
        leave_on_left = if penalty1 < penalty2 {
            sv.spl_ldatum_exists
        } else {
            sv.spl_rdatum_exists
        };
    }

    if !leave_on_left {
        core::mem::swap(&mut sv.spl_left, &mut sv.spl_right);
        core::mem::swap(&mut sv.spl_ldatum, &mut sv.spl_rdatum);
        entry_sl = GISTENTRY::init(sv.spl_ldatum, 0, false, false);
        entry_sr = GISTENTRY::init(sv.spl_rdatum, 0, false, false);
    }

    if sv.spl_ldatum_exists {
        let (d, _null) =
            gistMakeUnionKey(mcx, giststate, attno, &entry_l, false, &entry_sl, false)?;
        sv.spl_ldatum = d;
    }
    if sv.spl_rdatum_exists {
        let (d, _null) =
            gistMakeUnionKey(mcx, giststate, attno, &entry_r, false, &entry_sr, false)?;
        sv.spl_rdatum = d;
    }

    sv.spl_ldatum_exists = false;
    sv.spl_rdatum_exists = false;
    Ok(())
}

// genericPickSplit: fallback when the opclass put everything on one side.
fn genericPickSplit(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    entryvec: &GistEntryVector,
    v: &mut GistSplitVec,
    attno: usize,
) -> PgResult<()> {
    let maxoff = (entryvec.n - 1) as OffsetNumber;

    v.spl_left = Vec::with_capacity(maxoff as usize + 2);
    v.spl_right = Vec::with_capacity(maxoff as usize + 2);

    for i in FirstOffsetNumber..=maxoff {
        if i <= (maxoff - FirstOffsetNumber + 1) / 2 {
            v.spl_left.push(i);
        } else {
            v.spl_right.push(i);
        }
    }

    // union's evec is DENSE 0-based (picksplit's entryvec is 1-based).
    let nleft = v.spl_left.len();
    {
        let evec = GistEntryVector {
            n: nleft as i32,
            vector: entryvec.vector[1..1 + nleft].to_vec(),
        };
        v.spl_ldatum = giststate.call_union(mcx, attno, &evec)?;
    }
    {
        let nright = v.spl_right.len();
        let evec = GistEntryVector {
            n: nright as i32,
            vector: entryvec.vector[1 + nleft..1 + nleft + nright].to_vec(),
        };
        v.spl_rdatum = giststate.call_union(mcx, attno, &evec)?;
    }
    Ok(())
}

// gistUserPicksplit; returns true when caller should re-split on the next
// column (spl_dontcare None = degenerate).
#[allow(clippy::too_many_arguments)]
fn gistUserPicksplit(
    mcx: Mcx<'_>,
    r: &Relation<'_>,
    entryvec: &GistEntryVector,
    attno: usize,
    v: &mut GistSplitVector,
    itup: &[ITup],
    giststate: &mut GistState<'_>,
) -> PgResult<bool> {
    {
        let sv = &mut v.splitVector;
        sv.spl_ldatum_exists = !v.spl_lisnull[attno];
        sv.spl_rdatum_exists = !v.spl_risnull[attno];
        sv.spl_ldatum = v.spl_lattr[attno];
        sv.spl_rdatum = v.spl_rattr[attno];
        sv.spl_left.clear();
        sv.spl_right.clear();
    }

    {
        let mut sv = core::mem::take(&mut v.splitVector);
        giststate.call_picksplit(mcx, attno, entryvec, &mut sv)?;
        v.splitVector = sv;
    }

    if v.splitVector.spl_left.is_empty() || v.splitVector.spl_right.is_empty() {
        // picksplit failed to create an actual split (C DEBUG1 + cope).
        let sv = &mut v.splitVector;
        sv.spl_ldatum_exists = !v.spl_lisnull[attno];
        sv.spl_rdatum_exists = !v.spl_risnull[attno];
        sv.spl_ldatum = v.spl_lattr[attno];
        sv.spl_rdatum = v.spl_rattr[attno];
        let mut sv = core::mem::take(&mut v.splitVector);
        genericPickSplit(mcx, giststate, entryvec, &mut sv, attno)?;
        v.splitVector = sv;
    } else {
        // old picksplit API compatibility hack
        let n = (entryvec.n - 1) as OffsetNumber;
        if let Some(last) = v.splitVector.spl_left.last_mut() {
            if *last == InvalidOffsetNumber {
                *last = n;
            }
        }
        if let Some(last) = v.splitVector.spl_right.last_mut() {
            if *last == InvalidOffsetNumber {
                *last = n;
            }
        }
    }

    if v.splitVector.spl_ldatum_exists || v.splitVector.spl_rdatum_exists {
        let (old_l, old_r) = (v.spl_lattr[attno], v.spl_rattr[attno]);
        let mut sv = core::mem::take(&mut v.splitVector);
        supportSecondarySplit(mcx, giststate, attno, &mut sv, old_l, old_r)?;
        v.splitVector = sv;
    }

    v.spl_lattr[attno] = v.splitVector.spl_ldatum;
    v.spl_rattr[attno] = v.splitVector.spl_rdatum;
    v.spl_lisnull[attno] = false;
    v.spl_risnull[attno] = false;

    v.spl_dontcare = None;

    if attno + 1 < giststate.nonLeafTupdesc.natts as usize {
        if gistKeyIsEQ(
            mcx,
            giststate,
            attno,
            v.splitVector.spl_ldatum,
            v.splitVector.spl_rdatum,
        )? {
            return Ok(true);
        }

        v.spl_dontcare = Some(vec![false; entryvec.n as usize + 1]);

        let valvec = &entryvec.vector;
        let num_dont_care = {
            // findDontCares borrows dontcare mutably; entries immutably.
            let valvec: Vec<GISTENTRY> = valvec.clone();
            findDontCares(mcx, giststate, &valvec, v, attno)?
        };

        if num_dont_care > 0 {
            let dc = v.spl_dontcare.take().expect("dontcare set");
            removeDontCares(&mut v.splitVector.spl_left, &dc);
            removeDontCares(&mut v.splitVector.spl_right, &dc);
            v.spl_dontcare = Some(dc);

            if v.splitVector.spl_left.is_empty() || v.splitVector.spl_right.is_empty() {
                v.spl_dontcare = None;
                return Ok(true);
            }

            gistunionsubkey(mcx, giststate, itup, v)?;

            if num_dont_care == 1 {
                let dc = v.spl_dontcare.as_ref().expect("dontcare set");
                let mut to_move = FirstOffsetNumber;
                while (to_move as i32) < entryvec.n {
                    if dc[to_move as usize] {
                        break;
                    }
                    to_move += 1;
                }
                debug_assert!((to_move as i32) < entryvec.n);
                placeOne(
                    mcx,
                    giststate,
                    r,
                    v,
                    itup[to_move as usize - 1],
                    to_move,
                    attno + 1,
                )?;
            } else {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

// gistSplitHalf.
fn gistSplitHalf(v: &mut GistSplitVec, len: usize) {
    v.spl_left = Vec::with_capacity(len);
    v.spl_right = Vec::with_capacity(len);
    for i in 1..=len {
        if i < len / 2 {
            v.spl_right.push(i as OffsetNumber);
        } else {
            v.spl_left.push(i as OffsetNumber);
        }
    }
}

/// gistSplitByKey: outside caller passes attno == 0 and all-true
/// spl_lisnull/spl_risnull.
pub fn gistSplitByKey(
    mcx: Mcx<'_>,
    r: &Relation<'_>,
    itup: &[ITup],
    giststate: &mut GistState<'_>,
    v: &mut GistSplitVector,
    attno: usize,
) -> PgResult<()> {
    let len = itup.len();
    let mut entryvec = GistEntryVector {
        n: len as i32 + 1,
        vector: vec![GISTENTRY::default(); len + 1],
    };
    let mut off_null_tuples: Vec<OffsetNumber> = Vec::with_capacity(len);

    for i in 1..=len {
        let (datum, is_null) = gist_index_getattr(itup[i - 1], attno + 1, giststate);
        entryvec.vector[i] = gistdentryinit(
            mcx,
            giststate,
            attno,
            datum,
            i as OffsetNumber,
            false,
            false,
            is_null,
        )?;
        if is_null {
            off_null_tuples.push(i as OffsetNumber);
        }
    }
    let n_off_null_tuples = off_null_tuples.len();

    if n_off_null_tuples == len {
        // all keys in attno column are null
        v.spl_risnull[attno] = true;
        v.spl_lisnull[attno] = true;
        if attno + 1 < giststate.nonLeafTupdesc.natts as usize {
            gistSplitByKey(mcx, r, itup, giststate, v, attno + 1)?;
        } else {
            gistSplitHalf(&mut v.splitVector, len);
        }
    } else if n_off_null_tuples > 0 {
        // split nulls to the right page, not-nulls to the left
        v.splitVector.spl_right = off_null_tuples;
        v.spl_risnull[attno] = true;

        v.splitVector.spl_left = Vec::with_capacity(len);
        let mut j = 0usize;
        for i in 1..=len as OffsetNumber {
            if j < v.splitVector.spl_right.len() && v.splitVector.spl_right[j] == i {
                j += 1;
            } else {
                v.splitVector.spl_left.push(i);
            }
        }

        if attno == 0 && giststate.nonLeafTupdesc.natts == 1 {
            v.spl_dontcare = None;
            gistunionsubkey(mcx, giststate, itup, v)?;
        }
    } else {
        // all keys are not-null: user-defined picksplit
        if gistUserPicksplit(mcx, r, &entryvec, attno, v, itup, giststate)? {
            debug_assert!(attno + 1 < giststate.nonLeafTupdesc.natts as usize);

            if v.spl_dontcare.is_none() {
                gistSplitByKey(mcx, r, itup, giststate, v, attno + 1)?;
            } else {
                let dc = v.spl_dontcare.take().expect("dontcare set");
                let mut newitup: Vec<ITup> = Vec::with_capacity(len);
                let mut map: Vec<OffsetNumber> = Vec::with_capacity(len);
                for i in 0..len {
                    if dc[i + 1] {
                        newitup.push(itup[i]);
                        map.push(i as OffsetNumber + 1);
                    }
                }
                debug_assert!(!newitup.is_empty());

                let mut backup_left = v.splitVector.spl_left.clone();
                let mut backup_right = v.splitVector.spl_right.clone();

                gistSplitByKey(mcx, r, &newitup, giststate, v, attno + 1)?;

                for &l in &v.splitVector.spl_left {
                    backup_left.push(map[l as usize - 1]);
                }
                for &rr in &v.splitVector.spl_right {
                    backup_right.push(map[rr as usize - 1]);
                }
                v.splitVector.spl_left = backup_left;
                v.splitVector.spl_right = backup_right;
            }
        }
    }

    if attno == 0 && giststate.nonLeafTupdesc.natts > 1 {
        v.spl_dontcare = None;
        gistunionsubkey(mcx, giststate, itup, v)?;
    }
    Ok(())
}
