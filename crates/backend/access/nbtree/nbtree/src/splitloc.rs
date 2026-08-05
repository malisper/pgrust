//! nbtsplitloc.c, transcribed whole: the fillfactor/rightmost/after-new-item
//! interval logic decides append-workload page utilization — shape changes
//! here are perf regressions.

use ::mcx::Mcx;
use ::types_core::{OffsetNumber, Size};
use ::types_error::{PgError, PgResult};
use ::types_rel::Relation;
use ::types_storage::bufpage::{ItemIdData, PageRef, SizeOfPageHeaderData};
use ::types_tuple::itemptr::{
    FirstOffsetNumber, ItemPointerData, ItemPointerGetBlockNumberNoCheck,
};

use crate::itup::{bt_tuple_is_posting, index_tuple_size, maxalign, t_tid, ITup};
use crate::page::{page_item, page_opaque};
use crate::utils::bt_keep_natts_fast;
use ::types_nbtree::{
    BTPageOpaqueData, BTREE_NONLEAF_FILLFACTOR, BTREE_SINGLEVAL_FILLFACTOR, P_FIRSTDATAKEY,
    P_FIRSTKEY, P_HIKEY, P_ISLEAF, P_RIGHTMOST,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum FindSplitStrat {
    Default,
    ManyDuplicates,
    SingleValue,
}

#[derive(Clone, Copy)]
struct SplitPoint {
    curdelta: i16,
    leftfree: i16,
    rightfree: i16,
    firstrightoff: OffsetNumber,
    newitemonleft: bool,
}

struct FindSplitData<'a, 'p, 'mcx> {
    rel: &'a Relation<'mcx>,
    origpage: &'a PageRef<'p>,
    newitem: ITup,
    newitemsz: Size,
    is_leaf: bool,
    is_rightmost: bool,
    newitemoff: OffsetNumber,
    leftspace: i32,
    rightspace: i32,
    olddataitemstotal: i32,
    minfirstrightsz: Size,
    splits: ::mcx::PgVec<'mcx, SplitPoint>,
    interval: usize,
}

const LEAF_SPLIT_DISTANCE: f64 = 0.050;
const INTERNAL_SPLIT_DISTANCE: f64 = 0.075;

/// _bt_findsplitloc. `newitemsz` is MAXALIGNed and does not include the line
/// pointer. Returns (firstrightoff, newitemonleft).
///
/// # Safety
/// `page` pinned + write-locked; `newitem` a live non-posting index tuple.
pub(crate) unsafe fn bt_findsplitloc<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    page: &PageRef<'_>,
    newitemoff: OffsetNumber,
    newitemsz: Size,
    newitem: ITup,
) -> PgResult<(OffsetNumber, bool)> {
    let opaque = page_opaque(page);
    let maxoff = page.max_offset_number();

    let space = (::types_core::BLCKSZ
        - SizeOfPageHeaderData
        - maxalign(core::mem::size_of::<BTPageOpaqueData>())) as i32;
    let leftspace = space;
    let mut rightspace = space;

    if !P_RIGHTMOST(&opaque) {
        let itemid = page.item_id(P_HIKEY);
        rightspace -= (maxalign(itemid.lp_len() as usize)
            + core::mem::size_of::<ItemIdData>()) as i32;
    }

    let olddataitemstotal = rightspace - page.exact_free_space() as i32;
    let leaffillfactor = rel.get_fillfactor(::types_nbtree::BTREE_DEFAULT_FILLFACTOR as i32);

    let newitemsz = newitemsz + core::mem::size_of::<ItemIdData>();
    debug_assert!(!bt_tuple_is_posting(newitem));

    let mut state = FindSplitData {
        rel,
        origpage: page,
        newitem,
        newitemsz,
        is_leaf: P_ISLEAF(&opaque),
        is_rightmost: P_RIGHTMOST(&opaque),
        newitemoff,
        leftspace,
        rightspace,
        olddataitemstotal,
        minfirstrightsz: usize::MAX,
        splits: ::mcx::vec_with_capacity_in(mcx, maxoff as usize)?,
        interval: 0,
    };

    let mut olddataitemstoleft: i32 = 0;
    let mut offnum = P_FIRSTDATAKEY(&opaque);
    while offnum <= maxoff {
        let itemid = page.item_id(offnum);
        let itemsz = maxalign(itemid.lp_len() as usize) + core::mem::size_of::<ItemIdData>();

        if offnum < newitemoff {
            recsplitloc(&mut state, offnum, false, olddataitemstoleft, itemsz);
        } else if offnum > newitemoff {
            recsplitloc(&mut state, offnum, true, olddataitemstoleft, itemsz);
        } else {
            recsplitloc(&mut state, offnum, false, olddataitemstoleft, itemsz);
            recsplitloc(&mut state, offnum, true, olddataitemstoleft, itemsz);
        }

        olddataitemstoleft += itemsz as i32;
        offnum += 1;
    }

    debug_assert!(olddataitemstoleft == olddataitemstotal);
    if newitemoff > maxoff {
        recsplitloc(&mut state, newitemoff, false, olddataitemstotal, 0);
    }

    if state.splits.is_empty() {
        return Err(no_feasible_split(rel));
    }

    let usemult;
    let mut fillfactormult: f64;
    if !state.is_leaf {
        usemult = state.is_rightmost;
        fillfactormult = BTREE_NONLEAF_FILLFACTOR as f64 / 100.0;
    } else if state.is_rightmost {
        usemult = true;
        fillfactormult = leaffillfactor as f64 / 100.0;
    } else {
        let mut aftermult = false;
        if afternewitemoff(&state, maxoff, leaffillfactor, &mut aftermult) {
            usemult = aftermult;
            if usemult {
                fillfactormult = leaffillfactor as f64 / 100.0;
            } else {
                for split in state.splits.iter() {
                    if split.newitemonleft && newitemoff == split.firstrightoff {
                        return Ok((newitemoff, true));
                    }
                }
                fillfactormult = 0.50;
            }
        } else {
            usemult = false;
            fillfactormult = 0.50;
        }
    }

    let leftpage = state.splits[0];
    let rightpage = state.splits[state.splits.len() - 1];

    deltasortsplits(&mut state, fillfactormult, usemult);
    state.interval = defaultinterval(&state);

    let mut strategy = FindSplitStrat::Default;
    let mut perfectpenalty = strategy_decide(&state, &leftpage, &rightpage, &mut strategy);

    match strategy {
        FindSplitStrat::Default => {}
        FindSplitStrat::ManyDuplicates => {
            debug_assert!(state.is_leaf);
            debug_assert!(perfectpenalty == state.rel.indnkeyatts());
            state.interval = state.splits.len();
        }
        FindSplitStrat::SingleValue => {
            debug_assert!(state.is_leaf);
            fillfactormult = BTREE_SINGLEVAL_FILLFACTOR as f64 / 100.0;
            deltasortsplits(&mut state, fillfactormult, true);
            state.interval = 1;
        }
    }
    let _ = &mut perfectpenalty;

    Ok(bestsplitloc(&state, perfectpenalty, strategy))
}

#[track_caller]
#[cold]
#[inline(never)]
fn no_feasible_split(rel: &Relation<'_>) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "could not find a feasible split point for index \"{}\"",
        rel.name()
    )))
}

/// _bt_recsplitloc.
unsafe fn recsplitloc(
    state: &mut FindSplitData<'_, '_, '_>,
    firstrightoff: OffsetNumber,
    newitemonleft: bool,
    olddataitemstoleft: i32,
    firstrightofforigpagetuplesz: Size,
) {
    let newitemisfirstright = firstrightoff == state.newitemoff && !newitemonleft;

    let mut postingsz: Size = 0;
    let firstrightsz = if newitemisfirstright {
        state.newitemsz
    } else {
        let firstrightsz = firstrightofforigpagetuplesz;
        if state.is_leaf && firstrightsz > 64 {
            let itemid = state.origpage.item_id(firstrightoff);
            let newhighkey = page_item(state.origpage, itemid);
            if bt_tuple_is_posting(newhighkey) {
                postingsz = index_tuple_size(newhighkey)
                    - crate::itup::bt_tuple_get_posting_offset(newhighkey);
            }
        }
        firstrightsz
    };

    let mut leftfree = state.leftspace - olddataitemstoleft;
    let mut rightfree = state.rightspace - (state.olddataitemstotal - olddataitemstoleft);

    if state.is_leaf {
        leftfree -= (firstrightsz + maxalign(core::mem::size_of::<ItemPointerData>())
            - postingsz) as i32;
    } else {
        leftfree -= firstrightsz as i32;
    }

    if newitemonleft {
        leftfree -= state.newitemsz as i32;
    } else {
        rightfree -= state.newitemsz as i32;
    }

    if !state.is_leaf {
        rightfree += firstrightsz as i32
            - (maxalign(crate::itup::INDEX_TUPLE_HEADER_SIZE)
                + core::mem::size_of::<ItemIdData>()) as i32;
    }

    if leftfree >= 0 && rightfree >= 0 {
        state.minfirstrightsz = state.minfirstrightsz.min(firstrightsz);
        state.splits.push(SplitPoint {
            curdelta: 0,
            leftfree: leftfree as i16,
            rightfree: rightfree as i16,
            firstrightoff,
            newitemonleft,
        });
    }
}

/// _bt_deltasortsplits.
fn deltasortsplits(state: &mut FindSplitData<'_, '_, '_>, fillfactormult: f64, usemult: bool) {
    for split in state.splits.iter_mut() {
        let delta: i16 = if usemult {
            (fillfactormult * split.leftfree as f64
                - (1.0 - fillfactormult) * split.rightfree as f64) as i16
        } else {
            split.leftfree - split.rightfree
        };
        split.curdelta = if delta < 0 { -delta } else { delta };
    }
    state.splits.sort_unstable_by_key(|s| s.curdelta);
}

/// _bt_afternewitemoff.
unsafe fn afternewitemoff(
    state: &FindSplitData<'_, '_, '_>,
    maxoff: OffsetNumber,
    leaffillfactor: i32,
    usemult: &mut bool,
) -> bool {
    debug_assert!(state.is_leaf && !state.is_rightmost);

    let nkeyatts = state.rel.indnkeyatts();
    if nkeyatts == 1 {
        return false;
    }
    if state.newitemoff == P_FIRSTKEY {
        return false;
    }
    if state.newitemsz != state.minfirstrightsz {
        return false;
    }
    if state.newitemsz * (maxoff as usize - 1) != state.olddataitemstotal as usize {
        return false;
    }
    if state.newitemsz
        > maxalign(crate::itup::INDEX_TUPLE_HEADER_SIZE + 16) + core::mem::size_of::<ItemIdData>()
    {
        return false;
    }

    if state.newitemoff > maxoff {
        let itemid = state.origpage.item_id(maxoff);
        let tup = page_item(state.origpage, itemid);
        let keepnatts = bt_keep_natts_fast(state.rel, tup, state.newitem);
        if keepnatts > 1 && keepnatts <= nkeyatts {
            *usemult = true;
            return true;
        }
        return false;
    }

    let itemid = state.origpage.item_id(state.newitemoff - 1);
    let tup = page_item(state.origpage, itemid);
    if bt_tuple_is_posting(tup) || !adjacenthtid(&t_tid(tup), &t_tid(state.newitem)) {
        return false;
    }
    let keepnatts = bt_keep_natts_fast(state.rel, tup, state.newitem);
    if keepnatts > 1 && keepnatts <= nkeyatts {
        let interp = state.newitemoff as f64 / (maxoff as f64 + 1.0);
        let leaffillfactormult = leaffillfactor as f64 / 100.0;
        *usemult = interp > leaffillfactormult;
        return true;
    }
    false
}

/// _bt_adjacenthtid.
fn adjacenthtid(lowhtid: &ItemPointerData, highhtid: &ItemPointerData) -> bool {
    let lowblk = ItemPointerGetBlockNumberNoCheck(lowhtid);
    let highblk = ItemPointerGetBlockNumberNoCheck(highhtid);
    if lowblk == highblk {
        return true;
    }
    if lowblk + 1 == highblk && highhtid.ip_posid == FirstOffsetNumber {
        return true;
    }
    false
}

/// _bt_bestsplitloc.
unsafe fn bestsplitloc(
    state: &FindSplitData<'_, '_, '_>,
    perfectpenalty: i32,
    strategy: FindSplitStrat,
) -> (OffsetNumber, bool) {
    let highsplit = state.interval.min(state.splits.len());
    let mut bestpenalty = i32::MAX;
    let mut lowsplit = 0usize;

    for i in 0..highsplit {
        let penalty = split_penalty(state, &state.splits[i]);
        if penalty < bestpenalty {
            bestpenalty = penalty;
            lowsplit = i;
        }
        if penalty <= perfectpenalty {
            break;
        }
    }

    let mut fin = &state.splits[lowsplit];

    if strategy == FindSplitStrat::ManyDuplicates
        && !state.is_rightmost
        && !fin.newitemonleft
        && fin.firstrightoff >= state.newitemoff
        && fin.firstrightoff < state.newitemoff + 9
    {
        fin = &state.splits[0];
    }

    (fin.firstrightoff, fin.newitemonleft)
}

/// _bt_defaultinterval.
fn defaultinterval(state: &FindSplitData<'_, '_, '_>) -> usize {
    let tolerance: i16 = if state.is_leaf {
        (state.olddataitemstotal as f64 * LEAF_SPLIT_DISTANCE) as i16
    } else {
        (state.olddataitemstotal as f64 * INTERNAL_SPLIT_DISTANCE) as i16
    };

    let spaceoptimal = &state.splits[0];
    let lowleftfree = spaceoptimal.leftfree - tolerance;
    let lowrightfree = spaceoptimal.rightfree - tolerance;
    let highleftfree = spaceoptimal.leftfree + tolerance;
    let highrightfree = spaceoptimal.rightfree + tolerance;

    for i in 1..state.splits.len() {
        let split = &state.splits[i];
        if split.leftfree < lowleftfree
            || split.rightfree < lowrightfree
            || split.leftfree > highleftfree
            || split.rightfree > highrightfree
        {
            return i;
        }
    }
    state.splits.len()
}

/// _bt_strategy.
unsafe fn strategy_decide(
    state: &FindSplitData<'_, '_, '_>,
    leftpage: &SplitPoint,
    rightpage: &SplitPoint,
    strategy: &mut FindSplitStrat,
) -> i32 {
    *strategy = FindSplitStrat::Default;
    let indnkeyatts = state.rel.indnkeyatts();

    if !state.is_leaf {
        return state.minfirstrightsz as i32;
    }

    let (leftinterval, rightinterval) = interval_edges(state);
    let leftmost = split_lastleft(state, leftinterval);
    let rightmost = split_firstright(state, rightinterval);

    let perfectpenalty = bt_keep_natts_fast(state.rel, leftmost, rightmost);
    if perfectpenalty <= indnkeyatts {
        return perfectpenalty;
    }

    let leftmost = split_lastleft(state, leftpage);
    let rightmost = split_firstright(state, rightpage);

    let perfectpenalty = bt_keep_natts_fast(state.rel, leftmost, rightmost);
    if perfectpenalty <= indnkeyatts {
        *strategy = FindSplitStrat::ManyDuplicates;
        return indnkeyatts;
    }

    if state.is_rightmost {
        *strategy = FindSplitStrat::SingleValue;
    } else {
        let itemid = state.origpage.item_id(P_HIKEY);
        let hikey = page_item(state.origpage, itemid);
        let perfectpenalty = bt_keep_natts_fast(state.rel, hikey, state.newitem);
        if perfectpenalty <= indnkeyatts {
            *strategy = FindSplitStrat::SingleValue;
        }
    }

    perfectpenalty
}

/// _bt_interval_edges.
fn interval_edges<'s>(
    state: &'s FindSplitData<'_, '_, '_>,
) -> (&'s SplitPoint, &'s SplitPoint) {
    let highsplit = state.interval.min(state.splits.len());
    let deltaoptimal = &state.splits[0];
    let mut leftinterval: Option<&SplitPoint> = None;
    let mut rightinterval: Option<&SplitPoint> = None;

    for i in (0..highsplit).rev() {
        let distant = &state.splits[i];
        if distant.firstrightoff < deltaoptimal.firstrightoff {
            if leftinterval.is_none() {
                leftinterval = Some(distant);
            }
        } else if distant.firstrightoff > deltaoptimal.firstrightoff {
            if rightinterval.is_none() {
                rightinterval = Some(distant);
            }
        } else if !distant.newitemonleft && deltaoptimal.newitemonleft {
            debug_assert!(distant.firstrightoff == state.newitemoff);
            if leftinterval.is_none() {
                leftinterval = Some(distant);
            }
        } else if distant.newitemonleft && !deltaoptimal.newitemonleft {
            debug_assert!(distant.firstrightoff == state.newitemoff);
            if rightinterval.is_none() {
                rightinterval = Some(distant);
            }
        } else {
            if leftinterval.is_none() {
                leftinterval = Some(distant);
            }
            if rightinterval.is_none() {
                rightinterval = Some(distant);
            }
        }

        if let (Some(l), Some(r)) = (leftinterval, rightinterval) {
            return (l, r);
        }
    }
    unreachable!("split interval had no edges");
}

/// _bt_split_penalty.
unsafe fn split_penalty(state: &FindSplitData<'_, '_, '_>, split: &SplitPoint) -> i32 {
    if !state.is_leaf {
        if !split.newitemonleft && split.firstrightoff == state.newitemoff {
            return state.newitemsz as i32;
        }
        let itemid = state.origpage.item_id(split.firstrightoff);
        return (maxalign(itemid.lp_len() as usize) + core::mem::size_of::<ItemIdData>()) as i32;
    }

    let lastleft = split_lastleft(state, split);
    let firstright = split_firstright(state, split);
    bt_keep_natts_fast(state.rel, lastleft, firstright)
}

/// _bt_split_lastleft.
unsafe fn split_lastleft(state: &FindSplitData<'_, '_, '_>, split: &SplitPoint) -> ITup {
    if split.newitemonleft && split.firstrightoff == state.newitemoff {
        return state.newitem;
    }
    let itemid = state.origpage.item_id(split.firstrightoff - 1);
    page_item(state.origpage, itemid)
}

/// _bt_split_firstright.
unsafe fn split_firstright(state: &FindSplitData<'_, '_, '_>, split: &SplitPoint) -> ITup {
    if !split.newitemonleft && split.firstrightoff == state.newitemoff {
        return state.newitem;
    }
    let itemid = state.origpage.item_id(split.firstrightoff);
    page_item(state.origpage, itemid)
}
