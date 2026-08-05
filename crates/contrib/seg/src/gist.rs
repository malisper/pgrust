//! gist_seg_ops (seg.c gseg_*) over the GISTENTRY fmgr protocol: the
//! classic 1-D GiST example — consistent, union, identity
//! compress/decompress (dropped from the opclass by the 1.2--1.3 upgrade),
//! penalty, sort-by-center picksplit, same.

use datum::Datum;
use types_error::PgResult;
use types_fmgr::{byref_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};

use crate::ops;
use crate::repr::Seg;

// access/stratnum.h RT* strategy numbers used by gist_seg_ops.
const RT_LEFT: u16 = 1;
const RT_OVER_LEFT: u16 = 2;
const RT_OVERLAP: u16 = 3;
const RT_OVER_RIGHT: u16 = 4;
const RT_RIGHT: u16 = 5;
const RT_SAME: u16 = 6;
const RT_CONTAINS: u16 = 7;
const RT_CONTAINED_BY: u16 = 8;
const RT_OLD_CONTAINS: u16 = 13;
const RT_OLD_CONTAINED_BY: u16 = 14;

// SAFETY: gist fmgr protocol — arg i is a live GISTENTRY pointer.
unsafe fn entry_arg<'a>(fcinfo: &Fcinfo, i: usize) -> &'a GISTENTRY {
    unsafe { &*(fcinfo.arg(i).as_usize() as *const GISTENTRY) }
}

// SAFETY: d is a live 12-byte SEG datum.
pub(crate) unsafe fn seg_datum(d: Datum) -> Seg {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract.
    Seg::from_bytes(unsafe { core::slice::from_raw_parts(p, crate::repr::SEG_SIZE) })
}

fn ret_seg(fcinfo: &Fcinfo, seg: &Seg) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &seg.to_bytes())
}

/// `gseg_consistent`.
pub fn fc_gseg_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u32() as u16;
    // All cases served by this function are exact.
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param live in the caller frame.
    unsafe { *recheck = false };

    // SAFETY: non-null seg datums per the strict catalog signature.
    let key = unsafe { seg_datum(entry.key) };
    let query = unsafe { seg_datum(fcinfo.arg(1)) };

    let res = if entry.page_is_leaf {
        // gseg_leaf_consistent
        match strategy {
            RT_LEFT => ops::seg_left(&key, &query),
            RT_OVER_LEFT => ops::seg_over_left(&key, &query),
            RT_OVERLAP => ops::seg_overlap(&key, &query),
            RT_OVER_RIGHT => ops::seg_over_right(&key, &query),
            RT_RIGHT => ops::seg_right(&key, &query),
            RT_SAME => ops::seg_cmp(&key, &query)? == 0,
            RT_CONTAINS | RT_OLD_CONTAINS => ops::seg_contains(&key, &query),
            RT_CONTAINED_BY | RT_OLD_CONTAINED_BY => ops::seg_contains(&query, &key),
            _ => false,
        }
    } else {
        // gseg_internal_consistent
        match strategy {
            RT_LEFT => !ops::seg_over_right(&key, &query),
            RT_OVER_LEFT => !ops::seg_right(&key, &query),
            RT_OVERLAP => ops::seg_overlap(&key, &query),
            RT_OVER_RIGHT => !ops::seg_left(&key, &query),
            RT_RIGHT => !ops::seg_over_left(&key, &query),
            RT_SAME | RT_CONTAINS | RT_OLD_CONTAINS => ops::seg_contains(&key, &query),
            RT_CONTAINED_BY | RT_OLD_CONTAINED_BY => ops::seg_overlap(&key, &query),
            _ => false,
        }
    };

    Ok(Datum::from_bool(res))
}

/// `gseg_union`.
pub fn fc_gseg_union(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let sizep = fcinfo.arg(1).as_usize() as *mut i32;

    let n = entryvec.n as usize;
    // SAFETY: non-null seg key datums.
    let mut out = unsafe { seg_datum(entryvec.vector[0].key) };
    for i in 1..n {
        // SAFETY: non-null seg key datums.
        let other = unsafe { seg_datum(entryvec.vector[i].key) };
        out = ops::seg_union(&out, &other);
    }
    // SAFETY: size out-param live in the caller frame.
    unsafe { *sizep = crate::repr::SEG_SIZE as i32 };
    ret_seg(fcinfo, &out)
}

/// `gseg_compress` — identity.
pub fn fc_gseg_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(fcinfo.arg(0))
}

/// `gseg_decompress` — identity.
pub fn fc_gseg_decompress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(fcinfo.arg(0))
}

/// `gseg_penalty`: change in length.
pub fn fc_gseg_penalty(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let origentry = unsafe { entry_arg(fcinfo, 0) };
    let newentry = unsafe { entry_arg(fcinfo, 1) };
    let result = fcinfo.arg(2).as_usize() as *mut f32;

    // SAFETY: non-null seg key datums.
    let orig = unsafe { seg_datum(origentry.key) };
    let new = unsafe { seg_datum(newentry.key) };
    let ud = ops::seg_union(&orig, &new);
    let tmp1 = ops::rt_seg_size(&ud);
    let tmp2 = ops::rt_seg_size(&orig);
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *result = tmp1 - tmp2 };
    Ok(fcinfo.arg(2))
}

/// `gseg_picksplit`: sort by center point, split at the middle.
pub fn fc_gseg_picksplit(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };

    // valid items are indexed 1..maxoff
    let maxoff = entryvec.n as usize - 1;

    // the auxiliary array, sorted by center (C computes the center as
    // lower*0.5f + upper*0.5f to dodge overflow)
    let mut sort_items: Vec<(f32, u16, Seg)> = Vec::with_capacity(maxoff);
    for i in 1..=maxoff {
        // SAFETY: non-null seg key datums.
        let seg = unsafe { seg_datum(entryvec.vector[i].key) };
        let center = seg.lower * 0.5f32 + seg.upper * 0.5f32;
        sort_items.push((center, i as u16, seg));
    }
    // C uses qsort with a <,==,> comparator; total_cmp is the deterministic
    // stand-in (only NaN centers order differently, and only page layout —
    // never query results — depends on it).
    sort_items.sort_by(|a, b| a.0.total_cmp(&b.0));

    // sort items below "firstright" go into the left side
    let firstright = maxoff / 2;

    let mut spl_left: Vec<u16> = Vec::new();
    let mut spl_right: Vec<u16> = Vec::new();

    let mut seg_l = sort_items[0].2;
    spl_left.push(sort_items[0].1);
    for item in &sort_items[1..firstright] {
        seg_l = ops::seg_union(&seg_l, &item.2);
        spl_left.push(item.1);
    }

    let mut seg_r = sort_items[firstright].2;
    spl_right.push(sort_items[firstright].1);
    for item in &sort_items[firstright + 1..maxoff] {
        seg_r = ops::seg_union(&seg_r, &item.2);
        spl_right.push(item.1);
    }

    v.spl_left = spl_left;
    v.spl_right = spl_right;
    v.spl_ldatum = ret_seg(fcinfo, &seg_l)?;
    v.spl_rdatum = ret_seg(fcinfo, &seg_r)?;
    Ok(fcinfo.arg(1))
}

/// `gseg_same`.
pub fn fc_gseg_same(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: non-null seg datums per the catalog signature.
    let a = unsafe { seg_datum(fcinfo.arg(0)) };
    let b = unsafe { seg_datum(fcinfo.arg(1)) };
    let same = ops::seg_cmp(&a, &b)? == 0;
    let result = fcinfo.arg(2).as_usize() as *mut bool;
    // SAFETY: result out-param live in the caller frame.
    unsafe { *result = same };
    Ok(fcinfo.arg(2))
}
