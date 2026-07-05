//! brin_minmax_multi.c: Multi Min/Max opclass for BRIN.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use ::datum::Datum;
use ::fmgr::FmgrInfo;
use ::mcx::Mcx;
use ::types_brin::{
    BrinColInfo, BrinDesc, BrinOpcKind, BrinValues, MinMaxMultiRanges, MinmaxOpaque,
    PG_BRIN_MINMAX_MULTI_SUMMARYOID,
};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_scan::scankey::{
    ScanKeyData, BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
    BTLessEqualStrategyNumber, BTLessStrategyNumber, BTMaxStrategyNumber, SK_ISNULL,
};
use ::types_storage::bufpage::MaxHeapTuplesPerPage;
use ::types_tuple::varatt::{varatt_is_1b, varatt_is_1b_e, varsize_any};

mod builtins;
mod qsort;
mod ranges;

pub use builtins::MINMAX_MULTI_BUILTINS;
use ranges::*;

pub(crate) const PROCNUM_DISTANCE: u16 = 11;

const MINMAX_BUFFER_FACTOR: i32 = 10;
const MINMAX_BUFFER_MIN: i32 = 256;
const MINMAX_BUFFER_MAX: i32 = 8192;
pub(crate) const MINMAX_BUFFER_LOAD_FACTOR: f64 = 0.5;

pub(crate) const MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE: i32 = 32;
// MinMaxMultiOptions image: vl_len_ u32 | i32 valuesPerRange @4 (size 8).
pub(crate) const MINMAXMULTIOPTIONS_SIZE: usize = 8;
pub(crate) const MINMAXMULTIOPTIONS_VPR_OFF: usize = 4;

// MinMaxMultiGetValuesPerRange: option value 0 falls back to the default.
fn minmax_multi_get_values_per_range(opts: Option<&[u8]>) -> i32 {
    match opts.map(|img| {
        i32::from_ne_bytes(
            img[MINMAXMULTIOPTIONS_VPR_OFF..MINMAXMULTIOPTIONS_VPR_OFF + 4]
                .try_into()
                .unwrap(),
        )
    }) {
        Some(v) if v != 0 => v,
        _ => MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE,
    }
}

pub fn brin_minmax_multi_opcinfo(_typoid: Oid) -> BrinColInfo {
    BrinColInfo {
        oi_opclass_options: None,
        oi_nstored: 1,
        oi_regular_nulls: true,
        kind: BrinOpcKind::MinMaxMulti,
        oi_typids: [PG_BRIN_MINMAX_MULTI_SUMMARYOID, 0, 0],
        minmax: MinmaxOpaque::default(),
        distance_procinfo: core::cell::RefCell::new(None),
        bloom: None,
        inclusion: None,
    }
}

/// PG_DETOAST_DATUM over the stored summary: the on-disk value may carry a
/// short header or be compressed (TOAST_INDEX_HACK); field access needs the
/// flat 4B image.
fn detoast_summary<'a>(mcx: Mcx<'a>, value: Datum) -> PgResult<&'a [u8]> {
    let p = value.as_usize() as *const u8;
    // SAFETY: stored summary datum is a live varlena image.
    unsafe {
        let image = core::slice::from_raw_parts(p, varsize_any(p));
        if varatt_is_1b(p) || varatt_is_1b_e(p) || varatt_is_compressed(p) {
            Ok(detoast::detoast_attr(mcx, image)?.leak())
        } else {
            Ok(image)
        }
    }
}

// VARATT_IS_COMPRESSED on a 4B varlena word.
// SAFETY: p live, at least 1 readable byte.
unsafe fn varatt_is_compressed(p: *const u8) -> bool {
    if cfg!(target_endian = "little") {
        (*p & 0x03) == 0x02
    } else {
        (*p & 0xC0) == 0x40
    }
}

fn clamp_buffer_size(target_maxvalues: i32, pages_per_range: u32) -> i32 {
    let mut maxvalues = (target_maxvalues * MINMAX_BUFFER_FACTOR)
        .min(MaxHeapTuplesPerPage as i32 * pages_per_range as i32);
    maxvalues = maxvalues.max(target_maxvalues);
    maxvalues = maxvalues.max(MINMAX_BUFFER_MIN);
    maxvalues.min(MINMAX_BUFFER_MAX)
}

/// brin_minmax_multi_add_value.
pub fn brin_minmax_multi_add_value(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    column: &mut BrinValues,
    newval: Datum,
    isnull: bool,
    colloid: Oid,
) -> PgResult<bool> {
    debug_assert!(!isnull);

    let attno = column.bv_attno;
    let att = bdesc.bd_tupdesc.attr(attno as usize - 1);
    let (attbyval, attlen, atttypid) = (att.attbyval, att.attlen, att.atttypid);

    let mut modified = false;

    if column.bv_allnulls {
        let target_maxvalues = minmax_multi_get_values_per_range(
            bdesc.bd_info[attno as usize - 1].oi_opclass_options.as_deref(),
        );
        let maxvalues = clamp_buffer_size(target_maxvalues, bdesc.bd_pages_per_range);

        let mut ranges = minmax_multi_init(maxvalues);
        ranges.attno = attno;
        ranges.colloid = colloid;
        ranges.typid = atttypid;
        ranges.target_maxvalues = target_maxvalues;
        ranges.cmp =
            minmax_multi_get_strategy_procinfo(bdesc, attno, atttypid, BTLessStrategyNumber)?;

        column.bv_allnulls = false;
        modified = true;
        column.bv_mem_value = Some(Box::new(ranges));
    } else if column.bv_mem_value.is_none() {
        let serialized = detoast_summary(mcx, column.bv_values[0])?;
        let stored_maxvalues = read_serialized_header(serialized).maxvalues;
        let maxvalues = clamp_buffer_size(stored_maxvalues, bdesc.bd_pages_per_range);

        let mut ranges = brin_range_deserialize(mcx, maxvalues, serialized)?;
        ranges.attno = attno;
        ranges.colloid = colloid;
        ranges.typid = atttypid;
        ranges.cmp =
            minmax_multi_get_strategy_procinfo(bdesc, attno, atttypid, BTLessStrategyNumber)?;

        column.bv_mem_value = Some(Box::new(ranges));
    }

    let ranges: &mut MinMaxMultiRanges =
        column.bv_mem_value.as_deref_mut().expect("initialized above");

    modified |= range_add_value(
        mcx, bdesc, colloid, attno, atttypid, attbyval, attlen, ranges, newval,
    )?;

    Ok(modified)
}

/// brin_minmax_multi_consistent: the multi-key form (C fn_nargs >= 4), so it
/// receives the attribute's whole key list at once.
pub fn brin_minmax_multi_consistent(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    column: &BrinValues,
    keys: &[&ScanKeyData],
    colloid: Oid,
) -> PgResult<bool> {
    let serialized = detoast_summary(mcx, column.bv_values[0])?;
    let maxvalues = read_serialized_header(serialized).maxvalues;
    let ranges = brin_range_deserialize(mcx, maxvalues, serialized)?;

    for rangeno in 0..ranges.nranges as usize {
        let minval = ranges.values[2 * rangeno];
        let maxval = ranges.values[2 * rangeno + 1];

        let mut matching = true;
        for key in keys {
            debug_assert!(key.sk_flags & SK_ISNULL == 0);
            let attno = key.sk_attno as u16;
            let subtype = key.sk_subtype;
            let value = key.sk_argument;

            let matches = match key.sk_strategy {
                s @ (BTLessStrategyNumber | BTLessEqualStrategyNumber) => {
                    let finfo = minmax_multi_get_strategy_procinfo(bdesc, attno, subtype, s)?;
                    call_bool(mcx, &finfo, colloid, minval, value)?
                }
                BTEqualStrategyNumber => {
                    let gt = minmax_multi_get_strategy_procinfo(
                        bdesc, attno, subtype, BTGreaterStrategyNumber,
                    )?;
                    if call_bool(mcx, &gt, colloid, minval, value)? {
                        false
                    } else {
                        let lt = minmax_multi_get_strategy_procinfo(
                            bdesc, attno, subtype, BTLessStrategyNumber,
                        )?;
                        !call_bool(mcx, &lt, colloid, maxval, value)?
                    }
                }
                s @ (BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber) => {
                    let finfo = minmax_multi_get_strategy_procinfo(bdesc, attno, subtype, s)?;
                    call_bool(mcx, &finfo, colloid, maxval, value)?
                }
                other => panic!("invalid strategy number {other}"),
            };

            matching &= matches;
            if !matching {
                break;
            }
        }
        if matching {
            return Ok(true);
        }
    }

    for i in 0..ranges.nvalues as usize {
        let val = ranges.values[(2 * ranges.nranges) as usize + i];

        let mut matching = true;
        for key in keys {
            if key.sk_flags & SK_ISNULL != 0 {
                continue;
            }
            let attno = key.sk_attno as u16;
            let subtype = key.sk_subtype;
            let value = key.sk_argument;

            let matches = match key.sk_strategy {
                s @ 1..=BTMaxStrategyNumber => {
                    let finfo = minmax_multi_get_strategy_procinfo(bdesc, attno, subtype, s)?;
                    call_bool(mcx, &finfo, colloid, val, value)?
                }
                other => panic!("invalid strategy number {other}"),
            };

            matching &= matches;
            if !matching {
                break;
            }
        }
        if matching {
            return Ok(true);
        }
    }

    Ok(false)
}

pub fn brin_minmax_multi_union(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    colloid: Oid,
    col_a: &mut BrinValues,
    col_b: &BrinValues,
) -> PgResult<()> {
    debug_assert!(col_a.bv_attno == col_b.bv_attno);
    debug_assert!(!col_a.bv_allnulls && !col_b.bv_allnulls);

    let attno = col_a.bv_attno;
    let atttypid = bdesc.bd_tupdesc.attr(attno as usize - 1).atttypid;

    let serialized_a = detoast_summary(mcx, col_a.bv_values[0])?;
    let serialized_b = detoast_summary(mcx, col_b.bv_values[0])?;

    let mut ranges_a =
        brin_range_deserialize(mcx, read_serialized_header(serialized_a).maxvalues, serialized_a)?;
    let ranges_b =
        brin_range_deserialize(mcx, read_serialized_header(serialized_b).maxvalues, serialized_b)?;

    let n_a = (ranges_a.nranges + ranges_a.nvalues) as usize;
    let n_b = (ranges_b.nranges + ranges_b.nvalues) as usize;

    let cmp = minmax_multi_get_strategy_procinfo(bdesc, attno, atttypid, BTLessStrategyNumber)?;

    {
        let ctx = ::mcx::MemoryContext::new_bump("minmax-multi context");
        let cmcx = ctx.mcx();

        let mut eranges: ::mcx::PgVec<'_, ExpandedRange> =
            ::mcx::vec_with_capacity_in(cmcx, n_a + n_b)?;
        eranges.resize(
            n_a + n_b,
            ExpandedRange { minval: Datum::null(), maxval: Datum::null(), collapsed: false },
        );
        fill_expanded_ranges(&mut eranges[..n_a], n_a, &ranges_a);
        fill_expanded_ranges(&mut eranges[n_a..], n_b, &ranges_b);

        let mut neranges = sort_expanded_ranges(cmcx, &cmp, colloid, &mut eranges)?;
        neranges = merge_overlapping_ranges(cmcx, &cmp, colloid, &mut eranges, neranges)?;
        assert_check_expanded_ranges(cmcx, bdesc, colloid, attno, atttypid, &eranges[..neranges])?;

        let distance_fn = minmax_multi_get_procinfo(bdesc, attno, PROCNUM_DISTANCE)?;
        let distances = build_distances(cmcx, &distance_fn, colloid, &eranges[..neranges])?;

        neranges = reduce_expanded_ranges(
            cmcx,
            &mut eranges,
            neranges,
            distances.as_deref(),
            ranges_a.maxvalues,
            &cmp,
            colloid,
        )?;
        assert_check_expanded_ranges(cmcx, bdesc, colloid, attno, atttypid, &eranges[..neranges])?;

        store_expanded_ranges(&mut ranges_a, &eranges[..neranges]);
    }

    col_a.bv_values[0] = brin_range_serialize(mcx, &mut ranges_a)?;
    Ok(())
}

/// brin_minmax_multi_serialize (the bv_serialize callback): compact the live
/// buffer to target_maxvalues and serialize into bv_values[0].
pub fn brin_minmax_multi_serialize(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    column: &mut BrinValues,
) -> PgResult<()> {
    let ranges = column.bv_mem_value.as_deref_mut().expect("live minmax-multi buffer");
    let target = ranges.target_maxvalues;
    compactify_ranges(mcx, bdesc, ranges, target)?;
    debug_assert!(ranges.nsorted == ranges.nvalues);
    column.bv_values[0] = brin_range_serialize(mcx, ranges)?;
    Ok(())
}

fn minmax_multi_get_procinfo(
    bdesc: &BrinDesc<'_>,
    attno: u16,
    procnum: u16,
) -> PgResult<FmgrInfo> {
    debug_assert!(procnum == PROCNUM_DISTANCE);
    let cache = &bdesc.bd_info[attno as usize - 1].distance_procinfo;

    if let Some(fi) = cache.borrow().as_ref() {
        return Ok(fi.clone());
    }

    let opfamily = bdesc.bd_opfamily[attno as usize - 1];
    let opcintype = bdesc.bd_opcintype[attno as usize - 1];
    let proc = lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, procnum as i16)?;
    if proc == 0 {
        panic!(
            "invalid opclass definition: missing support function {procnum} for column {attno}"
        );
    }
    let finfo = fmgr_core::fmgr_info(proc)?;
    *cache.borrow_mut() = Some(finfo.clone());
    Ok(finfo)
}

// minmax_multi_get_strategy_procinfo: per-subtype cache of the btree
// comparison procs (rule-5 cache), same shape as brin_minmax.
fn minmax_multi_get_strategy_procinfo(
    bdesc: &BrinDesc<'_>,
    attno: u16,
    subtype: Oid,
    strategynum: u16,
) -> PgResult<FmgrInfo> {
    debug_assert!(strategynum >= 1 && strategynum <= BTMaxStrategyNumber);
    let opaque = &bdesc.bd_info[attno as usize - 1].minmax;

    if opaque.cached_subtype.get() != subtype {
        *opaque.strategy_procinfos.borrow_mut() = [const { None }; 5];
        opaque.cached_subtype.set(subtype);
    }

    {
        let cache = opaque.strategy_procinfos.borrow();
        if let Some(fi) = &cache[strategynum as usize - 1] {
            return Ok(fi.clone());
        }
    }

    let opfamily = bdesc.bd_opfamily[attno as usize - 1];
    let atttypid = bdesc.bd_tupdesc.attr(attno as usize - 1).atttypid;
    let oprid = lsyscache::get_opfamily_member(opfamily, atttypid, subtype, strategynum as i16)?;
    if oprid == 0 {
        panic!("missing operator {strategynum}({atttypid},{subtype}) in opfamily {opfamily}");
    }
    let proc = lsyscache::get_opcode(oprid)?;
    debug_assert!(proc != 0);
    let finfo = fmgr_core::fmgr_info(proc)?;
    opaque.strategy_procinfos.borrow_mut()[strategynum as usize - 1] = Some(finfo.clone());
    Ok(finfo)
}
