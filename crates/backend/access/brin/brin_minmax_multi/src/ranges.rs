use ::datum::Datum;
use ::fmgr::{function_call2_coll_in, FmgrInfo};
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_brin::{BrinDesc, MinMaxMultiRanges};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_tuple::varatt::varsize_any;

use crate::qsort::pg_qsort_arg;
use crate::{
    minmax_multi_get_procinfo, minmax_multi_get_strategy_procinfo, BTEqualStrategyNumber,
    BTGreaterStrategyNumber, BTLessStrategyNumber, MINMAX_BUFFER_LOAD_FACTOR, PROCNUM_DISTANCE,
};

#[derive(Clone, Copy)]
pub struct ExpandedRange {
    pub minval: Datum,
    pub maxval: Datum,
    pub collapsed: bool,
}

#[derive(Clone, Copy)]
pub struct DistanceValue {
    pub index: i32,
    pub value: f64,
}

#[inline]
pub fn call_bool(
    mcx: Mcx<'_>,
    finfo: &FmgrInfo,
    colloid: Oid,
    a: Datum,
    b: Datum,
) -> PgResult<bool> {
    let mut f = finfo.clone();
    Ok(function_call2_coll_in(&mut f, colloid, mcx, a, b)?.as_bool())
}

pub fn compare_values(
    mcx: Mcx<'_>,
    cmp: &FmgrInfo,
    colloid: Oid,
    a: Datum,
    b: Datum,
) -> PgResult<i32> {
    if call_bool(mcx, cmp, colloid, a, b)? {
        return Ok(-1);
    }
    if call_bool(mcx, cmp, colloid, b, a)? {
        return Ok(1);
    }
    Ok(0)
}

pub fn compare_expanded_ranges(
    mcx: Mcx<'_>,
    cmp: &FmgrInfo,
    colloid: Oid,
    ra: &ExpandedRange,
    rb: &ExpandedRange,
) -> PgResult<i32> {
    if call_bool(mcx, cmp, colloid, ra.minval, rb.minval)? {
        return Ok(-1);
    }
    if call_bool(mcx, cmp, colloid, rb.minval, ra.minval)? {
        return Ok(1);
    }
    if call_bool(mcx, cmp, colloid, ra.maxval, rb.maxval)? {
        return Ok(-1);
    }
    if call_bool(mcx, cmp, colloid, rb.maxval, ra.maxval)? {
        return Ok(1);
    }
    Ok(0)
}

pub fn minmax_multi_init(maxvalues: i32) -> MinMaxMultiRanges {
    debug_assert!(maxvalues > 0);
    MinMaxMultiRanges {
        typid: 0,
        colloid: 0,
        attno: 0,
        cmp: FmgrInfo::unresolved(),
        nranges: 0,
        nsorted: 0,
        nvalues: 0,
        maxvalues,
        target_maxvalues: 0,
        values: vec![Datum::null(); maxvalues as usize],
    }
}

pub fn range_deduplicate_values(mcx: Mcx<'_>, range: &mut MinMaxMultiRanges) -> PgResult<()> {
    if range.nsorted == range.nvalues {
        return Ok(());
    }

    let colloid = range.colloid;
    let cmp = range.cmp.clone();
    let start = (2 * range.nranges) as usize;

    pg_qsort_arg(
        &mut range.values[start..start + range.nvalues as usize],
        |a, b| compare_values(mcx, &cmp, colloid, *a, *b),
    )?;

    let mut n = 1usize;
    for i in 1..range.nvalues as usize {
        if compare_values(
            mcx,
            &cmp,
            colloid,
            range.values[start + i - 1],
            range.values[start + i],
        )? == 0
        {
            continue;
        }
        range.values[start + n] = range.values[start + i];
        n += 1;
    }

    range.nvalues = n as i32;
    range.nsorted = n as i32;

    assert_check_ranges(mcx, &cmp, colloid, range)?;
    Ok(())
}

const SERIALIZED_HEADER: usize = 20;

// SerializedRanges header layout: vl_len_ 0..4, typid 4..8, nranges 8..12,
// nvalues 12..16, maxvalues 16..20, data 20.. — the on-disk bytes must be
// byte-identical to C.
pub struct SerializedHeader {
    pub typid: Oid,
    pub nranges: i32,
    pub nvalues: i32,
    pub maxvalues: i32,
}

pub fn read_serialized_header(image: &[u8]) -> SerializedHeader {
    SerializedHeader {
        typid: u32::from_ne_bytes(image[4..8].try_into().unwrap()),
        nranges: i32::from_ne_bytes(image[8..12].try_into().unwrap()),
        nvalues: i32::from_ne_bytes(image[12..16].try_into().unwrap()),
        maxvalues: i32::from_ne_bytes(image[16..20].try_into().unwrap()),
    }
}

// SAFETY: p is a live NUL-terminated cstring.
unsafe fn cstring_len(p: *const u8) -> usize {
    let mut n = 0usize;
    while p.add(n).read() != 0 {
        n += 1;
    }
    n + 1
}

/// brin_range_serialize: build the on-disk varlena image; the returned Datum
/// points at an `mcx` allocation.
pub fn brin_range_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    range: &mut MinMaxMultiRanges,
) -> PgResult<Datum> {
    debug_assert!(range.nranges >= 0 && range.nsorted >= 0 && range.nvalues >= 0);
    debug_assert!(range.maxvalues > 0 && range.target_maxvalues > 0);
    debug_assert!(2 * range.nranges + range.nvalues <= range.target_maxvalues);
    debug_assert!(range.target_maxvalues <= range.maxvalues);
    debug_assert!(range.nvalues >= range.nsorted);

    range_deduplicate_values(mcx, range)?;

    let nvalues = (2 * range.nranges + range.nvalues) as usize;
    let typid = range.typid;
    let typbyval = lsyscache::get_typbyval(typid)?;
    let typlen = lsyscache::get_typlen(typid)?;

    let mut len = SERIALIZED_HEADER;
    for i in 0..nvalues {
        // SAFETY: by-ref values are live copies owned by the memtuple context.
        unsafe {
            if typlen == -1 {
                len += varsize_any(range.values[i].as_usize() as *const u8);
            } else if typlen == -2 {
                len += cstring_len(range.values[i].as_usize() as *const u8);
            } else {
                debug_assert!(typlen > 0);
                len += typlen as usize;
            }
        }
    }

    let mut out: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, len)?;
    out.resize(len, 0);
    out[0..4].copy_from_slice(&(((len as u32) << 2).to_ne_bytes()));
    out[4..8].copy_from_slice(&typid.to_ne_bytes());
    out[8..12].copy_from_slice(&range.nranges.to_ne_bytes());
    out[12..16].copy_from_slice(&range.nvalues.to_ne_bytes());
    out[16..20].copy_from_slice(&range.target_maxvalues.to_ne_bytes());

    let mut off = SERIALIZED_HEADER;
    for i in 0..nvalues {
        let v = range.values[i];
        if typbyval {
            let word = v.as_u64().to_ne_bytes();
            out[off..off + typlen as usize].copy_from_slice(&word[..typlen as usize]);
            off += typlen as usize;
        } else {
            // SAFETY: live by-ref value, sized by its header / fixed typlen.
            unsafe {
                let p = v.as_usize() as *const u8;
                let sz = if typlen == -1 {
                    varsize_any(p)
                } else if typlen == -2 {
                    cstring_len(p)
                } else {
                    typlen as usize
                };
                out[off..off + sz].copy_from_slice(core::slice::from_raw_parts(p, sz));
                off += sz;
            }
        }
        debug_assert!(off <= len);
    }
    debug_assert!(off == len);

    Ok(Datum::from_usize(out.leak().as_ptr() as usize))
}

const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

fn fetch_byval(bytes: &[u8], typlen: i16) -> Datum {
    match typlen {
        1 => Datum::from_char(bytes[0] as i8),
        2 => Datum::from_i16(i16::from_ne_bytes(bytes[0..2].try_into().unwrap())),
        4 => Datum::from_i32(i32::from_ne_bytes(bytes[0..4].try_into().unwrap())),
        8 => Datum::from_i64(i64::from_ne_bytes(bytes[0..8].try_into().unwrap())),
        other => panic!("unsupported byval length: {other}"),
    }
}

/// brin_range_deserialize; by-ref values are copied into one `mcx` chunk with
/// MAXALIGN spacing (C's single-palloc layout).
pub fn brin_range_deserialize<'mcx>(
    mcx: Mcx<'mcx>,
    maxvalues: i32,
    image: &[u8],
) -> PgResult<MinMaxMultiRanges> {
    let hdr = read_serialized_header(image);
    debug_assert!(hdr.nranges >= 0 && hdr.nvalues >= 0 && hdr.maxvalues > 0);

    let nvalues = (2 * hdr.nranges + hdr.nvalues) as usize;
    debug_assert!(nvalues as i32 <= hdr.maxvalues);
    debug_assert!(hdr.maxvalues <= maxvalues);

    let mut range = minmax_multi_init(maxvalues);
    range.nranges = hdr.nranges;
    range.nvalues = hdr.nvalues;
    range.nsorted = hdr.nvalues;
    range.target_maxvalues = hdr.maxvalues;
    range.typid = hdr.typid;

    let typbyval = lsyscache::get_typbyval(hdr.typid)?;
    let typlen = lsyscache::get_typlen(hdr.typid)?;

    let data = &image[SERIALIZED_HEADER..];

    let mut datalen = 0usize;
    if !typbyval {
        let mut p = 0usize;
        for _ in 0..nvalues {
            let sz = if typlen > 0 {
                typlen as usize
            } else if typlen == -1 {
                // SAFETY: value image lies inside `data`.
                unsafe { varsize_any(data.as_ptr().add(p)) }
            } else {
                debug_assert!(typlen == -2);
                // SAFETY: as above; NUL-terminated within data.
                unsafe { cstring_len(data.as_ptr().add(p)) }
            };
            datalen += maxalign(sz);
            p += sz;
        }
    }

    let mut chunk: *mut u8 = core::ptr::null_mut();
    if datalen > 0 {
        let mut buf: PgVec<'mcx, u64> = vec_with_capacity_in(mcx, datalen / 8)?;
        buf.resize(datalen / 8, 0);
        chunk = buf.leak().as_mut_ptr().cast::<u8>();
    }

    let mut p = 0usize;
    let mut dataoff = 0usize;
    for i in 0..nvalues {
        if typbyval {
            range.values[i] = fetch_byval(&data[p..], typlen);
            p += typlen as usize;
        } else {
            let sz = if typlen > 0 {
                typlen as usize
            } else if typlen == -1 {
                // SAFETY: value image lies inside `data`.
                unsafe { varsize_any(data.as_ptr().add(p)) }
            } else {
                // SAFETY: as above.
                unsafe { cstring_len(data.as_ptr().add(p)) }
            };
            // SAFETY: chunk has datalen MAXALIGNed bytes; sz bytes from data.
            unsafe {
                let dst = chunk.add(dataoff);
                core::ptr::copy_nonoverlapping(data.as_ptr().add(p), dst, sz);
                range.values[i] = Datum::from_usize(dst as usize);
            }
            dataoff += maxalign(sz);
            p += sz;
        }
        debug_assert!(p <= data.len());
    }
    debug_assert!(p == data.len());

    Ok(range)
}

pub fn has_matching_range(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    colloid: Oid,
    ranges: &MinMaxMultiRanges,
    newval: Datum,
    attno: u16,
    typid: Oid,
) -> PgResult<bool> {
    if ranges.nranges == 0 {
        return Ok(false);
    }

    let minvalue = ranges.values[0];
    let maxvalue = ranges.values[(2 * ranges.nranges - 1) as usize];

    let lt = minmax_multi_get_strategy_procinfo(bdesc, attno, typid, BTLessStrategyNumber)?;
    if call_bool(mcx, &lt, colloid, newval, minvalue)? {
        return Ok(false);
    }

    let gt = minmax_multi_get_strategy_procinfo(bdesc, attno, typid, BTGreaterStrategyNumber)?;
    if call_bool(mcx, &gt, colloid, newval, maxvalue)? {
        return Ok(false);
    }

    let mut start: i32 = 0;
    let mut end: i32 = ranges.nranges - 1;
    loop {
        if start > end {
            return Ok(false);
        }
        let midpoint = (start + end) / 2;

        let minvalue = ranges.values[(2 * midpoint) as usize];
        let maxvalue = ranges.values[(2 * midpoint + 1) as usize];

        if call_bool(mcx, &lt, colloid, newval, minvalue)? {
            end = midpoint - 1;
            continue;
        }
        if call_bool(mcx, &gt, colloid, newval, maxvalue)? {
            start = midpoint + 1;
            continue;
        }
        return Ok(true);
    }
}

pub fn range_contains_value(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    colloid: Oid,
    attno: u16,
    typid: Oid,
    ranges: &MinMaxMultiRanges,
    newval: Datum,
    full: bool,
) -> PgResult<bool> {
    if has_matching_range(mcx, bdesc, colloid, ranges, newval, attno, typid)? {
        return Ok(true);
    }

    let eq = minmax_multi_get_strategy_procinfo(bdesc, attno, typid, BTEqualStrategyNumber)?;

    if ranges.nsorted >= 16 {
        let base = (2 * ranges.nranges) as usize;
        let cmp = ranges.cmp.clone();
        let sorted = &ranges.values[base..base + ranges.nsorted as usize];
        if bsearch_values(mcx, &cmp, ranges.colloid, sorted, newval)? {
            return Ok(true);
        }
    } else {
        for i in
            (2 * ranges.nranges) as usize..(2 * ranges.nranges + ranges.nsorted) as usize
        {
            if call_bool(mcx, &eq, colloid, newval, ranges.values[i])? {
                return Ok(true);
            }
        }
    }

    if !full {
        return Ok(false);
    }

    for i in (2 * ranges.nranges + ranges.nsorted) as usize
        ..(2 * ranges.nranges + ranges.nvalues) as usize
    {
        if call_bool(mcx, &eq, colloid, newval, ranges.values[i])? {
            return Ok(true);
        }
    }

    Ok(false)
}

fn bsearch_values(
    mcx: Mcx<'_>,
    cmp: &FmgrInfo,
    colloid: Oid,
    sorted: &[Datum],
    key: Datum,
) -> PgResult<bool> {
    let mut lo: isize = 0;
    let mut hi: isize = sorted.len() as isize - 1;
    while lo <= hi {
        let mid = (lo + hi) / 2;
        let c = compare_values(mcx, cmp, colloid, key, sorted[mid as usize])?;
        if c < 0 {
            hi = mid - 1;
        } else if c > 0 {
            lo = mid + 1;
        } else {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn fill_expanded_ranges(
    eranges: &mut [ExpandedRange],
    neranges: usize,
    ranges: &MinMaxMultiRanges,
) {
    debug_assert!(neranges == (ranges.nranges + ranges.nvalues) as usize);

    let mut idx = 0usize;
    for i in 0..ranges.nranges as usize {
        eranges[idx] = ExpandedRange {
            minval: ranges.values[2 * i],
            maxval: ranges.values[2 * i + 1],
            collapsed: false,
        };
        idx += 1;
    }
    for i in 0..ranges.nvalues as usize {
        let v = ranges.values[(2 * ranges.nranges) as usize + i];
        eranges[idx] = ExpandedRange { minval: v, maxval: v, collapsed: true };
        idx += 1;
    }
    debug_assert!(idx == neranges);
}

pub fn sort_expanded_ranges(
    mcx: Mcx<'_>,
    cmp: &FmgrInfo,
    colloid: Oid,
    eranges: &mut [ExpandedRange],
) -> PgResult<usize> {
    let neranges = eranges.len();
    debug_assert!(neranges > 0);

    pg_qsort_arg(eranges, |a, b| compare_expanded_ranges(mcx, cmp, colloid, a, b))?;

    let mut n = 1usize;
    for i in 1..neranges {
        let a = eranges[i - 1];
        let b = eranges[i];
        if compare_expanded_ranges(mcx, cmp, colloid, &a, &b)? == 0 {
            continue;
        }
        if i != n {
            eranges[n] = eranges[i];
        }
        n += 1;
    }
    debug_assert!(n > 0 && n <= neranges);
    Ok(n)
}

pub fn merge_overlapping_ranges(
    mcx: Mcx<'_>,
    cmp: &FmgrInfo,
    colloid: Oid,
    eranges: &mut [ExpandedRange],
    mut neranges: usize,
) -> PgResult<usize> {
    let mut idx = 0usize;
    while idx + 1 < neranges {
        if call_bool(mcx, cmp, colloid, eranges[idx].maxval, eranges[idx + 1].minval)? {
            idx += 1;
            continue;
        }

        if call_bool(mcx, cmp, colloid, eranges[idx].maxval, eranges[idx + 1].maxval)? {
            eranges[idx].maxval = eranges[idx + 1].maxval;
        }
        eranges[idx].collapsed = false;

        eranges.copy_within(idx + 2..neranges, idx + 1);
        neranges -= 1;
    }
    Ok(neranges)
}

pub fn build_distances<'mcx>(
    mcx: Mcx<'mcx>,
    distance_fn: &FmgrInfo,
    colloid: Oid,
    eranges: &[ExpandedRange],
) -> PgResult<Option<PgVec<'mcx, DistanceValue>>> {
    let neranges = eranges.len();
    debug_assert!(neranges > 0);
    if neranges == 1 {
        return Ok(None);
    }

    let ndistances = neranges - 1;
    let mut distances: PgVec<'mcx, DistanceValue> = vec_with_capacity_in(mcx, ndistances)?;
    for i in 0..ndistances {
        let mut f = distance_fn.clone();
        let r = function_call2_coll_in(
            &mut f,
            colloid,
            mcx,
            eranges[i].maxval,
            eranges[i + 1].minval,
        )?;
        distances.push(DistanceValue { index: i as i32, value: r.as_f64() });
    }

    // compare_distances: descending by value. C's qsort call here is pg_qsort
    // (port.h:478 remaps qsort to pg_qsort), so pg_qsort_arg tie order is the
    // parity requirement — which equal-distance gaps survive reduction.
    pg_qsort_arg(&mut distances, |da, db| {
        Ok(if da.value < db.value {
            1
        } else if da.value > db.value {
            -1
        } else {
            0
        })
    })?;

    Ok(Some(distances))
}

pub fn build_expanded_ranges<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: &FmgrInfo,
    colloid: Oid,
    ranges: &MinMaxMultiRanges,
) -> PgResult<(PgVec<'mcx, ExpandedRange>, usize)> {
    let neranges = (ranges.nranges + ranges.nvalues) as usize;
    let mut eranges: PgVec<'mcx, ExpandedRange> = vec_with_capacity_in(mcx, neranges)?;
    eranges.resize(
        neranges,
        ExpandedRange { minval: Datum::null(), maxval: Datum::null(), collapsed: false },
    );
    fill_expanded_ranges(&mut eranges, neranges, ranges);
    let n = sort_expanded_ranges(mcx, cmp, colloid, &mut eranges)?;
    Ok((eranges, n))
}

pub fn count_values(cranges: &[ExpandedRange]) -> i32 {
    let mut count = 0;
    for r in cranges {
        count += if r.collapsed { 1 } else { 2 };
    }
    count
}

pub fn reduce_expanded_ranges<'mcx>(
    mcx: Mcx<'mcx>,
    eranges: &mut [ExpandedRange],
    neranges: usize,
    distances: Option<&[DistanceValue]>,
    max_values: i32,
    cmp: &FmgrInfo,
    colloid: Oid,
) -> PgResult<usize> {
    let ndistances = neranges as i32 - 1;
    let keep = max_values / 2 - 1;

    if keep >= ndistances {
        return Ok(neranges);
    }

    let mut values: PgVec<'mcx, Datum> = vec_with_capacity_in(mcx, max_values as usize)?;
    values.push(eranges[0].minval);
    values.push(eranges[neranges - 1].maxval);

    let distances = distances.expect("ndistances > 0 implies distances");
    for d in distances.iter().take(keep as usize) {
        let index = d.index as usize;
        debug_assert!(index + 1 < neranges);
        values.push(eranges[index].maxval);
        values.push(eranges[index + 1].minval);
        debug_assert!(values.len() <= max_values as usize);
    }
    debug_assert!(values.len() % 2 == 0);

    pg_qsort_arg(&mut values, |a, b| compare_values(mcx, cmp, colloid, *a, *b))?;

    let nvalues = values.len();
    for i in 0..nvalues / 2 {
        let collapsed =
            compare_values(mcx, cmp, colloid, values[2 * i], values[2 * i + 1])? == 0;
        eranges[i] = ExpandedRange {
            minval: values[2 * i],
            maxval: values[2 * i + 1],
            collapsed,
        };
    }

    Ok(nvalues / 2)
}

pub fn store_expanded_ranges(
    ranges: &mut MinMaxMultiRanges,
    eranges: &[ExpandedRange],
) {
    let mut idx = 0usize;

    ranges.nranges = 0;
    for r in eranges {
        if !r.collapsed {
            ranges.values[idx] = r.minval;
            ranges.values[idx + 1] = r.maxval;
            idx += 2;
            ranges.nranges += 1;
        }
    }

    ranges.nvalues = 0;
    for r in eranges {
        if r.collapsed {
            ranges.values[idx] = r.minval;
            idx += 1;
            ranges.nvalues += 1;
        }
    }

    ranges.nsorted = ranges.nvalues;

    debug_assert!(count_values(eranges) == 2 * ranges.nranges + ranges.nvalues);
    debug_assert!(2 * ranges.nranges + ranges.nvalues <= ranges.maxvalues);
}

pub fn ensure_free_space_in_buffer(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    colloid: Oid,
    attno: u16,
    typid: Oid,
    range: &mut MinMaxMultiRanges,
) -> PgResult<bool> {
    if 2 * range.nranges + range.nvalues < range.maxvalues {
        return Ok(false);
    }

    let cmp = minmax_multi_get_strategy_procinfo(bdesc, attno, typid, BTLessStrategyNumber)?;

    range_deduplicate_values(mcx, range)?;

    if (2 * range.nranges + range.nvalues) as f64
        <= range.maxvalues as f64 * MINMAX_BUFFER_LOAD_FACTOR
    {
        return Ok(true);
    }

    // Scratch context per compaction (C's local AllocSet): distance/comparator
    // calls may allocate heavily and this can run many times per page range.
    {
        let ctx = ::mcx::MemoryContext::new_bump("minmax-multi context");
        let cmcx = ctx.mcx();

        let (mut eranges, mut neranges) = build_expanded_ranges(cmcx, &cmp, colloid, range)?;
        assert_check_expanded_ranges(cmcx, bdesc, colloid, attno, typid, &eranges[..neranges])?;

        let distance_fn = minmax_multi_get_procinfo(bdesc, attno, PROCNUM_DISTANCE)?;
        let distances = build_distances(cmcx, &distance_fn, colloid, &eranges[..neranges])?;

        neranges = reduce_expanded_ranges(
            cmcx,
            &mut eranges,
            neranges,
            distances.as_deref(),
            (range.maxvalues as f64 * MINMAX_BUFFER_LOAD_FACTOR) as i32,
            &cmp,
            colloid,
        )?;
        assert_check_expanded_ranges(cmcx, bdesc, colloid, attno, typid, &eranges[..neranges])?;

        debug_assert!(
            count_values(&eranges[..neranges]) as f64
                <= range.maxvalues as f64 * MINMAX_BUFFER_LOAD_FACTOR
        );

        store_expanded_ranges(range, &eranges[..neranges]);
    }
    assert_check_ranges(mcx, &cmp, colloid, range)?;

    Ok(true)
}

pub fn range_add_value(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    colloid: Oid,
    attno: u16,
    typid: Oid,
    attbyval: bool,
    attlen: i16,
    ranges: &mut MinMaxMultiRanges,
    newval: Datum,
) -> PgResult<bool> {
    let cmp = minmax_multi_get_strategy_procinfo(bdesc, attno, typid, BTLessStrategyNumber)?;

    assert_check_ranges(mcx, &cmp, colloid, ranges)?;

    let modified = ensure_free_space_in_buffer(mcx, bdesc, colloid, attno, typid, ranges)?;

    if range_contains_value(mcx, bdesc, colloid, attno, typid, ranges, newval, false)? {
        return Ok(modified);
    }

    let newval = ::adt_scalar::datum_ops::datum_copy(mcx, newval, attbyval, attlen)?;

    ranges.values[(2 * ranges.nranges + ranges.nvalues) as usize] = newval;
    ranges.nvalues += 1;

    if ranges.nvalues == 1 {
        ranges.nsorted = 1;
    }

    assert_check_ranges(mcx, &cmp, colloid, ranges)?;
    debug_assert!(range_contains_value(
        mcx, bdesc, colloid, attno, typid, ranges, newval, true
    )?);

    Ok(true)
}

pub fn compactify_ranges(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    ranges: &mut MinMaxMultiRanges,
    max_values: i32,
) -> PgResult<()> {
    if ranges.nranges * 2 + ranges.nvalues <= max_values && ranges.nsorted == ranges.nvalues {
        return Ok(());
    }

    let colloid = ranges.colloid;
    let cmp = minmax_multi_get_strategy_procinfo(
        bdesc,
        ranges.attno,
        ranges.typid,
        BTLessStrategyNumber,
    )?;
    let distance_fn = minmax_multi_get_procinfo(bdesc, ranges.attno, PROCNUM_DISTANCE)?;

    {
        let ctx = ::mcx::MemoryContext::new_bump("minmax-multi context");
        let cmcx = ctx.mcx();

        let (mut eranges, mut neranges) = build_expanded_ranges(cmcx, &cmp, colloid, ranges)?;
        let distances = build_distances(cmcx, &distance_fn, colloid, &eranges[..neranges])?;

        neranges = reduce_expanded_ranges(
            cmcx,
            &mut eranges,
            neranges,
            distances.as_deref(),
            max_values,
            &cmp,
            colloid,
        )?;

        debug_assert!(count_values(&eranges[..neranges]) <= max_values);

        store_expanded_ranges(ranges, &eranges[..neranges]);
    }
    assert_check_ranges(mcx, &cmp, colloid, ranges)?;
    Ok(())
}

fn assert_array_order(
    mcx: Mcx<'_>,
    cmp: &FmgrInfo,
    colloid: Oid,
    values: &[Datum],
) -> PgResult<()> {
    for i in 1..values.len() {
        debug_assert!(call_bool(mcx, cmp, colloid, values[i - 1], values[i])?);
    }
    Ok(())
}

pub fn assert_check_ranges(
    mcx: Mcx<'_>,
    cmp: &FmgrInfo,
    colloid: Oid,
    ranges: &MinMaxMultiRanges,
) -> PgResult<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    debug_assert!(ranges.nranges >= 0 && ranges.nsorted >= 0);
    debug_assert!(ranges.nvalues >= ranges.nsorted);
    debug_assert!(ranges.maxvalues >= 2 * ranges.nranges + ranges.nvalues);
    debug_assert!(ranges.typid != 0);

    let base = (2 * ranges.nranges) as usize;
    assert_array_order(mcx, cmp, colloid, &ranges.values[..base])?;
    assert_array_order(mcx, cmp, colloid, &ranges.values[base..base + ranges.nsorted as usize])?;

    if ranges.nranges > 0 {
        for i in 0..ranges.nvalues as usize {
            let value = ranges.values[base + i];
            if call_bool(mcx, cmp, colloid, value, ranges.values[0])? {
                continue;
            }
            if call_bool(mcx, cmp, colloid, ranges.values[base - 1], value)? {
                continue;
            }
            let mut start: i32 = 0;
            let mut end: i32 = ranges.nranges - 1;
            while start <= end {
                let midpoint = (start + end) / 2;
                if call_bool(mcx, cmp, colloid, value, ranges.values[(2 * midpoint) as usize])? {
                    end = midpoint - 1;
                    continue;
                }
                if call_bool(
                    mcx,
                    cmp,
                    colloid,
                    ranges.values[(2 * midpoint + 1) as usize],
                    value,
                )? {
                    start = midpoint + 1;
                    continue;
                }
                debug_assert!(false, "point value covered by a range");
                break;
            }
        }
    }

    if ranges.nsorted > 0 {
        let sorted = &ranges.values[base..base + ranges.nsorted as usize];
        for i in ranges.nsorted as usize..ranges.nvalues as usize {
            debug_assert!(!bsearch_values(
                mcx,
                &ranges.cmp,
                ranges.colloid,
                sorted,
                ranges.values[base + i]
            )?);
        }
    }
    Ok(())
}

pub fn assert_check_expanded_ranges(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    colloid: Oid,
    attno: u16,
    typid: Oid,
    eranges: &[ExpandedRange],
) -> PgResult<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    let eq = minmax_multi_get_strategy_procinfo(bdesc, attno, typid, BTEqualStrategyNumber)?;
    let lt = minmax_multi_get_strategy_procinfo(bdesc, attno, typid, BTLessStrategyNumber)?;

    for r in eranges {
        let ok = if r.collapsed {
            call_bool(mcx, &eq, colloid, r.minval, r.maxval)?
        } else {
            call_bool(mcx, &lt, colloid, r.minval, r.maxval)?
        };
        debug_assert!(ok);
    }
    for i in 1..eranges.len() {
        debug_assert!(call_bool(mcx, &lt, colloid, eranges[i - 1].maxval, eranges[i].minval)?);
    }
    Ok(())
}
