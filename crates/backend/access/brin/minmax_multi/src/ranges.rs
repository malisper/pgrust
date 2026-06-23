//! The minmax-multi range arithmetic: the in-memory [`MinmaxMultiRanges`]
//! buffer, the greedy-merge compaction spine, membership search, and the
//! sort/dedup helpers. Ported 1:1 from `brin_minmax_multi.c`; comparisons and
//! distances dispatch by OID through the canonical-`Datum` fmgr lane (`cmp` /
//! `distance` are the resolved procedure OIDs from the caller's opaque cache).

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use brin::{BrinDesc, MinmaxMultiRanges};
use ::types_core::primitive::{AttrNumber, Oid};
use ::types_error::PgResult;
use types_tuple::heaptuple::Datum;

use crate::codec::SerializedRanges;
use crate::{
    call_distance2, call_strategy2, minmax_multi_get_strategy_procinfo, BTEqualStrategyNumber,
    BTGreaterStrategyNumber, BTLessStrategyNumber, MINMAX_BUFFER_LOAD_FACTOR,
};

/// `ExpandedRange` (brin_minmax_multi.c:237): an interval `[minval, maxval]`,
/// flagged `collapsed` when `minval == maxval`.
#[derive(Clone)]
pub struct ExpandedRange<'mcx> {
    pub minval: Datum<'mcx>,
    pub maxval: Datum<'mcx>,
    pub collapsed: bool,
}

impl<'mcx> ExpandedRange<'mcx> {
    pub fn empty() -> Self {
        Self {
            minval: Datum::null(),
            maxval: Datum::null(),
            collapsed: false,
        }
    }
}

/// `DistanceValue` (brin_minmax_multi.c:248): a gap's index and its distance.
#[derive(Clone, Copy)]
pub struct DistanceValue {
    pub index: i32,
    pub value: f64,
}

// ---------------------------------------------------------------------------
// minmax_multi_init / range_deduplicate_values (brin_minmax_multi.c:485 / 515)
// ---------------------------------------------------------------------------

/// `minmax_multi_init(maxvalues)` (brin_minmax_multi.c:485): allocate a
/// [`MinmaxMultiRanges`] sized for `maxvalues` boundary values.
pub fn minmax_multi_init<'mcx>(
    mcx: Mcx<'mcx>,
    maxvalues: i32,
) -> PgResult<MinmaxMultiRanges<'mcx>> {
    debug_assert!(maxvalues > 0);

    let mut values: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, maxvalues as usize)?;
    for _ in 0..maxvalues {
        values.push(Datum::null());
    }

    Ok(MinmaxMultiRanges {
        typid: 0,
        colloid: 0,
        attno: 0,
        cmp: 0,
        nranges: 0,
        nsorted: 0,
        nvalues: 0,
        maxvalues,
        target_maxvalues: 0,
        values,
    })
}

/// `range_deduplicate_values(range)` (brin_minmax_multi.c:515): sort and
/// deduplicate the single-point values.
pub fn range_deduplicate_values<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    range: &mut MinmaxMultiRanges<'mcx>,
) -> PgResult<()> {
    // If there are no unsorted values, we're done.
    if range.nsorted == range.nvalues {
        return Ok(());
    }

    let start = (2 * range.nranges) as usize;
    let colloid = range.colloid;

    // qsort_arg(&values[start], nvalues, compare_values, cxt)
    sort_datums_by_compare(
        mcx,
        cmp,
        colloid,
        &mut range.values[start..start + range.nvalues as usize],
    )?;

    let mut n = 1usize;
    for i in 1..range.nvalues as usize {
        // same as preceding value, so skip it
        if compare_values(
            mcx,
            cmp,
            colloid,
            &range.values[start + i - 1].clone(),
            &range.values[start + i].clone(),
        )? == 0
        {
            continue;
        }
        range.values[start + n] = range.values[start + i].clone();
        n += 1;
    }

    range.nvalues = n as i32;
    range.nsorted = n as i32;

    Ok(())
}

// ---------------------------------------------------------------------------
// (de)serialize between MinmaxMultiRanges and SerializedRanges
//   (brin_minmax_multi.c:575 / 720)
// ---------------------------------------------------------------------------

/// `brin_range_serialize(range)` (brin_minmax_multi.c:575): build the compact
/// [`SerializedRanges`] from a [`MinmaxMultiRanges`] (after deduplicating the
/// unsorted point part). The actual byte encoding lives in
/// [`crate::codec::serialize_summary`].
pub fn brin_range_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    _colloid: Oid,
    range: &mut MinmaxMultiRanges<'mcx>,
) -> PgResult<SerializedRanges<'mcx>> {
    debug_assert!(range.nranges >= 0);
    debug_assert!(range.nsorted >= 0);
    debug_assert!(range.nvalues >= 0);
    debug_assert!(range.maxvalues > 0);
    debug_assert!(range.target_maxvalues > 0);
    // at this point the range should be compacted to the target size
    debug_assert!(2 * range.nranges + range.nvalues <= range.target_maxvalues);
    debug_assert!(range.target_maxvalues <= range.maxvalues);
    debug_assert!(range.nvalues >= range.nsorted);

    // deduplicate values, if there's an unsorted part
    range_deduplicate_values(mcx, cmp, range)?;

    let nvalues = 2 * range.nranges + range.nvalues;

    let mut values: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, nvalues as usize)?;
    for v in range.values.iter().take(nvalues as usize) {
        values.push(v.clone());
    }

    Ok(SerializedRanges {
        typid: range.typid,
        nranges: range.nranges,
        nvalues: range.nvalues,
        maxvalues: range.target_maxvalues,
        values,
    })
}

/// `brin_range_deserialize(maxvalues, serialized)` (brin_minmax_multi.c:720):
/// deserialize a [`SerializedRanges`] into a [`MinmaxMultiRanges`] sized for
/// `maxvalues`.
pub fn brin_range_deserialize<'mcx>(
    mcx: Mcx<'mcx>,
    maxvalues: i32,
    serialized: &SerializedRanges<'mcx>,
) -> PgResult<MinmaxMultiRanges<'mcx>> {
    debug_assert!(serialized.nranges >= 0);
    debug_assert!(serialized.nvalues >= 0);
    debug_assert!(serialized.maxvalues > 0);

    let nvalues = 2 * serialized.nranges + serialized.nvalues;
    debug_assert!(nvalues <= serialized.maxvalues);
    debug_assert!(serialized.maxvalues <= maxvalues);

    let mut range = minmax_multi_init(mcx, maxvalues)?;

    range.nranges = serialized.nranges;
    range.nvalues = serialized.nvalues;
    range.nsorted = serialized.nvalues;
    range.maxvalues = maxvalues;
    range.target_maxvalues = serialized.maxvalues;
    range.typid = serialized.typid;

    for (i, v) in serialized.values.iter().take(nvalues as usize).enumerate() {
        range.values[i] = v.clone();
    }

    Ok(range)
}

// ---------------------------------------------------------------------------
// comparators (brin_minmax_multi.c:895 / 857 / 1304)
// ---------------------------------------------------------------------------

/// `compare_values(a, b, arg)` (brin_minmax_multi.c:895): compare via the cached
/// less-than comparator, returning -1 / 0 / 1.
pub fn compare_values<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    a: &Datum<'mcx>,
    b: &Datum<'mcx>,
) -> PgResult<i32> {
    if call_strategy2(mcx, cmp, colloid, a, b)? {
        return Ok(-1);
    }
    if call_strategy2(mcx, cmp, colloid, b, a)? {
        return Ok(1);
    }
    Ok(0)
}

/// `compare_expanded_ranges(a, b, arg)` (brin_minmax_multi.c:857): order by
/// minval then maxval, using the cached less-than comparator.
pub fn compare_expanded_ranges<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    ra: &ExpandedRange<'mcx>,
    rb: &ExpandedRange<'mcx>,
) -> PgResult<i32> {
    if call_strategy2(mcx, cmp, colloid, &ra.minval, &rb.minval)? {
        return Ok(-1);
    }
    if call_strategy2(mcx, cmp, colloid, &rb.minval, &ra.minval)? {
        return Ok(1);
    }
    if call_strategy2(mcx, cmp, colloid, &ra.maxval, &rb.maxval)? {
        return Ok(-1);
    }
    if call_strategy2(mcx, cmp, colloid, &rb.maxval, &ra.maxval)? {
        return Ok(1);
    }
    Ok(0)
}

/// `compare_distances(a, b)` (brin_minmax_multi.c:1304): sort distances in
/// *descending* order (longest gaps first).
fn compare_distances(da: &DistanceValue, db: &DistanceValue) -> core::cmp::Ordering {
    if da.value < db.value {
        core::cmp::Ordering::Greater
    } else if da.value > db.value {
        core::cmp::Ordering::Less
    } else {
        core::cmp::Ordering::Equal
    }
}

// ---------------------------------------------------------------------------
// range membership (brin_minmax_multi.c:920 / 1044)
// ---------------------------------------------------------------------------

/// `has_matching_range(bdesc, colloid, ranges, newval, attno, typid)`
/// (brin_minmax_multi.c:920): binary-search the regular ranges to see if
/// `newval` falls into one of them.
pub fn has_matching_range<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    colloid: Oid,
    ranges: &MinmaxMultiRanges<'mcx>,
    newval: &Datum<'mcx>,
    attno: AttrNumber,
    typid: Oid,
) -> PgResult<bool> {
    if ranges.nranges == 0 {
        return Ok(false);
    }

    let minvalue0 = &ranges.values[0];
    let maxvalue_last = &ranges.values[(2 * ranges.nranges - 1) as usize];

    // less than the absolute minimum?
    let lt = minmax_multi_get_strategy_procinfo(bdesc, attno, typid, BTLessStrategyNumber)?;
    if call_strategy2(mcx, lt, colloid, newval, minvalue0)? {
        return Ok(false);
    }

    // greater than the existing maximum?
    let gt = minmax_multi_get_strategy_procinfo(bdesc, attno, typid, BTGreaterStrategyNumber)?;
    if call_strategy2(mcx, gt, colloid, newval, maxvalue_last)? {
        return Ok(false);
    }

    // binary search on individual ranges
    let mut start: i32 = 0;
    let mut end: i32 = ranges.nranges - 1;
    loop {
        // ran out of ranges
        if start > end {
            return Ok(false);
        }

        let midpoint = (start + end) / 2;

        let minvalue = &ranges.values[(2 * midpoint) as usize];
        let maxvalue = &ranges.values[(2 * midpoint + 1) as usize];

        // smaller than this range's min -> recurse left
        if call_strategy2(mcx, lt, colloid, newval, minvalue)? {
            end = midpoint - 1;
            continue;
        }

        // greater than this range's max -> recurse right
        if call_strategy2(mcx, gt, colloid, newval, maxvalue)? {
            start = midpoint + 1;
            continue;
        }

        // found a matching range
        return Ok(true);
    }
}

/// `range_contains_value(bdesc, colloid, attno, typid, ranges, newval, full)`
/// (brin_minmax_multi.c:1044): whether `newval` is in the range list. `full`
/// controls whether the unsorted point part is searched too.
pub fn range_contains_value<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    colloid: Oid,
    attno: AttrNumber,
    typid: Oid,
    ranges: &MinmaxMultiRanges<'mcx>,
    newval: &Datum<'mcx>,
    full: bool,
) -> PgResult<bool> {
    // inspect the ranges first
    if has_matching_range(mcx, bdesc, colloid, ranges, newval, attno, typid)? {
        return Ok(true);
    }

    let eq = minmax_multi_get_strategy_procinfo(bdesc, attno, typid, BTEqualStrategyNumber)?;
    let cmp = minmax_multi_get_strategy_procinfo(bdesc, attno, typid, BTLessStrategyNumber)?;

    // sequential or binary search of the sorted part
    if ranges.nsorted >= 16 {
        let base = (2 * ranges.nranges) as usize;
        if bsearch_datum(
            mcx,
            cmp,
            colloid,
            &ranges.values[base..base + ranges.nsorted as usize],
            newval,
        )?
        .is_some()
        {
            return Ok(true);
        }
    } else {
        let start = (2 * ranges.nranges) as usize;
        let stop = (2 * ranges.nranges + ranges.nsorted) as usize;
        for i in start..stop {
            if call_strategy2(mcx, eq, colloid, newval, &ranges.values[i])? {
                return Ok(true);
            }
        }
    }

    if !full {
        return Ok(false);
    }

    // inspect the unsorted part
    let start = (2 * ranges.nranges + ranges.nsorted) as usize;
    let stop = (2 * ranges.nranges + ranges.nvalues) as usize;
    for i in start..stop {
        if call_strategy2(mcx, eq, colloid, newval, &ranges.values[i])? {
            return Ok(true);
        }
    }

    Ok(false)
}

// ---------------------------------------------------------------------------
// expanded ranges (brin_minmax_multi.c:1133 .. 1591)
// ---------------------------------------------------------------------------

/// `fill_expanded_ranges(eranges, neranges, ranges)` (brin_minmax_multi.c:1133):
/// expand `ranges` into the `eranges` slice (`nranges` regular ranges then
/// `nvalues` collapsed points).
pub fn fill_expanded_ranges<'mcx>(
    eranges: &mut [ExpandedRange<'mcx>],
    neranges: i32,
    ranges: &MinmaxMultiRanges<'mcx>,
) {
    debug_assert_eq!(neranges, ranges.nranges + ranges.nvalues);

    let mut idx = 0usize;
    for i in 0..ranges.nranges as usize {
        eranges[idx].minval = ranges.values[2 * i].clone();
        eranges[idx].maxval = ranges.values[2 * i + 1].clone();
        eranges[idx].collapsed = false;
        idx += 1;
        debug_assert!(idx as i32 <= neranges);
    }

    for i in 0..ranges.nvalues as usize {
        let v = ranges.values[(2 * ranges.nranges) as usize + i].clone();
        eranges[idx].minval = v.clone();
        eranges[idx].maxval = v;
        eranges[idx].collapsed = true;
        idx += 1;
        debug_assert!(idx as i32 <= neranges);
    }

    debug_assert_eq!(idx as i32, neranges);
}

/// `sort_expanded_ranges(cmp, colloid, eranges, neranges)`
/// (brin_minmax_multi.c:1178): sort and deduplicate the expanded ranges,
/// returning the deduplicated count.
pub fn sort_expanded_ranges<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    eranges: &mut [ExpandedRange<'mcx>],
    neranges: i32,
) -> PgResult<i32> {
    debug_assert!(neranges > 0);

    sort_expanded_by_compare(mcx, cmp, colloid, &mut eranges[..neranges as usize])?;

    // deduplicate the ranges
    let mut n = 1usize;
    for i in 1..neranges as usize {
        let a = eranges[i - 1].clone();
        let b = eranges[i].clone();
        if compare_expanded_ranges(mcx, cmp, colloid, &a, &b)? == 0 {
            continue;
        }
        if i != n {
            eranges[n] = eranges[i].clone();
        }
        n += 1;
    }

    debug_assert!(n > 0 && n as i32 <= neranges);
    Ok(n as i32)
}

/// `merge_overlapping_ranges(cmp, colloid, eranges, neranges)`
/// (brin_minmax_multi.c:1230): merge overlapping (pre-sorted) expanded ranges in
/// place, returning the new count.
pub fn merge_overlapping_ranges<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    eranges: &mut [ExpandedRange<'mcx>],
    mut neranges: i32,
) -> PgResult<i32> {
    let mut idx = 0i32;
    while idx < neranges - 1 {
        let i = idx as usize;
        // no overlap if maxval[idx] < minval[idx+1]
        if call_strategy2(mcx, cmp, colloid, &eranges[i].maxval, &eranges[i + 1].minval)? {
            idx += 1;
            continue;
        }

        // they overlap; keep the larger upper bound
        if call_strategy2(mcx, cmp, colloid, &eranges[i].maxval, &eranges[i + 1].maxval)? {
            eranges[i].maxval = eranges[i + 1].maxval.clone();
        }

        // no longer collapsed
        eranges[i].collapsed = false;

        // shift the remaining ranges left by 1 (memmove from idx+2 to idx+1)
        let to_move = (neranges - (idx + 2)) as usize;
        for k in 0..to_move {
            eranges[i + 1 + k] = eranges[i + 2 + k].clone();
        }

        neranges -= 1;
    }

    Ok(neranges)
}

/// `build_distances(distanceFn, colloid, eranges, neranges)`
/// (brin_minmax_multi.c:1328): compute the gap distance between consecutive
/// ranges and sort them in descending order. Returns `None` for a single range.
pub fn build_distances<'mcx>(
    mcx: Mcx<'mcx>,
    distance: Oid,
    colloid: Oid,
    eranges: &[ExpandedRange<'mcx>],
    neranges: i32,
) -> PgResult<Option<PgVec<'mcx, DistanceValue>>> {
    debug_assert!(neranges > 0);

    if neranges == 1 {
        return Ok(None);
    }

    let ndistances = (neranges - 1) as usize;
    let mut distances: PgVec<'mcx, DistanceValue> = vec_with_capacity_in(mcx, ndistances)?;

    for i in 0..ndistances {
        let a1 = &eranges[i].maxval;
        let a2 = &eranges[i + 1].minval;
        let value = call_distance2(mcx, distance, colloid, a1, a2)?;
        distances.push(DistanceValue {
            index: i as i32,
            value,
        });
    }

    // qsort(distances, compare_distances): descending by value.
    distances.sort_by(compare_distances);

    Ok(Some(distances))
}

/// `build_expanded_ranges(cmp, colloid, ranges)` (brin_minmax_multi.c:1385):
/// build and sort/dedup the expanded ranges for `ranges`.
pub fn build_expanded_ranges<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    ranges: &MinmaxMultiRanges<'mcx>,
) -> PgResult<(PgVec<'mcx, ExpandedRange<'mcx>>, i32)> {
    let neranges = ranges.nranges + ranges.nvalues;
    let mut eranges: PgVec<'mcx, ExpandedRange<'mcx>> =
        vec_with_capacity_in(mcx, neranges as usize)?;
    for _ in 0..neranges {
        eranges.push(ExpandedRange::empty());
    }

    fill_expanded_ranges(&mut eranges, neranges, ranges);

    let neranges = sort_expanded_ranges(mcx, cmp, colloid, &mut eranges, neranges)?;

    Ok((eranges, neranges))
}

/// `count_values(cranges, ncranges)` (brin_minmax_multi.c:1414): boundary values
/// needed to store the expanded ranges (1 per collapsed, 2 per regular).
pub fn count_values(cranges: &[ExpandedRange<'_>], ncranges: i32) -> i32 {
    let mut count = 0;
    for r in cranges.iter().take(ncranges as usize) {
        if r.collapsed {
            count += 1;
        } else {
            count += 2;
        }
    }
    count
}

/// `reduce_expanded_ranges(eranges, neranges, distances, max_values, ...)`
/// (brin_minmax_multi.c:1475): merge ranges by their largest gaps until the
/// boundary-value count drops below `max_values`.
pub fn reduce_expanded_ranges<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    eranges: &mut [ExpandedRange<'mcx>],
    neranges: i32,
    distances: Option<&[DistanceValue]>,
    max_values: i32,
) -> PgResult<i32> {
    let ndistances = neranges - 1;
    let keep = max_values / 2 - 1;

    // maybe already low enough
    if keep >= ndistances {
        return Ok(neranges);
    }

    // collect the boundary values
    let mut values: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, max_values as usize)?;

    // global min/max from the first/last range
    values.push(eranges[0].minval.clone());
    values.push(eranges[(neranges - 1) as usize].maxval.clone());

    let distances = distances.expect("distances must be present when reducing");

    for d in distances.iter().take(keep as usize) {
        let index = d.index;
        debug_assert!(index >= 0 && (index + 1) < neranges);
        values.push(eranges[index as usize].maxval.clone());
        values.push(eranges[(index + 1) as usize].minval.clone());
        debug_assert!(values.len() as i32 <= max_values);
    }

    debug_assert!(values.len() % 2 == 0);

    // qsort_arg(values, compare_values, cxt)
    sort_datums_by_compare(mcx, cmp, colloid, &mut values)?;

    let nvalues = values.len() as i32;
    for i in 0..(nvalues / 2) as usize {
        eranges[i].minval = values[2 * i].clone();
        eranges[i].maxval = values[2 * i + 1].clone();
        eranges[i].collapsed =
            compare_values(mcx, cmp, colloid, &values[2 * i], &values[2 * i + 1])? == 0;
    }

    Ok(nvalues / 2)
}

/// `store_expanded_ranges(ranges, eranges, neranges)`
/// (brin_minmax_multi.c:1557): store the boundary values from `eranges` back
/// into `ranges` (regular ranges first, then collapsed points).
pub fn store_expanded_ranges<'mcx>(
    ranges: &mut MinmaxMultiRanges<'mcx>,
    eranges: &[ExpandedRange<'mcx>],
    neranges: i32,
) {
    let mut idx = 0usize;

    // first copy in the regular ranges
    ranges.nranges = 0;
    for r in eranges.iter().take(neranges as usize) {
        if !r.collapsed {
            ranges.values[idx] = r.minval.clone();
            idx += 1;
            ranges.values[idx] = r.maxval.clone();
            idx += 1;
            ranges.nranges += 1;
        }
    }

    // now copy in the collapsed ones
    ranges.nvalues = 0;
    for r in eranges.iter().take(neranges as usize) {
        if r.collapsed {
            ranges.values[idx] = r.minval.clone();
            idx += 1;
            ranges.nvalues += 1;
        }
    }

    // all the values are sorted
    ranges.nsorted = ranges.nvalues;

    debug_assert_eq!(
        count_values(eranges, neranges),
        2 * ranges.nranges + ranges.nvalues
    );
    debug_assert!(2 * ranges.nranges + ranges.nvalues <= ranges.maxvalues);
}

// ---------------------------------------------------------------------------
// ensure_free_space_in_buffer / range_add_value / compactify_ranges
//   (brin_minmax_multi.c:1600 / 1701 / 1787)
// ---------------------------------------------------------------------------

/// `ensure_free_space_in_buffer(bdesc, colloid, attno, attr, range)`
/// (brin_minmax_multi.c:1600): make room for at least one new value,
/// deduplicating and compacting as needed.
pub fn ensure_free_space_in_buffer<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    colloid: Oid,
    attno: AttrNumber,
    attr_typid: Oid,
    range: &mut MinmaxMultiRanges<'mcx>,
) -> PgResult<bool> {
    // free space already?
    if 2 * range.nranges + range.nvalues < range.maxvalues {
        return Ok(false);
    }

    // we'll certainly need the comparator
    let cmp = minmax_multi_get_strategy_procinfo(bdesc, attno, attr_typid, BTLessStrategyNumber)?;

    // deduplicate values, if there's an unsorted part
    range_deduplicate_values(mcx, cmp, range)?;

    // did deduplication free enough?
    if (2 * range.nranges + range.nvalues) as f64
        <= range.maxvalues as f64 * MINMAX_BUFFER_LOAD_FACTOR
    {
        return Ok(true);
    }

    // combine some of the existing ranges
    let (mut eranges, mut neranges) = build_expanded_ranges(mcx, cmp, colloid, range)?;

    AssertCheckExpandedRanges(mcx, cmp, colloid, &eranges, neranges)?;

    let distance = crate::minmax_multi_get_procinfo(bdesc, attno, crate::PROCNUM_DISTANCE)?;
    let distances = build_distances(mcx, distance, colloid, &eranges, neranges)?;

    neranges = reduce_expanded_ranges(
        mcx,
        cmp,
        colloid,
        &mut eranges,
        neranges,
        distances.as_deref(),
        (range.maxvalues as f64 * MINMAX_BUFFER_LOAD_FACTOR) as i32,
    )?;

    AssertCheckExpandedRanges(mcx, cmp, colloid, &eranges, neranges)?;

    debug_assert!(
        (count_values(&eranges, neranges) as f64)
            <= range.maxvalues as f64 * MINMAX_BUFFER_LOAD_FACTOR
    );

    store_expanded_ranges(range, &eranges, neranges);

    AssertCheckRanges(mcx, cmp, colloid, range)?;

    Ok(true)
}

/// `range_add_value(bdesc, colloid, attno, attr, ranges, newval)`
/// (brin_minmax_multi.c:1701): add `newval`, returning whether the range changed.
pub fn range_add_value<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    colloid: Oid,
    attno: AttrNumber,
    attr_typid: Oid,
    attr_byval: bool,
    attr_len: i16,
    ranges: &mut MinmaxMultiRanges<'mcx>,
    newval: &Datum<'mcx>,
) -> PgResult<bool> {
    // we'll certainly need the comparator
    let cmp = minmax_multi_get_strategy_procinfo(bdesc, attno, attr_typid, BTLessStrategyNumber)?;

    AssertCheckRanges(mcx, cmp, colloid, ranges)?;

    // make room for at least one new value
    let mut modified = ensure_free_space_in_buffer(mcx, bdesc, colloid, attno, attr_typid, ranges)?;

    // bail out if already covered (not searching the unsorted part)
    if range_contains_value(mcx, bdesc, colloid, attno, attr_typid, ranges, newval, false)? {
        return Ok(modified);
    }

    // make a copy of the value (datumCopy(newval, attbyval, attlen))
    let newval = scalar_seams::datum_copy::call(mcx, newval, attr_byval, attr_len)?;

    // insert into the values array
    let pos = (2 * ranges.nranges + ranges.nvalues) as usize;
    ranges.values[pos] = newval.clone();
    ranges.nvalues += 1;

    // first value can be considered sorted
    if ranges.nvalues == 1 {
        ranges.nsorted = 1;
    }

    AssertCheckRanges(mcx, cmp, colloid, ranges)?;

    // Check the range contains the value we just added.
    debug_assert!(range_contains_value(
        mcx, bdesc, colloid, attno, attr_typid, ranges, &newval, true
    )?);

    modified = true;
    Ok(modified)
}

/// `compactify_ranges(bdesc, ranges, max_values)` (brin_minmax_multi.c:1787):
/// compact the accumulated values down to `max_values`, used at serialize time.
pub fn compactify_ranges<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    distance: Oid,
    ranges: &mut MinmaxMultiRanges<'mcx>,
    max_values: i32,
) -> PgResult<()> {
    // do we need to compact anything?
    if (ranges.nranges * 2 + ranges.nvalues <= max_values) && (ranges.nsorted == ranges.nvalues) {
        return Ok(());
    }

    let colloid = ranges.colloid;

    let (mut eranges, mut neranges) = build_expanded_ranges(mcx, cmp, colloid, ranges)?;
    let distances = build_distances(mcx, distance, colloid, &eranges, neranges)?;

    neranges = reduce_expanded_ranges(
        mcx,
        cmp,
        colloid,
        &mut eranges,
        neranges,
        distances.as_deref(),
        max_values,
    )?;

    debug_assert!(count_values(&eranges, neranges) <= max_values);

    store_expanded_ranges(ranges, &eranges, neranges);

    AssertCheckRanges(mcx, cmp, colloid, ranges)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// sort/search helpers (qsort_arg / bsearch_arg replacements).
// ---------------------------------------------------------------------------

/// Sort a slice of datums by the less-than comparator (a fallible insertion sort
/// propagating comparator errors; replaces the C `qsort_arg`).
fn sort_datums_by_compare<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    values: &mut [Datum<'mcx>],
) -> PgResult<()> {
    for i in 1..values.len() {
        let mut j = i;
        while j > 0 {
            let a = values[j - 1].clone();
            let b = values[j].clone();
            if compare_values(mcx, cmp, colloid, &a, &b)? > 0 {
                values.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

/// Sort expanded ranges by `compare_expanded_ranges` (fallible insertion sort).
fn sort_expanded_by_compare<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    ranges: &mut [ExpandedRange<'mcx>],
) -> PgResult<()> {
    for i in 1..ranges.len() {
        let mut j = i;
        while j > 0 {
            let a = ranges[j - 1].clone();
            let b = ranges[j].clone();
            if compare_expanded_ranges(mcx, cmp, colloid, &a, &b)? > 0 {
                ranges.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

/// `bsearch_arg(&key, base, n, compare_values, cxt)`: binary search of a sorted
/// datum slice, returning the matching index (or `None`).
fn bsearch_datum<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    base: &[Datum<'mcx>],
    key: &Datum<'mcx>,
) -> PgResult<Option<usize>> {
    let mut lo = 0i64;
    let mut hi = base.len() as i64 - 1;
    while lo <= hi {
        let mid = ((lo + hi) / 2) as usize;
        let c = compare_values(mcx, cmp, colloid, key, &base[mid])?;
        match c.cmp(&0) {
            core::cmp::Ordering::Less => hi = mid as i64 - 1,
            core::cmp::Ordering::Greater => lo = mid as i64 + 1,
            core::cmp::Ordering::Equal => return Ok(Some(mid)),
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Assertion checkers (USE_ASSERT_CHECKING; debug builds only).
//   AssertArrayOrder / AssertCheckRanges / AssertCheckExpandedRanges
// ---------------------------------------------------------------------------

/// `AssertArrayOrder(cmp, colloid, values, nvalues)` (brin_minmax_multi.c:278):
/// the values must be strictly less-than ordered.
fn AssertArrayOrder<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    values: &[Datum<'mcx>],
    nvalues: i32,
) -> PgResult<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    let mut i: i32 = 0;
    while i < nvalues - 1 {
        let idx = i as usize;
        let lt = call_strategy2(mcx, cmp, colloid, &values[idx], &values[idx + 1])?;
        debug_assert!(lt);
        i += 1;
    }
    Ok(())
}

/// `AssertCheckRanges(ranges, cmpFn, colloid)` (brin_minmax_multi.c:295):
/// structural invariants of a [`MinmaxMultiRanges`]. A no-op outside debug.
pub fn AssertCheckRanges<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    ranges: &MinmaxMultiRanges<'mcx>,
) -> PgResult<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    debug_assert!(ranges.nranges >= 0);
    debug_assert!(ranges.nsorted >= 0);
    debug_assert!(ranges.nvalues >= ranges.nsorted);
    debug_assert!(ranges.maxvalues >= 2 * ranges.nranges + ranges.nvalues);
    debug_assert!(ranges.typid != 0);

    // ranges are strictly ordered
    AssertArrayOrder(mcx, cmp, colloid, &ranges.values, 2 * ranges.nranges)?;
    // sorted points are strictly ordered
    let base = (2 * ranges.nranges) as usize;
    AssertArrayOrder(mcx, cmp, colloid, &ranges.values[base..], ranges.nsorted)?;

    // Check that none of the values are not covered by ranges (both sorted and
    // unsorted) (brin_minmax_multi.c:322-399).
    if ranges.nranges > 0 {
        for i in 0..ranges.nvalues as usize {
            let minvalue0 = &ranges.values[0];
            let maxvalue_last = &ranges.values[(2 * ranges.nranges - 1) as usize];
            let value = &ranges.values[base + i];

            // If the value is smaller than the lower bound in the first range
            // then it cannot possibly be in any of the ranges.
            if call_strategy2(mcx, cmp, colloid, value, minvalue0)? {
                continue;
            }

            // Likewise, if the value is larger than the upper bound of the final
            // range, then it cannot possibly be inside any of the ranges.
            if call_strategy2(mcx, cmp, colloid, maxvalue_last, value)? {
                continue;
            }

            // bsearch the ranges to see if 'value' fits within any of them.
            let mut start: i32 = 0;
            let mut end: i32 = ranges.nranges - 1;
            loop {
                // this means we ran out of ranges in the last step
                if start > end {
                    break;
                }
                let midpoint = (start + end) / 2;

                let minvalue = &ranges.values[(2 * midpoint) as usize];
                let maxvalue = &ranges.values[(2 * midpoint + 1) as usize];

                // Is the value smaller than the minval? Recurse left.
                if call_strategy2(mcx, cmp, colloid, value, minvalue)? {
                    end = midpoint - 1;
                    continue;
                }

                // Is the value greater than the maxval? Recurse right.
                if call_strategy2(mcx, cmp, colloid, maxvalue, value)? {
                    start = midpoint + 1;
                    continue;
                }

                // hey, we found a matching range
                debug_assert!(false, "value unexpectedly covered by a range");
                break;
            }
        }
    }

    // and values in the unsorted part must not be in the sorted part
    // (brin_minmax_multi.c:401-417) — bsearch_arg over the sorted prefix using
    // the range's cached compare_values context (ranges->cmp / ranges->colloid).
    if ranges.nsorted > 0 {
        let sorted = &ranges.values[base..base + ranges.nsorted as usize];
        for i in ranges.nsorted as usize..ranges.nvalues as usize {
            let value = &ranges.values[base + i];
            let found = bsearch_compare_values(mcx, ranges.cmp, ranges.colloid, sorted, value)?;
            debug_assert!(!found, "unsorted value unexpectedly present in the sorted part");
        }
    }

    Ok(())
}

/// `bsearch_arg(&value, sorted, nsorted, sizeof(Datum), compare_values, &cxt)`
/// (brin_minmax_multi.c:413): binary-search a strictly-ordered `Datum` slice for
/// `value` using [`compare_values`]; returns whether a match was found. Used only
/// by the debug `AssertCheckRanges` invariant.
fn bsearch_compare_values<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    sorted: &[Datum<'mcx>],
    value: &Datum<'mcx>,
) -> PgResult<bool> {
    let mut lo: isize = 0;
    let mut hi: isize = sorted.len() as isize - 1;
    while lo <= hi {
        let mid = (lo + hi) / 2;
        let r = compare_values(mcx, cmp, colloid, value, &sorted[mid as usize])?;
        if r < 0 {
            hi = mid - 1;
        } else if r > 0 {
            lo = mid + 1;
        } else {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `AssertCheckExpandedRanges(bdesc, colloid, attno, ranges, nranges)`
/// (brin_minmax_multi.c:425): the expanded ranges are individually valid,
/// ordered, and non-overlapping. A no-op outside debug.
#[allow(non_snake_case)]
pub fn AssertCheckExpandedRanges<'mcx>(
    mcx: Mcx<'mcx>,
    cmp: Oid,
    colloid: Oid,
    ranges: &[ExpandedRange<'mcx>],
    nranges: i32,
) -> PgResult<()> {
    if !cfg!(debug_assertions) {
        return Ok(());
    }
    // each range valid: minval (==|<) maxval
    for r in ranges.iter().take(nranges as usize) {
        let res = if r.collapsed {
            // collapsed: minval == maxval, i.e. !(min < max) && !(max < min)
            !call_strategy2(mcx, cmp, colloid, &r.minval, &r.maxval)?
                && !call_strategy2(mcx, cmp, colloid, &r.maxval, &r.minval)?
        } else {
            call_strategy2(mcx, cmp, colloid, &r.minval, &r.maxval)?
        };
        debug_assert!(res);
    }
    // ordered, non-overlapping: maxval[i] < minval[i+1]
    let mut i: i32 = 0;
    while i < nranges - 1 {
        let idx = i as usize;
        let r = call_strategy2(mcx, cmp, colloid, &ranges[idx].maxval, &ranges[idx + 1].minval)?;
        debug_assert!(r);
        i += 1;
    }
    Ok(())
}
