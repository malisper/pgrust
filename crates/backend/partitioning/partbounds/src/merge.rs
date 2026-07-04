use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_pathnodes::{
    DatumImage, JoinType, PartitionBoundInfoData, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT,
    JOIN_RIGHT, JOIN_RIGHT_ANTI, JOIN_SEMI,
};

use crate::{KIND_VALUE, PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST, PARTITION_STRATEGY_RANGE};

#[inline]
fn is_outer_join(jointype: JoinType) -> bool {
    matches!(
        jointype,
        JOIN_LEFT | JOIN_FULL | JOIN_RIGHT | JOIN_ANTI | JOIN_RIGHT_ANTI
    )
}

pub fn partition_bounds_equal(
    partnatts: i32,
    b1: &PartitionBoundInfoData<'_>,
    b2: &PartitionBoundInfoData<'_>,
) -> bool {
    if b1.strategy != b2.strategy
        || b1.ndatums != b2.ndatums
        || b1.nindexes != b2.nindexes
        || b1.null_index != b2.null_index
        || b1.default_index != b2.default_index
    {
        return false;
    }
    for i in 0..b1.nindexes as usize {
        if b1.indexes[i] != b2.indexes[i] {
            return false;
        }
    }
    if b1.strategy as u8 == PARTITION_STRATEGY_HASH {
        for i in 0..b1.ndatums as usize {
            debug_assert!(b1.datums[i] == b2.datums[i]);
        }
        return true;
    }
    if b1.kind.is_some() != b2.kind.is_some() {
        return false;
    }
    for i in 0..b1.ndatums as usize {
        for j in 0..partnatts as usize {
            if let (Some(k1), Some(k2)) = (&b1.kind, &b2.kind) {
                if k1[i][j] != k2[i][j] {
                    return false;
                }
                if k1[i][j] != KIND_VALUE {
                    continue;
                }
            }
            // DatumImage equality is datumIsEqual: raw image compare, never
            // the partitioning operator.
            if b1.datums[i][j] != b2.datums[i][j] {
                return false;
            }
        }
    }
    true
}

pub struct MergeRel<'a, 'm> {
    pub nparts: i32,
    pub boundinfo: &'a PartitionBoundInfoData<'m>,
    pub is_dummy: &'a [bool],
}

impl MergeRel<'_, '_> {
    fn dummy(&self, part_index: i32) -> bool {
        debug_assert!(part_index >= 0);
        self.is_dummy[part_index as usize]
    }
}

pub struct PartitionBoundsMergeResult<'m> {
    pub merged_bounds: PartitionBoundInfoData<'m>,
    // Per merged partition: input partition index on each side, -1 = dummy.
    pub outer_parts: PgVec<'m, i32>,
    pub inner_parts: PgVec<'m, i32>,
}

pub fn partition_bounds_merge<'m, C>(
    mcx: Mcx<'m>,
    partnatts: i32,
    cmp: &mut C,
    outer: &MergeRel<'_, 'm>,
    inner: &MergeRel<'_, 'm>,
    jointype: JoinType,
) -> PgResult<Option<PartitionBoundsMergeResult<'m>>>
where
    C: FnMut(usize, &DatumImage<'m>, &DatumImage<'m>) -> PgResult<i32>,
{
    assert!(
        matches!(
            jointype,
            JOIN_INNER | JOIN_LEFT | JOIN_FULL | JOIN_SEMI | JOIN_ANTI
        ),
        "partition_bounds_merge: unsupported join type {jointype}"
    );
    assert!(outer.boundinfo.strategy == inner.boundinfo.strategy);
    assert!(outer.is_dummy.len() == outer.nparts as usize);
    assert!(inner.is_dummy.len() == inner.nparts as usize);

    match outer.boundinfo.strategy as u8 {
        // C merges only list/range; equal hash bounds are handled by
        // partition_bounds_equal upstream.
        PARTITION_STRATEGY_HASH => Ok(None),
        PARTITION_STRATEGY_LIST => merge_list_bounds(mcx, cmp, outer, inner, jointype),
        PARTITION_STRATEGY_RANGE => {
            merge_range_bounds(mcx, partnatts, cmp, outer, inner, jointype)
        }
        other => panic!("partition_bounds_merge: unexpected strategy {}", other as char),
    }
}

struct PartitionMap<'m> {
    nparts: i32,
    merged_indexes: PgVec<'m, i32>,
    merged: PgVec<'m, bool>,
    did_remapping: bool,
    old_indexes: PgVec<'m, i32>,
}

fn init_partition_map<'m>(mcx: Mcx<'m>, nparts: i32) -> PgResult<PartitionMap<'m>> {
    let n = nparts as usize;
    let mut merged_indexes: PgVec<'m, i32> = mcx::vec_with_capacity_in(mcx, n)?;
    merged_indexes.resize(n, -1);
    let mut merged: PgVec<'m, bool> = mcx::vec_with_capacity_in(mcx, n)?;
    merged.resize(n, false);
    let mut old_indexes: PgVec<'m, i32> = mcx::vec_with_capacity_in(mcx, n)?;
    old_indexes.resize(n, -1);
    Ok(PartitionMap { nparts, merged_indexes, merged, did_remapping: false, old_indexes })
}

fn clone_row<'m>(mcx: Mcx<'m>, row: &[DatumImage<'m>]) -> PgVec<'m, DatumImage<'m>> {
    let mut v: PgVec<'m, DatumImage<'m>> = PgVec::new_in(mcx);
    v.reserve(row.len());
    for d in row {
        v.push(d.clone());
    }
    v
}

fn clone_kinds<'m>(mcx: Mcx<'m>, kinds: &[i8]) -> PgResult<PgVec<'m, i8>> {
    let mut v: PgVec<'m, i8> = mcx::vec_with_capacity_in(mcx, kinds.len())?;
    v.extend_from_slice(kinds);
    Ok(v)
}

fn merge_list_bounds<'m, C>(
    mcx: Mcx<'m>,
    cmp: &mut C,
    outer: &MergeRel<'_, 'm>,
    inner: &MergeRel<'_, 'm>,
    jointype: JoinType,
) -> PgResult<Option<PartitionBoundsMergeResult<'m>>>
where
    C: FnMut(usize, &DatumImage<'m>, &DatumImage<'m>) -> PgResult<i32>,
{
    let outer_bi = outer.boundinfo;
    let inner_bi = inner.boundinfo;
    let mut outer_has_default = outer_bi.default_index != -1;
    let mut inner_has_default = inner_bi.default_index != -1;
    let outer_default = outer_bi.default_index;
    let inner_default = inner_bi.default_index;
    let mut outer_has_null = outer_bi.null_index != -1;
    let mut inner_has_null = inner_bi.null_index != -1;

    assert!(outer_bi.strategy as u8 == PARTITION_STRATEGY_LIST);
    assert!(outer_bi.kind.is_none() && inner_bi.kind.is_none());

    let mut outer_map = init_partition_map(mcx, outer.nparts)?;
    let mut inner_map = init_partition_map(mcx, inner.nparts)?;

    let mut next_index: i32 = 0;
    let mut null_index: i32 = -1;
    let mut default_index: i32 = -1;
    let mut merged_datums: PgVec<'m, PgVec<'m, DatumImage<'m>>> = PgVec::new_in(mcx);
    let mut merged_indexes: PgVec<'m, i32> = PgVec::new_in(mcx);

    if outer_has_default && outer.dummy(outer_default) {
        outer_has_default = false;
    }
    if inner_has_default && inner.dummy(inner_default) {
        inner_has_default = false;
    }

    let mut outer_pos: i32 = 0;
    let mut inner_pos: i32 = 0;
    while outer_pos < outer_bi.ndatums || inner_pos < inner_bi.ndatums {
        let mut outer_index: i32 = -1;
        let mut inner_index: i32 = -1;
        let mut merged_datum: Option<&PgVec<'m, DatumImage<'m>>> = None;
        let mut merged_index: i32 = -1;

        if outer_pos < outer_bi.ndatums {
            outer_index = outer_bi.indexes[outer_pos as usize];
            if outer.dummy(outer_index) {
                outer_pos += 1;
                continue;
            }
        }
        if inner_pos < inner_bi.ndatums {
            inner_index = inner_bi.indexes[inner_pos as usize];
            if inner.dummy(inner_index) {
                inner_pos += 1;
                continue;
            }
        }

        let outer_datums = if outer_pos < outer_bi.ndatums {
            Some(&outer_bi.datums[outer_pos as usize])
        } else {
            None
        };
        let inner_datums = if inner_pos < inner_bi.ndatums {
            Some(&inner_bi.datums[inner_pos as usize])
        } else {
            None
        };

        // A finished side compares as if it held one extra highest value.
        let cmpval: i32 = if outer_pos >= outer_bi.ndatums {
            1
        } else if inner_pos >= inner_bi.ndatums {
            -1
        } else {
            cmp(0, &outer_datums.unwrap()[0], &inner_datums.unwrap()[0])?
        };

        if cmpval == 0 {
            debug_assert!(outer_index >= 0 && inner_index >= 0);
            merged_index = merge_matching_partitions(
                &mut outer_map,
                &mut inner_map,
                outer_index,
                inner_index,
                &mut next_index,
            );
            if merged_index == -1 {
                return Ok(None);
            }
            merged_datum = outer_datums;
            outer_pos += 1;
            inner_pos += 1;
        } else if cmpval < 0 {
            debug_assert!(outer_pos < outer_bi.ndatums);
            if inner_has_default || is_outer_join(jointype) {
                outer_index = outer_bi.indexes[outer_pos as usize];
                debug_assert!(outer_index >= 0);
                merged_index = process_outer_partition(
                    &mut outer_map,
                    &mut inner_map,
                    outer_has_default,
                    inner_has_default,
                    outer_index,
                    inner_default,
                    jointype,
                    &mut next_index,
                    &mut default_index,
                );
                if merged_index == -1 {
                    return Ok(None);
                }
                merged_datum = outer_datums;
            }
            outer_pos += 1;
        } else {
            debug_assert!(inner_pos < inner_bi.ndatums);
            if outer_has_default || jointype == JOIN_FULL {
                inner_index = inner_bi.indexes[inner_pos as usize];
                debug_assert!(inner_index >= 0);
                merged_index = process_inner_partition(
                    &mut outer_map,
                    &mut inner_map,
                    outer_has_default,
                    inner_has_default,
                    inner_index,
                    outer_default,
                    jointype,
                    &mut next_index,
                    &mut default_index,
                );
                if merged_index == -1 {
                    return Ok(None);
                }
                merged_datum = inner_datums;
            }
            inner_pos += 1;
        }

        if merged_index >= 0 && merged_index != default_index {
            merged_datums.push(clone_row(mcx, merged_datum.unwrap()));
            merged_indexes.push(merged_index);
        }
    }

    if outer_has_null && outer.dummy(outer_bi.null_index) {
        outer_has_null = false;
    }
    if inner_has_null && inner.dummy(inner_bi.null_index) {
        inner_has_null = false;
    }

    if outer_has_null || inner_has_null {
        merge_null_partitions(
            &mut outer_map,
            &mut inner_map,
            outer_has_null,
            inner_has_null,
            outer_bi.null_index,
            inner_bi.null_index,
            jointype,
            &mut next_index,
            &mut null_index,
        );
    } else {
        debug_assert!(null_index == -1);
    }

    if outer_has_default || inner_has_default {
        merge_default_partitions(
            &mut outer_map,
            &mut inner_map,
            outer_has_default,
            inner_has_default,
            outer_default,
            inner_default,
            jointype,
            &mut next_index,
            &mut default_index,
        );
    } else {
        debug_assert!(default_index == -1);
    }

    if next_index > 0 {
        if outer_map.did_remapping || inner_map.did_remapping {
            debug_assert!(jointype == JOIN_FULL);
            fix_merged_indexes(&outer_map, &inner_map, next_index, &mut merged_indexes);
        }
        let (outer_parts, inner_parts) =
            generate_matching_part_pairs(mcx, &outer_map, &inner_map, next_index)?;
        debug_assert!(!outer_parts.is_empty());
        debug_assert!(outer_parts.len() == inner_parts.len());
        debug_assert!(outer_parts.len() as i32 <= next_index);
        let merged_bounds = build_merged_partition_bounds(
            mcx,
            outer_bi.strategy,
            merged_datums,
            None,
            merged_indexes,
            null_index,
            default_index,
        );
        return Ok(Some(PartitionBoundsMergeResult { merged_bounds, outer_parts, inner_parts }));
    }
    Ok(None)
}

#[derive(Clone, Copy)]
struct RangeBound<'a, 'm> {
    index: i32,
    datums: &'a [DatumImage<'m>],
    kind: &'a [i8],
    lower: bool,
}

impl RangeBound<'_, '_> {
    fn empty(lower: bool) -> Self {
        RangeBound { index: -1, datums: &[], kind: &[], lower }
    }
}

fn merge_range_bounds<'m, C>(
    mcx: Mcx<'m>,
    partnatts: i32,
    cmp: &mut C,
    outer: &MergeRel<'_, 'm>,
    inner: &MergeRel<'_, 'm>,
    jointype: JoinType,
) -> PgResult<Option<PartitionBoundsMergeResult<'m>>>
where
    C: FnMut(usize, &DatumImage<'m>, &DatumImage<'m>) -> PgResult<i32>,
{
    let outer_bi = outer.boundinfo;
    let inner_bi = inner.boundinfo;
    let mut outer_has_default = outer_bi.default_index != -1;
    let mut inner_has_default = inner_bi.default_index != -1;
    let outer_default = outer_bi.default_index;
    let inner_default = inner_bi.default_index;

    assert!(outer_bi.strategy as u8 == PARTITION_STRATEGY_RANGE);

    let mut outer_map = init_partition_map(mcx, outer.nparts)?;
    let mut inner_map = init_partition_map(mcx, inner.nparts)?;

    let mut next_index: i32 = 0;
    let mut default_index: i32 = -1;
    let mut merged_datums: PgVec<'m, PgVec<'m, DatumImage<'m>>> = PgVec::new_in(mcx);
    let mut merged_kinds: PgVec<'m, PgVec<'m, i8>> = PgVec::new_in(mcx);
    let mut merged_indexes: PgVec<'m, i32> = PgVec::new_in(mcx);

    if outer_has_default && outer.dummy(outer_default) {
        outer_has_default = false;
    }
    if inner_has_default && inner.dummy(inner_default) {
        inner_has_default = false;
    }

    let mut outer_lb_pos: i32 = 0;
    let mut inner_lb_pos: i32 = 0;
    let mut outer_lb = RangeBound::empty(true);
    let mut outer_ub = RangeBound::empty(false);
    let mut inner_lb = RangeBound::empty(true);
    let mut inner_ub = RangeBound::empty(false);

    let mut outer_index =
        get_range_partition(outer, &mut outer_lb_pos, &mut outer_lb, &mut outer_ub);
    let mut inner_index =
        get_range_partition(inner, &mut inner_lb_pos, &mut inner_lb, &mut inner_ub);

    while outer_index >= 0 || inner_index >= 0 {
        let overlap;
        let lb_cmpval;
        let ub_cmpval;
        let mut merged_lb = RangeBound::empty(true);
        let mut merged_ub = RangeBound::empty(false);
        let mut merged_index: i32 = -1;

        // A finished side compares as if it held one extra highest range.
        if outer_index == -1 {
            overlap = false;
            lb_cmpval = 1;
            ub_cmpval = 1;
        } else if inner_index == -1 {
            overlap = false;
            lb_cmpval = -1;
            ub_cmpval = -1;
        } else {
            let (ov, lbc, ubc) = compare_range_partitions(
                cmp, partnatts, &outer_lb, &outer_ub, &inner_lb, &inner_ub,
            )?;
            overlap = ov;
            lb_cmpval = lbc;
            ub_cmpval = ubc;
        }

        if overlap {
            debug_assert!(outer_index >= 0 && inner_index >= 0);
            debug_assert!(
                outer_map.merged_indexes[outer_index as usize] == -1
                    && !outer_map.merged[outer_index as usize]
            );
            debug_assert!(
                inner_map.merged_indexes[inner_index as usize] == -1
                    && !inner_map.merged[inner_index as usize]
            );

            merged_index = merge_matching_partitions(
                &mut outer_map,
                &mut inner_map,
                outer_index,
                inner_index,
                &mut next_index,
            );
            debug_assert!(merged_index >= 0);

            get_merged_range_bounds(
                jointype,
                &outer_lb,
                &outer_ub,
                &inner_lb,
                &inner_ub,
                lb_cmpval,
                ub_cmpval,
                &mut merged_lb,
                &mut merged_ub,
            );

            let save_outer_ub = outer_ub;
            let save_inner_ub = inner_ub;

            outer_index =
                get_range_partition(outer, &mut outer_lb_pos, &mut outer_lb, &mut outer_ub);
            inner_index =
                get_range_partition(inner, &mut inner_lb_pos, &mut inner_lb, &mut inner_ub);

            // A partition overlapping two partitions on the other side is
            // unsupported; likewise a non-overlapping remainder that would
            // pair with the other side's default.
            if ub_cmpval > 0
                && inner_index >= 0
                && compare_range_bounds(cmp, partnatts, &save_outer_ub, &inner_lb)? > 0
            {
                return Ok(None);
            }
            if ub_cmpval < 0
                && outer_index >= 0
                && compare_range_bounds(cmp, partnatts, &outer_lb, &save_inner_ub)? < 0
            {
                return Ok(None);
            }
            if (outer_has_default && (lb_cmpval > 0 || ub_cmpval < 0))
                || (inner_has_default && (lb_cmpval < 0 || ub_cmpval > 0))
            {
                return Ok(None);
            }
        } else if ub_cmpval < 0 {
            debug_assert!(outer_index >= 0);
            debug_assert!(
                outer_map.merged_indexes[outer_index as usize] == -1
                    && !outer_map.merged[outer_index as usize]
            );
            if inner_has_default || is_outer_join(jointype) {
                merged_index = process_outer_partition(
                    &mut outer_map,
                    &mut inner_map,
                    outer_has_default,
                    inner_has_default,
                    outer_index,
                    inner_default,
                    jointype,
                    &mut next_index,
                    &mut default_index,
                );
                if merged_index == -1 {
                    return Ok(None);
                }
                merged_lb = outer_lb;
                merged_ub = outer_ub;
            }
            outer_index =
                get_range_partition(outer, &mut outer_lb_pos, &mut outer_lb, &mut outer_ub);
        } else {
            debug_assert!(ub_cmpval > 0);
            debug_assert!(inner_index >= 0);
            debug_assert!(
                inner_map.merged_indexes[inner_index as usize] == -1
                    && !inner_map.merged[inner_index as usize]
            );
            if outer_has_default || jointype == JOIN_FULL {
                merged_index = process_inner_partition(
                    &mut outer_map,
                    &mut inner_map,
                    outer_has_default,
                    inner_has_default,
                    inner_index,
                    outer_default,
                    jointype,
                    &mut next_index,
                    &mut default_index,
                );
                if merged_index == -1 {
                    return Ok(None);
                }
                merged_lb = inner_lb;
                merged_ub = inner_ub;
            }
            inner_index =
                get_range_partition(inner, &mut inner_lb_pos, &mut inner_lb, &mut inner_ub);
        }

        if merged_index >= 0 && merged_index != default_index {
            add_merged_range_bounds(
                mcx,
                cmp,
                partnatts,
                &merged_lb,
                &merged_ub,
                merged_index,
                &mut merged_datums,
                &mut merged_kinds,
                &mut merged_indexes,
            )?;
        }
    }

    if outer_has_default || inner_has_default {
        merge_default_partitions(
            &mut outer_map,
            &mut inner_map,
            outer_has_default,
            inner_has_default,
            outer_default,
            inner_default,
            jointype,
            &mut next_index,
            &mut default_index,
        );
    } else {
        debug_assert!(default_index == -1);
    }

    if next_index > 0 {
        debug_assert!(!outer_map.did_remapping);
        debug_assert!(!inner_map.did_remapping);
        let (outer_parts, inner_parts) =
            generate_matching_part_pairs(mcx, &outer_map, &inner_map, next_index)?;
        debug_assert!(!outer_parts.is_empty());
        debug_assert!(outer_parts.len() == inner_parts.len());
        debug_assert!(outer_parts.len() as i32 == next_index);
        let merged_bounds = build_merged_partition_bounds(
            mcx,
            outer_bi.strategy,
            merged_datums,
            Some(merged_kinds),
            merged_indexes,
            -1,
            default_index,
        );
        return Ok(Some(PartitionBoundsMergeResult { merged_bounds, outer_parts, inner_parts }));
    }
    Ok(None)
}

fn merge_matching_partitions(
    outer_map: &mut PartitionMap<'_>,
    inner_map: &mut PartitionMap<'_>,
    outer_index: i32,
    inner_index: i32,
    next_index: &mut i32,
) -> i32 {
    debug_assert!(outer_index >= 0 && outer_index < outer_map.nparts);
    debug_assert!(inner_index >= 0 && inner_index < inner_map.nparts);
    let oi = outer_index as usize;
    let ii = inner_index as usize;
    let outer_merged_index = outer_map.merged_indexes[oi];
    let inner_merged_index = inner_map.merged_indexes[ii];
    let outer_merged = outer_map.merged[oi];
    let inner_merged = inner_map.merged[ii];

    if outer_merged_index >= 0 && inner_merged_index >= 0 {
        if outer_merged_index == inner_merged_index {
            debug_assert!(outer_merged && inner_merged);
            return outer_merged_index;
        }
        if !outer_merged && !inner_merged {
            // List-only re-map: keep the smaller merged index so canonical
            // list order still follows each partition's smallest value.
            if outer_merged_index < inner_merged_index {
                outer_map.merged[oi] = true;
                inner_map.merged_indexes[ii] = outer_merged_index;
                inner_map.merged[ii] = true;
                inner_map.did_remapping = true;
                inner_map.old_indexes[ii] = inner_merged_index;
                return outer_merged_index;
            } else {
                inner_map.merged[ii] = true;
                outer_map.merged_indexes[oi] = inner_merged_index;
                outer_map.merged[oi] = true;
                outer_map.did_remapping = true;
                outer_map.old_indexes[oi] = outer_merged_index;
                return inner_merged_index;
            }
        }
        return -1;
    }

    debug_assert!(outer_merged_index == -1 || inner_merged_index == -1);

    if outer_merged_index == -1 && inner_merged_index == -1 {
        let merged_index = *next_index;
        debug_assert!(!outer_merged && !inner_merged);
        outer_map.merged_indexes[oi] = merged_index;
        outer_map.merged[oi] = true;
        inner_map.merged_indexes[ii] = merged_index;
        inner_map.merged[ii] = true;
        *next_index += 1;
        return merged_index;
    }
    if outer_merged_index >= 0 && !outer_map.merged[oi] {
        debug_assert!(inner_merged_index == -1 && !inner_merged);
        inner_map.merged_indexes[ii] = outer_merged_index;
        inner_map.merged[ii] = true;
        outer_map.merged[oi] = true;
        return outer_merged_index;
    }
    if inner_merged_index >= 0 && !inner_map.merged[ii] {
        debug_assert!(outer_merged_index == -1 && !outer_merged);
        outer_map.merged_indexes[oi] = inner_merged_index;
        outer_map.merged[oi] = true;
        inner_map.merged[ii] = true;
        return inner_merged_index;
    }
    -1
}

fn process_outer_partition(
    outer_map: &mut PartitionMap<'_>,
    inner_map: &mut PartitionMap<'_>,
    outer_has_default: bool,
    inner_has_default: bool,
    outer_index: i32,
    inner_default: i32,
    jointype: JoinType,
    next_index: &mut i32,
    default_index: &mut i32,
) -> i32 {
    let mut merged_index;
    debug_assert!(outer_index >= 0);

    if inner_has_default {
        debug_assert!(inner_default >= 0);
        // Defaults on both sides would give the inner default two matches.
        if outer_has_default {
            return -1;
        }
        merged_index =
            merge_matching_partitions(outer_map, inner_map, outer_index, inner_default, next_index);
        if merged_index == -1 {
            return -1;
        }
        if jointype == JOIN_FULL {
            if *default_index == -1 {
                *default_index = merged_index;
            } else {
                debug_assert!(*default_index == merged_index);
            }
        }
    } else {
        debug_assert!(is_outer_join(jointype));
        debug_assert!(jointype != JOIN_RIGHT);
        merged_index = outer_map.merged_indexes[outer_index as usize];
        if merged_index == -1 {
            merged_index = merge_partition_with_dummy(outer_map, outer_index, next_index);
        }
    }
    merged_index
}

fn process_inner_partition(
    outer_map: &mut PartitionMap<'_>,
    inner_map: &mut PartitionMap<'_>,
    outer_has_default: bool,
    inner_has_default: bool,
    inner_index: i32,
    outer_default: i32,
    jointype: JoinType,
    next_index: &mut i32,
    default_index: &mut i32,
) -> i32 {
    let mut merged_index;
    debug_assert!(inner_index >= 0);

    if outer_has_default {
        debug_assert!(outer_default >= 0);
        // Defaults on both sides would give the outer default two matches.
        if inner_has_default {
            return -1;
        }
        merged_index =
            merge_matching_partitions(outer_map, inner_map, outer_default, inner_index, next_index);
        if merged_index == -1 {
            return -1;
        }
        if is_outer_join(jointype) {
            debug_assert!(jointype != JOIN_RIGHT);
            if *default_index == -1 {
                *default_index = merged_index;
            } else {
                debug_assert!(*default_index == merged_index);
            }
        }
    } else {
        debug_assert!(jointype == JOIN_FULL);
        merged_index = inner_map.merged_indexes[inner_index as usize];
        if merged_index == -1 {
            merged_index = merge_partition_with_dummy(inner_map, inner_index, next_index);
        }
    }
    merged_index
}

fn merge_null_partitions(
    outer_map: &mut PartitionMap<'_>,
    inner_map: &mut PartitionMap<'_>,
    outer_has_null: bool,
    inner_has_null: bool,
    outer_null: i32,
    inner_null: i32,
    jointype: JoinType,
    next_index: &mut i32,
    null_index: &mut i32,
) {
    let mut consider_outer_null = false;
    let mut consider_inner_null = false;

    debug_assert!(outer_has_null || inner_has_null);
    debug_assert!(*null_index == -1);

    if outer_has_null {
        debug_assert!(outer_null >= 0 && outer_null < outer_map.nparts);
        if outer_map.merged_indexes[outer_null as usize] == -1 {
            consider_outer_null = true;
        }
    }
    if inner_has_null {
        debug_assert!(inner_null >= 0 && inner_null < inner_map.nparts);
        if inner_map.merged_indexes[inner_null as usize] == -1 {
            consider_inner_null = true;
        }
    }

    if !consider_outer_null && !consider_inner_null {
        return;
    }

    if consider_outer_null && !consider_inner_null {
        if is_outer_join(jointype) {
            debug_assert!(jointype != JOIN_RIGHT);
            *null_index = merge_partition_with_dummy(outer_map, outer_null, next_index);
        }
    } else if !consider_outer_null && consider_inner_null {
        if jointype == JOIN_FULL {
            *null_index = merge_partition_with_dummy(inner_map, inner_null, next_index);
        }
    } else {
        // INNER/SEMI never join two NULL keys (strict clauses), so both NULL
        // partitions are simply eliminated there.
        if is_outer_join(jointype) {
            debug_assert!(jointype != JOIN_RIGHT);
            *null_index =
                merge_matching_partitions(outer_map, inner_map, outer_null, inner_null, next_index);
            debug_assert!(*null_index >= 0);
        }
    }
}

fn merge_default_partitions(
    outer_map: &mut PartitionMap<'_>,
    inner_map: &mut PartitionMap<'_>,
    outer_has_default: bool,
    inner_has_default: bool,
    outer_default: i32,
    inner_default: i32,
    jointype: JoinType,
    next_index: &mut i32,
    default_index: &mut i32,
) {
    let mut outer_merged_index = -1;
    let mut inner_merged_index = -1;

    debug_assert!(outer_has_default || inner_has_default);

    if outer_has_default {
        debug_assert!(outer_default >= 0 && outer_default < outer_map.nparts);
        outer_merged_index = outer_map.merged_indexes[outer_default as usize];
    }
    if inner_has_default {
        debug_assert!(inner_default >= 0 && inner_default < inner_map.nparts);
        inner_merged_index = inner_map.merged_indexes[inner_default as usize];
    }

    if outer_has_default && !inner_has_default {
        if is_outer_join(jointype) {
            debug_assert!(jointype != JOIN_RIGHT);
            if outer_merged_index == -1 {
                debug_assert!(*default_index == -1);
                *default_index = merge_partition_with_dummy(outer_map, outer_default, next_index);
            } else {
                debug_assert!(*default_index == outer_merged_index);
            }
        } else {
            debug_assert!(*default_index == -1);
        }
    } else if !outer_has_default && inner_has_default {
        if jointype == JOIN_FULL {
            if inner_merged_index == -1 {
                debug_assert!(*default_index == -1);
                *default_index = merge_partition_with_dummy(inner_map, inner_default, next_index);
            } else {
                debug_assert!(*default_index == inner_merged_index);
            }
        } else {
            debug_assert!(*default_index == -1);
        }
    } else {
        debug_assert!(outer_merged_index == -1 && inner_merged_index == -1);
        debug_assert!(*default_index == -1);
        *default_index =
            merge_matching_partitions(outer_map, inner_map, outer_default, inner_default, next_index);
        debug_assert!(*default_index >= 0);
    }
}

fn merge_partition_with_dummy(map: &mut PartitionMap<'_>, index: i32, next_index: &mut i32) -> i32 {
    let merged_index = *next_index;
    debug_assert!(index >= 0 && index < map.nparts);
    debug_assert!(map.merged_indexes[index as usize] == -1);
    debug_assert!(!map.merged[index as usize]);
    map.merged_indexes[index as usize] = merged_index;
    // The merged flag stays false: a later real match may re-merge this slot.
    *next_index += 1;
    merged_index
}

fn fix_merged_indexes(
    outer_map: &PartitionMap<'_>,
    inner_map: &PartitionMap<'_>,
    nmerged: i32,
    merged_indexes: &mut [i32],
) {
    debug_assert!(nmerged > 0);
    let mut new_indexes = alloc_scratch_i32(nmerged as usize);

    if outer_map.did_remapping {
        for i in 0..outer_map.nparts as usize {
            let merged_index = outer_map.old_indexes[i];
            if merged_index >= 0 {
                new_indexes[merged_index as usize] = outer_map.merged_indexes[i];
            }
        }
    }
    if inner_map.did_remapping {
        for i in 0..inner_map.nparts as usize {
            let merged_index = inner_map.old_indexes[i];
            if merged_index >= 0 {
                new_indexes[merged_index as usize] = inner_map.merged_indexes[i];
            }
        }
    }

    for slot in merged_indexes.iter_mut() {
        debug_assert!(*slot >= 0);
        if new_indexes[*slot as usize] >= 0 {
            *slot = new_indexes[*slot as usize];
        }
    }
}

// std Vec justified: pure -1-filled scratch local to one cold planner call.
fn alloc_scratch_i32(n: usize) -> Vec<i32> {
    vec![-1; n]
}

fn generate_matching_part_pairs<'m>(
    mcx: Mcx<'m>,
    outer_map: &PartitionMap<'_>,
    inner_map: &PartitionMap<'_>,
    nmerged: i32,
) -> PgResult<(PgVec<'m, i32>, PgVec<'m, i32>)> {
    debug_assert!(nmerged > 0);
    let mut outer_indexes = alloc_scratch_i32(nmerged as usize);
    let mut inner_indexes = alloc_scratch_i32(nmerged as usize);

    let max_nparts = outer_map.nparts.max(inner_map.nparts);
    for i in 0..max_nparts {
        if i < outer_map.nparts {
            let merged_index = outer_map.merged_indexes[i as usize];
            if merged_index >= 0 {
                debug_assert!(merged_index < nmerged);
                outer_indexes[merged_index as usize] = i;
            }
        }
        if i < inner_map.nparts {
            let merged_index = inner_map.merged_indexes[i as usize];
            if merged_index >= 0 {
                debug_assert!(merged_index < nmerged);
                inner_indexes[merged_index as usize] = i;
            }
        }
    }

    let mut outer_parts: PgVec<'m, i32> = mcx::vec_with_capacity_in(mcx, nmerged as usize)?;
    let mut inner_parts: PgVec<'m, i32> = mcx::vec_with_capacity_in(mcx, nmerged as usize)?;
    for i in 0..nmerged as usize {
        // Both dummy: the merged slot was vacated by list re-merging; skip.
        if outer_indexes[i] == -1 && inner_indexes[i] == -1 {
            continue;
        }
        outer_parts.push(outer_indexes[i]);
        inner_parts.push(inner_indexes[i]);
    }
    Ok((outer_parts, inner_parts))
}

fn build_merged_partition_bounds<'m>(
    mcx: Mcx<'m>,
    strategy: i8,
    merged_datums: PgVec<'m, PgVec<'m, DatumImage<'m>>>,
    merged_kinds: Option<PgVec<'m, PgVec<'m, i8>>>,
    mut merged_indexes: PgVec<'m, i32>,
    null_index: i32,
    default_index: i32,
) -> PartitionBoundInfoData<'m> {
    let ndatums = merged_datums.len() as i32;
    let mut nindexes = ndatums;

    let kind = if strategy as u8 == PARTITION_STRATEGY_RANGE {
        let mk = merged_kinds.unwrap();
        debug_assert!(mk.len() as i32 == ndatums);
        // Range partitioning carries ndatums+1 indexes.
        merged_indexes.push(-1);
        nindexes += 1;
        Some(mk)
    } else {
        debug_assert!(strategy as u8 == PARTITION_STRATEGY_LIST);
        debug_assert!(merged_kinds.is_none());
        None
    };
    debug_assert!(merged_indexes.len() as i32 == nindexes);

    let mut out = PartitionBoundInfoData::new(mcx);
    out.strategy = strategy;
    out.ndatums = ndatums;
    out.nindexes = nindexes;
    out.null_index = null_index;
    out.default_index = default_index;
    out.indexes = merged_indexes;
    out.datums = merged_datums;
    out.kind = kind;
    out
}

fn get_range_partition<'a, 'm>(
    view: &MergeRel<'a, 'm>,
    lb_pos: &mut i32,
    lb: &mut RangeBound<'a, 'm>,
    ub: &mut RangeBound<'a, 'm>,
) -> i32 {
    loop {
        let part_index = get_range_partition_internal(view.boundinfo, lb_pos, lb, ub);
        if part_index == -1 {
            return -1;
        }
        if !view.dummy(part_index) {
            return part_index;
        }
    }
}

fn get_range_partition_internal<'a, 'm>(
    bi: &'a PartitionBoundInfoData<'m>,
    lb_pos: &mut i32,
    lb: &mut RangeBound<'a, 'm>,
    ub: &mut RangeBound<'a, 'm>,
) -> i32 {
    if *lb_pos >= bi.ndatums {
        return -1;
    }
    debug_assert!(*lb_pos + 1 < bi.ndatums);

    let kind = bi.kind.as_ref().expect("merge_range_bounds: boundinfo.kind missing");
    let p = *lb_pos as usize;

    lb.index = bi.indexes[p];
    lb.datums = &bi.datums[p];
    lb.kind = &kind[p];
    lb.lower = true;
    ub.index = bi.indexes[p + 1];
    ub.datums = &bi.datums[p + 1];
    ub.kind = &kind[p + 1];
    ub.lower = false;

    debug_assert!(ub.index >= 0);

    // Next lower bound: the slot after the upper bound if it carries no
    // partition, else the upper bound doubles as the next lower bound.
    if *lb_pos + 2 >= bi.ndatums {
        *lb_pos = bi.ndatums;
    } else if bi.indexes[(*lb_pos + 2) as usize] < 0 {
        *lb_pos += 2;
    } else {
        *lb_pos += 1;
    }

    ub.index
}

fn compare_range_partitions<'m, C>(
    cmp: &mut C,
    partnatts: i32,
    outer_lb: &RangeBound<'_, 'm>,
    outer_ub: &RangeBound<'_, 'm>,
    inner_lb: &RangeBound<'_, 'm>,
    inner_ub: &RangeBound<'_, 'm>,
) -> PgResult<(bool, i32, i32)>
where
    C: FnMut(usize, &DatumImage<'m>, &DatumImage<'m>) -> PgResult<i32>,
{
    if compare_range_bounds(cmp, partnatts, outer_ub, inner_lb)? < 0 {
        return Ok((false, -1, -1));
    }
    if compare_range_bounds(cmp, partnatts, outer_lb, inner_ub)? > 0 {
        return Ok((false, 1, 1));
    }
    let lb_cmpval = compare_range_bounds(cmp, partnatts, outer_lb, inner_lb)?;
    let ub_cmpval = compare_range_bounds(cmp, partnatts, outer_ub, inner_ub)?;
    Ok((true, lb_cmpval, ub_cmpval))
}

fn get_merged_range_bounds<'a, 'm>(
    jointype: JoinType,
    outer_lb: &RangeBound<'a, 'm>,
    outer_ub: &RangeBound<'a, 'm>,
    inner_lb: &RangeBound<'a, 'm>,
    inner_ub: &RangeBound<'a, 'm>,
    lb_cmpval: i32,
    ub_cmpval: i32,
    merged_lb: &mut RangeBound<'a, 'm>,
    merged_ub: &mut RangeBound<'a, 'm>,
) {
    match jointype {
        JOIN_INNER | JOIN_SEMI => {
            *merged_lb = if lb_cmpval > 0 { *outer_lb } else { *inner_lb };
            *merged_ub = if ub_cmpval < 0 { *outer_ub } else { *inner_ub };
        }
        JOIN_LEFT | JOIN_ANTI => {
            *merged_lb = *outer_lb;
            *merged_ub = *outer_ub;
        }
        JOIN_FULL => {
            *merged_lb = if lb_cmpval < 0 { *outer_lb } else { *inner_lb };
            *merged_ub = if ub_cmpval > 0 { *outer_ub } else { *inner_ub };
        }
        other => panic!("unrecognized join type: {other}"),
    }
}

fn add_merged_range_bounds<'m, C>(
    mcx: Mcx<'m>,
    cmp: &mut C,
    partnatts: i32,
    merged_lb: &RangeBound<'_, 'm>,
    merged_ub: &RangeBound<'_, 'm>,
    merged_index: i32,
    merged_datums: &mut PgVec<'m, PgVec<'m, DatumImage<'m>>>,
    merged_kinds: &mut PgVec<'m, PgVec<'m, i8>>,
    merged_indexes: &mut PgVec<'m, i32>,
) -> PgResult<()>
where
    C: FnMut(usize, &DatumImage<'m>, &DatumImage<'m>) -> PgResult<i32>,
{
    let cmpval: i32 = if merged_datums.is_empty() {
        debug_assert!(merged_kinds.is_empty() && merged_indexes.is_empty());
        1
    } else {
        // lower1 = false so an equal previous upper bound compares equal, not
        // smaller, letting it be reused as this partition's lower bound.
        let c = partition_rbound_cmp(
            cmp,
            partnatts,
            merged_lb.datums,
            merged_lb.kind,
            false,
            merged_datums.last().unwrap(),
            merged_kinds.last().unwrap(),
            false,
        )?;
        debug_assert!(c >= 0);
        c
    };

    if cmpval > 0 {
        merged_datums.push(clone_row(mcx, merged_lb.datums));
        merged_kinds.push(clone_kinds(mcx, merged_lb.kind)?);
        merged_indexes.push(-1);
    }
    merged_datums.push(clone_row(mcx, merged_ub.datums));
    merged_kinds.push(clone_kinds(mcx, merged_ub.kind)?);
    merged_indexes.push(merged_index);
    Ok(())
}

fn partition_rbound_cmp<'m, C>(
    cmp: &mut C,
    partnatts: i32,
    datums1: &[DatumImage<'m>],
    kind1: &[i8],
    lower1: bool,
    datums2: &[DatumImage<'m>],
    kind2: &[i8],
    lower2: bool,
) -> PgResult<i32>
where
    C: FnMut(usize, &DatumImage<'m>, &DatumImage<'m>) -> PgResult<i32>,
{
    let mut colnum: i32 = 0;
    let mut cmpval: i32 = 0;
    for i in 0..partnatts as usize {
        colnum += 1;
        if kind1[i] < kind2[i] {
            return Ok(-colnum);
        } else if kind1[i] > kind2[i] {
            return Ok(colnum);
        } else if kind1[i] != KIND_VALUE {
            break;
        }
        cmpval = cmp(i, &datums1[i], &datums2[i])?;
        if cmpval != 0 {
            break;
        }
    }
    if cmpval == 0 && lower1 != lower2 {
        cmpval = if lower1 { 1 } else { -1 };
    }
    Ok(if cmpval == 0 {
        0
    } else if cmpval < 0 {
        -colnum
    } else {
        colnum
    })
}

fn compare_range_bounds<'m, C>(
    cmp: &mut C,
    partnatts: i32,
    b1: &RangeBound<'_, 'm>,
    b2: &RangeBound<'_, 'm>,
) -> PgResult<i32>
where
    C: FnMut(usize, &DatumImage<'m>, &DatumImage<'m>) -> PgResult<i32>,
{
    let c = partition_rbound_cmp(
        cmp, partnatts, b1.datums, b1.kind, b1.lower, b2.datums, b2.kind, b2.lower,
    )?;
    Ok(c.signum())
}
