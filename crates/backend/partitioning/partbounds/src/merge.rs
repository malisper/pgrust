//! Faithful 1:1 port of the partitionwise-join partition-bound *merge* cluster
//! of PostgreSQL 18.3 `partbounds.c` (the `partition_bounds_merge` family,
//! ~600 lines): the RANGE / LIST per-strategy merges plus their helpers
//! (`merge_list_bounds` / `merge_range_bounds` / `init_partition_map` /
//! `merge_matching_partitions` / `process_outer_partition` /
//! `process_inner_partition` / `merge_null_partitions` /
//! `merge_default_partitions` / `merge_partition_with_dummy` /
//! `fix_merged_indexes` / `generate_matching_part_pairs` /
//! `build_merged_partition_bounds` / `get_range_partition` /
//! `compare_range_partitions` / `get_merged_range_bounds` /
//! `add_merged_range_bounds`).
//!
//! This is the partitionwise-join leg, distinct from the tuple-routing leg in
//! `lib.rs`. It operates entirely on the planner-side value model: the input
//! bounds come from `RelOptInfo::boundinfo` (the flat
//! `pathnodes::PartitionBoundInfoData` with `DatumImage` datums), the
//! per-key comparison support functions/collations come from the join rel's
//! shared `PartitionScheme`, and the partition-to-partition pairings are
//! returned as `RelId`s instead of `RelOptInfo *`.
//!
//! The C function `partition_bounds_merge(partnatts, partsupfunc, partcollation,
//! outer_rel, inner_rel, jointype, &outer_parts, &inner_parts)` is reshaped to
//! `(root, rel1, rel2, jointype) -> Option<(merged_boundinfo, outer_parts,
//! inner_parts)>` to match `compute_partition_bounds` (joinrels.c:1739).

use types_error::{PgError, PgResult};
use pathnodes::{DatumImage, JoinType, PartitionBoundInfoData, PlannerInfo, RelId};
use pathnodes::{JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_SEMI};

use types_core::fmgr::FmgrInfo;
use types_core::primitive::Oid;
use types_tuple::heaptuple::Datum;

use partbounds_seams as seams;

/// `PARTITION_STRATEGY_LIST = 'l'` / `PARTITION_STRATEGY_RANGE = 'r'`
/// (`catalog/partition.h`) as carried in the `i8` strategy field.
const PARTITION_STRATEGY_LIST: i8 = b'l' as i8;
const PARTITION_STRATEGY_RANGE: i8 = b'r' as i8;
const PARTITION_STRATEGY_HASH: i8 = b'h' as i8;

/// `PARTITION_RANGE_DATUM_VALUE` discriminant as an `i8` in
/// `PartitionBoundInfoData::kind`. (`MINVALUE = -1`, `VALUE = 0`, `MAXVALUE =
/// 1` in the C enum.)
const PARTITION_RANGE_DATUM_VALUE: i8 = 0;

/// `IS_OUTER_JOIN(jointype)` (nodes.h): true for LEFT, FULL, RIGHT, ANTI,
/// RIGHT_ANTI. Mirrors `(1 << jointype) & ((1 << JOIN_LEFT) | (1 << JOIN_FULL)
/// | (1 << JOIN_RIGHT) | (1 << JOIN_ANTI) | (1 << JOIN_RIGHT_ANTI))`. In the
/// merge cluster the relevant outer types are LEFT/FULL/ANTI.
#[inline]
fn is_outer_join(jointype: JoinType) -> bool {
    matches!(
        jointype,
        JOIN_LEFT | JOIN_FULL | JOIN_RIGHT | JOIN_ANTI | 7 /* JOIN_RIGHT_ANTI */
    )
}

/// `DatumGetInt32(FunctionCall2Coll(&finfo, collation, a1, a2))` over the
/// planner-layer [`DatumImage`] carrier. Reconstructs live `Datum`s in a
/// throwaway call context (mirroring `call_cmp` in `lib.rs`), then dispatches
/// the partition comparison support function by its stamped OID.
fn cmp_images(finfo: &FmgrInfo, collation: Oid, a: &DatumImage, b: &DatumImage) -> PgResult<i32> {
    use mcx::MemoryContext;
    let ctx = MemoryContext::new("partition_merge_cmp");
    let da = image_to_datum(ctx.mcx(), a)?;
    let db = image_to_datum(ctx.mcx(), b)?;
    let result = crate::call_cmp_oid(finfo.fn_oid, collation, ctx.mcx(), da, db)?;
    Ok(result)
}

/// Rebuild a live `Datum` from a planner-layer [`DatumImage`] inside `mcx`.
fn image_to_datum<'mcx>(mcx: mcx::Mcx<'mcx>, img: &DatumImage) -> PgResult<Datum<'mcx>> {
    match img {
        DatumImage::ByVal(w) => Ok(Datum::from_usize(*w)),
        DatumImage::Bytes(bytes) => Datum::from_byref_bytes_in(mcx, bytes),
    }
}

/* ==========================================================================
 * PartitionMap / PartitionRangeBound — the C scratch structs
 * ======================================================================== */

/// `PartitionMap` (partbounds.c:76) — mapping from a joining relation's
/// partitions to merged partitions.
struct PartitionMap {
    nparts: i32,
    merged_indexes: Vec<i32>,
    merged: Vec<bool>,
    did_remapping: bool,
    old_indexes: Vec<i32>,
}

/// `PartitionRangeBound` (partbounds.c:64) — one bound of a range partition, in
/// the planner-layer value model. `datums`/`kind` reference rows of the input
/// `PartitionBoundInfoData`; we hold owned clones so the merge can shuffle them.
#[derive(Clone)]
struct RangeBound {
    index: i32,
    datums: Vec<DatumImage>,
    kind: Vec<i8>,
    lower: bool,
}

impl RangeBound {
    /// `{-1, NULL, NULL, lower}` initialiser used for the per-iteration
    /// merged_lb / merged_ub scratch bounds.
    fn empty(lower: bool) -> Self {
        RangeBound {
            index: -1,
            datums: Vec::new(),
            kind: Vec::new(),
            lower,
        }
    }
}

/* ==========================================================================
 * input view — RelOptInfo fields the merge reads
 * ======================================================================== */

/// A read-only snapshot of one input rel's partitioning data, cloned out of
/// `RelOptInfo` so the merge algorithm can run without re-borrowing `root`.
struct RelView {
    nparts: i32,
    boundinfo: PartitionBoundInfoData,
    part_rels: Vec<Option<RelId>>,
}

impl RelView {
    fn snapshot(root: &PlannerInfo, rel: RelId) -> PgResult<RelView> {
        let r = root.rel(rel);
        let boundinfo = r
            .boundinfo
            .as_deref()
            .cloned()
            .ok_or_else(|| PgError::error("partition_bounds_merge: input rel has no boundinfo"))?;
        Ok(RelView {
            nparts: r.nparts,
            boundinfo,
            part_rels: r.part_rels.clone(),
        })
    }
}

/// `is_dummy_partition(rel, part_index)` (partbounds.c:1842) — the partition is
/// dummy if `rel->part_rels[i]` is NULL, or the child rel has been proven empty.
fn is_dummy_partition(root: &PlannerInfo, view: &RelView, part_index: i32) -> bool {
    debug_assert!(part_index >= 0);
    match view.part_rels.get(part_index as usize).copied().flatten() {
        None => true,
        Some(child) => seams::is_dummy_rel::call(root, child),
    }
}

/* ==========================================================================
 * partition_bounds_merge — the dispatcher (partbounds.c:1118)
 * ======================================================================== */

pub fn partition_bounds_merge(
    root: &mut PlannerInfo,
    rel1: RelId,
    rel2: RelId,
    jointype: JoinType,
) -> PgResult<
    Option<(
        PartitionBoundInfoData,
        Vec<Option<RelId>>,
        Vec<Option<RelId>>,
    )>,
> {
    // Currently, this function is called only from try_partitionwise_join(), so
    // the join type should be INNER, LEFT, FULL, SEMI, or ANTI.
    debug_assert!(matches!(
        jointype,
        JOIN_INNER | JOIN_LEFT | JOIN_FULL | JOIN_SEMI | JOIN_ANTI
    ));

    let outer = RelView::snapshot(root, rel1)?;
    let inner = RelView::snapshot(root, rel2)?;

    // The partitioning strategies should be the same.
    debug_assert!(outer.boundinfo.strategy == inner.boundinfo.strategy);

    // partsupfunc / partcollation / partnatts come from the join rel's shared
    // partition scheme. The two inputs share the same scheme, so either input's
    // scheme is fine; read it once.
    let scheme = root.rel(rel1).part_scheme.as_ref().ok_or_else(|| {
        PgError::error("partition_bounds_merge: partitioned rel must have a part_scheme")
    })?;
    let partnatts = scheme.partnatts as i32;
    let partsupfunc = scheme.partsupfunc.clone();
    let partcollation = scheme.partcollation.clone();

    match outer.boundinfo.strategy {
        PARTITION_STRATEGY_HASH => {
            // For hash partitioned tables, we currently support partitioned join
            // only when they have exactly the same partition bounds; that case
            // is handled by partition_bounds_equal upstream, so report
            // not-mergeable here.
            Ok(None)
        }
        PARTITION_STRATEGY_LIST => merge_list_bounds(
            root,
            &partsupfunc,
            &partcollation,
            &outer,
            &inner,
            jointype,
        ),
        PARTITION_STRATEGY_RANGE => merge_range_bounds(
            root,
            partnatts,
            &partsupfunc,
            &partcollation,
            &outer,
            &inner,
            jointype,
        ),
        _ => Ok(None),
    }
}

#[inline]
fn bound_has_default(bi: &PartitionBoundInfoData) -> bool {
    bi.default_index != -1
}
#[inline]
fn bound_accepts_nulls(bi: &PartitionBoundInfoData) -> bool {
    bi.null_index != -1
}

/* ==========================================================================
 * merge_list_bounds (partbounds.c:1197)
 * ======================================================================== */

#[allow(clippy::too_many_arguments)]
fn merge_list_bounds(
    root: &PlannerInfo,
    partsupfunc: &[FmgrInfo],
    partcollation: &[Oid],
    outer: &RelView,
    inner: &RelView,
    jointype: JoinType,
) -> PgResult<
    Option<(
        PartitionBoundInfoData,
        Vec<Option<RelId>>,
        Vec<Option<RelId>>,
    )>,
> {
    let outer_bi = &outer.boundinfo;
    let inner_bi = &inner.boundinfo;

    let mut outer_has_default = bound_has_default(outer_bi);
    let mut inner_has_default = bound_has_default(inner_bi);
    let outer_default = outer_bi.default_index;
    let inner_default = inner_bi.default_index;
    let mut outer_has_null = bound_accepts_nulls(outer_bi);
    let mut inner_has_null = bound_accepts_nulls(inner_bi);

    debug_assert!(outer_bi.strategy == inner_bi.strategy && outer_bi.strategy == PARTITION_STRATEGY_LIST);
    // List partitioning doesn't require kinds.
    debug_assert!(outer_bi.kind.is_none() && inner_bi.kind.is_none());

    let mut outer_map = init_partition_map(outer.nparts);
    let mut inner_map = init_partition_map(inner.nparts);

    let mut next_index: i32 = 0;
    let mut null_index: i32 = -1;
    let mut default_index: i32 = -1;
    let mut merged_datums: Vec<Vec<DatumImage>> = Vec::new();
    let mut merged_indexes: Vec<i32> = Vec::new();

    // If the default partitions (if any) have been proven empty, deem them
    // non-existent.
    if outer_has_default && is_dummy_partition(root, outer, outer_default) {
        outer_has_default = false;
    }
    if inner_has_default && is_dummy_partition(root, inner, inner_default) {
        inner_has_default = false;
    }

    // Result accumulator: success path returns Some, the C `goto cleanup` with
    // an un-set merged_bounds returns None.
    let mut bail = false;

    // Merge partitions from both sides.
    let mut outer_pos: i32 = 0;
    let mut inner_pos: i32 = 0;
    while outer_pos < outer_bi.ndatums || inner_pos < inner_bi.ndatums {
        let mut outer_index: i32 = -1;
        let mut inner_index: i32 = -1;
        let mut merged_datum: Option<Vec<DatumImage>> = None;
        let mut merged_index: i32 = -1;

        if outer_pos < outer_bi.ndatums {
            outer_index = outer_bi.indexes[outer_pos as usize];
            if is_dummy_partition(root, outer, outer_index) {
                outer_pos += 1;
                continue;
            }
        }
        if inner_pos < inner_bi.ndatums {
            inner_index = inner_bi.indexes[inner_pos as usize];
            if is_dummy_partition(root, inner, inner_index) {
                inner_pos += 1;
                continue;
            }
        }

        // Get the list values.
        let outer_datums: Option<&Vec<DatumImage>> = if outer_pos < outer_bi.ndatums {
            Some(&outer_bi.datums[outer_pos as usize])
        } else {
            None
        };
        let inner_datums: Option<&Vec<DatumImage>> = if inner_pos < inner_bi.ndatums {
            Some(&inner_bi.datums[inner_pos as usize])
        } else {
            None
        };

        // Set cmpval as if the finished side has an extra higher value.
        let cmpval: i32 = if outer_pos >= outer_bi.ndatums {
            1
        } else if inner_pos >= inner_bi.ndatums {
            -1
        } else {
            let od = outer_datums.expect("outer_datums");
            let id = inner_datums.expect("inner_datums");
            cmp_images(&partsupfunc[0], partcollation[0], &od[0], &id[0])?
        };

        if cmpval == 0 {
            // Two list values match exactly.
            debug_assert!(outer_pos < outer_bi.ndatums);
            debug_assert!(inner_pos < inner_bi.ndatums);
            debug_assert!(outer_index >= 0);
            debug_assert!(inner_index >= 0);

            merged_index = merge_matching_partitions(
                &mut outer_map,
                &mut inner_map,
                outer_index,
                inner_index,
                &mut next_index,
            );
            if merged_index == -1 {
                bail = true;
                break;
            }
            merged_datum = outer_datums.cloned();

            outer_pos += 1;
            inner_pos += 1;
        } else if cmpval < 0 {
            // A list value missing from the inner side.
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
                    bail = true;
                    break;
                }
                merged_datum = outer_datums.cloned();
            }

            outer_pos += 1;
        } else {
            // A list value missing from the outer side.
            debug_assert!(cmpval > 0);
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
                    bail = true;
                    break;
                }
                merged_datum = inner_datums.cloned();
            }

            inner_pos += 1;
        }

        // If we assigned a merged partition, add the list value and index.
        if merged_index >= 0 && merged_index != default_index {
            merged_datums.push(merged_datum.expect("merged_datum set when merged_index >= 0"));
            merged_indexes.push(merged_index);
        }
    }

    if bail {
        return Ok(None);
    }

    // If the NULL partitions (if any) have been proven empty, deem them
    // non-existent.
    if outer_has_null && is_dummy_partition(root, outer, outer_bi.null_index) {
        outer_has_null = false;
    }
    if inner_has_null && is_dummy_partition(root, inner, inner_bi.null_index) {
        inner_has_null = false;
    }

    // Merge the NULL partitions if any.
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

    // Merge the default partitions if any.
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

    // If we have merged partitions, create the partition bounds.
    if next_index > 0 {
        // Fix the merged_indexes list if necessary.
        if outer_map.did_remapping || inner_map.did_remapping {
            debug_assert!(jointype == JOIN_FULL);
            fix_merged_indexes(&outer_map, &inner_map, next_index, &mut merged_indexes);
        }

        let (outer_parts, inner_parts) =
            generate_matching_part_pairs(outer, inner, &outer_map, &inner_map, next_index)?;
        debug_assert!(!outer_parts.is_empty());
        debug_assert!(!inner_parts.is_empty());
        debug_assert!(outer_parts.len() == inner_parts.len());
        debug_assert!(outer_parts.len() as i32 <= next_index);

        let merged_bounds = build_merged_partition_bounds(
            outer_bi.strategy,
            merged_datums,
            None,
            merged_indexes,
            null_index,
            default_index,
        );
        return Ok(Some((merged_bounds, outer_parts, inner_parts)));
    }

    Ok(None)
}

/* ==========================================================================
 * merge_range_bounds (partbounds.c:1505)
 * ======================================================================== */

#[allow(clippy::too_many_arguments)]
fn merge_range_bounds(
    root: &PlannerInfo,
    partnatts: i32,
    partsupfunc: &[FmgrInfo],
    partcollation: &[Oid],
    outer: &RelView,
    inner: &RelView,
    jointype: JoinType,
) -> PgResult<
    Option<(
        PartitionBoundInfoData,
        Vec<Option<RelId>>,
        Vec<Option<RelId>>,
    )>,
> {
    let outer_bi = &outer.boundinfo;
    let inner_bi = &inner.boundinfo;

    let mut outer_has_default = bound_has_default(outer_bi);
    let mut inner_has_default = bound_has_default(inner_bi);
    let outer_default = outer_bi.default_index;
    let inner_default = inner_bi.default_index;

    debug_assert!(outer_bi.strategy == inner_bi.strategy && outer_bi.strategy == PARTITION_STRATEGY_RANGE);

    let mut outer_map = init_partition_map(outer.nparts);
    let mut inner_map = init_partition_map(inner.nparts);

    let mut next_index: i32 = 0;
    let mut default_index: i32 = -1;
    let mut merged_datums: Vec<Vec<DatumImage>> = Vec::new();
    let mut merged_kinds: Vec<Vec<i8>> = Vec::new();
    let mut merged_indexes: Vec<i32> = Vec::new();

    if outer_has_default && is_dummy_partition(root, outer, outer_default) {
        outer_has_default = false;
    }
    if inner_has_default && is_dummy_partition(root, inner, inner_default) {
        inner_has_default = false;
    }

    let mut bail = false;

    // Merge partitions from both sides.
    let mut outer_lb_pos: i32 = 0;
    let mut inner_lb_pos: i32 = 0;
    let mut outer_lb = RangeBound::empty(true);
    let mut outer_ub = RangeBound::empty(false);
    let mut inner_lb = RangeBound::empty(true);
    let mut inner_ub = RangeBound::empty(false);

    let mut outer_index = get_range_partition(
        root,
        outer,
        &mut outer_lb_pos,
        &mut outer_lb,
        &mut outer_ub,
    );
    let mut inner_index = get_range_partition(
        root,
        inner,
        &mut inner_lb_pos,
        &mut inner_lb,
        &mut inner_ub,
    );

    while outer_index >= 0 || inner_index >= 0 {
        let overlap;
        let ub_cmpval;
        let lb_cmpval;
        let mut merged_lb = RangeBound::empty(true);
        let mut merged_ub = RangeBound::empty(false);
        let mut merged_index: i32 = -1;

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
                partnatts,
                partsupfunc,
                partcollation,
                &outer_lb,
                &outer_ub,
                &inner_lb,
                &inner_ub,
            )?;
            overlap = ov;
            lb_cmpval = lbc;
            ub_cmpval = ubc;
        }

        if overlap {
            // Two ranges overlap; form a join pair.
            debug_assert!(outer_index >= 0);
            debug_assert!(
                outer_map.merged_indexes[outer_index as usize] == -1
                    && !outer_map.merged[outer_index as usize]
            );
            debug_assert!(inner_index >= 0);
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
            )?;

            // Save the upper bounds of both partitions for use below.
            let save_outer_ub = outer_ub.clone();
            let save_inner_ub = inner_ub.clone();

            // Move to the next pair of ranges.
            outer_index = get_range_partition(
                root,
                outer,
                &mut outer_lb_pos,
                &mut outer_lb,
                &mut outer_ub,
            );
            inner_index = get_range_partition(
                root,
                inner,
                &mut inner_lb_pos,
                &mut inner_lb,
                &mut inner_ub,
            );

            // Overlap with the next partition on the other side -> give up.
            if ub_cmpval > 0
                && inner_index >= 0
                && compare_range_bounds(partnatts, partsupfunc, partcollation, &save_outer_ub, &inner_lb)?
                    > 0
            {
                bail = true;
                break;
            }
            if ub_cmpval < 0
                && outer_index >= 0
                && compare_range_bounds(partnatts, partsupfunc, partcollation, &outer_lb, &save_inner_ub)?
                    < 0
            {
                bail = true;
                break;
            }

            // Non-overlapping portion could find a partner in the default
            // partition on the other side -> give up.
            if (outer_has_default && (lb_cmpval > 0 || ub_cmpval < 0))
                || (inner_has_default && (lb_cmpval < 0 || ub_cmpval > 0))
            {
                bail = true;
                break;
            }
        } else if ub_cmpval < 0 {
            // A non-overlapping outer range.
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
                    bail = true;
                    break;
                }
                merged_lb = outer_lb.clone();
                merged_ub = outer_ub.clone();
            }

            outer_index = get_range_partition(
                root,
                outer,
                &mut outer_lb_pos,
                &mut outer_lb,
                &mut outer_ub,
            );
        } else {
            // A non-overlapping inner range.
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
                    bail = true;
                    break;
                }
                merged_lb = inner_lb.clone();
                merged_ub = inner_ub.clone();
            }

            inner_index = get_range_partition(
                root,
                inner,
                &mut inner_lb_pos,
                &mut inner_lb,
                &mut inner_ub,
            );
        }

        // If we assigned a merged partition, add the range bounds and index.
        if merged_index >= 0 && merged_index != default_index {
            add_merged_range_bounds(
                partnatts,
                partsupfunc,
                partcollation,
                &merged_lb,
                &merged_ub,
                merged_index,
                &mut merged_datums,
                &mut merged_kinds,
                &mut merged_indexes,
            )?;
        }
    }

    if bail {
        return Ok(None);
    }

    // Merge the default partitions if any.
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

    // If we have merged partitions, create the partition bounds.
    if next_index > 0 {
        // Unlike list partitioning, we wouldn't have re-merged partitions.
        debug_assert!(!outer_map.did_remapping);
        debug_assert!(!inner_map.did_remapping);

        let (outer_parts, inner_parts) =
            generate_matching_part_pairs(outer, inner, &outer_map, &inner_map, next_index)?;
        debug_assert!(!outer_parts.is_empty());
        debug_assert!(!inner_parts.is_empty());
        debug_assert!(outer_parts.len() == inner_parts.len());
        debug_assert!(outer_parts.len() as i32 == next_index);

        let merged_bounds = build_merged_partition_bounds(
            outer_bi.strategy,
            merged_datums,
            Some(merged_kinds),
            merged_indexes,
            -1,
            default_index,
        );
        return Ok(Some((merged_bounds, outer_parts, inner_parts)));
    }

    Ok(None)
}

/* ==========================================================================
 * init_partition_map (partbounds.c:1810)
 * ======================================================================== */

fn init_partition_map(nparts: i32) -> PartitionMap {
    let n = nparts.max(0) as usize;
    PartitionMap {
        nparts,
        merged_indexes: vec![-1; n],
        merged: vec![false; n],
        did_remapping: false,
        old_indexes: vec![-1; n],
    }
}

/* ==========================================================================
 * merge_matching_partitions (partbounds.c:1861)
 * ======================================================================== */

fn merge_matching_partitions(
    outer_map: &mut PartitionMap,
    inner_map: &mut PartitionMap,
    outer_index: i32,
    inner_index: i32,
    next_index: &mut i32,
) -> i32 {
    debug_assert!(outer_index >= 0 && outer_index < outer_map.nparts);
    let outer_merged_index = outer_map.merged_indexes[outer_index as usize];
    let outer_merged = outer_map.merged[outer_index as usize];
    debug_assert!(inner_index >= 0 && inner_index < inner_map.nparts);
    let inner_merged_index = inner_map.merged_indexes[inner_index as usize];
    let inner_merged = inner_map.merged[inner_index as usize];

    let oi = outer_index as usize;
    let ii = inner_index as usize;

    // Both already assigned a merged partition.
    if outer_merged_index >= 0 && inner_merged_index >= 0 {
        if outer_merged_index == inner_merged_index {
            debug_assert!(outer_merged);
            debug_assert!(inner_merged);
            return outer_merged_index;
        }
        if !outer_merged && !inner_merged {
            // Only happens for list partitioning. Re-map to the smaller index.
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

    // At least one of the given partitions should not yet have been merged.
    debug_assert!(outer_merged_index == -1 || inner_merged_index == -1);

    if outer_merged_index == -1 && inner_merged_index == -1 {
        let merged_index = *next_index;
        debug_assert!(!outer_merged);
        debug_assert!(!inner_merged);
        outer_map.merged_indexes[oi] = merged_index;
        outer_map.merged[oi] = true;
        inner_map.merged_indexes[ii] = merged_index;
        inner_map.merged[ii] = true;
        *next_index += 1;
        return merged_index;
    }
    if outer_merged_index >= 0 && !outer_map.merged[oi] {
        debug_assert!(inner_merged_index == -1);
        debug_assert!(!inner_merged);
        inner_map.merged_indexes[ii] = outer_merged_index;
        inner_map.merged[ii] = true;
        outer_map.merged[oi] = true;
        return outer_merged_index;
    }
    if inner_merged_index >= 0 && !inner_map.merged[ii] {
        debug_assert!(outer_merged_index == -1);
        debug_assert!(!outer_merged);
        outer_map.merged_indexes[oi] = inner_merged_index;
        outer_map.merged[oi] = true;
        inner_map.merged[ii] = true;
        return inner_merged_index;
    }
    -1
}

/* ==========================================================================
 * process_outer_partition (partbounds.c:1979)
 * ======================================================================== */

#[allow(clippy::too_many_arguments)]
fn process_outer_partition(
    outer_map: &mut PartitionMap,
    inner_map: &mut PartitionMap,
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

        // Both sides have a default -> not handled.
        if outer_has_default {
            return -1;
        }

        merged_index = merge_matching_partitions(
            outer_map,
            inner_map,
            outer_index,
            inner_default,
            next_index,
        );
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

/* ==========================================================================
 * process_inner_partition (partbounds.c:2061)
 * ======================================================================== */

#[allow(clippy::too_many_arguments)]
fn process_inner_partition(
    outer_map: &mut PartitionMap,
    inner_map: &mut PartitionMap,
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

        // Both sides have a default -> not handled.
        if inner_has_default {
            return -1;
        }

        merged_index = merge_matching_partitions(
            outer_map,
            inner_map,
            outer_default,
            inner_index,
            next_index,
        );
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

/* ==========================================================================
 * merge_null_partitions (partbounds.c:2146)
 * ======================================================================== */

#[allow(clippy::too_many_arguments)]
fn merge_null_partitions(
    outer_map: &mut PartitionMap,
    inner_map: &mut PartitionMap,
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
        debug_assert!(outer_has_null);
        if is_outer_join(jointype) {
            debug_assert!(jointype != JOIN_RIGHT);
            *null_index = merge_partition_with_dummy(outer_map, outer_null, next_index);
        }
    } else if !consider_outer_null && consider_inner_null {
        debug_assert!(inner_has_null);
        if jointype == JOIN_FULL {
            *null_index = merge_partition_with_dummy(inner_map, inner_null, next_index);
        }
    } else {
        debug_assert!(consider_outer_null && consider_inner_null);
        debug_assert!(outer_has_null);
        debug_assert!(inner_has_null);
        if is_outer_join(jointype) {
            debug_assert!(jointype != JOIN_RIGHT);
            *null_index = merge_matching_partitions(
                outer_map,
                inner_map,
                outer_null,
                inner_null,
                next_index,
            );
            debug_assert!(*null_index >= 0);
        }
    }
}

/* ==========================================================================
 * merge_default_partitions (partbounds.c:2256)
 * ======================================================================== */

#[allow(clippy::too_many_arguments)]
fn merge_default_partitions(
    outer_map: &mut PartitionMap,
    inner_map: &mut PartitionMap,
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
                *default_index =
                    merge_partition_with_dummy(outer_map, outer_default, next_index);
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
                *default_index =
                    merge_partition_with_dummy(inner_map, inner_default, next_index);
            } else {
                debug_assert!(*default_index == inner_merged_index);
            }
        } else {
            debug_assert!(*default_index == -1);
        }
    } else {
        debug_assert!(outer_has_default && inner_has_default);
        debug_assert!(outer_merged_index == -1);
        debug_assert!(inner_merged_index == -1);
        debug_assert!(*default_index == -1);
        *default_index = merge_matching_partitions(
            outer_map,
            inner_map,
            outer_default,
            inner_default,
            next_index,
        );
        debug_assert!(*default_index >= 0);
    }
}

/* ==========================================================================
 * merge_partition_with_dummy (partbounds.c:2366)
 * ======================================================================== */

fn merge_partition_with_dummy(map: &mut PartitionMap, index: i32, next_index: &mut i32) -> i32 {
    let merged_index = *next_index;
    debug_assert!(index >= 0 && index < map.nparts);
    debug_assert!(map.merged_indexes[index as usize] == -1);
    debug_assert!(!map.merged[index as usize]);
    map.merged_indexes[index as usize] = merged_index;
    // Leave the merged flag alone!
    *next_index += 1;
    merged_index
}

/* ==========================================================================
 * fix_merged_indexes (partbounds.c:2384)
 * ======================================================================== */

fn fix_merged_indexes(
    outer_map: &PartitionMap,
    inner_map: &PartitionMap,
    nmerged: i32,
    merged_indexes: &mut [i32],
) {
    debug_assert!(nmerged > 0);

    let mut new_indexes = vec![-1i32; nmerged as usize];

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
        let merged_index = *slot;
        debug_assert!(merged_index >= 0);
        if new_indexes[merged_index as usize] >= 0 {
            *slot = new_indexes[merged_index as usize];
        }
    }
}

/* ==========================================================================
 * generate_matching_part_pairs (partbounds.c:2438)
 * ======================================================================== */

fn generate_matching_part_pairs(
    outer: &RelView,
    inner: &RelView,
    outer_map: &PartitionMap,
    inner_map: &PartitionMap,
    nmerged: i32,
) -> PgResult<(Vec<Option<RelId>>, Vec<Option<RelId>>)> {
    let outer_nparts = outer_map.nparts;
    let inner_nparts = inner_map.nparts;

    debug_assert!(nmerged > 0);

    let mut outer_indexes = vec![-1i32; nmerged as usize];
    let mut inner_indexes = vec![-1i32; nmerged as usize];

    debug_assert!(outer_nparts == outer.nparts);
    debug_assert!(inner_nparts == inner.nparts);
    let max_nparts = outer_nparts.max(inner_nparts);
    for i in 0..max_nparts {
        if i < outer_nparts {
            let merged_index = outer_map.merged_indexes[i as usize];
            if merged_index >= 0 {
                debug_assert!(merged_index < nmerged);
                outer_indexes[merged_index as usize] = i;
            }
        }
        if i < inner_nparts {
            let merged_index = inner_map.merged_indexes[i as usize];
            if merged_index >= 0 {
                debug_assert!(merged_index < nmerged);
                inner_indexes[merged_index as usize] = i;
            }
        }
    }

    let mut outer_parts: Vec<Option<RelId>> = Vec::new();
    let mut inner_parts: Vec<Option<RelId>> = Vec::new();
    for i in 0..nmerged as usize {
        let outer_index = outer_indexes[i];
        let inner_index = inner_indexes[i];

        // If both partitions are dummy, the merged partition was removed when
        // re-merging in merge_matching_partitions(); ignore it.
        if outer_index == -1 && inner_index == -1 {
            continue;
        }

        let op = if outer_index >= 0 {
            outer.part_rels[outer_index as usize]
        } else {
            None
        };
        let ip = if inner_index >= 0 {
            inner.part_rels[inner_index as usize]
        } else {
            None
        };
        outer_parts.push(op);
        inner_parts.push(ip);
    }

    Ok((outer_parts, inner_parts))
}

/* ==========================================================================
 * build_merged_partition_bounds (partbounds.c:2517)
 * ======================================================================== */

fn build_merged_partition_bounds(
    strategy: i8,
    merged_datums: Vec<Vec<DatumImage>>,
    merged_kinds: Option<Vec<Vec<i8>>>,
    mut merged_indexes: Vec<i32>,
    null_index: i32,
    default_index: i32,
) -> PartitionBoundInfoData {
    let mut ndatums = merged_datums.len() as i32;

    let kind = if strategy == PARTITION_STRATEGY_RANGE {
        let mk = merged_kinds.expect("range merge produces merged_kinds");
        debug_assert!(mk.len() as i32 == ndatums);
        // There are ndatums+1 indexes in the case of range partitioning.
        merged_indexes.push(-1);
        ndatums += 1;
        Some(mk)
    } else {
        debug_assert!(strategy == PARTITION_STRATEGY_LIST);
        debug_assert!(merged_kinds.is_none());
        None
    };

    debug_assert!(merged_indexes.len() as i32 == ndatums);

    PartitionBoundInfoData {
        strategy,
        ndatums: merged_datums.len() as i32,
        nindexes: ndatums,
        null_index,
        default_index,
        indexes: merged_indexes,
        datums: merged_datums,
        kind,
        // interleaved_parts is always NULL for join relations.
        interleaved_parts: Default::default(),
    }
}

/* ==========================================================================
 * get_range_partition / get_range_partition_internal (partbounds.c:2580/2601)
 * ======================================================================== */

fn get_range_partition(
    root: &PlannerInfo,
    view: &RelView,
    lb_pos: &mut i32,
    lb: &mut RangeBound,
    ub: &mut RangeBound,
) -> i32 {
    debug_assert!(view.boundinfo.strategy == PARTITION_STRATEGY_RANGE);
    loop {
        let part_index = get_range_partition_internal(&view.boundinfo, lb_pos, lb, ub);
        if part_index == -1 {
            return -1;
        }
        if !is_dummy_partition(root, view, part_index) {
            return part_index;
        }
    }
}

fn get_range_partition_internal(
    bi: &PartitionBoundInfoData,
    lb_pos: &mut i32,
    lb: &mut RangeBound,
    ub: &mut RangeBound,
) -> i32 {
    if *lb_pos >= bi.ndatums {
        return -1;
    }

    // A lower bound should have at least one more bound after it.
    debug_assert!(*lb_pos + 1 < bi.ndatums);

    let kind = bi.kind.as_ref().expect("range boundinfo has kind array");
    let p = *lb_pos as usize;

    // Set the lower bound.
    lb.index = bi.indexes[p];
    lb.datums = bi.datums[p].clone();
    lb.kind = kind[p].clone();
    lb.lower = true;
    // Set the upper bound.
    ub.index = bi.indexes[p + 1];
    ub.datums = bi.datums[p + 1].clone();
    ub.kind = kind[p + 1].clone();
    ub.lower = false;

    // The index assigned to an upper bound should be valid.
    debug_assert!(ub.index >= 0);

    // Advance the position to the next lower bound.
    if *lb_pos + 2 >= bi.ndatums {
        *lb_pos = bi.ndatums;
    } else if bi.indexes[(*lb_pos + 2) as usize] < 0 {
        *lb_pos += 2;
    } else {
        *lb_pos += 1;
    }

    ub.index
}

/* ==========================================================================
 * compare_range_partitions (partbounds.c:2661)
 * ======================================================================== */

/// Returns `(overlap, lb_cmpval, ub_cmpval)`.
fn compare_range_partitions(
    partnatts: i32,
    partsupfuncs: &[FmgrInfo],
    partcollations: &[Oid],
    outer_lb: &RangeBound,
    outer_ub: &RangeBound,
    inner_lb: &RangeBound,
    inner_ub: &RangeBound,
) -> PgResult<(bool, i32, i32)> {
    // outer ub < inner lb -> not overlapping.
    if compare_range_bounds(partnatts, partsupfuncs, partcollations, outer_ub, inner_lb)? < 0 {
        return Ok((false, -1, -1));
    }
    // outer lb > inner ub -> not overlapping.
    if compare_range_bounds(partnatts, partsupfuncs, partcollations, outer_lb, inner_ub)? > 0 {
        return Ok((false, 1, 1));
    }
    let lb_cmpval =
        compare_range_bounds(partnatts, partsupfuncs, partcollations, outer_lb, inner_lb)?;
    let ub_cmpval =
        compare_range_bounds(partnatts, partsupfuncs, partcollations, outer_ub, inner_ub)?;
    Ok((true, lb_cmpval, ub_cmpval))
}

/* ==========================================================================
 * get_merged_range_bounds (partbounds.c:2710)
 * ======================================================================== */

#[allow(clippy::too_many_arguments)]
fn get_merged_range_bounds(
    jointype: JoinType,
    outer_lb: &RangeBound,
    outer_ub: &RangeBound,
    inner_lb: &RangeBound,
    inner_ub: &RangeBound,
    lb_cmpval: i32,
    ub_cmpval: i32,
    merged_lb: &mut RangeBound,
    merged_ub: &mut RangeBound,
) -> PgResult<()> {
    match jointype {
        JOIN_INNER | JOIN_SEMI => {
            *merged_lb = if lb_cmpval > 0 { outer_lb.clone() } else { inner_lb.clone() };
            *merged_ub = if ub_cmpval < 0 { outer_ub.clone() } else { inner_ub.clone() };
        }
        JOIN_LEFT | JOIN_ANTI => {
            *merged_lb = outer_lb.clone();
            *merged_ub = outer_ub.clone();
        }
        JOIN_FULL => {
            *merged_lb = if lb_cmpval < 0 { outer_lb.clone() } else { inner_lb.clone() };
            *merged_ub = if ub_cmpval > 0 { outer_ub.clone() } else { inner_ub.clone() };
        }
        _ => {
            return Err(PgError::error(format!(
                "unrecognized join type: {}",
                jointype
            )));
        }
    }
    Ok(())
}

/* ==========================================================================
 * add_merged_range_bounds (partbounds.c:2774)
 * ======================================================================== */

#[allow(clippy::too_many_arguments)]
fn add_merged_range_bounds(
    partnatts: i32,
    partsupfuncs: &[FmgrInfo],
    partcollations: &[Oid],
    merged_lb: &RangeBound,
    merged_ub: &RangeBound,
    merged_index: i32,
    merged_datums: &mut Vec<Vec<DatumImage>>,
    merged_kinds: &mut Vec<Vec<i8>>,
    merged_indexes: &mut Vec<i32>,
) -> PgResult<()> {
    let cmpval: i32 = if merged_datums.is_empty() {
        // First merged partition.
        debug_assert!(merged_kinds.is_empty());
        debug_assert!(merged_indexes.is_empty());
        1
    } else {
        debug_assert!(!merged_kinds.is_empty());
        debug_assert!(!merged_indexes.is_empty());

        // Get the last upper bound.
        let prev_ub = RangeBound {
            index: *merged_indexes.last().unwrap(),
            datums: merged_datums.last().unwrap().clone(),
            kind: merged_kinds.last().unwrap().clone(),
            lower: false,
        };

        // We pass lower1 = false to prevent partition_rbound_cmp from
        // considering the last upper bound smaller than the merged partition's
        // lower bound when the two range bounds compare equal.
        let c = partition_rbound_cmp(
            partnatts,
            partsupfuncs,
            partcollations,
            &merged_lb.datums,
            &merged_lb.kind,
            false,
            &prev_ub,
        )?;
        debug_assert!(c >= 0);
        c
    };

    // If the lower bound is higher than the last upper bound, add the lower
    // bound with index -1; else reuse the last upper bound as the lower bound.
    if cmpval > 0 {
        merged_datums.push(merged_lb.datums.clone());
        merged_kinds.push(merged_lb.kind.clone());
        merged_indexes.push(-1);
    }

    // Add the upper bound and index of the merged partition.
    merged_datums.push(merged_ub.datums.clone());
    merged_kinds.push(merged_ub.kind.clone());
    merged_indexes.push(merged_index);
    Ok(())
}

/* ==========================================================================
 * partition_rbound_cmp / compare_range_bounds (partbounds.c)
 * ======================================================================== */

/// `partition_rbound_cmp(partnatts, partsupfunc, partcollation, datums1, kind1,
/// lower1, b2)` (partbounds.c) — port over the planner-layer [`DatumImage`]
/// carrier. Mirrors the arena-model `partition_rbound_cmp` in `lib.rs`.
fn partition_rbound_cmp(
    partnatts: i32,
    partsupfunc: &[FmgrInfo],
    partcollation: &[Oid],
    datums1: &[DatumImage],
    kind1: &[i8],
    lower1: bool,
    b2: &RangeBound,
) -> PgResult<i32> {
    let mut colnum: i32 = 0;
    let mut cmpval: i32 = 0;
    let datums2 = &b2.datums;
    let kind2 = &b2.kind;
    let lower2 = b2.lower;

    for i in 0..partnatts as usize {
        colnum += 1;

        // Handle unbounded columns first (MINVALUE < VALUE < MAXVALUE encoded as
        // -1 < 0 < 1).
        let k1 = kind1[i];
        let k2 = kind2[i];
        if k1 < k2 {
            return Ok(-colnum);
        } else if k1 > k2 {
            return Ok(colnum);
        } else if k1 != PARTITION_RANGE_DATUM_VALUE {
            // Both MINVALUE or both MAXVALUE.
            break;
        }

        cmpval = cmp_images(&partsupfunc[i], partcollation[i], &datums1[i], &datums2[i])?;
        if cmpval != 0 {
            break;
        }
    }

    // If equal, consider inclusivity: exclusive (upper) is smaller.
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

/// `compare_range_bounds(...)` macro (partbounds.c:88) — call
/// `partition_rbound_cmp` on bound1's datums/kind/lower against bound2, then
/// reduce the signed-column-number result to its sign (the merge cluster only
/// inspects the sign).
fn compare_range_bounds(
    partnatts: i32,
    partsupfunc: &[FmgrInfo],
    partcollation: &[Oid],
    bound1: &RangeBound,
    bound2: &RangeBound,
) -> PgResult<i32> {
    let c = partition_rbound_cmp(
        partnatts,
        partsupfunc,
        partcollation,
        &bound1.datums,
        &bound1.kind,
        bound1.lower,
        bound2,
    )?;
    Ok(c.signum())
}
