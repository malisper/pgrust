//! rangetypes_spgist.c: SP-GiST quad-tree over ranges mapped to 2d-points
//! (lower bound = x, upper bound = y; empties in a 5th root quadrant).
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use ::adt_rangetypes::ops;
use ::adt_rangetypes::{
    cached_range_info, range_cmp_bounds, range_deserialize, range_is_empty, range_serialize,
    range_type_oid, RangeBound, RangeInfo,
};
use ::datum::Datum;
use ::mcx::Mcx;
use ::rangetypes_gist::{
    pg_qsort_arg, varlena_image, RANGESTRAT_ADJACENT, RANGESTRAT_AFTER, RANGESTRAT_BEFORE,
    RANGESTRAT_CONTAINED_BY, RANGESTRAT_CONTAINS, RANGESTRAT_CONTAINS_ELEM, RANGESTRAT_EQ,
    RANGESTRAT_OVERLAPS, RANGESTRAT_OVERLEFT, RANGESTRAT_OVERRIGHT,
};
use ::types_core::{Oid, ANYRANGEOID, VOIDOID};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use ::types_spgist::state::{
    spgChooseIn, spgChooseOut, spgInnerConsistentIn, spgInnerConsistentOut, spgLeafConsistentIn,
    spgLeafConsistentOut, spgPickSplitIn, spgPickSplitOut,
};
use ::types_spgist::{spgConfigIn, spgConfigOut};

#[track_caller]
#[cold]
fn unrecognized_range_strategy(strategy: u16) -> Box<PgError> {
    Box::new(PgError::error(format!("unrecognized range strategy: {strategy}")))
}

fn fc_spg_range_quad_config(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol — args are live in/out structs.
    let _cfgin = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgConfigIn) };
    let cfg = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgConfigOut) };
    cfg.prefixType = ANYRANGEOID;
    cfg.labelType = VOIDOID;
    cfg.canReturnData = true;
    cfg.longValuesOK = false;
    Ok(Datum::null())
}

/// Quadrant of `tst` relative to `centroid` (1-4; empties are 5); ties go to
/// the higher quadrant along the perpendicular axis.
fn get_quadrant(mcx: Mcx<'_>, ri: &mut RangeInfo, centroid: &[u8], tst: &[u8]) -> PgResult<i16> {
    let (centroid_lower, centroid_upper, _centroid_empty) = range_deserialize(&ri.elem, centroid);
    let (lower, upper, empty) = range_deserialize(&ri.elem, tst);

    if empty {
        return Ok(5);
    }

    Ok(if range_cmp_bounds(mcx, ri, &lower, &centroid_lower)? >= 0 {
        if range_cmp_bounds(mcx, ri, &upper, &centroid_upper)? >= 0 {
            1
        } else {
            2
        }
    } else if range_cmp_bounds(mcx, ri, &upper, &centroid_upper)? >= 0 {
        4
    } else {
        3
    })
}

fn fc_spg_range_quad_choose(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgChooseIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgChooseOut) };
    let mcx = fcinfo.result_mcx();

    // SAFETY: choose datum is a live range varlena.
    let in_range = unsafe { varlena_image(mcx, input.datum) }?;
    let rest_datum = Datum::from_usize(in_range.as_ptr() as usize);

    if input.allTheSame {
        // nodeN is set by the core for match-node on an allTheSame tuple.
        *out = spgChooseOut::MatchNode { nodeN: 0, levelAdd: 0, restDatum: rest_datum };
        return Ok(Datum::null());
    }

    // Centroid-less node: node 0 empties, node 1 the rest.
    if !input.hasPrefix {
        *out = spgChooseOut::MatchNode {
            nodeN: if range_is_empty(in_range) { 0 } else { 1 },
            levelAdd: 1,
            restDatum: rest_datum,
        };
        return Ok(Datum::null());
    }

    let ri = cached_range_info(
        f.expect("spg_range_quad_choose: NULL flinfo"),
        range_type_oid(in_range),
    )?;
    // SAFETY: prefix datum is a live range varlena.
    let centroid = unsafe { varlena_image(mcx, input.prefixDatum) }?;
    let quadrant = get_quadrant(mcx, ri, centroid, in_range)?;
    debug_assert!((quadrant as i32) <= input.nNodes);

    *out = spgChooseOut::MatchNode {
        nodeN: quadrant as i32 - 1,
        levelAdd: 1,
        restDatum: rest_datum,
    };
    Ok(Datum::null())
}

fn fc_spg_range_quad_picksplit(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgPickSplitIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgPickSplitOut) };
    let mcx = fcinfo.result_mcx();
    let n = input.nTuples as usize;

    // SAFETY: nTuples datums per protocol.
    let datums = unsafe { core::slice::from_raw_parts(input.datums, n) };

    let mut ranges = Vec::with_capacity(n);
    for &d in datums {
        // SAFETY: leaf datums are live range varlenas.
        ranges.push(unsafe { varlena_image(mcx, d) }?);
    }

    let ri = cached_range_info(
        f.expect("spg_range_quad_picksplit: NULL flinfo"),
        range_type_oid(ranges[0]),
    )?;

    let mut lower_bounds = Vec::with_capacity(n);
    let mut upper_bounds = Vec::with_capacity(n);
    for &r in &ranges {
        let (lower, upper, empty) = range_deserialize(&ri.elem, r);
        if !empty {
            lower_bounds.push(lower);
            upper_bounds.push(upper);
        }
    }
    let non_empty_count = lower_bounds.len();

    let mut map: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut leaf_datums: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;

    // All empty: centroid-less node, everything in node 0.
    if non_empty_count == 0 {
        out.nNodes = 2;
        out.hasPrefix = false;
        out.prefixDatum = Datum::from_usize(0);
        out.nodeLabels = core::ptr::null();
        for &r in &ranges {
            leaf_datums.push(Datum::from_usize(r.as_ptr() as usize));
            map.push(0);
        }
        out.mapTuplesToNodes = map.as_mut_ptr();
        core::mem::forget(map);
        out.leafTupleDatums = leaf_datums.as_ptr();
        core::mem::forget(leaf_datums);
        return Ok(Datum::null());
    }

    pg_qsort_arg(&mut lower_bounds, |a, b| range_cmp_bounds(mcx, ri, a, b))?;
    pg_qsort_arg(&mut upper_bounds, |a, b| range_cmp_bounds(mcx, ri, a, b))?;

    let mut med_lower = lower_bounds[non_empty_count / 2];
    let mut med_upper = upper_bounds[non_empty_count / 2];
    let centroid = range_serialize(mcx, ri, &mut med_lower, &mut med_upper, false, None)?
        .expect("hard error path returns Some");
    let centroid = ::adt_multirangetypes::leak_image(centroid);
    out.hasPrefix = true;
    out.prefixDatum = Datum::from_usize(centroid.as_ptr() as usize);

    // Empty-ranges node exists only at the root.
    out.nNodes = if input.level == 0 { 5 } else { 4 };
    out.nodeLabels = core::ptr::null();

    for &r in &ranges {
        let quadrant = get_quadrant(mcx, ri, centroid, r)?;
        leaf_datums.push(Datum::from_usize(r.as_ptr() as usize));
        map.push(quadrant as i32 - 1);
    }
    out.mapTuplesToNodes = map.as_mut_ptr();
    core::mem::forget(map);
    out.leafTupleDatums = leaf_datums.as_ptr();
    core::mem::forget(leaf_datums);

    Ok(Datum::null())
}

/// Are bounds adjacent to `arg` smaller (-1) or >= (1) than the centroid?
fn adjacent_cmp_bounds(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    arg: &RangeBound,
    centroid: &RangeBound,
) -> PgResult<i32> {
    debug_assert!(arg.lower != centroid.lower);

    let cmp = range_cmp_bounds(mcx, ri, arg, centroid)?;

    if centroid.lower {
        // arg is an upper bound: search left only when arg is smaller than,
        // and not adjacent to, the centroid.
        if cmp < 0 && !ops::bounds_adjacent(mcx, ri, *arg, *centroid)? {
            Ok(-1)
        } else {
            Ok(1)
        }
    } else {
        // arg is a lower bound: search left when arg <= centroid.
        Ok(if cmp <= 0 { -1 } else { 1 })
    }
}

/// 0 = the previous level's traversal already ruled this direction out.
fn adjacent_inner_consistent(
    mcx: Mcx<'_>,
    ri: &mut RangeInfo,
    arg: &RangeBound,
    centroid: &RangeBound,
    prev: Option<&RangeBound>,
) -> PgResult<i32> {
    if let Some(prev) = prev {
        let prevcmp = adjacent_cmp_bounds(mcx, ri, arg, prev)?;
        let cmp = range_cmp_bounds(mcx, ri, centroid, prev)?;
        if (prevcmp < 0 && cmp >= 0) || (prevcmp > 0 && cmp < 0) {
            return Ok(0);
        }
    }
    adjacent_cmp_bounds(mcx, ri, arg, centroid)
}

fn fc_spg_range_quad_inner_consistent(
    f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgInnerConsistentIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgInnerConsistentOut) };
    let mcx = fcinfo.result_mcx();

    let mut need_previous = false;

    if input.allTheSame {
        let n = input.nNodes as usize;
        let mut node_numbers: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n)?;
        for i in 0..n {
            node_numbers.push(i as i32);
        }
        out.nNodes = input.nNodes;
        out.nodeNumbers = node_numbers.as_ptr();
        core::mem::forget(node_numbers);
        return Ok(Datum::null());
    }

    // SAFETY: nkeys scankeys per protocol.
    let scankeys =
        unsafe { core::slice::from_raw_parts(input.scankeys, input.nkeys.max(0) as usize) };

    let mut which: u32;

    if !input.hasPrefix {
        // Centroid-less node: node 0 holds empty ranges, node 1 the rest.
        debug_assert!(input.nNodes == 2);
        which = (1 << 1) | (1 << 2);
        for key in scankeys {
            let strategy = key.sk_strategy;
            let empty = if strategy != RANGESTRAT_CONTAINS_ELEM {
                // SAFETY: range scankey argument is a live varlena.
                range_is_empty(unsafe { varlena_image(mcx, key.sk_argument) }?)
            } else {
                false
            };

            match strategy {
                RANGESTRAT_BEFORE | RANGESTRAT_OVERLEFT | RANGESTRAT_OVERLAPS
                | RANGESTRAT_OVERRIGHT | RANGESTRAT_AFTER | RANGESTRAT_ADJACENT => {
                    if empty {
                        which = 0;
                    } else {
                        which &= 1 << 2;
                    }
                }
                RANGESTRAT_CONTAINS => {
                    if !empty {
                        which &= 1 << 2;
                    }
                }
                RANGESTRAT_CONTAINED_BY => {
                    if empty {
                        which &= 1 << 1;
                    }
                }
                RANGESTRAT_CONTAINS_ELEM => {
                    which &= 1 << 2;
                }
                RANGESTRAT_EQ => {
                    if empty {
                        which &= 1 << 1;
                    } else {
                        which &= 1 << 2;
                    }
                }
                other => return Err(unrecognized_range_strategy(other)),
            }
            if which == 0 {
                break;
            }
        }
    } else {
        // SAFETY: prefix datum is a live range varlena.
        let centroid = unsafe { varlena_image(mcx, input.prefixDatum) }?;
        let ri = cached_range_info(
            f.expect("spg_range_quad_inner_consistent: NULL flinfo"),
            range_type_oid(centroid),
        )?;
        let (centroid_lower, centroid_upper, _centroid_empty) =
            range_deserialize(&ri.elem, centroid);

        debug_assert!(input.nNodes == 4 || input.nNodes == 5);
        which = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5);

        for key in scankeys {
            let mut strategy = key.sk_strategy;

            let mut prev_lower: Option<RangeBound> = None;
            let mut prev_upper: Option<RangeBound> = None;

            // Bounding-box restrictions derived from the scan strategy.
            let mut min_lower: Option<&RangeBound> = None;
            let mut max_lower: Option<&RangeBound> = None;
            let mut min_upper: Option<&RangeBound> = None;
            let mut max_upper: Option<&RangeBound> = None;
            let mut inclusive = true;
            let mut strict_empty = true;

            let (range, lower, upper, empty): (&[u8], RangeBound, RangeBound, bool) =
                if strategy == RANGESTRAT_CONTAINS_ELEM {
                    // Expand the element to a singleton range and treat as
                    // RANGESTRAT_CONTAINS.
                    strategy = RANGESTRAT_CONTAINS;
                    (
                        &[],
                        RangeBound {
                            inclusive: true,
                            infinite: false,
                            lower: true,
                            val: key.sk_argument,
                        },
                        RangeBound {
                            inclusive: true,
                            infinite: false,
                            lower: false,
                            val: key.sk_argument,
                        },
                        false,
                    )
                } else {
                    // SAFETY: range scankey argument is a live varlena.
                    let r = unsafe { varlena_image(mcx, key.sk_argument) }?;
                    let (l, u, e) = range_deserialize(&ri.elem, r);
                    (r, l, u, e)
                };

            match strategy {
                RANGESTRAT_BEFORE => {
                    max_upper = Some(&lower);
                    inclusive = false;
                }
                RANGESTRAT_OVERLEFT => {
                    max_upper = Some(&upper);
                }
                RANGESTRAT_OVERLAPS => {
                    max_lower = Some(&upper);
                    min_upper = Some(&lower);
                }
                RANGESTRAT_OVERRIGHT => {
                    min_lower = Some(&lower);
                }
                RANGESTRAT_AFTER => {
                    min_lower = Some(&upper);
                    inclusive = false;
                }
                RANGESTRAT_ADJACENT => {
                    if !empty {
                        if input.traversalValue != 0 {
                            // SAFETY: traversalValue is our own copied
                            // centroid image from the previous level.
                            let prev_centroid = unsafe {
                                varlena_image(mcx, Datum::from_usize(input.traversalValue))
                            }?;
                            let (pl, pu, _pe) = range_deserialize(&ri.elem, prev_centroid);
                            prev_lower = Some(pl);
                            prev_upper = Some(pu);
                        }

                        // Bounds adjacent to arg's lower lie just below
                        // Y=lower: quadrants 2/3 or 1/4.
                        let cmp = adjacent_inner_consistent(
                            mcx,
                            ri,
                            &lower,
                            &centroid_upper,
                            prev_upper.as_ref(),
                        )?;
                        let which1 = if cmp > 0 {
                            (1 << 1) | (1 << 4)
                        } else if cmp < 0 {
                            (1 << 2) | (1 << 3)
                        } else {
                            0
                        };

                        // Adjacent to arg's upper: quadrants 3/4 or 1/2.
                        let cmp = adjacent_inner_consistent(
                            mcx,
                            ri,
                            &upper,
                            &centroid_lower,
                            prev_lower.as_ref(),
                        )?;
                        let which2 = if cmp > 0 {
                            (1 << 1) | (1 << 2)
                        } else if cmp < 0 {
                            (1 << 3) | (1 << 4)
                        } else {
                            0
                        };

                        which &= which1 | which2;
                        need_previous = true;
                    }
                }
                RANGESTRAT_CONTAINS => {
                    strict_empty = false;
                    if !empty {
                        which &= (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);
                        max_lower = Some(&lower);
                        min_upper = Some(&upper);
                    }
                }
                RANGESTRAT_CONTAINED_BY => {
                    strict_empty = false;
                    if empty {
                        which &= 1 << 5;
                    } else {
                        min_lower = Some(&lower);
                        max_upper = Some(&upper);
                    }
                }
                RANGESTRAT_EQ => {
                    strict_empty = false;
                    which &= 1 << get_quadrant(mcx, ri, centroid, range)?;
                }
                other => return Err(unrecognized_range_strategy(other)),
            }

            if strict_empty {
                if empty {
                    which = 0;
                    break;
                }
                which &= (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);
            }

            if let Some(min_lower) = min_lower {
                if range_cmp_bounds(mcx, ri, &centroid_lower, min_lower)? <= 0 {
                    which &= (1 << 1) | (1 << 2) | (1 << 5);
                }
            }
            if let Some(max_lower) = max_lower {
                let cmp = range_cmp_bounds(mcx, ri, &centroid_lower, max_lower)?;
                if cmp > 0 || (!inclusive && cmp == 0) {
                    which &= (1 << 3) | (1 << 4) | (1 << 5);
                }
            }
            if let Some(min_upper) = min_upper {
                if range_cmp_bounds(mcx, ri, &centroid_upper, min_upper)? <= 0 {
                    which &= (1 << 1) | (1 << 4) | (1 << 5);
                }
            }
            if let Some(max_upper) = max_upper {
                let cmp = range_cmp_bounds(mcx, ri, &centroid_upper, max_upper)?;
                if cmp > 0 || (!inclusive && cmp == 0) {
                    which &= (1 << 2) | (1 << 3) | (1 << 5);
                }
            }

            if which == 0 {
                break;
            }
        }
    }

    let n_nodes_in = input.nNodes as usize;
    let mut node_numbers: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n_nodes_in)?;
    let mut traversal_values: ::mcx::PgVec<'_, usize> =
        ::mcx::vec_with_capacity_in(mcx, if need_previous { n_nodes_in } else { 0 })?;

    for i in 1..=input.nNodes {
        if which & (1u32 << i) != 0 {
            if need_previous {
                // C datumCopy of this centroid for the child level.
                // SAFETY: prefix datum is a live range varlena.
                let img =
                    unsafe { varlena_image(input.traversalMemoryContext, input.prefixDatum) }?;
                let mut copy: ::mcx::PgVec<'_, u8> =
                    ::mcx::vec_with_capacity_in(input.traversalMemoryContext, img.len())?;
                copy.extend_from_slice(img);
                let copied = ::adt_multirangetypes::leak_image(copy);
                traversal_values.push(copied.as_ptr() as usize);
            }
            node_numbers.push(i - 1);
        }
    }

    out.nNodes = node_numbers.len() as i32;
    out.nodeNumbers = node_numbers.as_ptr();
    core::mem::forget(node_numbers);
    if need_previous {
        out.traversalValues = traversal_values.as_ptr();
        core::mem::forget(traversal_values);
    }

    Ok(Datum::null())
}

fn fc_spg_range_quad_leaf_consistent(
    f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgLeafConsistentIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgLeafConsistentOut) };
    let mcx = fcinfo.result_mcx();

    // All tests are exact.
    out.recheck = false;
    out.leafValue = input.leafDatum;

    // SAFETY: leaf datum is a live range varlena.
    let leaf = unsafe { varlena_image(mcx, input.leafDatum) }?;
    let ri = cached_range_info(
        f.expect("spg_range_quad_leaf_consistent: NULL flinfo"),
        range_type_oid(leaf),
    )?;

    // SAFETY: nkeys scankeys per protocol.
    let scankeys =
        unsafe { core::slice::from_raw_parts(input.scankeys, input.nkeys.max(0) as usize) };

    let mut res = true;
    for key in scankeys {
        let key_datum = key.sk_argument;
        res = match key.sk_strategy {
            RANGESTRAT_CONTAINS_ELEM => {
                ops::range_contains_elem_internal(mcx, ri, leaf, key_datum)?
            }
            strategy => {
                // SAFETY: range scankey argument is a live varlena.
                let query = unsafe { varlena_image(mcx, key_datum) }?;
                match strategy {
                    RANGESTRAT_BEFORE => ops::range_before_internal(mcx, ri, leaf, query)?,
                    RANGESTRAT_OVERLEFT => ops::range_overleft_internal(mcx, ri, leaf, query)?,
                    RANGESTRAT_OVERLAPS => ops::range_overlaps_internal(mcx, ri, leaf, query)?,
                    RANGESTRAT_OVERRIGHT => ops::range_overright_internal(mcx, ri, leaf, query)?,
                    RANGESTRAT_AFTER => ops::range_after_internal(mcx, ri, leaf, query)?,
                    RANGESTRAT_ADJACENT => ops::range_adjacent_internal(mcx, ri, leaf, query)?,
                    RANGESTRAT_CONTAINS => ops::range_contains_internal(mcx, ri, leaf, query)?,
                    RANGESTRAT_CONTAINED_BY => {
                        ops::range_contained_by_internal(mcx, ri, leaf, query)?
                    }
                    RANGESTRAT_EQ => ops::range_eq_internal(mcx, ri, leaf, query)?,
                    other => return Err(unrecognized_range_strategy(other)),
                }
            }
        };
        if !res {
            break;
        }
    }

    Ok(Datum::from_bool(res))
}

const fn b(
    foid: Oid,
    name: &'static str,
    nargs: i16,
    func: ::types_fmgr::PGFunction,
) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const RANGETYPES_SPGIST_BUILTINS: &[FmgrBuiltin] = &[
    b(3469, "spg_range_quad_config", 2, fc_spg_range_quad_config),
    b(3470, "spg_range_quad_choose", 2, fc_spg_range_quad_choose),
    b(3471, "spg_range_quad_picksplit", 2, fc_spg_range_quad_picksplit),
    b(3472, "spg_range_quad_inner_consistent", 2, fc_spg_range_quad_inner_consistent),
    b(3473, "spg_range_quad_leaf_consistent", 2, fc_spg_range_quad_leaf_consistent),
];
