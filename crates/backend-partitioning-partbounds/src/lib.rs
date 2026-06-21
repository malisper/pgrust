//! Owner crate for `backend/partitioning/partbounds.c` — the partition-bound
//! comparison / search / hash routines reached from the executor's tuple-routing
//! path (`execPartition.c`'s `get_partition_for_tuple`).
//!
//! This crate installs the routing seams declared in
//! `backend-partitioning-partbounds-seams`:
//!
//!   * [`compute_partition_hash_value`] — combined hash for HASH routing,
//!   * [`partition_list_bsearch`] / [`partition_list_datum_cmp`] — LIST search,
//!   * [`partition_range_datum_bsearch`] / [`partition_rbound_datum_cmp`] —
//!     RANGE search.
//!
//! These are pure bound-math: they operate on the owned `PartitionKeyData` /
//! `PartitionBoundInfoData` vocabulary and dispatch the per-key comparison /
//! hash support functions through the fmgr `function_call2_coll_datum` seam
//! (`FunctionCall2Coll` in C). The comparison/hash support functions can
//! `ereport(ERROR)`, carried on `Err`.
//!
//! Faithful 1:1 port of the corresponding routines in PostgreSQL 18.3
//! `partbounds.c`. The wider bound-construction / qual-building / partitionwise-
//! join merge families of `partbounds.c` are NOT yet ported here (they bottom
//! out on the unported `transformPartitionBound` / catalog node-construction
//! machinery); only the routing-search leg lands.

mod qual;
mod satisfies_hash_partition;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, MemoryContext, PgBox, PgVec};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_nodes::ddlnodes::PartitionBoundSpec;
use types_nodes::nodes::Node;
use types_nodes::partition::{
    PartitionBoundInfo, PartitionBoundInfoData, PartitionDescData, PartitionKeyData,
    PartitionRangeDatumKind, PartitionStrategy,
};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_fmgr_fmgr_seams as fmgr;

/// `HASH_PARTITION_SEED` (`catalog/partition.h`) — the seed combined with each
/// partition key's hash, matching the C constant bit-for-bit.
const HASH_PARTITION_SEED: u64 = 0x7A5B_2236_7996_DCFD;

/// `PARTITION_RANGE_DATUM_VALUE` discriminant as carried in the owned
/// `PartitionRangeDatumKind`.
const fn is_minvalue(k: PartitionRangeDatumKind) -> bool {
    matches!(k, PartitionRangeDatumKind::MinValue)
}
const fn is_maxvalue(k: PartitionRangeDatumKind) -> bool {
    matches!(k, PartitionRangeDatumKind::MaxValue)
}

/// `hash_combine64(a, b)` (`common/hashfn.h`) — combine two 64-bit hashes,
/// matching the C constant and shifts bit-for-bit.
#[inline]
fn hash_combine64(a: u64, b: u64) -> u64 {
    // a ^= b + 0x49a0f4dd15e5a8e3 + (a << 54) + (a >> 7);
    a ^ b
        .wrapping_add(0x49a0_f4dd_15e5_a8e3)
        .wrapping_add(a << 54)
        .wrapping_add(a >> 7)
}

/// `DatumGetInt32(FunctionCall2Coll(&partsupfunc[i], collation, a1, a2))`.
///
/// The routing seams carry no `mcx`; the C `FunctionCall2Coll` allocates only
/// the transient call frame, so we mirror `function_call3_seam` and create a
/// throwaway context for the dispatch. The comparison support function is
/// re-resolved by its lookup OID stamped on the `FmgrInfo`.
fn call_cmp(
    finfo: &FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
) -> PgResult<i32> {
    let ctx = MemoryContext::new("partition_cmp");
    let result =
        fmgr::function_call2_coll_datum::call(ctx.mcx(), finfo.fn_oid, collation, arg1, arg2)?
            .as_i32();
    Ok(result)
}

/// `DatumGetUInt64(FunctionCall2Coll(&partsupfunc[i], collation, value, seed))`.
fn call_hash(
    finfo: &FmgrInfo,
    collation: Oid,
    value: Datum,
    seed: Datum,
) -> PgResult<u64> {
    let ctx = MemoryContext::new("partition_hash");
    let result =
        fmgr::function_call2_coll_datum::call(ctx.mcx(), finfo.fn_oid, collation, value, seed)?
            .as_u64();
    Ok(result)
}

/// `elog(ERROR, msg)` — internal error on a caller/catalog inconsistency.
fn elog_error(msg: impl Into<String>) -> PgError {
    PgError::error(msg.into())
}

/* ===========================================================================
 * private bound structs used by the construction / qsort paths
 * ========================================================================= */

/// `PartitionHashBound` (partbounds.c) — one bound of a hash partition.
#[derive(Clone, Copy, Debug, Default)]
struct PartitionHashBound {
    modulus: i32,
    remainder: i32,
    index: i32,
}

/// `PartitionListValue` (partbounds.c) — one value coming from some list
/// partition (the owned `Datum` carried by value).
#[derive(Clone, Debug)]
struct PartitionListValue {
    index: i32,
    value: Datum<'static>,
}

/// `PartitionRangeBound` (partbounds.c) — one bound of a range partition,
/// expanded from a list of [`types_nodes::ddlnodes::PartitionRangeDatum`].
#[derive(Clone, Debug)]
struct PartitionRangeBound {
    /// `int index` — partition's position in the original list.
    index: i32,
    /// `Datum *datums` — the per-column bound values (undefined for non-VALUE
    /// columns).
    datums: Vec<Datum<'static>>,
    /// `PartitionRangeDatumKind *kind` — per-column MINVALUE/VALUE/MAXVALUE.
    kind: Vec<PartitionRangeDatumKind>,
    /// `bool lower` — is this a lower bound?
    lower: bool,
}

/// `Int32GetDatum(X)` — sign-extend into the full-width Datum.
#[inline]
fn int32_get_datum(value: i32) -> Datum<'static> {
    Datum::from_i32(value)
}

/// `partition_bound_accepts_nulls(bi)` — `bi->null_index != -1`.
#[inline]
fn partition_bound_accepts_nulls(bi: &PartitionBoundInfoData) -> bool {
    bi.null_index != -1
}

/// `partition_bound_has_default(bi)` — `bi->default_index != -1`.
#[inline]
fn partition_bound_has_default(bi: &PartitionBoundInfoData) -> bool {
    bi.default_index != -1
}

/// `datumCopy(value, typByVal, typLen)` through the scalar-datum seam, charged
/// to `mcx`. The copy lives in `mcx` (C: `palloc` in the current context), so
/// it carries the `'mcx` lifetime of the bound storage being built.
fn datum_copy<'mcx>(
    mcx: Mcx<'mcx>,
    value: &Datum,
    typbyval: bool,
    typlen: i16,
) -> PgResult<Datum<'mcx>> {
    backend_utils_adt_scalar_seams::datum_copy::call(mcx, value, typbyval, typlen)
}

/* ===========================================================================
 * partition_bounds_create + create_{hash,list,range}_bounds
 * ========================================================================= */

/// `partition_bounds_create(boundspecs, nparts, key, mapping)` (partbounds.c) —
/// build a [`PartitionBoundInfoData`] from a list of partition bound specs.
/// Returns the bound info and the `*mapping` array (original index → canonical
/// index).
pub fn partition_bounds_create<'mcx>(
    mcx: Mcx<'mcx>,
    boundspecs: &[&PartitionBoundSpec<'_>],
    nparts: usize,
    key: &PartitionKeyData<'_>,
) -> PgResult<(PartitionBoundInfo<'mcx>, PgVec<'mcx, i32>)> {
    debug_assert!(nparts > 0);

    // *mapping = palloc(sizeof(int) * nparts); init to -1.
    let mut mapping: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, nparts)?;
    mapping.resize(nparts, -1);

    let bound = match key.strategy {
        PartitionStrategy::Hash => create_hash_bounds(mcx, boundspecs, nparts, key, &mut mapping)?,
        PartitionStrategy::List => create_list_bounds(mcx, boundspecs, nparts, key, &mut mapping)?,
        PartitionStrategy::Range => create_range_bounds(mcx, boundspecs, nparts, key, &mut mapping)?,
    };

    Ok((Some(alloc_in(mcx, bound)?), mapping))
}

/// `create_hash_bounds` (partbounds.c) — bounds for a hash partitioned table.
fn create_hash_bounds<'mcx>(
    mcx: Mcx<'mcx>,
    boundspecs: &[&PartitionBoundSpec<'_>],
    nparts: usize,
    key: &PartitionKeyData<'_>,
    mapping: &mut [i32],
) -> PgResult<PartitionBoundInfoData<'mcx>> {
    // hbounds = palloc(nparts * sizeof(PartitionHashBound)); convert from node.
    let mut hbounds: Vec<PartitionHashBound> = Vec::with_capacity(nparts);
    for (i, spec) in boundspecs.iter().enumerate().take(nparts) {
        if spec.strategy != PartitionStrategy::Hash as i8 {
            return Err(elog_error("invalid strategy in partition bound spec"));
        }
        hbounds.push(PartitionHashBound {
            modulus: spec.modulus,
            remainder: spec.remainder,
            index: i as i32,
        });
    }

    // Sort all the bounds in ascending order.
    hbounds.sort_by(|a, b| {
        partition_hbound_cmp(a.modulus, a.remainder, b.modulus, b.remainder).cmp(&0)
    });

    // After sorting, moduli are now stored in ascending order.
    let greatest_modulus = hbounds[nparts - 1].modulus;

    // datums[nparts], each a 2-element [modulus, remainder] row.
    let mut datums: PgVec<'mcx, PgVec<'mcx, Datum<'mcx>>> = vec_with_capacity_in(mcx, nparts)?;
    // indexes[greatest_modulus], all -1.
    let mut indexes: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, greatest_modulus as usize)?;
    indexes.resize(greatest_modulus as usize, -1);

    // Size the datums spine; each row is filled below in canonical order.
    for _ in 0..nparts {
        datums.push(PgVec::new_in(mcx));
    }
    for (i, hb) in hbounds.iter().enumerate().take(nparts) {
        let modulus = hb.modulus;
        let mut remainder = hb.remainder;

        let mut row: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, 2)?;
        row.push(int32_get_datum(modulus));
        row.push(int32_get_datum(remainder));
        datums[i] = row;

        while remainder < greatest_modulus {
            // overlap?
            debug_assert!(indexes[remainder as usize] == -1);
            indexes[remainder as usize] = i as i32;
            remainder += modulus;
        }

        mapping[hb.index as usize] = i as i32;
    }
    // pfree(hbounds) — owned Vec dropped.

    Ok(PartitionBoundInfoData {
        strategy: key.strategy,
        ndatums: nparts as i32,
        datums,
        kind: None,
        interleaved_parts: None,
        nindexes: greatest_modulus,
        indexes,
        // No special hash partitions.
        null_index: -1,
        default_index: -1,
    })
}

/// `partition_hbound_cmp(modulus1, remainder1, modulus2, remainder2)`
/// (partbounds.c) — compare modulus first, then remainder.
fn partition_hbound_cmp(modulus1: i32, remainder1: i32, modulus2: i32, remainder2: i32) -> i32 {
    if modulus1 < modulus2 {
        return -1;
    }
    if modulus1 > modulus2 {
        return 1;
    }
    if modulus1 == modulus2 && remainder1 != remainder2 {
        return if remainder1 > remainder2 { 1 } else { -1 };
    }
    0
}

/// `lfirst_node(Const, c)` — borrow a `Const` out of a `listdatums` node. C
/// asserts the node is a `Const`; a mismatch is a caller/parser bug.
fn const_from_node<'a>(node: &'a Node<'_>) -> PgResult<&'a types_nodes::primnodes::Const> {
    node.as_const()
        .ok_or_else(|| elog_error("partition list bound spec datum is not a Const"))
}

/// `get_non_null_list_datum_count` (partbounds.c) — count the non-null Datums
/// across all partitions.
fn get_non_null_list_datum_count(
    boundspecs: &[&PartitionBoundSpec<'_>],
    nparts: usize,
) -> PgResult<usize> {
    let mut count = 0usize;
    for spec in boundspecs.iter().take(nparts) {
        for d in spec.listdatums.iter() {
            let val = const_from_node(d)?;
            if !val.constisnull {
                count += 1;
            }
        }
    }
    Ok(count)
}

/// `create_list_bounds` (partbounds.c) — bounds for a list partitioned table.
fn create_list_bounds<'mcx>(
    mcx: Mcx<'mcx>,
    boundspecs: &[&PartitionBoundSpec<'_>],
    nparts: usize,
    key: &PartitionKeyData<'_>,
    mapping: &mut [i32],
) -> PgResult<PartitionBoundInfoData<'mcx>> {
    let mut next_index: i32 = 0;
    let mut default_index: i32 = -1;
    let mut null_index: i32 = -1;

    let ndatums = get_non_null_list_datum_count(boundspecs, nparts)?;
    // all_values = palloc(ndatums * sizeof(PartitionListValue)).
    let mut all_values: Vec<PartitionListValue> = Vec::with_capacity(ndatums);

    // Create a unified list of non-null values across all partitions.
    for (i, spec) in boundspecs.iter().enumerate().take(nparts) {
        if spec.strategy != PartitionStrategy::List as i8 {
            return Err(elog_error("invalid strategy in partition bound spec"));
        }

        // Note the default partition; no datum to add.
        if spec.is_default {
            default_index = i as i32;
            continue;
        }

        for d in spec.listdatums.iter() {
            let val = const_from_node(d)?;
            if !val.constisnull {
                all_values.push(PartitionListValue {
                    index: i as i32,
                    value: val.constvalue.clone(),
                });
            } else {
                // Never put a null into the values array; save the index.
                if null_index != -1 {
                    return Err(elog_error("found null more than once"));
                }
                null_index = i as i32;
            }
        }
    }

    debug_assert!(all_values.len() == ndatums);

    // qsort_arg(all_values, ndatums, ..., qsort_partition_list_value_cmp, key).
    let finfo = &key.partsupfunc[0];
    let collation = key.partcollation[0];
    let mut sort_err: Option<PgError> = None;
    all_values.sort_by(|a, b| {
        match call_cmp(finfo, collation, a.value.clone(), b.value.clone()) {
            Ok(v) => v.cmp(&0),
            Err(e) => {
                if sort_err.is_none() {
                    sort_err = Some(e);
                }
                core::cmp::Ordering::Equal
            }
        }
    });
    if let Some(e) = sort_err {
        return Err(e);
    }

    let mut datums: PgVec<'mcx, PgVec<'mcx, Datum<'mcx>>> = vec_with_capacity_in(mcx, ndatums)?;
    let mut indexes: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, ndatums)?;

    // Copy values.  Canonical indexes range from 0..nparts-1.
    for v in all_values.iter() {
        let orig_index = v.index;

        let mut row: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, 1)?;
        row.push(datum_copy(mcx, &v.value, key.parttypbyval[0], key.parttyplen[0])?);
        datums.push(row);

        // If the old index has no mapping, assign one.
        if mapping[orig_index as usize] == -1 {
            mapping[orig_index as usize] = next_index;
            next_index += 1;
        }

        indexes.push(mapping[orig_index as usize]);
    }
    // pfree(all_values).

    let mut boundinfo = PartitionBoundInfoData {
        strategy: key.strategy,
        ndatums: ndatums as i32,
        datums,
        kind: None,
        interleaved_parts: None,
        nindexes: ndatums as i32,
        indexes,
        // Will be set correctly below.
        null_index: -1,
        default_index: -1,
    };

    // Set the canonical value for null_index, if any.
    if null_index != -1 {
        debug_assert!(null_index >= 0);
        if mapping[null_index as usize] == -1 {
            mapping[null_index as usize] = next_index;
            next_index += 1;
        }
        boundinfo.null_index = mapping[null_index as usize];
    }

    // Set the canonical value for default_index, if any.
    if default_index != -1 {
        debug_assert!(default_index >= 0);
        debug_assert!(mapping[default_index as usize] == -1);
        mapping[default_index as usize] = next_index;
        next_index += 1;
        boundinfo.default_index = mapping[default_index as usize];
    }

    // Calculate interleaved partitions.
    if nparts > 1 {
        // Short-circuit: only 1 Datum allowed per partition?
        let accepts_nulls = partition_bound_accepts_nulls(&boundinfo) as i32;
        let has_default = partition_bound_has_default(&boundinfo) as i32;
        if boundinfo.ndatums + accepts_nulls + has_default != nparts as i32 {
            let mut last_index = -1;

            for i in 0..boundinfo.nindexes as usize {
                let index = boundinfo.indexes[i];

                if index < last_index {
                    add_interleaved(mcx, &mut boundinfo.interleaved_parts, index)?;
                } else if partition_bound_accepts_nulls(&boundinfo)
                    && index == boundinfo.null_index
                {
                    add_interleaved(mcx, &mut boundinfo.interleaved_parts, index)?;
                }

                last_index = index;
            }
        }

        // The DEFAULT partition is the catch-all; mark it interleaved.
        if partition_bound_has_default(&boundinfo) {
            let di = boundinfo.default_index;
            add_interleaved(mcx, &mut boundinfo.interleaved_parts, di)?;
        }
    }

    // All partitions must now have been assigned canonical indexes.
    debug_assert!(next_index == nparts as i32);
    Ok(boundinfo)
}

/// `boundinfo->interleaved_parts = bms_add_member(boundinfo->interleaved_parts,
/// x)` over the node-field [`Bitmapset`].
fn add_interleaved<'mcx>(
    mcx: Mcx<'mcx>,
    set: &mut Option<PgBox<'mcx, types_nodes::bitmapset::Bitmapset<'mcx>>>,
    x: i32,
) -> PgResult<()> {
    let updated = backend_nodes_core_seams::bms_add_member::call(mcx, set.take(), x)?;
    *set = Some(updated);
    Ok(())
}

/// `make_one_partition_rbound(key, index, datums, lower)` (partbounds.c) — build
/// a [`PartitionRangeBound`] from a list of `PartitionRangeDatum` nodes.
fn make_one_partition_rbound(
    key: &PartitionKeyData<'_>,
    index: i32,
    datum_nodes: &[mcx::PgBox<'_, Node<'_>>],
    lower: bool,
) -> PgResult<PartitionRangeBound> {
    debug_assert!(!datum_nodes.is_empty());

    let partnatts = key.partnatts as usize;
    let mut datums: Vec<Datum<'static>> = Vec::with_capacity(partnatts);
    datums.resize(partnatts, Datum::null());
    let mut kind: Vec<PartitionRangeDatumKind> = Vec::with_capacity(partnatts);
    kind.resize(partnatts, PartitionRangeDatumKind::Value);

    for (i, node) in datum_nodes.iter().enumerate() {
        let datum = match (**node).as_partitionrangedatum() {
            Some(prd) => prd,
            None => return Err(elog_error("range bound spec datum is not a PartitionRangeDatum")),
        };

        // What's contained in this range datum?
        kind[i] = datum.kind;

        if datum.kind == PartitionRangeDatumKind::Value {
            // The contained value is a Const node.
            let value = datum
                .value
                .as_ref()
                .ok_or_else(|| elog_error("invalid range bound datum"))?;
            let val = const_from_node(value)?;
            if val.constisnull {
                return Err(elog_error("invalid range bound datum"));
            }
            datums[i] = val.constvalue.clone();
        }
    }

    Ok(PartitionRangeBound {
        index,
        datums,
        kind,
        lower,
    })
}

/// `partition_rbound_cmp(partnatts, partsupfunc, partcollation, datums1, kind1,
/// lower1, b2)` (partbounds.c) — compare range bound 1 against `*b2`. Returns 0
/// if equal; otherwise non-zero whose sign indicates ordering and whose
/// magnitude is the 1-based key number of the first mismatching column.
fn partition_rbound_cmp(
    key: &PartitionKeyData<'_>,
    datums1: &[Datum<'_>],
    kind1: &[PartitionRangeDatumKind],
    lower1: bool,
    b2: &PartitionRangeBound,
) -> PgResult<i32> {
    let partnatts = key.partnatts as i32;
    let mut colnum: i32 = 0;
    let mut cmpval: i32 = 0;
    let datums2 = &b2.datums;
    let kind2 = &b2.kind;
    let lower2 = b2.lower;

    for i in 0..partnatts as usize {
        colnum += 1;

        // Handle unbounded columns first.
        let k1 = kind1[i] as i32;
        let k2 = kind2[i] as i32;
        if k1 < k2 {
            return Ok(-colnum);
        } else if k1 > k2 {
            return Ok(colnum);
        } else if kind1[i] != PartitionRangeDatumKind::Value {
            // Both MINVALUE or both MAXVALUE.
            break;
        }

        cmpval = call_cmp(
            &key.partsupfunc[i],
            key.partcollation[i],
            datums1[i].clone(),
            datums2[i].clone(),
        )?;
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

/// `create_range_bounds` (partbounds.c) — bounds for a range partitioned table.
fn create_range_bounds<'mcx>(
    mcx: Mcx<'mcx>,
    boundspecs: &[&PartitionBoundSpec<'_>],
    nparts: usize,
    key: &PartitionKeyData<'_>,
    mapping: &mut [i32],
) -> PgResult<PartitionBoundInfoData<'mcx>> {
    let mut default_index: i32 = -1;
    let mut next_index: i32 = 0;

    // all_bounds = palloc0(2 * nparts * ...): a Vec of the rbounds, in order.
    let mut all_bounds: Vec<PartitionRangeBound> = Vec::with_capacity(2 * nparts);

    for (i, spec) in boundspecs.iter().enumerate().take(nparts) {
        if spec.strategy != PartitionStrategy::Range as i8 {
            return Err(elog_error("invalid strategy in partition bound spec"));
        }

        if spec.is_default {
            default_index = i as i32;
            continue;
        }

        let lower = make_one_partition_rbound(key, i as i32, &spec.lowerdatums, true)?;
        let upper = make_one_partition_rbound(key, i as i32, &spec.upperdatums, false)?;
        all_bounds.push(lower);
        all_bounds.push(upper);
    }

    debug_assert!(
        all_bounds.len() == nparts * 2
            || (default_index != -1 && all_bounds.len() == (nparts - 1) * 2)
    );

    // Sort all the bounds in ascending order.
    let mut sort_err: Option<PgError> = None;
    all_bounds.sort_by(|b1, b2| {
        match partition_rbound_cmp(key, &b1.datums, &b1.kind, b1.lower, b2) {
            Ok(v) => v.cmp(&0),
            Err(e) => {
                if sort_err.is_none() {
                    sort_err = Some(e);
                }
                core::cmp::Ordering::Equal
            }
        }
    });
    if let Some(e) = sort_err {
        return Err(e);
    }

    // Save distinct bounds from all_bounds into rbounds.
    let mut rbounds: Vec<PartitionRangeBound> = Vec::with_capacity(all_bounds.len());
    let mut prev: Option<usize> = None;
    for i in 0..all_bounds.len() {
        let mut is_distinct = false;

        for jcol in 0..key.partnatts as usize {
            match prev {
                None => {
                    is_distinct = true;
                    break;
                }
                Some(pidx) => {
                    let cur = &all_bounds[i];
                    let p = &all_bounds[pidx];
                    if cur.kind[jcol] != p.kind[jcol] {
                        is_distinct = true;
                        break;
                    }
                    // Both MINVALUE/MAXVALUE: stop, treat as equal.
                    if cur.kind[jcol] != PartitionRangeDatumKind::Value {
                        break;
                    }
                    let cmpval = call_cmp(
                        &key.partsupfunc[jcol],
                        key.partcollation[jcol],
                        cur.datums[jcol].clone(),
                        p.datums[jcol].clone(),
                    )?;
                    if cmpval != 0 {
                        is_distinct = true;
                        break;
                    }
                }
            }
        }

        if is_distinct {
            rbounds.push(all_bounds[i].clone());
        }

        prev = Some(i);
    }
    drop(all_bounds);

    // Update ndatums to hold the count of distinct datums.
    let ndatums = rbounds.len();
    let partnatts = key.partnatts as usize;

    let mut datums: PgVec<'mcx, PgVec<'mcx, Datum<'mcx>>> = vec_with_capacity_in(mcx, ndatums)?;
    let mut kind_outer: PgVec<'mcx, PgVec<'mcx, PartitionRangeDatumKind>> =
        vec_with_capacity_in(mcx, ndatums)?;
    // An additional -1 is stored as the last element of indexes[].
    let mut indexes: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, ndatums + 1)?;

    for rb in rbounds.iter() {
        let mut datum_row: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, partnatts)?;
        let mut kind_row: PgVec<'mcx, PartitionRangeDatumKind> =
            vec_with_capacity_in(mcx, partnatts)?;
        for jcol in 0..partnatts {
            if rb.kind[jcol] == PartitionRangeDatumKind::Value {
                datum_row.push(datum_copy(
                    mcx,
                    &rb.datums[jcol],
                    key.parttypbyval[jcol],
                    key.parttyplen[jcol],
                )?);
            } else {
                datum_row.push(Datum::null());
            }
            kind_row.push(rb.kind[jcol]);
        }
        datums.push(datum_row);
        kind_outer.push(kind_row);

        // Lower bounds get invalid (-1) indexes.
        if rb.lower {
            indexes.push(-1);
        } else {
            let orig_index = rb.index;
            if mapping[orig_index as usize] == -1 {
                mapping[orig_index as usize] = next_index;
                next_index += 1;
            }
            indexes.push(mapping[orig_index as usize]);
        }
    }
    // pfree(rbounds).

    let mut boundinfo = PartitionBoundInfoData {
        strategy: key.strategy,
        ndatums: ndatums as i32,
        datums,
        kind: Some(kind_outer),
        interleaved_parts: None,
        nindexes: ndatums as i32 + 1,
        indexes,
        // No special null-accepting range partition.
        null_index: -1,
        // Will be set correctly below.
        default_index: -1,
    };

    // Set the canonical value for default_index, if any.
    if default_index != -1 {
        debug_assert!(default_index >= 0 && mapping[default_index as usize] == -1);
        mapping[default_index as usize] = next_index;
        next_index += 1;
        boundinfo.default_index = mapping[default_index as usize];
    }

    // The extra -1 element.
    boundinfo.indexes.push(-1);

    debug_assert!(next_index == nparts as i32);
    Ok(boundinfo)
}

/* ===========================================================================
 * partition_bounds_copy
 * ========================================================================= */

/// `partition_bounds_copy(src, key)` (partbounds.c) — return a copy of `src`,
/// with bound data types described by `key`.
pub fn partition_bounds_copy<'mcx>(
    mcx: Mcx<'mcx>,
    src: &PartitionBoundInfoData<'_>,
    key: &PartitionKeyData<'_>,
) -> PgResult<PgBox<'mcx, PartitionBoundInfoData<'mcx>>> {
    let ndatums = src.ndatums;
    let nindexes = src.nindexes;
    let partnatts = key.partnatts as usize;

    debug_assert!(key.strategy != PartitionStrategy::List || partnatts == 1);

    // Copy the kind[] array (only RANGE has a non-NULL kind).
    let kind = match &src.kind {
        Some(src_kind) => {
            debug_assert!(key.strategy == PartitionStrategy::Range);
            let mut dest_kind: PgVec<'mcx, PgVec<'mcx, PartitionRangeDatumKind>> =
                vec_with_capacity_in(mcx, ndatums as usize)?;
            for i in 0..ndatums as usize {
                let mut kind_row: PgVec<'mcx, PartitionRangeDatumKind> =
                    vec_with_capacity_in(mcx, partnatts)?;
                for jcol in 0..partnatts {
                    kind_row.push(src_kind[i][jcol]);
                }
                dest_kind.push(kind_row);
            }
            Some(dest_kind)
        }
        None => None,
    };
    let has_kind = kind.is_some();

    // Copy interleaved partitions for LIST partitioned tables.
    let interleaved_parts = match &src.interleaved_parts {
        Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
        None => None,
    };

    // For hash partitioning, datums array has two elements.
    let hash_part = key.strategy == PartitionStrategy::Hash;
    let natts = if hash_part { 2 } else { partnatts };

    let mut datums: PgVec<'mcx, PgVec<'mcx, Datum<'mcx>>> =
        vec_with_capacity_in(mcx, ndatums as usize)?;
    for i in 0..ndatums as usize {
        let mut row: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, natts)?;
        for jcol in 0..natts {
            let (byval, typlen) = if hash_part {
                // Always int4.
                (true, core::mem::size_of::<i32>() as i16)
            } else {
                (key.parttypbyval[jcol], key.parttyplen[jcol])
            };

            let copy_value = match &kind {
                Some(k) => k[i][jcol] == PartitionRangeDatumKind::Value,
                None => true,
            };
            if copy_value {
                row.push(datum_copy(mcx, &src.datums[i][jcol], byval, typlen)?);
            } else {
                row.push(Datum::null());
            }
        }
        datums.push(row);
    }

    let mut indexes: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, nindexes as usize)?;
    for i in 0..nindexes as usize {
        indexes.push(src.indexes[i]);
    }

    let _ = has_kind;
    alloc_in(
        mcx,
        PartitionBoundInfoData {
            strategy: src.strategy,
            ndatums,
            datums,
            kind,
            interleaved_parts,
            nindexes,
            indexes,
            null_index: src.null_index,
            default_index: src.default_index,
        },
    )
}

/* ===========================================================================
 * partition_rbound_datum_cmp
 * ========================================================================= */

/// `partition_rbound_datum_cmp(partsupfunc, partcollation, rb_datums, rb_kind,
/// tuple_datums, n_tuple_datums)` (partbounds.c): compare a range bound
/// (`rb_datums`/`rb_kind`) against a tuple's partition key (`tuple_datums`).
///
/// Returns `<0`, `0`, or `>0`. The per-key comparison support functions and
/// collations are read from the partitioned table's `PartitionKeyData`,
/// matching C's `partsupfunc[i]` / `partcollation[i]`.
pub fn partition_rbound_datum_cmp(
    key: &PartitionKeyData,
    rb_datums: &[Datum],
    rb_kind: &[PartitionRangeDatumKind],
    tuple_datums: &[Datum],
    n_tuple_datums: i32,
) -> PgResult<i32> {
    let mut cmpval: i32 = -1;

    for i in 0..n_tuple_datums as usize {
        if is_minvalue(rb_kind[i]) {
            return Ok(-1);
        } else if is_maxvalue(rb_kind[i]) {
            return Ok(1);
        }

        cmpval = call_cmp(
            &key.partsupfunc[i],
            key.partcollation[i],
            rb_datums[i].clone(),
            tuple_datums[i].clone(),
        )?;
        if cmpval != 0 {
            break;
        }
    }

    Ok(cmpval)
}

/* ===========================================================================
 * partition_list_bsearch / partition_list_datum_cmp
 * ========================================================================= */

/// `partition_list_bsearch(partsupfunc, partcollation, boundinfo, value,
/// &is_equal)` (partbounds.c): binary-search a LIST partition's bounds for
/// `value`. Returns `(bound_offset, is_equal)`; `bound_offset == -1` when the
/// value is below all bounds.
pub fn partition_list_bsearch(
    key: &PartitionKeyData,
    boundinfo: &PartitionBoundInfoData,
    value: Datum,
) -> PgResult<(i32, bool)> {
    let finfo = &key.partsupfunc[0];
    let collation = key.partcollation[0];

    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums - 1;
    let mut is_equal = false;

    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let cmpval = call_cmp(
            finfo,
            collation,
            boundinfo.datums[mid as usize][0].clone(),
            value.clone(),
        )?;
        if cmpval <= 0 {
            lo = mid;
            is_equal = cmpval == 0;
            if is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }

    Ok((lo, is_equal))
}

/// `FunctionCall2Coll(&key->partsupfunc[0], key->partcollation[0], last_datum,
/// value)` for a LIST partition's cached-find double-check
/// (`get_partition_for_tuple`): compare the last-found LIST bound datum against
/// the new key datum using the partition's first support (comparison) function.
pub fn partition_list_datum_cmp(
    key: &PartitionKeyData,
    last_datum: Datum,
    value: Datum,
) -> PgResult<i32> {
    call_cmp(&key.partsupfunc[0], key.partcollation[0], last_datum, value)
}

/* ===========================================================================
 * partition_range_datum_bsearch
 * ========================================================================= */

/// `partition_range_datum_bsearch(partsupfunc, partcollation, boundinfo,
/// nvalues, values, &is_equal)` (partbounds.c): binary-search a RANGE
/// partition's bounds for the key tuple. Returns `(bound_offset, is_equal)`.
pub fn partition_range_datum_bsearch(
    key: &PartitionKeyData,
    boundinfo: &PartitionBoundInfoData,
    nvalues: i32,
    values: &[Datum],
) -> PgResult<(i32, bool)> {
    let kind_outer = boundinfo
        .kind
        .as_ref()
        .expect("range boundinfo has no kind array");

    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums - 1;
    let mut is_equal = false;

    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let cmpval = partition_rbound_datum_cmp(
            key,
            &boundinfo.datums[mid as usize],
            &kind_outer[mid as usize],
            values,
            nvalues,
        )?;
        if cmpval <= 0 {
            lo = mid;
            is_equal = cmpval == 0;
            if is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }

    Ok((lo, is_equal))
}

/* ===========================================================================
 * compute_partition_hash_value
 * ========================================================================= */

/// `compute_partition_hash_value(partnatts, partsupfunc, partcollation, values,
/// isnull)` (partbounds.c): the combined hash of the partition-key values for
/// HASH routing. Nulls are ignored.
pub fn compute_partition_hash_value(
    key: &PartitionKeyData,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<u64> {
    let mut row_hash: u64 = 0;
    let seed = Datum::from_u64(HASH_PARTITION_SEED);

    for i in 0..key.partnatts as usize {
        // Nulls are just ignored.
        if !isnull[i] {
            let hash = call_hash(
                &key.partsupfunc[i],
                key.partcollation[i],
                values[i].clone(),
                seed.clone(),
            )?;
            row_hash = hash_combine64(row_hash, hash);
        }
    }

    Ok(row_hash)
}

/* ===========================================================================
 * partition_range_bsearch / partition_hash_bsearch
 * ========================================================================= */

/// `partition_range_bsearch(partnatts, partsupfunc, partcollation, boundinfo,
/// probe, *cmpval)` (partbounds.c) — index of the greatest range bound `<=` the
/// probe bound, or -1 if all are greater. `*cmpval` is set to 0 on exact match,
/// else a signed 1-based first-mismatching-column number.
fn partition_range_bsearch(
    key: &PartitionKeyData<'_>,
    boundinfo: &PartitionBoundInfoData<'_>,
    probe: &PartitionRangeBound,
) -> PgResult<(i32, i32)> {
    let kind_outer = boundinfo
        .kind
        .as_ref()
        .expect("range boundinfo has no kind array");

    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums - 1;
    let mut cmpval: i32 = 0;

    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        cmpval = partition_rbound_cmp(
            key,
            &boundinfo.datums[mid as usize],
            &kind_outer[mid as usize],
            boundinfo.indexes[mid as usize] == -1,
            probe,
        )?;
        if cmpval <= 0 {
            lo = mid;
            if cmpval == 0 {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }

    Ok((lo, cmpval))
}

/// `partition_hash_bsearch(boundinfo, modulus, remainder)` (partbounds.c) —
/// index of the greatest `(modulus, remainder)` pair `<=` the probe, or -1 if
/// all are greater.
fn partition_hash_bsearch(
    boundinfo: &PartitionBoundInfoData<'_>,
    modulus: i32,
    remainder: i32,
) -> i32 {
    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums - 1;

    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let bound_modulus = boundinfo.datums[mid as usize][0].as_i32();
        let bound_remainder = boundinfo.datums[mid as usize][1].as_i32();
        let cmpval = partition_hbound_cmp(bound_modulus, bound_remainder, modulus, remainder);
        if cmpval <= 0 {
            lo = mid;
            if cmpval == 0 {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }

    lo
}

/* ===========================================================================
 * check_new_partition_bound
 * ========================================================================= */

/// `get_rel_name(oid)` for an overlap/conflict error message, via the lsyscache
/// seam. A `None` (dropped catalog row) falls back to the OID text so the error
/// still names something concrete.
fn get_rel_name<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<String> {
    match backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, relid)? {
        Some(s) => Ok(s.as_str().to_string()),
        None => Ok(format!("{relid}")),
    }
}

/// `ERRCODE_INVALID_OBJECT_DEFINITION` (`42P17`) for the conflict/overlap
/// errors, matching C.
fn invalid_object_def(msg: String) -> PgError {
    PgError::new(types_error::ERROR, msg)
        .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
}

/// An `ERRCODE_INVALID_OBJECT_DEFINITION` error carrying both a primary message
/// and `errdetail`.
fn invalid_object_def_detail(msg: String, detail: String) -> PgError {
    invalid_object_def(msg).with_detail(detail)
}

/// `check_new_partition_bound(relname, parent, spec, pstate)` (partbounds.c) —
/// verify the new partition bound is valid and does not overlap any existing
/// partition. Faithful 1:1 port; the parent's `PartitionKey`/`PartitionDesc` are
/// passed in by the caller.
pub fn check_new_partition_bound<'mcx, 'k, 'd, 's>(
    mcx: Mcx<'mcx>,
    relname: &str,
    key: &PartitionKeyData<'k>,
    partdesc: &PartitionDescData<'d>,
    spec: &PartitionBoundSpec<'s>,
    pstate: Option<&types_nodes::parsestmt::ParseState<'_>>,
) -> PgResult<()> {
    // Attach `parser_errposition(pstate, location)` as the cursor position,
    // matching the C ereport's trailing `parser_errposition(...)` (no-op when
    // pstate is NULL or location < 0).
    let errpos = |location: i32| -> i32 {
        match pstate {
            Some(ps) if location >= 0 => {
                backend_parser_small1_seams::parser_errposition::call(ps, location).unwrap_or(0)
            }
            _ => 0,
        }
    };
    let boundinfo = partdesc.boundinfo.as_deref();
    let mut with: i32 = -1;
    let mut overlap = false;
    let mut overlap_location: i32 = -1;

    if spec.is_default {
        // The default partition never conflicts with any other partition's
        // bounds; the only possible problem is that one already exists.
        match boundinfo {
            None => return Ok(()),
            Some(bi) if !partition_bound_has_default(bi) => return Ok(()),
            Some(bi) => {
                // Default partition already exists, error out.
                let other = get_rel_name(mcx, partdesc.oids[bi.default_index as usize])?;
                return Err(invalid_object_def(format!(
                    "partition \"{relname}\" conflicts with existing default partition \"{other}\""
                ))
                .with_cursor_position(errpos(spec.location)));
            }
        }
    }

    match key.strategy {
        PartitionStrategy::Hash => {
            debug_assert!(spec.strategy == PartitionStrategy::Hash as i8);
            debug_assert!(spec.remainder >= 0 && spec.remainder < spec.modulus);

            if partdesc.nparts > 0 {
                let bi = boundinfo.expect("hash partdesc with nparts>0 has boundinfo");

                // Every modulus must be a factor of the next larger modulus.
                let offset = partition_hash_bsearch(bi, spec.modulus, spec.remainder);
                if offset < 0 {
                    // All existing moduli are >= the new one, so the new one
                    // must be a factor of the smallest (first) one.
                    let next_modulus = bi.datums[0][0].as_i32();
                    if next_modulus % spec.modulus != 0 {
                        let other = get_rel_name(mcx, partdesc.oids[0])?;
                        return Err(invalid_object_def_detail(
                            "every hash partition modulus must be a factor of the next larger modulus".to_string(),
                            format!(
                                "The new modulus {} is not a factor of {}, the modulus of existing partition \"{}\".",
                                spec.modulus, next_modulus, other
                            ),
                        ));
                    }
                } else {
                    let prev_modulus = bi.datums[offset as usize][0].as_i32();
                    if spec.modulus % prev_modulus != 0 {
                        let other = get_rel_name(mcx, partdesc.oids[offset as usize])?;
                        return Err(invalid_object_def_detail(
                            "every hash partition modulus must be a factor of the next larger modulus".to_string(),
                            format!(
                                "The new modulus {} is not divisible by {}, the modulus of existing partition \"{}\".",
                                spec.modulus, prev_modulus, other
                            ),
                        ));
                    }

                    if (offset + 1) < bi.ndatums {
                        let next_modulus = bi.datums[(offset + 1) as usize][0].as_i32();
                        if next_modulus % spec.modulus != 0 {
                            let other = get_rel_name(mcx, partdesc.oids[(offset + 1) as usize])?;
                            return Err(invalid_object_def_detail(
                            "every hash partition modulus must be a factor of the next larger modulus".to_string(),
                            format!(
                                    "The new modulus {} is not a factor of {}, the modulus of existing partition \"{}\".",
                                    spec.modulus, next_modulus, other
                                ),
                        ));
                        }
                    }
                }

                let greatest_modulus = bi.nindexes;
                let mut remainder = spec.remainder;
                if remainder >= greatest_modulus {
                    remainder %= greatest_modulus;
                }

                // Check every potentially-conflicting remainder.
                loop {
                    if bi.indexes[remainder as usize] != -1 {
                        overlap = true;
                        overlap_location = spec.location;
                        with = bi.indexes[remainder as usize];
                        break;
                    }
                    remainder += spec.modulus;
                    if remainder >= greatest_modulus {
                        break;
                    }
                }
            }
        }
        PartitionStrategy::List => {
            debug_assert!(spec.strategy == PartitionStrategy::List as i8);

            if partdesc.nparts > 0 {
                let bi = boundinfo.expect("list partdesc with nparts>0 has boundinfo");
                debug_assert!(
                    bi.strategy == PartitionStrategy::List
                        && (bi.ndatums > 0
                            || partition_bound_accepts_nulls(bi)
                            || partition_bound_has_default(bi))
                );

                for cell in spec.listdatums.iter() {
                    let val = const_from_node(cell)?;
                    overlap_location = val.location;
                    if !val.constisnull {
                        let (offset, is_equal) =
                            partition_list_bsearch(key, bi, val.constvalue.clone())?;
                        if offset >= 0 && is_equal {
                            overlap = true;
                            with = bi.indexes[offset as usize];
                            break;
                        }
                    } else if partition_bound_accepts_nulls(bi) {
                        overlap = true;
                        with = bi.null_index;
                        break;
                    }
                }
            }
        }
        PartitionStrategy::Range => {
            debug_assert!(spec.strategy == PartitionStrategy::Range as i8);
            let lower = make_one_partition_rbound(key, -1, &spec.lowerdatums, true)?;
            let upper = make_one_partition_rbound(key, -1, &spec.upperdatums, false)?;

            // First check if the resulting range would be empty.
            // partition_rbound_cmp cannot return zero here (the lower-bound
            // flags differ).
            let cmpval =
                partition_rbound_cmp(key, &lower.datums, &lower.kind, true, &upper)?;
            debug_assert!(cmpval != 0);
            if cmpval > 0 {
                let lower_str =
                    backend_partitioning_partbounds_seams::get_range_partbound_string::call(
                        mcx,
                        &spec.lowerdatums,
                    )?;
                let upper_str =
                    backend_partitioning_partbounds_seams::get_range_partbound_string::call(
                        mcx,
                        &spec.upperdatums,
                    )?;
                // C: parser_errposition(pstate, datum->location) where
                // datum = list_nth(spec->lowerdatums, cmpval - 1).
                let datum_loc = range_datum_location(&spec.lowerdatums, cmpval - 1);
                return Err(invalid_object_def_detail(
                            format!("empty range bound specified for partition \"{relname}\""),
                            format!(
                        "Specified lower bound {lower_str} is greater than or equal to upper bound {upper_str}."
                    ),
                        ).with_cursor_position(errpos(datum_loc)));
            }

            if partdesc.nparts > 0 {
                let bi = boundinfo.expect("range partdesc with nparts>0 has boundinfo");
                debug_assert!(
                    bi.strategy == PartitionStrategy::Range
                        && (bi.ndatums > 0 || partition_bound_has_default(bi))
                );

                // Test whether the new lower bound (inclusive) lies inside an
                // existing partition, or in a gap.
                let (offset, _bs_cmpval) = partition_range_bsearch(key, bi, &lower)?;

                if bi.indexes[(offset + 1) as usize] < 0 {
                    // Check that the new partition fits in the gap: the new upper
                    // bound must be <= the lower bound of the next partition.
                    if (offset + 1) < bi.ndatums {
                        let datums = &bi.datums[(offset + 1) as usize];
                        let kind_outer = bi
                            .kind
                            .as_ref()
                            .expect("range boundinfo has no kind array");
                        let kind = &kind_outer[(offset + 1) as usize];
                        let is_lower = bi.indexes[(offset + 1) as usize] == -1;

                        let cmpval =
                            partition_rbound_cmp(key, datums, kind, is_lower, &upper)?;
                        if cmpval < 0 {
                            // The new partition overlaps the existing partition
                            // between offset + 1 and offset + 2.  C points to the
                            // problematic key in the upper datums list.
                            overlap = true;
                            overlap_location =
                                range_datum_location(&spec.upperdatums, cmpval.abs() - 1);
                            with = bi.indexes[(offset + 2) as usize];
                        }
                    }
                } else {
                    // The new partition overlaps the existing partition between
                    // offset and offset + 1.  C points to the problematic key in
                    // the lower datums list; on equality, the first one.
                    overlap = true;
                    overlap_location = if _bs_cmpval == 0 {
                        range_datum_location(&spec.lowerdatums, 0)
                    } else {
                        range_datum_location(&spec.lowerdatums, _bs_cmpval.abs() - 1)
                    };
                    with = bi.indexes[(offset + 1) as usize];
                }
            }
        }
    }

    if overlap {
        debug_assert!(with >= 0);
        let other = get_rel_name(mcx, partdesc.oids[with as usize])?;
        return Err(invalid_object_def(format!(
            "partition \"{relname}\" would overlap partition \"{other}\""
        ))
        .with_cursor_position(errpos(overlap_location)));
    }

    Ok(())
}

/// `((PartitionRangeDatum *) list_nth(datums, idx))->location`, or -1 when the
/// index is out of range or the node is not a `PartitionRangeDatum`.
fn range_datum_location(
    datums: &PgVec<'_, types_nodes::nodes::NodePtr<'_>>,
    idx: i32,
) -> i32 {
    if idx < 0 {
        return -1;
    }
    match datums.get(idx as usize) {
        Some(node) => (**node)
            .as_partitionrangedatum()
            .map(|d| d.location)
            .unwrap_or(-1),
        None => -1,
    }
}

/* ===========================================================================
 * partition_bounds_equal (partbounds.c:896)
 * ========================================================================= */

/// `datumIsEqual(value1, value2, typByVal, typLen)` (datum.c) restricted to the
/// planner-layer [`DatumImage`] representation of a partition-bound datum. The
/// by-value arm compares the machine words; the by-ref arm compares the raw
/// bytes (length then `memcmp`), exactly as `datumIsEqual` does for a flat
/// pass-by-reference value. `typbyval`/`typlen` are implied by the image arm.
fn datum_image_is_equal(
    a: &types_pathnodes::DatumImage,
    b: &types_pathnodes::DatumImage,
) -> bool {
    use types_pathnodes::DatumImage;
    match (a, b) {
        (DatumImage::ByVal(x), DatumImage::ByVal(y)) => x == y,
        (DatumImage::Bytes(x), DatumImage::Bytes(y)) => x == y,
        // Mixed arms can only occur if the two bounds disagree on by-val-ness,
        // which means the values are not equal.
        _ => false,
    }
}

/// `partition_bounds_equal(partnatts, parttyplen, parttypbyval, b1, b2)`
/// (partbounds.c:896). Are two partition-bound collections logically equal? Used
/// for partitionwise join: when two partitioned inputs have exactly equal
/// bounds, their same-position partitions pair 1:1. `PartitionBoundInfo` is a
/// canonical representation, so a faithful structural compare (no partitioning
/// operator) decides equality. `parttyplen`/`parttypbyval` are unused here
/// because the [`DatumImage`] carrier already encodes by-val-ness per datum; the
/// parameters are retained to mirror the C signature.
fn partition_bounds_equal(
    _partnatts: i32,
    _parttyplen: &[i16],
    _parttypbyval: &[bool],
    b1: &types_pathnodes::PartitionBoundInfoData,
    b2: &types_pathnodes::PartitionBoundInfoData,
) -> bool {
    // PARTITION_STRATEGY_HASH = 'h'.
    const PARTITION_STRATEGY_HASH: i8 = b'h' as i8;

    if b1.strategy != b2.strategy {
        return false;
    }
    if b1.ndatums != b2.ndatums {
        return false;
    }
    if b1.nindexes != b2.nindexes {
        return false;
    }
    if b1.null_index != b2.null_index {
        return false;
    }
    if b1.default_index != b2.default_index {
        return false;
    }

    // For all partition strategies, the indexes[] arrays have to match.
    if b1.indexes != b2.indexes {
        return false;
    }

    // Finally, compare the datums[] arrays.
    if b1.strategy == PARTITION_STRATEGY_HASH {
        // For hash, the datums[] arrays are the same iff the indexes[] arrays
        // are (partbounds.c:924) — the modulus/remainder layout makes datums a
        // function of indexes. Having matched indexes above, we are done.
    } else {
        for i in 0..(b1.ndatums as usize) {
            let row1 = &b1.datums[i];
            let row2 = &b2.datums[i];
            let partnatts = row1.len().min(row2.len());
            for j in 0..partnatts {
                // For range partitions, the bounds might not be finite.
                if let (Some(k1), Some(k2)) = (b1.kind.as_ref(), b2.kind.as_ref()) {
                    // The different kinds of bound all differ from each other.
                    if k1[i][j] != k2[i][j] {
                        return false;
                    }
                    // Non-finite bounds are equal without further examination.
                    // PARTITION_RANGE_DATUM_VALUE = 0.
                    if k1[i][j] != 0 {
                        continue;
                    }
                }
                // Compare the actual values bit-for-bit (datumIsEqual).
                if !datum_image_is_equal(&row1[j], &row2[j]) {
                    return false;
                }
            }
        }
    }
    true
}

/// `partition_bounds_merge(partnatts, partsupfunc, partcollation, rel1, rel2,
/// jointype, &outer_parts, &inner_parts)` (partbounds.c:1118) — merge the
/// partition bounds of two inputs whose bounds are *not* identical but may be
/// compatible (range/list merge producing the per-segment partition pairings).
///
/// The merge leg (`partition_range_bounds_merge` / `partition_list_bounds_merge`
/// and `merge_*` helpers, ~600 lines of partbounds.c) is not yet ported. Until
/// it lands, report "not mergeable" (`Ok(None)`), which is a faithful outcome of
/// `partition_bounds_merge` itself: `compute_partition_bounds` then sets
/// `joinrel->nparts = 0` and the join is planned as an ordinary (non
/// partitionwise) join — correct results, just without the per-partition
/// optimization for non-identical bounds. Identical-bounds partitionwise join
/// (the common case) is fully handled by `partition_bounds_equal` above and does
/// not reach this path.
fn partition_bounds_merge(
    _root: &mut types_pathnodes::PlannerInfo,
    _rel1: types_pathnodes::RelId,
    _rel2: types_pathnodes::RelId,
    _jointype: types_pathnodes::JoinType,
) -> PgResult<
    Option<(
        types_pathnodes::PartitionBoundInfoData,
        std::vec::Vec<Option<types_pathnodes::RelId>>,
        std::vec::Vec<Option<types_pathnodes::RelId>>,
    )>,
> {
    Ok(None)
}

/* ===========================================================================
 * seam installation
 * ========================================================================= */

/// Install the partbounds routing seams. Called once from `seams-init`.
pub fn init_seams() {
    use backend_partitioning_partbounds_seams as seams;

    seams::partition_bounds_equal::set(partition_bounds_equal);
    seams::partition_bounds_merge::set(partition_bounds_merge);
    seams::partition_bounds_create::set(partition_bounds_create);
    seams::partition_bounds_copy::set(partition_bounds_copy);
    seams::compute_partition_hash_value::set(compute_partition_hash_value);
    seams::partition_list_bsearch::set(partition_list_bsearch);
    seams::partition_list_datum_cmp::set(partition_list_datum_cmp);
    seams::partition_range_datum_bsearch::set(partition_range_datum_bsearch);
    seams::partition_rbound_datum_cmp::set(partition_rbound_datum_cmp);
    seams::check_new_partition_bound::set(check_new_partition_bound);
    seams::qual_from_partbound::set(qual::qual_from_partbound_seam);
    seams::get_qual_from_partbound::set(get_qual_from_partbound_seam);

    // Register the SQL-callable hash-partition predicate builtin (OID 5028).
    satisfies_hash_partition::register();
}

/// Adapter installing `get_qual_from_partbound` (partbounds.c:249): build the
/// implicit-AND qual `Node` list from a directly-supplied bound spec.
fn get_qual_from_partbound_seam<'mcx>(
    mcx: Mcx<'mcx>,
    parent_relid: Oid,
    key: &PartitionKeyData<'_>,
    spec: &PartitionBoundSpec<'_>,
    parent_partdesc: Option<&PartitionDescData<'_>>,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    let exprs = qual::get_qual_from_partbound(mcx, parent_relid, key, spec, parent_partdesc)?;
    let mut out: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for e in exprs {
        out.push(Node::mk_expr(mcx, e)?);
    }
    Ok(out)
}

/* ===========================================================================
 * unit tests — bound construction
 * ========================================================================= */

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::PgVec;
    use types_core::primitive::ParseLoc;
    use types_nodes::ddlnodes::PartitionBoundSpec;
    use types_nodes::partition::PartitionStrategy;

    /// A minimal `PartitionKeyData` for tests that only read `strategy` and the
    /// per-attribute typlen/typbyval/coll (hash needs none of the latter).
    fn key_for<'mcx>(
        mcx: Mcx<'mcx>,
        strategy: PartitionStrategy,
        partnatts: i16,
    ) -> PartitionKeyData<'mcx> {
        let n = partnatts as usize;
        let mut partcollation = PgVec::new_in(mcx);
        let mut partsupfunc = PgVec::new_in(mcx);
        let mut parttyplen = PgVec::new_in(mcx);
        let mut parttypbyval = PgVec::new_in(mcx);
        for _ in 0..n {
            partcollation.push(Oid::from(0u32));
            partsupfunc.push(FmgrInfo::default());
            parttyplen.push(4i16);
            parttypbyval.push(true);
        }
        PartitionKeyData {
            strategy,
            partnatts,
            partattrs: PgVec::new_in(mcx),
            partexprs: PgVec::new_in(mcx),
            partopfamily: PgVec::new_in(mcx),
            partopcintype: PgVec::new_in(mcx),
            partsupfunc,
            partcollation,
            parttypid: PgVec::new_in(mcx),
            parttypmod: PgVec::new_in(mcx),
            parttyplen,
            parttypbyval,
            parttypalign: PgVec::new_in(mcx),
            parttypcoll: PgVec::new_in(mcx),
        }
    }

    fn hash_spec<'mcx>(mcx: Mcx<'mcx>, modulus: i32, remainder: i32) -> PartitionBoundSpec<'mcx> {
        PartitionBoundSpec {
            strategy: PartitionStrategy::Hash as i8,
            is_default: false,
            modulus,
            remainder,
            listdatums: PgVec::new_in(mcx),
            lowerdatums: PgVec::new_in(mcx),
            upperdatums: PgVec::new_in(mcx),
            location: -1 as ParseLoc,
        }
    }

    /// Hash bounds: 4 partitions, modulus 4 with remainders {0,1,2,3} fed out of
    /// order. After sorting by (modulus, remainder), datums are ascending and
    /// `indexes[r]` points at the canonical partition for remainder `r`. The
    /// `mapping` maps each original spec slot to its canonical index.
    #[test]
    fn create_hash_bounds_sorts_and_maps() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let key = key_for(mcx, PartitionStrategy::Hash, 1);

        // Original order: remainders 2, 0, 3, 1 (all modulus 4).
        let specs_owned = [
            hash_spec(mcx, 4, 2),
            hash_spec(mcx, 4, 0),
            hash_spec(mcx, 4, 3),
            hash_spec(mcx, 4, 1),
        ];
        let specs: Vec<&PartitionBoundSpec> = specs_owned.iter().collect();

        let (bound, mapping) = partition_bounds_create(mcx, &specs, 4, &key).unwrap();
        let bi = bound.unwrap();

        assert_eq!(bi.strategy, PartitionStrategy::Hash);
        assert_eq!(bi.ndatums, 4);
        assert_eq!(bi.nindexes, 4); // greatest_modulus
        assert_eq!(bi.null_index, -1);
        assert_eq!(bi.default_index, -1);
        assert!(bi.kind.is_none());

        // After sorting, datums ascend by (modulus, remainder): r = 0,1,2,3.
        for (canon, &expected_rem) in [0, 1, 2, 3].iter().enumerate() {
            assert_eq!(bi.datums[canon][0].as_i32(), 4); // modulus
            assert_eq!(bi.datums[canon][1].as_i32(), expected_rem as i32);
        }
        // indexes[remainder] == canonical index == remainder here.
        for r in 0..4usize {
            assert_eq!(bi.indexes[r], r as i32);
        }
        // mapping: original slots had remainders [2,0,3,1] -> canonical [2,0,3,1].
        assert_eq!(&mapping[..], &[2, 0, 3, 1]);
    }

    /// Hash bounds with mixed moduli: modulus 2 (remainder 1) interleaves with
    /// modulus 4 (remainders 0,2). greatest_modulus=4; the modulus-2 partition
    /// fills remainders 1 and 3.
    #[test]
    fn create_hash_bounds_mixed_moduli() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let key = key_for(mcx, PartitionStrategy::Hash, 1);

        let specs_owned = [
            hash_spec(mcx, 4, 0),
            hash_spec(mcx, 2, 1),
            hash_spec(mcx, 4, 2),
        ];
        let specs: Vec<&PartitionBoundSpec> = specs_owned.iter().collect();

        let (bound, _mapping) = partition_bounds_create(mcx, &specs, 3, &key).unwrap();
        let bi = bound.unwrap();

        assert_eq!(bi.nindexes, 4);
        // Sorted ascending: (2,1),(4,0),(4,2) -> canonical 0,1,2.
        assert_eq!((bi.datums[0][0].as_i32(), bi.datums[0][1].as_i32()), (2, 1));
        assert_eq!((bi.datums[1][0].as_i32(), bi.datums[1][1].as_i32()), (4, 0));
        assert_eq!((bi.datums[2][0].as_i32(), bi.datums[2][1].as_i32()), (4, 2));
        // remainder 1 and 3 -> canonical 0 (the modulus-2 partition);
        // remainder 0 -> canonical 1; remainder 2 -> canonical 2.
        assert_eq!(bi.indexes[0], 1);
        assert_eq!(bi.indexes[1], 0);
        assert_eq!(bi.indexes[2], 2);
        assert_eq!(bi.indexes[3], 0);
    }

    /// `partition_hbound_cmp` orders by modulus first, then remainder.
    #[test]
    fn hbound_cmp_orders_modulus_then_remainder() {
        assert!(partition_hbound_cmp(2, 5, 4, 0) < 0); // smaller modulus first
        assert!(partition_hbound_cmp(4, 0, 2, 5) > 0);
        assert!(partition_hbound_cmp(4, 1, 4, 3) < 0); // same modulus, remainder
        assert!(partition_hbound_cmp(4, 3, 4, 1) > 0);
        assert_eq!(partition_hbound_cmp(4, 2, 4, 2), 0);
    }

    /// `partition_rbound_cmp` over non-VALUE columns (MINVALUE/MAXVALUE) needs no
    /// fmgr dispatch: MINVALUE < VALUE < MAXVALUE, and at equal finite columns a
    /// lower (inclusive) bound sorts after an upper (exclusive) one.
    #[test]
    fn rbound_cmp_unbounded_and_inclusivity() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let key = key_for(mcx, PartitionStrategy::Range, 1);

        use PartitionRangeDatumKind::{MaxValue, MinValue};

        let maxval = PartitionRangeBound {
            index: 0,
            datums: vec![Datum::null()],
            kind: vec![MaxValue],
            lower: false,
        };
        // MINVALUE column vs MAXVALUE column: -1 at colnum 1 (no fmgr dispatch).
        let c = partition_rbound_cmp(&key, &[Datum::null()], &[MinValue], true, &maxval).unwrap();
        assert_eq!(c, -1);

        // MAXVALUE vs MINVALUE -> +1.
        let minval = PartitionRangeBound {
            index: 0,
            datums: vec![Datum::null()],
            kind: vec![MinValue],
            lower: true,
        };
        let c2 = partition_rbound_cmp(&key, &[Datum::null()], &[MaxValue], false, &minval).unwrap();
        assert_eq!(c2, 1);

        // Both MAXVALUE, equal column: breaks out; differing inclusivity then
        // decides (lower=true sorts after exclusive upper) -> positive.
        let c3 = partition_rbound_cmp(&key, &[Datum::null()], &[MaxValue], true, &maxval).unwrap();
        assert!(c3 > 0);
    }
}
