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

use mcx::MemoryContext;
use types_core::fmgr::FmgrInfo;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::partition::{
    PartitionBoundInfoData, PartitionKeyData, PartitionRangeDatumKind,
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
 * seam installation
 * ========================================================================= */

/// Install the partbounds routing seams. Called once from `seams-init`.
pub fn init_seams() {
    use backend_partitioning_partbounds_seams as seams;

    seams::compute_partition_hash_value::set(compute_partition_hash_value);
    seams::partition_list_bsearch::set(partition_list_bsearch);
    seams::partition_list_datum_cmp::set(partition_list_datum_cmp);
    seams::partition_range_datum_bsearch::set(partition_range_datum_bsearch);
    seams::partition_rbound_datum_cmp::set(partition_rbound_datum_cmp);
}
