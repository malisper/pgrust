//! `statistics` family — `lsyscache.c` lookups keyed on `pg_statistic`
//! (`STATRELATTINH` syscache and the `get_attstatsslot` slot-extraction
//! machinery).
//!
//! C entry points covered here: `get_attstatsslot` (+ `free_attstatsslot` as
//! the returned slot's `Drop`), plus the `ExecHashBuildSkewHash` MCV probe
//! (`get_attstatsslot_mcv`).
//!
//! The catalog reads bottom out in unported neighbors: the `pg_statistic`
//! syscache tuple is read and searched through the `syscache` owner's seams,
//! and the `stavaluesN` / `stanumbersN` arrays are detoasted/deconstructed
//! through the `arrayfuncs` owner's seams. Each panics loudly until that owner
//! lands. The slot-matching loop and the assembly of the `AttStatsSlot` are
//! this unit's own logic.

use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid};
use types_datum::Datum;
// Canonical unified value (the Datum-unification keystone) for the value-form
// `get_attstatsslot_value_datums` path (by-reference stats elements by value).
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_error::PgResult;
use types_selfuncs::{
    AttStatsSlot, StatsTuple, ATTSTATSSLOT_NUMBERS, ATTSTATSSLOT_VALUES,
};

use backend_utils_adt_arrayfuncs_seams as arrayfuncs_seams;

/// `FLOAT4OID` (`pg_type.dat`) — the `float4` (`real`) type OID. The
/// `pg_statistic.stanumbers` array is always `float4[]`.
const FLOAT4OID: Oid = 700;
/// `pg_type` storage attributes of `float4` (`pg_type.dat`): 4-byte, pass-by-
/// value, int-aligned. `get_attstatsslot`'s NUMBERS slot decodes with these.
const FLOAT4_TYPLEN: i16 = 4;
const FLOAT4_TYPALIGN: u8 = b'i';
use backend_utils_cache_lsyscache_seams as own_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_cache_syscache_seams::STATISTIC_NUM_SLOTS;

/// `STATISTIC_KIND_MCV` (pg_statistic.h) — most-common-values slot kind.
/// C: `#define STATISTIC_KIND_MCV 1`.
const STATISTIC_KIND_MCV: i32 = 1;

/// `Anum_pg_statistic_stanumbers1` (pg_statistic generated `_d.h`) — the
/// attribute number (1-based) of the first `stanumbers` slot column. The
/// `pg_statistic` columns are, in order: `starelid`(1), `staattnum`(2),
/// `stainherit`(3), `stanullfrac`(4), `stawidth`(5), `stadistinct`(6),
/// `stakind1..5`(7..11), `staop1..5`(12..16), `stacoll1..5`(17..21),
/// `stanumbers1..5`(22..26), `stavalues1..5`(27..31).
const ANUM_PG_STATISTIC_STANUMBERS1: AttrNumber = 22;
/// `Anum_pg_statistic_stavalues1` (pg_statistic generated `_d.h`).
const ANUM_PG_STATISTIC_STAVALUES1: AttrNumber = 27;

/// `get_attstatsslot(&sslot, statstuple, reqkind, reqop, flags)` (lsyscache.c).
///
/// Extract the contents of a "slot" of a `pg_statistic` tuple. Returns
/// `Some(slot)` if the requested slot type was found, else `None` (C: returns
/// `false` and zeroes `*sslot`).
pub fn get_attstatsslot<'mcx>(
    mcx: Mcx<'mcx>,
    stats_tuple: StatsTuple,
    reqkind: i32,
    reqop: Oid,
    flags: i32,
) -> PgResult<Option<AttStatsSlot<'mcx>>> {
    // Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(statstuple);
    // The fixed-width slot metadata (stakindN / staopN / stacollN). Reading it
    // off the syscache tuple is the syscache owner's concern.
    let stats = syscache_seams::pg_statistic_slot_meta::call(stats_tuple)?;

    // for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
    //   if ((&stats->stakind1)[i] == reqkind &&
    //       (reqop == InvalidOid || (&stats->staop1)[i] == reqop)) break;
    let mut i = STATISTIC_NUM_SLOTS;
    for slot in 0..STATISTIC_NUM_SLOTS {
        if stats.stakind[slot] as i32 == reqkind
            && (reqop == InvalidOid || stats.staop[slot] == reqop)
        {
            i = slot;
            break;
        }
    }
    // if (i >= STATISTIC_NUM_SLOTS) return false;   /* not there */
    if i >= STATISTIC_NUM_SLOTS {
        return Ok(None);
    }

    // sslot->staop = (&stats->staop1)[i];
    // sslot->stacoll = (&stats->stacoll1)[i];
    let staop = stats.staop[i];
    let stacoll = stats.stacoll[i];

    let mut valuetype = InvalidOid;
    // C zeroes *sslot up front, so the un-requested arrays stay empty (NULL/0).
    let mut values: PgVec<'mcx, Datum> = PgVec::new_in(mcx);
    let mut numbers: PgVec<'mcx, f32> = PgVec::new_in(mcx);

    if flags & ATTSTATSSLOT_VALUES != 0 {
        // val = SysCacheGetAttrNotNull(STATRELATTINH, statstuple,
        //                              Anum_pg_statistic_stavalues1 + i);
        let val = syscache_seams::syscache_get_attr_not_null_statistic::call(
            stats_tuple,
            ANUM_PG_STATISTIC_STAVALUES1 + i as AttrNumber,
        )?;

        // statarray = DatumGetArrayTypePCopy(val);
        // arrayelemtype = ARR_ELEMTYPE(statarray);
        // sslot->valuetype = arrayelemtype;
        // The syscache projection yields the canonical unified `Datum<'mcx>`;
        // the stavalues column is a pass-by-reference `anyarray`, so `val` is a
        // `Datum::ByRef` carrying the on-disk array image bytes (NOT a pointer
        // word). Read the element type + deconstruct directly off those bytes
        // via the byte-based arrayfuncs seams (the bare-word `array_get_elemtype`
        // / `deconstruct_array` seams expect a pointer-word Datum and would
        // panic on a `ByRef`).
        let array_bytes = val.as_ref_bytes();
        let arrayelemtype = arrayfuncs_seams::array_get_elemtype_bytes::call(mcx, array_bytes)?;
        valuetype = arrayelemtype;

        // typeTuple = SearchSysCache1(TYPEOID, arrayelemtype); ... typeForm;
        // -> get_typlenbyvalalign wraps exactly this TYPEOID lookup. Cache
        //    lookup failure raises `cache lookup failed for type %u`.
        let type_form = own_seams::get_typlenbyvalalign::call(arrayelemtype)?;

        // deconstruct_array(statarray, arrayelemtype, typlen, typbyval,
        //                   typalign, &sslot->values, NULL, &sslot->nvalues);
        // NULLs not expected, so the per-element isnull is ignored.
        let elems = arrayfuncs_seams::deconstruct_array_bytes::call(
            mcx,
            array_bytes,
            arrayelemtype,
            type_form.typlen,
            type_form.typbyval,
            type_form.typalign as core::ffi::c_char,
        )?;
        let mut out: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, elems.len())?;
        for (datum, _isnull) in elems.iter().copied() {
            out.push(datum);
        }
        values = out;
        // The detoast-copy's pass-by-ref lifetime / pfree of the array object
        // (sslot->values_arr vs immediate pfree) is subsumed: deconstruct_array
        // returns owned copies in `mcx`, freed when `values` drops.
    }

    if flags & ATTSTATSSLOT_NUMBERS != 0 {
        // val = SysCacheGetAttrNotNull(STATRELATTINH, statstuple,
        //                              Anum_pg_statistic_stanumbers1 + i);
        let val = syscache_seams::syscache_get_attr_not_null_statistic::call(
            stats_tuple,
            ANUM_PG_STATISTIC_STANUMBERS1 + i as AttrNumber,
        )?;

        // statarray = DatumGetArrayTypePCopy(val);
        // verify 1-D float4 no-nulls array; sslot->numbers = ARR_DATA_PTR;
        // sslot->nnumbers = narrayelem.  The `stanumbers` attribute is always a
        // 1-D `float4[]` (pg_statistic.h). The syscache projection yields the
        // value as the canonical `Datum::ByRef` on-disk array byte image; decode
        // it directly via the bytes-form `deconstruct_array_values_bytes` (which,
        // unlike the pointer-word `array_get_float4_values`, accepts the owned
        // byte image rather than a C pointer word). Each element is a `float4`
        // by-value `Datum`. The mcx copies subsume `free_attstatsslot`'s pfree.
        let elems = arrayfuncs_seams::deconstruct_array_values_bytes::call(
            mcx,
            val.as_ref_bytes(),
            FLOAT4OID,
            FLOAT4_TYPLEN,
            true,
            FLOAT4_TYPALIGN as core::ffi::c_char,
        )?;
        let mut out: PgVec<'mcx, f32> = mcx::vec_with_capacity_in(mcx, elems.len())?;
        for (datum, _isnull) in elems.iter() {
            out.push(datum.as_f32());
        }
        numbers = out;
    }

    Ok(Some(AttStatsSlot {
        staop,
        stacoll,
        valuetype,
        values,
        numbers,
    }))
}

/// `get_attstatsslot(&sslot, statstuple, reqkind, reqop, ATTSTATSSLOT_VALUES)`
/// yielding the matched slot's value array as canonical value-carrying
/// [`DatumV`]s.
///
/// Identical slot-matching to [`get_attstatsslot`], but the `stavaluesN` array
/// is deconstructed via the value-form `deconstruct_array_values_bytes` (each
/// by-reference element captured by value as `Datum::ByRef`) instead of the
/// bare-word `deconstruct_array_bytes` whose by-reference element is a
/// non-dereferenceable in-buffer offset. Consumers that must decode a
/// by-reference element type (the inet/cidr selectivity estimators) read here
/// until the shared `AttStatsSlot.values` field re-type campaign lands.
pub fn get_attstatsslot_value_datums<'mcx>(
    mcx: Mcx<'mcx>,
    stats_tuple: StatsTuple,
    reqkind: i32,
    reqop: Oid,
) -> PgResult<Option<PgVec<'mcx, DatumV<'mcx>>>> {
    // for (i = 0; i < STATISTIC_NUM_SLOTS; i++) match (stakind, staop) — the
    // same slot-selection get_attstatsslot performs.
    let stats = syscache_seams::pg_statistic_slot_meta::call(stats_tuple)?;
    let mut i = STATISTIC_NUM_SLOTS;
    for slot in 0..STATISTIC_NUM_SLOTS {
        if stats.stakind[slot] as i32 == reqkind
            && (reqop == InvalidOid || stats.staop[slot] == reqop)
        {
            i = slot;
            break;
        }
    }
    if i >= STATISTIC_NUM_SLOTS {
        return Ok(None);
    }

    // val = SysCacheGetAttrNotNull(STATRELATTINH, statstuple,
    //                              Anum_pg_statistic_stavalues1 + i);
    let val = syscache_seams::syscache_get_attr_not_null_statistic::call(
        stats_tuple,
        ANUM_PG_STATISTIC_STAVALUES1 + i as AttrNumber,
    )?;

    // arrayelemtype = ARR_ELEMTYPE(DatumGetArrayTypeP(val)); deconstruct with
    // the element type's storage attributes — but yield canonical values.
    let array_bytes = val.as_ref_bytes();
    let arrayelemtype = arrayfuncs_seams::array_get_elemtype_bytes::call(mcx, array_bytes)?;
    let type_form = own_seams::get_typlenbyvalalign::call(arrayelemtype)?;

    let elems = arrayfuncs_seams::deconstruct_array_values_bytes::call(
        mcx,
        array_bytes,
        arrayelemtype,
        type_form.typlen,
        type_form.typbyval,
        type_form.typalign as core::ffi::c_char,
    )?;
    let mut out: PgVec<'mcx, DatumV<'mcx>> = mcx::vec_with_capacity_in(mcx, elems.len())?;
    // NULLs not expected in a stavalues array (C ignores the per-element isnull).
    for (datum, _isnull) in elems.iter() {
        out.push(datum.clone_in(mcx)?);
    }
    Ok(Some(out))
}

/// `SearchSysCache3(STATRELATTINH, ...)` + MCV-slot probe
/// (`get_attstatsslot_mcv`) used by `ExecHashBuildSkewHash`.
///
/// Mirrors the head of `ExecHashBuildSkewHash`: look up the `pg_statistic` row
/// for `(relid, attnum, inherit)`, then extract the MCV slot's `values` /
/// `numbers`. Returns `None` when there is no `pg_statistic` row
/// (`!HeapTupleIsValid`) or no MCV slot. The syscache pin is released here.
pub fn get_attstatsslot_mcv<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    inherit: bool,
) -> PgResult<Option<(PgVec<'mcx, Datum>, PgVec<'mcx, f32>)>> {
    // statsTuple = SearchSysCache3(STATRELATTINH, relid, attnum, inherit);
    // if (!HeapTupleIsValid(statsTuple)) return;
    let stats_tuple =
        match syscache_seams::search_statrelattinh::call(mcx, relid, attnum, inherit)? {
            Some(t) => t,
            None => return Ok(None),
        };

    // get_attstatsslot(&sslot, statsTuple, STATISTIC_KIND_MCV, InvalidOid,
    //                  ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS)
    let slot = get_attstatsslot(
        mcx,
        stats_tuple,
        STATISTIC_KIND_MCV,
        InvalidOid,
        ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
    );

    // Always release the syscache pin (C: ReleaseSysCache(statsTuple)),
    // including on the error path, then propagate.
    let slot = match slot {
        Ok(s) => s,
        Err(e) => {
            // Best-effort release before surfacing the error (the C code's
            // ReleaseSysCache runs once it owns statsTuple; here a get_attstatsslot
            // failure is an ereport(ERROR) that longjmps past it in C, so the
            // pin is reclaimed at subxact abort — but in the owned model we
            // release explicitly to avoid leaking the pin).
            syscache_seams::release_stats_tuple::call(stats_tuple);
            return Err(e);
        }
    };

    let result = slot.map(|s| (s.values, s.numbers));

    syscache_seams::release_stats_tuple::call(stats_tuple);

    Ok(result)
}

/// `get_attavgwidth(relid, attnum)` (lsyscache.c): the average stored width of
/// a column from `pg_statistic`, or `0` if no data.
///
/// ```c
/// if (get_attavgwidth_hook) {
///     stawidth = (*get_attavgwidth_hook)(relid, attnum);
///     if (stawidth > 0) return stawidth;
/// }
/// tp = SearchSysCache3(STATRELATTINH, relid, attnum, BoolGetDatum(false));
/// if (HeapTupleIsValid(tp)) {
///     stawidth = ((Form_pg_statistic) GETSTRUCT(tp))->stawidth;
///     ReleaseSysCache(tp);
///     if (stawidth > 0) return stawidth;
/// }
/// return 0;
/// ```
///
/// The `get_attavgwidth_hook` planner hook is never installed in this port, so
/// (as in C with a NULL hook) only the catalog path runs.
pub fn get_attavgwidth(relid: Oid, attnum: AttrNumber) -> PgResult<i32> {
    if let Some(stawidth) = syscache_seams::pg_statistic_stawidth::call(relid, attnum)? {
        if stawidth > 0 {
            return Ok(stawidth);
        }
    }
    Ok(0)
}

/// `free_attstatsslot(sslot)` (lsyscache.c): release a slot obtained from
/// [`get_attstatsslot`].
///
/// ```c
/// void free_attstatsslot(AttStatsSlot *sslot) {
///     if (sslot->values_arr) pfree(sslot->values_arr);
///     if (sslot->numbers_arr) pfree(sslot->numbers_arr);
/// }
/// ```
///
/// In the owned model the slot's `values` / `numbers` `PgVec`s are dropped
/// (and their `mcx` storage reclaimed) when the slot is dropped; this entry
/// point consumes the slot to make that release explicit, mirroring the C
/// `pfree`s of `values_arr` / `numbers_arr`.
pub fn free_attstatsslot(sslot: AttStatsSlot<'_>) {
    drop(sslot);
}
