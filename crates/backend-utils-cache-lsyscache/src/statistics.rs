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
use types_error::PgResult;
use types_selfuncs::{
    AttStatsSlot, StatsTuple, ATTSTATSSLOT_NUMBERS, ATTSTATSSLOT_VALUES,
};

use backend_utils_adt_arrayfuncs_seams as arrayfuncs_seams;
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
        let arrayelemtype = arrayfuncs_seams::array_get_elemtype::call(mcx, val)?;
        valuetype = arrayelemtype;

        // typeTuple = SearchSysCache1(TYPEOID, arrayelemtype); ... typeForm;
        // -> get_typlenbyvalalign wraps exactly this TYPEOID lookup. Cache
        //    lookup failure raises `cache lookup failed for type %u`.
        let type_form = own_seams::get_typlenbyvalalign::call(arrayelemtype)?;

        // deconstruct_array(statarray, arrayelemtype, typlen, typbyval,
        //                   typalign, &sslot->values, NULL, &sslot->nvalues);
        // NULLs not expected, so the per-element isnull is ignored.
        let elems = arrayfuncs_seams::deconstruct_array::call(
            mcx,
            val,
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
        // sslot->nnumbers = narrayelem;  (the validation + extraction is the
        // arrayfuncs owner's concern). In C `numbers` points into the detoasted
        // array kept in numbers_arr; the owned model holds an mcx copy whose
        // Drop subsumes free_attstatsslot's pfree.
        numbers = arrayfuncs_seams::array_get_float4_values::call(mcx, val)?;
    }

    Ok(Some(AttStatsSlot {
        staop,
        stacoll,
        valuetype,
        values,
        numbers,
    }))
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
