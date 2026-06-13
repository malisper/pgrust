//! `statistics` family — `lsyscache.c` lookups keyed on `pg_statistic`
//! (`STATRELATTINH` syscache and the `get_attstatsslot` slot-extraction
//! machinery).
//!
//! SCAFFOLD STAGE: signatures mirror the seam declarations; bodies are
//! `todo!()` until the SearchSysCache + array-deconstruct logic lands.
//!
//! C entry points covered here: `get_attstatsslot` (+ `free_attstatsslot` as
//! the returned slot's `Drop`), plus the `ExecHashBuildSkewHash` MCV probe
//! (`get_attstatsslot_mcv`).

use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, Oid};
use types_datum::Datum;
use types_error::PgResult;
use types_selfuncs::{AttStatsSlot, StatsTuple};

/// `get_attstatsslot(&sslot, statstuple, reqkind, reqop, flags)` (lsyscache.c).
pub fn get_attstatsslot<'mcx>(
    _mcx: Mcx<'mcx>,
    _stats_tuple: StatsTuple,
    _reqkind: i32,
    _reqop: Oid,
    _flags: i32,
) -> PgResult<Option<AttStatsSlot<'mcx>>> {
    todo!("get_attstatsslot: find matching pg_statistic slot, deconstruct values/numbers into mcx")
}

/// `SearchSysCache3(STATRELATTINH, ...)` + MCV-slot probe
/// (`get_attstatsslot_mcv`) used by `ExecHashBuildSkewHash`.
pub fn get_attstatsslot_mcv<'mcx>(
    _mcx: Mcx<'mcx>,
    _relid: Oid,
    _attnum: AttrNumber,
    _inherit: bool,
) -> PgResult<Option<(PgVec<'mcx, Datum>, PgVec<'mcx, f32>)>> {
    todo!("get_attstatsslot_mcv: STATRELATTINH row -> MCV slot (values, numbers)")
}
