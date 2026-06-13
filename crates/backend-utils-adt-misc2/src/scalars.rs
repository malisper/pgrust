//! Family `scalars` — `src/backend/utils/adt/tid.c` + `windowfuncs.c`.
//!
//! Two cohesive scalar/SRF-helper clusters grouped together: the `tid` type
//! (ItemPointer) I/O and operators — `tidin` / `tidout` / `tidrecv` /
//! `tidsend`, the comparison/equality family (`tideq` … `bttidcmp`,
//! `tidlarger` / `tidsmaller`), `hashtid` / `hashtidextended`, and the
//! `currtid_*` lookups — and the SQL window support functions in
//! windowfuncs.c (rank / dense_rank / percent_rank / cume_dist / ntile /
//! row_number, the lead/lag/first/last/nth_value `leadlag_common` family, and
//! the matching `*_support` planner-support functions).
//!
//! TID ops are pure scalar transforms (no Mcx beyond text formatting); the
//! window functions drive the executor's WindowObject through the adt-infra
//! SRF/window boundary (seamed to its real owner). Values cross as `Datum`.
//! Independent of the keystone.

use mcx::Mcx;
use types_datum::Datum;
use types_error::PgResult;

// --- tid.c ---

/// `tidin(str)`.
pub fn tidin<'mcx>(_mcx: Mcx<'mcx>, _string: Option<&str>) -> PgResult<Datum> {
    todo!("tidin")
}

/// `tidout(itemPtr)`.
pub fn tidout<'mcx>(_mcx: Mcx<'mcx>, _item_ptr: Datum) -> PgResult<Datum> {
    todo!("tidout")
}

/// `tidrecv(buf)`.
pub fn tidrecv<'mcx>(_mcx: Mcx<'mcx>, _buf: &[u8]) -> PgResult<Datum> {
    todo!("tidrecv")
}

/// `tidsend(itemPtr)`.
pub fn tidsend<'mcx>(_mcx: Mcx<'mcx>, _item_ptr: Datum) -> PgResult<Datum> {
    todo!("tidsend")
}

/// `bttidcmp(arg1, arg2)` — three-way TID comparison shared by the operators.
pub fn bttidcmp(_arg1: Datum, _arg2: Datum) -> PgResult<i32> {
    todo!("bttidcmp")
}

/// `hashtid(key)`.
pub fn hashtid(_key: Datum) -> PgResult<u32> {
    todo!("hashtid")
}

/// `hashtidextended(key, seed)`.
pub fn hashtidextended(_key: Datum, _seed: u64) -> PgResult<u64> {
    todo!("hashtidextended")
}

/// `currtid_byrelname(relname, tid)`.
pub fn currtid_byrelname<'mcx>(
    _mcx: Mcx<'mcx>,
    _relname: &str,
    _tid: Datum,
) -> PgResult<Datum> {
    todo!("currtid_byrelname")
}

// --- windowfuncs.c ---

/// `window_row_number(fcinfo)`.
pub fn window_row_number<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("window_row_number")
}

/// `window_rank(fcinfo)`.
pub fn window_rank<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("window_rank")
}

/// `window_dense_rank(fcinfo)`.
pub fn window_dense_rank<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("window_dense_rank")
}

/// `window_percent_rank(fcinfo)`.
pub fn window_percent_rank<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("window_percent_rank")
}

/// `window_cume_dist(fcinfo)`.
pub fn window_cume_dist<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("window_cume_dist")
}

/// `window_ntile(fcinfo)`.
pub fn window_ntile<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("window_ntile")
}

/// `leadlag_common(...)` — shared engine for lead/lag with/without
/// offset/default.
pub fn leadlag_common<'mcx>(
    _mcx: Mcx<'mcx>,
    _forward: bool,
    _with_offset: bool,
    _with_default: bool,
) -> PgResult<Datum> {
    todo!("leadlag_common")
}

/// `window_first_value(fcinfo)` / `window_last_value` / `window_nth_value`.
pub fn window_first_value<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("window_first_value")
}

/// `window_last_value(fcinfo)`.
pub fn window_last_value<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("window_last_value")
}

/// `window_nth_value(fcinfo)`.
pub fn window_nth_value<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("window_nth_value")
}
