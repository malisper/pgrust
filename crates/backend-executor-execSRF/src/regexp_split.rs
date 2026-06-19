//! `regexp_split_to_table(text, text [, text])` (OIDs 2765/2766) registered as an
//! executor-frame set-returning function.
//!
//! `regexp.c`'s `regexp_split_to_table` is a value-per-call SRF: it carries a
//! `regexp_matches_ctx` across SRF calls and emits one split substring per call
//! via `build_regexp_split_result`, terminating with `SRF_RETURN_DONE` when the
//! whole string has been consumed. The matching + split core (the `setup_regexp_matches`
//! glob scan and the `build_regexp_split_result` substring extraction) is ported
//! in `backend-utils-adt-regexp::{regexp_split_to_table,build_regexp_split_result}`.
//!
//! Here that core is assembled into a [`types_nodes::execexpr::PGFunction`] (the
//! executor-frame ABI whose call frame carries the LIVE `ReturnSetInfo`) and
//! registered in this unit's executor-frame SRF table from
//! [`register_regexp_split`] (called by `init_seams`) — the executor-frame
//! `fmgrtab.c` analogue for these SRFs, exactly as `generate_series`/`unnest`
//! are. It bypasses the by-OID builtin registry (whose `types_fmgr::PGFunction`
//! frame's `resultinfo` is tag-only — the WONTFIX dual-home) so the function
//! reads/writes a live `ReturnSetInfo`. `SELECT regexp_split_to_table('a,b,c',
//! ',')` reaches this via nodeProjectSet → ExecMakeFunctionResultSet; `SELECT *
//! FROM regexp_split_to_table(...)` via nodeFunctionscan →
//! ExecMakeTableFunctionResult.
//!
//! The owned model runs the whole glob match + split once on the first call
//! (mirroring C's per-call ordering — one substring between successive matches,
//! plus the trailing tail), then emits one `text` row per call.

use core::any::Any;

use mcx::{Mcx, PgBox};
use types_core::Oid;
use types_nodes::execexpr::ExprDoneCond;
use types_nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_fmgr_funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};

use crate::register_srf;

/// `regexp_split_to_table(text, text, text)` (OID 2765) and the no-flags
/// `regexp_split_to_table(text, text)` (OID 2766) share this value core.
const REGEXP_SPLIT_TO_TABLE: Oid = 2765;
const REGEXP_SPLIT_TO_TABLE_NO_FLAGS: Oid = 2766;

/// Register `regexp_split_to_table` in the executor-frame SRF table.
pub(crate) fn register_regexp_split() {
    register_srf(REGEXP_SPLIT_TO_TABLE, regexp_split_to_table);
    register_srf(REGEXP_SPLIT_TO_TABLE_NO_FLAGS, regexp_split_to_table);
}

/// The materialized cross-call state for `regexp_split_to_table` (C:
/// `regexp_matches_ctx` + `funcctx->call_cntr`/`max_calls`). The whole split
/// sequence is produced once on the first call (storage order preserved) and
/// emitted one text row per call. Each row is the raw text payload (no varlena
/// header), wrapped into a `text` Datum per call.
struct SplitFctx {
    /// The split substrings (text payload bytes) in order.
    rows: Vec<Vec<u8>>,
    /// The next row index to emit (C: `funcctx->call_cntr`).
    next: usize,
}

/// Erase a `'static` cross-call state value into the `FuncCallContext.user_fctx`
/// carrier (C: `funcctx->user_fctx = palloc(...)`).
fn erase_user_fctx<'mcx, T: Any>(mcx: Mcx<'mcx>, v: T) -> PgBox<'mcx, dyn Any> {
    let boxed = mcx::alloc_in(mcx, v).expect("alloc user_fctx");
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn Any` vtable.
    unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) }
}

/// Read a by-reference `text` argument `index` as its VARDATA payload bytes
/// (C: `PG_GETARG_TEXT_PP` → `VARDATA_ANY`). The executor frame carries the
/// header-ful varlena image on the by-ref side channel; skip the 4-byte length
/// word to reach the payload.
fn arg_text_payload(fcinfo: &FunctionCallInfoBaseData<'_>, index: usize) -> Vec<u8> {
    let image = match fcinfo.ref_arg(index) {
        Some(FmgrArgRef::Varlena(b)) => b.as_slice(),
        _ => panic!("regexp_split_to_table: text arg {index} missing from by-ref lane"),
    };
    let payload = if image.len() >= 4 { &image[4..] } else { image };
    payload.to_vec()
}

/// `regexp_split_to_table(PG_FUNCTION_ARGS)` (regexp.c) over the executor frame.
/// Drives the value-per-call protocol; `SRF_RETURN_NEXT` / `SRF_RETURN_DONE` are
/// the `isDone` writes + the multi-call teardown.
fn regexp_split_to_table<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("regexp_split_to_table: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { funcctx = SRF_FIRSTCALL_INIT(); ... }
    if fcinfo.fn_extra.is_none() {
        // C: orig_str = PG_GETARG_TEXT_PP(0); pattern = PG_GETARG_TEXT_PP(1);
        //    flags = (PG_NARGS() > 2) ? PG_GETARG_TEXT_PP(2) : NULL;
        //    collation = PG_GET_COLLATION();
        // Each text arrives header-ful on the by-ref side channel. Materialize
        // the whole split sequence once (setup_regexp_matches glob scan + the
        // per-call build_regexp_split_result), copying each row into a
        // lifetime-free `Vec<u8>` so the cross-call state can live behind the
        // `dyn Any` user_fctx carrier across the row series. The immutable borrow
        // of `fcinfo` must end before the mutable SRF setup calls, so it is scoped.
        let rows: Vec<Vec<u8>> = {
            let orig_str = arg_text_payload(fcinfo, 0);
            let pattern = arg_text_payload(fcinfo, 1);
            let flags = if fcinfo.nargs > 2 {
                Some(arg_text_payload(fcinfo, 2))
            } else {
                None
            };
            let collation = fcinfo.fncollation;

            let materialized = backend_utils_adt_regexp::regexp_split_to_table(
                mcx,
                &orig_str,
                &pattern,
                flags.as_deref(),
                collation,
            )
            .unwrap_or_else(|e| std::panic::panic_any(e));

            materialized.iter().map(|r| r.as_slice().to_vec()).collect()
        };

        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(mcx, SplitFctx { rows, next: 0 });
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    // C: funcctx = SRF_PERCALL_SETUP(); fctx = funcctx->user_fctx;
    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let state: &mut SplitFctx = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<SplitFctx>()
        .expect("user_fctx is SplitFctx");

    // C: if (splitctx->next_match <= splitctx->nmatches) { ... SRF_RETURN_NEXT ... }
    if state.next < state.rows.len() {
        // SRF_RETURN_NEXT(funcctx, PointerGetDatum(cstring_to_text(...))).
        let datum = backend_utils_adt_varlena_seams::bytes_to_varlena_v::call(
            mcx,
            &state.rows[state.next],
        )
        .unwrap_or_else(|e| std::panic::panic_any(e));
        state.next += 1;
        funcctx.call_cntr += 1;
        set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
        fcinfo.isnull = false;
        datum
    } else {
        // SRF_RETURN_DONE(funcctx).
        end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
        set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
        fcinfo.isnull = true;
        Datum::null()
    }
}

/// `rsi->isDone = cond` (the `SRF_RETURN_NEXT`/`SRF_RETURN_DONE` write onto the
/// live `ReturnSetInfo` the executor frame carries).
fn set_isdone(fcinfo: &mut FunctionCallInfoBaseData<'_>, cond: ExprDoneCond) {
    fcinfo
        .resultinfo
        .as_mut()
        .expect("resultinfo present for an SRF call")
        .isDone = cond;
}
