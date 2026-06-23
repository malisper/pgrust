//! `regexp_matches(text, text [, text])` (OIDs 2763/2764) registered as
//! executor-frame set-returning functions.
//!
//! `regexp.c`'s `regexp_matches` is a value-per-call SRF that emits one
//! `text[]` row per match (the capturing-subpattern slices for that match, or
//! the whole-match slice when there are no subpatterns). Its glob match +
//! per-row build core (`setup_regexp_matches` + `build_regexp_match_result`) is
//! ported in `backend-utils-adt-regexp`, exposed as `regexp_matches` which
//! materializes the whole sequence of rows (each row a `Vec<Option<payload>>`).
//!
//! Here that core is driven over the executor frame: the whole row sequence is
//! materialized once on the first call (mirroring C's per-call ordering), and one
//! `text[]` array Datum is emitted per call. Registered from
//! [`register_regexp_matches`] (called by `init_seams`) — the executor-frame
//! `fmgrtab.c` analogue, exactly as `regexp_split_to_table` is. `SELECT
//! regexp_matches('abc','(\w)','g')` reaches this via nodeProjectSet; `SELECT *
//! FROM regexp_matches(...)` via nodeFunctionscan.

use core::any::Any;

use ::mcx::{Mcx, PgBox};
use ::types_core::Oid;
use ::nodes::execexpr::ExprDoneCond;
use ::types_error::PgResult;
use ::nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};

use crate::register_srf;

/// `regexp_matches(text, text, text)` (OID 2764) and the no-flags
/// `regexp_matches(text, text)` (OID 2763) share this value core.
const REGEXP_MATCHES: Oid = 2764;
const REGEXP_MATCHES_NO_FLAGS: Oid = 2763;

/// Register `regexp_matches` in the executor-frame SRF table.
pub(crate) fn register_regexp_matches() {
    register_srf(REGEXP_MATCHES, regexp_matches);
    register_srf(REGEXP_MATCHES_NO_FLAGS, regexp_matches);
}

/// One materialized `regexp_matches` row: the per-element `text` payloads (a
/// `None` element is a SQL-NULL array element — an unmatched subpattern). Stored
/// lifetime-free behind the `dyn Any` `user_fctx` carrier; each row is rebuilt
/// into a `text[]` array Datum per call.
type MatchRow = Vec<Option<Vec<u8>>>;

/// The materialized cross-call state for `regexp_matches` (C:
/// `regexp_matches_ctx` + `funcctx->call_cntr`/`max_calls`). The whole match
/// sequence is produced once on the first call and emitted one `text[]` row per
/// call.
struct MatchesFctx {
    /// The match rows in order.
    rows: Vec<MatchRow>,
    /// The next row index to emit (C: `funcctx->call_cntr`).
    next: usize,
}

/// Erase a `'static` cross-call state value into the `FuncCallContext.user_fctx`
/// carrier (C: `funcctx->user_fctx = palloc(...)`).
fn erase_user_fctx<'mcx, T: Any>(mcx: Mcx<'mcx>, v: T) -> PgBox<'mcx, dyn Any> {
    let boxed = ::mcx::alloc_in(mcx, v).expect("alloc user_fctx");
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
        _ => panic!("regexp_matches: text arg {index} missing from by-ref lane"),
    };
    // `VARDATA_ANY`: skip ONE header byte for a short (1-byte, low-bit-set)
    // header, else `VARHDRSZ`. A small stored text reaches an fmgr arg verbatim
    // once `SHORT_VARLENA_PACKING` is on; a fixed 4-byte strip would drop three
    // payload bytes. No-op while the flag is off (every value is 4-byte).
    let payload: &[u8] = match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= 4 => &image[4..],
        _ => &[],
    };
    payload.to_vec()
}

/// `regexp_matches(PG_FUNCTION_ARGS)` (regexp.c) over the executor frame. Drives
/// the value-per-call protocol; `SRF_RETURN_NEXT` / `SRF_RETURN_DONE` are the
/// `isDone` writes + the multi-call teardown.
fn regexp_matches<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("regexp_matches: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { funcctx = SRF_FIRSTCALL_INIT(); ... }
    if fcinfo.fn_extra.is_none() {
        // Materialize the whole match sequence once (setup_regexp_matches glob
        // scan + the per-call build_regexp_match_result), copying each row into a
        // lifetime-free shape so the cross-call state lives behind the `dyn Any`
        // user_fctx carrier. The immutable borrow of `fcinfo` ends before the
        // mutable SRF setup calls, so it is scoped.
        let rows: Vec<MatchRow> = {
            let orig_str = arg_text_payload(fcinfo, 0);
            let pattern = arg_text_payload(fcinfo, 1);
            let flags = if fcinfo.nargs > 2 {
                Some(arg_text_payload(fcinfo, 2))
            } else {
                None
            };
            let collation = fcinfo.fncollation;

            let materialized = regexp::regexp_matches(
                mcx,
                &orig_str,
                &pattern,
                flags.as_deref(),
                collation,
            )?;

            materialized
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|elem| elem.as_ref().map(|p| p.as_slice().to_vec()))
                        .collect::<MatchRow>()
                })
                .collect()
        };

        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(mcx, MatchesFctx { rows, next: 0 });
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    // C: funcctx = SRF_PERCALL_SETUP(); fctx = funcctx->user_fctx;
    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let state: &mut MatchesFctx = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<MatchesFctx>()
        .expect("user_fctx is MatchesFctx");

    // C: if (matchctx->next_match < matchctx->nmatches) { SRF_RETURN_NEXT(...) }
    if state.next < state.rows.len() {
        // SRF_RETURN_NEXT(funcctx, PointerGetDatum(build_regexp_match_result));
        // Build the text[] array image (header-ful, per-element NULLs preserved)
        // and cross it on the by-ref lane, exactly as the scalar regexp_match
        // builtin returns its text[].
        let views: Vec<Option<&[u8]>> = state.rows[state.next]
            .iter()
            .map(|e| e.as_ref().map(|p| p.as_slice()))
            .collect();
        let image =
            arrayfuncs::construct::build_text_array_nullable(mcx, &views)?;
        let mut buf = ::mcx::PgVec::new_in(mcx);
        buf.try_reserve(image.len())
            .map_err(|_| mcx.oom(image.len()))?;
        buf.extend_from_slice(image.as_slice());

        state.next += 1;
        funcctx.call_cntr += 1;
        set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
        fcinfo.isnull = false;
        Ok(Datum::ByRef(buf))
    } else {
        // SRF_RETURN_DONE(funcctx).
        end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
        set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
        fcinfo.isnull = true;
        Ok(Datum::null())
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
