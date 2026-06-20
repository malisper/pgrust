//! `string_to_table(text, text [, text])` (OIDs 6160/6161) registered as
//! executor-frame set-returning functions.
//!
//! `varlena.c`'s `text_to_table` / `text_to_table_null` are SRFs emitting one
//! `text` row per field of the split-up input string (split on the separator,
//! with the optional 3rd argument mapping a matching field to SQL NULL). The
//! split value core (`split_text`, returning the ordered fields with their
//! per-field NULL flags) is ported in
//! `backend-utils-adt-varlena::split_format`.
//!
//! Here that core is driven over the executor frame: the whole field sequence is
//! produced once on the first call, and one `text` (or NULL) row is emitted per
//! call. Registered from [`register_string_to_table`] (called by `init_seams`) —
//! the executor-frame `fmgrtab.c` analogue, exactly as `regexp_split_to_table`
//! is. `SELECT string_to_table('a,b,c', ',')` reaches this via nodeProjectSet;
//! `SELECT * FROM string_to_table(...)` via nodeFunctionscan.

use core::any::Any;

use mcx::{Mcx, PgBox};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::execexpr::ExprDoneCond;
use types_nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_fmgr_funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};

use crate::register_srf;

/// `string_to_table(text, text)` (OID 6160, `text_to_table`) and
/// `string_to_table(text, text, text)` (OID 6161, `text_to_table_null`) share
/// this value core; the 3-arg form supplies the null-string.
const STRING_TO_TABLE: Oid = 6160;
const STRING_TO_TABLE_NULL: Oid = 6161;

/// Register `string_to_table` in the executor-frame SRF table.
pub(crate) fn register_string_to_table() {
    register_srf(STRING_TO_TABLE, string_to_table);
    register_srf(STRING_TO_TABLE_NULL, string_to_table);
}

/// One materialized split field: the `text` payload bytes and whether it maps to
/// SQL NULL (matched the null-string). Stored lifetime-free behind the `dyn Any`
/// `user_fctx` carrier.
struct Field {
    bytes: Vec<u8>,
    is_null: bool,
}

/// The materialized cross-call state for `string_to_table` (C: the `split_text`
/// field sequence + `funcctx->call_cntr`/`max_calls`).
struct TableFctx {
    fields: Vec<Field>,
    /// The next field index to emit (C: `funcctx->call_cntr`).
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
/// (C: `PG_GETARG_TEXT_PP` → `VARDATA_ANY`), or `None` when the argument cell is
/// SQL NULL. `string_to_table` is NOT strict: a NULL separator (each char a
/// field) and a NULL null-string are meaningful.
fn arg_text_payload_opt(
    fcinfo: &FunctionCallInfoBaseData<'_>,
    index: usize,
) -> Option<Vec<u8>> {
    if index >= fcinfo.args.len() || fcinfo.args[index].isnull {
        return None;
    }
    let image = match fcinfo.ref_arg(index) {
        Some(FmgrArgRef::Varlena(b)) => b.as_slice(),
        _ => return None,
    };
    let payload = if image.len() >= 4 { &image[4..] } else { image };
    Some(payload.to_vec())
}

/// `text_to_table(PG_FUNCTION_ARGS)` (varlena.c:4808) over the executor frame.
/// Drives the value-per-call protocol; `SRF_RETURN_NEXT` / `SRF_RETURN_DONE` are
/// the `isDone` writes + the multi-call teardown.
fn string_to_table<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("string_to_table: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { funcctx = SRF_FIRSTCALL_INIT(); ... }
    if fcinfo.fn_extra.is_none() {
        // C: split_text(fcinfo, ...) — split the input on fldsep, applying the
        // optional null-string. The whole field sequence is materialized once and
        // emitted one row per call. The immutable borrow of `fcinfo` ends before
        // the mutable SRF setup calls, so it is scoped.
        let fields: Vec<Field> = {
            let input = arg_text_payload_opt(fcinfo, 0);
            let fldsep = arg_text_payload_opt(fcinfo, 1);
            let null_string = if fcinfo.nargs > 2 {
                arg_text_payload_opt(fcinfo, 2)
            } else {
                None
            };
            let collation = fcinfo.fncollation;

            // C: a NULL input string produces no rows (split_text returns None).
            match backend_utils_adt_varlena::split_format::split_text(
                mcx,
                input.as_deref(),
                fldsep.as_deref(),
                null_string.as_deref(),
                collation,
            )? {
                None => Vec::new(),
                Some(split) => split
                    .iter()
                    .map(|f| Field {
                        bytes: f.bytes.as_slice().to_vec(),
                        is_null: f.is_null,
                    })
                    .collect(),
            }
        };

        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(mcx, TableFctx { fields, next: 0 });
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    // C: funcctx = SRF_PERCALL_SETUP(); fctx = funcctx->user_fctx;
    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let state: &mut TableFctx = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<TableFctx>()
        .expect("user_fctx is TableFctx");

    if state.next < state.fields.len() {
        // Read the field out of the cross-call state (ending the funcctx borrow
        // before the `fcinfo` writes below).
        let (is_null, datum) = {
            let field = &state.fields[state.next];
            if field.is_null {
                (true, Datum::null())
            } else {
                let datum = backend_utils_adt_varlena_seams::bytes_to_varlena_v::call(
                    mcx,
                    &field.bytes,
                )?;
                (false, datum)
            }
        };
        state.next += 1;
        funcctx.call_cntr += 1;
        set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
        fcinfo.isnull = is_null;
        Ok(datum)
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
