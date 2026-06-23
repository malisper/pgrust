//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `rls.c` (the two `row_security_active` overloads) and
//! `pg_controldata.c` (the four `pg_control_*` composite-returning functions).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate::rls`] /
//! [`crate::pg_controldata`], and writes back the result word (or, for the
//! `pg_control_*` composite rows, the formed-tuple image on the by-reference
//! `Composite` lane). [`register_backend_utils_misc_more_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`.

use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use ::types_core::Oid;
use ::types_error::PgResult;
// The composite-Datum builder cores produce `::types_tuple::Datum` (the `'mcx`
// column-value carrier), distinct from the bare fmgr ABI word `Datum`.
use ::types_tuple::Datum as DatumV;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("rls fn: missing arg")
        .value
        .as_oid()
}

/// `PG_GETARG_TEXT_PP(i)` → `text_to_cstring`: a `text` arg's detoasted
/// `VARDATA_ANY` payload bytes on the by-ref lane, decoded as UTF-8. C builds a
/// NUL-terminated `char *` from the text body; the by-ref lane already delivers
/// the detoasted payload, so the body bytes are decoded directly.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("rls fn: text arg missing from by-ref lane");
    let bytes = vardata_any(image);
    core::str::from_utf8(bytes).expect("rls fn: text arg not valid UTF-8")
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image:
/// skip ONE header byte for a short (1-byte, low-bit-set) header, else `VARHDRSZ`
/// (4). A small stored value arrives short-headed once `SHORT_VARLENA_PACKING` is
/// on; a fixed 4-byte strip would drop three payload bytes. No-op while off.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= 4 => &image[4..],
        _ => &[],
    }
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// A scratch context for the cores' transient allocations (C charges them to
/// `CurrentMemoryContext`). The `Mcx`-free established pattern: a per-call
/// working context.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("rls fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `row_security_active(oid)` (pg_proc oid 3298).
fn fc_row_security_active(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let table_oid = arg_oid(fcinfo, 0);
    let ctx = scratch_mcx();
    let b = crate::rls::row_security_active(ctx.mcx(), table_oid)?;
    Ok(ret_bool(b))
}

/// `row_security_active_name(text)` (pg_proc oid 3299).
fn fc_row_security_active_name(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let name = arg_text(fcinfo, 0);
    let ctx = scratch_mcx();
    let b = crate::rls::row_security_active_name(ctx.mcx(), name)?;
    Ok(ret_bool(b))
}

// ===========================================================================
// pg_controldata.c — pg_control_* composite-returning functions.
//
// These are reached in target-list (scalar) position through the by-OID fmgr
// builtin registry (this layer); the canonical FROM-clause form
// (`SELECT * FROM pg_control_system()`) is dispatched through the executor-frame
// SRF table (backend-executor-execSRF's `control_srf`), which calls the same
// `pg_controldata::pg_control_*_datum` composite builders. Both home the result
// as a composite `Datum` — here onto the fmgr frame's by-reference `Composite`
// lane (read back as a `Datum::Composite` row by the dispatch result mapper).
// ===========================================================================

/// Carry a composite-record `Datum` (built by a `pg_control_*_datum` core) onto
/// the fmgr frame's by-reference `Composite` lane, returning the `(Datum) 0`
/// placeholder word. The core's `record_from_values` →
/// `HeapTupleGetDatum` hands back the self-describing composite image as a
/// `Datum::ByRef`, which the `Composite` lane carries verbatim.
fn ret_record(fcinfo: &mut FunctionCallInfoBaseData, built: PgResult<DatumV<'_>>) -> PgResult<Datum> {
    match built? {
        DatumV::ByRef(bytes) => {
            fcinfo.set_ref_result(RefPayload::Composite(bytes.as_slice().to_vec()));
            Ok(Datum::from_usize(0))
        }
        DatumV::Composite(t) => {
            fcinfo.set_ref_result(RefPayload::Composite(t.to_datum_image()));
            Ok(Datum::from_usize(0))
        }
        _ => panic!("pg_control_* fmgr: record_from_values produced a non-composite Datum"),
    }
}

/// `pg_control_system()` (pg_controldata.c, pg_proc oid 3441) — 4-column row.
fn fc_pg_control_system(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let ctx = scratch_mcx();
    let built = crate::pg_controldata::pg_control_system_datum(ctx.mcx());
    ret_record(fcinfo, built)
}

/// `pg_control_checkpoint()` (pg_controldata.c, pg_proc oid 3442) — 18-column row.
fn fc_pg_control_checkpoint(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let ctx = scratch_mcx();
    let built = crate::pg_controldata::pg_control_checkpoint_datum(ctx.mcx());
    ret_record(fcinfo, built)
}

/// `pg_control_recovery()` (pg_controldata.c, pg_proc oid 3443) — 5-column row.
fn fc_pg_control_recovery(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let ctx = scratch_mcx();
    let built = crate::pg_controldata::pg_control_recovery_datum(ctx.mcx());
    ret_record(fcinfo, built)
}

/// `pg_control_init()` (pg_controldata.c, pg_proc oid 3444) — 12-column row.
fn fc_pg_control_init(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let ctx = scratch_mcx();
    let built = crate::pg_controldata::pg_control_init_datum(ctx.mcx());
    ret_record(fcinfo, built)
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        func,
    )
}

/// Register every `rls.c` builtin (C: their `fmgr_builtins[]` rows). Called from
/// this crate's `init_seams()`. OIDs/nargs/retset from `pg_proc.dat`; both are
/// strict (no `proisstrict => 'f'`), nargs = 1, not retset.
pub fn register_backend_utils_misc_more_builtins() {
    fmgr_core::register_builtins_native([
        builtin(3298, "row_security_active", 1, true, false, fc_row_security_active),
        builtin(
            3299,
            "row_security_active_name",
            1,
            true,
            false,
            fc_row_security_active_name,
        ),
        // pg_controldata.c — composite-returning, no args, strict, not retset.
        builtin(3441, "pg_control_system", 0, true, false, fc_pg_control_system),
        builtin(3442, "pg_control_checkpoint", 0, true, false, fc_pg_control_checkpoint),
        builtin(3443, "pg_control_recovery", 0, true, false, fc_pg_control_recovery),
        builtin(3444, "pg_control_init", 0, true, false, fc_pg_control_init),
    ]);
}
