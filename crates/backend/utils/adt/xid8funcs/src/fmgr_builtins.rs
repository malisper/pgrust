//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! transaction-id / transaction-status functions in `xid8funcs.c` whose
//! argument/result types are expressible at the current fmgr boundary (the
//! 64-bit `xid8`/`int8` words and the `text` status string).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_xid8funcs_builtins`] registers every row
//! into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! The `pg_snapshot` family (I/O, accessors, `pg_snapshot_xip` set-returning,
//! `pg_visible_in_snapshot`) is NOT registered here: those cross the
//! `pg_snapshot`/`txid_snapshot` varlena carrier and the `FuncCallContext`
//! set-returning glue, neither of which is expressible at this boundary (see the
//! crate docs). Only the scalar / text transaction-id functions register.

use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use ::types_core::FullTransactionId;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// Read arg `i` as a `FullTransactionId`. C's `pg_xact_status` reads
/// `PG_GETARG_FullTransactionId(0)` (`DatumGetFullTransactionId`), and the
/// `txid_status(int8)` SQL wrapper passes the same 64-bit word through
/// `PG_GETARG_INT64`; either way the bits are the `.value` of the FXID.
#[inline]
fn arg_fxid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> FullTransactionId {
    let w = fcinfo
        .arg(i)
        .expect("xid8funcs fn: missing arg")
        .value
        .as_u64();
    FullTransactionId::from_u64(w)
}

/// `PG_RETURN_FullTransactionId(v)` / `PG_RETURN_INT64((int64) U64From...(v))`:
/// both store the 64-bit FXID value in the by-val result word.
#[inline]
fn ret_fxid(v: FullTransactionId) -> Datum {
    Datum::from_u64(v.to_u64())
}

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;

/// Build a header-ful 4-byte-header varlena image from a payload.
#[inline]
fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + VARHDRSZ;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(payload);
    img
}

/// Set a `text` result on the by-ref lane as a header-ful varlena image,
/// mirroring `PG_RETURN_TEXT_P(cstring_to_text(...))`.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, s: &str) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(varlena_image(s.as_bytes())));
    Datum::from_usize(0)
}

/// `PG_RETURN_NULL()`: mark the result NULL and return a dummy word.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_current_xact_id() -> xid8` (xid8funcs.c:333) and its `int8`-typed alias
/// `txid_current()`. The core assigns + returns the top FXID (erroring during
/// recovery); both prorettypes store the same 64-bit word.
fn fc_pg_current_xact_id(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let _ = fcinfo;
    let fxid = crate::pg_current_xact_id()?;
    Ok(ret_fxid(fxid))
}

/// `pg_current_xact_id_if_assigned() -> xid8 or NULL` (xid8funcs.c:351) and its
/// `int8`-typed alias `txid_current_if_assigned()`. `None` is `PG_RETURN_NULL`.
fn fc_pg_current_xact_id_if_assigned(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    match crate::pg_current_xact_id_if_assigned()? {
        Some(fxid) => Ok(ret_fxid(fxid)),
        None => Ok(ret_null(fcinfo)),
    }
}

/// `pg_xact_status(xid8) -> text or NULL` (xid8funcs.c:639) and its `int8`-typed
/// alias `txid_status(int8)`. The core returns the status string or `None`
/// (wrapped / truncated / too-old XID → `PG_RETURN_NULL`).
fn fc_pg_xact_status(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let fxid = arg_fxid(fcinfo, 0);
    match crate::pg_xact_status(fxid)? {
        Some(status) => Ok(ret_text(fcinfo, status)),
        None => Ok(ret_null(fcinfo)),
    }
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("xid8funcs fn: cstring arg missing from by-ref lane")
}

/// Set a `cstring` (`*out`) result on the by-ref lane.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// `pg_snapshot_in(cstring) -> pg_snapshot` (xid8funcs.c:407). The core parses
/// (via `parse_snapshot`) and sorts; the resulting `pg_snapshot` crosses as its
/// header-ful varlena image. Forward the soft `ErrorSaveContext` installed on
/// the frame by InputFunctionCallSafe so a recoverable parse failure `ereturn`s
/// into the sink (returning `Ok(None)`) instead of throwing past `invoke?`.
fn fc_pg_snapshot_in(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // Copy the cstring first: `arg_cstring` borrows `fcinfo` immutably while
    // `escontext_mut` needs it mutably.
    let s = arg_cstring(fcinfo, 0).to_owned();
    match crate::pg_snapshot_in(&s, fcinfo.escontext_mut())? {
        Some(snap) => {
            fcinfo.set_ref_result(RefPayload::Varlena(snap.to_varlena_bytes()));
            Ok(Datum::from_usize(0))
        }
        // Soft-error path: escontext recorded the failure; return a NULL
        // placeholder the caller discards after `soft_error_occurred()`.
        None => Ok(Datum::null()),
    }
}

/// `pg_snapshot_out(pg_snapshot) -> cstring` (xid8funcs.c:435). The arg arrives
/// as the header-ful `pg_snapshot` varlena image on the by-ref lane; reconstruct
/// it and format `xmin:xmax:xip,...`.
fn fc_pg_snapshot_out(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("pg_snapshot_out: by-ref pg_snapshot arg missing from by-ref lane");
    let snap = match crate::PgSnapshot::from_varlena_bytes(image) {
        Some(snap) => snap,
        None => return Err(::types_error::PgError::error("invalid pg_snapshot image")),
    };
    let s = crate::pg_snapshot_out(&snap);
    Ok(ret_cstring(fcinfo, s))
}

/// Read a by-ref `pg_snapshot` arg as a reconstructed [`crate::PgSnapshot`].
#[inline]
fn arg_snapshot(fcinfo: &FunctionCallInfoBaseData, i: usize) -> PgResult<crate::PgSnapshot> {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("xid8funcs fn: by-ref pg_snapshot arg missing from by-ref lane");
    match crate::PgSnapshot::from_varlena_bytes(image) {
        Some(snap) => Ok(snap),
        None => Err(::types_error::PgError::error("invalid pg_snapshot image")),
    }
}

/// Set a `pg_snapshot` (varlena) result on the by-ref lane.
#[inline]
fn ret_snapshot(fcinfo: &mut FunctionCallInfoBaseData, snap: &crate::PgSnapshot) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(snap.to_varlena_bytes()));
    Datum::from_usize(0)
}

/// `pg_current_snapshot() -> pg_snapshot` (xid8funcs.c:480). No args; takes the
/// current snapshot and returns its header-ful varlena image.
fn fc_pg_current_snapshot(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let snap = crate::pg_current_snapshot()?;
    Ok(ret_snapshot(fcinfo, &snap))
}

/// `pg_snapshot_recv(internal) -> pg_snapshot` (xid8funcs.c:451). The wire
/// message arrives verbatim on the by-ref lane; a `Pq8Cursor` walks it.
fn fc_pg_snapshot_recv(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let image: Vec<u8> = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .unwrap_or(&[])
        .to_vec();
    let mut cur = crate::Pq8Cursor::new(&image);
    let snap = crate::pg_snapshot_recv(&mut cur)?;
    Ok(ret_snapshot(fcinfo, &snap))
}

/// `pg_snapshot_send(pg_snapshot) -> bytea` (xid8funcs.c:495). The core emits
/// the raw wire bytes; `pq_endtypsend` wraps them into a header-ful `bytea`.
fn fc_pg_snapshot_send(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let snap = arg_snapshot(fcinfo, 0)?;
    let wire = crate::pg_snapshot_send(&snap);
    fcinfo.set_ref_result(RefPayload::Varlena(varlena_image(&wire)));
    Ok(Datum::from_usize(0))
}

/// `pg_visible_in_snapshot(xid8, pg_snapshot) -> bool` (xid8funcs.c:554).
fn fc_pg_visible_in_snapshot(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let value = arg_fxid(fcinfo, 0);
    let snap = arg_snapshot(fcinfo, 1)?;
    Ok(Datum::from_bool(crate::pg_visible_in_snapshot(value, &snap)))
}

/// `pg_snapshot_xmin(pg_snapshot) -> xid8` (xid8funcs.c:568).
fn fc_pg_snapshot_xmin(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let snap = arg_snapshot(fcinfo, 0)?;
    Ok(ret_fxid(crate::pg_snapshot_xmin(&snap)))
}

/// `pg_snapshot_xmax(pg_snapshot) -> xid8` (xid8funcs.c:582).
fn fc_pg_snapshot_xmax(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let snap = arg_snapshot(fcinfo, 0)?;
    Ok(ret_fxid(crate::pg_snapshot_xmax(&snap)))
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
    native: PgFnNative,
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
        native,
    )
}

/// Register every scalar / text `xid8funcs.c` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`. OIDs /
/// nargs / strict / retset transcribed exactly from `pg_proc.dat` (all default
/// `proisstrict => 't'`, none `proretset`). The `int8`-typed `txid_*` aliases
/// share the same prosrc cores as their `xid8` counterparts.
pub fn register_xid8funcs_builtins() {
    fmgr_core::register_builtins_native([
        // ---- current transaction id (no args) ----
        builtin(2943, "pg_current_xact_id", 0, true, false, fc_pg_current_xact_id),
        builtin(5059, "pg_current_xact_id", 0, true, false, fc_pg_current_xact_id),
        builtin(
            3348,
            "pg_current_xact_id_if_assigned",
            0,
            true,
            false,
            fc_pg_current_xact_id_if_assigned,
        ),
        builtin(
            5060,
            "pg_current_xact_id_if_assigned",
            0,
            true,
            false,
            fc_pg_current_xact_id_if_assigned,
        ),
        // ---- transaction status (1 arg) ----
        builtin(3360, "pg_xact_status", 1, true, false, fc_pg_xact_status),
        builtin(5066, "pg_xact_status", 1, true, false, fc_pg_xact_status),
        // ---- pg_snapshot I/O (txid_snapshot aliases share the OIDs) ----
        builtin(2939, "pg_snapshot_in", 1, true, false, fc_pg_snapshot_in),
        builtin(2940, "pg_snapshot_out", 1, true, false, fc_pg_snapshot_out),
        builtin(5055, "pg_snapshot_in", 1, true, false, fc_pg_snapshot_in),
        builtin(5056, "pg_snapshot_out", 1, true, false, fc_pg_snapshot_out),
        // ---- pg_snapshot binary I/O (txid_snapshot aliases share OIDs) ----
        builtin(2941, "pg_snapshot_recv", 1, true, false, fc_pg_snapshot_recv),
        builtin(2942, "pg_snapshot_send", 1, true, false, fc_pg_snapshot_send),
        builtin(5057, "pg_snapshot_recv", 1, true, false, fc_pg_snapshot_recv),
        builtin(5058, "pg_snapshot_send", 1, true, false, fc_pg_snapshot_send),
        // ---- pg_snapshot accessors (scalar) ----
        builtin(2944, "pg_current_snapshot", 0, true, false, fc_pg_current_snapshot),
        builtin(5061, "pg_current_snapshot", 0, true, false, fc_pg_current_snapshot),
        builtin(2945, "pg_snapshot_xmin", 1, true, false, fc_pg_snapshot_xmin),
        builtin(5062, "pg_snapshot_xmin", 1, true, false, fc_pg_snapshot_xmin),
        builtin(2946, "pg_snapshot_xmax", 1, true, false, fc_pg_snapshot_xmax),
        builtin(5063, "pg_snapshot_xmax", 1, true, false, fc_pg_snapshot_xmax),
        builtin(2948, "pg_visible_in_snapshot", 2, true, false, fc_pg_visible_in_snapshot),
        builtin(5065, "pg_visible_in_snapshot", 2, true, false, fc_pg_visible_in_snapshot),
        // pg_snapshot_xip (2947/5064) is set-returning — needs the
        // FuncCallContext SRF glue; left in the baseline.
    ]);
}
