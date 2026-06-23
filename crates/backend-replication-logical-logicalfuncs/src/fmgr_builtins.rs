//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the
//! `logicalfuncs.c` `pg_logical_emit_message_{text,bytea}` SQL functions.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! `pg_lsn` result word. [`register_logicalfuncs_builtins`] registers both rows
//! into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! (and the `fmgr_isbuiltin` fast path) resolves them. OIDs / nargs / strict /
//! retset are transcribed exactly from `pg_proc.dat`.
//!
//! The two functions share one C body shape (`pg_logical_emit_message_bytea`
//! reads `(transactional bool, prefix text, message bytea, flush bool)`;
//! `_text` differs only in the third arg's declared type — `text` instead of
//! `bytea` — and forwards to the bytea core). Both return `pg_lsn`
//! (`XLogRecPtr`, an 8-byte by-value word — `LSNGetDatum` / `Datum::from_u64`),
//! which IS expressible at the boundary. The `pg_logical_slot_{get,peek}_changes`
//! SRFs (set-returning, materialize-mode) are NOT registered here — they live in
//! the executor-frame SRF home.

use types_core::XLogRecPtr;
use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_BOOL(i)` → `DatumGetBool`: any nonzero word reads back as `true`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo
        .arg(i)
        .expect("logicalfuncs fn: missing bool arg")
        .value
        .as_bool()
}

/// `PG_GETARG_TEXT_PP(i)` / `PG_GETARG_BYTEA_PP(i)` → `VARDATA_ANY`: the
/// `text`/`bytea` arg's payload bytes on the by-ref lane (the C body reads
/// `text_to_cstring(prefix)` for the prefix — its bytes before any NUL — and
/// `VARDATA_ANY` / `VARSIZE_ANY_EXHDR` for the message; both reduce to the
/// header-less payload image here).
#[inline]
fn arg_varlena_payload<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("logicalfuncs fn: varlena arg missing from by-ref lane");
    vardata_any(image)
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

/// `PG_RETURN_LSN(v)`: a `pg_lsn`/`XLogRecPtr` result word (C: `LSNGetDatum`
/// over the 8-byte by-value `XLogRecPtr`).
#[inline]
fn ret_lsn(v: XLogRecPtr) -> Datum {
    Datum::from_u64(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_logical_emit_message_text(transactional bool, prefix text, message text,
/// flush bool)` (logicalfuncs.c:381) — `pg_lsn` result.
fn fc_pg_logical_emit_message_text(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let transactional = arg_bool(fcinfo, 0);
    let prefix = arg_varlena_payload(fcinfo, 1).to_vec();
    let data = arg_varlena_payload(fcinfo, 2).to_vec();
    let flush = arg_bool(fcinfo, 3);
    Ok(ret_lsn(crate::pg_logical_emit_message_text(
        transactional,
        &prefix,
        &data,
        flush,
    )?))
}

/// `pg_logical_emit_message_bytea(transactional bool, prefix text, message
/// bytea, flush bool)` (logicalfuncs.c:367) — `pg_lsn` result.
fn fc_pg_logical_emit_message_bytea(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let transactional = arg_bool(fcinfo, 0);
    let prefix = arg_varlena_payload(fcinfo, 1).to_vec();
    let data = arg_varlena_payload(fcinfo, 2).to_vec();
    let flush = arg_bool(fcinfo, 3);
    Ok(ret_lsn(crate::pg_logical_emit_message_bytea(
        transactional,
        &prefix,
        &data,
        flush,
    )?))
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

/// Register the two `pg_logical_emit_message_*` fmgr builtins (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
///
/// OIDs / nargs / strict / retset transcribed from `pg_proc.dat`: both are
/// `provolatile => 'v'`, inherit `proisstrict BKI_DEFAULT(t)` (neither overrides
/// it, so `strict = true`), take 4 args, and neither is `proretset` (so
/// `retset = false`).
pub fn register_logicalfuncs_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // pg_logical_emit_message_text(bool, text, text, bool) -> pg_lsn
        builtin(
            3577,
            "pg_logical_emit_message_text",
            4,
            true,
            false,
            fc_pg_logical_emit_message_text,
        ),
        // pg_logical_emit_message_bytea(bool, text, bytea, bool) -> pg_lsn
        builtin(
            3578,
            "pg_logical_emit_message_bytea",
            4,
            true,
            false,
            fc_pg_logical_emit_message_bytea,
        ),
    ]);
}
