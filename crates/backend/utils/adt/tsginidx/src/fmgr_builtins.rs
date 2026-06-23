//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `tsginidx.c` functions whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Only `gin_cmp_tslexeme` (oid 3724, the `tsvector_ops` GIN `compare` support
//! function) is registered here: it takes two `text` arguments and returns
//! `int4`, both of which map cleanly onto the boundary. Each entry is a
//! `fc_<name>` adapter that reads its arguments off the fmgr call frame, calls
//! the matching value core (ported in this crate), and writes back the result
//! word. [`register_tsginidx_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves it. OIDs /
//! nargs / strict / retset are transcribed exactly from `pg_proc.dat`.
//!
//! The remaining tsginidx entry points (`gin_extract_tsvector` /
//! `gin_extract_tsquery` / `gin_tsquery_consistent` /
//! `gin_tsquery_triconsistent` / `gin_cmp_prefix` and the back-compat stubs)
//! are NOT registered here: their `internal`-typed array / GIN-check / out-param
//! arguments (`Datum *entries`, `bool *check`, `int32 *nentries`,
//! `bool **partialmatch`, `Pointer **extra_data`, `int32 *searchMode`) are not
//! expressible on the scalar/by-ref fmgr boundary — they are dispatched through
//! the GIN opclass support-proc family instead.

use ::datum::Datum;
use ::types_error::PgResult;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_TEXT_PP(i)` → `VARDATA_ANY`: a `text` arg's detoasted,
/// header-stripped payload on the by-ref lane (the boundary owns the varlena
/// framing). `gin_cmp_tslexeme`'s core consumes exactly these header-less bytes.
///
/// `gin_cmp_tslexeme` is the `tsvector_ops` GIN compare proc, called during index
/// build/probe to order the extracted lexeme `text` entry keys. A short lexeme
/// (most are) is stored short-packed in the GIN entry tuple once
/// `SHORT_VARLENA_PACKING` is on, so it reaches here with a 1-byte header; a fixed
/// 4-byte strip would land 3 bytes into (or past) the payload — C's
/// `PG_GETARG_TEXT_PP` is `VARDATA_ANY`, header-form-agnostic. Skip ONE byte for a
/// genuine short header (low bit set, but not the lone `0x01` external tag), else
/// `VARHDRSZ`. No-op while the flag is OFF (every stored key is 4B).
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("tsginidx fn: text arg missing from by-ref lane");
    // VARDATA_ANY: a short (1-byte, low-bit-set) header skips ONE byte, an
    // ordinary 4-byte header skips VARHDRSZ.
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        _ => &image[::datum::varlena::VARHDRSZ..],
    }
}

#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `gin_cmp_tslexeme(text, text) -> int4` (tsginidx.c:23). C reads
/// `VARDATA_ANY` / `VARSIZE_ANY_EXHDR` of both `text` args and returns the
/// `tsCompareString` result; here the two args already arrive header-stripped.
fn fc_gin_cmp_tslexeme(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_text(fcinfo, 0);
    let b = arg_text(fcinfo, 1);
    Ok(ret_i32(crate::gin_cmp_tslexeme(a, b)))
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

/// The shared fmgr-frame entry point for every `tsvector_ops` GIN support proc
/// whose `internal`-typed out-parameters cannot cross the by-word fmgr `Datum`
/// lane (`gin_extract_tsvector` / `gin_extract_tsquery` / `gin_tsquery_consistent`
/// / `gin_tsquery_triconsistent` / `gin_cmp_prefix` and the back-compat stubs).
///
/// In the owned model the GIN access method invokes these procs through the
/// typed by-OID dispatch in `backend-access-gin-core-probe::dispatch`, reading
/// `FmgrInfo::fn_oid` — never `fn_addr`. This frame entry is therefore never
/// reached on any port path; it exists only so the `fmgr_builtins[]` row carries
/// a non-`None` callable (matching C's table) and `fmgr_isbuiltin` / `fmgr_info`
/// can resolve the `internal` prosrc name. It raises a clear error if a future
/// fmgr-frame call site is ever added, pointing at the dispatch seam to use.
fn fc_gin_tsvector_via_dispatch(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let foid = fcinfo.flinfo.as_ref().map(|fi| fi.fn_oid).unwrap_or(0);
    Err(::types_error::PgError::error(format!(
        "GIN tsvector_ops support function (OID {foid}) must be invoked through \
         the typed opclass dispatch (gin_extract_value / gin_extract_query / \
         gin_consistent_call_{{bool,tri}} seams), not the fmgr frame; the owned \
         GIN access method dispatches these by FmgrInfo.fn_oid"
    )))
}

fn dispatch_builtin(foid: u32, name: &str, nargs: i16) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            // Every tsginidx.c support proc is proisstrict => 't' and not
            // proretset in pg_proc.dat.
            nargs,
            strict: true,
            retset: false,
            func: None,
        },
        fc_gin_tsvector_via_dispatch,
    )
}

/// Register the scalar `tsginidx.c` builtins (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs / nargs / strict / retset
/// transcribed exactly from `pg_proc.dat` (`gin_cmp_tslexeme`:
/// `proisstrict => 't'`, 2 args, not retset).
///
/// Besides `gin_cmp_tslexeme` (the lone fmgr-boundary-expressible row), the
/// `internal`-out-param GIN `tsvector_ops` support procs are registered with a
/// dispatch-only frame entry so `index_getprocinfo` → `fmgr_info` can resolve
/// their `internal` prosrc names (without which `CREATE INDEX ... USING gin
/// (tsvector_ops)` errors `internal function "gin_extract_tsvector" is not in
/// internal lookup table`); they are actually invoked through the by-OID typed
/// dispatch in `backend-access-gin-core-probe::dispatch`. OIDs / prosrc names /
/// nargs from `pg_proc.dat` (fmgr resolves by prosrc name, so the `*_2args` /
/// `*_5args` / `*_6args` / `*_oldsig` rows carry their prosrc, not proname).
pub fn register_tsginidx_builtins() {
    fmgr_core::register_builtins_native([builtin(
        3724,
        "gin_cmp_tslexeme",
        2,
        true,
        false,
        fc_gin_cmp_tslexeme,
    )]);
    fmgr_core::register_builtins_native([
        // GIN comparePartial (gin_cmp_prefix): declared 4-arg in pg_proc, body
        // uses 2 (the strategy/extra_data args are NOT_USED).
        dispatch_builtin(2700, "gin_cmp_prefix", 4),
        // extractValue.
        dispatch_builtin(3656, "gin_extract_tsvector", 3),
        dispatch_builtin(3077, "gin_extract_tsvector_2args", 2),
        // extractQuery.
        dispatch_builtin(3657, "gin_extract_tsquery", 7),
        dispatch_builtin(3087, "gin_extract_tsquery_5args", 5),
        dispatch_builtin(3791, "gin_extract_tsquery_oldsig", 7),
        // consistent.
        dispatch_builtin(3658, "gin_tsquery_consistent", 8),
        dispatch_builtin(3088, "gin_tsquery_consistent_6args", 6),
        dispatch_builtin(3792, "gin_tsquery_consistent_oldsig", 8),
        // triConsistent.
        dispatch_builtin(3921, "gin_tsquery_triconsistent", 7),
    ]);
}
