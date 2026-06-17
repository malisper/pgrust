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

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_TEXT_PP(i)` → `VARDATA_ANY`: a `text` arg's detoasted,
/// header-stripped payload on the by-ref lane (the boundary owns the varlena
/// framing). `gin_cmp_tslexeme`'s core consumes exactly these header-less bytes.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("tsginidx fn: text arg missing from by-ref lane")
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
fn fc_gin_cmp_tslexeme(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_text(fcinfo, 0);
    let b = arg_text(fcinfo, 1);
    ret_i32(crate::gin_cmp_tslexeme(a, b))
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
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register the scalar `tsginidx.c` builtins (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs / nargs / strict / retset
/// transcribed exactly from `pg_proc.dat` (`gin_cmp_tslexeme`:
/// `proisstrict => 't'`, 2 args, not retset).
pub fn register_tsginidx_builtins() {
    backend_utils_fmgr_core::register_builtins([builtin(
        3724,
        "gin_cmp_tslexeme",
        2,
        true,
        false,
        fc_gin_cmp_tslexeme,
    )]);
}
