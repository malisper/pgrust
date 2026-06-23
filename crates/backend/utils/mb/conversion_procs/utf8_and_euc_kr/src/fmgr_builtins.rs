//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the
//! `utf8_and_euc_kr.c` conversion procedures.
//!
//! In C these two functions (`euc_kr_to_utf8`, `utf8_to_euc_kr`) live in the
//! `$libdir/utf8_and_euc_kr` shared object and are reached, by OID, through the
//! `pg_conversion` catalog via `OidFunctionCall6` (see `convert_via_proc` in
//! `mbutils.c`). This port mocks that dynamic loading by registering the two
//! procedures directly in the fmgr builtin fast-path table (the in-process
//! builtin registry), keyed by their canonical `pg_proc.dat` OIDs (4364 / 4365),
//! exactly as a builtin would be. The `fc_*` adapter reads the standard
//! conversion-proc argument frame and delegates to the value core in
//! [`crate`].
//!
//! Conversion-proc fmgr ABI (`pg_proc.dat`:
//! `(int4 src_encoding, int4 dest_encoding, cstring src, internal dest,
//!   int4 len, bool noError) -> int4`):
//!   * `args[0]` / `args[1]` — source / destination encoding ids (`int4`).
//!   * `ref_args[2]` — the source bytes (C `cstring`; carried raw on the by-ref
//!     byte lane so a non-UTF-8 source encoding survives losslessly).
//!   * `ref_args[3]` — the destination buffer the callee writes (starts empty).
//!   * `args[4]` — source length (`int4`); unused here (the core reads the slice
//!     length), kept for ABI parity.
//!   * `args[5]` — `no_error` (`bool`).
//! Return value: the number of *source* bytes consumed (`int4`), with the
//! converted output written into `ref_args[3]`.

use ::types_core::Oid;
use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

/// `pg_proc.dat`: `euc_kr_to_utf8` OID.
const F_EUC_KR_TO_UTF8: Oid = 4364;
/// `pg_proc.dat`: `utf8_to_euc_kr` OID.
const F_UTF8_TO_EUC_KR: Oid = 4365;

/// Read an `int4` by-value argument.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .args
        .get(i)
        .map(|nd| nd.value.as_i32())
        .unwrap_or(0)
}

/// Read a `bool` by-value argument.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo
        .args
        .get(i)
        .map(|nd| nd.value.as_bool())
        .unwrap_or(false)
}

/// Read the source bytes (`ref_args[2]`, a C `cstring` carried raw).
#[inline]
fn src_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    match fcinfo.ref_arg(i) {
        Some(RefPayload::Varlena(b)) => b.as_slice(),
        Some(RefPayload::Cstring(s)) => s.as_bytes(),
        _ => &[],
    }
}

/// Write the converted output into the destination referent (`ref_args[3]`).
#[inline]
fn write_dest(fcinfo: &mut FunctionCallInfoBaseData, i: usize, bytes: Vec<u8>) {
    if fcinfo.ref_args.len() <= i {
        fcinfo.ref_args.resize_with(i + 1, || None);
    }
    fcinfo.ref_args[i] = Some(RefPayload::Varlena(bytes));
}

/// fmgr adapter for `euc_kr_to_utf8`.
fn fc_euc_kr_to_utf8(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let src_encoding = arg_i32(fcinfo, 0);
    let dest_encoding = arg_i32(fcinfo, 1);
    let no_error = arg_bool(fcinfo, 5);
    let src = src_bytes(fcinfo, 2).to_vec();
    let res = crate::euc_kr_to_utf8(src_encoding, dest_encoding, &src, no_error)?;
    let converted = res.converted;
    write_dest(fcinfo, 3, res.bytes);
    Ok(Datum::from_i32(converted))
}

/// fmgr adapter for `utf8_to_euc_kr`.
fn fc_utf8_to_euc_kr(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let src_encoding = arg_i32(fcinfo, 0);
    let dest_encoding = arg_i32(fcinfo, 1);
    let no_error = arg_bool(fcinfo, 5);
    let src = src_bytes(fcinfo, 2).to_vec();
    let res = crate::utf8_to_euc_kr(src_encoding, dest_encoding, &src, no_error)?;
    let converted = res.converted;
    write_dest(fcinfo, 3, res.bytes);
    Ok(Datum::from_i32(converted))
}

fn builtin(foid: Oid, name: &str, func: PgFnNative) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            // (int4, int4, cstring, internal, int4, bool) -> int4
            nargs: 6,
            strict: true,
            retset: false,
            func: None,
        },
        func,
    )
}

/// Register the `utf8_and_euc_kr` conversion procedures in the fmgr builtin
/// registry (the mocked equivalent of loading `$libdir/utf8_and_euc_kr`).
pub fn register_utf8_and_euc_kr_builtins() {
    fmgr_core::register_builtins_native([
        builtin(F_EUC_KR_TO_UTF8, "euc_kr_to_utf8", fc_euc_kr_to_utf8),
        builtin(F_UTF8_TO_EUC_KR, "utf8_to_euc_kr", fc_utf8_to_euc_kr),
    ]);
}
