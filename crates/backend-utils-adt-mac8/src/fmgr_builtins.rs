//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `macaddr8`
//! functions in `mac8.c` whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_mac8_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves
//! them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! # The by-reference `macaddr8` / `macaddr` convention
//!
//! `macaddr8` (and `macaddr`, produced by the `macaddr8tomacaddr` conversion)
//! are fixed-length pass-by-reference types. Their values cross the fmgr
//! boundary on the by-reference side channel as `RefPayload::Varlena(image)`,
//! where `image` is the eight (resp. six) raw struct bytes — the exact
//! `sizeof(macaddr8)` / `sizeof(macaddr)` in-memory image. These types have no
//! varlena header (they are fixed-length by-ref, not varlena), so the image is
//! header-less and symmetric on the arg and result lanes.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use types_network::{macaddr, macaddr8};

/// A scratch context for the `_send` adapter to frame the `bytea` result, copied
/// onto the by-ref lane before it is dropped (C: the palloc'd result lives in
/// the caller's context; here it crosses by value).
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("macaddr8 fmgr scratch")
}

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_MACADDR8_P(i)`: decode a `macaddr8` arg from its eight-byte by-ref
/// image on the side channel.
#[inline]
fn arg_macaddr8(fcinfo: &FunctionCallInfoBaseData, i: usize) -> macaddr8 {
    let b = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("macaddr8 fn: by-ref `macaddr8` arg missing from by-ref lane");
    assert!(b.len() >= 8, "macaddr8 fn: by-ref image too short");
    macaddr8 {
        a: b[0],
        b: b[1],
        c: b[2],
        d: b[3],
        e: b[4],
        f: b[5],
        g: b[6],
        h: b[7],
    }
}

/// `PG_GETARG_MACADDR_P(i)`: decode a `macaddr` arg from its six-byte image
/// (used by the `macaddr -> macaddr8` conversion).
#[inline]
fn arg_macaddr(fcinfo: &FunctionCallInfoBaseData, i: usize) -> macaddr {
    let b = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("macaddr8 fn: by-ref `macaddr` arg missing from by-ref lane");
    assert!(b.len() >= 6, "macaddr8 fn: by-ref macaddr image too short");
    macaddr {
        a: b[0],
        b: b[1],
        c: b[2],
        d: b[3],
        e: b[4],
        f: b[5],
    }
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("macaddr8 fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_INT64(i)`: arg `i`'s full word as a signed 64-bit int.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("macaddr8 fn: missing arg").value.as_i64()
}

/// Set a `macaddr8` (by-reference) result on the by-ref lane.
#[inline]
fn ret_macaddr8(fcinfo: &mut FunctionCallInfoBaseData, m: macaddr8) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(vec![
        m.a, m.b, m.c, m.d, m.e, m.f, m.g, m.h,
    ]));
    Datum::from_usize(0)
}

/// Set a `macaddr` (by-reference) result on the by-ref lane.
#[inline]
fn ret_macaddr(fcinfo: &mut FunctionCallInfoBaseData, m: macaddr) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(vec![m.a, m.b, m.c, m.d, m.e, m.f]));
    Datum::from_usize(0)
}

/// Set a `cstring` (`_out`) result on the by-ref lane.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

// ---------------------------------------------------------------------------
// fc_ adapters (Result-native: `ereport(ERROR)` travels as `Err(PgError)`
// straight back to the fmgr dispatch `invoke_builtin`, no panic/catch_unwind).
// ---------------------------------------------------------------------------

fn fc_macaddr8_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    // Forward `fcinfo->context` (the soft ErrorSaveContext installed by
    // InputFunctionCallSafe) so a recoverable parse failure `ereturn`s into the
    // soft sink instead of throwing — matching fc_int2in. On the soft path the
    // body returns `Ok(None)`; the caller checks `soft_error_occurred()` first
    // and discards this placeholder result word.
    let escontext = fcinfo.escontext_mut();
    Ok(match crate::macaddr8_in(&s, escontext)? {
        Some(m) => ret_macaddr8(fcinfo, m),
        None => Datum::null(),
    })
}

fn fc_macaddr8_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = arg_macaddr8(fcinfo, 0);
    let s = String::from_utf8(crate::macaddr8_out(&m)).expect("macaddr8_out: valid utf8");
    Ok(ret_cstring(fcinfo, s))
}

macro_rules! fc_cmp_bool {
    ($name:ident, $core:path) => {
        fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let a = arg_macaddr8(fcinfo, 0);
            let b = arg_macaddr8(fcinfo, 1);
            Ok(Datum::from_bool($core(&a, &b)))
        }
    };
}

fc_cmp_bool!(fc_macaddr8_eq, crate::macaddr8_eq);
fc_cmp_bool!(fc_macaddr8_lt, crate::macaddr8_lt);
fc_cmp_bool!(fc_macaddr8_le, crate::macaddr8_le);
fc_cmp_bool!(fc_macaddr8_gt, crate::macaddr8_gt);
fc_cmp_bool!(fc_macaddr8_ge, crate::macaddr8_ge);
fc_cmp_bool!(fc_macaddr8_ne, crate::macaddr8_ne);

fn fc_macaddr8_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_macaddr8(fcinfo, 0);
    let b = arg_macaddr8(fcinfo, 1);
    Ok(Datum::from_i32(crate::macaddr8_cmp(&a, &b)))
}

fn fc_hashmacaddr8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = arg_macaddr8(fcinfo, 0);
    Ok(Datum::from_i32(crate::hashmacaddr8(&m) as i32))
}

fn fc_hashmacaddr8extended(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = arg_macaddr8(fcinfo, 0);
    let seed = arg_int64(fcinfo, 1) as u64;
    Ok(Datum::from_i64(crate::hashmacaddr8extended(&m, seed) as i64))
}

fn fc_macaddr8_not(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = arg_macaddr8(fcinfo, 0);
    Ok(ret_macaddr8(fcinfo, crate::macaddr8_not(&m)))
}

fn fc_macaddr8_and(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_macaddr8(fcinfo, 0);
    let b = arg_macaddr8(fcinfo, 1);
    Ok(ret_macaddr8(fcinfo, crate::macaddr8_and(&a, &b)))
}

fn fc_macaddr8_or(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_macaddr8(fcinfo, 0);
    let b = arg_macaddr8(fcinfo, 1);
    Ok(ret_macaddr8(fcinfo, crate::macaddr8_or(&a, &b)))
}

/// `macaddr8_recv(internal) -> macaddr8` (oid 3446). The raw wire-message bytes
/// ride the by-ref lane; the core reads the EUI-48/EUI-64 octets off them.
fn fc_macaddr8_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let src = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("macaddr8_recv: by-ref message arg missing from by-ref lane");
    let m = crate::macaddr8_recv(src)?;
    Ok(ret_macaddr8(fcinfo, m))
}

/// `macaddr8_send(macaddr8) -> bytea` (oid 3447). The core returns the eight
/// wire octets; frame them into a `bytea` varlena image through the same
/// pq_*typsend path C uses, on the by-ref `Varlena` result lane.
fn fc_macaddr8_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = arg_macaddr8(fcinfo, 0);
    let wire = crate::macaddr8_send(&m);
    let scratch = scratch_mcx();
    let mut buf = backend_libpq_pqformat::pq_begintypsend(scratch.mcx())?;
    backend_libpq_pqformat::pq_sendbytes(&mut buf, &wire)?;
    let bytea = backend_libpq_pqformat::pq_endtypsend(buf);
    fcinfo.set_ref_result(RefPayload::Varlena(bytea.as_bytes().to_vec()));
    Ok(Datum::from_usize(0))
}

/// `macaddr8_trunc(macaddr8) -> macaddr8` (oid 4112). Zeroes the last five
/// octets (the device portion).
fn fc_macaddr8_trunc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = arg_macaddr8(fcinfo, 0);
    Ok(ret_macaddr8(fcinfo, crate::macaddr8_trunc(&m)))
}

fn fc_macaddr8_set7bit(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = arg_macaddr8(fcinfo, 0);
    Ok(ret_macaddr8(fcinfo, crate::macaddr8_set7bit(&m)))
}

fn fc_macaddrtomacaddr8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = arg_macaddr(fcinfo, 0);
    Ok(ret_macaddr8(fcinfo, crate::macaddrtomacaddr8(&m)))
}

fn fc_macaddr8tomacaddr(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = arg_macaddr8(fcinfo, 0);
    let r = crate::macaddr8tomacaddr(&m)?;
    Ok(ret_macaddr(fcinfo, r))
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

/// Register the `mac8.c` `macaddr8` builtins (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs/strict/retset transcribed
/// exactly from `pg_proc.dat` (every row is `proisstrict => 't'`, non-retset).
pub fn register_mac8_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(4110, "macaddr8_in", 1, true, false, fc_macaddr8_in),
        builtin(4111, "macaddr8_out", 1, true, false, fc_macaddr8_out),
        builtin(3446, "macaddr8_recv", 1, true, false, fc_macaddr8_recv),
        builtin(3447, "macaddr8_send", 1, true, false, fc_macaddr8_send),
        builtin(4112, "macaddr8_trunc", 1, true, false, fc_macaddr8_trunc),
        builtin(4113, "macaddr8_eq", 2, true, false, fc_macaddr8_eq),
        builtin(4114, "macaddr8_lt", 2, true, false, fc_macaddr8_lt),
        builtin(4115, "macaddr8_le", 2, true, false, fc_macaddr8_le),
        builtin(4116, "macaddr8_gt", 2, true, false, fc_macaddr8_gt),
        builtin(4117, "macaddr8_ge", 2, true, false, fc_macaddr8_ge),
        builtin(4118, "macaddr8_ne", 2, true, false, fc_macaddr8_ne),
        builtin(4119, "macaddr8_cmp", 2, true, false, fc_macaddr8_cmp),
        builtin(328, "hashmacaddr8", 1, true, false, fc_hashmacaddr8),
        builtin(781, "hashmacaddr8extended", 2, true, false, fc_hashmacaddr8extended),
        builtin(4120, "macaddr8_not", 1, true, false, fc_macaddr8_not),
        builtin(4121, "macaddr8_and", 2, true, false, fc_macaddr8_and),
        builtin(4122, "macaddr8_or", 2, true, false, fc_macaddr8_or),
        builtin(4125, "macaddr8_set7bit", 1, true, false, fc_macaddr8_set7bit),
        builtin(4123, "macaddrtomacaddr8", 1, true, false, fc_macaddrtomacaddr8),
        builtin(4124, "macaddr8tomacaddr", 1, true, false, fc_macaddr8tomacaddr),
    ]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_datum::NullableDatum;

    fn in_macaddr8(s: &str) -> Vec<u8> {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Cstring(s.to_string()))];
        let native =
            backend_utils_fmgr_core::native_builtin(4110).expect("macaddr8_in registered");
        native(&mut fcinfo).unwrap();
        match fcinfo.take_ref_result().expect("macaddr8_in produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("macaddr8_in returned non-varlena: {other:?}"),
        }
    }

    fn out_macaddr8(image: &[u8]) -> String {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(image.to_vec()))];
        let native =
            backend_utils_fmgr_core::native_builtin(4111).expect("macaddr8_out registered");
        native(&mut fcinfo).unwrap();
        match fcinfo.take_ref_result().expect("macaddr8_out produced a result") {
            RefPayload::Cstring(s) => s,
            other => panic!("macaddr8_out returned non-cstring: {other:?}"),
        }
    }

    fn call_cmp(oid: u32, a: &[u8], b: &[u8]) -> bool {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let native = backend_utils_fmgr_core::native_builtin(oid).expect("op registered");
        native(&mut fcinfo).unwrap().as_bool()
    }

    fn call_unary_macaddr8(oid: u32, a: &[u8]) -> Vec<u8> {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.to_vec()))];
        let native = backend_utils_fmgr_core::native_builtin(oid).expect("op registered");
        native(&mut fcinfo).unwrap();
        match fcinfo.take_ref_result().expect("op produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("non-varlena result: {other:?}"),
        }
    }

    #[test]
    fn byref_macaddr8_in_out_roundtrip() {
        crate::init_seams();
        // EUI-48 input is converted to EUI-64 by inserting ff:fe in the middle.
        let img = in_macaddr8("08:00:2b:01:02:03");
        assert_eq!(img, vec![0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03]);
        assert_eq!(out_macaddr8(&img), "08:00:2b:ff:fe:01:02:03");
    }

    #[test]
    fn byref_macaddr8_compare_and_bitwise() {
        crate::init_seams();
        let a = in_macaddr8("08:00:2b:01:02:03:04:05");
        let b = in_macaddr8("08:00:2b:01:02:03:04:06");
        assert!(call_cmp(4114, &a, &b)); // lt
        assert!(call_cmp(4118, &a, &b)); // ne
        assert!(!call_cmp(4113, &a, &b)); // eq
        // NOT of all-zeros is all-ones.
        let z = in_macaddr8("00:00:00:00:00:00:00:00");
        assert_eq!(call_unary_macaddr8(4120, &z), vec![0xff; 8]);
    }

    #[test]
    fn byref_macaddr8_set7bit_and_conversions() {
        crate::init_seams();
        let a = in_macaddr8("08:00:2b:01:02:03:04:05");
        // set7bit flips the locally-administered bit (0x02) of the first octet.
        let s = call_unary_macaddr8(4125, &a);
        assert_eq!(s[0], 0x0a);
        // macaddr -> macaddr8 inserts ff:fe.
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(vec![
            0x08, 0x00, 0x2b, 0x01, 0x02, 0x03,
        ]))];
        let native = backend_utils_fmgr_core::native_builtin(4123).unwrap();
        native(&mut fcinfo).unwrap();
        let m8 = match fcinfo.take_ref_result().unwrap() {
            RefPayload::Varlena(v) => v,
            o => panic!("{o:?}"),
        };
        assert_eq!(m8, vec![0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03]);
        // macaddr8 -> macaddr (only valid for ff:fe-formatted EUI-64).
        let m6 = call_macaddr8tomacaddr(&m8);
        assert_eq!(m6, vec![0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
    }

    fn call_macaddr8tomacaddr(a: &[u8]) -> Vec<u8> {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.to_vec()))];
        let native = backend_utils_fmgr_core::native_builtin(4124).unwrap();
        native(&mut fcinfo).unwrap();
        match fcinfo.take_ref_result().unwrap() {
            RefPayload::Varlena(v) => v,
            o => panic!("{o:?}"),
        }
    }
}
