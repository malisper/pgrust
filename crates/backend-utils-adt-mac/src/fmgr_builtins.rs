//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `macaddr`
//! functions in `mac.c` whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_mac_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves
//! them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! # The by-reference `macaddr` convention
//!
//! `macaddr` is a fixed-length pass-by-reference type. Its values cross the fmgr
//! boundary on the by-reference side channel: a `macaddr` ARG arrives as
//! `fcinfo.ref_arg(i) == Some(RefPayload::Varlena(image))` and a `macaddr`
//! RESULT is set via `fcinfo.set_ref_result(RefPayload::Varlena(image))`. The
//! `image` is the six raw struct bytes `[a, b, c, d, e, f]` — the exact
//! `sizeof(macaddr)` in-memory image C reads via `PG_GETARG_MACADDR_P` and the
//! body `macaddr_send` writes. `macaddr` has no varlena header (it is a
//! fixed-length by-ref type, not a varlena), so the image is header-less and
//! symmetric on the arg and result lanes.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};
use types_network::macaddr;

/// A scratch context for the `_send` adapter to frame the `bytea` result, copied
/// onto the by-ref lane before it is dropped (C: the palloc'd result lives in
/// the caller's context; here it crosses by value).
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("macaddr fmgr scratch")
}

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_MACADDR_P(i)`: decode a `macaddr` arg from its six-byte by-ref
/// image on the side channel.
#[inline]
fn arg_macaddr(fcinfo: &FunctionCallInfoBaseData, i: usize) -> macaddr {
    let b = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("macaddr fn: by-ref `macaddr` arg missing from by-ref lane");
    assert!(b.len() >= 6, "macaddr fn: by-ref image too short");
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
        .expect("macaddr fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_INT64(i)`: arg `i`'s full word as a signed 64-bit int.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("macaddr fn: missing arg").value.as_i64()
}

/// Set a `macaddr` (by-reference) result on the by-ref lane and return the dummy
/// by-value word. The bytes are the six raw struct bytes.
#[inline]
fn ret_macaddr(fcinfo: &mut FunctionCallInfoBaseData, m: macaddr) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(vec![m.a, m.b, m.c, m.d, m.e, m.f]));
    Datum::from_usize(0)
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

/// Unwrap a `PgResult`, re-raising its error through `raise`.
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_macaddr_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    // Forward `fcinfo->context` (the soft ErrorSaveContext installed by
    // InputFunctionCallSafe) so a recoverable parse failure `ereturn`s into the
    // soft sink instead of throwing — matching fc_int2in. On the soft path the
    // body returns `Ok(None)`; the caller checks `soft_error_occurred()` first
    // and discards this placeholder result word.
    let escontext = fcinfo.escontext_mut();
    match ok(crate::macaddr_in(&s, escontext)) {
        Some(m) => ret_macaddr(fcinfo, m),
        None => Datum::null(),
    }
}

fn fc_macaddr_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = arg_macaddr(fcinfo, 0);
    let s = String::from_utf8(crate::macaddr_out(&m)).expect("macaddr_out: valid utf8");
    ret_cstring(fcinfo, s)
}

macro_rules! fc_cmp_bool {
    ($name:ident, $core:path) => {
        fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = arg_macaddr(fcinfo, 0);
            let b = arg_macaddr(fcinfo, 1);
            Datum::from_bool($core(&a, &b))
        }
    };
}

fc_cmp_bool!(fc_macaddr_eq, crate::macaddr_eq);
fc_cmp_bool!(fc_macaddr_lt, crate::macaddr_lt);
fc_cmp_bool!(fc_macaddr_le, crate::macaddr_le);
fc_cmp_bool!(fc_macaddr_gt, crate::macaddr_gt);
fc_cmp_bool!(fc_macaddr_ge, crate::macaddr_ge);
fc_cmp_bool!(fc_macaddr_ne, crate::macaddr_ne);

fn fc_macaddr_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_macaddr(fcinfo, 0);
    let b = arg_macaddr(fcinfo, 1);
    Datum::from_i32(crate::macaddr_cmp(&a, &b))
}

fn fc_hashmacaddr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = arg_macaddr(fcinfo, 0);
    Datum::from_i32(crate::hashmacaddr(&m) as i32)
}

fn fc_hashmacaddrextended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = arg_macaddr(fcinfo, 0);
    let seed = arg_int64(fcinfo, 1) as u64;
    Datum::from_i64(crate::hashmacaddrextended(&m, seed) as i64)
}

fn fc_macaddr_not(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = arg_macaddr(fcinfo, 0);
    ret_macaddr(fcinfo, crate::macaddr_not(&m))
}

fn fc_macaddr_and(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_macaddr(fcinfo, 0);
    let b = arg_macaddr(fcinfo, 1);
    ret_macaddr(fcinfo, crate::macaddr_and(&a, &b))
}

fn fc_macaddr_or(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_macaddr(fcinfo, 0);
    let b = arg_macaddr(fcinfo, 1);
    ret_macaddr(fcinfo, crate::macaddr_or(&a, &b))
}

/// `macaddr_recv(internal) -> macaddr` (oid 2494). The raw wire-message bytes
/// ride the by-ref lane (as `oidrecv`/`uuid_recv` do); the core reads the six
/// octets off them.
fn fc_macaddr_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let src = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("macaddr_recv: by-ref message arg missing from by-ref lane");
    let m = ok(crate::macaddr_recv(src));
    ret_macaddr(fcinfo, m)
}

/// `macaddr_send(macaddr) -> bytea` (oid 2495). The core returns the six wire
/// octets; frame them into a `bytea` varlena image through the same pq_*typsend
/// path C uses, and carry it on the by-ref `Varlena` result lane.
fn fc_macaddr_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = arg_macaddr(fcinfo, 0);
    let wire = crate::macaddr_send(&m);
    let scratch = scratch_mcx();
    let mut buf =
        ok(backend_libpq_pqformat::pq_begintypsend(scratch.mcx()));
    ok(backend_libpq_pqformat::pq_sendbytes(&mut buf, &wire));
    let bytea = backend_libpq_pqformat::pq_endtypsend(buf);
    fcinfo.set_ref_result(RefPayload::Varlena(bytea.as_bytes().to_vec()));
    Datum::from_usize(0)
}

fn fc_macaddr_trunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = arg_macaddr(fcinfo, 0);
    ret_macaddr(fcinfo, crate::macaddr_trunc(&m))
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

/// Register the `mac.c` `macaddr` builtins (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs/strict/retset transcribed
/// exactly from `pg_proc.dat` (every row is `proisstrict => 't'`, non-retset).
pub fn register_mac_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(436, "macaddr_in", 1, true, false, fc_macaddr_in),
        builtin(437, "macaddr_out", 1, true, false, fc_macaddr_out),
        builtin(2494, "macaddr_recv", 1, true, false, fc_macaddr_recv),
        builtin(2495, "macaddr_send", 1, true, false, fc_macaddr_send),
        builtin(830, "macaddr_eq", 2, true, false, fc_macaddr_eq),
        builtin(831, "macaddr_lt", 2, true, false, fc_macaddr_lt),
        builtin(832, "macaddr_le", 2, true, false, fc_macaddr_le),
        builtin(833, "macaddr_gt", 2, true, false, fc_macaddr_gt),
        builtin(834, "macaddr_ge", 2, true, false, fc_macaddr_ge),
        builtin(835, "macaddr_ne", 2, true, false, fc_macaddr_ne),
        builtin(836, "macaddr_cmp", 2, true, false, fc_macaddr_cmp),
        builtin(399, "hashmacaddr", 1, true, false, fc_hashmacaddr),
        builtin(778, "hashmacaddrextended", 2, true, false, fc_hashmacaddrextended),
        builtin(3144, "macaddr_not", 1, true, false, fc_macaddr_not),
        builtin(3145, "macaddr_and", 2, true, false, fc_macaddr_and),
        builtin(3146, "macaddr_or", 2, true, false, fc_macaddr_or),
        // macaddr_trunc: trunc(macaddr) -> macaddr (mac.c:319).
        builtin(753, "macaddr_trunc", 1, true, false, fc_macaddr_trunc),
    ]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_datum::NullableDatum;

    /// Build a `macaddr` six-byte image via the registered `macaddr_in` (OID 436).
    fn in_macaddr(s: &str) -> Vec<u8> {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Cstring(s.to_string()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(436).expect("macaddr_in registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("macaddr_in produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("macaddr_in returned non-varlena: {other:?}"),
        }
    }

    /// Render a `macaddr` image via the registered `macaddr_out` (OID 437).
    fn out_macaddr(image: &[u8]) -> String {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(image.to_vec()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(437).expect("macaddr_out registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("macaddr_out produced a result") {
            RefPayload::Cstring(s) => s,
            other => panic!("macaddr_out returned non-cstring: {other:?}"),
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
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("op registered");
        (entry.func.unwrap())(&mut fcinfo).as_bool()
    }

    fn call_bin_macaddr(oid: u32, a: &[u8], b: &[u8]) -> Vec<u8> {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("op registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("op produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("non-varlena result: {other:?}"),
        }
    }

    #[test]
    fn byref_macaddr_in_out_roundtrip() {
        crate::init_seams();
        let img = in_macaddr("08:00:2b:01:02:03");
        assert_eq!(img, vec![0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
        assert_eq!(out_macaddr(&img), "08:00:2b:01:02:03");
    }

    #[test]
    fn byref_macaddr_compare() {
        crate::init_seams();
        let a = in_macaddr("08:00:2b:01:02:03");
        let b = in_macaddr("08:00:2b:01:02:04");
        assert!(call_cmp(831, &a, &b)); // lt
        assert!(call_cmp(835, &a, &b)); // ne
        assert!(!call_cmp(830, &a, &b)); // eq
        assert!(call_cmp(830, &a, &a)); // eq self
    }

    #[test]
    fn byref_macaddr_bitwise_and_trunc() {
        crate::init_seams();
        let a = in_macaddr("ff:ff:ff:ff:ff:ff");
        let b = in_macaddr("08:00:2b:01:02:03");
        // AND with all-ones yields b.
        assert_eq!(call_bin_macaddr(3145, &a, &b), b);
        // OR with all-ones yields all-ones.
        assert_eq!(call_bin_macaddr(3146, &a, &b), a);
        // trunc zeroes the last three octets.
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(b.clone()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(753).unwrap();
        (entry.func.unwrap())(&mut fcinfo);
        let tr = match fcinfo.take_ref_result().unwrap() {
            RefPayload::Varlena(v) => v,
            o => panic!("{o:?}"),
        };
        assert_eq!(tr, vec![0x08, 0x00, 0x2b, 0, 0, 0]);
    }

    #[test]
    fn byref_hashmacaddr_through_registry() {
        crate::init_seams();
        let a = in_macaddr("08:00:2b:01:02:03");
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.clone()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(399).unwrap();
        let h = (entry.func.unwrap())(&mut fcinfo).as_i32();
        // Matches the value-core hash of the same bytes.
        assert_eq!(h, crate::hashmacaddr(&arg_macaddr_from(&a)) as i32);
    }

    fn arg_macaddr_from(b: &[u8]) -> macaddr {
        macaddr { a: b[0], b: b[1], c: b[2], d: b[3], e: b[4], f: b[5] }
    }
}
