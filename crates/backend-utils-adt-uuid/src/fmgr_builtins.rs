//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `uuid.c` functions whose argument/result types are expressible at the
//! current fmgr boundary (the scalar `uuid` I/O, comparison, and hash
//! functions).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result word / by-reference payload. [`register_uuid_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`.
//!
//! # The by-reference `uuid` convention
//!
//! `uuid` is a pass-by-reference, FIXED-LENGTH (16-byte) type (`typlen = 16`,
//! `typbyval = f`); it is NOT a varlena. Its canonical `ByRef` byte image is the
//! raw `pg_uuid_t.data` — exactly `UUID_LEN` bytes, with no varlena header. So
//! a `uuid` ARG arrives as `fcinfo.ref_arg(i) == Some(RefPayload::Varlena(16
//! bytes))` carrying that raw image, and a `uuid` RESULT is set the same way.
//! The value cores (`uuid_in`/`uuid_eq`/...) operate on `&pg_uuid_t` (the raw
//! 16 bytes), so the wrapper reconstructs `pg_uuid_t` from the ref-lane bytes
//! and writes the raw `data` back out — symmetric on the arg and result lanes.
//!
//! `uuid_recv` takes `internal` (a `StringInfo` wire buffer): the raw message
//! bytes ride the by-ref lane as `Varlena`, exactly as `oidrecv` does.
//! `uuid_send` returns `bytea`: its full varlena image rides the result lane.

use types_datetime::Interval;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use types_stringinfo::StringInfo;
use types_uuid::{pg_uuid_t, UUID_LEN};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_UUID_P(i)` → `DatumGetUUIDP`: reconstruct the `pg_uuid_t` from the
/// raw `UUID_LEN`-byte image on the by-reference side channel.
#[inline]
fn arg_uuid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> pg_uuid_t {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("uuid fn: by-ref `uuid` arg missing from by-ref lane");
    assert_eq!(
        bytes.len(),
        UUID_LEN,
        "uuid fn: by-ref `uuid` arg must be exactly {UUID_LEN} bytes"
    );
    let mut data = [0u8; UUID_LEN];
    data.copy_from_slice(bytes);
    pg_uuid_t { data }
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("uuid fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_POINTER(i)` for a `StringInfo` (the `uuid_recv` wire buffer): the
/// raw message bytes on the by-ref lane.
#[inline]
fn arg_msg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("uuid fn: by-ref message arg missing from by-ref lane")
}

/// `PG_GETARG_INT64(i)`: arg `i`'s full word as a signed 64-bit int.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("uuid fn: missing arg").value.as_i64()
}

/// `PG_GETARG_INTERVAL_P(i)`: decode an `interval` arg from its 16-byte by-ref
/// image on the side channel (`time:i64`, `day:i32`, `month:i32`, all LE) — the
/// same POD layout the `datetime` fmgr adapters use.
#[inline]
fn arg_interval(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Interval {
    let b = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("uuid fn: by-ref `interval` arg missing from by-ref lane");
    assert!(b.len() >= 16, "uuid fn: interval image must be >= 16 bytes");
    Interval {
        time: i64::from_le_bytes(b[0..8].try_into().expect("interval image >= 16 bytes")),
        day: i32::from_le_bytes(b[8..12].try_into().expect("interval image >= 16 bytes")),
        month: i32::from_le_bytes(b[12..16].try_into().expect("interval image >= 16 bytes")),
    }
}

/// Set a `uuid` (by-reference, fixed-length) result on the by-ref lane: the raw
/// `UUID_LEN`-byte image. Returns the dummy by-value word.
#[inline]
fn ret_uuid(fcinfo: &mut FunctionCallInfoBaseData, uuid: pg_uuid_t) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(uuid.data.to_vec()));
    Datum::from_usize(0)
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// Set a `bytea` (`_send`) result on the by-ref lane: the full varlena image.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

#[inline]
fn ret_i64(v: i64) -> Datum {
    Datum::from_i64(v)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// resulting bytes are copied onto the by-ref lane before it is dropped (C: the
/// palloc'd result lives in the caller's context; here it crosses by value).
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("uuid fmgr scratch")
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

/// `uuid_in(cstring) -> uuid` (oid 2952). A hard parse is used at this boundary
/// (the soft `ErrorSaveContext` is not modeled on the fmgr frame, matching every
/// other adt `_in`); `string_to_uuid` rethrows the syntax error as `Err`.
fn fc_uuid_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let uuid = ok(crate::uuid_in(s.as_bytes(), None));
    ret_uuid(fcinfo, uuid)
}

/// `uuid_out(uuid) -> cstring` (oid 2953). The core returns the cstring bytes
/// including the trailing NUL (C's `palloc`'d buffer); strip it for the by-ref
/// `cstring` lane.
fn fc_uuid_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let uuid = arg_uuid(fcinfo, 0);
    let raw = crate::uuid_out(&uuid);
    let body = raw.strip_suffix(&[0u8]).unwrap_or(&raw);
    ret_cstring(fcinfo, String::from_utf8_lossy(body).into_owned())
}

/// `uuid_recv(internal) -> uuid` (oid 2961). The raw message bytes ride the
/// by-ref lane; rebuild a `StringInfo` over them, exactly as `oidrecv` does.
fn fc_uuid_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let src = arg_msg_bytes(fcinfo, 0);
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        raise(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    let uuid = ok(crate::uuid_recv(&mut buf));
    ret_uuid(fcinfo, uuid)
}

/// `uuid_send(uuid) -> bytea` (oid 2962). The core builds the `bytea`'s full
/// varlena image; carry it on the by-ref `Varlena` result lane.
fn fc_uuid_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let uuid = arg_uuid(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok(crate::uuid_send(m.mcx(), &uuid)).as_bytes().to_vec();
    ret_varlena(fcinfo, bytes)
}

/// Body of a binary `(uuid, uuid) -> bool` comparison builtin around a
/// `fn(&pg_uuid_t, &pg_uuid_t) -> bool` (pure) core.
macro_rules! fc_cmp_bool {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = arg_uuid(fcinfo, 0);
            let b = arg_uuid(fcinfo, 1);
            ret_bool($core(&a, &b))
        }
    };
}

fc_cmp_bool!(fc_uuid_eq, crate::uuid_eq);
fc_cmp_bool!(fc_uuid_ne, crate::uuid_ne);
fc_cmp_bool!(fc_uuid_lt, crate::uuid_lt);
fc_cmp_bool!(fc_uuid_le, crate::uuid_le);
fc_cmp_bool!(fc_uuid_gt, crate::uuid_gt);
fc_cmp_bool!(fc_uuid_ge, crate::uuid_ge);

/// `uuid_cmp(uuid, uuid) -> int4` (oid 2960): the raw `memcmp` sign.
fn fc_uuid_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_uuid(fcinfo, 0);
    let b = arg_uuid(fcinfo, 1);
    ret_i32(crate::uuid_cmp(&a, &b))
}

/// `uuid_hash(uuid) -> int4` (oid 2963).
fn fc_uuid_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let uuid = arg_uuid(fcinfo, 0);
    // C: PG_RETURN_INT32 of a uint32 hash word (reinterpret, not a range cast).
    ret_i32(crate::uuid_hash(&uuid) as i32)
}

/// `uuid_hash_extended(uuid, int8) -> int8` (oid 3412).
fn fc_uuid_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let uuid = arg_uuid(fcinfo, 0);
    let seed = arg_int64(fcinfo, 1) as u64;
    ret_i64(crate::uuid_hash_extended(&uuid, seed) as i64)
}

/// `gen_random_uuid() -> uuid` (oids 3432 / 6428). A v4 random UUID; the core
/// draws randomness through the strong-random seam (may panic if that seam is
/// unported at call time, like every other seam-backed builtin).
fn fc_gen_random_uuid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let uuid = ok(crate::gen_random_uuid());
    ret_uuid(fcinfo, uuid)
}

/// `uuidv7() -> uuid` (oid 6429). A v7 (time-ordered) UUID.
fn fc_uuidv7(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let uuid = ok(crate::uuidv7());
    ret_uuid(fcinfo, uuid)
}

/// `uuidv7(interval) -> uuid` (oid 6430). A v7 UUID with the timestamp shifted
/// by the given interval.
fn fc_uuidv7_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let shift = arg_interval(fcinfo, 0);
    let uuid = ok(crate::uuidv7_interval(&shift));
    ret_uuid(fcinfo, uuid)
}

/// `uuid_extract_timestamp(uuid) -> timestamptz` (oid 6342). Returns SQL NULL
/// (via `fcinfo->isnull`) when the UUID carries no extractable timestamp.
fn fc_uuid_extract_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let uuid = arg_uuid(fcinfo, 0);
    match crate::uuid_extract_timestamp(&uuid) {
        Some(ts) => Datum::from_i64(ts),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

/// `uuid_extract_version(uuid) -> int4` (oid 6343). Returns SQL NULL when the
/// UUID is not an RFC-9562 (variant-2) UUID.
fn fc_uuid_extract_version(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let uuid = arg_uuid(fcinfo, 0);
    match crate::uuid_extract_version(&uuid) {
        Some(v) => Datum::from_i32(v as i32),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
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

/// Register every expressible scalar `uuid.c` builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/strict/retset
/// transcribed exactly from `pg_proc.dat` (all `proisstrict => 't'`, none
/// `proretset`).
pub fn register_uuid_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- I/O ----
        builtin(2952, "uuid_in", 1, true, false, fc_uuid_in),
        builtin(2953, "uuid_out", 1, true, false, fc_uuid_out),
        builtin(2961, "uuid_recv", 1, true, false, fc_uuid_recv),
        builtin(2962, "uuid_send", 1, true, false, fc_uuid_send),
        // ---- comparison -> bool ----
        builtin(2956, "uuid_eq", 2, true, false, fc_uuid_eq),
        builtin(2959, "uuid_ne", 2, true, false, fc_uuid_ne),
        builtin(2954, "uuid_lt", 2, true, false, fc_uuid_lt),
        builtin(2955, "uuid_le", 2, true, false, fc_uuid_le),
        builtin(2958, "uuid_gt", 2, true, false, fc_uuid_gt),
        builtin(2957, "uuid_ge", 2, true, false, fc_uuid_ge),
        // ---- 3-way comparison -> int4 ----
        builtin(2960, "uuid_cmp", 2, true, false, fc_uuid_cmp),
        // ---- hash ----
        builtin(2963, "uuid_hash", 1, true, false, fc_uuid_hash),
        builtin(3412, "uuid_hash_extended", 2, true, false, fc_uuid_hash_extended),
        // ---- generation (v4 / v7) ----
        builtin(3432, "gen_random_uuid", 0, true, false, fc_gen_random_uuid),
        builtin(6428, "gen_random_uuid", 0, true, false, fc_gen_random_uuid),
        builtin(6429, "uuidv7", 0, true, false, fc_uuidv7),
        builtin(6430, "uuidv7_interval", 1, true, false, fc_uuidv7_interval),
        // ---- timestamp / version extraction ----
        builtin(6342, "uuid_extract_timestamp", 1, true, false, fc_uuid_extract_timestamp),
        builtin(6343, "uuid_extract_version", 1, true, false, fc_uuid_extract_version),
    ]);
}

// ===========================================================================
// End-to-end proof: a by-reference `uuid` builtin is genuinely callable through
// the fmgr registry.
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use types_datum::NullableDatum;
    use types_fmgr::FunctionCallInfoBaseData;

    /// Build a raw 16-byte `uuid` image from its canonical text via the
    /// registered `uuid_in` path (proving the in-function too).
    fn uuid_image(s: &str) -> Vec<u8> {
        register_uuid_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Cstring(s.to_string()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(2952).expect("uuid_in registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("uuid_in produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("uuid_in: unexpected result lane {other:?}"),
        }
    }

    /// Render a uuid image back to text through the registered `uuid_out`.
    fn uuid_text(image: &[u8]) -> String {
        register_uuid_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(image.to_vec()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(2953).expect("uuid_out registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("uuid_out produced a result") {
            RefPayload::Cstring(s) => s,
            other => panic!("uuid_out: unexpected result lane {other:?}"),
        }
    }

    fn call_cmp_bool(oid: u32, a: &[u8], b: &[u8]) -> bool {
        register_uuid_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        let d = (entry.func.unwrap())(&mut fcinfo);
        d.as_bool()
    }

    fn call_cmp_i32(oid: u32, a: &[u8], b: &[u8]) -> i32 {
        register_uuid_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        let d = (entry.func.unwrap())(&mut fcinfo);
        d.as_i32()
    }

    /// THE PROOF: `uuid_in -> uuid_out` round-trips a canonical UUID through the
    /// fmgr registry by OID, with the `uuid` value crossing on the by-reference
    /// lane as its raw 16-byte image.
    #[test]
    fn byref_uuid_io_round_trip_through_registry() {
        let s = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        let img = uuid_image(s);
        assert_eq!(img.len(), UUID_LEN);
        assert_eq!(uuid_text(&img), s);

        // A relaxed input form (braces + no dashes) renders canonically.
        let img2 = uuid_image("{a0eebc999c0b4ef8bb6d6bb9bd380a11}");
        assert_eq!(uuid_text(&img2), s);
        assert_eq!(img, img2);
    }

    /// Comparison operators and `uuid_cmp` through the registry by OID.
    #[test]
    fn byref_uuid_compare_through_registry() {
        let lo = uuid_image("00000000-0000-0000-0000-000000000001");
        let hi = uuid_image("00000000-0000-0000-0000-000000000002");

        // uuid_eq 2956, ne 2959, lt 2954, le 2955, gt 2958, ge 2957.
        assert!(!call_cmp_bool(2956, &lo, &hi)); // lo == hi -> false
        assert!(call_cmp_bool(2959, &lo, &hi)); //  lo != hi -> true
        assert!(call_cmp_bool(2954, &lo, &hi)); //  lo <  hi -> true
        assert!(call_cmp_bool(2955, &lo, &hi)); //  lo <= hi -> true
        assert!(!call_cmp_bool(2958, &lo, &hi)); // lo >  hi -> false
        assert!(!call_cmp_bool(2957, &lo, &hi)); // lo >= hi -> false
        assert!(call_cmp_bool(2956, &lo, &lo)); //  lo == lo -> true

        // uuid_cmp 2960: -1 / 0 / 1 (sign of memcmp).
        assert!(call_cmp_i32(2960, &lo, &hi) < 0);
        assert_eq!(call_cmp_i32(2960, &lo, &lo), 0);
        assert!(call_cmp_i32(2960, &hi, &lo) > 0);
    }

    /// Hash builtins through the registry by OID: equal UUIDs hash equal, and
    /// the extended hash with a seed is callable and stable.
    #[test]
    fn byref_uuid_hash_through_registry() {
        register_uuid_builtins();
        let a = uuid_image("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        let b = a.clone();

        let h = |oid: u32, img: &[u8]| -> i32 {
            let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
            fcinfo.args = vec![NullableDatum::value(Datum::null())];
            fcinfo.ref_args = vec![Some(RefPayload::Varlena(img.to_vec()))];
            let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("registered");
            (entry.func.unwrap())(&mut fcinfo).as_i32()
        };
        assert_eq!(h(2963, &a), h(2963, &b));

        // uuid_hash_extended 3412 (uuid, int8) -> int8.
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::from_i64(0)),
        ];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.clone())), None];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(3412).expect("registered");
        let h1 = (entry.func.unwrap())(&mut fcinfo).as_i64();

        let mut fcinfo2 = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo2.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::from_i64(0)),
        ];
        fcinfo2.ref_args = vec![Some(RefPayload::Varlena(b)), None];
        let entry2 = backend_utils_fmgr_core::fmgr_isbuiltin(3412).expect("registered");
        let h2 = (entry2.func.unwrap())(&mut fcinfo2).as_i64();
        assert_eq!(h1, h2);
    }

    /// `uuid_send` -> `uuid_recv` round-trips through the registry by OID.
    #[test]
    fn byref_uuid_send_recv_through_registry() {
        register_uuid_builtins();
        let s = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        let img = uuid_image(s);

        // uuid_send 2962: uuid -> bytea (full varlena image on the ref lane).
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(img.clone()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(2962).expect("uuid_send registered");
        (entry.func.unwrap())(&mut fcinfo);
        let bytea = match fcinfo.take_ref_result().expect("uuid_send result") {
            RefPayload::Varlena(b) => b,
            other => panic!("uuid_send: unexpected lane {other:?}"),
        };
        // The bytea body (after the 4-byte varlena header) is the raw 16 bytes.
        assert_eq!(&bytea[4..], &img[..]);

        // uuid_recv 2961: the wire bytes are the raw 16 bytes (pq_getmsgbytes),
        // i.e. the bytea payload sans varlena header.
        let mut fcinfo2 = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo2.args = vec![NullableDatum::value(Datum::null())];
        fcinfo2.ref_args = vec![Some(RefPayload::Varlena(bytea[4..].to_vec()))];
        let entry2 = backend_utils_fmgr_core::fmgr_isbuiltin(2961).expect("uuid_recv registered");
        (entry2.func.unwrap())(&mut fcinfo2);
        let got = match fcinfo2.take_ref_result().expect("uuid_recv result") {
            RefPayload::Varlena(b) => b,
            other => panic!("uuid_recv: unexpected lane {other:?}"),
        };
        assert_eq!(got, img);
        assert_eq!(uuid_text(&got), s);
    }
}
