//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `numeric.c` functions whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_numeric_builtins`] registers every row into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! # The by-reference `numeric` convention
//!
//! `numeric` is a pass-by-reference (varlena) type. Its values cross the fmgr
//! boundary on the by-reference side channel: a `numeric` ARG arrives as
//! `fcinfo.ref_arg(i) == Some(RefPayload::Varlena(image))` and a `numeric`
//! RESULT is set via `fcinfo.set_ref_result(RefPayload::Varlena(image))`. The
//! bare by-value word is meaningless for these (it is the null/dummy word, exactly
//! as the canonical->ABI bridge `datum_to_ref_arg`/`ref_out_to_datum` in
//! fmgr-core arranges: a `ByRef` canonical Datum becomes `(null word,
//! Some(Varlena(bytes)))` and vice-versa).
//!
//! Unlike the `text`/`bytea` family (`backend-utils-adt-varlena`), which strips
//! the 4-byte varlena header at the boundary, the `image` here is the COMPLETE
//! numeric varlena byte image INCLUDING its `VARHDRSZ` header. That is the form
//! `numeric.c`'s codec produces and consumes: `set_var_from_num` /
//! `numeric_data_from_bytes` validate `image[0..4]` against
//! `SET_VARSIZE(image.len())`, and `make_result` writes the header into
//! `buf[..VARHDRSZ]`. The bridge carries `ByRef` bytes verbatim (no strip), so a
//! `numeric` `ByRef` value's bytes ARE its full varlena image, symmetric on the
//! arg and result lanes.

use std::cmp::Ordering;

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_NUMERIC(i)`: a `numeric` arg's full varlena byte image, read from
/// the by-reference side channel (the boundary carries it un-stripped).
#[inline]
fn arg_numeric<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("numeric fn: by-ref `numeric` arg missing from by-ref lane")
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("numeric fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_INT32(i)`: the low 32 bits of arg `i`'s word, sign-extended.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("numeric fn: missing arg").value.as_i32()
}

/// `PG_GETARG_INT64(i)`: arg `i`'s full word as a signed 64-bit int.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("numeric fn: missing arg").value.as_i64()
}

/// Set a `numeric` (by-reference) result on the by-ref lane and return the dummy
/// by-value word. The bytes are the full numeric varlena image (with header).
#[inline]
fn ret_numeric(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
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
    mcx::MemoryContext::new("numeric fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
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

/// `numeric_in(cstring, oid, int4) -> numeric` (oid 1701). The `typelem` oid arg
/// (arg 1) is unused by `numeric_in`, exactly as in C; the typmod is arg 2.
fn fc_numeric_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let image = ok(crate::io::numeric_in(m.mcx(), s, typmod));
    ret_numeric(fcinfo, image.as_slice().to_vec())
}

/// `numeric_out(numeric) -> cstring` (oid 1702).
fn fc_numeric_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    let m = scratch_mcx();
    let s = ok(crate::io::numeric_out(m.mcx(), num));
    ret_cstring(fcinfo, s)
}

/// Body of a unary `numeric -> numeric` builtin around a `fn(Mcx, &[u8]) ->
/// PgResult<PgVec<u8>>` core.
macro_rules! fc_unary_numeric {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let num = arg_numeric(fcinfo, 0);
            let m = scratch_mcx();
            let image = ok($core(m.mcx(), num));
            ret_numeric(fcinfo, image.as_slice().to_vec())
        }
    };
}

fc_unary_numeric!(fc_numeric_abs, crate::ops_sql::numeric_abs);
fc_unary_numeric!(fc_numeric_uminus, crate::ops_sql::numeric_uminus);
fc_unary_numeric!(fc_numeric_uplus, crate::ops_sql::numeric_uplus);

/// Body of a binary `(numeric, numeric) -> numeric` builtin around a
/// `fn(Mcx, &[u8], &[u8]) -> PgResult<PgVec<u8>>` core.
macro_rules! fc_binary_numeric {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = arg_numeric(fcinfo, 0);
            let b = arg_numeric(fcinfo, 1);
            let m = scratch_mcx();
            let image = ok($core(m.mcx(), a, b));
            ret_numeric(fcinfo, image.as_slice().to_vec())
        }
    };
}

fc_binary_numeric!(fc_numeric_add, crate::ops_sql::numeric_add);
fc_binary_numeric!(fc_numeric_sub, crate::ops_sql::numeric_sub);
fc_binary_numeric!(fc_numeric_mul, crate::ops_sql::numeric_mul);
fc_binary_numeric!(fc_numeric_div, crate::ops_sql::numeric_div);

/// Body of a binary `(numeric, numeric) -> bool` comparison builtin around a
/// `fn(&[u8], &[u8]) -> bool` (pure) core.
macro_rules! fc_cmp_bool {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = arg_numeric(fcinfo, 0);
            let b = arg_numeric(fcinfo, 1);
            ret_bool($core(a, b))
        }
    };
}

fc_cmp_bool!(fc_numeric_eq, crate::ops_sql::numeric_eq);
fc_cmp_bool!(fc_numeric_ne, crate::ops_sql::numeric_ne);
fc_cmp_bool!(fc_numeric_lt, crate::ops_sql::numeric_lt);
fc_cmp_bool!(fc_numeric_le, crate::ops_sql::numeric_le);
fc_cmp_bool!(fc_numeric_gt, crate::ops_sql::numeric_gt);
fc_cmp_bool!(fc_numeric_ge, crate::ops_sql::numeric_ge);

/// `numeric_cmp(numeric, numeric) -> int4` (oid 1769): -1/0/1.
fn fc_numeric_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_numeric(fcinfo, 0);
    let b = arg_numeric(fcinfo, 1);
    let c = match crate::ops_sql::numeric_cmp(a, b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    };
    ret_i32(c)
}

/// `hash_numeric(numeric) -> int4` (oid 432).
fn fc_hash_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    // C: PG_RETURN_INT32 of a uint32 hash word (reinterpret, not numeric range).
    ret_i32(crate::aggregate::hash_numeric(num) as i32)
}

/// `hash_numeric_extended(numeric, int8) -> int8` (oid 780).
fn fc_hash_numeric_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = arg_numeric(fcinfo, 0);
    let seed = arg_int64(fcinfo, 1) as u64;
    ret_i64(crate::aggregate::hash_numeric_extended(num, seed) as i64)
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

/// Register every expressible scalar `numeric.c` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`
/// (all of these are `proisstrict => 't'` default and none `proretset`).
pub fn register_numeric_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // I/O: cstring <-> numeric.
        builtin(1701, "numeric_in", 3, true, false, fc_numeric_in),
        builtin(1702, "numeric_out", 1, true, false, fc_numeric_out),
        // Unary numeric -> numeric.
        builtin(1704, "numeric_abs", 1, true, false, fc_numeric_abs),
        builtin(1771, "numeric_uminus", 1, true, false, fc_numeric_uminus),
        builtin(1915, "numeric_uplus", 1, true, false, fc_numeric_uplus),
        // Binary numeric arithmetic -> numeric.
        builtin(1724, "numeric_add", 2, true, false, fc_numeric_add),
        builtin(1725, "numeric_sub", 2, true, false, fc_numeric_sub),
        builtin(1726, "numeric_mul", 2, true, false, fc_numeric_mul),
        builtin(1727, "numeric_div", 2, true, false, fc_numeric_div),
        // Comparison -> bool.
        builtin(1718, "numeric_eq", 2, true, false, fc_numeric_eq),
        builtin(1719, "numeric_ne", 2, true, false, fc_numeric_ne),
        builtin(1722, "numeric_lt", 2, true, false, fc_numeric_lt),
        builtin(1723, "numeric_le", 2, true, false, fc_numeric_le),
        builtin(1720, "numeric_gt", 2, true, false, fc_numeric_gt),
        builtin(1721, "numeric_ge", 2, true, false, fc_numeric_ge),
        // 3-way comparison -> int4.
        builtin(1769, "numeric_cmp", 2, true, false, fc_numeric_cmp),
        // Hash.
        builtin(432, "hash_numeric", 1, true, false, fc_hash_numeric),
        builtin(
            780,
            "hash_numeric_extended",
            2,
            true,
            false,
            fc_hash_numeric_extended,
        ),
        // The pre-existing typmod-output (kept).
        builtin(2918, "numerictypmodout", 1, true, false, fc_numerictypmodout),
    ]);
}

/// `numerictypmodout(int4) -> cstring` (oid 2918): the typmod output function,
/// producing "(prec,scale)" or "". The core allocates a NUL-terminated cstring
/// byte buffer through `Mcx`; we strip the trailing NUL and decode to a `String`
/// for the by-ref `cstring` lane.
fn fc_numerictypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = arg_int32(fcinfo, 0);
    let m = scratch_mcx();
    let s = match crate::ops_sql::numerictypmodout(m.mcx(), typmod) {
        Ok(bytes) => {
            // Drop the trailing NUL terminator produced by PG_RETURN_CSTRING.
            let raw = bytes.as_slice();
            let body = raw.strip_suffix(&[0u8]).unwrap_or(raw);
            String::from_utf8_lossy(body).into_owned()
        }
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, s)
}

// ===========================================================================
// End-to-end proof: a by-reference `numeric` builtin is genuinely callable
// through the fmgr registry.
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use types_datum::NullableDatum;
    use types_fmgr::FunctionCallInfoBaseData;

    /// Build a fresh numeric varlena image from its decimal text via the
    /// registered `numeric_in` path (proving the in-function too).
    fn numeric_image(s: &str) -> Vec<u8> {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 3, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),   // cstring (by-ref)
            NullableDatum::value(Datum::from_u32(0)), // typelem oid (unused)
            NullableDatum::value(Datum::from_i32(-1)), // typmod = -1
        ];
        fcinfo.ref_args = vec![Some(RefPayload::Cstring(s.to_string())), None, None];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(1701)
            .expect("numeric_in registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("numeric_in produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("numeric_in: unexpected result lane {other:?}"),
        }
    }

    /// Invoke a registered by-ref numeric builtin by OID through the fmgr
    /// registry, passing `numeric` args on the by-ref lane and reading the
    /// `numeric` result back off the by-ref lane.
    fn call_binary_numeric(oid: u32, a: &[u8], b: &[u8]) -> Vec<u8> {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid)
            .expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("numeric op produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("numeric op: unexpected result lane {other:?}"),
        }
    }

    fn call_cmp_bool(oid: u32, a: &[u8], b: &[u8]) -> bool {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid)
            .expect("builtin registered");
        let d = (entry.func.unwrap())(&mut fcinfo);
        d.as_bool()
    }

    /// Render a numeric image back to text through the registered `numeric_out`.
    fn numeric_text(image: &[u8]) -> String {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(image.to_vec()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(1702)
            .expect("numeric_out registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("numeric_out produced a result") {
            RefPayload::Cstring(s) => s,
            other => panic!("numeric_out: unexpected result lane {other:?}"),
        }
    }

    /// THE PROOF: `1::numeric + 2::numeric == 3::numeric`, computed entirely
    /// through the fmgr registry by OID, with `numeric` args/result crossing on
    /// the by-reference lane.
    #[test]
    fn byref_numeric_add_through_registry() {
        let one = numeric_image("1");
        let two = numeric_image("2");
        // numeric_add oid 1724.
        let sum = call_binary_numeric(1724, &one, &two);
        assert_eq!(numeric_text(&sum), "3");
    }

    #[test]
    fn byref_numeric_arithmetic_and_compare() {
        let six = numeric_image("6");
        let four = numeric_image("4");
        // 6 - 4 = 2, 6 * 4 = 24, 6 / 4 = 1.5 (full dscale per numeric_div).
        assert_eq!(numeric_text(&call_binary_numeric(1725, &six, &four)), "2");
        assert_eq!(numeric_text(&call_binary_numeric(1726, &six, &four)), "24");
        assert_eq!(
            numeric_text(&call_binary_numeric(1727, &six, &four)),
            "1.5000000000000000"
        );
        // Comparisons (oids 1718 eq, 1722 lt, 1720 gt).
        assert!(!call_cmp_bool(1718, &six, &four)); // 6 == 4 -> false
        assert!(!call_cmp_bool(1722, &six, &four)); // 6 < 4  -> false
        assert!(call_cmp_bool(1720, &six, &four)); // 6 > 4  -> true
        assert!(call_cmp_bool(1718, &six, &six)); // 6 == 6 -> true
    }

    #[test]
    fn byref_numeric_unary_and_cmp_int() {
        let neg = numeric_image("-7");
        // numeric_abs oid 1704 -> 7; numeric_uminus oid 1771 -> 7.
        assert_eq!(numeric_text(&call_unary(1704, &neg)), "7");
        assert_eq!(numeric_text(&call_unary(1771, &neg)), "7");

        // numeric_cmp oid 1769: cmp(-7, 7) = -1, cmp(7,7)=0, cmp(7,-7)=1.
        let pos = numeric_image("7");
        assert_eq!(call_cmp_i32(1769, &neg, &pos), -1);
        assert_eq!(call_cmp_i32(1769, &pos, &pos), 0);
        assert_eq!(call_cmp_i32(1769, &pos, &neg), 1);
    }

    fn call_unary(oid: u32, a: &[u8]) -> Vec<u8> {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.to_vec()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid)
            .expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("unary numeric produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("unary numeric: unexpected result lane {other:?}"),
        }
    }

    fn call_cmp_i32(oid: u32, a: &[u8], b: &[u8]) -> i32 {
        register_numeric_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid)
            .expect("builtin registered");
        let d = (entry.func.unwrap())(&mut fcinfo);
        d.as_i32()
    }
}
