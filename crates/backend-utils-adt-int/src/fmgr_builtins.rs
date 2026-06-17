//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `int.c` functions whose argument/result types are expressible at the current
//! fmgr boundary (the scalar `int2`/`int4` I/O, comparison, arithmetic, bitwise,
//! cast, and `in_range` operators).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core (already ported in this crate),
//! and writes back the result word / by-reference payload. [`register_int_builtins`]
//! registers every row into the fmgr-core builtin table (C: `fmgr_builtins[]`),
//! so by-OID dispatch and the `fmgr_isbuiltin` fast path (relied on by early
//! catalog scankeys for `int4eq`/`int2eq`/...) resolve them. OIDs / nargs /
//! strict / retset are transcribed exactly from `pg_proc.dat`.
//!
//! NOT registered here (deferred with their carriers, see the crate docs):
//! the `int2vector` family and `generate_series_int4` (the SRF needs the funcapi
//! protocol, not the one-shot fmgr boundary). All registered builtins are
//! `proisstrict => 't'` and not retset.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};
use types_stringinfo::StringInfo;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

#[inline]
fn arg_i16(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i16 {
    fcinfo.arg(i).expect("int fn: missing arg").value.as_i16()
}
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("int fn: missing arg").value.as_i32()
}
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("int fn: missing arg").value.as_bool()
}
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("int fn: cstring arg missing from by-ref lane")
}
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("int fn: by-ref arg missing from by-ref lane")
}

#[inline]
fn ret_i16(v: i16) -> Datum {
    Datum::from_i16(v)
}
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("int fmgr scratch")
}

fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

/// `Result<T, _>::Ok` → result word; `Err` → `raise` (the one fmgr dispatch
/// point's `catch_unwind`).
macro_rules! ok_or_raise {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => raise(e),
        }
    };
}

/// Decode a `recv` builtin: build a `StringInfo` over a copy of the wire bytes
/// (charged to a scratch context that outlives the read) and run `decode`.
fn with_recv_buf<T>(
    src: &[u8],
    decode: impl FnOnce(&mut StringInfo<'_>) -> types_error::PgResult<T>,
) -> T {
    let m = scratch_mcx();
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        raise(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    ok_or_raise!(decode(&mut buf))
}

// ---------------------------------------------------------------------------
// I/O
// ---------------------------------------------------------------------------

fn fc_int2in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(ok_or_raise!(crate::int2in(arg_cstring(fcinfo, 0), None)))
}
fn fc_int2out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = crate::int2out(arg_i16(fcinfo, 0));
    ret_cstring(fcinfo, s)
}
fn fc_int2recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(with_recv_buf(arg_varlena(fcinfo, 0), crate::int2recv))
}
fn fc_int2send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let bytes = ok_or_raise!(crate::int2send(m.mcx(), arg_i16(fcinfo, 0)))
        .as_bytes()
        .to_vec();
    ret_varlena(fcinfo, bytes)
}
fn fc_int4in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok_or_raise!(crate::int4in(arg_cstring(fcinfo, 0), None)))
}
fn fc_int4out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = crate::int4out(arg_i32(fcinfo, 0));
    ret_cstring(fcinfo, s)
}
fn fc_int4recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(with_recv_buf(arg_varlena(fcinfo, 0), crate::int4recv))
}
fn fc_int4send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let bytes = ok_or_raise!(crate::int4send(m.mcx(), arg_i32(fcinfo, 0)))
        .as_bytes()
        .to_vec();
    ret_varlena(fcinfo, bytes)
}

// ---------------------------------------------------------------------------
// Casts
// ---------------------------------------------------------------------------

fn fc_i2toi4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::i2toi4(arg_i16(fcinfo, 0)))
}
fn fc_i4toi2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(ok_or_raise!(crate::i4toi2(arg_i32(fcinfo, 0))))
}
fn fc_int4_bool(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::int4_bool(arg_i32(fcinfo, 0)))
}
fn fc_bool_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::bool_int4(arg_bool(fcinfo, 0)))
}

// ---------------------------------------------------------------------------
// Comparison operators (int4 / int2 / int24 / int42)
// ---------------------------------------------------------------------------

macro_rules! cmp44 {
    ($fc:ident, $core:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            ret_bool(crate::$core(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
        }
    };
}
macro_rules! cmp22 {
    ($fc:ident, $core:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            ret_bool(crate::$core(arg_i16(fcinfo, 0), arg_i16(fcinfo, 1)))
        }
    };
}
macro_rules! cmp24 {
    ($fc:ident, $core:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            ret_bool(crate::$core(arg_i16(fcinfo, 0), arg_i32(fcinfo, 1)))
        }
    };
}
macro_rules! cmp42 {
    ($fc:ident, $core:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            ret_bool(crate::$core(arg_i32(fcinfo, 0), arg_i16(fcinfo, 1)))
        }
    };
}

cmp44!(fc_int4eq, int4eq);
cmp44!(fc_int4ne, int4ne);
cmp44!(fc_int4lt, int4lt);
cmp44!(fc_int4le, int4le);
cmp44!(fc_int4gt, int4gt);
cmp44!(fc_int4ge, int4ge);
cmp22!(fc_int2eq, int2eq);
cmp22!(fc_int2ne, int2ne);
cmp22!(fc_int2lt, int2lt);
cmp22!(fc_int2le, int2le);
cmp22!(fc_int2gt, int2gt);
cmp22!(fc_int2ge, int2ge);
cmp24!(fc_int24eq, int24eq);
cmp24!(fc_int24ne, int24ne);
cmp24!(fc_int24lt, int24lt);
cmp24!(fc_int24le, int24le);
cmp24!(fc_int24gt, int24gt);
cmp24!(fc_int24ge, int24ge);
cmp42!(fc_int42eq, int42eq);
cmp42!(fc_int42ne, int42ne);
cmp42!(fc_int42lt, int42lt);
cmp42!(fc_int42le, int42le);
cmp42!(fc_int42gt, int42gt);
cmp42!(fc_int42ge, int42ge);

// ---------------------------------------------------------------------------
// Arithmetic (int4 / int2 / int24 / int42) — fallible (overflow/divide).
// ---------------------------------------------------------------------------

// (arg1:i32, arg2:i32) -> PgResult<i32>
macro_rules! arith44 {
    ($fc:ident, $core:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            ret_i32(ok_or_raise!(crate::$core(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1))))
        }
    };
}
// (arg1:i16, arg2:i16) -> PgResult<i16>
macro_rules! arith22 {
    ($fc:ident, $core:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            ret_i16(ok_or_raise!(crate::$core(arg_i16(fcinfo, 0), arg_i16(fcinfo, 1))))
        }
    };
}
// (arg1:i16, arg2:i32) -> PgResult<i32>
macro_rules! arith24 {
    ($fc:ident, $core:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            ret_i32(ok_or_raise!(crate::$core(arg_i16(fcinfo, 0), arg_i32(fcinfo, 1))))
        }
    };
}
// (arg1:i32, arg2:i16) -> PgResult<i32>
macro_rules! arith42 {
    ($fc:ident, $core:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            ret_i32(ok_or_raise!(crate::$core(arg_i32(fcinfo, 0), arg_i16(fcinfo, 1))))
        }
    };
}

arith44!(fc_int4pl, int4pl);
arith44!(fc_int4mi, int4mi);
arith44!(fc_int4mul, int4mul);
arith44!(fc_int4div, int4div);
arith44!(fc_int4mod, int4mod);
arith22!(fc_int2pl, int2pl);
arith22!(fc_int2mi, int2mi);
arith22!(fc_int2mul, int2mul);
arith22!(fc_int2div, int2div);
arith22!(fc_int2mod, int2mod);
arith24!(fc_int24pl, int24pl);
arith24!(fc_int24mi, int24mi);
arith24!(fc_int24mul, int24mul);
arith24!(fc_int24div, int24div);
arith42!(fc_int42pl, int42pl);
arith42!(fc_int42mi, int42mi);
arith42!(fc_int42mul, int42mul);
arith42!(fc_int42div, int42div);

// Unary / inc — fallible (overflow).
fn fc_int4um(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok_or_raise!(crate::int4um(arg_i32(fcinfo, 0))))
}
fn fc_int4up(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::int4up(arg_i32(fcinfo, 0)))
}
fn fc_int4inc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok_or_raise!(crate::int4inc(arg_i32(fcinfo, 0))))
}
fn fc_int2um(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(ok_or_raise!(crate::int2um(arg_i16(fcinfo, 0))))
}
fn fc_int2up(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(crate::int2up(arg_i16(fcinfo, 0)))
}
fn fc_int4abs(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok_or_raise!(crate::int4abs(arg_i32(fcinfo, 0))))
}
fn fc_int2abs(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(ok_or_raise!(crate::int2abs(arg_i16(fcinfo, 0))))
}
fn fc_int4gcd(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok_or_raise!(crate::int4gcd(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1))))
}
fn fc_int4lcm(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(ok_or_raise!(crate::int4lcm(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1))))
}

// larger / smaller — infallible.
fn fc_int4larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::int4larger(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_int4smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::int4smaller(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_int2larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(crate::int2larger(arg_i16(fcinfo, 0), arg_i16(fcinfo, 1)))
}
fn fc_int2smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(crate::int2smaller(arg_i16(fcinfo, 0), arg_i16(fcinfo, 1)))
}

// ---------------------------------------------------------------------------
// Bitwise — infallible. (shifts take an i32 shift count.)
// ---------------------------------------------------------------------------

fn fc_int4and(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::int4and(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_int4or(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::int4or(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_int4xor(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::int4xor(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_int4not(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::int4not(arg_i32(fcinfo, 0)))
}
fn fc_int4shl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::int4shl(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_int4shr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::int4shr(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_int2and(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(crate::int2and(arg_i16(fcinfo, 0), arg_i16(fcinfo, 1)))
}
fn fc_int2or(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(crate::int2or(arg_i16(fcinfo, 0), arg_i16(fcinfo, 1)))
}
fn fc_int2xor(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(crate::int2xor(arg_i16(fcinfo, 0), arg_i16(fcinfo, 1)))
}
fn fc_int2not(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(crate::int2not(arg_i16(fcinfo, 0)))
}
fn fc_int2shl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(crate::int2shl(arg_i16(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_int2shr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i16(crate::int2shr(arg_i16(fcinfo, 0), arg_i32(fcinfo, 1)))
}

// ---------------------------------------------------------------------------
// in_range support functions: (val, base, offset, sub:bool, less:bool) -> bool.
// ---------------------------------------------------------------------------

fn fc_in_range_int4_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok_or_raise!(crate::in_range_int4_int4(
        arg_i32(fcinfo, 0),
        arg_i32(fcinfo, 1),
        arg_i32(fcinfo, 2),
        arg_bool(fcinfo, 3),
        arg_bool(fcinfo, 4),
    )))
}
fn fc_in_range_int4_int2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok_or_raise!(crate::in_range_int4_int2(
        arg_i32(fcinfo, 0),
        arg_i32(fcinfo, 1),
        arg_i16(fcinfo, 2),
        arg_bool(fcinfo, 3),
        arg_bool(fcinfo, 4),
    )))
}
fn fc_in_range_int2_int2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok_or_raise!(crate::in_range_int2_int2(
        arg_i16(fcinfo, 0),
        arg_i16(fcinfo, 1),
        arg_i16(fcinfo, 2),
        arg_bool(fcinfo, 3),
        arg_bool(fcinfo, 4),
    )))
}
fn fc_in_range_int2_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(ok_or_raise!(crate::in_range_int2_int4(
        arg_i16(fcinfo, 0),
        arg_i16(fcinfo, 1),
        arg_i32(fcinfo, 2),
        arg_bool(fcinfo, 3),
        arg_bool(fcinfo, 4),
    )))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict: true,
        retset: false,
        func: Some(func),
    }
}

/// Register every scalar `int.c` builtin (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs from `pg_proc.dat`; all
/// are `proisstrict => 't'` and not retset.
///
/// `in_range_int2_int8` / `in_range_int4_int8` are NOT registered: the
/// `int8`-offset cores live in this crate but the offset arg is an `int8`
/// (i64), which `int.c` delegates to `int8` arithmetic — they are registered by
/// the `int8.c` owner alongside the other `int8` `in_range`s. (Kept here as
/// value cores for that owner to call.)
pub fn register_int_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- I/O ----
        builtin(38, "int2in", 1, fc_int2in),
        builtin(39, "int2out", 1, fc_int2out),
        builtin(2404, "int2recv", 1, fc_int2recv),
        builtin(2405, "int2send", 1, fc_int2send),
        builtin(42, "int4in", 1, fc_int4in),
        builtin(43, "int4out", 1, fc_int4out),
        builtin(2406, "int4recv", 1, fc_int4recv),
        builtin(2407, "int4send", 1, fc_int4send),
        // ---- casts ----
        builtin(313, "i2toi4", 1, fc_i2toi4),
        builtin(314, "i4toi2", 1, fc_i4toi2),
        builtin(2557, "int4_bool", 1, fc_int4_bool),
        builtin(2558, "bool_int4", 1, fc_bool_int4),
        // ---- comparison operators ----
        builtin(65, "int4eq", 2, fc_int4eq),
        builtin(144, "int4ne", 2, fc_int4ne),
        builtin(66, "int4lt", 2, fc_int4lt),
        builtin(149, "int4le", 2, fc_int4le),
        builtin(147, "int4gt", 2, fc_int4gt),
        builtin(150, "int4ge", 2, fc_int4ge),
        builtin(63, "int2eq", 2, fc_int2eq),
        builtin(145, "int2ne", 2, fc_int2ne),
        builtin(64, "int2lt", 2, fc_int2lt),
        builtin(148, "int2le", 2, fc_int2le),
        builtin(146, "int2gt", 2, fc_int2gt),
        builtin(151, "int2ge", 2, fc_int2ge),
        builtin(158, "int24eq", 2, fc_int24eq),
        builtin(164, "int24ne", 2, fc_int24ne),
        builtin(160, "int24lt", 2, fc_int24lt),
        builtin(166, "int24le", 2, fc_int24le),
        builtin(162, "int24gt", 2, fc_int24gt),
        builtin(168, "int24ge", 2, fc_int24ge),
        builtin(159, "int42eq", 2, fc_int42eq),
        builtin(165, "int42ne", 2, fc_int42ne),
        builtin(161, "int42lt", 2, fc_int42lt),
        builtin(167, "int42le", 2, fc_int42le),
        builtin(163, "int42gt", 2, fc_int42gt),
        builtin(169, "int42ge", 2, fc_int42ge),
        // ---- arithmetic ----
        builtin(177, "int4pl", 2, fc_int4pl),
        builtin(181, "int4mi", 2, fc_int4mi),
        builtin(141, "int4mul", 2, fc_int4mul),
        builtin(154, "int4div", 2, fc_int4div),
        builtin(156, "int4mod", 2, fc_int4mod),
        builtin(176, "int2pl", 2, fc_int2pl),
        builtin(180, "int2mi", 2, fc_int2mi),
        builtin(152, "int2mul", 2, fc_int2mul),
        builtin(153, "int2div", 2, fc_int2div),
        builtin(155, "int2mod", 2, fc_int2mod),
        builtin(178, "int24pl", 2, fc_int24pl),
        builtin(182, "int24mi", 2, fc_int24mi),
        builtin(170, "int24mul", 2, fc_int24mul),
        builtin(172, "int24div", 2, fc_int24div),
        builtin(179, "int42pl", 2, fc_int42pl),
        builtin(183, "int42mi", 2, fc_int42mi),
        builtin(171, "int42mul", 2, fc_int42mul),
        builtin(173, "int42div", 2, fc_int42div),
        builtin(212, "int4um", 1, fc_int4um),
        builtin(1912, "int4up", 1, fc_int4up),
        builtin(766, "int4inc", 1, fc_int4inc),
        builtin(213, "int2um", 1, fc_int2um),
        builtin(1911, "int2up", 1, fc_int2up),
        builtin(1251, "int4abs", 1, fc_int4abs),
        builtin(1253, "int2abs", 1, fc_int2abs),
        builtin(5044, "int4gcd", 2, fc_int4gcd),
        builtin(5046, "int4lcm", 2, fc_int4lcm),
        builtin(768, "int4larger", 2, fc_int4larger),
        builtin(769, "int4smaller", 2, fc_int4smaller),
        builtin(770, "int2larger", 2, fc_int2larger),
        builtin(771, "int2smaller", 2, fc_int2smaller),
        // ---- bitwise ----
        builtin(1898, "int4and", 2, fc_int4and),
        builtin(1899, "int4or", 2, fc_int4or),
        builtin(1900, "int4xor", 2, fc_int4xor),
        builtin(1901, "int4not", 1, fc_int4not),
        builtin(1902, "int4shl", 2, fc_int4shl),
        builtin(1903, "int4shr", 2, fc_int4shr),
        builtin(1892, "int2and", 2, fc_int2and),
        builtin(1893, "int2or", 2, fc_int2or),
        builtin(1894, "int2xor", 2, fc_int2xor),
        builtin(1895, "int2not", 1, fc_int2not),
        builtin(1896, "int2shl", 2, fc_int2shl),
        builtin(1897, "int2shr", 2, fc_int2shr),
        // ---- in_range ----
        builtin(4128, "in_range_int4_int4", 5, fc_in_range_int4_int4),
        builtin(4129, "in_range_int4_int2", 5, fc_in_range_int4_int2),
        builtin(4132, "in_range_int2_int2", 5, fc_in_range_int2_int2),
        builtin(4131, "in_range_int2_int4", 5, fc_in_range_int2_int4),
    ]);
}
