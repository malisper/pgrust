//! fmgr-shaped wrappers (`fc_<cname>`) and the registry table (`INT_BUILTINS`)
//! the fmgr-core unit consumes. int2vectorin/out and the recv/send functions
//! are not registrable yet: they need an allocation/wire convention at the
//! frame (fmgr-core / pqcomm units); their value cores live in the crate root.

use alloc::string::String;

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

#[cold]
#[inline(never)]
fn soft_context_unported(name: &str) -> ! {
    panic!("{name}: fcinfo.context soft-error demux is fmgr-core's unit (not ported)")
}

// C pallocs each cstring result into the per-row context; here the backend
// thread owns retained scratch (rules 7/10; fn_extra was measured out: its
// dyn-Any downcast is a per-row virtual type_id call). The returned Datum
// aliases the scratch: consume it before the next out-function call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; 16]> =
        const { core::cell::UnsafeCell::new([0; 16]) };
}

fn in_arg<'a>(fcinfo: &'a Fcinfo, name: &'static str) -> alloc::borrow::Cow<'a, str> {
    if fcinfo.context.is_some() {
        soft_context_unported(name);
    }
    // SAFETY: catalog arg 0 of the in-functions is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    String::from_utf8_lossy(s.to_bytes())
}

pub fn fc_int2in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let num = in_arg(fcinfo, "int2in");
    Ok(Datum::from_i16(crate::int2in(&num, None)?))
}

pub fn fc_int2out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let v = a.value.as_i16();
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::int2out(v, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_int4in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let num = in_arg(fcinfo, "int4in");
    Ok(Datum::from_i32(crate::int4in(&num, None)?))
}

pub fn fc_int4out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let v = a.value.as_i32();
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::int4out(v, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

macro_rules! fc1 {
    ($($fc:ident: $core:ident($get:ident) -> $from:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a] = fcinfo.args_n::<1>();
            Ok(Datum::$from(crate::$core(a.value.$get())))
        }
    )*};
}

macro_rules! fc1t {
    ($($fc:ident: $core:ident($get:ident) -> $from:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a] = fcinfo.args_n::<1>();
            Ok(Datum::$from(crate::$core(a.value.$get())?))
        }
    )*};
}

macro_rules! fc2 {
    ($($fc:ident: $core:ident($ga:ident, $gb:ident) -> $from:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::$from(crate::$core(a.value.$ga(), b.value.$gb())))
        }
    )*};
}

macro_rules! fc2t {
    ($($fc:ident: $core:ident($ga:ident, $gb:ident) -> $from:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::$from(crate::$core(a.value.$ga(), b.value.$gb())?))
        }
    )*};
}

macro_rules! fc_in_range {
    ($($fc:ident: $core:ident($gv:ident, $gb:ident, $go:ident);)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [v, b, o, s, l] = fcinfo.args_n::<5>();
            Ok(Datum::from_bool(crate::$core(
                v.value.$gv(),
                b.value.$gb(),
                o.value.$go(),
                s.value.as_bool(),
                l.value.as_bool(),
            )?))
        }
    )*};
}

fc1! {
    fc_i2toi4: i2toi4(as_i16) -> from_i32;
    fc_int4_bool: int4_bool(as_i32) -> from_bool;
    fc_bool_int4: bool_int4(as_bool) -> from_i32;
    fc_int4up: int4up(as_i32) -> from_i32;
    fc_int2up: int2up(as_i16) -> from_i16;
    fc_int4not: int4not(as_i32) -> from_i32;
    fc_int2not: int2not(as_i16) -> from_i16;
}

fc1t! {
    fc_i4toi2: i4toi2(as_i32) -> from_i16;
    fc_int4um: int4um(as_i32) -> from_i32;
    fc_int4inc: int4inc(as_i32) -> from_i32;
    fc_int2um: int2um(as_i16) -> from_i16;
    fc_int4abs: int4abs(as_i32) -> from_i32;
    fc_int2abs: int2abs(as_i16) -> from_i16;
}

fc2! {
    fc_int4eq: int4eq(as_i32, as_i32) -> from_bool;
    fc_int4ne: int4ne(as_i32, as_i32) -> from_bool;
    fc_int4lt: int4lt(as_i32, as_i32) -> from_bool;
    fc_int4le: int4le(as_i32, as_i32) -> from_bool;
    fc_int4gt: int4gt(as_i32, as_i32) -> from_bool;
    fc_int4ge: int4ge(as_i32, as_i32) -> from_bool;
    fc_int2eq: int2eq(as_i16, as_i16) -> from_bool;
    fc_int2ne: int2ne(as_i16, as_i16) -> from_bool;
    fc_int2lt: int2lt(as_i16, as_i16) -> from_bool;
    fc_int2le: int2le(as_i16, as_i16) -> from_bool;
    fc_int2gt: int2gt(as_i16, as_i16) -> from_bool;
    fc_int2ge: int2ge(as_i16, as_i16) -> from_bool;
    fc_int24eq: int24eq(as_i16, as_i32) -> from_bool;
    fc_int24ne: int24ne(as_i16, as_i32) -> from_bool;
    fc_int24lt: int24lt(as_i16, as_i32) -> from_bool;
    fc_int24le: int24le(as_i16, as_i32) -> from_bool;
    fc_int24gt: int24gt(as_i16, as_i32) -> from_bool;
    fc_int24ge: int24ge(as_i16, as_i32) -> from_bool;
    fc_int42eq: int42eq(as_i32, as_i16) -> from_bool;
    fc_int42ne: int42ne(as_i32, as_i16) -> from_bool;
    fc_int42lt: int42lt(as_i32, as_i16) -> from_bool;
    fc_int42le: int42le(as_i32, as_i16) -> from_bool;
    fc_int42gt: int42gt(as_i32, as_i16) -> from_bool;
    fc_int42ge: int42ge(as_i32, as_i16) -> from_bool;
    fc_int4larger: int4larger(as_i32, as_i32) -> from_i32;
    fc_int4smaller: int4smaller(as_i32, as_i32) -> from_i32;
    fc_int2larger: int2larger(as_i16, as_i16) -> from_i16;
    fc_int2smaller: int2smaller(as_i16, as_i16) -> from_i16;
    fc_int4and: int4and(as_i32, as_i32) -> from_i32;
    fc_int4or: int4or(as_i32, as_i32) -> from_i32;
    fc_int4xor: int4xor(as_i32, as_i32) -> from_i32;
    fc_int4shl: int4shl(as_i32, as_i32) -> from_i32;
    fc_int4shr: int4shr(as_i32, as_i32) -> from_i32;
    fc_int2and: int2and(as_i16, as_i16) -> from_i16;
    fc_int2or: int2or(as_i16, as_i16) -> from_i16;
    fc_int2xor: int2xor(as_i16, as_i16) -> from_i16;
    fc_int2shl: int2shl(as_i16, as_i32) -> from_i16;
    fc_int2shr: int2shr(as_i16, as_i32) -> from_i16;
}

fc2t! {
    fc_int4pl: int4pl(as_i32, as_i32) -> from_i32;
    fc_int4mi: int4mi(as_i32, as_i32) -> from_i32;
    fc_int4mul: int4mul(as_i32, as_i32) -> from_i32;
    fc_int4div: int4div(as_i32, as_i32) -> from_i32;
    fc_int4mod: int4mod(as_i32, as_i32) -> from_i32;
    fc_int2pl: int2pl(as_i16, as_i16) -> from_i16;
    fc_int2mi: int2mi(as_i16, as_i16) -> from_i16;
    fc_int2mul: int2mul(as_i16, as_i16) -> from_i16;
    fc_int2div: int2div(as_i16, as_i16) -> from_i16;
    fc_int2mod: int2mod(as_i16, as_i16) -> from_i16;
    fc_int24pl: int24pl(as_i16, as_i32) -> from_i32;
    fc_int24mi: int24mi(as_i16, as_i32) -> from_i32;
    fc_int24mul: int24mul(as_i16, as_i32) -> from_i32;
    fc_int24div: int24div(as_i16, as_i32) -> from_i32;
    fc_int42pl: int42pl(as_i32, as_i16) -> from_i32;
    fc_int42mi: int42mi(as_i32, as_i16) -> from_i32;
    fc_int42mul: int42mul(as_i32, as_i16) -> from_i32;
    fc_int42div: int42div(as_i32, as_i16) -> from_i32;
    fc_int4gcd: int4gcd(as_i32, as_i32) -> from_i32;
    fc_int4lcm: int4lcm(as_i32, as_i32) -> from_i32;
}

fc_in_range! {
    fc_in_range_int4_int4: in_range_int4_int4(as_i32, as_i32, as_i32);
    fc_in_range_int4_int2: in_range_int4_int2(as_i32, as_i32, as_i16);
    fc_in_range_int4_int8: in_range_int4_int8(as_i32, as_i32, as_i64);
    fc_in_range_int2_int4: in_range_int2_int4(as_i16, as_i16, as_i32);
    fc_in_range_int2_int2: in_range_int2_int2(as_i16, as_i16, as_i16);
    fc_in_range_int2_int8: in_range_int2_int8(as_i16, as_i16, as_i64);
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

// pg_proc.dat rows for int.c (all proisstrict, none retset). Not present:
// recv/send (2404-2407), int2vectorin/out (40/41) and int2vectorrecv/send
// (2410/2411) — see module doc; generate_series[_step]_int4 (1066/1067) and
// generate_series_int4_support (3994) ride the funcapi/planner frames.
pub const INT_BUILTINS: &[FmgrBuiltin] = &[
    b(38, "int2in", 1, fc_int2in),
    b(39, "int2out", 1, fc_int2out),
    b(42, "int4in", 1, fc_int4in),
    b(43, "int4out", 1, fc_int4out),
    b(313, "i2toi4", 1, fc_i2toi4),
    b(314, "i4toi2", 1, fc_i4toi2),
    b(2557, "int4_bool", 1, fc_int4_bool),
    b(2558, "bool_int4", 1, fc_bool_int4),
    b(65, "int4eq", 2, fc_int4eq),
    b(144, "int4ne", 2, fc_int4ne),
    b(66, "int4lt", 2, fc_int4lt),
    b(149, "int4le", 2, fc_int4le),
    b(147, "int4gt", 2, fc_int4gt),
    b(150, "int4ge", 2, fc_int4ge),
    b(63, "int2eq", 2, fc_int2eq),
    b(145, "int2ne", 2, fc_int2ne),
    b(64, "int2lt", 2, fc_int2lt),
    b(148, "int2le", 2, fc_int2le),
    b(146, "int2gt", 2, fc_int2gt),
    b(151, "int2ge", 2, fc_int2ge),
    b(158, "int24eq", 2, fc_int24eq),
    b(164, "int24ne", 2, fc_int24ne),
    b(160, "int24lt", 2, fc_int24lt),
    b(166, "int24le", 2, fc_int24le),
    b(162, "int24gt", 2, fc_int24gt),
    b(168, "int24ge", 2, fc_int24ge),
    b(159, "int42eq", 2, fc_int42eq),
    b(165, "int42ne", 2, fc_int42ne),
    b(161, "int42lt", 2, fc_int42lt),
    b(167, "int42le", 2, fc_int42le),
    b(163, "int42gt", 2, fc_int42gt),
    b(169, "int42ge", 2, fc_int42ge),
    b(177, "int4pl", 2, fc_int4pl),
    b(181, "int4mi", 2, fc_int4mi),
    b(141, "int4mul", 2, fc_int4mul),
    b(154, "int4div", 2, fc_int4div),
    b(156, "int4mod", 2, fc_int4mod),
    b(176, "int2pl", 2, fc_int2pl),
    b(180, "int2mi", 2, fc_int2mi),
    b(152, "int2mul", 2, fc_int2mul),
    b(153, "int2div", 2, fc_int2div),
    b(155, "int2mod", 2, fc_int2mod),
    b(178, "int24pl", 2, fc_int24pl),
    b(182, "int24mi", 2, fc_int24mi),
    b(170, "int24mul", 2, fc_int24mul),
    b(172, "int24div", 2, fc_int24div),
    b(179, "int42pl", 2, fc_int42pl),
    b(183, "int42mi", 2, fc_int42mi),
    b(171, "int42mul", 2, fc_int42mul),
    b(173, "int42div", 2, fc_int42div),
    b(212, "int4um", 1, fc_int4um),
    b(1912, "int4up", 1, fc_int4up),
    b(766, "int4inc", 1, fc_int4inc),
    b(213, "int2um", 1, fc_int2um),
    b(1911, "int2up", 1, fc_int2up),
    b(1251, "int4abs", 1, fc_int4abs),
    b(1253, "int2abs", 1, fc_int2abs),
    b(5044, "int4gcd", 2, fc_int4gcd),
    b(5046, "int4lcm", 2, fc_int4lcm),
    b(768, "int4larger", 2, fc_int4larger),
    b(769, "int4smaller", 2, fc_int4smaller),
    b(770, "int2larger", 2, fc_int2larger),
    b(771, "int2smaller", 2, fc_int2smaller),
    b(1898, "int4and", 2, fc_int4and),
    b(1899, "int4or", 2, fc_int4or),
    b(1900, "int4xor", 2, fc_int4xor),
    b(1901, "int4not", 1, fc_int4not),
    b(1902, "int4shl", 2, fc_int4shl),
    b(1903, "int4shr", 2, fc_int4shr),
    b(1892, "int2and", 2, fc_int2and),
    b(1893, "int2or", 2, fc_int2or),
    b(1894, "int2xor", 2, fc_int2xor),
    b(1895, "int2not", 1, fc_int2not),
    b(1896, "int2shl", 2, fc_int2shl),
    b(1897, "int2shr", 2, fc_int2shr),
    // mod/abs pg_proc aliases (proname mod/abs, same prosrc).
    b(940, "int2mod", 2, fc_int2mod),
    b(941, "int4mod", 2, fc_int4mod),
    b(1397, "int4abs", 1, fc_int4abs),
    b(1398, "int2abs", 1, fc_int2abs),
    b(4128, "in_range_int4_int4", 5, fc_in_range_int4_int4),
    b(4129, "in_range_int4_int2", 5, fc_in_range_int4_int2),
    b(4127, "in_range_int4_int8", 5, fc_in_range_int4_int8),
    b(4131, "in_range_int2_int4", 5, fc_in_range_int2_int4),
    b(4132, "in_range_int2_int2", 5, fc_in_range_int2_int2),
    b(4130, "in_range_int2_int8", 5, fc_in_range_int2_int8),
];
