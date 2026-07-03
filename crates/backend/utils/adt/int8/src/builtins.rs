//! fmgr-shaped wrappers (`fc_<cname>`) and the registry table (`INT8_BUILTINS`)
//! the fmgr-core unit consumes. Not here: the prosupport body int8inc_support
//! (6236). recv/send (2408/2409) ride the binary-wire fmgr frame
//! (types_fmgr::wire).

use alloc::string::String;

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

pub fn fc_int8recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_i64(crate::int8recv(buf)?))
}

pub fn fc_int8send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::int8send(mcx, a.value.as_i64())?))
}

// C pallocs the cstring result into the per-row context; here the backend
// thread owns retained scratch (rules 7/10; fn_extra was measured out: its
// dyn-Any downcast is a per-row virtual type_id call). The returned Datum
// aliases the scratch: consume it before the next out-function call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; 24]> =
        const { core::cell::UnsafeCell::new([0; 24]) };
}

pub fn fc_int8in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of int8in is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let num = String::from_utf8_lossy(s.to_bytes());
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_i64(crate::int8in(&num, esc)?))
}

pub fn fc_int8out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let val = a.value.as_i64();
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::int8out(val, buf);
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

// C's int8inc_any/int8dec_any/int8inc_float8_float8 read only arg0 off wider
// frames; arity follows pg_proc (2/2/3), the read stays arg0.
macro_rules! fc_agg_inc {
    ($($fc:ident: $core:ident, $n:literal;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let args = fcinfo.args_n::<$n>();
            Ok(Datum::from_i64(crate::$core(args[0].value.as_i64())?))
        }
    )*};
}

fc1! {
    fc_int8up: int8up(as_i64) -> from_i64;
    fc_int8not: int8not(as_i64) -> from_i64;
    fc_int48: int48(as_i32) -> from_i64;
    fc_int28: int28(as_i16) -> from_i64;
    fc_i8tod: i8tod(as_i64) -> from_f64;
    fc_i8tof: i8tof(as_i64) -> from_f32;
    fc_oidtoi8: oidtoi8(as_oid) -> from_i64;
}

fc1t! {
    fc_int8um: int8um(as_i64) -> from_i64;
    fc_int8abs: int8abs(as_i64) -> from_i64;
    fc_int8inc: int8inc(as_i64) -> from_i64;
    fc_int8dec: int8dec(as_i64) -> from_i64;
    fc_int84: int84(as_i64) -> from_i32;
    fc_int82: int82(as_i64) -> from_i16;
    fc_dtoi8: dtoi8(as_f64) -> from_i64;
    fc_ftoi8: ftoi8(as_f32) -> from_i64;
    fc_i8tooid: i8tooid(as_i64) -> from_oid;
}

// C home is hashfunc.c (no hash-AM adt crate yet); the low half xors the
// (sign-complemented) high half so int2/int4/int8 hash equal for equal values.
pub fn fc_hashint8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let val = a.value.as_i64();
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    let lohalf = lohalf ^ if val >= 0 { hihalf } else { !hihalf };
    Ok(Datum::from_u32(::hashfn::hash_bytes_uint32(lohalf)))
}

fc2! {
    fc_int8eq: int8eq(as_i64, as_i64) -> from_bool;
    fc_int8ne: int8ne(as_i64, as_i64) -> from_bool;
    fc_int8lt: int8lt(as_i64, as_i64) -> from_bool;
    fc_int8gt: int8gt(as_i64, as_i64) -> from_bool;
    fc_int8le: int8le(as_i64, as_i64) -> from_bool;
    fc_int8ge: int8ge(as_i64, as_i64) -> from_bool;
    fc_int84eq: int84eq(as_i64, as_i32) -> from_bool;
    fc_int84ne: int84ne(as_i64, as_i32) -> from_bool;
    fc_int84lt: int84lt(as_i64, as_i32) -> from_bool;
    fc_int84gt: int84gt(as_i64, as_i32) -> from_bool;
    fc_int84le: int84le(as_i64, as_i32) -> from_bool;
    fc_int84ge: int84ge(as_i64, as_i32) -> from_bool;
    fc_int48eq: int48eq(as_i32, as_i64) -> from_bool;
    fc_int48ne: int48ne(as_i32, as_i64) -> from_bool;
    fc_int48lt: int48lt(as_i32, as_i64) -> from_bool;
    fc_int48gt: int48gt(as_i32, as_i64) -> from_bool;
    fc_int48le: int48le(as_i32, as_i64) -> from_bool;
    fc_int48ge: int48ge(as_i32, as_i64) -> from_bool;
    fc_int82eq: int82eq(as_i64, as_i16) -> from_bool;
    fc_int82ne: int82ne(as_i64, as_i16) -> from_bool;
    fc_int82lt: int82lt(as_i64, as_i16) -> from_bool;
    fc_int82gt: int82gt(as_i64, as_i16) -> from_bool;
    fc_int82le: int82le(as_i64, as_i16) -> from_bool;
    fc_int82ge: int82ge(as_i64, as_i16) -> from_bool;
    fc_int28eq: int28eq(as_i16, as_i64) -> from_bool;
    fc_int28ne: int28ne(as_i16, as_i64) -> from_bool;
    fc_int28lt: int28lt(as_i16, as_i64) -> from_bool;
    fc_int28gt: int28gt(as_i16, as_i64) -> from_bool;
    fc_int28le: int28le(as_i16, as_i64) -> from_bool;
    fc_int28ge: int28ge(as_i16, as_i64) -> from_bool;
    fc_int8larger: int8larger(as_i64, as_i64) -> from_i64;
    fc_int8smaller: int8smaller(as_i64, as_i64) -> from_i64;
    fc_int8and: int8and(as_i64, as_i64) -> from_i64;
    fc_int8or: int8or(as_i64, as_i64) -> from_i64;
    fc_int8xor: int8xor(as_i64, as_i64) -> from_i64;
    fc_int8shl: int8shl(as_i64, as_i32) -> from_i64;
    fc_int8shr: int8shr(as_i64, as_i32) -> from_i64;
}

fc2t! {
    fc_int8pl: int8pl(as_i64, as_i64) -> from_i64;
    fc_int8mi: int8mi(as_i64, as_i64) -> from_i64;
    fc_int8mul: int8mul(as_i64, as_i64) -> from_i64;
    fc_int8div: int8div(as_i64, as_i64) -> from_i64;
    fc_int8mod: int8mod(as_i64, as_i64) -> from_i64;
    fc_int8gcd: int8gcd(as_i64, as_i64) -> from_i64;
    fc_int8lcm: int8lcm(as_i64, as_i64) -> from_i64;
    fc_int84pl: int84pl(as_i64, as_i32) -> from_i64;
    fc_int84mi: int84mi(as_i64, as_i32) -> from_i64;
    fc_int84mul: int84mul(as_i64, as_i32) -> from_i64;
    fc_int84div: int84div(as_i64, as_i32) -> from_i64;
    fc_int48pl: int48pl(as_i32, as_i64) -> from_i64;
    fc_int48mi: int48mi(as_i32, as_i64) -> from_i64;
    fc_int48mul: int48mul(as_i32, as_i64) -> from_i64;
    fc_int48div: int48div(as_i32, as_i64) -> from_i64;
    fc_int82pl: int82pl(as_i64, as_i16) -> from_i64;
    fc_int82mi: int82mi(as_i64, as_i16) -> from_i64;
    fc_int82mul: int82mul(as_i64, as_i16) -> from_i64;
    fc_int82div: int82div(as_i64, as_i16) -> from_i64;
    fc_int28pl: int28pl(as_i16, as_i64) -> from_i64;
    fc_int28mi: int28mi(as_i16, as_i64) -> from_i64;
    fc_int28mul: int28mul(as_i16, as_i64) -> from_i64;
    fc_int28div: int28div(as_i16, as_i64) -> from_i64;
}

fc_agg_inc! {
    fc_int8inc_any: int8inc_any, 2;
    fc_int8dec_any: int8dec_any, 2;
    fc_int8inc_float8_float8: int8inc_float8_float8, 3;
}

pub fn fc_in_range_int8_int8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [v, b, o, s, l] = fcinfo.args_n::<5>();
    Ok(Datum::from_bool(crate::in_range_int8_int8(
        v.value.as_i64(),
        b.value.as_i64(),
        o.value.as_i64(),
        s.value.as_bool(),
        l.value.as_bool(),
    )?))
}

// generate_series_step_int8 (OIDs 1068 3-arg / 1069 2-arg share the C body;
// PG_NARGS demuxes) over the funcapi ValuePerCall frame.
pub fn fc_generate_series_step_int8(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("generate_series_int8: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let start = fcinfo.arg(0).as_i64();
        let finish = fcinfo.arg(1).as_i64();
        let step = if fcinfo.nargs() == 3 { fcinfo.arg(2).as_i64() } else { 1 };
        let state = crate::GenerateSeriesInt8::new(start, finish, step)?;
        let fctx = ::funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(alloc::boxed::Box::new(state));
    }
    let next = ::funcapi::per_MultiFuncCall(flinfo)
        .user_fctx
        .as_mut()
        .expect("generate_series_int8: user_fctx set at first call")
        .downcast_mut::<crate::GenerateSeriesInt8>()
        .expect("generate_series_int8: user_fctx is GenerateSeriesInt8")
        .next();
    match next {
        Some(v) => Ok(::funcapi::srf_return_next(flinfo, fcinfo, Datum::from_i64(v))),
        None => Ok(::funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

// generate_series_int8_support (OID 3995): SupportRequestRows over all-Const
// args; anything else returns NULL so callers fall back (Param estimation
// unported, matching the int4 support body).
pub fn fc_generate_series_int8_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let p = a.value.as_usize() as *mut ();
    // SAFETY: prosupport contract — the internal arg points at a live
    // tag-first support-request node exclusively owned by this call.
    let Some(req) = (unsafe { ::types_nodes::supportnodes::support_request_rows_mut(p) }) else {
        return Ok(Datum::from_usize(0));
    };
    let args = match req.node.and_then(|n| n.as_func_expr()) {
        Some(fe) => &fe.args,
        None => return Ok(Datum::from_usize(0)),
    };
    let mut vals = [1i64; 3];
    for (i, arg) in args.iter().enumerate() {
        match arg.as_const() {
            Some(c) if c.constisnull => {
                req.rows = 0.0;
                return Ok(Datum::from_usize(p as usize));
            }
            Some(c) => vals[i] = c.constvalue.as_i64(),
            None => return Ok(Datum::from_usize(0)),
        }
    }
    match crate::generate_series_int8_rows(vals[0] as f64, vals[1] as f64, vals[2] as f64) {
        Some(rows) => {
            req.rows = rows;
            Ok(Datum::from_usize(p as usize))
        }
        None => Ok(Datum::from_usize(0)),
    }
}

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
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

// pg_proc.dat rows for int8.c.
pub const INT8_BUILTINS: &[FmgrBuiltin] = &[
    srf(1068, "generate_series_step_int8", 3, fc_generate_series_step_int8),
    srf(1069, "generate_series_int8", 2, fc_generate_series_step_int8),
    b(3995, "generate_series_int8_support", 1, fc_generate_series_int8_support),
    b(460, "int8in", 1, fc_int8in),
    b(461, "int8out", 1, fc_int8out),
    b(2408, "int8recv", 1, fc_int8recv),
    b(2409, "int8send", 1, fc_int8send),
    b(949, "hashint8", 1, fc_hashint8),
    b(467, "int8eq", 2, fc_int8eq),
    b(468, "int8ne", 2, fc_int8ne),
    b(469, "int8lt", 2, fc_int8lt),
    b(470, "int8gt", 2, fc_int8gt),
    b(471, "int8le", 2, fc_int8le),
    b(472, "int8ge", 2, fc_int8ge),
    b(474, "int84eq", 2, fc_int84eq),
    b(475, "int84ne", 2, fc_int84ne),
    b(476, "int84lt", 2, fc_int84lt),
    b(477, "int84gt", 2, fc_int84gt),
    b(478, "int84le", 2, fc_int84le),
    b(479, "int84ge", 2, fc_int84ge),
    b(852, "int48eq", 2, fc_int48eq),
    b(853, "int48ne", 2, fc_int48ne),
    b(854, "int48lt", 2, fc_int48lt),
    b(855, "int48gt", 2, fc_int48gt),
    b(856, "int48le", 2, fc_int48le),
    b(857, "int48ge", 2, fc_int48ge),
    b(1856, "int82eq", 2, fc_int82eq),
    b(1857, "int82ne", 2, fc_int82ne),
    b(1858, "int82lt", 2, fc_int82lt),
    b(1859, "int82gt", 2, fc_int82gt),
    b(1860, "int82le", 2, fc_int82le),
    b(1861, "int82ge", 2, fc_int82ge),
    b(1850, "int28eq", 2, fc_int28eq),
    b(1851, "int28ne", 2, fc_int28ne),
    b(1852, "int28lt", 2, fc_int28lt),
    b(1853, "int28gt", 2, fc_int28gt),
    b(1854, "int28le", 2, fc_int28le),
    b(1855, "int28ge", 2, fc_int28ge),
    b(462, "int8um", 1, fc_int8um),
    b(1910, "int8up", 1, fc_int8up),
    b(463, "int8pl", 2, fc_int8pl),
    b(464, "int8mi", 2, fc_int8mi),
    b(465, "int8mul", 2, fc_int8mul),
    b(466, "int8div", 2, fc_int8div),
    b(1230, "int8abs", 1, fc_int8abs),
    b(945, "int8mod", 2, fc_int8mod),
    b(5045, "int8gcd", 2, fc_int8gcd),
    b(5047, "int8lcm", 2, fc_int8lcm),
    b(1219, "int8inc", 1, fc_int8inc),
    b(3546, "int8dec", 1, fc_int8dec),
    b(2804, "int8inc_any", 2, fc_int8inc_any),
    b(3547, "int8dec_any", 2, fc_int8dec_any),
    b(2805, "int8inc_float8_float8", 3, fc_int8inc_float8_float8),
    b(1236, "int8larger", 2, fc_int8larger),
    b(1237, "int8smaller", 2, fc_int8smaller),
    b(1274, "int84pl", 2, fc_int84pl),
    b(1275, "int84mi", 2, fc_int84mi),
    b(1276, "int84mul", 2, fc_int84mul),
    b(1277, "int84div", 2, fc_int84div),
    b(1278, "int48pl", 2, fc_int48pl),
    b(1279, "int48mi", 2, fc_int48mi),
    b(1280, "int48mul", 2, fc_int48mul),
    b(1281, "int48div", 2, fc_int48div),
    b(837, "int82pl", 2, fc_int82pl),
    b(838, "int82mi", 2, fc_int82mi),
    b(839, "int82mul", 2, fc_int82mul),
    b(840, "int82div", 2, fc_int82div),
    b(841, "int28pl", 2, fc_int28pl),
    b(942, "int28mi", 2, fc_int28mi),
    b(943, "int28mul", 2, fc_int28mul),
    b(948, "int28div", 2, fc_int28div),
    b(1904, "int8and", 2, fc_int8and),
    b(1905, "int8or", 2, fc_int8or),
    b(1906, "int8xor", 2, fc_int8xor),
    b(1907, "int8not", 1, fc_int8not),
    b(1908, "int8shl", 2, fc_int8shl),
    b(1909, "int8shr", 2, fc_int8shr),
    b(480, "int84", 1, fc_int84),
    b(481, "int48", 1, fc_int48),
    b(714, "int82", 1, fc_int82),
    b(754, "int28", 1, fc_int28),
    b(482, "i8tod", 1, fc_i8tod),
    b(483, "dtoi8", 1, fc_dtoi8),
    b(652, "i8tof", 1, fc_i8tof),
    b(653, "ftoi8", 1, fc_ftoi8),
    b(1287, "i8tooid", 1, fc_i8tooid),
    b(1288, "oidtoi8", 1, fc_oidtoi8),
    // mod/abs pg_proc aliases (proname mod/abs, same prosrc).
    b(947, "int8mod", 2, fc_int8mod),
    b(1396, "int8abs", 1, fc_int8abs),
    b(4126, "in_range_int8_int8", 5, fc_in_range_int8_int8),
];
