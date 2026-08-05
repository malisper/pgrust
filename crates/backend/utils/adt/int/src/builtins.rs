//! fmgr-shaped wrappers (`fc_<cname>`) and the registry table (`INT_BUILTINS`)
//! the fmgr-core unit consumes; value cores live in the crate root.

use alloc::string::String;

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use core::ptr::NonNull;

use ::types_fmgr::{
    thin_arg, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction, ThinBuiltin, ThinFcinfo,
};

// C pallocs each cstring result into the per-row context; here the backend
// thread owns retained scratch (rules 7/10; fn_extra was measured out: its
// dyn-Any downcast is a per-row virtual type_id call). The returned Datum
// aliases the scratch: consume it before the next out-function call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; 16]> =
        const { core::cell::UnsafeCell::new([0; 16]) };
}

fn in_arg<'a>(fcinfo: &'a Fcinfo) -> alloc::borrow::Cow<'a, str> {
    // SAFETY: catalog arg 0 of the in-functions is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    String::from_utf8_lossy(s.to_bytes())
}

pub fn fc_int2in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let num = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_i16(crate::int2in(&num, esc)?))
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

pub fn fc_int2vectorin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the result context outlives this call frame (set_result_mcx
    // contract); detached so isnull stays assignable on the soft-error path.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let num = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    match crate::int2vectorin(mcx, &num, esc)? {
        Some(img) => {
            let d = Datum::from_usize(img.as_ptr() as usize);
            core::mem::forget(img);
            Ok(d)
        }
        None => {
            fcinfo.isnull = true;
            Ok(Datum::null())
        }
    }
}

pub fn fc_int2vectorout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let [a] = fcinfo.args_n::<1>();
    let p = a.value.as_usize() as *const u8;
    // SAFETY: int2vector datums are never toasted (plain storage); header +
    // dim1 i16 payload are readable in-tuple bytes.
    let (hdr, values) = unsafe {
        let hdr = core::ptr::read_unaligned(p.cast::<types_array::int2vector>());
        let n = hdr.dim1.max(0) as usize;
        let vals =
            core::slice::from_raw_parts(p.add(crate::INT2VECTOR_HDRSZ).cast::<i16>(), n);
        (hdr, vals)
    };
    let mut out = crate::int2vectorout(mcx, hdr.ndim, hdr.dataoffset, hdr.elemtype, values)?;
    out.push(0);
    Ok(::types_fmgr::cstring_result(out))
}

pub fn fc_int4in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let num = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_i32(crate::int4in(&num, esc)?))
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

pub fn fc_int2recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_i16(crate::int2recv(buf)?))
}

pub fn fc_int2send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::int2send(mcx, a.value.as_i16())?))
}

pub fn fc_int4recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_i32(crate::int4recv(buf)?))
}

pub fn fc_int4send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::int4send(mcx, a.value.as_i32())?))
}

macro_rules! fc1 {
    ($($fc:ident $th:ident: $core:ident($get:ident) -> $from:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a] = fcinfo.args_n::<1>();
            Ok(Datum::$from(crate::$core(a.value.$get())))
        }

        /// # Safety
        /// Thin-ABI call: `fcinfo` is a live image with >= 1 arg.
        pub unsafe fn $th(fcinfo: NonNull<ThinFcinfo>) -> PgResult<Datum> {
            // SAFETY: thin contract — the registered arity is 1.
            let a = unsafe { thin_arg(fcinfo, 0) };
            Ok(Datum::$from(crate::$core(a.value.$get())))
        }
    )*};
}

macro_rules! fc1t {
    ($($fc:ident $th:ident: $core:ident($get:ident) -> $from:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a] = fcinfo.args_n::<1>();
            Ok(Datum::$from(crate::$core(a.value.$get())?))
        }

        /// # Safety
        /// Thin-ABI call: `fcinfo` is a live image with >= 1 arg.
        pub unsafe fn $th(fcinfo: NonNull<ThinFcinfo>) -> PgResult<Datum> {
            // SAFETY: thin contract — the registered arity is 1.
            let a = unsafe { thin_arg(fcinfo, 0) };
            Ok(Datum::$from(crate::$core(a.value.$get())?))
        }
    )*};
}

macro_rules! fc2 {
    ($($fc:ident $th:ident: $core:ident($ga:ident, $gb:ident) -> $from:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::$from(crate::$core(a.value.$ga(), b.value.$gb())))
        }

        /// # Safety
        /// Thin-ABI call: `fcinfo` is a live image with >= 2 args.
        pub unsafe fn $th(fcinfo: NonNull<ThinFcinfo>) -> PgResult<Datum> {
            // SAFETY: thin contract — the registered arity is 2.
            let (a, b) = unsafe { (thin_arg(fcinfo, 0), thin_arg(fcinfo, 1)) };
            Ok(Datum::$from(crate::$core(a.value.$ga(), b.value.$gb())))
        }
    )*};
}

macro_rules! fc2t {
    ($($fc:ident $th:ident: $core:ident($ga:ident, $gb:ident) -> $from:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::$from(crate::$core(a.value.$ga(), b.value.$gb())?))
        }

        /// # Safety
        /// Thin-ABI call: `fcinfo` is a live image with >= 2 args.
        pub unsafe fn $th(fcinfo: NonNull<ThinFcinfo>) -> PgResult<Datum> {
            // SAFETY: thin contract — the registered arity is 2.
            let (a, b) = unsafe { (thin_arg(fcinfo, 0), thin_arg(fcinfo, 1)) };
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
    fc_i2toi4 th_i2toi4: i2toi4(as_i16) -> from_i32;
    fc_int4_bool th_int4_bool: int4_bool(as_i32) -> from_bool;
    fc_bool_int4 th_bool_int4: bool_int4(as_bool) -> from_i32;
    fc_int4up th_int4up: int4up(as_i32) -> from_i32;
    fc_int2up th_int2up: int2up(as_i16) -> from_i16;
    fc_int4not th_int4not: int4not(as_i32) -> from_i32;
    fc_int2not th_int2not: int2not(as_i16) -> from_i16;
}

fc1t! {
    fc_i4toi2 th_i4toi2: i4toi2(as_i32) -> from_i16;
    fc_int4um th_int4um: int4um(as_i32) -> from_i32;
    fc_int4inc th_int4inc: int4inc(as_i32) -> from_i32;
    fc_int2um th_int2um: int2um(as_i16) -> from_i16;
    fc_int4abs th_int4abs: int4abs(as_i32) -> from_i32;
    fc_int2abs th_int2abs: int2abs(as_i16) -> from_i16;
}

// C home is hashfunc.c (no hash-AM adt crate yet); grouping/hashjoin reach it
// only through fmgr, so the registry row is the compat surface.
pub fn fc_hashint4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_u32(::hashfn::hash_bytes_uint32(a.value.as_i32() as u32)))
}

pub fn fc_hashint2(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_u32(::hashfn::hash_bytes_uint32(a.value.as_i16() as i32 as u32)))
}

pub fn fc_hashint2extended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, seed] = fcinfo.args_n::<2>();
    Ok(Datum::from_u64(::hashfn::hash_bytes_uint32_extended(
        a.value.as_i16() as i32 as u32,
        seed.value.as_u64(),
    )))
}

// C hashchar is `hash_uint32((int32) PG_GETARG_CHAR(0))`, whose value
// depends on the platform's `char` signedness: sign-extended on
// x86-64/macOS, zero-extended on aarch64 (unsigned char). pgrust pins the
// aarch64 arm — zero-extend the byte — so `"char"` hashes are stable on
// the deployment target and independent of the compiler's char sign.
// (Ruling 2026-07-29; upstream is platform-dependent, bug reported.)
pub fn fc_hashchar(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let c = a.value.as_char() as u8 as u32;
    Ok(Datum::from_u32(::hashfn::hash_bytes_uint32(c)))
}

pub fn fc_hashcharextended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, seed] = fcinfo.args_n::<2>();
    let c = a.value.as_char() as u8 as u32;
    Ok(Datum::from_u64(::hashfn::hash_bytes_uint32_extended(
        c,
        seed.value.as_i64() as u64,
    )))
}

pub fn fc_hashint4extended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, seed] = fcinfo.args_n::<2>();
    Ok(Datum::from_u64(::hashfn::hash_bytes_uint32_extended(
        a.value.as_i32() as u32,
        seed.value.as_u64(),
    )))
}

fc2! {
    fc_int4eq th_int4eq: int4eq(as_i32, as_i32) -> from_bool;
    fc_int4ne th_int4ne: int4ne(as_i32, as_i32) -> from_bool;
    fc_int4lt th_int4lt: int4lt(as_i32, as_i32) -> from_bool;
    fc_int4le th_int4le: int4le(as_i32, as_i32) -> from_bool;
    fc_int4gt th_int4gt: int4gt(as_i32, as_i32) -> from_bool;
    fc_int4ge th_int4ge: int4ge(as_i32, as_i32) -> from_bool;
    fc_int2eq th_int2eq: int2eq(as_i16, as_i16) -> from_bool;
    fc_int2ne th_int2ne: int2ne(as_i16, as_i16) -> from_bool;
    fc_int2lt th_int2lt: int2lt(as_i16, as_i16) -> from_bool;
    fc_int2le th_int2le: int2le(as_i16, as_i16) -> from_bool;
    fc_int2gt th_int2gt: int2gt(as_i16, as_i16) -> from_bool;
    fc_int2ge th_int2ge: int2ge(as_i16, as_i16) -> from_bool;
    fc_int24eq th_int24eq: int24eq(as_i16, as_i32) -> from_bool;
    fc_int24ne th_int24ne: int24ne(as_i16, as_i32) -> from_bool;
    fc_int24lt th_int24lt: int24lt(as_i16, as_i32) -> from_bool;
    fc_int24le th_int24le: int24le(as_i16, as_i32) -> from_bool;
    fc_int24gt th_int24gt: int24gt(as_i16, as_i32) -> from_bool;
    fc_int24ge th_int24ge: int24ge(as_i16, as_i32) -> from_bool;
    fc_int42eq th_int42eq: int42eq(as_i32, as_i16) -> from_bool;
    fc_int42ne th_int42ne: int42ne(as_i32, as_i16) -> from_bool;
    fc_int42lt th_int42lt: int42lt(as_i32, as_i16) -> from_bool;
    fc_int42le th_int42le: int42le(as_i32, as_i16) -> from_bool;
    fc_int42gt th_int42gt: int42gt(as_i32, as_i16) -> from_bool;
    fc_int42ge th_int42ge: int42ge(as_i32, as_i16) -> from_bool;
    fc_int4larger th_int4larger: int4larger(as_i32, as_i32) -> from_i32;
    fc_int4smaller th_int4smaller: int4smaller(as_i32, as_i32) -> from_i32;
    fc_int2larger th_int2larger: int2larger(as_i16, as_i16) -> from_i16;
    fc_int2smaller th_int2smaller: int2smaller(as_i16, as_i16) -> from_i16;
    fc_int4and th_int4and: int4and(as_i32, as_i32) -> from_i32;
    fc_int4or th_int4or: int4or(as_i32, as_i32) -> from_i32;
    fc_int4xor th_int4xor: int4xor(as_i32, as_i32) -> from_i32;
    fc_int4shl th_int4shl: int4shl(as_i32, as_i32) -> from_i32;
    fc_int4shr th_int4shr: int4shr(as_i32, as_i32) -> from_i32;
    fc_int2and th_int2and: int2and(as_i16, as_i16) -> from_i16;
    fc_int2or th_int2or: int2or(as_i16, as_i16) -> from_i16;
    fc_int2xor th_int2xor: int2xor(as_i16, as_i16) -> from_i16;
    fc_int2shl th_int2shl: int2shl(as_i16, as_i32) -> from_i16;
    fc_int2shr th_int2shr: int2shr(as_i16, as_i32) -> from_i16;
}

fc2t! {
    fc_int4pl th_int4pl: int4pl(as_i32, as_i32) -> from_i32;
    fc_int4mi th_int4mi: int4mi(as_i32, as_i32) -> from_i32;
    fc_int4mul th_int4mul: int4mul(as_i32, as_i32) -> from_i32;
    fc_int4div th_int4div: int4div(as_i32, as_i32) -> from_i32;
    fc_int4mod th_int4mod: int4mod(as_i32, as_i32) -> from_i32;
    fc_int2pl th_int2pl: int2pl(as_i16, as_i16) -> from_i16;
    fc_int2mi th_int2mi: int2mi(as_i16, as_i16) -> from_i16;
    fc_int2mul th_int2mul: int2mul(as_i16, as_i16) -> from_i16;
    fc_int2div th_int2div: int2div(as_i16, as_i16) -> from_i16;
    fc_int2mod th_int2mod: int2mod(as_i16, as_i16) -> from_i16;
    fc_int24pl th_int24pl: int24pl(as_i16, as_i32) -> from_i32;
    fc_int24mi th_int24mi: int24mi(as_i16, as_i32) -> from_i32;
    fc_int24mul th_int24mul: int24mul(as_i16, as_i32) -> from_i32;
    fc_int24div th_int24div: int24div(as_i16, as_i32) -> from_i32;
    fc_int42pl th_int42pl: int42pl(as_i32, as_i16) -> from_i32;
    fc_int42mi th_int42mi: int42mi(as_i32, as_i16) -> from_i32;
    fc_int42mul th_int42mul: int42mul(as_i32, as_i16) -> from_i32;
    fc_int42div th_int42div: int42div(as_i32, as_i16) -> from_i32;
    fc_int4gcd th_int4gcd: int4gcd(as_i32, as_i32) -> from_i32;
    fc_int4lcm th_int4lcm: int4lcm(as_i32, as_i32) -> from_i32;
}

fc_in_range! {
    fc_in_range_int4_int4: in_range_int4_int4(as_i32, as_i32, as_i32);
    fc_in_range_int4_int2: in_range_int4_int2(as_i32, as_i32, as_i16);
    fc_in_range_int4_int8: in_range_int4_int8(as_i32, as_i32, as_i64);
    fc_in_range_int2_int4: in_range_int2_int4(as_i16, as_i16, as_i32);
    fc_in_range_int2_int2: in_range_int2_int2(as_i16, as_i16, as_i16);
    fc_in_range_int2_int8: in_range_int2_int8(as_i16, as_i16, as_i64);
}

// generate_series_step_int4 (OIDs 1066 3-arg / 1067 2-arg share the C body;
// PG_NARGS demuxes) over the funcapi ValuePerCall frame.
pub fn fc_generate_series_step_int4(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("generate_series_int4: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let start = fcinfo.arg(0).as_i32();
        let finish = fcinfo.arg(1).as_i32();
        let step = if fcinfo.nargs() == 3 { fcinfo.arg(2).as_i32() } else { 1 };
        let state = crate::series::GenerateSeriesInt4::new(start, finish, step)?;
        let fctx = ::funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(alloc::boxed::Box::new(state));
    }
    let next = ::funcapi::per_MultiFuncCall(flinfo)
        .user_fctx
        .as_mut()
        .expect("generate_series_int4: user_fctx set at first call")
        .downcast_mut::<crate::series::GenerateSeriesInt4>()
        .expect("generate_series_int4: user_fctx is GenerateSeriesInt4")
        .next();
    match next {
        Some(v) => Ok(::funcapi::srf_return_next(flinfo, fcinfo, Datum::from_i32(v))),
        None => Ok(::funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

// generate_series_int4_support (OID 3994): SupportRequestRows over all-Const
// args; anything else returns NULL so callers fall back (C: estimate the
// planner-folded exprs — we read Consts directly, Param estimation unported).
pub fn fc_generate_series_int4_support(
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
    let mut vals = [1i32; 3];
    for (i, arg) in args.iter().enumerate() {
        match arg.as_const() {
            Some(c) if c.constisnull => {
                req.rows = 0.0;
                return Ok(Datum::from_usize(p as usize));
            }
            Some(c) => vals[i] = c.constvalue.as_i32(),
            None => return Ok(Datum::from_usize(0)),
        }
    }
    match crate::series::generate_series_int4_rows(
        vals[0] as f64,
        vals[1] as f64,
        vals[2] as f64,
    ) {
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

// pg_proc.dat rows for int.c. Not present: int2vectorrecv/send (2410/2411)
// live with the oidvector pair in arrayfuncs::builtins (array_recv/array_send
// delegations need the array io meta cache).
pub const INT_BUILTINS: &[FmgrBuiltin] = &[
    b(2404, "int2recv", 1, fc_int2recv),
    b(2405, "int2send", 1, fc_int2send),
    b(2406, "int4recv", 1, fc_int4recv),
    b(2407, "int4send", 1, fc_int4send),
    srf(1066, "generate_series_step_int4", 3, fc_generate_series_step_int4),
    srf(1067, "generate_series_int4", 2, fc_generate_series_step_int4),
    b(3994, "generate_series_int4_support", 1, fc_generate_series_int4_support),
    b(38, "int2in", 1, fc_int2in),
    b(39, "int2out", 1, fc_int2out),
    b(40, "int2vectorin", 1, fc_int2vectorin),
    b(41, "int2vectorout", 1, fc_int2vectorout),
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
    b(446, "hashcharextended", 2, fc_hashcharextended),
    b(450, "hashint4", 1, fc_hashint4),
    b(425, "hashint4extended", 2, fc_hashint4extended),
    b(449, "hashint2", 1, fc_hashint2),
    b(441, "hashint2extended", 2, fc_hashint2extended),
    b(454, "hashchar", 1, fc_hashchar),
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

const fn t(foid: Oid, nargs: i16, func: PGFunction, thin: ::types_fmgr::PGFunctionThin) -> ThinBuiltin {
    ThinBuiltin { foid, nargs, func, thin }
}

// Thin-ABI twins of the fc1!/fc1t!/fc2!/fc2t! wrappers above (same cores, so
// the error surface is identical by construction; none read flinfo or write
// isnull).
pub static INT_THIN: &[ThinBuiltin] = &[
    t(63, 2, fc_int2eq, th_int2eq),
    t(64, 2, fc_int2lt, th_int2lt),
    t(65, 2, fc_int4eq, th_int4eq),
    t(66, 2, fc_int4lt, th_int4lt),
    t(141, 2, fc_int4mul, th_int4mul),
    t(144, 2, fc_int4ne, th_int4ne),
    t(145, 2, fc_int2ne, th_int2ne),
    t(146, 2, fc_int2gt, th_int2gt),
    t(147, 2, fc_int4gt, th_int4gt),
    t(148, 2, fc_int2le, th_int2le),
    t(149, 2, fc_int4le, th_int4le),
    t(150, 2, fc_int4ge, th_int4ge),
    t(151, 2, fc_int2ge, th_int2ge),
    t(152, 2, fc_int2mul, th_int2mul),
    t(153, 2, fc_int2div, th_int2div),
    t(154, 2, fc_int4div, th_int4div),
    t(155, 2, fc_int2mod, th_int2mod),
    t(156, 2, fc_int4mod, th_int4mod),
    t(158, 2, fc_int24eq, th_int24eq),
    t(159, 2, fc_int42eq, th_int42eq),
    t(160, 2, fc_int24lt, th_int24lt),
    t(161, 2, fc_int42lt, th_int42lt),
    t(162, 2, fc_int24gt, th_int24gt),
    t(163, 2, fc_int42gt, th_int42gt),
    t(164, 2, fc_int24ne, th_int24ne),
    t(165, 2, fc_int42ne, th_int42ne),
    t(166, 2, fc_int24le, th_int24le),
    t(167, 2, fc_int42le, th_int42le),
    t(168, 2, fc_int24ge, th_int24ge),
    t(169, 2, fc_int42ge, th_int42ge),
    t(170, 2, fc_int24mul, th_int24mul),
    t(171, 2, fc_int42mul, th_int42mul),
    t(172, 2, fc_int24div, th_int24div),
    t(173, 2, fc_int42div, th_int42div),
    t(176, 2, fc_int2pl, th_int2pl),
    t(177, 2, fc_int4pl, th_int4pl),
    t(178, 2, fc_int24pl, th_int24pl),
    t(179, 2, fc_int42pl, th_int42pl),
    t(180, 2, fc_int2mi, th_int2mi),
    t(181, 2, fc_int4mi, th_int4mi),
    t(182, 2, fc_int24mi, th_int24mi),
    t(183, 2, fc_int42mi, th_int42mi),
    t(212, 1, fc_int4um, th_int4um),
    t(213, 1, fc_int2um, th_int2um),
    t(313, 1, fc_i2toi4, th_i2toi4),
    t(314, 1, fc_i4toi2, th_i4toi2),
    t(766, 1, fc_int4inc, th_int4inc),
    t(768, 2, fc_int4larger, th_int4larger),
    t(769, 2, fc_int4smaller, th_int4smaller),
    t(770, 2, fc_int2larger, th_int2larger),
    t(771, 2, fc_int2smaller, th_int2smaller),
    t(940, 2, fc_int2mod, th_int2mod),
    t(941, 2, fc_int4mod, th_int4mod),
    t(1251, 1, fc_int4abs, th_int4abs),
    t(1253, 1, fc_int2abs, th_int2abs),
    t(1397, 1, fc_int4abs, th_int4abs),
    t(1398, 1, fc_int2abs, th_int2abs),
    t(1892, 2, fc_int2and, th_int2and),
    t(1893, 2, fc_int2or, th_int2or),
    t(1894, 2, fc_int2xor, th_int2xor),
    t(1895, 1, fc_int2not, th_int2not),
    t(1896, 2, fc_int2shl, th_int2shl),
    t(1897, 2, fc_int2shr, th_int2shr),
    t(1898, 2, fc_int4and, th_int4and),
    t(1899, 2, fc_int4or, th_int4or),
    t(1900, 2, fc_int4xor, th_int4xor),
    t(1901, 1, fc_int4not, th_int4not),
    t(1902, 2, fc_int4shl, th_int4shl),
    t(1903, 2, fc_int4shr, th_int4shr),
    t(1911, 1, fc_int2up, th_int2up),
    t(1912, 1, fc_int4up, th_int4up),
    t(2557, 1, fc_int4_bool, th_int4_bool),
    t(2558, 1, fc_bool_int4, th_bool_int4),
    t(5044, 2, fc_int4gcd, th_int4gcd),
    t(5046, 2, fc_int4lcm, th_int4lcm),
];
