//! fmgr-shaped wrappers (`fc_<cname>`) and the `FLOAT_BUILTINS` registry table
//! for fmgr-core. Not registrable yet (frame conventions pending, the int.c
//! precedent): btfloat{4,8}sortsupport (3132/3133, SortSupport node), and the
//! aggregate transition/final rows (208, 222, 276, 1830-1832, 2512-2513,
//! 2806-2817, 3342 — float8[] transvalue allocation belongs to the agg frame);
//! their value cores live in the crate. recv/send (2424-2427) ride the
//! binary-wire fmgr frame (types_fmgr::wire).

use alloc::borrow::Cow;
use alloc::string::String;

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

// C float{4,8}recv/send bodies: pq_getmsgfloat{4,8} (advances the buffer
// cursor, which the binary-bind whole-buffer check depends on) / begintypsend
// + pq_sendfloat + endtypsend.
pub fn fc_float4recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_f32(pqformat::pq_getmsgfloat4(buf)?))
}

pub fn fc_float4send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mut b = pqformat::pq_begintypsend(fcinfo.result_mcx())?;
    pqformat::pq_sendfloat4(&mut b, a.value.as_f32())?;
    Ok(varlena_result(pqformat::pq_endtypsend(b)))
}

pub fn fc_float8recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_f64(pqformat::pq_getmsgfloat8(buf)?))
}

pub fn fc_float8send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mut b = pqformat::pq_begintypsend(fcinfo.result_mcx())?;
    pqformat::pq_sendfloat8(&mut b, a.value.as_f64())?;
    Ok(varlena_result(pqformat::pq_endtypsend(b)))
}

// C pallocs each cstring result into the per-row context; the backend thread
// owns retained scratch instead (rule 7). The returned Datum aliases the
// scratch: consume it before the next out-function call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; crate::MAXDOUBLEWIDTH]> =
        const { core::cell::UnsafeCell::new([0; crate::MAXDOUBLEWIDTH]) };
}

fn in_arg<'a>(fcinfo: &'a Fcinfo) -> Cow<'a, str> {
    // SAFETY: catalog arg 0 of the in-functions is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    String::from_utf8_lossy(s.to_bytes())
}

pub fn fc_float4in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let num = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_f32(crate::float4in(&num, esc)?))
}

pub fn fc_float8in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let num = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_f64(crate::float8in(&num, esc)?))
}

pub fn fc_float4out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let v = a.value.as_f32();
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::float4out(v, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_float8out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let v = a.value.as_f64();
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::float8out(v, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_dpi(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f64(crate::dpi()))
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

fc1! {
    fc_float4abs: float4abs(as_f32) -> from_f32;
    fc_float4um: float4um(as_f32) -> from_f32;
    fc_float4up: float4up(as_f32) -> from_f32;
    fc_float8abs: float8abs(as_f64) -> from_f64;
    fc_float8um: float8um(as_f64) -> from_f64;
    fc_float8up: float8up(as_f64) -> from_f64;
    fc_ftod: ftod(as_f32) -> from_f64;
    fc_i4tod: i4tod(as_i32) -> from_f64;
    fc_i2tod: i2tod(as_i16) -> from_f64;
    fc_i4tof: i4tof(as_i32) -> from_f32;
    fc_i2tof: i2tof(as_i16) -> from_f32;
    fc_dround: dround(as_f64) -> from_f64;
    fc_dceil: dceil(as_f64) -> from_f64;
    fc_dfloor: dfloor(as_f64) -> from_f64;
    fc_dsign: dsign(as_f64) -> from_f64;
    fc_dtrunc: dtrunc(as_f64) -> from_f64;
    fc_dsinh: dsinh(as_f64) -> from_f64;
    fc_dasinh: dasinh(as_f64) -> from_f64;
}

fc1t! {
    fc_dtof: dtof(as_f64) -> from_f32;
    fc_dtoi4: dtoi4(as_f64) -> from_i32;
    fc_dtoi2: dtoi2(as_f64) -> from_i16;
    fc_ftoi4: ftoi4(as_f32) -> from_i32;
    fc_ftoi2: ftoi2(as_f32) -> from_i16;
    fc_dsqrt: dsqrt(as_f64) -> from_f64;
    fc_dcbrt: dcbrt(as_f64) -> from_f64;
    fc_dexp: dexp(as_f64) -> from_f64;
    fc_dlog1: dlog1(as_f64) -> from_f64;
    fc_dlog10: dlog10(as_f64) -> from_f64;
    fc_dacos: dacos(as_f64) -> from_f64;
    fc_dasin: dasin(as_f64) -> from_f64;
    fc_datan: datan(as_f64) -> from_f64;
    fc_dcos: dcos(as_f64) -> from_f64;
    fc_dcot: dcot(as_f64) -> from_f64;
    fc_dsin: dsin(as_f64) -> from_f64;
    fc_dtan: dtan(as_f64) -> from_f64;
    fc_dacosd: dacosd(as_f64) -> from_f64;
    fc_dasind: dasind(as_f64) -> from_f64;
    fc_datand: datand(as_f64) -> from_f64;
    fc_dcosd: dcosd(as_f64) -> from_f64;
    fc_dcotd: dcotd(as_f64) -> from_f64;
    fc_dsind: dsind(as_f64) -> from_f64;
    fc_dtand: dtand(as_f64) -> from_f64;
    fc_degrees: degrees(as_f64) -> from_f64;
    fc_radians: radians(as_f64) -> from_f64;
    fc_dcosh: dcosh(as_f64) -> from_f64;
    fc_dtanh: dtanh(as_f64) -> from_f64;
    fc_dacosh: dacosh(as_f64) -> from_f64;
    fc_datanh: datanh(as_f64) -> from_f64;
    fc_derf: derf(as_f64) -> from_f64;
    fc_derfc: derfc(as_f64) -> from_f64;
    fc_dgamma: dgamma(as_f64) -> from_f64;
    fc_dlgamma: dlgamma(as_f64) -> from_f64;
}

fc2! {
    fc_float4larger: float4larger(as_f32, as_f32) -> from_f32;
    fc_float4smaller: float4smaller(as_f32, as_f32) -> from_f32;
    fc_float8larger: float8larger(as_f64, as_f64) -> from_f64;
    fc_float8smaller: float8smaller(as_f64, as_f64) -> from_f64;
    fc_float4eq: float4_eq(as_f32, as_f32) -> from_bool;
    fc_float4ne: float4_ne(as_f32, as_f32) -> from_bool;
    fc_float4lt: float4_lt(as_f32, as_f32) -> from_bool;
    fc_float4le: float4_le(as_f32, as_f32) -> from_bool;
    fc_float4gt: float4_gt(as_f32, as_f32) -> from_bool;
    fc_float4ge: float4_ge(as_f32, as_f32) -> from_bool;
    fc_float8eq: float8_eq(as_f64, as_f64) -> from_bool;
    fc_float8ne: float8_ne(as_f64, as_f64) -> from_bool;
    fc_float8lt: float8_lt(as_f64, as_f64) -> from_bool;
    fc_float8le: float8_le(as_f64, as_f64) -> from_bool;
    fc_float8gt: float8_gt(as_f64, as_f64) -> from_bool;
    fc_float8ge: float8_ge(as_f64, as_f64) -> from_bool;
    fc_float48eq: float48eq(as_f32, as_f64) -> from_bool;
    fc_float48ne: float48ne(as_f32, as_f64) -> from_bool;
    fc_float48lt: float48lt(as_f32, as_f64) -> from_bool;
    fc_float48le: float48le(as_f32, as_f64) -> from_bool;
    fc_float48gt: float48gt(as_f32, as_f64) -> from_bool;
    fc_float48ge: float48ge(as_f32, as_f64) -> from_bool;
    fc_float84eq: float84eq(as_f64, as_f32) -> from_bool;
    fc_float84ne: float84ne(as_f64, as_f32) -> from_bool;
    fc_float84lt: float84lt(as_f64, as_f32) -> from_bool;
    fc_float84le: float84le(as_f64, as_f32) -> from_bool;
    fc_float84gt: float84gt(as_f64, as_f32) -> from_bool;
    fc_float84ge: float84ge(as_f64, as_f32) -> from_bool;
    fc_btfloat4cmp: btfloat4cmp(as_f32, as_f32) -> from_i32;
    fc_btfloat8cmp: btfloat8cmp(as_f64, as_f64) -> from_i32;
    fc_btfloat48cmp: btfloat48cmp(as_f32, as_f64) -> from_i32;
    fc_btfloat84cmp: btfloat84cmp(as_f64, as_f32) -> from_i32;
}

fc2t! {
    fc_float4pl: float4_pl(as_f32, as_f32) -> from_f32;
    fc_float4mi: float4_mi(as_f32, as_f32) -> from_f32;
    fc_float4mul: float4_mul(as_f32, as_f32) -> from_f32;
    fc_float4div: float4_div(as_f32, as_f32) -> from_f32;
    fc_float8pl: float8_pl(as_f64, as_f64) -> from_f64;
    fc_float8mi: float8_mi(as_f64, as_f64) -> from_f64;
    fc_float8mul: float8_mul(as_f64, as_f64) -> from_f64;
    fc_float8div: float8_div(as_f64, as_f64) -> from_f64;
    fc_float48pl: float48pl(as_f32, as_f64) -> from_f64;
    fc_float48mi: float48mi(as_f32, as_f64) -> from_f64;
    fc_float48mul: float48mul(as_f32, as_f64) -> from_f64;
    fc_float48div: float48div(as_f32, as_f64) -> from_f64;
    fc_float84pl: float84pl(as_f64, as_f32) -> from_f64;
    fc_float84mi: float84mi(as_f64, as_f32) -> from_f64;
    fc_float84mul: float84mul(as_f64, as_f32) -> from_f64;
    fc_float84div: float84div(as_f64, as_f32) -> from_f64;
    fc_datan2: datan2(as_f64, as_f64) -> from_f64;
    fc_datan2d: datan2d(as_f64, as_f64) -> from_f64;
    fc_dpow: dpow(as_f64, as_f64) -> from_f64;
}

pub fn fc_width_bucket_float8(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let [op, b1, b2, count] = fcinfo.args_n::<4>();
    Ok(Datum::from_i32(crate::width_bucket_float8(
        op.value.as_f64(),
        b1.value.as_f64(),
        b2.value.as_f64(),
        count.value.as_i32(),
    )?))
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

fc_in_range! {
    fc_in_range_float8_float8: in_range_float8_float8(as_f64, as_f64, as_f64);
    fc_in_range_float4_float8: in_range_float4_float8(as_f32, as_f32, as_f64);
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

// pg_proc.dat rows for float.c (all proisstrict, none retset); alias OIDs
// (log/ln/round/... over the same prosrc) each get their row, as in C's
// fmgr_builtins[].
pub const FLOAT_BUILTINS: &[FmgrBuiltin] = &[
    b(2424, "float4recv", 1, fc_float4recv),
    b(2425, "float4send", 1, fc_float4send),
    b(2426, "float8recv", 1, fc_float8recv),
    b(2427, "float8send", 1, fc_float8send),
    b(200, "float4in", 1, fc_float4in),
    b(201, "float4out", 1, fc_float4out),
    b(202, "float4mul", 2, fc_float4mul),
    b(203, "float4div", 2, fc_float4div),
    b(204, "float4pl", 2, fc_float4pl),
    b(205, "float4mi", 2, fc_float4mi),
    b(206, "float4um", 1, fc_float4um),
    b(207, "float4abs", 1, fc_float4abs),
    b(209, "float4larger", 2, fc_float4larger),
    b(211, "float4smaller", 2, fc_float4smaller),
    b(214, "float8in", 1, fc_float8in),
    b(215, "float8out", 1, fc_float8out),
    b(216, "float8mul", 2, fc_float8mul),
    b(217, "float8div", 2, fc_float8div),
    b(218, "float8pl", 2, fc_float8pl),
    b(219, "float8mi", 2, fc_float8mi),
    b(220, "float8um", 1, fc_float8um),
    b(221, "float8abs", 1, fc_float8abs),
    b(223, "float8larger", 2, fc_float8larger),
    b(224, "float8smaller", 2, fc_float8smaller),
    b(228, "dround", 1, fc_dround),
    b(229, "dtrunc", 1, fc_dtrunc),
    b(230, "dsqrt", 1, fc_dsqrt),
    b(231, "dcbrt", 1, fc_dcbrt),
    b(232, "dpow", 2, fc_dpow),
    b(233, "dexp", 1, fc_dexp),
    b(234, "dlog1", 1, fc_dlog1),
    b(235, "i2tod", 1, fc_i2tod),
    b(236, "i2tof", 1, fc_i2tof),
    b(237, "dtoi2", 1, fc_dtoi2),
    b(238, "ftoi2", 1, fc_ftoi2),
    b(279, "float48mul", 2, fc_float48mul),
    b(280, "float48div", 2, fc_float48div),
    b(281, "float48pl", 2, fc_float48pl),
    b(282, "float48mi", 2, fc_float48mi),
    b(283, "float84mul", 2, fc_float84mul),
    b(284, "float84div", 2, fc_float84div),
    b(285, "float84pl", 2, fc_float84pl),
    b(286, "float84mi", 2, fc_float84mi),
    b(287, "float4eq", 2, fc_float4eq),
    b(288, "float4ne", 2, fc_float4ne),
    b(289, "float4lt", 2, fc_float4lt),
    b(290, "float4le", 2, fc_float4le),
    b(291, "float4gt", 2, fc_float4gt),
    b(292, "float4ge", 2, fc_float4ge),
    b(293, "float8eq", 2, fc_float8eq),
    b(294, "float8ne", 2, fc_float8ne),
    b(295, "float8lt", 2, fc_float8lt),
    b(296, "float8le", 2, fc_float8le),
    b(297, "float8gt", 2, fc_float8gt),
    b(298, "float8ge", 2, fc_float8ge),
    b(299, "float48eq", 2, fc_float48eq),
    b(300, "float48ne", 2, fc_float48ne),
    b(301, "float48lt", 2, fc_float48lt),
    b(302, "float48le", 2, fc_float48le),
    b(303, "float48gt", 2, fc_float48gt),
    b(304, "float48ge", 2, fc_float48ge),
    b(305, "float84eq", 2, fc_float84eq),
    b(306, "float84ne", 2, fc_float84ne),
    b(307, "float84lt", 2, fc_float84lt),
    b(308, "float84le", 2, fc_float84le),
    b(309, "float84gt", 2, fc_float84gt),
    b(310, "float84ge", 2, fc_float84ge),
    b(311, "ftod", 1, fc_ftod),
    b(312, "dtof", 1, fc_dtof),
    b(316, "i4tod", 1, fc_i4tod),
    b(317, "dtoi4", 1, fc_dtoi4),
    b(318, "i4tof", 1, fc_i4tof),
    b(319, "ftoi4", 1, fc_ftoi4),
    b(320, "width_bucket_float8", 4, fc_width_bucket_float8),
    b(354, "btfloat4cmp", 2, fc_btfloat4cmp),
    b(355, "btfloat8cmp", 2, fc_btfloat8cmp),
    b(1194, "dlog10", 1, fc_dlog10),
    b(1339, "dlog10", 1, fc_dlog10),
    b(1340, "dlog10", 1, fc_dlog10),
    b(1341, "dlog1", 1, fc_dlog1),
    b(1342, "dround", 1, fc_dround),
    b(1343, "dtrunc", 1, fc_dtrunc),
    b(1344, "dsqrt", 1, fc_dsqrt),
    b(1345, "dcbrt", 1, fc_dcbrt),
    b(1346, "dpow", 2, fc_dpow),
    b(1347, "dexp", 1, fc_dexp),
    b(1368, "dpow", 2, fc_dpow),
    b(1394, "float4abs", 1, fc_float4abs),
    b(1395, "float8abs", 1, fc_float8abs),
    b(1600, "dasin", 1, fc_dasin),
    b(1601, "dacos", 1, fc_dacos),
    b(1602, "datan", 1, fc_datan),
    b(1603, "datan2", 2, fc_datan2),
    b(1604, "dsin", 1, fc_dsin),
    b(1605, "dcos", 1, fc_dcos),
    b(1606, "dtan", 1, fc_dtan),
    b(1607, "dcot", 1, fc_dcot),
    b(1608, "degrees", 1, fc_degrees),
    b(1609, "radians", 1, fc_radians),
    b(1610, "dpi", 0, fc_dpi),
    b(1913, "float4up", 1, fc_float4up),
    b(1914, "float8up", 1, fc_float8up),
    b(2194, "btfloat48cmp", 2, fc_btfloat48cmp),
    b(2195, "btfloat84cmp", 2, fc_btfloat84cmp),
    b(2308, "dceil", 1, fc_dceil),
    b(2309, "dfloor", 1, fc_dfloor),
    b(2310, "dsign", 1, fc_dsign),
    b(2320, "dceil", 1, fc_dceil),
    b(2462, "dsinh", 1, fc_dsinh),
    b(2463, "dcosh", 1, fc_dcosh),
    b(2464, "dtanh", 1, fc_dtanh),
    b(2465, "dasinh", 1, fc_dasinh),
    b(2466, "dacosh", 1, fc_dacosh),
    b(2467, "datanh", 1, fc_datanh),
    b(2731, "dasind", 1, fc_dasind),
    b(2732, "dacosd", 1, fc_dacosd),
    b(2733, "datand", 1, fc_datand),
    b(2734, "datan2d", 2, fc_datan2d),
    b(2735, "dsind", 1, fc_dsind),
    b(2736, "dcosd", 1, fc_dcosd),
    b(2737, "dtand", 1, fc_dtand),
    b(2738, "dcotd", 1, fc_dcotd),
    b(4139, "in_range_float8_float8", 5, fc_in_range_float8_float8),
    b(4140, "in_range_float4_float8", 5, fc_in_range_float4_float8),
    b(6219, "derf", 1, fc_derf),
    b(6220, "derfc", 1, fc_derfc),
    b(6383, "dgamma", 1, fc_dgamma),
    b(6384, "dlgamma", 1, fc_dlgamma),
];
