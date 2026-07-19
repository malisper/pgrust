//! fmgr wrappers (`fc_*`) + `NETWORK_BUILTINS` for fmgr-core. inet/cidr are
//! varlena values: args read in place via PackedVarlena (PG_GETARG_INET_PP),
//! new values built as SET_INET_VARSIZE images into the armed result mcx.

use std::borrow::Cow;

use ::datum::{Datum, Varlena};
use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::{InetRef, InetValue, INET_OUT_BUFLEN};

fn arg_inet<'a>(fcinfo: &'a Fcinfo, i: usize) -> InetRef<'a> {
    // SAFETY: catalog args of these fns are non-null inet/cidr varlenas;
    // inet never TOASTs external (22 bytes max), so the detoast arm is Ok.
    let pv = unsafe { fcinfo.arg_varlena_packed(i) }.expect("inet arg detoast");
    InetRef::from_payload(pv.data())
}

fn in_arg<'a>(fcinfo: &'a Fcinfo) -> Cow<'a, str> {
    // SAFETY: catalog arg 0 of inet_in/cidr_in is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    String::from_utf8_lossy(s.to_bytes())
}

fn inet_result(fcinfo: &Fcinfo, v: &InetValue) -> PgResult<Datum> {
    let (img, len) = v.image();
    byref_result(fcinfo.result_mcx(), &img[..len])
}

fn text_result<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Varlena<'mcx>> {
    let mut image = ::mcx::vec_with_capacity_in(mcx, ::datum::VARHDRSZ + payload.len())?;
    ::mcx::vec_append_bytes(&mut image, &[0u8; ::datum::VARHDRSZ])?;
    ::mcx::vec_append_bytes(&mut image, payload)?;
    Ok(Varlena::from_image(image))
}

fn network_in_dat(fcinfo: &mut Fcinfo, is_cidr: bool) -> PgResult<Datum> {
    let s = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    match crate::network_in(&s, is_cidr, esc)? {
        Some(v) => {
            let (img, len) = v.image();
            byref_result(fcinfo.result_mcx(), &img[..len])
        }
        None => Ok(Datum::from_usize(0)),
    }
}

pub fn fc_inet_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    network_in_dat(fcinfo, false)
}

pub fn fc_cidr_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    network_in_dat(fcinfo, true)
}

// C pallocs the cstring per row; the backend thread owns retained scratch
// (the int.c out-function precedent).
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; INET_OUT_BUFLEN + 1]> =
        const { core::cell::UnsafeCell::new([0; INET_OUT_BUFLEN + 1]) };
}

fn out_dat(fcinfo: &Fcinfo, is_cidr: bool) -> PgResult<Datum> {
    let ip = arg_inet(fcinfo, 0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::network_out_into(ip, is_cidr, &mut buf[..INET_OUT_BUFLEN])?;
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_inet_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    out_dat(fcinfo, false)
}

pub fn fc_cidr_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    out_dat(fcinfo, true)
}

fn recv_dat(fcinfo: &mut Fcinfo, is_cidr: bool) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let v = crate::network_recv(buf, is_cidr)?;
    inet_result(fcinfo, &v)
}

pub fn fc_inet_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    recv_dat(fcinfo, false)
}

pub fn fc_cidr_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    recv_dat(fcinfo, true)
}

fn send_dat(fcinfo: &Fcinfo, is_cidr: bool) -> PgResult<Datum> {
    let ip = arg_inet(fcinfo, 0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::network_send(mcx, ip, is_cidr)?))
}

pub fn fc_inet_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    send_dat(fcinfo, false)
}

pub fn fc_cidr_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    send_dat(fcinfo, true)
}

macro_rules! fc_cmp {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (a, b) = (arg_inet(fcinfo, 0), arg_inet(fcinfo, 1));
            Ok(Datum::from_bool(crate::network_cmp_internal(a, b) $op 0))
        }
    )*};
}

fc_cmp! {
    fc_network_lt: <;
    fc_network_le: <=;
    fc_network_eq: ==;
    fc_network_ge: >=;
    fc_network_gt: >;
    fc_network_ne: !=;
}

pub fn fc_network_cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (arg_inet(fcinfo, 0), arg_inet(fcinfo, 1));
    Ok(Datum::from_i32(crate::network_cmp_internal(a, b)))
}

pub fn fc_network_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (arg_inet(fcinfo, 0), arg_inet(fcinfo, 1));
    let i = if crate::network_cmp_internal(a, b) < 0 { 0 } else { 1 };
    Ok(fcinfo.arg(i))
}

pub fn fc_network_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (arg_inet(fcinfo, 0), arg_inet(fcinfo, 1));
    let i = if crate::network_cmp_internal(a, b) > 0 { 0 } else { 1 };
    Ok(fcinfo.arg(i))
}

macro_rules! fc_bool2 {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (a, b) = (arg_inet(fcinfo, 0), arg_inet(fcinfo, 1));
            Ok(Datum::from_bool(crate::$core(a, b)))
        }
    )*};
}

fc_bool2! {
    fc_network_sub: network_sub;
    fc_network_subeq: network_subeq;
    fc_network_sup: network_sup;
    fc_network_supeq: network_supeq;
    fc_network_overlap: network_overlap;
    fc_inet_same_family: inet_same_family;
}

pub fn fc_hashinet(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_u32(crate::hashinet_bytes(arg_inet(fcinfo, 0))))
}

pub fn fc_hashinetextended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let seed = fcinfo.arg(1).as_u64();
    Ok(Datum::from_u64(crate::hashinet_bytes_extended(
        arg_inet(fcinfo, 0),
        seed,
    )))
}

macro_rules! fc_text1 {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let ip = arg_inet(fcinfo, 0);
            let mut buf = [0u8; INET_OUT_BUFLEN];
            let len = crate::$core(ip, &mut buf)?;
            let mcx = fcinfo.result_mcx();
            Ok(varlena_result(text_result(mcx, &buf[..len])?))
        }
    )*};
}

fc_text1! {
    fc_network_host: network_host_into;
    fc_network_show: network_show_into;
    fc_inet_abbrev: inet_abbrev_into;
    fc_cidr_abbrev: cidr_abbrev_into;
}

macro_rules! fc_inet1 {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let v = crate::$core(arg_inet(fcinfo, 0));
            inet_result(fcinfo, &v)
        }
    )*};
}

fc_inet1! {
    fc_network_broadcast: network_broadcast;
    fc_network_network: network_network;
    fc_network_netmask: network_netmask;
    fc_network_hostmask: network_hostmask;
    fc_inetnot: inetnot;
}

pub fn fc_network_masklen(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(arg_inet(fcinfo, 0).bits as i32))
}

pub fn fc_network_family(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::network_family(arg_inet(fcinfo, 0))))
}

pub fn fc_inet_to_cidr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = crate::inet_to_cidr(arg_inet(fcinfo, 0))?;
    inet_result(fcinfo, &v)
}

pub fn fc_inet_set_masklen(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let bits = fcinfo.arg_i32(1);
    let v = crate::inet_set_masklen(arg_inet(fcinfo, 0), bits)?;
    inet_result(fcinfo, &v)
}

pub fn fc_cidr_set_masklen(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let bits = fcinfo.arg_i32(1);
    let v = crate::cidr_set_masklen(arg_inet(fcinfo, 0), bits)?;
    inet_result(fcinfo, &v)
}

pub fn fc_inet_merge(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = crate::inet_merge(arg_inet(fcinfo, 0), arg_inet(fcinfo, 1))?;
    inet_result(fcinfo, &v)
}

pub fn fc_inetand(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = crate::inetand(arg_inet(fcinfo, 0), arg_inet(fcinfo, 1))?;
    inet_result(fcinfo, &v)
}

pub fn fc_inetor(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = crate::inetor(arg_inet(fcinfo, 0), arg_inet(fcinfo, 1))?;
    inet_result(fcinfo, &v)
}

pub fn fc_inetpl(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let addend = fcinfo.arg_i64(1);
    let v = crate::internal_inetpl(arg_inet(fcinfo, 0), addend)?;
    inet_result(fcinfo, &v)
}

pub fn fc_inetmi_int8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let addend = fcinfo.arg_i64(1);
    let v = crate::internal_inetpl(arg_inet(fcinfo, 0), addend.wrapping_neg())?;
    inet_result(fcinfo, &v)
}

pub fn fc_inetmi(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::inetmi(
        arg_inet(fcinfo, 0),
        arg_inet(fcinfo, 1),
    )?))
}

// C handles only SupportRequestIndexCondition here; the planner's closed-set
// dispatch (indxpath.rs) owns that leg, so an fmgr arrival of it is a bug.
// Every other request tag is C's NULL return.
pub fn fc_network_subset_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use ::types_nodes::NodeTag;
    let p = fcinfo.arg(0).as_usize() as *const NodeTag;
    // SAFETY: prosupport contract — arg points at a live tag-first node.
    let tag = unsafe { *p };
    assert_ne!(
        tag,
        NodeTag::T_SupportRequestIndexCondition,
        "network_subset_support: IndexCondition must ride the indxpath closed set"
    );
    Ok(Datum::from_usize(0))
}

// wasm32: the wasi libc crate has no netdb/socket constants; musl values.
// Sessions on wasm are always socketless (ip::sockaddr_family reads
// AF_UNSPEC), so the quartet below returns NULL as C's ss_family switch does.
#[cfg(not(target_family = "wasm"))]
use libc::{AF_INET, AF_INET6, NI_NUMERICHOST, NI_NUMERICSERV};
#[cfg(target_family = "wasm")]
mod wasm_netconsts {
    pub const AF_INET: i32 = 2;
    pub const AF_INET6: i32 = 10;
    pub const NI_NUMERICHOST: i32 = 1;
    pub const NI_NUMERICSERV: i32 = 2;
}
#[cfg(target_family = "wasm")]
use wasm_netconsts::*;

// network.c session-introspection quartet (inet_client_addr &c). Unix-socket
// and socketless sessions return NULL, as C's ss_family switch does.
fn session_addr(fcinfo: &mut Fcinfo, local: bool) -> PgResult<Datum> {
    let Some(sa) = session_sockaddr(local) else {
        return Ok(fcinfo.return_null());
    };
    let mut host = String::new();
    let rc = ::ip::pg_getnameinfo_all(
        &sa,
        Some(&mut host),
        None,
        NI_NUMERICHOST | NI_NUMERICSERV,
    );
    if rc != 0 {
        return Ok(fcinfo.return_null());
    }
    clean_ipv6_addr(::ip::sockaddr_family(&sa), &mut host);
    let v = crate::network_in(&host, false, None)?.expect("numeric host is valid inet input");
    inet_result(fcinfo, &v)
}

fn session_port(fcinfo: &mut Fcinfo, local: bool) -> PgResult<Datum> {
    let Some(sa) = session_sockaddr(local) else {
        return Ok(fcinfo.return_null());
    };
    let mut serv = String::new();
    let rc = ::ip::pg_getnameinfo_all(
        &sa,
        None,
        Some(&mut serv),
        NI_NUMERICHOST | NI_NUMERICSERV,
    );
    if rc != 0 {
        return Ok(fcinfo.return_null());
    }
    // DirectFunctionCall1(int4in): a numeric-service string always parses.
    let port: i32 = serv.parse().expect("numeric service string");
    Ok(Datum::from_i32(port))
}

fn session_sockaddr(local: bool) -> Option<::ip::SockAddr> {
    if !::init_small::globals::HaveMyProcPort() {
        return None;
    }
    let sa = ::init_small::globals::WithMyProcPort(|p| if local { p.laddr } else { p.raddr });
    match ::ip::sockaddr_family(&sa) {
        AF_INET | AF_INET6 => Some(sa),
        _ => None,
    }
}

// network.c:2060 clean_ipv6_addr — drop any '%zone' suffix from an IPv6
// numeric-host string (stored inet values carry no zone). pub: C exports it
// through builtins.h for pgstatfuncs.c's client_addr surfacing (C:541/956).
pub fn clean_ipv6_addr(family: i32, addr: &mut String) {
    if family == AF_INET6 {
        if let Some(pos) = addr.find('%') {
            addr.truncate(pos);
        }
    }
}

pub fn fc_inet_client_addr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    session_addr(fcinfo, false)
}

pub fn fc_inet_client_port(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    session_port(fcinfo, false)
}

pub fn fc_inet_server_addr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    session_addr(fcinfo, true)
}

pub fn fc_inet_server_port(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    session_port(fcinfo, true)
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

const fn nb(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: false,
        retset: false,
        func,
    }
}

// pg_proc.dat rows (all proisstrict, none retset), OID-ascending.
pub const NETWORK_BUILTINS: &[FmgrBuiltin] = &[
    b(422, "hashinet", 1, fc_hashinet),
    b(598, "inet_abbrev", 1, fc_inet_abbrev),
    b(599, "cidr_abbrev", 1, fc_cidr_abbrev),
    b(605, "inet_set_masklen", 2, fc_inet_set_masklen),
    b(635, "cidr_set_masklen", 2, fc_cidr_set_masklen),
    b(683, "network_network", 1, fc_network_network),
    b(696, "network_netmask", 1, fc_network_netmask),
    b(697, "network_masklen", 1, fc_network_masklen),
    b(698, "network_broadcast", 1, fc_network_broadcast),
    b(699, "network_host", 1, fc_network_host),
    b(711, "network_family", 1, fc_network_family),
    b(730, "network_show", 1, fc_network_show),
    b(779, "hashinetextended", 2, fc_hashinetextended),
    nb(2196, "inet_client_addr", 0, fc_inet_client_addr),
    nb(2197, "inet_client_port", 0, fc_inet_client_port),
    nb(2198, "inet_server_addr", 0, fc_inet_server_addr),
    nb(2199, "inet_server_port", 0, fc_inet_server_port),
    b(910, "inet_in", 1, fc_inet_in),
    b(911, "inet_out", 1, fc_inet_out),
    b(920, "network_eq", 2, fc_network_eq),
    b(921, "network_lt", 2, fc_network_lt),
    b(922, "network_le", 2, fc_network_le),
    b(923, "network_gt", 2, fc_network_gt),
    b(924, "network_ge", 2, fc_network_ge),
    b(925, "network_ne", 2, fc_network_ne),
    b(926, "network_cmp", 2, fc_network_cmp),
    b(927, "network_sub", 2, fc_network_sub),
    b(928, "network_subeq", 2, fc_network_subeq),
    b(929, "network_sup", 2, fc_network_sup),
    b(930, "network_supeq", 2, fc_network_supeq),
    b(1173, "network_subset_support", 1, fc_network_subset_support),
    b(1267, "cidr_in", 1, fc_cidr_in),
    b(1362, "network_hostmask", 1, fc_network_hostmask),
    b(1427, "cidr_out", 1, fc_cidr_out),
    b(1715, "inet_to_cidr", 1, fc_inet_to_cidr),
    b(2496, "inet_recv", 1, fc_inet_recv),
    b(2497, "inet_send", 1, fc_inet_send),
    b(2498, "cidr_recv", 1, fc_cidr_recv),
    b(2499, "cidr_send", 1, fc_cidr_send),
    b(2627, "inetnot", 1, fc_inetnot),
    b(2628, "inetand", 2, fc_inetand),
    b(2629, "inetor", 2, fc_inetor),
    b(2630, "inetpl", 2, fc_inetpl),
    b(2632, "inetmi_int8", 2, fc_inetmi_int8),
    b(2633, "inetmi", 2, fc_inetmi),
    b(3551, "network_overlap", 2, fc_network_overlap),
    b(3562, "network_larger", 2, fc_network_larger),
    b(3563, "network_smaller", 2, fc_network_smaller),
    b(4063, "inet_merge", 2, fc_inet_merge),
    b(4071, "inet_same_family", 2, fc_inet_same_family),
];
