use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

macro_rules! fc_oid2 {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::from_bool(crate::$core(a.value.as_oid(), b.value.as_oid())))
        }
    )*};
}

fc_oid2! {
    fc_oideq: oideq;
    fc_oidne: oidne;
    fc_oidlt: oidlt;
    fc_oidle: oidle;
    fc_oidgt: oidgt;
    fc_oidge: oidge;
}

macro_rules! fc_oid2_oid {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::from_oid(crate::$core(a.value.as_oid(), b.value.as_oid())))
        }
    )*};
}

fc_oid2_oid! {
    fc_oidlarger: oidlarger;
    fc_oidsmaller: oidsmaller;
}

// Result Datum aliases the scratch: consume before the next out call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; 16]> =
        const { core::cell::UnsafeCell::new([0; 16]) };
}

pub fn fc_xidout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let v = a.value.as_u32();
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::xidout(v, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

macro_rules! fc_xid2 {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::from_bool(crate::$core(a.value.as_u32(), b.value.as_u32())))
        }
    )*};
}

fc_xid2! {
    fc_xideq: xideq;
    fc_xidneq: xidneq;
}

pub fn fc_xid_age(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_i32(crate::xid_age(a.value.as_u32())?))
}

pub fn fc_mxid_age(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_i32(crate::mxid_age(a.value.as_u32())?))
}

pub fn fc_oidin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of oidin is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    // SAFETY: context, if set, rides per the ErrorSaveNode contract.
    let esc = unsafe { fcinfo.soft_error_context() };
    let (v, _) = ::numutils::uint32in_subr(&s, false, "oid", esc)?;
    Ok(Datum::from_oid(v))
}

// C pallocs 12 bytes per row; the int.c retained-TLS out convention instead.
std::thread_local! {
    static OID_OUT_SCRATCH: core::cell::UnsafeCell<[u8; 12]> =
        const { core::cell::UnsafeCell::new([0; 12]) };
}

pub fn fc_oidout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let v = a.value.as_oid();
    OID_OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = ::numutils::pg_ultoa_n(v, &mut buf[..11]);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_oidrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of oidrecv is internal (StringInfo).
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_oid(::pqformat::pq_getmsgint(buf, 4)? as u32))
}

pub fn fc_oidsend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, a.value.as_oid())?;
    Ok(::types_fmgr::varlena_result(::pqformat::pq_endtypsend(buf)))
}

pub fn fc_xidin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of xidin is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    // SAFETY: context, if set, rides per the ErrorSaveNode contract.
    let esc = unsafe { fcinfo.soft_error_context() };
    let (v, _) = ::numutils::uint32in_subr(&s, false, "xid", esc)?;
    Ok(Datum::from_u32(v))
}

pub fn fc_cidin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of cidin is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    // SAFETY: context, if set, rides per the ErrorSaveNode contract.
    let esc = unsafe { fcinfo.soft_error_context() };
    let (v, _) = ::numutils::uint32in_subr(&s, false, "cid", esc)?;
    Ok(Datum::from_u32(v))
}

pub fn fc_cidout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let v = a.value.as_u32();
    OID_OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = ::numutils::pg_ultoa_n(v, &mut buf[..11]);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_cideq(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_bool(a.value.as_u32() == b.value.as_u32()))
}

pub fn fc_xidrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of xidrecv/cidrecv is internal (StringInfo).
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_u32(::pqformat::pq_getmsgint(buf, 4)? as u32))
}

pub fn fc_xidsend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, a.value.as_u32())?;
    Ok(::types_fmgr::varlena_result(::pqformat::pq_endtypsend(buf)))
}

pub fn fc_hash_uint32(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_u32(::hashfn::hash_bytes_uint32(a.value.as_u32())))
}

pub fn fc_hash_uint32_extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_u64(::hashfn::hash_bytes_uint32_extended(
        a.value.as_u32(),
        b.value.as_i64() as u64,
    )))
}

pub fn fc_xid8in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of xid8in is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    // SAFETY: context, if set, rides per the ErrorSaveNode contract.
    let esc = unsafe { fcinfo.soft_error_context() };
    let (v, _) = ::numutils::uint64in_subr(&s, false, "xid8", esc)?;
    Ok(Datum::from_u64(v))
}

std::thread_local! {
    static XID8_OUT_SCRATCH: core::cell::UnsafeCell<[u8; 21]> =
        const { core::cell::UnsafeCell::new([0; 21]) };
}

pub fn fc_xid8out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let v = a.value.as_u64();
    XID8_OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = ::numutils::pg_ulltoa_n(v, &mut buf[..20]);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_xid8recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of xid8recv is internal (StringInfo).
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_u64(::pqformat::pq_getmsgint64(buf)? as u64))
}

pub fn fc_xid8send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint64(&mut buf, a.value.as_u64())?;
    Ok(::types_fmgr::varlena_result(::pqformat::pq_endtypsend(buf)))
}

macro_rules! fc_xid8_cmp {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::from_bool(a.value.as_u64() $op b.value.as_u64()))
        }
    )*};
}

fc_xid8_cmp! {
    fc_xid8eq: ==;
    fc_xid8ne: !=;
    fc_xid8lt: <;
    fc_xid8gt: >;
    fc_xid8le: <=;
    fc_xid8ge: >=;
}

pub fn fc_xid8cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i32(crate::xid8cmp(a.value.as_u64(), b.value.as_u64())))
}

pub fn fc_xid8toxid(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_u32(a.value.as_u64() as u32))
}

pub fn fc_xid8_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_u64(a.value.as_u64().max(b.value.as_u64())))
}

pub fn fc_xid8_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_u64(a.value.as_u64().min(b.value.as_u64())))
}

pub fn fc_hashxid8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let val = a.value.as_i64();
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    let lohalf = lohalf ^ if val >= 0 { hihalf } else { !hihalf };
    Ok(Datum::from_u32(::hashfn::hash_bytes_uint32(lohalf)))
}

pub fn fc_hashxid8extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    let val = a.value.as_i64();
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    let lohalf = lohalf ^ if val >= 0 { hihalf } else { !hihalf };
    Ok(Datum::from_u64(::hashfn::hash_bytes_uint32_extended(
        lohalf,
        b.value.as_i64() as u64,
    )))
}

use crate::Tid;

#[inline]
fn arg_tid(fcinfo: &Fcinfo, i: usize) -> Tid {
    // SAFETY: catalog args of these strict fns are non-null 6-byte tid
    // blocks (BlockIdData hi/lo u16 + OffsetNumber u16), live for the call.
    let b = unsafe { fcinfo.arg_fixed(i, 6) };
    let hi = u16::from_ne_bytes([b[0], b[1]]);
    let lo = u16::from_ne_bytes([b[2], b[3]]);
    Tid {
        block: ((hi as u32) << 16) | lo as u32,
        offset: u16::from_ne_bytes([b[4], b[5]]),
    }
}

fn tid_image(tid: Tid) -> [u8; 6] {
    let hi = ((tid.block >> 16) as u16).to_ne_bytes();
    let lo = (tid.block as u16).to_ne_bytes();
    let off = tid.offset.to_ne_bytes();
    [hi[0], hi[1], lo[0], lo[1], off[0], off[1]]
}

fn tid_result(fcinfo: &mut Fcinfo, tid: Tid) -> PgResult<Datum> {
    ::types_fmgr::byref_result(fcinfo.result_mcx(), &tid_image(tid))
}

#[cold]
fn tid_syntax(s: &str) -> ::types_error::PgError {
    ::types_error::PgError::error(format!("invalid input syntax for type {}: \"{s}\"", "tid"))
        .with_sqlstate(::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION)
}

pub fn fc_tidin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of tidin is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    match crate::tidin(s.as_bytes()) {
        Some(tid) => tid_result(fcinfo, tid),
        None => {
            // SAFETY: context, if set, rides per the ErrorSaveNode contract.
            let esc = unsafe { fcinfo.soft_error_context() };
            ::types_error::ereturn(esc, Datum::null(), tid_syntax(&s))
        }
    }
}

std::thread_local! {
    static TID_OUT_SCRATCH: core::cell::UnsafeCell<[u8; 32]> =
        const { core::cell::UnsafeCell::new([0; 32]) };
}

pub fn fc_tidout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let tid = arg_tid(fcinfo, 0);
    TID_OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::tidout(tid, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_tidrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let block = ::pqformat::pq_getmsgint(buf, 4)? as u32;
    let offset = ::pqformat::pq_getmsgint(buf, 2)? as u16;
    tid_result(fcinfo, Tid { block, offset })
}

pub fn fc_tidsend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let tid = arg_tid(fcinfo, 0);
    let mcx = fcinfo.result_mcx();
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, tid.block)?;
    ::pqformat::pq_sendint16(&mut buf, tid.offset)?;
    Ok(::types_fmgr::varlena_result(::pqformat::pq_endtypsend(buf)))
}

macro_rules! fc_tid_cmp {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (a, b) = (arg_tid(fcinfo, 0), arg_tid(fcinfo, 1));
            Ok(Datum::from_bool(crate::tid_cmp(a, b) $op 0))
        }
    )*};
}

fc_tid_cmp! {
    fc_tideq: ==;
    fc_tidne: !=;
    fc_tidlt: <;
    fc_tidgt: >;
    fc_tidle: <=;
    fc_tidge: >=;
}

pub fn fc_bttidcmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (arg_tid(fcinfo, 0), arg_tid(fcinfo, 1));
    Ok(Datum::from_i32(crate::tid_cmp(a, b)))
}

pub fn fc_tidlarger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (arg_tid(fcinfo, 0), arg_tid(fcinfo, 1));
    tid_result(fcinfo, crate::tid_larger(a, b))
}

pub fn fc_tidsmaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = (arg_tid(fcinfo, 0), arg_tid(fcinfo, 1));
    tid_result(fcinfo, crate::tid_smaller(a, b))
}

pub fn fc_hashtid(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a live 6-byte tid block; C hashes the raw
    // unpadded field bytes.
    let b = unsafe { fcinfo.arg_fixed(0, 6) };
    Ok(Datum::from_u32(::hashfn::hash_bytes(b)))
}

pub fn fc_hashtidextended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: as fc_hashtid.
    let b = unsafe { fcinfo.arg_fixed(0, 6) };
    let seed = fcinfo.arg_i64(1) as u64;
    Ok(Datum::from_u64(::hashfn::hash_bytes_extended(b, seed)))
}


pub fn fc_oidvectoreq(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict catalog args are non-null plain-storage oidvectors;
    // dim1 Oids follow the 24-byte header (buildoidvector shape).
    let read = |d: Datum| unsafe {
        let p = d.as_usize() as *const ::array::oidvector;
        let v = &*p;
        debug_assert!(v.ndim == 1 && v.dataoffset == 0);
        core::slice::from_raw_parts(p.add(1) as *const Oid, v.dim1.max(0) as usize)
    };
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_bool(read(a.value) == read(b.value)))
}

// SAFETY: strict catalog arg is a non-null plain-storage oidvector (the
// fc_oidvectoreq read contract).
unsafe fn oidvector_bytes(fcinfo: &Fcinfo, i: usize) -> PgResult<&[u8]> {
    let p = fcinfo.arg(i).as_usize() as *const ::array::oidvector;
    let v = unsafe { &*p };
    // C (hashfunc.c hashoidvector) calls check_valid_oidvector, which
    // ereports. A debug_assert here left release builds hashing vectors C
    // refuses (proofs finding: release-effective gate owed).
    ::nbt_compare::check_valid_oidvector(v)?;
    Ok(unsafe {
        core::slice::from_raw_parts(
            p.add(1) as *const u8,
            (v.dim1.max(0) as usize) * core::mem::size_of::<Oid>(),
        )
    })
}

pub fn fc_hashoidvector(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: as fc_oidvectoreq.
    let b = unsafe { oidvector_bytes(fcinfo, 0) }?;
    Ok(Datum::from_u32(::hashfn::hash_bytes(b)))
}

pub fn fc_hashoidvectorextended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: as fc_oidvectoreq.
    let b = unsafe { oidvector_bytes(fcinfo, 0) }?;
    let seed = fcinfo.arg_i64(1) as u64;
    Ok(Datum::from_u64(::hashfn::hash_bytes_extended(b, seed)))
}

pub fn fc_currtid_byrelname(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of currtid_byrelname is a non-null text (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let relname = String::from_utf8_lossy(payload.data()).into_owned();
    let tid = arg_tid(fcinfo, 1);
    let mcx = fcinfo.result_mcx();
    let ip = ::types_tuple::ItemPointerData::new(tid.block, tid.offset);
    let result = crate::currtid_byrelname(mcx, &relname, ip)?;
    tid_result(fcinfo, Tid { block: ::types_tuple::ItemPointerGetBlockNumberNoCheck(&result), offset: result.ip_posid })
}

// SAFETY: as fc_oidvectoreq, but layout-checked (SQL-boundary contract).
unsafe fn arg_oidvector_checked<'a>(
    fcinfo: &Fcinfo,
    i: usize,
) -> PgResult<(&'a ::array::oidvector, &'a [Oid])> {
    let p = fcinfo.arg(i).as_usize() as *const ::array::oidvector;
    let v = unsafe { &*p };
    ::nbt_compare::check_valid_oidvector(v)?;
    let values =
        unsafe { core::slice::from_raw_parts(p.add(1) as *const Oid, v.dim1.max(0) as usize) };
    Ok((v, values))
}

const OIDVECTOR_HDRSZ: usize = core::mem::size_of::<::array::oidvector>();

pub fn fc_oidvectorin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of oidvectorin is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    // SAFETY: context, if set, rides per the ErrorSaveNode contract.
    let mut esc = unsafe { fcinfo.soft_error_context() };
    let mcx = fcinfo.result_mcx();
    let mut img: ::mcx::PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, OIDVECTOR_HDRSZ + 32 * 4)?;
    img.resize(OIDVECTOR_HDRSZ, 0);
    let mut rest: &str = &s;
    let mut n: i32 = 0;
    loop {
        // C-locale isspace (includes \x0B).
        rest = rest
            .trim_start_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\x0B' | '\x0C' | '\r'));
        if rest.is_empty() {
            break;
        }
        let (v, r) = ::numutils::uint32in_subr(rest, true, "oid", esc.as_deref_mut())?;
        ::mcx::vec_append_bytes(&mut img, &v.to_ne_bytes())?;
        rest = r;
        n += 1;
    }
    let total = img.len();
    img[0..4].copy_from_slice(&::datum::varlena::set_varsize_4b(total));
    img[4..8].copy_from_slice(&1i32.to_ne_bytes());
    img[8..12].copy_from_slice(&0i32.to_ne_bytes());
    img[12..16].copy_from_slice(&(::types_core::catalog::OIDOID as u32).to_ne_bytes());
    img[16..20].copy_from_slice(&n.to_ne_bytes());
    img[20..24].copy_from_slice(&0i32.to_ne_bytes());
    Ok(Datum::from_usize(img.leak().as_ptr() as usize))
}

pub fn fc_oidvectorout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict catalog arg is a non-null plain-storage oidvector.
    let (_, values) = unsafe { arg_oidvector_checked(fcinfo, 0) }?;
    let mcx = fcinfo.result_mcx();
    let mut out: ::mcx::PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, values.len() * 12 + 1)?;
    let mut scratch = [0u8; 10];
    for (i, &v) in values.iter().enumerate() {
        if i != 0 {
            ::mcx::vec_append_bytes(&mut out, b" ")?;
        }
        let len = ::numutils::pg_ultoa_n(v, &mut scratch);
        ::mcx::vec_append_bytes(&mut out, &scratch[..len])?;
    }
    ::mcx::vec_append_bytes(&mut out, b"\0")?;
    Ok(::types_fmgr::cstring_result(out))
}

macro_rules! fc_oidvector_cmp {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: as fc_oidvectoreq.
            let (a, a_values) = unsafe { arg_oidvector_checked(fcinfo, 0) }?;
            let (b, b_values) = unsafe { arg_oidvector_checked(fcinfo, 1) }?;
            let cmp = ::nbt_compare::btoidvectorcmp(a, a_values, b, b_values);
            Ok(Datum::from_bool(cmp $op 0))
        }
    )*};
}

fc_oidvector_cmp! {
    fc_oidvectorne: !=;
    fc_oidvectorlt: <;
    fc_oidvectorle: <=;
    fc_oidvectorge: >=;
    fc_oidvectorgt: >;
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

// pg_proc.dat rows (all proisstrict, none retset), OID-ascending.
pub const SCALAR_BUILTINS: &[FmgrBuiltin] = &[
    b(51, "xidout", 1, fc_xidout),
    b(68, "xideq", 2, fc_xideq),
    b(3308, "xidneq", 2, fc_xidneq),
    b(1181, "xid_age", 1, fc_xid_age),
    b(3939, "mxid_age", 1, fc_mxid_age),
    b(184, "oideq", 2, fc_oideq),
    b(185, "oidne", 2, fc_oidne),
    b(54, "oidvectorin", 1, fc_oidvectorin),
    b(55, "oidvectorout", 1, fc_oidvectorout),
    b(619, "oidvectorne", 2, fc_oidvectorne),
    b(677, "oidvectorlt", 2, fc_oidvectorlt),
    b(678, "oidvectorle", 2, fc_oidvectorle),
    b(679, "oidvectoreq", 2, fc_oidvectoreq),
    b(680, "oidvectorge", 2, fc_oidvectorge),
    b(681, "oidvectorgt", 2, fc_oidvectorgt),
    b(457, "hashoidvector", 1, fc_hashoidvector),
    b(776, "hashoidvectorextended", 2, fc_hashoidvectorextended),
    b(716, "oidlt", 2, fc_oidlt),
    b(1965, "oidlarger", 2, fc_oidlarger),
    b(1966, "oidsmaller", 2, fc_oidsmaller),
    b(717, "oidle", 2, fc_oidle),
    b(1638, "oidgt", 2, fc_oidgt),
    b(1639, "oidge", 2, fc_oidge),
    b(1798, "oidin", 1, fc_oidin),
    b(1799, "oidout", 1, fc_oidout),
    b(2418, "oidrecv", 1, fc_oidrecv),
    b(2419, "oidsend", 1, fc_oidsend),
    b(50, "xidin", 1, fc_xidin),
    b(52, "cidin", 1, fc_cidin),
    b(53, "cidout", 1, fc_cidout),
    b(69, "cideq", 2, fc_cideq),
    b(445, "hashoidextended", 2, fc_hash_uint32_extended),
    b(453, "hashoid", 1, fc_hash_uint32),
    b(1319, "xideq", 2, fc_xideq),
    b(2440, "xidrecv", 1, fc_xidrecv),
    b(2441, "xidsend", 1, fc_xidsend),
    b(2442, "cidrecv", 1, fc_xidrecv),
    b(2443, "cidsend", 1, fc_xidsend),
    b(3309, "xidneq", 2, fc_xidneq),
    b(5034, "xid8lt", 2, fc_xid8lt),
    b(5035, "xid8gt", 2, fc_xid8gt),
    b(5036, "xid8le", 2, fc_xid8le),
    b(5037, "xid8ge", 2, fc_xid8ge),
    b(5070, "xid8in", 1, fc_xid8in),
    b(5071, "xid8toxid", 1, fc_xid8toxid),
    b(5081, "xid8out", 1, fc_xid8out),
    b(5082, "xid8recv", 1, fc_xid8recv),
    b(5083, "xid8send", 1, fc_xid8send),
    b(5084, "xid8eq", 2, fc_xid8eq),
    b(5085, "xid8ne", 2, fc_xid8ne),
    b(5096, "xid8cmp", 2, fc_xid8cmp),
    b(5097, "xid8_larger", 2, fc_xid8_larger),
    b(5098, "xid8_smaller", 2, fc_xid8_smaller),
    b(6419, "hashxid", 1, fc_hash_uint32),
    b(6420, "hashxidextended", 2, fc_hash_uint32_extended),
    b(6421, "hashxid8", 1, fc_hashxid8),
    b(6422, "hashxid8extended", 2, fc_hashxid8extended),
    b(6423, "hashcid", 1, fc_hash_uint32),
    b(6424, "hashcidextended", 2, fc_hash_uint32_extended),
    b(48, "tidin", 1, fc_tidin),
    b(49, "tidout", 1, fc_tidout),
    b(1265, "tidne", 2, fc_tidne),
    b(1292, "tideq", 2, fc_tideq),
    b(2233, "hashtid", 1, fc_hashtid),
    b(2234, "hashtidextended", 2, fc_hashtidextended),
    b(2438, "tidrecv", 1, fc_tidrecv),
    b(2439, "tidsend", 1, fc_tidsend),
    b(2790, "tidgt", 2, fc_tidgt),
    b(2791, "tidlt", 2, fc_tidlt),
    b(2792, "tidge", 2, fc_tidge),
    b(2793, "tidle", 2, fc_tidle),
    b(2794, "bttidcmp", 2, fc_bttidcmp),
    b(2795, "tidlarger", 2, fc_tidlarger),
    b(2796, "tidsmaller", 2, fc_tidsmaller),
    b(1294, "currtid_byrelname", 2, fc_currtid_byrelname),
];
