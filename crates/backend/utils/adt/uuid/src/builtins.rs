//! fmgr wrappers (`fc_*`) + `UUID_BUILTINS` for fmgr-core. Value cores live in
//! the crate root.

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::{PgUuid, UUID_OUT_LEN};

#[inline]
fn arg_uuid(fcinfo: &Fcinfo, i: usize) -> PgUuid {
    // SAFETY: catalog args of these strict fns are non-null 16-byte uuid blocks.
    *unsafe { fcinfo.arg_uuid(i) }
}

pub fn fc_uuid_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of uuid_in is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let uuid = crate::uuid_in(s.to_bytes(), esc)?;
    byref_result(fcinfo.result_mcx(), &uuid)
}

// C pallocs the cstring per row; the backend thread owns retained scratch
// (the macaddr_out precedent). The Datum aliases it until the next out call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; UUID_OUT_LEN + 1]> =
        const { core::cell::UnsafeCell::new([0; UUID_OUT_LEN + 1]) };
}

pub fn fc_uuid_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let uuid = arg_uuid(fcinfo, 0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let body: &mut [u8; UUID_OUT_LEN] = (&mut buf[..UUID_OUT_LEN]).try_into().unwrap();
        let len = crate::uuid_out_into(&uuid, body);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

macro_rules! fc_uuid2 {
    ($($fc:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (a, b) = (arg_uuid(fcinfo, 0), arg_uuid(fcinfo, 1));
            Ok(Datum::$conv(crate::$core(&a, &b)))
        }
    )*};
}

fc_uuid2! {
    fc_uuid_lt: uuid_lt -> from_bool;
    fc_uuid_le: uuid_le -> from_bool;
    fc_uuid_eq: uuid_eq -> from_bool;
    fc_uuid_ge: uuid_ge -> from_bool;
    fc_uuid_gt: uuid_gt -> from_bool;
    fc_uuid_ne: uuid_ne -> from_bool;
    fc_uuid_cmp: uuid_internal_cmp -> from_i32;
}

pub fn fc_uuid_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let uuid = crate::uuid_recv(buf)?;
    byref_result(fcinfo.result_mcx(), &uuid)
}

pub fn fc_uuid_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let uuid = arg_uuid(fcinfo, 0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::uuid_send(mcx, &uuid)?))
}

pub fn fc_uuid_hash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_u32(crate::uuid_hash(&arg_uuid(fcinfo, 0))))
}

pub fn fc_uuid_hash_extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let seed = fcinfo.arg(1).as_u64();
    Ok(Datum::from_u64(crate::uuid_hash_extended(
        &arg_uuid(fcinfo, 0),
        seed,
    )))
}

pub fn fc_uuid_extract_timestamp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    match crate::uuid_extract_timestamp(&arg_uuid(fcinfo, 0)) {
        Some(ts) => Ok(Datum::from_i64(ts)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_uuid_extract_version(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    match crate::uuid_extract_version(&arg_uuid(fcinfo, 0)) {
        Some(v) => Ok(Datum::from_u16(v)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_gen_random_uuid(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let uuid = crate::gen_random_uuid()?;
    byref_result(fcinfo.result_mcx(), &uuid)
}

pub fn fc_uuidv7(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let uuid = crate::uuidv7()?;
    byref_result(fcinfo.result_mcx(), &uuid)
}

pub fn fc_uuidv7_interval(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of uuidv7_interval is a non-null interval
    // (typlen 16, typalign d), live for the call.
    let shift = unsafe {
        let p = fcinfo.arg_ptr(0);
        ::adt_datetime::Interval {
            time: (p as *const i64).read_unaligned(),
            day: (p.add(8) as *const i32).read_unaligned(),
            month: (p.add(12) as *const i32).read_unaligned(),
        }
    };
    let uuid = crate::uuidv7_interval(&shift)?;
    byref_result(fcinfo.result_mcx(), &uuid)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

// pg_proc.dat rows for uuid.c (all proisstrict, none retset), OID-ascending.
// 3300 uuid_sortsupport / 6410 uuid_skipsupport unregistered (node frame absent).
pub const UUID_BUILTINS: &[FmgrBuiltin] = &[
    b(2952, "uuid_in", 1, fc_uuid_in),
    b(2953, "uuid_out", 1, fc_uuid_out),
    b(2954, "uuid_lt", 2, fc_uuid_lt),
    b(2955, "uuid_le", 2, fc_uuid_le),
    b(2956, "uuid_eq", 2, fc_uuid_eq),
    b(2957, "uuid_ge", 2, fc_uuid_ge),
    b(2958, "uuid_gt", 2, fc_uuid_gt),
    b(2959, "uuid_ne", 2, fc_uuid_ne),
    b(2960, "uuid_cmp", 2, fc_uuid_cmp),
    b(2961, "uuid_recv", 1, fc_uuid_recv),
    b(2962, "uuid_send", 1, fc_uuid_send),
    b(2963, "uuid_hash", 1, fc_uuid_hash),
    b(3412, "uuid_hash_extended", 2, fc_uuid_hash_extended),
    b(3432, "gen_random_uuid", 0, fc_gen_random_uuid),
    b(6342, "uuid_extract_timestamp", 1, fc_uuid_extract_timestamp),
    b(6343, "uuid_extract_version", 1, fc_uuid_extract_version),
    b(6428, "gen_random_uuid", 0, fc_gen_random_uuid),
    b(6429, "uuidv7", 0, fc_uuidv7),
    b(6430, "uuidv7_interval", 1, fc_uuidv7_interval),
];
