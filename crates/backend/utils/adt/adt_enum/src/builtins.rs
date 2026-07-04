use datum::Datum;
use types_core::{InvalidOid, NAMEDATALEN};
use types_error::PgResult;
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

pub fn fc_enum_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of enum_in is cstring (typlen -2).
    let name = unsafe { fcinfo.arg_cstring(0) };
    let name = core::str::from_utf8(name.to_bytes()).unwrap_or("");
    let [_, typoid] = fcinfo.args_n::<2>();
    // SAFETY: context, if set, rides per the ErrorSaveNode contract.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_oid(
        crate::enum_in(name, typoid.value.as_oid(), esc)?.unwrap_or(InvalidOid),
    ))
}

// C pstrdups the label per call; retained backend scratch, the bool/oid
// out-function precedent. The Datum aliases it until the next out call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; NAMEDATALEN as usize]> =
        const { core::cell::UnsafeCell::new([0; NAMEDATALEN as usize]) };
}

fn label_cstring(label: &[u8]) -> Datum {
    OUT_SCRATCH.with(|s| {
        // SAFETY: single-threaded backend; no other live borrow of the scratch.
        let buf = unsafe { &mut *s.get() };
        buf[..label.len()].copy_from_slice(label);
        buf[label.len()] = 0;
        Datum::from_usize(buf.as_ptr() as usize)
    })
}

pub fn fc_enum_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let en = crate::enum_out(a.value.as_oid())?;
    Ok(label_cstring(en.enumlabel.name_str()))
}

pub fn fc_enum_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg 0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let [_, typoid] = fcinfo.args_n::<2>();
    let mcx = fcinfo.result_mcx();
    let rawbytes = buf.len().saturating_sub(buf.cursor);
    let name = pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    let name = core::str::from_utf8(&name).unwrap_or("");
    // Hard error either way here (no ereturn in the C).
    match crate::enum_in(name, typoid.value.as_oid(), None)? {
        Some(oid) => Ok(Datum::from_oid(oid)),
        None => unreachable!("enum_in without escontext returned soft error"),
    }
}

pub fn fc_enum_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let en = crate::enum_out(a.value.as_oid())?;
    let mcx = fcinfo.result_mcx();
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendtext(&mut buf, en.enumlabel.name_str())?;
    Ok(varlena_result(pqformat::pq_endtypsend(buf)))
}

pub fn fc_enum_eq(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_bool(a.value.as_oid() == b.value.as_oid()))
}

pub fn fc_enum_ne(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_bool(a.value.as_oid() != b.value.as_oid()))
}

macro_rules! fc_cmp {
    ($($name:ident: $op:tt $rhs:literal;)+) => {
        $(pub fn $name(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            Ok(Datum::from_bool(crate::cmp_via(fcinfo, flinfo)? $op $rhs))
        })+
    };
}

fc_cmp! {
    fc_enum_lt: < 0;
    fc_enum_le: <= 0;
    fc_enum_ge: >= 0;
    fc_enum_gt: > 0;
}

pub fn fc_enum_cmp(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::cmp_via(fcinfo, flinfo)?))
}

pub fn fc_enum_smaller(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    let (a, b) = (a.value.as_oid(), b.value.as_oid());
    Ok(Datum::from_oid(if crate::cmp_via(fcinfo, flinfo)? < 0 { a } else { b }))
}

pub fn fc_enum_larger(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    let (a, b) = (a.value.as_oid(), b.value.as_oid());
    Ok(Datum::from_oid(if crate::cmp_via(fcinfo, flinfo)? > 0 { a } else { b }))
}

pub fn fc_hashenum(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_u32(hashfn::hash_bytes_uint32(a.value.as_oid())))
}

pub fn fc_hashenumextended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, seed] = fcinfo.args_n::<2>();
    Ok(Datum::from_u64(hashfn::hash_bytes_uint32_extended(a.value.as_oid(), seed.value.as_u64())))
}

pub fn fc_enum_first(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_oid(crate::enum_first_last(mcx, flinfo.as_deref(), false)?))
}

pub fn fc_enum_last(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_oid(crate::enum_first_last(mcx, flinfo.as_deref(), true)?))
}

pub fn fc_enum_range_bounds(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let lower = if fcinfo.argisnull(0) { InvalidOid } else { fcinfo.arg(0).as_oid() };
    let upper = if fcinfo.argisnull(1) { InvalidOid } else { fcinfo.arg(1).as_oid() };
    let enumtypoid = crate::enum_range_typoid(flinfo.as_deref())?;
    let mcx = fcinfo.result_mcx();
    crate::enum_range_internal(mcx, enumtypoid, lower, upper)
}

pub fn fc_enum_range_all(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let enumtypoid = crate::enum_range_typoid(flinfo.as_deref())?;
    let mcx = fcinfo.result_mcx();
    crate::enum_range_internal(mcx, enumtypoid, InvalidOid, InvalidOid)
}

const fn b(foid: types_core::Oid, name: &'static str, nargs: i16, strict: bool, func: types_fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict, retset: false, func }
}

// OIDs verified against pg_proc.dat 18.3.
pub static ENUM_BUILTINS: &[FmgrBuiltin] = &[
    b(3506, "enum_in", 2, true, fc_enum_in),
    b(3507, "enum_out", 1, true, fc_enum_out),
    b(3508, "enum_eq", 2, true, fc_enum_eq),
    b(3509, "enum_ne", 2, true, fc_enum_ne),
    b(3510, "enum_lt", 2, true, fc_enum_lt),
    b(3511, "enum_gt", 2, true, fc_enum_gt),
    b(3512, "enum_le", 2, true, fc_enum_le),
    b(3513, "enum_ge", 2, true, fc_enum_ge),
    b(3514, "enum_cmp", 2, true, fc_enum_cmp),
    b(3515, "hashenum", 1, true, fc_hashenum),
    b(3414, "hashenumextended", 2, true, fc_hashenumextended),
    b(3524, "enum_smaller", 2, true, fc_enum_smaller),
    b(3525, "enum_larger", 2, true, fc_enum_larger),
    b(3528, "enum_first", 1, false, fc_enum_first),
    b(3529, "enum_last", 1, false, fc_enum_last),
    b(3530, "enum_range_bounds", 2, false, fc_enum_range_bounds),
    b(3531, "enum_range_all", 1, false, fc_enum_range_all),
    b(3532, "enum_recv", 2, true, fc_enum_recv),
    b(3533, "enum_send", 1, true, fc_enum_send),
];
