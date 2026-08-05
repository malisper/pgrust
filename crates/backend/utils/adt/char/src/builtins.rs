use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

macro_rules! fc_char2 {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::from_bool(crate::$core(a.value.as_char(), b.value.as_char())))
        }
    )*};
}

fc_char2! {
    fc_chareq: chareq;
    fc_charne: charne;
    fc_charlt: charlt;
    fc_charle: charle;
    fc_chargt: chargt;
    fc_charge: charge;
}

pub fn fc_charin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of charin is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    Ok(Datum::from_char(crate::charin(s.to_bytes())))
}

// C pallocs the (at most 5-byte) cstring per call; backend-thread retained
// scratch is the out-function precedent (bool.c/int.c). The Datum aliases it
// until the next charout on this thread.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; 5]> =
        const { core::cell::UnsafeCell::new([0; 5]) };
}

pub fn fc_charout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let ch = a.value.as_char();
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let mut img = [0u8; 4];
        let n = crate::charout(ch, &mut img);
        buf[..n].copy_from_slice(&img[..n]);
        buf[n] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_charrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_char(crate::charrecv(buf)?))
}

pub fn fc_charsend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::charsend(mcx, a.value.as_char())?))
}

pub fn fc_chartoi4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_i32(crate::chartoi4(a.value.as_char())))
}

pub fn fc_i4tochar(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_char(crate::i4tochar(a.value.as_i32())?))
}

pub fn fc_text_char(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 is a non-null text varlena, live for the call.
    let t = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_char(crate::text_char(t.data())))
}

pub fn fc_char_text(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::char_text(mcx, a.value.as_char())?))
}

pub fn fc_hashchar(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_u32(::hashfn::hash_bytes_uint32(a.value.as_char() as i32 as u32)))
}

pub fn fc_hashcharextended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, seed] = fcinfo.args_n::<2>();
    Ok(Datum::from_u64(::hashfn::hash_bytes_uint32_extended(
        a.value.as_char() as i32 as u32,
        seed.value.as_i64() as u64,
    )))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

// pg_proc.dat rows (all proisstrict, none retset), OID-ascending.
pub const CHAR_BUILTINS: &[FmgrBuiltin] = &[
    b(33, "charout", 1, fc_charout),
    b(61, "chareq", 2, fc_chareq),
    b(70, "charne", 2, fc_charne),
    b(72, "charle", 2, fc_charle),
    b(73, "chargt", 2, fc_chargt),
    b(74, "charge", 2, fc_charge),
    b(77, "chartoi4", 1, fc_chartoi4),
    b(78, "i4tochar", 1, fc_i4tochar),
    b(944, "text_char", 1, fc_text_char),
    b(946, "char_text", 1, fc_char_text),
    b(1245, "charin", 1, fc_charin),
    b(1246, "charlt", 2, fc_charlt),
    b(2434, "charrecv", 1, fc_charrecv),
    b(2435, "charsend", 1, fc_charsend),
];
