//! fmgr wrappers (`fc_*`) + `BOOL_BUILTINS` for fmgr-core. Still deferred:
//! boolrecv/boolsend (pqformat wire frame is a separate unit) and the
//! bool_accum family (agg internal-state frame); value cores in the crate root.

use alloc::borrow::Cow;
use alloc::string::String;

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

#[cold]
#[inline(never)]
fn soft_context_unported(name: &str) -> ! {
    panic!("{name}: fcinfo.context soft-error demux is fmgr-core's unit (not ported)")
}

fn in_arg<'a>(fcinfo: &'a Fcinfo, name: &'static str) -> Cow<'a, str> {
    if fcinfo.context.is_some() {
        soft_context_unported(name);
    }
    // SAFETY: catalog arg 0 of boolin is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    String::from_utf8_lossy(s.to_bytes())
}

pub fn fc_boolin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let s = in_arg(fcinfo, "boolin");
    Ok(Datum::from_bool(crate::boolin(&s, None)?))
}

// C pallocs the 2-byte cstring per row; the backend thread owns retained
// scratch (the int.c out-function precedent). The Datum aliases it until the
// next out call on this thread.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; 2]> =
        const { core::cell::UnsafeCell::new([0; 2]) };
}

pub fn fc_boolout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let b = a.value.as_bool();
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        buf[0] = crate::boolout(b);
        buf[1] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

macro_rules! fc_bool2 {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::from_bool(crate::$core(a.value.as_bool(), b.value.as_bool())))
        }
    )*};
}

fc_bool2! {
    fc_booleq: booleq;
    fc_boolne: boolne;
    fc_boollt: boollt;
    fc_boolgt: boolgt;
    fc_boolle: boolle;
    fc_boolge: boolge;
    fc_booland_statefunc: booland_statefunc;
    fc_boolor_statefunc: boolor_statefunc;
}

pub fn fc_booltext(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::booltext(mcx, a.value.as_bool())?))
}

pub fn fc_hashbool(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_u32(crate::hashbool(a.value.as_bool())))
}

pub fn fc_hashboolextended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, seed] = fcinfo.args_n::<2>();
    Ok(Datum::from_u64(crate::hashboolextended(
        a.value.as_bool(),
        seed.value.as_u64(),
    )))
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
pub const BOOL_BUILTINS: &[FmgrBuiltin] = &[
    b(56, "boollt", 2, fc_boollt),
    b(57, "boolgt", 2, fc_boolgt),
    b(60, "booleq", 2, fc_booleq),
    b(84, "boolne", 2, fc_boolne),
    b(1242, "boolin", 1, fc_boolin),
    b(1243, "boolout", 1, fc_boolout),
    b(1691, "boolle", 2, fc_boolle),
    b(1692, "boolge", 2, fc_boolge),
    b(2515, "booland_statefunc", 2, fc_booland_statefunc),
    b(2516, "boolor_statefunc", 2, fc_boolor_statefunc),
    b(2971, "booltext", 1, fc_booltext),
    b(6417, "hashbool", 1, fc_hashbool),
    b(6418, "hashboolextended", 2, fc_hashboolextended),
];
