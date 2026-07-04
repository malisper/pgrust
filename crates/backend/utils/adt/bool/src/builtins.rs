//! fmgr wrappers (`fc_*`) + `BOOL_BUILTINS` for fmgr-core; value cores in
//! the crate root. recv/send ride the binary-wire fmgr frame
//! (types_fmgr::wire).

use alloc::borrow::Cow;
use alloc::string::String;

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

pub fn fc_boolrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_bool(crate::boolrecv(buf)?))
}

pub fn fc_boolsend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::boolsend(mcx, a.value.as_bool())?))
}

fn in_arg<'a>(fcinfo: &'a Fcinfo) -> Cow<'a, str> {
    // SAFETY: catalog arg 0 of boolin is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    String::from_utf8_lossy(s.to_bytes())
}

pub fn fc_boolin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let s = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_bool(crate::boolin(&s, esc)?))
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

const fn bn(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
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
#[cold]
fn bool_non_agg_context() -> alloc::boxed::Box<::types_error::PgError> {
    alloc::boxed::Box::new(::types_error::PgError::error(
        "aggregate function called in non-aggregate context",
    ))
}

// bool_accum/bool_accum_inv/bool_alltrue/bool_anytrue (bool.c): INTERNAL
// BoolAggState allocated in the aggcontext on first call.
fn bool_agg_common(fcinfo: &mut Fcinfo, inverse: bool) -> PgResult<Datum> {
    use crate::BoolAggState;
    let [a, b] = *fcinfo.args_n::<2>();
    // SAFETY: fcinfo.context is the executor's live AggStateNode.
    let Some(agg_mcx) = (unsafe { fcinfo.agg_context() }) else {
        return Err(bool_non_agg_context());
    };
    let val = if b.isnull { None } else { Some(b.value.as_bool()) };
    let stp: *mut BoolAggState = if a.isnull {
        if inverse {
            return crate::bool_accum_inv(None, val).map(|_| unreachable!());
        }
        let layout = core::alloc::Layout::new::<BoolAggState>();
        let raw = ::mcx::Allocator::allocate(&agg_mcx, layout)
            .map_err(|_| agg_mcx.oom(layout.size()))?;
        let p: *mut BoolAggState = raw.cast().as_ptr();
        // SAFETY: fresh aggcontext allocation of the exact layout.
        unsafe { p.write(BoolAggState::default()) };
        p
    } else {
        a.value.as_usize() as *mut BoolAggState
    };
    // SAFETY: a non-null arg0 is the aggcontext-lived state this transfn
    // chain returned; no other reference is live during the call.
    unsafe {
        let st = if inverse {
            crate::bool_accum_inv(Some(*stp), val)?
        } else {
            crate::bool_accum(Some(*stp), val)
        };
        *stp = st;
    }
    Ok(Datum::from_usize(stp as usize))
}

pub fn fc_bool_accum(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    bool_agg_common(fcinfo, false)
}

pub fn fc_bool_accum_inv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    bool_agg_common(fcinfo, true)
}

fn bool_agg_final(fcinfo: &mut Fcinfo, any: bool) -> PgResult<Datum> {
    use crate::BoolAggState;
    let [a] = *fcinfo.args_n::<1>();
    let state = if a.isnull {
        None
    } else {
        // SAFETY: a non-null arg0 is the aggcontext-lived state; read-only.
        Some(unsafe { &*(a.value.as_usize() as *const BoolAggState) })
    };
    let r = if any { crate::bool_anytrue(state) } else { crate::bool_alltrue(state) };
    match r {
        Some(v) => Ok(Datum::from_bool(v)),
        None => {
            fcinfo.isnull = true;
            Ok(Datum::null())
        }
    }
}

pub fn fc_bool_alltrue(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    bool_agg_final(fcinfo, false)
}

pub fn fc_bool_anytrue(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    bool_agg_final(fcinfo, true)
}

pub const BOOL_BUILTINS: &[FmgrBuiltin] = &[
    b(56, "boollt", 2, fc_boollt),
    b(57, "boolgt", 2, fc_boolgt),
    b(60, "booleq", 2, fc_booleq),
    b(84, "boolne", 2, fc_boolne),
    b(1242, "boolin", 1, fc_boolin),
    b(1243, "boolout", 1, fc_boolout),
    b(1691, "boolle", 2, fc_boolle),
    b(1692, "boolge", 2, fc_boolge),
    b(2436, "boolrecv", 1, fc_boolrecv),
    b(2437, "boolsend", 1, fc_boolsend),
    b(2515, "booland_statefunc", 2, fc_booland_statefunc),
    b(2516, "boolor_statefunc", 2, fc_boolor_statefunc),
    b(2971, "booltext", 1, fc_booltext),
    bn(3496, "bool_accum", 2, fc_bool_accum),
    bn(3497, "bool_accum_inv", 2, fc_bool_accum_inv),
    b(3498, "bool_alltrue", 1, fc_bool_alltrue),
    b(3499, "bool_anytrue", 1, fc_bool_anytrue),
    b(6417, "hashbool", 1, fc_hashbool),
    b(6418, "hashboolextended", 2, fc_hashboolextended),
];
