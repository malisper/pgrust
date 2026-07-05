//! fmgr wrappers + `REGPROC_BUILTINS`. reg* recv/send are oidrecv/oidsend
//! shapes on the binary-wire frame; to_reg* run the in-core under a local
//! non-details soft context (C DirectInputFunctionCallSafe).

use std::borrow::Cow;

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::{PgResult, SoftErrorContext};
use ::types_fmgr::{
    cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

fn in_arg<'a>(fcinfo: &'a Fcinfo) -> Cow<'a, str> {
    // SAFETY: catalog arg 0 of every reg*in is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    String::from_utf8_lossy(s.to_bytes())
}

macro_rules! fc_reg_in {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let result = {
                let s = in_arg(fcinfo);
                // SAFETY: context, if set, rides per the ErrorSaveNode contract.
                let esc = unsafe { fcinfo.soft_error_context() };
                crate::$core(fcinfo.result_mcx(), &s, esc)?
            };
            Ok(match result {
                Some(oid) => Datum::from_oid(oid),
                None => fcinfo.return_null(),
            })
        }
    )*};
}

fc_reg_in! {
    fc_regprocin: regprocin;
    fc_regprocedurein: regprocedurein;
    fc_regoperin: regoperin;
    fc_regoperatorin: regoperatorin;
    fc_regcollationin: regcollationin;
    fc_regconfigin: regconfigin;
    fc_regdictionaryin: regdictionaryin;
    fc_regclassin: regclassin;
    fc_regtypein: regtypein;
    fc_regnamespacein: regnamespacein;
    fc_regrolein: regrolein;
}

// Retained TLS scratch, reset at call entry: printtup's text lane stays on
// its unarmed fast path (the cash/int out-fn convention); the datum aliases
// the context until the next reg*out call on this thread.
fn with_out_scratch<R>(f: impl FnOnce(::mcx::Mcx<'_>) -> R) -> R {
    std::thread_local! {
        static OUT_CTX: core::cell::UnsafeCell<Option<::mcx::MemoryContext>> =
            const { core::cell::UnsafeCell::new(None) };
    }
    OUT_CTX.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let slot = unsafe { &mut *c.get() };
        let m = slot.get_or_insert_with(|| ::mcx::MemoryContext::new_bump("RegOutScratch"));
        m.reset();
        f(m.mcx())
    })
}

macro_rules! fc_reg_out {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let oid = fcinfo.arg(0).as_oid();
            let _ = fcinfo;
            with_out_scratch(|mcx| Ok(cstring_result(crate::$core(mcx, oid)?)))
        }
    )*};
}

fc_reg_out! {
    fc_regprocout: regprocout;
    fc_regprocedureout: regprocedureout;
    fc_regoperout: regoperout;
    fc_regoperatorout: regoperatorout;
    fc_regcollationout: regcollationout;
    fc_regconfigout: regconfigout;
    fc_regdictionaryout: regdictionaryout;
    fc_regclassout: regclassout;
    fc_regtypeout: regtypeout;
    fc_regnamespaceout: regnamespaceout;
    fc_regroleout: regroleout;
}

macro_rules! fc_to_reg {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let result = {
                // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
                let payload = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let payload = payload.data();
                let s = String::from_utf8_lossy(payload);
                let mut soft = SoftErrorContext::new(false);
                let r = crate::$core(fcinfo.result_mcx(), &s, Some(&mut soft))?;
                if soft.error_occurred() { None } else { r }
            };
            Ok(match result {
                Some(oid) => Datum::from_oid(oid),
                None => fcinfo.return_null(),
            })
        }
    )*};
}

fc_to_reg! {
    fc_to_regproc: regprocin;
    fc_to_regprocedure: regprocedurein;
    fc_to_regoper: regoperin;
    fc_to_regoperator: regoperatorin;
    fc_to_regcollation: regcollationin;
    fc_to_regclass: regclassin;
    fc_to_regtype: regtypein;
    fc_to_regnamespace: regnamespacein;
    fc_to_regrole: regrolein;
}

pub fn fc_to_regtypemod(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let result = {
        // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
        let payload = unsafe { fcinfo.arg_varlena_packed(0) }?;
        let payload = payload.data();
        let s = String::from_utf8_lossy(payload);
        let mut soft = SoftErrorContext::new(false);
        let r = crate::to_regtypemod(fcinfo.result_mcx(), &s, Some(&mut soft))?;
        if soft.error_occurred() { None } else { r }
    };
    Ok(match result {
        Some(typmod) => Datum::from_i32(typmod),
        None => fcinfo.return_null(),
    })
}

pub fn fc_text_regclass(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let payload = payload.data();
    let s = String::from_utf8_lossy(payload);
    Ok(Datum::from_oid(crate::text_regclass(
        fcinfo.result_mcx(),
        &s,
    )?))
}

pub fn fc_reg_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_oid(pqformat::pq_getmsgint(buf, 4)?))
}

pub fn fc_reg_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let oid = fcinfo.arg(0).as_oid();
    let mut b = pqformat::pq_begintypsend(fcinfo.result_mcx())?;
    pqformat::pq_sendint32(&mut b, oid)?;
    Ok(varlena_result(pqformat::pq_endtypsend(b).into()))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const REGPROC_BUILTINS: &[FmgrBuiltin] = &[
    b(44, "regprocin", 1, fc_regprocin),
    b(45, "regprocout", 1, fc_regprocout),
    b(1079, "text_regclass", 1, fc_text_regclass),
    b(2212, "regprocedurein", 1, fc_regprocedurein),
    b(2213, "regprocedureout", 1, fc_regprocedureout),
    b(2214, "regoperin", 1, fc_regoperin),
    b(2215, "regoperout", 1, fc_regoperout),
    b(2216, "regoperatorin", 1, fc_regoperatorin),
    b(2217, "regoperatorout", 1, fc_regoperatorout),
    b(2218, "regclassin", 1, fc_regclassin),
    b(2219, "regclassout", 1, fc_regclassout),
    b(2220, "regtypein", 1, fc_regtypein),
    b(2221, "regtypeout", 1, fc_regtypeout),
    b(2444, "regprocrecv", 1, fc_reg_recv),
    b(2445, "regprocsend", 1, fc_reg_send),
    b(2446, "regprocedurerecv", 1, fc_reg_recv),
    b(2448, "regoperrecv", 1, fc_reg_recv),
    b(2449, "regopersend", 1, fc_reg_send),
    b(2450, "regoperatorrecv", 1, fc_reg_recv),
    b(2451, "regoperatorsend", 1, fc_reg_send),
    b(2447, "regproceduresend", 1, fc_reg_send),
    b(2452, "regclassrecv", 1, fc_reg_recv),
    b(2453, "regclasssend", 1, fc_reg_send),
    b(2454, "regtyperecv", 1, fc_reg_recv),
    b(2455, "regtypesend", 1, fc_reg_send),
    b(3476, "to_regoperator", 1, fc_to_regoperator),
    b(3479, "to_regprocedure", 1, fc_to_regprocedure),
    b(3492, "to_regoper", 1, fc_to_regoper),
    b(3493, "to_regtype", 1, fc_to_regtype),
    b(3736, "regconfigin", 1, fc_regconfigin),
    b(3737, "regconfigout", 1, fc_regconfigout),
    b(3738, "regconfigrecv", 1, fc_reg_recv),
    b(3739, "regconfigsend", 1, fc_reg_send),
    b(3771, "regdictionaryin", 1, fc_regdictionaryin),
    b(3772, "regdictionaryout", 1, fc_regdictionaryout),
    b(3773, "regdictionaryrecv", 1, fc_reg_recv),
    b(3774, "regdictionarysend", 1, fc_reg_send),
    b(3494, "to_regproc", 1, fc_to_regproc),
    b(3495, "to_regclass", 1, fc_to_regclass),
    b(4084, "regnamespacein", 1, fc_regnamespacein),
    b(4085, "regnamespaceout", 1, fc_regnamespaceout),
    b(4086, "to_regnamespace", 1, fc_to_regnamespace),
    b(4087, "regnamespacerecv", 1, fc_reg_recv),
    b(4088, "regnamespacesend", 1, fc_reg_send),
    b(4092, "regroleout", 1, fc_regroleout),
    b(4093, "to_regrole", 1, fc_to_regrole),
    b(4094, "regrolerecv", 1, fc_reg_recv),
    b(4095, "regrolesend", 1, fc_reg_send),
    b(4098, "regrolein", 1, fc_regrolein),
    b(4193, "regcollationin", 1, fc_regcollationin),
    b(4194, "regcollationout", 1, fc_regcollationout),
    b(4195, "to_regcollation", 1, fc_to_regcollation),
    b(4196, "regcollationrecv", 1, fc_reg_recv),
    b(4197, "regcollationsend", 1, fc_reg_send),
    b(6317, "to_regtypemod", 1, fc_to_regtypemod),
];
