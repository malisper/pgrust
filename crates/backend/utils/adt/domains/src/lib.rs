//! domains.c I/O slice; constraint checks route through
//! typcache_seams::domain_check_input (compiled-check engine lives with
//! execexpr — this crate sits under fmgr_core).

#![allow(non_snake_case)]

use datum::Datum;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH};
use types_fmgr::{
    input_function_call_safe, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
};

const TYPTYPE_DOMAIN: i8 = b'd' as i8;

struct DomainIOData {
    domain_type: Oid,
    typioparam: Oid,
    typtypmod: i32,
    proc: FmgrInfo,
}

fn domain_state_setup(domainType: Oid, binary: bool) -> PgResult<DomainIOData> {
    let Some(base) = syscache_seams::pg_type_base_shape::call(domainType)? else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for type {domainType}"
        ))));
    };
    if base.typtype != TYPTYPE_DOMAIN {
        let t = format_type::format_type_be(domainType).unwrap_or_else(|_| domainType.to_string());
        return Err(Box::new(
            PgError::error(format!("type {t} is not a domain"))
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
        ));
    }
    let mut typtypmod = -1;
    let baseType = lsyscache::getBaseTypeAndTypmod(domainType, &mut typtypmod)?;
    // C domain_state_setup(binary): the base type's typreceive for the wire
    // lane, typinput for the text lane.
    let (typiofunc, typioparam) = if binary {
        lsyscache::getTypeBinaryInputInfo(baseType)?
    } else {
        lsyscache::getTypeInputInfo(baseType)?
    };
    let proc = fmgr_seams::fmgr_info::call(typiofunc)?;
    Ok(DomainIOData { domain_type: domainType, typioparam, typtypmod, proc })
}

pub fn fc_domain_in(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // InputFunctionCallSafe passes a NULL cstring with isnull=false (C's
    // convention); NULL-ness of arg 0 is the pointer, not the null flag.
    let string = if fcinfo.args[0].isnull || fcinfo.arg(0).as_usize() == 0 {
        None
    } else {
        // SAFETY: non-null arg 0 of domain_in is a cstring.
        Some(unsafe { fcinfo.arg_cstring(0) })
    };
    if fcinfo.args[1].isnull {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }
    let domainType = fcinfo.arg(1).as_oid();

    let flinfo = flinfo.expect("domain_in: NULL flinfo");
    let stale = match flinfo.fn_extra_ref::<DomainIOData>() {
        Some(d) => d.domain_type != domainType,
        None => true,
    };
    if stale {
        flinfo.set_fn_extra(domain_state_setup(domainType, false)?);
    }

    let mcx = fcinfo.result_mcx();
    // SAFETY: fcinfo.context, if set, is a live ErrorSaveNode armed for this call.
    let esc = unsafe { fcinfo.error_save_node() };
    let mut value = Datum::null();
    let my = flinfo.fn_extra_mut::<DomainIOData>().expect("just installed");
    if !input_function_call_safe(
        &mut my.proc,
        string,
        my.typioparam,
        my.typtypmod,
        mcx,
        esc,
        &mut value,
    )? {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }

    // SAFETY: fcinfo.context, if set, is a live ErrorSaveNode armed for this call.
    let esc = unsafe { fcinfo.error_save_node() };
    typcache_seams::domain_check_input::call(
        value,
        string.is_none(),
        domainType,
        esc.map(|n| &mut n.ctx),
    )?;

    if string.is_none() {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }
    Ok(value)
}

// C domain_recv (domains.c): the base type's typreceive converts the wire
// bytes, then the domain's constraints are checked — hard errors only (no
// soft-error lane on the binary side, matching C).
pub fn fc_domain_recv(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // domain_recv is non-strict; NULL-ness of arg 0 is the pointer (C's
    // ReceiveFunctionCall passes a NULL buf for a NULL wire value).
    let buf_is_null = fcinfo.args[0].isnull || fcinfo.arg(0).as_usize() == 0;
    if fcinfo.args[1].isnull {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }
    let domainType = fcinfo.arg(1).as_oid();

    let flinfo = flinfo.expect("domain_recv: NULL flinfo");
    let stale = match flinfo.fn_extra_ref::<DomainIOData>() {
        Some(d) => d.domain_type != domainType,
        None => true,
    };
    if stale {
        flinfo.set_fn_extra(domain_state_setup(domainType, true)?);
    }

    let mcx = fcinfo.result_mcx();
    let my = flinfo.fn_extra_mut::<DomainIOData>().expect("just installed");
    let buf = if buf_is_null {
        None
    } else {
        // SAFETY: non-null recv arg 0 is the live StringInfo pointer per the
        // recv ABI.
        Some(unsafe { fcinfo.arg_stringinfo(0) })
    };
    let value =
        types_fmgr::receive_function_call(&mut my.proc, buf, my.typioparam, my.typtypmod, mcx)?;

    typcache_seams::domain_check_input::call(value, buf_is_null, domainType, None)?;

    if buf_is_null {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }
    Ok(value)
}

// C's extra/mcxt per-callsite memo collapses into the engine's per-domain memo.
pub fn domain_check(value: Datum, isnull: bool, domainType: Oid) -> PgResult<()> {
    typcache_seams::domain_check_input::call(value, isnull, domainType, None)
}

pub fn domain_check_safe(
    value: Datum,
    isnull: bool,
    domainType: Oid,
    escontext: &mut types_error::SoftErrorContext,
) -> PgResult<bool> {
    typcache_seams::domain_check_input::call(value, isnull, domainType, Some(escontext))?;
    Ok(!escontext.error_occurred())
}

pub const DOMAINS_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 2597,
        name: "domain_in",
        nargs: 3,
        strict: false,
        retset: false,
        func: fc_domain_in,
    },
    FmgrBuiltin {
        foid: 2598,
        name: "domain_recv",
        nargs: 3,
        strict: false,
        retset: false,
        func: fc_domain_recv,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use types_error::SoftErrorContext;

    fn fake_check(
        value: Datum,
        isnull: bool,
        _domain_type: Oid,
        escontext: Option<&mut SoftErrorContext>,
    ) -> PgResult<()> {
        if isnull || value.as_i32() < 0 {
            let err = PgError::error("value for domain d violates check constraint")
                .with_sqlstate(types_error::ERRCODE_CHECK_VIOLATION);
            return types_error::ereturn(escontext, (), err);
        }
        Ok(())
    }

    #[test]
    fn check_and_check_safe() {
        typcache_seams::domain_check_input::set(fake_check);
        assert!(domain_check(Datum::from_i32(7), false, 1).is_ok());
        assert!(domain_check(Datum::from_i32(-1), false, 1).is_err());

        let mut esc = SoftErrorContext::new(true);
        assert!(domain_check_safe(Datum::from_i32(7), false, 1, &mut esc).unwrap());
        let mut esc = SoftErrorContext::new(true);
        assert!(!domain_check_safe(Datum::from_i32(-1), false, 1, &mut esc).unwrap());
        assert!(esc.error_occurred());
        let mut esc = SoftErrorContext::new(false);
        assert!(!domain_check_safe(Datum::null(), true, 1, &mut esc).unwrap());
    }
}
