//! domains.c I/O slice; constraint checks route through
//! typcache_seams::domain_check_input (compiled-check engine lives with
//! execexpr — this crate sits under fmgr_core).

#![allow(non_snake_case)]

use datum::Datum;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_UNDEFINED_OBJECT};
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
    // domains.c:91 validates domainType through lookup_type_cache(), whose
    // miss is the user-facing typcache.c:471-473 ereport (domain_in /
    // domain_recv are callable from SQL with an arbitrary OID).
    let Some(typ) = syscache_seams::lookup_pg_type_typcache_shape::call(domainType)? else {
        return Err(Box::new(
            PgError::error(format!("type with OID {domainType} does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ));
    };
    if !typ.typisdefined {
        let name = String::from_utf8_lossy(typ.typname.name_str()).into_owned();
        return Err(Box::new(
            PgError::error(format!("type \"{name}\" is only a shell"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    if typ.typtype != TYPTYPE_DOMAIN {
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
    if typcache_seams::domain_prepare_constraints::is_installed() {
        typcache_seams::domain_prepare_constraints::call(domainType)?;
    }
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

// domain_check_internal's domain_state_setup(binary = true): the base type
// must have a receive function even though no bytes are received.
// Unit harnesses stub only the check seam; the catalog lookups are gated on
// their seams being installed (the acl/amutils precedent).
pub fn domain_check_setup(domainType: Oid) -> PgResult<()> {
    if syscache_seams::pg_type_base_shape::is_installed()
        && syscache_seams::pg_type_io_shape::is_installed()
    {
        lsyscache::getTypeBinaryInputInfo(lsyscache::getBaseType(domainType)?)?;
    }
    Ok(())
}

// C's extra/mcxt per-callsite memo collapses into the engine's per-domain memo.
pub fn domain_check(value: Datum, isnull: bool, domainType: Oid) -> PgResult<()> {
    domain_check_setup(domainType)?;
    typcache_seams::domain_check_input::call(value, isnull, domainType, None)
}

pub fn domain_check_safe(
    value: Datum,
    isnull: bool,
    domainType: Oid,
    escontext: &mut types_error::SoftErrorContext,
) -> PgResult<bool> {
    domain_check_setup(domainType)?;
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

    // Domain 1 sits over a type with a receive function, domain 2 over one
    // without (aclitem's shape): domain_check_internal's binary setup.
    fn install_type_stubs() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            typcache_seams::domain_check_input::set(fake_check);
            syscache_seams::pg_type_base_shape::set(|typid| {
                Ok(match typid {
                    1 | 2 => Some(syscache_seams::PgTypeBaseShape {
                        typtype: TYPTYPE_DOMAIN,
                        typbasetype: typid + 100,
                        typtypmod: -1,
                        typelem: 0,
                        typsubscript: 0,
                    }),
                    101 | 102 => Some(syscache_seams::PgTypeBaseShape {
                        typtype: b'b' as i8,
                        typbasetype: 0,
                        typtypmod: -1,
                        typelem: 0,
                        typsubscript: 0,
                    }),
                    _ => None,
                })
            });
            syscache_seams::pg_type_io_shape::set(|typid| {
                Ok(match typid {
                    101 | 102 => Some(syscache_seams::PgTypeIoShape {
                        oid: typid,
                        typinput: 42,
                        typoutput: 43,
                        typreceive: if typid == 101 { 44 } else { 0 },
                        typsend: 45,
                        typmodin: 0,
                        typmodout: 0,
                        typelem: 0,
                        typlen: 4,
                        typbyval: true,
                        typalign: b'i' as i8,
                        typdelim: b',' as i8,
                        typisdefined: true,
                    }),
                    _ => None,
                })
            });
        });
    }

    // audit-18.6 fp-adt-domains#2: domains.c:389 domain_check_internal sets
    // up with binary = true, so a domain over a type without a receive
    // function (aclitem) is "no binary input function available for type
    // ..." (42883) before any constraint runs.
    #[test]
    fn check_requires_base_type_receive_function() {
        install_type_stubs();
        let err = domain_check(Datum::from_i32(7), false, 2).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_FUNCTION);
        assert!(err.message().starts_with("no binary input function available for type"), "{}", err.message());
        let mut esc = SoftErrorContext::new(true);
        let err = domain_check_safe(Datum::null(), true, 2, &mut esc).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_FUNCTION);
    }

    #[test]
    fn check_and_check_safe() {
        install_type_stubs();
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

    // audit-18.6 a186-candidate-fp-adt-domains-4ac55b6371915f873e14-1:
    // domains.c:91 lookup_type_cache() on a bogus OID (domain_in is callable
    // from SQL with any OID) is the user-facing typcache.c:471-473 error
    // "type with OID %u does not exist", ERRCODE_UNDEFINED_OBJECT (42704).
    #[test]
    fn bogus_domain_oid_is_undefined_object() {
        syscache_seams::lookup_pg_type_typcache_shape::set(|_| Ok(None));
        let expect_err = |r: PgResult<DomainIOData>| match r {
            Err(e) => e,
            Ok(_) => panic!("bogus domain OID must fail"),
        };
        let err = expect_err(domain_state_setup(999999, false));
        assert_eq!(err.message(), "type with OID 999999 does not exist");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_OBJECT);
        let err = expect_err(domain_state_setup(0, true));
        assert_eq!(err.message(), "type with OID 0 does not exist");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_OBJECT);
    }
}
