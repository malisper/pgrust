//! Family `regproc` — `src/backend/utils/adt/regproc.c`.
//!
//! I/O for the `reg*` object-identifier alias types: regproc / regprocedure /
//! regoper / regoperator / regclass / regcollation / regtype / regconfig /
//! regdictionary / regrole / regnamespace, each with in/out/recv/send plus the
//! soft `to_reg*` variants; the `format_procedure*` / `format_operator*`
//! helpers; and the name-parsing utilities `stringToQualifiedNameList`,
//! `parseNameAndArgTypes`, `parseNumericOid`, `parseDashOrOid`.
//!
//! These resolve names against the system catalogs (namespace search, syscache
//! lookups) and allocate result text, so they take `Mcx`, surface `ereport`s
//! as `PgResult`, route soft errors through a [`SoftErrorContext`], and reach
//! the catalogs through seams in their real owners:
//!
//!   - search-path resolution + visibility + `makeRangeVarFromNameList` +
//!     `get_collation_oid` / `get_ts_dict_oid` / `get_ts_config_oid` /
//!     `get_namespace_oid` / `RangeVarGetRelid`: `backend-catalog-namespace`
//!     (ported — installed seams);
//!   - syscache row projections (`SearchSysCache1` GETSTRUCT reads):
//!     `backend-utils-cache-syscache` (ported);
//!   - `get_namespace_name` / `get_namespace_name_or_temp`:
//!     `backend-utils-cache-lsyscache` (genuinely unported owner — seam-and-panic);
//!   - `format_type_be` / `format_type_be_qualified`:
//!     `backend-utils-adt-format-type` (unported owner);
//!   - `quote_identifier` / `quote_qualified_identifier`:
//!     `backend-utils-adt-ruleutils` (unported owner);
//!   - `parseTypeString`: `backend-parser-parse-type` (unported owner);
//!   - `oidin`: `backend-utils-adt-oid` (unported owner);
//!   - `get_role_oid`: `backend-utils-adt-acl` (unported owner);
//!   - `GetUserNameFromId`: `backend-utils-init-miscinit` (unported owner);
//!   - `SplitIdentifierString`: `backend-utils-adt-varlena` (unported owner);
//!   - `GetDatabaseEncodingName`: `backend-utils-mb-mbutils` (unported owner).
//!
//! The `oid`-binary I/O `reg*recv` / `reg*send` are byte-for-byte `oidrecv` /
//! `oidsend` (per the C "share code" comments) and are not duplicated here;
//! they are the `oid` type's own functions, exposed by the `oid` adt unit.

// C-faithful function names (the SQL-callable `reg*in`/`stringToQualifiedNameList`
// / `parseNameAndArgTypes` names mirror the C identifiers for the audit).
#![allow(non_snake_case)]
// `regtypein` keeps `mcx` for signature symmetry with the other `reg*in`
// routines even though `parseTypeString` allocates in its own owner context.
#![allow(unused_variables)]

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::{vec_with_capacity_in, Mcx, PgString, PgVec};
use types_core::{InvalidOid, Oid, OidIsValid, FUNC_MAX_ARGS};
use types_error::{
    PgError, PgResult, SoftErrorContext, ERRCODE_AMBIGUOUS_FUNCTION,
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_NAME, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_TOO_MANY_ARGUMENTS, ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_UNDEFINED_PARAMETER, ERRCODE_UNDEFINED_SCHEMA, ERRCODE_UNDEFINED_TABLE,
};

use backend_catalog_namespace_seams as namespace;
use backend_utils_adt_format_type_seams as format_type;
use backend_utils_adt_oid_seams as oid;
use backend_utils_adt_ruleutils_seams as ruleutils;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;
use backend_parser_parse_type_seams as parse_type;
use port_pgstrcasecmp::pg_strcasecmp;

/// `scanner_isspace` (scansup.c): the lexer's {space} set — NOT Unicode
/// whitespace. A pure char classifier, reproduced locally (the scansup unit
/// is not its own crate); identical to the copy in `backend-utils-error`.
fn scanner_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

/// `FORMAT_PROC_INVALID_AS_NULL` (utils/regproc.h).
pub const FORMAT_PROC_INVALID_AS_NULL: u16 = 0x01;
/// `FORMAT_PROC_FORCE_QUALIFY` (utils/regproc.h).
pub const FORMAT_PROC_FORCE_QUALIFY: u16 = 0x02;
/// `FORMAT_OPERATOR_INVALID_AS_NULL` (utils/regproc.h).
pub const FORMAT_OPERATOR_INVALID_AS_NULL: u16 = 0x01;
/// `FORMAT_OPERATOR_FORCE_QUALIFY` (utils/regproc.h).
pub const FORMAT_OPERATOR_FORCE_QUALIFY: u16 = 0x02;

/// Borrow a `&[String]` qualified-name list as the `&[&str]` the namespace
/// seams take.
fn as_str_slice(names: &[String]) -> Vec<&str> {
    names.iter().map(String::as_str).collect()
}

/* ****************************************************************************
 *   USER I/O ROUTINES
 * ************************************************************************** */

/// `regprocin(pro_name_or_oid)` — converts "proname" to proc OID.
///
/// `Ok(None)` is the C `PG_RETURN_NULL` (only via a soft-error `escontext`).
pub fn regprocin(
    mcx: Mcx<'_>,
    pro_name_or_oid: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let mut escontext = escontext;

    /* Handle "-" or numeric OID */
    if let Some(result) = parseDashOrOid(pro_name_or_oid, escontext.as_deref_mut())? {
        return Ok(Some(result));
    }

    /* Else it's a name, possibly schema-qualified */

    // (Bootstrap-mode `regproc` values must be OIDs; bootstrap mode is not
    // modeled in this build — the bootstrap processor never reaches here.)

    /*
     * Normal case: parse the name into components and see if it matches any
     * pg_proc entries in the current search path.
     */
    let names = match stringToQualifiedNameList(mcx, pro_name_or_oid, escontext.as_deref_mut())? {
        Some(names) => names,
        None => return Ok(None),
    };

    let name_refs = as_str_slice(&names);
    let clist = namespace::funcname_get_candidates::call(
        mcx, &name_refs, -1, &[], false, false, false, true,
    )?;

    if clist.is_empty() {
        return ereturn_oid(
            escontext,
            err_undefined_function(alloc::format!(
                "function \"{pro_name_or_oid}\" does not exist"
            )),
        );
    } else if clist.len() > 1 {
        return ereturn_oid(
            escontext,
            err_ambiguous_function(alloc::format!(
                "more than one function named \"{pro_name_or_oid}\""
            )),
        );
    }

    Ok(Some(clist[0].oid))
}

/// `to_regproc(pro_name)` — converts "proname" to proc OID, NULL if not found.
pub fn to_regproc(mcx: Mcx<'_>, pro_name: &str) -> PgResult<Option<Oid>> {
    let mut escontext = SoftErrorContext::new(false);
    match regprocin(mcx, pro_name, Some(&mut escontext))? {
        Some(oid) if !escontext.error_occurred() => Ok(Some(oid)),
        _ => Ok(None),
    }
}

/// `regprocout(proid)` — converts proc OID to "pro_name".
pub fn regprocout<'mcx>(mcx: Mcx<'mcx>, proid: Oid) -> PgResult<PgString<'mcx>> {
    if proid == InvalidOid {
        return PgString::from_str_in("-", mcx);
    }

    match syscache::proc_row_by_oid::call(mcx, proid)? {
        Some(procform) => {
            let proname = procform.proname.as_str();
            // (Bootstrap mode not modeled — always take the namespace path.)

            /*
             * Would this proc be found (uniquely!) by regprocin? If not,
             * qualify it.
             */
            let single = [proname];
            let clist = namespace::funcname_get_candidates::call(
                mcx, &single, -1, &[], false, false, false, false,
            )?;
            let nspname: Option<PgString<'mcx>> =
                if clist.len() == 1 && clist[0].oid == proid {
                    None
                } else {
                    lsyscache::get_namespace_name::call(mcx, procform.pronamespace)?
                };

            ruleutils::quote_qualified_identifier::call(
                mcx,
                nspname.as_deref(),
                proname,
            )
        }
        None => {
            /* If OID doesn't match any pg_proc entry, return it numerically */
            PgString::from_str_in(&proid.to_string(), mcx)
        }
    }
}

/// `regprocedurein(pro_name_or_oid)` — converts "proname(args)" to proc OID.
pub fn regprocedurein(
    mcx: Mcx<'_>,
    pro_name_or_oid: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let mut escontext = escontext;

    /* Handle "-" or numeric OID */
    if let Some(result) = parseDashOrOid(pro_name_or_oid, escontext.as_deref_mut())? {
        return Ok(Some(result));
    }

    /*
     * Else it's a name and arguments.  Parse the name and arguments, look up
     * potential matches in the current namespace search list, and scan to see
     * which one exactly matches the given argument types.
     */
    let (names, argtypes) = match parseNameAndArgTypes(
        mcx,
        pro_name_or_oid,
        false,
        escontext.as_deref_mut(),
    )? {
        Some(parsed) => parsed,
        None => return Ok(None),
    };

    let name_refs = as_str_slice(&names);
    let nargs = argtypes.len() as i32;
    let clist = namespace::funcname_get_candidates::call(
        mcx, &name_refs, nargs, &[], false, false, false, true,
    )?;

    let mut found: Option<Oid> = None;
    for cand in clist.iter() {
        if cand.args.as_slice() == argtypes.as_slice() {
            found = Some(cand.oid);
            break;
        }
    }

    match found {
        Some(oid) => Ok(Some(oid)),
        None => ereturn_oid(
            escontext,
            err_undefined_function(alloc::format!(
                "function \"{pro_name_or_oid}\" does not exist"
            )),
        ),
    }
}

/// `to_regprocedure(pro_name)` — soft variant of [`regprocedurein`].
pub fn to_regprocedure(mcx: Mcx<'_>, pro_name: &str) -> PgResult<Option<Oid>> {
    let mut escontext = SoftErrorContext::new(false);
    match regprocedurein(mcx, pro_name, Some(&mut escontext))? {
        Some(oid) if !escontext.error_occurred() => Ok(Some(oid)),
        _ => Ok(None),
    }
}

/// `format_procedure(procedure_oid)` — converts proc OID to "pro_name(args)".
pub fn format_procedure<'mcx>(mcx: Mcx<'mcx>, procedure_oid: Oid) -> PgResult<PgString<'mcx>> {
    Ok(format_procedure_extended(mcx, procedure_oid, 0)?.expect("flags 0 never returns None"))
}

/// `format_procedure_qualified(procedure_oid)`.
pub fn format_procedure_qualified<'mcx>(
    mcx: Mcx<'mcx>,
    procedure_oid: Oid,
) -> PgResult<PgString<'mcx>> {
    Ok(format_procedure_extended(mcx, procedure_oid, FORMAT_PROC_FORCE_QUALIFY)?
        .expect("FORCE_QUALIFY alone never returns None"))
}

/// `format_procedure_extended(procedure_oid, flags)` — converts procedure OID
/// to "pro_name(args)". `None` is the C `NULL` (only with
/// `FORMAT_PROC_INVALID_AS_NULL`).
pub fn format_procedure_extended<'mcx>(
    mcx: Mcx<'mcx>,
    procedure_oid: Oid,
    flags: u16,
) -> PgResult<Option<PgString<'mcx>>> {
    match syscache::proc_row_by_oid::call(mcx, procedure_oid)? {
        Some(procform) => {
            let proname = procform.proname.as_str();
            let nargs = procform.pronargs;

            // (Bootstrap mode not modeled — Assert(!IsBootstrapProcessingMode()).)

            let mut buf = String::new();

            /*
             * Would this proc be found (given the right args) by
             * regprocedurein?  If not, or if caller requests it, we need to
             * qualify it.
             */
            let nspname: Option<PgString<'mcx>> =
                if (flags & FORMAT_PROC_FORCE_QUALIFY) == 0
                    && namespace::function_is_visible::call(mcx, procedure_oid)?
                {
                    None
                } else {
                    lsyscache::get_namespace_name::call(mcx, procform.pronamespace)?
                };

            let qualified = ruleutils::quote_qualified_identifier::call(
                mcx,
                nspname.as_deref(),
                proname,
            )?;
            buf.push_str(&qualified);
            buf.push('(');
            for i in 0..nargs as usize {
                let thisargtype = procform.proargtypes[i];

                if i > 0 {
                    buf.push(',');
                }
                let argname = if (flags & FORMAT_PROC_FORCE_QUALIFY) != 0 {
                    format_type::format_type_be_qualified::call(mcx, thisargtype)?
                } else {
                    format_type::format_type_be::call(mcx, thisargtype)?
                };
                buf.push_str(&argname);
            }
            buf.push(')');

            Ok(Some(PgString::from_str_in(&buf, mcx)?))
        }
        None if (flags & FORMAT_PROC_INVALID_AS_NULL) != 0 => Ok(None),
        None => Ok(Some(PgString::from_str_in(&procedure_oid.to_string(), mcx)?)),
    }
}

/// `format_procedure_parts(procedure_oid, &objnames, &objargs, missing_ok)` —
/// objname/objargs representation feeding `get_object_address`. `None` is the
/// `missing_ok` "didn't exist" return; otherwise `(objnames, objargs)`.
pub fn format_procedure_parts<'mcx>(
    mcx: Mcx<'mcx>,
    procedure_oid: Oid,
    missing_ok: bool,
) -> PgResult<Option<(PgVec<'mcx, PgString<'mcx>>, PgVec<'mcx, PgString<'mcx>>)>> {
    let procform = match syscache::proc_row_by_oid::call(mcx, procedure_oid)? {
        Some(p) => p,
        None => {
            if !missing_ok {
                return Err(err_internal(alloc::format!(
                    "cache lookup failed for procedure with OID {procedure_oid}"
                )));
            }
            return Ok(None);
        }
    };

    let nargs = procform.pronargs;

    let nspname = lsyscache::get_namespace_name_or_temp::call(mcx, procform.pronamespace)?
        .ok_or_else(|| {
            err_internal(alloc::format!(
                "cache lookup failed for namespace {}",
                procform.pronamespace
            ))
        })?;
    let mut objnames = vec_with_capacity_in(mcx, 2)?;
    objnames.push(nspname);
    objnames.push(PgString::from_str_in(procform.proname.as_str(), mcx)?);

    let mut objargs = vec_with_capacity_in(mcx, nargs as usize)?;
    for i in 0..nargs as usize {
        let thisargtype = procform.proargtypes[i];
        objargs.push(format_type::format_type_be_qualified::call(mcx, thisargtype)?);
    }

    Ok(Some((objnames, objargs)))
}

/// `regprocedureout(proid)` — converts proc OID to "pro_name(args)".
pub fn regprocedureout<'mcx>(mcx: Mcx<'mcx>, proid: Oid) -> PgResult<PgString<'mcx>> {
    if proid == InvalidOid {
        PgString::from_str_in("-", mcx)
    } else {
        format_procedure(mcx, proid)
    }
}

/* -------------------------------------------------------------------------
 * regoper / regoperator
 * ---------------------------------------------------------------------- */

/// `regoperin(opr_name_or_oid)` — converts "oprname" to operator OID.
pub fn regoperin(
    mcx: Mcx<'_>,
    opr_name_or_oid: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let mut escontext = escontext;

    /* Handle "0" or numeric OID */
    if let Some(result) = parseNumericOid(opr_name_or_oid, escontext.as_deref_mut())? {
        return Ok(Some(result));
    }

    /* Else it's a name, possibly schema-qualified */
    let names = match stringToQualifiedNameList(mcx, opr_name_or_oid, escontext.as_deref_mut())? {
        Some(names) => names,
        None => return Ok(None),
    };

    let name_refs = as_str_slice(&names);
    let clist = namespace::opername_get_candidates::call(mcx, &name_refs, b'\0', true)?;

    if clist.is_empty() {
        return ereturn_oid(
            escontext,
            err_undefined_function(alloc::format!(
                "operator does not exist: {opr_name_or_oid}"
            )),
        );
    } else if clist.len() > 1 {
        return ereturn_oid(
            escontext,
            err_ambiguous_function(alloc::format!(
                "more than one operator named {opr_name_or_oid}"
            )),
        );
    }

    Ok(Some(clist[0].oid))
}

/// `to_regoper(opr_name)` — soft variant of [`regoperin`].
pub fn to_regoper(mcx: Mcx<'_>, opr_name: &str) -> PgResult<Option<Oid>> {
    let mut escontext = SoftErrorContext::new(false);
    match regoperin(mcx, opr_name, Some(&mut escontext))? {
        Some(oid) if !escontext.error_occurred() => Ok(Some(oid)),
        _ => Ok(None),
    }
}

/// `regoperout(oprid)` — converts operator OID to "opr_name".
pub fn regoperout<'mcx>(mcx: Mcx<'mcx>, oprid: Oid) -> PgResult<PgString<'mcx>> {
    if oprid == InvalidOid {
        return PgString::from_str_in("0", mcx);
    }

    match syscache::oper_row_by_oid::call(mcx, oprid)? {
        Some(operform) => {
            let oprname = operform.oprname.as_str();
            // (Bootstrap mode not modeled.)

            /*
             * Would this oper be found (uniquely!) by regoperin? If not,
             * qualify it.
             */
            let single = [oprname];
            let clist = namespace::opername_get_candidates::call(mcx, &single, b'\0', false)?;
            if clist.len() == 1 && clist[0].oid == oprid {
                PgString::from_str_in(oprname, mcx)
            } else {
                let nspname = lsyscache::get_namespace_name::call(mcx, operform.oprnamespace)?
                    .ok_or_else(|| {
                        err_internal(alloc::format!(
                            "cache lookup failed for namespace {}",
                            operform.oprnamespace
                        ))
                    })?;
                let nspname = ruleutils::quote_identifier::call(mcx, &nspname)?;
                PgString::from_str_in(&alloc::format!("{nspname}.{oprname}"), mcx)
            }
        }
        None => PgString::from_str_in(&oprid.to_string(), mcx),
    }
}

/// `regoperatorin(opr_name_or_oid)` — converts "oprname(args)" to operator OID.
pub fn regoperatorin(
    mcx: Mcx<'_>,
    opr_name_or_oid: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let mut escontext = escontext;

    /* Handle "0" or numeric OID */
    if let Some(result) = parseNumericOid(opr_name_or_oid, escontext.as_deref_mut())? {
        return Ok(Some(result));
    }

    /*
     * Else it's a name and arguments.  Parse the name and arguments, look up
     * potential matches in the current namespace search list, and scan to see
     * which one exactly matches the given argument types.
     */
    let (names, argtypes) = match parseNameAndArgTypes(
        mcx,
        opr_name_or_oid,
        true,
        escontext.as_deref_mut(),
    )? {
        Some(parsed) => parsed,
        None => return Ok(None),
    };

    if argtypes.len() == 1 {
        return ereturn_oid(
            escontext,
            err_undefined_parameter("missing argument".to_string()).with_hint(
                "Use NONE to denote the missing argument of a unary operator.".to_string(),
            ),
        );
    }
    if argtypes.len() != 2 {
        return ereturn_oid(
            escontext,
            err_too_many_arguments("too many arguments".to_string())
                .with_hint("Provide two argument types for operator.".to_string()),
        );
    }

    let name_refs = as_str_slice(&names);
    let result = namespace::opername_get_oprid::call(mcx, &name_refs, argtypes[0], argtypes[1])?;

    if !OidIsValid(result) {
        return ereturn_oid(
            escontext,
            err_undefined_function(alloc::format!(
                "operator does not exist: {opr_name_or_oid}"
            )),
        );
    }

    Ok(Some(result))
}

/// `to_regoperator(opr_name_or_oid)` — soft variant of [`regoperatorin`].
pub fn to_regoperator(mcx: Mcx<'_>, opr_name_or_oid: &str) -> PgResult<Option<Oid>> {
    let mut escontext = SoftErrorContext::new(false);
    match regoperatorin(mcx, opr_name_or_oid, Some(&mut escontext))? {
        Some(oid) if !escontext.error_occurred() => Ok(Some(oid)),
        _ => Ok(None),
    }
}

/// `format_operator_extended(operator_oid, flags)` — converts operator OID to
/// "opr_name(args)". `None` is the C `NULL` (only with
/// `FORMAT_OPERATOR_INVALID_AS_NULL`).
pub fn format_operator_extended<'mcx>(
    mcx: Mcx<'mcx>,
    operator_oid: Oid,
    flags: u16,
) -> PgResult<Option<PgString<'mcx>>> {
    match syscache::oper_row_by_oid::call(mcx, operator_oid)? {
        Some(operform) => {
            let oprname = operform.oprname.as_str();
            // (Bootstrap mode not modeled.)

            let mut buf = String::new();

            /*
             * Would this oper be found (given the right args) by
             * regoperatorin?  If not, or if caller explicitly requests it, we
             * need to qualify it.
             */
            if (flags & FORMAT_OPERATOR_FORCE_QUALIFY) != 0
                || !namespace::operator_is_visible::call(mcx, operator_oid)?
            {
                let nspname = lsyscache::get_namespace_name::call(mcx, operform.oprnamespace)?
                    .ok_or_else(|| {
                        err_internal(alloc::format!(
                            "cache lookup failed for namespace {}",
                            operform.oprnamespace
                        ))
                    })?;
                let nspname = ruleutils::quote_identifier::call(mcx, &nspname)?;
                buf.push_str(&nspname);
                buf.push('.');
            }

            buf.push_str(oprname);
            buf.push('(');

            if OidIsValid(operform.oprleft) {
                let t = if (flags & FORMAT_OPERATOR_FORCE_QUALIFY) != 0 {
                    format_type::format_type_be_qualified::call(mcx, operform.oprleft)?
                } else {
                    format_type::format_type_be::call(mcx, operform.oprleft)?
                };
                buf.push_str(&t);
                buf.push(',');
            } else {
                buf.push_str("NONE,");
            }

            if OidIsValid(operform.oprright) {
                let t = if (flags & FORMAT_OPERATOR_FORCE_QUALIFY) != 0 {
                    format_type::format_type_be_qualified::call(mcx, operform.oprright)?
                } else {
                    format_type::format_type_be::call(mcx, operform.oprright)?
                };
                buf.push_str(&t);
                buf.push(')');
            } else {
                buf.push_str("NONE)");
            }

            Ok(Some(PgString::from_str_in(&buf, mcx)?))
        }
        None if (flags & FORMAT_OPERATOR_INVALID_AS_NULL) != 0 => Ok(None),
        None => Ok(Some(PgString::from_str_in(&operator_oid.to_string(), mcx)?)),
    }
}

/// `format_operator(operator_oid)`.
pub fn format_operator<'mcx>(mcx: Mcx<'mcx>, operator_oid: Oid) -> PgResult<PgString<'mcx>> {
    Ok(format_operator_extended(mcx, operator_oid, 0)?.expect("flags 0 never returns None"))
}

/// `format_operator_qualified(operator_oid)`.
pub fn format_operator_qualified<'mcx>(
    mcx: Mcx<'mcx>,
    operator_oid: Oid,
) -> PgResult<PgString<'mcx>> {
    Ok(format_operator_extended(mcx, operator_oid, FORMAT_OPERATOR_FORCE_QUALIFY)?
        .expect("FORCE_QUALIFY alone never returns None"))
}

/// `format_operator_parts(operator_oid, &objnames, &objargs, missing_ok)`.
pub fn format_operator_parts<'mcx>(
    mcx: Mcx<'mcx>,
    operator_oid: Oid,
    missing_ok: bool,
) -> PgResult<Option<(PgVec<'mcx, PgString<'mcx>>, PgVec<'mcx, PgString<'mcx>>)>> {
    let opr_form = match syscache::oper_row_by_oid::call(mcx, operator_oid)? {
        Some(o) => o,
        None => {
            if !missing_ok {
                return Err(err_internal(alloc::format!(
                    "cache lookup failed for operator with OID {operator_oid}"
                )));
            }
            return Ok(None);
        }
    };

    let nspname = lsyscache::get_namespace_name_or_temp::call(mcx, opr_form.oprnamespace)?
        .ok_or_else(|| {
            err_internal(alloc::format!(
                "cache lookup failed for namespace {}",
                opr_form.oprnamespace
            ))
        })?;
    let mut objnames = vec_with_capacity_in(mcx, 2)?;
    objnames.push(nspname);
    objnames.push(PgString::from_str_in(opr_form.oprname.as_str(), mcx)?);

    let mut objargs = vec_with_capacity_in(mcx, 2)?;
    if OidIsValid(opr_form.oprleft) {
        objargs.push(format_type::format_type_be_qualified::call(mcx, opr_form.oprleft)?);
    }
    if OidIsValid(opr_form.oprright) {
        objargs.push(format_type::format_type_be_qualified::call(mcx, opr_form.oprright)?);
    }

    Ok(Some((objnames, objargs)))
}

/// `regoperatorout(oprid)` — converts operator OID to "opr_name(args)".
pub fn regoperatorout<'mcx>(mcx: Mcx<'mcx>, oprid: Oid) -> PgResult<PgString<'mcx>> {
    if oprid == InvalidOid {
        PgString::from_str_in("0", mcx)
    } else {
        format_operator(mcx, oprid)
    }
}

/* -------------------------------------------------------------------------
 * regclass
 * ---------------------------------------------------------------------- */

/// `regclassin(class_name_or_oid)` — converts "classname" to class OID.
pub fn regclassin(
    mcx: Mcx<'_>,
    class_name_or_oid: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let mut escontext = escontext;

    /* Handle "-" or numeric OID */
    if let Some(result) = parseDashOrOid(class_name_or_oid, escontext.as_deref_mut())? {
        return Ok(Some(result));
    }

    /* Else it's a name, possibly schema-qualified */
    let names = match stringToQualifiedNameList(mcx, class_name_or_oid, escontext.as_deref_mut())? {
        Some(names) => names,
        None => return Ok(None),
    };

    /* We might not even have permissions on this relation; don't lock it. */
    let name_refs = as_str_slice(&names);
    let rv = namespace::make_range_var_from_name_list::call(&name_refs)?;
    let result = namespace::range_var_get_relid::call(
        mcx,
        &rv,
        types_storage::lock::NoLock,
        true,
    )?;

    if !OidIsValid(result) {
        return ereturn_oid(
            escontext,
            err_undefined_table(alloc::format!(
                "relation \"{}\" does not exist",
                name_list_to_string(&names)
            )),
        );
    }

    Ok(Some(result))
}

/// `to_regclass(class_name)` — soft variant of [`regclassin`].
pub fn to_regclass(mcx: Mcx<'_>, class_name: &str) -> PgResult<Option<Oid>> {
    let mut escontext = SoftErrorContext::new(false);
    match regclassin(mcx, class_name, Some(&mut escontext))? {
        Some(oid) if !escontext.error_occurred() => Ok(Some(oid)),
        _ => Ok(None),
    }
}

/// `regclassout(classid)` — converts class OID to "class_name".
pub fn regclassout<'mcx>(mcx: Mcx<'mcx>, classid: Oid) -> PgResult<PgString<'mcx>> {
    if classid == InvalidOid {
        return PgString::from_str_in("-", mcx);
    }

    match syscache::relation_namespace_and_name::call(mcx, classid)? {
        Some(classform) => {
            let classname = classform.name.as_str();
            // (Bootstrap mode not modeled.)

            /* Would this class be found by regclassin? If not, qualify it. */
            let nspname: Option<PgString<'mcx>> = if namespace::relation_is_visible::call(mcx, classid)? {
                None
            } else {
                lsyscache::get_namespace_name::call(mcx, classform.namespace)?
            };

            ruleutils::quote_qualified_identifier::call(mcx, nspname.as_deref(), classname)
        }
        None => PgString::from_str_in(&classid.to_string(), mcx),
    }
}

/* -------------------------------------------------------------------------
 * regcollation
 * ---------------------------------------------------------------------- */

/// `regcollationin(collation_name_or_oid)`.
pub fn regcollationin(
    mcx: Mcx<'_>,
    collation_name_or_oid: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let mut escontext = escontext;

    if let Some(result) = parseDashOrOid(collation_name_or_oid, escontext.as_deref_mut())? {
        return Ok(Some(result));
    }

    let names =
        match stringToQualifiedNameList(mcx, collation_name_or_oid, escontext.as_deref_mut())? {
            Some(names) => names,
            None => return Ok(None),
        };

    let name_refs = as_str_slice(&names);
    let result = namespace::get_collation_oid::call(mcx, &name_refs, true)?;

    if !OidIsValid(result) {
        let enc = backend_utils_mb_mbutils_seams::get_database_encoding_name::call();
        return ereturn_oid(
            escontext,
            err_undefined_object(alloc::format!(
                "collation \"{}\" for encoding \"{}\" does not exist",
                name_list_to_string(&names),
                enc
            )),
        );
    }

    Ok(Some(result))
}

/// `to_regcollation(collation_name)` — soft variant of [`regcollationin`].
pub fn to_regcollation(mcx: Mcx<'_>, collation_name: &str) -> PgResult<Option<Oid>> {
    let mut escontext = SoftErrorContext::new(false);
    match regcollationin(mcx, collation_name, Some(&mut escontext))? {
        Some(oid) if !escontext.error_occurred() => Ok(Some(oid)),
        _ => Ok(None),
    }
}

/// `regcollationout(collationid)`.
pub fn regcollationout<'mcx>(mcx: Mcx<'mcx>, collationid: Oid) -> PgResult<PgString<'mcx>> {
    if collationid == InvalidOid {
        return PgString::from_str_in("-", mcx);
    }

    match syscache::collation_namespace_and_name::call(mcx, collationid)? {
        Some(collationform) => {
            let collationname = collationform.name.as_str();
            // (Bootstrap mode not modeled.)

            let nspname: Option<PgString<'mcx>> =
                if namespace::collation_is_visible::call(mcx, collationid)? {
                    None
                } else {
                    lsyscache::get_namespace_name::call(mcx, collationform.namespace)?
                };

            ruleutils::quote_qualified_identifier::call(mcx, nspname.as_deref(), collationname)
        }
        None => PgString::from_str_in(&collationid.to_string(), mcx),
    }
}

/* -------------------------------------------------------------------------
 * regtype
 * ---------------------------------------------------------------------- */

/// `regtypein(typ_name_or_oid)`.
pub fn regtypein(
    mcx: Mcx<'_>,
    typ_name_or_oid: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let mut escontext = escontext;

    if let Some(result) = parseDashOrOid(typ_name_or_oid, escontext.as_deref_mut())? {
        return Ok(Some(result));
    }

    /*
     * Normal case: invoke the full parser to deal with special cases such as
     * array syntax.  We don't need to check for parseTypeString failure,
     * since we'll just return anyway.
     */
    let soft = escontext.is_some();
    match parse_type::parse_type_string::call(typ_name_or_oid, soft)? {
        Some((result, _typmod)) => Ok(Some(result)),
        None => {
            // Soft error: parseTypeString reported into escontext.
            if let Some(ctx) = escontext {
                ctx.mark_error_occurred();
            }
            // C still does PG_RETURN_OID(result) with result possibly
            // InvalidOid; the SQL function returns NULL only because the
            // soft-error machinery short-circuits. Surface NULL.
            Ok(None)
        }
    }
}

/// `to_regtype(typ_name)` — soft variant of [`regtypein`].
pub fn to_regtype(mcx: Mcx<'_>, typ_name: &str) -> PgResult<Option<Oid>> {
    let mut escontext = SoftErrorContext::new(false);
    match regtypein(mcx, typ_name, Some(&mut escontext))? {
        Some(oid) if !escontext.error_occurred() => Ok(Some(oid)),
        _ => Ok(None),
    }
}

/// `to_regtypemod(typ_name)` — converts "typename" to its type modifier, NULL
/// if not found.
pub fn to_regtypemod(_mcx: Mcx<'_>, typ_name: &str) -> PgResult<Option<i32>> {
    /* We rely on parseTypeString to parse the input. */
    match parse_type::parse_type_string::call(typ_name, true)? {
        Some((_typid, typmod)) => Ok(Some(typmod)),
        None => Ok(None),
    }
}

/// `regtypeout(typid)`.
pub fn regtypeout<'mcx>(mcx: Mcx<'mcx>, typid: Oid) -> PgResult<PgString<'mcx>> {
    if typid == InvalidOid {
        return PgString::from_str_in("-", mcx);
    }

    match syscache::type_namespace_and_name::call(mcx, typid)? {
        Some(_typeform) => {
            // (Bootstrap mode not modeled — always format_type_be.)
            format_type::format_type_be::call(mcx, typid)
        }
        None => PgString::from_str_in(&typid.to_string(), mcx),
    }
}

/* -------------------------------------------------------------------------
 * regconfig
 * ---------------------------------------------------------------------- */

/// `regconfigin(cfg_name_or_oid)`.
pub fn regconfigin(
    mcx: Mcx<'_>,
    cfg_name_or_oid: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let mut escontext = escontext;

    if let Some(result) = parseDashOrOid(cfg_name_or_oid, escontext.as_deref_mut())? {
        return Ok(Some(result));
    }

    let names = match stringToQualifiedNameList(mcx, cfg_name_or_oid, escontext.as_deref_mut())? {
        Some(names) => names,
        None => return Ok(None),
    };

    let name_refs = as_str_slice(&names);
    let result = namespace::get_ts_config_oid::call(&name_refs, true)?;

    if !OidIsValid(result) {
        return ereturn_oid(
            escontext,
            err_undefined_object(alloc::format!(
                "text search configuration \"{}\" does not exist",
                name_list_to_string(&names)
            )),
        );
    }

    Ok(Some(result))
}

/// `regconfigout(cfgid)`.
pub fn regconfigout<'mcx>(mcx: Mcx<'mcx>, cfgid: Oid) -> PgResult<PgString<'mcx>> {
    if cfgid == InvalidOid {
        return PgString::from_str_in("-", mcx);
    }

    match syscache::ts_config_namespace_and_name::call(mcx, cfgid)? {
        Some(cfgform) => {
            let cfgname = cfgform.name.as_str();

            /* Would this config be found by regconfigin? If not, qualify it. */
            let nspname: Option<PgString<'mcx>> =
                if namespace::ts_config_is_visible::call(mcx, cfgid)? {
                    None
                } else {
                    lsyscache::get_namespace_name::call(mcx, cfgform.namespace)?
                };

            ruleutils::quote_qualified_identifier::call(mcx, nspname.as_deref(), cfgname)
        }
        None => PgString::from_str_in(&cfgid.to_string(), mcx),
    }
}

/* -------------------------------------------------------------------------
 * regdictionary
 * ---------------------------------------------------------------------- */

/// `regdictionaryin(dict_name_or_oid)`.
pub fn regdictionaryin(
    mcx: Mcx<'_>,
    dict_name_or_oid: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let mut escontext = escontext;

    if let Some(result) = parseDashOrOid(dict_name_or_oid, escontext.as_deref_mut())? {
        return Ok(Some(result));
    }

    let names = match stringToQualifiedNameList(mcx, dict_name_or_oid, escontext.as_deref_mut())? {
        Some(names) => names,
        None => return Ok(None),
    };

    let name_refs = as_str_slice(&names);
    let result = namespace::get_ts_dict_oid::call(mcx, &name_refs, true)?;

    if !OidIsValid(result) {
        return ereturn_oid(
            escontext,
            err_undefined_object(alloc::format!(
                "text search dictionary \"{}\" does not exist",
                name_list_to_string(&names)
            )),
        );
    }

    Ok(Some(result))
}

/// `regdictionaryout(dictid)`.
pub fn regdictionaryout<'mcx>(mcx: Mcx<'mcx>, dictid: Oid) -> PgResult<PgString<'mcx>> {
    if dictid == InvalidOid {
        return PgString::from_str_in("-", mcx);
    }

    match syscache::ts_dict_namespace_and_name::call(mcx, dictid)? {
        Some(dictform) => {
            let dictname = dictform.name.as_str();

            /* Would this dictionary be found by regdictionaryin? If not, qualify it. */
            let nspname: Option<PgString<'mcx>> =
                if namespace::ts_dictionary_is_visible::call(mcx, dictid)? {
                    None
                } else {
                    lsyscache::get_namespace_name::call(mcx, dictform.namespace)?
                };

            ruleutils::quote_qualified_identifier::call(mcx, nspname.as_deref(), dictname)
        }
        None => PgString::from_str_in(&dictid.to_string(), mcx),
    }
}

/* -------------------------------------------------------------------------
 * regrole
 * ---------------------------------------------------------------------- */

/// `regrolein(role_name_or_oid)`.
pub fn regrolein(
    mcx: Mcx<'_>,
    role_name_or_oid: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let mut escontext = escontext;

    if let Some(result) = parseDashOrOid(role_name_or_oid, escontext.as_deref_mut())? {
        return Ok(Some(result));
    }

    /* Normal case: see if the name matches any pg_authid entry. */
    let names = match stringToQualifiedNameList(mcx, role_name_or_oid, escontext.as_deref_mut())? {
        Some(names) => names,
        None => return Ok(None),
    };

    if names.len() != 1 {
        return ereturn_oid(
            escontext,
            err_invalid_name("invalid name syntax".to_string()),
        );
    }

    let result = backend_utils_adt_acl_seams::get_role_oid::call(&names[0], true)?;

    if !OidIsValid(result) {
        return ereturn_oid(
            escontext,
            err_undefined_object(alloc::format!("role \"{}\" does not exist", names[0])),
        );
    }

    Ok(Some(result))
}

/// `to_regrole(role_name)` — soft variant of [`regrolein`].
pub fn to_regrole(mcx: Mcx<'_>, role_name: &str) -> PgResult<Option<Oid>> {
    let mut escontext = SoftErrorContext::new(false);
    match regrolein(mcx, role_name, Some(&mut escontext))? {
        Some(oid) if !escontext.error_occurred() => Ok(Some(oid)),
        _ => Ok(None),
    }
}

/// `regroleout(roleoid)`.
pub fn regroleout<'mcx>(mcx: Mcx<'mcx>, roleoid: Oid) -> PgResult<PgString<'mcx>> {
    if roleoid == InvalidOid {
        return PgString::from_str_in("-", mcx);
    }

    match backend_utils_init_miscinit_seams::get_user_name_from_id::call(mcx, roleoid, true)? {
        Some(name) => {
            /* pstrdup is not really necessary, but it avoids a compiler warning */
            ruleutils::quote_identifier::call(mcx, &name)
        }
        None => {
            /* If OID doesn't match any role, return it numerically */
            PgString::from_str_in(&roleoid.to_string(), mcx)
        }
    }
}

/* -------------------------------------------------------------------------
 * regnamespace
 * ---------------------------------------------------------------------- */

/// `regnamespacein(nsp_name_or_oid)`.
pub fn regnamespacein(
    mcx: Mcx<'_>,
    nsp_name_or_oid: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let mut escontext = escontext;

    if let Some(result) = parseDashOrOid(nsp_name_or_oid, escontext.as_deref_mut())? {
        return Ok(Some(result));
    }

    /* Normal case: see if the name matches any pg_namespace entry. */
    let names = match stringToQualifiedNameList(mcx, nsp_name_or_oid, escontext.as_deref_mut())? {
        Some(names) => names,
        None => return Ok(None),
    };

    if names.len() != 1 {
        return ereturn_oid(
            escontext,
            err_invalid_name("invalid name syntax".to_string()),
        );
    }

    let result = namespace::get_namespace_oid::call(&names[0], true)?;

    if !OidIsValid(result) {
        return ereturn_oid(
            escontext,
            err_undefined_schema(alloc::format!("schema \"{}\" does not exist", names[0])),
        );
    }

    Ok(Some(result))
}

/// `to_regnamespace(nsp_name)` — soft variant of [`regnamespacein`].
pub fn to_regnamespace(mcx: Mcx<'_>, nsp_name: &str) -> PgResult<Option<Oid>> {
    let mut escontext = SoftErrorContext::new(false);
    match regnamespacein(mcx, nsp_name, Some(&mut escontext))? {
        Some(oid) if !escontext.error_occurred() => Ok(Some(oid)),
        _ => Ok(None),
    }
}

/// `regnamespaceout(nspid)`.
pub fn regnamespaceout<'mcx>(mcx: Mcx<'mcx>, nspid: Oid) -> PgResult<PgString<'mcx>> {
    if nspid == InvalidOid {
        return PgString::from_str_in("-", mcx);
    }

    match lsyscache::get_namespace_name::call(mcx, nspid)? {
        Some(name) => {
            /* pstrdup is not really necessary, but it avoids a compiler warning */
            ruleutils::quote_identifier::call(mcx, &name)
        }
        None => {
            /* If OID doesn't match any namespace, return it numerically */
            PgString::from_str_in(&nspid.to_string(), mcx)
        }
    }
}

/// `text_regclass(relname)` — convert text to regclass (an implicit cast that
/// supports legacy forms of `nextval()`). Locking is suppressed.
pub fn text_regclass(mcx: Mcx<'_>, relname: &str) -> PgResult<Oid> {
    let names = textToQualifiedNameList(mcx, relname)?;
    let name_refs = as_str_slice(&names);
    let rv = namespace::make_range_var_from_name_list::call(&name_refs)?;

    /* We might not even have permissions on this relation; don't lock it. */
    namespace::range_var_get_relid::call(mcx, &rv, types_storage::lock::NoLock, false)
}

/* ****************************************************************************
 *   SUPPORT ROUTINES
 * ************************************************************************** */

/// `stringToQualifiedNameList(string, escontext)` — split a possibly-quoted
/// dotted name into its component identifiers. `Ok(None)` is the C `NIL`
/// (invalid input reported into a soft-error context).
pub fn stringToQualifiedNameList(
    mcx: Mcx<'_>,
    string: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Vec<String>>> {
    /* C pstrdup's a modifiable copy; SplitIdentifierString does that itself. */
    let namelist = backend_utils_adt_varlena_seams::split_identifier_string::call(mcx, string, '.')?;

    let namelist = match namelist {
        Some(list) => list,
        None => {
            return ereturn_namelist(
                escontext,
                err_invalid_name("invalid name syntax".to_string()),
            )
        }
    };

    if namelist.is_empty() {
        return ereturn_namelist(
            escontext,
            err_invalid_name("invalid name syntax".to_string()),
        );
    }

    let mut result = Vec::with_capacity(namelist.len());
    for curname in namelist.iter() {
        result.push(curname.as_str().to_string());
    }

    Ok(Some(result))
}

/// `textToQualifiedNameList(textval)` (varlena.c semantics) — like
/// [`stringToQualifiedNameList`] but never soft-errors (the SQL `text`
/// entry points always throw); an empty/invalid name raises directly.
fn textToQualifiedNameList(mcx: Mcx<'_>, string: &str) -> PgResult<Vec<String>> {
    stringToQualifiedNameList(mcx, string, None)?
        .ok_or_else(|| err_invalid_name("invalid name syntax".to_string()))
}

/// `parseNumericOid(string, &result, escontext)` — if `string` is all-digits
/// (and not empty), convert directly to OID and return `Some`. Otherwise
/// `None`.
fn parseNumericOid(
    string: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    let bytes = string.as_bytes();
    if !bytes.is_empty() && bytes.iter().all(|b| b.is_ascii_digit()) {
        let soft = escontext.is_some();
        /* We need not care here whether oidin() fails or not. */
        let parsed = oid::oidin::call(string, soft)?;
        if let (Some(ctx), None) = (escontext, parsed) {
            ctx.mark_error_occurred();
        }
        // DatumGetObjectId of the (possibly InvalidOid) result.
        return Ok(Some(parsed.unwrap_or(InvalidOid)));
    }

    Ok(None)
}

/// `parseDashOrOid(string, &result, escontext)` — as [`parseNumericOid`], but
/// also accept "-" as meaning 0 (InvalidOid).
fn parseDashOrOid(
    string: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    /* '-' ? */
    if string == "-" {
        return Ok(Some(InvalidOid));
    }

    /* Numeric OID? */
    parseNumericOid(string, escontext)
}

/// `parseNameAndArgTypes(string, allowNone, &names, &nargs, argtypes,
/// escontext)` — parse "name(type, ...)" into a qualified name list and an
/// array of argument type OIDs. `Ok(None)` is the C `false` return (soft
/// error). `argtypes` is bounded by `FUNC_MAX_ARGS`.
pub fn parseNameAndArgTypes(
    mcx: Mcx<'_>,
    string: &str,
    allow_none: bool,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<(Vec<String>, Vec<Oid>)>> {
    let mut escontext = escontext;

    /* We need a modifiable copy of the input string. */
    let raw: Vec<u8> = string.as_bytes().to_vec();

    /* Scan to find the expected left paren; mustn't be quoted */
    let mut in_quote = false;
    let mut lparen: Option<usize> = None;
    for (i, &c) in raw.iter().enumerate() {
        if c == b'"' {
            in_quote = !in_quote;
        } else if c == b'(' && !in_quote {
            lparen = Some(i);
            break;
        }
    }
    let lparen = match lparen {
        Some(i) => i,
        None => {
            return ereturn_parsed(
                escontext,
                err_invalid_text_representation("expected a left parenthesis".to_string()),
            )
        }
    };

    /* Separate the name and parse it into a list */
    let name_part = core::str::from_utf8(&raw[..lparen]).expect("input was valid UTF-8");
    let names = match stringToQualifiedNameList(mcx, name_part, escontext.as_deref_mut())? {
        Some(names) => names,
        None => return Ok(None),
    };

    /* The remainder after '(' */
    let mut rest = &raw[lparen + 1..];

    /* Check for the trailing right parenthesis and remove it */
    // ptr2 scans back from end over trailing whitespace; the last non-space
    // must be ')'.
    let mut end = rest.len();
    while end > 0 {
        let c = rest[end - 1];
        if !scanner_isspace(c) {
            break;
        }
        end -= 1;
    }
    if end == 0 || rest[end - 1] != b')' {
        return ereturn_parsed(
            escontext,
            err_invalid_text_representation("expected a right parenthesis".to_string()),
        );
    }
    /* Drop the ')' (and anything after, which was only trailing whitespace). */
    rest = &rest[..end - 1];

    /* Separate the remaining string into comma-separated type names */
    let mut argtypes: Vec<Oid> = Vec::new();
    let mut had_comma = false;
    let mut pos = 0usize;

    loop {
        /* allow leading whitespace */
        while pos < rest.len() && scanner_isspace(rest[pos]) {
            pos += 1;
        }
        if pos >= rest.len() {
            /* End of string.  Okay unless we had a comma before. */
            if had_comma {
                return ereturn_parsed(
                    escontext,
                    err_invalid_text_representation("expected a type name".to_string()),
                );
            }
            break;
        }

        let typename_start = pos;
        /* Find end of type name --- end of string or comma, but not a quoted
         * or parenthesized comma. */
        in_quote = false;
        let mut paren_count: i32 = 0;
        while pos < rest.len() {
            let c = rest[pos];
            if c == b'"' {
                in_quote = !in_quote;
            } else if c == b',' && !in_quote && paren_count == 0 {
                break;
            } else if !in_quote {
                match c {
                    b'(' | b'[' => paren_count += 1,
                    b')' | b']' => paren_count -= 1,
                    _ => {}
                }
            }
            pos += 1;
        }
        if in_quote || paren_count != 0 {
            return ereturn_parsed(
                escontext,
                err_invalid_text_representation("improper type name".to_string()),
            );
        }

        let mut typename_end = pos;
        if pos < rest.len() && rest[pos] == b',' {
            had_comma = true;
            pos += 1;
        } else {
            had_comma = false;
            debug_assert!(pos >= rest.len());
        }
        /* Lop off trailing whitespace */
        while typename_end > typename_start && scanner_isspace(rest[typename_end - 1]) {
            typename_end -= 1;
        }

        let typename =
            core::str::from_utf8(&rest[typename_start..typename_end]).expect("UTF-8 input");

        let typeid: Oid;
        if allow_none && pg_strcasecmp(typename.as_bytes(), b"none") == 0 {
            /* Special case for NONE */
            typeid = InvalidOid;
        } else {
            /* Use full parser to resolve the type name */
            let soft = escontext.is_some();
            match parse_type::parse_type_string::call(typename, soft)? {
                Some((tid, _typmod)) => typeid = tid,
                None => {
                    if let Some(ctx) = escontext.as_deref_mut() {
                        ctx.mark_error_occurred();
                    }
                    return Ok(None);
                }
            }
        }

        if argtypes.len() >= FUNC_MAX_ARGS {
            return ereturn_parsed(
                escontext,
                err_too_many_arguments("too many arguments".to_string()),
            );
        }

        argtypes.push(typeid);
    }

    Ok(Some((names, argtypes)))
}

/* -------------------------------------------------------------------------
 * Small local helpers
 * ---------------------------------------------------------------------- */

/// `NameListToString` for error-message text: dot-joined, unquoted (the C
/// `NameListToString` used in the error messages here).
fn name_list_to_string(names: &[String]) -> String {
    names.join(".")
}

/* Error constructors mirroring the C `errcode(...) + errmsg(...)` sites. */
fn err_undefined_function(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_UNDEFINED_FUNCTION)
}
fn err_ambiguous_function(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_AMBIGUOUS_FUNCTION)
}
fn err_undefined_table(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_UNDEFINED_TABLE)
}
fn err_undefined_object(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
}
fn err_undefined_schema(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_UNDEFINED_SCHEMA)
}
fn err_invalid_name(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INVALID_NAME)
}
fn err_too_many_arguments(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_TOO_MANY_ARGUMENTS)
}
fn err_undefined_parameter(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_UNDEFINED_PARAMETER)
}
fn err_invalid_text_representation(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}
fn err_internal(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `ereturn(escontext, (Datum) 0, ...)` for an `Oid`-valued I/O routine: a
/// soft-error context absorbs the error and yields `Ok(None)` (C `(Datum) 0`,
/// surfaced as SQL NULL); without one it is a hard error.
fn ereturn_oid(
    escontext: Option<&mut SoftErrorContext>,
    error: PgError,
) -> PgResult<Option<Oid>> {
    types_error::ereturn(escontext, None, error)
}

/// `ereturn(escontext, NIL, ...)` for [`stringToQualifiedNameList`].
fn ereturn_namelist(
    escontext: Option<&mut SoftErrorContext>,
    error: PgError,
) -> PgResult<Option<Vec<String>>> {
    types_error::ereturn(escontext, None, error)
}

/// `ereturn(escontext, false, ...)` for [`parseNameAndArgTypes`].
fn ereturn_parsed(
    escontext: Option<&mut SoftErrorContext>,
    error: PgError,
) -> PgResult<Option<(Vec<String>, Vec<Oid>)>> {
    types_error::ereturn(escontext, None, error)
}
