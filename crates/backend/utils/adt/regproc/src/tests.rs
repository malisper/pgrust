use std::cell::RefCell;
use std::collections::HashMap;

use mcx::{Mcx, MemoryContext};
use types_core::{InvalidOid, Oid};
use types_error::SoftErrorContext;
use types_tuple::NameData;

use super::*;

const NS_CATALOG: Oid = 11;
const NS_PUBLIC: Oid = 2200;
const NS_S1: Oid = 5001;
const REL_PG_CLASS: Oid = 1259;
const REL_T1: Oid = 20001;
const REL_MIXED: Oid = 20002;
const REL_S1_T1: Oid = 20101;
const REL_DUP_CAT: Oid = 30001;
const REL_DUP_PUB: Oid = 30002;
const PROC_NOW: Oid = 1299;
const PROC_LOWER_TEXT: Oid = 870;
const PROC_LOWER_ANY: Oid = 871;
const PROC_SF_CAT: Oid = 40001;
const PROC_SF_PUB: Oid = 40002;
const ROLE_M: Oid = 10;

thread_local! {
    static NS_BY_NAME: RefCell<HashMap<String, Oid>> = RefCell::new(HashMap::new());
    static RELS: RefCell<HashMap<(String, Oid), Oid>> = RefCell::new(HashMap::new());
    static PROCS: RefCell<Vec<(String, Oid, Oid, Vec<Oid>)>> = const { RefCell::new(Vec::new()) };
    static PATH: RefCell<Vec<Oid>> = const { RefCell::new(Vec::new()) };
}

fn ns_name(oid: Oid) -> Option<String> {
    NS_BY_NAME.with(|m| m.borrow().iter().find(|(_, &v)| v == oid).map(|(k, _)| k.clone()))
}

fn install_fakes() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        miscinit_seams::get_user_id::set(|| ROLE_M);
        namespace_seams::type_is_visible::set(|_| Ok(true));
        namespace_seams::is_temp_namespace::set(|_| false);
        syscache_seams::pg_type_typnamespace::set(|_| Ok(Some(11)));
        aclchk_seams::object_aclcheck::set(|_, _, _, _| Ok(0));
        aclchk_seams::aclcheck_error::set(|_, _, name| {
            Err(Box::new(PgError::error(format!("permission denied for schema {name}"))))
        });
        syscache_seams::lookup_pg_namespace_oid_by_name::set(|nspname| {
            Ok(NS_BY_NAME.with(|m| m.borrow().get(nspname).copied()).unwrap_or(InvalidOid))
        });
        syscache_seams::pg_namespace_nspname::set(|nspid| {
            Ok(ns_name(nspid).map(|n| {
                let mut nd = NameData::default();
                nd.namestrcpy(&n);
                nd
            }))
        });
        syscache_seams::pg_class_relname::set(|relid| {
            Ok(RELS.with(|m| {
                m.borrow().iter().find(|(_, &v)| v == relid).map(|((name, _), _)| {
                    let mut nd = NameData::default();
                    nd.namestrcpy(name);
                    nd
                })
            }))
        });
        syscache_seams::lookup_pg_class_ls_shape::set(|relid| {
            Ok(RELS.with(|m| {
                m.borrow().iter().find(|(_, &v)| v == relid).map(|((_, nsp), _)| {
                    syscache_seams::PgClassLsShape {
                        relnamespace: *nsp,
                        reltype: InvalidOid,
                        relam: 2,
                        reltablespace: InvalidOid,
                        relnatts: 1,
                        relkind: b'r' as i8,
                        relpersistence: b'p' as i8,
                        relispartition: false,
                        relhassubclass: false,
                    }
                })
            }))
        });
        syscache_seams::lookup_pg_proc_name_candidates::set(|mcx, proname| {
            // PgVec::new_in: element embeds a PgVec, no-drop gate (projections.rs precedent).
            let mut out = mcx::PgVec::new_in(mcx);
            PROCS.with(|p| -> PgResult<()> {
                for (name, oid, nsp, args) in p.borrow().iter() {
                    if name == proname {
                        let mut av = mcx::PgVec::new_in(mcx);
                        for &a in args {
                            av.push(a);
                        }
                        out.push(syscache_seams::PgProcCandidate {
                            oid: *oid,
                            pronamespace: *nsp,
                            pronargs: args.len() as i16,
                            pronargdefaults: 0,
                            provariadic: InvalidOid,
                            proargtypes: av,
                        });
                    }
                }
                Ok(())
            })?;
            Ok(out)
        });
        syscache_seams::pg_proc_proname::set(|funcid| {
            Ok(PROCS.with(|p| {
                p.borrow().iter().find(|(_, oid, _, _)| *oid == funcid).map(|(name, ..)| {
                    let mut nd = NameData::default();
                    nd.namestrcpy(name);
                    nd
                })
            }))
        });
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok(PROCS.with(|p| {
                p.borrow().iter().find(|(_, oid, _, _)| *oid == funcid).map(|(_, _, nsp, args)| {
                    syscache_seams::PgProcShape {
                        prolang: 12,
                        prosecdef: false,
                        proconfig_isnull: true,
                        pronamespace: *nsp,
                        prorettype: 25,
                        provariadic: InvalidOid,
                        prosupport: InvalidOid,
                        pronargs: args.len() as i16,
                        prokind: b'f' as i8,
                        provolatile: b'i' as i8,
                        proparallel: b's' as i8,
                        proretset: false,
                        proisstrict: true,
                        proleakproof: false,
                    }
                })
            }))
        });
        syscache_seams::lookup_pg_proc_signature::set(|mcx, funcid| {
            Ok(PROCS.with(|p| {
                p.borrow().iter().find(|(_, oid, _, _)| *oid == funcid).map(|(_, _, _, args)| {
                    let mut av = mcx::PgVec::new_in(mcx);
                    for &a in args {
                        av.push(a);
                    }
                    (25, av)
                })
            }))
        });
        regproc_seams::parse_type_string::set(|_mcx, typename, esc| {
            match typename.to_ascii_lowercase().as_str() {
                "text" => Ok(Some((25, -1))),
                "integer" | "int4" => Ok(Some((23, -1))),
                "anyarray" => Ok(Some((2277, -1))),
                _ => types_error::ereturn(
                    esc,
                    None,
                    PgError::error(format!("type \"{typename}\" does not exist"))
                        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
                ),
            }
        });
        syscache_seams::lookup_authid_by_rolname::set(|rolname| {
            Ok((rolname == "malisper").then_some((ROLE_M, true)))
        });
        syscache_seams::lookup_authid_rolname::set(|mcx, roleid| {
            Ok(if roleid == ROLE_M {
                Some(mcx::PgString::from_str_in("malisper", mcx)?)
            } else {
                None
            })
        });
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            let mk = |name: &str, typelem: Oid, typsubscript: Oid, typstorage: u8| {
                let mut nd = NameData::default();
                nd.namestrcpy(name);
                syscache_seams::PgTypeTypcacheShape {
                    typname: nd,
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typstorage: typstorage as i8,
                    typtype: b'b' as i8,
                    typisdefined: true,
                    typrelid: InvalidOid,
                    typsubscript,
                    typelem,
                    typarray: InvalidOid,
                    typcollation: InvalidOid,
                }
            };
            Ok(match typid {
                23 => Some(mk("int4", InvalidOid, InvalidOid, b'p')),
                1043 => Some(mk("varchar", InvalidOid, InvalidOid, b'x')),
                1007 => Some(mk("_int4", 23, 6179, b'x')),
                2205 => Some(mk("regclass", InvalidOid, InvalidOid, b'p')),
                _ => None,
            })
        });
        namespace_seams::fetch_search_path::set(|mcx, _include_implicit| {
            let mut v = mcx::vec_new_in(mcx);
            PATH.with(|p| {
                for &oid in p.borrow().iter() {
                    v.push(oid);
                }
            });
            Ok(v)
        });
        namespace_seams::range_var_get_relid::set(|_mcx, rv, _lockmode, missing_ok| {
            let relid = match rv.schemaname {
                Some(schema) => {
                    let nsp =
                        NS_BY_NAME.with(|m| m.borrow().get(schema).copied()).unwrap_or(InvalidOid);
                    if !OidIsValid(nsp) {
                        if missing_ok {
                            return Ok(InvalidOid);
                        }
                        return Err(Box::new(
                            PgError::error(format!("schema \"{schema}\" does not exist"))
                                .with_sqlstate(types_error::ERRCODE_UNDEFINED_SCHEMA),
                        ));
                    }
                    RELS.with(|m| m.borrow().get(&(rv.relname.to_string(), nsp)).copied())
                        .unwrap_or(InvalidOid)
                }
                None => PATH.with(|p| {
                    p.borrow()
                        .iter()
                        .find_map(|nsp| {
                            RELS.with(|m| m.borrow().get(&(rv.relname.to_string(), *nsp)).copied())
                        })
                        .unwrap_or(InvalidOid)
                }),
            };
            if !OidIsValid(relid) && !missing_ok {
                return Err(Box::new(
                    PgError::error(format!("relation \"{}\" does not exist", rv.relname))
                        .with_sqlstate(types_error::ERRCODE_UNDEFINED_TABLE),
                ));
            }
            Ok(relid)
        });
    });

    NS_BY_NAME.with(|m| {
        let mut m = m.borrow_mut();
        m.clear();
        m.insert("pg_catalog".into(), NS_CATALOG);
        m.insert("public".into(), NS_PUBLIC);
        m.insert("s1".into(), NS_S1);
    });
    RELS.with(|m| {
        let mut m = m.borrow_mut();
        m.clear();
        m.insert(("pg_class".into(), NS_CATALOG), REL_PG_CLASS);
        m.insert(("t1".into(), NS_PUBLIC), REL_T1);
        m.insert(("Mixed Case".into(), NS_PUBLIC), REL_MIXED);
        m.insert(("t1".into(), NS_S1), REL_S1_T1);
        m.insert(("dup".into(), NS_CATALOG), REL_DUP_CAT);
        m.insert(("dup".into(), NS_PUBLIC), REL_DUP_PUB);
    });
    PROCS.with(|p| {
        *p.borrow_mut() = vec![
            ("now".into(), PROC_NOW, NS_CATALOG, vec![]),
            ("lower".into(), PROC_LOWER_TEXT, NS_CATALOG, vec![25]),
            ("lower".into(), PROC_LOWER_ANY, NS_CATALOG, vec![2277]),
            ("sfunc".into(), PROC_SF_CAT, NS_CATALOG, vec![23]),
            ("sfunc".into(), PROC_SF_PUB, NS_PUBLIC, vec![23]),
        ];
    });
    PATH.with(|p| *p.borrow_mut() = vec![NS_CATALOG, NS_PUBLIC]);
}

fn with_mcx<R>(f: impl FnOnce(Mcx<'_>) -> R) -> R {
    install_fakes();
    let ctx = MemoryContext::new("regproc-test");
    f(ctx.mcx())
}

fn out_str<'a>(v: &'a RegName<'_>) -> &'a str {
    core::str::from_utf8(&v[..v.len() - 1]).unwrap()
}

#[test]
fn numeric_and_dash_paths() {
    with_mcx(|mcx| {
        assert_eq!(regclassin(mcx, "-", None).unwrap(), Some(0));
        assert_eq!(regclassin(mcx, "0", None).unwrap(), Some(0));
        assert_eq!(regclassin(mcx, "1259", None).unwrap(), Some(1259));
        assert_eq!(regclassin(mcx, "4294967295", None).unwrap(), Some(4294967295));
        assert_eq!(regtypein(mcx, "23", None).unwrap(), Some(23));
        assert_eq!(regprocin(mcx, "1299", None).unwrap(), Some(1299));

        let err = regclassin(mcx, "4294967296", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(err.message(), "value \"4294967296\" is out of range for type oid");
        let err = regclassin(mcx, "99999999999999999999", None).unwrap_err();
        assert_eq!(err.message(), "value \"99999999999999999999\" is out of range for type oid");

        let mut soft = SoftErrorContext::new(true);
        assert_eq!(regclassin(mcx, "4294967296", Some(&mut soft)).unwrap(), Some(0));
        assert!(soft.error_occurred());
    });
}

#[test]
fn regclassin_names() {
    with_mcx(|mcx| {
        assert_eq!(regclassin(mcx, "pg_class", None).unwrap(), Some(REL_PG_CLASS));
        assert_eq!(regclassin(mcx, "pg_catalog.pg_class", None).unwrap(), Some(REL_PG_CLASS));
        assert_eq!(regclassin(mcx, "PG_CLASS", None).unwrap(), Some(REL_PG_CLASS));
        assert_eq!(regclassin(mcx, "  pg_class  ", None).unwrap(), Some(REL_PG_CLASS));
        assert_eq!(regclassin(mcx, "\"Mixed Case\"", None).unwrap(), Some(REL_MIXED));
        assert_eq!(regclassin(mcx, "s1.t1", None).unwrap(), Some(REL_S1_T1));

        let err = regclassin(mcx, "\"PG_CLASS\"", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_TABLE);
        assert_eq!(err.message(), "relation \"PG_CLASS\" does not exist");
        let err = regclassin(mcx, "  1259", None).unwrap_err();
        assert_eq!(err.message(), "relation \"1259\" does not exist");
        let err = regclassin(mcx, "no_such_schema.rel", None).unwrap_err();
        assert_eq!(err.message(), "relation \"no_such_schema.rel\" does not exist");
        let err = regclassin(mcx, "", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_NAME);
        assert_eq!(err.message(), "invalid name syntax");
        let err = regclassin(mcx, "...", None).unwrap_err();
        assert_eq!(err.message(), "invalid name syntax");
        let err = regclassin(mcx, "a.b.c.d", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_SYNTAX_ERROR);
        assert_eq!(err.message(), "improper relation name (too many dotted names): a.b.c.d");
    });
}

#[test]
fn to_regclass_soft_semantics() {
    with_mcx(|mcx| {
        // C DirectInputFunctionCallSafe: soft failure -> NULL, "-" -> Datum 0.
        let mut soft = SoftErrorContext::new(false);
        assert_eq!(regclassin(mcx, "no_such_rel", Some(&mut soft)).unwrap(), Some(0));
        assert!(soft.error_occurred());
        let mut soft = SoftErrorContext::new(false);
        assert_eq!(regclassin(mcx, "", Some(&mut soft)).unwrap(), None);
        assert!(soft.error_occurred());
        let mut soft = SoftErrorContext::new(false);
        assert_eq!(regclassin(mcx, "-", Some(&mut soft)).unwrap(), Some(0));
        assert!(!soft.error_occurred());
        // Hard even under soft context, like C makeRangeVarFromNameList.
        let mut soft = SoftErrorContext::new(false);
        assert!(regclassin(mcx, "a.b.c.d", Some(&mut soft)).is_err());
    });
}

#[test]
fn regclassout_visibility_and_quoting() {
    with_mcx(|mcx| {
        assert_eq!(out_str(&regclassout(mcx, InvalidOid).unwrap()), "-");
        assert_eq!(out_str(&regclassout(mcx, REL_PG_CLASS).unwrap()), "pg_class");
        assert_eq!(out_str(&regclassout(mcx, REL_MIXED).unwrap()), "\"Mixed Case\"");
        assert_eq!(out_str(&regclassout(mcx, REL_S1_T1).unwrap()), "s1.t1");
        assert_eq!(out_str(&regclassout(mcx, REL_DUP_CAT).unwrap()), "dup");
        assert_eq!(out_str(&regclassout(mcx, REL_DUP_PUB).unwrap()), "public.dup");
        assert_eq!(out_str(&regclassout(mcx, 12345678).unwrap()), "12345678");
        assert_eq!(out_str(&regclassout(mcx, 4294967295).unwrap()), "4294967295");
    });
}

#[test]
fn regprocin_lookup() {
    with_mcx(|mcx| {
        assert_eq!(regprocin(mcx, "now", None).unwrap(), Some(PROC_NOW));
        assert_eq!(regprocin(mcx, "pg_catalog.now", None).unwrap(), Some(PROC_NOW));
        assert_eq!(regprocin(mcx, "pg_catalog.\"now\"", None).unwrap(), Some(PROC_NOW));
        // Same-signature shadowing: pg_catalog precedes public in the path.
        assert_eq!(regprocin(mcx, "sfunc", None).unwrap(), Some(PROC_SF_CAT));
        assert_eq!(regprocin(mcx, "public.sfunc", None).unwrap(), Some(PROC_SF_PUB));

        let err = regprocin(mcx, "no_such_func", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);
        assert_eq!(err.message(), "function \"no_such_func\" does not exist");
        let err = regprocin(mcx, "lower", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_AMBIGUOUS_FUNCTION);
        assert_eq!(err.message(), "more than one function named \"lower\"");
        let mut soft = SoftErrorContext::new(false);
        assert_eq!(regprocin(mcx, "lower", Some(&mut soft)).unwrap(), Some(0));
        assert!(soft.error_occurred());
    });
}

#[test]
fn regprocout_qualification() {
    with_mcx(|mcx| {
        assert_eq!(out_str(&regprocout(mcx, InvalidOid).unwrap()), "-");
        assert_eq!(out_str(&regprocout(mcx, PROC_NOW).unwrap()), "now");
        // Ambiguous unqualified lookup -> qualified.
        assert_eq!(out_str(&regprocout(mcx, PROC_LOWER_TEXT).unwrap()), "pg_catalog.lower");
        assert_eq!(out_str(&regprocout(mcx, PROC_SF_PUB).unwrap()), "public.sfunc");
        assert_eq!(out_str(&regprocout(mcx, PROC_SF_CAT).unwrap()), "sfunc");
        assert_eq!(out_str(&regprocout(mcx, 99999999).unwrap()), "99999999");
    });
}

#[test]
fn regprocedurein_lookup() {
    with_mcx(|mcx| {
        assert_eq!(regprocedurein(mcx, "-", None).unwrap(), Some(0));
        assert_eq!(regprocedurein(mcx, "1299", None).unwrap(), Some(1299));
        assert_eq!(regprocedurein(mcx, "now()", None).unwrap(), Some(PROC_NOW));
        assert_eq!(regprocedurein(mcx, "now(  )", None).unwrap(), Some(PROC_NOW));
        assert_eq!(regprocedurein(mcx, "lower(text)", None).unwrap(), Some(PROC_LOWER_TEXT));
        assert_eq!(regprocedurein(mcx, "lower(anyarray)", None).unwrap(), Some(PROC_LOWER_ANY));
        assert_eq!(
            regprocedurein(mcx, "pg_catalog.lower( text )", None).unwrap(),
            Some(PROC_LOWER_TEXT)
        );
        // Same-signature shadowing: pg_catalog precedes public in the path.
        assert_eq!(regprocedurein(mcx, "sfunc(integer)", None).unwrap(), Some(PROC_SF_CAT));
        assert_eq!(regprocedurein(mcx, "public.sfunc(integer)", None).unwrap(), Some(PROC_SF_PUB));

        let err = regprocedurein(mcx, "lower", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
        assert_eq!(err.message(), "expected a left parenthesis");
        let err = regprocedurein(mcx, "lower(text", None).unwrap_err();
        assert_eq!(err.message(), "expected a right parenthesis");
        let err = regprocedurein(mcx, "lower(text,)", None).unwrap_err();
        assert_eq!(err.message(), "expected a type name");
        let err = regprocedurein(mcx, "lower(\"text)", None).unwrap_err();
        assert_eq!(err.message(), "improper type name");
        let err = regprocedurein(mcx, "lower(integer)", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);
        assert_eq!(err.message(), "function \"lower(integer)\" does not exist");

        let mut soft = SoftErrorContext::new(false);
        assert_eq!(regprocedurein(mcx, "no_such(text)", Some(&mut soft)).unwrap(), Some(0));
        assert!(soft.error_occurred());
        let mut soft = SoftErrorContext::new(false);
        assert_eq!(regprocedurein(mcx, "lower", Some(&mut soft)).unwrap(), None);
        assert!(soft.error_occurred());
    });
}

#[test]
fn funcname_missing_schema_is_function_not_found() {
    // C FuncnameGetCandidates(missing_ok=true): a missing schema yields no
    // candidates ("function does not exist"), never a schema error.
    with_mcx(|mcx| {
        let err = regprocin(mcx, "ng_catalog.now", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);
        assert_eq!(err.message(), "function \"ng_catalog.now\" does not exist");
        let err = regprocedurein(mcx, "ng_catalog.lower(text)", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);
        assert_eq!(err.message(), "function \"ng_catalog.lower(text)\" does not exist");
        let mut soft = SoftErrorContext::new(true);
        assert_eq!(regprocin(mcx, "ng_catalog.now", Some(&mut soft)).unwrap(), Some(0));
        assert!(soft.error_occurred());
        assert_eq!(
            soft.error().unwrap().message(),
            "function \"ng_catalog.now\" does not exist"
        );
    });
}

#[test]
fn regprocedureout_formats() {
    with_mcx(|mcx| {
        assert_eq!(out_str(&regprocedureout(mcx, InvalidOid).unwrap()), "-");
        assert_eq!(out_str(&regprocedureout(mcx, PROC_NOW).unwrap()), "now()");
        assert_eq!(out_str(&regprocedureout(mcx, 99999999).unwrap()), "99999999");
    });
}

#[test]
fn regtypein_name_arm() {
    with_mcx(|mcx| {
        assert_eq!(regtypein(mcx, "-", None).unwrap(), Some(0));
        assert_eq!(regtypein(mcx, "23", None).unwrap(), Some(23));
        assert_eq!(regtypein(mcx, "integer", None).unwrap(), Some(23));
        let err = regtypein(mcx, "no_such_type", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
        let mut soft = SoftErrorContext::new(false);
        assert_eq!(regtypein(mcx, "no_such_type", Some(&mut soft)).unwrap(), None);
        assert!(soft.error_occurred());
    });
}

#[test]
fn to_regtypemod_soft_fail() {
    with_mcx(|mcx| {
        assert_eq!(to_regtypemod(mcx, "integer", None).unwrap(), Some(-1));
        assert_eq!(to_regtypemod(mcx, "text", None).unwrap(), Some(-1));
        let err = to_regtypemod(mcx, "no_such_type", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
        let mut soft = SoftErrorContext::new(false);
        assert_eq!(to_regtypemod(mcx, "no_such_type", Some(&mut soft)).unwrap(), None);
        assert!(soft.error_occurred());
    });
}

#[test]
fn regtypeout_formats() {
    with_mcx(|mcx| {
        assert_eq!(out_str(&regtypeout(mcx, InvalidOid).unwrap()), "-");
        assert_eq!(out_str(&regtypeout(mcx, 23).unwrap()), "integer");
        assert_eq!(out_str(&regtypeout(mcx, 1043).unwrap()), "character varying");
        assert_eq!(out_str(&regtypeout(mcx, 1007).unwrap()), "integer[]");
        assert_eq!(out_str(&regtypeout(mcx, 2205).unwrap()), "regclass");
        assert_eq!(out_str(&regtypeout(mcx, 99999999).unwrap()), "99999999");
    });
}

#[test]
fn regnamespace_in_out() {
    with_mcx(|mcx| {
        assert_eq!(regnamespacein(mcx, "pg_catalog", None).unwrap(), Some(NS_CATALOG));
        assert_eq!(regnamespacein(mcx, "\"pg_catalog\"", None).unwrap(), Some(NS_CATALOG));
        assert_eq!(regnamespacein(mcx, "-", None).unwrap(), Some(0));
        let err = regnamespacein(mcx, "no_such_ns", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_SCHEMA);
        assert_eq!(err.message(), "schema \"no_such_ns\" does not exist");
        let err = regnamespacein(mcx, "a.b", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_NAME);

        assert_eq!(out_str(&regnamespaceout(mcx, InvalidOid).unwrap()), "-");
        assert_eq!(out_str(&regnamespaceout(mcx, NS_CATALOG).unwrap()), "pg_catalog");
        assert_eq!(out_str(&regnamespaceout(mcx, 99999999).unwrap()), "99999999");
    });
}

#[test]
fn regrole_in_out() {
    with_mcx(|mcx| {
        assert_eq!(regrolein(mcx, "malisper", None).unwrap(), Some(ROLE_M));
        assert_eq!(regrolein(mcx, "-", None).unwrap(), Some(0));
        let err = regrolein(mcx, "no_such_role", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
        assert_eq!(err.message(), "role \"no_such_role\" does not exist");
        let err = regrolein(mcx, "a.b", None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_NAME);

        assert_eq!(out_str(&regroleout(mcx, InvalidOid).unwrap()), "-");
        assert_eq!(out_str(&regroleout(mcx, ROLE_M).unwrap()), "malisper");
        assert_eq!(out_str(&regroleout(mcx, 99999999).unwrap()), "99999999");
    });
}

#[test]
fn text_regclass_hard_path() {
    with_mcx(|mcx| {
        assert_eq!(text_regclass(mcx, "t1").unwrap(), REL_T1);
        assert_eq!(text_regclass(mcx, "s1.t1").unwrap(), REL_S1_T1);
        assert!(text_regclass(mcx, "no_such_rel").is_err());
    });
}

#[test]
fn builtins_table_matches_pg_proc() {
    // (oid, name, nargs) transcribed from pg_proc.dat / fmgr_core CANONICAL.
    let expected: &[(Oid, &str, i16)] = &[
        (44, "regprocin", 1),
        (45, "regprocout", 1),
        (1079, "text_regclass", 1),
        (2212, "regprocedurein", 1),
        (2213, "regprocedureout", 1),
        (2214, "regoperin", 1),
        (2215, "regoperout", 1),
        (2216, "regoperatorin", 1),
        (2217, "regoperatorout", 1),
        (2218, "regclassin", 1),
        (2219, "regclassout", 1),
        (2220, "regtypein", 1),
        (2221, "regtypeout", 1),
        (2444, "regprocrecv", 1),
        (2445, "regprocsend", 1),
        (2446, "regprocedurerecv", 1),
        (2448, "regoperrecv", 1),
        (2449, "regopersend", 1),
        (2450, "regoperatorrecv", 1),
        (2451, "regoperatorsend", 1),
        (2447, "regproceduresend", 1),
        (2452, "regclassrecv", 1),
        (2453, "regclasssend", 1),
        (2454, "regtyperecv", 1),
        (2455, "regtypesend", 1),
        (3476, "to_regoperator", 1),
        (3479, "to_regprocedure", 1),
        (3492, "to_regoper", 1),
        (3493, "to_regtype", 1),
        (3736, "regconfigin", 1),
        (3737, "regconfigout", 1),
        (3738, "regconfigrecv", 1),
        (3739, "regconfigsend", 1),
        (3771, "regdictionaryin", 1),
        (3772, "regdictionaryout", 1),
        (3773, "regdictionaryrecv", 1),
        (3774, "regdictionarysend", 1),
        (3494, "to_regproc", 1),
        (3495, "to_regclass", 1),
        (4084, "regnamespacein", 1),
        (4085, "regnamespaceout", 1),
        (4086, "to_regnamespace", 1),
        (4087, "regnamespacerecv", 1),
        (4088, "regnamespacesend", 1),
        (4092, "regroleout", 1),
        (4093, "to_regrole", 1),
        (4094, "regrolerecv", 1),
        (4095, "regrolesend", 1),
        (4098, "regrolein", 1),
        (4193, "regcollationin", 1),
        (4194, "regcollationout", 1),
        (4195, "to_regcollation", 1),
        (4196, "regcollationrecv", 1),
        (4197, "regcollationsend", 1),
        (6317, "to_regtypemod", 1),
    ];
    assert_eq!(builtins::REGPROC_BUILTINS.len(), expected.len());
    for (b, (oid, name, nargs)) in builtins::REGPROC_BUILTINS.iter().zip(expected) {
        assert_eq!(
            (b.foid, b.name, b.nargs, b.strict, b.retset),
            (*oid, *name, *nargs, true, false)
        );
    }
}
