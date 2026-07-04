use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use mcx::MemoryContext;
use types_core::{InvalidOid, Oid, PG_CATALOG_NAMESPACE, RELPERSISTENCE_PERMANENT};
use types_error::PgResult;
use types_tuple::NameData;

use crate::*;

const USER_A: Oid = 10;
const NS_PUBLIC: Oid = 2200;
const NS_S1: Oid = 5001;
const NS_TEMP: Oid = 16700;
const NS_TEMP_TOAST: Oid = 16701;
const REL_T1: Oid = 20001;

thread_local! {
    static NS_BY_NAME: RefCell<HashMap<String, Oid>> = RefCell::new(HashMap::new());
    static RELS: RefCell<HashMap<(String, Oid), Oid>> = RefCell::new(HashMap::new());
    static ROLNAME: RefCell<Option<String>> = const { RefCell::new(None) };
    static USER: Cell<Oid> = const { Cell::new(USER_A) };
    static ACL_DENIED: RefCell<Vec<Oid>> = const { RefCell::new(Vec::new()) };
}

fn install_fakes() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        miscinit_seams::get_user_id::set(|| USER.with(Cell::get));
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        aclchk_seams::object_aclcheck::set(|_classid, objid, _roleid, _mode| {
            Ok(if ACL_DENIED.with(|d| d.borrow().contains(&objid)) {
                1
            } else {
                0
            })
        });
        aclchk_seams::aclcheck_error::set(|_result, _objtype, name| {
            Err(Box::new(types_error::PgError::error(format!(
                "permission denied for schema {name}"
            ))))
        });
        syscache_seams::lookup_authid_rolname::set(|mcx, _roleid| {
            Ok(match ROLNAME.with(|r| r.borrow().clone()) {
                Some(n) => Some(mcx::PgString::from_str_in(&n, mcx)?),
                None => None,
            })
        });
        syscache_seams::pg_namespace_nspname::set(|nspid| {
            Ok(NS_BY_NAME.with(|m| {
                m.borrow().iter().find(|(_, &v)| v == nspid).map(|(k, _)| {
                    let mut nd = NameData::default();
                    nd.namestrcpy(k);
                    nd
                })
            }))
        });
        syscache_seams::lookup_pg_namespace_oid_by_name::set(|nspname| {
            Ok(NS_BY_NAME
                .with(|m| m.borrow().get(nspname).copied())
                .unwrap_or(InvalidOid))
        });
        syscache_seams::lookup_pg_class_relid_by_name::set(|relname, nsp| {
            Ok(RELS
                .with(|m| m.borrow().get(&(relname.to_string(), nsp)).copied())
                .unwrap_or(InvalidOid))
        });
        inval_seams::accept_invalidation_messages::set(|| Ok(()));
        lmgr_seams::lock_relation_oid::set(|_, _| Ok(()));
        lmgr_seams::unlock_relation_oid::set(|_, _| Ok(()));
        crate::init_seams();
    });

    NS_BY_NAME.with(|m| {
        let mut m = m.borrow_mut();
        m.clear();
        m.insert("pg_catalog".into(), PG_CATALOG_NAMESPACE);
        m.insert("public".into(), NS_PUBLIC);
        m.insert("s1".into(), NS_S1);
        m.insert("pg_temp_7".into(), NS_TEMP);
        m.insert("pg_toast_temp_7".into(), NS_TEMP_TOAST);
    });
    RELS.with(|m| {
        let mut m = m.borrow_mut();
        m.clear();
        m.insert(("t1".into(), NS_PUBLIC), REL_T1);
    });
    ROLNAME.with(|r| *r.borrow_mut() = None);
    ACL_DENIED.with(|d| d.borrow_mut().clear());
    USER.with(|u| u.set(USER_A));
}

fn set_search_path(v: &str) {
    NAMESPACE_SEARCH_PATH.with(|s| *s.borrow_mut() = Some(v.to_string()));
    assign_search_path(Some(v));
}

#[test]
fn temp_predicates_and_state() {
    install_fakes();

    assert!(!isTempNamespace(NS_TEMP));
    assert!(!isTempToastNamespace(NS_TEMP_TOAST));
    assert!(!isTempOrTempToastNamespace(NS_TEMP));
    assert_eq!(GetTempNamespaceState(), (InvalidOid, InvalidOid));

    SetTempNamespaceState(NS_TEMP, NS_TEMP_TOAST);
    assert!(isTempNamespace(NS_TEMP));
    assert!(!isTempNamespace(NS_TEMP_TOAST));
    assert!(isTempToastNamespace(NS_TEMP_TOAST));
    assert!(isTempOrTempToastNamespace(NS_TEMP));
    assert!(isTempOrTempToastNamespace(NS_TEMP_TOAST));
    assert_eq!(GetTempToastNamespace(), NS_TEMP_TOAST);
    assert_eq!(GetTempNamespaceState(), (NS_TEMP, NS_TEMP_TOAST));

    assert!(isAnyTempNamespace(NS_TEMP).unwrap());
    assert!(isAnyTempNamespace(NS_TEMP_TOAST).unwrap());
    assert!(!isAnyTempNamespace(NS_PUBLIC).unwrap());
    assert!(!isOtherTempNamespace(NS_TEMP).unwrap());

    assert_eq!(GetTempNamespaceProcNumber(NS_TEMP).unwrap(), 7);
    assert_eq!(GetTempNamespaceProcNumber(NS_TEMP_TOAST).unwrap(), 7);
    assert_eq!(
        GetTempNamespaceProcNumber(NS_PUBLIC).unwrap(),
        types_core::INVALID_PROC_NUMBER
    );
    assert_eq!(
        GetTempNamespaceProcNumber(99999).unwrap(),
        types_core::INVALID_PROC_NUMBER
    );
}

#[test]
fn at_eoxact_noop_without_temp_creation() {
    install_fakes();
    AtEOXact_Namespace(true, false);
    AtEOXact_Namespace(false, true);
    AtEOSubXact_Namespace(true, 5, 4);
    AtEOSubXact_Namespace(false, 5, 4);
}

fn get_relname_relid_in_path(relname: &str) -> Oid {
    RelnameGetRelid(relname).unwrap()
}

#[test]
fn search_path_resolution_and_caching() {
    install_fakes();
    set_search_path("public, s1");

    assert_eq!(get_relname_relid_in_path("t1"), REL_T1);
    assert_eq!(get_relname_relid_in_path("nope"), InvalidOid);

    let ctx = MemoryContext::new("test");
    let path = fetch_search_path(ctx.mcx(), true).unwrap();
    assert_eq!(path.as_slice(), &[PG_CATALOG_NAMESPACE, NS_PUBLIC, NS_S1]);
    let explicit = fetch_search_path(ctx.mcx(), false).unwrap();
    assert_eq!(explicit.as_slice(), &[NS_PUBLIC, NS_S1]);

    // ACL-denied schemas drop out of the path after invalidation.
    ACL_DENIED.with(|d| d.borrow_mut().push(NS_S1));
    set_search_path("public, s1");
    // Same string, still-valid cache: oidlist is cached, so s1 stays until a
    // syscache invalidation clears the cache.
    let cached = fetch_search_path(ctx.mcx(), false).unwrap();
    assert_eq!(cached.as_slice(), &[NS_PUBLIC, NS_S1]);

    crate::path::invalidate_search_path_cache();
    assign_search_path(Some("public, s1"));
    let after_inval = fetch_search_path(ctx.mcx(), false).unwrap();
    assert_eq!(after_inval.as_slice(), &[NS_PUBLIC]);
}

#[test]
fn dollar_user_and_missing_schemas() {
    install_fakes();
    ROLNAME.with(|r| *r.borrow_mut() = Some("s1".to_string()));
    set_search_path("\"$user\", missing_schema, public");

    let ctx = MemoryContext::new("test");
    let path = fetch_search_path(ctx.mcx(), false).unwrap();
    assert_eq!(path.as_slice(), &[NS_S1, NS_PUBLIC]);
}

#[test]
fn user_change_invalidates_path() {
    install_fakes();
    set_search_path("public");
    let ctx = MemoryContext::new("test");
    assert_eq!(
        fetch_search_path(ctx.mcx(), false).unwrap().as_slice(),
        &[NS_PUBLIC]
    );

    ACL_DENIED.with(|d| d.borrow_mut().push(NS_PUBLIC));
    crate::path::invalidate_search_path_cache();
    USER.with(|u| u.set(USER_A + 1));
    // Different roleid forces recompute even though the string is unchanged.
    assert_eq!(
        fetch_search_path(ctx.mcx(), false).unwrap().as_slice(),
        &[] as &[Oid]
    );
}

#[test]
fn matcher_generation_fast_path() {
    install_fakes();
    set_search_path("public");
    let ctx = MemoryContext::new("test");

    let mut matcher = GetSearchPathMatcher(ctx.mcx()).unwrap();
    assert!(matcher.addCatalog);
    assert!(!matcher.addTemp);
    assert_eq!(matcher.schemas.as_slice(), &[NS_PUBLIC]);
    assert!(SearchPathMatchesCurrentEnvironment(&mut matcher).unwrap());

    let copy = CopySearchPathMatcher(ctx.mcx(), &matcher).unwrap();
    assert_eq!(copy.generation, matcher.generation);

    set_search_path("s1, public");
    assert!(!SearchPathMatchesCurrentEnvironment(&mut matcher).unwrap());

    set_search_path("public");
    // Path content is back to the original; matcher matches again and its
    // generation is refreshed to the new active generation.
    assert!(SearchPathMatchesCurrentEnvironment(&mut matcher).unwrap());
    let gen_now = GetSearchPathMatcher(ctx.mcx()).unwrap().generation;
    assert_eq!(matcher.generation, gen_now);

    let mut zero_gen = SearchPathMatcher {
        schemas: mcx::slice_in(ctx.mcx(), &[NS_PUBLIC]).unwrap(),
        addCatalog: true,
        addTemp: false,
        generation: 0,
    };
    assert!(SearchPathMatchesCurrentEnvironment(&mut zero_gen).unwrap());
}

#[test]
fn range_var_lookups() {
    install_fakes();
    set_search_path("public");

    let rv = |schema: Option<&'static str>, name: &'static str| rel_vocab::RangeVar {
        catalogname: None,
        schemaname: schema,
        relname: name,
        inh: true,
        relpersistence: RELPERSISTENCE_PERMANENT,
        location: -1,
    };

    assert_eq!(RangeVarGetRelid(&rv(None, "t1"), 1, false).unwrap(), REL_T1);
    assert_eq!(
        RangeVarGetRelid(&rv(None, "gone"), 1, true).unwrap(),
        InvalidOid
    );

    let err = RangeVarGetRelid(&rv(None, "gone"), 1, false).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_TABLE);
    assert!(err.message().contains("relation \"gone\" does not exist"));

    let err = RangeVarGetRelid(&rv(Some("no_such"), "t1"), 1, false).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_SCHEMA);

    assert_eq!(
        RangeVarGetRelid(&rv(Some("no_such"), "t1"), 1, true).unwrap(),
        InvalidOid
    );
}

#[test]
fn check_search_path_validates_syntax() {
    install_fakes();
    let ctx = MemoryContext::new("test");
    assert!(check_search_path(ctx.mcx(), "a, b, \"quoted, name\"").unwrap());
    assert!(check_search_path(ctx.mcx(), "").unwrap());

    // The syntax check needs no live GUC error sink until it fails; install
    // one for the failure case.
    if !guc_seams::guc_check_errdetail::is_installed() {
        guc_seams::guc_check_errdetail::set(|_| {});
    }
    assert!(!check_search_path(ctx.mcx(), "a,, b").unwrap());
    assert!(!check_search_path(ctx.mcx(), "\"unterminated").unwrap());
}

#[test]
fn lookup_namespace_helpers() {
    install_fakes();

    assert_eq!(LookupNamespaceNoError("pg_temp").unwrap(), InvalidOid);
    assert_eq!(
        LookupExplicitNamespace("pg_temp", true).unwrap(),
        InvalidOid
    );

    let denied: PgResult<Oid> = {
        ACL_DENIED.with(|d| d.borrow_mut().push(NS_S1));
        LookupExplicitNamespace("s1", false)
    };
    assert!(denied.unwrap_err().message().contains("permission denied"));
}

fn install_proc_candidates() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_proc_name_candidates::set(|mcx, proname| {
            let mut v = mcx::PgVec::new_in(mcx);
            let mut cand = |oid, nsp, args: &[Oid], variadic, ndefaults| {
                let mut a = mcx::vec_with_capacity_in(mcx, args.len()).unwrap();
                for &t in args {
                    a.push(t);
                }
                syscache_seams::PgProcCandidate {
                    oid,
                    pronamespace: nsp,
                    pronargs: args.len() as i16,
                    pronargdefaults: ndefaults,
                    provariadic: variadic,
                    proargtypes: a,
                }
            };
            match proname {
                "f" => {
                    v.push(cand(9001, PG_CATALOG_NAMESPACE, &[23], InvalidOid, 0));
                    v.push(cand(9002, 9999, &[23], InvalidOid, 0));
                    v.push(cand(9003, PG_CATALOG_NAMESPACE, &[23, 23], InvalidOid, 0));
                }
                "vf" => {
                    v.push(cand(9004, PG_CATALOG_NAMESPACE, &[2277], 2283, 0));
                }
                "pf" => {
                    v.push(cand(9005, PG_CATALOG_NAMESPACE, &[23], InvalidOid, 0));
                    v.push(cand(9006, PG_CATALOG_NAMESPACE, &[1007], 23, 0));
                }
                "df" => {
                    v.push(cand(9007, PG_CATALOG_NAMESPACE, &[23], InvalidOid, 0));
                    v.push(cand(9008, PG_CATALOG_NAMESPACE, &[23, 23], InvalidOid, 1));
                }
                "amb" => {
                    v.push(cand(9010, PG_CATALOG_NAMESPACE, &[23, 1007], 23, 0));
                    v.push(cand(9011, PG_CATALOG_NAMESPACE, &[1007], 23, 0));
                }
                "nf" => {
                    v.push(cand(
                        9020,
                        PG_CATALOG_NAMESPACE,
                        &[23, 25, 23],
                        InvalidOid,
                        1,
                    ));
                }
                "outp" => {
                    v.push(cand(9021, PG_CATALOG_NAMESPACE, &[23], InvalidOid, 0));
                }
                _ => {}
            }
            Ok(v)
        });
        fn strs<'m>(mcx: mcx::Mcx<'m>, names: &[&str]) -> mcx::PgVec<'m, mcx::PgString<'m>> {
            let mut v = mcx::PgVec::new_in(mcx);
            for n in names {
                v.push(mcx::PgString::from_str_in(n, mcx).unwrap());
            }
            v
        }
        fn oids<'m>(mcx: mcx::Mcx<'m>, types: &[Oid]) -> mcx::PgVec<'m, Oid> {
            let mut v = mcx::vec_with_capacity_in(mcx, types.len()).unwrap();
            v.extend_from_slice(types);
            v
        }
        fn modes<'m>(mcx: mcx::Mcx<'m>, ms: &[u8]) -> mcx::PgVec<'m, i8> {
            let mut v = mcx::vec_with_capacity_in(mcx, ms.len()).unwrap();
            for &m in ms {
                v.push(m as i8);
            }
            v
        }
        syscache_seams::pg_proc_result_arrays::set(|mcx, funcid| {
            Ok(match funcid {
                9020 => Some(syscache_seams::PgProcResultArraysShape {
                    proallargtypes: None,
                    proargmodes: None,
                    proargnames: Some(strs(mcx, &["a", "b", "c"])),
                }),
                9021 => Some(syscache_seams::PgProcResultArraysShape {
                    proallargtypes: Some(oids(mcx, &[23, 25])),
                    proargmodes: Some(modes(mcx, b"io")),
                    proargnames: Some(strs(mcx, &["a", "b"])),
                }),
                _ => None,
            })
        });
    });
}

#[test]
fn funcname_candidates_filter_arity_and_visibility() {
    install_fakes();
    install_proc_candidates();
    set_search_path("public");

    let ctx = MemoryContext::new("t");
    let cands = crate::FuncnameGetCandidates(ctx.mcx(), &["f"], 1, &[], true, true).unwrap();
    // 9002 is in an off-path namespace; 9003 has the wrong arity.
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].oid, 9001);
    assert_eq!(cands[0].args.as_slice(), &[23]);
}

#[test]
fn variadic_candidate_expands() {
    install_fakes();
    install_proc_candidates();
    set_search_path("public");

    let ctx = MemoryContext::new("t");
    let cands = crate::FuncnameGetCandidates(ctx.mcx(), &["vf"], 3, &[], true, true).unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].oid, 9004);
    assert_eq!(cands[0].nargs, 3);
    assert_eq!(cands[0].nvargs, 3);
    assert_eq!(cands[0].va_elem_type, 2283);
    assert_eq!(cands[0].args.as_slice(), &[2283, 2283, 2283]);

    // expand_variadic=false: the raw signature.
    let cands = crate::FuncnameGetCandidates(ctx.mcx(), &["vf"], 1, &[], false, true).unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].nvargs, 0);
    assert_eq!(cands[0].args.as_slice(), &[2277]);
}

#[test]
fn nonvariadic_masks_variadic_with_same_expansion() {
    install_fakes();
    install_proc_candidates();
    set_search_path("public");

    let ctx = MemoryContext::new("t");
    let cands = crate::FuncnameGetCandidates(ctx.mcx(), &["pf"], 1, &[], true, true).unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].oid, 9005);
    assert_eq!(cands[0].nvargs, 0);
}

#[test]
fn defaults_candidate_conflicts_with_exact_arity_sibling() {
    install_fakes();
    install_proc_candidates();
    set_search_path("public");

    // C's own ambiguity example: f(int) vs f(int, int DEFAULT ...) at one
    // arg — dedup ignores defaulted args, preference is undecidable.
    let ctx = MemoryContext::new("t");
    let cands = crate::FuncnameGetCandidates(ctx.mcx(), &["df"], 1, &[], true, true).unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].oid, InvalidOid);
    assert_eq!(cands[0].args.as_slice(), &[23]);

    // At two args only the defaulted signature matches; no expansion needed.
    let cands = crate::FuncnameGetCandidates(ctx.mcx(), &["df"], 2, &[], true, true).unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].oid, 9008);
    assert_eq!(cands[0].ndargs, 0);
}

#[test]
fn undecidable_duplicate_marked_ambiguous() {
    install_fakes();
    install_proc_candidates();
    set_search_path("public");

    // f(int, VARIADIC int[]) vs f(VARIADIC int[]) at 2 args: C marks the
    // surviving entry InvalidOid (parse_func turns it into "not unique").
    let ctx = MemoryContext::new("t");
    let cands = crate::FuncnameGetCandidates(ctx.mcx(), &["amb"], 2, &[], true, true).unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].oid, InvalidOid);
    assert_eq!(cands[0].args.as_slice(), &[23, 23]);
}

#[test]
fn named_notation_builds_argnumbers_mapping() {
    install_fakes();
    install_proc_candidates();
    set_search_path("public");

    // nf(a int, b text, c int DEFAULT ...): nf(1, c => 2, b => 't').
    let ctx = MemoryContext::new("t");
    let cands = crate::FuncnameGetCandidatesExtended(
        ctx.mcx(),
        &["nf"],
        3,
        &["c", "b"],
        true,
        true,
        false,
        false,
    )
    .unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].oid, 9020);
    assert_eq!(cands[0].nargs, 3);
    assert_eq!(cands[0].ndargs, 0);
    assert_eq!(cands[0].argnumbers.as_ref().unwrap().as_slice(), &[0, 2, 1]);
    assert_eq!(cands[0].args.as_slice(), &[23, 23, 25]);
}

#[test]
fn named_notation_fills_defaults_after_supplied_args() {
    install_fakes();
    install_proc_candidates();
    set_search_path("public");

    // nf(1, b => 't'): c is defaulted, mapped after the supplied arguments.
    let ctx = MemoryContext::new("t");
    let cands = crate::FuncnameGetCandidatesExtended(
        ctx.mcx(),
        &["nf"],
        2,
        &["b"],
        true,
        true,
        false,
        false,
    )
    .unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].oid, 9020);
    assert_eq!(cands[0].nargs, 3);
    assert_eq!(cands[0].ndargs, 1);
    assert_eq!(cands[0].argnumbers.as_ref().unwrap().as_slice(), &[0, 1, 2]);
    assert_eq!(cands[0].args.as_slice(), &[23, 25, 23]);

    // b unsupplied and undefaulted: no candidate.
    let cands = crate::FuncnameGetCandidatesExtended(
        ctx.mcx(),
        &["nf"],
        2,
        &["c"],
        true,
        true,
        false,
        false,
    )
    .unwrap();
    assert!(cands.is_empty());

    // Unknown parameter name: no candidate.
    let cands = crate::FuncnameGetCandidatesExtended(
        ctx.mcx(),
        &["nf"],
        3,
        &["z", "b"],
        true,
        true,
        false,
        false,
    )
    .unwrap();
    assert!(cands.is_empty());

    // A named argument colliding with a positional one: no candidate.
    let cands = crate::FuncnameGetCandidatesExtended(
        ctx.mcx(),
        &["nf"],
        3,
        &["a", "b"],
        true,
        true,
        false,
        false,
    )
    .unwrap();
    assert!(cands.is_empty());
}

#[test]
fn include_out_arguments_substitutes_proallargtypes() {
    install_fakes();
    install_proc_candidates();
    set_search_path("public");

    // outp(a int, OUT b text): pronargs 1 in proargtypes, 2 with OUT args.
    let ctx = MemoryContext::new("t");
    let cands = crate::FuncnameGetCandidatesExtended(
        ctx.mcx(),
        &["outp"],
        2,
        &[],
        false,
        false,
        true,
        false,
    )
    .unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].oid, 9021);
    assert_eq!(cands[0].nargs, 2);
    assert_eq!(cands[0].nominal_nargs, 2);
    assert_eq!(cands[0].args.as_slice(), &[23, 25]);
    assert!(cands[0].argnumbers.is_none());

    // Without include_out_arguments the OUT column is invisible.
    let cands = crate::FuncnameGetCandidatesExtended(
        ctx.mcx(),
        &["outp"],
        2,
        &[],
        false,
        false,
        false,
        false,
    )
    .unwrap();
    assert!(cands.is_empty());

    // Named notation against proallargtypes positions (b is an OUT arg).
    let cands = crate::FuncnameGetCandidatesExtended(
        ctx.mcx(),
        &["outp"],
        2,
        &["b"],
        false,
        false,
        true,
        false,
    )
    .unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].oid, 9021);
    assert_eq!(cands[0].argnumbers.as_ref().unwrap().as_slice(), &[0, 1]);
    assert_eq!(cands[0].args.as_slice(), &[23, 25]);
}
