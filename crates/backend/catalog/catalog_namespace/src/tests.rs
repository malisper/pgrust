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
    static BOOTSTRAP: Cell<bool> = const { Cell::new(false) };
    static USER: Cell<Oid> = const { Cell::new(USER_A) };
    static ACL_DENIED: RefCell<Vec<Oid>> = const { RefCell::new(Vec::new()) };
    // InitTempTableNamespace's RecoveryInProgress()/IsParallelWorker() arms.
    static IN_RECOVERY: Cell<bool> = const { Cell::new(false) };
    static PARALLEL_WORKER: Cell<bool> = const { Cell::new(false) };
}

fn install_fakes() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        miscinit_seams::get_user_id::set(|| USER.with(Cell::get));
        miscinit_seams::is_bootstrap_processing_mode::set(|| BOOTSTRAP.get());
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
        transam_xlog_seams::recovery_in_progress::set(|| IN_RECOVERY.with(Cell::get));
        parallel_seams::is_parallel_worker::set(|| PARALLEL_WORKER.with(Cell::get));
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
    BOOTSTRAP.set(false);
    USER.with(|u| u.set(USER_A));
    IN_RECOVERY.with(|c| c.set(false));
    PARALLEL_WORKER.with(|c| c.set(false));
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

// Regression: a session-state swap (as a pooled worker thread does between
// tasks/sessions) must NOT rewind ACTIVE_PATH_GENERATION. If it did, the same
// generation value could denote two different search-path states on one thread,
// letting a matcher captured under session A falsely fast-path-match session B
// and execute A's name->OID resolutions under B (cache poisoning). C keeps
// activePathGeneration strictly monotonic per backend.
#[test]
fn session_swap_does_not_rewind_generation() {
    install_fakes();
    let ctx = MemoryContext::new("test");

    // Parked baseline captured at the current generation, as a pooled helper
    // does at bind time. Prime the derived path so the residual base path is a
    // known value ([pg_catalog, public]) and later swaps deterministically flip
    // it, forcing the generation to advance.
    set_search_path("public");
    let _ = GetSearchPathMatcher(ctx.mcx()).unwrap();
    let baseline = CaptureSessionNamespaceState();

    // Session A runs with search_path resolving to s1 and captures a matcher.
    set_search_path("s1");
    let matcher_a = GetSearchPathMatcher(ctx.mcx()).unwrap();
    assert_eq!(matcher_a.schemas.as_slice(), &[NS_S1]);

    // Task A exits: restore the parked baseline. This previously rewound the
    // generation counter.
    ReplaceSessionNamespaceState(&baseline);

    // Session B arrives on the same thread with a DIFFERENT search path.
    set_search_path("public");

    // A's stale matcher (schemas=[s1]) must not match B's environment
    // (schemas=[public]). With the generation rewind bug this returned true via
    // the generation-equality fast path; with the monotonic counter the
    // generations differ and the full schema comparison correctly rejects it.
    let mut stale = CopySearchPathMatcher(ctx.mcx(), &matcher_a).unwrap();
    assert!(!SearchPathMatchesCurrentEnvironment(&mut stale).unwrap());

    // And B's own generation must be strictly newer than A's, never a reused
    // value.
    let gen_b = GetSearchPathMatcher(ctx.mcx()).unwrap().generation;
    assert!(gen_b > matcher_a.generation);
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

    let err = RangeVarGetRelid(&rv(None, "gone"), 1, false).err().unwrap();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_TABLE);
    assert!(err.message().contains("relation \"gone\" does not exist"));

    let err = RangeVarGetRelid(&rv(Some("no_such"), "t1"), 1, false).err().unwrap();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_SCHEMA);

    assert_eq!(
        RangeVarGetRelid(&rv(Some("no_such"), "t1"), 1, true).unwrap(),
        InvalidOid
    );
}

// A non-UTF-8 part resolves exactly like a missing name at C's lookup step.
#[test]
fn range_var_lookups_from_name_bytes() {
    install_fakes();
    set_search_path("public");
    dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".into())));

    assert_eq!(RangeVarGetRelidFromNameBytes(&[b"t1"], 1, false).unwrap(), REL_T1);
    assert_eq!(RangeVarGetRelidFromNameBytes(&[b"\xE9abc"], 1, true).unwrap(), InvalidOid);

    let err = RangeVarGetRelidFromNameBytes(&[b"\xE9abc"], 1, false).err().unwrap();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_TABLE);
    assert_eq!(err.message_raw.as_deref(), Some(b"relation \"\xE9abc\" does not exist".as_slice()));

    let err = RangeVarGetRelidFromNameBytes(&[b"public", b"\xE9"], 1, false).err().unwrap();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_TABLE);
    assert_eq!(err.message_raw.as_deref(), Some(b"relation \"public.\xE9\" does not exist".as_slice()));

    let err = RangeVarGetRelidFromNameBytes(&[b"no_such", b"\xE9"], 1, false).err().unwrap();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_SCHEMA);

    let err = RangeVarGetRelidFromNameBytes(&[b"\xE9", b"t1"], 1, false).err().unwrap();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_SCHEMA);
    assert_eq!(err.message_raw.as_deref(), Some(b"schema \"\xE9\" does not exist".as_slice()));
    assert_eq!(RangeVarGetRelidFromNameBytes(&[b"\xE9", b"t1"], 1, true).unwrap(), InvalidOid);

    let err = RangeVarGetRelidFromNameBytes(&[b"a", b"b", b"c", b"\xE9"], 1, false)
        .err()
        .unwrap();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
    assert_eq!(
        err.message_raw.as_deref(),
        Some(b"improper relation name (too many dotted names): a.b.c.\xE9".as_slice())
    );

    for missing_ok in [false, true] {
        let err = RangeVarGetRelidFromNameBytes(&[b"other", b"\xE9", b"t1"], 1, missing_ok)
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message_raw.as_deref(),
            Some(b"cross-database references are not implemented: \"other.\xE9.t1\"".as_slice()));
    }
    let err = RangeVarGetRelidFromNameBytes(&[b"testdb", b"public", b"\xE9"], 1, false)
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_TABLE);
    assert_eq!(err.message_raw.as_deref(),
        Some(b"relation \"public.\xE9\" does not exist".as_slice()));
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
    assert!(denied.err().unwrap().message().contains("permission denied"));
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

// namespace.c:4428-4432: a hot-standby session never creates its temp
// namespace (ERRCODE_READ_ONLY_SQL_TRANSACTION), checked right after the
// database ACL and before any pg_temp_N lookup.
#[test]
fn temp_namespace_refused_during_recovery() {
    install_fakes();
    IN_RECOVERY.with(|c| c.set(true));
    let ctx = MemoryContext::new("t");
    let err = GetTempTableNamespace(ctx.mcx())
        .err()
        .expect("InitTempTableNamespace must refuse during recovery");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_READ_ONLY_SQL_TRANSACTION);
    assert_eq!(err.message(), "cannot create temporary tables during recovery");
    assert_eq!(my_temp_namespace(), InvalidOid);
}

// namespace.c:4434-4438: a parallel worker never creates its temp namespace
// either (same SQLSTATE, its own message); reached from SQL by a PARALLEL
// SAFE function calling current_schema() with pg_temp first in search_path.
#[test]
fn temp_namespace_refused_in_parallel_worker() {
    install_fakes();
    PARALLEL_WORKER.with(|c| c.set(true));
    let ctx = MemoryContext::new("t");
    let err = GetTempTableNamespace(ctx.mcx())
        .err()
        .expect("InitTempTableNamespace must refuse in a parallel worker");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_READ_ONLY_SQL_TRANSACTION);
    assert_eq!(
        err.message(),
        "cannot create temporary tables during a parallel operation"
    );
    assert_eq!(my_temp_namespace(), InvalidOid);
}

// namespace.c:1225-1295 reads proallargtypes/proargmodes/proargnames off the
// pinned candidate tuple; the Rust re-probe of PROCOID can only miss if the
// row vanished, and a miss is C's elog(ERROR, "cache lookup failed for
// function %u") — never a server panic.
#[test]
fn funcname_candidates_arrays_miss_is_an_error_not_a_panic() {
    install_fakes();
    install_proc_candidates();
    set_search_path("public");

    let ctx = MemoryContext::new("t");
    // Named notation forces the array re-probe; the fake holds no arrays for
    // 9001 (the PROCOID miss).
    let err = crate::FuncnameGetCandidatesExtended(
        ctx.mcx(),
        &["f"],
        1,
        &["a"],
        true,
        true,
        false,
        false,
    )
    .err()
    .expect("a vanished pg_proc row must be reported as an error");
    assert_eq!(err.message(), "cache lookup failed for function 9001");
}

#[test]
fn session_namespace_noninitializing_reads_and_borrowed_teardown() {
    clear_path_state();
    assert_eq!(base_path_len(), 0);
    PATH.with(|slot| assert!(slot.borrow().is_none()));
    with_path_state_mut(|st| {
        st.base_search_path = mcx::slice_in(st.mcx, &[NS_PUBLIC])?;
        let value = mcx::alloc_leak_in(st.mcx, [7u8; 16384]).unwrap();
        assert!(std::panic::catch_unwind(clear_path_state).is_err());
        assert_eq!(value[16383], 7);
        Ok(())
    }).unwrap();
    assert!(std::panic::catch_unwind(|| with_path(|_| clear_path_state())).is_err());
    assert_eq!(base_path_nth(0), NS_PUBLIC);
    clear_path_state();
}

#[test]
fn session_namespace_cleanup_rebuilds_worker_path_and_matcher() {
    install_fakes();
    clear_path_state();
    MY_TEMP_NAMESPACE.set(InvalidOid);
    MY_TEMP_TOAST_NAMESPACE.set(InvalidOid);
    MY_TEMP_NAMESPACE_SUB_ID.set(InvalidSubTransactionId);
    let ctx = MemoryContext::new("namespace caller");
    set_search_path("public");
    let copied = fetch_search_path(ctx.mcx(), true).unwrap();
    let mut matcher = GetSearchPathMatcher(ctx.mcx()).unwrap();
    let old_generation = matcher.generation;
    clear_path_state();
    assert!(!BASE_SEARCH_PATH_VALID.get());
    assert!(ACTIVE_PATH_GENERATION.get() > old_generation);
    assert_eq!(copied.as_slice(), &[PG_CATALOG_NAMESPACE, NS_PUBLIC]);
    set_search_path("s1");
    ResetTempNamespaceStateForRetainedPark();
    assert!(!SearchPathMatchesCurrentEnvironment(&mut matcher).unwrap());
    assert_eq!(fetch_search_path(ctx.mcx(), false).unwrap().as_slice(), &[NS_S1]);
    clear_path_state();
    BOOTSTRAP.set(true);
    InitializeSearchPath().unwrap();
    assert_eq!(base_path_nth(0), PG_CATALOG_NAMESPACE);
    assert!(BASE_SEARCH_PATH_VALID.get());
    BOOTSTRAP.set(false);
    clear_path_state();
}

#[test]
fn session_namespace_error_unwind_keeps_owner_valid() {
    clear_path_state();
    let result = with_path_state_mut(|st| -> PgResult<()> {
        st.base_search_path = mcx::slice_in(st.mcx, &[NS_PUBLIC])?;
        Err(Box::new(types_error::PgError::error("test unwind")))
    });
    assert!(result.is_err());
    assert_eq!(base_path_nth(0), NS_PUBLIC);
    clear_path_state();
    let result = mcx::McxOwned::<PathStateTy>::try_new(
        MemoryContext::new("namespace failed construction").with_limit(1),
        |mcx| Ok(PathState { mcx, base_search_path: PgVec::new_in(mcx) }),
    );
    assert!(result.is_err());
    assert_eq!(base_path_len(), 0);
}

#[test]
#[ignore = "process-global accounting; run alone with --test-threads=1"]
fn session_namespace_reclaims_complete_context() {
    clear_path_state();
    let before = mcx::global_footprint::bytes();
    for _ in 0..64 {
        with_path_state_mut(|st| {
            st.base_search_path = mcx::slice_in(st.mcx, &[NS_PUBLIC; 8192])?;
            Ok(())
        }).unwrap();
        clear_path_state();
        assert_eq!(mcx::global_footprint::bytes(), before);
    }
}

#[test]
#[ignore = "installs process-global cleanup sink; run alone with --test-threads=1"]
fn session_namespace_registered_cleanup_reinitializes() {
    thread_local! {
        static CLEANUPS: RefCell<Vec<Box<dyn FnOnce()>>> = const { RefCell::new(Vec::new()) };
    }
    fn record(_phase: mcx::SessionCleanupPhase, cleanup: Box<dyn FnOnce()>) {
        CLEANUPS.with(|slot| slot.borrow_mut().push(cleanup));
    }
    clear_path_state();
    mcx::set_session_cleanup_sink(record);
    for _ in 0..3 {
        with_path_state_mut(|st| {
            st.base_search_path = mcx::slice_in(st.mcx, &[NS_PUBLIC])?;
            Ok(())
        }).unwrap();
        BASE_SEARCH_PATH_VALID.set(true);
        let callbacks = CLEANUPS.with(|slot| std::mem::take(&mut *slot.borrow_mut()));
        assert_eq!(callbacks.len(), 1);
        for callback in callbacks { callback(); }
        assert!(!BASE_SEARCH_PATH_VALID.get());
        assert_eq!(base_path_len(), 0);
        PATH.with(|slot| assert!(slot.borrow().is_none()));
    }
    mcx::set_session_cleanup_sink(|_, _| {});
}
