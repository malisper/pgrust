use mcx::{Mcx, MemoryContext};
use parser_small1::{make_parsestate, ParseExprKind, ParseState};
use types_core::catalog::{INT2OID, INT4OID, INT8OID, NUMERICOID, VOIDOID};
use types_core::{InvalidOid, Oid};
use types_error::{ERRCODE_UNDEFINED_FUNCTION, ERRCODE_WRONG_OBJECT_TYPE};
use types_nodes::rawnodes::FuncCall;
use types_nodes::{Node, NodeList, String as PgStr};

use crate::ParseFuncOrColumn;

const ANYOID: Oid = 2276;
const PG_CATALOG: Oid = 11;

fn proc_candidate<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    args: &[Oid],
) -> syscache_seams::PgProcCandidate<'mcx> {
    let mut v = mcx::vec_with_capacity_in(mcx, args.len()).unwrap();
    for &a in args {
        v.push(a);
    }
    syscache_seams::PgProcCandidate {
        oid,
        pronamespace: PG_CATALOG,
        pronargs: args.len() as i16,
        pronargdefaults: 0,
        provariadic: InvalidOid,
        proargtypes: v,
    }
}

fn proc_shape(rettype: Oid, nargs: i16, prokind: u8) -> syscache_seams::PgProcShape {
    syscache_seams::PgProcShape {
        prolang: 12,
        prosecdef: false,
        proconfig_isnull: true,
        pronamespace: PG_CATALOG,
        prorettype: rettype,
        provariadic: InvalidOid,
        prosupport: InvalidOid,
        pronargs: nargs,
        prokind: prokind as i8,
        provolatile: b'i' as i8,
        proparallel: b's' as i8,
        proretset: false,
        proisstrict: false,
        proleakproof: false,
    }
}

fn agg_shape(transfn: Oid, transtype: Oid) -> syscache_seams::PgAggregateShape {
    syscache_seams::PgAggregateShape {
        aggkind: b'n' as i8,
        aggnumdirectargs: 0,
        aggtransfn: transfn,
        aggfinalfn: InvalidOid,
        aggcombinefn: 463,
        aggserialfn: InvalidOid,
        aggdeserialfn: InvalidOid,
        aggfinalextra: false,
        aggfinalmodify: b'r' as i8,
        aggsortop: 0,
        aggtranstype: transtype,
        aggmtransfn: 0,
        aggminvtransfn: 0,
        aggmfinalfn: 0,
        aggmfinalextra: false,
        aggmfinalmodify: b'r' as i8,
        aggmtranstype: 0,
        aggtransspace: 0,
    }
}

fn install_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        miscinit_seams::get_user_id::set(|| 10);
        syscache_seams::lookup_pg_type_oid_by_name::set(|_, _| Ok(InvalidOid));
        syscache_seams::lookup_pg_proc_name_candidates::set(|mcx, proname| {
            let mut v = mcx::PgVec::new_in(mcx);
            match proname {
                "count" => {
                    v.push(proc_candidate(mcx, 2147, &[ANYOID]));
                    v.push(proc_candidate(mcx, 2803, &[]));
                }
                "sum" => {
                    v.push(proc_candidate(mcx, 2107, &[INT8OID]));
                    v.push(proc_candidate(mcx, 2108, &[INT4OID]));
                    v.push(proc_candidate(mcx, 2109, &[INT2OID]));
                }
                "foo" => {
                    v.push(proc_candidate(mcx, 9999, &[]));
                }
                "nfunc" => {
                    v.push(proc_candidate(mcx, 8888, &[INT4OID]));
                }
                _ => {}
            }
            Ok(v)
        });
        syscache_seams::pg_proc_result_arrays::set(|mcx, funcid| {
            Ok((funcid == 8888).then(|| {
                let mut names = mcx::PgVec::new_in(mcx);
                names.push(mcx::PgString::from_str_in("x", mcx).unwrap());
                syscache_seams::PgProcResultArraysShape {
                    proallargtypes: None,
                    proargmodes: None,
                    proargnames: Some(names),
                }
            }))
        });
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok(match funcid {
                8888 => Some(proc_shape(INT4OID, 1, b'f')),
                2803 => Some(proc_shape(INT8OID, 0, b'a')),
                2147 => Some(proc_shape(INT8OID, 1, b'a')),
                2107 => Some(proc_shape(NUMERICOID, 1, b'a')),
                2108 => Some(proc_shape(INT8OID, 1, b'a')),
                2109 => Some(proc_shape(INT8OID, 1, b'a')),
                9999 => Some(proc_shape(INT4OID, 0, b'f')),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_aggregate_shape::set(|aggfnoid| {
            Ok(match aggfnoid {
                2803 => Some(agg_shape(1219, INT8OID)),
                2147 => Some(agg_shape(769, INT8OID)),
                2108 => Some(agg_shape(1841, INT8OID)),
                2109 => Some(agg_shape(1840, INT8OID)),
                _ => None,
            })
        });
    });
}

fn func_call<'mcx>(
    mcx: Mcx<'mcx>,
    name: &'static str,
    agg_star: bool,
    agg_distinct: bool,
) -> Node<'mcx> {
    let funcname = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: name }).unwrap()).unwrap();
    Node::mk(
        mcx,
        FuncCall {
            funcname,
            args: NodeList::nil(),
            agg_order: NodeList::nil(),
            agg_filter: None,
            over: None,
            agg_within_group: false,
            agg_star,
            agg_distinct,
            func_variadic: false,
            funcformat: types_nodes::CoercionForm::COERCE_EXPLICIT_CALL,
            location: 7,
        },
    )
    .unwrap()
}

fn call<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    fc_node: Node<'mcx>,
    fargs: NodeList<'mcx>,
    arg_types: &[Oid],
) -> types_error::PgResult<Node<'mcx>> {
    let fc = fc_node.as_func_call().unwrap();
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;
    ParseFuncOrColumn(
        mcx, pstate, &fc.funcname, fargs, arg_types, fc, None, None, false, false, fc.location,
    )
}

fn void_param<'mcx>(mcx: Mcx<'mcx>) -> Node<'mcx> {
    Node::mk(
        mcx,
        types_nodes::primnodes::Param {
            paramkind: types_nodes::primnodes::ParamKind::PARAM_EXTERN,
            paramid: 1,
            paramtype: VOIDOID,
            paramtypmod: -1,
            paramcollid: InvalidOid,
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn void_param_is_discarded_before_lookup() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let fc = func_call(mcx, "foo", false, false);
    let fargs = NodeList::make1(mcx, void_param(mcx)).unwrap();
    let node = call(mcx, &mut pstate, fc, fargs, &[VOIDOID]).unwrap();

    let f = node.as_func_expr().unwrap();
    assert_eq!(f.funcid, 9999);
    assert!(f.args.is_nil());
}

#[test]
fn void_const_is_not_discarded() {
    install_selection_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let fc = func_call(mcx, "foo", false, false);
    let c = Node::mk_const(mcx, VOIDOID, -1, InvalidOid, 4, datum::Datum::null(), true, true)
        .unwrap();
    let fargs = NodeList::make1(mcx, c).unwrap();
    let err = call(mcx, &mut pstate, fc, fargs, &[VOIDOID]).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);
}

#[test]
fn void_param_kept_for_column_syntax_leg() {
    install_selection_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let fc_node = func_call(mcx, "foo", false, false);
    let fc = fc_node.as_func_call().unwrap();
    let fargs = NodeList::make1(mcx, void_param(mcx)).unwrap();
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_SELECT_TARGET;
    let err = ParseFuncOrColumn(
        mcx, &mut pstate, &fc.funcname, fargs, &[VOIDOID], fc, None, None, false, true,
        fc.location,
    )
    .unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);
}

#[test]
fn count_star_builds_aggref() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let fc = func_call(mcx, "count", true, false);
    let node = call(mcx, &mut pstate, fc, NodeList::nil(), &[]).unwrap();

    let agg = node.as_aggref().unwrap();
    assert_eq!(agg.aggfnoid, 2803);
    assert_eq!(agg.aggtype, INT8OID);
    assert!(agg.aggstar);
    assert!(agg.args.is_nil());
    assert!(agg.aggargtypes.is_nil());
    assert_eq!(agg.aggkind, types_nodes::primnodes::AGGKIND_NORMAL);
    assert_eq!(agg.agglevelsup, 0);
    assert_eq!(agg.aggsplit, types_nodes::primnodes::AGGSPLIT_SIMPLE);
    assert_eq!((agg.aggno, agg.aggtransno), (-1, -1));
    assert_eq!(agg.location, 7);
    assert!(pstate.p_hasAggs.get());
}

#[test]
fn sum_of_int4_var_builds_aggref() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let fargs = NodeList::make1(mcx, var).unwrap();
    let fc = func_call(mcx, "sum", false, false);
    let node = call(mcx, &mut pstate, fc, fargs, &[INT4OID]).unwrap();

    let agg = node.as_aggref().unwrap();
    assert_eq!(agg.aggfnoid, 2108);
    assert_eq!(agg.aggtype, INT8OID);
    assert!(!agg.aggstar);
    assert_eq!(agg.args.len(), 1);
    let tle = agg.args.nth(0).as_target_entry().unwrap();
    assert_eq!(tle.resno, 1);
    assert!(!tle.resjunk);
    assert_eq!(tle.expr.as_var().unwrap().vartype, INT4OID);
    assert_eq!(agg.aggargtypes.len(), 1);
    assert_eq!(agg.aggargtypes.nth(0), INT4OID);
    assert!(pstate.p_hasAggs.get());
}

#[test]
fn count_of_int4_resolves_through_any() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let fargs = NodeList::make1(mcx, var).unwrap();
    let fc = func_call(mcx, "count", false, false);
    let node = call(mcx, &mut pstate, fc, fargs, &[INT4OID]).unwrap();

    let agg = node.as_aggref().unwrap();
    assert_eq!(agg.aggfnoid, 2147);
    assert_eq!(agg.aggtype, INT8OID);
    // ANY-target coercion passes the arg through; aggargtypes keeps int4.
    assert_eq!(agg.args.nth(0).as_target_entry().unwrap().expr.as_var().unwrap().vartype, INT4OID);
    assert_eq!(agg.aggargtypes.len(), 1);
    assert_eq!(agg.aggargtypes.nth(0), INT4OID);
}

#[test]
fn unknown_function_is_42883() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let fc = func_call(mcx, "nosuchfunc", false, false);
    let err = call(mcx, &mut pstate, fc, NodeList::nil(), &[]).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);
    assert!(err.message().contains("function nosuchfunc() does not exist"), "{}", err.message());
}

#[test]
fn parameterless_aggregate_without_star_is_42809() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let fc = func_call(mcx, "count", false, false);
    let err = call(mcx, &mut pstate, fc, NodeList::nil(), &[]).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_WRONG_OBJECT_TYPE);
    assert!(
        err.message()
            .contains("count(*) must be used to call a parameterless aggregate function"),
        "{}",
        err.message()
    );
}

#[test]
fn star_on_normal_function_is_42809() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let fc = func_call(mcx, "foo", true, false);
    let err = call(mcx, &mut pstate, fc, NodeList::nil(), &[]).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_WRONG_OBJECT_TYPE);
    assert!(
        err.message().contains("foo(*) specified, but foo is not an aggregate function"),
        "{}",
        err.message()
    );
}

#[test]
#[should_panic(expected = "seam not installed: parse_clause_seams::transform_agg_order_distinct")]
fn distinct_aggregate_panics() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let fargs = NodeList::make1(mcx, var).unwrap();
    let fc = func_call(mcx, "sum", false, true);
    let _ = call(mcx, &mut pstate, fc, fargs, &[INT4OID]);
}

#[test]
fn aggregate_in_where_kind_is_42803() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_WHERE;

    let fc_node = func_call(mcx, "count", true, false);
    let fc = fc_node.as_func_call().unwrap();
    let err =
        ParseFuncOrColumn(mcx, &mut pstate, &fc.funcname, NodeList::nil(), &[], fc, None, None, false, false, 7)
            .map(|_| ())
            .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_GROUPING_ERROR);
    assert!(
        err.message().contains("aggregate functions are not allowed in WHERE"),
        "{}",
        err.message()
    );
}

const FLOAT8OID: Oid = 701;
const TEXTOID: Oid = 25;
const VARBITOID: Oid = 1562;
const UNKNOWNOID: Oid = 705;
const ANYCOMPATIBLEOID: Oid = 5077;
const ANYCOMPATIBLEARRAYOID: Oid = 5078;

fn install_selection_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        install_fixture();
        syscache_seams::pg_type_category::set(|typid| {
            Ok(Some(match typid {
                INT4OID | INT8OID | INT2OID | NUMERICOID => (b'N' as i8, false),
                FLOAT8OID => (b'N' as i8, true),
                TEXTOID => (b'S' as i8, true),
                VARBITOID => (b'V' as i8, false),
                UNKNOWNOID => (b'X' as i8, false),
                ANYCOMPATIBLEOID | ANYCOMPATIBLEARRAYOID => (b'P' as i8, false),
                _ => return Ok(None),
            }))
        });
        syscache_seams::pg_type_base_shape::set(|_| {
            Ok(Some(syscache_seams::PgTypeBaseShape {
                typtype: b'b' as i8,
                typbasetype: InvalidOid,
                typtypmod: -1,
                typelem: InvalidOid,
                typsubscript: InvalidOid,
            }))
        });
        syscache_seams::pg_type_element_shape::set(|_| {
            Ok(Some(syscache_seams::PgTypeElementShape {
                typelem: InvalidOid,
                typsubscript: InvalidOid,
            }))
        });
        syscache_seams::pg_type_typrelid::set(|_| Ok(Some(InvalidOid)));
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            let mut typname = types_tuple::NameData::default();
            typname.namestrcpy(if typid == VOIDOID { "void" } else { "t" });
            Ok(Some(syscache_seams::PgTypeTypcacheShape {
                typname,
                typlen: 4,
                typbyval: true,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typtype: if typid == VOIDOID { b'p' as i8 } else { b'b' as i8 },
                typisdefined: true,
                typrelid: InvalidOid,
                typsubscript: InvalidOid,
                typelem: InvalidOid,
                typarray: InvalidOid,
                typcollation: InvalidOid,
            }))
        });
        namespace_seams::type_is_visible::set(|_| Ok(true));
        syscache_seams::lookup_pg_cast_shape::set(|src, tgt| {
            Ok(match (src, tgt) {
                (INT4OID, FLOAT8OID) | (INT4OID, NUMERICOID) => {
                    Some(syscache_seams::PgCastShape {
                        oid: 1,
                        castfunc: 2,
                        castcontext: b'i' as i8,
                        castmethod: b'f' as i8,
                    })
                }
                _ => None,
            })
        });
    });
}

// pg_operator.dat: 965 = float8^float8 (dpow), 1038 = numeric^numeric.
#[test]
fn power_int4_int4_selects_preferred_float8() {
    install_selection_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let cands = [
        catalog_namespace::OperCandidate { oid: 1038, args: [NUMERICOID, NUMERICOID] },
        catalog_namespace::OperCandidate { oid: 965, args: [FLOAT8OID, FLOAT8OID] },
    ];
    let input = [INT4OID, INT4OID];
    let matched = crate::func_match_argtypes(mcx, &input, &cands).unwrap();
    assert_eq!(matched.len(), 2);
    let winner = crate::func_select_candidate(&input, matched).unwrap().unwrap();
    assert_eq!(winner.oid, 965);
}

// pg_operator.dat: 654 = text||text (textcat); STRING wins the unknown slots
// and text is the preferred string type.
#[test]
fn concat_unknown_unknown_selects_textcat() {
    install_selection_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let cands = [
        catalog_namespace::OperCandidate {
            oid: 349,
            args: [ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEOID],
        },
        catalog_namespace::OperCandidate { oid: 1797, args: [VARBITOID, VARBITOID] },
        catalog_namespace::OperCandidate { oid: 654, args: [TEXTOID, TEXTOID] },
    ];
    let input = [UNKNOWNOID, UNKNOWNOID];
    let matched = crate::func_match_argtypes(mcx, &input, &cands).unwrap();
    assert_eq!(matched.len(), 3);
    let winner = crate::func_select_candidate(&input, matched).unwrap().unwrap();
    assert_eq!(winner.oid, 654);
}

// Same-known-type last-gasp heuristic: sum(int4-domain-free unknown mix) is
// ambiguous, but int8+unknown resolves once the known type is unique.
#[test]
fn all_unknown_same_category_nonpreferred_is_ambiguous() {
    install_selection_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let cands = [
        catalog_namespace::OperCandidate { oid: 11, args: [INT8OID, INT8OID] },
        catalog_namespace::OperCandidate { oid: 12, args: [INT4OID, INT4OID] },
    ];
    let input = [UNKNOWNOID, UNKNOWNOID];
    let matched = crate::func_match_argtypes(mcx, &input, &cands).unwrap();
    assert_eq!(matched.len(), 2);
    assert!(crate::func_select_candidate(&input, matched).unwrap().is_none());
}

fn named_arg<'mcx>(mcx: Mcx<'mcx>, name: &'static str, arg: Node<'mcx>, loc: i32) -> Node<'mcx> {
    Node::mk(
        mcx,
        types_nodes::NamedArgExpr { arg: Some(arg), name: Some(name), argnumber: -1, location: loc },
    )
    .unwrap()
}

#[test]
fn named_argument_resolves_and_numbers() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let fargs = NodeList::make1(mcx, named_arg(mcx, "x", var, 13)).unwrap();
    let fc = func_call(mcx, "nfunc", false, false);
    let node = call(mcx, &mut pstate, fc, fargs, &[INT4OID]).unwrap();

    let f = node.as_func_expr().unwrap();
    assert_eq!(f.funcid, 8888);
    assert_eq!(f.funcresulttype, INT4OID);
    assert_eq!(f.args.len(), 1);
    let na = f.args.nth(0).as_named_arg_expr().unwrap();
    assert_eq!(na.name, Some("x"));
    assert_eq!(na.argnumber, 0);
    assert_eq!(na.location, 13);
    assert_eq!(na.arg.expect("NamedArgExpr has an arg").as_var().unwrap().vartype, INT4OID);
}

#[test]
fn duplicate_argument_name_is_42601() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let v1 = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let v2 = Node::mk_var(mcx, 1, 2, INT4OID, -1, InvalidOid, 0).unwrap();
    let mut fargs = NodeList::make1(mcx, named_arg(mcx, "x", v1, 13)).unwrap();
    fargs.lappend(mcx, named_arg(mcx, "x", v2, 21)).unwrap();
    let fc = func_call(mcx, "nfunc", false, false);
    let err =
        call(mcx, &mut pstate, fc, fargs, &[INT4OID, INT4OID]).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
    assert!(
        err.message().contains("argument name \"x\" used more than once"),
        "{}",
        err.message()
    );
}

#[test]
fn positional_after_named_is_42601() {
    install_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let v1 = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();
    let v2 = Node::mk_var(mcx, 1, 2, INT4OID, -1, InvalidOid, 0).unwrap();
    let mut fargs = NodeList::make1(mcx, named_arg(mcx, "x", v1, 13)).unwrap();
    fargs.lappend(mcx, v2).unwrap();
    let fc = func_call(mcx, "nfunc", false, false);
    let err =
        call(mcx, &mut pstate, fc, fargs, &[INT4OID, INT4OID]).map(|_| ()).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
    assert!(
        err.message().contains("positional argument cannot follow named argument"),
        "{}",
        err.message()
    );
}

// C ParseComplexProjection RECORD-Var leg: (record_var).field resolves via
// expandRecordVariable drilling to the defining subquery.
mod complex_projection_record {
    use super::*;
    use types_core::catalog::{DEFAULT_COLLATION_OID, RECORDOID, TEXTOID};
    use types_nodes::{NodeList, RTEKind};

    fn install_record_fixture() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            parse_func_seams::expandRecordVariable::set(parse_target::expandRecordVariable);
            syscache_seams::lookup_pg_type_shape::set(|typid| {
                Ok(Some(types_tuple::PgTypeShape {
                    typlen: if typid == TEXTOID { -1 } else { 4 },
                    typbyval: typid != TEXTOID,
                    typalign: b'i' as i8,
                    typstorage: b'p' as i8,
                    typcollation: if typid == TEXTOID { DEFAULT_COLLATION_OID } else { InvalidOid },
                }))
            });
        });
    }

    fn record_var<'mcx>(mcx: Mcx<'mcx>, varno: i32, varattno: i16) -> Node<'mcx> {
        Node::mk(
            mcx,
            types_nodes::Var {
                varno,
                varattno,
                vartype: RECORDOID,
                vartypmod: -1,
                varcollid: InvalidOid,
                varnullingrels: types_nodes::Bitmapset::empty(),
                varlevelsup: 0,
                varreturningtype: types_nodes::VarReturningType::VAR_RETURNING_DEFAULT,
                varnosyn: varno as types_core::Index,
                varattnosyn: varattno,
                location: -1,
            },
        )
        .unwrap()
    }

    fn subquery_rte<'mcx>(
        mcx: Mcx<'mcx>,
        aliasname: &'mcx str,
        tlist: NodeList<'mcx>,
        colnames: NodeList<'mcx>,
        rtable: NodeList<'mcx>,
    ) -> Node<'mcx> {
        let q = mcx::leak_in(
            mcx::alloc_in(
                mcx,
                types_nodes::parsenodes::Query { targetList: tlist, rtable, ..Default::default() },
            )
            .unwrap(),
        );
        let eref = Node::mk_mut(mcx, types_nodes::Alias { aliasname: Some(aliasname), colnames })
            .unwrap()
            .seal_ref();
        Node::mk(
            mcx,
            types_nodes::RangeTblEntry {
                rtekind: RTEKind::RTE_SUBQUERY,
                subquery: Some(q),
                eref: Some(eref),
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn record_var_field_projection() {
        install_record_fixture();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut pstate = make_parsestate(mcx, None);

        let c1 =
            Node::mk_const(mcx, INT4OID, -1, InvalidOid, 4, datum::Datum::null(), true, true)
                .unwrap();
        let c2 = Node::mk_const(
            mcx,
            TEXTOID,
            -1,
            DEFAULT_COLLATION_OID,
            -1,
            datum::Datum::null(),
            true,
            false,
        )
        .unwrap();
        let inner_tlist = NodeList::make2(
            mcx,
            Node::mk_target_entry(mcx, c1, 1, Some("a"), false).unwrap(),
            Node::mk_target_entry(mcx, c2, 2, Some("b"), false).unwrap(),
        )
        .unwrap();
        let inner_names = NodeList::make2(
            mcx,
            Node::mk_string(mcx, "a").unwrap(),
            Node::mk_string(mcx, "b").unwrap(),
        )
        .unwrap();
        let inner = subquery_rte(mcx, "inner_ss", inner_tlist, inner_names, NodeList::nil());

        let outer_tlist = NodeList::make1(
            mcx,
            Node::mk_target_entry(mcx, record_var(mcx, 1, 0), 1, Some("r"), false).unwrap(),
        )
        .unwrap();
        let outer_names = NodeList::make1(mcx, Node::mk_string(mcx, "r").unwrap()).unwrap();
        let outer = subquery_rte(
            mcx,
            "outer_ss",
            outer_tlist,
            outer_names,
            NodeList::make1(mcx, inner).unwrap(),
        );
        pstate.p_rtable.lappend(mcx, outer).unwrap();

        let first_arg = record_var(mcx, 1, 1);
        let got =
            crate::ParseComplexProjection(mcx, &mut pstate, "b", first_arg, -1).unwrap();
        let fs = got.expect("column b resolves").as_field_select().unwrap();
        assert_eq!(fs.fieldnum, 2);
        assert_eq!(fs.resulttype, TEXTOID);
        assert_eq!(fs.resultcollid, DEFAULT_COLLATION_OID);

        let none =
            crate::ParseComplexProjection(mcx, &mut pstate, "nosuch", first_arg, -1).unwrap();
        assert!(none.is_none());
    }

    // C parse_func.c:592-599 — a call `b(record_var)` where `b` is not a
    // function is retried as `(record_var).b` (misc.sql's name(hobbies_r)).
    #[test]
    fn func_call_notation_falls_back_to_field_projection() {
        install_fixture();
        install_record_fixture();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut pstate = make_parsestate(mcx, None);

        let c1 =
            Node::mk_const(mcx, INT4OID, -1, InvalidOid, 4, datum::Datum::null(), true, true)
                .unwrap();
        let c2 = Node::mk_const(
            mcx,
            TEXTOID,
            -1,
            DEFAULT_COLLATION_OID,
            -1,
            datum::Datum::null(),
            true,
            false,
        )
        .unwrap();
        let inner_tlist = NodeList::make2(
            mcx,
            Node::mk_target_entry(mcx, c1, 1, Some("a"), false).unwrap(),
            Node::mk_target_entry(mcx, c2, 2, Some("b"), false).unwrap(),
        )
        .unwrap();
        let inner_names = NodeList::make2(
            mcx,
            Node::mk_string(mcx, "a").unwrap(),
            Node::mk_string(mcx, "b").unwrap(),
        )
        .unwrap();
        let inner = subquery_rte(mcx, "inner_ss", inner_tlist, inner_names, NodeList::nil());

        let outer_tlist = NodeList::make1(
            mcx,
            Node::mk_target_entry(mcx, record_var(mcx, 1, 0), 1, Some("r"), false).unwrap(),
        )
        .unwrap();
        let outer_names = NodeList::make1(mcx, Node::mk_string(mcx, "r").unwrap()).unwrap();
        let outer = subquery_rte(
            mcx,
            "outer_ss",
            outer_tlist,
            outer_names,
            NodeList::make1(mcx, inner).unwrap(),
        );
        pstate.p_rtable.lappend(mcx, outer).unwrap();

        let first_arg = record_var(mcx, 1, 1);
        let fargs = NodeList::make1(mcx, first_arg).unwrap();
        let fc = crate::tests::func_call(mcx, "b", false, false);
        let node = crate::tests::call(mcx, &mut pstate, fc, fargs, &[RECORDOID]).unwrap();

        let fs = node.as_field_select().unwrap();
        assert_eq!(fs.fieldnum, 2);
        assert_eq!(fs.resulttype, TEXTOID);
    }
}
