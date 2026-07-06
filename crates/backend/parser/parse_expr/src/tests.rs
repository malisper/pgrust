use mcx::{Mcx, MemoryContext};
use parser_small1::{make_parsestate, ParseExprKind};
use types_core::catalog::INT4OID;
use types_core::InvalidOid;
use types_nodes::rawnodes::{A_Expr_Kind, ValUnion};
use types_nodes::{Integer, Node, NodeList, String as PgStr};

use crate::{expr_collation, expr_location, expr_type, transformExpr};

fn int_const<'mcx>(mcx: Mcx<'mcx>, ival: i32, location: i32) -> Node<'mcx> {
    Node::mk_a_const(mcx, Some(ValUnion::Integer(Integer { ival })), location).unwrap()
}

#[test]
fn a_const_transforms_to_const_and_restores_expr_kind() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_OTHER;

    let out = transformExpr(
        mcx,
        &mut pstate,
        int_const(mcx, 42, 7),
        ParseExprKind::EXPR_KIND_SELECT_TARGET,
    )
    .unwrap();

    assert_eq!(pstate.p_expr_kind, ParseExprKind::EXPR_KIND_OTHER);
    let c = out.as_const().unwrap();
    assert_eq!(c.consttype, INT4OID);
    assert_eq!(c.location, 7);
    assert_eq!(expr_type(out), INT4OID);
    assert_eq!(expr_collation(out), InvalidOid);
    assert_eq!(expr_location(out), 7);
}

#[test]
fn already_transformed_var_passes_through() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, InvalidOid, 0).unwrap();

    let out =
        transformExpr(mcx, &mut pstate, var, ParseExprKind::EXPR_KIND_SELECT_TARGET).unwrap();
    assert_eq!(expr_type(out), INT4OID);
    assert!(out.as_var().is_some());
}

#[test]
fn paramref_without_hook_is_42p02() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let pref = Node::mk_param_ref(mcx, 3, 7).unwrap();

    let err = transformExpr(mcx, &mut pstate, pref, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_PARAMETER);
}

fn install_oper_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        miscinit_seams::get_user_id::set(|| 10);
        // transformExpressionList seam (groupingsets' whole-fn design, which
        // superseded insert-lane F1's star probe): the rigs feed star-free
        // sources, so per-item transformExpr + SetToDefault passthrough is
        // the exact C path these tests pin.
        parse_func_seams::transformExpressionList::set(|mcx, pstate, exprlist, kind, allow_default| {
            let saved = pstate.p_expr_kind;
            pstate.p_expr_kind = kind;
            let mut out = types_nodes::NodeList::nil();
            for e in exprlist.iter() {
                if allow_default
                    && e.node_tag() == types_nodes::NodeTag::T_SetToDefault
                {
                    out.lappend(mcx, e)?;
                } else {
                    out.lappend(mcx, super::transformExprRecurse(mcx, pstate, e)?)?;
                }
            }
            pstate.p_expr_kind = saved;
            Ok(out)
        });
        syscache_seams::lookup_pg_operator_candidates::set(|mcx, name, l, r| {
            let mut v = mcx::vec_with_capacity_in(mcx, 1)?;
            if l == INT4OID && r == INT4OID {
                match name {
                    "+" => v.push((551, 11)),
                    "=" => v.push((96, 11)),
                    "<>" => v.push((518, 11)),
                    _ => {}
                }
            }
            Ok(v)
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            // 551 = int4pl (proc 177 -> int4); 96 = int4eq (proc 65 -> bool);
            // 518 = int4ne (proc 144 -> bool); pg_operator.dat/pg_proc.dat.
            Ok(match opno {
                551 => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                    oprleft: INT4OID,
                    oprright: INT4OID,
                    oprresult: INT4OID,
                    oprcom: 551,
                    oprnegate: InvalidOid,
                    oprcode: 177,
                    oprrest: InvalidOid,
                    oprjoin: InvalidOid,
                    oprcanmerge: false,
                    oprcanhash: false,
                }),
                96 => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                    oprleft: INT4OID,
                    oprright: INT4OID,
                    oprresult: types_core::catalog::BOOLOID,
                    oprcom: 96,
                    oprnegate: 518,
                    oprcode: 65,
                    oprrest: 101,
                    oprjoin: 105,
                    oprcanmerge: true,
                    oprcanhash: true,
                }),
                518 => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                    oprleft: INT4OID,
                    oprright: INT4OID,
                    oprresult: types_core::catalog::BOOLOID,
                    oprcom: 518,
                    oprnegate: 96,
                    oprcode: 144,
                    oprrest: 102,
                    oprjoin: 106,
                    oprcanmerge: false,
                    oprcanhash: false,
                }),
                _ => None,
            })
        });
        syscache_seams::pg_operator_name_candidates_exist::set(|name, _| {
            Ok(name == "+" || name == "=" || name == "<>")
        });
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok(matches!(funcid, 177 | 65 | 144).then_some(syscache_seams::PgProcShape {
                prolang: 12,
                prosecdef: false,
                proconfig_isnull: true,
                pronamespace: 11,
                prorettype: if funcid == 177 { INT4OID } else { types_core::catalog::BOOLOID },
                provariadic: InvalidOid,
                prosupport: InvalidOid,
                pronargs: 2,
                prokind: b'f' as i8,
                provolatile: b'i' as i8,
                proparallel: b's' as i8,
                proretset: false,
                proisstrict: true,
                proleakproof: false,
            }))
        });
        // 1007 = _int4.
        syscache_seams::pg_type_typarray::set(|typid| Ok((typid == INT4OID).then_some(1007)));
    });
}

#[test]
fn a_expr_in_transforms_to_scalar_array_op_expr() {
    install_typecast_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let in_aexpr = |op: &'static str| {
        let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: op }).unwrap()).unwrap();
        let mut items = NodeList::nil();
        items.lappend(mcx, int_const(mcx, 1, 28)).unwrap();
        items.lappend(mcx, int_const(mcx, 2, 31)).unwrap();
        Node::mk(
            mcx,
            types_nodes::rawnodes::A_Expr {
                kind: A_Expr_Kind::AEXPR_IN,
                name,
                lexpr: Some(int_const(mcx, 7, 20)),
                rexpr: Some(Node::mk_list(mcx, items).unwrap()),
                rexpr_list_start: 27,
                rexpr_list_end: 33,
                location: 22,
            },
        )
        .unwrap()
    };

    let out = transformExpr(mcx, &mut pstate, in_aexpr("="), ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .unwrap();
    let saop = out.as_scalar_array_op_expr().unwrap();
    assert!(saop.useOr);
    assert_eq!((saop.opno, saop.opfuncid), (96, 65));
    assert_eq!((saop.hashfuncid, saop.negfuncid), (InvalidOid, InvalidOid));
    assert_eq!(saop.location, 22);
    assert_eq!(saop.args.len(), 2);
    assert_eq!(saop.args.nth(0).as_const().unwrap().constvalue.as_i32(), 7);
    let arr = saop.args.nth(1).as_array_expr().unwrap();
    assert_eq!((arr.array_typeid, arr.element_typeid), (1007, INT4OID));
    assert!(!arr.multidims);
    assert_eq!((arr.list_start, arr.list_end, arr.location), (27, 33, -1));
    assert_eq!(arr.elements.len(), 2);
    assert_eq!(expr_type(out), types_core::catalog::BOOLOID);

    let out =
        transformExpr(mcx, &mut pstate, in_aexpr("<>"), ParseExprKind::EXPR_KIND_SELECT_TARGET)
            .unwrap();
    let saop = out.as_scalar_array_op_expr().unwrap();
    assert!(!saop.useOr);
    assert_eq!((saop.opno, saop.opfuncid), (518, 144));
}

#[test]
fn a_expr_op_transforms_to_op_expr() {
    install_oper_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "+" }).unwrap()).unwrap();
    let aexpr = Node::mk_a_expr(
        mcx,
        A_Expr_Kind::AEXPR_OP,
        name,
        Some(int_const(mcx, 1, 7)),
        Some(int_const(mcx, 1, 11)),
        9,
    )
    .unwrap();

    let out = transformExpr(mcx, &mut pstate, aexpr, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .unwrap();

    let op = out.as_op_expr().unwrap();
    assert_eq!((op.opno, op.opfuncid, op.opresulttype), (551, 177, INT4OID));
    assert!(!op.opretset);
    assert_eq!(op.args.len(), 2);
    assert_eq!(op.args.nth(0).as_const().unwrap().consttype, INT4OID);
    assert_eq!(op.location, 9);
    assert_eq!(expr_type(out), INT4OID);
    assert_eq!(expr_location(out), 7);
}

#[test]
fn a_expr_nullif_transforms_to_null_if_expr() {
    install_oper_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "=" }).unwrap()).unwrap();
    let aexpr = Node::mk_a_expr(
        mcx,
        A_Expr_Kind::AEXPR_NULLIF,
        name,
        Some(int_const(mcx, 1, 7)),
        Some(int_const(mcx, 2, 11)),
        9,
    )
    .unwrap();

    let out = transformExpr(mcx, &mut pstate, aexpr, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .unwrap();

    assert_eq!(out.node_tag(), types_nodes::NodeTag::T_NullIfExpr);
    let n = out.as_null_if_expr().unwrap();
    assert_eq!((n.opno, n.opfuncid, n.opresulttype), (96, 65, INT4OID));
    assert!(!n.opretset);
    assert_eq!(n.args.len(), 2);
    assert_eq!(n.location, 9);
    assert_eq!(expr_type(out), INT4OID);
}

#[test]
fn a_expr_nullif_non_boolean_op_errors() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "+" }).unwrap()).unwrap();
    let aexpr = Node::mk_a_expr(
        mcx,
        A_Expr_Kind::AEXPR_NULLIF,
        name,
        Some(int_const(mcx, 1, 7)),
        Some(int_const(mcx, 2, 11)),
        9,
    )
    .unwrap();

    let err = transformExpr(mcx, &mut pstate, aexpr, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DATATYPE_MISMATCH);
    assert_eq!(err.message(), "NULLIF requires = operator to yield boolean");
}

#[test]
fn expr_location_takes_leftmost_of_node_and_args() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "+" }).unwrap()).unwrap();
    let aexpr = Node::mk_a_expr(
        mcx,
        A_Expr_Kind::AEXPR_OP,
        name,
        Some(int_const(mcx, 1, 7)),
        Some(int_const(mcx, 1, 11)),
        9,
    )
    .unwrap();
    assert_eq!(expr_location(aexpr), 7);

    let noleft = Node::mk_a_expr(
        mcx,
        A_Expr_Kind::AEXPR_OP,
        NodeList::default(),
        None,
        Some(int_const(mcx, 1, 11)),
        9,
    )
    .unwrap();
    assert_eq!(expr_location(noleft), 9);
}

#[test]
fn transform_null_equals_guc_roundtrip() {
    crate::init_seams();
    assert!(!crate::transform_null_equals());
    guc_tables::vars::Transform_null_equals.write(true);
    assert!(guc_tables::vars::Transform_null_equals.read());
    guc_tables::vars::Transform_null_equals.write(false);
}

fn bool_const<'mcx>(mcx: Mcx<'mcx>, v: bool, location: i32) -> Node<'mcx> {
    Node::mk_a_const(
        mcx,
        Some(ValUnion::Boolean(types_nodes::Boolean { boolval: v })),
        location,
    )
    .unwrap()
}

#[test]
fn bool_expr_transforms_and_or_not() {
    use types_nodes::BoolExprType::*;
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    for (op, nargs) in [(AND_EXPR, 2), (OR_EXPR, 2), (NOT_EXPR, 1)] {
        let mut args = NodeList::nil();
        for i in 0..nargs {
            args.lappend(mcx, bool_const(mcx, i == 0, 7 + i)).unwrap();
        }
        let raw = Node::mk(mcx, types_nodes::BoolExpr { boolop: op, args, location: 3 }).unwrap();
        let out =
            transformExpr(mcx, &mut pstate, raw, ParseExprKind::EXPR_KIND_SELECT_TARGET).unwrap();
        let b = out.as_bool_expr().unwrap();
        assert_eq!(b.boolop, op);
        assert_eq!(b.args.len(), nargs as usize);
        assert_eq!(b.args.nth(0).as_const().unwrap().consttype, types_core::catalog::BOOLOID);
        assert_eq!(b.location, 3);
        assert_eq!(expr_type(out), types_core::catalog::BOOLOID);
        assert_eq!(expr_collation(out), InvalidOid);
        assert_eq!(expr_location(out), 3);
    }
}

fn install_typecast_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    install_oper_fixture();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_namespace_oid_by_name::set(|nspname| {
            Ok(if nspname == "pg_catalog" { 11 } else { InvalidOid })
        });
        aclchk_seams::object_aclcheck::set(|_, _, _, _| Ok(0));
        syscache_seams::lookup_pg_type_oid_by_name::set(|typname, nsp| {
            Ok(if typname == "int4" && nsp == 11 { INT4OID } else { InvalidOid })
        });
        syscache_seams::pg_type_isdefined::set(|_| Ok(Some(true)));
        syscache_seams::pg_type_typtype::set(|_| Ok(Some(b'b' as i8)));
        syscache_seams::pg_type_base_shape::set(|typid| {
            // 1007 = _int4; 6179 = array_subscript_handler.
            Ok(Some(syscache_seams::PgTypeBaseShape {
                typtype: b'b' as i8,
                typbasetype: InvalidOid,
                typtypmod: -1,
                typelem: if typid == 1007 { INT4OID } else { InvalidOid },
                typsubscript: if typid == 1007 { 6179 } else { InvalidOid },
            }))
        });
        syscache_seams::pg_type_io_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(syscache_seams::PgTypeIoShape {
                    oid: INT4OID,
                    typinput: 42,
                    typoutput: 43,
                    typreceive: 2406,
                    typsend: 2407,
                    typmodin: InvalidOid,
                    typmodout: InvalidOid,
                    typelem: InvalidOid,
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }),
                types_core::catalog::TEXTOID => Some(syscache_seams::PgTypeIoShape {
                    oid: types_core::catalog::TEXTOID,
                    typinput: 46,
                    typoutput: 47,
                    typreceive: 2414,
                    typsend: 2415,
                    typmodin: InvalidOid,
                    typmodout: InvalidOid,
                    typelem: InvalidOid,
                    typlen: -1,
                    typbyval: false,
                    typalign: b'i' as i8,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            let is_text = typid == types_core::catalog::TEXTOID;
            Ok(Some(types_tuple::PgTypeShape {
                typlen: if is_text { -1 } else { 4 },
                typbyval: !is_text,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typcollation: if is_text { 100 } else { InvalidOid },
            }))
        });
        syscache_seams::pg_type_category::set(|typid| {
            Ok(Some(match typid {
                types_core::catalog::TEXTOID => (b'S' as i8, true),
                types_core::catalog::BOOLOID => (b'B' as i8, true),
                _ => (b'N' as i8, false),
            }))
        });
    });
}

fn typecast_int4<'mcx>(mcx: Mcx<'mcx>, s: &'mcx str, arg_loc: i32, cast_loc: i32) -> Node<'mcx> {
    let mut names = NodeList::nil();
    names.lappend(mcx, Node::mk(mcx, PgStr { sval: "pg_catalog" }).unwrap()).unwrap();
    names.lappend(mcx, Node::mk(mcx, PgStr { sval: "int4" }).unwrap()).unwrap();
    let tn = Node::mk(
        mcx,
        types_nodes::TypeName {
            names,
            typemod: -1,
            location: cast_loc + 2,
            ..Default::default()
        },
    )
    .unwrap();
    let arg = Node::mk_a_const(
        mcx,
        Some(ValUnion::String(PgStr { sval: s })),
        arg_loc,
    )
    .unwrap();
    Node::mk(
        mcx,
        types_nodes::TypeCast { arg: Some(arg), typeName: Some(tn), location: cast_loc },
    )
    .unwrap()
}

#[test]
fn type_cast_of_string_literal_runs_input_function() {
    install_typecast_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(b"SELECT '42'::int4");

    let tc = typecast_int4(mcx, "42", 7, 11);
    let out =
        transformExpr(mcx, &mut pstate, tc, ParseExprKind::EXPR_KIND_SELECT_TARGET).unwrap();
    let c = out.as_const().unwrap();
    assert_eq!(c.consttype, INT4OID);
    assert!(!c.constisnull);
    assert_eq!(c.constvalue.as_i32(), 42);
    assert_eq!(c.location, 7);
}

#[test]
fn type_cast_bad_literal_is_22p02_with_cursor() {
    install_typecast_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(b"SELECT 'foo'::int4");

    let tc = typecast_int4(mcx, "foo", 7, 12);
    let err = transformExpr(mcx, &mut pstate, tc, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
    // C: errposition at the literal (location 7 -> 1-based char 8).
    assert_eq!(err.cursor_position(), Some(8));
}

#[test]
fn coalesce_selects_common_type_and_coerces_unknown() {
    install_typecast_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let mut args = NodeList::nil();
    args.lappend(mcx, Node::mk_a_const(mcx, None, 16).unwrap()).unwrap();
    args.lappend(mcx, int_const(mcx, 42, 22)).unwrap();
    let raw = Node::mk(
        mcx,
        types_nodes::primnodes::CoalesceExpr {
            coalescetype: InvalidOid,
            coalescecollid: InvalidOid,
            args,
            location: 7,
        },
    )
    .unwrap();

    let out =
        transformExpr(mcx, &mut pstate, raw, ParseExprKind::EXPR_KIND_SELECT_TARGET).unwrap();
    let c = out.as_coalesce_expr().unwrap();
    assert_eq!(c.coalescetype, INT4OID);
    assert_eq!(c.args.len(), 2);
    let null_arg = c.args.nth(0).as_const().unwrap();
    assert!(null_arg.constisnull);
    assert_eq!(null_arg.consttype, INT4OID);
    assert_eq!(c.args.nth(1).as_const().unwrap().constvalue.as_i32(), 42);
    assert_eq!(c.location, 7);
    assert_eq!(expr_type(out), INT4OID);
    assert_eq!(expr_location(out), 7);
}

#[test]
fn minmax_greatest_over_int4() {
    use types_nodes::primnodes::{MinMaxExpr, MinMaxOp};
    install_typecast_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let mut args = NodeList::nil();
    for (i, v) in [1, 2, 3].iter().enumerate() {
        args.lappend(mcx, int_const(mcx, *v, 16 + 3 * i as i32)).unwrap();
    }
    let raw = Node::mk(
        mcx,
        MinMaxExpr {
            minmaxtype: InvalidOid,
            minmaxcollid: InvalidOid,
            inputcollid: InvalidOid,
            op: MinMaxOp::IS_GREATEST,
            args,
            location: 7,
        },
    )
    .unwrap();

    let out =
        transformExpr(mcx, &mut pstate, raw, ParseExprKind::EXPR_KIND_SELECT_TARGET).unwrap();
    let m = out.as_min_max_expr().unwrap();
    assert_eq!(m.minmaxtype, INT4OID);
    assert_eq!(m.op, MinMaxOp::IS_GREATEST);
    assert_eq!(m.args.len(), 3);
    assert_eq!(expr_type(out), INT4OID);
}

#[test]
fn case_when_common_type_text_with_default_else() {
    use types_nodes::primnodes::{CaseExpr, CaseWhen};
    install_typecast_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let cond = Node::mk_a_const(
        mcx,
        Some(ValUnion::Boolean(types_nodes::Boolean { boolval: true })),
        12,
    )
    .unwrap();
    let result = Node::mk_a_const(mcx, Some(ValUnion::String(PgStr { sval: "lt" })), 22).unwrap();
    let when = Node::mk(
        mcx,
        CaseWhen { expr: Some(cond), result: Some(result), location: 12 },
    )
    .unwrap();
    let raw = Node::mk(
        mcx,
        CaseExpr {
            casetype: InvalidOid,
            casecollid: InvalidOid,
            arg: None,
            args: NodeList::make1(mcx, when).unwrap(),
            defresult: None,
            location: 7,
        },
    )
    .unwrap();

    let out =
        transformExpr(mcx, &mut pstate, raw, ParseExprKind::EXPR_KIND_SELECT_TARGET).unwrap();
    let c = out.as_case_expr().unwrap();
    assert_eq!(c.casetype, types_core::catalog::TEXTOID);
    assert!(c.arg.is_none());
    let w = c.args.nth(0).as_case_when().unwrap();
    assert_eq!(w.expr.unwrap().as_const().unwrap().consttype, types_core::catalog::BOOLOID);
    let r = w.result.unwrap().as_const().unwrap();
    assert_eq!(r.consttype, types_core::catalog::TEXTOID);
    assert_eq!(r.constcollid, 100);
    // C: absent ELSE becomes a NULL default, coerced to the common type.
    let d = c.defresult.unwrap().as_const().unwrap();
    assert!(d.constisnull);
    assert_eq!(d.consttype, types_core::catalog::TEXTOID);
    assert_eq!(expr_type(out), types_core::catalog::TEXTOID);
    assert_eq!(expr_location(out), 7);
}

#[test]
fn sql_value_function_transform_assigns_type() {
    use types_core::catalog::{DATEOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID};
    use types_nodes::primnodes::{SQLValueFunction, SQLValueFunctionOp as Op};

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    for (op, typmod, typ, want_typmod) in [
        (Op::SVFOP_CURRENT_DATE, -1, DATEOID, -1),
        (Op::SVFOP_CURRENT_TIME, -1, TIMETZOID, -1),
        (Op::SVFOP_CURRENT_TIME_N, 2, TIMETZOID, 2),
        (Op::SVFOP_CURRENT_TIMESTAMP, -1, TIMESTAMPTZOID, -1),
        (Op::SVFOP_CURRENT_TIMESTAMP_N, 3, TIMESTAMPTZOID, 3),
        (Op::SVFOP_LOCALTIME, -1, TIMEOID, -1),
        (Op::SVFOP_LOCALTIME_N, 6, TIMEOID, 6),
        (Op::SVFOP_LOCALTIMESTAMP, -1, TIMESTAMPOID, -1),
        (Op::SVFOP_LOCALTIMESTAMP_N, 0, TIMESTAMPOID, 0),
    ] {
        let raw =
            Node::mk(mcx, SQLValueFunction { op, r#type: 0, typmod, location: 7 }).unwrap();
        let out =
            transformExpr(mcx, &mut pstate, raw, ParseExprKind::EXPR_KIND_SELECT_TARGET)
                .unwrap();
        let svf = out.as_sql_value_function().unwrap();
        assert_eq!(svf.op, op);
        assert_eq!(svf.r#type, typ);
        assert_eq!(svf.typmod, want_typmod);
        assert_eq!(svf.location, 7);
        assert_eq!(expr_type(out), typ);
        assert_eq!(expr_collation(out), InvalidOid);
        assert_eq!(expr_location(out), 7);
        assert_eq!(crate::expr_typmod(out), want_typmod);
    }
}

#[test]
fn sql_value_function_negative_precision_is_22023() {
    use types_nodes::primnodes::{SQLValueFunction, SQLValueFunctionOp as Op};

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let raw = Node::mk(
        mcx,
        SQLValueFunction { op: Op::SVFOP_CURRENT_TIMESTAMP_N, r#type: 0, typmod: -2, location: 7 },
    )
    .unwrap();
    let err = transformExpr(mcx, &mut pstate, raw, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);
    assert!(err.message().contains("precision must not be negative"));
}

#[test]
fn nullif_transforms_to_null_if_expr_with_first_operand_type() {
    install_oper_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "=" }).unwrap()).unwrap();
    let aexpr = Node::mk_a_expr(
        mcx,
        A_Expr_Kind::AEXPR_NULLIF,
        name,
        Some(int_const(mcx, 1, 14)),
        Some(int_const(mcx, 2, 17)),
        7,
    )
    .unwrap();

    let out = transformExpr(mcx, &mut pstate, aexpr, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .unwrap();
    let n = out.as_null_if_expr().unwrap();
    assert_eq!((n.opno, n.opfuncid), (96, 65));
    // C retags the boolean OpExpr; the result type becomes the first operand's.
    assert_eq!(n.opresulttype, INT4OID);
    assert!(!n.opretset);
    assert_eq!(n.args.len(), 2);
    assert_eq!(n.location, 7);
    assert_eq!(expr_type(out), INT4OID);
}

#[test]
fn nullif_with_nonboolean_operator_is_42804() {
    install_oper_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let name = NodeList::make1(mcx, Node::mk(mcx, PgStr { sval: "+" }).unwrap()).unwrap();
    let aexpr = Node::mk_a_expr(
        mcx,
        A_Expr_Kind::AEXPR_NULLIF,
        name,
        Some(int_const(mcx, 1, 14)),
        Some(int_const(mcx, 2, 17)),
        7,
    )
    .unwrap();

    let err = transformExpr(mcx, &mut pstate, aexpr, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DATATYPE_MISMATCH);
    assert_eq!(err.message(), "NULLIF requires = operator to yield boolean");
}

fn row_expr<'mcx>(mcx: Mcx<'mcx>, args: NodeList<'mcx>, location: i32) -> Node<'mcx> {
    Node::mk(
        mcx,
        types_nodes::RowExpr {
            args,
            row_typeid: InvalidOid,
            row_format: types_nodes::CoercionForm::COERCE_EXPLICIT_CALL,
            colnames: NodeList::nil(),
            location,
        },
    )
    .unwrap()
}

fn multi_assign_ref<'mcx>(
    mcx: Mcx<'mcx>,
    source: Node<'mcx>,
    colno: i32,
    ncolumns: i32,
) -> Node<'mcx> {
    Node::mk(
        mcx,
        types_nodes::rawnodes::MultiAssignRef { source: Some(source), colno, ncolumns },
    )
    .unwrap()
}

#[test]
fn multi_assign_ref_row_expr_source_yields_columns_in_order() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let mut args = NodeList::nil();
    args.lappend(mcx, int_const(mcx, 11, 20)).unwrap();
    args.lappend(mcx, int_const(mcx, 22, 24)).unwrap();
    let row = row_expr(mcx, args, 18);

    let out1 = transformExpr(
        mcx,
        &mut pstate,
        multi_assign_ref(mcx, row, 1, 2),
        ParseExprKind::EXPR_KIND_UPDATE_SOURCE,
    )
    .unwrap();
    assert_eq!(out1.as_const().unwrap().constvalue.as_i32(), 11);
    // The transformed RowExpr parks in p_multiassign_exprs as a resjunk TLE.
    assert_eq!(pstate.p_multiassign_exprs.len(), 1);
    let tle = pstate.p_multiassign_exprs.nth(0).as_target_entry().unwrap();
    assert!(tle.resjunk);
    assert_eq!(tle.resno, 0);

    let out2 = transformExpr(
        mcx,
        &mut pstate,
        multi_assign_ref(mcx, row, 2, 2),
        ParseExprKind::EXPR_KIND_UPDATE_SOURCE,
    )
    .unwrap();
    assert_eq!(out2.as_const().unwrap().constvalue.as_i32(), 22);
    // The last column pops the RowExpr back off the list.
    assert_eq!(pstate.p_multiassign_exprs.len(), 0);
}

#[test]
fn multi_assign_ref_row_keeps_set_to_default_untransformed() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let mut args = NodeList::nil();
    args.lappend(mcx, Node::mk(mcx, types_nodes::SetToDefault::default()).unwrap()).unwrap();
    args.lappend(mcx, int_const(mcx, 5, 24)).unwrap();
    let row = row_expr(mcx, args, 18);

    let out = transformExpr(
        mcx,
        &mut pstate,
        multi_assign_ref(mcx, row, 1, 2),
        ParseExprKind::EXPR_KIND_UPDATE_SOURCE,
    )
    .unwrap();
    assert!(out.as_set_to_default().is_some());
}

#[test]
fn multi_assign_ref_column_count_mismatch_is_42601() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let mut args = NodeList::nil();
    args.lappend(mcx, int_const(mcx, 1, 20)).unwrap();
    let row = row_expr(mcx, args, 18);

    let err = transformExpr(
        mcx,
        &mut pstate,
        multi_assign_ref(mcx, row, 1, 2),
        ParseExprKind::EXPR_KIND_UPDATE_SOURCE,
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
    assert_eq!(err.message(), "number of columns does not match number of values");
}

#[test]
fn multi_assign_ref_bad_source_is_0a000() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let err = transformExpr(
        mcx,
        &mut pstate,
        multi_assign_ref(mcx, int_const(mcx, 1, 20), 1, 1),
        ParseExprKind::EXPR_KIND_UPDATE_SOURCE,
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(
        err.message(),
        "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression"
    );
}

fn install_sub_analyze_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        analyze_seams::parse_sub_analyze::set(|mcx, _tree, _pstate, _cte, _locked, _resolve| {
            let mut tl = NodeList::nil();
            for resno in 1..=2i16 {
                let var =
                    Node::mk_var(mcx, 1, resno, INT4OID, -1, InvalidOid, 0).unwrap();
                tl.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        types_nodes::TargetEntry {
                            expr: var,
                            resno,
                            resname: None,
                            ressortgroupref: 0,
                            resorigtbl: InvalidOid,
                            resorigcol: 0,
                            resjunk: false,
                        },
                    )
                    .unwrap(),
                )
                .unwrap();
            }
            Ok(types_nodes::parsenodes::Query {
                commandType: types_nodes::CmdType::CMD_SELECT,
                targetList: tl,
                ..Default::default()
            })
        });
    });
}

#[test]
fn multi_assign_ref_sublink_source_yields_multiexpr_params() {
    install_sub_analyze_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let sublink = |loc| {
        Node::mk(
            mcx,
            types_nodes::SubLink {
                subLinkType: types_nodes::SubLinkType::EXPR_SUBLINK,
                subLinkId: 0,
                testexpr: None,
                operName: NodeList::nil(),
                subselect: int_const(mcx, 0, loc),
                location: loc,
            },
        )
        .unwrap()
    };

    let out1 = transformExpr(
        mcx,
        &mut pstate,
        multi_assign_ref(mcx, sublink(18), 1, 2),
        ParseExprKind::EXPR_KIND_UPDATE_SOURCE,
    )
    .unwrap();
    let p1 = out1.as_param().unwrap();
    assert_eq!(p1.paramkind, types_nodes::ParamKind::PARAM_MULTIEXPR);
    assert_eq!(p1.paramid, (1 << 16) | 1);
    assert_eq!(p1.paramtype, INT4OID);
    // The relabeled MULTIEXPR SubLink parks in p_multiassign_exprs (subLinkId
    // = its 1-based position there) and stays after the last column.
    assert_eq!(pstate.p_multiassign_exprs.len(), 1);
    let parked = pstate.p_multiassign_exprs.nth(0).as_target_entry().unwrap();
    let sl = parked.expr.as_sub_link().unwrap();
    assert_eq!(sl.subLinkType, types_nodes::SubLinkType::MULTIEXPR_SUBLINK);
    assert_eq!(sl.subLinkId, 1);

    let out2 = transformExpr(
        mcx,
        &mut pstate,
        multi_assign_ref(mcx, sublink(18), 2, 2),
        ParseExprKind::EXPR_KIND_UPDATE_SOURCE,
    )
    .unwrap();
    assert_eq!(out2.as_param().unwrap().paramid, (1 << 16) | 2);
    assert_eq!(pstate.p_multiassign_exprs.len(), 1);
}

#[test]
fn multi_assign_ref_sublink_column_count_mismatch_is_42601() {
    install_sub_analyze_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let sublink = Node::mk(
        mcx,
        types_nodes::SubLink {
            subLinkType: types_nodes::SubLinkType::EXPR_SUBLINK,
            subLinkId: 0,
            testexpr: None,
            operName: NodeList::nil(),
            subselect: int_const(mcx, 0, 18),
            location: 18,
        },
    )
    .unwrap();

    let err = transformExpr(
        mcx,
        &mut pstate,
        multi_assign_ref(mcx, sublink, 1, 3),
        ParseExprKind::EXPR_KIND_UPDATE_SOURCE,
    )
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
    assert_eq!(err.message(), "number of columns does not match number of values");
}

fn column_ref<'mcx>(mcx: Mcx<'mcx>, names: &[&'mcx str], location: i32) -> Node<'mcx> {
    let mut fields = NodeList::nil();
    for n in names {
        fields.lappend(mcx, Node::mk(mcx, PgStr { sval: n }).unwrap()).unwrap();
    }
    Node::mk(mcx, types_nodes::rawnodes::ColumnRef { fields, location }).unwrap()
}

#[test]
fn column_ref_too_many_dotted_names_is_42601() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let cref = column_ref(mcx, &["a", "b", "c", "d", "e"], 7);
    let err = transformExpr(mcx, &mut pstate, cref, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
    assert_eq!(err.message(), "improper qualified name (too many dotted names): a.b.c.d.e");
}

#[test]
fn column_ref_wrong_catalog_is_0a000() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        dbcommands_seams::get_database_name::set(|_| Ok(Some("thisdb".to_string())));
        namespace_seams::range_var_get_relid::set(|_, _, _, _| Ok(InvalidOid));
    });
    install_typecast_fixture();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);

    let cref = column_ref(mcx, &["otherdb", "ns", "rel", "col"], 7);
    let err = transformExpr(mcx, &mut pstate, cref, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(
        err.message(),
        "cross-database references are not implemented: otherdb.ns.rel.col"
    );

    // Matching catalog name proceeds to (and fails) relation lookup instead.
    let cref = column_ref(mcx, &["thisdb", "ns", "rel", "col"], 7);
    let err = transformExpr(mcx, &mut pstate, cref, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .map(|_| ())
        .unwrap_err();
    assert_ne!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
}

#[test]
fn empty_array_without_cast_errors() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut pstate = make_parsestate(mcx, None);
    let a = Node::mk(
        mcx,
        types_nodes::rawnodes::A_ArrayExpr {
            elements: NodeList::nil(),
            list_start: 6,
            list_end: 7,
            location: 0,
        },
    )
    .unwrap();
    let err = transformExpr(mcx, &mut pstate, a, ParseExprKind::EXPR_KIND_SELECT_TARGET)
        .err()
        .expect("empty ARRAY[] must error");
    assert_eq!(err.message(), "cannot determine type of empty array");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INDETERMINATE_DATATYPE);
}
