//! copyfuncs.c copyObject for the utility statements standard_ProcessUtility
//! must copy when readOnlyTree (plancache-held trees; utility.c:613). Direct
//! arms cover the statements that flow through cached plans; embedded
//! expression trees round-trip through outfuncs/readfuncs (C-equivalent,
//! slower — this path is per-cached-utility-execution, never per-row). Every
//! other statement tag is a loud panic naming the missing arm.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::parsenodes::{DefElem, ExecuteStmt, TransactionStmt, VariableSetStmt};
use types_nodes::plannodes::PlannedStmt;
use types_nodes::primnodes::FuncExpr;
use types_nodes::rawnodes::{
    A_Const, A_Expr, A_Star, ColumnRef, FuncCall, ParamRef, TypeCast, TypeName, ValUnion,
};
use types_nodes::{Node, NodeList, NodeTag};

pub fn copy_utility_planned_stmt<'d>(
    mcx: Mcx<'d>,
    src: &PlannedStmt<'_>,
) -> PgResult<&'d PlannedStmt<'d>> {
    debug_assert!(src.planTree.is_none());
    let utility_stmt = src.utilityStmt.expect("utility PlannedStmt holds utilityStmt");
    let copy = PlannedStmt {
        commandType: src.commandType,
        queryId: src.queryId,
        planId: src.planId,
        canSetTag: src.canSetTag,
        utilityStmt: Some(copy_stmt(mcx, utility_stmt)?),
        stmt_location: src.stmt_location,
        stmt_len: src.stmt_len,
        ..PlannedStmt::default()
    };
    Ok(Node::mk(mcx, copy)?.as_planned_stmt().expect("PlannedStmt"))
}

fn copy_stmt<'d>(mcx: Mcx<'d>, node: Node<'_>) -> PgResult<Node<'d>> {
    match node.node_tag() {
        NodeTag::T_TransactionStmt => {
            let s = node.as_transaction_stmt().expect("TransactionStmt");
            Node::mk(
                mcx,
                TransactionStmt {
                    kind: s.kind,
                    options: copy_raw_list(mcx, &s.options)?,
                    savepoint_name: opt_str_in(mcx, s.savepoint_name)?,
                    gid: opt_str_in(mcx, s.gid)?,
                    chain: s.chain,
                    location: s.location,
                },
            )
        }
        NodeTag::T_VariableSetStmt => {
            let s = node.as_variable_set_stmt().expect("VariableSetStmt");
            Node::mk(
                mcx,
                VariableSetStmt {
                    kind: s.kind,
                    name: opt_str_in(mcx, s.name)?,
                    args: copy_raw_list(mcx, &s.args)?,
                    jumble_args: s.jumble_args,
                    is_local: s.is_local,
                    location: s.location,
                },
            )
        }
        NodeTag::T_ExecuteStmt => {
            let s = node.as_execute_stmt().expect("ExecuteStmt");
            Node::mk(
                mcx,
                ExecuteStmt {
                    name: opt_str_in(mcx, s.name)?,
                    params: copy_raw_list(mcx, &s.params)?,
                },
            )
        }
        NodeTag::T_CallStmt => {
            let s = node.as_call_stmt().expect("CallStmt");
            let funccall = match s.funccall {
                Some(fc) => Some(
                    Node::mk(mcx, copy_func_call(mcx, fc)?)?
                        .as_variant::<FuncCall>()
                        .expect("FuncCall"),
                ),
                None => None,
            };
            let funcexpr = match s.funcexpr {
                Some(fe) => Some(copy_func_expr(mcx, fe)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::rawnodes::CallStmt {
                    funccall,
                    funcexpr,
                    outargs: copy_raw_list(mcx, &s.outargs)?,
                },
            )
        }
        NodeTag::T_ExplainStmt => {
            let s = node.as_variant::<types_nodes::parsenodes::ExplainStmt>().expect("ExplainStmt");
            Node::mk(
                mcx,
                types_nodes::parsenodes::ExplainStmt {
                    query: copy_raw_opt(mcx, s.query)?,
                    options: copy_raw_list(mcx, &s.options)?,
                },
            )
        }
        NodeTag::T_VacuumStmt => {
            let s = node.as_vacuum_stmt().expect("VacuumStmt");
            Node::mk(
                mcx,
                types_nodes::parsenodes::VacuumStmt {
                    options: copy_raw_list(mcx, &s.options)?,
                    rels: copy_raw_list(mcx, &s.rels)?,
                    is_vacuumcmd: s.is_vacuumcmd,
                },
            )
        }
        other => panic!(
            "copyObject (copyfuncs.c): utility statement {other:?} copy arm unported \
             (standard_ProcessUtility readOnlyTree, cached-plan execution)"
        ),
    }
}

fn copy_func_call<'d>(mcx: Mcx<'d>, fc: &FuncCall<'_>) -> PgResult<FuncCall<'d>> {
    Ok(FuncCall {
        funcname: copy_raw_list(mcx, &fc.funcname)?,
        args: copy_raw_list(mcx, &fc.args)?,
        agg_order: copy_raw_list(mcx, &fc.agg_order)?,
        agg_filter: copy_raw_opt(mcx, fc.agg_filter)?,
        over: copy_raw_opt(mcx, fc.over)?,
        agg_within_group: fc.agg_within_group,
        agg_star: fc.agg_star,
        agg_distinct: fc.agg_distinct,
        func_variadic: fc.func_variadic,
        funcformat: fc.funcformat,
        location: fc.location,
    })
}

fn copy_func_expr<'d>(mcx: Mcx<'d>, fe: &FuncExpr<'_>) -> PgResult<&'d FuncExpr<'d>> {
    let copy = FuncExpr {
        funcid: fe.funcid,
        funcresulttype: fe.funcresulttype,
        funcretset: fe.funcretset,
        funcvariadic: fe.funcvariadic,
        funcformat: fe.funcformat,
        funccollid: fe.funccollid,
        inputcollid: fe.inputcollid,
        args: copy_raw_list(mcx, &fe.args)?,
        location: fe.location,
    };
    Ok(Node::mk(mcx, copy)?.as_variant::<FuncExpr>().expect("FuncExpr"))
}

fn copy_raw_list<'d>(mcx: Mcx<'d>, list: &NodeList<'_>) -> PgResult<NodeList<'d>> {
    let mut out = NodeList::nil();
    for cell in list.iter() {
        out.lappend(mcx, copy_raw(mcx, cell)?)?;
    }
    Ok(out)
}

fn copy_raw_opt<'d>(mcx: Mcx<'d>, node: Option<Node<'_>>) -> PgResult<Option<Node<'d>>> {
    match node {
        Some(n) => Ok(Some(copy_raw(mcx, n)?)),
        None => Ok(None),
    }
}

// Raw grammar nodes have no outfuncs arms, so the tags reachable from the
// copied statements get direct arms; everything else (transformed expression
// trees) falls through to the out/read round trip.
fn copy_raw<'d>(mcx: Mcx<'d>, node: Node<'_>) -> PgResult<Node<'d>> {
    match node.node_tag() {
        NodeTag::T_String => {
            let s = node.as_string().expect("String");
            Node::mk(mcx, types_nodes::String { sval: str_in(mcx, s.sval)? })
        }
        NodeTag::T_Integer => {
            let i = node.as_integer().expect("Integer");
            Node::mk(mcx, types_nodes::Integer { ival: i.ival })
        }
        NodeTag::T_Float => {
            let f = node.as_float().expect("Float");
            Node::mk(mcx, types_nodes::Float { fval: str_in(mcx, f.fval)? })
        }
        NodeTag::T_Boolean => {
            let b = node.as_boolean().expect("Boolean");
            Node::mk(mcx, types_nodes::Boolean { boolval: b.boolval })
        }
        NodeTag::T_BitString => {
            let b = node.as_variant::<types_nodes::BitString>().expect("BitString");
            Node::mk(mcx, types_nodes::BitString { bsval: str_in(mcx, b.bsval)? })
        }
        NodeTag::T_A_Const => {
            let c = node.as_variant::<A_Const>().expect("A_Const");
            let val = match &c.val {
                Some(v) => Some(copy_val(mcx, v)?),
                None => None,
            };
            Node::mk(mcx, A_Const { val, location: c.location })
        }
        NodeTag::T_A_Star => Node::mk(mcx, A_Star),
        NodeTag::T_ParamRef => {
            let p = node.as_variant::<ParamRef>().expect("ParamRef");
            Node::mk(mcx, ParamRef { number: p.number, location: p.location })
        }
        NodeTag::T_DefElem => {
            let d = node.as_def_elem().expect("DefElem");
            Node::mk(
                mcx,
                DefElem {
                    defnamespace: opt_str_in(mcx, d.defnamespace)?,
                    defname: opt_str_in(mcx, d.defname)?,
                    arg: copy_raw_opt(mcx, d.arg)?,
                    defaction: d.defaction,
                    location: d.location,
                },
            )
        }
        NodeTag::T_TypeCast => {
            let t = node.as_variant::<TypeCast>().expect("TypeCast");
            Node::mk(
                mcx,
                TypeCast {
                    arg: copy_raw_opt(mcx, t.arg)?,
                    typeName: copy_raw_opt(mcx, t.typeName)?,
                    location: t.location,
                },
            )
        }
        NodeTag::T_TypeName => {
            let t = node.as_variant::<TypeName>().expect("TypeName");
            Node::mk(
                mcx,
                TypeName {
                    names: copy_raw_list(mcx, &t.names)?,
                    typeOid: t.typeOid,
                    setof: t.setof,
                    pct_type: t.pct_type,
                    typmods: copy_raw_list(mcx, &t.typmods)?,
                    typemod: t.typemod,
                    arrayBounds: copy_raw_list(mcx, &t.arrayBounds)?,
                    location: t.location,
                },
            )
        }
        NodeTag::T_ColumnRef => {
            let c = node.as_variant::<ColumnRef>().expect("ColumnRef");
            Node::mk(
                mcx,
                ColumnRef { fields: copy_raw_list(mcx, &c.fields)?, location: c.location },
            )
        }
        NodeTag::T_A_Expr => {
            let a = node.as_variant::<A_Expr>().expect("A_Expr");
            Node::mk(
                mcx,
                A_Expr {
                    kind: a.kind,
                    name: copy_raw_list(mcx, &a.name)?,
                    lexpr: copy_raw_opt(mcx, a.lexpr)?,
                    rexpr: copy_raw_opt(mcx, a.rexpr)?,
                    rexpr_list_start: a.rexpr_list_start,
                    rexpr_list_end: a.rexpr_list_end,
                    location: a.location,
                },
            )
        }
        NodeTag::T_FuncCall => {
            let fc = node.as_variant::<FuncCall>().expect("FuncCall");
            Node::mk(mcx, copy_func_call(mcx, fc)?)
        }
        NodeTag::T_VacuumRelation => {
            let v = node
                .as_variant::<types_nodes::parsenodes::VacuumRelation>()
                .expect("VacuumRelation");
            Node::mk(
                mcx,
                types_nodes::parsenodes::VacuumRelation {
                    relation: copy_raw_opt(mcx, v.relation)?,
                    oid: v.oid,
                    va_cols: copy_raw_list(mcx, &v.va_cols)?,
                },
            )
        }
        NodeTag::T_RangeVar => {
            let r = node.as_range_var().expect("RangeVar");
            let alias = match r.alias {
                Some(a) => Some(
                    Node::mk(
                        mcx,
                        types_nodes::primnodes::Alias {
                            aliasname: opt_str_in(mcx, a.aliasname)?,
                            colnames: copy_raw_list(mcx, &a.colnames)?,
                        },
                    )?
                    .as_variant::<types_nodes::primnodes::Alias>()
                    .expect("Alias"),
                ),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::RangeVar {
                    catalogname: opt_str_in(mcx, r.catalogname)?,
                    schemaname: opt_str_in(mcx, r.schemaname)?,
                    relname: opt_str_in(mcx, r.relname)?,
                    inh: r.inh,
                    relpersistence: r.relpersistence,
                    alias,
                    location: r.location,
                },
            )
        }
        NodeTag::T_List => {
            let l = node.as_list().expect("List");
            Node::mk_list(mcx, copy_raw_list(mcx, l)?)
        }
        _ => copy_via_out_read(mcx, node),
    }
}

fn copy_val<'d>(mcx: Mcx<'d>, v: &ValUnion<'_>) -> PgResult<ValUnion<'d>> {
    Ok(match v {
        ValUnion::Integer(i) => ValUnion::Integer(types_nodes::Integer { ival: i.ival }),
        ValUnion::Float(f) => ValUnion::Float(types_nodes::Float { fval: str_in(mcx, f.fval)? }),
        ValUnion::Boolean(b) => ValUnion::Boolean(types_nodes::Boolean { boolval: b.boolval }),
        ValUnion::String(s) => ValUnion::String(types_nodes::String { sval: str_in(mcx, s.sval)? }),
        ValUnion::BitString(b) => {
            ValUnion::BitString(types_nodes::BitString { bsval: str_in(mcx, b.bsval)? })
        }
    })
}

fn copy_via_out_read<'d>(mcx: Mcx<'d>, node: Node<'_>) -> PgResult<Node<'d>> {
    // SAFETY: nodeToString only reads the tree; the unified handle does not
    // outlive the serialize call.
    let node = unsafe { core::mem::transmute::<Node<'_>, Node<'d>>(node) };
    let s = outfuncs::nodeToString(mcx, node)?;
    readfuncs::stringToNode(mcx, s.as_str())
}

fn str_in<'d>(mcx: Mcx<'d>, s: &str) -> PgResult<&'d str> {
    let v = mcx::slice_in(mcx, s.as_bytes())?;
    Ok(core::str::from_utf8(v.leak()).expect("copied str stays UTF-8"))
}

fn opt_str_in<'d>(mcx: Mcx<'d>, s: Option<&str>) -> PgResult<Option<&'d str>> {
    match s {
        Some(s) => Ok(Some(str_in(mcx, s)?)),
        None => Ok(None),
    }
}
