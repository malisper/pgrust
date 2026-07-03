#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use mcx::Mcx;
use parse_expr::{expr_type, transformExpr};
use parser_small1::{ParseExprKind, ParseNamespaceItem, ParseState};
use types_core::catalog::{TEXTOID, UNKNOWNOID};
use types_core::AttrNumber;
use types_error::PgResult;
use types_nodes::rawnodes::{A_Expr_Kind, ColumnRef};
use types_nodes::{CoercionForm, Node, NodeList, NodeTag, RTEKind, TargetEntry};

pub fn transformTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    targetlist: &NodeList<'mcx>,
    exprKind: ParseExprKind,
) -> PgResult<NodeList<'mcx>> {
    let mut p_target = NodeList::nil();
    debug_assert!(pstate.p_multiassign_exprs.is_nil());
    let expand_star = exprKind != ParseExprKind::EXPR_KIND_UPDATE_SOURCE;

    for o_target in targetlist {
        let res = o_target
            .as_res_target()
            .unwrap_or_else(|| panic!("targetlist element is not a ResTarget: {o_target:?}"));
        let val = res.val.expect("ResTarget.val is never NULL in a raw targetlist");

        if expand_star {
            if let Some(cref) = val.as_column_ref() {
                if cref.fields.last().is_some_and(|f| f.node_tag() == NodeTag::T_A_Star) {
                    p_target.concat(mcx, &ExpandColumnRefStar(mcx, pstate, cref)?)?;
                    continue;
                }
            } else if val.node_tag() == NodeTag::T_A_Indirection {
                panic!(
                    "transformTargetList (parse_target.c): ExpandIndirectionStar \
                     unported — unit backend-parser-parse-target"
                );
            }
        }

        let te = transformTargetEntry(mcx, pstate, val, None, exprKind, res.name, false)?;
        p_target.lappend(mcx, te)?;
    }

    if !pstate.p_multiassign_exprs.is_nil() {
        panic!(
            "transformTargetList (parse_target.c): multiassign resjunk attach \
             (UPDATE tlist) unported — unit backend-parser-parse-target"
        );
    }

    Ok(p_target)
}

pub fn transformTargetEntry<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    expr: Option<Node<'mcx>>,
    exprKind: ParseExprKind,
    colname: Option<&'mcx str>,
    resjunk: bool,
) -> PgResult<Node<'mcx>> {
    let expr = match expr {
        Some(e) => e,
        None => {
            if exprKind == ParseExprKind::EXPR_KIND_UPDATE_SOURCE
                && node.node_tag() == NodeTag::T_SetToDefault
            {
                node
            } else {
                transformExpr(mcx, pstate, node, exprKind)?
            }
        }
    };

    let colname = match colname {
        None if !resjunk => Some(FigureColname(node)),
        other => other,
    };

    let resno = pstate.p_next_resno as AttrNumber;
    pstate.p_next_resno += 1;
    Node::mk_target_entry(mcx, expr, resno, colname, resjunk)
}

pub fn markTargetListOrigins<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    targetlist: &NodeList<'mcx>,
) -> PgResult<()> {
    for tle_node in targetlist {
        let tle = tle_node.as_target_entry().unwrap();
        markTargetListOrigin(pstate, tle_node, tle.expr, 0)?;
    }
    Ok(())
}

fn markTargetListOrigin<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    tle_node: Node<'mcx>,
    expr: Node<'mcx>,
    levelsup: i32,
) -> PgResult<()> {
    let Some(var) = expr.as_var() else {
        return Ok(());
    };
    let netlevelsup = var.varlevelsup as i32 + levelsup;
    let rte = parse_relation::GetRTEByRangeTablePosn(pstate, var.varno, netlevelsup);
    let attnum = var.varattno;

    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            // SAFETY: parse analysis holds exclusive access to the targetlist
            // it just built; the `var` borrow is from expr, not tle_node.
            unsafe {
                tle_node
                    .with_mut::<TargetEntry, _>(|t| {
                        t.resorigtbl = rte.relid;
                        t.resorigcol = attnum;
                    })
                    .unwrap();
            }
        }
        RTEKind::RTE_FUNCTION
        | RTEKind::RTE_VALUES
        | RTEKind::RTE_TABLEFUNC
        | RTEKind::RTE_NAMEDTUPLESTORE
        | RTEKind::RTE_RESULT => {}
        other @ (RTEKind::RTE_SUBQUERY
        | RTEKind::RTE_JOIN
        | RTEKind::RTE_CTE
        | RTEKind::RTE_GROUP) => panic!(
            "markTargetListOrigin (parse_target.c): {other:?} recursion arm unported — \
             unit backend-parser-parse-target"
        ),
    }
    Ok(())
}

pub fn resolveTargetListUnknowns<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    targetlist: &NodeList<'mcx>,
) -> PgResult<()> {
    for tle_node in targetlist {
        let tle = tle_node.as_target_entry().unwrap();
        let restype = expr_type(tle.expr);
        if restype == UNKNOWNOID {
            let coerced = coerce::coerce_type(
                mcx,
                pstate,
                tle.expr,
                restype,
                TEXTOID,
                -1,
                coerce::COERCION_IMPLICIT,
                CoercionForm::COERCE_IMPLICIT_CAST,
                -1,
            )?;
            // SAFETY: parse analysis holds exclusive access to the targetlist
            // it just built; the `tle` borrow is not used past this point.
            unsafe {
                tle_node.with_mut::<TargetEntry, _>(|t| t.expr = coerced).unwrap();
            }
        }
    }
    Ok(())
}

fn ExpandColumnRefStar<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    cref: &ColumnRef<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let fields = cref.fields.as_slice();
    if fields.len() == 1 {
        return ExpandAllTables(mcx, pstate, cref.location);
    }

    let field_str = |n: Node<'mcx>| {
        n.as_string().map(|s| s.sval).expect("ColumnRef qualifier is a String")
    };
    let (nspname, relname) = match fields.len() {
        2 => (None, field_str(fields[0])),
        3 => (Some(field_str(fields[0])), field_str(fields[1])),
        4 => panic!(
            "ExpandColumnRefStar (parse_target.c): catalog-qualified star needs \
             get_database_name — unit backend-parser-parse-target"
        ),
        _ => panic!(
            "ExpandColumnRefStar (parse_target.c): >4 dotted names — C raises 42601; \
             arm unported with the catalog-qualified lane"
        ),
    };

    let mut levels_up = 0;
    let nsitem = parse_relation::refnameNamespaceItem(
        pstate,
        nspname,
        relname,
        cref.location,
        Some(&mut levels_up),
    )?;
    let Some(nsitem) = nsitem else {
        let rv = Node::mk_mut(
            mcx,
            types_nodes::RangeVar {
                schemaname: nspname.map(|s| str_in(mcx, s)).transpose()?,
                relname: Some(str_in(mcx, relname)?),
                location: cref.location,
                ..Default::default()
            },
        )?
        .seal_ref();
        return Err(parse_relation::errorMissingRTE(mcx, pstate, rv));
    };

    ExpandSingleTable(mcx, pstate, nsitem, levels_up, cref.location, true)
}

fn ExpandAllTables<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    location: i32,
) -> PgResult<NodeList<'mcx>> {
    let mut target = NodeList::nil();
    let mut found_table = false;

    // p_namespace is iterated by index: expandNSItemAttrs needs &mut pstate
    // (p_next_resno) while the vec's items are 'mcx-borrowed.
    for i in 0..pstate.p_namespace.len() {
        let nsitem = pstate.p_namespace[i];
        if !nsitem.p_cols_visible {
            continue;
        }
        debug_assert!(!nsitem.p_lateral_only);
        found_table = true;
        target.concat(
            mcx,
            &parse_relation::expandNSItemAttrs(mcx, pstate, nsitem, 0, true, location)?,
        )?;
    }

    if !found_table {
        return Err(star_with_no_tables(pstate, location));
    }
    Ok(target)
}

fn ExpandSingleTable<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    nsitem: &ParseNamespaceItem<'mcx>,
    sublevels_up: i32,
    location: i32,
    make_target_entry: bool,
) -> PgResult<NodeList<'mcx>> {
    if make_target_entry {
        return parse_relation::expandNSItemAttrs(
            mcx,
            pstate,
            nsitem,
            sublevels_up,
            true,
            location,
        );
    }
    let rte = nsitem.p_rte;
    let (vars, _) = parse_relation::expandNSItemVars(mcx, pstate, nsitem, sublevels_up, location)?;
    if rte.rtekind == RTEKind::RTE_RELATION {
        let perminfo = nsitem.p_perminfo.expect("relation nsitem has perminfo");
        // SAFETY: perminfo nodes are read only through transient as_* lookups;
        // no derived reference is live across this call.
        unsafe {
            perminfo.with_mut::<types_nodes::RTEPermissionInfo, _>(|p| {
                p.requiredPerms |= types_nodes::parsenodes::ACL_SELECT
            })
        }
        .expect("p_perminfo is RTEPermissionInfo");
    }
    for var_node in &vars {
        let var = var_node.as_var().expect("expandNSItemVars yields Vars");
        parse_relation::markVarForSelectPriv(mcx, pstate, var)?;
    }
    Ok(vars)
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

#[cold]
fn star_with_no_tables(
    pstate: &ParseState<'_, '_>,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("SELECT * with no tables specified is not valid")
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_target.c", 0, "ExpandAllTables")),
    )
}

pub fn FigureColname<'mcx>(node: Node<'mcx>) -> &'mcx str {
    FigureColnameInternal(node).unwrap_or("?column?")
}

fn FigureColnameInternal<'mcx>(node: Node<'mcx>) -> Option<&'mcx str> {
    match node.node_tag() {
        NodeTag::T_ColumnRef => {
            let mut fname = None;
            for f in &node.as_column_ref().unwrap().fields {
                if let Some(s) = f.as_string() {
                    fname = Some(s.sval);
                }
            }
            fname
        }
        NodeTag::T_A_Expr => {
            if node.as_a_expr().unwrap().kind == A_Expr_Kind::AEXPR_NULLIF {
                Some("nullif")
            } else {
                None
            }
        }
        NodeTag::T_FuncCall => {
            let fc = node.as_func_call().unwrap();
            fc.funcname.last().and_then(|n| n.as_string()).map(|s| s.sval)
        }
        NodeTag::T_A_Const | NodeTag::T_ParamRef => None,
        other => panic!(
            "FigureColnameInternal (parse_target.c): arm for {other:?} unported — \
             unit backend-parser-parse-target"
        ),
    }
}
