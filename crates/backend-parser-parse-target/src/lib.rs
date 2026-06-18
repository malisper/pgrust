//! Port of `src/backend/parser/parse_target.c` (PostgreSQL 18.3) — handle
//! target lists (SELECT/UPDATE/RETURNING tlists, INSERT column lists, `foo.*`
//! star expansion, INSERT/UPDATE assigned-expression coercion + indirection,
//! and the FigureColname column-name heuristics).
//!
//! # Owned model
//!
//! The raw-grammar `Node *` input is a [`types_nodes::nodes::Node`] (the
//! `ResTarget`/`ColumnRef`/`A_Indirection`/… vocabulary); transformed
//! expressions are [`types_nodes::primnodes::Expr`]; a `List *` is a `PgVec` on
//! the raw side and a `Vec`/`PgVec` on the typed side; a `NULL` is `None`. There
//! is no `extern "C"` and no raw pointers.
//!
//! # Seams
//!
//! `transformExpr` (parse_expr.c) is reached through
//! `backend-parser-parse-expr-seams` to avoid the parse_target ⇆ parse_expr
//! crate cycle (parse_expr will later call back into parse_target). The merged
//! sibling owners (`parse_relation.c`, `parse_coerce.c`, `parse_type.c`,
//! `parse_node.c` in small1) and the catalog support (`lsyscache.c`,
//! `funcapi.c`, `format_type`, `tupdesc.c`, `dbcommands.c`) are called
//! directly / through their installed seams.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::string::String;

use mcx::{alloc_in, Mcx, PgBox, PgString, PgVec};

use types_core::{AttrNumber, InvalidAttrNumber, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_AMBIGUOUS_COLUMN, ERRCODE_CANNOT_COERCE, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_COLUMN, ERROR,
};
use types_tuple::heaptuple::{RECORDOID, TEXTOID, UNKNOWNOID};

use types_nodes::nodes::{ntag, Node, NodePtr};
use types_nodes::parsenodes::{
    RangeTblEntry, RTE_CTE, RTE_FUNCTION, RTE_GROUP, RTE_JOIN, RTE_NAMEDTUPLESTORE, RTE_RELATION,
    RTE_RESULT, RTE_SUBQUERY, RTE_TABLEFUNC, RTE_VALUES,
};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::{
    CaseTestExpr, CoercionForm, Expr, FieldSelect, FieldStore, MinMaxOp, SQLValueFunctionOp,
    SetToDefault, SubLinkType, SubscriptingRef, Var,
};
use types_nodes::primnodes::XmlExprOp;
use types_nodes::rawnodes::{A_Indirection, ColumnRef, ResTarget};
use types_parsenodes::CoercionContext;

use types_acl::acl::ACL_SELECT;

use backend_utils_error::ereport;
use backend_nodes_core::makefuncs::{make_null_const, make_target_entry, make_var};
use backend_nodes_core::nodefuncs::{expr_collation, expr_location, expr_type, expr_typmod};

use backend_parser_relation as parse_relation;
use backend_parser_coerce as parse_coerce;
use backend_parser_parse_type as parse_type;
use backend_parser_small1 as parse_node;

use backend_parser_parse_expr_seams as parse_expr;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_init_small_seams as globals;

// ===========================================================================
// Small helpers (the C `strVal` / `IsA` / `llast` idioms).
// ===========================================================================

/// `strVal(node)` — the string contents of a `String` value node.
fn str_val<'a>(node: &'a Node<'_>) -> &'a str {
    match node.node_tag() {
        ntag::T_String => node.expect_string().sval.as_str(),
        _ => "",
    }
}

/// `IsA(node, String)`.
fn is_string(node: &Node<'_>) -> bool {
    node.is_string()
}

/// Convert a raw-grammar `SetToDefault` node into the typed primnode form (the
/// C uses one struct; the split model keeps the raw node's `location` only on
/// the raw side, which the typed `Expr::SetToDefault` does not carry).
fn raw_settodefault_to_prim(d: &types_nodes::rawexprnodes::SetToDefault) -> SetToDefault {
    SetToDefault {
        typeId: d.type_id,
        typeMod: d.type_mod,
        collation: d.collation,
        location: d.location,
    }
}

/// `NameStr(attr->attname)` as a `&str`.
fn attname_str(attr: &types_tuple::heaptuple::FormData_pg_attribute) -> &str {
    let bytes = attr.attname.name_str();
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).unwrap_or("")
}

/// `parser_errposition(pstate, location)` (parse_node.c) — translate a token
/// location into the 1-based cursor position. Infallible.
fn parser_errposition(pstate: &ParseState<'_>, location: i32) -> i32 {
    backend_parser_small1_seams::parser_errposition::call(pstate, location).unwrap_or(0)
}

/// `format_type_be(typid)` as an owned `String` for error messages.
fn format_type(typid: Oid) -> PgResult<String> {
    backend_utils_adt_format_type::format_type_be_owned(typid)
}

/// `RelationGetRelationName(rel)`.
fn rel_name<'a>(rd: &'a types_rel::RelationData<'_>) -> &'a str {
    rd.rd_rel.relname.as_str()
}

/// `GetCTETargetList(cte)` (parsenodes.h macro) — the CTE's output target list:
/// the `Query`'s `targetList` for SELECT, else its `returningList`.
fn get_cte_target_list<'a, 'mcx>(
    cte: &'a types_nodes::rawnodes::CommonTableExpr<'mcx>,
) -> &'a [types_nodes::primnodes::TargetEntry<'mcx>] {
    let q = cte
        .ctequery
        .as_deref()
        .and_then(|n| n.as_query())
        .unwrap_or_else(|| panic!("GetCTETargetList: cte->ctequery is not a Query"));
    if q.commandType == types_nodes::nodes::CmdType::CMD_SELECT {
        &q.targetList
    } else {
        &q.returningList
    }
}

// ===========================================================================
// transformTargetEntry (parse_target.c:74).
// ===========================================================================

/// `transformTargetEntry(pstate, node, expr, exprKind, colname, resjunk)` —
/// transform any ordinary expression node into a targetlist entry.
pub fn transformTargetEntry<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    node: Option<Node<'mcx>>,
    expr: Option<Expr>,
    expr_kind: ParseExprKind,
    colname: Option<String>,
    resjunk: bool,
) -> PgResult<types_nodes::primnodes::TargetEntry<'mcx>> {
    // Transform the node if caller didn't do it already.
    let expr = match expr {
        Some(e) => Some(e),
        None => {
            // If it's a SetToDefault node and we should allow that, pass it
            // through unmodified.
            if expr_kind == ParseExprKind::EXPR_KIND_UPDATE_SOURCE
                && node.as_ref().is_some_and(|n| n.is_settodefault())
            {
                node.as_ref()
                    .and_then(|n| n.as_settodefault())
                    .map(|d| Expr::SetToDefault(raw_settodefault_to_prim(d)))
            } else {
                parse_expr::transformExpr::call(pstate, clone_opt_node(&node, mcx)?, expr_kind)?
            }
        }
    };

    let colname = if colname.is_none() && !resjunk {
        // Generate a suitable column name for a column without explicit AS.
        FigureColname(node.as_ref())
    } else {
        colname
    };

    let resno = pstate.p_next_resno as AttrNumber;
    pstate.p_next_resno += 1;

    make_target_entry(
        mcx,
        expr.expect("transformTargetEntry: NULL expr"),
        resno,
        colname.as_deref(),
        resjunk,
    )
}

/// Clone an `Option<Node>` for the `transformExpr` call (the C reads `node`
/// twice: once to transform, once for `FigureColname`).
fn clone_opt_node<'mcx>(
    node: &Option<Node<'mcx>>,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    match node {
        Some(n) => Ok(Some(n.clone_in(mcx)?)),
        None => Ok(None),
    }
}

// ===========================================================================
// transformTargetList (parse_target.c:120).
// ===========================================================================

/// `transformTargetList(pstate, targetlist, exprKind)` — turn a list of
/// `ResTarget`s into a list of `TargetEntry`s.
pub fn transformTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    targetlist: PgVec<'mcx, ResTarget<'mcx>>,
    expr_kind: ParseExprKind,
) -> PgResult<PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>> {
    // Shouldn't have any leftover multiassign items at start.
    debug_assert!(pstate.p_multiassign_exprs.is_empty());

    // Expand "something.*" in SELECT and RETURNING, but not UPDATE.
    let expand_star = expr_kind != ParseExprKind::EXPR_KIND_UPDATE_SOURCE;

    let mut p_target: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> = PgVec::new_in(mcx);

    for res in targetlist.into_iter() {
        // Check for "something.*".
        if expand_star {
            match res.val.as_deref().map(|n| n.node_tag()) {
                Some(ntag::T_ColumnRef) => {
                    let cref = res.val.as_deref().unwrap().expect_columnref();
                    if cref.fields.last().map(|n| &**n).is_some_and(|n| n.is_a_star()) {
                        let expanded = ExpandColumnRefStar(mcx, pstate, cref, true)?;
                        push_te_list(mcx, &mut p_target, expanded.into_targets())?;
                        continue;
                    }
                }
                Some(ntag::T_A_Indirection) => {
                    let ind = res.val.as_deref().unwrap().expect_a_indirection();
                    if ind.indirection.last().map(|n| &**n).is_some_and(|n| n.is_a_star()) {
                        let expanded = ExpandIndirectionStar(mcx, pstate, ind, true, expr_kind)?;
                        push_te_list(mcx, &mut p_target, expanded.into_targets())?;
                        continue;
                    }
                }
                _ => {}
            }
        }

        // Not "something.*", so transform as a single expression.
        let name = res.name.as_deref().map(String::from);
        let val = res.val.map(|b| PgBox::into_inner(b));
        let te = transformTargetEntry(mcx, pstate, val, None, expr_kind, name, false)?;
        push_te(mcx, &mut p_target, te)?;
    }

    // Attach any multiassign resjunk items to the end of the targetlist.
    if !pstate.p_multiassign_exprs.is_empty() {
        debug_assert!(expr_kind == ParseExprKind::EXPR_KIND_UPDATE_SOURCE);
        let multis = core::mem::replace(&mut pstate.p_multiassign_exprs, PgVec::new_in(mcx));
        for te in multis.into_iter() {
            push_te(mcx, &mut p_target, te)?;
        }
    }

    Ok(p_target)
}

/// Append one `TargetEntry` to a `PgVec` (fallible reserve, infallible push).
fn push_te<'mcx>(
    mcx: Mcx<'mcx>,
    list: &mut PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
    te: types_nodes::primnodes::TargetEntry<'mcx>,
) -> PgResult<()> {
    list.try_reserve(1).map_err(|_| mcx.oom(1))?;
    list.push(te);
    Ok(())
}

/// `list_concat(list, more)` over a `TargetEntry` list.
fn push_te_list<'mcx>(
    mcx: Mcx<'mcx>,
    list: &mut PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
    more: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
) -> PgResult<()> {
    list.try_reserve(more.len()).map_err(|_| mcx.oom(more.len()))?;
    for te in more.into_iter() {
        list.push(te);
    }
    Ok(())
}

/// `list_concat(list, more)` over an expression list.
fn push_expr_list<'mcx>(
    mcx: Mcx<'mcx>,
    list: &mut PgVec<'mcx, Expr>,
    more: PgVec<'mcx, Expr>,
) -> PgResult<()> {
    list.try_reserve(more.len()).map_err(|_| mcx.oom(more.len()))?;
    for e in more.into_iter() {
        list.push(e);
    }
    Ok(())
}

/// `lappend(list, e)` over an expression list.
fn push_expr<'mcx>(mcx: Mcx<'mcx>, list: &mut PgVec<'mcx, Expr>, e: Expr) -> PgResult<()> {
    list.try_reserve(1).map_err(|_| mcx.oom(1))?;
    list.push(e);
    Ok(())
}

// ===========================================================================
// transformExpressionList (parse_target.c:219).
// ===========================================================================

/// `transformExpressionList(pstate, exprlist, exprKind, allowDefault)` — the
/// `transformTargetList` transformation for bare expressions (ROW()/VALUES()).
pub fn transformExpressionList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    exprlist: PgVec<'mcx, NodePtr<'mcx>>,
    expr_kind: ParseExprKind,
    allow_default: bool,
) -> PgResult<PgVec<'mcx, Expr>> {
    let mut result: PgVec<'mcx, Expr> = PgVec::new_in(mcx);

    for e in exprlist.into_iter() {
        let e = PgBox::into_inner(e);

        // Check for "something.*".
        match e.node_tag() {
            ntag::T_ColumnRef => {
                let cref = e.expect_columnref();
                if cref.fields.last().map(|n| &**n).is_some_and(|n| n.is_a_star()) {
                    let expanded = ExpandColumnRefStar(mcx, pstate, cref, false)?;
                    push_expr_list(mcx, &mut result, expanded.into_exprs())?;
                    continue;
                }
            }
            ntag::T_A_Indirection => {
                let ind = e.expect_a_indirection();
                if ind.indirection.last().map(|n| &**n).is_some_and(|n| n.is_a_star()) {
                    let expanded =
                        ExpandIndirectionStar(mcx, pstate, ind, false, expr_kind)?;
                    push_expr_list(mcx, &mut result, expanded.into_exprs())?;
                    continue;
                }
            }
            _ => {}
        }

        // Not "something.*", so transform as a single expression.  If it's a
        // SetToDefault node and we should allow that, pass it through unmodified.
        let transformed = if allow_default && e.is_settodefault() {
            Expr::SetToDefault(raw_settodefault_to_prim(e.expect_settodefault()))
        } else {
            parse_expr::transformExpr::call(pstate, Some(e), expr_kind)?
                .expect("transformExpressionList: NULL expr")
        };

        push_expr(mcx, &mut result, transformed)?;
    }

    Ok(result)
}

/// Result of a star-expansion: either decorated `TargetEntry`s (top-level
/// SELECT/RETURNING, `make_target_entry == true`) or bare `Expr`s (ROW()/
/// VALUES(), `make_target_entry == false`), mirroring C's `make_target_entry`
/// flag.  The producer always returns the variant matching the flag.
pub enum ExpandResult<'mcx> {
    Targets(PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>),
    Exprs(PgVec<'mcx, Expr>),
}

impl<'mcx> ExpandResult<'mcx> {
    fn into_targets(self) -> PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> {
        match self {
            ExpandResult::Targets(v) => v,
            ExpandResult::Exprs(_) => panic!("ExpandResult: expected TargetEntry list"),
        }
    }

    fn into_exprs(self) -> PgVec<'mcx, Expr> {
        match self {
            ExpandResult::Exprs(v) => v,
            ExpandResult::Targets(_) => panic!("ExpandResult: expected Expr list"),
        }
    }
}

// ===========================================================================
// resolveTargetListUnknowns (parse_target.c:287).
// ===========================================================================

/// `resolveTargetListUnknowns(pstate, targetlist)` — convert any unknown-type
/// targetlist entries to type TEXT.
pub fn resolveTargetListUnknowns<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    targetlist: &mut [types_nodes::primnodes::TargetEntry<'mcx>],
) -> PgResult<()> {
    for tle in targetlist.iter_mut() {
        let restype = expr_type(tle.expr.as_deref())?;

        if restype == UNKNOWNOID {
            let expr = tle.expr.take().map(|b| PgBox::into_inner(b));
            let coerced = parse_coerce::coerce_type(
                mcx,
                Some(pstate),
                expr,
                restype,
                TEXTOID,
                -1,
                CoercionContext::COERCION_IMPLICIT,
                CoercionForm::COERCE_IMPLICIT_CAST,
                -1,
            )?;
            tle.expr = match coerced {
                Some(e) => Some(alloc_in(mcx, e)?),
                None => None,
            };
        }
    }
    Ok(())
}

// ===========================================================================
// markTargetListOrigins / markTargetListOrigin (parse_target.c:317).
// ===========================================================================

/// `markTargetListOrigins(pstate, targetlist)` — mark targetlist columns that
/// are simple Vars with the source table's OID and column number.
pub fn markTargetListOrigins<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    targetlist: &mut [types_nodes::primnodes::TargetEntry<'mcx>],
) -> PgResult<()> {
    for tle in targetlist.iter_mut() {
        // markTargetListOrigin(pstate, tle, (Var *) tle->expr, 0).
        let var = tle.expr.as_deref().and_then(|e| e.as_var()).cloned();
        markTargetListOrigin(mcx, pstate, tle, var.as_ref(), 0)?;
    }
    Ok(())
}

/// `markTargetListOrigin(pstate, tle, var, levelsup)` — if `var` is a Var of a
/// plain relation, mark `tle` with its origin.
fn markTargetListOrigin<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    tle: &mut types_nodes::primnodes::TargetEntry<'mcx>,
    var: Option<&Var>,
    levelsup: i32,
) -> PgResult<()> {
    let Some(var) = var else {
        return Ok(());
    };

    let netlevelsup = var.varlevelsup as i32 + levelsup;
    let rte = parse_relation::GetRTEByRangeTablePosn(pstate, var.varno, netlevelsup);
    let attnum = var.varattno;

    match rte.rtekind {
        RTE_RELATION => {
            // It's a table or view, report it.
            tle.resorigtbl = rte.relid;
            tle.resorigcol = attnum;
        }
        RTE_SUBQUERY => {
            // Subselect-in-FROM: copy up from the subselect.
            if attnum != InvalidAttrNumber {
                let subquery = rte.subquery.as_deref().expect("subquery set");
                let ste = parse_relation::get_tle_by_resno(&subquery.targetList, attnum);
                match ste {
                    Some(ste) if !ste.resjunk => {
                        tle.resorigtbl = ste.resorigtbl;
                        tle.resorigcol = ste.resorigcol;
                    }
                    _ => {
                        return Err(ereport(ERROR)
                            .errmsg_internal(alloc::format!(
                                "subquery {} does not have attribute {}",
                                rte_eref_aliasname(rte),
                                attnum
                            ))
                            .into_error());
                    }
                }
            }
        }
        RTE_JOIN | RTE_FUNCTION | RTE_VALUES | RTE_TABLEFUNC | RTE_NAMEDTUPLESTORE | RTE_RESULT => {
            // not a simple relation, leave it unmarked.
        }
        RTE_CTE => {
            // CTE reference: copy up from the subquery, if possible.
            if attnum != InvalidAttrNumber && !rte.self_reference {
                let cte = parse_relation::GetCTEForRTE(mcx, pstate, rte, netlevelsup)?;
                let extra_cols = (if cte.search_clause.is_some() { 1 } else { 0 })
                    + (if cte.cycle_clause.is_some() { 2 } else { 0 });
                let tl = get_cte_target_list(&cte);

                // The RTE for the CTE already has the search/cycle columns, but
                // the subquery won't, so skip looking those up.
                if extra_cols != 0
                    && attnum as usize > tl.len()
                    && attnum as usize <= tl.len() + extra_cols
                {
                    return Ok(());
                }

                let ste = parse_relation::get_tle_by_resno(tl, attnum);
                match ste {
                    Some(ste) if !ste.resjunk => {
                        tle.resorigtbl = ste.resorigtbl;
                        tle.resorigcol = ste.resorigcol;
                    }
                    _ => {
                        return Err(ereport(ERROR)
                            .errmsg_internal(alloc::format!(
                                "CTE {} does not have attribute {}",
                                rte_eref_aliasname(rte),
                                attnum
                            ))
                            .into_error());
                    }
                }
            }
        }
        RTE_GROUP => {
            // We couldn't get here: the RTE_GROUP RTE has not been added.
        }
    }

    Ok(())
}

/// `rte->eref->aliasname`.
fn rte_eref_aliasname<'a>(rte: &'a RangeTblEntry<'_>) -> &'a str {
    rte.eref
        .as_deref()
        .and_then(|a| a.aliasname.as_deref())
        .unwrap_or("")
}

// ===========================================================================
// transformAssignedExpr (parse_target.c:454).
// ===========================================================================

/// `transformAssignedExpr(pstate, expr, exprKind, colname, attrno,
/// indirection, location)` — prepare an expression for assignment to a column
/// of the target table (INSERT/UPDATE).
pub fn transformAssignedExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    expr: Option<Expr>,
    expr_kind: ParseExprKind,
    colname: &str,
    attrno: i32,
    indirection: &PgVec<'mcx, NodePtr<'mcx>>,
    location: i32,
) -> PgResult<Expr> {
    // Save and restore identity of expression type we're parsing.
    debug_assert!(expr_kind != ParseExprKind::EXPR_KIND_NONE);
    let sv_expr_kind = pstate.p_expr_kind;
    pstate.p_expr_kind = expr_kind;

    let (attrtype, attrtypmod, attrcollation) = {
        let rd = pstate
            .p_target_relation
            .as_ref()
            .expect("transformAssignedExpr: p_target_relation NULL");

        if attrno <= 0 {
            pstate.p_expr_kind = sv_expr_kind;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(alloc::format!("cannot assign to system column \"{colname}\""))
                .errposition(parser_errposition(pstate, location))
                .into_error());
        }
        let attrtype = parse_relation::attnumTypeId(rd, attrno)?;
        let att = rd.rd_att.attr((attrno - 1) as usize);
        (attrtype, att.atttypmod, att.attcollation)
    };

    let mut expr = expr;

    // If the expression is a DEFAULT placeholder, insert the attribute's
    // type/typmod/collation into it; also reject DEFAULT on a subfield/element.
    if let Some(Expr::SetToDefault(def)) = expr.as_mut() {
        def.typeId = attrtype;
        def.typeMod = attrtypmod;
        def.collation = attrcollation;
        if !indirection.is_empty() {
            let is_indices = indirection
                .first()
                .map(|n| &**n)
                .is_some_and(|n| n.is_a_indices());
            pstate.p_expr_kind = sv_expr_kind;
            if is_indices {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("cannot set an array element to DEFAULT")
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("cannot set a subfield to DEFAULT")
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }
        }
    }

    // Now we can use exprType() safely.
    let type_id = expr_type(expr.as_ref())?;

    let result = if !indirection.is_empty() {
        // There is indirection on the target column: prepare an array or
        // subfield assignment expression.
        let col_var: Node<'mcx> = if pstate.p_is_insert {
            // INSERT INTO table (col.something): no real source value, insert a
            // NULL constant.
            Node::Expr(Expr::Const(make_null_const(
                mcx,
                attrtype,
                attrtypmod,
                attrcollation,
            )?))
        } else {
            // Build a Var for the column to be updated.
            let rtindex = pstate
                .p_target_nsitem
                .as_ref()
                .expect("p_target_nsitem set")
                .p_rtindex;
            let mut var = make_var(
                rtindex,
                attrno as AttrNumber,
                attrtype,
                attrtypmod,
                attrcollation,
                0,
            );
            var.location = location;
            Node::Expr(Expr::Var(var))
        };

        let rhs = expr_to_node(expr.expect("transformAssignedExpr: NULL expr for indirection"));
        let assigned = transformAssignmentIndirection(
            mcx,
            pstate,
            Some(col_var),
            colname,
            false,
            attrtype,
            attrtypmod,
            attrcollation,
            indirection,
            0,
            rhs,
            CoercionContext::COERCION_ASSIGNMENT,
            location,
        )?;
        node_to_expr(assigned)
    } else {
        // Normal non-qualified target column: type checking and coercion.
        let orig_expr = expr.expect("transformAssignedExpr: NULL expr");
        let orig_location = expr_location(Some(&orig_expr))?;
        let coerced = parse_coerce::coerce_to_target_type(
            mcx,
            Some(pstate),
            orig_expr,
            type_id,
            attrtype,
            attrtypmod,
            CoercionContext::COERCION_ASSIGNMENT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        match coerced {
            Some(e) => e,
            None => {
                pstate.p_expr_kind = sv_expr_kind;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(alloc::format!(
                        "column \"{}\" is of type {} but expression is of type {}",
                        colname,
                        format_type(attrtype)?,
                        format_type(type_id)?
                    ))
                    .errhint("You will need to rewrite or cast the expression.")
                    .errposition(parser_errposition(pstate, orig_location))
                    .into_error());
            }
        }
    };

    pstate.p_expr_kind = sv_expr_kind;
    Ok(result)
}

// ===========================================================================
// updateTargetListEntry (parse_target.c:621).
// ===========================================================================

/// `updateTargetListEntry(pstate, tle, colname, attrno, indirection, location)`
/// — prepare an UPDATE TargetEntry for assignment to a column.
pub fn updateTargetListEntry<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    tle: &mut types_nodes::primnodes::TargetEntry<'mcx>,
    colname: String,
    attrno: i32,
    indirection: &PgVec<'mcx, NodePtr<'mcx>>,
    location: i32,
) -> PgResult<()> {
    // Fix up expression as needed.
    let expr = tle.expr.take().map(|b| PgBox::into_inner(b));
    let fixed = transformAssignedExpr(
        mcx,
        pstate,
        expr,
        ParseExprKind::EXPR_KIND_UPDATE_TARGET,
        &colname,
        attrno,
        indirection,
        location,
    )?;
    tle.expr = Some(alloc_in(mcx, fixed)?);

    // Set the resno/resname to identify the target column.
    tle.resno = attrno as AttrNumber;
    tle.resname = Some(PgString::from_str_in(&colname, mcx)?);
    Ok(())
}

// ===========================================================================
// transformAssignmentIndirection (parse_target.c:685).
// ===========================================================================

/// `transformAssignmentIndirection(...)` — process indirection (field selection
/// or subscripting) of the target column in INSERT/UPDATE/assignment.  Recurses
/// for multiple levels; adjacent A_Indices are a single multidimensional
/// subscript operation.  `indirection_cell` is an index into `indirection`.
pub fn transformAssignmentIndirection<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    basenode: Option<Node<'mcx>>,
    target_name: &str,
    target_is_subscripting: bool,
    target_type_id: Oid,
    target_typmod: i32,
    target_collation: Oid,
    indirection: &PgVec<'mcx, NodePtr<'mcx>>,
    indirection_cell: usize,
    rhs: Node<'mcx>,
    ccontext: CoercionContext,
    location: i32,
) -> PgResult<Node<'mcx>> {
    let mut subscripts: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);

    // Set up a substitution if we have indirection but no basenode.  We abuse
    // CaseTestExpr for this.  (C: `if (indirection_cell && !basenode)`.)
    let basenode: Option<Node<'mcx>> = match basenode {
        Some(b) => Some(b),
        None => {
            if indirection_cell < indirection.len() {
                Some(Node::Expr(Expr::CaseTestExpr(CaseTestExpr {
                    typeId: target_type_id,
                    typeMod: target_typmod,
                    collation: target_collation,
                })))
            } else {
                None
            }
        }
    };

    let mut rhs = rhs;

    // Split field-selection operations apart from subscripting.
    let mut i = indirection_cell;
    while i < indirection.len() {
        let n = &indirection[i];

        if n.is_a_indices() {
            subscripts.try_reserve(1).map_err(|_| mcx.oom(1))?;
            subscripts.push(alloc_in(mcx, n.clone_in(mcx)?)?);
        } else if n.is_a_star() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("row expansion via \"*\" is not supported here")
                .errposition(parser_errposition(pstate, location))
                .into_error());
        } else {
            debug_assert!(is_string(n));
            let field = String::from(str_val(n));

            // Process subscripts before this field selection.
            if !subscripts.is_empty() {
                return transformAssignmentSubscripts(
                    mcx,
                    pstate,
                    basenode,
                    target_name,
                    target_type_id,
                    target_typmod,
                    target_collation,
                    subscripts,
                    indirection,
                    Some(i),
                    rhs,
                    ccontext,
                    location,
                );
            }

            // No subscripts, so process field selection here.  Look up the
            // composite type, accounting for a domain over composite.
            let (base_type_id, base_type_mod) =
                lsyscache::get_base_type_and_typmod::call(target_type_id)
                    .map(|(b, m)| (b, m))?;
            // getBaseTypeAndTypmod uses the *passed* typmod as the starting
            // value; lsyscache::get_base_type_and_typmod ignores the input
            // typmod, so seed it from target_typmod for the non-domain case.
            let base_type_mod = if base_type_id == target_type_id {
                target_typmod
            } else {
                base_type_mod
            };

            let typrelid = parse_type::typeidTypeRelid(base_type_id)?;
            if !OidIsValid(typrelid) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(alloc::format!(
                        "cannot assign to field \"{}\" of column \"{}\" because its type {} is not a composite type",
                        field, target_name, format_type(target_type_id)?
                    ))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            let attnum = lsyscache::get_attnum::call(typrelid, &field)?;
            if attnum == InvalidAttrNumber {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(alloc::format!(
                        "cannot assign to field \"{}\" of column \"{}\" because there is no such column in data type {}",
                        field, target_name, format_type(target_type_id)?
                    ))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }
            if attnum < 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(alloc::format!("cannot assign to system column \"{field}\""))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            let (field_type_id, field_typmod, field_collation) =
                lsyscache::get_atttypetypmodcoll::call(typrelid, attnum)?;

            // Recurse to create appropriate RHS for field assign.
            rhs = transformAssignmentIndirection(
                mcx,
                pstate,
                None,
                &field,
                false,
                field_type_id,
                field_typmod,
                field_collation,
                indirection,
                i + 1,
                rhs,
                ccontext,
                location,
            )?;

            // Build a FieldStore node (list_make1 of the RHS and the attnum).
            // basenode is the CaseTestExpr substitution (or the caller's base);
            // it is always present on the field-selection path.
            let arg_expr = basenode
                .as_ref()
                .map(|n| node_expr_ref(n).clone())
                .expect("FieldStore arg basenode present");
            let fstore = FieldStore {
                arg: Some(alloc::boxed::Box::new(arg_expr)),
                newvals: alloc::vec![node_to_expr(rhs)],
                fieldnums: alloc::vec![attnum],
                resulttype: base_type_id,
            };

            // If target is a domain, apply constraints.
            if base_type_id != target_type_id {
                return Ok(Node::Expr(parse_coerce::coerce_to_domain(
                    mcx,
                    Expr::FieldStore(fstore),
                    base_type_id,
                    base_type_mod,
                    target_type_id,
                    CoercionContext::COERCION_IMPLICIT,
                    CoercionForm::COERCE_IMPLICIT_CAST,
                    location,
                    false,
                )?));
            }

            return Ok(Node::Expr(Expr::FieldStore(fstore)));
        }

        i += 1;
    }

    // Process trailing subscripts, if any.
    if !subscripts.is_empty() {
        return transformAssignmentSubscripts(
            mcx,
            pstate,
            basenode,
            target_name,
            target_type_id,
            target_typmod,
            target_collation,
            subscripts,
            indirection,
            None,
            rhs,
            ccontext,
            location,
        );
    }

    // Base case: just coerce RHS to match target type ID.
    let rhs_type = expr_type(Some(node_expr_ref(&rhs)))?;
    let rhs_expr = node_to_expr(rhs);
    let result = parse_coerce::coerce_to_target_type(
        mcx,
        Some(pstate),
        rhs_expr,
        rhs_type,
        target_type_id,
        target_typmod,
        ccontext,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?;
    match result {
        Some(e) => Ok(Node::Expr(e)),
        None => {
            if target_is_subscripting {
                Err(ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(alloc::format!(
                        "subscripted assignment to \"{}\" requires type {} but expression is of type {}",
                        target_name, format_type(target_type_id)?, format_type(rhs_type)?
                    ))
                    .errhint("You will need to rewrite or cast the expression.")
                    .errposition(parser_errposition(pstate, location))
                    .into_error())
            } else {
                Err(ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(alloc::format!(
                        "subfield \"{}\" is of type {} but expression is of type {}",
                        target_name, format_type(target_type_id)?, format_type(rhs_type)?
                    ))
                    .errhint("You will need to rewrite or cast the expression.")
                    .errposition(parser_errposition(pstate, location))
                    .into_error())
            }
        }
    }
}

/// `transformAssignmentSubscripts(...)` — helper for
/// `transformAssignmentIndirection`: process container assignment.
/// `next_indirection` is the index of the next cell to process, or `None`.
fn transformAssignmentSubscripts<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    basenode: Option<Node<'mcx>>,
    target_name: &str,
    target_type_id: Oid,
    target_typmod: i32,
    target_collation: Oid,
    subscripts: PgVec<'mcx, NodePtr<'mcx>>,
    indirection: &PgVec<'mcx, NodePtr<'mcx>>,
    next_indirection: Option<usize>,
    rhs: Node<'mcx>,
    ccontext: CoercionContext,
    location: i32,
) -> PgResult<Node<'mcx>> {
    debug_assert!(!subscripts.is_empty());

    // Identify the actual container type involved.
    let mut container_type = target_type_id;
    let mut container_typmod = target_typmod;
    parse_node::transformContainerType(&mut container_type, &mut container_typmod)?;

    // Convert the accumulated A_Indices subscript nodes into the slice the
    // transformContainerSubscripts owner consumes.
    let mut indices: alloc::vec::Vec<types_nodes::rawnodes::A_Indices<'mcx>> =
        alloc::vec::Vec::with_capacity(subscripts.len());
    for n in subscripts.into_iter() {
        match PgBox::into_inner(n).into_a_indices() {
            Some(a) => indices.push(a),
            None => panic!("transformAssignmentSubscripts: non-A_Indices subscript"),
        }
    }

    // basenode is the CaseTestExpr substitution; always present in assignment.
    let container_base = basenode
        .map(node_to_expr)
        .expect("transformAssignmentSubscripts: basenode present");

    // Process subscripts and identify required type for RHS.
    let mut sbsref: SubscriptingRef = parse_node::transformContainerSubscripts(
        mcx,
        pstate,
        container_base,
        container_type,
        container_typmod,
        &indices,
        true,
    )?;

    let type_needed = sbsref.refrestype;
    let typmod_needed = sbsref.reftypmod;

    // Container normally has same collation as its elements, except a domain
    // over a container: use the base type's collation.
    let collation_needed = if container_type == target_type_id {
        target_collation
    } else {
        lsyscache::get_typcollation::call(container_type)?
    };

    // Recurse to create appropriate RHS for container assign.
    let rhs = transformAssignmentIndirection(
        mcx,
        pstate,
        None,
        target_name,
        true,
        type_needed,
        typmod_needed,
        collation_needed,
        indirection,
        next_indirection.unwrap_or(indirection.len()),
        rhs,
        ccontext,
        location,
    )?;

    // Insert the already-coerced RHS into the SubscriptingRef; reset
    // refrestype/reftypmod back to the container type's values.
    sbsref.refassgnexpr = Some(alloc::boxed::Box::new(node_to_expr(rhs)));
    sbsref.refrestype = container_type;
    sbsref.reftypmod = container_typmod;

    let mut result = Expr::SubscriptingRef(sbsref);

    // If target was a domain over container, coerce up to the domain.
    if container_type != target_type_id {
        let resulttype = expr_type(Some(&result))?;
        let coerced = parse_coerce::coerce_to_target_type(
            mcx,
            Some(pstate),
            result,
            resulttype,
            target_type_id,
            target_typmod,
            ccontext,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        result = match coerced {
            Some(e) => e,
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_CANNOT_COERCE)
                    .errmsg(alloc::format!(
                        "cannot cast type {} to {}",
                        format_type(resulttype)?,
                        format_type(target_type_id)?
                    ))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }
        };
    }

    Ok(Node::Expr(result))
}

// ===========================================================================
// checkInsertTargets (parse_target.c:1017).
// ===========================================================================

/// `checkInsertTargets(pstate, cols, &attrnos)` — generate a list of INSERT
/// column targets if not supplied, or validate the supplied names; also return
/// the columns' attribute numbers.
pub fn checkInsertTargets<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    cols: PgVec<'mcx, ResTarget<'mcx>>,
) -> PgResult<(PgVec<'mcx, ResTarget<'mcx>>, PgVec<'mcx, i32>)> {
    let mut attrnos: PgVec<'mcx, i32> = PgVec::new_in(mcx);

    if cols.is_empty() {
        // Generate default column list for INSERT.
        let mut out: PgVec<'mcx, ResTarget<'mcx>> = PgVec::new_in(mcx);
        let rd = pstate
            .p_target_relation
            .as_ref()
            .expect("checkInsertTargets: p_target_relation NULL");
        let numcol = rd.rd_att.attrs.len();

        for i in 0..numcol {
            let att = rd.rd_att.attr(i);
            if att.attisdropped {
                continue;
            }
            let col = ResTarget {
                name: Some(PgString::from_str_in(attname_str(att), mcx)?),
                indirection: PgVec::new_in(mcx),
                val: None,
                location: -1,
            };
            out.try_reserve(1).map_err(|_| mcx.oom(1))?;
            out.push(col);
            attrnos.try_reserve(1).map_err(|_| mcx.oom(1))?;
            attrnos.push((i + 1) as i32);
        }
        Ok((out, attrnos))
    } else {
        // Validate user-supplied INSERT column list.
        let mut wholecols: Option<PgBox<'mcx, types_nodes::bitmapset::Bitmapset<'mcx>>> = None;
        let mut partialcols: Option<PgBox<'mcx, types_nodes::bitmapset::Bitmapset<'mcx>>> = None;

        for col in cols.iter() {
            let name = col.name.as_deref().unwrap_or("");
            let rd = pstate
                .p_target_relation
                .as_ref()
                .expect("checkInsertTargets: p_target_relation NULL");

            // Lookup column name, ereport on failure.
            let attrno = parse_relation::attnameAttNum(rd, name, false)?;
            if attrno == InvalidAttrNumber as i32 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(alloc::format!(
                        "column \"{}\" of relation \"{}\" does not exist",
                        name,
                        rel_name(rd)
                    ))
                    .errposition(parser_errposition(pstate, col.location))
                    .into_error());
            }

            // Check for duplicates, but only of whole columns.
            if col.indirection.is_empty() {
                // whole column; must not have any other assignment.
                if backend_nodes_core::bitmapset::bms_is_member(attrno as i32, wholecols.as_deref())
                    || backend_nodes_core::bitmapset::bms_is_member(attrno as i32, partialcols.as_deref())
                {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DUPLICATE_COLUMN)
                        .errmsg(alloc::format!("column \"{name}\" specified more than once"))
                        .errposition(parser_errposition(pstate, col.location))
                        .into_error());
                }
                wholecols =
                    Some(backend_nodes_core::bitmapset::bms_add_member(mcx, wholecols, attrno as i32)?);
            } else {
                // partial column; must not have any whole assignment.
                if backend_nodes_core::bitmapset::bms_is_member(attrno as i32, wholecols.as_deref()) {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DUPLICATE_COLUMN)
                        .errmsg(alloc::format!("column \"{name}\" specified more than once"))
                        .errposition(parser_errposition(pstate, col.location))
                        .into_error());
                }
                partialcols =
                    Some(backend_nodes_core::bitmapset::bms_add_member(mcx, partialcols, attrno as i32)?);
            }

            attrnos.try_reserve(1).map_err(|_| mcx.oom(1))?;
            attrnos.push(attrno as i32);
        }

        Ok((cols, attrnos))
    }
}

// ===========================================================================
// ExpandColumnRefStar (parse_target.c:1122).
// ===========================================================================

/// `crserr` enum in `ExpandColumnRefStar`.
#[derive(Clone, Copy, PartialEq)]
enum CrsErr {
    NoRte,
    WrongDb,
    TooMany,
}

/// `ExpandColumnRefStar(pstate, cref, make_target_entry)` — transform `foo.*`
/// into a list of expressions or targetlist entries (last/only item is '*' in a
/// ColumnRef).
fn ExpandColumnRefStar<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    cref: &ColumnRef<'mcx>,
    make_target_entry: bool,
) -> PgResult<ExpandResult<'mcx>> {
    let fields = &cref.fields;
    let numnames = fields.len();

    if numnames == 1 {
        // Target item is a bare '*', expand all tables.  Grammar only accepts
        // bare '*' at top level of SELECT.
        debug_assert!(make_target_entry);
        return Ok(ExpandResult::Targets(ExpandAllTables(mcx, pstate, cref.location)?));
    }

    // Target item is relation.*, expand that table.
    let mut nspname: Option<String> = None;
    let mut relname: Option<String> = None;
    let mut resolved: Option<(i32, usize)> = None;
    let mut crserr = CrsErr::NoRte;

    // Give the PreParseColumnRefHook, if any, first shot.
    if let Some(hook) = pstate.p_pre_columnref_hook {
        let cref_clone = cref.clone_in(mcx)?;
        let node = hook(pstate, &cref_clone)?;
        if let Some(node) = node {
            return ExpandRowReference(mcx, pstate, PgBox::into_inner(node), make_target_entry);
        }
    }

    match numnames {
        2 => {
            relname = Some(String::from(str_val(&fields[0])));
            resolved =
                parse_relation::refnameNamespaceItem(pstate, None, relname.as_deref().unwrap(), cref.location, true)?;
        }
        3 => {
            nspname = Some(String::from(str_val(&fields[0])));
            relname = Some(String::from(str_val(&fields[1])));
            resolved = parse_relation::refnameNamespaceItem(
                pstate,
                nspname.as_deref(),
                relname.as_deref().unwrap(),
                cref.location,
                true,
            )?;
        }
        4 => {
            let catname = str_val(&fields[0]);
            // Check the catalog name and then ignore it.
            if catalogname_differs(mcx, catname)? {
                crserr = CrsErr::WrongDb;
            } else {
                nspname = Some(String::from(str_val(&fields[1])));
                relname = Some(String::from(str_val(&fields[2])));
                resolved = parse_relation::refnameNamespaceItem(
                    pstate,
                    nspname.as_deref(),
                    relname.as_deref().unwrap(),
                    cref.location,
                    true,
                )?;
            }
        }
        _ => {
            crserr = CrsErr::TooMany;
        }
    }

    // Give the PostParseColumnRefHook, if any, a chance.
    if let Some(hook) = pstate.p_post_columnref_hook {
        let cref_clone = cref.clone_in(mcx)?;
        // We cheat by passing the RangeTblEntry, not a Var, as the translation.
        let rte_node: Option<NodePtr<'mcx>> = match resolved {
            Some((_, ns_idx)) => {
                let rte = pstate.p_namespace[ns_idx]
                    .p_rte
                    .as_deref()
                    .expect("p_rte set");
                Some(alloc_in(mcx, Node::RangeTblEntry(rte.clone_in(mcx)?))?)
            }
            None => None,
        };
        let node = hook(pstate, &cref_clone, rte_node)?;
        if let Some(node) = node {
            if resolved.is_some() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_AMBIGUOUS_COLUMN)
                    .errmsg(alloc::format!(
                        "column reference \"{}\" is ambiguous",
                        name_list_to_string(fields)
                    ))
                    .errposition(parser_errposition(pstate, cref.location))
                    .into_error());
            }
            return ExpandRowReference(mcx, pstate, PgBox::into_inner(node), make_target_entry);
        }
    }

    // Throw error if no translation found.
    let (levels_up, ns_idx) = match resolved {
        Some(r) => r,
        None => {
            match crserr {
                CrsErr::NoRte => {
                    let rv = make_range_var_node(mcx, nspname.as_deref(), relname.as_deref(), cref.location)?;
                    parse_relation::errorMissingRTE(mcx, pstate, &rv)?;
                    unreachable!("errorMissingRTE always errors")
                }
                CrsErr::WrongDb => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg(alloc::format!(
                            "cross-database references are not implemented: {}",
                            name_list_to_string(fields)
                        ))
                        .errposition(parser_errposition(pstate, cref.location))
                        .into_error());
                }
                CrsErr::TooMany => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(alloc::format!(
                            "improper qualified name (too many dotted names): {}",
                            name_list_to_string(fields)
                        ))
                        .errposition(parser_errposition(pstate, cref.location))
                        .into_error());
                }
            }
        }
    };

    // Expand the nsitem into fields.
    ExpandSingleTable(mcx, pstate, ns_idx, levels_up, cref.location, make_target_entry)
}

/// `makeRangeVar(schemaname, relname, location)` building the raw-grammar
/// `RangeVar` node (catalogname NULL, inh true, relpersistence PERMANENT, no
/// alias) that `errorMissingRTE` consumes.
fn make_range_var_node<'mcx>(
    mcx: Mcx<'mcx>,
    schemaname: Option<&str>,
    relname: Option<&str>,
    location: i32,
) -> PgResult<types_nodes::rawnodes::RangeVar<'mcx>> {
    Ok(types_nodes::rawnodes::RangeVar {
        catalogname: None,
        schemaname: match schemaname {
            Some(s) => Some(PgString::from_str_in(s, mcx)?),
            None => None,
        },
        relname: match relname {
            Some(s) => Some(PgString::from_str_in(s, mcx)?),
            None => None,
        },
        inh: true,
        relpersistence: types_tuple::access::RELPERSISTENCE_PERMANENT as i8,
        alias: None,
        location,
    })
}

/// `NameListToString(fields)` — render a dotted name list (`*` for A_Star).
fn name_list_to_string(fields: &PgVec<'_, NodePtr<'_>>) -> String {
    let mut s = String::new();
    for (idx, n) in fields.iter().enumerate() {
        if idx > 0 {
            s.push('.');
        }
        match n.node_tag() {
            ntag::T_String => s.push_str(n.expect_string().sval.as_str()),
            ntag::T_A_Star => s.push('*'),
            _ => {}
        }
    }
    s
}

/// `strcmp(catname, get_database_name(MyDatabaseId)) != 0`.
fn catalogname_differs(mcx: Mcx<'_>, catname: &str) -> PgResult<bool> {
    let dbname = backend_commands_dbcommands_seams::get_database_name::call(
        mcx,
        globals::my_database_id::call(),
    )?;
    Ok(dbname.as_deref() != Some(catname))
}

// ===========================================================================
// ExpandAllTables (parse_target.c:1296).
// ===========================================================================

/// `ExpandAllTables(pstate, location)` — transform '*' into a list of
/// targetlist entries for each relation visible for unqualified access.
fn ExpandAllTables<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    location: i32,
) -> PgResult<PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>> {
    let mut target: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> = PgVec::new_in(mcx);
    let mut found_table = false;

    // Snapshot the indices of cols-visible nsitems so we can call the
    // &mut-pstate expandNSItemAttrs without aliasing.
    let mut visible: PgVec<'mcx, usize> = PgVec::new_in(mcx);
    for (idx, nsitem) in pstate.p_namespace.iter().enumerate() {
        if !nsitem.p_cols_visible {
            continue;
        }
        debug_assert!(!nsitem.p_lateral_only);
        visible.try_reserve(1).map_err(|_| mcx.oom(1))?;
        visible.push(idx);
    }

    for idx in visible.iter() {
        found_table = true;
        let attrs = parse_relation::expandNSItemAttrs(mcx, pstate, *idx, 0, true, location)?;
        push_te_list(mcx, &mut target, attrs)?;
    }

    // Check for "SELECT *;".
    if !found_table {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("SELECT * with no tables specified is not valid")
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    Ok(target)
}

// ===========================================================================
// ExpandIndirectionStar (parse_target.c:1348).
// ===========================================================================

/// `ExpandIndirectionStar(pstate, ind, make_target_entry, exprKind)` —
/// transform `foo.*` where '*' is the last item in A_Indirection.
fn ExpandIndirectionStar<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    ind: &A_Indirection<'mcx>,
    make_target_entry: bool,
    expr_kind: ParseExprKind,
) -> PgResult<ExpandResult<'mcx>> {
    // Strip off the '*' to create a reference to the rowtype object.
    let mut ind = ind.clone_in(mcx)?;
    let new_len = ind.indirection.len().saturating_sub(1);
    ind.indirection.truncate(new_len);

    // Transform that.
    let expr = parse_expr::transformExpr::call(pstate, Some(Node::A_Indirection(ind)), expr_kind)?
        .expect("ExpandIndirectionStar: NULL expr");

    // Expand the rowtype expression into individual fields.
    ExpandRowReference(mcx, pstate, expr_to_node(expr), make_target_entry)
}

// ===========================================================================
// ExpandSingleTable (parse_target.c:1374).
// ===========================================================================

/// `ExpandSingleTable(pstate, nsitem, sublevels_up, location,
/// make_target_entry)` — `foo` is a simple RTE reference; generate Vars.
/// `nsitem_index` is the index into `pstate.p_namespace`.
fn ExpandSingleTable<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    nsitem_index: usize,
    sublevels_up: i32,
    location: i32,
    make_target_entry: bool,
) -> PgResult<ExpandResult<'mcx>> {
    if make_target_entry {
        // expandNSItemAttrs handles permissions marking.
        let attrs =
            parse_relation::expandNSItemAttrs(mcx, pstate, nsitem_index, sublevels_up, true, location)?;
        return Ok(ExpandResult::Targets(attrs));
    }

    // make_target_entry == false: produce bare Vars.
    let (rtekind, rtindex) = {
        let nsitem = &pstate.p_namespace[nsitem_index];
        let rte = nsitem.p_rte.as_deref().expect("p_rte set");
        (rte.rtekind, nsitem.p_rtindex)
    };

    // expandNSItemVars borrows pstate and the nsitem immutably together.
    let vars = {
        let nsitem = &pstate.p_namespace[nsitem_index];
        parse_relation::expandNSItemVars(mcx, pstate, nsitem, sublevels_up, location, None)?
    };

    // Require read access to the table (handles zero-column relations).  Only
    // for RTE_RELATION; not for a join (its component tables were already
    // marked).
    if rtekind == RTE_RELATION {
        debug_assert!(pstate.p_namespace[nsitem_index].p_perminfo.is_some());
        let perminfo_index = {
            let rte = &pstate.p_rtable[(rtindex - 1) as usize];
            parse_relation::getRTEPermissionInfo(&pstate.p_rteperminfos, rte)?
        };
        pstate.p_rteperminfos[perminfo_index].requiredPerms |= ACL_SELECT;
    }

    // Require read access to each column, and collect the Vars.
    let mut result: PgVec<'mcx, Expr> = PgVec::new_in(mcx);
    for varnode in vars.into_iter() {
        let var = match node_to_var(PgBox::into_inner(varnode)) {
            Some(v) => v,
            None => panic!("ExpandSingleTable: expansion produced a non-Var"),
        };
        parse_relation::markVarForSelectPriv(mcx, pstate, &var)?;
        push_expr(mcx, &mut result, Expr::Var(var))?;
    }

    Ok(ExpandResult::Exprs(result))
}

// ===========================================================================
// ExpandRowReference (parse_target.c:1426).
// ===========================================================================

/// `ExpandRowReference(pstate, expr, make_target_entry)` — `foo` is an
/// arbitrary composite-typed expression.
fn ExpandRowReference<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    expr: Node<'mcx>,
    make_target_entry: bool,
) -> PgResult<ExpandResult<'mcx>> {
    // If the rowtype expression is a whole-row Var, expand the fields as simple
    // Vars.
    if let Some(var) = expr.as_var() {
        if var.varattno == InvalidAttrNumber {
            let (varno, varlevelsup, location) = (var.varno, var.varlevelsup as i32, var.location);
            let nsitem_index = nsitem_index_for_var(pstate, varno, varlevelsup)?;
            return ExpandSingleTable(
                mcx,
                pstate,
                nsitem_index,
                varlevelsup,
                location,
                make_target_entry,
            );
        }
    }

    // Otherwise: generate multiple copies of the expression and do
    // FieldSelects.  Verify it's a composite type and get the tupdesc.
    // get_expr_result_tupdesc(no_error=false) never returns NULL; Assert(tupleDesc).
    let tuple_desc = if let Some(var) = expr.as_var() {
        if var.vartype == RECORDOID {
            expandRecordVariable(mcx, pstate, var, 0)?
        } else {
            unwrap_tupdesc(get_expr_result_tupdesc_node(mcx, &expr)?)
        }
    } else {
        unwrap_tupdesc(get_expr_result_tupdesc_node(mcx, &expr)?)
    };

    // Generate a list of references to the individual fields.
    let num_attrs = tuple_desc.attrs.len();
    let mut targets: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>> = PgVec::new_in(mcx);
    let mut exprs: PgVec<'mcx, Expr> = PgVec::new_in(mcx);

    for i in 0..num_attrs {
        let att = tuple_desc.attr(i);
        if att.attisdropped {
            continue;
        }
        let fselect = FieldSelect {
            arg: Some(alloc::boxed::Box::new(node_to_expr(expr.clone_in(mcx)?))),
            fieldnum: (i + 1) as AttrNumber,
            resulttype: att.atttypid,
            resulttypmod: att.atttypmod,
            resultcollid: att.attcollation,
        };

        if make_target_entry {
            let resno = pstate.p_next_resno as AttrNumber;
            pstate.p_next_resno += 1;
            let te = backend_nodes_core::makefuncs::make_target_entry(
                mcx,
                Expr::FieldSelect(fselect),
                resno,
                Some(attname_str(att)),
                false,
            )?;
            push_te(mcx, &mut targets, te)?;
        } else {
            push_expr(mcx, &mut exprs, Expr::FieldSelect(fselect))?;
        }
    }

    if make_target_entry {
        Ok(ExpandResult::Targets(targets))
    } else {
        Ok(ExpandResult::Exprs(exprs))
    }
}

/// Find the index in `pstate.p_namespace` of the nsitem whose `p_rtindex`
/// matches a whole-row Var's `varno`, at the given nesting depth — equivalent
/// to `GetNSItemByRangeTablePosn` followed by an identity lookup.
fn nsitem_index_for_var(pstate: &ParseState<'_>, varno: i32, sublevels_up: i32) -> PgResult<usize> {
    // C: GetNSItemByRangeTablePosn(pstate, var->varno, var->varlevelsup).
    let mut ps = pstate;
    let mut su = sublevels_up;
    while su > 0 {
        su -= 1;
        ps = ps
            .parentParseState
            .as_deref()
            .expect("nsitem_index_for_var: pstate stack underflow");
    }
    // The nsitem must live in this same level's namespace (whole-row Var path
    // only arises for top-level pstate references in ExpandRowReference).
    if core::ptr::eq(ps, pstate) {
        for (idx, nsitem) in pstate.p_namespace.iter().enumerate() {
            if nsitem.p_rtindex == varno {
                return Ok(idx);
            }
        }
    }
    Err(ereport(ERROR)
        .errmsg_internal("nsitem not found (internal error)")
        .into_error())
}

// ===========================================================================
// expandRecordVariable (parse_target.c:1521).
// ===========================================================================

/// `expandRecordVariable(pstate, var, levelsup)` — get the tuple descriptor for
/// a Var of type RECORD, drilling down to the ultimate defining expression.
pub fn expandRecordVariable<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    var: &Var,
    levelsup: i32,
) -> PgResult<types_tuple::heaptuple::TupleDescData<'mcx>> {
    debug_assert!(var.vartype == RECORDOID);

    let netlevelsup = var.varlevelsup as i32 + levelsup;
    let rte = parse_relation::GetRTEByRangeTablePosn(pstate, var.varno, netlevelsup);
    let attnum = var.varattno;

    if attnum == InvalidAttrNumber {
        // Whole-row reference to an RTE, so expand the known fields.
        let mut names: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        let mut vars: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        parse_relation::expandRTE(
            mcx,
            rte,
            var.varno,
            0,
            var.varreturningtype,
            var.location,
            false,
            Some(&mut names),
            Some(&mut vars),
        )?;

        let mut tuple_desc =
            backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, vars.len() as i32)?;
        debug_assert!(names.len() == vars.len());
        for (i0, (lname, lvar)) in names.iter().zip(vars.iter()).enumerate() {
            let i = (i0 + 1) as AttrNumber;
            let label = str_val(lname);
            let varnode = node_expr_ref(lvar);
            backend_access_common_tupdesc::TupleDescInitEntry(
                &mut tuple_desc,
                i,
                Some(label),
                expr_type(Some(varnode))?,
                expr_typmod(Some(varnode))?,
                0,
            )?;
            backend_access_common_tupdesc::TupleDescInitEntryCollation(
                &mut tuple_desc,
                i,
                expr_collation(Some(varnode))?,
            )?;
        }
        return Ok(tuple_desc);
    }

    // default if we can't drill down: the var itself.
    let mut drilled: Option<Node<'mcx>> = None;

    match rte.rtekind {
        RTE_RELATION | RTE_VALUES | RTE_NAMEDTUPLESTORE | RTE_RESULT => {
            // Should not occur (a column of these shouldn't have type RECORD);
            // fall through and fail at the bottom.
        }
        RTE_SUBQUERY => {
            let subquery = rte.subquery.as_deref().expect("subquery set");
            let ste = parse_relation::get_tle_by_resno(&subquery.targetList, attnum);
            let ste = match ste {
                Some(ste) if !ste.resjunk => ste,
                _ => {
                    return Err(ereport(ERROR)
                        .errmsg_internal(alloc::format!(
                            "subquery {} does not have attribute {}",
                            rte_eref_aliasname(rte),
                            attnum
                        ))
                        .into_error())
                }
            };
            let expr = ste.expr.as_deref().expect("ste->expr set");
            if let Some(inner_var) = expr.as_var() {
                // Recurse into the sub-select with an additional ParseState
                // level (to keep step with varlevelsup); the subquery RTE might
                // be from an outer query level.
                let mut ps: &ParseState = pstate;
                for _ in 0..netlevelsup {
                    ps = ps.parentParseState.as_deref().expect("parentParseState set");
                }
                let inner_var = inner_var.clone();
                let mypstate = make_fake_pstate(mcx, ps, &subquery.rtable)?;
                return expandRecordVariable(mcx, &mypstate, &inner_var, 0);
            }
            drilled = Some(Node::Expr(expr.clone()));
        }
        RTE_JOIN => {
            debug_assert!(attnum > 0 && (attnum as usize) <= rte.joinaliasvars.len());
            let expr = &rte.joinaliasvars[(attnum - 1) as usize];
            // We intentionally don't strip implicit coercions here.
            if let Some(inner_var) = node_expr_ref(expr).as_var() {
                let inner_var = inner_var.clone();
                return expandRecordVariable(mcx, pstate, &inner_var, netlevelsup);
            }
            drilled = Some(expr.clone_in(mcx)?);
            // (expr here is a Node from joinaliasvars; Node::clone_in applies.)
        }
        RTE_FUNCTION => {
            // Couldn't get here unless a function declared a RECORD result
            // column, which is not allowed.
        }
        RTE_TABLEFUNC => {
            // Table function cannot have RECORD-type columns.
        }
        RTE_CTE => {
            if !rte.self_reference {
                let cte = parse_relation::GetCTEForRTE(mcx, pstate, rte, netlevelsup)?;
                let tl = get_cte_target_list(&cte);
                let ste = parse_relation::get_tle_by_resno(tl, attnum);
                let ste = match ste {
                    Some(ste) if !ste.resjunk => ste,
                    _ => {
                        return Err(ereport(ERROR)
                            .errmsg_internal(alloc::format!(
                                "CTE {} does not have attribute {}",
                                rte_eref_aliasname(rte),
                                attnum
                            ))
                            .into_error())
                    }
                };
                let expr = ste.expr.as_deref().expect("ste->expr set");
                if let Some(inner_var) = expr.as_var() {
                    let inner_var = inner_var.clone();
                    let mut ps: &ParseState = pstate;
                    let total = rte.ctelevelsup as i32 + netlevelsup;
                    for _ in 0..total {
                        ps = ps.parentParseState.as_deref().expect("parentParseState set");
                    }
                    let ctequery_rtable = match cte.ctequery.as_deref().and_then(|n| n.as_query()) {
                        Some(q) => clone_rtable(&q.rtable, mcx)?,
                        None => panic!("expandRecordVariable: cte->ctequery is not a Query"),
                    };
                    let mypstate = make_fake_pstate_owned(mcx, ps, ctequery_rtable)?;
                    return expandRecordVariable(mcx, &mypstate, &inner_var, 0);
                }
                drilled = Some(Node::Expr(expr.clone()));
            }
        }
        RTE_GROUP => {
            // Couldn't get here: the RTE_GROUP RTE has not been added.
        }
    }

    // We now have an expression we can't expand any more.
    let expr_node = match drilled {
        Some(n) => n,
        None => Node::Expr(Expr::Var(var.clone())),
    };
    Ok(unwrap_tupdesc(get_expr_result_tupdesc_node(mcx, &expr_node)?))
}

/// `get_expr_result_tupdesc(expr, false)` over a raw `Node` expression.
fn get_expr_result_tupdesc_node<'mcx>(
    mcx: Mcx<'mcx>,
    expr: &Node<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    backend_utils_fmgr_funcapi::result_type::get_expr_result_tupdesc(mcx, Some(expr), false)
}

/// `Assert(tupleDesc)` — get_expr_result_tupdesc with no_error=false never
/// returns NULL.
fn unwrap_tupdesc<'mcx>(
    td: types_tuple::heaptuple::TupleDesc<'mcx>,
) -> types_tuple::heaptuple::TupleDescData<'mcx> {
    PgBox::into_inner(td.expect("get_expr_result_tupdesc returned NULL"))
}

/// `list_copy(rtable)` of a range table into `mcx`.
fn clone_rtable<'mcx>(
    rtable: &PgVec<'_, RangeTblEntry<'_>>,
    mcx: Mcx<'mcx>,
) -> PgResult<PgVec<'mcx, RangeTblEntry<'mcx>>> {
    let mut out: PgVec<'mcx, RangeTblEntry<'mcx>> = PgVec::new_in(mcx);
    out.try_reserve(rtable.len()).map_err(|_| mcx.oom(rtable.len()))?;
    for rte in rtable.iter() {
        out.push(rte.clone_in(mcx)?);
    }
    Ok(out)
}

/// Build the C `mypstate` fake `ParseState` from `expandRecordVariable`:
/// `mypstate.parentParseState = pstate; mypstate.p_rtable = subquery->rtable;`
/// with the rest of the struct zeroed.  The C borrows `pstate` as the parent;
/// the owned model holds the parent chain by value, so the (read-only) walked
/// ancestor chain is cloned into `mcx`.  Only `p_rtable` and
/// `parentParseState` are populated — the recursion reads nothing else.
fn make_fake_pstate<'mcx>(
    mcx: Mcx<'mcx>,
    parent: &ParseState<'mcx>,
    rtable: &PgVec<'_, RangeTblEntry<'_>>,
) -> PgResult<ParseState<'mcx>> {
    make_fake_pstate_owned(mcx, parent, clone_rtable(rtable, mcx)?)
}

/// As [`make_fake_pstate`], but the caller already owns the cloned rtable.
fn make_fake_pstate_owned<'mcx>(
    mcx: Mcx<'mcx>,
    parent: &ParseState<'mcx>,
    rtable: PgVec<'mcx, RangeTblEntry<'mcx>>,
) -> PgResult<ParseState<'mcx>> {
    let mut mypstate = ParseState::new(mcx)?;
    mypstate.parentParseState = Some(PgBox::new_in(clone_pstate_chain(mcx, parent)?, mcx));
    mypstate.p_rtable = rtable;
    Ok(mypstate)
}

/// Clone the read-only spine of a `ParseState` for the fake-pstate recursion:
/// the `p_rtable` and (recursively) the `parentParseState`.  No other field is
/// read by `GetRTEByRangeTablePosn`/`GetCTEForRTE`.
fn clone_pstate_chain<'mcx>(
    mcx: Mcx<'mcx>,
    src: &ParseState<'mcx>,
) -> PgResult<ParseState<'mcx>> {
    let mut out = ParseState::new(mcx)?;
    out.p_rtable = clone_rtable(&src.p_rtable, mcx)?;
    out.parentParseState = match src.parentParseState.as_deref() {
        Some(p) => Some(PgBox::new_in(clone_pstate_chain(mcx, p)?, mcx)),
        None => None,
    };
    Ok(out)
}

// ===========================================================================
// FigureColname / FigureIndexColname / FigureColnameInternal
// (parse_target.c:1712).
// ===========================================================================

/// `FigureColname(node)` — guess a suitable column name; defaults to
/// "?column?".
pub fn FigureColname(node: Option<&Node<'_>>) -> Option<String> {
    let mut name: Option<String> = None;
    let _ = FigureColnameInternal(node, &mut name);
    Some(name.unwrap_or_else(|| String::from("?column?")))
}

/// `FigureIndexColname(node)` — like `FigureColname`, but returns `None` if no
/// good name can be picked.
pub fn FigureIndexColname(node: Option<&Node<'_>>) -> Option<String> {
    let mut name: Option<String> = None;
    let _ = FigureColnameInternal(node, &mut name);
    name
}

/// `FigureColnameInternal(node, name)` — internal workhorse.  Return value is
/// the strength of confidence (0/1/2); when nonzero, `*name` is set.
fn FigureColnameInternal(node: Option<&Node<'_>>, name: &mut Option<String>) -> i32 {
    let mut strength = 0;

    let Some(node) = node else {
        return strength;
    };

    match node.node_tag() {
        ntag::T_ColumnRef => {
            let cref = node.expect_columnref();
            // find last field name, if any, ignoring "*".
            let mut fname: Option<String> = None;
            for f in cref.fields.iter() {
                if is_string(f) {
                    fname = Some(String::from(str_val(f)));
                }
            }
            if let Some(fname) = fname {
                *name = Some(fname);
                return 2;
            }
        }
        ntag::T_A_Indirection => {
            let ind = node.expect_a_indirection();
            // find last field name, if any, ignoring "*" and subscripts.
            let mut fname: Option<String> = None;
            for f in ind.indirection.iter() {
                if is_string(f) {
                    fname = Some(String::from(str_val(f)));
                }
            }
            if let Some(fname) = fname {
                *name = Some(fname);
                return 2;
            }
            return FigureColnameInternal(ind.arg.as_deref(), name);
        }
        ntag::T_FuncCall => {
            let fc = node.expect_funccall();
            // strVal(llast(funcname)).
            if let Some(last) = fc.funcname.last() {
                *name = Some(String::from(str_val(last)));
            }
            return 2;
        }
        ntag::T_A_Expr => {
            let ae = node.expect_a_expr();
            if ae.kind == types_nodes::rawnodes::A_Expr_Kind::AEXPR_NULLIF {
                // make nullif() act like a regular function.
                *name = Some(String::from("nullif"));
                return 2;
            }
        }
        ntag::T_TypeCast => {
            let tc = node.expect_typecast();
            strength = FigureColnameInternal(tc.arg.as_deref(), name);
            if strength <= 1 {
                if let Some(tn) = tc.typeName.as_deref() {
                    if let Some(last) = tn.names.last() {
                        *name = Some(String::from(str_val(last)));
                        return 1;
                    }
                }
            }
        }
        ntag::T_CollateClause => {
            let cc = node.expect_collateclause();
            return FigureColnameInternal(cc.arg.as_deref(), name);
        }
        ntag::T_GroupingFunc => {
            // make GROUPING() act like a regular function.
            *name = Some(String::from("grouping"));
            return 2;
        }
        ntag::T_SubLink => {
            let sublink = node.expect_sublink();
            match sublink.sub_link_type {
                SubLinkType::Exists => {
                    *name = Some(String::from("exists"));
                    return 2;
                }
                SubLinkType::Array => {
                    *name = Some(String::from("array"));
                    return 2;
                }
                SubLinkType::Expr => {
                    // Get column name of the subquery's single target.  The
                    // subquery has probably already been transformed; check.
                    if let Some(query) = sublink.subselect.as_deref().and_then(|n| n.as_query()) {
                        if let Some(te) = query.targetList.first() {
                            if let Some(resname) = te.resname.as_deref() {
                                *name = Some(String::from(resname));
                                return 2;
                            }
                        }
                    }
                }
                SubLinkType::MultiExpr
                | SubLinkType::All
                | SubLinkType::Any
                | SubLinkType::RowCompare
                | SubLinkType::Cte => {
                    // operator-like nodes have no names.
                }
            }
        }
        ntag::T_CaseExpr => {
            let ce = node.expect_caseexpr();
            strength = FigureColnameInternal(ce.defresult.as_deref(), name);
            if strength <= 1 {
                *name = Some(String::from("case"));
                return 1;
            }
        }
        ntag::T_A_ArrayExpr => {
            // make ARRAY[] act like a function.
            *name = Some(String::from("array"));
            return 2;
        }
        ntag::T_RowExpr => {
            // make ROW() act like a function.
            *name = Some(String::from("row"));
            return 2;
        }
        ntag::T_CoalesceExpr => {
            *name = Some(String::from("coalesce"));
            return 2;
        }
        ntag::T_MinMaxExpr => {
            let mm = node.expect_minmaxexpr();
            match mm.op {
                MinMaxOp::IS_GREATEST => {
                    *name = Some(String::from("greatest"));
                    return 2;
                }
                MinMaxOp::IS_LEAST => {
                    *name = Some(String::from("least"));
                    return 2;
                }
            }
        }
        ntag::T_SQLValueFunction => {
            let svf = node.expect_sqlvaluefunction();
            match svf.op {
                SQLValueFunctionOp::SVFOP_CURRENT_DATE => {
                    *name = Some(String::from("current_date"));
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_TIME
                | SQLValueFunctionOp::SVFOP_CURRENT_TIME_N => {
                    *name = Some(String::from("current_time"));
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP
                | SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP_N => {
                    *name = Some(String::from("current_timestamp"));
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_LOCALTIME
                | SQLValueFunctionOp::SVFOP_LOCALTIME_N => {
                    *name = Some(String::from("localtime"));
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP
                | SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP_N => {
                    *name = Some(String::from("localtimestamp"));
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_ROLE => {
                    *name = Some(String::from("current_role"));
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_USER => {
                    *name = Some(String::from("current_user"));
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_USER => {
                    *name = Some(String::from("user"));
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_SESSION_USER => {
                    *name = Some(String::from("session_user"));
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_CATALOG => {
                    *name = Some(String::from("current_catalog"));
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_SCHEMA => {
                    *name = Some(String::from("current_schema"));
                    return 2;
                }
            }
        }
        ntag::T_XmlExpr => {
            let xe = node.expect_xmlexpr();
            match xe.op {
                XmlExprOp::IS_XMLCONCAT => {
                    *name = Some(String::from("xmlconcat"));
                    return 2;
                }
                XmlExprOp::IS_XMLELEMENT => {
                    *name = Some(String::from("xmlelement"));
                    return 2;
                }
                XmlExprOp::IS_XMLFOREST => {
                    *name = Some(String::from("xmlforest"));
                    return 2;
                }
                XmlExprOp::IS_XMLPARSE => {
                    *name = Some(String::from("xmlparse"));
                    return 2;
                }
                XmlExprOp::IS_XMLPI => {
                    *name = Some(String::from("xmlpi"));
                    return 2;
                }
                XmlExprOp::IS_XMLROOT => {
                    *name = Some(String::from("xmlroot"));
                    return 2;
                }
                XmlExprOp::IS_XMLSERIALIZE => {
                    *name = Some(String::from("xmlserialize"));
                    return 2;
                }
                XmlExprOp::IS_DOCUMENT => {
                    // nothing.
                }
            }
        }
        // The remaining C cases (T_MergeSupportFunc, T_XmlSerialize, and the
        // SQL/JSON node family T_JsonParseExpr/T_JsonScalarExpr/
        // T_JsonSerializeExpr/T_JsonObjectConstructor/T_JsonArrayConstructor/
        // T_JsonArrayQueryConstructor/T_JsonObjectAgg/T_JsonArrayAgg/
        // T_JsonFuncExpr) are raw-grammar node kinds not yet modeled in the
        // `Node` enum, so they are not constructible here and fall through to
        // the default (strength 0) exactly as an absent node does in C.
        _ => {}
    }

    strength
}

// ===========================================================================
// Node/Expr conversion helpers.
// ===========================================================================

/// Move an `Expr` into a raw `Node` wrapper (`Node::Expr`).
fn expr_to_node<'mcx>(e: Expr) -> Node<'mcx> {
    Node::Expr(e)
}

/// `(Node *) expr` — unwrap a `Node::Expr` to its inner `Expr`.
fn node_to_expr<'mcx>(n: Node<'mcx>) -> Expr {
    match n {
        Node::Expr(e) => e,
        other => panic!("node_to_expr: non-Expr node ({other:?}) where an Expr was required"),
    }
}

/// Borrow the inner `Expr` of a `Node::Expr` for an `exprType`/`exprTypmod`
/// inspection.
fn node_expr_ref<'a, 'mcx>(n: &'a Node<'mcx>) -> &'a Expr {
    match n {
        Node::Expr(e) => e,
        _ => panic!("node_expr_ref: non-Expr node where an Expr was required"),
    }
}

/// Extract a `Var` from a `Node::Expr(Expr::Var)`.
fn node_to_var(n: Node<'_>) -> Option<Var> {
    n.into_var()
}

// ===========================================================================
// Inward seam installer (owner of `backend-parser-target-seams`).
// ===========================================================================

/// Seam body for `backend_parser_target_seams::transform_target_entry`, the
/// `transformTargetEntry` leg parse_clause.c reaches across the
/// parse_target ⇆ parse_clause cycle. The consumer always supplies an
/// already-transformed `expr` (it ran `transformExpr` itself), so it is passed
/// by value as `Some(expr)`; `node` is the original parse node (cloned for the
/// `FigureColname` read inside `transformTargetEntry`).
fn transform_target_entry_seam<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    node: &Node<'mcx>,
    expr: Expr,
    expr_kind: ParseExprKind,
    colname: Option<&str>,
    resjunk: bool,
) -> PgResult<types_nodes::primnodes::TargetEntry<'mcx>> {
    transformTargetEntry(
        mcx,
        pstate,
        Some(node.clone_in(mcx)?),
        Some(expr),
        expr_kind,
        colname.map(String::from),
        resjunk,
    )
}

/// Seam body for `backend_parser_target_seams::FigureColname` — the column-name
/// heuristic parse_clause.c's `transformRangeFunction` reaches for its
/// per-function alias names. The C `FigureColname` always returns a non-NULL
/// `char *` (defaulting to `"?column?"`); the owned `FigureColname` mirrors
/// that, so the seam allocates the result into the caller's `mcx`.
fn figure_colname_seam<'mcx>(
    mcx: Mcx<'mcx>,
    node: &Node<'mcx>,
) -> PgResult<mcx::PgString<'mcx>> {
    let name = FigureColname(Some(node)).unwrap_or_else(|| String::from("?column?"));
    mcx::PgString::from_str_in(&name, mcx)
}

/// Seam adapter for `expand_record_variable` (declared in
/// `backend-parser-relation-seams` because it is `::call`ed by
/// `ParseComplexProjection` in parse_func.c, but the C body
/// `expandRecordVariable` lives in parse_target.c, this crate). Adapts the
/// owner's bare-`TupleDescData` return to the seam's `TupleDesc`
/// (= `Option<PgBox<TupleDescData>>`) shape: the C function never returns NULL
/// (`get_expr_result_tupdesc(..., false)` raises instead), so this always yields
/// `Some`.
fn expand_record_variable_seam<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    var: &Var,
    levelsup: i32,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    let td = expandRecordVariable(mcx, pstate, var, levelsup)?;
    Ok(Some(mcx::alloc_in(mcx, td)?))
}

/// Install this crate's inward seams (owner of `backend-parser-target-seams`,
/// which maps to `parse_target.c`).
pub fn init_seams() {
    backend_parser_target_seams::transform_target_entry::set(transform_target_entry_seam);
    backend_parser_target_seams::FigureColname::set(figure_colname_seam);
    // expandRecordVariable's C body lives here (parse_target.c); the seam is
    // declared in relation-seams only because parse_func.c is its caller.
    backend_parser_relation_seams::expand_record_variable::set(expand_record_variable_seam);
}

#[cfg(test)]
mod tests;
