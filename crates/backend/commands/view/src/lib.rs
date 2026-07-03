//! view.c CREATE VIEW lane. CREATE OR REPLACE, WITH CHECK OPTION, reloptions,
//! and temp views are loud panics.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::catalog::{RELPERSISTENCE_PERMANENT, RELPERSISTENCE_UNLOGGED};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR};
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind};
use types_nodes::rawnodes::{ColumnDef, CreateStmt, OnCommitAction, TypeName, ViewCheckOption, ViewStmt};
use types_nodes::{Node, RawStmt};
use types_portal::QueryEnvHandle;
use types_rel::RELKIND_VIEW;

// DefineView (view.c).
pub fn DefineView<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ViewStmt<'mcx>,
    query_string: &str,
    stmt_location: i32,
    stmt_len: i32,
) -> PgResult<Oid> {
    let rawstmt = RawStmt {
        stmt: stmt.query,
        stmt_location,
        stmt_len,
    };
    let mut viewParse =
        parser_analyze::parse_analyze_fixedparams(mcx, &rawstmt, query_string, &[], QueryEnvHandle::NULL)?;

    if viewParse.utilityStmt.is_some() {
        return Err(feature_not_supported("views must not contain SELECT INTO"));
    }
    if viewParse.commandType != CmdType::CMD_SELECT {
        return Err(Box::new(PgError::error("unexpected parse analysis result")));
    }
    if viewParse.hasModifyingCTE {
        return Err(feature_not_supported(
            "views must not contain data-modifying statements in WITH",
        ));
    }
    if stmt.withCheckOption != ViewCheckOption::NO_CHECK_OPTION {
        panic!("DefineView (view.c): WITH CHECK OPTION lane unported (updatable views)");
    }
    if !stmt.options.is_nil() {
        panic!("DefineView (view.c): view reloptions lane unported");
    }

    if !stmt.aliases.is_nil() {
        let mut alias_iter = stmt.aliases.iter();
        let mut next_alias = alias_iter.next();
        for item in viewParse.targetList.iter() {
            if next_alias.is_none() {
                break;
            }
            let te = item.as_target_entry().expect("targetList entry");
            if te.resjunk {
                continue;
            }
            let alias = next_alias.expect("alias").as_string().expect("alias is a String").sval;
            // SAFETY: tree is statement-owned; no derived refs live.
            unsafe {
                item.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.resname = Some(alias))
                    .expect("TargetEntry");
            }
            next_alias = alias_iter.next();
        }
        if next_alias.is_some() {
            return Err(Box::new(
                PgError::error("CREATE VIEW specifies more column names than columns")
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR),
            ));
        }
    }

    let view = stmt.view.expect("ViewStmt.view");
    if view.relpersistence == RELPERSISTENCE_UNLOGGED {
        return Err(Box::new(
            PgError::error("views cannot be unlogged because they do not have storage")
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }
    if view.relpersistence != RELPERSISTENCE_PERMANENT {
        panic!("DefineView (view.c): temp view lane unported");
    }
    if isQueryUsingTempRelation(&viewParse)? {
        panic!("DefineView (view.c): implicit temp view promotion unported");
    }

    DefineVirtualRelation(mcx, stmt, &mut viewParse, query_string)
}

// DefineVirtualRelation (view.c), create lane; OR REPLACE is loud.
fn DefineVirtualRelation<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ViewStmt<'mcx>,
    viewParse: &mut Query<'mcx>,
    query_string: &str,
) -> PgResult<Oid> {
    let mut attrList = NodeList::nil();
    for item in viewParse.targetList.iter() {
        let te = item.as_target_entry().expect("targetList entry");
        if te.resjunk {
            continue;
        }
        let type_oid = parse_expr::expr_type(te.expr);
        let coll_oid = parse_expr::expr_collation(te.expr);
        let mut tn = Node::build::<TypeName>(mcx)?;
        tn.typeOid = type_oid;
        tn.typemod = parse_expr::expr_typmod(te.expr);
        tn.location = -1;
        let mut def = Node::build::<ColumnDef>(mcx)?;
        def.colname = te.resname;
        def.typeName = Some(tn.seal());
        def.inhcount = 0;
        def.is_local = true;
        def.collOid = coll_oid;
        def.location = -1;
        if lsyscache::type_is_collatable(type_oid)? {
            if coll_oid == InvalidOid {
                return Err(Box::new(
                    PgError::error(format!(
                        "could not determine which collation to use for view column \"{}\"",
                        te.resname.unwrap_or("")
                    ))
                    .with_sqlstate(types_error::ERRCODE_INDETERMINATE_COLLATION)
                    .with_hint("Use the COLLATE clause to set the collation explicitly."),
                ));
            }
        } else {
            debug_assert!(coll_oid == InvalidOid);
        }
        attrList.lappend(mcx, def.seal())?;
    }

    if stmt.replace {
        panic!("DefineVirtualRelation (view.c): CREATE OR REPLACE VIEW lane unported");
    }

    let mut createStmt = Node::build::<CreateStmt>(mcx)?;
    createStmt.relation = stmt.view;
    createStmt.tableElts = attrList;
    createStmt.inhRelations = NodeList::nil();
    createStmt.constraints = NodeList::nil();
    createStmt.options = NodeList::nil();
    createStmt.oncommit = OnCommitAction::ONCOMMIT_NOOP;
    createStmt.tablespacename = None;
    createStmt.if_not_exists = false;

    let view_oid = tablecmds::DefineRelation(mcx, &createStmt, RELKIND_VIEW, InvalidOid, query_string)?;
    xact::CommandCounterIncrement()?;
    StoreViewQuery(mcx, view_oid, viewParse, stmt.replace)?;
    Ok(view_oid)
}

// StoreViewQuery -> DefineViewRules (view.c): the ON SELECT _RETURN rule.
pub fn StoreViewQuery<'mcx>(
    mcx: Mcx<'mcx>,
    viewOid: Oid,
    viewParse: &mut Query<'mcx>,
    replace: bool,
) -> PgResult<()> {
    let query_node = Node::mk(mcx, core::mem::take(viewParse))?;
    let action = NodeList::make1(mcx, query_node)?;
    rewrite_define::DefineQueryRewrite(
        mcx,
        rewrite_define::ViewSelectRuleName,
        viewOid,
        None,
        CmdType::CMD_SELECT,
        true,
        replace,
        action,
    )?;
    Ok(())
}

// isQueryUsingTempRelation (rewriteManip.c), view-creation slice: plain
// relation and subquery RTEs; anything else in the tree that could hide a
// temp relation is unreachable here (CTEs/sublinks hit outfuncs' loud arms
// before this matters).
fn isQueryUsingTempRelation(query: &Query<'_>) -> PgResult<bool> {
    for item in query.rtable.iter() {
        let rte = item.as_range_tbl_entry().expect("rtable entry");
        match rte.rtekind {
            RTEKind::RTE_RELATION => {
                if lsyscache::get_rel_persistence(rte.relid)? as u8
                    != RELPERSISTENCE_PERMANENT
                {
                    return Ok(true);
                }
            }
            RTEKind::RTE_SUBQUERY => {
                if isQueryUsingTempRelation(rte.subquery.expect("subquery"))? {
                    return Ok(true);
                }
            }
            _ => {}
        }
    }
    Ok(false)
}

#[cold]
#[inline(never)]
fn feature_not_supported(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}
