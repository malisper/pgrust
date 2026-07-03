// parse_cte.c, non-recursive WITH slice: RECURSIVE, SEARCH/CYCLE, and
// data-modifying CTEs are loud panics naming their lanes.
#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::{
    ERRCODE_DUPLICATE_ALIAS, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_COLUMN_REFERENCE,
    ErrorLocation, PgError, PgResult, ERROR,
};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::CommonTableExpr;
use types_nodes::primnodes::TargetEntry;
use types_nodes::{Node, NodeList, NodeTag};

use parser_small1::{parser_errposition, ParseState};

pub fn transformWithClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    with: Node<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let wc = with.as_with_clause().expect("withClause is a WithClause");
    debug_assert!(pstate.p_ctenamespace.is_nil());
    debug_assert!(pstate.p_future_ctes.is_nil());

    for (i, cte_node) in wc.ctes.iter().enumerate() {
        {
            let cte = cte_node.as_common_table_expr().expect("WITH list cell");
            for later in wc.ctes.iter().skip(i + 1) {
                let cte2 = later.as_common_table_expr().expect("WITH list cell");
                if cte2.ctename == cte.ctename {
                    return Err(duplicate_cte_name(
                        pstate,
                        cte2.ctename.unwrap_or(""),
                        cte2.location,
                    ));
                }
            }
            if cte.ctequery.expect("CTE has no query").node_tag() != NodeTag::T_SelectStmt {
                panic!(
                    "transformWithClause (parse_cte.c): data-modifying CTE \"{}\"; \
                     DML WITH lane",
                    cte.ctename.unwrap_or("")
                );
            }
        }
        // SAFETY: parser-owned tree under analysis; no live derived refs.
        unsafe {
            cte_node.with_mut::<CommonTableExpr, _>(|c| {
                c.cterecursive = false;
                c.cterefcount = 0;
            })
        };
    }

    if wc.recursive {
        panic!("transformWithClause (parse_cte.c): WITH RECURSIVE; recursive CTE lane");
    }

    // C list_copy: fresh cells, shared CommonTableExpr nodes.
    pstate.p_future_ctes = wc.ctes.clone_in(mcx)?;
    for (i, cte_node) in wc.ctes.iter().enumerate() {
        analyzeCTE(mcx, pstate, cte_node)?;
        pstate.p_ctenamespace.lappend(mcx, cte_node)?;
        let mut rest = NodeList::nil();
        for later in wc.ctes.iter().skip(i + 1) {
            rest.lappend(mcx, later)?;
        }
        pstate.p_future_ctes = rest;
    }

    pstate.p_ctenamespace.clone_in(mcx)
}

fn analyzeCTE<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    cte_node: Node<'mcx>,
) -> PgResult<()> {
    let (ctequery, location) = {
        let cte = cte_node.as_common_table_expr().expect("WITH list cell");
        if cte.search_clause.is_some() || cte.cycle_clause.is_some() {
            panic!("analyzeCTE (parse_cte.c): SEARCH/CYCLE clause; recursive CTE lane");
        }
        (cte.ctequery.expect("CTE has no query"), cte.location)
    };
    debug_assert!(ctequery.node_tag() != NodeTag::T_Query);

    let mut query = crate::parse_sub_analyze(mcx, ctequery, pstate, Some(cte_node), false, true)?;

    if query.utilityStmt.is_some() {
        return Err(elog_error("unexpected utility statement in WITH"));
    }
    if query.commandType != CmdType::CMD_SELECT && pstate.parentParseState.is_some() {
        return Err(Box::new(
            elog::ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(
                    "WITH clause containing a data-modifying statement must be at the top level"
                        .to_string(),
                )
                .errposition(parser_errposition(
                    pstate,
                    location,
                    mbutils::GetDatabaseEncoding(),
                ))
                .into_error()
                .with_error_location(ErrorLocation::new("parse_cte.c", 0, "analyzeCTE")),
        ));
    }

    query.canSetTag = false;
    let query_node = Node::mk(mcx, query)?;
    // SAFETY: parser-owned tree under analysis; no live derived refs.
    unsafe { cte_node.with_mut::<CommonTableExpr, _>(|c| c.ctequery = Some(query_node)) };

    analyzeCTETargetList(mcx, pstate, cte_node)
}

fn analyzeCTETargetList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    cte_node: Node<'mcx>,
) -> PgResult<()> {
    let (tlist, aliascolnames, ctename, location) = {
        let cte = cte_node.as_common_table_expr().expect("WITH list cell");
        debug_assert!(cte.ctecolnames.is_nil());
        debug_assert!(!cte.cterecursive);
        let q = cte
            .ctequery
            .expect("analyzed ctequery")
            .as_query()
            .expect("analyzed ctequery is a Query");
        // GetCTETargetList's returningList arm is dead (DML CTEs are loud).
        debug_assert!(q.commandType == CmdType::CMD_SELECT);
        (
            q.targetList.clone_in(mcx)?,
            cte.aliascolnames.clone_in(mcx)?,
            cte.ctename.unwrap_or(""),
            cte.location,
        )
    };

    let numaliases = aliascolnames.len() as i32;
    let mut colnames = aliascolnames;
    let mut ctypes = types_nodes::list::OidList::nil();
    let mut ctypmods = types_nodes::list::IntList::nil();
    let mut ccolls = types_nodes::list::OidList::nil();
    let mut varattno: i32 = 0;
    for te_node in &tlist {
        let te = te_node.as_variant::<TargetEntry>().expect("tlist cell");
        if te.resjunk {
            continue;
        }
        varattno += 1;
        debug_assert_eq!(varattno, te.resno as i32);
        if varattno > numaliases {
            let name = te.resname.expect("non-junk tlist entry has resname");
            colnames.lappend(mcx, Node::mk_string(mcx, name)?)?;
        }
        ctypes.lappend(mcx, parse_expr::expr_type(te.expr))?;
        ctypmods.lappend(mcx, parse_expr::expr_typmod(te.expr))?;
        ccolls.lappend(mcx, parse_expr::expr_collation(te.expr))?;
    }
    if varattno < numaliases {
        return Err(Box::new(
            elog::ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                .errmsg(format!(
                    "WITH query \"{ctename}\" has {varattno} columns available but \
                     {numaliases} columns specified"
                ))
                .errposition(parser_errposition(
                    pstate,
                    location,
                    mbutils::GetDatabaseEncoding(),
                ))
                .into_error()
                .with_error_location(ErrorLocation::new("parse_cte.c", 0, "analyzeCTETargetList")),
        ));
    }

    // SAFETY: parser-owned tree under analysis; no live derived refs.
    unsafe {
        cte_node.with_mut::<CommonTableExpr, _>(|c| {
            c.ctecolnames = colnames;
            c.ctecoltypes = ctypes;
            c.ctecoltypmods = ctypmods;
            c.ctecolcollations = ccolls;
        })
    };
    Ok(())
}

#[cold]
#[inline(never)]
fn duplicate_cte_name(pstate: &ParseState<'_, '_>, name: &str, location: i32) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_ALIAS)
            .errmsg(format!("WITH query name \"{name}\" specified more than once"))
            .errposition(parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_cte.c", 0, "transformWithClause")),
    )
}

#[cold]
#[inline(never)]
fn elog_error(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()))
}
