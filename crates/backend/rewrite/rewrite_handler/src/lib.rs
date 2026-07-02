#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, QuerySource, RTEKind, RangeTblEntry};
use types_rel::{
    AccessShareLock, NoLock, RowShareLock, LOCKMODE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_VIEW,
};

#[cfg(test)]
mod tests;

pub fn init_seams() {
    rewrite_handler_seams::query_rewrite::set(QueryRewrite);
}

pub fn QueryRewrite<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: Query<'mcx>,
) -> PgResult<PgVec<'mcx, Query<'mcx>>> {
    debug_assert_eq!(parsetree.querySource, QuerySource::QSRC_ORIGINAL);
    debug_assert!(parsetree.canSetTag);

    let input_query_id = parsetree.queryId;
    let orig_cmd_type = parsetree.commandType;

    let mut results = RewriteQuery(mcx, parsetree)?;

    for query in results.iter_mut() {
        fireRIRrules(mcx, query)?;
        query.queryId = input_query_id;
    }

    let mut found_original = false;
    let mut last_instead: Option<usize> = None;
    for (i, query) in results.iter().enumerate() {
        if query.querySource == QuerySource::QSRC_ORIGINAL {
            debug_assert!(query.canSetTag);
            debug_assert!(!found_original);
            found_original = true;
        } else {
            debug_assert!(!query.canSetTag);
            if query.commandType == orig_cmd_type
                && matches!(
                    query.querySource,
                    QuerySource::QSRC_INSTEAD_RULE | QuerySource::QSRC_QUAL_INSTEAD_RULE
                )
            {
                last_instead = Some(i);
            }
        }
    }
    if !found_original {
        if let Some(i) = last_instead {
            results[i].canSetTag = true;
        }
    }

    Ok(results)
}

fn RewriteQuery<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: Query<'mcx>,
) -> PgResult<PgVec<'mcx, Query<'mcx>>> {
    let event = parsetree.commandType;

    if !parsetree.cteList.is_nil() {
        panic!(
            "RewriteQuery (rewriteHandler.c): WITH-clause rewrite needs CommonTableExpr \
             (types_nodes parsenodes unported)"
        );
    }

    if event != CmdType::CMD_SELECT && event != CmdType::CMD_UTILITY {
        panic!(
            "RewriteQuery (rewriteHandler.c): INSERT/UPDATE/DELETE/MERGE rewrite needs \
             rewriteTargetListIU/matchLocks/fireRules (pg_rewrite + column-defaults vocab \
             unported)"
        );
    }

    let mut rewritten = mcx::vec_with_capacity_in(mcx, 1)?;
    rewritten.push(parsetree);
    Ok(rewritten)
}

fn fireRIRrules<'mcx>(mcx: Mcx<'mcx>, parsetree: &Query<'mcx>) -> PgResult<()> {
    if !parsetree.cteList.is_nil() {
        panic!(
            "fireRIRrules (rewriteHandler.c): CTE descent + rewriteSearchAndCycle need \
             CommonTableExpr (types_nodes parsenodes unported)"
        );
    }
    debug_assert!(parsetree.onConflict.is_none());

    let mut rt_index = 0;
    while rt_index < parsetree.rtable.len() {
        let node = parsetree.rtable.nth(rt_index);
        rt_index += 1;
        let rte = rte_of(node);

        if rte.rtekind == RTEKind::RTE_SUBQUERY {
            let sub = rte.subquery.expect("subquery RTE has a subquery");
            fireRIRrules(mcx, sub)?;
            debug_assert!(!sub.hasRowSecurity);
            continue;
        }
        if rte.rtekind != RTEKind::RTE_RELATION {
            continue;
        }
        if rte.relkind == RELKIND_MATVIEW {
            continue;
        }
        // C divergence: the rangeTableEntry_used skip (a walker over the whole
        // query) is not needed until rule expansion can insert unreferenced
        // RTEs; every arm below is check-only, so extra visits are inert.
        let rel = table::table_open(mcx, rte.relid, NoLock)?;
        if rel.rd_rel.relkind == RELKIND_VIEW {
            panic!(
                "fireRIRrules (rewriteHandler.c): view expansion needs \
                 ApplyRetrieveRule/rd_rules (pg_rewrite vocab unported)"
            );
        }
        // C divergence: the trimmed relcache entry has no rd_rules, so a
        // non-view relation carrying user CREATE RULE rules is undetectable
        // until pg_rewrite lands (none exist in a stock initdb).
        table::table_close(rel, NoLock)?;
    }

    if parsetree.hasSubLinks {
        panic!(
            "fireRIRrules (rewriteHandler.c): sublink descent needs query_tree_walker \
             (backend-nodes nodeFuncs.c unported)"
        );
    }

    for node in parsetree.rtable.iter() {
        let rte = rte_of(node);
        if rte.rtekind != RTEKind::RTE_RELATION
            || (rte.relkind != RELKIND_RELATION && rte.relkind != RELKIND_PARTITIONED_TABLE)
        {
            continue;
        }
        let rel = table::table_open(mcx, rte.relid, NoLock)?;
        if rel.rd_rel.relrowsecurity {
            panic!(
                "fireRIRrules (rewriteHandler.c): row-level security needs \
                 get_row_security_policies (rowsecurity.c unported)"
            );
        }
        table::table_close(rel, NoLock)?;
    }

    Ok(())
}

pub fn AcquireRewriteLocks<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    forExecute: bool,
    forUpdatePushedDown: bool,
) -> PgResult<()> {
    for node in parsetree.rtable.iter() {
        let rtekind = rte_of(node).rtekind;
        match rtekind {
            RTEKind::RTE_RELATION => {
                let (relid, rellockmode) = {
                    let rte = rte_of(node);
                    (rte.relid, rte.rellockmode)
                };
                let lockmode: LOCKMODE = if !forExecute {
                    AccessShareLock
                } else if forUpdatePushedDown && rellockmode == AccessShareLock {
                    // SAFETY: the rewriter owns the just-analyzed tree
                    // single-threaded; no reference derived from `node` is
                    // live across this write.
                    unsafe { node.with_mut::<RangeTblEntry, _>(|r| r.rellockmode = RowShareLock) };
                    RowShareLock
                } else {
                    rellockmode
                };

                let rel = table::table_open(mcx, relid, lockmode)?;
                let relkind = rel.rd_rel.relkind;
                table::table_close(rel, NoLock)?;
                // SAFETY: as above — exclusive, single-threaded tree fixup.
                unsafe { node.with_mut::<RangeTblEntry, _>(|r| r.relkind = relkind) };
            }
            RTEKind::RTE_JOIN => {
                panic!(
                    "AcquireRewriteLocks (rewriteHandler.c): dropped-column fixup of \
                     joinaliasvars needs strip_implicit_coercions + \
                     get_rte_attribute_is_dropped (nodeFuncs.c/parse_relation.c unported)"
                );
            }
            RTEKind::RTE_SUBQUERY => {
                let pushed_down = forUpdatePushedDown || {
                    if parsetree.rowMarks.is_nil() {
                        false
                    } else {
                        panic!(
                            "AcquireRewriteLocks (rewriteHandler.c): FOR UPDATE/SHARE \
                             pushdown needs get_parse_rowmark/RowMarkClause \
                             (parse_relation.c unported)"
                        );
                    }
                };
                let sub = rte_of(node).subquery.expect("subquery RTE has a subquery");
                AcquireRewriteLocks(mcx, sub, forExecute, pushed_down)?;
            }
            _ => {}
        }
    }

    if !parsetree.cteList.is_nil() {
        panic!(
            "AcquireRewriteLocks (rewriteHandler.c): WITH descent needs CommonTableExpr \
             (types_nodes parsenodes unported)"
        );
    }

    if parsetree.hasSubLinks {
        panic!(
            "AcquireRewriteLocks (rewriteHandler.c): sublink descent needs \
             query_tree_walker (backend-nodes nodeFuncs.c unported)"
        );
    }

    Ok(())
}

fn rte_of<'mcx>(node: Node<'mcx>) -> &'mcx RangeTblEntry<'mcx> {
    node.as_range_tbl_entry().expect("rtable holds RangeTblEntry nodes")
}
