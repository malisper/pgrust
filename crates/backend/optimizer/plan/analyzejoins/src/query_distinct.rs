//! `query_supports_distinctness` / `query_is_distinct_for` / `distinct_col_search`
//! (analyzejoins.c:1078-1277) — the `RTE_SUBQUERY` distinctness legs.
//!
//! # The #294 unblock
//!
//! For a subquery innerrel, uniqueness is proven not by indexes but by the
//! subquery's own shape: a DISTINCT/GROUP BY/aggregate/HAVING/set-op output is
//! certain to be distinct on the right columns. C reaches the sub-`Query`
//! through `root->simple_rte_array[rel->relid]->subquery`; this repo carries the
//! sub-`Query` inline on the [`RangeTblEntry`](::nodes::parsenodes::RangeTblEntry)
//! (`subquery: Option<Box<Query<'mcx>>>`), resolved through the planner-run RTE
//! store (`simple_rte_array[relid]` → `RangeTblEntryId` →
//! [`PlannerRun::resolve_rte`]). The `groupClause`/`distinctClause` items are
//! `Node`s wrapping [`SortGroupClause`]; `groupingSets` items wrap
//! [`GroupingSet`]; `setOperations` is a `Node` wrapping [`SetOperationStmt`].

use ::types_core::primitive::Oid;
use ::nodes::nodes::{ntag, Node};
use ::nodes::rawnodes::{GroupingSet, SetOperationStmt, SortGroupClause, GROUPING_SET_EMPTY};
use ::nodes::copy_query::Query;

use ::vars::tlist::get_sortgroupclause_tle;
use ::lsyscache::opfamily_operator::equality_ops_are_compatible;

/// `OidIsValid(oid)` — a valid OID is nonzero (InvalidOid == 0).
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != 0
}

/// Resolve a `groupClause`/`distinctClause` list item (`Node *`) to its
/// [`SortGroupClause`]. C `(SortGroupClause *) lfirst(l)`; the list element is
/// always a SortGroupClause node in these clauses.
#[inline]
fn as_sort_group_clause<'a>(node: &'a Node<'_>) -> &'a SortGroupClause {
    match node.node_tag() {
        ntag::T_SortGroupClause => node.expect_sortgroupclause(),
        _ => panic!("expected SortGroupClause in group/distinct clause, got {node:?}"),
    }
}

/// `query_supports_distinctness(query)` (analyzejoins.c:1078) — could the query
/// possibly be proven distinct on some set of output columns? A cheap pre-check
/// for [`query_is_distinct_for`].
pub fn query_supports_distinctness(query: &Query<'_>) -> bool {
    /* SRFs break distinctness except with DISTINCT, see below */
    if query.hasTargetSRFs && query.distinctClause.is_empty() {
        return false;
    }

    /* check for features we can prove distinctness with */
    if !query.distinctClause.is_empty()
        || !query.groupClause.is_empty()
        || !query.groupingSets.is_empty()
        || query.hasAggs
        || query.havingQual.is_some()
        || query.setOperations.is_some()
    {
        return true;
    }

    false
}

/// `distinct_col_search(colno, colnos, opids)` (analyzejoins.c:1265) — if `colno`
/// is in `colnos`, return the corresponding element of `opids`, else InvalidOid
/// (the first match if duplicates).
fn distinct_col_search(colno: i32, colnos: &[i32], opids: &[Oid]) -> Oid {
    for (i, &c) in colnos.iter().enumerate() {
        if colno == c {
            return opids[i];
        }
    }
    0 /* InvalidOid */
}

/// `query_is_distinct_for(query, colnos, opids)` (analyzejoins.c:1116) — does the
/// query never return duplicates of the specified output columns?
///
/// `colnos` is a list of output column numbers (resno's); `opids` the
/// corresponding upper-level equality operators. The two lists are parallel.
pub fn query_is_distinct_for(query: &Query<'_>, colnos: &[i32], opids: &[Oid]) -> bool {
    debug_assert_eq!(colnos.len(), opids.len());

    /*
     * DISTINCT (including DISTINCT ON) guarantees uniqueness if all the columns
     * in the DISTINCT clause appear in colnos and operator semantics match.
     */
    if !query.distinctClause.is_empty() {
        let mut all_matched = true;
        for sgc_node in &query.distinctClause {
            let sgc = as_sort_group_clause(sgc_node);
            let tle = get_sortgroupclause_tle(sgc, &query.targetList)
                .expect("get_sortgroupclause_tle (distinctClause)");
            let opid = distinct_col_search(tle.resno as i32, colnos, opids);
            if !oid_is_valid(opid)
                || !equality_ops_are_compatible(opid, sgc.eqop)
                    .expect("equality_ops_are_compatible")
            {
                all_matched = false; /* exit early if no match */
                break;
            }
        }
        if all_matched {
            return true; /* had matches for all */
        }
    }

    /*
     * Otherwise, a set-returning function in the query's targetlist can result
     * in returning duplicate rows, despite any grouping.
     */
    if query.hasTargetSRFs {
        return false;
    }

    /*
     * Similarly, GROUP BY without GROUPING SETS guarantees uniqueness if all the
     * grouped columns appear in colnos and operator semantics match.
     */
    if !query.groupClause.is_empty() && query.groupingSets.is_empty() {
        let mut all_matched = true;
        for sgc_node in &query.groupClause {
            let sgc = as_sort_group_clause(sgc_node);
            let tle = get_sortgroupclause_tle(sgc, &query.targetList)
                .expect("get_sortgroupclause_tle (groupClause)");
            let opid = distinct_col_search(tle.resno as i32, colnos, opids);
            if !oid_is_valid(opid)
                || !equality_ops_are_compatible(opid, sgc.eqop)
                    .expect("equality_ops_are_compatible")
            {
                all_matched = false;
                break;
            }
        }
        if all_matched {
            return true;
        }
    } else if !query.groupingSets.is_empty() {
        /*
         * If we have grouping sets with expressions, we probably don't have
         * uniqueness and analysis would be hard. Punt.
         */
        if !query.groupClause.is_empty() {
            return false;
        }

        /*
         * If we have no groupClause (therefore no grouping expressions), we
         * might have one or many empty grouping sets. If there's just one, then
         * we're returning only one row and are certainly unique. But otherwise,
         * we know we're certainly not unique.
         */
        if query.groupingSets.len() == 1 {
            let gs_node = &*query.groupingSets[0];
            let gs: &GroupingSet = match gs_node.node_tag() {
                ntag::T_GroupingSet => gs_node.expect_groupingset(),
                _ => panic!("expected GroupingSet in groupingSets, got {gs_node:?}"),
            };
            if gs.kind == GROUPING_SET_EMPTY {
                return true;
            }
        }
        return false;
    } else {
        /*
         * If we have no GROUP BY, but do have aggregates or HAVING, then the
         * result is at most one row so it's surely unique, for any operators.
         */
        if query.hasAggs || query.havingQual.is_some() {
            return true;
        }
    }

    /*
     * UNION, INTERSECT, EXCEPT guarantee uniqueness of the whole output row,
     * except with ALL.
     */
    if let Some(setop_node) = &query.setOperations {
        let setop_n = &**setop_node;
        let topop: &SetOperationStmt = match setop_n.node_tag() {
            ntag::T_SetOperationStmt => setop_n.expect_setoperationstmt(),
            _ => panic!("expected SetOperationStmt in setOperations, got {setop_n:?}"),
        };
        debug_assert!(topop.op != ::nodes::rawnodes::SETOP_NONE);

        if !topop.all {
            /* We're good if all the nonjunk output columns are in colnos */
            let mut lg = topop.groupClauses.iter();
            let mut all_matched = true;
            for tle in query.targetList.iter() {
                if tle.resjunk {
                    continue; /* ignore resjunk columns */
                }

                /* non-resjunk columns should have grouping clauses */
                let sgc_node = lg.next().expect("set-op groupClauses exhausted");
                let sgc = as_sort_group_clause(sgc_node);

                let opid = distinct_col_search(tle.resno as i32, colnos, opids);
                if !oid_is_valid(opid)
                    || !equality_ops_are_compatible(opid, sgc.eqop)
                        .expect("equality_ops_are_compatible")
                {
                    all_matched = false;
                    break;
                }
            }
            if all_matched {
                return true;
            }
        }
    }

    false
}
