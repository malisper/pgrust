//! `getInsertSelectQuery` (rewriteManip.c) — given a rule-action query, find
//! the `Query` that actually contains the OLD/NEW placeholder range-table
//! entries (the given query for ordinary actions, or the pushed-down SELECT
//! subquery for an INSERT/SELECT action).

use types_error::{PgError, PgResult};
use types_nodes::copy_query::Query;
use types_nodes::nodes::CmdType;
use types_nodes::parsenodes::RTEKind;

/// `PRS2_OLD_VARNO` / `PRS2_NEW_VARNO` (primnodes.h) — the rangetable indexes
/// (1-based) of the OLD and NEW placeholder entries in a rule-action query.
const PRS2_OLD_VARNO: usize = 1;
const PRS2_NEW_VARNO: usize = 2;

/// `rt_fetch(varno, rtable)->eref->aliasname == name` (the 1-based fetch).
fn rt_alias_is(query: &Query<'_>, varno: usize, name: &str) -> bool {
    let Some(rte) = query.rtable.get(varno - 1) else {
        return false;
    };
    match &rte.eref {
        Some(alias) => alias.aliasname.as_deref() == Some(name),
        None => false,
    }
}

/// `getInsertSelectQuery(parsetree, NULL)` (rewriteManip.c:1090) — the
/// `subquery_ptr` out-parameter is always `NULL` at the rewriteDefine.c call
/// sites, so it is omitted. Returns a borrow of the input or its nested SELECT
/// subquery. Mirrors C's `elog(ERROR, ...)` paths as `Err`.
pub fn getInsertSelectQuery<'a, 'mcx>(parsetree: &'a Query<'mcx>) -> PgResult<&'a Query<'mcx>> {
    // if (parsetree->commandType != CMD_INSERT) return parsetree;
    if parsetree.commandType != CmdType::CMD_INSERT {
        return Ok(parsetree);
    }

    /*
     * Currently, this is ONLY applied to rule-action queries, and so we
     * expect to find the OLD and NEW placeholder entries in the given query.
     * If they're not there, it must be an INSERT/SELECT in which they've been
     * pushed down to the SELECT.
     */
    if parsetree.rtable.len() >= 2
        && rt_alias_is(parsetree, PRS2_OLD_VARNO, "old")
        && rt_alias_is(parsetree, PRS2_NEW_VARNO, "new")
    {
        return Ok(parsetree);
    }

    // Assert(parsetree->jointree && IsA(parsetree->jointree, FromExpr));
    let jointree = parsetree
        .jointree
        .as_ref()
        .ok_or_else(|| PgError::error("expected to find SELECT subquery"))?;
    // if (list_length(parsetree->jointree->fromlist) != 1) elog(ERROR, ...);
    if jointree.fromlist.len() != 1 {
        return Err(PgError::error("expected to find SELECT subquery"));
    }
    // rtr = (RangeTblRef *) linitial(parsetree->jointree->fromlist);
    // if (!IsA(rtr, RangeTblRef)) elog(ERROR, ...);
    let rtr = match jointree.fromlist[0].as_rangetblref() {
        Some(rtr) => rtr,
        None => return Err(PgError::error("expected to find SELECT subquery")),
    };
    // selectrte = rt_fetch(rtr->rtindex, parsetree->rtable);
    let selectrte = parsetree
        .rtable
        .get(rtr.rtindex as usize - 1)
        .ok_or_else(|| PgError::error("expected to find SELECT subquery"))?;
    // if (!(selectrte->rtekind == RTE_SUBQUERY && selectrte->subquery &&
    //       IsA(selectrte->subquery, Query) &&
    //       selectrte->subquery->commandType == CMD_SELECT)) elog(ERROR, ...);
    if selectrte.rtekind != RTEKind::RTE_SUBQUERY {
        return Err(PgError::error("expected to find SELECT subquery"));
    }
    let selectquery = selectrte
        .subquery
        .as_ref()
        .ok_or_else(|| PgError::error("expected to find SELECT subquery"))?;
    if selectquery.commandType != CmdType::CMD_SELECT {
        return Err(PgError::error("expected to find SELECT subquery"));
    }

    // selectquery = selectrte->subquery;
    // if (list_length(selectquery->rtable) >= 2 && OLD/NEW present) return it;
    if selectquery.rtable.len() >= 2
        && rt_alias_is(selectquery, PRS2_OLD_VARNO, "old")
        && rt_alias_is(selectquery, PRS2_NEW_VARNO, "new")
    {
        return Ok(selectquery);
    }

    // elog(ERROR, "could not find rule placeholders");
    Err(PgError::error("could not find rule placeholders"))
}

/// The index-based rendering of the C `getInsertSelectQuery(parsetree,
/// &subquery_ptr)` out-parameter form (rewriteManip.c:1090), as the DML rule
/// engine's `rewriteRuleAction` needs it.
///
/// The C signature returns the sub-action `Query *` and, via `subquery_ptr`, the
/// `Query **` slot through which the caller can re-bind the (possibly mutated)
/// sub-action back into the INSERT...SELECT's rangetable. Over the owned value
/// model a `Query **` into another query's RTE is an *index*: this returns
/// `Ok(None)` when the sub-action IS the parsetree itself (the C `subquery_ptr ==
/// NULL` case), or `Ok(Some(rtindex))` (1-based) of the `RTE_SUBQUERY` whose
/// `.subquery` is the SELECT sub-action (the C non-NULL `subquery_ptr` case). The
/// caller obtains the borrow with `&parsetree.rtable[rtindex - 1].subquery` and
/// re-binds the mutated sub-action through that same slot. The validation logic
/// is identical to [`getInsertSelectQuery`].
pub fn getInsertSelectQueryIndex(parsetree: &Query<'_>) -> PgResult<Option<usize>> {
    // if (parsetree->commandType != CMD_INSERT) { *subquery_ptr = NULL; return; }
    if parsetree.commandType != CmdType::CMD_INSERT {
        return Ok(None);
    }

    // OLD/NEW present at top level -> the action itself is the sub-action.
    if parsetree.rtable.len() >= 2
        && rt_alias_is(parsetree, PRS2_OLD_VARNO, "old")
        && rt_alias_is(parsetree, PRS2_NEW_VARNO, "new")
    {
        return Ok(None);
    }

    let jointree = parsetree
        .jointree
        .as_ref()
        .ok_or_else(|| PgError::error("expected to find SELECT subquery"))?;
    if jointree.fromlist.len() != 1 {
        return Err(PgError::error("expected to find SELECT subquery"));
    }
    let rtr = match jointree.fromlist[0].as_rangetblref() {
        Some(rtr) => rtr,
        None => return Err(PgError::error("expected to find SELECT subquery")),
    };
    let rtindex = rtr.rtindex as usize;
    let selectrte = parsetree
        .rtable
        .get(rtindex - 1)
        .ok_or_else(|| PgError::error("expected to find SELECT subquery"))?;
    if selectrte.rtekind != RTEKind::RTE_SUBQUERY {
        return Err(PgError::error("expected to find SELECT subquery"));
    }
    let selectquery = selectrte
        .subquery
        .as_ref()
        .ok_or_else(|| PgError::error("expected to find SELECT subquery"))?;
    if selectquery.commandType != CmdType::CMD_SELECT {
        return Err(PgError::error("expected to find SELECT subquery"));
    }

    if selectquery.rtable.len() >= 2
        && rt_alias_is(selectquery, PRS2_OLD_VARNO, "old")
        && rt_alias_is(selectquery, PRS2_NEW_VARNO, "new")
    {
        return Ok(Some(rtindex));
    }

    Err(PgError::error("could not find rule placeholders"))
}
