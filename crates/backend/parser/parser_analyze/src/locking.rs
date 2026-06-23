//! The FOR [KEY] UPDATE/SHARE locking family of `parser/analyze.c`:
//! `LCS_asString`, `CheckSelectLocking`, `transformLockingClause`,
//! `applyLockingClause`.

use alloc::format;

use ::mcx::Mcx;
use ::types_acl::acl::ACL_SELECT_FOR_UPDATE;
use ::types_error::PgResult;
use ::nodes::copy_query::Query;
use ::nodes::nodes::Node;
use ::nodes::parsenodes::RTEKind;
use ::nodes::parsestmt::ParseState;
use ::nodes::rawnodes::{
    LockClauseStrength, LockWaitPolicy, LockingClause, RowMarkClause,
};

use crate::elog_error;

/// `LCS_asString(strength)` — printable name of a row-locking strength.
pub fn LCS_asString(strength: LockClauseStrength) -> &'static str {
    match strength {
        LockClauseStrength::LCS_NONE => "FOR some", /* C: Assert(false) then falls through */
        LockClauseStrength::LCS_FORKEYSHARE => "FOR KEY SHARE",
        LockClauseStrength::LCS_FORSHARE => "FOR SHARE",
        LockClauseStrength::LCS_FORNOKEYUPDATE => "FOR NO KEY UPDATE",
        LockClauseStrength::LCS_FORUPDATE => "FOR UPDATE",
    }
}

/// Build a row-locking parse-analysis error carrying the offending
/// `RangeVar`'s token position (C: `parser_errposition(pstate, thisrel->location)`).
fn locking_errpos(
    pstate: &ParseState<'_>,
    errcode: ::types_error::SqlState,
    msg: alloc::string::String,
    location: i32,
) -> ::types_error::PgError {
    utils_error::ereport(::types_error::ERROR)
        .errcode(errcode)
        .errmsg(msg)
        .errposition(small1::parser_errposition(pstate, location))
        .into_error()
}

/// `CheckSelectLocking(qry, strength)` — reject FOR UPDATE/SHARE combined with
/// features it cannot support.
pub fn CheckSelectLocking(qry: &Query<'_>, strength: LockClauseStrength) -> PgResult<()> {
    debug_assert!(strength != LockClauseStrength::LCS_NONE);

    if qry.setOperations.is_some() {
        return Err(elog_error(format!(
            "{} is not allowed with UNION/INTERSECT/EXCEPT",
            LCS_asString(strength)
        )));
    }
    if !qry.distinctClause.is_empty() {
        return Err(elog_error(format!(
            "{} is not allowed with DISTINCT clause",
            LCS_asString(strength)
        )));
    }
    if !qry.groupClause.is_empty() || !qry.groupingSets.is_empty() {
        return Err(elog_error(format!(
            "{} is not allowed with GROUP BY clause",
            LCS_asString(strength)
        )));
    }
    if qry.havingQual.is_some() {
        return Err(elog_error(format!(
            "{} is not allowed with HAVING clause",
            LCS_asString(strength)
        )));
    }
    if qry.hasAggs {
        return Err(elog_error(format!(
            "{} is not allowed with aggregate functions",
            LCS_asString(strength)
        )));
    }
    if qry.hasWindowFuncs {
        return Err(elog_error(format!(
            "{} is not allowed with window functions",
            LCS_asString(strength)
        )));
    }
    if qry.hasTargetSRFs {
        return Err(elog_error(format!(
            "{} is not allowed with set-returning functions in the target list",
            LCS_asString(strength)
        )));
    }
    Ok(())
}

/// `transformLockingClause(pstate, qry, lc, pushedDown)` — replace the named (or
/// all) relations of a FOR [KEY] UPDATE/SHARE clause by their range-table
/// indexes, recording row marks and required permissions.
pub fn transformLockingClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    qry: &mut Query<'mcx>,
    lc: &LockingClause<'mcx>,
    pushed_down: bool,
) -> PgResult<()> {
    CheckSelectLocking(qry, lc.strength)?;

    if lc.lockedRels.is_empty() {
        // Lock all regular tables used in query and its subqueries.
        let nrtes = qry.rtable.len();
        for idx in 0..nrtes {
            let i = (idx + 1) as types_core::primitive::Index;
            let (in_from_cl, rtekind) = {
                let rte = &qry.rtable[idx];
                (rte.inFromCl, rte.rtekind)
            };
            if !in_from_cl {
                continue;
            }
            match rtekind {
                RTEKind::RTE_RELATION => {
                    applyLockingClause(mcx, qry, i, lc.strength, lc.waitPolicy, pushed_down)?;
                    let perm_idx = {
                        let rte = &qry.rtable[idx];
                        parser_relation::getRTEPermissionInfo(&qry.rteperminfos, rte)?
                    };
                    qry.rteperminfos[perm_idx].requiredPerms |= ACL_SELECT_FOR_UPDATE;
                }
                RTEKind::RTE_SUBQUERY => {
                    applyLockingClause(mcx, qry, i, lc.strength, lc.waitPolicy, pushed_down)?;
                    // Propagate to all of subquery's rels via an "all rels" clause.
                    let allrels = LockingClause {
                        lockedRels: ::mcx::PgVec::new_in(mcx),
                        strength: lc.strength,
                        waitPolicy: lc.waitPolicy,
                    };
                    let mut sub = qry.rtable[idx].subquery.take();
                    if let Some(subq) = sub.as_deref_mut() {
                        transformLockingClause(mcx, pstate, subq, &allrels, true)?;
                    }
                    qry.rtable[idx].subquery = sub;
                }
                _ => { /* ignore JOIN/SPECIAL/FUNCTION/VALUES/CTE RTEs */ }
            }
        }
    } else {
        // Lock just the named tables.
        for thisrel_node in lc.lockedRels.iter() {
            let thisrel = match thisrel_node.as_ref().as_rangevar() {
                Some(rv) => rv,
                None => return Err(elog_error("locked relation is not a RangeVar")),
            };

            if thisrel.catalogname.is_some() || thisrel.schemaname.is_some() {
                return Err(elog_error(format!(
                    "{} must specify unqualified relation names",
                    LCS_asString(lc.strength)
                )));
            }
            let thisrel_name = thisrel
                .relname
                .as_deref()
                .ok_or_else(|| elog_error("locked relation has no name"))?
                .to_string();
            // C: parser_errposition(pstate, thisrel->location).
            let thisrel_location = thisrel.location;

            let nrtes = qry.rtable.len();
            let mut found = false;
            for idx in 0..nrtes {
                let i = (idx + 1) as types_core::primitive::Index;
                let (in_from_cl, rtekind, rtename) = {
                    let rte = &qry.rtable[idx];
                    if !rte.inFromCl {
                        (false, rte.rtekind, alloc::string::String::new())
                    } else {
                        // Determine the visible relation name for this RTE.
                        let mut name = rte
                            .eref
                            .as_ref()
                            .and_then(|e| e.aliasname.as_deref())
                            .unwrap_or("")
                            .to_string();
                        let mut skip = false;
                        if rte.alias.is_none() {
                            match rte.rtekind {
                                RTEKind::RTE_JOIN => {
                                    match rte.join_using_alias.as_ref() {
                                        None => skip = true,
                                        Some(a) => {
                                            name = a
                                                .aliasname
                                                .as_deref()
                                                .unwrap_or("")
                                                .to_string()
                                        }
                                    }
                                }
                                RTEKind::RTE_SUBQUERY | RTEKind::RTE_VALUES => skip = true,
                                _ => {}
                            }
                        }
                        if skip {
                            (false, rte.rtekind, alloc::string::String::new())
                        } else {
                            (true, rte.rtekind, name)
                        }
                    }
                };
                if !in_from_cl {
                    continue;
                }

                if rtename != thisrel_name {
                    continue;
                }

                found = true;
                match rtekind {
                    RTEKind::RTE_RELATION => {
                        applyLockingClause(mcx, qry, i, lc.strength, lc.waitPolicy, pushed_down)?;
                        let perm_idx = {
                            let rte = &qry.rtable[idx];
                            parser_relation::getRTEPermissionInfo(&qry.rteperminfos, rte)?
                        };
                        qry.rteperminfos[perm_idx].requiredPerms |= ACL_SELECT_FOR_UPDATE;
                    }
                    RTEKind::RTE_SUBQUERY => {
                        applyLockingClause(mcx, qry, i, lc.strength, lc.waitPolicy, pushed_down)?;
                        let allrels = LockingClause {
                            lockedRels: ::mcx::PgVec::new_in(mcx),
                            strength: lc.strength,
                            waitPolicy: lc.waitPolicy,
                        };
                        let mut sub = qry.rtable[idx].subquery.take();
                        if let Some(subq) = sub.as_deref_mut() {
                            transformLockingClause(mcx, pstate, subq, &allrels, true)?;
                        }
                        qry.rtable[idx].subquery = sub;
                    }
                    RTEKind::RTE_JOIN => {
                        return Err(locking_errpos(
                            pstate,
                            ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                            format!("{} cannot be applied to a join", LCS_asString(lc.strength)),
                            thisrel_location,
                        ))
                    }
                    RTEKind::RTE_FUNCTION => {
                        return Err(locking_errpos(
                            pstate,
                            ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                            format!(
                                "{} cannot be applied to a function",
                                LCS_asString(lc.strength)
                            ),
                            thisrel_location,
                        ))
                    }
                    RTEKind::RTE_TABLEFUNC => {
                        return Err(locking_errpos(
                            pstate,
                            ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                            format!(
                                "{} cannot be applied to a table function",
                                LCS_asString(lc.strength)
                            ),
                            thisrel_location,
                        ))
                    }
                    RTEKind::RTE_VALUES => {
                        return Err(locking_errpos(
                            pstate,
                            ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                            format!("{} cannot be applied to VALUES", LCS_asString(lc.strength)),
                            thisrel_location,
                        ))
                    }
                    RTEKind::RTE_CTE => {
                        return Err(locking_errpos(
                            pstate,
                            ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                            format!(
                                "{} cannot be applied to a WITH query",
                                LCS_asString(lc.strength)
                            ),
                            thisrel_location,
                        ))
                    }
                    RTEKind::RTE_NAMEDTUPLESTORE => {
                        return Err(locking_errpos(
                            pstate,
                            ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                            format!(
                                "{} cannot be applied to a named tuplestore",
                                LCS_asString(lc.strength)
                            ),
                            thisrel_location,
                        ))
                    }
                    other => {
                        return Err(elog_error(format!(
                            "unrecognized RTE type: {}",
                            other as i32
                        )))
                    }
                }
                break; /* out of inner loop */
            }
            if !found {
                return Err(locking_errpos(
                    pstate,
                    ::types_error::ERRCODE_UNDEFINED_TABLE,
                    format!(
                        "relation \"{}\" in {} clause not found in FROM clause",
                        thisrel_name,
                        LCS_asString(lc.strength)
                    ),
                    thisrel_location,
                ));
            }
        }
    }
    Ok(())
}

/// `applyLockingClause(qry, rtindex, strength, waitPolicy, pushedDown)` — record
/// locking info for a single range-table item.
pub fn applyLockingClause<'mcx>(
    mcx: Mcx<'mcx>,
    qry: &mut Query<'mcx>,
    rtindex: types_core::primitive::Index,
    strength: LockClauseStrength,
    wait_policy: LockWaitPolicy,
    pushed_down: bool,
) -> PgResult<()> {
    debug_assert!(strength != LockClauseStrength::LCS_NONE);

    /* If it's an explicit clause, make sure hasForUpdate gets set */
    if !pushed_down {
        qry.hasForUpdate = true;
    }

    /* Check for pre-existing entry for same rtindex (C: get_parse_rowmark). */
    for rc_node in qry.rowMarks.iter_mut() {
        if let Some(rc) = rc_node.as_mut().as_rowmarkclause_mut() {
            if rc.rti == rtindex {
                rc.strength = max_strength(rc.strength, strength);
                rc.waitPolicy = max_wait_policy(rc.waitPolicy, wait_policy);
                rc.pushedDown = rc.pushedDown && pushed_down;
                return Ok(());
            }
        }
    }

    /* Make a new RowMarkClause */
    let rc = RowMarkClause {
        rti: rtindex,
        strength,
        waitPolicy: wait_policy,
        pushedDown: pushed_down,
    };
    qry.rowMarks
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<RowMarkClause>()))?;
    let node = ::mcx::alloc_in(mcx, Node::mk_row_mark_clause(mcx, rc)?)?;
    qry.rowMarks.push(node);
    Ok(())
}

/// `Max(LockClauseStrength, LockClauseStrength)` — the C `Max` over the enum's
/// integer order (stronger == larger).
fn max_strength(a: LockClauseStrength, b: LockClauseStrength) -> LockClauseStrength {
    if (a as i32) >= (b as i32) {
        a
    } else {
        b
    }
}

/// `Max(LockWaitPolicy, LockWaitPolicy)` — NOWAIT > SKIP LOCKED > wait.
fn max_wait_policy(a: LockWaitPolicy, b: LockWaitPolicy) -> LockWaitPolicy {
    if (a as i32) >= (b as i32) {
        a
    } else {
        b
    }
}
