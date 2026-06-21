//! Port of `src/backend/rewrite/rowsecurity.c` — routines to support policies
//! for row-level security (RLS).
//!
//! For normal queries, RLS is handled by calling [`get_row_security_policies`]
//! during rewrite, for each RTE of the query. It returns the expressions defined
//! by the table's policies, prepended to the RTE's `securityQuals`; for queries
//! which modify the table, the policies' WITH CHECK clauses are returned as
//! `WithCheckOption`s prepended to the query's `withCheckOptions`.
//!
//! The decision of whether RLS should apply is made by `check_enable_rls`
//! (`utils/misc/rls.c`, owner `backend-utils-misc-more`); the policies
//! themselves are loaded into the relcache entry's `rd_rsdesc` by
//! `RelationBuildRowSecurity` (relcache build family) and read per-query through
//! the relcache [`relation_row_security`] reader seam, which deep-copies each
//! policy's qual into the caller's `mcx` arena (the C `copyObject` the rewriter
//! performs before re-pointing a qual's Vars). C reads
//! `relation->rd_rsdesc->policies` directly off the open relation; the trimmed
//! per-query relation handle carries no `rd_rsdesc`, hence the reader seam.
//!
//! The extension policy hooks (`row_security_policy_hook_permissive` /
//! `_restrictive`) are global function pointers that are always NULL in this
//! single-process port (no `LOAD`able C modules), so the hook legs are
//! unreachable — they are noted at their C call sites and elided.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{alloc_in, Mcx, PgString, PgVec};

use types_acl::acl::{
    ACL_DELETE_CHR, ACL_ID_PUBLIC, ACL_INSERT_CHR, ACL_SELECT, ACL_SELECT_CHR, ACL_UPDATE,
    ACL_UPDATE_CHR,
};
use types_core::primitive::OidIsValid;
use types_core::Oid;
use types_error::{PgError, PgResult, ERROR};
use types_nodes::nodes::{CmdType, Node, NodePtr};
use types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
use types_nodes::primnodes::{BoolExprType, Expr};
use types_nodes::rawnodes::{WCOKind, WithCheckOption};

use backend_nodes_core::makefuncs::{make_bool_const, make_bool_expr};
use backend_nodes_equalfuncs::equal_node;
use backend_rewrite_core::change::ChangeVarNodes;
use backend_rewrite_rewriteDefine::setRuleCheckAsUser;
use backend_utils_adt_acl::role_membership::has_privs_of_role;
use backend_utils_cache_relcache_seams::{relation_row_security, RowSecurityPolicyImage};
use backend_utils_init_miscinit::GetUserId;
use backend_utils_misc_more::rls::check_enable_rls;

use types_acl::CheckEnableRlsResult;
use types_storage::lock::NoLock;

// `RELKIND_*` (the `i8` stored in `RangeTblEntry.relkind`).
const RELKIND_RELATION: i8 = b'r' as i8;
const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;

// `pg_policy.polcmd` chars, as `i8` (matching the policy image `polcmd`).
const POLICY_CMD_ALL: i8 = b'*' as i8;
const POLCMD_SELECT: i8 = ACL_SELECT_CHR as i8;
const POLCMD_INSERT: i8 = ACL_INSERT_CHR as i8;
const POLCMD_UPDATE: i8 = ACL_UPDATE_CHR as i8;
const POLCMD_DELETE: i8 = ACL_DELETE_CHR as i8;

/// The four C out-params of `get_row_security_policies`, bundled.
pub struct RlsPolicies<'mcx> {
    /// New security quals to prepend to the RTE's `securityQuals`.
    pub security_quals: PgVec<'mcx, NodePtr<'mcx>>,
    /// New `WithCheckOption`s to prepend to the query's `withCheckOptions`.
    pub with_check_options: PgVec<'mcx, NodePtr<'mcx>>,
    /// `hasRowSecurity` — true if RLS is enabled (even if no quals were added).
    pub has_row_security: bool,
    /// `hasSubLinks` — true if any qual added contains a sublink.
    pub has_sub_links: bool,
}

/// `get_row_security_policies(root, rte, rt_index, &securityQuals,
/// &withCheckOptions, &hasRowSecurity, &hasSubLinks)` (rowsecurity.c:97).
///
/// Returns the four C out-params bundled as [`RlsPolicies`]; the caller prepends
/// the lists and ORs the flags into the `Query`/RTE. `result_relation`,
/// `query_command_type`, `returning_present` and `on_conflict_update` are the
/// fields the C reads off `root` (`Query`), passed explicitly so this crate need
/// not depend on the `Query` value type.
#[allow(clippy::too_many_arguments)]
pub fn get_row_security_policies<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rt_index: i32,
    result_relation: i32,
    query_command_type: CmdType,
    returning_present: bool,
    on_conflict_update: bool,
    rteperminfos: &[RTEPermissionInfo<'mcx>],
) -> PgResult<RlsPolicies<'mcx>> {
    let mut out = RlsPolicies {
        security_quals: PgVec::new_in(mcx),
        with_check_options: PgVec::new_in(mcx),
        has_row_security: false,
        has_sub_links: false,
    };

    debug_assert!(rte.rtekind == RTEKind::RTE_RELATION);

    // If this is not a normal relation, just return immediately.
    if rte.relkind != RELKIND_RELATION && rte.relkind != RELKIND_PARTITIONED_TABLE {
        return Ok(out);
    }

    // perminfo = getRTEPermissionInfo(root->rteperminfos, rte);
    let perminfo_idx = backend_parser_relation::getRTEPermissionInfo(rteperminfos, rte)?;
    let check_as_user = rteperminfos[perminfo_idx].checkAsUser;
    let required_perms = rteperminfos[perminfo_idx].requiredPerms;

    // Switch to checkAsUser if it's set.
    let user_id = if OidIsValid(check_as_user) {
        check_as_user
    } else {
        GetUserId()
    };

    // Determine the state of RLS for this, pass checkAsUser explicitly.
    let rls_status = check_enable_rls(mcx, rte.relid, check_as_user, false)?;

    // If there is no RLS on this table at all, nothing to do.
    if rls_status == CheckEnableRlsResult::RlsNone {
        return Ok(out);
    }

    // RLS_NONE_ENV means we are not doing any RLS now, but that may change with
    // changes to the environment, so we mark it as hasRowSecurity to force a
    // re-plan when the environment changes.
    if rls_status == CheckEnableRlsResult::RlsNoneEnv {
        out.has_row_security = true;
        return Ok(out);
    }

    // RLS is enabled for this relation.
    //
    // C: rel = table_open(rte->relid, NoLock). We open it to obtain the relation
    // name for the WithCheckOption `relname` and to mirror the C lifecycle; the
    // policies themselves are read through the relcache reader seam (the trimmed
    // handle carries no `rd_rsdesc`). The relation is already locked by the
    // caller's analyze/lock phase, so `NoLock` matches C.
    let rel = backend_access_table_table::table_open(mcx, rte.relid, NoLock)?;
    let relname = PgString::from_str_in(rel.name(), mcx)?;

    // The whole `rd_rsdesc->policies` list, re-homed into `mcx`. `None` is the C
    // `rd_rsdesc == NULL`; an empty list means RLS-enabled with no policies
    // (default-deny handled by add_security_quals / add_with_check_options).
    let policies: PgVec<'mcx, RowSecurityPolicyImage<'mcx>> =
        relation_row_security::call(mcx, rte.relid)?.unwrap_or_else(|| PgVec::new_in(mcx));

    let command_type = if rt_index == result_relation {
        query_command_type
    } else {
        CmdType::CMD_SELECT
    };

    // For a SELECT, if UPDATE privileges are required (eg: FOR [KEY]
    // UPDATE/SHARE), add the UPDATE USING quals first.
    if command_type == CmdType::CMD_SELECT && (required_perms & ACL_UPDATE) != 0 {
        let (perm, restr) = get_policies_for_relation(&policies, CmdType::CMD_UPDATE, user_id)?;
        add_security_quals(mcx, &policies, rt_index, &perm, &restr, &mut out)?;
    }

    // For SELECT, UPDATE and DELETE, add security quals to enforce the USING
    // policies.
    let (permissive_policies, restrictive_policies) =
        get_policies_for_relation(&policies, command_type, user_id)?;

    if command_type == CmdType::CMD_SELECT
        || command_type == CmdType::CMD_UPDATE
        || command_type == CmdType::CMD_DELETE
    {
        add_security_quals(
            mcx,
            &policies,
            rt_index,
            &permissive_policies,
            &restrictive_policies,
            &mut out,
        )?;
    }

    // During UPDATE/DELETE/MERGE, if SELECT rights are also required, collect up
    // CMD_SELECT policies and add them via add_security_quals first.
    if (command_type == CmdType::CMD_UPDATE
        || command_type == CmdType::CMD_DELETE
        || command_type == CmdType::CMD_MERGE)
        && (required_perms & ACL_SELECT) != 0
    {
        let (perm, restr) = get_policies_for_relation(&policies, CmdType::CMD_SELECT, user_id)?;
        add_security_quals(mcx, &policies, rt_index, &perm, &restr, &mut out)?;
    }

    // For INSERT and UPDATE, add withCheckOptions to verify new records.
    if command_type == CmdType::CMD_INSERT || command_type == CmdType::CMD_UPDATE {
        debug_assert!(rt_index == result_relation);

        let kind = if command_type == CmdType::CMD_INSERT {
            WCOKind::WCO_RLS_INSERT_CHECK
        } else {
            WCOKind::WCO_RLS_UPDATE_CHECK
        };
        add_with_check_options(
            mcx,
            &policies,
            &relname,
            rt_index,
            kind,
            &permissive_policies,
            &restrictive_policies,
            false,
            &mut out,
        )?;

        // Get and add ALL/SELECT policies, if SELECT rights are required (eg:
        // RETURNING). Added as WCO policies so an error is raised on violation.
        if (required_perms & ACL_SELECT) != 0 {
            let (sel_perm, sel_restr) =
                get_policies_for_relation(&policies, CmdType::CMD_SELECT, user_id)?;
            add_with_check_options(
                mcx,
                &policies,
                &relname,
                rt_index,
                kind,
                &sel_perm,
                &sel_restr,
                true,
                &mut out,
            )?;
        }

        // For INSERT ... ON CONFLICT DO UPDATE we need additional policy checks
        // for the UPDATE which may be applied to the same RTE.
        if command_type == CmdType::CMD_INSERT && on_conflict_update {
            let (conflict_perm, conflict_restr) =
                get_policies_for_relation(&policies, CmdType::CMD_UPDATE, user_id)?;

            // Enforce the USING clauses of the UPDATE policies using WCOs.
            add_with_check_options(
                mcx,
                &policies,
                &relname,
                rt_index,
                WCOKind::WCO_RLS_CONFLICT_CHECK,
                &conflict_perm,
                &conflict_restr,
                true,
                &mut out,
            )?;

            let mut conflict_sel_perm: Vec<usize> = Vec::new();
            let mut conflict_sel_restr: Vec<usize> = Vec::new();
            if (required_perms & ACL_SELECT) != 0 {
                let (p, r) = get_policies_for_relation(&policies, CmdType::CMD_SELECT, user_id)?;
                conflict_sel_perm = p;
                conflict_sel_restr = r;
                add_with_check_options(
                    mcx,
                    &policies,
                    &relname,
                    rt_index,
                    WCOKind::WCO_RLS_CONFLICT_CHECK,
                    &conflict_sel_perm,
                    &conflict_sel_restr,
                    true,
                    &mut out,
                )?;
            }

            // Enforce the WITH CHECK clauses of the UPDATE policies.
            add_with_check_options(
                mcx,
                &policies,
                &relname,
                rt_index,
                WCOKind::WCO_RLS_UPDATE_CHECK,
                &conflict_perm,
                &conflict_restr,
                false,
                &mut out,
            )?;

            // Add ALL/SELECT policies as WCO_RLS_UPDATE_CHECK WCOs.
            if (required_perms & ACL_SELECT) != 0 {
                add_with_check_options(
                    mcx,
                    &policies,
                    &relname,
                    rt_index,
                    WCOKind::WCO_RLS_UPDATE_CHECK,
                    &conflict_sel_perm,
                    &conflict_sel_restr,
                    true,
                    &mut out,
                )?;
            }
        }
    }

    // FOR MERGE, fetch policies for UPDATE, DELETE and INSERT (and ALL).
    if command_type == CmdType::CMD_MERGE {
        // Fetch the UPDATE policies for the existing target row before UPDATE.
        let (merge_update_perm, merge_update_restr) =
            get_policies_for_relation(&policies, CmdType::CMD_UPDATE, user_id)?;

        // WCO_RLS_MERGE_UPDATE_CHECK checks UPDATE USING quals on the existing
        // target row.
        add_with_check_options(
            mcx,
            &policies,
            &relname,
            rt_index,
            WCOKind::WCO_RLS_MERGE_UPDATE_CHECK,
            &merge_update_perm,
            &merge_update_restr,
            true,
            &mut out,
        )?;

        // Enforce the WITH CHECK clauses of the UPDATE policies.
        add_with_check_options(
            mcx,
            &policies,
            &relname,
            rt_index,
            WCOKind::WCO_RLS_UPDATE_CHECK,
            &merge_update_perm,
            &merge_update_restr,
            false,
            &mut out,
        )?;

        // Add ALL/SELECT policies as WCO_RLS_UPDATE_CHECK WCOs.
        let mut merge_sel_perm: Vec<usize> = Vec::new();
        let mut merge_sel_restr: Vec<usize> = Vec::new();
        if (required_perms & ACL_SELECT) != 0 {
            let (p, r) = get_policies_for_relation(&policies, CmdType::CMD_SELECT, user_id)?;
            merge_sel_perm = p;
            merge_sel_restr = r;
            add_with_check_options(
                mcx,
                &policies,
                &relname,
                rt_index,
                WCOKind::WCO_RLS_UPDATE_CHECK,
                &merge_sel_perm,
                &merge_sel_restr,
                true,
                &mut out,
            )?;
        }

        // Fetch the DELETE policies for the existing target row before DELETE.
        let (merge_delete_perm, merge_delete_restr) =
            get_policies_for_relation(&policies, CmdType::CMD_DELETE, user_id)?;

        // WCO_RLS_MERGE_DELETE_CHECK checks DELETE USING quals on the existing
        // target row.
        add_with_check_options(
            mcx,
            &policies,
            &relname,
            rt_index,
            WCOKind::WCO_RLS_MERGE_DELETE_CHECK,
            &merge_delete_perm,
            &merge_delete_restr,
            true,
            &mut out,
        )?;

        // INSERT policies: checked during ExecInsert, but added to
        // withCheckOptions.
        let (merge_insert_perm, merge_insert_restr) =
            get_policies_for_relation(&policies, CmdType::CMD_INSERT, user_id)?;
        add_with_check_options(
            mcx,
            &policies,
            &relname,
            rt_index,
            WCOKind::WCO_RLS_INSERT_CHECK,
            &merge_insert_perm,
            &merge_insert_restr,
            false,
            &mut out,
        )?;

        // Add ALL/SELECT policies as WCO_RLS_INSERT_CHECK WCOs, when RETURNING
        // is specified and SELECT rights are required.
        if (required_perms & ACL_SELECT) != 0 && returning_present {
            add_with_check_options(
                mcx,
                &policies,
                &relname,
                rt_index,
                WCOKind::WCO_RLS_INSERT_CHECK,
                &merge_sel_perm,
                &merge_sel_restr,
                true,
                &mut out,
            )?;
        }
    }

    rel.close(NoLock)?;

    // Copy checkAsUser to the row security quals and WithCheckOption checks, in
    // case they contain subqueries referring to other relations. The C calls
    // setRuleCheckAsUser on the whole list node; here we walk each element.
    for q in out.security_quals.iter_mut() {
        setRuleCheckAsUser(mcx, q, check_as_user);
    }
    for w in out.with_check_options.iter_mut() {
        setRuleCheckAsUser(mcx, w, check_as_user);
    }

    // Mark this query as having row security, so plancache can invalidate it
    // when necessary (eg: role changes).
    out.has_row_security = true;

    Ok(out)
}

/// `get_policies_for_relation(relation, cmd, user_id, &permissive_policies,
/// &restrictive_policies)` (rowsecurity.c:540).
///
/// Returns `(permissive, restrictive)` as indices into `policies` (the relation's
/// re-homed `rd_rsdesc->policies`). The extension hooks are always NULL here.
fn get_policies_for_relation(
    policies: &[RowSecurityPolicyImage<'_>],
    cmd: CmdType,
    user_id: Oid,
) -> PgResult<(Vec<usize>, Vec<usize>)> {
    let mut permissive: Vec<usize> = Vec::new();
    let mut restrictive: Vec<usize> = Vec::new();

    for (idx, policy) in policies.iter().enumerate() {
        // Always add ALL policies, if they exist.
        let cmd_matches = if policy.polcmd == POLICY_CMD_ALL {
            true
        } else {
            match cmd {
                CmdType::CMD_SELECT => policy.polcmd == POLCMD_SELECT,
                CmdType::CMD_INSERT => policy.polcmd == POLCMD_INSERT,
                CmdType::CMD_UPDATE => policy.polcmd == POLCMD_UPDATE,
                CmdType::CMD_DELETE => policy.polcmd == POLCMD_DELETE,
                // MERGE derives from the policies defined for other commands.
                CmdType::CMD_MERGE => false,
                other => {
                    return Err(PgError::new(
                        ERROR,
                        format!("unrecognized policy command type {}", other as i32),
                    ))
                }
            }
        };

        if cmd_matches && check_role_for_policy(&policy.roles, user_id)? {
            if policy.permissive {
                permissive.push(idx);
            } else {
                restrictive.push(idx);
            }
        }
    }

    // Sort restrictive policies by name for well-defined WCO ordering.
    sort_policies_by_name(policies, &mut restrictive);

    // The extension policy hooks (row_security_policy_hook_restrictive /
    // _permissive) are global function pointers that are always NULL in this
    // single-process port (no LOADable modules), so their legs are unreachable.

    Ok((permissive, restrictive))
}

/// `sort_policies_by_name(policies)` (rowsecurity.c:664) — sort by policy name
/// (used for restrictive policies only). Resolves each index to its descriptor
/// for the name comparison.
fn sort_policies_by_name(policies: &[RowSecurityPolicyImage<'_>], idxs: &mut [usize]) {
    idxs.sort_by(|&a, &b| {
        match row_security_policy_cmp(&policies[a], &policies[b]) {
            n if n < 0 => core::cmp::Ordering::Less,
            0 => core::cmp::Ordering::Equal,
            _ => core::cmp::Ordering::Greater,
        }
    });
}

/// `row_security_policy_cmp(a, b)` (rowsecurity.c:673) — `list_sort` comparator
/// to sort `RowSecurityPolicy` entries by name. The owned `policy_name` is never
/// NULL for built-in policies (only extension policies can omit it, which this
/// port never produces), so the C NULL-guard is unreachable; the byte-wise
/// compare reproduces C's `strcmp` order.
fn row_security_policy_cmp(pa: &RowSecurityPolicyImage<'_>, pb: &RowSecurityPolicyImage<'_>) -> i32 {
    match pa
        .policy_name
        .as_str()
        .as_bytes()
        .cmp(pb.policy_name.as_str().as_bytes())
    {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// `add_security_quals(rt_index, permissive_policies, restrictive_policies,
/// &securityQuals, &hasSubLinks)` (rowsecurity.c:699).
///
/// Add security quals to enforce the specified RLS policies, restricting access
/// to existing data. If there are no permissive policies, an implicit
/// default-deny (`false`) qual is added.
fn add_security_quals<'mcx>(
    mcx: Mcx<'mcx>,
    policies: &[RowSecurityPolicyImage<'mcx>],
    rt_index: i32,
    permissive_policies: &[usize],
    restrictive_policies: &[usize],
    out: &mut RlsPolicies<'mcx>,
) -> PgResult<()> {
    // Collect up the permissive quals (the USING clauses).
    let mut permissive_quals: Vec<Expr> = Vec::new();
    for &i in permissive_policies {
        let policy = &policies[i];
        if let Some(qual) = policy.qual.as_deref() {
            permissive_quals.push(node_to_expr_clone(qual, mcx)?);
            out.has_sub_links |= policy.hassublinks;
        }
    }

    // We must have permissive quals, always, or no rows are visible.
    if !permissive_quals.is_empty() {
        // Add security quals for the restrictive policies' USING clauses,
        // AND-combined by being added one at a time.
        for &i in restrictive_policies {
            let policy = &policies[i];
            if let Some(qual) = policy.qual.as_deref() {
                let mut node = qual.clone_in(mcx)?;
                ChangeVarNodes(&mut node, 1, rt_index, 0, mcx);
                append_unique_node(&mut out.security_quals, node, mcx)?;
                out.has_sub_links |= policy.hassublinks;
            }
        }

        // A single security qual OR-combining the permissive USING clauses.
        let rowsec_expr = if permissive_quals.len() == 1 {
            permissive_quals.pop().unwrap()
        } else {
            make_bool_expr(BoolExprType::OR_EXPR, permissive_quals, -1)
        };
        let mut node = Node::mk_expr(mcx, rowsec_expr)?;
        ChangeVarNodes(&mut node, 1, rt_index, 0, mcx);
        append_unique_node(&mut out.security_quals, node, mcx)?;
    } else {
        // No permissive policy => a single always-false clause (default deny).
        let false_const = make_bool_const(false, false);
        let node = Node::mk_const(mcx, false_const)?;
        out.security_quals.push(alloc_in(mcx, node)?);
    }

    Ok(())
}

/// `add_with_check_options(rel, rt_index, kind, permissive_policies,
/// restrictive_policies, &withCheckOptions, &hasSubLinks, force_using)`
/// (rowsecurity.c:795).
///
/// Add `WithCheckOption`s of the specified kind to check that new records added
/// by an INSERT or UPDATE are consistent with the policies. `force_using` forces
/// use of each policy's USING clause even when a WITH CHECK clause exists.
#[allow(clippy::too_many_arguments)]
fn add_with_check_options<'mcx>(
    mcx: Mcx<'mcx>,
    policies: &[RowSecurityPolicyImage<'mcx>],
    relname: &PgString<'mcx>,
    rt_index: i32,
    kind: WCOKind,
    permissive_policies: &[usize],
    restrictive_policies: &[usize],
    force_using: bool,
    out: &mut RlsPolicies<'mcx>,
) -> PgResult<()> {
    // QUAL_FOR_WCO(policy): with_check_qual unless force_using or it is NULL.
    fn qual_for_wco<'a, 'mcx>(
        policy: &'a RowSecurityPolicyImage<'mcx>,
        force_using: bool,
    ) -> Option<&'a Node<'mcx>> {
        if !force_using {
            if let Some(wc) = policy.with_check_qual.as_deref() {
                return Some(wc);
            }
        }
        policy.qual.as_deref()
    }

    // Collect up the permissive policy clauses.
    let mut permissive_quals: Vec<Expr> = Vec::new();
    for &i in permissive_policies {
        let policy = &policies[i];
        if let Some(qual) = qual_for_wco(policy, force_using) {
            permissive_quals.push(node_to_expr_clone(qual, mcx)?);
            out.has_sub_links |= policy.hassublinks;
        }
    }

    if !permissive_quals.is_empty() {
        // A single WCO for all permissive clauses, OR-combined. No policy name,
        // since a failure means no policy granted permission.
        let qual_expr = if permissive_quals.len() == 1 {
            permissive_quals.pop().unwrap()
        } else {
            make_bool_expr(BoolExprType::OR_EXPR, permissive_quals, -1)
        };
        let mut qual_node = Node::mk_expr(mcx, qual_expr)?;
        ChangeVarNodes(&mut qual_node, 1, rt_index, 0, mcx);

        let wco = WithCheckOption {
            kind,
            relname: Some(relname.clone_in(mcx)?),
            polname: None,
            qual: Some(alloc_in(mcx, qual_node)?),
            cascaded: false,
        };
        let wco_node = Node::mk_with_check_option(mcx, wco)?;
        append_unique_node(&mut out.with_check_options, wco_node, mcx)?;

        // One WCO per restrictive policy clause (AND-combined), each carrying
        // the policy name for error reporting.
        for &i in restrictive_policies {
            let policy = &policies[i];
            if let Some(qual) = qual_for_wco(policy, force_using) {
                let mut qnode = qual.clone_in(mcx)?;
                ChangeVarNodes(&mut qnode, 1, rt_index, 0, mcx);
                let wco = WithCheckOption {
                    kind,
                    relname: Some(relname.clone_in(mcx)?),
                    polname: Some(PgString::from_str_in(policy.policy_name.as_str(), mcx)?),
                    qual: Some(alloc_in(mcx, qnode)?),
                    cascaded: false,
                };
                let wco_node = Node::mk_with_check_option(mcx, wco)?;
                append_unique_node(&mut out.with_check_options, wco_node, mcx)?;
                out.has_sub_links |= policy.hassublinks;
            }
        }
    } else {
        // No policy clauses => a single always-false WCO (default deny).
        let false_const = make_bool_const(false, false);
        let qual_node = Node::mk_const(mcx, false_const)?;
        let wco = WithCheckOption {
            kind,
            relname: Some(relname.clone_in(mcx)?),
            polname: None,
            qual: Some(alloc_in(mcx, qual_node)?),
            cascaded: false,
        };
        let wco_node = Node::mk_with_check_option(mcx, wco)?;
        out.with_check_options.push(alloc_in(mcx, wco_node)?);
    }

    Ok(())
}

/// `check_role_for_policy(policy_roles, user_id)` (rowsecurity.c:915) —
/// determine if the policy should be applied for the current role. The relcache
/// build already decoded `ArrayType *roles` to its element `Oid[]`.
fn check_role_for_policy(roles: &[Oid], user_id: Oid) -> PgResult<bool> {
    // Quick fall-thru for policies applied to all roles (PUBLIC).
    if roles.first().copied() == Some(ACL_ID_PUBLIC) {
        return Ok(true);
    }

    for &role in roles {
        if has_privs_of_role(user_id, role)? {
            return Ok(true);
        }
    }

    Ok(false)
}

// ---------------------------------------------------------------------------
// Small node helpers
// ---------------------------------------------------------------------------

/// Deep-copy a policy qual `Node` into `mcx` and extract its inner `Expr` (the
/// policy quals are always expression nodes, so OR-combining works on `Expr`).
fn node_to_expr_clone<'mcx>(node: &Node<'_>, mcx: Mcx<'mcx>) -> PgResult<Expr<'mcx>> {
    let cloned = node.clone_in(mcx)?;
    cloned
        .into_expr()
        .ok_or_else(|| PgError::new(ERROR, "RLS policy qual is not an expression".to_string()))
}

/// `list_append_unique(*list, node)` over a node list: append unless an equal
/// node is already present (C uses `equal()` semantics).
fn append_unique_node<'mcx>(
    list: &mut PgVec<'mcx, NodePtr<'mcx>>,
    node: Node<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    for existing in list.iter() {
        if equal_node(existing, &node) {
            return Ok(());
        }
    }
    list.push(alloc_in(mcx, node)?);
    Ok(())
}
