#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid, BOOLOID};
use types_error::{PgError, PgResult};
use types_nodes::equal::equal;
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{
    Query, RTEKind, RangeTblEntry, WCOKind, WithCheckOption, ACL_SELECT, ACL_UPDATE,
};
use types_nodes::primnodes::{BoolExpr, BoolExprType, Const, OnConflictAction};
use types_nodes::NodeTag;
use types_rel::{NoLock, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};

use adt_acl::has_privs_of_role;
use miscinit::GetUserId;
use relcache::rowsecurity::{RelationGetRowSecurityDesc, RowSecurityPolicyMeta};
use rls_seams::CheckEnableRls;

const ACL_ID_PUBLIC: Oid = 0;
const POLCMD_ALL: u8 = b'*';
const POLCMD_SELECT: u8 = b'r';
const POLCMD_INSERT: u8 = b'a';
const POLCMD_UPDATE: u8 = b'w';
const POLCMD_DELETE: u8 = b'd';

pub struct RlsQuals<'mcx> {
    pub security_quals: PgVec<'mcx, Node<'mcx>>,
    pub with_check_options: PgVec<'mcx, Node<'mcx>>,
    pub has_row_security: bool,
    pub has_sub_links: bool,
}

pub fn get_row_security_policies<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Query<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    rt_index: i32,
) -> PgResult<RlsQuals<'mcx>> {
    let mut out = RlsQuals {
        security_quals: PgVec::new_in(mcx),
        with_check_options: PgVec::new_in(mcx),
        has_row_security: false,
        has_sub_links: false,
    };

    debug_assert!(rte.rtekind == RTEKind::RTE_RELATION);

    if rte.relkind != RELKIND_RELATION && rte.relkind != RELKIND_PARTITIONED_TABLE {
        return Ok(out);
    }

    let perminfo = parse_relation::getRTEPermissionInfo(&parsetree.rteperminfos, rte)?
        .as_rte_permission_info()
        .expect("rteperminfos holds RTEPermissionInfo");
    let check_as_user = perminfo.checkAsUser;
    let required_perms = perminfo.requiredPerms;

    let user_id = if check_as_user != InvalidOid { check_as_user } else { GetUserId() };

    let rls_status = rls_seams::check_enable_rls::call(rte.relid, check_as_user, false)?;

    if rls_status == CheckEnableRls::RlsNone {
        return Ok(out);
    }
    if rls_status == CheckEnableRls::RlsNoneEnv {
        out.has_row_security = true;
        return Ok(out);
    }

    let rel = table::table_open(mcx, rte.relid, NoLock)?;
    let relname = mcx_str(mcx, rel.name())?;

    let rsdesc = RelationGetRowSecurityDesc(mcx, rte.relid)?;
    let policies: &[RowSecurityPolicyMeta] = &rsdesc.policies;

    let command_type = if rt_index == parsetree.resultRelation {
        parsetree.commandType
    } else {
        CmdType::CMD_SELECT
    };

    if command_type == CmdType::CMD_SELECT && required_perms & ACL_UPDATE != 0 {
        let (perm, restr) = get_policies_for_relation(mcx, policies, CmdType::CMD_UPDATE, user_id)?;
        add_security_quals(mcx, policies, &perm, &restr, rt_index, &mut out)?;
    }

    let (permissive, restrictive) =
        get_policies_for_relation(mcx, policies, command_type, user_id)?;

    if command_type == CmdType::CMD_SELECT
        || command_type == CmdType::CMD_UPDATE
        || command_type == CmdType::CMD_DELETE
    {
        add_security_quals(mcx, policies, &permissive, &restrictive, rt_index, &mut out)?;
    }

    if (command_type == CmdType::CMD_UPDATE
        || command_type == CmdType::CMD_DELETE
        || command_type == CmdType::CMD_MERGE)
        && required_perms & ACL_SELECT != 0
    {
        let (perm, restr) = get_policies_for_relation(mcx, policies, CmdType::CMD_SELECT, user_id)?;
        add_security_quals(mcx, policies, &perm, &restr, rt_index, &mut out)?;
    }

    if command_type == CmdType::CMD_INSERT || command_type == CmdType::CMD_UPDATE {
        debug_assert!(rt_index == parsetree.resultRelation);

        let kind = if command_type == CmdType::CMD_INSERT {
            WCOKind::WCO_RLS_INSERT_CHECK
        } else {
            WCOKind::WCO_RLS_UPDATE_CHECK
        };
        add_with_check_options(
            mcx, policies, relname, rt_index, kind, &permissive, &restrictive, false, &mut out,
        )?;

        if required_perms & ACL_SELECT != 0 {
            let (sel_perm, sel_restr) =
                get_policies_for_relation(mcx, policies, CmdType::CMD_SELECT, user_id)?;
            add_with_check_options(
                mcx, policies, relname, rt_index, kind, &sel_perm, &sel_restr, true, &mut out,
            )?;
        }

        let on_conflict_update = parsetree
            .onConflict
            .and_then(|n| n.as_on_conflict_expr())
            .is_some_and(|oc| oc.action == OnConflictAction::ONCONFLICT_UPDATE);
        if command_type == CmdType::CMD_INSERT && on_conflict_update {
            let (conf_perm, conf_restr) =
                get_policies_for_relation(mcx, policies, CmdType::CMD_UPDATE, user_id)?;

            add_with_check_options(
                mcx,
                policies,
                relname,
                rt_index,
                WCOKind::WCO_RLS_CONFLICT_CHECK,
                &conf_perm,
                &conf_restr,
                true,
                &mut out,
            )?;

            let mut conf_sel_perm = PgVec::new_in(mcx);
            let mut conf_sel_restr = PgVec::new_in(mcx);
            if required_perms & ACL_SELECT != 0 {
                (conf_sel_perm, conf_sel_restr) =
                    get_policies_for_relation(mcx, policies, CmdType::CMD_SELECT, user_id)?;
                add_with_check_options(
                    mcx,
                    policies,
                    relname,
                    rt_index,
                    WCOKind::WCO_RLS_CONFLICT_CHECK,
                    &conf_sel_perm,
                    &conf_sel_restr,
                    true,
                    &mut out,
                )?;
            }

            add_with_check_options(
                mcx,
                policies,
                relname,
                rt_index,
                WCOKind::WCO_RLS_UPDATE_CHECK,
                &conf_perm,
                &conf_restr,
                false,
                &mut out,
            )?;

            if required_perms & ACL_SELECT != 0 {
                add_with_check_options(
                    mcx,
                    policies,
                    relname,
                    rt_index,
                    WCOKind::WCO_RLS_UPDATE_CHECK,
                    &conf_sel_perm,
                    &conf_sel_restr,
                    true,
                    &mut out,
                )?;
            }
        }
    }

    if command_type == CmdType::CMD_MERGE {
        let (mu_perm, mu_restr) =
            get_policies_for_relation(mcx, policies, CmdType::CMD_UPDATE, user_id)?;

        add_with_check_options(
            mcx,
            policies,
            relname,
            rt_index,
            WCOKind::WCO_RLS_MERGE_UPDATE_CHECK,
            &mu_perm,
            &mu_restr,
            true,
            &mut out,
        )?;

        add_with_check_options(
            mcx,
            policies,
            relname,
            rt_index,
            WCOKind::WCO_RLS_UPDATE_CHECK,
            &mu_perm,
            &mu_restr,
            false,
            &mut out,
        )?;

        let mut msel_perm = PgVec::new_in(mcx);
        let mut msel_restr = PgVec::new_in(mcx);
        if required_perms & ACL_SELECT != 0 {
            (msel_perm, msel_restr) =
                get_policies_for_relation(mcx, policies, CmdType::CMD_SELECT, user_id)?;
            add_with_check_options(
                mcx,
                policies,
                relname,
                rt_index,
                WCOKind::WCO_RLS_UPDATE_CHECK,
                &msel_perm,
                &msel_restr,
                true,
                &mut out,
            )?;
        }

        let (md_perm, md_restr) =
            get_policies_for_relation(mcx, policies, CmdType::CMD_DELETE, user_id)?;
        add_with_check_options(
            mcx,
            policies,
            relname,
            rt_index,
            WCOKind::WCO_RLS_MERGE_DELETE_CHECK,
            &md_perm,
            &md_restr,
            true,
            &mut out,
        )?;

        let (mi_perm, mi_restr) =
            get_policies_for_relation(mcx, policies, CmdType::CMD_INSERT, user_id)?;
        add_with_check_options(
            mcx,
            policies,
            relname,
            rt_index,
            WCOKind::WCO_RLS_INSERT_CHECK,
            &mi_perm,
            &mi_restr,
            false,
            &mut out,
        )?;

        if required_perms & ACL_SELECT != 0 && !parsetree.returningList.is_nil() {
            add_with_check_options(
                mcx,
                policies,
                relname,
                rt_index,
                WCOKind::WCO_RLS_INSERT_CHECK,
                &msel_perm,
                &msel_restr,
                true,
                &mut out,
            )?;
        }
    }

    table::table_close(rel, NoLock)?;

    for &q in out.security_quals.iter() {
        set_check_as_user(q, check_as_user);
    }
    for &w in out.with_check_options.iter() {
        set_check_as_user(w, check_as_user);
    }

    out.has_row_security = true;

    Ok(out)
}

fn get_policies_for_relation<'mcx>(
    mcx: Mcx<'mcx>,
    policies: &[RowSecurityPolicyMeta],
    cmd: CmdType,
    user_id: Oid,
) -> PgResult<(PgVec<'mcx, u32>, PgVec<'mcx, u32>)> {
    let mut permissive: PgVec<'mcx, u32> = PgVec::new_in(mcx);
    let mut restrictive: PgVec<'mcx, u32> = PgVec::new_in(mcx);

    for (idx, policy) in policies.iter().enumerate() {
        let cmd_matches = policy.polcmd == POLCMD_ALL
            || match cmd {
                CmdType::CMD_SELECT => policy.polcmd == POLCMD_SELECT,
                CmdType::CMD_INSERT => policy.polcmd == POLCMD_INSERT,
                CmdType::CMD_UPDATE => policy.polcmd == POLCMD_UPDATE,
                CmdType::CMD_DELETE => policy.polcmd == POLCMD_DELETE,
                CmdType::CMD_MERGE => false,
                other => {
                    return Err(unrecognized_policy_command(other as i32));
                }
            };

        if cmd_matches && check_role_for_policy(&policy.roles, user_id)? {
            if policy.permissive {
                permissive.push(idx as u32);
            } else {
                restrictive.push(idx as u32);
            }
        }
    }

    restrictive.sort_by(|&a, &b| {
        policies[a as usize]
            .policy_name
            .as_bytes()
            .cmp(policies[b as usize].policy_name.as_bytes())
    });

    // The extension policy hooks (row_security_policy_hook_permissive /
    // _restrictive) are always NULL here: no loadable C modules.

    Ok((permissive, restrictive))
}

fn add_security_quals<'mcx>(
    mcx: Mcx<'mcx>,
    policies: &[RowSecurityPolicyMeta],
    permissive_policies: &[u32],
    restrictive_policies: &[u32],
    rt_index: i32,
    out: &mut RlsQuals<'mcx>,
) -> PgResult<()> {
    let mut permissive_quals: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for &i in permissive_policies {
        let policy = &policies[i as usize];
        if let Some(src) = policy.qual_src.as_ref() {
            permissive_quals.push(readfuncs::stringToNode(mcx, src.as_str())?);
            out.has_sub_links |= policy_hassublinks(mcx, policy)?;
        }
    }

    if !permissive_quals.is_empty() {
        for &i in restrictive_policies {
            let policy = &policies[i as usize];
            if let Some(src) = policy.qual_src.as_ref() {
                let qual = readfuncs::stringToNode(mcx, src.as_str())?;
                rewrite_manip::ChangeVarNodes(mcx, qual, 1, rt_index, 0)?;
                list_append_unique(&mut out.security_quals, qual);
                out.has_sub_links |= policy_hassublinks(mcx, policy)?;
            }
        }

        let rowsec_expr = if permissive_quals.len() == 1 {
            permissive_quals[0]
        } else {
            make_or_expr(mcx, &permissive_quals)?
        };
        rewrite_manip::ChangeVarNodes(mcx, rowsec_expr, 1, rt_index, 0)?;
        list_append_unique(&mut out.security_quals, rowsec_expr);
    } else {
        out.security_quals.push(make_false_const(mcx)?);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn add_with_check_options<'mcx>(
    mcx: Mcx<'mcx>,
    policies: &[RowSecurityPolicyMeta],
    relname: &'mcx str,
    rt_index: i32,
    kind: WCOKind,
    permissive_policies: &[u32],
    restrictive_policies: &[u32],
    force_using: bool,
    out: &mut RlsQuals<'mcx>,
) -> PgResult<()> {
    fn qual_for_wco<'a>(policy: &'a RowSecurityPolicyMeta, force_using: bool) -> Option<&'a str> {
        if !force_using {
            if let Some(wc) = policy.with_check_src.as_ref() {
                return Some(wc.as_str());
            }
        }
        policy.qual_src.as_ref().map(|s| s.as_str())
    }

    let mut permissive_quals: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for &i in permissive_policies {
        let policy = &policies[i as usize];
        if let Some(src) = qual_for_wco(policy, force_using) {
            permissive_quals.push(readfuncs::stringToNode(mcx, src)?);
            out.has_sub_links |= policy_hassublinks(mcx, policy)?;
        }
    }

    if !permissive_quals.is_empty() {
        let qual = if permissive_quals.len() == 1 {
            permissive_quals[0]
        } else {
            make_or_expr(mcx, &permissive_quals)?
        };
        rewrite_manip::ChangeVarNodes(mcx, qual, 1, rt_index, 0)?;

        let wco = Node::mk(
            mcx,
            WithCheckOption {
                kind,
                relname: Some(relname),
                polname: None,
                qual: Some(qual),
                cascaded: false,
            },
        )?;
        list_append_unique(&mut out.with_check_options, wco);

        for &i in restrictive_policies {
            let policy = &policies[i as usize];
            if let Some(src) = qual_for_wco(policy, force_using) {
                let qual = readfuncs::stringToNode(mcx, src)?;
                rewrite_manip::ChangeVarNodes(mcx, qual, 1, rt_index, 0)?;
                let wco = Node::mk(
                    mcx,
                    WithCheckOption {
                        kind,
                        relname: Some(relname),
                        polname: Some(mcx_str(mcx, policy.policy_name.as_str())?),
                        qual: Some(qual),
                        cascaded: false,
                    },
                )?;
                list_append_unique(&mut out.with_check_options, wco);
                out.has_sub_links |= policy_hassublinks(mcx, policy)?;
            }
        }
    } else {
        let wco = Node::mk(
            mcx,
            WithCheckOption {
                kind,
                relname: Some(relname),
                polname: None,
                qual: Some(make_false_const(mcx)?),
                cascaded: false,
            },
        )?;
        out.with_check_options.push(wco);
    }

    Ok(())
}

fn check_role_for_policy(roles: &[Oid], user_id: Oid) -> PgResult<bool> {
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

// C caches hassublinks (qual OR with_check) in the rsdesc at build time; the
// text-holding cache recomputes it per use — behavior-identical.
fn policy_hassublinks(mcx: Mcx<'_>, policy: &RowSecurityPolicyMeta) -> PgResult<bool> {
    for src in [policy.qual_src.as_ref(), policy.with_check_src.as_ref()].into_iter().flatten() {
        if expr_has_sublink(readfuncs::stringToNode(mcx, src.as_str())?)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn expr_has_sublink<'mcx>(node: Node<'mcx>) -> PgResult<bool> {
    struct F;
    impl<'mcx> nodes_core::NodeWalker<'mcx> for F {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == NodeTag::T_SubLink {
                return Ok(true);
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let mut f = F;
    use nodes_core::NodeWalker as _;
    f.visit(node)
}

// setRuleCheckAsUser (rewriteDefine.c) over a bare expression: stamp every
// rteperminfo of every Query reachable under SubLinks.
fn set_check_as_user(node: Node<'_>, userid: Oid) {
    struct S {
        userid: Oid,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for S {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == NodeTag::T_Query {
                let q = node.as_query().expect("Query");
                for pnode in q.rteperminfos.iter() {
                    let uid = self.userid;
                    // SAFETY: freshly built tree; exclusively ours.
                    unsafe {
                        pnode.with_mut::<types_nodes::parsenodes::RTEPermissionInfo, _>(|p| {
                            p.checkAsUser = uid;
                        })
                    }
                    .expect("rteperminfos holds RTEPermissionInfo");
                }
                return nodes_core::query_tree_walker(q, self, 0);
            }
            nodes_core::expression_tree_walker(node, self)
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            for pnode in q.rteperminfos.iter() {
                let uid = self.userid;
                // SAFETY: as above.
                unsafe {
                    pnode.with_mut::<types_nodes::parsenodes::RTEPermissionInfo, _>(|p| {
                        p.checkAsUser = uid;
                    })
                }
                .expect("rteperminfos holds RTEPermissionInfo");
            }
            nodes_core::query_tree_walker(q, self, 0)
        }
    }
    let mut w = S { userid };
    use nodes_core::NodeWalker as _;
    w.visit(node).expect("set_check_as_user walker is infallible");
}

fn list_append_unique<'mcx>(list: &mut PgVec<'mcx, Node<'mcx>>, node: Node<'mcx>) {
    for &existing in list.iter() {
        if equal(existing, node) {
            return;
        }
    }
    list.push(node);
}

fn make_or_expr<'mcx>(mcx: Mcx<'mcx>, args: &PgVec<'mcx, Node<'mcx>>) -> PgResult<Node<'mcx>> {
    let list = types_nodes::list::NodeList::from_slice(mcx, args)?;
    Node::mk(mcx, BoolExpr { boolop: BoolExprType::OR_EXPR, args: list, location: -1 })
}

fn make_false_const(mcx: Mcx<'_>) -> PgResult<Node<'_>> {
    Node::mk(
        mcx,
        Const {
            consttype: BOOLOID,
            consttypmod: -1,
            constcollid: InvalidOid,
            constlen: 1,
            constvalue: datum::Datum::from_bool(false),
            constisnull: false,
            constbyval: true,
            location: -1,
        },
    )
}

fn mcx_str<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    Ok(core::str::from_utf8(v.leak()).expect("utf8"))
}

#[track_caller]
#[cold]
#[inline(never)]
fn unrecognized_policy_command(cmd: i32) -> Box<PgError> {
    Box::new(PgError::error(format!("unrecognized policy command type {cmd}")))
}
