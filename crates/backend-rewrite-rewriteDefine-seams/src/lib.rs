//! Seam declarations for the `backend-rewrite-rewriteDefine` unit
//! (`rewrite/rewriteDefine.c`).
//!
//! The owning unit installs these from its `init_seams()`; until then a call
//! panics loudly.

use types_core::Oid;

seam_core::seam!(
    /// `setRuleCheckAsUser(node, userid)` (rewriteDefine.c): recursively scan a
    /// query tree and set the `checkAsUser` field to `userid` in all of its
    /// `RTEPermissionInfo`s (descending into subquery / CTE / sublink Querys).
    ///
    /// Called by `RelationBuildRuleLock` (relcache.c) when a rule is loaded:
    /// for an `ON SELECT` rule defining a (non-security-invoker) view the
    /// permission checks on the relations the rule references must be performed
    /// as the view owner, so every cached rule-action Query has its
    /// `checkAsUser` stamped with the owner's role here. The walk is infallible
    /// (a pure field mutation), so the seam returns `()`.
    pub fn set_rule_check_as_user<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        query: &mut types_nodes::copy_query::Query<'mcx>,
        userid: Oid,
    )
);

seam_core::seam!(
    /// `setRuleCheckAsUser(node, userid)` (rewriteDefine.c) entered at a generic
    /// expression `Node` — used for the rule's `qual` (`rule->qual`), which may
    /// embed SubLink subselects whose RTEPermissionInfos also need stamping.
    /// Infallible field mutation.
    pub fn set_rule_check_as_user_node<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        node: &mut types_nodes::nodes::Node<'mcx>,
        userid: Oid,
    )
);
