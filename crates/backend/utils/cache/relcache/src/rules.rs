use std::rc::Rc;

use mcx::{Mcx, MemoryContext};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::Node;

use crate::with_state;

// RewriteRule (rd_rules entry, relcache.c RelationBuildRuleLock): the qual
// and action trees are stringToNode'd ONCE at cache fill into the entry's
// rule context and held for the life of the cache entry. Consumers read them
// in place while they hold the `Rc<RdRules>` (which pins the context across
// an invalidation) and `copy_*` before modifying — C's copyObject per use.
pub struct RewriteRuleMeta {
    pub rule_id: Oid,
    // C: rewrite_form->ev_type - '0' (CmdType numeric value).
    pub event: i32,
    pub enabled: u8,
    pub is_instead: bool,
    // Owned by the enclosing RdRules' rulescxt; valid while that Rc lives.
    // Never modify in place: every reader of this cache shares the tree.
    pub qual: Option<Node<'static>>,
    // The ev_action List of Query nodes; same ownership as `qual`.
    pub actions: Node<'static>,
}

impl RewriteRuleMeta {
    #[inline]
    pub fn has_qual(&self) -> bool {
        self.qual.is_some()
    }

    // copyObject(rule->actions) into the caller's context.
    pub fn copy_actions<'mcx>(&self, mcx: Mcx<'mcx>) -> PgResult<Node<'mcx>> {
        copyfuncs::copy_object(mcx, self.actions)
    }

    // copyObject(rule->qual) into the caller's context.
    pub fn copy_qual<'mcx>(&self, mcx: Mcx<'mcx>) -> PgResult<Option<Node<'mcx>>> {
        match self.qual {
            Some(q) => Ok(Some(copyfuncs::copy_object(mcx, q)?)),
            None => Ok(None),
        }
    }
}

pub struct RdRules {
    // std Vec justified: Rc-owned droppy owner outside the arenas
    // (rd_supportinfo precedent); drop = C's MemoryContextDelete(rulescxt).
    pub rules: Vec<RewriteRuleMeta>,
    // C's rulescxt ("relation rules" child of CacheMemoryContext): owns every
    // tree in `rules`. Declared last so the trees' handles drop first; freed
    // with the last holder, i.e. after invalidation once no consumer is mid-use.
    _rulescxt: MemoryContext,
}

// Rule-5 cache keyed by relid in the relcache state, not a RelationData
// field (trimmed entry has no relhasrules; callers key on relkind).
pub fn RelationGetRules<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Option<Rc<RdRules>>> {
    if let Some(hit) = with_state(|st| st.rules_cache.get(&relid).cloned()) {
        return Ok(Some(hit));
    }
    // No state borrow across the scan: it re-enters the relcache.
    let rows = relcache_build_seams::scan_pg_rewrite::call(mcx, relid)?;
    if rows.is_empty() {
        return Ok(None);
    }
    let rulescxt = MemoryContext::new("relation rules");
    // SAFETY: every allocation below lands in `rulescxt`, which the returned
    // RdRules owns and drops after its `rules` field; the handles are only
    // reachable through the Rc<RdRules>, so nothing outlives the context.
    let rmcx: Mcx<'static> = unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>(rulescxt.mcx()) };
    let mut rules: Vec<RewriteRuleMeta> = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        rules.push(RewriteRuleMeta {
            rule_id: row.rule_id,
            event: (row.ev_type - b'0') as i32,
            enabled: row.ev_enabled,
            is_instead: row.is_instead,
            qual: if row.ev_qual == "<>" {
                None
            } else {
                Some(readfuncs::stringToNode(rmcx, row.ev_qual)?)
            },
            actions: readfuncs::stringToNode(rmcx, row.ev_action)?,
        });
    }
    let built = Rc::new(RdRules { rules, _rulescxt: rulescxt });
    with_state(|st| st.rules_cache.insert(relid, Rc::clone(&built)));
    Ok(Some(built))
}

pub(crate) fn forget(relid: Oid) {
    with_state(|st| st.rules_cache.remove(&relid));
}

pub(crate) fn RelationGetRulesShapes<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<Vec<relcache_seams::RuleShape<'mcx>>> {
    match RelationGetRules(mcx, relid)? {
        None => Ok(Vec::new()),
        Some(rules) => rules
            .rules
            .iter()
            .map(|r| {
                Ok(relcache_seams::RuleShape {
                    event: r.event,
                    is_instead: r.is_instead,
                    actions: r.copy_actions(mcx)?,
                })
            })
            .collect(),
    }
}

pub(crate) fn RelationHasRules(relid: Oid) -> PgResult<bool> {
    let cx = MemoryContext::new("RelationHasRules");
    Ok(RelationGetRules(cx.mcx(), relid)?.is_some())
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::Once;

    use mcx::MemoryContext;
    use relcache_build_seams::PgRewriteRuleShape;

    // A real _RETURN rule action (pg_stat_activity), so the fill parses it.
    const EV_ACTION: &str =
        include_str!("../../../../nodes/readfuncs/src/fixtures/pg_stat_activity.ev_action");

    thread_local! {
        static SCANS: Cell<u32> = const { Cell::new(0) };
    }

    fn install() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            relcache_build_seams::scan_pg_rewrite::set(|mcx, ev_class| {
                SCANS.with(|c| c.set(c.get() + 1));
                let mut rows = mcx::vec_with_capacity_in(mcx, 1)?;
                if ev_class == 21000 {
                    rows.push(PgRewriteRuleShape {
                        rule_id: 31000,
                        ev_type: b'1',
                        ev_enabled: b'O',
                        is_instead: true,
                        ev_qual: "<>",
                        ev_action: EV_ACTION,
                    });
                }
                Ok(rows)
            });
        });
    }

    #[test]
    fn rules_cache_hit_and_inval() {
        install();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        SCANS.with(|c| c.set(0));

        let r = super::RelationGetRules(mcx, 21000).unwrap().expect("view has a rule");
        assert_eq!(r.rules.len(), 1);
        let rule = &r.rules[0];
        assert_eq!(rule.rule_id, 31000);
        assert_eq!(rule.event, 1);
        assert_eq!(rule.enabled, b'O');
        assert!(rule.is_instead);
        assert!(!rule.has_qual());
        // Parsed once at fill: the cached tree is the ev_action List.
        let actions = rule.actions.as_list().expect("ev_action is a List");
        assert_eq!(actions.len(), 1);
        assert!(actions.nth(0).as_query().is_some());
        assert_eq!(SCANS.with(|c| c.get()), 1);

        // A second use is a cache hit sharing the same trees (no re-parse).
        let again = super::RelationGetRules(mcx, 21000).unwrap().unwrap();
        assert_eq!(SCANS.with(|c| c.get()), 1);
        assert!(std::rc::Rc::ptr_eq(&r, &again));
        assert!(again.rules[0].actions.ptr_eq(rule.actions));

        // Per-use copies land in the caller's context and serialize identically.
        let copy = rule.copy_actions(mcx).unwrap();
        assert!(!copy.ptr_eq(rule.actions));
        let a = outfuncs::nodeToString(mcx, copy).unwrap().as_str().to_string();
        let orig = outfuncs::nodeToString(mcx, rule.actions).unwrap().as_str().to_string();
        assert_eq!(a, orig);
        assert!(rule.copy_qual(mcx).unwrap().is_none());

        // Invalidation drops the map entry; a holder keeps its trees alive
        // (C: the old rulescxt outlives the rebuild for whoever still reads it).
        super::forget(21000);
        let rebuilt = super::RelationGetRules(mcx, 21000).unwrap().unwrap();
        assert_eq!(SCANS.with(|c| c.get()), 2);
        assert!(!std::rc::Rc::ptr_eq(&r, &rebuilt));
        let b = outfuncs::nodeToString(mcx, r.rules[0].actions).unwrap().as_str().to_string();
        assert_eq!(a, b);

        assert!(super::RelationGetRules(mcx, 21001).unwrap().is_none());
        assert!(super::RelationGetRules(mcx, 21001).unwrap().is_none());
        assert_eq!(SCANS.with(|c| c.get()), 4);
    }
}
