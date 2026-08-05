use std::rc::Rc;

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

use crate::{cache_mcx, with_state};

// C divergence: rd_rules caches stringToNode'd trees (copyObject per use);
// this cache keeps the text; the consumer reads a fresh tree per use.
pub struct RewriteRuleMeta {
    pub rule_id: Oid,
    // C: rewrite_form->ev_type - '0' (CmdType numeric value).
    pub event: i32,
    pub enabled: u8,
    pub is_instead: bool,
    pub qual_src: Option<PgString<'static>>,
    pub action_src: PgString<'static>,
}

pub struct RdRules {
    // std Vec justified: Rc-owned droppy owner outside the arenas
    // (rd_supportinfo precedent); drop = C's MemoryContextDelete(rulescxt).
    pub rules: Vec<RewriteRuleMeta>,
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
    let cmcx = cache_mcx();
    let mut rules: Vec<RewriteRuleMeta> = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        rules.push(RewriteRuleMeta {
            rule_id: row.rule_id,
            event: (row.ev_type - b'0') as i32,
            enabled: row.ev_enabled,
            is_instead: row.is_instead,
            qual_src: if row.ev_qual == "<>" {
                None
            } else {
                Some(PgString::from_str_in(row.ev_qual, cmcx)?)
            },
            action_src: PgString::from_str_in(row.ev_action, cmcx)?,
        });
    }
    let built = Rc::new(RdRules { rules });
    with_state(|st| st.rules_cache.insert(relid, Rc::clone(&built)));
    Ok(Some(built))
}

pub(crate) fn forget(relid: Oid) {
    with_state(|st| st.rules_cache.remove(&relid));
}

pub(crate) fn RelationGetRulesShapes(relid: Oid) -> PgResult<Vec<relcache_seams::RuleShape>> {
    let mcx = cache_mcx();
    match RelationGetRules(mcx, relid)? {
        None => Ok(Vec::new()),
        Some(rules) => Ok(rules
            .rules
            .iter()
            .map(|r| relcache_seams::RuleShape {
                event: r.event,
                is_instead: r.is_instead,
                action_src: r.action_src.as_str().to_string(),
            })
            .collect()),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::Once;

    use mcx::MemoryContext;
    use relcache_build_seams::PgRewriteRuleShape;

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
                        ev_action: "({QUERY})",
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
        assert!(rule.qual_src.is_none());
        assert_eq!(rule.action_src.as_str(), "({QUERY})");
        assert_eq!(SCANS.with(|c| c.get()), 1);

        let again = super::RelationGetRules(mcx, 21000).unwrap().unwrap();
        assert_eq!(SCANS.with(|c| c.get()), 1);
        assert!(std::rc::Rc::ptr_eq(&r, &again));

        super::forget(21000);
        let _ = super::RelationGetRules(mcx, 21000).unwrap().unwrap();
        assert_eq!(SCANS.with(|c| c.get()), 2);

        assert!(super::RelationGetRules(mcx, 21001).unwrap().is_none());
        assert!(super::RelationGetRules(mcx, 21001).unwrap().is_none());
        assert_eq!(SCANS.with(|c| c.get()), 4);
    }
}
