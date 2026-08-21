//! The sqe statement-plane memo (server-tax C8; the lanev4 lx4stmt
//! ProgCache port in the P2-1 idiom — dispatch.md §2/§5, the recorded
//! P2-1 remainder).
//!
//! ## The law (O-OLTP-3, carried)
//!
//! Lower ONCE per `(plancache handle, generation)`; every subsequent
//! execution of the same built GENERIC plan binds the memoized verdict —
//! no recognizer walk, no family election, no `plan_from_ap`. Refusals
//! memoize too: the typed cause replays through the ordinary
//! `RefuseCause::refuse` constructor, so the census still ticks once per
//! execution (refusal-enum Rule D — the tick site IS the constructor).
//!
//! ## Keys (#512) and invalidation (#511)
//!
//! The key is the `plancache_portal_seams::plan_identity` supply:
//! `(plansource handle, generation at build)`, generic executions only —
//! one-shot/custom plans route fresh per execution (C replan parity; a
//! custom plan can never be handed back by a later `GetCachedPlan`, so
//! caching it can never hit). `BuildCachedPlan` bumps the generation on
//! EVERY replan, so every meaning-changing event (relcache/PROCOID/
//! TYPEOID invalidation, search_path, RLS — the full C-parity set) lands
//! as a structural miss by construction. Eager invalidation:
//! `engine_plan_dropped` (DropCachedPlan) and `engine_plan_cache_reset`
//! (ResetPlanCache), installed at [`init_seams`], run on the owning
//! backend thread (the plancache is thread-local; so is this plane).
//!
//! ## What a memoized verdict does NOT cover
//!
//! Runtime-conditioned gates (posture, EPQ, cursor/SPI cadence, junk
//! filter, bind params, the born-RED seed) run per execution AHEAD of the
//! memo probe in `run_columnar` — a memo hit answers correctly under
//! every runtime condition by construction. Engine currency (the data
//! plane: a new published generation, TRUNCATE, DDL) is validated per
//! execution by the caller against the engine registry's head probe
//! (`seam::engine_is_current`) — a stale artifact is dropped and the
//! statement re-lowers.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use ::types_portal::CachedPlanHandle;

use super::refusal::RefuseCause;
use super::seam::LoweredStmt;

/// The #512 identity: one built generic plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PlanKey {
    pub handle: u64,
    pub generation: i64,
}

/// The memoized dispatch verdict for one (handle, generation).
#[derive(Clone)]
pub(crate) enum StmtVerdict {
    /// A memoized refusal: the typed cause replays without re-derivation
    /// (the census re-ticks at `refuse()` per execution). `engine` is the
    /// engine the refusal was derived AGAINST for engine-conditioned
    /// causes (family election / lowering vocabulary — bank-dependent);
    /// `None` = plan-pure (recognizer-walk causes), valid for the plan's
    /// whole lifetime.
    Refused {
        cause: RefuseCause,
        engine: Option<(u32, Arc<sqe::engine::Engine>)>,
    },
    /// The memoized lowered artifact (verdict + elected plan + engine
    /// binding). Valid while the caller's engine-currency probe answers
    /// the SAME engine (Arc identity against the registry).
    Lowered(Arc<LoweredStmt>),
}

#[derive(Default)]
struct Counters {
    hits: u64,
    stores: u64,
    supersedes: u64,
    stale_drops: u64,
    plan_invalidations: u64,
    resets: u64,
}

#[derive(Default)]
struct MemoPlane {
    /// handle → the CURRENT generation's verdict. One live entry per
    /// plan handle: older generations supersede eagerly.
    entries: HashMap<u64, (i64, StmtVerdict)>,
    counters: Counters,
}

thread_local! {
    // tls-dtor: plain-data for THIS class, but a last-owner evicted Arc<Engine> drop joins pool threads from a TLS dtor (hang class) — graveyard follow-up in tls-teardown-law.md.
    static PLANE: RefCell<MemoPlane> = RefCell::new(MemoPlane::default());
}

/// Resolve the memo key for this execution: `None` = no plancache handle
/// (simple-protocol one-shot), identity seam not installed, or a custom
/// plan (never memoized — C replan parity).
pub(crate) fn plan_key(cplan: CachedPlanHandle) -> Option<PlanKey> {
    if cplan.is_null() || !plancache_portal_seams::plan_identity::is_installed() {
        return None;
    }
    let (handle, generation, generic) = plancache_portal_seams::plan_identity::call(cplan);
    generic.then_some(PlanKey { handle, generation })
}

/// Probe the memo at `key`'s exact generation. A stale-generation entry
/// is dropped eagerly (supersede) and misses.
pub(crate) fn probe(key: PlanKey) -> Option<StmtVerdict> {
    PLANE.with(|p| {
        let mut p = p.borrow_mut();
        match p.entries.get(&key.handle) {
            Some((generation, v)) if *generation == key.generation => {
                let v = v.clone();
                p.counters.hits += 1;
                Some(v)
            }
            Some(_) => {
                p.entries.remove(&key.handle);
                p.counters.supersedes += 1;
                None
            }
            None => None,
        }
    })
}

/// Store one verdict (overwrites — a newer generation supersedes).
pub(crate) fn store(key: PlanKey, v: StmtVerdict) {
    PLANE.with(|p| {
        let mut p = p.borrow_mut();
        p.entries.insert(key.handle, (key.generation, v));
        p.counters.stores += 1;
    });
}

/// Drop a verdict whose engine currency failed (stale data epoch — the
/// caller re-lowers and re-stores).
pub(crate) fn drop_stale(key: PlanKey) {
    PLANE.with(|p| {
        let mut p = p.borrow_mut();
        if p.entries.remove(&key.handle).is_some() {
            p.counters.stale_drops += 1;
        }
    });
}

/// Test/e2e witness: (hits, stores, supersedes, stale_drops).
pub(crate) fn counters() -> (u64, u64, u64, u64) {
    PLANE.with(|p| {
        let c = &p.borrow().counters;
        (c.hits, c.stores, c.supersedes, c.stale_drops)
    })
}

// ---------------------------------------------------------------------------
// #511 handlers (installed once per process at init_seams; each runs on
// the owning backend thread — the plancache is thread-local).
// ---------------------------------------------------------------------------

fn plan_dropped_handler(h: ::types_portal::PlanSourceHandle) {
    PLANE.with(|p| {
        let mut p = p.borrow_mut();
        if p.entries.remove(&h.0).is_some() {
            p.counters.plan_invalidations += 1;
        }
    });
}

fn plan_cache_reset_handler() {
    PLANE.with(|p| {
        let mut p = p.borrow_mut();
        p.entries.clear();
        p.counters.resets += 1;
    });
}

pub fn init_seams() {
    plancache_portal_seams::engine_plan_dropped::set(plan_dropped_handler);
    plancache_portal_seams::engine_plan_cache_reset::set(plan_cache_reset_handler);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(cause: RefuseCause) -> StmtVerdict {
        StmtVerdict::Refused { cause, engine: None }
    }

    fn probe_cause(k: PlanKey) -> Option<RefuseCause> {
        match probe(k) {
            Some(StmtVerdict::Refused { cause, .. }) => Some(cause),
            Some(StmtVerdict::Lowered(_)) => panic!("refusal expected"),
            None => None,
        }
    }

    /// One store, executions 2..N hit; a different handle misses; a
    /// generation bump is a structural miss that supersedes eagerly.
    #[test]
    fn store_hit_supersede() {
        let k = PlanKey { handle: 9007, generation: 1 };
        assert!(probe(k).is_none(), "cold miss");
        let (h0, s0, _, _) = counters();
        store(k, refused(RefuseCause::BindParams));
        assert_eq!(probe_cause(k), Some(RefuseCause::BindParams));
        let (h1, s1, _, _) = counters();
        assert_eq!((h1 - h0, s1 - s0), (1, 1), "hit/store witnesses move");
        assert!(probe(PlanKey { handle: 9008, generation: 1 }).is_none());
        // Rebuild: same handle, new generation — stale entry drops.
        let k2 = PlanKey { handle: 9007, generation: 2 };
        assert!(probe(k2).is_none(), "generation bump misses structurally");
        assert!(probe(k).is_none(), "stale generation superseded, not shadowed");
        plan_dropped_handler(::types_portal::PlanSourceHandle(9007));
    }

    /// The #511 faces leave nothing behind.
    #[test]
    fn invalidation_faces_clear() {
        let k = PlanKey { handle: 9107, generation: 3 };
        store(k, refused(RefuseCause::EpqRecheck));
        plan_dropped_handler(::types_portal::PlanSourceHandle(9107));
        assert!(probe(k).is_none(), "dropped plan leaves nothing behind");
        store(k, refused(RefuseCause::EpqRecheck));
        plan_cache_reset_handler();
        assert!(probe(k).is_none(), "reset leaves nothing behind");
    }

    /// Stale-engine drops release the entry for a fresh re-lower.
    #[test]
    fn stale_engine_drop() {
        let k = PlanKey { handle: 9207, generation: 1 };
        store(k, refused(RefuseCause::CursorCadence));
        drop_stale(k);
        assert!(probe(k).is_none());
    }
}
