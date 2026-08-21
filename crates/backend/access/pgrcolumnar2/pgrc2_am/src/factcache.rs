//! M5a: the PROCESS-GRAIN footer-fact cache — the S-2 planner wiring's
//! re-home target (the M4-S5 revert's measured-cost note in plancat.rs;
//! triage row "S-2 planner wiring RE-HOMES to M5a behind a process-grain
//! fact cache").
//!
//! ## Why process grain
//!
//! The deep footer fold ([`crate::footer`]: NDV HLL union + per-column
//! disk bytes + width fold) walks every part's zstd-wrapped meta sections
//! across all columns. The session TLS cache amortizes it per BACKEND —
//! but the meta band's cells are cold-session plans, and the S5 landing
//! measured the per-connection cold fold at **+3ms/plan (1m bank, 6
//! parts) and +22ms/plan (10m, 52 parts) across 105 columns** — the whole
//! meta band regressed 0.001→0.023 and the consult was REVERTED. Sealed
//! parts are immutable and a manifest generation's content never changes,
//! so the fold is a pure function of the generation: ONE process-wide fold
//! per published generation is the correct amortization grain. A cold
//! session's first plan probes this cache in nanoseconds (one mutex + one
//! BTreeMap probe + one Arc clone) instead of re-walking parts.
//!
//! ## Staleness — structurally impossible
//!
//! Two independent guarantees, either alone sufficient:
//!
//! 1. **Content addressing.** The key is `(spcOid, dbOid, relfilenumber)`
//!    → entries pinned by `(gen, publisher_fxid, schema_fingerprint)`.
//!    Publish bumps `gen` (generational manifest law); an aborted gen
//!    re-published under the same number carries a DIFFERENT
//!    `publisher_fxid` (epoch-qualified 64-bit — recycling cannot alias,
//!    the manifest-header law); a DDL that changes column shape changes
//!    the schema fingerprint. A probe that does not match all three is a
//!    MISS. Entries are immutable snapshots of immutable inputs.
//! 2. **Resolve-precedes-read** (the [`crate::footer`] law, unchanged):
//!    every ask resolves the effective manifest FIRST — visibility-correct
//!    per backend (commit-checked) — and probes under the gen it resolved.
//!    A backend inside an uncommitted-publish transaction resolves ITS
//!    gen; other backends resolve the committed one; the two never share
//!    an entry (distinct gens), so cross-visibility contamination cannot
//!    occur.
//!
//! The relcache-invalidation faces (the #511/#512 lineage carried by
//! [`crate::inval`]) additionally DROP entries on relcache invalidation
//! (DDL, DROP/TRUNCATE) — pure memory hygiene under the staleness law
//! above, and the bound on dead weight: entries for dropped tables die
//! with the invalidation instead of postmaster lifetime. A PUBLISH needs
//! no invalidation at all: it bumps `gen`, the next resolve carries the
//! new key, and the probe structurally misses (old-gen entries age out of
//! the [`SLOTS_PER_REL`] ring at the next fold).
//!
//! ## Concurrency posture
//!
//! First-fold races are benign: two backends cold on the same generation
//! both fold and publish identical content (pure function); last write
//! wins, both serve truth. The map holds up to [`SLOTS_PER_REL`] entries
//! per relation so a not-yet-committed publish's fold (gen g+1, folded by
//! the publishing backend) never evicts the committed generation's entry
//! (gen g, everyone else's) during the publish window.

use std::collections::BTreeMap;
use std::sync::Arc;

use types_core::Oid;

use crate::footer::FooterFacts;

/// Entries kept per relation slot: the committed generation + one
/// publish-window successor (see module doc "Concurrency posture").
const SLOTS_PER_REL: usize = 2;

/// One relation's physical slot: tablespace, database, relfilenumber.
/// relfilenumber alone is NOT process-unique — backends of different
/// databases share this cache (the session-TLS cache never had to care).
pub type RelSlot = (u32, u32, u64);

/// One immutable fold, pinned to its exact inputs.
#[derive(Debug, Clone)]
struct Entry {
    gen: u64,
    publisher_fxid: u64,
    schema_fp: u64,
    facts: Arc<FooterFacts>,
}

#[derive(Debug)]
struct Cache {
    /// rel slot → newest-first entries (≤ [`SLOTS_PER_REL`]).
    map: BTreeMap<RelSlot, Vec<Entry>>,
    /// relid → rel slot, recorded at ask time so the relcache callback
    /// can translate its currency (relid) into ours.
    relids: BTreeMap<u32, RelSlot>,
    /// Fold-vintage ANALYZE witness (M5a engine ANALYZE fold): rel slots
    /// whose pg_statistic rows this process wrote via the fold — the S-1
    /// pg_stat read leg's honest SketchFold stamping input.
    fold_vintage: std::collections::BTreeSet<RelSlot>,
    /// Test/census witnesses.
    probe_hits: u64,
    probe_misses: u64,
    deep_folds: u64,
}

impl Cache {
    const fn new() -> Cache {
        Cache {
            map: BTreeMap::new(),
            relids: BTreeMap::new(),
            fold_vintage: std::collections::BTreeSet::new(),
            probe_hits: 0,
            probe_misses: 0,
            deep_folds: 0,
        }
    }
}

pgsync::process_global! {
    /// The process-shared fact cache (const-init static, no OnceLock —
    /// the determinism ledger's `once` category stays empty; the
    /// [`crate::inval`] REGISTRY pattern).
    static FACTS: pgsync::Mutex<Cache> = pgsync::Mutex::new(Cache::new());
}

/// Probe for the fold of exactly `(gen, publisher_fxid, schema_fp)` at
/// `slot`. A hit is served in nanoseconds and can never be stale (module
/// doc law 1). Only DEEP folds are stored, so a hit always carries
/// `ndv`/`cols`.
pub fn probe(slot: RelSlot, gen: u64, publisher_fxid: u64, schema_fp: u64) -> Option<Arc<FooterFacts>> {
    let mut c = pgsync::lock(&FACTS);
    let hit = c.map.get(&slot).and_then(|v| {
        v.iter()
            .find(|e| e.gen == gen && e.publisher_fxid == publisher_fxid && e.schema_fp == schema_fp)
            .map(|e| Arc::clone(&e.facts))
    });
    if hit.is_some() {
        c.probe_hits += 1;
    } else {
        c.probe_misses += 1;
    }
    hit
}

/// Publish one deep fold. Newest-gen-first ordering, truncated to
/// [`SLOTS_PER_REL`]; an exact-key duplicate refreshes in place (the
/// benign first-fold race — identical content by purity).
pub fn publish(
    slot: RelSlot,
    gen: u64,
    publisher_fxid: u64,
    schema_fp: u64,
    facts: Arc<FooterFacts>,
) {
    debug_assert!(facts.ndv.is_some() && facts.cols.is_some(), "factcache stores deep folds only");
    let mut c = pgsync::lock(&FACTS);
    c.deep_folds += 1;
    let v = c.map.entry(slot).or_default();
    if let Some(e) = v
        .iter_mut()
        .find(|e| e.gen == gen && e.publisher_fxid == publisher_fxid && e.schema_fp == schema_fp)
    {
        e.facts = facts;
        return;
    }
    v.push(Entry { gen, publisher_fxid, schema_fp, facts });
    v.sort_by(|a, b| b.gen.cmp(&a.gen));
    v.truncate(SLOTS_PER_REL);
}

/// Record `relid` → `slot` so [`invalidate_relid`] can find the entries
/// (called on every footer ask, before any insert — the register-before-
/// install law's cheap sibling: recording is idempotent and unkeyed
/// entries are only reachable via probes that revalidate anyway).
pub fn record_relid(relid: Oid, slot: RelSlot) {
    let mut c = pgsync::lock(&FACTS);
    c.relids.insert(relid, slot);
}

/// The relcache-invalidation face (#511/#512 lineage, wired from
/// [`crate::inval::invalidate_relid`]): drop the relation's entries.
/// `relid == 0` = full-cache invalidation event. Hygiene only — the
/// staleness law never depended on this firing (module doc).
pub fn invalidate_relid(relid: Oid) {
    let mut c = pgsync::lock(&FACTS);
    if relid == 0 {
        c.map.clear();
        c.relids.clear();
        c.fold_vintage.clear();
        return;
    }
    if let Some(slot) = c.relids.remove(&relid) {
        c.map.remove(&slot);
        // Vintage is dropped too: after DDL/DROP the conservative
        // AnalyzeSample label stands until a fresh fold-ANALYZE (a
        // reused relfilenumber must never inherit SketchFold provenance).
        c.fold_vintage.remove(&slot);
    }
}

/// Witness counters: `(probe_hits, probe_misses, deep_folds)`. The M5a
/// no-regression cell's structural claim is `deep_folds` staying flat
/// across cold sessions of one generation while `probe_hits` climbs.
pub fn counters() -> (u64, u64, u64) {
    let c = pgsync::lock(&FACTS);
    (c.probe_hits, c.probe_misses, c.deep_folds)
}

/// Record fold-vintage pg_statistic rows for `slot` (module doc field).
pub fn record_fold_vintage(slot: RelSlot) {
    let mut c = pgsync::lock(&FACTS);
    c.fold_vintage.insert(slot);
}

/// The fold-vintage probe (process-lifetime; survives publish — vintage
/// describes HOW the rows were produced, not their freshness).
pub fn is_fold_vintage(slot: RelSlot) -> bool {
    let c = pgsync::lock(&FACTS);
    c.fold_vintage.contains(&slot)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deep(gen: u64, rows: u64) -> Arc<FooterFacts> {
        Arc::new(FooterFacts {
            gen,
            rows,
            bytes: rows * 10,
            ndv: Some(vec![rows]),
            cols: Some(crate::footer::ColCostFacts {
                disk_bytes: vec![rows * 10],
                avg_width: vec![8],
            }),
        })
    }

    const SLOT_A: RelSlot = (1663, 5, 90_001);
    const SLOT_B: RelSlot = (1663, 6, 90_001); // same relfilenumber, other DB

    #[test]
    fn probe_is_content_addressed() {
        let slot = (1663, 5, 77_001);
        publish(slot, 3, 100, 42, deep(3, 1000));
        assert!(probe(slot, 3, 100, 42).is_some(), "exact key hits");
        assert!(probe(slot, 4, 100, 42).is_none(), "publish bumped gen: miss");
        assert!(
            probe(slot, 3, 101, 42).is_none(),
            "same gen number re-published under another fxid (aborted-gen reuse): miss"
        );
        assert!(probe(slot, 3, 100, 43).is_none(), "schema fingerprint drift: miss");
    }

    #[test]
    fn cross_database_slots_never_alias() {
        publish(SLOT_A, 1, 7, 1, deep(1, 111));
        publish(SLOT_B, 1, 7, 1, deep(1, 222));
        assert_eq!(probe(SLOT_A, 1, 7, 1).unwrap().rows, 111);
        assert_eq!(probe(SLOT_B, 1, 7, 1).unwrap().rows, 222);
    }

    #[test]
    fn publish_window_keeps_the_committed_generation() {
        let slot = (1663, 5, 77_002);
        publish(slot, 5, 100, 1, deep(5, 500));
        // The publishing backend folds its own uncommitted gen 6 …
        publish(slot, 6, 200, 1, deep(6, 600));
        // … and everyone still resolving gen 5 keeps hitting.
        assert_eq!(probe(slot, 5, 100, 1).unwrap().rows, 500);
        assert_eq!(probe(slot, 6, 200, 1).unwrap().rows, 600);
        // A third generation evicts the oldest (SLOTS_PER_REL = 2).
        publish(slot, 7, 300, 1, deep(7, 700));
        assert!(probe(slot, 5, 100, 1).is_none());
        assert_eq!(probe(slot, 7, 300, 1).unwrap().rows, 700);
    }

    #[test]
    fn relcache_invalidation_drops_the_relation() {
        let slot = (1663, 5, 77_003);
        publish(slot, 1, 9, 1, deep(1, 10));
        record_relid(31337, slot);
        invalidate_relid(31337);
        assert!(probe(slot, 1, 9, 1).is_none(), "invalidation dropped the slot");
        // Unknown relids are a no-op, never a panic.
        invalidate_relid(31338);
    }

    #[test]
    fn duplicate_publish_refreshes_in_place() {
        let slot = (1663, 5, 77_004);
        publish(slot, 2, 50, 3, deep(2, 42));
        publish(slot, 2, 50, 3, deep(2, 42)); // the benign first-fold race
        assert_eq!(probe(slot, 2, 50, 3).unwrap().rows, 42);
        let c = pgsync::lock(&FACTS);
        assert_eq!(c.map.get(&slot).unwrap().len(), 1, "no duplicate entries");
    }
}
