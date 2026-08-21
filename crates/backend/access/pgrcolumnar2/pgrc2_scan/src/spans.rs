//! The reader's claim-unit space: global granule ordinals over an opened
//! part set — claim-plane CONSUMER #2 (PC-2.1/PC-2.3/PC-3.1).
//!
//! Units are whole granules at each part's OWN SB-10 grain; part edges are
//! the hard boundaries (a dict-epoch edge IS a part edge — spec §7 Law A),
//! so a span never crosses a part and every claim pins exactly one part.
//! The unit space is a pure function of the part set (granule counts from
//! sealed footers) — DOP- and schedule-independent by construction, the
//! PC-3.4 precondition.

use pgrc2_claim::MorselSource;

/// One part's slice of the global granule ordinal space.
#[derive(Debug, Clone, Copy)]
pub struct PartUnits {
    /// Global ordinal of this part's granule 0.
    pub base: u64,
    pub granules: u32,
}

/// The MorselSource over a fixed, opened part set (static space: fully
/// published at construction).
pub struct GranuleSpans {
    parts: Vec<PartUnits>,
    total: u64,
}

impl GranuleSpans {
    pub fn new(granule_counts: &[u32]) -> GranuleSpans {
        let mut parts = Vec::with_capacity(granule_counts.len());
        let mut base = 0u64;
        for &gc in granule_counts {
            parts.push(PartUnits {
                base,
                granules: gc,
            });
            base += gc as u64;
        }
        GranuleSpans {
            parts,
            total: base,
        }
    }

    /// Locate a unit: (part index, granule ordinal within the part).
    pub fn locate(&self, unit: u64) -> (usize, u32) {
        debug_assert!(unit < self.total);
        // Parts are few and bases ascend; binary search. Zero-granule
        // parts share their successor's base — skip forward past them
        // (a unit never belongs to an empty part).
        let mut idx = match self.parts.binary_search_by(|p| p.base.cmp(&unit)) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        while self.parts[idx].granules == 0
            || unit >= self.parts[idx].base + self.parts[idx].granules as u64
        {
            idx += 1;
        }
        (idx, (unit - self.parts[idx].base) as u32)
    }

    pub fn total_granules(&self) -> u64 {
        self.total
    }
}

/// The PRUNED claim-unit space (M4-S5, the prune chokepoint's claim half):
/// claim units are INDICES into an ascending survivor list of global
/// granule units — a verdict-eliminated granule has no index and therefore
/// NEVER claims (prune-first execution order, by construction). Part edges
/// remain the hard boundaries: [`MorselSource::next_boundary_after`] maps
/// through the survivor list, so one claim still pins exactly one part
/// (PC-2.1). The space is a pure function of (part set, derived verdicts) —
/// leader-derived once, schedule-independent (PC-3.4 holds).
pub struct SurvivorSpans {
    /// Ascending global units (the derive's survivors).
    units: Vec<u64>,
    /// For each survivor index, the exclusive index where its part ends
    /// (precomputed so `next_boundary_after` is O(1)).
    part_end: Vec<u64>,
    /// Claim geometry (M4-S6: supplied by the ReadPlan's batch-geometry
    /// face; any constant is byte-identical — ordering lives in the
    /// unit-keyed sink). Defaults to the standing 4/4 posture.
    ramp_seed: u64,
    coalesce_max: u64,
}

impl SurvivorSpans {
    /// Build from the dense space + the derived survivor list (ascending).
    pub fn new(dense: &GranuleSpans, units: Vec<u64>) -> SurvivorSpans {
        debug_assert!(units.windows(2).all(|w| w[0] < w[1]), "survivors ascend");
        SurvivorSpans::new_grouped(dense, units)
    }

    /// The ORDERED claim-space form (v2-64, the zone-adaptive topN
    /// traversal): units arrive in a caller-chosen TRAVERSAL order — not
    /// necessarily ascending — and claim indices walk that order front to
    /// back. Part edges stay the hard boundaries by construction: the run
    /// detection below finds maximal same-part streaks, so a claim never
    /// crosses a part (PC-2.1 holds; a part split across non-adjacent runs
    /// simply yields more, shorter runs). The space remains a pure
    /// function of its inputs (order included) — schedule-independent
    /// (PC-3.4 holds for any consumer whose sink is order-keyed, which is
    /// the standing sink law).
    pub fn new_grouped(dense: &GranuleSpans, units: Vec<u64>) -> SurvivorSpans {
        let mut part_end = vec![0u64; units.len()];
        let mut i = 0usize;
        while i < units.len() {
            let (pidx, _) = dense.locate(units[i]);
            let mut j = i + 1;
            while j < units.len() && dense.locate(units[j]).0 == pidx {
                j += 1;
            }
            for k in i..j {
                part_end[k] = j as u64;
            }
            i = j;
        }
        SurvivorSpans { units, part_end, ramp_seed: 4, coalesce_max: 4 }
    }

    /// The geometry face (M4-S6): claim ramp/coalesce as PLAN facts —
    /// the one planning face decides them; the claim plane consumes.
    pub fn with_geometry(mut self, ramp_seed: u64, coalesce_max: u64) -> SurvivorSpans {
        self.ramp_seed = ramp_seed.max(1);
        self.coalesce_max = coalesce_max.max(1);
        self
    }

    /// The global unit at survivor index `idx`.
    pub fn unit_at(&self, idx: u64) -> u64 {
        self.units[idx as usize]
    }

    /// The survivor units of an index span (the claim's unit list — all in
    /// one part, by the boundary law).
    pub fn units_of(&self, span: pgrc2_claim::Span) -> &[u64] {
        &self.units[span.start as usize..span.end as usize]
    }

    pub fn survivor_count(&self) -> u64 {
        self.units.len() as u64
    }
}

/// The WINNER-GRANULE refetch space (M4-S7): **SpanClaimSource** — the
/// contract's v4 vocabulary for the adapter that claims ONLY winner
/// granules (PC-2.5's claim-narrow face is the SOLE sub-unit claim shape;
/// AB-5.3's winner-only late materialization is the consumer). Columnar
/// rowids address granules directly (AB-5.2 totality), so the unit space
/// is the sorted, deduped winner-granule list — mechanically the
/// [`SurvivorSpans`] shape (indices into an ascending global-unit list,
/// part edges hard boundaries), reused as the inner space so the claim
/// mechanics have exactly one implementation. The refetch drive claims
/// index spans, then NARROWS to one winner granule at a time through
/// [`pgrc2_claim::ClaimGuard::narrow`] and hydrates only the winner rows
/// (`decode_sel`).
pub struct SpanClaimSource {
    inner: SurvivorSpans,
}

impl SpanClaimSource {
    /// Build from the dense space + the ascending winner-granule unit
    /// list (sorted, deduped — the caller's BTreeMap order).
    pub fn new(dense: &GranuleSpans, winner_units: Vec<u64>) -> SpanClaimSource {
        SpanClaimSource { inner: SurvivorSpans::new(dense, winner_units) }
    }

    /// The global unit at winner index `idx`.
    pub fn unit_at(&self, idx: u64) -> u64 {
        self.inner.unit_at(idx)
    }

    pub fn winner_granules(&self) -> u64 {
        self.inner.survivor_count()
    }
}

impl MorselSource for SpanClaimSource {
    fn total_units(&self) -> Option<u64> {
        self.inner.total_units()
    }
    fn published(&self) -> u64 {
        self.inner.published()
    }
    fn next_boundary_after(&self, unit: u64) -> u64 {
        self.inner.next_boundary_after(unit)
    }
    fn ramp_seed(&self) -> u64 {
        self.inner.ramp_seed()
    }
    fn coalesce_max(&self) -> u64 {
        self.inner.coalesce_max()
    }
}

impl MorselSource for SurvivorSpans {
    fn total_units(&self) -> Option<u64> {
        Some(self.units.len() as u64)
    }

    fn published(&self) -> u64 {
        self.units.len() as u64
    }

    fn next_boundary_after(&self, unit: u64) -> u64 {
        self.part_end[unit as usize]
    }

    /// The planned geometry (default = the dense space's 4/4 posture; any
    /// constant is byte-identical — ordering lives in the unit-keyed sink).
    fn ramp_seed(&self) -> u64 {
        self.ramp_seed
    }

    fn coalesce_max(&self) -> u64 {
        self.coalesce_max
    }
}

impl MorselSource for GranuleSpans {
    fn total_units(&self) -> Option<u64> {
        Some(self.total)
    }

    fn published(&self) -> u64 {
        self.total
    }

    /// Part edges are the hard boundaries (PC-2.1: a claim never crosses a
    /// part edge or dict-epoch edge; the two coincide here).
    fn next_boundary_after(&self, unit: u64) -> u64 {
        let (idx, _) = self.locate(unit);
        self.parts[idx].base + self.parts[idx].granules as u64
    }

    /// Startup ramp = coalesce posture = a small whole-granule span. Any
    /// constant here yields byte-identical output (ordering lives in the
    /// unit-keyed sink, never the claim schedule); 4 granules balances
    /// claim traffic against skew-driven imbalance at QA scale.
    fn ramp_seed(&self) -> u64 {
        4
    }

    fn coalesce_max(&self) -> u64 {
        4
    }
}
