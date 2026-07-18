//! H6: QPG-lite species-guided seed scheduling for the campaign loop.
//!
//! Plain-English idea: the campaign runs one generated workload per "seed" (a
//! number that fully determines the workload). The plan-species census
//! (src/metrics.rs) fingerprints every executed SELECT's query plan; a seed
//! that mints a fingerprint the campaign has never seen is called PRODUCTIVE.
//! QPG (Ba & Rigger, ICSE 2023) showed that steering a fuzzer toward inputs
//! that reach new query plans finds deeper plans and more bugs. This module is
//! the "lite" version of that policy: when a seed is productive, spend the
//! next few iterations on NEIGHBOR seeds (nearby seed values — with a seeded
//! generator each distinct seed re-derives a related-but-different workload)
//! before going back to plain sequential seeds. Unproductive streaks decay
//! back to pure sequential ("random") scheduling.
//!
//! Everything is deterministic given the campaign seed base: the schedule is
//! a pure function of (seed_base, budget, config, the productive/unproductive
//! answers), and those answers are themselves deterministic because plan
//! generation and both engines are.
//!
//! ## OFF BY DEFAULT — the FSE'21 blind-arm law
//!
//! The scheduler is behind the `--species-sched` campaign flag and OFF by
//! default. Reason (src/metrics.rs module doc, Böhme/Liyanage/Wüstholz,
//! ESEC/FSE 2021): the Good-Turing residual U = f1/n is only sound for BLIND
//! (blackbox, feedback-free) campaigns. A guided arm adapts to what it found,
//! which biases the estimator by up to four orders of magnitude. So:
//!   - blind arms (flag off) keep the H5 seed sequence BYTE-IDENTICAL
//!     (seed_base, seed_base+1, ..) and their U stays quotable;
//!   - guided arms (flag on) get their U SUPPRESSED in metrics.json and on
//!     stdout, replaced by an explicit unsound-under-feedback note.
//! The pinned census vocabulary (verdict classes) is never touched by any of
//! this — scheduling is a metrics/ordering concern only.

use std::collections::{BTreeSet, VecDeque};

/// Campaign-level scheduling configuration (CLI-carried).
#[derive(Debug, Clone)]
pub struct ScheduleConfig {
    /// Master switch. False = blind arm = H5-identical sequential seeds.
    pub enabled: bool,
    /// Follow-on budget: how many neighbor seeds a productive seed earns.
    pub neighbors: u32,
    /// Decay: after this many consecutive unproductive seeds, drop any queued
    /// neighbors and return to pure sequential scheduling.
    pub decay: u32,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        ScheduleConfig { enabled: false, neighbors: 4, decay: 8 }
    }
}

/// Neighbor stride: the 1000th prime. Neighbor j of a productive seed s is
/// s + j*7919 — a NEARBY seed value (the charter's cheap mutation: with a
/// seeded ChaCha8 generator, any distinct seed re-derives a fresh workload,
/// so "nearby" buys determinism and collision-freedom against the sequential
/// fresh band rather than plan similarity). Collisions with already-run or
/// already-queued seeds are skipped via the seen-set.
pub const NEIGHBOR_STRIDE: u64 = 7919;

/// Per-campaign scheduling statistics (metrics.json `schedule` section).
#[derive(Debug, Clone, Default)]
pub struct ScheduleStats {
    pub enabled: bool,
    pub neighbors: u32,
    pub decay: u32,
    /// Seeds actually executed (== the campaign seed budget).
    pub seeds_total: u64,
    /// Seeds drawn from the sequential (fresh) band.
    pub fresh_seeds: u64,
    /// Seeds drawn from the neighbor queue (guided follow-ons).
    pub guided_seeds: u64,
    /// Seeds that minted at least one new plan species.
    pub productive_seeds: u64,
    /// Productive seeds that were themselves guided follow-ons (the payoff
    /// counter: QPG-lite "worked" when this is > 0).
    pub productive_guided: u64,
    /// Times an unproductive streak cleared the neighbor queue.
    pub decays: u64,
}

impl ScheduleStats {
    /// Fraction of executed seeds that minted a new species.
    pub fn productive_fraction(&self) -> Option<f64> {
        if self.seeds_total == 0 {
            None
        } else {
            Some(self.productive_seeds as f64 / self.seeds_total as f64)
        }
    }
}

/// The scheduler. Drive it as:
/// `while let Some(seed) = sched.next_seed() { ...run...; sched.report(p); }`
#[derive(Debug)]
pub struct SpeciesScheduler {
    cfg: ScheduleConfig,
    seed_base: u64,
    budget: u64,
    yielded: u64,
    /// Offset of the next fresh (sequential) seed: seed_base + fresh_next.
    fresh_next: u64,
    /// Queued neighbor seeds (FIFO).
    queue: VecDeque<u64>,
    /// Every seed ever yielded or queued — dedup guard so a neighbor never
    /// re-runs a seed the fresh band already ran (or will run next).
    seen: BTreeSet<u64>,
    /// Consecutive unproductive seeds (any kind).
    streak: u32,
    /// The seed most recently yielded and whether it was guided; consumed by
    /// `report`.
    pending_report: Option<(u64, bool)>,
    pub stats: ScheduleStats,
}

/// Queued-neighbor cap: bounds memory and keeps a productivity burst from
/// starving the fresh band for the whole campaign.
const QUEUE_CAP: usize = 64;

impl SpeciesScheduler {
    pub fn new(cfg: ScheduleConfig, seed_base: u64, budget: u64) -> Self {
        let stats = ScheduleStats {
            enabled: cfg.enabled,
            neighbors: cfg.neighbors,
            decay: cfg.decay,
            ..ScheduleStats::default()
        };
        SpeciesScheduler {
            cfg,
            seed_base,
            budget,
            yielded: 0,
            fresh_next: 0,
            queue: VecDeque::new(),
            seen: BTreeSet::new(),
            streak: 0,
            pending_report: None,
            stats,
        }
    }

    /// Next seed to run, or None when the campaign budget is spent. Guided
    /// follow-ons and fresh seeds draw from the SAME budget: the campaign
    /// always executes exactly `budget` seeds.
    pub fn next_seed(&mut self) -> Option<u64> {
        assert!(self.pending_report.is_none(), "report() the previous seed first");
        if self.yielded >= self.budget {
            return None;
        }
        // Guided first: drain queued neighbors (skipping any the campaign
        // already ran — dedup keeps every executed workload distinct).
        if self.cfg.enabled {
            while let Some(s) = self.queue.pop_front() {
                if self.seen.insert(s) {
                    self.yielded += 1;
                    self.stats.seeds_total += 1;
                    self.stats.guided_seeds += 1;
                    self.pending_report = Some((s, true));
                    return Some(s);
                }
            }
        }
        // Fresh sequential band (H5-identical when the flag is off: the seen
        // set only ever holds prior sequential seeds, so nothing skips).
        loop {
            let s = self.seed_base + self.fresh_next;
            self.fresh_next += 1;
            if self.seen.insert(s) {
                self.yielded += 1;
                self.stats.seeds_total += 1;
                self.stats.fresh_seeds += 1;
                self.pending_report = Some((s, false));
                return Some(s);
            }
        }
    }

    /// Report whether the just-run seed minted a new plan species. In blind
    /// mode this only tallies stats; in guided mode a productive seed earns
    /// `neighbors` queued follow-ons and an unproductive streak of `decay`
    /// clears the queue (back to pure sequential).
    pub fn report(&mut self, productive: bool) {
        let (seed, guided) =
            self.pending_report.take().expect("report() without a next_seed()");
        if productive {
            self.stats.productive_seeds += 1;
            if guided {
                self.stats.productive_guided += 1;
            }
        }
        if !self.cfg.enabled {
            return;
        }
        if productive {
            self.streak = 0;
            for j in 1..=u64::from(self.cfg.neighbors) {
                if self.queue.len() >= QUEUE_CAP {
                    break;
                }
                let n = seed.wrapping_add(j.wrapping_mul(NEIGHBOR_STRIDE));
                if !self.seen.contains(&n) && !self.queue.contains(&n) {
                    self.queue.push_back(n);
                }
            }
        } else {
            self.streak += 1;
            if self.streak >= self.cfg.decay && !self.queue.is_empty() {
                self.queue.clear();
                self.stats.decays += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive a scheduler against a scripted novelty oracle: `oracle(seed)`
    /// says whether that seed is productive. Returns the yielded sequence.
    fn drive(cfg: ScheduleConfig, base: u64, budget: u64, oracle: impl Fn(u64) -> bool) -> Vec<u64> {
        let mut s = SpeciesScheduler::new(cfg, base, budget);
        let mut out = Vec::new();
        while let Some(seed) = s.next_seed() {
            out.push(seed);
            s.report(oracle(seed));
        }
        out
    }

    #[test]
    fn blind_mode_is_h5_identical_sequential() {
        // Flag off: whatever the oracle says, the sequence is exactly
        // seed_base..seed_base+budget — the H5 behavior, byte-identical.
        for oracle in [true, false] {
            let got = drive(ScheduleConfig::default(), 1000, 50, |_| oracle);
            let want: Vec<u64> = (1000..1050).collect();
            assert_eq!(got, want);
        }
    }

    #[test]
    fn budget_is_exact_in_both_modes() {
        let blind = drive(ScheduleConfig::default(), 1, 100, |s| s % 3 == 0);
        assert_eq!(blind.len(), 100);
        let guided = drive(
            ScheduleConfig { enabled: true, neighbors: 4, decay: 8 },
            1,
            100,
            |s| s % 3 == 0,
        );
        assert_eq!(guided.len(), 100);
    }

    #[test]
    fn guided_mode_is_deterministic() {
        let cfg = ScheduleConfig { enabled: true, neighbors: 3, decay: 5 };
        let a = drive(cfg.clone(), 42, 200, |s| s % 7 == 0);
        let b = drive(cfg, 42, 200, |s| s % 7 == 0);
        assert_eq!(a, b);
    }

    #[test]
    fn productive_seed_earns_neighbors_then_returns_to_fresh() {
        // Only seed 10 is productive: expect 10, then its 2 neighbors, then
        // fresh seeds again.
        let cfg = ScheduleConfig { enabled: true, neighbors: 2, decay: 100 };
        let got = drive(cfg, 10, 5, |s| s == 10);
        assert_eq!(
            got,
            vec![10, 10 + NEIGHBOR_STRIDE, 10 + 2 * NEIGHBOR_STRIDE, 11, 12]
        );
    }

    #[test]
    fn unproductive_streak_decays_queue() {
        // Seed 1 is productive and earns 8 neighbors; nothing else ever is,
        // so after `decay` consecutive misses the queue clears and the rest
        // of the budget is fresh sequential.
        let cfg = ScheduleConfig { enabled: true, neighbors: 8, decay: 3 };
        let got = drive(cfg, 1, 10, |s| s == 1);
        let n = NEIGHBOR_STRIDE;
        // 1 (productive), 3 guided misses, decay clears remaining 5 queued,
        // then fresh 2, 3, 4, 5, 6, 7.
        assert_eq!(
            got,
            vec![1, 1 + n, 1 + 2 * n, 1 + 3 * n, 2, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn neighbor_dedup_never_reruns_a_seed() {
        // Every seed productive: heavy queueing; still no duplicates.
        let cfg = ScheduleConfig { enabled: true, neighbors: 4, decay: 8 };
        let got = drive(cfg, 5, 300, |_| true);
        let mut sorted = got.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), got.len(), "duplicate seed executed");
    }

    #[test]
    fn stats_count_fresh_guided_productive() {
        let cfg = ScheduleConfig { enabled: true, neighbors: 2, decay: 100 };
        let mut s = SpeciesScheduler::new(cfg, 10, 5);
        let mut seq = Vec::new();
        while let Some(seed) = s.next_seed() {
            seq.push(seed);
            s.report(seed == 10);
        }
        assert_eq!(s.stats.seeds_total, 5);
        assert_eq!(s.stats.fresh_seeds, 3);
        assert_eq!(s.stats.guided_seeds, 2);
        assert_eq!(s.stats.productive_seeds, 1);
        assert_eq!(s.stats.productive_guided, 0);
        assert_eq!(s.stats.productive_fraction(), Some(0.2));
    }
}
