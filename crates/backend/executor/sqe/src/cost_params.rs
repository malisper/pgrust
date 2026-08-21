//! [ruling 3] Election cost parameters — the PG-cost-model idiom
//! (election-inputs-law.md §2.2, RATIFIED 2026-08-18, PG-style):
//! election constants become FORMULAS over named knobs with physical
//! meanings and documented units (the seq_page_cost/cpu_tuple_cost
//! discipline), calibrated per target family.
//!
//! BINDING SAFETY CONDITION (the ruling's own words): on the 96-thread
//! rig the formulas reproduce the election behavior of record EXACTLY
//! (`width96_reproduces_historical_constants` is the gate). The record
//! moved ONCE, deliberately: Michael ruled (2026-08-18, election-inputs
//! -law.md rulings) adoption of the width-ladder cell's MEASURED warm
//! engagement costs for E2 (width-ladder-cell.md, run 20260818T163253Z)
//! — the historical solved-for values were re-identified as the COLD
//! first-engagement price (kept below as COLD_* constants). All other
//! knobs still reproduce their historical width-96 derivations. At
//! other widths the derived laws scale as the physics says they should;
//! family election stays width-free (F3), so SERVABILITY never moves.
//!
//! Per-target calibration (the R3 CI-lane story, §4 of the law doc)
//! re-cuts these values per target family with provenance'd cells; until
//! that lane lands they are module constants — GUC plumbing is
//! deliberately NOT grown here (the pool/guc seam is another lane's;
//! wiring `TARGET` through a `pgrust.sqe_*` knob later is a seam-side
//! change that never touches these formulas).

/// The per-target election cost model. Every field carries units and
/// provenance; every derived law is a method so the formula lives in
/// exactly one place.
#[derive(Clone, Copy, Debug)]
pub struct CostParams {
    /// Per-thread fused decode+eval fold rate, rows/ms/thread (E1).
    /// Provenance: RESULTS-optdecode, 96-way c8g rig — the historical
    /// `ROWS_PER_MS` scalar, now explicitly per-thread.
    pub fold_rows_per_thread_ms: u64,
    /// Claim-plane setup cost, fixed part, nanoseconds (E2 band): the
    /// per-claim bookkeeping paid once regardless of width.
    /// Provenance: MEASURED — width-ladder cell 20260818 (c8g.16xlarge,
    /// run 20260818T163253Z; docs/design/sqe/width-ladder-cell.md §2.2),
    /// warm parked-pool engagement of the real fused fold in the
    /// well-conditioned claim regime (parts >= 4·w): the fit
    /// `setup(w) ≈ 132,808 + 29,410·w` ns, rounded. Ruled adopted by
    /// Michael 2026-08-18 (the law doc's rulings section).
    pub claim_setup_base_ns: u64,
    /// Claim-plane setup cost, per parked worker to spin up, ns/worker
    /// (E2 band): more workers cost more to wake — measured, this is
    /// per-run worker-state init (Scratch/CurCache), not the µs-class
    /// futex wake. Provenance: MEASURED (see `claim_setup_base_ns`).
    pub claim_setup_per_worker_ns: u64,
    /// Scatter-pass owner grains per worker (E7 clamp floor): the
    /// partition floor exists to hand every pool worker at least this
    /// many owner grains. Provenance: DERIVED — k_grain in the measured
    /// 2–4 band (law doc §2.2) with next_pow2(2·96) = 256 reproducing
    /// the historical floor exactly.
    pub owner_grains_per_worker: usize,
    /// Partition ceiling from scatter cost, partitions (E7 upper clamp,
    /// T-class). Provenance: the 40m-NDV sweep's 8192 (§7 sizing law) —
    /// a scatter-cost constant, not a width law; stays per-target.
    pub scatter_partition_cap: usize,
    /// Gather pass-2 owner grains per worker at pool saturation (E16):
    /// the survivor_gather `.min(2048)` was documented as "the pass-2
    /// owner grain saturates the pool there" — this is that sentence as
    /// a number. Provenance: DERIVED — next_pow2(16·96) = 2048
    /// reproduces the historical clamp exactly.
    pub gather_sat_grains_per_worker: usize,
    /// Dense count-array slot width, bytes (K-class, format-derived):
    /// one u64 count slot per domain value.
    pub dense_slot_bytes: usize,
    /// Dense-tier L2 budget, bytes (E8, T-class): the target's private
    /// L2 the dense count array must sit in. Provenance: c8g 2MiB L2
    /// (§7); 2MiB / 8B = 256K reproduces the historical `256 * 1024`
    /// dense-domain bound exactly — the F7 single authority's value.
    pub dense_l2_budget_bytes: usize,
    /// [sqe-hugedom] Direct-array grouped-state budget for UNBOUNDED
    /// answers, bytes (E17): the shared accumulator array's byte cap
    /// when the answer materializes every group. Provenance: the
    /// historical `DIRECT_ARRAY_BYTES_CAP = 1 << 28` (tpch-convergence-1
    /// mechanism 1: a 6M-key one-Sum-lane fold = 144 MB sits inside; the
    /// cap keeps the array a fraction of laptop-class memory).
    pub direct_array_budget_bytes: usize,
    /// [sqe-hugedom] Direct-array grouped-state budget for BOUNDED
    /// answers, bytes (E17b): when the ANSWER is provably small (pushed
    /// top-k <= GROUP_ROW_CAP, or a fused HAVING that emits only
    /// survivors), the accumulator may span a much wider witnessed
    /// domain — admission additionally demands every lane be zero-init
    /// (Sum/Count), so the array rides alloc_zeroed lazily-mapped pages
    /// and RSS tracks OCCUPANCY, not the domain width. Memory law: this
    /// is a per-query transient freed at the answer boundary; the
    /// domain-width virtual reservation is bounded here, the resident
    /// bound is `touched keys x slot bytes`. Provenance: this lane's cut
    /// at 8x the unbounded budget (2 GiB — a 20M-key 3-lane fold =
    /// 1.12 GB sits inside; a 6M-domain Q18-class fold touches 144 MB
    /// resident); re-cut per target like every other knob.
    pub direct_array_bounded_budget_bytes: usize,
    /// E18 — grouped-statement resident allowance per pool worker, bytes
    /// (spill-design.md §2): the work_mem-class knob the grouped SPILL
    /// budget law scales by pool width. Provenance: sized so the target
    /// rig's default-arm grouped working sets of record stay resident (no
    /// behavior change at default budgets); re-cut per target with the
    /// election-inputs discipline like every other knob. GUC plumbing
    /// stays at the pool/guc seam's lane (the sortagg-budget precedent).
    pub grouped_budget_per_worker_bytes: u64,
}

/// The target profile of record (Graviton c8g provenance, width-96
/// derivations). Per-target re-cuts replace this constant, never the
/// formulas.
pub const TARGET: CostParams = CostParams {
    fold_rows_per_thread_ms: 1_400_000,
    claim_setup_base_ns: 133_000,
    claim_setup_per_worker_ns: 29_400,
    owner_grains_per_worker: 2,
    scatter_partition_cap: 8192,
    gather_sat_grains_per_worker: 16,
    dense_slot_bytes: 8,
    dense_l2_budget_bytes: 2 * 1024 * 1024,
    direct_array_budget_bytes: 1 << 28,
    direct_array_bounded_budget_bytes: 1 << 31,
    grouped_budget_per_worker_bytes: 64 * 1024 * 1024,
};

/// The COLD first-engagement line (width-ladder cell §2.2/§5 condition
/// 2): a fresh engine's first fold pays bank first-touch on top of the
/// warm engagement — measured `setup_cold(w) ≈ 1,142,512 + 143,318·w`
/// ns on the same c8g cell. Evaluated at width 96 this is 14.90ms —
/// today's retired defaults (14.84ms) to within 0.4%: the historical
/// solved-for values were a faithful price of a COLD first engagement.
/// Kept as the documented cold price; deliberately UNUSED — if a
/// first-touch-sensitive surface appears (serverless cold starts), it
/// enters as a separate cold-start term, never as E2's steady-state
/// value.
pub const COLD_CLAIM_SETUP_BASE_NS: u64 = 1_142_512;
/// See `COLD_CLAIM_SETUP_BASE_NS`.
pub const COLD_CLAIM_SETUP_PER_WORKER_NS: u64 = 143_318;

/// The active parameter set. Module-constant today (see module doc for
/// the deliberate no-GUC note).
#[inline]
pub fn target() -> &'static CostParams {
    &TARGET
}

impl CostParams {
    /// Claim-plane setup cost at pool width `w`, nanoseconds:
    /// `setup(w) = base + per_worker · w`.
    #[inline]
    pub fn claim_setup_ns(&self, width: usize) -> u64 {
        self.claim_setup_base_ns
            .saturating_add(self.claim_setup_per_worker_ns.saturating_mul(width as u64))
    }

    /// E2 — the serial cutoff as a derived law (law doc §2.2 row 1):
    /// serial iff `T_serial < T_parallel`, i.e.
    /// `rows/rate < rows/(rate·w) + setup(w)`  ⇒
    /// `rows < rate · setup(w) · w/(w−1)`.
    /// Exact integer evaluation in u128; at width 96 this is precisely
    /// the historical `SERIAL_CUTOFF_MS × ROWS_PER_MS = 21,000,000`.
    /// Width ≤ 1 has no parallel body to beat: always serial.
    #[inline]
    pub fn serial_cutoff_rows(&self, width: usize) -> u64 {
        if width <= 1 {
            return u64::MAX;
        }
        let rate = self.fold_rows_per_thread_ms as u128; // rows/ms/thread
        let setup = self.claim_setup_ns(width) as u128; // ns
        let w = width as u128;
        // rows = rate[rows/ms] · setup[ns] · w / ((w−1) · 1e6[ns/ms])
        let rows = rate * setup * w / ((w - 1) * 1_000_000);
        u64::try_from(rows).unwrap_or(u64::MAX)
    }

    /// E7 — the partition-count lower clamp as a width law: enough owner
    /// grains to hand every worker `owner_grains_per_worker`, rounded to
    /// a power of two (the partition law's arithmetic), never above the
    /// scatter ceiling. `next_pow2(2·96) = 256` — the historical floor.
    #[inline]
    pub fn partition_floor(&self, width: usize) -> usize {
        (self.owner_grains_per_worker.saturating_mul(width.max(1)))
            .next_power_of_two()
            .min(self.scatter_partition_cap)
    }

    /// E16 — survivor_gather's pass-2 partition ceiling: the width at
    /// which the pass-2 owner grain saturates the pool, capped by the
    /// scatter ceiling. `min(8192, next_pow2(16·96)) = 2048` — the
    /// historical `.min(2048)`.
    #[inline]
    pub fn gather_partition_cap(&self, width: usize) -> usize {
        (self.gather_sat_grains_per_worker.saturating_mul(width.max(1)))
            .next_power_of_two()
            .min(self.scatter_partition_cap)
    }

    /// E17 — the dense direct-array PRIVATE-fold bound: per-worker
    /// private accumulator lanes (merged once per worker at generation
    /// finish) are elected when the whole per-worker slot footprint —
    /// one count slot plus (value + non-null count) per fold lane —
    /// sits in the target's private L2; above it the shared atomic
    /// array serves (its memory is width-free, and at large domains
    /// slot collisions — the tiny-domain cache-line contention that
    /// motivates this bound — are rare). Provenance: the g16 diagnosis
    /// cell (P7-2 ledger, grouped-by-16, 10M rows): the shared atomic
    /// array at dom=16 put every worker's 3 RMWs/row on the same two
    /// cache lines — 619ms of pass1 against lanev2's 7ms serve; the
    /// same formula's L2 term as E8, so the bound is a derivation, not
    /// a new constant.
    #[inline]
    pub fn dense_private_fit(&self, dn: usize, fold_lanes: usize) -> bool {
        dn.saturating_mul(self.dense_slot_bytes.saturating_mul(1 + 2 * fold_lanes))
            <= self.dense_l2_budget_bytes
    }

    /// E8/F1 — the dense-tier domain bound, F7's single authority:
    /// the dense count array must sit in the L2 budget,
    /// `dom ≤ l2_budget / slot_bytes`. 2MiB / 8B = 256K — the
    /// historical `256 * 1024` (and the retirement of family.rs's
    /// disagreeing `1 << 20`).
    #[inline]
    pub fn dense_domain_cap(&self) -> u128 {
        (self.dense_l2_budget_bytes / self.dense_slot_bytes.max(1)) as u128
    }

    /// E17/E17b — [sqe-hugedom] the direct-array accumulator byte budget
    /// under the answer-bound law: an UNBOUNDED answer keeps the
    /// resident-sized budget; a BOUNDED answer (pushed top-k or fused
    /// HAVING, zero-init lanes only) gets the occupancy-priced budget.
    #[inline]
    pub fn direct_array_cap(&self, bounded: bool) -> usize {
        if bounded {
            self.direct_array_bounded_budget_bytes
        } else {
            self.direct_array_budget_bytes
        }
    }

    /// E17 (answer face) — [cap-retire] the grouped ANSWER-plane
    /// materialization budget, bytes: the exact-counted finalize answer
    /// bytes (group staging rows + rendered emit lanes) a grouped
    /// statement may hand to the protocol whole (spill-design.md §3.4,
    /// RULED 2026-08-19). Provenance: this is E17's unbounded-
    /// materialization budget wearing its second face — the SAME
    /// "unbounded materialization stays a fraction of laptop-class
    /// memory" law that sized the direct-array state plane (extend,
    /// don't duplicate: one authority, one constant, one re-cut per
    /// target). At 256 MiB the former q31 wrong-answer class (1.33M true
    /// groups, ~85 MB emitted) serves its FULL correct answer with 3x
    /// headroom.
    #[inline]
    pub fn answer_budget_bytes(&self) -> u64 {
        self.direct_array_budget_bytes as u64
    }

    /// E18 — the grouped-statement SPILL byte budget as a width law:
    /// `budget = per_worker × width` (spill-design.md §2). The E18b
    /// shares (per pass-1 worker, per pass-2 partition owner) divide this
    /// back by width at the consumer.
    #[inline]
    pub fn grouped_budget_bytes(&self, width: usize) -> u64 {
        self.grouped_budget_per_worker_bytes
            .saturating_mul(width.max(1) as u64)
    }

    /// E18-M — the MACHINE floor of the grouped budget (the wave-3
    /// submission unrefusal, fp lineage 4e7396f9/45d921c8/24e4ef2c/
    /// 6ca60acb/3f343893/bac34e64): the width law prices the SAME
    /// statement differently on boxes whose vCPU:memory ratios differ
    /// (c6a.4xlarge 16t/32GiB refused the six 100m grouped shapes at a
    /// 1 GiB budget that the c8g.16xlarge tax rig served RESIDENT at
    /// 4 GiB — the scatter plane's true size, rows x SCATTER_ROW_BYTES,
    /// is machine-independent). The budget's real law is "unbounded
    /// materialization stays a fraction of THIS machine's memory" (the
    /// direct-array authority, re-cut to the box): the effective budget
    /// is the width law OR one eighth of physical RAM, whichever is
    /// larger. The floor is a monotone widening: it admits resident
    /// service (or keeps a spill-armed shape resident) where the width
    /// law refused or spilled — answers are arm-independent (the spill
    /// identity gates) and state stays under the floor by the same
    /// accounting. Kill switch PGRUST_SQE_GROUPED_MEM_FLOOR=0 restores
    /// the width law verbatim.
    #[inline]
    pub fn grouped_machine_floor_bytes(&self, machine_mem_bytes: u64) -> u64 {
        machine_mem_bytes / 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [ruling 3, BINDING] The safety gate: at the 96-thread rig
    /// geometry the formulas reproduce the election constants of record
    /// EXACTLY. E2's record is the width-ladder cell (RULED adopted by
    /// Michael 2026-08-18; docs/design/sqe/width-ladder-cell.md §2.2,
    /// c8g run 20260818T163253Z — the measurement of record): measured
    /// warm engagement 133,000 + 29,400·w ns replaces the width-96
    /// solved-for cold line (now the COLD_* constants). A red here is a
    /// wrong derivation or an unruled constant move, full stop.
    #[test]
    fn width96_reproduces_historical_constants() {
        let cp = target();
        // E2 (20260818 cell): setup(96) = 133,000 + 29,400·96 ns.
        assert_eq!(cp.claim_setup_ns(96), 2_955_400);
        // ... giving the measured-warm serial cutoff at width 96
        // (was the historical 21,000,000 under the cold line).
        assert_eq!(cp.serial_cutoff_rows(96), 4_181_113);
        // The retired defaults remain the documented COLD price.
        assert_eq!(COLD_CLAIM_SETUP_BASE_NS, 1_142_512);
        assert_eq!(COLD_CLAIM_SETUP_PER_WORKER_NS, 143_318);
        // E7: the partition clamp [256, 8192].
        assert_eq!(cp.partition_floor(96), 256);
        assert_eq!(cp.scatter_partition_cap, 8192);
        // E16: survivor_gather's `.min(2048)`.
        assert_eq!(cp.gather_partition_cap(96), 2048);
        // E8/F1 (the F7 resolution): dense-domain single authority at
        // agg_tier's historical 256K — family.rs's 1<<20 is retired.
        assert_eq!(cp.dense_domain_cap(), 256 * 1024);
        // E1: the per-thread rate scalar itself is unchanged.
        assert_eq!(cp.fold_rows_per_thread_ms, 1_400_000);
        // E17: the unbounded direct-array budget is the historical
        // DIRECT_ARRAY_BYTES_CAP; E17b is this lane's 8x bounded cut
        // (re-cut per target like every other knob).
        assert_eq!(cp.direct_array_cap(false), 1 << 28);
        assert_eq!(cp.direct_array_cap(true), 1 << 31);
        // E17 answer face ([cap-retire]): the finalize answer-bytes law
        // reads the SAME unbounded-materialization constant (one
        // authority — extend, don't duplicate).
        assert_eq!(cp.answer_budget_bytes(), 1 << 28);
        // E18: the grouped spill budget law at rig width (64 MiB × 96).
        assert_eq!(cp.grouped_budget_bytes(96), 6 * 1024 * 1024 * 1024);
    }

    /// Off-design widths move in the physical direction (narrower pool →
    /// smaller cutoff/floor; wider pool → larger), monotonically.
    #[test]
    fn derived_laws_scale_with_width() {
        let cp = target();
        // Serial cutoff shrinks toward rate·setup(w) at laptop widths —
        // the laptop stops holding 21M-row shapes serial (law doc §2.2).
        assert!(cp.serial_cutoff_rows(8) < cp.serial_cutoff_rows(96));
        assert!(cp.serial_cutoff_rows(96) < cp.serial_cutoff_rows(192));
        assert_eq!(cp.serial_cutoff_rows(1), u64::MAX); // no parallel body
        // Partition floor tracks the pool, never exceeds the ceiling.
        assert_eq!(cp.partition_floor(8), 16);
        assert_eq!(cp.partition_floor(192), 512);
        assert!(cp.partition_floor(1 << 20) <= cp.scatter_partition_cap);
        // Gather cap likewise.
        assert_eq!(cp.gather_partition_cap(8), 128);
        assert!(cp.gather_partition_cap(1 << 20) <= cp.scatter_partition_cap);
    }
}
