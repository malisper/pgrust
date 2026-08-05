//! Admission ledger v2 (single-executor migration Phase 0.1, WS-B): the ONE
//! width authority for the runtime. Slot-indexed 1:1 with the scheduler's
//! slot array; per-entry granted/target width words arbitrate how many
//! workers may serve each admitted resource group.
//!
//! Split-altitude discipline (copied from the Membership mutex, sched.rs):
//! `inner` is touched only at MEMBERSHIP EVENTS (admit / retire / target
//! recompute); the per-slot [`EntryWords`] are cache-padded atomics read
//! Relaxed on the hot paths (per pick candidate, per join/leave, per claim
//! boundary). Everything here is ADVISORY width policy: execution safety —
//! the slot word, the pin board, the finalization counter, the permit
//! semaphore — is untouched and owns correctness exactly as before, so
//! every ledger word is Relaxed and a stale read resolves through the
//! ordinary revalidation paths (Retry / the generation gate).
//!
//! LOCK ORDER: membership → ledger.inner, never inverted. `admit` and
//! `retire` run under the scheduler's membership lock (start_rg_locked /
//! release_slot_locked) and take `inner`; the hot-path entries
//! (`try_join` / `leave` / `should_continue` / `renudge` / `wants_workers`)
//! NEVER take `inner` — a worker holding a claim can never deadlock against
//! a submitting leader. The GANG face (`admit_gang` / `settle_gang` /
//! `retire_gang`, PHASE3-CLOSE WS-WIDTH — the audited successor of WS-O
//! wave 2's external face) takes `inner` ALONE from executor leader
//! threads that never hold the membership lock — taking the innermost
//! lock only always respects the order; a gang-face caller must NEVER
//! hold the membership lock (there is no code path that does, and none
//! may be added).
//!
//! # Gang policy lineage (WS-O wave 2 adjudications, all ratified)
//!
//! The gang face carries the WS-O external-face policy VERBATIM (the W4
//! removal deleted the external slab, not the policy): HEADROOM-ONLY
//! grants (never displace admitted pool width; grant MAY BE 0 — the
//! caller MUST have a serial path, Gather's leader-local scan); FROZEN
//! grants (no claim boundaries to shed at; the fairness envelope of
//! freezing stays a fleet measurement before any default-ON); RECOMPUTE
//! PARTICIPATION (active width charged before fair shares; liveness floor
//! target ≥ 1 wins); the COMPOSITION RULE (granted counts ACTIVE width —
//! `settle_gang` records launched/live and only THAT is charged; a
//! consumer must never ALSO clamp itself from the same numbers — ledger
//! clamps width, arm-side DOPCAP clamps footprint); LEADER NOT COUNTED
//! (C parity — parallel_leader_participation rides free); CAPACITY
//! FAIL-OPEN (at most [`MAX_GANG_ENTRIES`] concurrent gangs; past it
//! `admit_gang` refuses (None) and the caller keeps today's uncapped
//! launch path — asserted in tests, never a production panic).
//!
//! Events:
//! - ARRIVAL NUDGE — [`AdmissionLedger::admit`] registers the entry and
//!   recomputes every target (incumbents over their new target shed at
//!   their next claim boundary via [`AdmissionLedger::should_continue`]);
//!   the returned [`ArrivalNudge`] carries the wake hint and the
//!   advertises flag (sub-JOIN_THRESHOLD entries never set an active bit
//!   and never wake the pool — the caller executes alone).
//! - WORKER-FREED RE-PICK — [`AdmissionLedger::leave`] /
//!   [`AdmissionLedger::retire`] return wake hints; freed capacity flows to
//!   under-target entries through the pick filter
//!   ([`AdmissionLedger::wants_workers`]) — the leaving worker re-picks on
//!   its own, the hint only covers PARKED workers when a slot transitions
//!   back to joinable.
//! - BOUNDED RE-NUDGE — an under-target entry at a claim boundary may
//!   request one wake ([`AdmissionLedger::renudge`]), budgeted by
//!   `renudge_left` (refilled at every recompute) so a stuck entry cannot
//!   wake-storm.
//!
//! # Unified gang entries (PHASE3-CLOSE WS-WIDTH — the ONE width authority)
//!
//! `PGRUST_RUNTIME_WIDTH_UNIFIED` retargets non-pool gangs onto the POOL
//! face's grant algebra: a gang admission is an ordinary row of the ONE
//! entry table (`LedgerEntry::Gang`, allocated past the slot region),
//! charged BY the one recompute (`recompute_locked` walks the one table:
//! gang ACTIVE width is a fixed off-the-top charge, then pool fair shares
//! split the remainder), read by the one snapshot, bounded by the one gang
//! capacity ([`MAX_GANG_ENTRIES`], the successor of the external cap).
//! Gang entries are **NON-SHEDDING (frozen)**: bgworkers have no claim
//! boundaries, so the grant is a fixed charge for the entry's lifetime.
//! The WS-O policy invariants above (headroom-only, grant-may-be-0,
//! frozen, leader-not-counted, anti-double-clamp, advisory-only,
//! per-startup cadence, capacity fail-open) carry VERBATIM to gang
//! entries; `admit_gang` takes `inner` ALONE (lock-order law above). The
//! WS-O external slab this face superseded was removed by the W4 audited
//! removal (this commit); the policy lineage section above preserves the
//! adjudications.
//!
//! Fair-share remainder: targets split the core budget equally over
//! admitted entries; the remainder is assigned in slot order here, and
//! WHICH entry actually receives spare width is resolved by the
//! pass-ordered stride pick composed with `wants_workers` — the ratified
//! "pass/stride stays on Slot, the ledger consumes it via the pick filter"
//! shape (integration contract 1c). The ledger never duplicates pass
//! accounting.
//!
//! Composition rule (integration contract 1c, recorded for the arm
//! plumbing increments): the LEDGER clamps WIDTH from cache headroom
//! (Σ target_i × bytes_i ≤ cache budget); arm-side DOPCAP clamps FOOTPRINT
//! at the granted width — never both from the same numbers, or the
//! combined effect over-narrows. Inc-1 ships `cache_bytes = u64::MAX`
//! (unbounded) and no arm supplies footprints: the mechanism is tested but
//! inert.
//!
//! Liveness floor: an admitted entry's target is ≥ 1 while it remains
//! admitted (admission ⇔ unfinalized work: retire runs when the RG leaves
//! its slot). The floor wins over the cache clamp by design — it may
//! transiently overshoot the cache budget by (entries − 1) × bytes in the
//! worst case, which is the price of the no-wedge guarantee the loom
//! liveness model asserts.
//!
//! Design/spec home: this module doc + notes/se-ws-b-ledger.md (per the
//! integration contract's R-V2DOC ruling, docs/design/morsel-runtime-v2.md
//! is assembled at integrate, not created by any workstream branch).

use crate::stats::RuntimeStats;
use crate::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use crate::sync::{lock, Mutex};
use crate::taskset::CachePadded;

/// Static budgets the ledger arbitrates (fixed at construction; the JOIN
/// threshold is additionally atomic-backed for the per-instance test hook,
/// the `set_decay_quantum_ns` precedent).
#[derive(Clone, Copy, Debug)]
pub struct LedgerBudgets {
    /// Core budget: max concurrent granted workers across all entries.
    /// Production: the pool's execution-permit count (config.workers).
    pub cores: u32,
    /// Utility permit budget B_util (Track-4 Q1, pool-qos-design.md §1.3):
    /// while ANY standard (foreground/maintenance) entry is admitted, the
    /// utility tier's targets split min(util_cores, cores − Σ standard
    /// targets) — SOFT cap, ratified: with no standard entry admitted,
    /// utility splits the full budget (work-conserving; the reclaim bound
    /// is the claim-boundary Yield, not the cap). The per-entry liveness
    /// floor (target ≥ 1) survives a zeroed utility budget — the no-wedge
    /// guarantee is class-blind.
    pub util_cores: u32,
    /// Shared-cache budget, bytes (DOPCAP-class width clamp).
    /// u64::MAX = unbounded — the inc-1 default until arms supply footprints.
    pub cache_bytes: u64,
    /// JOIN_THRESHOLD: entries with est_work_ns below this never advertise
    /// (no active bit, no wakes). 0 = every entry advertises.
    /// Default from PGRUST_RUNTIME_LEDGER_JOIN_US (placeholder 0 until the
    /// calibration lane measures real join cost).
    pub join_threshold_ns: u64,
    /// Re-nudge budget per target recompute (bounded event-driven widening).
    /// Default 4.
    pub renudge_max: u32,
}

impl LedgerBudgets {
    /// Production budgets: core budget = the execution-permit count; cache
    /// unbounded (inc-1: no arm supplies footprints); JOIN threshold from
    /// the env (default 0 = inert — an empirical default is the calibration
    /// fleet lane's, not a guess shipped as policy); renudge budget 4.
    pub fn from_env(cores: u32) -> LedgerBudgets {
        LedgerBudgets {
            cores: cores.max(1),
            util_cores: util_cores_default(cores.max(1)),
            cache_bytes: u64::MAX,
            join_threshold_ns: join_threshold_default_ns(),
            renudge_max: 4,
        }
    }
}

/// `PGRUST_RUNTIME_UTIL_PERMITS`: the utility permit budget B_util
/// (calibration knob on the PGRUST_RUNTIME_STRIDE precedent, not product
/// surface). Default max(1, cores/8). Read once.
fn util_cores_default(cores: u32) -> u32 {
    static V: std::sync::OnceLock<Option<u32>> = std::sync::OnceLock::new();
    V.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_UTIL_PERMITS")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|p| *p > 0)
    })
    .unwrap_or_else(|| (cores / 8).max(1))
    .min(cores)
}

/// `PGRUST_RUNTIME_LEDGER_JOIN_US` (µs → ns), default 0: no entry is ever
/// sub-threshold, the JOIN mechanism is inert. Read once.
fn join_threshold_default_ns() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_LEDGER_JOIN_US")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(|us| us.saturating_mul(1000))
            .unwrap_or(0)
    })
}

/// Desirable-width input, per query: min(ceiling, predicted optimum, cache)
/// per the migration doc §0.1. Deliberately ns-denominated / AM-agnostic
/// (integration contract 1c): arms derive `ceiling` from arm_dop and
/// granule geometry; the ledger never sees granule counts.
#[derive(Clone, Copy, Debug)]
pub struct WidthRequest {
    /// Hard ceiling (granule count / arm dop / GUC); >= 1.
    pub ceiling: u32,
    /// dopmap/α-class predicted optimum; u32::MAX = unknown.
    pub predicted: u32,
    /// Per-worker cache footprint estimate, bytes; 0 = negligible.
    pub cache_bytes_per_worker: u64,
    /// Estimated total work, ns; u64::MAX = unknown (always advertises).
    pub est_work_ns: u64,
}

impl WidthRequest {
    /// The no-information request (inc-1 default for existing submit paths).
    pub fn unbounded(ceiling: u32) -> WidthRequest {
        WidthRequest {
            ceiling: ceiling.max(1),
            predicted: u32::MAX,
            cache_bytes_per_worker: 0,
            est_work_ns: u64::MAX,
        }
    }
}

/// Budget tier of a pool entry (Track-4 Q1). Standard = foreground +
/// maintenance (the full-budget fair split, today's algebra exactly);
/// Utility = the capped tier (soft B_util cap, pool-qos-design.md §1.3).
/// Mapped from [`crate::rg::RgClass`] at the ONE admit call site
/// (sched.rs start_rg_locked); the hot words (`target`/`granted`) stay
/// class-blind — the class is fully compiled into the recompute's targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LedgerClass {
    Standard,
    Utility,
}

/// What the submitter must do after admit (the ledger never touches the
/// park lot — the Scheduler owns wakes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArrivalNudge {
    /// Parked workers the arrival should wake (0 = sub-threshold / no headroom).
    pub wake: u32,
    /// False = sub-JOIN_THRESHOLD: never set the active bit, never wake.
    pub advertises: bool,
}

/// Claim-boundary verdict for a worker serving `slot`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimVerdict {
    Continue,
    /// Target dropped below granted (arrival narrowed us / cache clamp):
    /// end the task via the existing TaskEnd::Budget path and re-pick.
    Yield,
}

/// Instrument readback (snapshot(); also the deterministic tests' oracle).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LedgerSnapshot {
    pub admitted: u32,
    pub granted_total: u32,
    pub target_total: u32,
    /// Utility-tier pool entries currently admitted (Track-4 Q1).
    pub util_admitted: u32,
    /// Σ targets over admitted utility-tier entries (⊆ target_total).
    pub util_target_total: u32,
    pub cache_charged_bytes: u64,
    pub yields: u64,
    pub renudges: u64,
    pub renudges_suppressed: u64,
    pub sub_threshold_admits: u64,
    /// Unified gang entries currently admitted (WS-WIDTH pool face).
    pub gang_admitted: u32,
    /// Σ frozen grant ceilings over admitted gang entries.
    pub gang_granted: u32,
    /// Σ ACTIVE gang width (the core-budget charge inside the ONE recompute).
    pub gang_active: u32,
    /// Gang admissions refused at the [`MAX_GANG_ENTRIES`] capacity
    /// (FAIL-OPEN — stock launch; minted as the DISTINGUISHABLE successor
    /// of the retired WS-O external-face refusal counter so dashboards
    /// never silently aliased the two mechanisms' counters).
    pub gang_cap_refusals: u64,
    /// Gang admissions granted WIDTH 0 (saturated box — the caller went
    /// serial). FM-3 visibility: sustained pool saturation serializing
    /// every Gather relaunch is ACCEPTED migration posture but must be
    /// VISIBLE; a real-workload regression escalates to a BOARD decision
    /// (gang minimum-grant floor), never silent code.
    pub gang_zero_grants: u64,
}

/// Hot per-slot width words, one padded line each (read per claim / per
/// pick candidate; written at joins/leaves and recomputes). All Relaxed —
/// advisory policy, never execution safety (module doc).
struct EntryWords {
    /// Workers currently granted to this slot. NOT reset at admit/retire:
    /// it counts live joined workers on the SLOT, so straggler leaves
    /// across a slot-reuse boundary stay balanced with their joins.
    granted: AtomicU32,
    /// Current allowed width; 0 = not admitted. granted<=target is advisory
    /// (transient overshoot resolves via Yield at the next claim boundary).
    target: AtomicU32,
    /// Admission epoch, bumped at admit — try_join re-reads it around the
    /// granted CAS to bound stale joins across an admission boundary. The
    /// UNMANAGED marker is `target == 0` (no admitted entry), NOT this
    /// word: a retired slot keeps a nonzero epoch, and a DAG fan-out may
    /// publish into it without a fresh admission — it must fail open.
    epoch: AtomicU32,
    /// Remaining bounded re-nudges this recompute window.
    renudge_left: AtomicU32,
    /// Advertises flag (1 = pool-visible). Layout addition to the contract's
    /// four words — flagged in notes/se-ws-b-ledger.md.
    advert: AtomicU32,
}

/// Unified gang-region capacity (PHASE3-CLOSE WS-WIDTH, invariant 8): at
/// most this many concurrently admitted gang entries past the slot region;
/// past it `admit_gang` FAILS OPEN (None → stock launch, never block,
/// never error; counted in `gang_cap_refusals`). The ONE non-pool bound
/// (successor of the retired WS-O 64-entry external cap).
pub(crate) const MAX_GANG_ENTRIES: usize = 64;

/// One unified gang entry (module doc "Unified gang entries"): FROZEN
/// non-shedding — `granted` never changes after admit (invariant 2);
/// `active` is the launched/live worker count within it (invariant 5's
/// composition rule: parked workers hold no grant). Lives in the ONE entry
/// table's gang region — no EntryWords, never on a hot path.
#[derive(Clone, Copy, Debug)]
struct GangEntry {
    granted: u32,
    active: u32,
}

/// A row of the ONE entry table (§1.4 of notes/se-p3close-width.md):
/// indices `[0, nslots)` hold Pool rows (slot-indexed 1:1, hot-path words
/// in `EntryWords[slot]`); indices `[nslots, nslots + MAX_GANG_ENTRIES)`
/// hold Gang rows (membership-event cadence only).
#[derive(Clone, Copy, Debug)]
enum LedgerEntry {
    Pool(WidthRequest, LedgerClass),
    Gang(GangEntry),
}

/// Membership-event state (admit/retire/recompute only — never on the
/// per-claim path; the Membership-mutex discipline, sched.rs).
struct LedgerInner {
    /// The ONE entry table: pool region `[0, nslots)` (admitted slots,
    /// advertising or not, with their requests) + gang region past it
    /// (grows on demand up to nslots + [`MAX_GANG_ENTRIES`]).
    table: Vec<Option<LedgerEntry>>,
    /// Admitted POOL entries (the fair-share divisor; gang entries are
    /// fixed charges, never shares).
    admitted: u32,
    /// Σ target_i × bytes_i over admitted entries.
    cache_charged: u64,
}

impl LedgerInner {
    /// Σ active unified-gang width — the core-budget charge the ONE
    /// recompute takes off the top (invariant 3).
    fn gang_active(&self) -> u64 {
        self.table
            .iter()
            .flatten()
            .map(|e| match e {
                LedgerEntry::Gang(g) => u64::from(g.active),
                LedgerEntry::Pool(..) => 0,
            })
            .sum()
    }

    /// Σ pool targets (entitlements — using grants instead would let a
    /// gang seize width an admitted query is about to take back), read
    /// from the hot words the recompute last published.
    fn pool_targets(&self, entries: &[CachePadded<EntryWords>]) -> u64 {
        self.table
            .iter()
            .enumerate()
            .filter(|(_, e)| matches!(e, Some(LedgerEntry::Pool(..))))
            .map(|(slot, _)| u64::from(entries[slot].target.load(Ordering::Relaxed)))
            .sum()
    }
}

/// Relaxed observability counters feeding [`LedgerSnapshot`].
#[derive(Default)]
struct LedgerStats {
    yields: AtomicU64,
    renudges: AtomicU64,
    renudges_suppressed: AtomicU64,
    sub_threshold_admits: AtomicU64,
    /// Gang admissions refused at [`MAX_GANG_ENTRIES`] (fail-open;
    /// invariant 8 — the DISTINGUISHABLE successor counter).
    gang_cap_refusals: AtomicU64,
    /// Gang admissions granted width 0 (FM-3 visibility).
    gang_zero_grants: AtomicU64,
}

pub struct AdmissionLedger {
    entries: Box<[CachePadded<EntryWords>]>,
    inner: Mutex<LedgerInner>,
    budgets: LedgerBudgets,
    /// budgets.join_threshold_ns, atomic-backed so deterministic tests can
    /// tighten it per instance (the set_decay_quantum_ns precedent).
    join_threshold_ns: AtomicU64,
    /// budgets.util_cores (B_util), atomic-backed for the same per-instance
    /// test-hook reason. Read only inside the recompute (membership
    /// cadence) — never on a hot path.
    util_cores: AtomicU32,
    stats: LedgerStats,
}

impl AdmissionLedger {
    pub fn new(nslots: usize, budgets: LedgerBudgets) -> AdmissionLedger {
        assert!(budgets.cores >= 1);
        AdmissionLedger {
            entries: (0..nslots)
                .map(|_| {
                    CachePadded(EntryWords {
                        granted: AtomicU32::new(0),
                        target: AtomicU32::new(0),
                        epoch: AtomicU32::new(0),
                        renudge_left: AtomicU32::new(0),
                        advert: AtomicU32::new(0),
                    })
                })
                .collect(),
            inner: Mutex::new(LedgerInner {
                table: (0..nslots).map(|_| None).collect(),
                admitted: 0,
                cache_charged: 0,
            }),
            join_threshold_ns: AtomicU64::new(budgets.join_threshold_ns),
            util_cores: AtomicU32::new(budgets.util_cores.min(budgets.cores)),
            budgets,
            stats: LedgerStats::default(),
        }
    }

    pub fn budgets(&self) -> LedgerBudgets {
        LedgerBudgets {
            join_threshold_ns: self.join_threshold_ns.load(Ordering::Relaxed),
            util_cores: self.util_cores.load(Ordering::Relaxed),
            ..self.budgets
        }
    }

    /// Test hook (per-instance JOIN threshold; see the field doc).
    pub(crate) fn set_join_threshold_ns(&self, ns: u64) {
        self.join_threshold_ns.store(ns, Ordering::SeqCst);
    }

    /// Test hook (per-instance utility budget B_util; see the field doc).
    /// Production keeps PGRUST_RUNTIME_UTIL_PERMITS (default cores/8,
    /// floor 1). Takes effect at the next recompute (membership event).
    pub(crate) fn set_util_cores(&self, n: u32) {
        self.util_cores.store(n.clamp(1, self.budgets.cores), Ordering::SeqCst);
    }

    /// ARRIVAL: register + recompute targets (incumbents over their new
    /// target shed at their next claim boundary). Called from
    /// start_rg_locked under the membership lock (lock order:
    /// membership → ledger.inner; never inverted). `class` selects the
    /// budget tier (Track-4 Q1) — Standard is today's algebra exactly.
    pub(crate) fn admit(
        &self,
        slot: usize,
        req: WidthRequest,
        class: LedgerClass,
    ) -> ArrivalNudge {
        let mut inner = lock(&self.inner);
        debug_assert!(slot < self.entries.len(), "pool admit into the gang region");
        debug_assert!(inner.table[slot].is_none(), "slot admitted twice without retire");
        inner.table[slot] = Some(LedgerEntry::Pool(req, class));
        inner.admitted += 1;
        let advertises =
            req.est_work_ns >= self.join_threshold_ns.load(Ordering::Relaxed);
        let e = &self.entries[slot];
        // Epoch first: a join racing this admission resolves against the new
        // epoch (try_join's CAS-undo), never against the retired occupant's.
        e.epoch.fetch_add(1, Ordering::Relaxed);
        e.advert.store(advertises as u32, Ordering::Relaxed);
        self.recompute_locked(&mut inner);
        let wake = if advertises {
            self.entries[slot].target.load(Ordering::Relaxed).min(self.budgets.cores)
        } else {
            RuntimeStats::tick(&self.stats.sub_threshold_admits);
            0
        };
        ArrivalNudge { wake, advertises }
    }

    /// COMPLETION/ABORT: drop the entry, recompute, return a wake hint (the
    /// number of surviving entries whose target ROSE — worker-freed re-pick
    /// propagation). Called from the slot-release path under the membership
    /// lock; a never-admitted slot (queued-abort completion, unmanaged
    /// occupant) is a no-op.
    pub(crate) fn retire(&self, slot: usize) -> u32 {
        let mut inner = lock(&self.inner);
        debug_assert!(slot < self.entries.len(), "pool retire into the gang region");
        if inner.table[slot].take().is_none() {
            return 0;
        }
        inner.admitted -= 1;
        let e = &self.entries[slot];
        e.target.store(0, Ordering::Relaxed);
        e.advert.store(0, Ordering::Relaxed);
        e.renudge_left.store(0, Ordering::Relaxed);
        // `granted` deliberately NOT reset: it counts live workers on the
        // slot; straggler leaves stay balanced across reuse (field doc).
        self.recompute_locked(&mut inner)
    }

    /// A worker joins slot's task set (evaluated before run_task). False =
    /// the grant would exceed target or the epoch moved — re-pick. CAS on
    /// granted only; no lock. Unmanaged slots (target 0 = no admitted
    /// entry: DAG fan-out siblings, retired-then-reused slots, knob
    /// toggles) fail OPEN but still count the worker, so a later
    /// admission's words start coherent with reality.
    pub(crate) fn try_join(&self, slot: usize) -> bool {
        let e = &self.entries[slot];
        let epoch = e.epoch.load(Ordering::Relaxed);
        let mut g = e.granted.load(Ordering::Relaxed);
        loop {
            let t = e.target.load(Ordering::Relaxed);
            if t != 0 && g >= t {
                return false;
            }
            match e.granted.compare_exchange_weak(
                g,
                g + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if e.epoch.load(Ordering::Relaxed) != epoch {
                        // The slot rolled to a new admission between the
                        // target read and the grant: undo and re-pick.
                        e.granted.fetch_sub(1, Ordering::Relaxed);
                        return false;
                    }
                    return true;
                }
                Err(cur) => g = cur,
            }
        }
    }

    /// A worker leaves (task end; balanced 1:1 with a successful try_join
    /// by the caller). Returns a wake hint: 1 ⇔ the slot just transitioned
    /// from not-joinable (granted ≥ target) back to joinable — parked
    /// workers may have skipped it at their last pick. Everything else
    /// rides the leaving worker's own re-pick or the bounded re-nudge.
    pub(crate) fn leave(&self, slot: usize) -> u32 {
        let e = &self.entries[slot];
        let mut cur = e.granted.load(Ordering::Relaxed);
        let before = loop {
            if cur == 0 {
                // Unbalanced leave (only reachable through a knob flip
                // mid-drive, which tests don't do): saturate, never wrap.
                debug_assert!(false, "ledger leave without a matching join");
                break 0;
            }
            match e.granted.compare_exchange_weak(
                cur,
                cur - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(prev) => break prev,
                Err(c) => cur = c,
            }
        };
        let t = e.target.load(Ordering::Relaxed);
        u32::from(t > 0 && before == t && e.advert.load(Ordering::Relaxed) != 0)
    }

    /// DECLARED-BLOCKING-SECTION donation (§2.8 composition; caught by the
    /// knob-ON suite wedging io_permit_seams_donate_core_to_standby): a
    /// worker entering a blocking section keeps its task and pin-board
    /// obligations but returns its WIDTH GRANT along with the execution
    /// permit — the standby absorbing the freed core must be joinable, or
    /// a width-saturated slot deadlocks the donation model (the blocked
    /// worker waits on work only the refused standby can run). Accounting
    /// IS [`AdmissionLedger::leave`] (granted −1 + the joinable-transition
    /// wake hint, which covers standbys parked Idle after the pick filter
    /// refused the then-saturated slot).
    pub(crate) fn donate(&self, slot: usize) -> u32 {
        self.leave(slot)
    }

    /// Blocking-section exit: retake the grant UNCONDITIONALLY (the permit
    /// is already reacquired; the worker resumes mid-task and cannot
    /// re-pick, so a refusal here would have no legal answer). May
    /// transiently overshoot target — resolved by Yield at the next claim
    /// boundary, the standard over-shed doctrine. No epoch re-check: the
    /// entry cannot retire while this worker's task is unsettled
    /// (finalization waits on its pin/marker), so the admission this grant
    /// belongs to is still the slot's occupant.
    pub(crate) fn rejoin(&self, slot: usize) {
        self.entries[slot].granted.fetch_add(1, Ordering::Relaxed);
    }

    /// UNIFIED GANG ADMIT (PHASE3-CLOSE WS-WIDTH; module doc "Unified gang
    /// entries"): register a non-pool parallel gang as a FROZEN
    /// NON-SHEDDING row of the ONE entry table and grant it HEADROOM-ONLY
    /// width — `min(requested, cores − Σ pool targets − Σ gang active)`
    /// under `inner` (taken ALONE; the caller never holds the membership
    /// lock — the lock-order law). Invariants 1/2: the grant never displaces admitted pool
    /// width, never changes after admit, and MAY BE 0 (counted in
    /// `gang_zero_grants`; the caller MUST have a serial path — Gather's
    /// leader-local scan). Pool targets are recomputed by the ONE
    /// recompute so the gang's active width is charged immediately
    /// (incumbents shed at their next claim boundary; the liveness floor
    /// target ≥ 1 still wins — invariant 3). None ⇔ [`MAX_GANG_ENTRIES`]
    /// — FAIL-OPEN, the caller keeps today's uncapped path (invariant 8:
    /// never block, never error).
    pub(crate) fn admit_gang(&self, requested: u32) -> Option<(usize, u32)> {
        let nslots = self.entries.len();
        let mut inner = lock(&self.inner);
        let id = match (nslots..inner.table.len()).find(|&i| inner.table[i].is_none()) {
            Some(free) => free,
            None if inner.table.len() < nslots + MAX_GANG_ENTRIES => {
                inner.table.push(None);
                inner.table.len() - 1
            }
            None => {
                RuntimeStats::tick(&self.stats.gang_cap_refusals);
                return None;
            }
        };
        let headroom = u64::from(self.budgets.cores)
            .saturating_sub(inner.pool_targets(&self.entries))
            .saturating_sub(inner.gang_active());
        let granted = requested.min(headroom.min(u64::from(u32::MAX)) as u32);
        if granted == 0 {
            RuntimeStats::tick(&self.stats.gang_zero_grants);
        }
        inner.table[id] = Some(LedgerEntry::Gang(GangEntry { granted, active: granted }));
        // Charge the grant against pool targets NOW through the ONE
        // recompute (narrowing only — the widened count is always 0 here).
        self.recompute_locked(&mut inner);
        Some((id, granted))
    }

    /// UNIFIED GANG SETTLE: record the gang's ACTIVE width (launched/live
    /// worker count — parked workers hold no grant; invariant 5's
    /// composition rule). Clamped to the frozen grant; callable repeatedly
    /// (launch, per-rescan relaunch, partial exit — invariant 7). Returns
    /// the number of pool entries whose target ROSE — the caller's wake
    /// hint.
    pub(crate) fn settle_gang(&self, id: usize, active: u32) -> u32 {
        let mut inner = lock(&self.inner);
        let Some(Some(LedgerEntry::Gang(g))) = inner.table.get_mut(id) else {
            debug_assert!(false, "settle_gang on a retired/unknown/non-gang entry");
            return 0;
        };
        debug_assert!(
            active <= g.granted,
            "gang settle above the frozen grant ({active} > {})",
            g.granted
        );
        g.active = active.min(g.granted);
        self.recompute_locked(&mut inner)
    }

    /// UNIFIED GANG RETIRE: drop the entry and return its width to the
    /// pool (the ONE recompute; the return value is the widened-targets
    /// wake hint). Exactly once per admit — the RAII lease owns the id.
    pub(crate) fn retire_gang(&self, id: usize) -> u32 {
        let nslots = self.entries.len();
        let mut inner = lock(&self.inner);
        debug_assert!(id >= nslots, "retire_gang on a pool slot");
        let Some(row) = inner.table.get_mut(id) else {
            debug_assert!(false, "retire_gang on an unknown id");
            return 0;
        };
        match row.take() {
            Some(LedgerEntry::Gang(_)) => self.recompute_locked(&mut inner),
            Some(other) => {
                *row = Some(other);
                debug_assert!(false, "retire_gang on a pool entry");
                0
            }
            None => {
                debug_assert!(false, "retire_gang on a retired entry");
                0
            }
        }
    }

    /// CLAIM-BOUNDARY decision: one Relaxed load pair (granted vs target)
    /// on the common path. Called from run_task's claim loop next to the
    /// existing is_aborted boundary check. Yield maps onto the existing
    /// TaskEnd::Budget path — the finalization protocol never sees the
    /// ledger. target == 0 (unmanaged or retired) fails open: the slot
    /// word / generation gate own those endings.
    pub(crate) fn should_continue(&self, slot: usize) -> ClaimVerdict {
        let e = &self.entries[slot];
        let t = e.target.load(Ordering::Relaxed);
        if t != 0 && e.granted.load(Ordering::Relaxed) > t {
            RuntimeStats::tick(&self.stats.yields);
            ClaimVerdict::Yield
        } else {
            ClaimVerdict::Continue
        }
    }

    /// BOUNDED RE-NUDGE from a claim boundary: true = caller should wake
    /// (inc-1: park.wake_all — targeted wakes via the WorkerMailbox masks
    /// are WS-B inc-2). Fires only while the entry is under target;
    /// decrements renudge_left, refilled at recompute — a stuck
    /// under-target entry cannot wake-storm.
    pub(crate) fn renudge(&self, slot: usize) -> bool {
        let e = &self.entries[slot];
        let t = e.target.load(Ordering::Relaxed);
        if t == 0 || e.granted.load(Ordering::Relaxed) >= t {
            return false;
        }
        let mut left = e.renudge_left.load(Ordering::Relaxed);
        loop {
            if left == 0 {
                RuntimeStats::tick(&self.stats.renudges_suppressed);
                return false;
            }
            match e.renudge_left.compare_exchange_weak(
                left,
                left - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    RuntimeStats::tick(&self.stats.renudges);
                    return true;
                }
                Err(c) => left = c,
            }
        }
    }

    /// Pick filter composed into pick_slot's stride scan: advertised and
    /// granted < target. Relaxed/advisory — a stale true resolves through
    /// try_join and the slot word into Retry, like every pick input.
    /// Unmanaged slots (target 0 = no admitted entry — including a RETIRED
    /// slot a DAG fan-out later publishes into without a fresh admission)
    /// fail open, or the reused slot would be filtered forever.
    pub(crate) fn wants_workers(&self, slot: usize) -> bool {
        let e = &self.entries[slot];
        let t = e.target.load(Ordering::Relaxed);
        if t == 0 {
            return true;
        }
        e.advert.load(Ordering::Relaxed) != 0 && e.granted.load(Ordering::Relaxed) < t
    }

    /// Whether publications into `slot` are pool-visible (set_active +
    /// publish wake). Unmanaged slots (target 0) fail open — DAG fan-out
    /// siblings, retired-then-reused slots, and knob-toggle windows behave
    /// exactly as before.
    pub(crate) fn advertises(&self, slot: usize) -> bool {
        let e = &self.entries[slot];
        e.target.load(Ordering::Relaxed) == 0 || e.advert.load(Ordering::Relaxed) != 0
    }

    pub fn snapshot(&self) -> LedgerSnapshot {
        let inner = lock(&self.inner);
        let mut granted_total = 0u32;
        let mut target_total = 0u32;
        let mut util_admitted = 0u32;
        let mut util_target_total = 0u32;
        let mut gang_admitted = 0u32;
        let mut gang_granted = 0u32;
        let mut gang_active = 0u32;
        // ONE walk of the ONE table — pool aggregates and gang aggregates
        // come from the same stats surface (§2.4 snapshot unification).
        for (idx, entry) in inner.table.iter().enumerate() {
            match entry {
                Some(LedgerEntry::Pool(_, class)) => {
                    let t = self.entries[idx].target.load(Ordering::Relaxed);
                    granted_total += self.entries[idx].granted.load(Ordering::Relaxed);
                    target_total += t;
                    if *class == LedgerClass::Utility {
                        util_admitted += 1;
                        util_target_total += t;
                    }
                }
                Some(LedgerEntry::Gang(g)) => {
                    gang_admitted += 1;
                    gang_granted += g.granted;
                    gang_active += g.active;
                }
                None => {}
            }
        }
        LedgerSnapshot {
            admitted: inner.admitted,
            granted_total,
            target_total,
            util_admitted,
            util_target_total,
            cache_charged_bytes: inner.cache_charged,
            yields: self.stats.yields.load(Ordering::Relaxed),
            renudges: self.stats.renudges.load(Ordering::Relaxed),
            renudges_suppressed: self.stats.renudges_suppressed.load(Ordering::Relaxed),
            sub_threshold_admits: self.stats.sub_threshold_admits.load(Ordering::Relaxed),
            gang_admitted,
            gang_granted,
            gang_active,
            gang_cap_refusals: self.stats.gang_cap_refusals.load(Ordering::Relaxed),
            gang_zero_grants: self.stats.gang_zero_grants.load(Ordering::Relaxed),
        }
    }

    /// Test oracle: (granted, target) of one slot.
    #[cfg(test)]
    pub(crate) fn debug_words(&self, slot: usize) -> (u32, u32) {
        (
            self.entries[slot].granted.load(Ordering::Relaxed),
            self.entries[slot].target.load(Ordering::Relaxed),
        )
    }

    /// Target recompute (membership-event cadence, under `inner`) — THE
    /// ONE grant algebra: one class-tiered walk of the one entry table.
    /// target_i = max(1, min(ceiling_i, predicted_i, fair_i, cache room)).
    /// Fair shares split the core budget — MINUS the non-shedding gang
    /// charges (recompute participation,
    /// module doc: the gangs' charge comes off the top; a zeroed budget
    /// still floors every pool target at 1, the no-wedge guarantee) —
    /// equally; the remainder lands in slot order (WHICH entry actually
    /// consumes spare width is the pass-ordered pick's decision — module
    /// doc). Gang rows are FROZEN: the walk never rewrites their grant
    /// (invariant 2) — they participate only as charges. The final max(1)
    /// is the liveness floor, which wins over the cache clamp by design.
    /// Refills every pool entry's re-nudge budget. Returns the number of
    /// entries whose target ROSE (the worker-freed wake hint).
    ///
    /// TWO TIERS (Track-4 Q1, pool-qos-design.md §1.3, SOFT cap ratified):
    /// Standard entries (foreground + maintenance) split the full budget
    /// among THEMSELVES — with no utility entries admitted this is the
    /// pre-Q1 walk value-identically (n_std == admitted). Utility entries
    /// then split `min(B_util, budget − Σ standard targets)` — or the FULL
    /// budget when no standard entry is admitted (work-conserving: an idle
    /// box runs utility wide; the arrival of a standard entry re-runs this
    /// recompute and over-target utility workers shed at their next claim
    /// boundary via should_continue's Yield — the ≤one-claim reclaim
    /// bound). Cache charging walks standard-first in slot order.
    fn recompute_locked(&self, inner: &mut LedgerInner) -> u32 {
        let n = inner.admitted;
        if n == 0 {
            inner.cache_charged = 0;
            return 0;
        }
        let budget = u64::from(self.budgets.cores)
            .saturating_sub(inner.gang_active())
            .min(u64::from(u32::MAX)) as u32;
        let n_util = inner
            .table
            .iter()
            .flatten()
            .filter(|e| matches!(e, LedgerEntry::Pool(_, LedgerClass::Utility)))
            .count() as u32;
        let n_std = n - n_util;
        let mut charged: u64 = 0;
        let mut widened = 0u32;
        let mut tier = |inner: &LedgerInner,
                        class: LedgerClass,
                        count: u32,
                        tier_budget: u32,
                        charged: &mut u64|
         -> (u32, u32) {
            // Returns (widened, Σ targets) over this tier's entries.
            if count == 0 {
                return (0, 0);
            }
            let base = tier_budget / count;
            let mut rem = tier_budget % count;
            let mut widened = 0u32;
            let mut total = 0u32;
            for (slot, entry) in inner.table.iter().enumerate() {
                let Some(LedgerEntry::Pool(req, c)) = entry else { continue };
                if *c != class {
                    continue;
                }
                let mut fair = base;
                if rem > 0 {
                    fair += 1;
                    rem -= 1;
                }
                let mut t = req
                    .ceiling
                    .max(1)
                    .min(req.predicted.max(1))
                    .min(fair.max(1));
                if req.cache_bytes_per_worker > 0 && self.budgets.cache_bytes != u64::MAX {
                    let room = self.budgets.cache_bytes.saturating_sub(*charged)
                        / req.cache_bytes_per_worker;
                    t = t.min(room.min(u64::from(u32::MAX)) as u32);
                }
                let t = t.max(1); // liveness floor: target >= 1 while admitted
                *charged = charged
                    .saturating_add(u64::from(t).saturating_mul(req.cache_bytes_per_worker));
                total += t;
                let e = &self.entries[slot];
                let old = e.target.swap(t, Ordering::Relaxed);
                if t > old {
                    widened += 1;
                }
                e.renudge_left.store(self.budgets.renudge_max, Ordering::Relaxed);
            }
            (widened, total)
        };
        let (w_std, std_total) =
            tier(inner, LedgerClass::Standard, n_std, budget, &mut charged);
        widened += w_std;
        if n_util > 0 {
            let util_budget = if n_std == 0 {
                budget // work-conserving idle box: utility runs wide
            } else {
                self.util_cores
                    .load(Ordering::Relaxed)
                    .min(budget.saturating_sub(std_total))
            };
            let (w_util, _) =
                tier(inner, LedgerClass::Utility, n_util, util_budget, &mut charged);
            widened += w_util;
        }
        inner.cache_charged = charged;
        widened
    }
}
