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
//! a submitting leader. The EXTERNAL face (`admit_external` /
//! `settle_external` / `retire_external`, WS-O wave 2) takes `inner` ALONE
//! from executor leader threads that never hold the membership lock —
//! taking the innermost lock only always respects the order; an
//! external-face caller must NEVER hold the membership lock (there is no
//! code path that does, and none may be added).
//!
//! # External width entries (WS-O wave 2 — the Gather/parallel-gang seam)
//!
//! Non-pool parallel gangs (Gather's bgworker helpers) charge width through
//! `inner`-only entries: no [`EntryWords`], no hot-path words — external
//! entries exist only at membership-event cadence. Policy (contract
//! adjudications, all ratified):
//!
//! - **HEADROOM-ONLY GRANTS**: an external admit is granted
//!   `min(requested, cores − Σ pool targets − Σ external active)` at the
//!   instant of admission — it never displaces admitted pool width. The
//!   grant MAY BE 0 (saturated box): the caller MUST have a serial path
//!   (Gather's leader-local scan is exactly that).
//! - **FROZEN GRANTS (inc-1)**: the grant ceiling never changes after
//!   admission — external gangs have no claim boundaries to shed at, so
//!   the ledger never narrows them (the fairness envelope of freezing is a
//!   fleet measurement before any default-ON).
//! - **RECOMPUTE PARTICIPATION**: external entries DO enter the target
//!   recompute — their ACTIVE width is charged against the core budget
//!   before fair shares split the remainder over pool entries (pool
//!   incumbents over their narrowed target shed at their next claim
//!   boundary, the ordinary over-shed doctrine; the liveness floor
//!   target ≥ 1 still wins).
//! - **COMPOSITION RULE (the WS-N/WS-O anti-double-clamp guard)**: granted
//!   counts ACTIVE width — `settle_external` records the launched/live
//!   worker count and only THAT is charged; parked standing-gang workers
//!   hold no grant. A consumer must never ALSO clamp itself from the same
//!   numbers (the ledger clamps width; arm-side DOPCAP clamps footprint at
//!   the granted width — the 1c rule restated for the external face).
//! - **LEADER NOT COUNTED (inc-1, C parity)**: requests are denominated in
//!   WORKERS; a participating leader rides free exactly as C's
//!   parallel_leader_participation. Reconciled in the C2 design note
//!   before C2 boards.
//! - **CAP, FAIL-OPEN**: at most [`MAX_EXTERNAL_ENTRIES`] (64) concurrent
//!   external entries; past the cap `admit_external` refuses (None) and
//!   the caller keeps today's uncapped launch path. Unit tests assert the
//!   refusal (the contract's "debug_assert in tests" — a production panic
//!   is deliberately NOT risked on a legal, if extreme, workload).
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
            cache_bytes: u64::MAX,
            join_threshold_ns: join_threshold_default_ns(),
            renudge_max: 4,
        }
    }
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
    pub cache_charged_bytes: u64,
    pub yields: u64,
    pub renudges: u64,
    pub renudges_suppressed: u64,
    pub sub_threshold_admits: u64,
    /// External width entries currently admitted (WS-O external face).
    pub external_admitted: u32,
    /// Σ frozen grant ceilings over admitted external entries.
    pub external_granted: u32,
    /// Σ ACTIVE external width (the core-budget charge).
    pub external_active: u32,
    /// External admissions refused at the 64-entry cap (fail-open).
    pub external_cap_refusals: u64,
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

/// External-face cap (contract adjudication): at most 64 concurrently
/// admitted external entries; past the cap admission FAILS OPEN (no lease —
/// the caller keeps today's uncapped launch path).
pub(crate) const MAX_EXTERNAL_ENTRIES: usize = 64;

/// One external width entry (module doc "External width entries"): a
/// non-pool parallel gang charging ACTIVE width against the core budget.
/// Lives under `inner` only — no EntryWords, never on a hot path.
#[derive(Clone, Copy, Debug)]
struct ExternalEntry {
    /// Frozen grant ceiling (headroom at admission; never changes — the
    /// caller may run at most this many workers).
    granted: u32,
    /// ACTIVE width currently charged (composition rule: parked workers
    /// hold no grant). Starts at `granted`; `settle_external` moves it
    /// within [0, granted].
    active: u32,
}

/// Membership-event state (admit/retire/recompute only — never on the
/// per-claim path; the Membership-mutex discipline, sched.rs).
struct LedgerInner {
    /// Admitted slots (advertising or not), with their requests.
    req: Vec<Option<WidthRequest>>, // len = nslots
    admitted: u32,
    /// Σ target_i × bytes_i over admitted entries.
    cache_charged: u64,
    /// External width entries (WS-O), slab-indexed by the id returned from
    /// `admit_external`; len grows on demand up to [`MAX_EXTERNAL_ENTRIES`].
    external: Vec<Option<ExternalEntry>>,
}

impl LedgerInner {
    /// Σ active external width — the core-budget charge of the gangs.
    fn external_active(&self) -> u64 {
        self.external.iter().flatten().map(|e| u64::from(e.active)).sum()
    }
}

/// Relaxed observability counters feeding [`LedgerSnapshot`].
#[derive(Default)]
struct LedgerStats {
    yields: AtomicU64,
    renudges: AtomicU64,
    renudges_suppressed: AtomicU64,
    sub_threshold_admits: AtomicU64,
    /// External admissions refused at the [`MAX_EXTERNAL_ENTRIES`] cap
    /// (fail-open; asserted by the cap unit test).
    external_cap_refusals: AtomicU64,
}

pub struct AdmissionLedger {
    entries: Box<[CachePadded<EntryWords>]>,
    inner: Mutex<LedgerInner>,
    budgets: LedgerBudgets,
    /// budgets.join_threshold_ns, atomic-backed so deterministic tests can
    /// tighten it per instance (the set_decay_quantum_ns precedent).
    join_threshold_ns: AtomicU64,
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
                req: (0..nslots).map(|_| None).collect(),
                admitted: 0,
                cache_charged: 0,
                external: Vec::new(),
            }),
            join_threshold_ns: AtomicU64::new(budgets.join_threshold_ns),
            budgets,
            stats: LedgerStats::default(),
        }
    }

    pub fn budgets(&self) -> LedgerBudgets {
        LedgerBudgets {
            join_threshold_ns: self.join_threshold_ns.load(Ordering::Relaxed),
            ..self.budgets
        }
    }

    /// Test hook (per-instance JOIN threshold; see the field doc).
    pub(crate) fn set_join_threshold_ns(&self, ns: u64) {
        self.join_threshold_ns.store(ns, Ordering::SeqCst);
    }

    /// ARRIVAL: register + recompute targets (incumbents over their new
    /// target shed at their next claim boundary). Called from
    /// start_rg_locked under the membership lock (lock order:
    /// membership → ledger.inner; never inverted).
    pub(crate) fn admit(&self, slot: usize, req: WidthRequest) -> ArrivalNudge {
        let mut inner = lock(&self.inner);
        debug_assert!(inner.req[slot].is_none(), "slot admitted twice without retire");
        inner.req[slot] = Some(req);
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
        if inner.req[slot].take().is_none() {
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

    /// EXTERNAL ADMIT (WS-O wave 2; module doc "External width entries"):
    /// register a non-pool parallel gang and grant it HEADROOM-ONLY width —
    /// `min(requested, cores − Σ pool targets − Σ external active)` under
    /// `inner` (taken ALONE; the caller never holds the membership lock —
    /// lock-order note in the module doc). The grant is FROZEN for the
    /// entry's lifetime and MAY BE 0 (the caller must have a serial path).
    /// Pool targets are recomputed so the new entry's active width is
    /// charged immediately (incumbents shed at their next claim boundary).
    /// None ⇔ the [`MAX_EXTERNAL_ENTRIES`] cap — FAIL-OPEN, the caller
    /// keeps today's uncapped path.
    pub(crate) fn admit_external(&self, requested: u32) -> Option<(usize, u32)> {
        let mut inner = lock(&self.inner);
        let id = match inner.external.iter().position(Option::is_none) {
            Some(free) => free,
            None if inner.external.len() < MAX_EXTERNAL_ENTRIES => {
                inner.external.push(None);
                inner.external.len() - 1
            }
            None => {
                RuntimeStats::tick(&self.stats.external_cap_refusals);
                return None;
            }
        };
        // Headroom at this instant: core budget minus what pool entries are
        // currently entitled to (their targets — using grants instead would
        // let a gang seize width an admitted query is about to take back)
        // minus the other gangs' active width. Pool floor overshoot
        // (Σ targets can exceed cores when entries > cores) saturates to 0.
        let pool_targets: u64 = inner
            .req
            .iter()
            .enumerate()
            .filter(|(_, r)| r.is_some())
            .map(|(slot, _)| u64::from(self.entries[slot].target.load(Ordering::Relaxed)))
            .sum();
        let headroom = u64::from(self.budgets.cores)
            .saturating_sub(pool_targets)
            .saturating_sub(inner.external_active());
        let granted = requested.min(headroom.min(u64::from(u32::MAX)) as u32);
        inner.external[id] = Some(ExternalEntry { granted, active: granted });
        // Charge the grant against pool targets NOW (recompute participation
        // — narrowing only, so the widened count is always 0 here).
        self.recompute_locked(&mut inner);
        Some((id, granted))
    }

    /// EXTERNAL SETTLE: record the gang's ACTIVE width (launched/live
    /// worker count — parked workers hold no grant; composition rule in the
    /// module doc). Clamped to the frozen grant; callable repeatedly
    /// (launch, per-rescan relaunch, partial exit). Returns the number of
    /// pool entries whose target ROSE — the caller's wake hint.
    pub(crate) fn settle_external(&self, id: usize, active: u32) -> u32 {
        let mut inner = lock(&self.inner);
        let Some(Some(e)) = inner.external.get_mut(id) else {
            debug_assert!(false, "settle_external on a retired/unknown entry");
            return 0;
        };
        debug_assert!(
            active <= e.granted,
            "external settle above the frozen grant ({active} > {})",
            e.granted
        );
        e.active = active.min(e.granted);
        self.recompute_locked(&mut inner)
    }

    /// EXTERNAL RETIRE: drop the entry and return its width to the pool
    /// (recompute; the return value is the widened-targets wake hint).
    /// Exactly once per admit — the RAII lease owns the id.
    pub(crate) fn retire_external(&self, id: usize) -> u32 {
        let mut inner = lock(&self.inner);
        let Some(slot) = inner.external.get_mut(id) else {
            debug_assert!(false, "retire_external on an unknown id");
            return 0;
        };
        if slot.take().is_none() {
            debug_assert!(false, "retire_external on a retired entry");
            return 0;
        }
        self.recompute_locked(&mut inner)
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
        for (slot, req) in inner.req.iter().enumerate() {
            if req.is_some() {
                granted_total += self.entries[slot].granted.load(Ordering::Relaxed);
                target_total += self.entries[slot].target.load(Ordering::Relaxed);
            }
        }
        let mut external_admitted = 0u32;
        let mut external_granted = 0u32;
        let mut external_active = 0u32;
        for e in inner.external.iter().flatten() {
            external_admitted += 1;
            external_granted += e.granted;
            external_active += e.active;
        }
        LedgerSnapshot {
            admitted: inner.admitted,
            granted_total,
            target_total,
            cache_charged_bytes: inner.cache_charged,
            yields: self.stats.yields.load(Ordering::Relaxed),
            renudges: self.stats.renudges.load(Ordering::Relaxed),
            renudges_suppressed: self.stats.renudges_suppressed.load(Ordering::Relaxed),
            sub_threshold_admits: self.stats.sub_threshold_admits.load(Ordering::Relaxed),
            external_admitted,
            external_granted,
            external_active,
            external_cap_refusals: self.stats.external_cap_refusals.load(Ordering::Relaxed),
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

    /// Target recompute (membership-event cadence, under `inner`):
    /// target_i = max(1, min(ceiling_i, predicted_i, fair_i, cache room)).
    /// Fair shares split the core budget — MINUS the external entries'
    /// active width (recompute participation, module doc: the gangs' charge
    /// comes off the top; a zeroed budget still floors every pool target at
    /// 1, the no-wedge guarantee) — equally; the remainder lands in
    /// slot order (WHICH entry actually consumes spare width is the
    /// pass-ordered pick's decision — module doc). The final max(1) is the
    /// liveness floor, which wins over the cache clamp by design. Refills
    /// every entry's re-nudge budget. Returns the number of entries whose
    /// target ROSE (the worker-freed wake hint).
    fn recompute_locked(&self, inner: &mut LedgerInner) -> u32 {
        let n = inner.admitted;
        if n == 0 {
            inner.cache_charged = 0;
            return 0;
        }
        let budget = u64::from(self.budgets.cores)
            .saturating_sub(inner.external_active())
            .min(u64::from(u32::MAX)) as u32;
        let base = budget / n;
        let mut rem = budget % n;
        let mut charged: u64 = 0;
        let mut widened = 0u32;
        for (slot, req) in inner.req.iter().enumerate() {
            let Some(req) = req else { continue };
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
                let room = self.budgets.cache_bytes.saturating_sub(charged)
                    / req.cache_bytes_per_worker;
                t = t.min(room.min(u64::from(u32::MAX)) as u32);
            }
            let t = t.max(1); // liveness floor: target >= 1 while admitted
            charged = charged
                .saturating_add(u64::from(t).saturating_mul(req.cache_bytes_per_worker));
            let e = &self.entries[slot];
            let old = e.target.swap(t, Ordering::Relaxed);
            if t > old {
                widened += 1;
            }
            e.renudge_left.store(self.budgets.renudge_max, Ordering::Relaxed);
        }
        inner.cache_charged = charged;
        widened
    }
}
