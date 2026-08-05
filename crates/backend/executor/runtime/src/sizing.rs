//! Duration-adaptive task sizing — the Umbra SIGMOD'21 §3.1 state machine
//! (lit-review §1.4), in granule units.
//!
//! Tasks are normalized by TIME, not rows: per-morsel duration varies >30×
//! across pipelines at fixed sizes. Per-pipeline state machine:
//!   - STARTUP: exponentially growing morsels, C0 (=16) granules, ×2, next
//!     doubling only if predicted to fit the remaining task budget
//!     (2·t_last ≤ t_max − Σt); the last morsel's measured throughput seeds
//!     the estimate.
//!   - DEFAULT: one morsel of T·t_max granules per task; after execution
//!     T' = α·T̂ + (1−α)·T with α = 0.8 (recent-heavy EWMA).
//!   - SHUTDOWN (photo finish): once predicted remaining time < W·t_max,
//!     morsels of duration max(remaining/W, t_min) so all workers reach the
//!     pipeline barrier together instead of one straggler holding it.
//!
//! t_max is tunable (default 2 ms — PGRUST_RUNTIME_TMAX_US; a "GUC" stays an
//! env knob for now: pg_settings byte-identity forbids new real GUCs, the
//! lane_pool precedent). Deterministic unit tests drive this with
//! [`crate::clock::VirtualClock`].

use crate::sync::{lock, Mutex};

pub const DEFAULT_T_MAX_NS: u64 = 2_000_000; // 2 ms (Umbra)
pub const DEFAULT_T_MIN_NS: u64 = 500_000; // photo-finish floor, t_max/4
pub const EWMA_ALPHA: f64 = 0.8; // weight of the NEW measurement (recent-heavy)

#[derive(Clone, Copy, Debug)]
pub struct SizingParams {
    pub t_max_ns: u64,
    pub t_min_ns: u64,
}

impl Default for SizingParams {
    fn default() -> Self {
        SizingParams { t_max_ns: DEFAULT_T_MAX_NS, t_min_ns: DEFAULT_T_MIN_NS }
    }
}

// ---------------------------------------------------------------------------
// Claim-duration DOP scaling (tails192, 48xl charter item #4).
//
// At high pipeline width the ~2ms claim cadence multiplies into shared-
// scheduler pressure: 191 workers x ~2ms claims ~= 95K shared-cursor touches
// per second, all CASing the same TaskSet cursor (the 48xl in-drive cycle-
// inflation family, notes/48xl-first-run-2026-07-15.md §3). Scale the
// per-task duration TARGET with the live width: identity at W <= 32
// (16-thread behavior unchanged by construction), linear ramp above, x2.5
// at 191 (2ms -> 5ms). The photo-finish floor t_min is deliberately NOT
// scaled — end-game claims still shrink to the same 500us floor, so the
// finish-spread posture is preserved (the elasticity oracle for this knob).
//
// DEFAULT OFF (opt-in PGRUST_RUNTIME_TMAX_DOPSCALE=1): the 48xl window-C
// A/B (2026-07-15, notes/tails192-lane.md) measured NO wall win from this
// lever at any width on the expr-key/10M-group arms and a +18% 10M-group@191 REGRESSION (iso-A
// exonerated the endgame split; dopscale-on was the delta). Suspected
// mechanism: a fast zone-skipped first epoch seeds tput high and the
// duration-targeted spanning oversizes claims of HEAVY epochs. Re-enable
// only behind a heavy-epoch guard + a fresh A/B (the recorded increment-2
// candidate: contention-responsive growth, which self-corrects).
// (env, not a GUC — pg_settings byte-identity law, the TMAX_US precedent.)
// ---------------------------------------------------------------------------
pub const DOPSCALE_W0: u64 = 32; // identity at or below this width
pub const DOPSCALE_W1: u64 = 191; // full multiplier at or above this width
pub const DOPSCALE_MAX_X: f64 = 2.5; // t_max multiplier at W1

pub fn dopscale_enabled() -> bool {
    static ON: crate::sync::OnceLock<bool> = crate::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_TMAX_DOPSCALE").is_ok_and(|v| v.trim() == "1")
    })
}

/// The pure width→params ramp (unit-tested independent of the env gate).
pub fn dopscale_ramp(params: SizingParams, w: u64) -> SizingParams {
    if w <= DOPSCALE_W0 {
        return params;
    }
    let frac = ((w - DOPSCALE_W0) as f64 / (DOPSCALE_W1 - DOPSCALE_W0) as f64).min(1.0);
    let x = 1.0 + (DOPSCALE_MAX_X - 1.0) * frac;
    SizingParams { t_max_ns: ((params.t_max_ns as f64) * x) as u64, t_min_ns: params.t_min_ns }
}

impl SizingParams {
    /// Width-scaled params for one task: t_max ramps with the live worker
    /// count, t_min (photo-finish floor) is untouched. Identity for
    /// `w <= DOPSCALE_W0` and unless the opt-in is set (default OFF — the
    /// window-C 10M-group@191 verdict above).
    pub fn scaled_for_width(self, w: u64) -> SizingParams {
        if !dopscale_enabled() {
            return self;
        }
        dopscale_ramp(self, w)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Startup,
    Default,
    Shutdown,
}

/// Which rule produced a morsel size (feeds the sizing-decision counters).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizingDecision {
    Ramp,
    Default,
    Shutdown,
}

/// Per-pipeline (per-TaskSet) shared sizing state. Touched once per morsel
/// under a Mutex — morsel cadence is ≥ hundreds of µs, so this is far below
/// contention; revisit in M1 if profiles ever say otherwise.
pub struct SizerShared {
    inner: Mutex<SizerInner>,
}

struct SizerInner {
    phase: Phase,
    /// Throughput estimate in granules per nanosecond. Meaningless in
    /// Startup (no estimate yet).
    tput: f64,
}

impl SizerShared {
    pub fn new() -> Self {
        SizerShared { inner: Mutex::new(SizerInner { phase: Phase::Startup, tput: 0.0 }) }
    }

    pub fn phase(&self) -> Phase {
        lock(&self.inner).phase
    }

    /// Throughput estimate (granules/ns); 0.0 while still in Startup.
    pub fn throughput(&self) -> f64 {
        lock(&self.inner).tput
    }
}

impl Default for SizerShared {
    fn default() -> Self {
        Self::new()
    }
}

enum TaskMode {
    /// Startup ramp in progress inside this task; next morsel size.
    Ramp { next: u64, spent_ns: u64, last_ns: u64 },
    /// Default/Shutdown: single right-sized morsel, then the task ends.
    Single,
}

/// Per-task sizing driver: one exists for the duration of one task (one
/// worker × one claim loop). The scheduler sees only tasks; morsels are
/// sized and claimed inside (Umbra's tasks-see-scheduler decoupling).
pub struct TaskSizer {
    params: SizingParams,
    mode: TaskMode,
    done: bool,
}

impl TaskSizer {
    pub fn new(params: SizingParams, c0: u64) -> Self {
        TaskSizer {
            params,
            mode: TaskMode::Ramp { next: c0.max(1), spent_ns: 0, last_ns: 0 },
            done: false,
        }
    }

    /// The task has consumed its budget (≈t_max) and must return to the
    /// scheduler (which may hand the worker the same task set again).
    pub fn task_done(&self) -> bool {
        self.done
    }

    /// Size the next morsel in granules. `remaining` is the unclaimed
    /// granule count, `workers` the number of workers currently on this
    /// pipeline (W of the photo finish). The claim path clamps the result to
    /// source boundaries and `remaining`.
    pub fn next_size(
        &mut self,
        shared: &SizerShared,
        remaining: u64,
        workers: u64,
    ) -> (u64, SizingDecision) {
        debug_assert!(!self.done);
        let mut inner = lock(&shared.inner);
        match inner.phase {
            Phase::Startup => {
                let next = match &self.mode {
                    TaskMode::Ramp { next, .. } => *next,
                    // Phase regressed? Impossible (Startup never re-entered);
                    // treat as ramp restart for robustness.
                    TaskMode::Single => 1,
                };
                (next, SizingDecision::Ramp)
            }
            Phase::Default | Phase::Shutdown => {
                let w = workers.max(1);
                if inner.phase == Phase::Default {
                    // Shutdown transition: predicted remaining time < W·t_max.
                    let remaining_ns = remaining as f64 / inner.tput.max(f64::MIN_POSITIVE);
                    if remaining_ns < (w * self.params.t_max_ns) as f64 {
                        inner.phase = Phase::Shutdown;
                    }
                }
                self.mode = TaskMode::Single;
                match inner.phase {
                    Phase::Default => {
                        let size = (inner.tput * self.params.t_max_ns as f64) as u64;
                        (size.max(1), SizingDecision::Default)
                    }
                    Phase::Shutdown => {
                        let floor = (inner.tput * self.params.t_min_ns as f64) as u64;
                        let size = (remaining / w).max(floor).max(1);
                        (size, SizingDecision::Shutdown)
                    }
                    Phase::Startup => unreachable!(),
                }
            }
        }
    }

    /// Record a morsel execution of `granules` granules taking `dt_ns`.
    pub fn observe(&mut self, shared: &SizerShared, granules: u64, dt_ns: u64) {
        let dt = dt_ns.max(1);
        let measured = granules as f64 / dt as f64;
        match &mut self.mode {
            TaskMode::Ramp { next, spent_ns, last_ns } => {
                *spent_ns += dt;
                *last_ns = dt;
                // Next doubling only if predicted to fit the remaining budget.
                if 2 * dt <= self.params.t_max_ns.saturating_sub(*spent_ns) {
                    *next = (*next).saturating_mul(2);
                } else {
                    // Ramp over: the LAST morsel's measured throughput seeds
                    // the estimate; if another worker seeded concurrently,
                    // fold ours in by EWMA instead of clobbering.
                    let mut inner = lock(&shared.inner);
                    match inner.phase {
                        Phase::Startup => {
                            inner.phase = Phase::Default;
                            inner.tput = measured;
                        }
                        _ => {
                            inner.tput = EWMA_ALPHA * measured + (1.0 - EWMA_ALPHA) * inner.tput;
                        }
                    }
                    self.done = true;
                }
            }
            TaskMode::Single => {
                let mut inner = lock(&shared.inner);
                inner.tput = EWMA_ALPHA * measured + (1.0 - EWMA_ALPHA) * inner.tput;
                self.done = true;
            }
        }
    }
}
