//! sqe runtime layer — standing Faces, condition cache, build-concurrency
//! law, phase timers. Ported from the PoC's sqe/mod.rs with the P1-1
//! reshapes of record (port-study/port-map.md §1, risks.md §1-§2):
//!
//! - `Faces` is PER-OPEN-RELATION state owned by an engine handle, keyed
//!   by relation identity through ownership — the process-global
//!   `OnceLock<Faces>` ("one bank per process" PoC shortcut) is dead.
//! - The condition cache is keyed by TYPED fingerprints (ir::ConjFp —
//!   attno + type oid + collation oid + op + canonical datum), so
//!   collation-illegal verdict sharing is unrepresentable.
//! - [RULING 2026-08-17, Michael] Outside the condition cache the engine
//!   persists NOTHING across queries. The `PGRUST_SQE_PLANE_CACHE`
//!   pricing arm is deleted; `reset_per_query` always runs.
//! - Measured-settled env arms deleted (risks.md §2 bucket a): lock-free
//!   memo builds, directory-metadata `is_dict`, shared-handle
//!   VerdictWords compute, part-parallel face builds, payload prewarm are
//!   the only arms; `PGRUST_SQE_GLOBALDICT` and every global stitched-
//!   dict path are gone (the plan prohibits them — "removed twice").
//! - Genuine tunables are constants-with-provenance on `SqeConfig`;
//!   nothing in this crate reads `std::env`.

use crate::answer::AnswerSet;
use crate::bank::Bank;
use crate::condcache::GVerdict;
use crate::ir::{ConjFp, Fingerprint};
use crate::kernels_dec::{sma_build, SmaFlat, VerdictWords};
use crate::pool::Pool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

/// (part_idx, granule, rows_in_granule, global_row_base) — scan::granule_walk.
pub type Unit = (usize, u32, u32, u64);

/// Engine configuration — construction-time parameters, never env reads.
#[derive(Clone, Debug)]
pub struct SqeConfig {
    /// Worker width for face builds and stencil claims (the pool width).
    pub threads: usize,
    /// F5 face-build concurrency cap: the demand path must not out-run
    /// the prefetch wave with N concurrent small preads (hot-shape: 96 parallel
    /// WordCol opens starved the issuers, +600ms). Provenance:
    /// PGRUST_SQE_FACE_CONC default on the 96-way c8g rig.
    pub face_conc: usize,
    /// Condcache populate admission: refuse planes denser than this
    /// survivors/rows ratio (decode_sel-per-verdict replay is SLOWER than
    /// the fused recompute above it — the M5 u6 finding). Provenance:
    /// PGRUST_SQE_CONDCACHE_MAXDENSITY default.
    pub condcache_max_density: f64,
    /// Populate policy (Michael's rung, 2026-08-17): first execution
    /// computes and does NOT populate; second populates; third replays.
    pub populate: PopulatePolicy,
    /// [ruling refined 2] stats/walk/sma metadata faces persist
    /// cross-query ("I'm fine to cache the stats section"). `false` =
    /// the pricing arm (was PGRUST_SQE_STATS_CACHE=0): evict them per
    /// query-run too.
    pub stats_cache: bool,
    /// [ruling refined 1] per-part verdict tables are condcache-CLASS
    /// (predicate-once-per-distinct over a part's dictionary, replayed by
    /// code — the same "evaluate once, replay" idea at dictionary grain,
    /// snapshot-safe: bound to one part's dict). They persist cross-query
    /// under the class's second-touch admission. `false` = the pricing
    /// arm (was PGRUST_SQE_VW_CACHE=0): per-query verdict tables.
    pub vw_cache: bool,
    /// [fpcombine] dict-grouped combine on per-part ENTRY FINGERPRINTS
    /// (entry_fp128 of each dict entry's bytes, computed per query run at
    /// entry grain) instead of per-fragment byte hashing + byte equality.
    /// Same 128-bit-equality-as-identity convention as the shipped
    /// text128 path. No cross-part state of any kind:
    /// fingerprints are a pure per-part function of one part's dict.
    /// Default ON (Michael's ruling 2026-08-17, on the seeded SipHash128
    /// entry_fp128 basis of 753b059fb3). Env is opt-OUT:
    /// PGRUST_SQE_FPCOMBINE=0 (or "off") disables; the rig's --fp flag
    /// is a kept-for-compat no-op.
    pub fpcombine: bool,
    /// [fpcombine] OPTIONAL cross-run persistence of the fingerprint
    /// plane under condcache-style SECOND-TOUCH admission. The plane is a
    /// pure function of ONE part's dict (snapshot-safe like the verdict
    /// tables), but the face-law ruling currently grants second-touch to
    /// the condition-cache class only. Default ON (Michael's ruling
    /// 2026-08-17; independent of fpcombine). Env is opt-OUT:
    /// PGRUST_SQE_FPCACHE=0 (or "off") disables; the rig's --fpcache
    /// flag is a kept-for-compat no-op.
    pub fpcache: bool,
    /// [sqe-tpch-mech] Direct-array grouped state: when a group key's
    /// domain is witnessed dense-and-bounded (exact part-record min/max,
    /// width inside `planner::direct_array_bytes_cap` — the answer-bound
    /// budget law: bounded answers price under the occupancy budget),
    /// the grouped fold
    /// elects a shared atomic accumulator array indexed `key - lo` (the
    /// tpch-floor Q18 idiom) instead of hash grouped state. Answers are
    /// byte-identical either arm (the A/B identity gate forces both).
    /// Default ON; env is opt-OUT: PGRUST_SQE_DIRECT_ARRAY=0 (or "off").
    pub direct_array: bool,
    /// [psma-consume] Consult sealed §8.2 PSMA sections at predicate
    /// grain: the driving selective-class int conjunct seeds its
    /// selection from the granule's candidate row slice (zone-maybe
    /// granules only; rows outside the slice are excluded without
    /// evaluation — psmaface.rs carries the soundness argument). Default
    /// ON; env is opt-OUT: PGRUST_SQE_PSMA=0 (or "off") — the A/B kill
    /// switch.
    pub psma: bool,
    /// [sortgrp v1] SortGrouped admission budget, BYTES: the witnessed
    /// payload-byte estimate (sort buffers + append arenas) a statement
    /// may hold; over-budget shapes refuse typed (`sortagg-over-budget`)
    /// — spill is P6-1 scope, OOM has no path. A named work_mem-class
    /// input per the election-inputs discipline (never a baked scalar);
    /// env override PGRUST_SQE_SORTAGG_BUDGET (bytes) for pricing/gate
    /// arms, GUC plumbing at the pool/guc seam's lane.
    pub sortagg_budget_bytes: u64,
    /// [winserve v1] WindowServe admission budget, BYTES: the witnessed
    /// scatter/sort/answer payload-byte estimate a window statement may
    /// hold (a window answer is O(rows)); over-budget shapes refuse
    /// typed (`winagg-over-budget`) — spill is P6-1 scope, OOM has no
    /// path. Same work_mem-class law as the sortagg budget; env override
    /// PGRUST_SQE_WINDOW_BUDGET (bytes).
    pub window_budget_bytes: u64,
    /// [claim-horizon] demand-paced prefetch of the claim frontier into
    /// the part residency caches (`horizon` module) — the cold-scan
    /// engine. Arms `horizon::engage` at bank open; the lead and tuning
    /// live on the module's PGRUST_SQE_HORIZON* envs. `false` = never
    /// engage (kill switch PGRUST_SQE_HORIZON=0/off resolves here).
    pub horizon: bool,
    /// [P6-1 spill] Grouped-agg spill arm (spill-design.md): partition-
    /// owned grouped state over the E18 budget spills per partition and
    /// k-way-merges at finalize; shapes with no spill arm refuse typed
    /// over budget. Default ON; env is opt-OUT: PGRUST_SQE_SPILL=0 (or
    /// "off") reverts to the legacy arm (no accounting, no spill, no
    /// budget refusal — behavior identical to the pre-spill engine).
    pub spill: bool,
    /// [P6-1 spill] Grouped-statement budget OVERRIDE, bytes, for
    /// pricing/gate arms (env PGRUST_SQE_GROUPED_BUDGET). `None` = the
    /// E18 law (`cost_params::grouped_budget_bytes(threads)` — the
    /// work_mem-class default). GUC plumbing at the pool/guc seam's
    /// lane, per the sortagg/window budget precedent.
    pub grouped_budget_override: Option<u64>,
    /// [E18-M] Physical RAM of THIS machine, bytes, for the grouped
    /// budget's machine floor (`cost_params::grouped_machine_floor_bytes`
    /// — the wave-3 submission unrefusal). Default = the box probe
    /// (/proc/meminfo, macOS sysctl), 0 when unprobeable or when the
    /// kill switch PGRUST_SQE_GROUPED_MEM_FLOOR=0/off disarms the floor
    /// (0 reproduces the width law verbatim). Tests set the field
    /// directly — no process-global state.
    pub machine_mem_bytes: u64,
    /// [cap-retire] Grouped ANSWER-plane budget OVERRIDE, bytes, for
    /// pricing/gate arms (env PGRUST_SQE_ANSWER_BUDGET). `None` = the
    /// E17 answer-face law (`cost_params::answer_budget_bytes()` — the
    /// unbounded-materialization authority). The finalize answer-bytes
    /// refusal (spill-design.md §3.4, RULED 2026-08-19) prices against
    /// the effective value; GUC plumbing at the pool/guc seam's lane,
    /// per the grouped-budget precedent.
    pub answer_budget_override: Option<u64>,
    /// [xquery-bound, CWE-770] Entry-count cap for the cross-query caches
    /// on the shared per-relation `Faces` whose keys embed attacker-chosen
    /// query constants (`cond`, `verdict_words`, `frames`, and the
    /// `touch`/`vw_touch` recurrence witnesses). Without a bound these grow
    /// linearly with the number of distinct predicate constants any client
    /// issues, so a stream of distinct-constant queries drives process
    /// memory to OOM (the caches are process-global, shared across all
    /// sessions, and survive disconnect). Each cache independently enforces
    /// this cap: when it is full and a new key arrives, it is cleared
    /// (clear-on-pressure) before the insert. Every entry is recomputable
    /// on the next miss, so eviction is a pure performance event — never a
    /// correctness one. Resident memory attributable to these caches is
    /// therefore O(cap) regardless of the distinct-constant stream length.
    /// Provenance: PGRUST_SQE_XQUERY_CACHE_MAX (entries) — a work_mem-class
    /// input per the budget-tunable discipline; 0 disables the cap.
    pub xquery_cache_max_entries: usize,
    /// [scan-cap-retire] Serve UNWITNESSED unbounded row-returning scans
    /// under the answer-face law (the q1/q5 idiom, extending the RULED
    /// cap-retire posture to the scan face): admission no longer demands
    /// the zone-plane survivor witness — the stencil counts the TRUE
    /// survivors and refuses typed (`scan-answer-over-cap`) the moment
    /// they exceed the cap, bounded state, never truncates, never OOMs.
    /// Provenance: ClickBench q19 (equality on a full-range hash column
    /// — zone planes prune nothing, the pre-scan witness can never exist
    /// on real data; r14 tax cells 20260821T035653Z/035759Z, M2
    /// `Q19|refused|tier/step/scan-rows-unwitnessed`). Default ON; env
    /// is opt-OUT: PGRUST_SQE_SCAN_CAP_RETIRE=0 (or "off") restores the
    /// admission witness gate verbatim.
    pub scan_cap_retire: bool,
    /// [scan-cap-retire] Scan ANSWER-plane row-cap OVERRIDE for
    /// pricing/gate arms (env PGRUST_SQE_SCAN_ANSWER_CAP, rows). `None`
    /// = `planner::GROUP_ROW_CAP` (the one answer-row cap law).
    pub scan_answer_cap_override: Option<u64>,
}

/// Opt-OUT env gate for the default-ON fp arms: only an explicit "0" /
/// "off" disables (any other value, or unset, leaves the default ON).
fn env_not_disabled(name: &str) -> bool {
    !matches!(std::env::var(name).as_deref(), Ok("0") | Ok("off"))
}

/// [bulkentries] Sequential full-dictionary builds engage the bulk entry
/// cursor ([`pgrc2_read::dicthandle::DictHandle::entries`]) instead of a
/// per-code `entry()` call: one frame-grain refill amortizes the
/// per-entry bounds + ensure + arm-dispatch cost. Both arms yield
/// bit-identical output (pinned by `tests/dict_fp_identity.rs`).
/// Default ON (Michael's ruling 2026-08-18); env is opt-OUT:
/// PGRUST_SQE_BULK_ENTRIES=0 (or "off") disables.
pub fn bulk_entries_on() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| env_not_disabled("PGRUST_SQE_BULK_ENTRIES"))
}

/// [E18-M] Physical RAM probe, bytes — Linux via /proc/meminfo (the
/// benchmark targets; the same source the entry conf's memory formulas
/// read), macOS via `sysctl hw.memsize` (dev boxes / `cargo test`); 0
/// when neither answers (the floor then contributes nothing and the
/// width law stands alone). Probed once per process.
fn machine_mem_probe() -> u64 {
    static MEM: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *MEM.get_or_init(|| {
        if let Ok(s) = std::fs::read_to_string("/proc/meminfo") {
            for line in s.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    if let Some(kb) = rest.split_whitespace().next().and_then(|t| t.parse::<u64>().ok()) {
                        return kb.saturating_mul(1024);
                    }
                }
            }
        }
        #[cfg(target_os = "macos")]
        if let Ok(out) =
            std::process::Command::new("/usr/sbin/sysctl").args(["-n", "hw.memsize"]).output()
        {
            if let Some(b) =
                String::from_utf8(out.stdout).ok().and_then(|s| s.trim().parse::<u64>().ok())
            {
                return b;
            }
        }
        0
    })
}

impl Default for SqeConfig {
    fn default() -> SqeConfig {
        SqeConfig {
            threads: 1,
            face_conc: 16,
            condcache_max_density: 0.5,
            populate: PopulatePolicy::Second,
            stats_cache: true,
            vw_cache: true,
            fpcombine: env_not_disabled("PGRUST_SQE_FPCOMBINE"),
            fpcache: env_not_disabled("PGRUST_SQE_FPCACHE"),
            direct_array: env_not_disabled("PGRUST_SQE_DIRECT_ARRAY"),
            psma: env_not_disabled("PGRUST_SQE_PSMA"),
            sortagg_budget_bytes: std::env::var("PGRUST_SQE_SORTAGG_BUDGET")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(256 * 1024 * 1024),
            window_budget_bytes: std::env::var("PGRUST_SQE_WINDOW_BUDGET")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(256 * 1024 * 1024),
            horizon: crate::horizon::horizon() > 0,
            spill: env_not_disabled("PGRUST_SQE_SPILL"),
            grouped_budget_override: std::env::var("PGRUST_SQE_GROUPED_BUDGET")
                .ok()
                .and_then(|v| v.parse().ok()),
            machine_mem_bytes: if env_not_disabled("PGRUST_SQE_GROUPED_MEM_FLOOR") {
                machine_mem_probe()
            } else {
                0
            },
            answer_budget_override: std::env::var("PGRUST_SQE_ANSWER_BUDGET")
                .ok()
                .and_then(|v| v.parse().ok()),
            xquery_cache_max_entries: std::env::var("PGRUST_SQE_XQUERY_CACHE_MAX")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8192),
            scan_cap_retire: env_not_disabled("PGRUST_SQE_SCAN_CAP_RETIRE"),
            scan_answer_cap_override: std::env::var("PGRUST_SQE_SCAN_ANSWER_CAP")
                .ok()
                .and_then(|v| v.parse().ok()),
        }
    }
}

impl SqeConfig {
    /// [P6-1 spill] The effective grouped-statement budget, bytes: the
    /// env/GUC override when present, else the E18 width law over the
    /// configured pool width (spill-design.md §2).
    #[inline]
    pub fn grouped_budget_bytes(&self) -> u64 {
        self.grouped_budget_override.unwrap_or_else(|| {
            let cp = crate::cost_params::target();
            cp.grouped_budget_bytes(self.threads)
                .max(cp.grouped_machine_floor_bytes(self.machine_mem_bytes))
        })
    }

    /// [cap-retire] The effective grouped ANSWER-plane budget, bytes:
    /// the env/GUC override when present, else the E17 answer-face law
    /// (spill-design.md §3.4).
    #[inline]
    pub fn answer_budget_bytes(&self) -> u64 {
        self.answer_budget_override
            .unwrap_or_else(|| crate::cost_params::target().answer_budget_bytes())
    }

    /// [scan-cap-retire] The effective scan answer-row cap: the env/gate
    /// override when present, else the one answer-row cap law
    /// (`planner::GROUP_ROW_CAP`).
    #[inline]
    pub fn scan_answer_cap(&self) -> u64 {
        self.scan_answer_cap_override.unwrap_or(crate::planner::GROUP_ROW_CAP)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PopulatePolicy {
    /// populate on the first execution, no density gate (PoC control arm)
    First,
    /// populate only on the SECOND execution of a fingerprint (default)
    Second,
    /// never populate (measurement arm)
    Never,
}

/// Per-(part, attno) dict facts, held open for the bank's lifetime.
pub struct DictFace {
    pub dh: Option<Arc<pgrc2_read::dicthandle::DictHandle>>,
    pub ncodes: u32,
    /// Local code of the empty string, if present (sorted dict: code 0).
    pub empty_code: Option<u32>,
}

/// One predicate's cached granule verdicts, aligned with `units` (the
/// anchor column's granule walk). Encodings per PLANNER-SPEC §1.7.
pub enum CVerdict {
    Skip,
    AllPass,
    Rows(Vec<u16>),
    Bitmap(Vec<u64>),
}

impl CVerdict {
    pub fn from_gverdict(g: GVerdict) -> CVerdict {
        match g {
            GVerdict::Skip => CVerdict::Skip,
            GVerdict::AllPass => CVerdict::AllPass,
            GVerdict::Rows(r) => CVerdict::Rows(r),
        }
    }
    /// Encoding election (§1.7) over a computed survivor list.
    pub fn encode(sel: Vec<u16>, rows: usize) -> CVerdict {
        if sel.is_empty() {
            CVerdict::Skip
        } else if sel.len() == rows {
            CVerdict::AllPass
        } else if crate::planner::cache_rowlist(sel.len(), rows) {
            CVerdict::Rows(sel)
        } else {
            let mut w = vec![0u64; rows.div_ceil(64)];
            for &r in &sel {
                w[(r >> 6) as usize] |= 1u64 << (r & 63);
            }
            CVerdict::Bitmap(w)
        }
    }
}

/// [famB M1] Typed cache sidecar (the hot-shape plan flag): per-granule survivor
/// CODE lists aligned with the verdict vector. u32::MAX = row not
/// dict-resolved (consumer re-hydrates via decode_sel).
pub enum CacheSidecar {
    Codes(Vec<Vec<u32>>),
}

pub struct PredCache {
    pub units: Arc<Vec<Unit>>,
    pub v: Vec<CVerdict>,
    /// Total surviving rows (exact when no Recheck entries).
    pub survivors: u64,
    /// [famB M1] Optional typed payload rider (None for plain planes).
    pub sidecar: Option<CacheSidecar>,
    /// [oracle, ruling Q4] The canonical structural key the plane's
    /// ConjFp was minted from — compared on every hit in oracle/CI builds
    /// (exec.rs::oracle_verify_cond). Absent in production builds.
    #[cfg(feature = "oracle")]
    pub skey: crate::ir::StructuralPred,
}

/// [oracle, ruling Q4] Overhead proof: in production builds PredCache is
/// bit-identical to a twin without the structural key — the field and
/// every compare compile away.
#[cfg(not(feature = "oracle"))]
const _: () = {
    struct Twin {
        _units: Arc<Vec<Unit>>,
        _v: Vec<CVerdict>,
        _survivors: u64,
        _sidecar: Option<CacheSidecar>,
    }
    assert!(std::mem::size_of::<PredCache>() == std::mem::size_of::<Twin>());
};

/// [coldstart] Per-key build cells: the map lock is held only to find or
/// create the key's cell; the BUILD runs outside it under the cell's own
/// OnceLock — distinct keys build concurrently, each key exactly once.
/// (The build-under-lock arm `PGRUST_SQE_COLD_LOCKFREE=0` was the
/// measured-settled control; deleted at port.)
pub struct Memo<K, V> {
    m: Mutex<HashMap<K, Arc<OnceLock<Arc<V>>>>>,
}

impl<K, V> Default for Memo<K, V> {
    fn default() -> Self {
        Memo { m: Mutex::new(HashMap::new()) }
    }
}

impl<K: std::hash::Hash + Eq + Clone, V> Memo<K, V> {
    pub fn get(&self, k: &K) -> Option<Arc<V>> {
        self.m.lock().unwrap().get(k).and_then(|c| c.get().cloned())
    }
    pub fn get_or_build(&self, k: K, build: impl FnOnce() -> V) -> Arc<V> {
        let cell = {
            let mut m = self.m.lock().unwrap();
            m.entry(k).or_default().clone()
        };
        if let Some(v) = cell.get() {
            return v.clone();
        }
        // F5: bounded build concurrency — the demand path must not out-run
        // the prefetch wave with 96 concurrent small preads.
        let _permit = FacePermit::acquire();
        cell.get_or_init(|| Arc::new(build())).clone()
    }
    /// [xquery-bound, CWE-770] Entry-capped variant of `get_or_build` for a
    /// memo whose key embeds attacker-chosen query constants (`frames`).
    /// When the map already holds `cap` distinct keys and `k` is new, it is
    /// cleared (clear-on-pressure) before the cell is created, so resident
    /// entries never exceed `cap`. Every value is recomputable on the next
    /// miss, so eviction is a pure performance event. `cap == 0` disables
    /// the bound (identical to `get_or_build`).
    pub fn get_or_build_bounded(&self, k: K, cap: usize, build: impl FnOnce() -> V) -> Arc<V> {
        let cell = {
            let mut m = self.m.lock().unwrap();
            if cap != 0 && m.len() >= cap && !m.contains_key(&k) {
                m.clear();
            }
            m.entry(k).or_default().clone()
        };
        if let Some(v) = cell.get() {
            return v.clone();
        }
        let _permit = FacePermit::acquire();
        cell.get_or_init(|| Arc::new(build())).clone()
    }
    pub fn contains(&self, k: &K) -> bool {
        self.m.lock().unwrap().get(k).map(|c| c.get().is_some()).unwrap_or(false)
    }
    pub fn clear(&self) {
        self.m.lock().unwrap().clear();
    }
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.m.lock().unwrap().len()
    }
}

/// [xquery-bound, CWE-770] Entry-count bound for a cross-query `Faces`
/// cache whose key embeds attacker-chosen query constants. Call while
/// holding the map lock, immediately BEFORE inserting `k`: when the map is
/// already at `cap` and does not hold `k`, it is cleared (clear-on-
/// pressure) so resident entries never exceed `cap`. Every entry these
/// maps hold is recomputable on the next miss, so eviction is a pure
/// performance event, never a correctness one — this bounds resident
/// memory at O(cap) regardless of how many distinct predicate constants a
/// client issues. `cap == 0` disables the bound.
#[inline]
fn bound_xquery_cache<K: std::hash::Hash + Eq, V>(m: &mut HashMap<K, V>, k: &K, cap: usize) {
    if cap != 0 && m.len() >= cap && !m.contains_key(k) {
        m.clear();
    }
}

/// [coldstart] F5 face-build concurrency cap. A PROCESS-wide semaphore on
/// purpose: it bounds concurrent cold preads machine-wide, which is a
/// device property, not a relation property. Cap value from SqeConfig
/// would need per-Faces plumbing through generic Memo cells; the constant
/// carries the provenance instead (16, measured on the 96-way c8g rig;
/// re-cut per target is R3/P1-2 work).
pub const FACE_CONC: usize = 16;

struct FaceSem {
    m: Mutex<usize>,
    cv: std::sync::Condvar,
}
static FACE_SEM: OnceLock<FaceSem> = OnceLock::new();

thread_local! {
    static HOLDS_PERMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub struct FacePermit(bool);
impl FacePermit {
    /// Re-entrant per thread: a build nested inside a build (sma -> walk,
    /// dict_col -> dict) rides the outer permit — no self-deadlock.
    pub fn acquire() -> FacePermit {
        let cap = FACE_CONC;
        if cap == 0 || HOLDS_PERMIT.with(|h| h.get()) {
            return FacePermit(false);
        }
        let s = FACE_SEM.get_or_init(|| FaceSem { m: Mutex::new(0), cv: std::sync::Condvar::new() });
        loop {
            let mut g = s.m.lock().unwrap();
            if *g < cap {
                *g += 1;
                HOLDS_PERMIT.with(|h| h.set(true));
                return FacePermit(true);
            }
            // Cancel-aware wait: check OUTSIDE the lock (a raise while
            // holding would poison the process-wide semaphore).
            let _ = s
                .cv
                .wait_timeout(g, std::time::Duration::from_millis(10))
                .unwrap();
            crate::cancel::checkpoint();
        }
    }
}
impl Drop for FacePermit {
    fn drop(&mut self) {
        if self.0 {
            HOLDS_PERMIT.with(|h| h.set(false));
            let s = FACE_SEM.get().unwrap();
            *s.m.lock().unwrap() -= 1;
            s.cv.notify_one();
        }
    }
}

/// [coldstart] F4: frame-shaped plans open dict faces for the TOUCHED
/// parts only (the bank-wide arm was the measured-settled control).
pub const TOUCHED_FACES: bool = true;
#[inline]
pub fn touched_faces() -> bool {
    TOUCHED_FACES
}

/// Per-part map, part-parallel (`threads` = the elected worker width,
/// FACE_CONC-capped by the permit law — data-bearing faces: dict/word).
/// Output is index-ordered.
pub fn par_parts<T: Send>(threads: usize, n: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    par_parts_w(threads, n, f, true)
}

/// [coldstart F5] Metadata section faces (walk / stats / sma: ~1-2 KB
/// section preads per part) run at FULL width — the F5 cap protects the
/// prefetch wave from data-sized demand reads, and measured NEGATIVE on
/// hot-shape 89-column stats sweep (293 vs 172 ms uncapped) where the preads
/// are tiny.
pub fn par_parts_meta<T: Send>(threads: usize, n: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    par_parts_w(threads, n, f, false)
}

fn par_parts_w<T: Send>(threads: usize, n: usize, f: impl Fn(usize) -> T + Sync, capped: bool) -> Vec<T> {
    let cap = if capped && FACE_CONC > 0 { FACE_CONC } else { usize::MAX };
    let t = threads.min(n.max(1)).min(cap);
    if t <= 1 {
        return (0..n)
            .map(|i| {
                crate::cancel::checkpoint();
                f(i)
            })
            .collect();
    }
    // Workers are already bounded by `t` (<= cap): they run their leaf
    // builds permit-free (a worker waiting on a permit held by the outer
    // builder that joins on it would deadlock). On the resident pool the
    // permit-free window is per-item (workers outlive this fan-out).
    let per: Vec<Vec<(usize, T)>> = crate::pool::with_scoped(|pool| {
        if let Some(pool) = pool {
            pool.run_capped(
                n,
                t,
                |_| Vec::new(),
                |acc, i| {
                    let held = HOLDS_PERMIT.with(|h| h.replace(true));
                    acc.push((i, f(i)));
                    HOLDS_PERMIT.with(|h| h.set(held));
                },
            )
        } else {
            crate::scan::par_range(
                n,
                t,
                |_| {
                    HOLDS_PERMIT.with(|h| h.set(true));
                    Vec::new()
                },
                |acc, i| acc.push((i, f(i))),
            )
        }
    });
    let mut all: Vec<(usize, T)> = per.into_iter().flatten().collect();
    all.sort_by_key(|(i, _)| *i);
    all.into_iter().map(|(_, t)| t).collect()
}

/// [cold2] Attribution census: dict entries whose BYTES were touched this
/// process (batch adds at the bulk consumers). Process-global counter —
/// instrumentation, not state.
pub static ENTRIES_TOUCHED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[inline]
pub fn census_entries(n: u64) {
    ENTRIES_TOUCHED.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}

/// TextReg build kind (famA faces).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RegKind {
    Par,
    Merge,
}

/// The standing faces of ONE open relation: built lazily, once per bank
/// open, owned by the engine handle (`SqeCtx` borrows them). All maps are
/// memoized behind mutexes (build once, then lock-free Arc reads on the
/// hot path). Every derived plane except `cond` is dropped per query
/// (the 2026-08-17 ruling, `reset_per_query`).
pub struct Faces {
    pub cfg: SqeConfig,
    walks: Memo<u32, Vec<Unit>>,
    stats: Memo<u32, crate::statsview::StatsView>,
    sma: Memo<u32, SmaFlat>,
    /// [psma-consume] §8.2 PSMA face per column (stats/zone-metadata
    /// class — persists with walk/stats/sma under `cfg.stats_cache`).
    psma: Memo<u32, crate::psmaface::PsmaFace>,
    dicts: Memo<(usize, u32), DictFace>,
    /// [coldstart] whole-column dict-face vector (every part).
    dict_cols: Memo<u32, Vec<Arc<DictFace>>>,
    verdict_words: Mutex<HashMap<(u32, Fingerprint), Arc<VerdictWords>>>,
    word_cols: Memo<(usize, u32), Option<crate::fused::WordCol>>,
    /// THE condition cache: typed conjunction fingerprint -> granule
    /// verdicts. The ONLY cross-query state (the ruling).
    cond: Mutex<HashMap<ConjFp, Arc<PredCache>>>,
    /// Condcache recurrence witness (was a process-global `TOUCH` map in
    /// exec.rs — rehomed next to the store it admits into; same lifetime,
    /// same future invalidation story).
    touch: Mutex<HashMap<ConjFp, u32>>,
    /// [ruling refined 1] Verdict-table recurrence witness: the condcache
    /// class gets ONE admission discipline — compute-first on the first
    /// execution in a fresh process, populate on recurrence.
    vw_touch: Mutex<HashMap<(u32, Fingerprint), u32>>,
    /// [oracle, ruling Q4] Structural keys of the verdict-word planes:
    /// the exact VarPredTerm each (attno, Fingerprint) key was minted
    /// from, compared on every hit. Absent in production builds.
    #[cfg(feature = "oracle")]
    vw_skey: Mutex<HashMap<(u32, Fingerprint), crate::ir::VarPredTerm>>,
    /// [fpcombine] cross-run fingerprint-plane store (cfg.fpcache only —
    /// the ruling-measurement arm; empty otherwise) + its second-touch
    /// witness. NOT cleared per query on purpose: the flag's whole point
    /// is pricing cross-query persistence; shape-verified against the
    /// current faces on every replay (part_merge::build_fps_cached).
    fp_planes: Mutex<HashMap<u32, Arc<Vec<Vec<u128>>>>>,
    fp_touch: Mutex<Vec<u32>>,
    /// Text registries (famA): (attno, kind) -> TextReg.
    regs: Mutex<HashMap<(u32, u8), Arc<crate::grouped::TextReg>>>,
    /// [rehomed, risks.md §1] the hot-shape shared frame memo — was a
    /// bank-blind global in kernels_f6; keyed by (cid, edt, counter, dlo,
    /// dhi). The frame is "a cached verdict at rowlist grain" (PLANNER-SPEC
    /// §2.6) — condition-cache-class state, so like `cond` it SURVIVES
    /// reset_per_query (the PoC behavior of record: FRAME_MEMO was only
    /// cleared between banks).
    ///
    /// [cache-identity] The key carries the predicate's eq/range COLUMN
    /// attnos (cid, edt) alongside the three constants. The frame is the
    /// per-granule survivor rowlists of `cid = counter AND dlo <= edt <=
    /// dhi`; the columns are as much a semantic input as the constants, so
    /// two queries sharing (counter, dlo, dhi) but built over different
    /// columns MUST NOT collide (the same rule ConjFp follows). Omitting
    /// them let a query on (a, b) poison the cached rowlists a query on
    /// (c, d) would then reuse.
    pub frames: Memo<(u32, u32, i64, i64, i64), crate::kernels_f6::Frame>,
    /// [rehomed, risks.md §1] the ord-remap memo — was attno-keyed global
    /// in kernels_g2.
    pub ord_remaps: Memo<u32, crate::kernels_g2::OrdRemap>,
}

impl Faces {
    pub fn new(cfg: SqeConfig) -> Faces {
        Faces {
            cfg,
            walks: Memo::default(),
            stats: Memo::default(),
            sma: Memo::default(),
            psma: Memo::default(),
            dicts: Memo::default(),
            dict_cols: Memo::default(),
            verdict_words: Mutex::new(HashMap::new()),
            word_cols: Memo::default(),
            cond: Mutex::new(HashMap::new()),
            touch: Mutex::new(HashMap::new()),
            vw_touch: Mutex::new(HashMap::new()),
            #[cfg(feature = "oracle")]
            vw_skey: Mutex::new(HashMap::new()),
            fp_planes: Mutex::new(HashMap::new()),
            fp_touch: Mutex::new(Vec::new()),
            regs: Mutex::new(HashMap::new()),
            frames: Memo::default(),
            ord_remaps: Memo::default(),
        }
    }

    /// Drop the per-query-run planes (the ruling, REFINED twice —
    /// 2026-08-17 coldstart wave). Cross-query persistence is allowed for
    /// exactly two families: (1) the CONDCACHE CLASS — `cond` plus the
    /// per-part verdict tables (`verdict_words`), the same evaluate-once
    /// /replay idea at dictionary grain, under the class's second-touch
    /// admission; (2) the STATS/ZONE-METADATA faces — walk / stats / sma.
    /// Everything else (dict faces/columns, word cols, textreg-that-is-
    /// not-replay, structural memos) is built if the query needs it,
    /// used, dropped. `cfg.stats_cache` / `cfg.vw_cache` price the two
    /// refinements.
    pub fn clear_derived(&self) {
        if !self.cfg.stats_cache {
            self.walks.clear();
            self.stats.clear();
            self.sma.clear();
            self.psma.clear();
        }
        self.dicts.clear();
        self.dict_cols.clear();
        if !self.cfg.vw_cache {
            self.verdict_words.lock().unwrap().clear();
        }
        self.word_cols.clear();
        self.regs.lock().unwrap().clear();
        // `frames` and `ord_remaps` survive on purpose: both persisted
        // across queries in the PoC (harness globals cleared only between
        // banks) — the frame is condcache-class state (§2.6), and the ord
        // remap rides the same recurrence argument. Dropping them here
        // would change the elected arm's hot-rep shape vs the reference.
    }

    pub fn walk(&self, bank: &Bank, attno: u32) -> Arc<Vec<Unit>> {
        self.walks.get_or_build(attno, || {
            let t0 = std::time::Instant::now();
            let w = crate::scan::granule_walk_par(bank, attno, self.cfg.threads);
            crate::coldledger::note("walk", format!("attno={attno}"), t0, (w.len() * 24) as u64, crate::coldledger::Reason::TouchedByQuery);
            w
        })
    }

    /// Whole-bank §8.1 stats face for one column, parsed once per query run.
    /// [json-rung4] Word-lane columns answer from the bank's open-time
    /// lane plane (persistent across statements; byte-identical bodies).
    pub fn stats(&self, bank: &Bank, attno: u32) -> Arc<crate::statsview::StatsView> {
        if let Some(v) = bank.lane_stats(attno) {
            return v;
        }
        self.stats.get_or_build(attno, || {
            let t0 = std::time::Instant::now();
            let s = crate::statsview::StatsView::open(bank, attno, self.cfg.threads);
            crate::coldledger::note("stats", format!("attno={attno}"), t0, 0, crate::coldledger::Reason::FlatStatsDerived);
            s
        })
    }

    /// Flat SMA face (amortized across every predicate on the column).
    pub fn sma(&self, bank: &Bank, attno: u32) -> Arc<SmaFlat> {
        let units = self.walk(bank, attno);
        self.sma.get_or_build(attno, || {
            let t0 = std::time::Instant::now();
            let s = sma_build(bank, attno, &units);
            crate::coldledger::note("sma", format!("attno={attno}"), t0, (units.len() * 16) as u64, crate::coldledger::Reason::FlatStatsDerived);
            s
        })
    }

    /// [psma-consume] §8.2 PSMA face for one column, built once (lazy
    /// section faults, sealed = immutable). `None` under the kill switch
    /// or when NO part sealed a Psma section for the column — consumers
    /// skip the per-granule consult entirely then.
    pub fn psma(&self, bank: &Bank, attno: u32) -> Option<Arc<crate::psmaface::PsmaFace>> {
        if !self.cfg.psma {
            return None;
        }
        let f = self.psma.get_or_build(attno, || {
            let t0 = std::time::Instant::now();
            let f = crate::psmaface::PsmaFace::open(bank, attno, self.cfg.threads);
            crate::coldledger::note("psma", format!("attno={attno}"), t0, 0, crate::coldledger::Reason::TouchedByQuery);
            f
        });
        f.any().then_some(f)
    }

    pub fn dict(&self, bank: &Bank, pi: usize, attno: u32) -> Arc<DictFace> {
        // A whole-column vector already built serves the part directly.
        if let Some(v) = self.dict_cols.get(&attno) {
            return v[pi].clone();
        }
        self.dicts.get_or_build((pi, attno), || Self::build_dict(bank, pi, attno))
    }

    fn build_dict(bank: &Bank, pi: usize, attno: u32) -> DictFace {
        let t0 = std::time::Instant::now();
        if crate::scan::is_dict(bank, pi, attno) {
            let dh = crate::scan::dict_handle(bank, pi, attno);
            let n = dh.ncodes();
            // [cold2] empty_code from the INDEX ONLY (byte_len_only): the
            // whole-payload probe arm is deleted (risks.md §2). Same
            // fact: entry 0's bytes are empty iff byte_len == 0.
            let empty_code = if n > 0 {
                if dh.byte_len_only(0).expect("dict index") == 0 { Some(0) } else { None }
            } else {
                None
            };
            crate::coldledger::note("dict", format!("attno={attno}"), t0, n as u64, crate::coldledger::Reason::TouchedByQuery);
            DictFace { dh: Some(Arc::new(dh)), ncodes: n, empty_code }
        } else {
            crate::coldledger::note("dict", format!("attno={attno}|raw"), t0, 0, crate::coldledger::Reason::TouchedByQuery);
            DictFace { dh: None, ncodes: 0, empty_code: None }
        }
    }

    /// [coldstart] Part-parallel prewarm of a SET of parts' dict faces
    /// (the touched parts only — class (a)); memoized per part as usual.
    pub fn dicts_for(&self, bank: &Bank, attno: u32, parts: &[usize]) {
        if parts.len() <= 1 || self.dict_cols.get(&attno).is_some() {
            return;
        }
        let t0 = std::time::Instant::now();
        let _ = par_parts(self.cfg.threads, parts.len(), |i| {
            let pi = parts[i];
            self.dicts.get_or_build((pi, attno), || Self::build_dict(bank, pi, attno))
        });
        crate::coldledger::note("dict_set", format!("attno={attno}|parts={}", parts.len()), t0, 0, crate::coldledger::Reason::TouchedByQuery);
    }

    /// [coldstart] Every part's dict face for one column, built ONCE,
    /// part-parallel — replaces the stencils' serial prologue loops.
    pub fn dicts_all(&self, bank: &Bank, attno: u32) -> Arc<Vec<Arc<DictFace>>> {
        self.dict_cols.get_or_build(attno, || {
            let t0 = std::time::Instant::now();
            let v = par_parts(self.cfg.threads, bank.parts.len(), |pi| {
                self.dicts.get_or_build((pi, attno), || Self::build_dict(bank, pi, attno))
            });
            crate::coldledger::note("dict_col", format!("attno={attno}|parts={}", v.len()), t0, 0, crate::coldledger::Reason::TouchedByQuery);
            v
        })
    }

    /// [cold2 C7] Publish the dict PAYLOAD regions of a face set
    /// part-parallel, BEFORE a byte-folding consumer's wide loop touches
    /// them. Callers: the byte-folding face getters ONLY — sparse
    /// consumers (frame plans, two_level, gathers) must NOT prewarm
    /// (hot-shape 1.2 GB lesson).
    pub fn prewarm_payloads(&self, dfs: &[Arc<DictFace>]) {
        let t0 = std::time::Instant::now();
        let n: u64 = par_parts(self.cfg.threads, dfs.len(), |pi| {
            if let Some(dh) = dfs[pi].dh.as_ref() {
                if dfs[pi].ncodes > 0 {
                    // [stack] whole-payload publication face: on the
                    // block-lazy arm this is one CRC'd whole-extent read +
                    // decompress-all (a first-entry touch would fault ONE
                    // block and starve the dense fold that follows).
                    let _ = dh.prewarm_payload();
                    return 1u64;
                }
            }
            0u64
        })
        .into_iter()
        .sum();
        if n > 0 {
            crate::coldledger::note("dict_pay", format!("parts={n}"), t0, 0, crate::coldledger::Reason::TouchedByQuery);
        }
    }

    /// Dict-entry verdict bitmap face for a varlena predicate, keyed
    /// (attno, typed term fingerprint) — built once, entry-grain parallel,
    /// shared dict handles (the per-worker-opens arm was the measured-
    /// settled control; deleted). `term` is the predicate term the key
    /// fingerprint was minted from (`term.fp` is the key; the term itself
    /// is the oracle builds' structural verify currency — ruling Q4).
    pub fn verdict_words(
        &self,
        bank: &Bank,
        attno: u32,
        term: &crate::ir::VarPredTerm,
        t: usize,
        pred: impl Fn(&[u8]) -> bool + Sync,
    ) -> Arc<VerdictWords> {
        let fingerprint = &term.fp;
        {
            let m = self.verdict_words.lock().unwrap();
            if let Some(v) = m.get(&(attno, fingerprint.clone())) {
                #[cfg(feature = "oracle")]
                {
                    let sk = self.vw_skey.lock().unwrap();
                    let stored = sk
                        .get(&(attno, fingerprint.clone()))
                        .expect("oracle: verdict-word plane stored without its structural key");
                    assert!(
                        stored == term,
                        "sqe oracle: fingerprint collision or encoder omission on the \
                         verdict-word plane (attno {attno}, fp {fingerprint}):\n  \
                         cached term:  {stored:?}\n  current term: {term:?}"
                    );
                }
                return v.clone();
            }
        }
        // [ruling refined 1] condcache-class admission: the FIRST
        // execution in a fresh process computes the table and does NOT
        // store it; recurrence stores. Same discipline as the condcache
        // populate rung (cfg.populate).
        let store = {
            let n = {
                let key = (attno, fingerprint.clone());
                let mut g = self.vw_touch.lock().unwrap();
                bound_xquery_cache(&mut g, &key, self.cfg.xquery_cache_max_entries);
                let e = g.entry(key).or_insert(0);
                *e += 1;
                *e
            };
            match self.cfg.populate {
                PopulatePolicy::First => true,
                PopulatePolicy::Never => false,
                PopulatePolicy::Second => n >= 2,
            }
        };
        let t0 = std::time::Instant::now();
        let faces = self.dicts_all(bank, attno);
        let nc: Vec<u32> = faces.iter().map(|d| d.ncodes).collect();
        let mut vw = VerdictWords::from_ncodes(attno, &nc);
        self.prewarm_payloads(&faces);
        let dhs: Vec<Option<std::sync::Arc<pgrc2_read::dicthandle::DictHandle>>> =
            faces.iter().map(|d| d.dh.clone()).collect();
        vw.compute_with(&dhs, t, &pred);
        crate::coldledger::note(
            "verdict_words",
            format!("attno={attno}|fp={fingerprint}|store={store}"),
            t0,
            0,
            if store { crate::coldledger::Reason::SecondTouch } else { crate::coldledger::Reason::TouchedByQuery },
        );
        let vw = Arc::new(vw);
        if !store {
            return vw;
        }
        #[cfg(feature = "oracle")]
        {
            let mut sk = self.vw_skey.lock().unwrap();
            if let Some(stored) = sk.get(&(attno, fingerprint.clone())) {
                assert!(
                    stored == term,
                    "sqe oracle: fingerprint collision or encoder omission on the \
                     verdict-word plane at publish (attno {attno}, fp {fingerprint}):\n  \
                     cached term:  {stored:?}\n  current term: {term:?}"
                );
            } else {
                sk.insert((attno, fingerprint.clone()), term.clone());
            }
        }
        let key = (attno, fingerprint.clone());
        let mut g = self.verdict_words.lock().unwrap();
        bound_xquery_cache(&mut g, &key, self.cfg.xquery_cache_max_entries);
        g.entry(key).or_insert(vw).clone()
    }

    pub fn word_col(&self, bank: &Bank, pi: usize, attno: u32) -> Arc<Option<crate::fused::WordCol>> {
        self.word_cols.get_or_build((pi, attno), || {
            let t0 = std::time::Instant::now();
            let w = crate::fused::WordCol::open(bank, pi, attno);
            crate::coldledger::note("word_col", format!("attno={attno}"), t0, 0, crate::coldledger::Reason::TouchedByQuery);
            w
        })
    }

    pub fn cond_get(&self, fingerprint: &ConjFp) -> Option<Arc<PredCache>> {
        self.cond.lock().unwrap().get(fingerprint).cloned()
    }

    pub fn cond_put(&self, fingerprint: &ConjFp, pc: PredCache) -> Arc<PredCache> {
        let bytes: u64 = pc
            .v
            .iter()
            .map(|v| match v {
                CVerdict::Rows(r) => 2 * r.len() as u64,
                CVerdict::Bitmap(w) => 8 * w.len() as u64,
                _ => 0,
            })
            .sum();
        crate::coldledger::note("cond_put", format!("{fingerprint}"), std::time::Instant::now(), bytes, crate::coldledger::Reason::SecondTouch);
        // [oracle, ruling Q4] first-publish-wins would silently drop a
        // structurally different plane arriving under the same fp —
        // verify the incumbent before or_insert discards the newcomer.
        #[cfg(feature = "oracle")]
        if let Some(existing) = self.cond.lock().unwrap().get(fingerprint) {
            assert!(
                existing.skey == pc.skey,
                "sqe oracle: fingerprint collision or encoder omission on the \
                 condition cache at publish (fp {fingerprint}):\n  \
                 cached structure:  {:?}\n  current structure: {:?}",
                existing.skey,
                pc.skey
            );
        }
        let mut g = self.cond.lock().unwrap();
        bound_xquery_cache(&mut g, fingerprint, self.cfg.xquery_cache_max_entries);
        g.entry(fingerprint.clone())
            .or_insert_with(|| Arc::new(pc))
            .clone()
    }

    /// Recurrence witness bump for one fingerprint; returns the execution
    /// count including this one.
    pub fn touch_bump(&self, fingerprint: &ConjFp) -> u32 {
        let mut g = self.touch.lock().unwrap();
        bound_xquery_cache(&mut g, fingerprint, self.cfg.xquery_cache_max_entries);
        let e = g.entry(fingerprint.clone()).or_insert(0);
        *e += 1;
        *e
    }

    /// [sqe-q2426] Read-only recurrence count (0 = never touched). Lets a
    /// warm-path publication attempt disarm after repeated refusals (a
    /// density-refused plane never admits — re-collecting its payload
    /// every hot execution is pure waste).
    pub fn touch_count(&self, fingerprint: &ConjFp) -> u32 {
        self.touch.lock().unwrap().get(fingerprint).copied().unwrap_or(0)
    }

    /// Text registry face (famA): built once per (attno, kind) with the
    /// study's own builders, so gid assignment is bit-identical with the
    /// handwritten oracles' registries.
    pub fn textreg(
        &self,
        bank: &Bank,
        pool: &Pool,
        attno: u32,
        kind: RegKind,
    ) -> Arc<crate::grouped::TextReg> {
        let k = (attno, kind as u8);
        {
            let m = self.regs.lock().unwrap();
            if let Some(r) = m.get(&k) {
                return r.clone();
            }
        }
        let t0 = std::time::Instant::now();
        let r = Arc::new(match kind {
            RegKind::Par => crate::grouped::TextReg::build_par(bank, attno, pool),
            RegKind::Merge => crate::grouped::TextReg::build_merge(bank, attno, pool),
        });
        crate::coldledger::note(
            "textreg",
            format!("attno={attno}|kind={}|gids={}", if kind == RegKind::Par { "par" } else { "merge" }, r.ngids()),
            t0,
            0,
            crate::coldledger::Reason::None,
        );
        self.regs.lock().unwrap().entry(k).or_insert(r).clone()
    }

    /// The cache as a queryable PROPERTY (PLANNER-SPEC §3.3).
    pub fn verdicts_available(&self, fingerprint: &ConjFp) -> bool {
        self.cond.lock().unwrap().contains_key(fingerprint)
    }

    /// [fpcombine] fp-plane replay lookup (cfg.fpcache arm only; callers
    /// shape-verify against the current faces).
    pub(crate) fn fp_plane_get(&self, attno: u32) -> Option<Arc<Vec<Vec<u128>>>> {
        self.fp_planes.lock().unwrap().get(&attno).cloned()
    }

    /// [fpcombine] second-touch witness: false on the first execution for
    /// `attno` in this process (compute, don't store), true on recurrence.
    pub(crate) fn fp_plane_touch(&self, attno: u32) -> bool {
        let mut t = self.fp_touch.lock().unwrap();
        if t.contains(&attno) {
            true
        } else {
            t.push(attno);
            false
        }
    }

    pub(crate) fn fp_plane_put(&self, attno: u32, v: Arc<Vec<Vec<u128>>>) {
        self.fp_planes.lock().unwrap().insert(attno, v);
    }
}

/// Per-query-run reset of every derived plane except the condition cache
/// (the ruling, structural where possible: the Faces planes clear here).
/// The whole-state per-query persistence maps are GONE (persist-rehome
/// lane, scratch-depot.md §6): pass-1 state now parks capacity-only in
/// capped query-agnostic StateParks + the worker scratch depot, so the
/// clears below are the small derived-election memos only.
pub fn reset_per_query(faces: &Faces) {
    // [claim-horizon] query boundary: banks whose cold window ended leave
    // observation; all-warm disables the observer (one relaxed atomic per
    // extent access remains).
    crate::horizon::on_query_boundary();
    faces.clear_derived();
    crate::planner::clear_run_witness_memo();
    // Per-query-run derived-election memos (eagerly cleared here;
    // code_agg's prep memo purges by generation at its consult).
    crate::stencils::survivor_gather::clear_memo();
    crate::stencils::two_level::clear_fg_memo();
    crate::stencils::code_agg::clear_prep();
}

/// The engine context: bank + resident pool + this relation's standing
/// faces. One per bank open; stencil entry points take (ctx, node).
pub struct SqeCtx<'b> {
    pub bank: &'b Bank,
    pub pool: &'b Pool,
    pub faces: &'b Faces,
}

// ---------------------------------------------------------------------------
// [sqe-m2] phase timers — the study's per-phase discipline. OFF by default
// (prints land inside timed regions); enabled by the rig's `--phase` flag
// on profiling pushes only, never on gate runs.
// ---------------------------------------------------------------------------

pub static PHASE_ON: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[inline]
pub fn phase_on() -> bool {
    // Server-grain profiling arm: the rig's --phase flag has no reach
    // into a live backend, so the env var arms the same timers there.
    // Profiling pushes only, never gate runs (prints land in timed
    // regions); default OFF, one relaxed load + a OnceLock read hot.
    static ENV: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENV.get_or_init(|| std::env::var("PGRUST_SQE_PHASE").is_ok())
        || PHASE_ON.load(std::sync::atomic::Ordering::Relaxed)
}

/// Print one phase split (ms since t0) when profiling is armed.
#[inline]
pub fn ph(q: u32, name: &str, t0: std::time::Instant) {
    if phase_on() {
        println!("SQEPHASE|q={q}|{name}|ms={:.3}", t0.elapsed().as_secs_f64() * 1e3);
    }
}

/// Arm-tagged phase split: honest-arm phases get a `_h` suffix so the
/// per-arm attribution is separable in the log.
#[inline]
pub fn phn(node: &crate::ir::PlanNode, name: &str, t0: std::time::Instant) {
    if phase_on() {
        let h = if node.params.flags & crate::ir::F_HONEST != 0 { "_h" } else { "" };
        println!(
            "SQEPHASE|q={}|{name}{h}|ms={:.3}",
            node.q,
            t0.elapsed().as_secs_f64() * 1e3
        );
    }
}

/// An owned engine handle over one open relation: the thing the AM bridge
/// constructs at P2-1; the rig and tests construct it directly.
pub struct Engine {
    pub bank: Bank,
    pub pool: Pool,
    pub faces: Faces,
}

impl Engine {
    pub fn new(bank: Bank, cfg: SqeConfig) -> Engine {
        // [claim-horizon] register the part set with the prefetch engine
        // (no-op when disarmed or empty; residency-only — never a
        // correctness face).
        let t0 = std::time::Instant::now();
        if cfg.horizon {
            crate::horizon::engage(&bank.parts);
        }
        let pool = Pool::new(cfg.threads);
        // [coldopen] Pool spawn + horizon engage attributed: the last
        // once-per-open span between open_schema and the first build.
        crate::coldledger::note(
            "open_pool",
            format!("threads={}", cfg.threads),
            t0,
            0,
            crate::coldledger::Reason::TouchedByQuery,
        );
        let faces = Faces::new(cfg);
        Engine { bank, pool, faces }
    }
    pub fn ctx(&self) -> SqeCtx<'_> {
        SqeCtx { bank: &self.bank, pool: &self.pool, faces: &self.faces }
    }
    pub fn run(&self, node: &crate::ir::PlanNode) -> AnswerSet {
        crate::exec::run_node(&self.ctx(), node)
    }
}

#[cfg(test)]
mod xquery_bound_tests {
    //! [xquery-bound, CWE-770] The cross-query condition/frame caches on the
    //! process-global per-relation `Faces` are keyed by attacker-chosen query
    //! constants. These tests pin the engine-level bounds that keep their
    //! resident memory O(cap) no matter how many distinct constants a client
    //! issues, and that eviction is clear-on-pressure (recomputable on miss).
    use super::*;

    #[test]
    fn hashmap_cache_plateaus_at_cap() {
        // Simulate an attacker streaming an unbounded number of distinct
        // fingerprints into a cross-query HashMap cache: with the bound the
        // resident entry count never exceeds `cap`.
        let cap = 8usize;
        let mut m: HashMap<u64, u64> = HashMap::new();
        for k in 0..10_000u64 {
            bound_xquery_cache(&mut m, &k, cap);
            m.insert(k, k);
            assert!(m.len() <= cap, "resident entries {} exceeded cap {}", m.len(), cap);
        }
        assert!(m.len() <= cap);
    }

    #[test]
    fn hashmap_cache_no_evict_on_existing_key() {
        // Re-touching a resident key must never trigger a clear (a hit is not
        // pressure): a full cache that keeps seeing the same keys stays warm.
        let cap = 4usize;
        let mut m: HashMap<u64, u64> = HashMap::new();
        for k in 0..cap as u64 {
            bound_xquery_cache(&mut m, &k, cap);
            m.insert(k, k);
        }
        assert_eq!(m.len(), cap);
        for _ in 0..1000 {
            let k = 2u64; // already resident
            bound_xquery_cache(&mut m, &k, cap);
            m.insert(k, k);
            assert_eq!(m.len(), cap, "hit on a resident key must not evict");
        }
    }

    #[test]
    fn hashmap_cache_cap_zero_disables_bound() {
        // cap == 0 is the opt-out (env PGRUST_SQE_XQUERY_CACHE_MAX=0):
        // behavior identical to the pre-bound unbounded map.
        let mut m: HashMap<u64, u64> = HashMap::new();
        for k in 0..1000u64 {
            bound_xquery_cache(&mut m, &k, 0);
            m.insert(k, k);
        }
        assert_eq!(m.len(), 1000);
    }

    #[test]
    fn memo_bounded_plateaus_and_recomputes() {
        // The `frames`-class memo bound: resident keys plateau at `cap`, and a
        // key evicted under pressure is rebuilt (a miss just recomputes — the
        // correctness invariant of the whole cache class).
        let cap = 4usize;
        let memo: Memo<u64, u64> = Memo::default();
        for k in 0..1000u64 {
            let v = memo.get_or_build_bounded(k, cap, || k * 10);
            assert_eq!(*v, k * 10);
            assert!(memo.len() <= cap, "resident memo entries {} exceeded cap {}", memo.len(), cap);
        }
        // Key 0 was long since evicted; requesting it recomputes correctly.
        let rebuilt = memo.get_or_build_bounded(0, cap, || 0u64);
        assert_eq!(*rebuilt, 0);
    }
}
