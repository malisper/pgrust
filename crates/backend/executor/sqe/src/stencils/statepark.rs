//! Capped state park [persist-rehome lane] — the PARK32 class made
//! lawful. The scratch-depot doc's documented follow-ups (hash_plane
//! site 4's PERSISTQ, survivor_gather's PERSIST slots, code_agg's
//! caller-TLS ARENA) each kept WHOLE per-worker pass-1 states in an
//! UNBOUNDED per-query static: contents were truncated per call (the
//! honest law) but the allocations survived every statement, outside
//! any memory law, and two of the three parked `CurCache`/`FcCache`
//! cursors across statements — the distinct.rs wrong-answer class
//! (a cursor keyed only by part index resurfacing another bank's or
//! column's stream).
//!
//! This park is the rehome target. Laws:
//! - **query-agnostic**: no query key. A fetched state is capacity
//!   only — every caller truncates/arms sizes before use (exactly what
//!   the per-query maps already did), so right-sized reuse comes from
//!   the arm, not the key.
//! - **capped (shrink law)**: park re-admits under a per-park byte cap;
//!   overflow drops eagerly. A park that once served a huge shape does
//!   not hold that high-water mark forever.
//! - **plain heap only**: NEVER park cursors (`CurCache`/`FcCache`) —
//!   parking a cursor pins bank mmaps across statements and can
//!   resurface another bank's (or another column's) stream. Callers
//!   drop cursors and park data buffers only.
//! - `Scratch` decode arenas do NOT ride here either — they ride the
//!   worker scratch depot (`scan::scratch_fetch`/`scratch_park`), which
//!   owns their reset law.
//!
//! [sqe-park-knobs 2026-08-18, MEASUREMENT BRANCH — HELD] The allocator
//! trade-study lane's lever surface. Under mimalloc (production
//! allocator, purge_delay 10ms) every fresh execution at large grain
//! re-commits/re-faults the over-cap pass-1/pass-2 states the shrink
//! law drops (~35ms/execute on the CI cluster q32 shape). Levers, all
//! env-gated so the grid runs without rebuilds (NOTE: the crate charter
//! says the engine reads no env — the landing threads these through
//! SqeConfig/GUCs; env is the measurement-branch form):
//! - `PGRUST_SQE_PARK32_MB` / `PGRUST_SQE_PARKOA_MB`: per-park byte-cap
//!   override in MB (parks constructed by [`StatePark::new_env`]).
//! - `PGRUST_SQE_OVERCAP` = `retain` (RULED default 2026-08-21; r14 tax matrix) | `drop` (the former shrink law) |
//!   `retain` (admit over-cap states, committed) | `advise` (admit
//!   over-cap states and `MADV_FREE` their data pages — RSS is
//!   kernel-reclaimable under pressure, refault-free reuse when
//!   pressure is absent; macOS purge-native behavior, Linux lazy free) |
//!   `decay` (admit over-cap states with a drop deadline — the
//!   recycled-pool shape of the allocator ruling's chartered follow-on,
//!   window `PGRUST_SQE_OVERCAP_DECAY_MS`, default 10000 = the ruled
//!   jemalloc `dirty_decay_ms` posture of
//!   docs/design/allocator-decision-2026-08-13.md).
//!   `PGRUST_SQE_BIGARENA=mmap|madv` is an alias for `advise`.
//! - `PGRUST_SQE_OVERCAP_BUDGET_MB`: byte budget of the over-cap tier
//!   (default 8192; over-budget states drop as today).
//! - `PGRUST_SQE_PREFAULT=1` (advise mode only; Michael's literature-pass
//!   added arm, 2026-08-18): on FETCH of an advised over-cap state,
//!   pre-fault its plain-data pages with `MADV_POPULATE_WRITE`
//!   (Linux 5.14+) before the fold loops — the ClickHouse hash-arena
//!   pattern (one syscall batches the post-reclaim fault storm; zeroing
//!   cost remains but leaves the timed hot loop). Warm path is a cheap
//!   page-table walk (pages already resident). No-op on macOS (no
//!   MADV_POPULATE_WRITE) and everywhere when the madvise is refused.
//!
//! Advised states obey the same capacity-only contract: `MADV_FREE`d
//! pages may be zero-filled by the kernel before reuse, so a fetched
//! state's CONTENTS are garbage by law (they already were) — only
//! caller-armed sizes are meaningful. Advisor closures must therefore
//! only ever advise plain-data pages (scalar Vec buffers), never pages
//! holding live Vec headers or any pointer-bearing struct.

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OvercapMode {
    Drop,
    Retain,
    Advise,
    Decay,
}

#[derive(Clone, Copy, Debug)]
struct Cfg {
    cap: usize,
    mode: OvercapMode,
    decay: Duration,
    budget: usize,
    /// advise+prefault arm: MADV_POPULATE_WRITE advised states on fetch
    prefault: bool,
}

struct Entry<T> {
    item: T,
    bytes: usize,
    /// admitted above the shrink-law cap (retain/advise/decay tier)
    overcap: bool,
    /// decay-tier drop deadline
    expiry: Option<Instant>,
}

/// A capped LIFO park of per-worker state. `const`-constructible so it
/// can live in a `static` at the stencil that owns the state type.
pub struct StatePark<T> {
    v: Mutex<Vec<Entry<T>>>,
    default_cap: usize,
    cap_env: Option<&'static str>,
    cfg: OnceLock<Cfg>,
    /// [q30-regress] width-priced shrink-law floor: the effective cap is
    /// `max(cap, floor)`. A flat byte cap under-admits exactly when the
    /// pool is wide — per-worker pass-1 states scale with width, so a
    /// park whose purpose is to retain `t` states must be allowed to
    /// hold `t` average states. Armed by the owning stencil per
    /// engagement (`arm_floor`), monotonic within a boot, bounded by
    /// `t x per-state budget` (t <= pool threads).
    floor: std::sync::atomic::AtomicUsize,
}

fn env_mb(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse::<usize>().ok().map(|mb| mb << 20)
}

impl<T> StatePark<T> {
    pub const fn new(cap_bytes: usize) -> StatePark<T> {
        StatePark {
            v: Mutex::new(Vec::new()),
            default_cap: cap_bytes,
            cap_env: None,
            cfg: OnceLock::new(),
            floor: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// A park whose byte cap is overridable via `env` (value in MB).
    pub const fn new_env(cap_bytes: usize, env: &'static str) -> StatePark<T> {
        StatePark {
            v: Mutex::new(Vec::new()),
            default_cap: cap_bytes,
            cap_env: Some(env),
            cfg: OnceLock::new(),
            floor: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn cfg(&self) -> Cfg {
        *self.cfg.get_or_init(|| {
            let cap = self
                .cap_env
                .and_then(env_mb)
                .unwrap_or(self.default_cap);
            // RULED default = retain (Michael 2026-08-21): the r14 tax matrix
            // measured drop's re-fault at ~12% scored geomean across the
            // corpus (1.0636 -> 0.9452); retained residency is bounded
            // (arena plateau, memwatchdog armed). `drop` stays one env away.
            let mode = match std::env::var("PGRUST_SQE_OVERCAP").as_deref() {
                Ok("retain") | Err(_) => OvercapMode::Retain,
                Ok("advise") => OvercapMode::Advise,
                Ok("decay") => OvercapMode::Decay,
                Ok("drop") => {
                    match std::env::var("PGRUST_SQE_BIGARENA").as_deref() {
                        Ok("mmap") | Ok("madv") | Ok("1") => OvercapMode::Advise,
                        _ => OvercapMode::Drop,
                    }
                }
                Ok(_) => OvercapMode::Retain,
            };
            let decay = Duration::from_millis(
                std::env::var("PGRUST_SQE_OVERCAP_DECAY_MS")
                    .ok()
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(10_000),
            );
            let budget = env_mb("PGRUST_SQE_OVERCAP_BUDGET_MB").unwrap_or(8192 << 20);
            let prefault = matches!(
                std::env::var("PGRUST_SQE_PREFAULT").as_deref(),
                Ok("1") | Ok("true")
            );
            Cfg { cap, mode, decay, budget, prefault }
        })
    }

    /// Test-only: pin this park's `Cfg` directly, bypassing the env
    /// read. Tests that pin a specific overcap law (drop/advise/retain)
    /// MUST use this instead of `std::env::set_var` — the process env
    /// is shared across parallel tests and the per-park `OnceLock`
    /// makes the first reader's snapshot sticky, so env-driven pins are
    /// order-dependent (the exact flake CI-ci caught on the drop-law
    /// pins after the ruled default moved to retain, 2026-08-21).
    #[cfg(test)]
    fn cfg_force(&self, mode: OvercapMode, prefault: bool) {
        let cfg = Cfg {
            cap: self.default_cap,
            mode,
            decay: Duration::from_millis(10_000),
            budget: 8192 << 20,
            prefault,
        };
        assert!(
            self.cfg.set(cfg).is_ok(),
            "cfg_force must run before the park's first use"
        );
    }

    /// [q30-regress] Raise the width-priced shrink-law floor (monotonic;
    /// see the `floor` field). Callers price it `t x per-state budget`.
    pub fn arm_floor(&self, floor_bytes: usize) {
        self.floor
            .fetch_max(floor_bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Pop a parked state (capacity only — arm sizes before use).
    pub fn fetch(&self) -> Option<T> {
        self.v.lock().unwrap().pop().map(|e| e.item)
    }

    /// Pop a parked state; under the advise+prefault arm
    /// (`PGRUST_SQE_OVERCAP=advise` + `PGRUST_SQE_PREFAULT=1`),
    /// `populate` is called on advised over-cap states BEFORE the caller's
    /// fold loops — `MADV_POPULATE_WRITE` the same plain-data pages the
    /// park advisor `MADV_FREE`d (and ONLY those pages). Contents remain
    /// garbage by law; only fault timing moves (out of the hot loop).
    pub fn fetch_with(&self, populate: impl Fn(&T)) -> Option<T> {
        let e = self.v.lock().unwrap().pop()?;
        let cfg = self.cfg();
        if e.overcap && cfg.mode == OvercapMode::Advise && cfg.prefault {
            populate(&e.item);
        }
        Some(e.item)
    }

    /// Pop up to `n` parked states.
    pub fn fetch_up_to(&self, n: usize) -> Vec<T> {
        let mut v = self.v.lock().unwrap();
        let take = n.min(v.len());
        let at = v.len() - take;
        v.split_off(at).into_iter().map(|e| e.item).collect()
    }

    /// Park `item` whose heap footprint is `bytes`: admitted iff the
    /// park stays under its byte cap, else dropped eagerly (shrink law).
    /// States with no plain-data advisor take the drop path on over-cap
    /// under `advise` exactly as under `drop` — see [`Self::park_with`].
    pub fn park(&self, item: T, bytes: usize) {
        self.park_inner(item, bytes, None::<fn(&T)>);
    }

    /// Park with a lazy-release advisor: on over-cap admission under
    /// `PGRUST_SQE_OVERCAP=advise`, `advise` is called to `MADV_FREE`
    /// the state's plain-data pages (and ONLY plain-data pages).
    pub fn park_with(&self, item: T, bytes: usize, advise: impl Fn(&T)) {
        self.park_inner(item, bytes, Some(advise));
    }

    fn park_inner(&self, item: T, bytes: usize, advise: Option<impl Fn(&T)>) {
        let cfg = self.cfg();
        let now = Instant::now();
        // Reap expired decay-tier states. Drops run under the lock —
        // acceptable at park cadence (end of an execute), and the
        // measurement the lane wants (the drop cost rides the execute
        // that crossed the deadline, exactly like an allocator decay
        // pass riding an allocation path; the pgalloc posture moves it
        // to a background thread — the landing shape).
        let mut expired: Vec<Entry<T>> = Vec::new();
        {
            let mut v = self.v.lock().unwrap();
            let mut i = 0;
            while i < v.len() {
                if v[i].expiry.is_some_and(|x| x <= now) {
                    expired.push(v.swap_remove(i));
                } else {
                    i += 1;
                }
            }
            let cap = cfg
                .cap
                .max(self.floor.load(std::sync::atomic::Ordering::Relaxed));
            let held: usize =
                v.iter().filter(|e| !e.overcap).map(|e| e.bytes).sum();
            if held + bytes <= cap {
                v.push(Entry { item, bytes, overcap: false, expiry: None });
                return;
            }
            // Over the shrink-law cap: the lever surface.
            match cfg.mode {
                OvercapMode::Drop => {}
                OvercapMode::Advise if advise.is_none() => {}
                OvercapMode::Retain | OvercapMode::Advise | OvercapMode::Decay => {
                    let over: usize =
                        v.iter().filter(|e| e.overcap).map(|e| e.bytes).sum();
                    if over + bytes <= cfg.budget {
                        if cfg.mode == OvercapMode::Advise {
                            advise.as_ref().unwrap()(&item);
                        }
                        let expiry = (cfg.mode == OvercapMode::Decay)
                            .then(|| now + cfg.decay);
                        v.push(Entry { item, bytes, overcap: true, expiry });
                        return;
                    }
                }
            }
            // fell through: drop `item` outside the lock
            drop(v);
            drop(item);
        }
        drop(expired);
    }

    /// (parked count, parked bytes) — the census instrument.
    pub fn stats(&self) -> (usize, usize) {
        let v = self.v.lock().unwrap();
        (v.len(), v.iter().map(|e| e.bytes).sum())
    }
}

/// Heap footprint helpers for the common parked shapes.
pub fn vec_bytes<T>(v: &Vec<T>) -> usize {
    v.capacity() * std::mem::size_of::<T>()
}

pub fn nested_bytes<T>(v: &[Vec<T>]) -> usize {
    std::mem::size_of_val(v) + v.iter().map(vec_bytes).sum::<usize>()
}

/// Lazy release of PLAIN-DATA buffers: `MADV_FREE` the page-aligned
/// interior of a scalar Vec's allocation. The kernel may reclaim (and
/// later zero-fill) the pages under pressure; re-dirtying un-frees them
/// with no syscall. NEVER call on a buffer holding Vec headers or any
/// pointer-bearing type — kernel zeroing would forge null pointers.
pub mod lazyfree {
    #[cfg(unix)]
    extern "C" {
        fn madvise(addr: *mut core::ffi::c_void, len: usize, advice: i32) -> i32;
        fn getpagesize() -> i32;
    }

    /// Darwin: plain MADV_FREE (5) leaves the pages in phys_footprint
    /// until reclaim; MADV_FREE_REUSABLE (7) moves them to the
    /// "reusable" class immediately (excluded from footprint, the
    /// Linux-decommit-comparable accounting). We advise REUSABLE and
    /// fall back to MADV_FREE where the kernel refuses it. NOTE
    /// (prototype): the matching MADV_FREE_REUSE on refetch is not
    /// issued — contents/behavior are unaffected (re-dirty un-frees),
    /// but Darwin's ledger can under-count the region after reuse; the
    /// landing pairs the calls.
    #[cfg(target_os = "macos")]
    const MADV_LAZY: i32 = 7; // MADV_FREE_REUSABLE
    #[cfg(target_os = "macos")]
    const MADV_LAZY_FALLBACK: i32 = 5; // MADV_FREE
    #[cfg(target_os = "linux")]
    const MADV_LAZY: i32 = 8; // MADV_FREE
    #[cfg(target_os = "linux")]
    const MADV_LAZY_FALLBACK: i32 = 8;

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    pub fn advise_bytes(ptr: *const u8, len: usize) {
        let pg = unsafe { getpagesize() } as usize;
        let start = (ptr as usize).next_multiple_of(pg);
        let end = (ptr as usize + len) & !(pg - 1);
        if end > start {
            // SAFETY: [start, end) lies strictly inside a live
            // allocation we own (page-aligned inward), and the parked
            // contents are garbage by the park's capacity-only law.
            unsafe {
                let p = start as *mut core::ffi::c_void;
                if madvise(p, end - start, MADV_LAZY) != 0 {
                    madvise(p, end - start, MADV_LAZY_FALLBACK);
                }
            }
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    pub fn advise_bytes(_ptr: *const u8, _len: usize) {}

    /// Pre-fault (advise+prefault arm): `MADV_POPULATE_WRITE` the same
    /// page-aligned interior [`advise_bytes`] lazily freed, batching the
    /// post-reclaim fault storm into one syscall before the fold loops
    /// (the ClickHouse hash-arena pattern). Linux 5.14+ only; silently a
    /// no-op where the kernel refuses the advice (the arm then measures
    /// as plain advise). Kernel-side page-table population only — no
    /// store is issued through `ptr`, so `&`-aliased plain-data buffers
    /// are fine (their contents are garbage by the park's law anyway).
    #[cfg(target_os = "linux")]
    pub fn populate_bytes(ptr: *const u8, len: usize) {
        const MADV_POPULATE_WRITE: i32 = 23;
        let pg = unsafe { getpagesize() } as usize;
        let start = (ptr as usize).next_multiple_of(pg);
        let end = (ptr as usize + len) & !(pg - 1);
        if end > start {
            // SAFETY: same interior contract as advise_bytes.
            unsafe {
                madvise(start as *mut core::ffi::c_void, end - start, MADV_POPULATE_WRITE);
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn populate_bytes(_ptr: *const u8, _len: usize) {}

    /// Advise a scalar Vec's data pages (plain data only).
    pub fn advise_vec<T: Copy>(v: &Vec<T>) {
        advise_bytes(v.as_ptr().cast(), v.capacity() * std::mem::size_of::<T>());
    }

    /// Pre-fault a scalar Vec's data pages (plain data only).
    pub fn populate_vec<T: Copy>(v: &Vec<T>) {
        populate_bytes(v.as_ptr().cast(), v.capacity() * std::mem::size_of::<T>());
    }

    /// Pre-fault every inner buffer of a nested Vec (outer headers never).
    pub fn populate_nested<T: Copy>(v: &Vec<Vec<T>>) {
        for b in v {
            populate_vec(b);
        }
    }

    /// Advise every inner buffer of a nested Vec. The OUTER buffer holds
    /// live Vec headers and is never advised.
    pub fn advise_nested<T: Copy>(v: &Vec<Vec<T>>) {
        for b in v {
            advise_vec(b);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shrink law (`PGRUST_SQE_OVERCAP=drop` arm — no longer the
    /// ruled default, which is retain since 2026-08-21; pinned here via
    /// direct Cfg so the drop law itself stays tested): a park never
    /// holds more than its byte cap; overflow drops eagerly instead of
    /// growing the high-water mark.
    #[test]
    fn park_respects_byte_cap() {
        let p: StatePark<Vec<u8>> = StatePark::new(100);
        p.cfg_force(OvercapMode::Drop, false);
        for _ in 0..10 {
            p.park(vec![0u8; 30], 30);
        }
        let (n, bytes) = p.stats();
        assert_eq!(n, 3, "cap admits exactly floor(100/30) items");
        assert!(bytes <= 100, "park over cap: {bytes}");
        // Fetch drains; freed budget re-admits.
        assert!(p.fetch().is_some());
        p.park(vec![0u8; 30], 30);
        assert_eq!(p.stats().0, 3);
    }

    /// Retain-arm twin of `park_respects_byte_cap` (the RULED default,
    /// Michael 2026-08-21): overflow past the shrink-law cap is not
    /// dropped — it is admitted into the over-cap tier, bounded by the
    /// over-cap budget, and drains through the same fetch path.
    #[test]
    fn park_retains_over_cap_under_retain() {
        let p: StatePark<Vec<u8>> = StatePark::new(100);
        p.cfg_force(OvercapMode::Retain, false);
        for _ in 0..10 {
            p.park(vec![0u8; 30], 30);
        }
        let (n, bytes) = p.stats();
        assert_eq!(n, 10, "retain admits over-cap states (budget tier)");
        assert_eq!(bytes, 300);
        for _ in 0..10 {
            assert!(p.fetch().is_some());
        }
        assert!(p.fetch().is_none());
    }

    /// The ruled default with no env set is retain — over-cap states
    /// are admitted. (No test in this module touches the process env,
    /// so this read is race-free; the drop law is one env away and is
    /// pinned above via direct Cfg.)
    #[test]
    fn default_overcap_mode_is_retain() {
        let p: StatePark<Vec<u8>> = StatePark::new(64);
        assert_eq!(p.cfg().mode, OvercapMode::Retain);
        p.park(vec![0u8; 65], 65);
        assert_eq!(p.stats(), (1, 65), "default retains an over-cap state");
    }

    /// An oversized single state is refused outright under the drop
    /// law — the park cannot pin one giant shape forever. (Drop arm
    /// pinned via direct Cfg; under the ruled retain default the same
    /// park would admit it into the budget tier — see
    /// `default_overcap_mode_is_retain`.)
    #[test]
    fn oversized_state_refused() {
        let p: StatePark<Vec<u8>> = StatePark::new(64);
        p.cfg_force(OvercapMode::Drop, false);
        p.park(vec![0u8; 65], 65);
        assert_eq!(p.stats(), (0, 0));
    }

    #[test]
    fn fetch_up_to_takes_lifo_tail() {
        let p: StatePark<u32> = StatePark::new(1 << 20);
        for i in 0..5u32 {
            p.park(i, 4);
        }
        let got = p.fetch_up_to(3);
        assert_eq!(got.len(), 3);
        assert_eq!(p.stats().0, 2);
        assert_eq!(p.fetch_up_to(10).len(), 2);
        assert!(p.fetch().is_none());
    }

    /// advise mode admits over-cap states with an advisor and their
    /// bytes ride the over-cap budget, not the shrink-law cap.
    #[test]
    fn advise_lane_smoke() {
        let p: StatePark<Vec<u64>> = StatePark::new(64);
        p.cfg_force(OvercapMode::Advise, false);
        p.park_with(vec![7u64; 4096], 32768, |v| lazyfree::advise_vec(v));
        assert_eq!(p.stats().0, 1, "over-cap state admitted under advise");
        let got = p.fetch().unwrap();
        assert!(got.capacity() >= 4096, "capacity-only contract survives");
    }

    /// advise+prefault arm: fetch_with calls the populate closure on an
    /// advised over-cap state (and only then) before handing it back.
    #[test]
    fn prefault_lane_smoke() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static HITS: AtomicUsize = AtomicUsize::new(0);
        let p: StatePark<Vec<u64>> = StatePark::new(64);
        p.cfg_force(OvercapMode::Advise, true);
        // under-cap state: never populated
        p.park_with(vec![1u64; 4], 32, |v| lazyfree::advise_vec(v));
        let _ = p.fetch_with(|_| {
            HITS.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(HITS.load(Ordering::Relaxed), 0, "under-cap never populated");
        // over-cap advised state: populated exactly once on fetch
        p.park_with(vec![7u64; 4096], 32768, |v| lazyfree::advise_vec(v));
        let got = p
            .fetch_with(|v| {
                HITS.fetch_add(1, Ordering::Relaxed);
                lazyfree::populate_vec(v);
            })
            .unwrap();
        assert_eq!(HITS.load(Ordering::Relaxed), 1, "advised state populated on fetch");
        assert!(got.capacity() >= 4096, "capacity-only contract survives");
    }
}
