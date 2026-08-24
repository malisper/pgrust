//! Claim-horizon prefetch — demand-paced REAL reads ahead of the scan
//! frontier.
//!
//! Bulk advisory readahead (`fadvise(WILLNEED)`) measured NEGATIVE on the
//! reference volume class: advisory readahead trickles at queue depth ~1
//! and queues AHEAD of the workers' demand preads, while demand reads run
//! well below the device's sequential bound. This module closes that gap
//! with REAL preads into the residency plane the claim path already
//! consults (`OpenPart` segs/unwrapped caches), paced by the claim
//! frontier:
//!
//! - Scans are morsel-driven; claims are granule-grain and ascend, so a
//!   stream's extent accesses ascend too. Every demand access (hit or
//!   miss) is observed through the shared crate's stream-fault observer;
//!   the observed frontier plus a bounded lead is the prefetch target for
//!   that stream.
//! - Prefetched extents are inserted through
//!   `OpenPart::prefetch_extent_run_holes` — the SAME insert-if-absent
//!   caches, CRC-validated, wrapped extents rebuilt with the same
//!   unwrapper adjudication the cursor uses. A claim that finds its
//!   extent resident proceeds; one that doesn't demand-reads exactly as
//!   today. Byte-identical answers by construction (R1/R2: this is a
//!   residency face, never a correctness face — no estimate here feeds a
//!   gate).
//! - Drain design (the ruling of record): workers are the only threads.
//!   Every non-resident access is the worker asking the scheduler for
//!   work — it first fetches coalesced ranges for the frontier window
//!   while the balancing rule says retrieval is behind (keep ~2x what the
//!   current fetchers pull outstanding), then falls through to its own
//!   demand read. Resident accesses never detour. No issuer pool, no
//!   spawned threads in the backend.
//! - Budget: prefetch touches ONLY extents of streams the query is
//!   already demand-reading, at most the configured lead per stream
//!   (byte-run region streams: the remainder of the region the demand
//!   path is already committed to reading whole; frame-cut dict payload
//!   streams: a bounded frame lead — the frontier gate). Peak residency
//!   ≈ today's end-of-scan residency; the lead is bounded.
//! - Cold-window gating: the observer is process-global and enabled only
//!   while some engaged bank is in its cold window (from engage until its
//!   first completed query run). Warm banks run today's exact path plus
//!   one relaxed atomic load per extent access.
//!
//! Env (SqeConfig-precedent construction-time reads, resolved once):
//! `PGRUST_SQE_HORIZON` = granule lead per stream; unset/`0`/`off` = OFF
//! (the default posture — see `horizon()`); `1`/`on` = the recipe lead.
//! Tuning:
//! `PGRUST_SQE_HORIZON_{DICT_H, DICT_FRONTIER, COALESCE_MB, HOLE_KB,
//! BUDGET_MB, FETCH_MB, FETCH_ROUNDS, PART_CHAIN, ADAPTIVE,
//! ADAPT_MIN_KB, ADAPT_MAX_MB, COOLING, WITNESS}`.

use pgrc2_format::enc::Wrapper;
use pgrc2_format::part::{ExtentRecord, StreamSectionHdr};
use pgrc2_read::openpart::{OpenPart, StreamFaultEvent};
use pgrc2_read::streams::ParsedStream;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

/// Default granule lead when armed without an explicit value. Provenance:
/// the reference lane's recommended recipe (the cells of record were cut
/// with this lead, worker-cooperative drain, part_chain 2, adaptive
/// byte-distance).
const DEFAULT_H: u64 = 768;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_flag(name: &str, default: bool) -> bool {
    match std::env::var(name).as_deref() {
        Ok("0") | Ok("off") => false,
        Ok(_) => true,
        Err(_) => default,
    }
}

/// The armed lead in granules; 0 = OFF. `PGRUST_SQE_HORIZON`:
/// UNSET / `0` / `off` = OFF (the default posture — the port's local
/// verification could not drop the OS page cache, so the reference
/// lane's cold win is not locally reproducible). DEFAULT ON at
/// `DEFAULT_H` (RULED Michael 2026-08-19 — the CI cluster A/B: ~14% cold
/// win, hot unharmed); `0`/`off` = the kill switch; a number > 1 = that
/// explicit lead.
pub fn horizon() -> u64 {
    static H: OnceLock<u64> = OnceLock::new();
    *H.get_or_init(|| match std::env::var("PGRUST_SQE_HORIZON").as_deref() {
        Ok("0") | Ok("off") => 0,
        Ok("1") | Ok("on") => DEFAULT_H,
        Ok(s) => s.parse().unwrap_or(DEFAULT_H),
        Err(_) => DEFAULT_H,
    })
}

/// Adaptive byte-distance lookahead (read-stream idiom): start small,
/// DOUBLE when a worker blocks on a non-resident extent, decay one step
/// per resident hit after a hold-off. The granule lead then only bounds
/// the lookahead. Default ON (the drain design's default distance).
fn adaptive() -> bool {
    static A: OnceLock<bool> = OnceLock::new();
    *A.get_or_init(|| env_flag("PGRUST_SQE_HORIZON_ADAPTIVE", true))
}

/// Frontier gate for FRAME-CUT dict payload streams: with frame-lazy dict
/// handles the demand path no longer reads the whole region, so
/// "issue it all on first touch" both over-reads sparse consumers and
/// races the demand frontier. The target is the demand extent plus
/// `PGRUST_SQE_HORIZON_DICT_H` frames. Single-extent regions keep the
/// whole-region issue (there is no smaller unit).
/// `PGRUST_SQE_HORIZON_DICT_FRONTIER=0` restores whole-region-on-first-touch.
fn dict_frontier() -> bool {
    static F: OnceLock<bool> = OnceLock::new();
    *F.get_or_init(|| env_flag("PGRUST_SQE_HORIZON_DICT_FRONTIER", true))
}

fn dict_h() -> u64 {
    static H: OnceLock<u64> = OnceLock::new();
    *H.get_or_init(|| env_u64("PGRUST_SQE_HORIZON_DICT_H", 1024))
}

/// Frame-cut dict payload stream: > 1 extent AND role DictPayload.
fn is_framecut_dict(ps: &ParsedStream) -> bool {
    ps.extents.len() > 1 && ps.role == pgrc2_format::part::StreamRole::DictPayload
}

/// Scan-resistant admission for prefetched images (default OFF — a
/// cooling-stage discipline mapped onto the process-level per-part
/// residency caches, NOT the OS page cache): a prefetch-admitted image is
/// probationary (its `touched` mark IS the probation tag); a cold-window
/// demand access promotes it (the observer's consume-mark removal); at
/// the bank's cold→warm boundary every still-probationary image is
/// evicted from the part caches, so warm runs see exactly the
/// demand-built residency set. Eviction is cache-only (identity
/// unaffected; a warm demand of an evicted extent re-faults CRC-checked
/// as a first access would). `PGRUST_SQE_HORIZON_COOLING=1` arms.
fn cooling() -> bool {
    static C: OnceLock<bool> = OnceLock::new();
    *C.get_or_init(|| env_flag("PGRUST_SQE_HORIZON_COOLING", false))
}

fn witness_armed() -> bool {
    static W: OnceLock<bool> = OnceLock::new();
    *W.get_or_init(|| env_flag("PGRUST_SQE_HORIZON_WITNESS", false))
}

#[derive(Default)]
struct Counters {
    demand_events: AtomicU64,
    demand_hits: AtomicU64,
    demand_miss: AtomicU64,
    pf_consumed_ext: AtomicU64,
    pf_consumed_bytes: AtomicU64,
    issued_ext: AtomicU64,
    issued_bytes: AtomicU64,
    already_ext: AtomicU64,
    io_bytes: AtomicU64,
    io_runs: AtomicU64,
    io_busy_ns: AtomicU64,
    worker_batches: AtomicU64,
    budget_stalls: AtomicU64,
    pf_wasted_ext: AtomicU64,
    pf_wasted_bytes: AtomicU64,
    dist_up: AtomicU64,
    dist_down: AtomicU64,
    dist_max_seen: AtomicU64,
    cooled_ext: AtomicU64,
    cooled_bytes: AtomicU64,
}

/// Aggregate mechanism witness (tests and the env-armed log line; never a
/// correctness input).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WitnessSnapshot {
    pub demand_events: u64,
    pub demand_hits: u64,
    pub demand_miss: u64,
    pub issued_ext: u64,
    pub issued_bytes: u64,
    pub already_ext: u64,
    pub io_bytes: u64,
    pub io_runs: u64,
    pub pf_consumed_ext: u64,
    pub pf_consumed_bytes: u64,
    pub worker_batches: u64,
    pub budget_stalls: u64,
    pub pf_wasted_ext: u64,
    pub pf_wasted_bytes: u64,
    pub cooled_ext: u64,
    pub cooled_bytes: u64,
}

/// One engaged bank: its part set (weak — the engine owns the parts; a
/// dropped engine's registrations die with it) and its cold-window state.
struct BankReg {
    parts: Vec<Weak<OpenPart>>,
    /// true from engage until the first completed query run that touched
    /// the bank (the cold window).
    cold: AtomicBool,
    /// Epoch (query-boundary counter) of the bank's first observed demand
    /// event; u64::MAX = none yet.
    first_demand_epoch: AtomicU64,
}

struct StreamState {
    bank_idx: usize,
    part_pos: usize,
    part: Weak<OpenPart>,
    ps: Option<ParsedStream>, // None = dead (Overflow / unmatched / dropped)
    granule_organized: bool,
    /// Next extent index to issue.
    issued_to: usize,
    /// Target position (granule for granule-organized, extent idx for
    /// byte-run; u64::MAX = whole stream).
    target: u64,
    inflight: bool,
    seeded: bool,
    /// This dead slot has been placed on the reclamation free list (guards
    /// against double-listing it across successive engage sweeps).
    freed: bool,
    /// A real demand event has touched this stream (chain gating: the
    /// cascade advances only from demand-touched streams, so prefetch
    /// stays <= part_chain parts ahead of demand).
    demand_seen: bool,
    /// adaptive: current lookahead distance in bytes; hold-off counter.
    dist_bytes: u64,
    holdoff: u64,
    /// adaptive: file byte position of the demand frontier (end of the
    /// last demanded extent) — the target is frontier + dist_bytes.
    frontier_end: u64,
}

struct Shared {
    h: u64,
    coalesce: u64,
    /// Per-fetch-batch byte cap (the worker's bounded detour quantum: an
    /// unbounded detour convoys the missing worker behind tens of MB of
    /// IO before its own read).
    fetch_quantum: u64,
    /// Bytes-of-admitted-but-unconsumed cap.
    budget: u64,
    /// Coalescing hole limit (bandwidth × latency).
    hole: u64,
    adapt_min: u64,
    adapt_max: u64,
    /// Max fetch batches one worker executes per miss event.
    fetch_rounds: u64,
    /// Cross-part chaining depth: when a stream is demand-touched or
    /// fully issued, seed the SAME (attno, path_ord, role) stream of the
    /// next part(s) — byte-run regions are single-extent-per-stream and
    /// granule streams stall at part boundaries otherwise.
    part_chain: usize,
    /// Workers currently in the fetch role (the balancing rule input).
    active_fetchers: AtomicU64,
    max_fetchers_seen: AtomicU64,
    /// Bytes prefetched and not yet consumed by a demand access.
    unconsumed: AtomicU64,
    /// Engaged banks (grow-only; weak parts — a dead bank costs a few
    /// words until the next engage sweeps its stream states).
    banks: RwLock<Vec<Arc<BankReg>>>,
    /// OpenPart identity (ptr) → (bank_idx, part_pos).
    part2loc: RwLock<HashMap<usize, (usize, usize)>>,
    /// (part ptr, attno, path_ord, role) → state slot.
    sids: Mutex<HashMap<(usize, u32, u32, u8), usize>>,
    states: Mutex<Vec<Arc<Mutex<StreamState>>>>,
    /// Reclaimed `states` slot indices (dead, quiesced) available for
    /// reuse, so `states` does not grow unboundedly across engine churn.
    free_states: Mutex<Vec<usize>>,
    queue: Mutex<Vec<usize>>,
    ctr: Counters,
    /// (part ptr, file_off, len) → prefetch-touched (hit/waste witness +
    /// cooling probation tags). `touched_n` mirrors the map size so the
    /// enabled-observer hit path skips the lock when no marks exist.
    touched: Mutex<HashMap<(usize, u64, u64), u64>>,
    touched_n: AtomicU64,
    /// Banks still in their cold window; observer enabled iff > 0.
    cold_banks: AtomicU64,
    /// Query-boundary counter (bumped by `reset_per_query`).
    epoch: AtomicU64,
}

static SHARED: OnceLock<Arc<Shared>> = OnceLock::new();

fn shared() -> &'static Arc<Shared> {
    SHARED.get_or_init(|| {
        Arc::new(Shared {
            h: horizon(),
            coalesce: env_u64("PGRUST_SQE_HORIZON_COALESCE_MB", 8) * 1024 * 1024,
            fetch_quantum: env_u64("PGRUST_SQE_HORIZON_FETCH_MB", 4) * 1024 * 1024,
            budget: env_u64("PGRUST_SQE_HORIZON_BUDGET_MB", 2048) * 1024 * 1024,
            hole: env_u64("PGRUST_SQE_HORIZON_HOLE_KB", 512) * 1024,
            adapt_min: env_u64("PGRUST_SQE_HORIZON_ADAPT_MIN_KB", 256) * 1024,
            adapt_max: env_u64("PGRUST_SQE_HORIZON_ADAPT_MAX_MB", 64) * 1024 * 1024,
            fetch_rounds: env_u64("PGRUST_SQE_HORIZON_FETCH_ROUNDS", 2),
            part_chain: env_u64("PGRUST_SQE_HORIZON_PART_CHAIN", 2) as usize,
            active_fetchers: AtomicU64::new(0),
            max_fetchers_seen: AtomicU64::new(0),
            unconsumed: AtomicU64::new(0),
            banks: RwLock::new(Vec::new()),
            part2loc: RwLock::new(HashMap::new()),
            sids: Mutex::new(HashMap::new()),
            states: Mutex::new(Vec::new()),
            free_states: Mutex::new(Vec::new()),
            queue: Mutex::new(Vec::new()),
            ctr: Counters::default(),
            touched: Mutex::new(HashMap::new()),
            touched_n: AtomicU64::new(0),
            cold_banks: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
        })
    })
}

/// Engage the prefetcher on a freshly opened bank (idempotent per part
/// set; called from `Engine::new` when the config arms the horizon).
/// No-op when disarmed or the bank has no parts.
pub fn engage(parts: &[Arc<OpenPart>]) {
    if horizon() == 0 || parts.is_empty() {
        return;
    }
    let sh = shared();
    if !install_observer(sh) {
        return; // observer slot taken by another consumer: safe OFF
    }
    {
        // Already engaged (engine registry re-open of the same part set)?
        let loc = sh.part2loc.read().unwrap();
        if parts.iter().all(|p| loc.contains_key(&(Arc::as_ptr(p) as usize))) {
            return;
        }
    }
    // Reclaim per-generation registry state whose parts have been
    // dropped. A dead part's ArcInner allocation stays pinned as long as
    // ANY Weak to it survives, so every dead Weak must actually be dropped
    // here — clearing `st.part` on dead slots and tombstoning dead banks
    // below — not merely left in place. Quiesced dead slots go on a free
    // list for reuse so `states` does not grow across engine-registry
    // churn.
    {
        let mut sids = sh.sids.lock().unwrap();
        let states = sh.states.lock().unwrap();
        sids.retain(|_, &mut slot| states[slot].lock().unwrap().part.strong_count() > 0);
        let mut newly_free: Vec<usize> = Vec::new();
        for (slot, st_arc) in states.iter().enumerate() {
            let mut st = st_arc.lock().unwrap();
            if st.part.strong_count() == 0 {
                // Drop the Weak (frees the pinned ArcInner) and mark dead.
                st.ps = None;
                st.part = Weak::new();
                // Reclaim the slot only when it is quiescent: `inflight`
                // is false iff the slot is neither queued nor draining, so
                // no stale queue entry can survive to point at a reused
                // slot.
                if !st.inflight && !st.freed {
                    st.freed = true;
                    newly_free.push(slot);
                }
            }
        }
        if !newly_free.is_empty() {
            sh.free_states.lock().unwrap().extend(newly_free);
        }
    }
    // Reclaim dead banks: once all of a bank's parts are dropped its Weak
    // handles still pin their ArcInner allocations (and the Vec holding
    // them). Replace each such entry with a shared empty tombstone —
    // bank_idx stays stable for part2loc/states, and every dead state of
    // the bank is itself already dead — so those Weaks are freed and
    // `banks` grows only by one pointer-sized slot per generation instead
    // of accreting dead part handles.
    {
        let mut banks = sh.banks.write().unwrap();
        let dead = |b: &Arc<BankReg>| {
            !b.parts.is_empty() && b.parts.iter().all(|w| w.strong_count() == 0)
        };
        if banks.iter().any(dead) {
            let tomb = Arc::new(BankReg {
                parts: Vec::new(),
                cold: AtomicBool::new(false),
                first_demand_epoch: AtomicU64::new(u64::MAX),
            });
            for b in banks.iter_mut() {
                if dead(b) {
                    *b = tomb.clone();
                }
            }
        }
    }
    let bank_idx;
    {
        let mut banks = sh.banks.write().unwrap();
        bank_idx = banks.len();
        banks.push(Arc::new(BankReg {
            parts: parts.iter().map(Arc::downgrade).collect(),
            cold: AtomicBool::new(true),
            first_demand_epoch: AtomicU64::new(u64::MAX),
        }));
    }
    {
        let mut loc = sh.part2loc.write().unwrap();
        loc.retain(|_, &mut (bi, pi)| {
            let banks = sh.banks.read().unwrap();
            banks[bi].parts.get(pi).map(|w| w.strong_count() > 0).unwrap_or(false)
        });
        for (pos, p) in parts.iter().enumerate() {
            loc.insert(Arc::as_ptr(p) as usize, (bank_idx, pos));
        }
    }
    sh.cold_banks.fetch_add(1, Ordering::Relaxed);
    pgrc2_read::openpart::set_stream_fault_observer_enabled(true);
}

/// Install the process-wide observer once; `false` = the single-install
/// slot was already taken by another consumer — the horizon then never
/// engages (its events would not arrive), which is the safe OFF.
fn install_observer(sh: &Arc<Shared>) -> bool {
    static INSTALLED: OnceLock<bool> = OnceLock::new();
    *INSTALLED.get_or_init(|| {
        let obs = sh.clone();
        pgrc2_read::openpart::set_stream_fault_observer(Box::new(move |ev| observe(&obs, ev)))
    })
}

/// Query-boundary hook (`engine::reset_per_query`): banks whose first
/// observed demand predates this boundary leave their cold window; when
/// no cold bank remains the observer is disabled (warm runs pay one
/// relaxed atomic load per extent access) and the residual backlog is
/// quiesced.
pub fn on_query_boundary() {
    let Some(sh) = SHARED.get() else { return };
    if sh.cold_banks.load(Ordering::Relaxed) == 0 {
        return;
    }
    let e = sh.epoch.fetch_add(1, Ordering::Relaxed) + 1;
    let warmed: Vec<Arc<BankReg>> = {
        let banks = sh.banks.read().unwrap();
        banks
            .iter()
            .filter(|b| {
                b.cold.load(Ordering::Relaxed)
                    && b.first_demand_epoch.load(Ordering::Relaxed) < e
            })
            .cloned()
            .collect()
    };
    for b in warmed {
        if !b.cold.swap(false, Ordering::Relaxed) {
            continue;
        }
        if cooling() {
            cool_bank(sh, &b);
        }
        if sh.cold_banks.fetch_sub(1, Ordering::Relaxed) == 1 {
            quiesce(sh);
            pgrc2_read::openpart::set_stream_fault_observer_enabled(false);
            if witness_armed() {
                let w = snapshot();
                eprintln!(
                    "SQEHORIZON|h={}|issued_ext={}|issued_bytes={}|already_ext={}|io_bytes={}|io_runs={}|demand_events={}|demand_hits={}|demand_miss={}|pf_hit_ext={}|pf_hit_bytes={}|worker_batches={}|budget_stalls={}|pf_wasted_ext={}|pf_wasted_bytes={}|cooled_ext={}|cooled_bytes={}",
                    sh.h,
                    w.issued_ext,
                    w.issued_bytes,
                    w.already_ext,
                    w.io_bytes,
                    w.io_runs,
                    w.demand_events,
                    w.demand_hits,
                    w.demand_miss,
                    w.pf_consumed_ext,
                    w.pf_consumed_bytes,
                    w.worker_batches,
                    w.budget_stalls,
                    w.pf_wasted_ext,
                    w.pf_wasted_bytes,
                    w.cooled_ext,
                    w.cooled_bytes,
                );
            }
        }
    }
}

/// All banks warm: drop the residual backlog (prefetch is demand-paced,
/// so it is normally empty; anything left would only run past demand) and
/// clear the drained slots' inflight marks so a later cold bank re-arms
/// cleanly.
fn quiesce(sh: &Arc<Shared>) {
    let stale: Vec<usize> = std::mem::take(&mut *sh.queue.lock().unwrap());
    for slot in stale {
        let st_arc = {
            let states = sh.states.lock().unwrap();
            states[slot].clone()
        };
        st_arc.lock().unwrap().inflight = false;
    }
    // Residual marks are prefetched-never-consumed images (the waste
    // witness when cooling is off — the images stay resident, only the
    // marks drain, keeping the warm hit path lock-free and the map
    // bounded).
    {
        let mut t = sh.touched.lock().unwrap();
        sh.ctr.pf_wasted_ext.fetch_add(t.len() as u64, Ordering::Relaxed);
        sh.ctr.pf_wasted_bytes.fetch_add(t.values().sum::<u64>(), Ordering::Relaxed);
        t.clear();
        sh.touched_n.store(0, Ordering::Relaxed);
    }
    sh.unconsumed.store(0, Ordering::Relaxed);
}

/// Cooling eviction at one bank's cold→warm boundary: drain this bank's
/// residual probation tags — exactly the prefetch-admitted,
/// never-demand-consumed extents — and evict those images from the part
/// residency caches. Budget bytes are released.
fn cool_bank(sh: &Arc<Shared>, bank: &BankReg) {
    let members: std::collections::HashSet<usize> = bank
        .parts
        .iter()
        .filter_map(|w| w.upgrade().map(|p| Arc::as_ptr(&p) as usize))
        .collect();
    let drained: Vec<((usize, u64, u64), u64)> = {
        let mut t = sh.touched.lock().unwrap();
        let keys: Vec<(usize, u64, u64)> =
            t.keys().filter(|(p, _, _)| members.contains(p)).copied().collect();
        let out = keys.iter().map(|k| (*k, t.remove(k).unwrap())).collect();
        sh.touched_n.store(t.len() as u64, Ordering::Relaxed);
        out
    };
    let mut per_part: HashMap<usize, Vec<(u64, u64)>> = HashMap::new();
    for ((pp, off, len), _) in drained {
        per_part.entry(pp).or_default().push((off, len));
    }
    let (mut n, mut bytes) = (0u64, 0u64);
    for w in &bank.parts {
        let Some(p) = w.upgrade() else { continue };
        if let Some(keys) = per_part.get(&(Arc::as_ptr(&p) as usize)) {
            let (en, eb) = p.evict_extent_images(keys);
            n += en;
            bytes += eb;
        }
    }
    let u = sh.unconsumed.load(Ordering::Relaxed);
    sh.unconsumed.fetch_sub(bytes.min(u), Ordering::Relaxed);
    sh.ctr.cooled_ext.fetch_add(n, Ordering::Relaxed);
    sh.ctr.cooled_bytes.fetch_add(bytes, Ordering::Relaxed);
}

fn observe(sh: &Arc<Shared>, ev: &StreamFaultEvent) {
    let c = &sh.ctr;
    c.demand_events.fetch_add(1, Ordering::Relaxed);
    if ev.resident {
        c.demand_hits.fetch_add(1, Ordering::Relaxed);
    } else {
        c.demand_miss.fetch_add(1, Ordering::Relaxed);
    }
    let loc = {
        let loc = sh.part2loc.read().unwrap();
        loc.get(&ev.part).copied()
    };
    let Some((bank_idx, part_pos)) = loc else { return };
    // Bank cold-window bookkeeping: record the epoch of the first demand.
    {
        let banks = sh.banks.read().unwrap();
        let b = &banks[bank_idx];
        if !b.cold.load(Ordering::Relaxed) {
            return; // warm bank (another bank keeps the observer enabled)
        }
        b.first_demand_epoch
            .fetch_min(sh.epoch.load(Ordering::Relaxed).max(1), Ordering::Relaxed);
    }
    // Hit/waste witness: consume the prefetch-touched mark (lock only
    // when marks exist).
    if sh.touched_n.load(Ordering::Relaxed) > 0 {
        let consumed = {
            let mut t = sh.touched.lock().unwrap();
            let v = t.remove(&(ev.part, ev.file_off, ev.len));
            sh.touched_n.store(t.len() as u64, Ordering::Relaxed);
            v
        };
        if let Some(bytes) = consumed {
            c.pf_consumed_ext.fetch_add(1, Ordering::Relaxed);
            c.pf_consumed_bytes.fetch_add(bytes, Ordering::Relaxed);
            let u = sh.unconsumed.load(Ordering::Relaxed);
            sh.unconsumed.fetch_sub(bytes.min(u), Ordering::Relaxed);
        }
    }
    // Frontier advance.
    let key = (ev.part, ev.attno, ev.path_ord, ev.role);
    let slot = {
        let sids = sh.sids.lock().unwrap();
        sids.get(&key).copied()
    };
    let slot = match slot {
        Some(s) => s,
        None => create_state(sh, key, bank_idx, part_pos),
    };
    let st_arc = {
        let states = sh.states.lock().unwrap();
        states[slot].clone()
    };
    let mut st = st_arc.lock().unwrap();
    if st.ps.is_none() {
        return;
    }
    let first_demand = !st.demand_seen;
    st.demand_seen = true;
    if !st.seeded {
        st.seeded = true;
        st.issued_to = ev.extent_idx as usize + 1;
        st.dist_bytes = sh.adapt_min;
    }
    // Adaptive distance: a blocking miss doubles, a hit decays one step
    // after the hold-off.
    if adaptive() && st.granule_organized {
        if !ev.resident {
            let nd = (st.dist_bytes * 2).min(sh.adapt_max);
            if nd > st.dist_bytes {
                c.dist_up.fetch_add(1, Ordering::Relaxed);
            }
            st.dist_bytes = nd;
            st.holdoff = 4;
            c.dist_max_seen.fetch_max(nd, Ordering::Relaxed);
        } else if st.holdoff > 0 {
            st.holdoff -= 1;
        } else if st.dist_bytes > sh.adapt_min {
            st.dist_bytes = (st.dist_bytes / 2).max(sh.adapt_min);
            c.dist_down.fetch_add(1, Ordering::Relaxed);
        }
        st.frontier_end = st.frontier_end.max(ev.file_off + ev.len);
    }
    let target = if st.granule_organized {
        if adaptive() && sh.h == u64::MAX {
            u64::MAX // bounded by dist_bytes below, not by granules
        } else {
            ((ev.granule_start + ev.granule_count) as u64).saturating_add(sh.h)
        }
    } else if dict_frontier() && st.ps.as_ref().map(is_framecut_dict).unwrap_or(false) {
        // Frame-cut dict payload region: pace the issue to the demand
        // frontier + DICT_H frames. Other multi-extent byte-runs keep the
        // whole-region issue.
        (ev.extent_idx as u64).saturating_add(dict_h())
    } else {
        // Byte-run region, single extent: the demand path reads the whole
        // region — issue it all on first touch.
        u64::MAX
    };
    if target > st.target {
        st.target = target;
    }
    let backlog = has_backlog(&st);
    let born_exhausted = st
        .ps
        .as_ref()
        .map(|ps| st.issued_to >= ps.extents.len())
        .unwrap_or(false);
    if backlog && !st.inflight {
        st.inflight = true;
        drop(st);
        sh.queue.lock().unwrap().push(slot);
    } else {
        drop(st);
    }
    // Chain from DEMAND (gated): the first demand touch of a stream
    // seeds the same stream in the next part_chain parts — the cascade
    // never runs further ahead than part_chain parts past demand.
    // Born-exhausted streams (single-extent regions) chain here too —
    // they never drain.
    if first_demand || born_exhausted {
        chain_next(sh, slot, bank_idx, part_pos);
    }
    // The worker IS the drainer exactly when its own claim is not
    // resident (it is about to stall on its own pread anyway): fetch
    // coalesced frontier ranges while the balancing rule says retrieval
    // is behind, then fall through to the demand read. A resident access
    // never detours — the priority rule.
    if !ev.resident {
        worker_fetch(sh);
    }
}

/// The fetch role. Balancing rule: keep ~2x what the current fetchers
/// pull outstanding — fetch iff unconsumed < 2 × max(1, active_fetchers)
/// × quantum, and under the byte budget; up to `fetch_rounds` batches,
/// then back to processing (the worker's own read).
fn worker_fetch(sh: &Arc<Shared>) {
    let mut rounds = 0;
    while rounds < sh.fetch_rounds {
        let active = sh.active_fetchers.load(Ordering::Relaxed);
        let target = 2 * (active + 1) * sh.fetch_quantum;
        let unconsumed = sh.unconsumed.load(Ordering::Relaxed);
        if unconsumed >= target.min(sh.budget) {
            break; // enough retrieved: process
        }
        let slot = { sh.queue.lock().unwrap().pop() };
        let Some(slot) = slot else { break }; // queue empty: drain back
        let a = sh.active_fetchers.fetch_add(1, Ordering::Relaxed) + 1;
        sh.max_fetchers_seen.fetch_max(a, Ordering::Relaxed);
        drain_slot(sh, slot);
        sh.active_fetchers.fetch_sub(1, Ordering::Relaxed);
        rounds += 1;
    }
}

fn has_backlog(st: &StreamState) -> bool {
    let Some(ps) = &st.ps else { return false };
    if st.issued_to >= ps.extents.len() {
        return false;
    }
    let rec = &ps.extents[st.issued_to];
    let pos = if st.granule_organized { rec.granule_start as u64 } else { st.issued_to as u64 };
    if pos >= st.target {
        return false;
    }
    if adaptive() && st.granule_organized {
        // Byte-distance bound: the next extent must start within
        // frontier_end + dist_bytes.
        return rec.file_off < st.frontier_end.saturating_add(st.dist_bytes);
    }
    true
}

fn create_state(sh: &Arc<Shared>, key: (usize, u32, u32, u8), bank_idx: usize, part_pos: usize) -> usize {
    // Resolve the ParsedStream from the part's stream directory (resident
    // by the time any extent is read — the cursor open parsed it).
    let part = {
        let banks = sh.banks.read().unwrap();
        banks[bank_idx].parts[part_pos].clone()
    };
    let mut found: Option<ParsedStream> = None;
    if let Some(p) = part.upgrade() {
        if let Ok(dir) = p.stream_directory() {
            for ps in dir.streams() {
                if ps.entry.attno == key.1 && ps.entry.path_ord == key.2 && ps.entry.role == key.3 {
                    found = Some(ps.clone());
                    break;
                }
            }
        }
    }
    let granule_organized = found.as_ref().map(|p| p.granule_organized()).unwrap_or(false);
    // Overflow is sparse-reference-directed — whole-stream prefetch
    // over-fetches by construction; leave it dead.
    if let Some(ps) = &found {
        if ps.role == pgrc2_format::part::StreamRole::Overflow {
            found = None;
        }
    }
    let st = StreamState {
        bank_idx,
        part_pos,
        part,
        ps: found,
        granule_organized,
        issued_to: 0,
        target: 0,
        inflight: false,
        seeded: false,
        freed: false,
        demand_seen: false,
        dist_bytes: 0,
        holdoff: 0,
        frontier_end: 0,
    };
    let mut sids = sh.sids.lock().unwrap();
    if let Some(&s) = sids.get(&key) {
        return s; // lost the race
    }
    let mut states = sh.states.lock().unwrap();
    // Reuse a reclaimed slot when one is available (the freed slot is
    // dead, out of `sids`, and — being quiesced when freed — carries no
    // stale queue entry), else grow `states`.
    let slot = match sh.free_states.lock().unwrap().pop() {
        Some(s) => {
            states[s] = Arc::new(Mutex::new(st));
            s
        }
        None => {
            states.push(Arc::new(Mutex::new(st)));
            states.len() - 1
        }
    };
    sids.insert(key, slot);
    slot
}

/// Seed the same stream in the next `part_chain` parts of the same bank:
/// create (or find) their states pre-armed (byte-run: whole region;
/// granule: the configured lead / adaptive window from the part start)
/// and enqueue backlog.
fn chain_next(sh: &Arc<Shared>, slot: usize, bank_idx: usize, part_pos: usize) {
    if sh.part_chain == 0 {
        return;
    }
    // Identify the stream of `slot`.
    let (attno, path_ord, role) = {
        let states = sh.states.lock().unwrap();
        let st = states[slot].lock().unwrap();
        match &st.ps {
            Some(ps) => (ps.entry.attno, ps.entry.path_ord, ps.entry.role),
            None => return,
        }
    };
    for d in 1..=sh.part_chain {
        let npos = part_pos + d;
        let npart = {
            let banks = sh.banks.read().unwrap();
            let b = &banks[bank_idx];
            match b.parts.get(npos) {
                Some(w) => match w.upgrade() {
                    Some(p) => p,
                    None => continue,
                },
                None => break,
            }
        };
        let key = (Arc::as_ptr(&npart) as usize, attno, path_ord, role);
        drop(npart);
        let nslot = {
            let sids = sh.sids.lock().unwrap();
            sids.get(&key).copied()
        };
        let nslot = match nslot {
            Some(s) => s,
            None => create_state(sh, key, bank_idx, npos),
        };
        let st_arc = {
            let states = sh.states.lock().unwrap();
            states[nslot].clone()
        };
        let mut st = st_arc.lock().unwrap();
        if st.ps.is_none() || st.seeded {
            continue; // demand already reached it (or dead)
        }
        st.seeded = true;
        st.issued_to = 0;
        st.dist_bytes = sh.adapt_min.max(sh.fetch_quantum);
        st.target = if st.granule_organized {
            if adaptive() && sh.h == u64::MAX { u64::MAX } else { sh.h }
        } else if dict_frontier() && st.ps.as_ref().map(is_framecut_dict).unwrap_or(false) {
            dict_h()
        } else {
            u64::MAX
        };
        // Frontier for the adaptive byte bound: the part start of this
        // stream's first extent.
        st.frontier_end = st.ps.as_ref().unwrap().extents.first().map(|r| r.file_off).unwrap_or(0);
        let backlog = has_backlog(&st);
        if backlog && !st.inflight {
            st.inflight = true;
            drop(st);
            sh.queue.lock().unwrap().push(nslot);
        }
    }
}

/// Execute one prepared batch: mark touched, coalesced real reads through
/// `prefetch_extent_run_holes` (wrapped extents rebuilt via the cursor's
/// unwrapper adjudication), counters.
fn issue_batch(
    sh: &Arc<Shared>,
    part: &Arc<OpenPart>,
    entry: &pgrc2_format::part::StreamEntry,
    granule_organized: bool,
    batch: &[(u32, ExtentRecord)],
) {
    let part_key = Arc::as_ptr(part) as usize;
    // Mark before issue so a racing demand consume is still witnessed.
    {
        let mut touched = sh.touched.lock().unwrap();
        for (_, rec) in batch {
            touched.insert((part_key, rec.file_off, rec.len), rec.len);
        }
        sh.touched_n.store(touched.len() as u64, Ordering::Relaxed);
    }
    let wrapped = entry.wrapper != Wrapper::None.as_u8();
    // Cooling provenance: only images THIS run inserts are probationary.
    // Already-resident drop-outs (demand got there first) must keep their
    // images AND lose their stale marks — a stale mark on a demand image
    // would (a) inflate the waste witness and (b) evict hot-set bytes at
    // the warm boundary.
    let mut inserted_log: Vec<(u64, u64)> = Vec::new();
    let log_arg: Option<&mut Vec<(u64, u64)>> =
        if cooling() { Some(&mut inserted_log) } else { None };
    let t0 = std::time::Instant::now();
    let stats = if wrapped {
        let uw = crate::bank::unwrappers()
            .iter()
            .find(|u| u.wrapper().as_u8() == entry.wrapper)
            .copied();
        match uw {
            Some(uw) => {
                let mut rebuild = |raw: &[u8]| -> pgrc2_read::ReadResult<Vec<u8>> {
                    let hdr = StreamSectionHdr::decode(raw)?;
                    if hdr.encoding != entry.encoding
                        || hdr.wrapper != entry.wrapper
                        || (granule_organized && hdr.width != entry.width)
                    {
                        return Err(pgrc2_read::ReadError::Format(
                            pgrc2_format::FormatError::Corrupt {
                                at: "prefetch: section header vs stream entry",
                            },
                        ));
                    }
                    let rebuilt = uw.unwrap_section(&hdr, raw)?;
                    let twin = StreamSectionHdr::decode(&rebuilt)?;
                    if twin.wrapper != Wrapper::None.as_u8() {
                        return Err(pgrc2_read::ReadError::Format(
                            pgrc2_format::FormatError::Corrupt {
                                at: "prefetch: unwrapped section still wrapped",
                            },
                        ));
                    }
                    Ok(rebuilt)
                };
                part.prefetch_extent_run_holes(
                    entry,
                    batch,
                    sh.coalesce,
                    sh.hole,
                    Some(&mut rebuild),
                    log_arg,
                )
            }
            None => (0, 0, 0, 0, 0),
        }
    } else {
        part.prefetch_extent_run_holes(entry, batch, sh.coalesce, sh.hole, None, log_arg)
    };
    let dt = t0.elapsed().as_nanos() as u64;
    if cooling() {
        // Un-mark batch keys that did NOT insert (already resident,
        // in-flight elsewhere, CRC/IO skip, or no unwrapper): their marks
        // are not probation tags. A mark a demand access consumed in the
        // window is already gone; `remove` on it is a no-op.
        let inserted: std::collections::HashSet<(u64, u64)> =
            inserted_log.iter().copied().collect();
        let mut touched = sh.touched.lock().unwrap();
        for (_, rec) in batch {
            if !inserted.contains(&(rec.file_off, rec.len)) {
                touched.remove(&(part_key, rec.file_off, rec.len));
            }
        }
        sh.touched_n.store(touched.len() as u64, Ordering::Relaxed);
    }
    let (inserted, inserted_bytes, already, io_bytes, io_runs) = stats;
    sh.ctr.issued_ext.fetch_add(inserted, Ordering::Relaxed);
    sh.ctr.issued_bytes.fetch_add(inserted_bytes, Ordering::Relaxed);
    sh.ctr.already_ext.fetch_add(already, Ordering::Relaxed);
    sh.ctr.io_bytes.fetch_add(io_bytes, Ordering::Relaxed);
    sh.ctr.io_runs.fetch_add(io_runs, Ordering::Relaxed);
    sh.ctr.io_busy_ns.fetch_add(dt, Ordering::Relaxed);
    sh.unconsumed.fetch_add(inserted_bytes, Ordering::Relaxed);
    sh.ctr.worker_batches.fetch_add(1, Ordering::Relaxed);
}

/// Claim one batch from `slot`'s backlog and issue it; requeue if backlog
/// remains.
fn drain_slot(sh: &Arc<Shared>, slot: usize) {
    // Byte budget: admitted-but-unconsumed prefetch at cap → refuse
    // (requeue; a later consume frees budget). Witnessed, never silent.
    if sh.unconsumed.load(Ordering::Relaxed) >= sh.budget {
        sh.ctr.budget_stalls.fetch_add(1, Ordering::Relaxed);
        sh.queue.lock().unwrap().push(slot);
        return;
    }
    let st_arc = {
        let states = sh.states.lock().unwrap();
        states[slot].clone()
    };
    // Claim a batch under the state lock, issue outside it.
    let (part, batch, entry, granule_organized, exhausted, bank_idx, part_pos) = {
        let mut st = st_arc.lock().unwrap();
        if st.ps.is_none() {
            st.inflight = false;
            return;
        }
        let Some(part) = st.part.upgrade() else {
            // Bank dropped mid-drain: the state is dead.
            st.ps = None;
            st.inflight = false;
            return;
        };
        let entry = st.ps.as_ref().unwrap().entry.clone();
        let go = st.granule_organized;
        let mut batch: Vec<(u32, ExtentRecord)> = Vec::new();
        let mut bytes = 0u64;
        let cap = sh.fetch_quantum;
        // Adaptive byte-distance bound applies inside the batch too.
        let (adapt_go, fe, db) = (adaptive() && go, st.frontier_end, st.dist_bytes);
        while st.issued_to < st.ps.as_ref().unwrap().extents.len() && bytes < cap {
            let rec = st.ps.as_ref().unwrap().extents[st.issued_to];
            let pos = if go { rec.granule_start as u64 } else { st.issued_to as u64 };
            if pos >= st.target {
                break;
            }
            if adapt_go && rec.file_off >= fe.saturating_add(db) {
                break;
            }
            bytes += rec.len;
            batch.push((st.issued_to as u32, rec));
            st.issued_to += 1;
        }
        let exhausted =
            st.issued_to >= st.ps.as_ref().unwrap().extents.len() && st.demand_seen;
        if batch.is_empty() {
            st.inflight = false;
            let (bi, pp) = (st.bank_idx, st.part_pos);
            drop(st);
            if exhausted {
                chain_next(sh, slot, bi, pp);
            }
            return;
        }
        (part, batch, entry, go, exhausted, st.bank_idx, st.part_pos)
    };
    issue_batch(sh, &part, &entry, granule_organized, &batch);
    if exhausted {
        chain_next(sh, slot, bank_idx, part_pos);
    }
    // Backlog re-check.
    let mut st = st_arc.lock().unwrap();
    if has_backlog(&st) {
        drop(st);
        sh.queue.lock().unwrap().push(slot);
    } else {
        st.inflight = false;
    }
}

/// Cumulative mechanism counters (monotone across queries; a test leg
/// diffs two snapshots around a run).
pub fn snapshot() -> WitnessSnapshot {
    let Some(sh) = SHARED.get() else { return WitnessSnapshot::default() };
    let c = &sh.ctr;
    WitnessSnapshot {
        demand_events: c.demand_events.load(Ordering::Relaxed),
        demand_hits: c.demand_hits.load(Ordering::Relaxed),
        demand_miss: c.demand_miss.load(Ordering::Relaxed),
        issued_ext: c.issued_ext.load(Ordering::Relaxed),
        issued_bytes: c.issued_bytes.load(Ordering::Relaxed),
        already_ext: c.already_ext.load(Ordering::Relaxed),
        io_bytes: c.io_bytes.load(Ordering::Relaxed),
        io_runs: c.io_runs.load(Ordering::Relaxed),
        pf_consumed_ext: c.pf_consumed_ext.load(Ordering::Relaxed),
        pf_consumed_bytes: c.pf_consumed_bytes.load(Ordering::Relaxed),
        worker_batches: c.worker_batches.load(Ordering::Relaxed),
        budget_stalls: c.budget_stalls.load(Ordering::Relaxed),
        pf_wasted_ext: c.pf_wasted_ext.load(Ordering::Relaxed),
        pf_wasted_bytes: c.pf_wasted_bytes.load(Ordering::Relaxed),
        cooled_ext: c.cooled_ext.load(Ordering::Relaxed),
        cooled_bytes: c.cooled_bytes.load(Ordering::Relaxed),
    }
}
