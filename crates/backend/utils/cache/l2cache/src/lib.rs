//! D3.2 shared immutable L2 cache substrate (docs/design/connection-scaling.md §D3).
//!
//! Process-global, generation-keyed store of immutable cache-entry bodies,
//! shared by every backend thread. The per-thread (L1) caches are untouched on
//! their hit paths; on an L1 miss the owning cache consults this store before
//! scanning the catalogs, and on an L2 miss exactly one thread builds the
//! entry (per-key build gate — the thundering-herd fix).
//!
//! # Generations (the invalidation design, as built)
//!
//! One monotonic generation counter per invalidation domain:
//! - catcache: one per syscache id (`CAT_DOMAINS` slots), bumped by every
//!   catcache sinval message for that cache and by catalog-flush messages;
//! - relcache: `REL_STRIPES` stripes over relid, bumped per relcache sinval
//!   message (relid == InvalidOid bumps every stripe).
//!
//! Bumps happen ONCE, on the SENDING side, strictly BEFORE the messages enter
//! the shared sinval queue (inval::eoxact send paths). Each backend keeps a
//! thread-local VIEW of every domain generation, advanced only when it
//! processes an invalidation message for that domain (or a full cache reset).
//! L2 lookups are keyed by the reader's view, so:
//! - a backend that has not yet processed a queued inval keeps reading the
//!   entries of its old generation (no forward time-travel mid-transaction;
//!   this is exactly today's L1 staleness-until-AcceptInvalidationMessages);
//! - a backend that has processed the inval reads at the new generation and
//!   misses, and the rebuilt entry can never be confused with the old one.
//!
//! Because the bump precedes queue insertion, a backend that processes the
//! message and re-syncs its view is guaranteed to see a generation at least
//! as new as the bump — an invalidation can never be "undone" by an L2 hit.
//!
//! A build that races a bump (global generation moved between the reader
//! capturing its view and finishing the catalog scan) is installed in the
//! builder's L1 but NOT published: the data read under the builder's own
//! catalog snapshot can be from either side of the concurrent commit, so it
//! must not be stamped with a generation other threads will trust.
//!
//! Superseded generations: entries stay alive while referenced (`Arc`); the
//! map retains at most the two newest generations per logical key (pruned on
//! insert), so a laggard backend usually still finds its older entry. When it
//! does not, it falls back to a private catalog read — which is exactly
//! today's (L2-less) behavior and carries today's semantics.
//!
//! # Uncommitted DDL (overlay rule)
//!
//! A session with pending, not-yet-broadcast invalidation messages (i.e. it
//! modified a catalog in the current transaction) bypasses L2 entirely — both
//! read and publish — until end of transaction (`private_build_mode`,
//! coarse-grained form of the design doc's per-key overlay: strictly safe,
//! DDL sessions simply build privately as they do today). Parallel workers
//! also bypass L2: a worker may observe its leader's uncommitted catalog
//! state, which must never be published.
//!
//! Kill switch: PGRUST_L2_CACHE=0 disables every L2 path.

use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use types_core::Oid;

/// syscache id space (syscache_ids.h SysCacheSize is 85; headroom for growth).
pub const CAT_DOMAINS: usize = 96;
/// relid stripes for relcache generations (power of two).
pub const REL_STRIPES: usize = 1024;

/// GUC `shared_catalog_cache` (PGC_POSTMASTER — flipping mid-life would
/// desync generation views against live L1 state; the cell is read live, but
/// the GUC engine refuses non-boot assignment) kills every L2 path when off.
/// PGRUST_L2_CACHE remains the harness override (env wins if set; cached at
/// first read — t35-law kill switch for the existing test suites/scripts).
#[inline]
pub fn enabled() -> bool {
    static ENV: std::sync::OnceLock<Option<bool>> = std::sync::OnceLock::new();
    if let Some(v) = *ENV.get_or_init(|| {
        std::env::var("PGRUST_L2_CACHE").ok().map(|v| v.trim() != "0")
    }) {
        return v;
    }
    guc_tables::backing::shared_catalog_cache()
}

// ---------------------------------------------------------------------------
// Private-build probes (uncommitted-DDL overlay + parallel workers)
// ---------------------------------------------------------------------------

// Installed by inval::init_seams (l2cache cannot depend on inval).
static PENDING_PROBE: std::sync::atomic::AtomicPtr<()> =
    std::sync::atomic::AtomicPtr::new(std::ptr::null_mut());
// Installed by snapmgr::init_seams: catalog reads under a historic snapshot
// (logical decoding) see PAST catalog states and must never be published.
static HISTORIC_PROBE: std::sync::atomic::AtomicPtr<()> =
    std::sync::atomic::AtomicPtr::new(std::ptr::null_mut());

pub fn set_pending_invals_probe(f: fn() -> bool) {
    PENDING_PROBE.store(f as *mut (), Ordering::Release);
}

pub fn set_historic_snapshot_probe(f: fn() -> bool) {
    HISTORIC_PROBE.store(f as *mut (), Ordering::Release);
}

#[inline]
fn probe(slot: &std::sync::atomic::AtomicPtr<()>) -> bool {
    let p = slot.load(Ordering::Acquire);
    if p.is_null() {
        return false;
    }
    // SAFETY: only the set_* fns above store here, always a fn() -> bool.
    let f: fn() -> bool = unsafe { std::mem::transmute(p) };
    f()
}

/// True when this thread must build cache entries privately (no L2 read or
/// publish): it holds unbroadcast invalidation messages (uncommitted DDL), it
/// is a parallel worker (may see its leader's uncommitted catalog state), or
/// it reads catalogs under a historic snapshot (logical decoding).
pub fn private_build_mode() -> bool {
    if probe(&PENDING_PROBE) || probe(&HISTORIC_PROBE) {
        return true;
    }
    parallel_seams::is_parallel_worker::is_installed()
        && parallel_seams::is_parallel_worker::call()
}

// ---------------------------------------------------------------------------
// Generations
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Domain {
    Cat(i32),
    Rel(Oid),
}

#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);
static CAT_GENS: [AtomicU64; CAT_DOMAINS] = [ZERO; CAT_DOMAINS];
static REL_GENS: [AtomicU64; REL_STRIPES] = [ZERO; REL_STRIPES];

#[inline]
fn rel_stripe(relid: Oid) -> usize {
    (relid as usize) & (REL_STRIPES - 1)
}

// Cache ids arrive from WAL commit records, two-phase state and the sinval
// queue; an id outside the catcache range (C: an unchecked array index)
// lands on a spare slot no reader consults.
static SPARE_GEN: AtomicU64 = AtomicU64::new(0);

#[inline]
fn gen_slot(domain: Domain) -> &'static AtomicU64 {
    match domain {
        Domain::Cat(id) => match CAT_GENS.get(usize::try_from(id).unwrap_or(usize::MAX)) {
            Some(slot) => slot,
            None => &SPARE_GEN,
        },
        Domain::Rel(relid) => &REL_GENS[rel_stripe(relid)],
    }
}

#[inline]
pub fn current_gen(domain: Domain) -> u64 {
    gen_slot(domain).load(Ordering::Acquire)
}

thread_local! {
    static VIEW_INIT: Cell<bool> = const { Cell::new(false) };
    static CAT_VIEW: [Cell<u64>; CAT_DOMAINS] = const { [const { Cell::new(0) }; CAT_DOMAINS] };
    static REL_VIEW: [Cell<u64>; REL_STRIPES] = const { [const { Cell::new(0) }; REL_STRIPES] };
}

/// Debug tracing for the D3.2 stressor (PGRUST_L2_DEBUG=1).
pub fn l2_debug_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_L2_DEBUG").is_ok_and(|v| v.trim() == "1"))
}

#[cold]
fn view_init_slow() {
    // First L2 use on this thread: adopt the current global generations. Any
    // sinval message queued for us before this point had its bump land first
    // (bump-before-queue), so the adopted view is never behind a message we
    // will later process — and processing it re-syncs anyway.
    for (i, g) in CAT_GENS.iter().enumerate() {
        let v = g.load(Ordering::Acquire);
        CAT_VIEW.with(|a| a[i].set(v));
    }
    for (i, g) in REL_GENS.iter().enumerate() {
        let v = g.load(Ordering::Acquire);
        REL_VIEW.with(|a| a[i].set(v));
    }
    VIEW_INIT.with(|c| c.set(true));
    if l2_debug_enabled() {
        eprintln!(
            "L2DBG view-init thr={:?} relstripe34={} relstripe39={}",
            std::thread::current().id(),
            REL_GENS[34].load(Ordering::SeqCst),
            REL_GENS[39].load(Ordering::SeqCst),
        );
    }
}

/// Re-adopt the current global generations (e.g. right after sinval
/// registration: messages queued before registration are skipped by design,
/// so the view must cover their bumps).
pub fn readopt_views() {
    VIEW_INIT.with(|c| c.set(false));
    view_init_slow();
}

#[inline]
fn ensure_view() {
    if !VIEW_INIT.with(|c| c.get()) {
        view_init_slow();
    }
}

/// This thread's generation view for `domain` (as of its last processed
/// invalidation touching the domain).
#[inline]
pub fn view_gen(domain: Domain) -> u64 {
    ensure_view();
    match domain {
        Domain::Cat(id) => CAT_VIEW.with(|a| a.get(id as usize).map_or(0, |c| c.get())),
        Domain::Rel(relid) => REL_VIEW.with(|a| a[rel_stripe(relid)].get()),
    }
}

/// Advance this thread's view of `domain` to the current global generation.
/// Called wherever sinval processing drops L1 entries of the domain.
#[inline]
/// Returns whether the view advanced.
pub fn sync_view(domain: Domain) -> bool {
    ensure_view();
    let g = current_gen(domain);
    match domain {
        Domain::Cat(id) => CAT_VIEW.with(|a| {
            if let Some(c) = a.get(id as usize) {
                if g > c.get() {
                    c.set(g);
                    return true;
                }
            }
            false
        }),
        Domain::Rel(relid) => REL_VIEW.with(|a| {
            let c = &a[rel_stripe(relid)];
            if g > c.get() {
                c.set(g);
                return true;
            }
            false
        }),
    }
}

pub fn sync_view_all_cat() -> bool {
    let mut advanced = false;
    ensure_view();
    for (i, g) in CAT_GENS.iter().enumerate() {
        let v = g.load(Ordering::Acquire);
        CAT_VIEW.with(|a| {
            if v > a[i].get() {
                a[i].set(v);
                advanced = true;
            }
        });
    }
    advanced
}

pub fn sync_view_all_rel() -> bool {
    ensure_view();
    let mut advanced = false;
    for (i, g) in REL_GENS.iter().enumerate() {
        let v = g.load(Ordering::Acquire);
        REL_VIEW.with(|a| {
            if v > a[i].get() {
                a[i].set(v);
                advanced = true;
            }
        });
    }
    advanced
}

pub fn sync_view_all() {
    sync_view_all_cat();
    sync_view_all_rel();
}

/// Snapshot every relcache stripe generation (one Acquire load each).
///
/// Init-file publish keying (relcache::initfile): the loader captures this
/// snapshot while holding RelCacheInitLock, so no unlink-protected DDL can
/// commit between "the file exists and is fresh" and "these are the
/// generations its content is current at" — entries decoded from the file are
/// then published at `snapshot[rel_stripe_of(relid)]`.
pub fn rel_gen_snapshot() -> Vec<u64> {
    REL_GENS.iter().map(|g| g.load(Ordering::Acquire)).collect()
}

/// The generation stripe `relid` maps to (index into [`rel_gen_snapshot`]).
#[inline]
pub fn rel_stripe_of(relid: Oid) -> usize {
    rel_stripe(relid)
}

/// Syscaches whose rows feed the SHARED relcache core without any relcache
/// invalidation of their own: an index entry's `rd_support` / `rd_opfamily` /
/// `rd_opcintype` (and the operator arrays) come from pg_amproc, pg_amop,
/// pg_opclass and pg_opfamily at build time. C keeps that material per
/// backend, so a change to those catalogs (ALTER OPERATOR FAMILY, a direct
/// `UPDATE pg_amproc` — amcheck's 005_opclass_damage) is seen by every NEW
/// backend, and its per-backend OpClassCache is dropped on the CLAOID /
/// AMPROCNUM syscache callbacks. Here a new session would mirror the shared
/// core built before the change and keep the old support function forever;
/// a catcache bump on one of these ids therefore also moves every relcache
/// stripe. Ids are C's fixed syscache identifiers (cache_syscache::cacheinfo:
/// AMOPOPID 3, AMOPSTRATEGY 4, AMPROCNUM 5, CLAAMNAMENSP 13, CLAOID 14,
/// OPFAMILYAMNAMENSP 41, OPFAMILYOID 42), pinned by a test there.
pub const RELCORE_DEPENDENT_CAT_IDS: [i32; 7] = [3, 4, 5, 13, 14, 41, 42];

/// Bump `domain`'s global generation (sender side, BEFORE the corresponding
/// sinval message enters the shared queue) and adopt it as this thread's own
/// view (the sender's caches already reflect the new state).
pub fn bump(domain: Domain) {
    ensure_view();
    let new = gen_slot(domain).fetch_add(1, Ordering::SeqCst) + 1;
    match domain {
        Domain::Cat(id) => {
            CAT_VIEW.with(|a| {
                if let Some(c) = a.get(id as usize) {
                    c.set(new);
                }
            });
            if RELCORE_DEPENDENT_CAT_IDS.contains(&id) {
                bump_all_rel();
            }
        }
        Domain::Rel(relid) => REL_VIEW.with(|a| a[rel_stripe(relid)].set(new)),
    }
}

pub fn bump_all_cat() {
    for id in 0..CAT_DOMAINS as i32 {
        bump(Domain::Cat(id));
    }
}

/// C re-creates every cache with the crashed backends; this process-global
/// store survives the in-process crash cycle, so every generation advances
/// (a change committed but killed before its inval send is otherwise served
/// stale forever).
pub fn bump_all_after_crash() {
    bump_all_cat();
    bump_all_rel();
}

pub fn bump_all_rel() {
    ensure_view();
    for (i, g) in REL_GENS.iter().enumerate() {
        let new = g.fetch_add(1, Ordering::SeqCst) + 1;
        REL_VIEW.with(|a| a[i].set(new));
    }
}

// ---------------------------------------------------------------------------
// The shared map
// ---------------------------------------------------------------------------

/// Map key below the generation: which cache, which database, which hash
/// bucket. Full logical-key equality is the caller's `matches` closure
/// (catcache compares stored keys; relcache keys are exact by relid).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct L2Key {
    pub kind: u8, // 0 = catcache, 1 = relcache
    pub id: u32,  // catcache: cache id; relcache: relid
    pub db: Oid,  // InvalidOid for shared catalogs
    pub hash: u32, // catcache hashValue; relcache: 0
}

pub const KIND_CAT: u8 = 0;
pub const KIND_REL: u8 = 1;

type L2Value = Arc<dyn Any + Send + Sync>;

struct Bucket {
    entries: Vec<(u64, L2Value, usize)>, // (gen, entry, approx bytes)
}

const SHARDS: usize = 64;

struct Shard {
    map: Mutex<HashMap<L2Key, Bucket>>,
    /// Live entry count for this shard (mutated only under `map`'s lock, so it
    /// stays exact); read to decide when the shard is over its budget.
    count: AtomicUsize,
}

fn shards() -> &'static [Shard; SHARDS] {
    static SHARDS_CELL: std::sync::OnceLock<Box<[Shard; SHARDS]>> = std::sync::OnceLock::new();
    SHARDS_CELL.get_or_init(|| {
        let v: Vec<Shard> = (0..SHARDS)
            .map(|_| Shard { map: Mutex::new(HashMap::new()), count: AtomicUsize::new(0) })
            .collect();
        let boxed: Box<[Shard; SHARDS]> = v.try_into().ok().unwrap();
        boxed
    })
}

/// Global entry budget for the whole shared L2 map (all shards combined).
///
/// The map is process-global and outlives every session, so — unlike the
/// per-backend L1 catcache, which is capped by `catcache_size_limit` — it needs
/// its own bound. Without one, an unprivileged session probing unboundedly many
/// distinct catalog keys (e.g. negative `to_regtype`/`to_regproc` lookups over
/// `generate_series`, each publishing an immortal negative entry, or generation
/// bumps that strand whole domains of superseded entries) grows server heap
/// without limit and can OOM the entire instance (CWE-770).
///
/// Eviction is correctness-preserving: an evicted entry is exactly a laggard L2
/// miss — the next probe re-builds it from the underlying catcache/catalog, the
/// same fallback used for a pruned superseded generation. Because eviction
/// removes arbitrary live entries (including stranded superseded generations),
/// it also reclaims those independently of same-key re-insertion.
///
/// Sized to roughly mirror the catcache ceiling scaled to a shared, all-session
/// store: ~262k entries at ~300 B/entry ≈ 75 MB hard cap. `0` disables the cap.
/// `PGRUST_L2_CACHE_MAX_ENTRIES` overrides it for harnesses (env wins if set;
/// cached at first read).
const L2_MAX_ENTRIES_DEFAULT: usize = 262_144;

#[inline]
fn l2_max_entries() -> usize {
    static ENV: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    if let Some(v) = *ENV.get_or_init(|| {
        std::env::var("PGRUST_L2_CACHE_MAX_ENTRIES")
            .ok()
            .and_then(|v| v.trim().parse().ok())
    }) {
        return v;
    }
    L2_MAX_ENTRIES_DEFAULT
}

/// Per-shard slice of the global budget (`0` == cap disabled). Ceil so the sum
/// of per-shard budgets is never below the configured global cap.
#[inline]
fn shard_budget() -> usize {
    let cap = l2_max_entries();
    if cap == 0 {
        return 0;
    }
    cap.div_ceil(SHARDS).max(1)
}

/// Evict arbitrary live entries from a shard's `map` until its `count` is back
/// within `budget`, keeping `count` exact. Returns `(entries_removed,
/// bytes_removed)` so the caller can apply the same deltas to the global stats.
///
/// Called under the shard lock right after an insert overruns the budget.
/// HashMap iteration order is randomized, so this is approximately random
/// eviction — correctness-preserving regardless of which entries are dropped
/// (an evicted entry is just a laggard miss that re-fetches from the catalog).
fn evict_shard(
    map: &mut HashMap<L2Key, Bucket>,
    count: &AtomicUsize,
    budget: usize,
) -> (usize, usize) {
    let mut n_removed = 0usize;
    let mut bytes_removed = 0usize;
    let mut empty: Vec<L2Key> = Vec::new();
    for (k, b) in map.iter_mut() {
        if count.load(Ordering::Relaxed) <= budget {
            break;
        }
        while let Some((_, _, sz)) = b.entries.pop() {
            n_removed += 1;
            bytes_removed += sz;
            count.fetch_sub(1, Ordering::Relaxed);
            if count.load(Ordering::Relaxed) <= budget {
                break;
            }
        }
        if b.entries.is_empty() {
            empty.push(*k);
        }
    }
    for k in empty {
        map.remove(&k);
    }
    (n_removed, bytes_removed)
}

#[inline]
fn shard_of(key: &L2Key) -> &'static Shard {
    // Cheap mix; the map inside re-hashes properly.
    let h = (key.id as usize)
        .wrapping_mul(0x9E37_79B9)
        .wrapping_add(key.hash as usize)
        .wrapping_add((key.kind as usize) << 5)
        .wrapping_add(key.db as usize);
    &shards()[(h >> 7) & (SHARDS - 1)]
}

// Stats (memory gate: the L2's total size counted once).
static ENTRIES: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);
static INSERTS: AtomicU64 = AtomicU64::new(0);
static HERD_WAITS: AtomicU64 = AtomicU64::new(0);

pub struct L2Stats {
    pub entries: usize,
    pub bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub herd_waits: u64,
}

pub fn stats() -> L2Stats {
    L2Stats {
        entries: ENTRIES.load(Ordering::Relaxed),
        bytes: BYTES.load(Ordering::Relaxed),
        hits: HITS.load(Ordering::Relaxed),
        misses: MISSES.load(Ordering::Relaxed),
        inserts: INSERTS.load(Ordering::Relaxed),
        herd_waits: HERD_WAITS.load(Ordering::Relaxed),
    }
}

/// Look up `key` at generation `gen`. `matches` performs full logical-key
/// comparison (hash collisions share a bucket).
pub fn lookup(
    key: L2Key,
    gen: u64,
    mut matches: impl FnMut(&(dyn Any + Send + Sync)) -> bool,
) -> Option<L2Value> {
    let shard = shard_of(&key);
    let map = shard.map.lock().unwrap();
    let hit = map.get(&key).and_then(|b| {
        b.entries
            .iter()
            .find(|(g, e, _)| *g == gen && matches(e.as_ref()))
            .map(|(_, e, _)| Arc::clone(e))
    });
    drop(map);
    if hit.is_some() {
        HITS.fetch_add(1, Ordering::Relaxed);
    } else {
        MISSES.fetch_add(1, Ordering::Relaxed);
    }
    hit
}

/// Publish an entry at (key, gen). `matches` identifies entries with the same
/// LOGICAL key (across generations) for pruning: after the insert, only the
/// two newest generations of that logical key are kept reachable — anything
/// older is unlinked (still alive while referenced; a laggard reader that
/// misses simply falls back to a private build, today's exact semantics).
///
/// If an entry with the same (gen, logical key) is already present (a benign
/// publish race between two builders), the existing entry wins and is
/// returned so racers converge on one allocation.
pub fn insert(
    key: L2Key,
    gen: u64,
    entry: L2Value,
    bytes: usize,
    mut matches: impl FnMut(&(dyn Any + Send + Sync)) -> bool,
) -> L2Value {
    let shard = shard_of(&key);
    let mut map = shard.map.lock().unwrap();
    let bucket = map.entry(key).or_insert_with(|| Bucket { entries: Vec::new() });

    if let Some((_, existing, _)) = bucket
        .entries
        .iter()
        .find(|(g, e, _)| *g == gen && matches(e.as_ref()))
    {
        return Arc::clone(existing);
    }

    bucket.entries.push((gen, Arc::clone(&entry), bytes));
    ENTRIES.fetch_add(1, Ordering::Relaxed);
    BYTES.fetch_add(bytes, Ordering::Relaxed);
    INSERTS.fetch_add(1, Ordering::Relaxed);
    shard.count.fetch_add(1, Ordering::Relaxed);

    // Prune this logical key down to its two newest generations.
    let mut newest = 0u64;
    let mut second = 0u64;
    for (g, e, _) in bucket.entries.iter() {
        if matches(e.as_ref()) {
            if *g > newest {
                second = newest;
                newest = *g;
            } else if *g > second && *g < newest {
                second = *g;
            }
        }
    }
    bucket.entries.retain(|(g, e, sz)| {
        let keep = !matches(e.as_ref()) || *g == newest || *g == second;
        if !keep {
            ENTRIES.fetch_sub(1, Ordering::Relaxed);
            BYTES.fetch_sub(*sz, Ordering::Relaxed);
            shard.count.fetch_sub(1, Ordering::Relaxed);
        }
        keep
    });

    // Enforce the global entry budget. Same-key pruning above only reclaims
    // superseded generations of THIS logical key; without a hard cap, entries
    // whose keys are never re-inserted (attacker-driven negative lookups,
    // generation-stranded entries of other domains) accumulate forever. Evict
    // arbitrary entries from this shard until it is back within its share of
    // the budget — a miss simply re-fetches from the underlying cache/catalog.
    let budget = shard_budget();
    if budget != 0 && shard.count.load(Ordering::Relaxed) > budget {
        let (n, b) = evict_shard(&mut map, &shard.count, budget);
        ENTRIES.fetch_sub(n, Ordering::Relaxed);
        BYTES.fetch_sub(b, Ordering::Relaxed);
    }
    entry
}

/// Test/debug: drop every entry (does not touch generations or views).
pub fn clear_all() {
    for s in shards().iter() {
        let mut map = s.map.lock().unwrap();
        for (_, b) in map.iter() {
            for (_, _, sz) in b.entries.iter() {
                ENTRIES.fetch_sub(1, Ordering::Relaxed);
                BYTES.fetch_sub(*sz, Ordering::Relaxed);
                s.count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        map.clear();
    }
}

// ---------------------------------------------------------------------------
// Per-key build gates (thundering-herd fix)
// ---------------------------------------------------------------------------

struct GateCell {
    owner: std::thread::ThreadId,
    state: Mutex<bool>, // done
    cv: Condvar,
}

fn gates() -> &'static Mutex<HashMap<(L2Key, u64), Arc<GateCell>>> {
    static GATES: std::sync::OnceLock<Mutex<HashMap<(L2Key, u64), Arc<GateCell>>>> =
        std::sync::OnceLock::new();
    GATES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub enum GateOutcome {
    /// This thread builds; publish, then drop the guard (wakes waiters).
    Owner(GateGuard),
    /// The gate owner finished; re-check L2.
    Waited,
    /// The bounded wait expired without the owner finishing (possible
    /// undetected deadlock): fall back to a private build, do not retry.
    TimedOut,
    /// Re-entered while owning this key's gate (recursive build): build
    /// privately, do not wait.
    Recursive,
}

pub struct GateGuard {
    key: (L2Key, u64),
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        let cell = gates().lock().unwrap().remove(&self.key);
        if let Some(cell) = cell {
            *cell.state.lock().unwrap() = true;
            cell.cv.notify_all();
        }
    }
}

/// Serialize builders of (key, gen). The wait is bounded (the gate is
/// invisible to the deadlock detector, and the owner may block on a
/// heavyweight lock a waiter holds): on timeout the waiter proceeds with a
/// private build — correct, merely duplicated work.
pub fn acquire_gate(key: L2Key, gen: u64) -> GateOutcome {
    let me = std::thread::current().id();
    let cell = {
        let mut g = gates().lock().unwrap();
        match g.get(&(key, gen)) {
            Some(cell) => {
                if cell.owner == me {
                    return GateOutcome::Recursive;
                }
                Arc::clone(cell)
            }
            None => {
                let cell = Arc::new(GateCell {
                    owner: me,
                    state: Mutex::new(false),
                    cv: Condvar::new(),
                });
                g.insert((key, gen), cell);
                return GateOutcome::Owner(GateGuard { key: (key, gen) });
            }
        }
    };
    HERD_WAITS.fetch_add(1, Ordering::Relaxed);
    let mut done = cell.state.lock().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !*done {
        let now = std::time::Instant::now();
        if now >= deadline {
            break;
        }
        let (guard, _timeout) = cell.cv.wait_timeout(done, deadline - now).unwrap();
        done = guard;
    }
    // Distinguish 'owner finished' from 'genuine timeout': the latter must not
    // be retried forever (the gate is invisible to the deadlock detector), so
    // callers fall back to a private build.
    if *done {
        GateOutcome::Waited
    } else {
        GateOutcome::TimedOut
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn out_of_range_cache_id_is_inert() {
        // ids arrive from WAL/2PC/sinval: 96..=127 decode but exceed CAT_DOMAINS.
        for id in [96, 100, 127] {
            super::bump(super::Domain::Cat(id));
            super::sync_view(super::Domain::Cat(id));
            let _ = super::current_gen(super::Domain::Cat(id));
        }
    }

    use super::*;

    #[derive(Debug)]
    struct E(u32);

    fn is_e(v: u32) -> impl FnMut(&(dyn Any + Send + Sync)) -> bool {
        move |a| a.downcast_ref::<E>().is_some_and(|e| e.0 == v)
    }

    fn key(id: u32) -> L2Key {
        L2Key { kind: KIND_CAT, id, db: 5, hash: id }
    }

    #[test]
    fn lookup_is_generation_exact() {
        let k = key(1001);
        insert(k, 3, Arc::new(E(1)), 8, is_e(1));
        assert!(lookup(k, 3, is_e(1)).is_some());
        assert!(lookup(k, 2, is_e(1)).is_none(), "older view must not see newer gen");
        assert!(lookup(k, 4, is_e(1)).is_none(), "newer view must not see older gen");
    }

    #[test]
    fn insert_keeps_two_newest_generations() {
        let k = key(1002);
        insert(k, 1, Arc::new(E(7)), 8, is_e(7));
        insert(k, 2, Arc::new(E(7)), 8, is_e(7));
        insert(k, 3, Arc::new(E(7)), 8, is_e(7));
        assert!(lookup(k, 1, is_e(7)).is_none(), "gen 1 pruned");
        assert!(lookup(k, 2, is_e(7)).is_some());
        assert!(lookup(k, 3, is_e(7)).is_some());
    }

    #[test]
    fn duplicate_publish_converges() {
        let k = key(1003);
        let a = insert(k, 1, Arc::new(E(9)), 8, is_e(9));
        let b = insert(k, 1, Arc::new(E(9)), 8, is_e(9));
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn hash_collisions_are_disambiguated_by_matcher() {
        let k = key(1004);
        insert(k, 1, Arc::new(E(1)), 8, is_e(1));
        insert(k, 1, Arc::new(E(2)), 8, is_e(2));
        assert!(lookup(k, 1, is_e(1)).is_some());
        assert!(lookup(k, 1, is_e(2)).is_some());
        assert!(lookup(k, 1, is_e(3)).is_none());
    }

    #[test]
    fn view_lags_until_sync() {
        let d = Domain::Cat(90); // slot untouched by other tests
        let v0 = view_gen(d);
        let g = gen_slot(d).fetch_add(1, Ordering::SeqCst) + 1; // foreign bump
        assert_eq!(view_gen(d), v0, "view must not advance before sync");
        sync_view(d);
        assert_eq!(view_gen(d), g);
    }

    #[test]
    fn bump_advances_own_view() {
        let d = Domain::Rel(0xF000_0001);
        bump(d);
        assert_eq!(view_gen(d), current_gen(d));
    }

    #[test]
    fn gate_owner_then_waiter() {
        let k = key(1005);
        let GateOutcome::Owner(guard) = acquire_gate(k, 1) else {
            panic!("first acquire must own");
        };
        // Same thread, same key: recursive.
        assert!(matches!(acquire_gate(k, 1), GateOutcome::Recursive));
        let t = {
            let k = k;
            std::thread::spawn(move || matches!(acquire_gate(k, 1), GateOutcome::Waited))
        };
        std::thread::sleep(Duration::from_millis(50));
        drop(guard); // wakes the waiter
        assert!(t.join().unwrap());
        // Gate is gone; next acquire owns again.
        assert!(matches!(acquire_gate(k, 1), GateOutcome::Owner(_)));
    }

    #[test]
    fn evict_shard_bounds_a_shard_to_its_budget() {
        // Simulate an attacker publishing many distinct-key negative entries
        // into one shard. Eviction must bring it back to budget and report the
        // exact deltas, with `count` kept consistent.
        let mut map: HashMap<L2Key, Bucket> = HashMap::new();
        let count = AtomicUsize::new(0);
        const N: usize = 500;
        const BYTES_EACH: usize = 300;
        for i in 0..N as u32 {
            // Distinct logical keys -> distinct buckets (no same-key pruning),
            // exactly the unbounded-growth vector.
            let k = L2Key { kind: KIND_CAT, id: 7, db: 1, hash: i };
            map.entry(k)
                .or_insert_with(|| Bucket { entries: Vec::new() })
                .entries
                .push((1, Arc::new(E(i)) as L2Value, BYTES_EACH));
            count.fetch_add(1, Ordering::Relaxed);
        }
        assert_eq!(count.load(Ordering::Relaxed), N);

        let budget = 64;
        let (n_removed, bytes_removed) = evict_shard(&mut map, &count, budget);
        assert_eq!(n_removed, N - budget, "must evict down to exactly budget");
        assert_eq!(bytes_removed, (N - budget) * BYTES_EACH);
        assert_eq!(count.load(Ordering::Relaxed), budget);
        let live: usize = map.values().map(|b| b.entries.len()).sum();
        assert_eq!(live, budget, "live entries match the tracked count");

        // Already within budget: a no-op.
        let (n2, b2) = evict_shard(&mut map, &count, budget);
        assert_eq!((n2, b2), (0, 0));
        assert_eq!(count.load(Ordering::Relaxed), budget);
    }

    #[test]
    fn shard_budget_covers_the_configured_cap() {
        // Per-shard budgets must sum to at least the global cap so the cap is
        // never under-enforced, and 0 (disabled) must propagate.
        let b = shard_budget();
        if l2_max_entries() == 0 {
            assert_eq!(b, 0);
        } else {
            assert!(b * SHARDS >= l2_max_entries());
            assert!(b >= 1);
        }
    }

    #[test]
    fn gate_wait_times_out() {
        let k = key(1006);
        let GateOutcome::Owner(_guard) = acquire_gate(k, 2) else {
            panic!("first acquire must own");
        };
        let t = std::thread::spawn(move || {
            let started = std::time::Instant::now();
            let out = acquire_gate(k, 2);
            (matches!(out, GateOutcome::TimedOut), started.elapsed())
        });
        let (waited, dur) = t.join().unwrap();
        assert!(waited);
        assert!(dur >= Duration::from_secs(2));
    }

    // A catcache bump on an opclass-family syscache moves every relcache
    // stripe (the shared core's index-access arrays come from those rows).
    #[test]
    fn opclass_catcache_bump_moves_every_rel_stripe() {
        let before = rel_gen_snapshot();
        bump(Domain::Cat(5)); // AMPROCNUM
        let after = rel_gen_snapshot();
        assert!(before.iter().zip(&after).all(|(b, a)| a > b));
        let before = rel_gen_snapshot();
        bump(Domain::Cat(0)); // an unrelated cache: rel stripes untouched
        assert_eq!(before, rel_gen_snapshot());
    }
}
