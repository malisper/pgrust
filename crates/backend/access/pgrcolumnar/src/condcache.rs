//! Condition cache (pgrust.condition_cache): a bounded in-memory cache of
//! per-window staged-qual survivor bitmaps over IMMUTABLE pgrcolumnar part
//! state — the ClickHouse QueryConditionCache mechanism rebuilt on pgrcolumnar
//! vocabulary. Hot re-executions of the same PREWHERE prefix over the same
//! sealed part serve the selection bitmap from memory and skip the qual's
//! decode + evaluation legs entirely; surviving windows still complete their
//! deform for downstream consumers, so a hit is byte-identical to a miss BY
//! CONSTRUCTION (the cached bits are exactly the bits the miss path stored,
//! the staged prefix is deterministic and non-erroring by lane admission,
//! and the requal tail re-runs per surviving row either way).
//!
//! KEY: (PartIdent, qual fingerprint, row group, window width).
//!   - PartIdent = (st_dev, st_ino, st_size, footer_off) captured at Part
//!     open — the part-cache staleness probe's exact vocabulary. pgrcolumnar
//!     parts are immutable once sealed: every COPY publish grows
//!     len/footer_off, every DROP/TRUNCATE/reingest recreates the inode, so
//!     a stale bitmap is unreachable through a live Part (its identity names
//!     content that no longer exists under any current identity).
//!   - fingerprint = laneexec's canonical 128-bit prefix hash (operator fn
//!     oid, commutation, collation, column, const value bytes; see
//!     translate::fingerprint_prefix). Volatile/param/subplan quals never
//!     reach it (walker + whitelist refusals), regex dict clauses refuse.
//! VALUE: per window slot (fixed window_rows stride within the RG), the
//! staged prefix's survivor bit words, published by a per-slot `known` bit
//! (Release; readers Acquire). Slots race only between parallel workers of
//! identical plans computing IDENTICAL bits over immutable data, so the
//! OR-publish is exact.
//!
//! MEMORY: entries charge their allocation against a global byte budget
//! (pgrust.condition_cache_size); inserts evict least-recently-used entries
//! (whole RG entries — the map stays small: one entry per (part, qual, RG)).
//! An entry over budget is handed back UNREGISTERED: the scan still memoizes
//! within itself, nothing persists.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Immutable part identity (see module doc). The all-zero identity is the
/// "unknown" sentinel — `get_or_insert` refuses to key on it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PartIdent {
    pub dev: u64,
    pub ino: u64,
    pub len: u64,
    pub footer_off: u64,
}

impl PartIdent {
    fn is_null(&self) -> bool {
        self.dev == 0 && self.ino == 0 && self.len == 0 && self.footer_off == 0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Key {
    part: PartIdent,
    fp: u128,
    rg: u32,
    window_rows: u32,
}

/// One row group's cached verdicts for one (part, qual, window-width).
pub struct RgEntry {
    // Slot s covers rows [s*window_rows, (s+1)*window_rows) of the RG's
    // granule-relative layout; its `wpw` words live at bits[s*wpw..].
    bits: Box<[AtomicU64]>,
    // Bit s of known[s/64] publishes slot s (fetch_or Release after the
    // value words; readers Acquire).
    known: Box<[AtomicU64]>,
    wpw: u32,
    nslots: u32,
    bytes: usize,
    last_used: AtomicU64,
}

impl RgEntry {
    fn new(nrows: u32, window_rows: u32) -> Arc<RgEntry> {
        let nslots = (nrows as usize).div_ceil(window_rows as usize);
        let wpw = (window_rows as usize).div_ceil(64);
        let nbits = nslots * wpw;
        let nknown = nslots.div_ceil(64);
        let bytes = (nbits + nknown) * 8 + std::mem::size_of::<RgEntry>();
        Arc::new(RgEntry {
            bits: (0..nbits).map(|_| AtomicU64::new(0)).collect(),
            known: (0..nknown).map(|_| AtomicU64::new(0)).collect(),
            wpw: wpw as u32,
            nslots: nslots as u32,
            bytes,
            last_used: AtomicU64::new(0),
        })
    }

    /// Serve slot `slot`'s bits into `sel` (whole words; the caller's live
    /// width is <= wpw*64 by the slot contract). false = not yet known.
    pub fn lookup(&self, slot: u32, sel: &mut [u64]) -> bool {
        if slot >= self.nslots {
            return false;
        }
        let w = (slot / 64) as usize;
        let bit = 1u64 << (slot % 64);
        if self.known[w].load(Ordering::Acquire) & bit == 0 {
            return false;
        }
        let base = (slot * self.wpw) as usize;
        let n = sel.len().min(self.wpw as usize);
        for (i, s) in sel[..n].iter_mut().enumerate() {
            *s = self.bits[base + i].load(Ordering::Relaxed);
        }
        for s in sel[n..].iter_mut() {
            *s = 0;
        }
        true
    }

    /// Record slot `slot`'s bits (`nwords` live words of `sel`; bits past
    /// the staged row count are zero by the caller's bitmap-init contract).
    pub fn store(&self, slot: u32, sel: &[u64], nwords: usize) {
        if slot >= self.nslots {
            return;
        }
        let base = (slot * self.wpw) as usize;
        for (i, &w) in sel[..nwords.min(self.wpw as usize)].iter().enumerate() {
            if w != 0 {
                self.bits[base + i].fetch_or(w, Ordering::Relaxed);
            }
        }
        self.known[(slot / 64) as usize].fetch_or(1u64 << (slot % 64), Ordering::Release);
    }
}

struct Inner {
    map: HashMap<Key, Arc<RgEntry>>,
    bytes: usize,
    gen: u64,
}

static CACHE: OnceLock<Mutex<Inner>> = OnceLock::new();
// Byte budget; set from the GUC at every scan arm (pgrust.condition_cache_size).
static CAPACITY: AtomicU64 = AtomicU64::new(100 * 1024 * 1024);
// pgstat-style counters (lane stats dump vocabulary): window-granular
// hits/misses, RG-entry-granular insertions/evictions.
static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);
static INSERTIONS: AtomicU64 = AtomicU64::new(0);
static EVICTIONS: AtomicU64 = AtomicU64::new(0);

fn cache() -> &'static Mutex<Inner> {
    CACHE.get_or_init(|| Mutex::new(Inner { map: HashMap::new(), bytes: 0, gen: 0 }))
}

pub fn set_capacity(bytes: u64) {
    CAPACITY.store(bytes, Ordering::Relaxed);
}

pub fn count_hit() {
    HITS.fetch_add(1, Ordering::Relaxed);
}

pub fn count_miss() {
    MISSES.fetch_add(1, Ordering::Relaxed);
}

/// Kill switch: PGRUST_CONDCACHE_SHARED_STATS=1 restores the eager
/// per-window fetch_add on the shared statics (pre-lane behavior).
fn shared_stats() -> bool {
    static EAGER: OnceLock<bool> = OnceLock::new();
    *EAGER.get_or_init(|| std::env::var_os("PGRUST_CONDCACHE_SHARED_STATS").is_some_and(|v| v == "1"))
}

/// Per-scan hit/miss stat cells. The process counters are observational
/// only (eviction is last_used-LRU, admission is the byte budget — neither
/// reads hits/misses), but the per-window `count_hit` fetch_add on the
/// shared statics was a 16-worker cache-line ping-pong worth 88% of the selective-qual grouped shape's
/// cycles at 100M mt16 (suite-profile census U7). Each scan counts in its
/// own plain cells and folds into the globals once at teardown (`Drop`) or
/// at an explicit stats read (`fold`).
#[derive(Default)]
pub struct LocalStats {
    hits: u64,
    misses: u64,
}

impl LocalStats {
    #[inline]
    pub fn count(&mut self, hit: bool) {
        if shared_stats() {
            if hit {
                count_hit();
            } else {
                count_miss();
            }
        } else if hit {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
    }

    /// Publish the local counts into the process counters and zero the
    /// cells (idempotent — safe to call before drop).
    pub fn fold(&mut self) {
        if self.hits > 0 {
            HITS.fetch_add(self.hits, Ordering::Relaxed);
            self.hits = 0;
        }
        if self.misses > 0 {
            MISSES.fetch_add(self.misses, Ordering::Relaxed);
            self.misses = 0;
        }
    }
}

impl Drop for LocalStats {
    fn drop(&mut self) {
        self.fold();
    }
}

/// (hits, misses, insertions, evictions) — cumulative process counters.
pub fn stats() -> (u64, u64, u64, u64) {
    (
        HITS.load(Ordering::Relaxed),
        MISSES.load(Ordering::Relaxed),
        INSERTIONS.load(Ordering::Relaxed),
        EVICTIONS.load(Ordering::Relaxed),
    )
}

/// Fetch-or-create the RG entry for one (part, qual, rg, window width). The
/// returned Arc serves every window of the RG lock-free; the global lock is
/// taken once per (scan, RG). An entry that cannot fit the budget (or a null
/// part identity) is returned UNREGISTERED — a scan-local memo, nothing
/// cached across queries.
pub fn get_or_insert(
    part: PartIdent,
    fp: u128,
    rg: u32,
    window_rows: u32,
    nrows: u32,
) -> Arc<RgEntry> {
    if part.is_null() {
        return RgEntry::new(nrows, window_rows);
    }
    let key = Key { part, fp, rg, window_rows };
    let mut inner = cache().lock().expect("condition cache lock poisoned");
    inner.gen += 1;
    let gen = inner.gen;
    if let Some(e) = inner.map.get(&key) {
        e.last_used.store(gen, Ordering::Relaxed);
        return Arc::clone(e);
    }
    let entry = RgEntry::new(nrows, window_rows);
    entry.last_used.store(gen, Ordering::Relaxed);
    let cap = CAPACITY.load(Ordering::Relaxed) as usize;
    if entry.bytes > cap {
        return entry; // unregistered scan-local memo
    }
    // LRU eviction to fit: the map holds whole-RG entries (one per
    // (part, qual, RG)) so it stays small; a min-scan per eviction is cheap.
    while inner.bytes + entry.bytes > cap {
        let Some((victim, vbytes)) = inner
            .map
            .iter()
            .min_by_key(|(_, e)| e.last_used.load(Ordering::Relaxed))
            .map(|(k, e)| (*k, e.bytes))
        else {
            break;
        };
        inner.map.remove(&victim);
        inner.bytes -= vbytes;
        EVICTIONS.fetch_add(1, Ordering::Relaxed);
    }
    inner.bytes += entry.bytes;
    inner.map.insert(key, Arc::clone(&entry));
    INSERTIONS.fetch_add(1, Ordering::Relaxed);
    entry
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(i: u64) -> PartIdent {
        PartIdent { dev: 1, ino: i, len: 4096, footer_off: 64 }
    }

    // The capacity knob and counters are process-global: tests that set or
    // read them serialize here (the harness runs tests concurrently).
    fn capacity_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn store_then_lookup_roundtrip_and_unknown_slots_miss() {
        let e = RgEntry::new(1000, 256);
        assert_eq!(e.nslots, 4);
        let mut out = [u64::MAX; 5];
        assert!(!e.lookup(0, &mut out));
        let sel = [0xdead_beef_0000_0001, 0, 0x8000_0000_0000_0000, 0xff];
        e.store(2, &sel, 4);
        assert!(!e.lookup(0, &mut out) && !e.lookup(1, &mut out) && !e.lookup(3, &mut out));
        assert!(e.lookup(2, &mut out));
        assert_eq!(&out[..4], &sel);
        assert_eq!(out[4], 0, "caller words past wpw are zeroed");
        // Out-of-range slots are inert.
        assert!(!e.lookup(99, &mut out));
        e.store(99, &sel, 4);
    }

    #[test]
    fn same_key_shares_entry_and_distinct_keys_do_not() {
        let _g = capacity_lock();
        set_capacity(100 * 1024 * 1024);
        let a = get_or_insert(ident(7001), 42, 0, 256, 8192);
        let b = get_or_insert(ident(7001), 42, 0, 256, 8192);
        assert!(Arc::ptr_eq(&a, &b));
        // Different qual fingerprint, rg, window width, and part identity
        // each mint a separate entry (no false sharing).
        for other in [
            get_or_insert(ident(7001), 43, 0, 256, 8192),
            get_or_insert(ident(7001), 42, 1, 256, 8192),
            get_or_insert(ident(7001), 42, 0, 128, 8192),
            get_or_insert(ident(7002), 42, 0, 256, 8192),
        ] {
            assert!(!Arc::ptr_eq(&a, &other));
        }
    }

    #[test]
    fn eviction_respects_capacity_and_lru_order() {
        let _g = capacity_lock();
        // Each 8192-row/256-window entry: 32 slots * 4 words + 1 known word
        // = 129 words = 1032 bytes + header. Budget for ~2 entries.
        let per = RgEntry::new(8192, 256).bytes;
        set_capacity((2 * per + per / 2) as u64);
        let a = get_or_insert(ident(8001), 1, 0, 256, 8192);
        let _b = get_or_insert(ident(8001), 1, 1, 256, 8192);
        // Touch a so rg=1 is the LRU victim when rg=2 inserts.
        let a2 = get_or_insert(ident(8001), 1, 0, 256, 8192);
        assert!(Arc::ptr_eq(&a, &a2));
        let _c = get_or_insert(ident(8001), 1, 2, 256, 8192);
        let a3 = get_or_insert(ident(8001), 1, 0, 256, 8192);
        assert!(Arc::ptr_eq(&a, &a3), "recently-used entry survived eviction");
        let (_, _, ins_before, _) = stats();
        let b2 = get_or_insert(ident(8001), 1, 1, 256, 8192);
        let (_, _, ins_after, _) = stats();
        assert_eq!(ins_after, ins_before + 1, "evicted entry re-inserts fresh");
        let mut out = [0u64; 4];
        assert!(!b2.lookup(0, &mut out), "re-inserted entry starts empty");
        // Restore the default so co-resident tests keep caching.
        set_capacity(100 * 1024 * 1024);
    }

    #[test]
    fn local_stats_stay_scan_local_until_fold_and_drop_folds_remainder() {
        // Serialized with the other counter-touching tests; HITS/MISSES are
        // only ever bumped by this test within the crate's unit suite, so
        // deltas are exact under the lock.
        let _g = capacity_lock();
        let (h0, m0, _, _) = stats();
        let mut ls = LocalStats::default();
        ls.count(true);
        ls.count(true);
        ls.count(false);
        let (h1, m1, _, _) = stats();
        assert_eq!((h1, m1), (h0, m0), "counts stay scan-local until fold");
        ls.fold();
        ls.fold(); // idempotent
        let (h2, m2, _, _) = stats();
        assert_eq!((h2 - h0, m2 - m0), (2, 1));
        ls.count(false);
        drop(ls);
        let (h3, m3, _, _) = stats();
        assert_eq!((h3 - h0, m3 - m0), (2, 2), "drop folds the remainder");
    }

    #[test]
    fn over_budget_and_null_ident_entries_stay_unregistered() {
        let _g = capacity_lock();
        set_capacity(16);
        let a = get_or_insert(ident(9001), 5, 0, 256, 8192);
        let b = get_or_insert(ident(9001), 5, 0, 256, 8192);
        assert!(!Arc::ptr_eq(&a, &b), "over-budget entries are scan-local");
        set_capacity(100 * 1024 * 1024);
        let n1 = get_or_insert(PartIdent { dev: 0, ino: 0, len: 0, footer_off: 0 }, 5, 0, 256, 64);
        let n2 = get_or_insert(PartIdent { dev: 0, ino: 0, len: 0, footer_off: 0 }, 5, 0, 256, 64);
        assert!(!Arc::ptr_eq(&n1, &n2), "null part identity never keys the cache");
    }
}
