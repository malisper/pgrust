//! The process-shared part registry: parsed-metadata sharing + the budgeted
//! part cache with pins and the inline LRU janitor (charter §1 "sharing is
//! day-one architecture"; spec §11 "equal identity ⇒ identical bytes").
//!
//! - **Key**: `(dev, ino, len)` — the stat-only identity prefix, so a cache
//!   hit costs one fstat and ZERO content reads (the stat-before-read
//!   discipline). The full §11 quadruple (+ uuid) lives on the entry;
//!   sealed-part immutability means one `(dev, ino, len)` can only ever name
//!   one byte image (the ino-reuse hole is closed by the AM integration via
//!   [`PartRegistry::invalidate`] — the named M3-G/H seam).
//! - **Pins are law**: a pinned part is NEVER evicted. Budget is capacity
//!   guidance — the janitor evicts least-recently-used unpinned parts until
//!   under budget; if everything left is pinned the cache runs over budget
//!   and counts the event (witness).
//! - **The janitor is inline** (at insert and [`PartRegistry::maintain`]),
//!   never a thread (no new thread populations), and LRU time is a logical
//!   clock — never wall time.
//! - **Eviction is a map removal, not a free**: in-flight `Arc<OpenPart>`
//!   holders (scans, dict handles) keep their part alive and
//!   generation-stable; its memory becomes the holder's working set and
//!   stops counting against the registry budget.

use std::collections::BTreeMap;
use std::sync::Arc;

use pgsync::atomic::{AtomicU64, Ordering};
use pgsync::Mutex;

use crate::io::PartIo;
use crate::openpart::{OpenPart, PartExpect};
use crate::ReadResult;

/// Stat-only registry key: `(dev, ino, len)`.
pub type PartKey = (u64, u64, u64);

/// Default cap on the number of live cache entries (charter §1 "sharing"
/// budget is guidance, not law). Each entry pins one raw kernel fd for its
/// whole life (`VfsPartIo`, outside the VFD EMFILE-LRU pool), so the byte
/// budget alone does NOT bound descriptor use: an entry whose part was opened
/// but never decoded has `resident()==0` and exerts zero budget pressure, so a
/// stream of such entries (e.g. scans that fetch no rows, or self-scans of
/// aborted publishes) would accumulate fds without bound. A count cap makes the
/// janitor reclaim LRU-unpinned entries by entry count as well, bounding held
/// descriptors regardless of resident bytes. A miss re-resolves from the
/// manifest, so eviction is correctness-preserving.
pub const DEFAULT_MAX_ENTRIES: u64 = 4096;

struct RegState {
    parts: BTreeMap<PartKey, Arc<OpenPart>>,
}

/// Measured-only registry counters (witnesses; no benchmark semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RegistryCounters {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub over_budget_events: u64,
}

/// An RAII pin: while alive, the pinned part cannot be evicted.
pub struct PartPin {
    part: Arc<OpenPart>,
}

impl PartPin {
    /// Pin an already-shared part.
    pub fn pin(part: &Arc<OpenPart>) -> PartPin {
        part.pin_count.fetch_add(1, Ordering::Relaxed);
        PartPin { part: part.clone() }
    }

    pub fn part(&self) -> &Arc<OpenPart> {
        &self.part
    }
}

impl Clone for PartPin {
    fn clone(&self) -> PartPin {
        PartPin::pin(&self.part)
    }
}

impl Drop for PartPin {
    fn drop(&mut self) {
        self.part.pin_count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The registry. Construct once per process scope and share by reference;
/// budget is runtime-settable (the GUC seam — zero env knobs, charter §11).
pub struct PartRegistry {
    state: Mutex<RegState>,
    /// Logical LRU clock (monotone counter).
    clock: AtomicU64,
    budget: AtomicU64,
    /// Cap on live entry count — bounds held kernel fds independently of the
    /// resident-byte budget (zero-resident entries still each hold one fd).
    max_entries: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    over_budget_events: AtomicU64,
}

impl PartRegistry {
    pub fn new(budget_bytes: u64) -> PartRegistry {
        PartRegistry {
            state: Mutex::new(RegState {
                parts: BTreeMap::new(),
            }),
            clock: AtomicU64::new(1),
            budget: AtomicU64::new(budget_bytes),
            max_entries: AtomicU64::new(DEFAULT_MAX_ENTRIES),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            over_budget_events: AtomicU64::new(0),
        }
    }

    pub fn set_budget(&self, bytes: u64) {
        self.budget.store(bytes, Ordering::Relaxed);
    }

    pub fn budget(&self) -> u64 {
        self.budget.load(Ordering::Relaxed)
    }

    /// Runtime-settable cap on live entry count (the fd-bound GUC seam). A
    /// value of 0 is clamped to 1 so the cache always holds at least the entry
    /// it just opened. Lowering it makes the next janitor pass reclaim
    /// LRU-unpinned entries down to the new cap.
    pub fn set_max_entries(&self, n: u64) {
        self.max_entries.store(n.max(1), Ordering::Relaxed);
    }

    pub fn max_entries(&self) -> u64 {
        self.max_entries.load(Ordering::Relaxed)
    }

    /// Open-or-share a part, returning it pinned. `open_io` opens the file
    /// and gathers stat facts; on a registry hit the freshly opened io is
    /// dropped without a single content read.
    pub fn open_pinned<F>(&self, expect: &PartExpect, open_io: F) -> ReadResult<PartPin>
    where
        F: FnOnce() -> ReadResult<Box<dyn PartIo>>,
    {
        let io = open_io()?;
        let (dev, ino) = io.dev_ino();
        let key: PartKey = (dev, ino, io.len());
        if let Some(existing) = self.lookup_touch(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(PartPin::pin(&existing));
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let part = Arc::new(OpenPart::open(io, expect)?);
        let mut st = lock(&self.state);
        let entry = st.parts.entry(key).or_insert_with(|| part).clone();
        // (A racing open of the same identity may have inserted first — its
        // entry wins; equal identity ⇒ identical bytes makes ours a benign
        // duplicate that drops here.)
        self.touch(&entry);
        let pin = PartPin::pin(&entry);
        self.janitor(&mut st);
        Ok(pin)
    }

    /// Shared lookup without opening (no pin; touches LRU).
    pub fn get(&self, key: PartKey) -> Option<Arc<OpenPart>> {
        self.lookup_touch(key)
    }

    /// Drop the entry for `key` (relcache-invalidation seam: closes the
    /// ino-reuse hole at the AM layer). In-flight holders are unaffected.
    pub fn invalidate(&self, key: PartKey) -> bool {
        lock(&self.state).parts.remove(&key).is_some()
    }

    /// Drop every entry (DROP TABLE / testing).
    pub fn clear(&self) {
        lock(&self.state).parts.clear();
    }

    /// Run the janitor now (callers with fault-heavy phases between opens).
    pub fn maintain(&self) {
        let mut st = lock(&self.state);
        self.janitor(&mut st);
    }

    /// Total segment-cache bytes across live entries.
    pub fn resident_bytes(&self) -> u64 {
        lock(&self.state).parts.values().map(|p| p.resident()).sum()
    }

    pub fn len(&self) -> usize {
        lock(&self.state).parts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn counters(&self) -> RegistryCounters {
        RegistryCounters {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            over_budget_events: self.over_budget_events.load(Ordering::Relaxed),
        }
    }

    fn lookup_touch(&self, key: PartKey) -> Option<Arc<OpenPart>> {
        let st = lock(&self.state);
        let p = st.parts.get(&key)?.clone();
        drop(st);
        self.touch(&p);
        Some(p)
    }

    fn touch(&self, part: &Arc<OpenPart>) {
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        part.last_used.store(now, Ordering::Relaxed);
    }

    /// Evict LRU unpinned entries while over EITHER budget: resident bytes over
    /// the byte budget, OR live entry count over the entry cap. The count cap
    /// bounds held kernel fds independently of resident bytes — a zero-resident
    /// entry exerts no byte pressure but still pins one fd (and, for an unlinked
    /// part, its inode), so byte-only eviction would let such entries (aborted
    /// self-publish self-scans, zero-row scans) accumulate without bound.
    /// Deterministic: the victim is min by (last_used, key) over a BTreeMap
    /// (stable order).
    fn janitor(&self, st: &mut RegState) {
        let budget = self.budget.load(Ordering::Relaxed);
        let max_entries = self.max_entries.load(Ordering::Relaxed) as usize;
        loop {
            let total: u64 = st.parts.values().map(|p| p.resident()).sum();
            if total <= budget && st.parts.len() <= max_entries {
                return;
            }
            let victim = st
                .parts
                .iter()
                .filter(|(_, p)| p.pin_count.load(Ordering::Relaxed) == 0)
                .min_by_key(|(k, p)| (p.last_used.load(Ordering::Relaxed), **k))
                .map(|(k, _)| *k);
            match victim {
                Some(k) => {
                    st.parts.remove(&k);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
                None => {
                    // Everything resident is pinned: run over budget,
                    // witnessed.
                    self.over_budget_events.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> pgsync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}
