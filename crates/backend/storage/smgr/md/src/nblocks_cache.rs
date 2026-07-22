//! Process-global relation-size cache: (RelFileLocator, fork) -> nblocks.
//!
//! C cannot have this: its smgr_cached_nblocks is per-backend and trusted
//! only during recovery, because cross-process coherence of size changes was
//! never solved (only the recovery-only cache landed upstream). One address
//! space changes the game: every in-process mutation of a relation file's
//! size flows through md (extend / zeroextend / truncate / create / unlink),
//! so updating the map at those points under the callers' existing locking
//! makes the cached value definitionally what lseek(SEEK_END) would report.
//! The one out-of-md size mutator (the columnar AM's direct-pwrite byte
//! stream) POISONS its key at writer open: poisoned keys always take the
//! real lseek walk, permanently, until the entry dies at unlink.
//!
//! Contract: `lookup` returns exactly what the mdnblocks segment walk would
//! return at every call site, or None (walk and `note_walked`). Temp
//! relations never enter the map (per-session files; the callers keep the
//! plain walk). Entries are removed at unlink/create (relfilenumber reuse
//! starts clean), purged by database at DROP/move DATABASE (file trees are
//! rm'd behind md's back there), and cleared wholesale around the unlogged-
//! relation reinit pass (init-fork copies rewrite main forks outside md).
//!
//! Concurrency: one read-mostly RwLock over a BTreeMap. Readers take the
//! shared lock for a point lookup; writers (rare: extension, truncate, DDL)
//! take it exclusively. Extension updates are monotonic max() so any
//! interleaving with a concurrent walk-populate converges on the true size;
//! truncation and other exact writes happen under the callers' exclusive
//! relation locking, exactly like the file operations they mirror.

use std::collections::BTreeMap;
use pgsync::RwLock;

use ::types_core::primitive::{BlockNumber, ForkNumber};
use ::types_core::Oid;
use ::types_storage::RelFileLocator;

/// Sentinel value: this key's file is mutated outside md (columnar direct
/// io); never cache, always walk. Distinct from any real size (nblocks is
/// a u32).
const POISON: u64 = u64::MAX;

// OnceLock-mediated init (pgsync loom papering law: no statics hold loom
// types — loom's RwLock::new is not const; std/native worlds are identical
// after the first deref). t44 composition fix for the blocking loom gate.
fn cache() -> &'static RwLock<BTreeMap<u128, u64>> {
    static CACHE: pgsync::OnceLock<RwLock<BTreeMap<u128, u64>>> = pgsync::OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// dbOid in the top bits so a whole database is one contiguous key range.
#[inline]
fn key(locator: RelFileLocator, forknum: ForkNumber) -> u128 {
    ((locator.dbOid as u128) << 96)
        | ((locator.spcOid as u128) << 64)
        | ((locator.relNumber as u128) << 32)
        | forknum as u128
}

#[inline]
fn db_range(db: Oid) -> (u128, u128) {
    let lo = (db as u128) << 96;
    let hi = lo | ((1u128 << 96) - 1);
    (lo, hi)
}

/// The read hook: Some(nblocks) iff cached and not poisoned.
pub fn lookup(locator: RelFileLocator, forknum: ForkNumber) -> Option<BlockNumber> {
    let g = cache().read().unwrap();
    match g.get(&key(locator, forknum)) {
        Some(&v) if v != POISON => Some(v as BlockNumber),
        _ => None,
    }
}

/// Populate after a real segment walk. Poison is sticky.
pub fn note_walked(locator: RelFileLocator, forknum: ForkNumber, nblocks: BlockNumber) {
    let mut g = cache().write().unwrap();
    let e = g.entry(key(locator, forknum)).or_insert(nblocks as u64);
    if *e != POISON {
        *e = nblocks as u64;
    }
}

/// Extension: the file now reaches at least `new_min` blocks. Only refreshes
/// an existing entry (absent stays absent — the next read walks); max() so a
/// stale smaller value can never overwrite a newer larger one.
pub fn extend_to(locator: RelFileLocator, forknum: ForkNumber, new_min: BlockNumber) {
    let mut g = cache().write().unwrap();
    if let Some(e) = g.get_mut(&key(locator, forknum)) {
        if *e != POISON && *e < new_min as u64 {
            *e = new_min as u64;
        }
    }
}

/// Truncation landed: the size is exactly `nblocks` now (caller holds the
/// exclusive relation lock the truncate itself required). Poison is sticky.
pub fn set_exact(locator: RelFileLocator, forknum: ForkNumber, nblocks: BlockNumber) {
    let mut g = cache().write().unwrap();
    let e = g.entry(key(locator, forknum)).or_insert(nblocks as u64);
    if *e != POISON {
        *e = nblocks as u64;
    }
}

/// A size-changing op errored partway: forget the value (next read walks the
/// real file). Poison survives.
pub fn invalidate(locator: RelFileLocator, forknum: ForkNumber) {
    let mut g = cache().write().unwrap();
    if let Some(&v) = g.get(&key(locator, forknum)) {
        if v != POISON {
            g.remove(&key(locator, forknum));
        }
    }
}

/// Unlink/create: the file identity is gone (or brand new) — drop the entry
/// entirely, poison included. A reused relfilenumber starts clean; a
/// columnar writer re-poisons at its next open.
pub fn remove(locator: RelFileLocator, forknum: ForkNumber) {
    cache().write().unwrap().remove(&key(locator, forknum));
}

/// Mark this fork as mutated outside md: never serve it from the cache.
pub fn poison(locator: RelFileLocator, forknum: ForkNumber) {
    cache().write().unwrap().insert(key(locator, forknum), POISON);
}

/// DROP/move DATABASE removes file trees with rmtree, not per-rel unlinks.
pub fn purge_db(db: Oid) {
    let (lo, hi) = db_range(db);
    let mut g = cache().write().unwrap();
    let doomed: Vec<u128> = g.range(lo..=hi).map(|(&k, _)| k).collect();
    for k in doomed {
        g.remove(&k);
    }
}

/// The unlogged-relation reinit pass copies init forks over main forks at
/// the file level, outside md — drop everything (startup-time, rare).
pub fn clear() {
    cache().write().unwrap().clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rl(rel: u32) -> RelFileLocator {
        RelFileLocator::new(1663, 5, rel)
    }
    const MAIN: ForkNumber = ForkNumber::MAIN_FORKNUM;

    // The map is process-global; tests share it. Each test uses its own
    // relNumber band so parallel test threads never alias.

    #[test]
    fn walk_then_lookup_roundtrip() {
        assert_eq!(lookup(rl(9_000_001), MAIN), None);
        note_walked(rl(9_000_001), MAIN, 42);
        assert_eq!(lookup(rl(9_000_001), MAIN), Some(42));
    }

    #[test]
    fn extend_is_monotonic_and_needs_an_entry() {
        extend_to(rl(9_000_002), MAIN, 7);
        assert_eq!(lookup(rl(9_000_002), MAIN), None, "absent stays absent");
        note_walked(rl(9_000_002), MAIN, 10);
        extend_to(rl(9_000_002), MAIN, 12);
        assert_eq!(lookup(rl(9_000_002), MAIN), Some(12));
        extend_to(rl(9_000_002), MAIN, 11);
        assert_eq!(lookup(rl(9_000_002), MAIN), Some(12), "max() semantics");
    }

    #[test]
    fn truncate_sets_exact_and_invalidate_forgets() {
        note_walked(rl(9_000_003), MAIN, 100);
        set_exact(rl(9_000_003), MAIN, 3);
        assert_eq!(lookup(rl(9_000_003), MAIN), Some(3));
        invalidate(rl(9_000_003), MAIN);
        assert_eq!(lookup(rl(9_000_003), MAIN), None);
    }

    #[test]
    fn poison_is_sticky_until_remove() {
        poison(rl(9_000_004), MAIN);
        assert_eq!(lookup(rl(9_000_004), MAIN), None);
        note_walked(rl(9_000_004), MAIN, 5);
        set_exact(rl(9_000_004), MAIN, 6);
        extend_to(rl(9_000_004), MAIN, 7);
        invalidate(rl(9_000_004), MAIN);
        assert_eq!(lookup(rl(9_000_004), MAIN), None, "poison survives all");
        remove(rl(9_000_004), MAIN);
        note_walked(rl(9_000_004), MAIN, 5);
        assert_eq!(lookup(rl(9_000_004), MAIN), Some(5), "remove clears poison");
    }

    #[test]
    fn purge_db_takes_only_that_database() {
        let mine = RelFileLocator::new(1663, 77001, 9_000_005);
        let other = RelFileLocator::new(1663, 77002, 9_000_006);
        note_walked(mine, MAIN, 1);
        note_walked(other, MAIN, 2);
        purge_db(77001);
        assert_eq!(lookup(mine, MAIN), None);
        assert_eq!(lookup(other, MAIN), Some(2));
    }

    #[test]
    fn forks_are_independent_keys() {
        let l = rl(9_000_007);
        note_walked(l, ForkNumber::MAIN_FORKNUM, 8);
        note_walked(l, ForkNumber::FSM_FORKNUM, 3);
        assert_eq!(lookup(l, ForkNumber::MAIN_FORKNUM), Some(8));
        assert_eq!(lookup(l, ForkNumber::FSM_FORKNUM), Some(3));
        remove(l, ForkNumber::FSM_FORKNUM);
        assert_eq!(lookup(l, ForkNumber::MAIN_FORKNUM), Some(8));
        assert_eq!(lookup(l, ForkNumber::FSM_FORKNUM), None);
    }
}
