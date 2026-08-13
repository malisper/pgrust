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
//! COHERENCE (the TRUNCATE-under-INSERT bug, connscale validation §6b'):
//! the mutation paths write the map under their callers' exclusive locking
//! (extension lock for growth, AccessExclusive for truncate), but the WALK
//! populate path has no such lock — smgrnblocks is called from read-side
//! sites (planner size estimates, seqscan setup) holding nothing that
//! excludes a concurrent extension. A walk is therefore a measurement of the
//! PAST by publish time, and publishing it unconditionally can overwrite a
//! newer authoritative value. Post-TRUNCATE that froze the new relfilenode's
//! size at 0 (hits never re-walk; the extend that would raise it died first
//! at bufmgr's beyond-EOF collision check), killing every concurrent writer
//! until the next relfilenode swap. C is immune: md.c mdnblocks lseeks on
//! every call.
//!
//! The guard: an `epoch` lives beside the map. A walker takes a `WalkToken`
//! (epoch snapshot) BEFORE touching the filesystem — `lookup_or_begin_walk`
//! does miss-detection and snapshot under one read lock — and `note_walked`
//! publishes only if, under the write lock, (a) the key is still absent and
//! (b) the epoch is unchanged. Every size mutation either INSERTS the key
//! (extend_to on a live entry, set_exact) or BUMPS the epoch (remove,
//! invalidate, purge_db, clear, extend_to on an absent entry), and does so
//! AFTER its file operation completes, so a stale walk is always detected
//! and discarded. A discarded publish costs one more walk at the next miss;
//! epoch bumps are DDL-rate, so the steady-state hit rate is untouched.
//!
//! Concurrency: one read-mostly RwLock over (epoch, BTreeMap). Readers take
//! the shared lock for a point lookup; writers (rare: extension, truncate,
//! DDL) take it exclusively. Extension updates are monotonic max() so any
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

struct Inner {
    /// Bumped by every mutation that leaves (or could leave) a key ABSENT:
    /// remove, invalidate, purge_db, clear, extend_to-on-absent. Mutations
    /// that leave the key present don't need it — note_walked's "still
    /// absent" check already discards the racing walk.
    epoch: u64,
    map: BTreeMap<u128, u64>,
}

/// Proof that a walker snapshotted the cache state before its filesystem
/// walk began. Deliberately not Copy/Clone: one walk, one publish attempt.
pub struct WalkToken {
    epoch: u64,
}

// OnceLock-mediated init (pgsync loom papering law: no statics hold loom
// types — loom's RwLock::new is not const; std/native worlds are identical
// after the first deref). t44 composition fix for the blocking loom gate.
fn cache() -> &'static RwLock<Inner> {
    static CACHE: pgsync::OnceLock<RwLock<Inner>> = pgsync::OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(Inner { epoch: 0, map: BTreeMap::new() }))
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
    match g.map.get(&key(locator, forknum)) {
        Some(&v) if v != POISON => Some(v as BlockNumber),
        _ => None,
    }
}

/// One probe serving the walker: the cached value on a hit, or the
/// WalkToken the eventual `note_walked` needs on a miss. Taking the token
/// inside the same critical section as the miss detection is what makes
/// "the walk began no earlier than the snapshot" true by construction.
pub fn lookup_or_begin_walk(
    locator: RelFileLocator,
    forknum: ForkNumber,
) -> Result<BlockNumber, WalkToken> {
    let g = cache().read().unwrap();
    match g.map.get(&key(locator, forknum)) {
        Some(&v) if v != POISON => Ok(v as BlockNumber),
        _ => Err(WalkToken { epoch: g.epoch }),
    }
}

/// Publish a real segment walk — IF nothing moved underneath it. The walk
/// measured the file at some instant between the token snapshot and now; if
/// the key is still absent and the epoch unchanged, no size mutation landed
/// in that window (they all insert the key or bump the epoch, after their
/// file op), so the measurement is still exact. Otherwise it is history:
/// drop it and let the next miss re-walk. Poison arrives as a present key,
/// so it is preserved by the same check.
pub fn note_walked(
    locator: RelFileLocator,
    forknum: ForkNumber,
    nblocks: BlockNumber,
    token: WalkToken,
) {
    let mut g = cache().write().unwrap();
    if g.epoch == token.epoch {
        let k = key(locator, forknum);
        if let std::collections::btree_map::Entry::Vacant(e) = g.map.entry(k) {
            e.insert(nblocks as u64);
        }
    }
}

/// Extension: the file now reaches at least `new_min` blocks. Refreshes an
/// existing entry with max() so a stale smaller value can never overwrite a
/// newer larger one. On an ABSENT entry it bumps the epoch instead of
/// inserting: the extension is a size mutation a concurrent walker cannot
/// have seen, and with no entry to collide with, the epoch is the only way
/// to make that walker's publish fail. (Not an insert: the mdwritev
/// recovery arm calls this with a lower bound that may undershoot the real
/// size, and an undershooting INSERT would be exactly the staleness this
/// module must never serve.)
pub fn extend_to(locator: RelFileLocator, forknum: ForkNumber, new_min: BlockNumber) {
    let mut g = cache().write().unwrap();
    match g.map.get_mut(&key(locator, forknum)) {
        Some(e) => {
            if *e != POISON && *e < new_min as u64 {
                *e = new_min as u64;
            }
        }
        None => g.epoch += 1,
    }
}

/// Truncation landed: the size is exactly `nblocks` now (caller holds the
/// exclusive relation lock the truncate itself required). Poison is sticky.
pub fn set_exact(locator: RelFileLocator, forknum: ForkNumber, nblocks: BlockNumber) {
    let mut g = cache().write().unwrap();
    let e = g.map.entry(key(locator, forknum)).or_insert(nblocks as u64);
    if *e != POISON {
        *e = nblocks as u64;
    }
}

/// A size-changing op errored partway: forget the value (next read walks the
/// real file). Poison survives. Always bumps the epoch — the file may have
/// changed size even when no entry existed to remove.
pub fn invalidate(locator: RelFileLocator, forknum: ForkNumber) {
    let mut g = cache().write().unwrap();
    g.epoch += 1;
    if let Some(&v) = g.map.get(&key(locator, forknum)) {
        if v != POISON {
            g.map.remove(&key(locator, forknum));
        }
    }
}

/// Unlink/create: the file identity is gone (or brand new) — drop the entry
/// entirely, poison included. A reused relfilenumber starts clean; a
/// columnar writer re-poisons at its next open.
pub fn remove(locator: RelFileLocator, forknum: ForkNumber) {
    let mut g = cache().write().unwrap();
    g.epoch += 1;
    g.map.remove(&key(locator, forknum));
}

/// Mark this fork as mutated outside md: never serve it from the cache.
pub fn poison(locator: RelFileLocator, forknum: ForkNumber) {
    cache().write().unwrap().map.insert(key(locator, forknum), POISON);
}

/// DROP/move DATABASE removes file trees with rmtree, not per-rel unlinks.
pub fn purge_db(db: Oid) {
    let (lo, hi) = db_range(db);
    let mut g = cache().write().unwrap();
    g.epoch += 1;
    let doomed: Vec<u128> = g.map.range(lo..=hi).map(|(&k, _)| k).collect();
    for k in doomed {
        g.map.remove(&k);
    }
}

/// The unlogged-relation reinit pass copies init forks over main forks at
/// the file level, outside md — drop everything (startup-time, rare).
pub fn clear() {
    let mut g = cache().write().unwrap();
    g.epoch += 1;
    g.map.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rl(rel: u32) -> RelFileLocator {
        RelFileLocator::new(1663, 5, rel)
    }
    const MAIN: ForkNumber = ForkNumber::MAIN_FORKNUM;

    // The map is process-global; tests share it. Each test uses its own
    // relNumber band so parallel test threads never alias keys — but the
    // EPOCH is one counter, so a concurrent test's remove/invalidate bump
    // would (correctly!) void this test's WalkToken. Serialize the module.
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn serial() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn walk_token(l: RelFileLocator) -> WalkToken {
        lookup_or_begin_walk(l, MAIN).expect_err("expected a cache miss")
    }

    #[test]
    fn walk_then_lookup_roundtrip() {
        let _g = serial();
        let l = rl(9_000_001);
        let t = walk_token(l);
        assert_eq!(lookup(l, MAIN), None);
        note_walked(l, MAIN, 42, t);
        assert_eq!(lookup(l, MAIN), Some(42));
        assert_eq!(lookup_or_begin_walk(l, MAIN).ok(), Some(42));
    }

    #[test]
    fn extend_is_monotonic_and_needs_an_entry() {
        let _g = serial();
        let l = rl(9_000_002);
        extend_to(l, MAIN, 7);
        assert_eq!(lookup(l, MAIN), None, "absent stays absent");
        let t = walk_token(l);
        note_walked(l, MAIN, 10, t);
        extend_to(l, MAIN, 12);
        assert_eq!(lookup(l, MAIN), Some(12));
        extend_to(l, MAIN, 11);
        assert_eq!(lookup(l, MAIN), Some(12), "max() semantics");
    }

    #[test]
    fn truncate_sets_exact_and_invalidate_forgets() {
        let _g = serial();
        let l = rl(9_000_003);
        let t = walk_token(l);
        note_walked(l, MAIN, 100, t);
        set_exact(l, MAIN, 3);
        assert_eq!(lookup(l, MAIN), Some(3));
        invalidate(l, MAIN);
        assert_eq!(lookup(l, MAIN), None);
    }

    #[test]
    fn poison_is_sticky_until_remove() {
        let _g = serial();
        let l = rl(9_000_004);
        let t = walk_token(l);
        poison(l, MAIN);
        assert_eq!(lookup(l, MAIN), None);
        note_walked(l, MAIN, 5, t);
        set_exact(l, MAIN, 6);
        extend_to(l, MAIN, 7);
        invalidate(l, MAIN);
        assert_eq!(lookup(l, MAIN), None, "poison survives all");
        remove(l, MAIN);
        let t = walk_token(l);
        note_walked(l, MAIN, 5, t);
        assert_eq!(lookup(l, MAIN), Some(5), "remove clears poison");
    }

    #[test]
    fn purge_db_takes_only_that_database() {
        let _g = serial();
        let mine = RelFileLocator::new(1663, 77001, 9_000_005);
        let other = RelFileLocator::new(1663, 77002, 9_000_006);
        let tm = walk_token(mine);
        let to = walk_token(other);
        note_walked(mine, MAIN, 1, tm);
        note_walked(other, MAIN, 2, to);
        purge_db(77001);
        assert_eq!(lookup(mine, MAIN), None);
        assert_eq!(lookup(other, MAIN), Some(2));
    }

    #[test]
    fn forks_are_independent_keys() {
        let _g = serial();
        let l = rl(9_000_007);
        let tm = lookup_or_begin_walk(l, ForkNumber::MAIN_FORKNUM).unwrap_err();
        let tf = lookup_or_begin_walk(l, ForkNumber::FSM_FORKNUM).unwrap_err();
        note_walked(l, ForkNumber::MAIN_FORKNUM, 8, tm);
        note_walked(l, ForkNumber::FSM_FORKNUM, 3, tf);
        assert_eq!(lookup(l, ForkNumber::MAIN_FORKNUM), Some(8));
        assert_eq!(lookup(l, ForkNumber::FSM_FORKNUM), Some(3));
        remove(l, ForkNumber::FSM_FORKNUM);
        assert_eq!(lookup(l, ForkNumber::MAIN_FORKNUM), Some(8));
        assert_eq!(lookup(l, ForkNumber::FSM_FORKNUM), None);
    }

    // --- The §6b' interleavings (TRUNCATE-under-INSERT storm) -------------

    /// The repro's exact shape: a read-side walker measures the fresh
    /// relfilenode at 0 blocks, an extender then walks (0), publishes,
    /// extends to 1 — and the reader's STALE 0 must not overwrite the 1.
    /// Pre-fix note_walked overwrote unconditionally: lookup returned 0
    /// forever and every extender died at the bufmgr beyond-EOF check.
    #[test]
    fn stale_walk_cannot_overwrite_a_concurrent_extension() {
        let _g = serial();
        let l = rl(9_000_010);
        let reader = walk_token(l); // reader lseeks: 0 blocks
        let extender = walk_token(l); // extender lseeks under the ext lock: 0
        note_walked(l, MAIN, 0, extender); // extender publishes first
        extend_to(l, MAIN, 1); // extender zeroextends block 0
        note_walked(l, MAIN, 0, reader); // reader's stale publish arrives late
        assert_eq!(lookup(l, MAIN), Some(1), "stale walk must be discarded");
    }

    /// Same race with the extender's own publish ALSO lost (epoch bumped by
    /// unrelated DDL during its walk): the extension's only trace is
    /// extend_to-on-absent, which must bump the epoch so the reader's stale
    /// publish still dies.
    #[test]
    fn extend_on_absent_entry_defeats_a_racing_walker() {
        let _g = serial();
        let l = rl(9_000_011);
        let reader = walk_token(l); // reader lseeks: 0 blocks
        extend_to(l, MAIN, 1); // extension with no entry and no walk-publish
        note_walked(l, MAIN, 0, reader);
        assert_eq!(lookup(l, MAIN), None, "stale walk must be discarded");
    }

    /// A walk straddling an unlink/create must not publish for the dead (or
    /// reborn) file identity.
    #[test]
    fn remove_defeats_a_racing_walker() {
        let _g = serial();
        let l = rl(9_000_012);
        let t0 = walk_token(l);
        note_walked(l, MAIN, 100, t0);
        let reader = lookup_or_begin_walk(l, MAIN); // hit: 100
        assert_eq!(reader.ok(), Some(100));
        remove(l, MAIN); // unlink (or create for reuse)
        // A token taken AFTER the remove is legitimately publishable: the
        // walk it covers began after the identity change and measured
        // whatever file now answers for the key.
        let fresh = walk_token(l);
        note_walked(l, MAIN, 100, fresh);
        assert_eq!(lookup(l, MAIN), Some(100));
        remove(l, MAIN);
        // ... but a token that PREDATES the remove is not.
        let l2 = rl(9_000_013);
        let pre = walk_token(l2);
        remove(l2, MAIN);
        note_walked(l2, MAIN, 55, pre);
        assert_eq!(lookup(l2, MAIN), None, "pre-remove token must be dead");
    }

    /// Truncation's set_exact wins over any straddling walker: the key is
    /// present at publish time, so the walk (which may have measured the
    /// PRE-truncate length) is discarded. This is the VACUUM-truncate twin
    /// of the TRUNCATE storm (stale-LARGE instead of stale-small).
    #[test]
    fn stale_walk_cannot_overwrite_a_truncation() {
        let _g = serial();
        let l = rl(9_000_014);
        let t0 = walk_token(l);
        note_walked(l, MAIN, 100, t0);
        invalidate(l, MAIN); // some error path forgot the size
        let reader = walk_token(l); // reader walks: sees 100 (pre-truncate)
        set_exact(l, MAIN, 3); // AEL truncation lands
        note_walked(l, MAIN, 100, reader);
        assert_eq!(lookup(l, MAIN), Some(3), "post-truncate size must win");
    }
}
