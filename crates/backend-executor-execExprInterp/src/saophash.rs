//! `saophash` — the `lib/simplehash.h` open-addressing hash table instantiated
//! by `execExprInterp.c` for `EEOP_HASHED_SCALARARRAYOP`.
//!
//! C generates this family with the template parameters declared in
//! `execExprInterp.c` (lines 195–236):
//!
//! ```c
//! #define SH_PREFIX saophash
//! #define SH_ELEMENT_TYPE ScalarArrayOpExprHashEntry   // { Datum key; uint32 status; uint32 hash; }
//! #define SH_KEY_TYPE Datum
//! #define SH_KEY key
//! #define SH_HASH_KEY(tb, key) saop_element_hash(tb, key)
//! #define SH_EQUAL(tb, a, b)   saop_hash_element_match(tb, a, b)
//! #define SH_STORE_HASH                         // entries cache their hash
//! #define SH_GET_HASH(tb, a)   a->hash
//! ```
//!
//! This is a faithful, idiomatic re-port of the macro-expanded `saophash_*`
//! functions (the ~14 generated routines in the c2rust reference): power-of-two
//! sizing with a bit-mask bucket index, Robin-Hood insertion with the
//! anti-clustering forced-grow guards (`SH_GROW_MAX_DIB = 25`,
//! `SH_GROW_MAX_MOVE = 150`, gated on `SH_GROW_MIN_FILLFACTOR = 0.1`), and the
//! ordered-copy `grow`.
//!
//! Per the [`opacity-inherited-never-introduced`] rule, the table is a real
//! typed struct ([`ScalarArrayOpExprHashTable`] / [`SaophashHash`] /
//! [`ScalarArrayOpExprHashEntry`], defined in the `types-nodes` keystone so the
//! step payload can carry it) rather than the opaque address word the prior
//! placeholder cached.
//!
//! The two template callbacks `SH_HASH_KEY` / `SH_EQUAL` are supplied by the
//! owner ([`crate::eval_scalar`]) as fallible closures that dispatch the SAOP's
//! hash / equality function through the fmgr seams (`function_call1_coll` /
//! `function_call2_coll`, re-resolved by OID — the crate's F0 fmgr contract),
//! so the table stays free of any direct fmgr dependency. They are fallible
//! because the dispatched function can `ereport(ERROR)`.

use types_datum::Datum;
// The table data structs live in the keystone (types-nodes) so the step payload
// can carry the real typed table; this crate owns the simplehash algorithms.
pub use types_nodes::saophash::{
    ScalarArrayOpExprHashEntry, ScalarArrayOpExprHashTable, SaophashHash,
};

/// `SH_FILLFACTOR` — grow when the load factor reaches 0.9.
const SH_FILLFACTOR: f64 = 0.9;
/// `SH_MAX_FILLFACTOR` — the load factor permitted only at `SH_MAX_SIZE`.
const SH_MAX_FILLFACTOR: f64 = 0.98;
/// `SH_GROW_MIN_FILLFACTOR` — the anti-clustering forced grow only fires once
/// the table is at least 10% full (below that, long probe chains are expected
/// and not worth growing for).
const SH_GROW_MIN_FILLFACTOR: f64 = 0.1;
/// `SH_GROW_MAX_DIB` — probe-distance bound that forces a grow (inlined as the
/// literal `25` in the c2rust reference).
const SH_GROW_MAX_DIB: u32 = 25;
/// `SH_GROW_MAX_MOVE` — open-slot search bound that forces a grow (inlined as
/// the literal `150`).
const SH_GROW_MAX_MOVE: i32 = 150;
/// `SH_MAX_SIZE` — `(uint64) PG_UINT32_MAX + 1` (= 2^32).
const SH_MAX_SIZE: u64 = (u32::MAX as u64) + 1;

/// `SH_STATUS_EMPTY` — an unused slot (a zeroed entry is empty).
const SH_STATUS_EMPTY: u32 = 0;
/// `SH_STATUS_IN_USE` — an occupied slot.
const SH_STATUS_IN_USE: u32 = 1;

/// `pg_nextpower2_64` — smallest power of two `>= num` (num must be >= 1).
fn pg_nextpower2_64(num: u64) -> u64 {
    debug_assert!(num >= 1);
    if num <= 1 {
        return 1;
    }
    // 1 << (64 - leading_zeros(num - 1))
    1u64 << (64 - (num - 1).leading_zeros())
}

/// `saophash_compute_size(newsize)` (simplehash.h) — round the requested size
/// up to a power of two (min 2) and guard against an over-large allocation.
fn saophash_compute_size(newsize: u64) -> u64 {
    let mut size = newsize.max(2);
    size = pg_nextpower2_64(size);
    // Overflow guard: sizeof(entry) * size must stay below UINT64_MAX/2.
    let entry_sz = core::mem::size_of::<ScalarArrayOpExprHashEntry>() as u64;
    assert!(
        entry_sz.saturating_mul(size) < u64::MAX / 2,
        "saophash: hash table too large"
    );
    size
}

/// The structural simplehash operations over the keystone-owned [`SaophashHash`]
/// header. An extension trait because the header type lives in `types-nodes`;
/// the algorithms (and the simplehash template instantiation) are owned here.
trait SaophashOps {
    fn update_parameters(&mut self, newsize: u64);
    fn initial_bucket(&self, hash: u32) -> u32;
    fn next(&self, curelem: u32) -> u32;
    fn prev(&self, curelem: u32) -> u32;
    fn distance(&self, optimal: u32, bucket: u32) -> u32;
}

impl SaophashOps for SaophashHash {
    /// `saophash_update_parameters` — recompute `size`/`sizemask`/`grow_threshold`.
    fn update_parameters(&mut self, newsize: u64) {
        let size = saophash_compute_size(newsize);
        self.size = size;
        self.sizemask = (size - 1) as u32;
        // Use the max fillfactor only at the largest possible size, so we don't
        // grow into the impossible 2^32+1.
        if self.size == SH_MAX_SIZE {
            self.grow_threshold = (size as f64 * SH_MAX_FILLFACTOR) as u32;
        } else {
            self.grow_threshold = (size as f64 * SH_FILLFACTOR) as u32;
        }
    }

    /// `saophash_initial_bucket(tb, hash)` — `hash & sizemask`.
    #[inline]
    fn initial_bucket(&self, hash: u32) -> u32 {
        hash & self.sizemask
    }

    /// `saophash_next(tb, curelem, startelem)` — `(curelem + 1) & sizemask`.
    /// `startelem` is part of the macro signature but unused for masked probing.
    #[inline]
    fn next(&self, curelem: u32) -> u32 {
        curelem.wrapping_add(1) & self.sizemask
    }

    /// `saophash_prev(tb, curelem, startelem)` — `(curelem - 1) & sizemask`.
    #[inline]
    fn prev(&self, curelem: u32) -> u32 {
        curelem.wrapping_sub(1) & self.sizemask
    }

    /// `saophash_distance(tb, optimal, bucket)` — distance-in-table from a
    /// slot's optimal (initial) bucket to its actual bucket, wrapping.
    #[inline]
    fn distance(&self, optimal: u32, bucket: u32) -> u32 {
        if optimal <= bucket {
            bucket - optimal
        } else {
            (self.size + bucket as u64 - optimal as u64) as u32
        }
    }
}

/// `saophash_create(ctx, nelements, private_data)` — allocate a table sized for
/// `nelements` distinct keys (rounded up via the fillfactor to the next power of
/// two). The C `ctx`/`private_data` are folded into the owning
/// [`ScalarArrayOpExprHashTable`].
pub fn saophash_create(nelements: u32) -> SaophashHash {
    let mut tb = SaophashHash::default();
    // size = min(SH_MAX_SIZE, nelements / fillfactor), computed in f64 exactly
    // as the macro does, then rounded up to a power of two.
    let scaled = nelements as f64 / SH_FILLFACTOR;
    let size = if (SH_MAX_SIZE as f64) < scaled {
        SH_MAX_SIZE as f64
    } else {
        scaled
    } as u64;
    let size = saophash_compute_size(size);
    tb.data = vec![ScalarArrayOpExprHashEntry::default(); size as usize];
    tb.update_parameters(size);
    tb
}

/// `saophash_grow(tb, newsize)` — reallocate to `newsize` and re-insert every
/// live entry. Faithful to the macro's two-phase ordered copy: phase 1 finds a
/// `startelem` (the first empty slot, or the first entry already sitting at its
/// optimal bucket); phase 2 walks circularly from there and linear-probes each
/// live entry into the first empty slot of the new table.
fn saophash_grow(tb: &mut SaophashHash, newsize: u64) {
    let oldsize = tb.size;
    let olddata = core::mem::take(&mut tb.data);

    let newsize = saophash_compute_size(newsize);
    tb.data = vec![ScalarArrayOpExprHashEntry::default(); newsize as usize];
    tb.update_parameters(newsize);

    // Phase 1: choose a starting slot that is not in the middle of a
    // wrapped-around probe chain.
    let mut startelem: u32 = 0;
    let mut i: u64 = 0;
    while i < oldsize {
        let oldentry = &olddata[i as usize];
        if oldentry.status != SH_STATUS_IN_USE {
            startelem = i as u32;
            break;
        } else {
            let optimal = tb.initial_bucket(oldentry.hash);
            if optimal as u64 == i {
                startelem = i as u32;
                break;
            }
        }
        i += 1;
    }

    // Phase 2: copy each live entry, in circular order from startelem, into the
    // first empty slot of its new probe chain.
    let mut copyelem = startelem;
    let mut i: u64 = 0;
    while i < oldsize {
        let oldentry = olddata[copyelem as usize];
        if oldentry.status == SH_STATUS_IN_USE {
            let startelem2 = tb.initial_bucket(oldentry.hash);
            let mut curelem = startelem2;
            loop {
                if tb.data[curelem as usize].status == SH_STATUS_EMPTY {
                    break;
                }
                curelem = tb.next(curelem);
            }
            tb.data[curelem as usize] = oldentry;
        }
        copyelem = copyelem.wrapping_add(1);
        if copyelem as u64 >= oldsize {
            copyelem = 0;
        }
        i += 1;
    }
}

/// The `SH_HASH_KEY` / `SH_EQUAL` callbacks. The simplehash macro inlines them;
/// here the owner ([`crate::eval_scalar`]) supplies them as closures that
/// dispatch the operator's hash / equality function through the fmgr seam, so
/// the table stays free of the fmgr dependency. They are fallible because the
/// dispatched function can `ereport(ERROR)`.
type HashFn<'a> = dyn FnMut(Datum) -> types_error::PgResult<u32> + 'a;
type EqualFn<'a> = dyn FnMut(Datum, Datum) -> types_error::PgResult<bool> + 'a;

/// `saophash_insert(tb, key, &found)` — insert `key`, returning whether it was
/// already present. Robin-Hood placement with the macro's anti-clustering
/// forced-grow guards. `hash_key` / `equal` are the `SH_HASH_KEY` / `SH_EQUAL`
/// callbacks (operator hash / equality, dispatched via fmgr by the owner).
pub fn saophash_insert(
    tb: &mut SaophashHash,
    key: Datum,
    hash_key: &mut HashFn<'_>,
    equal: &mut EqualFn<'_>,
) -> types_error::PgResult<bool> {
    let hash = hash_key(key)?;
    saophash_insert_hash_internal(tb, key, hash, equal)
}

/// `saophash_insert_hash_internal(tb, key, hash, &found)` — the Robin-Hood core.
fn saophash_insert_hash_internal(
    tb: &mut SaophashHash,
    key: Datum,
    hash: u32,
    equal: &mut EqualFn<'_>,
) -> types_error::PgResult<bool> {
    'restart: loop {
        let mut insertdist: u32 = 0;

        // Grow if we're at/over the threshold.
        if tb.members >= tb.grow_threshold {
            assert!(tb.size != SH_MAX_SIZE, "saophash: hash table size exceeded");
            saophash_grow(tb, tb.size.wrapping_mul(2));
        }

        let startelem = tb.initial_bucket(hash);
        let mut curelem = startelem;

        // Probe loop: find a home for `key`, possibly stealing a richer slot.
        loop {
            {
                let entry = &mut tb.data[curelem as usize];
                if entry.status == SH_STATUS_EMPTY {
                    tb.members += 1;
                    let e = &mut tb.data[curelem as usize];
                    e.key = key;
                    e.hash = hash;
                    e.status = SH_STATUS_IN_USE;
                    return Ok(false);
                }
            }
            let entry_hash = tb.data[curelem as usize].hash;
            let entry_key = tb.data[curelem as usize].key;
            if hash == entry_hash && equal(entry_key, key)? {
                return Ok(true); // key already present
            }

            let curoptimal = tb.initial_bucket(entry_hash);
            let curdist = tb.distance(curoptimal, curelem);

            if insertdist > curdist {
                // Robin Hood: steal this slot for `key` and shift the poorer
                // run forward to open a gap.
                break;
            }

            curelem = tb.next(curelem);
            insertdist += 1;

            // Anti-clustering: a too-long probe chain forces a grow (once the
            // table is at least minimally full).
            if insertdist > SH_GROW_MAX_DIB
                && (tb.members as f64 / tb.size as f64) >= SH_GROW_MIN_FILLFACTOR
            {
                tb.grow_threshold = 0;
                continue 'restart;
            }
        }

        // Robin-Hood shift phase. `curelem` is the slot we want for `key`; find
        // the next empty slot ahead of it.
        let mut emptyelem = curelem;
        let mut emptydist: i32 = 0;
        loop {
            emptyelem = tb.next(emptyelem);
            if tb.data[emptyelem as usize].status == SH_STATUS_EMPTY {
                break;
            }
            emptydist += 1;
            if emptydist > SH_GROW_MAX_MOVE
                && (tb.members as f64 / tb.size as f64) >= SH_GROW_MIN_FILLFACTOR
            {
                tb.grow_threshold = 0;
                continue 'restart;
            }
        }

        // Shift entries one slot forward (from the empty slot back to curelem),
        // then drop `key` into the opened slot.
        let mut moveelem = emptyelem;
        while moveelem != curelem {
            let src = tb.prev(moveelem);
            tb.data[moveelem as usize] = tb.data[src as usize];
            moveelem = src;
        }
        tb.members += 1;
        let e = &mut tb.data[curelem as usize];
        e.key = key;
        e.hash = hash;
        e.status = SH_STATUS_IN_USE;
        return Ok(false);
    }
}

/// `saophash_lookup(tb, key)` — return whether `key` is present. (C returns the
/// entry pointer; the only caller, `ExecEvalHashedScalarArrayOp`, just tests it
/// against NULL.) `hash_key` / `equal` are the `SH_HASH_KEY` / `SH_EQUAL`
/// callbacks.
pub fn saophash_lookup(
    tb: &SaophashHash,
    key: Datum,
    hash_key: &mut HashFn<'_>,
    equal: &mut EqualFn<'_>,
) -> types_error::PgResult<bool> {
    let hash = hash_key(key)?;
    let startelem = tb.initial_bucket(hash);
    let mut curelem = startelem;
    loop {
        let entry = &tb.data[curelem as usize];
        if entry.status == SH_STATUS_EMPTY {
            return Ok(false);
        }
        if hash == entry.hash && equal(entry.key, key)? {
            return Ok(true);
        }
        curelem = tb.next(curelem);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deterministic SH_HASH_KEY / SH_EQUAL standing in for the fmgr-dispatched
    // callbacks (these tests exercise the *structural* simplehash, not fmgr):
    // hash the low 32 bits of the Datum word, compare for exact equality.
    fn hk(k: Datum) -> types_error::PgResult<u32> {
        Ok(k.as_usize() as u32)
    }
    fn eq(a: Datum, b: Datum) -> types_error::PgResult<bool> {
        Ok(a.as_usize() == b.as_usize())
    }

    #[test]
    fn insert_lookup_round_trip_with_grow() {
        // Size for a handful, then insert far past the fillfactor to force
        // several grows; every key must remain findable and absent keys must
        // miss. This validates create sizing, Robin-Hood insert, grow's ordered
        // re-insert, and the masked probe wraparound.
        let mut tb = saophash_create(4);
        let n: usize = 1000;
        for i in 0..n {
            let key = Datum::from_usize(i * 2 + 1); // odd keys
            let found = saophash_insert(&mut tb, key, &mut hk, &mut eq).unwrap();
            assert!(!found, "fresh key {i} reported as already present");
        }
        assert_eq!(tb.members as usize, n);

        // Re-inserting an existing key reports found and does not grow members.
        let again =
            saophash_insert(&mut tb, Datum::from_usize(1), &mut hk, &mut eq).unwrap();
        assert!(again);
        assert_eq!(tb.members as usize, n);

        for i in 0..n {
            let present = saophash_lookup(&tb, Datum::from_usize(i * 2 + 1), &mut hk, &mut eq)
                .unwrap();
            assert!(present, "inserted key {i} not found after grows");
            let absent =
                saophash_lookup(&tb, Datum::from_usize(i * 2), &mut hk, &mut eq).unwrap();
            assert!(!absent, "never-inserted even key {} found", i * 2);
        }
        // size is a power of two and large enough to hold n under the fillfactor.
        assert!(tb.size.is_power_of_two());
        assert!((tb.size as f64) * SH_FILLFACTOR >= n as f64);
    }

    #[test]
    fn compute_size_is_power_of_two_min_two() {
        assert_eq!(saophash_compute_size(0), 2);
        assert_eq!(saophash_compute_size(1), 2);
        assert_eq!(saophash_compute_size(2), 2);
        assert_eq!(saophash_compute_size(3), 4);
        assert_eq!(saophash_compute_size(5), 8);
        assert_eq!(saophash_compute_size(1024), 1024);
        assert_eq!(saophash_compute_size(1025), 2048);
    }

    #[test]
    fn distance_wraps_across_table() {
        let mut tb = SaophashHash::default();
        tb.update_parameters(8); // size 8, sizemask 7
        assert_eq!(tb.distance(2, 5), 3); // optimal <= bucket
        assert_eq!(tb.distance(6, 1), 3); // wrapped: 8 + 1 - 6 = 3
        assert_eq!(tb.distance(0, 0), 0);
    }
}
