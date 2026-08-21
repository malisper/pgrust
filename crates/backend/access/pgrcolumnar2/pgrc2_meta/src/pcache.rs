//! The in-memory predicate-hash granule bitmap cache (O-11: in-memory v1;
//! persistence is the RESERVED `pcache` sidecar slot, spec §16 — NOT built
//! here). One bit per granule: set = the granule SURVIVES pruning for this
//! predicate (verdict ≠ AllFail). Keyed `(part_uuid, predicate
//! fingerprint)` (spec §11: part identity keys every cache; "equal
//! identity ⇒ identical bytes" makes hits sound).
//!
//! Envelope disciplines carried from the sidecar contract (spec §16), in
//! their in-memory form: **HIT / BUILT / STALE witnesses** (measured-only
//! counters), and **refuse-and-rebuild on ANY mismatch** — an entry whose
//! shape disagrees with the caller's facts (granule count drift) is
//! dropped and rebuilt, never trusted, never patched.
//!
//! Placement is the CALLER's: this is a plain owned struct (no statics, no
//! locks, no thread-locals — R1–R6 worker confinement is the consumer's
//! contract, and the determinism census stays untouched). Deterministic
//! eviction: a budget of entries, evicting the least-recently-used tick
//! (BTreeMap keys keep every walk ordered — no RandomState anywhere).

use std::collections::BTreeMap;

use crate::hash::FoldChain;
use crate::verdict::ZonePredicate;

/// One pruning-survivor bitmap (1 bit per granule; set = scan it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GranuleBitmap {
    granule_count: u32,
    words: Vec<u64>,
}

impl GranuleBitmap {
    /// All-set (nothing pruned) — the sound starting point.
    pub fn all_set(granule_count: u32) -> GranuleBitmap {
        let words = (granule_count as usize).div_ceil(64);
        let mut b = GranuleBitmap {
            granule_count,
            words: vec![u64::MAX; words],
        };
        b.mask_tail();
        b
    }

    /// All-clear.
    pub fn all_clear(granule_count: u32) -> GranuleBitmap {
        GranuleBitmap {
            granule_count,
            words: vec![0u64; (granule_count as usize).div_ceil(64)],
        }
    }

    fn mask_tail(&mut self) {
        let n = self.granule_count as usize;
        let nwords = self.words.len();
        if let Some(last) = self.words.last_mut() {
            let keep = n - (nwords - 1) * 64;
            if keep < 64 {
                *last &= (1u64 << keep) - 1;
            }
        }
    }

    pub fn granule_count(&self) -> u32 {
        self.granule_count
    }

    pub fn set(&mut self, g: u32, survives: bool) {
        assert!(g < self.granule_count, "granule ordinal out of range");
        let w = (g / 64) as usize;
        let bit = 1u64 << (g % 64);
        if survives {
            self.words[w] |= bit;
        } else {
            self.words[w] &= !bit;
        }
    }

    pub fn survives(&self, g: u32) -> bool {
        assert!(g < self.granule_count, "granule ordinal out of range");
        self.words[(g / 64) as usize] >> (g % 64) & 1 == 1
    }

    /// Surviving-granule count (an EA-counter fact, measured-only).
    pub fn survivor_count(&self) -> u32 {
        self.words.iter().map(|w| w.count_ones()).sum()
    }
}

/// Lookup outcome witnesses (the qceil-envelope vocabulary, in-memory
/// form).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    /// Served from cache.
    Hit,
    /// Absent; built and inserted.
    Built,
    /// Present but mismatched (granule-count drift): refused and rebuilt.
    StaleRebuilt,
}

/// Measured-only witness counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheCounters {
    pub hits: u64,
    pub built: u64,
    pub stale_rebuilt: u64,
    pub evicted: u64,
}

#[derive(Debug)]
struct Entry {
    bitmap: GranuleBitmap,
    last_used: u64,
}

/// The cache (see module doc). `budget` bounds the ENTRY count;
/// deterministic LRU eviction by insertion/use tick.
#[derive(Debug)]
pub struct PredicateCache {
    entries: BTreeMap<([u8; 16], u64), Entry>,
    budget: usize,
    tick: u64,
    counters: CacheCounters,
}

impl PredicateCache {
    /// `budget` must be ≥ 1 (a zero-budget cache is a contradiction —
    /// released assert, driver protocol).
    pub fn new(budget: usize) -> PredicateCache {
        assert!(budget >= 1, "predicate cache budget must be >= 1");
        PredicateCache {
            entries: BTreeMap::new(),
            budget,
            tick: 0,
            counters: CacheCounters::default(),
        }
    }

    pub fn counters(&self) -> CacheCounters {
        self.counters
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Look up `(part_uuid, fingerprint)`; on miss (or on ANY shape
    /// mismatch — refuse-and-rebuild) call `build` and insert. Returns the
    /// witness and the (now cached) bitmap. The builder's result must
    /// match `granule_count` (released assert — builder protocol).
    pub fn lookup_or_build(
        &mut self,
        part_uuid: [u8; 16],
        fingerprint: u64,
        granule_count: u32,
        build: impl FnOnce() -> GranuleBitmap,
    ) -> (CacheOutcome, &GranuleBitmap) {
        self.tick += 1;
        let key = (part_uuid, fingerprint);
        let outcome = match self.entries.get_mut(&key) {
            Some(e) if e.bitmap.granule_count() == granule_count => {
                e.last_used = self.tick;
                self.counters.hits += 1;
                CacheOutcome::Hit
            }
            Some(_) => {
                // Mismatch: refuse-and-rebuild, witness STALE.
                self.entries.remove(&key);
                self.counters.stale_rebuilt += 1;
                let bitmap = build();
                assert_eq!(
                    bitmap.granule_count(),
                    granule_count,
                    "predicate-cache builder returned a mismatched bitmap"
                );
                self.insert_with_eviction(key, bitmap);
                CacheOutcome::StaleRebuilt
            }
            None => {
                self.counters.built += 1;
                let bitmap = build();
                assert_eq!(
                    bitmap.granule_count(),
                    granule_count,
                    "predicate-cache builder returned a mismatched bitmap"
                );
                self.insert_with_eviction(key, bitmap);
                CacheOutcome::Built
            }
        };
        (
            outcome,
            &self.entries.get(&key).expect("just ensured").bitmap,
        )
    }

    fn insert_with_eviction(&mut self, key: ([u8; 16], u64), bitmap: GranuleBitmap) {
        if self.entries.len() >= self.budget {
            // Deterministic LRU: smallest tick wins eviction; the ordered
            // map breaks ties (there are none — ticks are unique).
            if let Some(victim) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| *k)
            {
                self.entries.remove(&victim);
                self.counters.evicted += 1;
            }
        }
        self.entries.insert(
            key,
            Entry {
                bitmap,
                last_used: self.tick,
            },
        );
    }
}

// ---------------------------------------------------------------------------
// predicate fingerprints
// ---------------------------------------------------------------------------

/// Fingerprint seed (ASCII "pgrc2pfp").
const FP_SEED: u64 = u64::from_le_bytes(*b"pgrc2pfp");

/// Op tags (fingerprint vocabulary — golden-pinned through the fingerprint
/// pins; renumbering is a cache-semantics change).
fn op_tag(p: &ZonePredicate<'_>) -> u64 {
    match p {
        ZonePredicate::Eq(_) => 1,
        ZonePredicate::Lt(_) => 2,
        ZonePredicate::Le(_) => 3,
        ZonePredicate::Gt(_) => 4,
        ZonePredicate::Ge(_) => 5,
        ZonePredicate::Between { .. } => 6,
        ZonePredicate::InSet(_) => 7,
        ZonePredicate::IsNull => 8,
        ZonePredicate::IsNotNull => 9,
        // M4-S5 (append-only; planned-5's Ne arm): a NEW tag, never a
        // renumbering — existing pins stand.
        ZonePredicate::Ne(_) => 10,
    }
}

fn fold_const(chain: &mut FoldChain, c: &crate::lower::LoweredConst<'_>) {
    match c.key {
        Some(k) => {
            chain.word(1);
            chain.word(k.raw() as u64);
        }
        None => chain.word(0),
    }
    match c.eq_bytes() {
        Some(b) => {
            chain.word(1);
            chain.bytes(b);
        }
        None => chain.word(0),
    }
}

/// The predicate fingerprint: a pure function of (attno, operator shape,
/// lowered constants) — no pointers, no iteration-order dependence.
/// Deterministic and golden-pinned; the OTHER key half (part identity)
/// comes from `format::ident::part_uuid`.
pub fn predicate_fingerprint(attno: u32, probe: &ZonePredicate<'_>) -> u64 {
    let mut chain = FoldChain::new(FP_SEED);
    chain.word(attno as u64);
    chain.word(op_tag(probe));
    match probe {
        ZonePredicate::Eq(c)
        | ZonePredicate::Ne(c)
        | ZonePredicate::Lt(c)
        | ZonePredicate::Le(c)
        | ZonePredicate::Gt(c)
        | ZonePredicate::Ge(c) => fold_const(&mut chain, c),
        ZonePredicate::Between {
            lo,
            lo_inc,
            hi,
            hi_inc,
        } => {
            chain.word(*lo_inc as u64);
            fold_const(&mut chain, lo);
            chain.word(*hi_inc as u64);
            fold_const(&mut chain, hi);
        }
        ZonePredicate::InSet(members) => {
            chain.word(members.len() as u64);
            for m in members.iter() {
                fold_const(&mut chain, m);
            }
        }
        ZonePredicate::IsNull | ZonePredicate::IsNotNull => {}
    }
    chain.finish()
}
