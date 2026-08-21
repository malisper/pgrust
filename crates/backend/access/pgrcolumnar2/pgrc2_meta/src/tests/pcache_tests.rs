//! Predicate-cache battery (the slice clause verbatim: "predicate-cache
//! keyed (part-uuid, fingerprint) with HIT/BUILT witnesses and
//! refuse-and-rebuild on mismatch") + fingerprint discrimination.

use crate::format::class::{CollationClass, StorageClass};
use crate::format::ident::{part_uuid, PartIdent};
use crate::lower::{lower_const, ConstInput};
use crate::pcache::{predicate_fingerprint, CacheOutcome, GranuleBitmap, PredicateCache};
use crate::profile::{MetaProfile, TypeSemantics};
use crate::verdict::ZonePredicate;

fn uuid(n: u64) -> [u8; 16] {
    part_uuid(&PartIdent {
        dev: n,
        ino: n * 7,
        len: 1000 + n,
        footer_off: 900 + n,
    })
}

fn int_probe(v: i64) -> u64 {
    let profile = MetaProfile::derive(
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        CollationClass::C,
        TypeSemantics::SignedInt,
    )
    .unwrap();
    let c = lower_const(&profile, ConstInput::Word(v as u64))
        .lowered()
        .unwrap();
    predicate_fingerprint(1, &ZonePredicate::Eq(c))
}

#[test]
fn hit_built_stale_witnesses() {
    let mut cache = PredicateCache::new(8);
    let fp = int_probe(42);
    let mut builds = 0;
    // First: BUILT.
    let (o, bm) = cache.lookup_or_build(uuid(1), fp, 10, || {
        builds += 1;
        let mut b = GranuleBitmap::all_set(10);
        b.set(3, false);
        b
    });
    assert_eq!(o, CacheOutcome::Built);
    assert_eq!(bm.survivor_count(), 9);
    assert!(!bm.survives(3));
    assert!(bm.survives(4));
    // Second: HIT, no rebuild.
    let (o, bm) = cache.lookup_or_build(uuid(1), fp, 10, || {
        builds += 1;
        GranuleBitmap::all_set(10)
    });
    assert_eq!(o, CacheOutcome::Hit);
    assert_eq!(bm.survivor_count(), 9, "the hit serves the ORIGINAL bitmap");
    assert_eq!(builds, 1, "a hit must not rebuild");
    // Mismatched granule count (shape drift): refuse-and-rebuild, STALE.
    let (o, bm) = cache.lookup_or_build(uuid(1), fp, 12, || {
        builds += 1;
        GranuleBitmap::all_set(12)
    });
    assert_eq!(o, CacheOutcome::StaleRebuilt);
    assert_eq!(bm.granule_count(), 12);
    assert_eq!(builds, 2);
    let c = cache.counters();
    assert_eq!((c.hits, c.built, c.stale_rebuilt), (1, 1, 1));
}

#[test]
fn keys_are_part_and_predicate_scoped() {
    let mut cache = PredicateCache::new(8);
    let fp_a = int_probe(1);
    let fp_b = int_probe(2);
    assert_ne!(fp_a, fp_b, "different constants must fingerprint apart");
    let (o, _) = cache.lookup_or_build(uuid(1), fp_a, 4, || GranuleBitmap::all_set(4));
    assert_eq!(o, CacheOutcome::Built);
    // Same predicate, different part: BUILT (part identity keys the cache).
    let (o, _) = cache.lookup_or_build(uuid(2), fp_a, 4, || GranuleBitmap::all_set(4));
    assert_eq!(o, CacheOutcome::Built);
    // Same part, different predicate: BUILT.
    let (o, _) = cache.lookup_or_build(uuid(1), fp_b, 4, || GranuleBitmap::all_set(4));
    assert_eq!(o, CacheOutcome::Built);
    // Original pair still hits.
    let (o, _) = cache.lookup_or_build(uuid(1), fp_a, 4, || GranuleBitmap::all_set(4));
    assert_eq!(o, CacheOutcome::Hit);
}

#[test]
fn fingerprints_discriminate_operator_shapes() {
    let profile = MetaProfile::derive(
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        CollationClass::C,
        TypeSemantics::SignedInt,
    )
    .unwrap();
    let c = lower_const(&profile, ConstInput::Word(5))
        .lowered()
        .unwrap();
    let eq = predicate_fingerprint(1, &ZonePredicate::Eq(c));
    let lt = predicate_fingerprint(1, &ZonePredicate::Lt(c));
    let le = predicate_fingerprint(1, &ZonePredicate::Le(c));
    let isnull = predicate_fingerprint(1, &ZonePredicate::IsNull);
    let notnull = predicate_fingerprint(1, &ZonePredicate::IsNotNull);
    let other_att = predicate_fingerprint(2, &ZonePredicate::Eq(c));
    let all = [eq, lt, le, isnull, notnull, other_att];
    for i in 0..all.len() {
        for j in 0..all.len() {
            if i != j {
                assert_ne!(all[i], all[j], "fingerprints {i}/{j} collide");
            }
        }
    }
    // Between inclusivity matters.
    let b1 = predicate_fingerprint(
        1,
        &ZonePredicate::Between {
            lo: c,
            lo_inc: true,
            hi: c,
            hi_inc: true,
        },
    );
    let b2 = predicate_fingerprint(
        1,
        &ZonePredicate::Between {
            lo: c,
            lo_inc: false,
            hi: c,
            hi_inc: true,
        },
    );
    assert_ne!(b1, b2);
    // Determinism: the same probe fingerprints identically.
    assert_eq!(eq, predicate_fingerprint(1, &ZonePredicate::Eq(c)));
}

#[test]
fn eviction_respects_the_budget_and_lru() {
    let mut cache = PredicateCache::new(2);
    let fp = int_probe(9);
    cache.lookup_or_build(uuid(1), fp, 4, || GranuleBitmap::all_set(4));
    cache.lookup_or_build(uuid(2), fp, 4, || GranuleBitmap::all_set(4));
    // Touch part 1 so part 2 is the LRU victim.
    let (o, _) = cache.lookup_or_build(uuid(1), fp, 4, || GranuleBitmap::all_set(4));
    assert_eq!(o, CacheOutcome::Hit);
    cache.lookup_or_build(uuid(3), fp, 4, || GranuleBitmap::all_set(4));
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.counters().evicted, 1);
    // Part 1 survived (recently used); part 2 was evicted.
    let (o, _) = cache.lookup_or_build(uuid(1), fp, 4, || GranuleBitmap::all_set(4));
    assert_eq!(o, CacheOutcome::Hit);
    let (o, _) = cache.lookup_or_build(uuid(2), fp, 4, || GranuleBitmap::all_set(4));
    assert_eq!(o, CacheOutcome::Built);
}

#[test]
fn bitmap_tail_masking() {
    let b = GranuleBitmap::all_set(70);
    assert_eq!(
        b.survivor_count(),
        70,
        "tail bits past granule_count stay clear"
    );
    let mut b = GranuleBitmap::all_clear(70);
    b.set(69, true);
    assert!(b.survives(69));
    assert_eq!(b.survivor_count(), 1);
}

#[test]
#[should_panic(expected = "mismatched bitmap")]
fn builder_shape_violation_panics() {
    let mut cache = PredicateCache::new(2);
    cache.lookup_or_build(uuid(1), 1, 4, || GranuleBitmap::all_set(5));
}
