//! Differential tests for the Relids representation.
//!
//! The oracle below is a verbatim, self-contained copy of the boxed
//! `Option<PgBox<Bitmapset>>` implementation (the representation of record
//! before the inline small-set swap). Every public `relids_*` helper is
//! driven against the oracle over directed edge cases and randomized op
//! sequences, asserting after EVERY op that the subject's observable value —
//! unset-ness plus the exact word slice, including non-canonical values such
//! as allocated all-zero sets and trailing zero words — is bitwise-identical
//! to the oracle's. Plans are a pure function of these observations, so
//! bitwise parity here is the plan-parity argument for any repr change.

extern crate std;

use std::vec::Vec;

use mcx::{box_new_in, vec_from_elem_in, Mcx, MemoryContext, PgBox, PgVec};

use crate::relids::{
    relids_add_member, relids_add_member_mut, relids_copy, relids_del_member, relids_difference,
    relids_empty, relids_equal, relids_from_words, relids_intersect, relids_is_empty,
    relids_is_member, relids_is_subset, relids_is_unset, relids_members, relids_num_members,
    relids_overlap, relids_singleton, relids_singleton_member, relids_subset_compare,
    relids_union, relids_word_slice, SubsetCmp,
};
use crate::Relids;

// ---------------------------------------------------------------------------
// Oracle: the boxed representation and its helpers, copied verbatim from the
// incumbent lib.rs:54-85 + relids.rs (renamed only). Do not "improve" this
// code: its exact behavior, canonical or not, is the specification.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum OBitmapset<'mcx> {
    Small(u64),
    Big(PgVec<'mcx, u64>),
}

impl<'mcx> OBitmapset<'mcx> {
    fn word_slice(&self) -> &[u64] {
        match self {
            OBitmapset::Small(w) => core::slice::from_ref(w),
            OBitmapset::Big(v) => v.as_slice(),
        }
    }
    fn word_slice_mut(&mut self) -> &mut [u64] {
        match self {
            OBitmapset::Small(w) => core::slice::from_mut(w),
            OBitmapset::Big(v) => v.as_mut_slice(),
        }
    }
}

type ORelids<'mcx> = Option<PgBox<'mcx, OBitmapset<'mcx>>>;

fn o_singleton<'mcx>(mcx: Mcx<'mcx>, x: u32) -> ORelids<'mcx> {
    if x < 64 {
        return Some(box_new_in(mcx, OBitmapset::Small(1u64 << x)));
    }
    let mut words = vec_from_elem_in(mcx, 0u64, (x as usize / 64) + 1);
    words[x as usize / 64] |= 1u64 << (x % 64);
    Some(box_new_in(mcx, OBitmapset::Big(words)))
}

fn o_overlap(a: &ORelids<'_>, b: &ORelids<'_>) -> bool {
    let (Some(a), Some(b)) = (a, b) else { return false };
    a.word_slice().iter().zip(b.word_slice().iter()).any(|(x, y)| x & y != 0)
}

fn o_equal(a: &ORelids<'_>, b: &ORelids<'_>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.word_slice() == b.word_slice(),
        _ => false,
    }
}

fn o_is_empty(a: &ORelids<'_>) -> bool {
    match a {
        None => true,
        Some(b) => b.word_slice().iter().all(|w| *w == 0),
    }
}

fn o_is_member(x: i32, a: &ORelids<'_>) -> bool {
    if x < 0 {
        return false;
    }
    match a {
        None => false,
        Some(b) => b
            .word_slice()
            .get(x as usize / 64)
            .is_some_and(|w| w & (1u64 << (x % 64)) != 0),
    }
}

fn o_num_members(a: &ORelids<'_>) -> i32 {
    match a {
        None => 0,
        Some(b) => b.word_slice().iter().map(|w| w.count_ones() as i32).sum(),
    }
}

fn o_is_subset(a: &ORelids<'_>, b: &ORelids<'_>) -> bool {
    let (Some(a), b) = (a, b) else { return true };
    let bw = b.as_ref().map_or(&[] as &[u64], |b| b.word_slice());
    for (i, w) in a.word_slice().iter().enumerate() {
        if *w == 0 {
            continue;
        }
        if w & !bw.get(i).copied().unwrap_or(0) != 0 {
            return false;
        }
    }
    true
}

fn o_singleton_member(a: &ORelids<'_>) -> Option<i32> {
    let mut found: Option<i32> = None;
    if let Some(b) = a {
        for (i, w) in b.word_slice().iter().enumerate() {
            let mut w = *w;
            while w != 0 {
                if found.is_some() {
                    return None;
                }
                found = Some((i * 64) as i32 + w.trailing_zeros() as i32);
                w &= w - 1;
            }
        }
    }
    found
}

fn o_union<'mcx>(mcx: Mcx<'mcx>, a: &ORelids<'mcx>, b: &ORelids<'mcx>) -> ORelids<'mcx> {
    let aw = a.as_ref().map_or(&[] as &[u64], |x| x.word_slice());
    let bw = b.as_ref().map_or(&[] as &[u64], |x| x.word_slice());
    let n = aw.len().max(bw.len());
    if n == 0 {
        return None;
    }
    if n == 1 {
        let w = aw.first().copied().unwrap_or(0) | bw.first().copied().unwrap_or(0);
        return Some(box_new_in(mcx, OBitmapset::Small(w)));
    }
    let mut words = vec_from_elem_in(mcx, 0u64, n);
    for (i, w) in words.iter_mut().enumerate() {
        *w = aw.get(i).copied().unwrap_or(0) | bw.get(i).copied().unwrap_or(0);
    }
    Some(box_new_in(mcx, OBitmapset::Big(words)))
}

fn o_intersect<'mcx>(mcx: Mcx<'mcx>, a: &ORelids<'mcx>, b: &ORelids<'mcx>) -> ORelids<'mcx> {
    let (Some(x), Some(y)) = (a, b) else { return None };
    let (xw, yw) = (x.word_slice(), y.word_slice());
    let n = xw.len().min(yw.len());
    if n == 0 {
        return None;
    }
    if n == 1 {
        return Some(box_new_in(mcx, OBitmapset::Small(xw[0] & yw[0])));
    }
    let mut words = vec_from_elem_in(mcx, 0u64, n);
    for (i, w) in words.iter_mut().enumerate() {
        *w = xw[i] & yw[i];
    }
    Some(box_new_in(mcx, OBitmapset::Big(words)))
}

fn o_add_member<'mcx>(mcx: Mcx<'mcx>, a: &ORelids<'mcx>, x: u32) -> ORelids<'mcx> {
    if a.is_none() {
        return o_singleton(mcx, x);
    }
    o_union(mcx, a, &o_singleton(mcx, x))
}

fn o_add_member_mut<'mcx>(mcx: Mcx<'mcx>, a: &mut ORelids<'mcx>, x: u32) {
    let wordnum = x as usize / 64;
    match a {
        Some(b) if b.word_slice().len() > wordnum => {
            b.word_slice_mut()[wordnum] |= 1u64 << (x % 64);
        }
        _ => *a = o_union(mcx, a, &o_singleton(mcx, x)),
    }
}

fn o_del_member<'mcx>(mcx: Mcx<'mcx>, a: &ORelids<'mcx>, x: i32) -> ORelids<'mcx> {
    let mut out = o_copy(mcx, a);
    if x >= 0 {
        if let Some(b) = out.as_mut() {
            if let Some(w) = b.word_slice_mut().get_mut(x as usize / 64) {
                *w &= !(1u64 << (x % 64));
            }
        }
    }
    out
}

fn o_difference<'mcx>(mcx: Mcx<'mcx>, a: &ORelids<'mcx>, b: &ORelids<'mcx>) -> ORelids<'mcx> {
    let Some(x) = a else { return None };
    let xw = x.word_slice();
    let bw = b.as_ref().map_or(&[] as &[u64], |y| y.word_slice());
    if xw.len() == 1 {
        let w = xw[0] & !bw.first().copied().unwrap_or(0);
        return Some(box_new_in(mcx, OBitmapset::Small(w)));
    }
    let mut words = vec_from_elem_in(mcx, 0u64, xw.len());
    for (i, w) in words.iter_mut().enumerate() {
        *w = xw[i] & !bw.get(i).copied().unwrap_or(0);
    }
    Some(box_new_in(mcx, OBitmapset::Big(words)))
}

fn o_members(a: &ORelids<'_>) -> Vec<i32> {
    let mut out = Vec::new();
    if let Some(b) = a {
        for (i, w) in b.word_slice().iter().enumerate() {
            let mut w = *w;
            while w != 0 {
                out.push((i * 64) as i32 + w.trailing_zeros() as i32);
                w &= w - 1;
            }
        }
    }
    out
}

fn o_copy<'mcx>(mcx: Mcx<'mcx>, a: &ORelids<'mcx>) -> ORelids<'mcx> {
    a.as_ref().map(|b| match &**b {
        OBitmapset::Small(w) => box_new_in(mcx, OBitmapset::Small(*w)),
        OBitmapset::Big(v) => {
            let mut words = PgVec::new_in(mcx);
            words.reserve(v.len());
            words.extend(v.iter().copied());
            box_new_in(mcx, OBitmapset::Big(words))
        }
    })
}

fn o_subset_compare(a: &ORelids<'_>, b: &ORelids<'_>) -> SubsetCmp {
    match (o_is_subset(a, b), o_is_subset(b, a)) {
        (true, true) => SubsetCmp::Equal,
        (true, false) => SubsetCmp::Subset1,
        (false, true) => SubsetCmp::Subset2,
        (false, false) => SubsetCmp::Different,
    }
}

// Oracle for relids_from_words: the historical per-member conversion loop
// (union of singletons over ascending members), exactly as pull_varnos_relids
// wrote it against the boxed representation.
fn o_from_words<'mcx>(mcx: Mcx<'mcx>, words: &[u64]) -> ORelids<'mcx> {
    let mut out: ORelids<'mcx> = None;
    for (i, w) in words.iter().enumerate() {
        let mut w = *w;
        while w != 0 {
            let x = (i * 64) as u32 + w.trailing_zeros();
            out = o_union(mcx, &out, &o_singleton(mcx, x));
            w &= w - 1;
        }
    }
    out
}

fn o_words<'a>(a: &'a ORelids<'_>) -> &'a [u64] {
    a.as_ref().map_or(&[] as &[u64], |b| b.word_slice())
}

// ---------------------------------------------------------------------------
// Equivalence assertions
// ---------------------------------------------------------------------------

/// The full observable value: unset-ness plus the exact backing words.
/// Word count and allocated-but-all-zero states are part of the value.
fn assert_same(tag: &str, s: &Relids<'_>, o: &ORelids<'_>) {
    assert_eq!(relids_is_unset(s), o.is_none(), "unset-ness diverged: {tag}");
    assert_eq!(relids_word_slice(s), o_words(o), "word slice diverged: {tag}");
}

fn assert_predicates(tag: &str, s1: &Relids<'_>, o1: &ORelids<'_>, s2: &Relids<'_>, o2: &ORelids<'_>) {
    assert_eq!(relids_equal(s1, s2), o_equal(o1, o2), "equal: {tag}");
    assert_eq!(relids_overlap(s1, s2), o_overlap(o1, o2), "overlap: {tag}");
    assert_eq!(relids_is_subset(s1, s2), o_is_subset(o1, o2), "is_subset: {tag}");
    assert_eq!(relids_subset_compare(s1, s2), o_subset_compare(o1, o2), "subset_compare: {tag}");
    assert_eq!(relids_is_empty(s1), o_is_empty(o1), "is_empty: {tag}");
    assert_eq!(relids_num_members(s1), o_num_members(o1), "num_members: {tag}");
    assert_eq!(relids_singleton_member(s1), o_singleton_member(o1), "singleton_member: {tag}");
    let sm: Vec<i32> = relids_members(s1).collect();
    assert_eq!(sm, o_members(o1), "members: {tag}");
    for x in [-1i32, 0, 1, 5, 63, 64, 65, 100, 127, 128, 130, 4096] {
        assert_eq!(relids_is_member(x, s1), o_is_member(x, o1), "is_member({x}): {tag}");
    }
}

// ---------------------------------------------------------------------------
// Directed edge cases: the non-canonical values the planner relies on.
// ---------------------------------------------------------------------------

#[test]
fn unset_vs_allocated_zero_are_distinct() {
    let ctx = MemoryContext::new("relids-test");
    let mcx = ctx.mcx();

    // Intersection of disjoint one-word sets: allocated all-zero, NOT unset.
    let a = relids_singleton(mcx, 3);
    let b = relids_singleton(mcx, 5);
    let i = relids_intersect(mcx, &a, &b);
    assert!(!relids_is_unset(&i));
    assert!(relids_is_empty(&i));
    assert_eq!(relids_word_slice(&i), &[0u64]);
    let oi = o_intersect(mcx, &o_singleton(mcx, 3), &o_singleton(mcx, 5));
    assert_same("intersect disjoint", &i, &oi);

    // It compares UNEQUAL to the unset value (word slices [0] vs []).
    let none = relids_empty();
    assert!(!relids_equal(&i, &none));
    assert!(relids_equal(&none, &relids_empty()));

    // Difference of a set with itself: same shape.
    let d = relids_difference(mcx, &a, &a);
    assert!(!relids_is_unset(&d));
    assert_eq!(relids_word_slice(&d), &[0u64]);

    // Unset in, unset out.
    assert!(relids_is_unset(&relids_union(mcx, &relids_empty(), &relids_empty())));
    assert!(relids_is_unset(&relids_intersect(mcx, &relids_empty(), &a)));
    assert!(relids_is_unset(&relids_difference(mcx, &relids_empty(), &a)));
    assert!(relids_is_unset(&relids_del_member(mcx, &relids_empty(), 5)));
    assert!(relids_is_unset(&relids_copy(mcx, &relids_empty())));
}

#[test]
fn word_count_is_part_of_the_value() {
    let ctx = MemoryContext::new("relids-test");
    let mcx = ctx.mcx();

    // {1, 70} then remove 70: two words with a trailing zero...
    let mut a = relids_singleton(mcx, 1);
    a = relids_add_member(mcx, &a, 70);
    let a_del = relids_del_member(mcx, &a, 70);
    assert_eq!(relids_word_slice(&a_del), &[2u64, 0]);
    // ...which is NOT equal to the one-word {1}: no trimming, ever.
    let b = relids_singleton(mcx, 1);
    assert_eq!(relids_word_slice(&b), &[2u64]);
    assert!(!relids_equal(&a_del, &b));
    // Same verdict from the oracle.
    let mut oa = o_singleton(mcx, 1);
    oa = o_add_member(mcx, &oa, 70);
    let oa_del = o_del_member(mcx, &oa, 70);
    assert!(!o_equal(&oa_del, &o_singleton(mcx, 1)));
    assert_same("del leaves trailing zero", &a_del, &oa_del);

    // But membership-level predicates still agree across lengths.
    assert!(relids_is_subset(&a_del, &b));
    assert!(relids_is_subset(&b, &a_del));
    assert_eq!(relids_subset_compare(&a_del, &b), SubsetCmp::Equal);

    // Difference keeps the left operand's length, incl. all-zero tails.
    let d = relids_difference(mcx, &a, &a);
    assert_eq!(relids_word_slice(&d), &[0u64, 0]);
}

#[test]
fn singleton_and_widening_shapes() {
    let ctx = MemoryContext::new("relids-test");
    let mcx = ctx.mcx();

    assert_eq!(relids_word_slice(&relids_singleton(mcx, 0)), &[1u64]);
    assert_eq!(relids_word_slice(&relids_singleton(mcx, 63)), &[1u64 << 63]);
    assert_eq!(relids_word_slice(&relids_singleton(mcx, 64)), &[0u64, 1]);
    assert_eq!(relids_word_slice(&relids_singleton(mcx, 127)), &[0u64, 1u64 << 63]);
    assert_eq!(relids_word_slice(&relids_singleton(mcx, 128)), &[0u64, 0, 1]);

    // add_member on unset == singleton; widening add keeps low words.
    let a = relids_add_member(mcx, &relids_empty(), 5);
    assert_eq!(relids_word_slice(&a), &[32u64]);
    let w = relids_add_member(mcx, &a, 70);
    assert_eq!(relids_word_slice(&w), &[32u64, 64]);

    // add_member_mut: in place within capacity, widen otherwise.
    let mut m = relids_singleton(mcx, 5);
    relids_add_member_mut(mcx, &mut m, 6);
    assert_eq!(relids_word_slice(&m), &[96u64]);
    relids_add_member_mut(mcx, &mut m, 70);
    assert_eq!(relids_word_slice(&m), &[96u64, 64]);
    relids_add_member_mut(mcx, &mut m, 0);
    assert_eq!(relids_word_slice(&m), &[97u64, 64]);
    let mut mu = relids_empty();
    relids_add_member_mut(mcx, &mut mu, 3);
    assert_eq!(relids_word_slice(&mu), &[8u64]);
}

#[test]
fn from_words_matches_member_loop() {
    let ctx = MemoryContext::new("relids-test");
    let mcx = ctx.mcx();

    // Directed: trims trailing zeros, all-zero input is unset.
    assert!(relids_is_unset(&relids_from_words(mcx, &[])));
    assert!(relids_is_unset(&relids_from_words(mcx, &[0])));
    assert!(relids_is_unset(&relids_from_words(mcx, &[0, 0, 0])));
    assert_eq!(relids_word_slice(&relids_from_words(mcx, &[5])), &[5u64]);
    assert_eq!(relids_word_slice(&relids_from_words(mcx, &[5, 0])), &[5u64]);
    assert_eq!(relids_word_slice(&relids_from_words(mcx, &[0, 1])), &[0u64, 1]);
    assert_eq!(relids_word_slice(&relids_from_words(mcx, &[1, 0, 2, 0, 0])), &[1u64, 0, 2]);

    // Differential over randomized word arrays (incl. trailing zeros).
    let mut rng = Rng(0x9e3779b97f4a7c15);
    for _ in 0..2000 {
        let len = (rng.next() % 5) as usize;
        let mut words = Vec::new();
        for _ in 0..len {
            words.push(match rng.next() % 4 {
                0 => 0u64,
                1 => 1u64 << (rng.next() % 64),
                2 => rng.next(),
                _ => rng.next() & 0xff,
            });
        }
        let s = relids_from_words(mcx, &words);
        let o = o_from_words(mcx, &words);
        assert_same("from_words random", &s, &o);
    }
}

// ---------------------------------------------------------------------------
// Randomized differential op sequences
// ---------------------------------------------------------------------------

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*: deterministic, dependency-free.
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn randomized_op_sequences_match_oracle() {
    for seed in [1u64, 42, 0xdeadbeef, 0x1784669931, 7, 0xfeedface] {
        let ctx = MemoryContext::new("relids-test");
        let mcx = ctx.mcx();
        run_sequence(mcx, seed);
    }
}

fn run_sequence(mcx: Mcx<'_>, seed: u64) {
    let mut rng = Rng(seed | 1);
    // Parallel pools: subject and oracle values built by identical op streams.
    let mut ss: Vec<Relids<'_>> = std::vec![relids_empty()];
    let mut os: Vec<ORelids<'_>> = std::vec![None];

    for step in 0..3000 {
        let tag = std::format!("seed {seed} step {step}");
        let i = rng.below(ss.len() as u64) as usize;
        let j = rng.below(ss.len() as u64) as usize;
        // Member domain 0..=130 spans one-word, two-word, three-word sets.
        let x = rng.below(131) as u32;
        let (s, o): (Relids<'_>, ORelids<'_>) = match rng.below(9) {
            0 => (relids_singleton(mcx, x), o_singleton(mcx, x)),
            1 => (relids_union(mcx, &ss[i], &ss[j]), o_union(mcx, &os[i], &os[j])),
            2 => (relids_intersect(mcx, &ss[i], &ss[j]), o_intersect(mcx, &os[i], &os[j])),
            3 => (relids_difference(mcx, &ss[i], &ss[j]), o_difference(mcx, &os[i], &os[j])),
            4 => (relids_add_member(mcx, &ss[i], x), o_add_member(mcx, &os[i], x)),
            5 => {
                // In-place add: mutate the pool entry in both worlds.
                relids_add_member_mut(mcx, &mut ss[i], x);
                o_add_member_mut(mcx, &mut os[i], x);
                assert_same(&tag, &ss[i], &os[i]);
                continue;
            }
            6 => {
                // Deletion domain includes negatives and out-of-range.
                let dx = rng.below(140) as i32 - 4;
                (relids_del_member(mcx, &ss[i], dx), o_del_member(mcx, &os[i], dx))
            }
            7 => (relids_copy(mcx, &ss[i]), o_copy(mcx, &os[i])),
            _ => (relids_empty(), None),
        };
        assert_same(&tag, &s, &o);
        assert_predicates(&tag, &s, &o, &ss[j], &os[j]);
        assert_predicates(&tag, &ss[j], &os[j], &s, &o);
        ss.push(s);
        os.push(o);
        // Bound the pool so pairwise checks stay cheap; drop from the front
        // (deterministically) to keep churn on both young and old values.
        if ss.len() > 48 {
            ss.remove(0);
            os.remove(0);
        }
    }
}
