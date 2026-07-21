//! Relids helpers (bms_* family, bitmapset.c, over the planner's Relids).
//!
//! Two representation arms, selected by the `boxed_relids` feature:
//! the by-value inline small-set repr (default) and the boxed incumbent
//! (bisection kill switch). Both produce bit-identical word slices for
//! every operation — including the incumbent's non-canonical values
//! (allocated all-zero sets distinct from the unset value; trailing zero
//! words preserved; result word count is `max`/`min`/left-length exactly as
//! the incumbent computed it) — so comparisons and plans are unchanged by
//! construction. Pinned by relids_differential_tests.

use mcx::{Mcx, PgVec};
use crate::{PathTarget, PlannerInfo, PtId, RelId, RelOptInfo, Relids, UpperRelationKind, RELOPT_UPPER_REL};

pub use repr::{
    relids_add_member, relids_add_member_mut, relids_copy, relids_del_member, relids_difference,
    relids_empty, relids_from_words, relids_intersect, relids_is_unset, relids_singleton,
    relids_union, relids_word_slice, relids_word_slice_mut, RELIDS_UNSET,
};

// ---------------------------------------------------------------------------
// Representation-dependent helpers: inline by-value arm (default).
// ---------------------------------------------------------------------------
#[cfg(not(feature = "boxed_relids"))]
mod repr {
    use mcx::{vec_from_elem_in, Mcx, PgVec};
    use crate::Relids;

    /// The unset value. Distinct from an allocated all-zero set: helpers
    /// preserve that distinction (e.g. `relids_intersect` of two disjoint
    /// one-word sets yields `Small(0)`, which compares unequal to `Empty`).
    #[inline]
    pub fn relids_empty<'mcx>() -> Relids<'mcx> {
        Relids::Empty
    }

    /// True only for the unset value — NOT for allocated all-zero sets.
    /// The representation-agnostic spelling of the boxed `.is_none()`.
    #[inline]
    pub fn relids_is_unset(a: &Relids<'_>) -> bool {
        matches!(a, Relids::Empty)
    }

    /// The unset value as a const, for ref-to-unset positions (promotes to
    /// `'static` exactly like the boxed arm's `&None`).
    pub const RELIDS_UNSET: Relids<'static> = Relids::Empty;

    /// The set's backing words; empty slice for the unset value. Word count
    /// is part of the value's identity (`relids_equal` compares slices
    /// verbatim), so this is the canonical observation point for parity.
    #[inline]
    pub fn relids_word_slice<'a>(a: &'a Relids<'_>) -> &'a [u64] {
        a.word_slice()
    }

    /// Mutable view of the backing words; empty slice for the unset value.
    #[inline]
    pub fn relids_word_slice_mut<'a>(a: &'a mut Relids<'_>) -> &'a mut [u64] {
        a.word_slice_mut()
    }

    pub fn relids_singleton<'mcx>(mcx: Mcx<'mcx>, x: u32) -> Relids<'mcx> {
        if x < 64 {
            return Relids::Small(1u64 << x);
        }
        let mut words = vec_from_elem_in(mcx, 0u64, (x as usize / 64) + 1);
        words[x as usize / 64] |= 1u64 << (x % 64);
        Relids::Big(words)
    }

    pub fn relids_union<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, b: &Relids<'mcx>) -> Relids<'mcx> {
        let (aw, bw) = (a.word_slice(), b.word_slice());
        let n = aw.len().max(bw.len());
        if n == 0 {
            return Relids::Empty;
        }
        if n == 1 {
            let w = aw.first().copied().unwrap_or(0) | bw.first().copied().unwrap_or(0);
            return Relids::Small(w);
        }
        let mut words = vec_from_elem_in(mcx, 0u64, n);
        for (i, w) in words.iter_mut().enumerate() {
            *w = aw.get(i).copied().unwrap_or(0) | bw.get(i).copied().unwrap_or(0);
        }
        Relids::Big(words)
    }

    pub fn relids_intersect<'mcx>(
        mcx: Mcx<'mcx>,
        a: &Relids<'mcx>,
        b: &Relids<'mcx>,
    ) -> Relids<'mcx> {
        // The unset value intersects to unset; allocated inputs yield an
        // allocated result even when all-zero (boxed-arm parity).
        if relids_is_unset(a) || relids_is_unset(b) {
            return Relids::Empty;
        }
        let (xw, yw) = (a.word_slice(), b.word_slice());
        let n = xw.len().min(yw.len());
        if n == 0 {
            return Relids::Empty;
        }
        if n == 1 {
            return Relids::Small(xw[0] & yw[0]);
        }
        let mut words = vec_from_elem_in(mcx, 0u64, n);
        for (i, w) in words.iter_mut().enumerate() {
            *w = xw[i] & yw[i];
        }
        Relids::Big(words)
    }

    pub fn relids_add_member<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, x: u32) -> Relids<'mcx> {
        match a {
            Relids::Empty => relids_singleton(mcx, x),
            // The hot arm: one-word set, one-word member — pure word math.
            // Value-identical to the union-with-singleton below (n == 1).
            Relids::Small(w) if x < 64 => Relids::Small(w | (1u64 << x)),
            _ => relids_union(mcx, a, &relids_singleton(mcx, x)),
        }
    }

    // bms_add_member's mutate-in-place shape; allocates only to widen.
    pub fn relids_add_member_mut<'mcx>(mcx: Mcx<'mcx>, a: &mut Relids<'mcx>, x: u32) {
        let wordnum = x as usize / 64;
        match a {
            Relids::Small(w) if wordnum == 0 => *w |= 1u64 << x,
            Relids::Big(v) if v.len() > wordnum => v.as_mut_slice()[wordnum] |= 1u64 << (x % 64),
            _ => *a = relids_union(mcx, a, &relids_singleton(mcx, x)),
        }
    }

    pub fn relids_del_member<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, x: i32) -> Relids<'mcx> {
        let mut out = relids_copy(mcx, a);
        if x >= 0 {
            if let Some(w) = out.word_slice_mut().get_mut(x as usize / 64) {
                *w &= !(1u64 << (x % 64));
            }
        }
        out
    }

    pub fn relids_difference<'mcx>(
        mcx: Mcx<'mcx>,
        a: &Relids<'mcx>,
        b: &Relids<'mcx>,
    ) -> Relids<'mcx> {
        let xw = a.word_slice();
        if xw.is_empty() {
            return Relids::Empty;
        }
        let bw = b.word_slice();
        if xw.len() == 1 {
            return Relids::Small(xw[0] & !bw.first().copied().unwrap_or(0));
        }
        let mut words = vec_from_elem_in(mcx, 0u64, xw.len());
        for (i, w) in words.iter_mut().enumerate() {
            *w = xw[i] & !bw.get(i).copied().unwrap_or(0);
        }
        Relids::Big(words)
    }

    pub fn relids_copy<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>) -> Relids<'mcx> {
        match a {
            Relids::Empty => Relids::Empty,
            Relids::Small(w) => Relids::Small(*w),
            Relids::Big(v) => {
                let mut words = PgVec::new_in(mcx);
                words.reserve(v.len());
                words.extend(v.iter().copied());
                Relids::Big(words)
            }
        }
    }

    /// Build a Relids from raw set words (e.g. a nodes-side bitmapset's
    /// words). Value-identical to the historical per-member
    /// `out = relids_union(out, relids_singleton(x))` loop for every input:
    /// that loop yields exactly `wordnum(max_member) + 1` words, so trailing
    /// zero words in the input are trimmed here, and an all-zero input
    /// yields the unset value.
    pub fn relids_from_words<'mcx>(mcx: Mcx<'mcx>, words: &[u64]) -> Relids<'mcx> {
        let n = words.iter().rposition(|w| *w != 0).map_or(0, |i| i + 1);
        if n == 0 {
            return Relids::Empty;
        }
        if n == 1 {
            return Relids::Small(words[0]);
        }
        let mut out = vec_from_elem_in(mcx, 0u64, n);
        out.copy_from_slice(&words[..n]);
        Relids::Big(out)
    }
}

// ---------------------------------------------------------------------------
// Representation-dependent helpers: boxed incumbent arm (bisection), verbatim.
// ---------------------------------------------------------------------------
#[cfg(feature = "boxed_relids")]
mod repr {
    use mcx::{box_new_in, vec_from_elem_in, Mcx, PgVec};
    use crate::{Bitmapset, Relids};

    /// The unset (`None`) Relids; distinct from an allocated all-zero set.
    #[inline]
    pub fn relids_empty<'mcx>() -> Relids<'mcx> {
        None
    }

    /// True only for the unset value — NOT for allocated all-zero sets.
    #[inline]
    pub fn relids_is_unset(a: &Relids<'_>) -> bool {
        a.is_none()
    }

    /// The unset value as a const, for ref-to-unset positions.
    pub const RELIDS_UNSET: Relids<'static> = None;

    /// The set's backing words; empty slice for the unset value.
    #[inline]
    pub fn relids_word_slice<'a>(a: &'a Relids<'_>) -> &'a [u64] {
        a.as_ref().map_or(&[] as &[u64], |b| b.word_slice())
    }

    /// Mutable view of the backing words; empty slice for the unset value.
    #[inline]
    pub fn relids_word_slice_mut<'a>(a: &'a mut Relids<'_>) -> &'a mut [u64] {
        a.as_mut().map_or(&mut [] as &mut [u64], |b| b.word_slice_mut())
    }

    pub fn relids_singleton<'mcx>(mcx: Mcx<'mcx>, x: u32) -> Relids<'mcx> {
        if x < 64 {
            return Some(box_new_in(mcx, Bitmapset::Small(1u64 << x)));
        }
        let mut words = vec_from_elem_in(mcx, 0u64, (x as usize / 64) + 1);
        words[x as usize / 64] |= 1u64 << (x % 64);
        Some(box_new_in(mcx, Bitmapset::Big(words)))
    }

    pub fn relids_union<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, b: &Relids<'mcx>) -> Relids<'mcx> {
        let aw = a.as_ref().map_or(&[] as &[u64], |x| x.word_slice());
        let bw = b.as_ref().map_or(&[] as &[u64], |x| x.word_slice());
        let n = aw.len().max(bw.len());
        if n == 0 {
            return None;
        }
        if n == 1 {
            let w = aw.first().copied().unwrap_or(0) | bw.first().copied().unwrap_or(0);
            return Some(box_new_in(mcx, Bitmapset::Small(w)));
        }
        let mut words = vec_from_elem_in(mcx, 0u64, n);
        for (i, w) in words.iter_mut().enumerate() {
            *w = aw.get(i).copied().unwrap_or(0) | bw.get(i).copied().unwrap_or(0);
        }
        Some(box_new_in(mcx, Bitmapset::Big(words)))
    }

    pub fn relids_intersect<'mcx>(
        mcx: Mcx<'mcx>,
        a: &Relids<'mcx>,
        b: &Relids<'mcx>,
    ) -> Relids<'mcx> {
        let (Some(x), Some(y)) = (a, b) else { return None };
        let (xw, yw) = (x.word_slice(), y.word_slice());
        let n = xw.len().min(yw.len());
        if n == 0 {
            return None;
        }
        if n == 1 {
            return Some(box_new_in(mcx, Bitmapset::Small(xw[0] & yw[0])));
        }
        let mut words = vec_from_elem_in(mcx, 0u64, n);
        for (i, w) in words.iter_mut().enumerate() {
            *w = xw[i] & yw[i];
        }
        Some(box_new_in(mcx, Bitmapset::Big(words)))
    }

    pub fn relids_add_member<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, x: u32) -> Relids<'mcx> {
        if a.is_none() {
            return relids_singleton(mcx, x);
        }
        relids_union(mcx, a, &relids_singleton(mcx, x))
    }

    // bms_add_member's mutate-in-place shape; allocates only to widen.
    pub fn relids_add_member_mut<'mcx>(mcx: Mcx<'mcx>, a: &mut Relids<'mcx>, x: u32) {
        let wordnum = x as usize / 64;
        match a {
            Some(b) if b.word_slice().len() > wordnum => {
                b.word_slice_mut()[wordnum] |= 1u64 << (x % 64);
            }
            _ => *a = relids_union(mcx, a, &relids_singleton(mcx, x)),
        }
    }

    pub fn relids_del_member<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, x: i32) -> Relids<'mcx> {
        let mut out = relids_copy(mcx, a);
        if x >= 0 {
            if let Some(b) = out.as_mut() {
                if let Some(w) = b.word_slice_mut().get_mut(x as usize / 64) {
                    *w &= !(1u64 << (x % 64));
                }
            }
        }
        out
    }

    pub fn relids_difference<'mcx>(
        mcx: Mcx<'mcx>,
        a: &Relids<'mcx>,
        b: &Relids<'mcx>,
    ) -> Relids<'mcx> {
        let Some(x) = a else { return None };
        let xw = x.word_slice();
        let bw = b.as_ref().map_or(&[] as &[u64], |y| y.word_slice());
        if xw.len() == 1 {
            let w = xw[0] & !bw.first().copied().unwrap_or(0);
            return Some(box_new_in(mcx, Bitmapset::Small(w)));
        }
        let mut words = vec_from_elem_in(mcx, 0u64, xw.len());
        for (i, w) in words.iter_mut().enumerate() {
            *w = xw[i] & !bw.get(i).copied().unwrap_or(0);
        }
        Some(box_new_in(mcx, Bitmapset::Big(words)))
    }

    pub fn relids_copy<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>) -> Relids<'mcx> {
        a.as_ref().map(|b| match &**b {
            Bitmapset::Small(w) => box_new_in(mcx, Bitmapset::Small(*w)),
            Bitmapset::Big(v) => {
                let mut words = PgVec::new_in(mcx);
                words.reserve(v.len());
                words.extend(v.iter().copied());
                box_new_in(mcx, Bitmapset::Big(words))
            }
        })
    }

    /// Build a Relids from raw set words; value-identical to the historical
    /// per-member `union(out, singleton(x))` loop (see the inline arm).
    pub fn relids_from_words<'mcx>(mcx: Mcx<'mcx>, words: &[u64]) -> Relids<'mcx> {
        let n = words.iter().rposition(|w| *w != 0).map_or(0, |i| i + 1);
        if n == 0 {
            return None;
        }
        if n == 1 {
            return Some(box_new_in(mcx, Bitmapset::Small(words[0])));
        }
        let mut out = vec_from_elem_in(mcx, 0u64, n);
        out.copy_from_slice(&words[..n]);
        Some(box_new_in(mcx, Bitmapset::Big(out)))
    }
}

// ---------------------------------------------------------------------------
// Representation-agnostic helpers: pure functions of the word slices, with
// the unset value observing as the empty slice. Identical to the historical
// Option-matching bodies for every input (a non-unset set's slice is never
// empty, so slice emptiness separates the unset arms exactly).
// ---------------------------------------------------------------------------

/// Move the value out, leaving the unset value behind (the boxed arm's
/// `Option::take`); the read-modify-write idiom for in-place field updates.
pub fn relids_take<'mcx>(a: &mut Relids<'mcx>) -> Relids<'mcx> {
    core::mem::replace(a, relids_empty())
}

pub fn relids_overlap(a: &Relids<'_>, b: &Relids<'_>) -> bool {
    relids_word_slice(a)
        .iter()
        .zip(relids_word_slice(b).iter())
        .any(|(x, y)| x & y != 0)
}

pub fn relids_equal(a: &Relids<'_>, b: &Relids<'_>) -> bool {
    if relids_is_unset(a) || relids_is_unset(b) {
        return relids_is_unset(a) && relids_is_unset(b);
    }
    relids_word_slice(a) == relids_word_slice(b)
}

pub fn relids_is_empty(a: &Relids<'_>) -> bool {
    relids_word_slice(a).iter().all(|w| *w == 0)
}

pub fn relids_is_member(x: i32, a: &Relids<'_>) -> bool {
    if x < 0 {
        return false;
    }
    relids_word_slice(a)
        .get(x as usize / 64)
        .is_some_and(|w| w & (1u64 << (x % 64)) != 0)
}

pub fn relids_num_members(a: &Relids<'_>) -> i32 {
    relids_word_slice(a).iter().map(|w| w.count_ones() as i32).sum()
}

pub fn relids_is_subset(a: &Relids<'_>, b: &Relids<'_>) -> bool {
    let bw = relids_word_slice(b);
    for (i, w) in relids_word_slice(a).iter().enumerate() {
        if *w == 0 {
            continue;
        }
        if w & !bw.get(i).copied().unwrap_or(0) != 0 {
            return false;
        }
    }
    true
}

pub fn relids_singleton_member(a: &Relids<'_>) -> Option<i32> {
    let mut found: Option<i32> = None;
    for (i, w) in relids_word_slice(a).iter().enumerate() {
        let mut w = *w;
        while w != 0 {
            if found.is_some() {
                return None;
            }
            found = Some((i * 64) as i32 + w.trailing_zeros() as i32);
            w &= w - 1;
        }
    }
    found
}

pub fn relids_members<'a>(a: &'a Relids<'_>) -> impl Iterator<Item = i32> + 'a {
    relids_word_slice(a).iter().enumerate().flat_map(|(i, w)| {
        let mut w = *w;
        core::iter::from_fn(move || {
            if w == 0 {
                return None;
            }
            let bit = w.trailing_zeros();
            w &= w - 1;
            Some((i * 64) as i32 + bit as i32)
        })
    })
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SubsetCmp {
    Equal,
    Subset1,
    Subset2,
    Different,
}

// bms_subset_compare (bitmapset.c).
pub fn relids_subset_compare(a: &Relids<'_>, b: &Relids<'_>) -> SubsetCmp {
    match (relids_is_subset(a, b), relids_is_subset(b, a)) {
        (true, true) => SubsetCmp::Equal,
        (true, false) => SubsetCmp::Subset1,
        (false, true) => SubsetCmp::Subset2,
        (false, false) => SubsetCmp::Different,
    }
}

// find_base_rel (relnode.c).
pub fn find_base_rel(root: &PlannerInfo<'_>, relid: i32) -> RelId {
    // C elog text plus the site/level: the message never reaches conforming
    // output, and the two find_base_rel homes are otherwise identical.
    assert!(
        relid > 0 && relid < root.simple_rel_array_size,
        "no relation entry for relid {relid} (find_base_rel, level {})",
        root.query_level
    );
    root.simple_rel_array[relid as usize].unwrap_or_else(|| {
        panic!("no relation entry for relid {relid} (find_base_rel, level {})", root.query_level)
    })
}

// find_childrel_parents (relnode.c): relids of all appendrel ancestors of a
// child rel (appendrels nest, so there can be several levels).
pub fn find_childrel_parents<'mcx>(root: &PlannerInfo<'mcx>, rel: RelId) -> Relids<'mcx> {
    let mcx = root.mcx;
    debug_assert!(root.rel(rel).reloptkind == crate::RELOPT_OTHER_MEMBER_REL);
    let mut result: Relids<'mcx> = relids_empty();
    let mut cur = rel;
    loop {
        let relid = root.rel(cur).relid;
        debug_assert!(relid > 0 && (relid as i32) < root.simple_rel_array_size);
        let appinfo = root.append_rel_array[relid as usize]
            .as_ref()
            .expect("child rel has an AppendRelInfo");
        let prelid = appinfo.parent_relid;
        result = relids_add_member(mcx, &result, prelid);
        cur = find_base_rel(root, prelid as i32);
        if root.rel(cur).reloptkind != crate::RELOPT_OTHER_MEMBER_REL {
            break;
        }
    }
    debug_assert!(root.rel(cur).reloptkind == crate::RELOPT_BASEREL);
    result
}

pub fn empty_pathtarget_id<'mcx>(root: &mut PlannerInfo<'mcx>) -> PtId {
    let mcx = root.mcx;
    root.alloc_pathtarget(PathTarget::new(mcx))
}

// fetch_upper_rel (relnode.c), relids=NULL form.
pub fn fetch_upper_rel<'mcx>(root: &mut PlannerInfo<'mcx>, kind: UpperRelationKind) -> RelId {
    fetch_upper_rel_with_relids(root, kind, relids_empty())
}

pub fn fetch_upper_rel_with_relids<'mcx>(
    root: &mut PlannerInfo<'mcx>,
    kind: UpperRelationKind,
    relids: Relids<'mcx>,
) -> RelId {
    for &id in root.upper_rels[kind as usize].iter() {
        if relids_equal(&root.rel(id).relids, &relids) {
            return id;
        }
    }

    let mcx = root.mcx;
    let mut upperrel = RelOptInfo::new(mcx);
    upperrel.reloptkind = RELOPT_UPPER_REL;
    upperrel.relids = relids;
    upperrel.consider_startup = root.tuple_fraction > 0.0;
    upperrel.nparts = -1;
    upperrel.rel_parallel_workers = -1;
    upperrel.baserestrict_min_security = u32::MAX;
    upperrel.pathtarget_id = Some(empty_pathtarget_id(root));
    let id = root.alloc_rel(upperrel);
    root.upper_rels[kind as usize].push(id);
    id
}

pub fn pgvec_clone_shallow<'mcx, T: Copy>(mcx: Mcx<'mcx>, v: &PgVec<'mcx, T>) -> PgVec<'mcx, T> {
    let mut out = PgVec::new_in(mcx);
    out.reserve(v.len());
    out.extend(v.iter().copied());
    out
}
