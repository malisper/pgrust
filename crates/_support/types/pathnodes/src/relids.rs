use mcx::{box_new_in, vec_from_elem_in, Mcx, PgVec};
use crate::{Bitmapset, PathTarget, PlannerInfo, PtId, RelId, RelOptInfo, Relids, UpperRelationKind, RELOPT_UPPER_REL};

pub fn relids_singleton<'mcx>(mcx: Mcx<'mcx>, x: u32) -> Relids<'mcx> {
    if x < 64 {
        return Some(box_new_in(mcx, Bitmapset::Small(1u64 << x)));
    }
    let mut words = vec_from_elem_in(mcx, 0u64, (x as usize / 64) + 1);
    words[x as usize / 64] |= 1u64 << (x % 64);
    Some(box_new_in(mcx, Bitmapset::Big(words)))
}

pub fn relids_overlap(a: &Relids<'_>, b: &Relids<'_>) -> bool {
    let (Some(a), Some(b)) = (a, b) else { return false };
    a.word_slice().iter().zip(b.word_slice().iter()).any(|(x, y)| x & y != 0)
}

pub fn relids_equal(a: &Relids<'_>, b: &Relids<'_>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.word_slice() == b.word_slice(),
        _ => false,
    }
}

pub fn relids_is_empty(a: &Relids<'_>) -> bool {
    match a {
        None => true,
        Some(b) => b.word_slice().iter().all(|w| *w == 0),
    }
}

pub fn relids_is_member(x: i32, a: &Relids<'_>) -> bool {
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

pub fn relids_num_members(a: &Relids<'_>) -> i32 {
    match a {
        None => 0,
        Some(b) => b.word_slice().iter().map(|w| w.count_ones() as i32).sum(),
    }
}

pub fn relids_is_subset(a: &Relids<'_>, b: &Relids<'_>) -> bool {
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

pub fn relids_singleton_member(a: &Relids<'_>) -> Option<i32> {
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

pub fn relids_intersect<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, b: &Relids<'mcx>) -> Relids<'mcx> {
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

pub fn relids_difference<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, b: &Relids<'mcx>) -> Relids<'mcx> {
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

pub fn relids_members<'a>(a: &'a Relids<'_>) -> impl Iterator<Item = i32> + 'a {
    a.iter()
        .flat_map(|b| b.word_slice().iter().enumerate())
        .flat_map(|(i, w)| {
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
    let mut result: Relids<'mcx> = None;
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
    fetch_upper_rel_with_relids(root, kind, None)
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
