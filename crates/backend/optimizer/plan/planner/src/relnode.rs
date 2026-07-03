use mcx::{box_new_in, vec_from_elem_in, Mcx, PgVec};
use types_nodes::parsenodes::RTEKind;
use types_pathnodes::{
    Bitmapset, PathTarget, PlannerInfo, PtId, RangeTblEntryId, RelId, RelOptInfo, Relids,
    UpperRelationKind, RELOPT_BASEREL, RELOPT_UPPER_REL,
};

pub fn relids_singleton<'mcx>(mcx: Mcx<'mcx>, x: u32) -> Relids<'mcx> {
    let mut words = vec_from_elem_in(mcx, 0u64, (x as usize / 64) + 1);
    words[x as usize / 64] |= 1u64 << (x % 64);
    Some(box_new_in(mcx, Bitmapset { words }))
}

pub fn relids_overlap(a: &Relids<'_>, b: &Relids<'_>) -> bool {
    let (Some(a), Some(b)) = (a, b) else { return false };
    a.words.iter().zip(b.words.iter()).any(|(x, y)| x & y != 0)
}

pub fn relids_equal(a: &Relids<'_>, b: &Relids<'_>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.words.as_slice() == b.words.as_slice(),
        _ => false,
    }
}

pub fn relids_is_empty(a: &Relids<'_>) -> bool {
    match a {
        None => true,
        Some(b) => b.words.iter().all(|w| *w == 0),
    }
}

pub fn relids_is_member(x: i32, a: &Relids<'_>) -> bool {
    if x < 0 {
        return false;
    }
    match a {
        None => false,
        Some(b) => b
            .words
            .get(x as usize / 64)
            .is_some_and(|w| w & (1u64 << (x % 64)) != 0),
    }
}

pub fn relids_num_members(a: &Relids<'_>) -> i32 {
    match a {
        None => 0,
        Some(b) => b.words.iter().map(|w| w.count_ones() as i32).sum(),
    }
}

pub fn relids_is_subset(a: &Relids<'_>, b: &Relids<'_>) -> bool {
    let (Some(a), b) = (a, b) else { return true };
    for (i, w) in a.words.iter().enumerate() {
        if *w == 0 {
            continue;
        }
        let bw = b.as_ref().and_then(|b| b.words.get(i)).copied().unwrap_or(0);
        if w & !bw != 0 {
            return false;
        }
    }
    true
}

pub fn relids_singleton_member(a: &Relids<'_>) -> Option<i32> {
    let mut found: Option<i32> = None;
    if let Some(b) = a {
        for (i, w) in b.words.iter().enumerate() {
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
    let n = a.as_ref().map_or(0, |x| x.words.len()).max(b.as_ref().map_or(0, |x| x.words.len()));
    if n == 0 {
        return None;
    }
    let mut words = vec_from_elem_in(mcx, 0u64, n);
    for (i, w) in words.iter_mut().enumerate() {
        *w = a.as_ref().and_then(|x| x.words.get(i)).copied().unwrap_or(0)
            | b.as_ref().and_then(|x| x.words.get(i)).copied().unwrap_or(0);
    }
    Some(box_new_in(mcx, Bitmapset { words }))
}

pub fn relids_intersect<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, b: &Relids<'mcx>) -> Relids<'mcx> {
    let (Some(x), Some(y)) = (a, b) else { return None };
    let n = x.words.len().min(y.words.len());
    if n == 0 {
        return None;
    }
    let mut words = vec_from_elem_in(mcx, 0u64, n);
    for (i, w) in words.iter_mut().enumerate() {
        *w = x.words[i] & y.words[i];
    }
    Some(box_new_in(mcx, Bitmapset { words }))
}

pub fn relids_add_member<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, x: u32) -> Relids<'mcx> {
    relids_union(mcx, a, &relids_singleton(mcx, x))
}

pub fn relids_del_member<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, x: i32) -> Relids<'mcx> {
    let mut out = relids_copy(mcx, a);
    if x >= 0 {
        if let Some(b) = out.as_mut() {
            if let Some(w) = b.words.get_mut(x as usize / 64) {
                *w &= !(1u64 << (x % 64));
            }
        }
    }
    out
}

pub fn relids_difference<'mcx>(mcx: Mcx<'mcx>, a: &Relids<'mcx>, b: &Relids<'mcx>) -> Relids<'mcx> {
    let Some(x) = a else { return None };
    let mut words = vec_from_elem_in(mcx, 0u64, x.words.len());
    for (i, w) in words.iter_mut().enumerate() {
        *w = x.words[i] & !b.as_ref().and_then(|y| y.words.get(i)).copied().unwrap_or(0);
    }
    Some(box_new_in(mcx, Bitmapset { words }))
}

pub fn relids_members<'a>(a: &'a Relids<'_>) -> impl Iterator<Item = i32> + 'a {
    a.iter()
        .flat_map(|b| b.words.iter().enumerate())
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
    a.as_ref().map(|b| {
        let mut words = PgVec::new_in(mcx);
        words.extend(b.words.iter().copied());
        box_new_in(mcx, Bitmapset { words })
    })
}

// find_base_rel (relnode.c).
pub fn find_base_rel(root: &PlannerInfo<'_>, relid: i32) -> RelId {
    assert!(relid > 0 && relid < root.simple_rel_array_size, "no relation entry for relid {relid}");
    root.simple_rel_array[relid as usize].unwrap_or_else(|| panic!("no relation entry for relid {relid}"))
}

fn empty_pathtarget_id<'mcx>(root: &mut PlannerInfo<'mcx>) -> PtId {
    let mcx = root.mcx;
    root.alloc_pathtarget(PathTarget::new(mcx))
}

// setup_simple_rel_arrays (relnode.c).
pub fn setup_simple_rel_arrays<'mcx>(root: &mut PlannerInfo<'mcx>, nrtable: usize) {
    let size = nrtable + 1;
    root.simple_rel_array_size = size as i32;
    root.simple_rel_array.clear();
    root.simple_rte_array.clear();
    root.simple_rel_array.reserve(size);
    root.simple_rte_array.reserve(size);
    root.simple_rel_array.extend(core::iter::repeat(None).take(size));
    root.simple_rte_array.push(RangeTblEntryId::Invalid);
    for i in 0..nrtable {
        root.simple_rte_array
            .push(RangeTblEntryId::Parse { query: root.parse, index: i as u32 });
    }
    debug_assert!(root.append_rel_list.is_empty());
}

// build_simple_rel (relnode.c), parentless arm (inheritance children are the
// M2 partition lane).
pub fn build_simple_rel<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    relid: u32,
    rtekind: RTEKind,
) -> types_error::PgResult<RelId> {
    let eref_max_attr = match rtekind {
        RTEKind::RTE_FUNCTION | RTEKind::RTE_VALUES | RTEKind::RTE_CTE => {
            run.rte(relid as usize).eref.expect("RTE has eref").colnames.len() as i16
        }
        _ => 0,
    };
    let root = &mut run.root;
    assert!(relid > 0 && (relid as i32) < root.simple_rel_array_size);
    assert!(root.simple_rel_array[relid as usize].is_none(), "rel {relid} already exists");

    let mcx = root.mcx;
    let mut rel = RelOptInfo::new(mcx);
    rel.reloptkind = RELOPT_BASEREL;
    rel.relids = relids_singleton(mcx, relid);
    rel.consider_startup = root.tuple_fraction > 0.0;
    rel.relid = relid;
    rel.rtekind = rtekind as u32;
    rel.rel_parallel_workers = -1;
    rel.nparts = -1;
    rel.baserestrict_min_security = u32::MAX;
    rel.pathtarget_id = Some(empty_pathtarget_id(root));

    match rtekind {
        RTEKind::RTE_RELATION => {
            // rel.userid comes from the RTE's perminfo checkAsUser; RTEs on
            // this lane panicked earlier when perminfoindex != 0.
            rel.userid = 0;
        }
        RTEKind::RTE_RESULT => {
            // RTE_RESULT has no columns, nor could it have a whole-row Var.
            rel.min_attr = 0;
            rel.max_attr = -1;
        }
        RTEKind::RTE_FUNCTION | RTEKind::RTE_VALUES | RTEKind::RTE_CTE => {
            rel.min_attr = 0;
            rel.max_attr = eref_max_attr;
            let span = (rel.max_attr - rel.min_attr + 1) as usize;
            rel.attr_widths = mcx::vec_from_elem_in(mcx, 0i32, span);
            rel.attr_needed = mcx::PgVec::new_in(mcx);
            for _ in 0..span {
                rel.attr_needed.push(None);
            }
        }
        other => panic!("build_simple_rel (relnode.c): rtekind {other:?}; M2 scan lane"),
    }

    let id = run.root.alloc_rel(rel);
    run.root.simple_rel_array[relid as usize] = Some(id);

    if rtekind == RTEKind::RTE_RELATION {
        let rte = run.rte(relid as usize);
        assert!(!rte.inh, "expand_inherited_rtentry: M2 partition lane");
        crate::plancat::get_relation_info(run, rte.relid, false, id)?;
    }

    Ok(id)
}

// fetch_upper_rel (relnode.c); only the relids=NULL form exists pre-partition.
pub fn fetch_upper_rel<'mcx>(root: &mut PlannerInfo<'mcx>, kind: UpperRelationKind) -> RelId {
    let none: Relids<'mcx> = None;
    for &id in root.upper_rels[kind as usize].iter() {
        if relids_equal(&root.rel(id).relids, &none) {
            return id;
        }
    }

    let mcx = root.mcx;
    let mut upperrel = RelOptInfo::new(mcx);
    upperrel.reloptkind = RELOPT_UPPER_REL;
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
