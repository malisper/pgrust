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

fn relids_equal(a: &Relids<'_>, b: &Relids<'_>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.words.as_slice() == b.words.as_slice(),
        _ => false,
    }
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

// build_simple_rel (relnode.c), parentless RTE_RESULT arm.
pub fn build_simple_rel<'mcx>(
    root: &mut PlannerInfo<'mcx>,
    relid: u32,
    rtekind: RTEKind,
) -> RelId {
    assert!(relid > 0 && (relid as i32) < root.simple_rel_array_size);
    assert!(root.simple_rel_array[relid as usize].is_none(), "rel {relid} already exists");
    if rtekind != RTEKind::RTE_RESULT {
        panic!(
            "build_simple_rel (relnode.c): rtekind {rtekind:?} needs \
             get_relation_info (plancat)/attr arrays; M2 scan lane"
        );
    }

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
    // RTE_RESULT has no columns, nor could it have a whole-row Var.
    rel.min_attr = 0;
    rel.max_attr = -1;
    rel.pathtarget_id = Some(empty_pathtarget_id(root));

    let id = root.alloc_rel(rel);
    root.simple_rel_array[relid as usize] = Some(id);
    id
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
