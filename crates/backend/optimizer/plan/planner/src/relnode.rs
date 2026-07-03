use mcx::{box_new_in, vec_from_elem_in, Mcx, PgVec};
use types_nodes::parsenodes::RTEKind;
use types_pathnodes::{
    Bitmapset, PathTarget, PlannerInfo, PtId, RangeTblEntryId, RelId, RelOptInfo, Relids,
    UpperRelationKind, RELOPT_BASEREL, RELOPT_UPPER_REL,
};

pub use types_pathnodes::relids::*;

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
        RTEKind::RTE_FUNCTION | RTEKind::RTE_VALUES | RTEKind::RTE_CTE
        | RTEKind::RTE_SUBQUERY => {
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
        RTEKind::RTE_FUNCTION | RTEKind::RTE_VALUES | RTEKind::RTE_CTE
        | RTEKind::RTE_SUBQUERY => {
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
        crate::plancat::get_relation_info(run, rte.relid, rte.inh, id)?;
    }

    Ok(id)
}

// build_simple_rel (relnode.c), inheritance-child arm: RELOPT_OTHER_MEMBER_REL
// plus parent back-links and apply_child_basequals.
pub fn build_simple_rel_child<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    relid: u32,
    parent: RelId,
) -> types_error::PgResult<RelId> {
    let rte = run.rte(relid as usize);
    debug_assert!(rte.rtekind == RTEKind::RTE_RELATION);
    let root = &mut run.root;
    assert!(relid > 0 && (relid as i32) < root.simple_rel_array_size);
    assert!(root.simple_rel_array[relid as usize].is_none(), "rel {relid} already exists");

    let mcx = root.mcx;
    let mut rel = RelOptInfo::new(mcx);
    rel.reloptkind = types_pathnodes::RELOPT_OTHER_MEMBER_REL;
    rel.relids = relids_singleton(mcx, relid);
    rel.consider_startup = root.tuple_fraction > 0.0;
    rel.relid = relid;
    rel.rtekind = RTEKind::RTE_RELATION as u32;
    rel.rel_parallel_workers = -1;
    rel.nparts = -1;
    rel.baserestrict_min_security = u32::MAX;
    rel.pathtarget_id = Some(empty_pathtarget_id(root));
    rel.userid = root.rel(parent).userid;
    rel.parent = Some(parent);
    let top = root.rel(parent).top_parent.unwrap_or(parent);
    rel.top_parent = Some(top);
    rel.top_parent_relids = relids_copy(mcx, &root.rel(top).relids);
    debug_assert!(
        root.rel(parent).nulling_relids.is_none()
            && root.rel(parent).lateral_relids.is_none()
            && root.rel(parent).direct_lateral_relids.is_none()
            && root.rel(parent).lateral_referencers.is_none(),
        "inherited outer-join/lateral propagation unported (loud upstream)"
    );

    let id = root.alloc_rel(rel);
    run.root.simple_rel_array[relid as usize] = Some(id);

    crate::plancat::get_relation_info(run, rte.relid, rte.inh, id)?;

    let appinfo = run.root.append_rel_array[relid as usize]
        .clone()
        .expect("child rel has an AppendRelInfo");
    if !crate::inherit::apply_child_basequals(run, parent, id, &appinfo)? {
        // mark_dummy_rel: constant-FALSE child qual, skip scanning.
        crate::allpaths::set_dummy_rel_pathlist(run, id)?;
    }
    Ok(id)
}

// fetch_upper_rel (relnode.c), relids=NULL form.
