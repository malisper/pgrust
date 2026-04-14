use crate::include::nodes::pathnodes::{
    PathTarget, PlannerInfo, RelOptInfo, RelOptKind, UpperRelEntry, UpperRelKind,
};

pub(super) fn find_upper_rel_index(
    root: &PlannerInfo,
    kind: UpperRelKind,
    relids: &[usize],
    reltarget: &PathTarget,
) -> Option<usize> {
    root.upper_rels.iter().position(|entry| {
        entry.kind == kind && entry.relids == relids && entry.reltarget == *reltarget
    })
}

pub(super) fn ensure_upper_rel_index(
    root: &mut PlannerInfo,
    kind: UpperRelKind,
    relids: &[usize],
    reltarget: PathTarget,
) -> usize {
    if let Some(index) = find_upper_rel_index(root, kind, relids, &reltarget) {
        return index;
    }
    root.upper_rels.push(UpperRelEntry {
        kind,
        relids: relids.to_vec(),
        rel: RelOptInfo::new(relids.to_vec(), RelOptKind::UpperRel, reltarget.clone()),
        reltarget,
    });
    root.upper_rels.len() - 1
}
