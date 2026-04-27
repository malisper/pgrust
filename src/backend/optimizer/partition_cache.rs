use crate::backend::parser::{
    CatalogLookup, LoweredPartitionSpec, deserialize_partition_bound, relation_partition_spec,
};
use crate::include::nodes::pathnodes::{PlannerInfo, PlannerPartitionChildBound};

pub(super) fn partition_spec(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Option<LoweredPartitionSpec> {
    if let Some(spec) = root.partition_spec_cache.borrow().get(&relation_oid) {
        return spec.clone();
    }

    let spec = catalog
        .relation_by_oid(relation_oid)
        .and_then(|relation| relation_partition_spec(&relation).ok());
    root.partition_spec_cache
        .borrow_mut()
        .insert(relation_oid, spec.clone());
    spec
}

pub(super) fn partition_child_bounds(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    parent_oid: u32,
) -> Vec<PlannerPartitionChildBound> {
    if let Some(children) = root.partition_child_bounds_cache.borrow().get(&parent_oid) {
        return children.clone();
    }

    let children = catalog
        .inheritance_children(parent_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .map(|row| {
            let bound = catalog
                .relation_by_oid(row.inhrelid)
                .and_then(|relation| relation.relpartbound)
                .and_then(|text| deserialize_partition_bound(&text).ok());
            PlannerPartitionChildBound { row, bound }
        })
        .collect::<Vec<_>>();
    root.partition_child_bounds_cache
        .borrow_mut()
        .insert(parent_oid, children.clone());
    children
}
