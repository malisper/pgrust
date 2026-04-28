use std::collections::BTreeSet;

use super::super::*;
use crate::backend::parser::{BoundRelation, CatalogLookup};

#[derive(Debug, Clone)]
pub(super) struct AlterTableWorkItem {
    pub relation: BoundRelation,
    pub recursing: bool,
    pub expected_parents: i16,
}

pub(super) fn relation_name_for_alter_error(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

pub(super) fn has_inheritance_children(catalog: &dyn CatalogLookup, relation_oid: u32) -> bool {
    catalog
        .inheritance_children(relation_oid)
        .into_iter()
        .any(|row| !row.inhdetachpending)
}

pub(super) fn build_alter_table_work_queue(
    catalog: &dyn CatalogLookup,
    root: &BoundRelation,
    only: bool,
) -> Result<Vec<AlterTableWorkItem>, ExecError> {
    if only {
        return Ok(vec![AlterTableWorkItem {
            relation: root.clone(),
            recursing: false,
            expected_parents: 0,
        }]);
    }

    let mut relation_oids = Vec::new();
    let mut seen = BTreeSet::new();
    collect_inheritance_tree(catalog, root.relation_oid, &mut seen, &mut relation_oids);
    let relation_oid_set = relation_oids.iter().copied().collect::<BTreeSet<_>>();

    relation_oids
        .into_iter()
        .map(|relation_oid| {
            let relation = if relation_oid == root.relation_oid {
                root.clone()
            } else {
                catalog
                    .lookup_relation_by_oid(relation_oid)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownTable(relation_oid.to_string()))
                    })?
            };
            let expected_parents = if relation_oid == root.relation_oid {
                0
            } else {
                catalog
                    .inheritance_parents(relation_oid)
                    .into_iter()
                    .filter(|parent| {
                        !parent.inhdetachpending && relation_oid_set.contains(&parent.inhparent)
                    })
                    .count()
                    .min(i16::MAX as usize) as i16
            };
            Ok(AlterTableWorkItem {
                relation,
                recursing: relation_oid != root.relation_oid,
                expected_parents,
            })
        })
        .collect()
}

pub(super) fn direct_inheritance_children(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Vec<u32> {
    let mut children = catalog
        .inheritance_children(relation_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .collect::<Vec<_>>();
    children.sort_by_key(|row| (row.inhseqno, row.inhrelid));
    children.into_iter().map(|row| row.inhrelid).collect()
}

fn collect_inheritance_tree(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    seen: &mut BTreeSet<u32>,
    out: &mut Vec<u32>,
) {
    if !seen.insert(relation_oid) {
        return;
    }
    out.push(relation_oid);
    for child_oid in direct_inheritance_children(catalog, relation_oid) {
        collect_inheritance_tree(catalog, child_oid, seen, out);
    }
}
