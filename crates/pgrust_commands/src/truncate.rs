use std::collections::{BTreeMap, BTreeSet};

use pgrust_analyze::{BoundRelation, CatalogLookup};
use pgrust_catalog_data::{CONSTRAINT_FOREIGN, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL};
use pgrust_catalog_data::{PG_CLASS_RELATION_OID, PgConstraintRow};
use pgrust_nodes::parsenodes::{ParseError, TruncateTableStatement};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TruncateError {
    Parse(ParseError),
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

pub fn resolve_truncate_relations(
    stmt: &TruncateTableStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(Vec<BoundRelation>, Vec<String>), TruncateError> {
    let mut targets = Vec::new();
    for target in &stmt.targets {
        let entry = lookup_truncate_relation(catalog, &target.relation_name)?;
        push_truncate_relation(catalog, entry, target.include_descendants, &mut targets)?;
    }
    let notices = if stmt.cascade {
        expand_truncate_cascade(catalog, &mut targets)?
    } else {
        check_truncate_restrict_foreign_keys(catalog, &targets)?;
        Vec::new()
    };
    Ok((targets, notices))
}

pub fn owned_sequence_oids_for_truncate(
    targets: &[BoundRelation],
    catalog: &dyn CatalogLookup,
) -> Vec<u32> {
    let mut sequence_oids = Vec::new();
    for target in targets {
        for depend in
            catalog.depend_rows_referencing(PG_CLASS_RELATION_OID, target.relation_oid, None)
        {
            if depend.classid == PG_CLASS_RELATION_OID
                && depend.objsubid == 0
                && depend.refobjsubid > 0
                && matches!(depend.deptype, DEPENDENCY_AUTO | DEPENDENCY_INTERNAL)
                && catalog
                    .class_row_by_oid(depend.objid)
                    .is_some_and(|row| row.relkind == 'S')
                && !sequence_oids.contains(&depend.objid)
            {
                sequence_oids.push(depend.objid);
            }
        }
    }
    sequence_oids
}

pub fn lookup_truncate_relation(
    catalog: &dyn CatalogLookup,
    relation_name: &str,
) -> Result<BoundRelation, TruncateError> {
    match catalog.lookup_any_relation(relation_name) {
        Some(entry) if entry.relkind == 'r' || entry.relkind == 'p' => Ok(entry),
        Some(_) => Err(TruncateError::Parse(ParseError::WrongObjectType {
            name: relation_name.to_string(),
            expected: "table",
        })),
        None => Err(TruncateError::Parse(ParseError::UnknownTable(
            relation_name.to_string(),
        ))),
    }
}

fn push_truncate_relation(
    catalog: &dyn CatalogLookup,
    entry: BoundRelation,
    include_descendants: bool,
    targets: &mut Vec<BoundRelation>,
) -> Result<(), TruncateError> {
    if !include_descendants && entry.relkind == 'p' {
        return Err(cannot_truncate_only_partitioned_table_error());
    }
    if include_descendants {
        let inheritors = catalog.find_all_inheritors(entry.relation_oid);
        if inheritors.is_empty() {
            push_unique_truncate_relation(targets, entry);
        } else {
            for oid in inheritors {
                if let Some(relation) = catalog.relation_by_oid(oid)
                    && (relation.relkind == 'r' || relation.relkind == 'p')
                {
                    push_unique_truncate_relation(targets, relation);
                }
            }
        }
    } else {
        push_unique_truncate_relation(targets, entry);
    }
    Ok(())
}

fn push_unique_truncate_relation(targets: &mut Vec<BoundRelation>, relation: BoundRelation) {
    if !targets
        .iter()
        .any(|target| target.relation_oid == relation.relation_oid)
    {
        targets.push(relation);
    }
}

fn expand_truncate_cascade(
    catalog: &dyn CatalogLookup,
    targets: &mut Vec<BoundRelation>,
) -> Result<Vec<String>, TruncateError> {
    let mut notices = Vec::new();
    loop {
        let covered = target_relation_oids(targets);
        let mut additions = truncate_foreign_key_rows(catalog, &covered)
            .into_iter()
            .filter(|row| {
                relation_covered_by_truncate(catalog, row.confrelid, &covered)
                    && !relation_covered_by_truncate(catalog, row.conrelid, &covered)
            })
            .filter_map(|row| catalog.relation_by_oid(row.conrelid))
            .collect::<Vec<_>>();
        additions.sort_by_key(|relation| relation.relation_oid);
        additions.dedup_by_key(|relation| relation.relation_oid);
        if additions.is_empty() {
            break;
        }
        for relation in additions {
            let start = targets.len();
            let include_descendants = relation.relkind == 'p';
            push_truncate_relation(catalog, relation, include_descendants, targets)?;
            for added in &targets[start..] {
                notices.push(format!(
                    "truncate cascades to table \"{}\"",
                    relation_name_for_oid(catalog, added.relation_oid)
                ));
            }
        }
    }
    Ok(notices)
}

fn check_truncate_restrict_foreign_keys(
    catalog: &dyn CatalogLookup,
    targets: &[BoundRelation],
) -> Result<(), TruncateError> {
    let covered = target_relation_oids(targets);
    let rows = truncate_foreign_key_rows(catalog, &covered);
    for target in targets {
        let mut references = rows
            .iter()
            .filter(|row| {
                relation_covered_by_truncate(catalog, row.confrelid, &[target.relation_oid])
                    && !relation_covered_by_truncate(catalog, row.conrelid, &covered)
            })
            .map(|row| {
                (
                    row.conrelid,
                    relation_name_for_oid(catalog, row.conrelid),
                    relation_name_for_oid(catalog, row.confrelid),
                )
            })
            .collect::<Vec<_>>();
        references.sort();
        references.dedup();
        if let Some((_, child, referenced)) = references.into_iter().next() {
            return Err(TruncateError::Detailed {
                message: "cannot truncate a table referenced in a foreign key constraint".into(),
                detail: Some(format!("Table \"{child}\" references \"{referenced}\".")),
                hint: Some(format!(
                    "Truncate table \"{child}\" at the same time, or use TRUNCATE ... CASCADE."
                )),
                sqlstate: "0A000",
            });
        }
    }
    Ok(())
}

fn truncate_foreign_key_rows(catalog: &dyn CatalogLookup, covered: &[u32]) -> Vec<PgConstraintRow> {
    let mut rows = catalog
        .constraint_rows()
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_FOREIGN)
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    for relation_oid in covered {
        for row in catalog.foreign_key_constraint_rows_referencing_relation(*relation_oid) {
            rows.entry(row.oid).or_insert(row);
        }
    }
    rows.into_values().collect()
}

fn relation_covered_by_truncate(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    covered: &[u32],
) -> bool {
    if covered.contains(&relation_oid) {
        return true;
    }
    let mut seen = BTreeSet::new();
    let mut pending = catalog.inheritance_parents(relation_oid);
    while let Some(parent) = pending.pop() {
        if !seen.insert(parent.inhparent) {
            continue;
        }
        if covered.contains(&parent.inhparent) {
            return true;
        }
        pending.extend(catalog.inheritance_parents(parent.inhparent));
    }
    false
}

fn target_relation_oids(targets: &[BoundRelation]) -> Vec<u32> {
    targets.iter().map(|target| target.relation_oid).collect()
}

pub fn relation_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

fn cannot_truncate_only_partitioned_table_error() -> TruncateError {
    TruncateError::Detailed {
        message: "cannot truncate only a partitioned table".into(),
        detail: None,
        hint: Some(
            "Do not specify the ONLY keyword, or use TRUNCATE ONLY on the partitions directly."
                .into(),
        ),
        sqlstate: "42809",
    }
}
