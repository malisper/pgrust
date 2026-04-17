use std::collections::BTreeMap;

use crate::backend::executor::ExecError;
use crate::backend::parser::{
    AlterTableAddConstraintStatement, AlterTableValidateConstraintStatement, BoundDeleteStatement,
    BoundInsertStatement, BoundRelation, BoundRelationConstraints, BoundUpdateStatement,
    CatalogLookup, ParseError, PreparedInsert, bind_relation_constraints,
    normalize_alter_table_add_constraint,
};
use crate::backend::storage::lmgr::TableLockMode;
use crate::backend::storage::smgr::RelFileLocator;
use crate::include::catalog::CONSTRAINT_FOREIGN;

pub(crate) type TableLockRequest = (RelFileLocator, TableLockMode);

pub(crate) fn insert_foreign_key_lock_requests(
    stmt: &BoundInsertStatement,
) -> Vec<TableLockRequest> {
    let mut requests = BTreeMap::new();
    add_lock_request(&mut requests, stmt.rel, TableLockMode::RowExclusive);
    add_relation_foreign_key_partner_locks(&mut requests, &stmt.relation_constraints);
    requests.into_iter().collect()
}

pub(crate) fn prepared_insert_foreign_key_lock_requests(
    prepared: &PreparedInsert,
) -> Vec<TableLockRequest> {
    let mut requests = BTreeMap::new();
    add_lock_request(&mut requests, prepared.rel, TableLockMode::RowExclusive);
    add_relation_foreign_key_partner_locks(&mut requests, &prepared.relation_constraints);
    requests.into_iter().collect()
}

pub(crate) fn update_foreign_key_lock_requests(
    stmt: &BoundUpdateStatement,
) -> Vec<TableLockRequest> {
    let mut requests = BTreeMap::new();
    for target in &stmt.targets {
        add_lock_request(&mut requests, target.rel, TableLockMode::RowExclusive);
        add_relation_foreign_key_partner_locks(&mut requests, &target.relation_constraints);
        for constraint in &target.referenced_by_foreign_keys {
            add_lock_request(
                &mut requests,
                constraint.child_rel,
                TableLockMode::ShareUpdateExclusive,
            );
        }
    }
    requests.into_iter().collect()
}

pub(crate) fn delete_foreign_key_lock_requests(
    stmt: &BoundDeleteStatement,
) -> Vec<TableLockRequest> {
    let mut requests = BTreeMap::new();
    for target in &stmt.targets {
        add_lock_request(&mut requests, target.rel, TableLockMode::RowExclusive);
        for constraint in &target.referenced_by_foreign_keys {
            add_lock_request(
                &mut requests,
                constraint.child_rel,
                TableLockMode::ShareUpdateExclusive,
            );
        }
    }
    requests.into_iter().collect()
}

pub(crate) fn relation_foreign_key_lock_requests(
    rel: RelFileLocator,
    constraints: &BoundRelationConstraints,
) -> Vec<TableLockRequest> {
    let mut requests = BTreeMap::new();
    add_lock_request(&mut requests, rel, TableLockMode::RowExclusive);
    add_relation_foreign_key_partner_locks(&mut requests, constraints);
    requests.into_iter().collect()
}

pub(crate) fn alter_table_add_constraint_lock_requests(
    relation: &BoundRelation,
    stmt: &AlterTableAddConstraintStatement,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<TableLockRequest>, ExecError> {
    let mut requests = BTreeMap::new();
    add_lock_request(&mut requests, relation.rel, TableLockMode::AccessExclusive);

    let normalized = normalize_alter_table_add_constraint(
        relation_basename(&stmt.table_name),
        relation.relation_oid,
        relation.relpersistence,
        &relation.desc,
        &catalog.constraint_rows_for_relation(relation.relation_oid),
        &stmt.constraint,
        catalog,
    )
    .map_err(ExecError::Parse)?;
    if let crate::backend::parser::NormalizedAlterTableConstraint::ForeignKey(action) = normalized {
        let referenced_relation = catalog
            .lookup_relation_by_oid(action.referenced_relation_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(action.referenced_table.clone()))
            })?;
        add_lock_request(
            &mut requests,
            referenced_relation.rel,
            TableLockMode::ShareUpdateExclusive,
        );
    }

    Ok(requests.into_iter().collect())
}

pub(crate) fn alter_table_validate_constraint_lock_requests(
    relation: &BoundRelation,
    stmt: &AlterTableValidateConstraintStatement,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<TableLockRequest>, ExecError> {
    let mut requests = BTreeMap::new();
    add_lock_request(&mut requests, relation.rel, TableLockMode::AccessExclusive);

    let rows = catalog.constraint_rows_for_relation(relation.relation_oid);
    let row = rows
        .iter()
        .find(|row| row.conname.eq_ignore_ascii_case(&stmt.constraint_name))
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "existing table constraint",
                actual: format!("constraint \"{}\" does not exist", stmt.constraint_name),
            })
        })?;
    if row.contype == CONSTRAINT_FOREIGN {
        let constraints = bind_relation_constraints(
            Some(relation_basename(&stmt.table_name)),
            relation.relation_oid,
            &relation.desc,
            catalog,
        )
        .map_err(ExecError::Parse)?;
        let constraint = constraints
            .foreign_keys
            .iter()
            .find(|constraint| {
                constraint
                    .constraint_name
                    .eq_ignore_ascii_case(&stmt.constraint_name)
            })
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "bound foreign key constraint",
                    actual: format!("missing foreign key binding for {}", stmt.constraint_name),
                })
            })?;
        add_lock_request(
            &mut requests,
            constraint.referenced_rel,
            TableLockMode::ShareUpdateExclusive,
        );
    }

    Ok(requests.into_iter().collect())
}

pub(crate) fn table_lock_relations(requests: &[TableLockRequest]) -> Vec<RelFileLocator> {
    requests.iter().map(|(rel, _)| *rel).collect()
}

fn add_relation_foreign_key_partner_locks(
    requests: &mut BTreeMap<RelFileLocator, TableLockMode>,
    constraints: &BoundRelationConstraints,
) {
    for constraint in &constraints.foreign_keys {
        add_lock_request(
            requests,
            constraint.referenced_rel,
            TableLockMode::ShareUpdateExclusive,
        );
    }
}

fn add_lock_request(
    requests: &mut BTreeMap<RelFileLocator, TableLockMode>,
    rel: RelFileLocator,
    mode: TableLockMode,
) {
    requests
        .entry(rel)
        .and_modify(|existing| *existing = existing.strongest(mode))
        .or_insert(mode);
}

fn relation_basename(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}
