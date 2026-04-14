use std::collections::BTreeMap;

use super::{CatalogTxnContext, ClientId, Database};
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{ExecError, RelationDesc};
use crate::backend::parser::{
    BoundRelation, CatalogLookup, ColumnDef, ParseError, derive_literal_default_value,
};
use crate::backend::utils::cache::syscache::{
    ensure_class_rows, ensure_depend_rows, ensure_namespace_rows, ensure_rewrite_rows,
};
use crate::include::catalog::{
    DEPENDENCY_NORMAL, PG_CATALOG_NAMESPACE_OID, PG_CLASS_RELATION_OID, PG_REWRITE_RELATION_OID,
    PUBLIC_NAMESPACE_OID,
};

pub(super) fn is_system_column_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "tableoid" | "ctid" | "xmin" | "xmax" | "cmin" | "cmax"
    )
}

pub(super) fn lookup_heap_relation_for_ddl(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if entry.relkind == 'r' => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        })),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

pub(super) fn namespace_oid_for_relation_name(name: &str) -> u32 {
    match name.split_once('.') {
        Some(("pg_catalog", _)) => PG_CATALOG_NAMESPACE_OID,
        Some(("public", _)) | None => PUBLIC_NAMESPACE_OID,
        _ => PUBLIC_NAMESPACE_OID,
    }
}

fn dependent_view_names_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
) -> Vec<String> {
    let namespaces = ensure_namespace_rows(db, client_id, txn_ctx)
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let classes = ensure_class_rows(db, client_id, txn_ctx)
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let rewrites = ensure_rewrite_rows(db, client_id, txn_ctx)
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();

    let mut names = ensure_depend_rows(db, client_id, txn_ctx)
        .into_iter()
        .filter(|row| {
            row.classid == PG_REWRITE_RELATION_OID
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == relation_oid
                && row.deptype == DEPENDENCY_NORMAL
        })
        .filter_map(|row| {
            let rewrite = rewrites.get(&row.objid)?;
            let class = classes.get(&rewrite.ev_class)?;
            let schema = namespaces
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            Some(match schema.as_str() {
                "public" | "pg_catalog" => class.relname.clone(),
                _ => format!("{schema}.{}", class.relname),
            })
        })
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

pub(super) fn reject_relation_with_dependent_views(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
    operation: &'static str,
) -> Result<(), ExecError> {
    let dependent_views = dependent_view_names_for_relation(db, client_id, txn_ctx, relation_oid);
    if dependent_views.is_empty() {
        return Ok(());
    }
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: operation,
        actual: format!(
            "cannot {operation}; view depends on it: {}",
            dependent_views.join(", ")
        ),
    }))
}

pub(super) fn validate_alter_table_add_column(
    desc: &RelationDesc,
    column: &ColumnDef,
) -> Result<crate::backend::executor::ColumnDesc, ExecError> {
    if !column.nullable {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ADD COLUMN without NOT NULL",
            actual: "NOT NULL".into(),
        }));
    }
    if column.primary_key {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ADD COLUMN without PRIMARY KEY",
            actual: "PRIMARY KEY".into(),
        }));
    }
    if column.unique {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ADD COLUMN without UNIQUE",
            actual: "UNIQUE".into(),
        }));
    }
    if is_system_column_name(&column.name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "non-system column name",
            actual: column.name.clone(),
        }));
    }
    if desc
        .columns
        .iter()
        .any(|existing| existing.name.eq_ignore_ascii_case(&column.name))
    {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "new column name",
            actual: format!("column already exists: {}", column.name),
        }));
    }

    let sql_type = match &column.ty {
        crate::backend::parser::RawTypeName::Builtin(sql_type) => *sql_type,
        crate::backend::parser::RawTypeName::Record => {
            return Err(ExecError::Parse(ParseError::UnsupportedType(
                "record".into(),
            )));
        }
        crate::backend::parser::RawTypeName::Named { name } => {
            return Err(ExecError::Parse(ParseError::UnsupportedType(name.clone())));
        }
    };
    let mut desc = column_desc(column.name.clone(), sql_type, true);
    desc.default_expr = column.default_expr.clone();
    if let Some(sql) = desc.default_expr.as_deref() {
        desc.missing_default_value = Some(derive_literal_default_value(sql, desc.sql_type)?);
    }
    Ok(desc)
}

pub(super) fn validate_alter_table_rename_column(
    desc: &RelationDesc,
    column_name: &str,
    new_column_name: &str,
) -> Result<String, ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for RENAME COLUMN",
            actual: column_name.to_string(),
        }));
    }
    if is_system_column_name(new_column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "new non-system column name",
            actual: new_column_name.to_string(),
        }));
    }
    if new_column_name.contains('.') {
        return Err(ExecError::Parse(ParseError::UnsupportedQualifiedName(
            new_column_name.to_string(),
        )));
    }
    if !desc
        .columns
        .iter()
        .any(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
    {
        return Err(ExecError::Parse(ParseError::UnknownColumn(
            column_name.to_string(),
        )));
    }

    let normalized_new = new_column_name.to_ascii_lowercase();
    if desc.columns.iter().any(|column| {
        !column.dropped
            && !column.name.eq_ignore_ascii_case(column_name)
            && column.name.eq_ignore_ascii_case(&normalized_new)
    }) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "new column name",
            actual: format!("column already exists: {new_column_name}"),
        }));
    }

    Ok(normalized_new)
}

pub(super) fn map_catalog_error(err: CatalogError) -> ExecError {
    match err {
        CatalogError::TableAlreadyExists(name) => {
            ExecError::Parse(ParseError::TableAlreadyExists(name))
        }
        CatalogError::UnknownTable(name) => ExecError::Parse(ParseError::TableDoesNotExist(name)),
        CatalogError::UnknownColumn(name) => ExecError::Parse(ParseError::UnknownColumn(name)),
        CatalogError::UnknownType(name) => ExecError::Parse(ParseError::UnsupportedType(name)),
        CatalogError::UniqueViolation(constraint) => ExecError::UniqueViolation { constraint },
        CatalogError::Interrupted(reason) => ExecError::Interrupted(reason),
        CatalogError::Io(_) | CatalogError::Corrupt(_) => {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "valid catalog state",
                actual: "catalog error".into(),
            })
        }
    }
}
