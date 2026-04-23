use std::collections::BTreeMap;

use super::{CatalogTxnContext, ClientId, Database};
use crate::backend::access::common::toast_compression::ensure_attribute_compression_supported;
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{ColumnDesc, ExecError, Expr, RelationDesc};
use crate::backend::parser::{
    BoundRelation, CatalogLookup, CheckConstraintAction, ColumnDef, NotNullConstraintAction,
    OwnedSequenceSpec, ParseError, RawTypeName, SqlExpr, SqlType, SqlTypeKind,
    bind_scalar_expr_in_scope, derive_literal_default_value,
    normalize_alter_table_add_column_constraints, raw_type_name_hint, resolve_raw_type_name,
};
use crate::backend::utils::cache::relcache::RelCacheEntry;
use crate::backend::utils::cache::syscache::{
    ensure_class_rows, ensure_depend_rows, ensure_namespace_rows, ensure_rewrite_rows,
};
use crate::include::access::htup::{AttributeCompression, AttributeStorage};
use crate::include::catalog::{
    CONSTRAINT_FOREIGN, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, PG_CATALOG_NAMESPACE_OID,
    PG_CLASS_RELATION_OID, PG_PROC_RELATION_OID, PG_REWRITE_RELATION_OID, PG_TRIGGER_RELATION_OID,
    PG_TYPE_RELATION_OID, PUBLIC_NAMESPACE_OID, builtin_range_name_for_sql_type,
    relkind_is_analyzable,
};
use crate::include::nodes::primnodes::{Var, user_attrno};

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

pub(super) fn lookup_trigger_relation_for_ddl(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'p' | 'v') => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table or view",
        })),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

pub(super) fn lookup_heap_relation_for_alter_table(
    catalog: &dyn CatalogLookup,
    name: &str,
    if_exists: bool,
) -> Result<Option<BoundRelation>, ExecError> {
    match lookup_heap_relation_for_ddl(catalog, name) {
        Ok(relation) => Ok(Some(relation)),
        Err(ExecError::Parse(ParseError::TableDoesNotExist(_))) if if_exists => Ok(None),
        Err(err) => Err(err),
    }
}

pub(super) fn lookup_table_or_partitioned_table_for_alter_table(
    catalog: &dyn CatalogLookup,
    name: &str,
    if_exists: bool,
) -> Result<Option<BoundRelation>, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'p') => Ok(Some(entry)),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        })),
        None if if_exists => Ok(None),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

pub(super) fn lookup_index_relation_for_alter_index(
    catalog: &dyn CatalogLookup,
    name: &str,
    if_exists: bool,
) -> Result<Option<BoundRelation>, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if entry.relkind == 'i' => Ok(Some(entry)),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "index",
        })),
        None if if_exists => Ok(None),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

pub(super) fn lookup_index_or_partitioned_index_for_alter_index_rename(
    catalog: &dyn CatalogLookup,
    name: &str,
    if_exists: bool,
) -> Result<Option<BoundRelation>, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'i' | 'I') => Ok(Some(entry)),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "index",
        })),
        None if if_exists => Ok(None),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

pub(super) fn lookup_rule_relation_for_ddl(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'v') => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table or view",
        })),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

pub(super) fn lookup_analyzable_relation_for_ddl(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if relkind_is_analyzable(entry.relkind) => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        })),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

fn auth_catalog_for_ddl(
    db: &Database,
    client_id: ClientId,
) -> Result<crate::pgrust::auth::AuthCatalog, ExecError> {
    db.auth_catalog(client_id, None).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "authorization catalog",
            actual: format!("{err:?}"),
        })
    })
}

pub(super) fn relation_kind_name(relkind: char) -> &'static str {
    match relkind {
        'c' => "type",
        'm' => "materialized view",
        'p' => "partitioned table",
        'S' => "sequence",
        'v' => "view",
        'i' | 'I' => "index",
        _ => "table",
    }
}

pub(super) fn ensure_relation_owner(
    db: &Database,
    client_id: ClientId,
    relation: &BoundRelation,
    display_name: &str,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = auth_catalog_for_ddl(db, client_id)?;
    if auth.has_effective_membership(relation.owner_oid, &auth_catalog) {
        return Ok(());
    }
    let owner_object_kind = match relation.relkind {
        'p' => "table",
        _ => relation_kind_name(relation.relkind),
    };
    Err(ExecError::DetailedError {
        message: format!("must be owner of {} {}", owner_object_kind, display_name),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

pub(super) fn ensure_can_set_role(
    db: &Database,
    client_id: ClientId,
    role_oid: u32,
    role_name: &str,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = auth_catalog_for_ddl(db, client_id)?;
    if auth.can_set_role(role_oid, &auth_catalog) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be able to SET ROLE \"{role_name}\""),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
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

fn relation_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

pub(crate) fn reject_relation_with_referencing_foreign_keys(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    operation: &'static str,
) -> Result<(), ExecError> {
    let mut references = catalog
        .constraint_rows()
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_FOREIGN && row.confrelid == relation_oid)
        .map(|row| (row.conname, relation_name_for_oid(catalog, row.conrelid)))
        .collect::<Vec<_>>();
    references.sort();
    references.dedup();

    let Some((constraint_name, child_relation_name)) = references.into_iter().next() else {
        return Ok(());
    };
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: operation,
        actual: format!(
            "foreign key constraint \"{constraint_name}\" on table \"{child_relation_name}\" references this table"
        ),
    }))
}

pub(crate) fn reject_column_with_foreign_key_dependencies(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    column_name: &str,
    attnum: i16,
    operation: &'static str,
) -> Result<(), ExecError> {
    let mut messages = catalog
        .constraint_rows()
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_FOREIGN)
        .filter_map(|row| {
            if row.conrelid == relation_oid
                && row
                    .conkey
                    .as_ref()
                    .is_some_and(|attnums| attnums.contains(&attnum))
            {
                return Some(format!(
                    "column \"{column_name}\" is used by foreign key constraint \"{}\"",
                    row.conname
                ));
            }
            if row.confrelid == relation_oid
                && row
                    .confkey
                    .as_ref()
                    .is_some_and(|attnums| attnums.contains(&attnum))
            {
                return Some(format!(
                    "column \"{column_name}\" is referenced by foreign key constraint \"{}\" on table \"{}\"",
                    row.conname,
                    relation_name_for_oid(catalog, row.conrelid),
                ));
            }
            None
        })
        .collect::<Vec<_>>();
    messages.sort();
    messages.dedup();

    let Some(actual) = messages.into_iter().next() else {
        return Ok(());
    };
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: operation,
        actual,
    }))
}

pub(crate) fn reject_column_with_trigger_dependencies(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    column_name: &str,
    attnum: i16,
) -> Result<(), ExecError> {
    let Some(visible) = catalog.materialize_visible_catalog() else {
        return Ok(());
    };
    let relation_name = relation_name_for_oid(catalog, relation_oid);
    let trigger_rows = catalog.trigger_rows_for_relation(relation_oid);
    let details = visible
        .depend_rows()
        .into_iter()
        .filter(|row| {
            row.classid == PG_TRIGGER_RELATION_OID
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == relation_oid
                && row.refobjsubid == i32::from(attnum)
        })
        .filter_map(|row| {
            trigger_rows
                .iter()
                .find(|trigger| trigger.oid == row.objid)
                .map(|trigger| {
                    format!(
                        "trigger {} on table {} depends on column {} of table {}",
                        trigger.tgname, relation_name, column_name, relation_name
                    )
                })
        })
        .collect::<Vec<_>>();
    if details.is_empty() {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!(
            "cannot drop column {} of table {} because other objects depend on it",
            column_name, relation_name
        ),
        detail: Some(details.join("\n")),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
    })
}

pub(crate) fn reject_index_with_referencing_foreign_keys(
    catalog: &dyn CatalogLookup,
    index_oid: u32,
    operation: &'static str,
) -> Result<(), ExecError> {
    let mut references = catalog
        .constraint_rows()
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_FOREIGN && row.conindid == index_oid)
        .map(|row| (row.conname, relation_name_for_oid(catalog, row.conrelid)))
        .collect::<Vec<_>>();
    references.sort();
    references.dedup();

    let Some((constraint_name, child_relation_name)) = references.into_iter().next() else {
        return Ok(());
    };
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: operation,
        actual: format!(
            "foreign key constraint \"{constraint_name}\" on table \"{child_relation_name}\" depends on the referenced key"
        ),
    }))
}

pub(super) fn reject_type_with_dependents(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    type_oid: u32,
    display_name: &str,
) -> Result<(), ExecError> {
    let catcache = db.backend_catcache(client_id, txn_ctx).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "catalog lookup",
            actual: format!("{err:?}"),
        })
    })?;
    let format_name = |namespace_oid: u32, object_name: &str| {
        let schema_name = catcache
            .namespace_by_oid(namespace_oid)
            .map(|row| row.nspname.clone())
            .unwrap_or_else(|| "public".to_string());
        match schema_name.as_str() {
            "public" | "pg_catalog" => object_name.to_string(),
            _ => format!("{schema_name}.{object_name}"),
        }
    };

    let mut dependents = catcache
        .depend_rows()
        .into_iter()
        .filter(|row| {
            row.refclassid == PG_TYPE_RELATION_OID
                && row.refobjid == type_oid
                && row.deptype != DEPENDENCY_INTERNAL
        })
        .filter_map(|row| match row.classid {
            PG_CLASS_RELATION_OID => {
                let class = catcache.class_by_oid(row.objid)?;
                Some(format!(
                    "{} {}",
                    relation_kind_name(class.relkind),
                    format_name(class.relnamespace, &class.relname)
                ))
            }
            PG_PROC_RELATION_OID => {
                let proc_row = catcache.proc_by_oid(row.objid)?;
                Some(format!(
                    "function {}",
                    format_name(proc_row.pronamespace, &proc_row.proname)
                ))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    dependents.sort();
    dependents.dedup();
    if dependents.is_empty() {
        return Ok(());
    }

    Err(ExecError::DetailedError {
        message: format!("cannot drop type {display_name} because other objects depend on it"),
        detail: Some(
            dependents
                .into_iter()
                .map(|name| format!("{name} depends on type {display_name}"))
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        hint: None,
        sqlstate: "2BP01",
    })
}

pub(super) fn reject_inheritance_tree_ddl(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    operation: &'static str,
) -> Result<(), ExecError> {
    if catalog.has_subclass(relation_oid) || !catalog.inheritance_parents(relation_oid).is_empty() {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            operation.to_string(),
        )));
    }
    Ok(())
}

pub(super) fn validate_alter_table_add_column(
    table_name: &str,
    relation_desc: &RelationDesc,
    column: &ColumnDef,
    existing_constraints: &[crate::include::catalog::PgConstraintRow],
    catalog: &dyn CatalogLookup,
) -> Result<AlterTableAddColumnPlan, ExecError> {
    let serial_kind = match column.ty {
        RawTypeName::Serial(kind) => Some(kind),
        _ => None,
    };
    if column.primary_key() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ADD COLUMN without PRIMARY KEY",
            actual: "PRIMARY KEY".into(),
        }));
    }
    if column.unique() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ADD COLUMN without UNIQUE",
            actual: "UNIQUE".into(),
        }));
    }
    if is_system_column_name(&column.name) {
        return Err(ExecError::DetailedError {
            message: format!(
                "column name \"{}\" conflicts with a system column name",
                column.name
            ),
            detail: None,
            hint: None,
            sqlstate: "42701",
        });
    }
    if relation_desc
        .columns
        .iter()
        .any(|existing| existing.name.eq_ignore_ascii_case(&column.name))
    {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "new column name",
            actual: format!("column already exists: {}", column.name),
        }));
    }

    let sql_type = match column.ty {
        RawTypeName::Serial(_) => raw_type_name_hint(&column.ty),
        _ => resolve_raw_type_name(&column.ty, catalog).map_err(ExecError::Parse)?,
    };
    let mut desc = column_desc(column.name.clone(), sql_type, serial_kind.is_none());
    if serial_kind.is_some() && column.default_expr.is_some() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "serial column without explicit DEFAULT",
            actual: format!(
                "multiple default values specified for column \"{}\"",
                column.name
            ),
        }));
    }
    if serial_kind.is_none() {
        desc.default_expr = column.default_expr.clone();
        if let Some(sql) = desc.default_expr.as_deref() {
            desc.missing_default_value = Some(derive_literal_default_value(sql, desc.sql_type)?);
        }
    }
    let constraint_actions =
        normalize_alter_table_add_column_constraints(table_name, column, existing_constraints)
            .map_err(ExecError::Parse)?;
    Ok(AlterTableAddColumnPlan {
        column: desc,
        owned_sequence: serial_kind.map(|serial_kind| OwnedSequenceSpec {
            column_index: relation_desc.columns.len(),
            column_name: column.name.clone(),
            serial_kind,
            sql_type,
        }),
        not_null_action: constraint_actions.not_null,
        check_actions: constraint_actions.checks,
    })
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

#[derive(Debug, Clone)]
pub(super) struct AlterTableAddColumnPlan {
    pub column: ColumnDesc,
    pub owned_sequence: Option<OwnedSequenceSpec>,
    pub not_null_action: Option<NotNullConstraintAction>,
    pub check_actions: Vec<CheckConstraintAction>,
}

#[derive(Debug, Clone)]
pub(super) struct AlterColumnTypePlan {
    pub column_index: usize,
    pub rewrite_expr: Expr,
    pub new_column: ColumnDesc,
}

#[derive(Debug, Clone)]
pub(super) struct AlterColumnDefaultPlan {
    pub column_name: String,
    pub default_expr_sql: Option<String>,
    pub default_sequence_oid: Option<u32>,
}

fn is_text_like_type(ty: SqlType) -> bool {
    !ty.is_array
        && matches!(
            ty.kind,
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
        )
}

pub(crate) fn format_sql_type_name(sql_type: SqlType) -> &'static str {
    if sql_type.is_range() {
        return builtin_range_name_for_sql_type(sql_type).unwrap_or("range");
    }
    if sql_type.is_multirange() {
        return crate::include::catalog::builtin_multirange_name_for_sql_type(sql_type)
            .unwrap_or("multirange");
    }
    match sql_type.kind {
        SqlTypeKind::AnyElement => "anyelement",
        SqlTypeKind::AnyArray => "anyarray",
        SqlTypeKind::AnyRange => "anyrange",
        SqlTypeKind::AnyMultirange => "anymultirange",
        SqlTypeKind::AnyCompatible => "anycompatible",
        SqlTypeKind::AnyCompatibleArray => "anycompatiblearray",
        SqlTypeKind::AnyCompatibleRange => "anycompatiblerange",
        SqlTypeKind::AnyCompatibleMultirange => "anycompatiblemultirange",
        SqlTypeKind::Record | SqlTypeKind::Composite => "record",
        SqlTypeKind::Internal => "internal",
        SqlTypeKind::Trigger => "trigger",
        SqlTypeKind::Void => "void",
        SqlTypeKind::FdwHandler => "fdw_handler",
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int2Vector => "int2vector",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Int8 => "bigint",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Tid => "tid",
        SqlTypeKind::Xid => "xid",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::RegClass => "regclass",
        SqlTypeKind::RegType => "regtype",
        SqlTypeKind::RegRole => "regrole",
        SqlTypeKind::RegOperator => "regoperator",
        SqlTypeKind::RegProcedure => "regprocedure",
        SqlTypeKind::OidVector => "oidvector",
        SqlTypeKind::Bit => "bit",
        SqlTypeKind::VarBit => "bit varying",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::Inet => "inet",
        SqlTypeKind::Cidr => "cidr",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Money => "money",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
        SqlTypeKind::Xml => "xml",
        SqlTypeKind::Date => "date",
        SqlTypeKind::Time => "time without time zone",
        SqlTypeKind::TimeTz => "time with time zone",
        SqlTypeKind::TsVector => "tsvector",
        SqlTypeKind::TsQuery => "tsquery",
        SqlTypeKind::RegConfig => "regconfig",
        SqlTypeKind::RegDictionary => "regdictionary",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Bool => "boolean",
        SqlTypeKind::Point => "point",
        SqlTypeKind::Lseg => "lseg",
        SqlTypeKind::Path => "path",
        SqlTypeKind::Box => "box",
        SqlTypeKind::Polygon => "polygon",
        SqlTypeKind::Line => "line",
        SqlTypeKind::Circle => "circle",
        SqlTypeKind::Timestamp => "timestamp without time zone",
        SqlTypeKind::TimestampTz => "timestamp with time zone",
        SqlTypeKind::Interval => "interval",
        SqlTypeKind::PgNodeTree => "pg_node_tree",
        SqlTypeKind::InternalChar => "\"char\"",
        SqlTypeKind::Char => "character",
        SqlTypeKind::Varchar => "character varying",
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
        SqlTypeKind::Multirange => unreachable!("multirange handled above"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NormalizedStatisticsTarget {
    pub value: i16,
    pub warning: Option<&'static str>,
}

pub(super) fn normalize_statistics_target(
    statistics_target: i32,
) -> Result<NormalizedStatisticsTarget, ExecError> {
    if statistics_target == -1 {
        return Ok(NormalizedStatisticsTarget {
            value: -1,
            warning: None,
        });
    }
    if statistics_target < 0 {
        return Err(ExecError::DetailedError {
            message: format!("statistics target {} is too low", statistics_target),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    if statistics_target > 10000 {
        return Ok(NormalizedStatisticsTarget {
            value: 10000,
            warning: Some("lowering statistics target to 10000"),
        });
    }
    Ok(NormalizedStatisticsTarget {
        value: i16::try_from(statistics_target).map_err(|_| {
            ExecError::Parse(ParseError::InvalidInteger(statistics_target.to_string()))
        })?,
        warning: None,
    })
}

pub(super) fn automatic_alter_type_cast_allowed(
    catalog: &dyn CatalogLookup,
    from: SqlType,
    to: SqlType,
) -> bool {
    if from == to {
        return true;
    }
    if from.kind == to.kind && from.is_array == to.is_array {
        return true;
    }
    if is_text_like_type(from) && is_text_like_type(to) {
        return true;
    }
    let Some(source_oid) = catalog.type_oid_for_sql_type(from) else {
        return false;
    };
    let Some(target_oid) = catalog.type_oid_for_sql_type(to) else {
        return false;
    };
    catalog
        .cast_by_source_target(source_oid, target_oid)
        .is_some_and(|row| row.castcontext != 'e')
}

pub(super) fn validate_alter_table_alter_column_default(
    catalog: &dyn CatalogLookup,
    desc: &RelationDesc,
    column_name: &str,
    default_expr: Option<&SqlExpr>,
    default_expr_sql: Option<&str>,
) -> Result<AlterColumnDefaultPlan, ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN DEFAULT",
            actual: column_name.to_string(),
        }));
    }
    let column_index = desc
        .columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
        })
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;
    let current_column = &desc.columns[column_index];

    if let Some(expr) = default_expr {
        let (_bound, default_type) =
            bind_scalar_expr_in_scope(expr, &[], catalog).map_err(ExecError::Parse)?;
        if !automatic_alter_type_cast_allowed(catalog, default_type, current_column.sql_type) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "column \"{}\" is of type {} but default expression is of type {}",
                    current_column.name,
                    format_sql_type_name(current_column.sql_type),
                    format_sql_type_name(default_type),
                ),
                detail: None,
                hint: Some("You will need to rewrite or cast the expression.".into()),
                sqlstate: "42804",
            });
        }
    }

    Ok(AlterColumnDefaultPlan {
        column_name: current_column.name.clone(),
        default_expr_sql: default_expr_sql.map(str::to_string),
        default_sequence_oid: default_expr_sql
            .and_then(crate::pgrust::database::default_sequence_oid_from_default_expr),
    })
}

pub(super) fn validate_alter_table_alter_column_options(
    desc: &RelationDesc,
    column_name: &str,
) -> Result<String, ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN SET/RESET options",
            actual: column_name.to_string(),
        }));
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;
    Ok(column.name.clone())
}

pub(super) fn validate_alter_table_alter_column_storage(
    desc: &RelationDesc,
    column_name: &str,
    storage: AttributeStorage,
) -> Result<(String, AttributeStorage), ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN SET STORAGE",
            actual: column_name.to_string(),
        }));
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;

    let type_default = column_desc("attstorage_check", column.sql_type, column.storage.nullable)
        .storage
        .attstorage;
    if storage != AttributeStorage::Plain && type_default == AttributeStorage::Plain {
        return Err(ExecError::DetailedError {
            message: format!(
                "column data type {} can only have storage PLAIN",
                format_sql_type_name(column.sql_type)
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    Ok((column.name.clone(), storage))
}

pub(super) fn validate_alter_table_alter_column_compression(
    desc: &RelationDesc,
    column_name: &str,
    compression: AttributeCompression,
) -> Result<(String, AttributeCompression), ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN SET COMPRESSION",
            actual: column_name.to_string(),
        }));
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;

    ensure_attribute_compression_supported(compression)?;

    let type_default = column_desc(
        "attcompression_check",
        column.sql_type,
        column.storage.nullable,
    )
    .storage
    .attstorage;
    if compression != AttributeCompression::Default && type_default == AttributeStorage::Plain {
        return Err(ExecError::DetailedError {
            message: format!(
                "column data type {} does not support compression",
                format_sql_type_name(column.sql_type)
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    Ok((column.name.clone(), compression))
}

pub(super) fn validate_alter_table_alter_column_statistics(
    desc: &RelationDesc,
    column_name: &str,
    statistics_target: i32,
) -> Result<(String, NormalizedStatisticsTarget), ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN SET STATISTICS",
            actual: column_name.to_string(),
        }));
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;
    Ok((
        column.name.clone(),
        normalize_statistics_target(statistics_target)?,
    ))
}

pub(super) fn validate_alter_index_alter_column_statistics(
    entry: &RelCacheEntry,
    index_name: &str,
    column_number: i16,
    statistics_target: i32,
) -> Result<(String, NormalizedStatisticsTarget), ExecError> {
    let index_meta = entry
        .index
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("relation \"{index_name}\" is not an index"),
            detail: None,
            hint: None,
            sqlstate: "42809",
        })?;
    let column_index = usize::try_from(column_number - 1).unwrap_or(usize::MAX);
    let column = entry
        .desc
        .columns
        .get(column_index)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!(
                "column number {} of relation \"{}\" does not exist",
                column_number, index_name
            ),
            detail: None,
            hint: None,
            sqlstate: "42703",
        })?;
    if column_number > index_meta.indnkeyatts {
        return Err(ExecError::DetailedError {
            message: format!(
                "cannot alter statistics on included column \"{}\" of index \"{}\"",
                column.name, index_name
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    if index_meta
        .indkey
        .get(column_index)
        .copied()
        .is_none_or(|attnum| attnum != 0)
    {
        return Err(ExecError::DetailedError {
            message: format!(
                "cannot alter statistics on non-expression column \"{}\" of index \"{}\"",
                column.name, index_name
            ),
            detail: None,
            hint: Some("Alter statistics on table column instead.".into()),
            sqlstate: "0A000",
        });
    }
    Ok((
        column.name.clone(),
        normalize_statistics_target(statistics_target)?,
    ))
}

fn alter_column_type_error(
    message: String,
    hint: Option<String>,
) -> Result<AlterColumnTypePlan, ExecError> {
    Err(ExecError::DetailedError {
        message,
        detail: None,
        hint,
        sqlstate: "42804",
    })
}

pub(super) fn validate_alter_table_alter_column_type(
    catalog: &dyn CatalogLookup,
    desc: &RelationDesc,
    column_name: &str,
    ty: &RawTypeName,
    using_expr: Option<&SqlExpr>,
) -> Result<AlterColumnTypePlan, ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN TYPE",
            actual: column_name.to_string(),
        }));
    }

    let column_index = desc
        .columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
        })
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;
    let current_column = &desc.columns[column_index];
    let target_sql_type = match ty {
        RawTypeName::Builtin(sql_type) => *sql_type,
        RawTypeName::Serial(kind) => {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "{} is only allowed in CREATE TABLE / ALTER TABLE ADD COLUMN",
                match kind {
                    crate::backend::parser::SerialKind::Small => "smallserial",
                    crate::backend::parser::SerialKind::Regular => "serial",
                    crate::backend::parser::SerialKind::Big => "bigserial",
                }
            ))));
        }
        RawTypeName::Record => {
            return Err(ExecError::Parse(ParseError::UnsupportedType(
                "record".into(),
            )));
        }
        RawTypeName::Named { name, .. } => {
            return Err(ExecError::Parse(ParseError::UnsupportedType(name.clone())));
        }
    };

    if let Some(default_sql) = current_column.default_expr.as_deref() {
        let default_expr =
            crate::backend::parser::parse_expr(default_sql).map_err(ExecError::Parse)?;
        let (_bound_default, default_type) =
            bind_scalar_expr_in_scope(&default_expr, &[], catalog).map_err(ExecError::Parse)?;
        if !automatic_alter_type_cast_allowed(catalog, default_type, target_sql_type) {
            return alter_column_type_error(
                format!(
                    "default for column \"{}\" cannot be cast automatically to type {}",
                    current_column.name,
                    format_sql_type_name(target_sql_type),
                ),
                None,
            );
        }
    }

    let scope_columns = desc
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.sql_type))
        .collect::<Vec<_>>();
    let (rewrite_expr, rewrite_type) = match using_expr {
        Some(expr) => {
            let (bound, from_type) = bind_scalar_expr_in_scope(expr, &scope_columns, catalog)
                .map_err(ExecError::Parse)?;
            (bound, from_type)
        }
        None => (
            Expr::Var(Var {
                varno: 1,
                varattno: user_attrno(column_index),
                varlevelsup: 0,
                vartype: current_column.sql_type,
            }),
            current_column.sql_type,
        ),
    };

    if !automatic_alter_type_cast_allowed(catalog, rewrite_type, target_sql_type) {
        if using_expr.is_some() {
            return alter_column_type_error(
                format!(
                    "result of USING clause for column \"{}\" cannot be cast automatically to type {}",
                    current_column.name,
                    format_sql_type_name(target_sql_type),
                ),
                Some("You might need to add an explicit cast.".into()),
            );
        }
        return alter_column_type_error(
            format!(
                "column \"{}\" cannot be cast automatically to type {}",
                current_column.name,
                format_sql_type_name(target_sql_type),
            ),
            Some(format!(
                "You might need to specify \"USING {}::{}\".",
                current_column.name,
                format_sql_type_name(target_sql_type),
            )),
        );
    }

    let mut new_column = column_desc(
        current_column.name.clone(),
        target_sql_type,
        current_column.storage.nullable,
    );
    new_column.storage.attstorage = current_column.storage.attstorage;
    new_column.storage.attcompression = current_column.storage.attcompression;
    new_column.attstattarget = current_column.attstattarget;
    new_column.not_null_constraint_oid = current_column.not_null_constraint_oid;
    new_column.not_null_constraint_name = current_column.not_null_constraint_name.clone();
    new_column.not_null_constraint_validated = current_column.not_null_constraint_validated;
    new_column.not_null_primary_key_owned = current_column.not_null_primary_key_owned;
    new_column.attrdef_oid = current_column.attrdef_oid;
    new_column.default_expr = current_column.default_expr.clone();
    new_column.missing_default_value = if current_column.default_sequence_oid.is_some() {
        None
    } else {
        current_column
            .default_expr
            .as_deref()
            .and_then(|sql| derive_literal_default_value(sql, target_sql_type).ok())
    };

    Ok(AlterColumnTypePlan {
        column_index,
        rewrite_expr: if rewrite_type == target_sql_type {
            rewrite_expr
        } else {
            Expr::Cast(Box::new(rewrite_expr), target_sql_type)
        },
        new_column,
    })
}

pub(super) fn map_catalog_error(err: CatalogError) -> ExecError {
    match err {
        CatalogError::TableAlreadyExists(name) => {
            ExecError::Parse(ParseError::TableAlreadyExists(name))
        }
        CatalogError::TypeAlreadyExists(name) => ExecError::DetailedError {
            message: format!("type \"{name}\" already exists"),
            detail: None,
            hint: None,
            sqlstate: "42710",
        },
        CatalogError::UnknownTable(name) => ExecError::Parse(ParseError::TableDoesNotExist(name)),
        CatalogError::UnknownColumn(name) => ExecError::Parse(ParseError::UnknownColumn(name)),
        CatalogError::UnknownType(name) => ExecError::Parse(ParseError::UnsupportedType(name)),
        CatalogError::UniqueViolation(constraint) => ExecError::UniqueViolation {
            constraint,
            detail: None,
        },
        CatalogError::Interrupted(reason) => ExecError::Interrupted(reason),
        CatalogError::Io(message) if message.starts_with("index row size ") => {
            ExecError::DetailedError {
                message,
                detail: None,
                hint: Some("Values larger than 1/3 of a buffer page cannot be indexed.".into()),
                sqlstate: "54000",
            }
        }
        CatalogError::Io(_) | CatalogError::Corrupt(_) => {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "valid catalog state",
                actual: "catalog error".into(),
            })
        }
    }
}
