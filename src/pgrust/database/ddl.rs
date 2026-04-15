use std::collections::BTreeMap;

use super::{CatalogTxnContext, ClientId, Database};
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{ColumnDesc, ExecError, Expr, RelationDesc};
use crate::backend::parser::{
    BoundRelation, CatalogLookup, ColumnDef, ParseError, RawTypeName, SqlExpr, SqlType,
    SqlTypeKind, bind_scalar_expr_in_scope, derive_literal_default_value, resolve_raw_type_name,
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
        'v' => "view",
        'i' => "index",
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
    Err(ExecError::DetailedError {
        message: format!(
            "must be owner of {} {}",
            relation_kind_name(relation.relkind),
            display_name
        ),
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

pub(super) fn validate_alter_table_add_column(
    desc: &RelationDesc,
    column: &ColumnDef,
    catalog: &dyn CatalogLookup,
) -> Result<crate::backend::executor::ColumnDesc, ExecError> {
    if !column.nullable() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ADD COLUMN without NOT NULL",
            actual: "NOT NULL".into(),
        }));
    }
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

    let sql_type = resolve_raw_type_name(&column.ty, catalog).map_err(ExecError::Parse)?;
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

#[derive(Debug, Clone)]
pub(super) struct AlterColumnTypePlan {
    pub column_index: usize,
    pub rewrite_expr: Expr,
    pub new_column: ColumnDesc,
}

fn is_text_like_type(ty: SqlType) -> bool {
    !ty.is_array
        && matches!(
            ty.kind,
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
        )
}

fn format_sql_type_name(sql_type: SqlType) -> &'static str {
    match sql_type.kind {
        SqlTypeKind::AnyArray => "anyarray",
        SqlTypeKind::Record | SqlTypeKind::Composite => "record",
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int2Vector => "int2vector",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Int8 => "bigint",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::OidVector => "oidvector",
        SqlTypeKind::Bit => "bit",
        SqlTypeKind::VarBit => "bit varying",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Money => "money",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
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
        SqlTypeKind::PgNodeTree => "pg_node_tree",
        SqlTypeKind::InternalChar => "\"char\"",
        SqlTypeKind::Char => "character",
        SqlTypeKind::Varchar => "character varying",
    }
}

fn automatic_alter_type_cast_allowed(
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
        None => (Expr::Column(column_index), current_column.sql_type),
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
    new_column.attstattarget = current_column.attstattarget;
    new_column.not_null_constraint_oid = current_column.not_null_constraint_oid;
    new_column.not_null_constraint_name = current_column.not_null_constraint_name.clone();
    new_column.not_null_constraint_validated = current_column.not_null_constraint_validated;
    new_column.not_null_primary_key_owned = current_column.not_null_primary_key_owned;
    new_column.attrdef_oid = current_column.attrdef_oid;
    new_column.default_expr = current_column.default_expr.clone();
    new_column.missing_default_value = current_column
        .default_expr
        .as_deref()
        .and_then(|sql| derive_literal_default_value(sql, target_sql_type).ok());

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
