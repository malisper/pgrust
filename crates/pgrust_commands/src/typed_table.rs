use pgrust_analyze::{BoundRelation, CatalogLookup};
use pgrust_catalog_data::{PG_CATALOG_NAMESPACE_OID, PgTypeRow};
use pgrust_nodes::parsenodes::{ParseError, SqlTypeKind};
use pgrust_nodes::primnodes::{ColumnDesc, RelationDesc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedTableError {
    Parse(ParseError),
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

pub fn resolve_standalone_composite_type(
    catalog: &dyn CatalogLookup,
    type_name: &str,
) -> Result<(PgTypeRow, BoundRelation), TypedTableError> {
    let type_row = catalog.type_by_name(type_name).ok_or_else(|| {
        TypedTableError::Parse(ParseError::UnsupportedType(type_name.to_string()))
    })?;
    if matches!(type_row.sql_type.kind, SqlTypeKind::Shell) {
        return Err(detailed_error(
            format!("type \"{}\" is only a shell", type_row.typname),
            None,
            "42809",
        ));
    }
    if !matches!(type_row.sql_type.kind, SqlTypeKind::Composite) || type_row.typrelid == 0 {
        return Err(detailed_error(
            format!("type {} is not a composite type", type_row.typname),
            None,
            "42809",
        ));
    }
    let class_row = catalog
        .class_row_by_oid(type_row.typrelid)
        .ok_or_else(|| missing_composite_relation(type_row.typrelid))?;
    if class_row.relkind != 'c' {
        return Err(detailed_error(
            format!("type {} is the row type of another table", type_row.typname),
            Some(
                "A typed table must use a stand-alone composite type created with CREATE TYPE."
                    .into(),
            ),
            "42809",
        ));
    }
    let relation = catalog
        .lookup_relation_by_oid(type_row.typrelid)
        .ok_or_else(|| missing_composite_relation(type_row.typrelid))?;
    Ok((type_row, relation))
}

pub fn reject_typed_table_ddl(
    relation: &BoundRelation,
    operation: &str,
) -> Result<(), TypedTableError> {
    if relation.of_type_oid != 0 {
        return Err(detailed_error(
            format!("cannot {operation} typed table"),
            None,
            "42809",
        ));
    }
    Ok(())
}

pub fn reject_alter_table_of_target(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    operation: &str,
) -> Result<(), TypedTableError> {
    if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
        return Err(TypedTableError::Parse(ParseError::UnexpectedToken {
            expected: "user table for typed table operation",
            actual: "system catalog".into(),
        }));
    }
    if relation.relpersistence == 't' {
        return Err(detailed_error(
            format!("{operation} is not supported for temporary tables"),
            None,
            "0A000",
        ));
    }
    if !catalog
        .inheritance_parents(relation.relation_oid)
        .is_empty()
        || catalog.has_subclass(relation.relation_oid)
    {
        return Err(detailed_error(
            "cannot change typed-table status of inherited table",
            None,
            "42809",
        ));
    }
    Ok(())
}

pub fn validate_typed_table_compatibility(
    relation: &BoundRelation,
    type_relation: &BoundRelation,
) -> Result<(), TypedTableError> {
    let table_columns = visible_columns(&relation.desc);
    let type_columns = visible_columns(&type_relation.desc);
    if table_columns.len() != type_columns.len() {
        return Err(typed_table_mismatch_error(
            "table has a different number of columns",
        ));
    }
    for (table_column, type_column) in table_columns.into_iter().zip(type_columns) {
        if !table_column.name.eq_ignore_ascii_case(&type_column.name) {
            return Err(typed_table_mismatch_error(&format!(
                "column \"{}\" has a different name",
                table_column.name
            )));
        }
        if table_column.sql_type != type_column.sql_type {
            return Err(typed_table_mismatch_error(&format!(
                "column \"{}\" has a different type",
                table_column.name
            )));
        }
        if table_column.collation_oid != type_column.collation_oid {
            return Err(typed_table_mismatch_error(&format!(
                "column \"{}\" has a different collation",
                table_column.name
            )));
        }
    }
    Ok(())
}

fn visible_columns(desc: &RelationDesc) -> Vec<&ColumnDesc> {
    desc.columns
        .iter()
        .filter(|column| !column.dropped)
        .collect()
}

fn missing_composite_relation(typrelid: u32) -> TypedTableError {
    TypedTableError::Parse(ParseError::UnexpectedToken {
        expected: "composite type relation",
        actual: format!("missing relation oid {typrelid}"),
    })
}

fn typed_table_mismatch_error(detail: &str) -> TypedTableError {
    detailed_error(
        "table is not compatible with composite type",
        Some(detail.to_string()),
        "42809",
    )
}

fn detailed_error(
    message: impl Into<String>,
    detail: Option<String>,
    sqlstate: &'static str,
) -> TypedTableError {
    TypedTableError::Detailed {
        message: message.into(),
        detail,
        hint: None,
        sqlstate,
    }
}
