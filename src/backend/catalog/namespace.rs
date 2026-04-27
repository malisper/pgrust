#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogNamespace {
    PgCatalog,
    Public,
    Temp,
}

use crate::backend::parser::{
    CreateTableAsStatement, CreateTableStatement, CreateViewStatement, ParseError,
    TablePersistence, normalize_create_table_as_name, normalize_create_table_name,
    normalize_create_view_name,
};

pub fn effective_search_path(
    temp_namespace_name: Option<&str>,
    configured_search_path: Option<&[String]>,
) -> Vec<String> {
    let mut path = Vec::new();
    let explicit = configured_search_path
        .map(|search_path| {
            search_path
                .iter()
                .map(|schema| schema.trim().to_ascii_lowercase())
                .filter(|schema| !schema.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec!["public".into()]);

    if let Some(namespace) = temp_namespace_name
        && !explicit
            .iter()
            .any(|schema| schema == "pg_temp" || schema == namespace)
    {
        path.push(namespace.to_string());
    }
    if !explicit.iter().any(|schema| schema == "pg_catalog") {
        path.push("pg_catalog".into());
    }
    for schema in explicit {
        if schema == "pg_temp" {
            if let Some(namespace) = temp_namespace_name
                && !path.iter().any(|existing| existing == namespace)
            {
                path.push(namespace.to_string());
            }
            continue;
        }
        if !path.iter().any(|existing| existing == &schema) {
            path.push(schema);
        }
    }
    path
}

pub fn resolve_unqualified_create_persistence(
    table_name: &str,
    persistence: TablePersistence,
    configured_search_path: Option<&[String]>,
) -> Result<TablePersistence, ParseError> {
    if persistence != TablePersistence::Permanent {
        return Ok(persistence);
    }

    let Some(search_path) = configured_search_path else {
        return Ok(persistence);
    };

    for schema in search_path {
        let schema = schema.trim().to_ascii_lowercase();
        match schema.as_str() {
            "" | "$user" => continue,
            "public" => return Ok(persistence),
            "pg_temp" => return Ok(TablePersistence::Temporary),
            "pg_catalog" => {
                return Err(ParseError::UnsupportedQualifiedName(format!(
                    "pg_catalog.{table_name}"
                )));
            }
            _ => continue,
        }
    }

    Err(ParseError::NoSchemaSelectedForCreate)
}

pub fn normalize_create_table_stmt_with_search_path(
    stmt: &CreateTableStatement,
    configured_search_path: Option<&[String]>,
) -> Result<(String, TablePersistence), ParseError> {
    let (table_name, persistence) = normalize_create_table_name(stmt)?;
    if stmt.schema_name.is_some() {
        return Ok((table_name, persistence));
    }
    Ok((
        table_name.clone(),
        resolve_unqualified_create_persistence(&table_name, persistence, configured_search_path)?,
    ))
}

pub fn normalize_create_table_as_stmt_with_search_path(
    stmt: &CreateTableAsStatement,
    configured_search_path: Option<&[String]>,
) -> Result<(String, TablePersistence), ParseError> {
    let (table_name, persistence) = normalize_create_table_as_name(stmt)?;
    if stmt.schema_name.is_some() {
        return Ok((table_name, persistence));
    }
    Ok((
        table_name.clone(),
        resolve_unqualified_create_persistence(&table_name, persistence, configured_search_path)?,
    ))
}

pub fn normalize_create_view_stmt_with_search_path(
    stmt: &CreateViewStatement,
    configured_search_path: Option<&[String]>,
) -> Result<String, ParseError> {
    let view_name = normalize_create_view_name(stmt)?;
    if stmt.schema_name.is_some() {
        return Ok(view_name);
    }
    resolve_unqualified_create_persistence(&view_name, stmt.persistence, configured_search_path)?;
    Ok(view_name)
}
