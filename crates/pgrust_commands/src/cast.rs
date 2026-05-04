use std::collections::BTreeSet;

use pgrust_analyze::{CatalogLookup, resolve_raw_type_name};
use pgrust_catalog_data::{
    BOOL_TYPE_OID, DEPENDENCY_NORMAL, INT4_TYPE_OID, PG_CAST_RELATION_OID, PG_PROC_RELATION_OID,
    PG_TYPE_RELATION_OID, PgCastRow, PgDependRow, PgProcRow, PgTypeRow, builtin_type_name_for_oid,
};
use pgrust_nodes::parsenodes::{CastContext, ParseError, RawTypeName};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CastCommandError {
    Parse(ParseError),
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

pub fn cast_context_code(context: CastContext) -> char {
    match context {
        CastContext::Explicit => 'e',
        CastContext::Assignment => 'a',
        CastContext::Implicit => 'i',
    }
}

pub fn resolve_cast_type_oid(
    catalog: &dyn CatalogLookup,
    raw: &RawTypeName,
) -> Result<u32, CastCommandError> {
    let sql_type = resolve_raw_type_name(raw, catalog).map_err(CastCommandError::Parse)?;
    catalog
        .type_oid_for_sql_type(sql_type)
        .ok_or_else(|| CastCommandError::Parse(ParseError::UnsupportedType(format!("{raw:?}"))))
}

pub fn resolve_cast_type_row(
    catalog: &dyn CatalogLookup,
    raw: &RawTypeName,
) -> Result<PgTypeRow, CastCommandError> {
    let oid = resolve_cast_type_oid(catalog, raw)?;
    catalog
        .type_by_oid(oid)
        .ok_or_else(|| CastCommandError::Parse(ParseError::UnsupportedType(format!("{raw:?}"))))
}

pub fn validate_binary_cast_physical_compatibility(
    catalog: &dyn CatalogLookup,
    source: &PgTypeRow,
    target: &PgTypeRow,
    format_type: impl Fn(u32) -> String,
) -> Result<(), CastCommandError> {
    if source.typlen == target.typlen
        && source.typalign == target.typalign
        && source.typelem == 0
        && target.typelem == 0
        && !source.sql_type.is_array
        && !target.sql_type.is_array
    {
        return Ok(());
    }
    let _ = catalog;
    Err(detailed_error(
        "source and target data types are not physically compatible",
        Some(format!(
            "{} and {} have different physical storage metadata",
            format_type(source.oid),
            format_type(target.oid)
        )),
        "42P17",
    ))
}

pub fn cast_dependency(refclassid: u32, refobjid: u32) -> PgDependRow {
    PgDependRow {
        classid: PG_CAST_RELATION_OID,
        objid: 0,
        objsubid: 0,
        refclassid,
        refobjid,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }
}

pub fn maybe_type_dependency(type_oid: u32) -> Option<PgDependRow> {
    builtin_type_name_for_oid(type_oid)
        .is_none()
        .then(|| cast_dependency(PG_TYPE_RELATION_OID, type_oid))
}

pub fn validate_cast_function(
    catalog: &dyn CatalogLookup,
    proc_row: &PgProcRow,
    source_oid: u32,
    target_oid: u32,
) -> Result<Vec<PgDependRow>, CastCommandError> {
    let arg_oids = proc_row
        .proargtypes
        .split_whitespace()
        .filter_map(|oid| oid.parse::<u32>().ok())
        .collect::<Vec<_>>();
    if !(1..=3).contains(&arg_oids.len()) {
        return Err(detailed_error(
            "cast function must take one to three arguments",
            None,
            "42P17",
        ));
    }
    let first_arg_oid = arg_oids[0];
    let in_cast = binary_coercible_cast_row(catalog, source_oid, first_arg_oid);
    if source_oid != first_arg_oid
        && domain_base_type_oid(catalog, source_oid) != Some(first_arg_oid)
        && in_cast.is_none()
    {
        return Err(detailed_error(
            "argument of cast function must match or be binary-coercible from source data type",
            None,
            "42P17",
        ));
    }
    if arg_oids.get(1).is_some_and(|oid| *oid != INT4_TYPE_OID) {
        return Err(detailed_error(
            "second argument of cast function must be type integer",
            None,
            "42P17",
        ));
    }
    if arg_oids.get(2).is_some_and(|oid| *oid != BOOL_TYPE_OID) {
        return Err(detailed_error(
            "third argument of cast function must be type boolean",
            None,
            "42P17",
        ));
    }
    let out_cast = binary_coercible_cast_row(catalog, proc_row.prorettype, target_oid);
    if proc_row.prorettype != target_oid
        && domain_base_type_oid(catalog, target_oid) != Some(proc_row.prorettype)
        && out_cast.is_none()
    {
        return Err(detailed_error(
            "return data type of cast function must match or be binary-coercible to target data type",
            None,
            "42P17",
        ));
    }
    if proc_row.proretset {
        return Err(detailed_error(
            "cast function must not return a set",
            None,
            "42P17",
        ));
    }

    let mut depends = vec![cast_dependency(PG_PROC_RELATION_OID, proc_row.oid)];
    if let Some(row) = in_cast {
        depends.push(cast_dependency(PG_CAST_RELATION_OID, row.oid));
    }
    if let Some(row) = out_cast {
        depends.push(cast_dependency(PG_CAST_RELATION_OID, row.oid));
    }
    Ok(depends)
}

pub fn binary_coercible_cast_row(
    catalog: &dyn CatalogLookup,
    source_oid: u32,
    target_oid: u32,
) -> Option<PgCastRow> {
    if source_oid == target_oid {
        return None;
    }
    catalog
        .cast_by_source_target(source_oid, target_oid)
        .filter(|row| row.castmethod == 'b')
}

pub fn is_binary_coercible(catalog: &dyn CatalogLookup, source_oid: u32, target_oid: u32) -> bool {
    source_oid == target_oid || binary_coercible_cast_row(catalog, source_oid, target_oid).is_some()
}

pub fn domain_base_type_oid(catalog: &dyn CatalogLookup, type_oid: u32) -> Option<u32> {
    let mut current_oid = type_oid;
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(current_oid) {
            return None;
        }
        let row = catalog.type_by_oid(current_oid)?;
        if row.typtype != 'd' || row.typbasetype == 0 {
            return Some(current_oid);
        }
        current_oid = row.typbasetype;
    }
}

fn detailed_error(
    message: impl Into<String>,
    detail: Option<String>,
    sqlstate: &'static str,
) -> CastCommandError {
    CastCommandError::Detailed {
        message: message.into(),
        detail,
        hint: None,
        sqlstate,
    }
}
