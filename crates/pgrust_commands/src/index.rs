use pgrust_analyze::{
    BoundRelation, CatalogLookup, is_system_column_name, resolve_collation_oid,
    sql_expr_mentions_system_column,
};
use pgrust_catalog_data::{
    ANYARRAYOID, ANYENUMOID, ANYMULTIRANGEOID, ANYRANGEOID, BIT_TYPE_OID, BOOL_TYPE_OID,
    BPCHAR_TYPE_OID, BRIN_AM_OID, BTREE_AM_OID, BYTEA_TYPE_OID, CIDR_TYPE_OID, DATE_TYPE_OID,
    ENUM_BTREE_OPCLASS_OID, ENUM_HASH_OPCLASS_OID, FLOAT4_TYPE_OID, FLOAT8_TYPE_OID, GIN_AM_OID,
    GIST_AM_OID, HASH_AM_OID, INET_TYPE_OID, INT2_TYPE_OID, INT4_TYPE_OID, INT8_TYPE_OID,
    INTERNAL_CHAR_TYPE_OID, INTERVAL_TYPE_OID, MONEY_TYPE_OID, NAME_TYPE_OID, NUMERIC_TYPE_OID,
    OID_TYPE_OID, PG_LSN_TYPE_OID, RECORD_TYPE_OID, SPGIST_AM_OID, TEXT_BRIN_MINMAX_OPCLASS_OID,
    TEXT_TYPE_OID, TID_TYPE_OID, TIMESTAMP_TYPE_OID, TIMESTAMPTZ_TYPE_OID, UUID_TYPE_OID,
    VARBIT_TYPE_OID, VARCHAR_TYPE_OID, bootstrap_pg_am_rows, builtin_range_rows,
    multirange_type_ref_for_sql_type, range_type_ref_for_sql_type,
};
use pgrust_catalog_data::{PgAmRow, PgOpclassRow};
use pgrust_catalog_store::CatalogIndexBuildOptions;
use pgrust_catalog_store::catcache::sql_type_oid;
use pgrust_nodes::parsenodes::{IndexColumnDef, ParseError, RelOption};
use pgrust_nodes::{SqlType, SqlTypeKind};

use crate::reloptions::{RelOptionError, index_reloptions};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexBuildError {
    Parse(ParseError),
    RelOption(RelOptionError),
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

pub fn create_index_access_method_row(
    method: Option<&str>,
) -> Result<(PgAmRow, Option<&'static str>), IndexBuildError> {
    let method = method.unwrap_or("btree");
    let (method, notice) = if method.eq_ignore_ascii_case("rtree") {
        (
            "gist",
            Some("substituting access method \"gist\" for obsolete method \"rtree\""),
        )
    } else {
        (method, None)
    };
    let row = bootstrap_pg_am_rows()
        .into_iter()
        .find(|row| row.amtype == 'i' && row.amname.eq_ignore_ascii_case(method))
        .ok_or_else(|| {
            IndexBuildError::Parse(ParseError::UnexpectedToken {
                expected: "supported index access method",
                actual: "unsupported index access method".into(),
            })
        })?;
    Ok((row, notice))
}

pub fn access_method_can_include(access_method_oid: u32) -> bool {
    matches!(
        access_method_oid,
        BTREE_AM_OID | GIST_AM_OID | SPGIST_AM_OID
    )
}

pub fn resolve_index_include_columns(
    relation: &BoundRelation,
    include_names: &[String],
    access_method: &PgAmRow,
) -> Result<Vec<IndexColumnDef>, IndexBuildError> {
    let include_columns = include_names
        .iter()
        .map(|name| {
            if is_system_column_name(name) {
                return Err(index_system_column_error());
            }
            if !relation
                .desc
                .columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(name))
            {
                return Err(IndexBuildError::Parse(ParseError::UnknownColumn(
                    name.clone(),
                )));
            }
            Ok(IndexColumnDef::from(name.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !include_columns.is_empty() && !access_method_can_include(access_method.oid) {
        return Err(IndexBuildError::Detailed {
            message: format!(
                "access method \"{}\" does not support included columns",
                access_method.amname
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(include_columns)
}

pub fn index_column_sql_type(
    relation: &BoundRelation,
    column: &IndexColumnDef,
) -> Result<SqlType, IndexBuildError> {
    if column.expr_sql.is_some() {
        return column.expr_type.ok_or_else(|| {
            IndexBuildError::Parse(ParseError::UnexpectedToken {
                expected: "inferred expression index type",
                actual: "missing expression index type".into(),
            })
        });
    }
    relation
        .desc
        .columns
        .iter()
        .find(|desc| desc.name.eq_ignore_ascii_case(&column.name))
        .map(|desc| desc.sql_type)
        .ok_or_else(|| IndexBuildError::Parse(ParseError::UnknownColumn(column.name.clone())))
}

pub fn reject_system_columns_in_index(
    columns: &[IndexColumnDef],
    predicate_sql: Option<&str>,
) -> Result<(), IndexBuildError> {
    for column in columns {
        if column
            .expr_sql
            .as_deref()
            .is_some_and(sql_expr_mentions_system_column)
            || (column.expr_sql.is_none() && is_system_column_name(&column.name))
        {
            return Err(index_system_column_error());
        }
    }
    if predicate_sql.is_some_and(sql_expr_mentions_system_column) {
        return Err(index_system_column_error());
    }
    Ok(())
}

fn index_system_column_error() -> IndexBuildError {
    IndexBuildError::Detailed {
        message: "index creation on system columns is not supported".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

pub fn index_column_type_oid(catalog: &dyn CatalogLookup, sql_type: SqlType) -> Option<u32> {
    let catalog_oid = sql_type_oid(sql_type);
    if catalog_oid != 0 {
        return Some(catalog_oid);
    }
    if (sql_type.is_range() || sql_type.is_multirange()) && sql_type.type_oid != 0 {
        return Some(sql_type.type_oid);
    }
    range_type_ref_for_sql_type(sql_type)
        .map(|range_type| range_type.type_oid())
        .or_else(|| {
            multirange_type_ref_for_sql_type(sql_type)
                .map(|multirange_type| multirange_type.type_oid())
        })
        .or_else(|| {
            (matches!(sql_type.element_type().kind, SqlTypeKind::Enum)
                && sql_type.element_type().type_oid != 0)
                .then_some(sql_type.element_type().type_oid)
        })
        .or_else(|| {
            catalog
                .type_rows()
                .into_iter()
                .find(|row| row.sql_type == sql_type)
                .map(|row| row.oid)
        })
}

pub fn opclass_accepts_type(opclass: &PgOpclassRow, type_oid: u32, sql_type: SqlType) -> bool {
    opclass.opcintype == type_oid
        || opclass_accepts_sql_type(opclass.opcintype, sql_type)
        || (matches!(
            opclass.opcintype,
            TEXT_TYPE_OID | BPCHAR_TYPE_OID | VARCHAR_TYPE_OID
        ) && (matches!(
            type_oid,
            NAME_TYPE_OID | TEXT_TYPE_OID | BPCHAR_TYPE_OID | VARCHAR_TYPE_OID
        ) || matches!(
            sql_type.kind,
            SqlTypeKind::Name | SqlTypeKind::Text | SqlTypeKind::Char | SqlTypeKind::Varchar
        )))
        || (opclass.opcintype == INET_TYPE_OID && type_oid == CIDR_TYPE_OID)
        || (opclass.opcintype == ANYARRAYOID && sql_type.is_array)
        || (opclass.opcintype == ANYRANGEOID
            && (sql_type.is_range()
                || range_type_ref_for_sql_type(sql_type).is_some()
                || builtin_range_rows()
                    .iter()
                    .any(|row| row.rngtypid == type_oid)))
        || (opclass.opcintype == ANYMULTIRANGEOID
            && (sql_type.is_multirange() || multirange_type_ref_for_sql_type(sql_type).is_some()))
        || (opclass.opcintype == ANYENUMOID
            && matches!(sql_type.element_type().kind, SqlTypeKind::Enum))
        || (opclass.opcintype == RECORD_TYPE_OID
            && matches!(sql_type.kind, SqlTypeKind::Record | SqlTypeKind::Composite))
}

pub fn opclass_accepts_sql_type(opcintype: u32, sql_type: SqlType) -> bool {
    if sql_type.is_array {
        return opcintype == ANYARRAYOID;
    }
    match sql_type.kind {
        SqlTypeKind::Bool => opcintype == BOOL_TYPE_OID,
        SqlTypeKind::Int2 => opcintype == INT2_TYPE_OID,
        SqlTypeKind::Int4 => opcintype == INT4_TYPE_OID,
        SqlTypeKind::Int8 => opcintype == INT8_TYPE_OID,
        SqlTypeKind::Oid => opcintype == OID_TYPE_OID,
        SqlTypeKind::Tid => opcintype == TID_TYPE_OID,
        SqlTypeKind::InternalChar => opcintype == INTERNAL_CHAR_TYPE_OID,
        SqlTypeKind::Name => opcintype == NAME_TYPE_OID,
        SqlTypeKind::Text => opcintype == TEXT_TYPE_OID,
        SqlTypeKind::Varchar => opcintype == VARCHAR_TYPE_OID,
        SqlTypeKind::Char => opcintype == BPCHAR_TYPE_OID,
        SqlTypeKind::Float4 => opcintype == FLOAT4_TYPE_OID,
        SqlTypeKind::Float8 => opcintype == FLOAT8_TYPE_OID,
        SqlTypeKind::Numeric => opcintype == NUMERIC_TYPE_OID,
        SqlTypeKind::Money => opcintype == MONEY_TYPE_OID,
        SqlTypeKind::Interval => opcintype == INTERVAL_TYPE_OID,
        SqlTypeKind::Date => opcintype == DATE_TYPE_OID,
        SqlTypeKind::Timestamp => opcintype == TIMESTAMP_TYPE_OID,
        SqlTypeKind::TimestampTz => opcintype == TIMESTAMPTZ_TYPE_OID,
        SqlTypeKind::Bytea => opcintype == BYTEA_TYPE_OID,
        SqlTypeKind::Uuid => opcintype == UUID_TYPE_OID,
        SqlTypeKind::Bit => opcintype == BIT_TYPE_OID,
        SqlTypeKind::VarBit => opcintype == VARBIT_TYPE_OID,
        SqlTypeKind::Cidr => matches!(opcintype, CIDR_TYPE_OID | INET_TYPE_OID),
        SqlTypeKind::Inet => opcintype == INET_TYPE_OID,
        SqlTypeKind::PgLsn => opcintype == PG_LSN_TYPE_OID,
        SqlTypeKind::Composite | SqlTypeKind::Record => opcintype == RECORD_TYPE_OID,
        _ => false,
    }
}

pub fn default_opclass_for_catalog_type(
    catalog: &dyn CatalogLookup,
    opclass_rows: &[PgOpclassRow],
    access_method_oid: u32,
    type_oid: u32,
    sql_type: SqlType,
) -> Option<PgOpclassRow> {
    if access_method_oid == BRIN_AM_OID
        && (type_oid == NAME_TYPE_OID || matches!(sql_type.kind, SqlTypeKind::Name))
    {
        return opclass_rows
            .iter()
            .find(|row| row.oid == TEXT_BRIN_MINMAX_OPCLASS_OID)
            .cloned();
    }
    if matches!(sql_type.element_type().kind, SqlTypeKind::Enum)
        || catalog
            .enum_rows()
            .iter()
            .any(|row| row.enumtypid == type_oid)
    {
        let fallback_oid = match access_method_oid {
            BTREE_AM_OID => Some(ENUM_BTREE_OPCLASS_OID),
            HASH_AM_OID => Some(ENUM_HASH_OPCLASS_OID),
            _ => None,
        };
        if let Some(fallback_oid) = fallback_oid {
            return opclass_rows
                .iter()
                .find(|row| row.oid == fallback_oid)
                .cloned();
        }
        return opclass_rows
            .iter()
            .find(|row| {
                row.opcmethod == access_method_oid && row.opcdefault && row.opcintype == ANYENUMOID
            })
            .cloned();
    }
    opclass_rows
        .iter()
        .find(|row| {
            row.opcmethod == access_method_oid
                && row.opcdefault
                && opclass_accepts_type(row, type_oid, sql_type)
        })
        .cloned()
}

pub fn resolve_create_index_build_options(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    access_method: &PgAmRow,
    columns: &[IndexColumnDef],
    options: &[RelOption],
) -> Result<CatalogIndexBuildOptions, IndexBuildError> {
    let opclass_rows = catalog.opclass_rows();
    let mut indclass = Vec::with_capacity(columns.len());
    let mut indclass_options = Vec::with_capacity(columns.len());
    let mut indcollation = Vec::with_capacity(columns.len());
    let mut indoption = Vec::with_capacity(columns.len());

    for column in columns {
        let sql_type = index_column_sql_type(relation, column)?;
        let type_oid = index_column_type_oid(catalog, sql_type).ok_or_else(|| {
            IndexBuildError::Parse(ParseError::UnsupportedType(
                column
                    .expr_sql
                    .clone()
                    .unwrap_or_else(|| column.name.clone()),
            ))
        })?;
        let type_name = catalog
            .type_by_oid(type_oid)
            .map(|row| row.typname)
            .unwrap_or_else(|| type_oid.to_string());
        let opclass = if let Some(opclass_name) = column.opclass.as_deref() {
            let opclass_lookup_name = opclass_name
                .rsplit_once('.')
                .map(|(_, name)| name)
                .unwrap_or(opclass_name);
            opclass_rows
                .iter()
                .find(|row| {
                    row.opcmethod == access_method.oid
                        && row.opcname.eq_ignore_ascii_case(opclass_lookup_name)
                        && opclass_accepts_type(row, type_oid, sql_type)
                })
                .cloned()
        } else {
            default_opclass_for_catalog_type(
                catalog,
                &opclass_rows,
                access_method.oid,
                type_oid,
                sql_type,
            )
        }
        .ok_or_else(|| {
            IndexBuildError::Parse(ParseError::MissingDefaultOpclass {
                access_method: access_method.amname.clone(),
                type_name,
            })
        })?;
        indclass_options.push(
            crate::reloptions::resolve_index_opclass_options(access_method.oid, &opclass, column)
                .map_err(IndexBuildError::RelOption)?,
        );
        indclass.push(opclass.oid);
        indcollation.push(
            column
                .collation
                .as_deref()
                .map(|collation| resolve_collation_oid(collation, catalog))
                .transpose()
                .map_err(IndexBuildError::Parse)?
                .unwrap_or(0),
        );
        let mut option = 0i16;
        if column.descending {
            option |= 0x0001;
        }
        if column.nulls_first.unwrap_or(column.descending) {
            option |= 0x0002;
        }
        indoption.push(option);
    }

    let (btree_options, brin_options, gist_options, gin_options, hash_options) =
        match access_method.oid {
            BTREE_AM_OID => (
                crate::reloptions::resolve_btree_options(options)
                    .map_err(IndexBuildError::RelOption)?,
                None,
                None,
                None,
                None,
            ),
            BRIN_AM_OID => (
                None,
                Some(
                    crate::reloptions::resolve_brin_options(options)
                        .map_err(IndexBuildError::RelOption)?,
                ),
                None,
                None,
                None,
            ),
            GIST_AM_OID => (
                None,
                None,
                Some(
                    crate::reloptions::resolve_gist_options(options)
                        .map_err(IndexBuildError::RelOption)?,
                ),
                None,
                None,
            ),
            GIN_AM_OID => (
                None,
                None,
                None,
                Some(
                    crate::reloptions::resolve_gin_options(options)
                        .map_err(IndexBuildError::RelOption)?,
                ),
                None,
            ),
            HASH_AM_OID => (
                None,
                None,
                None,
                None,
                Some(
                    crate::reloptions::resolve_hash_options(options)
                        .map_err(IndexBuildError::RelOption)?,
                ),
            ),
            SPGIST_AM_OID => {
                crate::reloptions::resolve_spgist_options(options)
                    .map_err(IndexBuildError::RelOption)?;
                (None, None, None, None, None)
            }
            _ => {
                if !options.is_empty() {
                    return Err(IndexBuildError::Parse(ParseError::UnexpectedToken {
                        expected: "simple index definition",
                        actual: "unsupported CREATE INDEX feature".into(),
                    }));
                }
                (None, None, None, None, None)
            }
        };

    Ok(CatalogIndexBuildOptions {
        am_oid: access_method.oid,
        indclass,
        indclass_options,
        indcollation,
        indoption,
        reloptions: index_reloptions(options),
        indnullsnotdistinct: false,
        indisexclusion: false,
        indimmediate: true,
        btree_options,
        brin_options,
        gist_options,
        gin_options,
        hash_options,
    })
}

pub fn default_create_index_name(
    relation_exists: impl Fn(&str) -> bool,
    table_name: &str,
    columns: &[IndexColumnDef],
) -> String {
    let schema = table_name.rsplit_once('.').map(|(schema, _)| schema);
    let relname = table_name.rsplit('.').next().unwrap_or(table_name);
    let key = columns
        .iter()
        .find_map(|column| {
            (!column.name.trim().is_empty()).then(|| column.name.trim().to_ascii_lowercase())
        })
        .unwrap_or_else(|| "expr".into());
    let key = key
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    let base = format!("{relname}_{key}_idx").to_ascii_lowercase();
    for suffix in 0usize.. {
        let local = if suffix == 0 {
            base.clone()
        } else {
            format!("{base}{suffix}")
        };
        let qualified = schema
            .map(|schema| format!("{schema}.{local}"))
            .unwrap_or_else(|| local.clone());
        if !relation_exists(&qualified) {
            return qualified;
        }
    }
    unreachable!("unbounded index name search should always return")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_index_name_uses_first_named_column_and_suffixes_collisions() {
        let columns = vec![IndexColumnDef::from("a-b")];
        let name = default_create_index_name(
            |candidate| candidate == "public.t_a_b_idx",
            "public.t",
            &columns,
        );

        assert_eq!(name, "public.t_a_b_idx1");
    }

    #[test]
    fn access_method_lookup_maps_obsolete_rtree_to_gist_notice() {
        let (row, notice) = create_index_access_method_row(Some("rtree")).unwrap();

        assert_eq!(row.amname, "gist");
        assert_eq!(
            notice,
            Some("substituting access method \"gist\" for obsolete method \"rtree\"")
        );
    }

    #[test]
    fn rejects_system_columns_in_index_keys_and_predicate() {
        let columns = vec![IndexColumnDef::from("ctid")];
        assert!(reject_system_columns_in_index(&columns, None).is_err());
        assert!(reject_system_columns_in_index(&[], Some("xmin = 1")).is_err());
        assert!(reject_system_columns_in_index(&[IndexColumnDef::from("user_col")], None).is_ok());
    }

    #[test]
    fn opclass_accepts_name_as_text_compatible() {
        assert!(opclass_accepts_sql_type(
            TEXT_TYPE_OID,
            SqlType::new(SqlTypeKind::Text)
        ));
        assert!(opclass_accepts_sql_type(
            NAME_TYPE_OID,
            SqlType::new(SqlTypeKind::Name)
        ));
        assert!(!opclass_accepts_sql_type(
            INT4_TYPE_OID,
            SqlType::new(SqlTypeKind::Text)
        ));
    }
}
