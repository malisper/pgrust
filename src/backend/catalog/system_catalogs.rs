use crate::backend::catalog::bootstrap::{bootstrap_relation_desc, BootstrapCatalogKind};
use crate::backend::catalog::catalog::Catalog;
use crate::backend::executor::{Expr, Plan, QueryColumn, RelationDesc};
use crate::backend::utils::cache::syscache::{
    caches_for_catalog, relation_lookup_by_name,
};
use crate::include::catalog::{
    PgAttributeRow, PgClassRow, PgNamespaceRow, PgTypeRow, PG_ATTRIBUTE_RELATION_OID,
    PG_CLASS_RELATION_OID, PG_NAMESPACE_RELATION_OID, PG_TYPE_RELATION_OID,
};
use crate::include::nodes::datum::Value;

pub fn build_system_catalog_plan(name: &str, catalog: &Catalog) -> Option<(Plan, RelationDesc)> {
    let (relcache, catcache) = caches_for_catalog(catalog);
    let entry = relation_lookup_by_name(&relcache, name)?;
    let (rows, output_columns, desc) = match entry.relation_oid {
        PG_NAMESPACE_RELATION_OID => {
            let rows = catcache
                .namespace_rows()
                .iter()
                .cloned()
                .map(namespace_row)
                .collect::<Vec<_>>();
            let desc = bootstrap_relation_desc(BootstrapCatalogKind::PgNamespace);
            let output_columns = desc_to_query_columns(&desc);
            (rows, output_columns, desc)
        }
        PG_CLASS_RELATION_OID => {
            let rows = catcache
                .class_rows()
                .iter()
                .cloned()
                .map(pg_class_row)
                .collect::<Vec<_>>();
            let desc = bootstrap_relation_desc(BootstrapCatalogKind::PgClass);
            let output_columns = desc_to_query_columns(&desc);
            (rows, output_columns, desc)
        }
        PG_ATTRIBUTE_RELATION_OID => {
            let rows = catcache
                .attribute_rows()
                .iter()
                .cloned()
                .map(pg_attribute_row)
                .collect::<Vec<_>>();
            let desc = bootstrap_relation_desc(BootstrapCatalogKind::PgAttribute);
            let output_columns = desc_to_query_columns(&desc);
            (rows, output_columns, desc)
        }
        PG_TYPE_RELATION_OID => {
            let rows = catcache
                .type_rows()
                .iter()
                .cloned()
                .map(pg_type_row)
                .collect::<Vec<_>>();
            let desc = bootstrap_relation_desc(BootstrapCatalogKind::PgType);
            let output_columns = desc_to_query_columns(&desc);
            (rows, output_columns, desc)
        }
        _ => return None,
    };

    Some((
        Plan::Values { rows, output_columns },
        desc,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::parser::{SqlType, SqlTypeKind};

    #[test]
    fn pg_class_plan_contains_bootstrap_and_user_relations() {
        let mut catalog = Catalog::default();
        catalog
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let (plan, desc) = build_system_catalog_plan("pg_class", &catalog).unwrap();
        assert_eq!(desc.columns.len(), 6);
        let rows = match plan {
            Plan::Values { rows, .. } => rows,
            other => panic!("expected values plan, got {other:?}"),
        };
        assert!(rows.iter().any(|row| matches!(&row[1], Expr::Const(Value::Text(name)) if name.as_str() == "pg_class")));
        assert!(rows.iter().any(|row| matches!(&row[1], Expr::Const(Value::Text(name)) if name.as_str() == "people")));
    }
}

fn desc_to_query_columns(desc: &RelationDesc) -> Vec<QueryColumn> {
    desc.columns
        .iter()
        .map(|col| QueryColumn {
            name: col.name.clone(),
            sql_type: col.sql_type,
        })
        .collect()
}

fn namespace_row(row: PgNamespaceRow) -> Vec<Expr> {
    vec![
        Expr::Const(Value::Int32(row.oid as i32)),
        Expr::Const(Value::Text(row.nspname.into())),
    ]
}

fn pg_class_row(row: PgClassRow) -> Vec<Expr> {
    vec![
        Expr::Const(Value::Int32(row.oid as i32)),
        Expr::Const(Value::Text(row.relname.into())),
        Expr::Const(Value::Int32(row.relnamespace as i32)),
        Expr::Const(Value::Int32(row.reltype as i32)),
        Expr::Const(Value::Int32(row.relfilenode as i32)),
        Expr::Const(Value::InternalChar(row.relkind as u8)),
    ]
}

fn pg_attribute_row(row: PgAttributeRow) -> Vec<Expr> {
    vec![
        Expr::Const(Value::Int32(row.attrelid as i32)),
        Expr::Const(Value::Text(row.attname.into())),
        Expr::Const(Value::Int32(row.atttypid as i32)),
        Expr::Const(Value::Int16(row.attnum)),
        Expr::Const(Value::Bool(row.attnotnull)),
        Expr::Const(Value::Int32(row.atttypmod)),
    ]
}

fn pg_type_row(row: PgTypeRow) -> Vec<Expr> {
    vec![
        Expr::Const(Value::Int32(row.oid as i32)),
        Expr::Const(Value::Text(row.typname.into())),
        Expr::Const(Value::Int32(row.typnamespace as i32)),
        Expr::Const(Value::Int32(row.typrelid as i32)),
    ]
}
