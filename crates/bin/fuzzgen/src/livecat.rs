//! Live `CatalogSource`: introspect tables/columns/types over a client
//! connection so the generator targets a real schema. Columns whose type
//! the generator doesn't model are skipped; tables left with no usable
//! columns are dropped from the snapshot. The fixture path stays the
//! default (crate::catalog::FixtureCatalog).

use crate::catalog::{Catalog, CatalogSource, Column, SqlType, Table};
use crate::client::{Client, ConnLost};

const INTROSPECT_SQL: &str = "\
SELECT c.relname, a.attname, a.atttypid::int4, a.attnotnull \
FROM pg_catalog.pg_class c \
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid \
WHERE c.relkind = 'r' AND n.nspname = 'public' \
  AND a.attnum > 0 AND NOT a.attisdropped \
ORDER BY c.relname, a.attnum;";

pub struct LiveCatalog {
    pub host: String,
    pub port: u16,
    pub db: String,
    pub user: String,
}

/// Fold introspection rows (relname, attname, atttypid, attnotnull) into a
/// catalog snapshot. Split out from the connection so it is unit-testable.
pub fn catalog_from_rows(rows: &[Vec<Option<String>>]) -> Result<Catalog, String> {
    let mut tables: Vec<Table> = Vec::new();
    for row in rows {
        let [Some(rel), Some(att), Some(oid), Some(notnull)] = row.as_slice() else {
            return Err(format!("introspection row with NULLs: {row:?}"));
        };
        let oid: u32 = oid.parse().map_err(|e| format!("bad atttypid {oid:?}: {e}"))?;
        let Some(ty) = SqlType::from_oid(oid) else {
            continue; // type the generator doesn't model
        };
        let nullable = notnull != "t";
        if tables.last().map(|t| t.name.as_str()) != Some(rel.as_str()) {
            // Live row counts and uniqueness are unknown: never claim the
            // single-row or unique-key hints. No pk introspection either:
            // live-catalog sessions get no DML targets and no state probes
            // (the fixture path has both).
            tables.push(Table {
                name: rel.clone(),
                columns: Vec::new(),
                at_most_one_row: false,
                pk: None,
                unique_key: None,
                pk_unique: false,
            });
        }
        let table = tables.last_mut().expect("just pushed");
        table.columns.push(Column { name: att.clone(), ty, nullable, ddl_type: None });
    }
    tables.retain(|t| !t.columns.is_empty());
    if tables.is_empty() {
        return Err("live catalog: no tables with generator-typed columns in public".to_string());
    }
    Ok(Catalog { tables })
}

impl CatalogSource for LiveCatalog {
    fn load_catalog(&self) -> Result<Catalog, String> {
        let mut client = Client::connect(&self.host, self.port, &self.db, &self.user)
            .map_err(|ConnLost(e)| format!("live catalog: {e}"))?;
        let results = client
            .simple_query(INTROSPECT_SQL)
            .map_err(|ConnLost(e)| format!("live catalog: {e}"))?;
        let result = results.last().ok_or("live catalog: empty exchange")?;
        if let Some((state, message)) = &result.error {
            return Err(format!("live catalog: introspection failed: {state} {message}"));
        }
        catalog_from_rows(&result.rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(rel: &str, att: &str, oid: u32, notnull: bool) -> Vec<Option<String>> {
        vec![
            Some(rel.to_string()),
            Some(att.to_string()),
            Some(oid.to_string()),
            Some(if notnull { "t" } else { "f" }.to_string()),
        ]
    }

    #[test]
    fn folds_rows_into_tables() {
        let rows = vec![
            row("t1", "a", 23, true),
            row("t1", "b", 701, false),
            row("t2", "c", 25, false),
        ];
        let cat = catalog_from_rows(&rows).unwrap();
        assert_eq!(cat.tables.len(), 2);
        assert_eq!(cat.tables[0].name, "t1");
        assert_eq!(cat.tables[0].columns.len(), 2);
        assert_eq!(cat.tables[0].columns[0].ty, SqlType::Int4);
        assert!(!cat.tables[0].columns[0].nullable);
        assert!(cat.tables[0].columns[1].nullable);
    }

    #[test]
    fn unknown_types_skipped_and_empty_tables_dropped() {
        let rows = vec![
            row("geom_only", "g", 600, false), // point: not modeled
            row("t1", "a", 23, false),
            row("t1", "g", 600, false),
        ];
        let cat = catalog_from_rows(&rows).unwrap();
        assert_eq!(cat.tables.len(), 1);
        assert_eq!(cat.tables[0].name, "t1");
        assert_eq!(cat.tables[0].columns.len(), 1);
    }

    #[test]
    fn all_unknown_is_an_error() {
        let rows = vec![row("g", "g", 600, false)];
        assert!(catalog_from_rows(&rows).is_err());
        assert!(catalog_from_rows(&[]).is_err());
    }
}
