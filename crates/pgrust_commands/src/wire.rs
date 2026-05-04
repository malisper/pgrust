use std::collections::HashMap;

use pgrust_analyze::CatalogLookup;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::QueryColumn;

#[derive(Default)]
pub struct WireCatalogMaps {
    role_names: Option<HashMap<u32, String>>,
    relation_names: Option<HashMap<u32, String>>,
    proc_names: Option<HashMap<u32, String>>,
    proc_signatures: Option<HashMap<u32, String>>,
    namespace_names: Option<HashMap<u32, String>>,
    enum_labels: Option<HashMap<(u32, u32), String>>,
}

#[derive(Default)]
struct WireCatalogMapNeeds {
    role_names: bool,
    relation_names: bool,
    proc_names: bool,
    proc_signatures: bool,
    namespace_names: bool,
    enum_labels: bool,
}

impl WireCatalogMaps {
    pub fn for_columns(catalog: &dyn CatalogLookup, columns: &[QueryColumn]) -> Self {
        Self::for_columns_with_proc_signature_map(catalog, columns, proc_signature_map)
    }

    pub fn for_columns_with_proc_signature_map(
        catalog: &dyn CatalogLookup,
        columns: &[QueryColumn],
        proc_signature_map_fn: impl Fn(&dyn CatalogLookup) -> HashMap<u32, String>,
    ) -> Self {
        let needs = WireCatalogMapNeeds::for_columns(columns);
        Self {
            role_names: needs.role_names.then(|| role_name_map(catalog)),
            relation_names: needs.relation_names.then(|| relation_name_map(catalog)),
            proc_names: needs.proc_names.then(|| proc_name_map(catalog)),
            proc_signatures: needs
                .proc_signatures
                .then(|| proc_signature_map_fn(catalog)),
            namespace_names: needs.namespace_names.then(|| namespace_name_map(catalog)),
            enum_labels: needs.enum_labels.then(|| enum_label_map(catalog)),
        }
    }

    pub fn role_names(&self) -> Option<&HashMap<u32, String>> {
        self.role_names.as_ref()
    }

    pub fn relation_names(&self) -> Option<&HashMap<u32, String>> {
        self.relation_names.as_ref()
    }

    pub fn proc_names(&self) -> Option<&HashMap<u32, String>> {
        self.proc_names.as_ref()
    }

    pub fn proc_signatures(&self) -> Option<&HashMap<u32, String>> {
        self.proc_signatures.as_ref()
    }

    pub fn namespace_names(&self) -> Option<&HashMap<u32, String>> {
        self.namespace_names.as_ref()
    }

    pub fn enum_labels(&self) -> Option<&HashMap<(u32, u32), String>> {
        self.enum_labels.as_ref()
    }
}

impl WireCatalogMapNeeds {
    fn for_columns(columns: &[QueryColumn]) -> Self {
        let mut needs = Self::default();
        for column in columns {
            needs.add_type(column.sql_type);
        }
        needs
    }

    fn add_type(&mut self, sql_type: SqlType) {
        if sql_type.is_array {
            if matches!(sql_type.element_type().kind, SqlTypeKind::Enum) {
                self.enum_labels = true;
            }
            return;
        }

        match sql_type.kind {
            SqlTypeKind::RegRole => self.role_names = true,
            SqlTypeKind::RegClass => self.relation_names = true,
            SqlTypeKind::RegProc => self.proc_names = true,
            SqlTypeKind::RegProcedure => {
                self.proc_names = true;
                self.proc_signatures = true;
            }
            SqlTypeKind::RegNamespace => self.namespace_names = true,
            SqlTypeKind::Enum => self.enum_labels = true,
            _ => {}
        }
    }
}

pub fn role_name_map(catalog: &dyn CatalogLookup) -> HashMap<u32, String> {
    catalog
        .authid_rows()
        .into_iter()
        .map(|row| (row.oid, row.rolname))
        .collect()
}

pub fn proc_name_map(catalog: &dyn CatalogLookup) -> HashMap<u32, String> {
    catalog
        .proc_rows()
        .into_iter()
        .map(|row| (row.oid, row.proname))
        .collect()
}

pub fn proc_signature_map(catalog: &dyn CatalogLookup) -> HashMap<u32, String> {
    catalog
        .proc_rows()
        .into_iter()
        .map(|row| {
            (
                row.oid,
                format!(
                    "{}({})",
                    row.proname,
                    row.proargtypes
                        .split_whitespace()
                        .filter_map(|oid| oid.parse::<u32>().ok())
                        .map(|oid| {
                            catalog
                                .type_by_oid(oid)
                                .map(|row| row.typname)
                                .unwrap_or_else(|| oid.to_string())
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            )
        })
        .collect()
}

pub fn relation_name_map(catalog: &dyn CatalogLookup) -> HashMap<u32, String> {
    catalog
        .class_rows()
        .into_iter()
        .map(|row| (row.oid, row.relname))
        .collect()
}

pub fn namespace_name_map(catalog: &dyn CatalogLookup) -> HashMap<u32, String> {
    catalog
        .namespace_rows()
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect()
}

pub fn enum_label_map(catalog: &dyn CatalogLookup) -> HashMap<(u32, u32), String> {
    catalog
        .enum_rows()
        .into_iter()
        .map(|row| ((row.enumtypid, row.oid), row.enumlabel))
        .collect()
}

pub fn annotate_query_columns_with_wire_type_oids(
    columns: &mut [QueryColumn],
    catalog: &dyn CatalogLookup,
) {
    for column in columns {
        if column.wire_type_oid.is_some() {
            continue;
        }
        if column.sql_type.is_array
            || matches!(
                column.sql_type.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            )
        {
            column.wire_type_oid = catalog.type_oid_for_sql_type(column.sql_type);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_catalog_map_needs_follow_column_types() {
        let columns = vec![
            QueryColumn::text("plain"),
            QueryColumn {
                name: "role".into(),
                sql_type: SqlType::new(SqlTypeKind::RegRole),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "proc".into(),
                sql_type: SqlType::new(SqlTypeKind::RegProcedure),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "enum_array".into(),
                sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Enum)),
                wire_type_oid: None,
            },
        ];
        let needs = WireCatalogMapNeeds::for_columns(&columns);
        assert!(needs.role_names);
        assert!(needs.proc_names);
        assert!(needs.proc_signatures);
        assert!(needs.enum_labels);
        assert!(!needs.relation_names);
    }

    #[test]
    fn annotates_array_and_record_wire_type_oids() {
        struct TypeOidCatalog;
        impl CatalogLookup for TypeOidCatalog {
            fn lookup_any_relation(&self, _name: &str) -> Option<pgrust_analyze::BoundRelation> {
                None
            }

            fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
                match sql_type.kind {
                    SqlTypeKind::Record => Some(2249),
                    SqlTypeKind::Int4 if sql_type.is_array => Some(1007),
                    _ => None,
                }
            }
        }

        let mut columns = vec![
            QueryColumn {
                name: "array".into(),
                sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Int4)),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "record".into(),
                sql_type: SqlType::new(SqlTypeKind::Record),
                wire_type_oid: None,
            },
            QueryColumn::text("text"),
        ];
        annotate_query_columns_with_wire_type_oids(&mut columns, &TypeOidCatalog);
        assert_eq!(columns[0].wire_type_oid, Some(1007));
        assert_eq!(columns[1].wire_type_oid, Some(2249));
        assert_eq!(columns[2].wire_type_oid, None);
    }
}
