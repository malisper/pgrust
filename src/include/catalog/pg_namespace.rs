use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{PG_CATALOG_NAMESPACE_OID, PUBLIC_NAMESPACE_OID};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgNamespaceRow {
    pub oid: u32,
    pub nspname: String,
}

pub fn pg_namespace_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("nspname", SqlType::new(SqlTypeKind::Text), false),
        ],
    }
}

pub fn bootstrap_pg_namespace_rows() -> [PgNamespaceRow; 2] {
    [
        PgNamespaceRow {
            oid: PG_CATALOG_NAMESPACE_OID,
            nspname: "pg_catalog".into(),
        },
        PgNamespaceRow {
            oid: PUBLIC_NAMESPACE_OID,
            nspname: "public".into(),
        },
    ]
}
