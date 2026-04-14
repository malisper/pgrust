use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, PG_CATALOG_NAMESPACE_OID, PG_TOAST_NAMESPACE_OID, PUBLIC_NAMESPACE_OID,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgNamespaceRow {
    pub oid: u32,
    pub nspname: String,
    pub nspowner: u32,
}

pub fn pg_namespace_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("nspname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("nspowner", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_namespace_rows() -> [PgNamespaceRow; 3] {
    [
        PgNamespaceRow {
            oid: PG_CATALOG_NAMESPACE_OID,
            nspname: "pg_catalog".into(),
            nspowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgNamespaceRow {
            oid: PG_TOAST_NAMESPACE_OID,
            nspname: "pg_toast".into(),
            nspowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgNamespaceRow {
            oid: PUBLIC_NAMESPACE_OID,
            nspname: "public".into(),
            nspowner: BOOTSTRAP_SUPERUSER_OID,
        },
    ]
}
