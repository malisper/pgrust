use crate::backend::catalog::catalog::{column_desc, CatalogEntry};
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::storage::smgr::RelFileLocator;
use crate::include::catalog::{
    PG_ATTRIBUTE_RELATION_OID, PG_CATALOG_NAMESPACE_OID, PG_CLASS_RELATION_OID,
    PG_NAMESPACE_RELATION_OID, PG_TYPE_RELATION_OID,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapCatalogKind {
    PgNamespace,
    PgClass,
    PgAttribute,
    PgType,
}

impl BootstrapCatalogKind {
    pub fn relation_oid(self) -> u32 {
        match self {
            Self::PgNamespace => PG_NAMESPACE_RELATION_OID,
            Self::PgClass => PG_CLASS_RELATION_OID,
            Self::PgAttribute => PG_ATTRIBUTE_RELATION_OID,
            Self::PgType => PG_TYPE_RELATION_OID,
        }
    }

    pub fn relation_name(self) -> &'static str {
        match self {
            Self::PgNamespace => "pg_namespace",
            Self::PgClass => "pg_class",
            Self::PgAttribute => "pg_attribute",
            Self::PgType => "pg_type",
        }
    }
}

pub fn bootstrap_catalog_kinds() -> [BootstrapCatalogKind; 4] {
    [
        BootstrapCatalogKind::PgNamespace,
        BootstrapCatalogKind::PgType,
        BootstrapCatalogKind::PgAttribute,
        BootstrapCatalogKind::PgClass,
    ]
}

pub fn bootstrap_catalog_entry(kind: BootstrapCatalogKind) -> CatalogEntry {
    CatalogEntry {
        rel: RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: kind.relation_oid(),
        },
        relation_oid: kind.relation_oid(),
        namespace_oid: bootstrap_namespace_oid(),
        row_type_oid: match kind {
            BootstrapCatalogKind::PgType => 71,
            BootstrapCatalogKind::PgAttribute => 75,
            BootstrapCatalogKind::PgClass => 83,
            BootstrapCatalogKind::PgNamespace => 0,
        },
        relkind: 'r',
        desc: bootstrap_relation_desc(kind),
    }
}

pub fn bootstrap_relation_desc(kind: BootstrapCatalogKind) -> RelationDesc {
    match kind {
        BootstrapCatalogKind::PgNamespace => RelationDesc {
            columns: vec![
                column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
                column_desc("nspname", SqlType::new(SqlTypeKind::Text), false),
            ],
        },
        BootstrapCatalogKind::PgClass => RelationDesc {
            columns: vec![
                column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
                column_desc("relname", SqlType::new(SqlTypeKind::Text), false),
                column_desc("relnamespace", SqlType::new(SqlTypeKind::Oid), false),
                column_desc("reltype", SqlType::new(SqlTypeKind::Oid), false),
                column_desc("relfilenode", SqlType::new(SqlTypeKind::Oid), false),
                column_desc("relkind", SqlType::new(SqlTypeKind::InternalChar), false),
            ],
        },
        BootstrapCatalogKind::PgAttribute => RelationDesc {
            columns: vec![
                column_desc("attrelid", SqlType::new(SqlTypeKind::Oid), false),
                column_desc("attname", SqlType::new(SqlTypeKind::Text), false),
                column_desc("atttypid", SqlType::new(SqlTypeKind::Oid), false),
                column_desc("attnum", SqlType::new(SqlTypeKind::Int2), false),
                column_desc("attnotnull", SqlType::new(SqlTypeKind::Bool), false),
                column_desc("atttypmod", SqlType::new(SqlTypeKind::Int4), false),
            ],
        },
        BootstrapCatalogKind::PgType => RelationDesc {
            columns: vec![
                column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
                column_desc("typname", SqlType::new(SqlTypeKind::Text), false),
                column_desc("typnamespace", SqlType::new(SqlTypeKind::Oid), false),
                column_desc("typrelid", SqlType::new(SqlTypeKind::Oid), false),
            ],
        },
    }
}

pub fn bootstrap_namespace_oid() -> u32 {
    PG_CATALOG_NAMESPACE_OID
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::CORE_BOOTSTRAP_RELATIONS;

    #[test]
    fn bootstrap_catalog_kinds_match_shared_bootstrap_relations() {
        let pairs: Vec<_> = bootstrap_catalog_kinds()
            .into_iter()
            .map(|kind| (kind.relation_oid(), kind.relation_name()))
            .collect();
        let shared: Vec<_> = CORE_BOOTSTRAP_RELATIONS
            .iter()
            .map(|rel| (rel.oid, rel.name))
            .collect();
        assert_eq!(pairs, shared);
    }

    #[test]
    fn pg_class_bootstrap_desc_contains_relkind() {
        let desc = bootstrap_relation_desc(BootstrapCatalogKind::PgClass);
        assert_eq!(desc.columns.last().map(|col| col.name.as_str()), Some("relkind"));
        assert_eq!(
            desc.columns.last().map(|col| col.sql_type),
            Some(SqlType::new(SqlTypeKind::InternalChar))
        );
    }
}
