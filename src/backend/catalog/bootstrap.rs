use crate::backend::catalog::catalog::CatalogEntry;
use crate::backend::storage::smgr::RelFileLocator;
use crate::include::catalog::{
    BootstrapCatalogKind, bootstrap_catalog_kinds as shared_bootstrap_catalog_kinds,
    bootstrap_namespace_oid, bootstrap_relation_desc,
};

pub fn bootstrap_catalog_kinds() -> [BootstrapCatalogKind; 17] {
    shared_bootstrap_catalog_kinds()
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
        row_type_oid: kind.row_type_oid(),
        relkind: 'r',
        desc: bootstrap_relation_desc(kind),
        index_meta: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{SqlType, SqlTypeKind};
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
        assert_eq!(
            desc.columns.last().map(|col| col.name.as_str()),
            Some("relkind")
        );
        assert_eq!(
            desc.columns.last().map(|col| col.sql_type),
            Some(SqlType::new(SqlTypeKind::InternalChar))
        );
    }
}
