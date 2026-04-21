use crate::backend::catalog::catalog::CatalogEntry;
use crate::backend::storage::smgr::RelFileLocator;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, BootstrapCatalogKind, CatalogScope, GLOBAL_TABLESPACE_OID,
    bootstrap_catalog_kinds as shared_bootstrap_catalog_kinds, bootstrap_namespace_oid,
    bootstrap_relation_desc,
};

pub fn bootstrap_catalog_kinds() -> [BootstrapCatalogKind; 36] {
    shared_bootstrap_catalog_kinds()
}

pub fn bootstrap_catalog_rel(kind: BootstrapCatalogKind, db_oid: u32) -> RelFileLocator {
    match kind.scope() {
        CatalogScope::Shared => RelFileLocator {
            spc_oid: GLOBAL_TABLESPACE_OID,
            db_oid: 0,
            rel_number: kind.relation_oid(),
        },
        CatalogScope::Database(_) => RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: kind.relation_oid(),
        },
    }
}

pub fn bootstrap_catalog_entry(kind: BootstrapCatalogKind) -> CatalogEntry {
    CatalogEntry {
        rel: bootstrap_catalog_rel(kind, 1),
        relation_oid: kind.relation_oid(),
        namespace_oid: bootstrap_namespace_oid(),
        owner_oid: BOOTSTRAP_SUPERUSER_OID,
        row_type_oid: kind.row_type_oid(),
        array_type_oid: 0,
        reltoastrelid: 0,
        relpersistence: 'p',
        relkind: 'r',
        am_oid: crate::include::catalog::relam_for_relkind('r'),
        relhassubclass: false,
        relhastriggers: false,
        relispartition: false,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: 0.0,
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
            desc.columns
                .iter()
                .rev()
                .nth(8)
                .map(|col| col.name.as_str()),
            Some("relkind")
        );
        assert_eq!(
            desc.columns.iter().rev().nth(8).map(|col| col.sql_type),
            Some(SqlType::new(SqlTypeKind::InternalChar))
        );
    }
}
