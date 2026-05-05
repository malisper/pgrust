pub mod bootstrap;
pub mod catalog;
pub mod catcache;
pub mod dependency_drop;
pub mod indexing;
pub mod invalidation;
pub mod materialize;
pub mod pg_aggregate;
pub mod pg_am;
pub mod pg_amop;
pub mod pg_amproc;
pub mod pg_attrdef;
pub mod pg_attribute;
pub mod pg_auth_members;
pub mod pg_authid;
pub mod pg_cast;
pub mod pg_class;
pub mod pg_collation;
pub mod pg_constraint;
pub mod pg_database;
pub mod pg_depend;
pub mod pg_event_trigger;
pub mod pg_foreign_data_wrapper;
pub mod pg_foreign_server;
pub mod pg_foreign_table;
pub mod pg_index;
pub mod pg_inherits;
pub mod pg_language;
pub mod pg_opclass;
pub mod pg_operator;
pub mod pg_opfamily;
pub mod pg_partitioned_table;
pub mod pg_policy;
pub mod pg_proc;
pub mod pg_publication;
pub mod pg_statistic_ext;
pub mod pg_tablespace;
pub mod pg_trigger;
pub mod pg_ts_config;
pub mod pg_ts_config_map;
pub mod pg_ts_dict;
pub mod pg_ts_parser;
pub mod pg_ts_template;
pub mod pg_type;
pub mod pg_user_mapping;
pub mod privileges;
pub mod relcache;
pub mod role_memberships;
pub mod role_settings;
pub mod roles;
pub mod rowcodec;
pub mod rows;
pub mod state;
pub mod store;
pub mod syscache;
pub mod toasting;

pub use catalog::{
    Catalog, CatalogEntry, CatalogError, CatalogIndexBuildOptions, CatalogIndexMeta,
};
pub use catcache::CatCache;
pub use dependency_drop::{CatalogDependencyGraph, DropBehavior, ObjectAddress};
pub use invalidation::{CatalogInvalidation, catalog_invalidation_from_effect};
pub use privileges::*;
pub use relcache::RelCache;
pub use role_memberships::*;
pub use role_settings::*;
pub use roles::*;
pub use store::{
    CatalogControl, CatalogMutationEffect, CatalogReadRuntime, CatalogStoreCore, CatalogStoreMode,
    CatalogStoreSnapshot, CatalogWriteRuntime, CreateTableResult, RuleDependencies,
    RuleOwnerDependency,
};

pub const DEFAULT_FIRST_REL_NUMBER: u32 = 16000;
pub const DEFAULT_FIRST_USER_OID: u32 = 16_384;
pub const FROZEN_TRANSACTION_ID: u32 = 2;

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_catalog_data::{INT4_TYPE_OID, PG_CLASS_RELATION_OID, PgNamespaceRow};
    use pgrust_nodes::Value;

    #[test]
    fn catcache_from_catalog_keeps_builtin_type_rows() {
        let cache = CatCache::from_catalog(&Catalog::default());

        assert_eq!(
            cache
                .type_by_oid(INT4_TYPE_OID)
                .map(|row| row.typname.as_str()),
            Some("int4")
        );
    }

    #[test]
    fn relcache_from_catalog_keeps_bootstrap_relations() {
        let cache = RelCache::from_catalog(&Catalog::default());

        assert!(cache.get_by_oid(PG_CLASS_RELATION_OID).is_some());
    }

    #[test]
    fn default_sequence_oid_parser_handles_oid_casts() {
        assert_eq!(
            relcache::default_sequence_oid_from_default_expr("nextval(123::oid)::int8"),
            Some(123)
        );
        assert_eq!(
            relcache::default_sequence_oid_from_default_expr("nextval(456)::int8"),
            Some(456)
        );
    }

    #[test]
    fn rowcodec_roundtrips_namespace_rows() {
        let row = PgNamespaceRow {
            oid: 11,
            nspname: "app".into(),
            nspowner: 10,
            nspacl: Some(vec!["app=UC/app".into()]),
        };

        let values = rowcodec::namespace_row_values(row.clone());
        let decoded = rowcodec::namespace_row_from_values(values).unwrap();

        assert_eq!(decoded, row);
    }

    #[test]
    fn syscache_decodes_namespace_tuples_and_bootstrap_types() {
        assert_eq!(syscache::SysCacheId::NAMESPACEOID.expected_keys(), 1);

        let row = PgNamespaceRow {
            oid: 11,
            nspname: "app".into(),
            nspowner: 10,
            nspacl: None,
        };
        let tuple = syscache::sys_cache_tuple_from_values(
            syscache::SysCacheId::NAMESPACEOID,
            rowcodec::namespace_row_values(row.clone()),
        )
        .unwrap();
        assert_eq!(tuple, syscache::SysCacheTuple::Namespace(row));

        let builtin = syscache::bootstrap_sys_cache_tuple(
            syscache::SysCacheId::TYPEOID,
            &[Value::Int64(i64::from(INT4_TYPE_OID))],
        );
        assert!(
            matches!(builtin, Some(syscache::SysCacheTuple::Type(row)) if row.oid == INT4_TYPE_OID)
        );
    }
}
