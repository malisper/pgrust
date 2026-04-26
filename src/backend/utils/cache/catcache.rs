use std::collections::BTreeMap;
use std::path::Path;

use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::{Catalog, catalog_attribute_collation_oid};
use crate::backend::catalog::loader::load_physical_catalog_rows;
use crate::backend::catalog::pg_aggregate::sort_pg_aggregate_rows;
use crate::backend::catalog::pg_am::sort_pg_am_rows;
use crate::backend::catalog::pg_amop::sort_pg_amop_rows;
use crate::backend::catalog::pg_amproc::sort_pg_amproc_rows;
use crate::backend::catalog::pg_attrdef::sort_pg_attrdef_rows;
use crate::backend::catalog::pg_attribute::sort_pg_attribute_rows;
use crate::backend::catalog::pg_auth_members::sort_pg_auth_members_rows;
use crate::backend::catalog::pg_authid::sort_pg_authid_rows;
use crate::backend::catalog::pg_cast::sort_pg_cast_rows;
use crate::backend::catalog::pg_collation::sort_pg_collation_rows;
use crate::backend::catalog::pg_constraint::sort_pg_constraint_rows;
use crate::backend::catalog::pg_database::sort_pg_database_rows;
use crate::backend::catalog::pg_depend::sort_pg_depend_rows;
use crate::backend::catalog::pg_foreign_data_wrapper::sort_pg_foreign_data_wrapper_rows;
use crate::backend::catalog::pg_index::sort_pg_index_rows;
use crate::backend::catalog::pg_inherits::sort_pg_inherits_rows;
use crate::backend::catalog::pg_language::sort_pg_language_rows;
use crate::backend::catalog::pg_opclass::sort_pg_opclass_rows;
use crate::backend::catalog::pg_operator::sort_pg_operator_rows;
use crate::backend::catalog::pg_opfamily::sort_pg_opfamily_rows;
use crate::backend::catalog::pg_policy::sort_pg_policy_rows;
use crate::backend::catalog::pg_proc::sort_pg_proc_rows;
use crate::backend::catalog::pg_publication::{
    sort_pg_publication_namespace_rows, sort_pg_publication_rel_rows, sort_pg_publication_rows,
};
use crate::backend::catalog::pg_statistic_ext::{
    sort_pg_statistic_ext_data_rows, sort_pg_statistic_ext_rows,
};
use crate::backend::catalog::pg_tablespace::sort_pg_tablespace_rows;
use crate::backend::catalog::pg_trigger::sort_pg_trigger_rows;
use crate::backend::catalog::pg_ts_config::sort_pg_ts_config_rows;
use crate::backend::catalog::pg_ts_config_map::sort_pg_ts_config_map_rows;
use crate::backend::catalog::pg_ts_dict::sort_pg_ts_dict_rows;
use crate::backend::catalog::pg_ts_parser::sort_pg_ts_parser_rows;
use crate::backend::catalog::pg_ts_template::sort_pg_ts_template_rows;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    ANYARRAYOID, BIT_ARRAY_TYPE_OID, BIT_TYPE_OID, BOOL_ARRAY_TYPE_OID, BOOL_TYPE_OID,
    BOX_TYPE_OID, BPCHAR_ARRAY_TYPE_OID, BPCHAR_TYPE_OID, BYTEA_ARRAY_TYPE_OID, BYTEA_TYPE_OID,
    CIRCLE_TYPE_OID, FLOAT4_ARRAY_TYPE_OID, FLOAT4_TYPE_OID, FLOAT8_ARRAY_TYPE_OID,
    FLOAT8_TYPE_OID, INT2_ARRAY_TYPE_OID, INT2_TYPE_OID, INT4_ARRAY_TYPE_OID, INT4_TYPE_OID,
    INT8_ARRAY_TYPE_OID, INT8_TYPE_OID, INTERNAL_CHAR_ARRAY_TYPE_OID, INTERNAL_CHAR_TYPE_OID,
    INTERVAL_ARRAY_TYPE_OID, INTERVAL_TYPE_OID, JSON_ARRAY_TYPE_OID, JSON_TYPE_OID,
    JSONB_ARRAY_TYPE_OID, JSONB_TYPE_OID, JSONPATH_ARRAY_TYPE_OID, JSONPATH_TYPE_OID,
    LINE_TYPE_OID, LSEG_TYPE_OID, MONEY_ARRAY_TYPE_OID, MONEY_TYPE_OID, NUMERIC_ARRAY_TYPE_OID,
    NUMERIC_TYPE_OID, OID_ARRAY_TYPE_OID, OID_TYPE_OID, PATH_TYPE_OID, POINT_TYPE_OID,
    POLYGON_TYPE_OID, PgAggregateRow, PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow,
    PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgCastRow, PgClassRow, PgCollationRow,
    PgConstraintRow, PgDatabaseRow, PgDependRow, PgForeignDataWrapperRow, PgIndexRow,
    PgInheritsRow, PgLanguageRow, PgNamespaceRow, PgOpclassRow, PgOperatorRow, PgOpfamilyRow,
    PgPartitionedTableRow, PgPolicyRow, PgProcRow, PgPublicationNamespaceRow, PgPublicationRelRow,
    PgPublicationRow, PgRewriteRow, PgStatisticExtDataRow, PgStatisticExtRow, PgStatisticRow,
    PgTablespaceRow, PgTriggerRow, PgTsConfigMapRow, PgTsConfigRow, PgTsDictRow, PgTsParserRow,
    PgTsTemplateRow, PgTypeRow, REGCONFIG_ARRAY_TYPE_OID, REGCONFIG_TYPE_OID,
    REGDICTIONARY_ARRAY_TYPE_OID, REGDICTIONARY_TYPE_OID, TEXT_ARRAY_TYPE_OID, TEXT_TYPE_OID,
    TID_ARRAY_TYPE_OID, TID_TYPE_OID, TIMESTAMP_ARRAY_TYPE_OID, TIMESTAMP_TYPE_OID,
    TSQUERY_ARRAY_TYPE_OID, TSQUERY_TYPE_OID, TSVECTOR_ARRAY_TYPE_OID, TSVECTOR_TYPE_OID,
    UUID_ARRAY_TYPE_OID, UUID_TYPE_OID, VARBIT_ARRAY_TYPE_OID, VARBIT_TYPE_OID,
    VARCHAR_ARRAY_TYPE_OID, VARCHAR_TYPE_OID, XID_ARRAY_TYPE_OID, XID_TYPE_OID, XML_ARRAY_TYPE_OID,
    XML_TYPE_OID, bootstrap_composite_type_rows, bootstrap_pg_aggregate_rows, bootstrap_pg_am_rows,
    bootstrap_pg_amop_rows, bootstrap_pg_amproc_rows, bootstrap_pg_cast_rows,
    bootstrap_pg_collation_rows, bootstrap_pg_constraint_rows,
    bootstrap_pg_foreign_data_wrapper_rows, bootstrap_pg_language_rows,
    bootstrap_pg_namespace_rows, bootstrap_pg_opclass_rows, bootstrap_pg_operator_rows,
    bootstrap_pg_opfamily_rows, bootstrap_pg_proc_rows, bootstrap_pg_ts_config_map_rows,
    bootstrap_pg_ts_config_rows, bootstrap_pg_ts_dict_rows, bootstrap_pg_ts_parser_rows,
    bootstrap_pg_ts_template_rows, builtin_type_rows, composite_array_type_row, composite_type_row,
    range_type_ref_for_sql_type, sort_pg_rewrite_rows,
};

#[derive(Debug, Clone, Default)]
pub struct CatCache {
    namespaces_by_name: BTreeMap<String, PgNamespaceRow>,
    namespaces_by_oid: BTreeMap<u32, PgNamespaceRow>,
    classes_by_name: BTreeMap<String, PgClassRow>,
    classes_by_oid: BTreeMap<u32, PgClassRow>,
    attributes_by_relid: BTreeMap<u32, Vec<PgAttributeRow>>,
    attrdefs_by_key: BTreeMap<(u32, i16), PgAttrdefRow>,
    depend_rows: Vec<PgDependRow>,
    foreign_data_wrapper_rows: Vec<PgForeignDataWrapperRow>,
    inherit_rows: Vec<PgInheritsRow>,
    partitioned_tables_by_relid: BTreeMap<u32, PgPartitionedTableRow>,
    index_rows: Vec<PgIndexRow>,
    rewrite_rows: Vec<PgRewriteRow>,
    trigger_rows: Vec<PgTriggerRow>,
    policy_rows: Vec<PgPolicyRow>,
    publication_rows: Vec<PgPublicationRow>,
    publication_rel_rows: Vec<PgPublicationRelRow>,
    publication_namespace_rows: Vec<PgPublicationNamespaceRow>,
    statistic_ext_rows: Vec<PgStatisticExtRow>,
    statistic_ext_data_rows: Vec<PgStatisticExtDataRow>,
    am_rows: Vec<PgAmRow>,
    amop_rows: Vec<PgAmopRow>,
    amproc_rows: Vec<PgAmprocRow>,
    authid_rows: Vec<PgAuthIdRow>,
    auth_members_rows: Vec<PgAuthMembersRow>,
    language_rows: Vec<PgLanguageRow>,
    ts_parser_rows: Vec<PgTsParserRow>,
    ts_template_rows: Vec<PgTsTemplateRow>,
    ts_dict_rows: Vec<PgTsDictRow>,
    ts_config_rows: Vec<PgTsConfigRow>,
    ts_config_map_rows: Vec<PgTsConfigMapRow>,
    constraint_rows: Vec<PgConstraintRow>,
    operator_rows: Vec<PgOperatorRow>,
    opclass_rows: Vec<PgOpclassRow>,
    opfamily_rows: Vec<PgOpfamilyRow>,
    proc_rows: Vec<PgProcRow>,
    aggregates_by_fnoid: BTreeMap<u32, PgAggregateRow>,
    cast_rows: Vec<PgCastRow>,
    collation_rows: Vec<PgCollationRow>,
    database_rows: Vec<PgDatabaseRow>,
    tablespace_rows: Vec<PgTablespaceRow>,
    statistic_rows: Vec<PgStatisticRow>,
    types_by_name: BTreeMap<String, PgTypeRow>,
    types_by_oid: BTreeMap<u32, PgTypeRow>,
}

fn namespace_row_prefer_replacement(
    existing: &PgNamespaceRow,
    replacement: &PgNamespaceRow,
) -> bool {
    replacement.nspacl.is_some() && existing.nspacl.is_none()
}

impl CatCache {
    fn insert_namespace_row(&mut self, row: PgNamespaceRow) {
        let name_key = row.nspname.to_ascii_lowercase();
        let should_replace = self
            .namespaces_by_oid
            .get(&row.oid)
            .is_none_or(|existing| namespace_row_prefer_replacement(existing, &row));
        if should_replace {
            self.namespaces_by_name.insert(name_key, row.clone());
            self.namespaces_by_oid.insert(row.oid, row);
        }
    }

    fn normalize_composite_array_types(&mut self) {
        let composite_rows = self
            .types_by_oid
            .values()
            .filter(|row| row.typrelid != 0)
            .cloned()
            .collect::<Vec<_>>();
        for row in composite_rows {
            if row.typarray == 0 {
                continue;
            }
            let Some(array_row) = self.types_by_oid.get_mut(&row.typarray) else {
                continue;
            };
            array_row.sql_type = SqlType::array_of(SqlType::named_composite(row.oid, row.typrelid));
            let updated = array_row.clone();
            self.types_by_name
                .insert(updated.typname.to_ascii_lowercase(), updated);
        }
    }

    pub fn from_catalog(catalog: &Catalog) -> Self {
        let mut cache = Self::default();

        for row in bootstrap_pg_namespace_rows() {
            cache.insert_namespace_row(row);
        }

        for row in builtin_type_rows() {
            cache
                .types_by_name
                .insert(row.typname.to_ascii_lowercase(), row.clone());
            cache.types_by_oid.insert(row.oid, row);
        }

        for row in bootstrap_composite_type_rows() {
            cache
                .types_by_name
                .insert(row.typname.to_ascii_lowercase(), row.clone());
            cache.types_by_oid.insert(row.oid, row);
        }
        cache.am_rows.extend(bootstrap_pg_am_rows());
        sort_pg_am_rows(&mut cache.am_rows);
        cache.amop_rows.extend(bootstrap_pg_amop_rows());
        sort_pg_amop_rows(&mut cache.amop_rows);
        cache.amproc_rows.extend(bootstrap_pg_amproc_rows());
        sort_pg_amproc_rows(&mut cache.amproc_rows);
        cache
            .authid_rows
            .extend(catalog.authid_rows().iter().cloned());
        sort_pg_authid_rows(&mut cache.authid_rows);
        cache
            .auth_members_rows
            .extend(catalog.auth_members_rows().iter().cloned());
        sort_pg_auth_members_rows(&mut cache.auth_members_rows);
        cache.language_rows.extend(bootstrap_pg_language_rows());
        sort_pg_language_rows(&mut cache.language_rows);
        cache.ts_parser_rows.extend(bootstrap_pg_ts_parser_rows());
        sort_pg_ts_parser_rows(&mut cache.ts_parser_rows);
        cache
            .ts_template_rows
            .extend(bootstrap_pg_ts_template_rows());
        sort_pg_ts_template_rows(&mut cache.ts_template_rows);
        cache.ts_dict_rows.extend(bootstrap_pg_ts_dict_rows());
        sort_pg_ts_dict_rows(&mut cache.ts_dict_rows);
        cache.ts_config_rows.extend(bootstrap_pg_ts_config_rows());
        sort_pg_ts_config_rows(&mut cache.ts_config_rows);
        cache
            .ts_config_map_rows
            .extend(bootstrap_pg_ts_config_map_rows());
        sort_pg_ts_config_map_rows(&mut cache.ts_config_map_rows);
        cache.constraint_rows.extend(bootstrap_pg_constraint_rows());
        sort_pg_constraint_rows(&mut cache.constraint_rows);
        cache.operator_rows.extend(bootstrap_pg_operator_rows());
        sort_pg_operator_rows(&mut cache.operator_rows);
        cache.opclass_rows.extend(bootstrap_pg_opclass_rows());
        sort_pg_opclass_rows(&mut cache.opclass_rows);
        cache.opfamily_rows.extend(bootstrap_pg_opfamily_rows());
        sort_pg_opfamily_rows(&mut cache.opfamily_rows);
        cache.proc_rows.extend(bootstrap_pg_proc_rows());
        sort_pg_proc_rows(&mut cache.proc_rows);
        for row in bootstrap_pg_aggregate_rows() {
            cache.aggregates_by_fnoid.insert(row.aggfnoid, row);
        }
        cache.cast_rows.extend(bootstrap_pg_cast_rows());
        sort_pg_cast_rows(&mut cache.cast_rows);
        cache.collation_rows.extend(bootstrap_pg_collation_rows());
        sort_pg_collation_rows(&mut cache.collation_rows);
        cache
            .foreign_data_wrapper_rows
            .extend(bootstrap_pg_foreign_data_wrapper_rows());
        sort_pg_foreign_data_wrapper_rows(&mut cache.foreign_data_wrapper_rows);
        cache
            .database_rows
            .extend(catalog.database_rows().iter().cloned());
        sort_pg_database_rows(&mut cache.database_rows);
        cache
            .tablespace_rows
            .extend(catalog.tablespace_rows().iter().cloned());
        sort_pg_tablespace_rows(&mut cache.tablespace_rows);

        for (name, entry) in catalog.entries() {
            if let Some((namespace, _)) = name.split_once('.')
                && !cache.namespaces_by_oid.contains_key(&entry.namespace_oid)
            {
                let namespace_row = PgNamespaceRow {
                    oid: entry.namespace_oid,
                    nspname: namespace.to_string(),
                    nspowner: entry.owner_oid,
                    nspacl: None,
                };
                cache.namespaces_by_name.insert(
                    namespace_row.nspname.to_ascii_lowercase(),
                    namespace_row.clone(),
                );
                cache
                    .namespaces_by_oid
                    .insert(namespace_row.oid, namespace_row);
            }

            let relname = catalog_object_name(name);
            let class_row = PgClassRow {
                oid: entry.relation_oid,
                relname: relname.to_string(),
                relnamespace: entry.namespace_oid,
                reltype: entry.row_type_oid,
                relowner: entry.owner_oid,
                relam: entry.am_oid,
                relfilenode: entry.rel.rel_number,
                reltablespace: 0,
                relpages: entry.relpages,
                reltuples: entry.reltuples,
                relallvisible: entry.relallvisible,
                relallfrozen: entry.relallfrozen,
                reltoastrelid: entry.reltoastrelid,
                relpersistence: entry.relpersistence,
                relkind: entry.relkind,
                relnatts: entry.desc.columns.len() as i16,
                relhassubclass: entry.relhassubclass,
                relhastriggers: entry.relhastriggers,
                relrowsecurity: entry.relrowsecurity,
                relforcerowsecurity: entry.relforcerowsecurity,
                relispopulated: entry.relispopulated,
                relispartition: entry.relispartition,
                relfrozenxid: entry.relfrozenxid,
                relpartbound: entry.relpartbound.clone(),
                reloptions: entry.reloptions.clone(),
                relacl: entry.relacl.clone(),
            };
            cache.classes_by_name.insert(
                normalize_catalog_name(name).to_ascii_lowercase(),
                class_row.clone(),
            );
            if let Some(namespace) = cache.namespaces_by_oid.get(&entry.namespace_oid) {
                cache.classes_by_name.insert(
                    format!("{}.{}", namespace.nspname.to_ascii_lowercase(), relname),
                    class_row.clone(),
                );
            }

            if let Some(row) = &entry.partitioned_table {
                cache
                    .partitioned_tables_by_relid
                    .insert(row.partrelid, row.clone());
            }
            cache.classes_by_oid.insert(class_row.oid, class_row);

            if entry.row_type_oid != 0 {
                let composite_type = composite_type_row(
                    relname,
                    entry.row_type_oid,
                    entry.namespace_oid,
                    entry.relation_oid,
                    entry.array_type_oid,
                );
                cache
                    .types_by_name
                    .insert(relname.to_ascii_lowercase(), composite_type.clone());
                cache
                    .types_by_oid
                    .insert(composite_type.oid, composite_type);
                if entry.array_type_oid != 0 {
                    let array_type = composite_array_type_row(
                        relname,
                        entry.array_type_oid,
                        entry.namespace_oid,
                        entry.row_type_oid,
                        entry.relation_oid,
                    );
                    cache
                        .types_by_name
                        .insert(array_type.typname.to_ascii_lowercase(), array_type.clone());
                    cache.types_by_oid.insert(array_type.oid, array_type);
                }
            }

            let mut attrs = entry
                .desc
                .columns
                .iter()
                .enumerate()
                .map(|(idx, column)| PgAttributeRow {
                    attrelid: entry.relation_oid,
                    attname: column.name.clone(),
                    atttypid: catalog_entry_sql_type_oid(catalog, column.sql_type),
                    attlen: column.storage.attlen,
                    attnum: idx.saturating_add(1) as i16,
                    attnotnull: !column.storage.nullable,
                    attisdropped: column.dropped,
                    atttypmod: column.sql_type.typmod,
                    attalign: column.storage.attalign,
                    attstorage: column.storage.attstorage,
                    attcompression: column.storage.attcompression,
                    attstattarget: column.attstattarget,
                    attinhcount: column.attinhcount,
                    attislocal: column.attislocal,
                    attidentity: column
                        .identity
                        .map(|kind| kind.catalog_char())
                        .unwrap_or('\0'),
                    attgenerated: column
                        .generated
                        .map(|kind| kind.catalog_char())
                        .unwrap_or('\0'),
                    attcollation: catalog_attribute_collation_oid(
                        entry.relation_oid,
                        column.collation_oid,
                    ),
                    sql_type: column.sql_type,
                })
                .collect::<Vec<_>>();
            sort_pg_attribute_rows(&mut attrs);
            cache.attributes_by_relid.insert(entry.relation_oid, attrs);

            let mut attrdefs = entry
                .desc
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, column)| {
                    Some(PgAttrdefRow {
                        oid: column.attrdef_oid?,
                        adrelid: entry.relation_oid,
                        adnum: idx.saturating_add(1) as i16,
                        adbin: column.default_expr.clone()?,
                    })
                })
                .collect::<Vec<_>>();
            sort_pg_attrdef_rows(&mut attrdefs);
            for row in attrdefs {
                cache.attrdefs_by_key.insert((row.adrelid, row.adnum), row);
            }

            if let Some(index_meta) = &entry.index_meta {
                cache.index_rows.push(PgIndexRow {
                    indexrelid: entry.relation_oid,
                    indrelid: index_meta.indrelid,
                    indnatts: index_meta.indkey.len() as i16,
                    indnkeyatts: index_meta.indclass.len() as i16,
                    indisunique: index_meta.indisunique,
                    indnullsnotdistinct: index_meta.indnullsnotdistinct,
                    indisprimary: index_meta.indisprimary,
                    indisexclusion: false,
                    indimmediate: true,
                    indisclustered: false,
                    indisvalid: index_meta.indisvalid,
                    indcheckxmin: false,
                    indisready: index_meta.indisready,
                    indislive: index_meta.indislive,
                    indisreplident: false,
                    indkey: index_meta.indkey.clone(),
                    indcollation: index_meta.indcollation.clone(),
                    indclass: index_meta.indclass.clone(),
                    indoption: index_meta.indoption.clone(),
                    indexprs: index_meta.indexprs.clone(),
                    indpred: index_meta.indpred.clone(),
                });
            }
        }

        cache
            .constraint_rows
            .extend(catalog.constraint_rows().iter().cloned());
        cache
            .depend_rows
            .extend(catalog.depend_rows().iter().cloned());
        cache
            .inherit_rows
            .extend(catalog.inherit_rows().iter().cloned());
        cache
            .rewrite_rows
            .extend(catalog.rewrite_rows().iter().cloned());
        cache.trigger_rows.extend(catalog.triggers.iter().cloned());
        cache
            .policy_rows
            .extend(catalog.policy_rows().iter().cloned());
        cache
            .publication_rows
            .extend(catalog.publication_rows().iter().cloned());
        cache
            .publication_rel_rows
            .extend(catalog.publication_rel_rows().iter().cloned());
        cache
            .publication_namespace_rows
            .extend(catalog.publication_namespace_rows().iter().cloned());
        cache
            .statistic_ext_rows
            .extend(catalog.statistic_ext_rows().iter().cloned());
        cache
            .statistic_ext_data_rows
            .extend(catalog.statistic_ext_data_rows().iter().cloned());
        sort_pg_constraint_rows(&mut cache.constraint_rows);
        sort_pg_depend_rows(&mut cache.depend_rows);
        sort_pg_inherits_rows(&mut cache.inherit_rows);
        sort_pg_rewrite_rows(&mut cache.rewrite_rows);
        sort_pg_trigger_rows(&mut cache.trigger_rows);
        sort_pg_policy_rows(&mut cache.policy_rows);
        sort_pg_publication_rows(&mut cache.publication_rows);
        sort_pg_publication_rel_rows(&mut cache.publication_rel_rows);
        sort_pg_publication_namespace_rows(&mut cache.publication_namespace_rows);
        sort_pg_statistic_ext_rows(&mut cache.statistic_ext_rows);
        sort_pg_statistic_ext_data_rows(&mut cache.statistic_ext_data_rows);
        sort_pg_index_rows(&mut cache.index_rows);

        cache.normalize_composite_array_types();
        cache
    }

    pub fn from_physical(base_dir: &Path) -> Result<Self, CatalogError> {
        let rows = load_physical_catalog_rows(base_dir)?;
        Ok(Self::from_rows(
            rows.namespaces,
            rows.classes,
            rows.attributes,
            rows.attrdefs,
            rows.depends,
            rows.inherits,
            rows.indexes,
            rows.rewrites,
            rows.triggers,
            rows.policies,
            rows.publications,
            rows.publication_rels,
            rows.publication_namespaces,
            rows.statistics_ext,
            rows.statistics_ext_data,
            rows.ams,
            rows.amops,
            rows.amprocs,
            rows.authids,
            rows.auth_members,
            rows.languages,
            rows.ts_parsers,
            rows.ts_templates,
            rows.ts_dicts,
            rows.ts_configs,
            rows.ts_config_maps,
            rows.constraints,
            rows.operators,
            rows.opclasses,
            rows.opfamilies,
            rows.partitioned_tables,
            rows.procs,
            rows.aggregates,
            rows.casts,
            rows.collations,
            rows.foreign_data_wrappers,
            rows.databases,
            rows.tablespaces,
            rows.statistics,
            rows.types,
        ))
    }

    pub fn from_rows(
        namespace_rows: Vec<PgNamespaceRow>,
        class_rows: Vec<PgClassRow>,
        attribute_rows: Vec<PgAttributeRow>,
        attrdef_rows: Vec<PgAttrdefRow>,
        depend_rows: Vec<PgDependRow>,
        inherit_rows: Vec<PgInheritsRow>,
        index_rows: Vec<PgIndexRow>,
        rewrite_rows: Vec<PgRewriteRow>,
        trigger_rows: Vec<PgTriggerRow>,
        policy_rows: Vec<PgPolicyRow>,
        publication_rows: Vec<PgPublicationRow>,
        publication_rel_rows: Vec<PgPublicationRelRow>,
        publication_namespace_rows: Vec<PgPublicationNamespaceRow>,
        statistic_ext_rows: Vec<PgStatisticExtRow>,
        statistic_ext_data_rows: Vec<PgStatisticExtDataRow>,
        am_rows: Vec<PgAmRow>,
        amop_rows: Vec<PgAmopRow>,
        amproc_rows: Vec<PgAmprocRow>,
        authid_rows: Vec<PgAuthIdRow>,
        auth_members_rows: Vec<PgAuthMembersRow>,
        language_rows: Vec<PgLanguageRow>,
        ts_parser_rows: Vec<PgTsParserRow>,
        ts_template_rows: Vec<PgTsTemplateRow>,
        ts_dict_rows: Vec<PgTsDictRow>,
        ts_config_rows: Vec<PgTsConfigRow>,
        ts_config_map_rows: Vec<PgTsConfigMapRow>,
        constraint_rows: Vec<PgConstraintRow>,
        operator_rows: Vec<PgOperatorRow>,
        opclass_rows: Vec<PgOpclassRow>,
        opfamily_rows: Vec<PgOpfamilyRow>,
        partitioned_table_rows: Vec<PgPartitionedTableRow>,
        proc_rows: Vec<PgProcRow>,
        aggregate_rows: Vec<PgAggregateRow>,
        cast_rows: Vec<PgCastRow>,
        collation_rows: Vec<PgCollationRow>,
        foreign_data_wrapper_rows: Vec<PgForeignDataWrapperRow>,
        database_rows: Vec<PgDatabaseRow>,
        tablespace_rows: Vec<PgTablespaceRow>,
        statistic_rows: Vec<PgStatisticRow>,
        type_rows: Vec<PgTypeRow>,
    ) -> Self {
        let mut cache = Self::default();
        for row in namespace_rows {
            cache.insert_namespace_row(row);
        }
        for row in type_rows {
            cache
                .types_by_name
                .insert(row.typname.to_ascii_lowercase(), row.clone());
            cache.types_by_oid.insert(row.oid, row);
        }
        for row in class_rows {
            cache
                .classes_by_name
                .insert(row.relname.to_ascii_lowercase(), row.clone());
            cache.classes_by_oid.insert(row.oid, row);
        }
        let mut attrs_by_relid = BTreeMap::<u32, Vec<PgAttributeRow>>::new();
        for row in attribute_rows {
            attrs_by_relid.entry(row.attrelid).or_default().push(row);
        }
        for rows in attrs_by_relid.values_mut() {
            sort_pg_attribute_rows(rows);
        }
        cache.attributes_by_relid = attrs_by_relid;
        let mut attrdefs = attrdef_rows;
        sort_pg_attrdef_rows(&mut attrdefs);
        for row in attrdefs {
            cache.attrdefs_by_key.insert((row.adrelid, row.adnum), row);
        }
        cache.depend_rows = depend_rows;
        sort_pg_depend_rows(&mut cache.depend_rows);
        cache.inherit_rows = inherit_rows;
        sort_pg_inherits_rows(&mut cache.inherit_rows);
        cache.index_rows = index_rows;
        sort_pg_index_rows(&mut cache.index_rows);
        cache.rewrite_rows = rewrite_rows;
        sort_pg_rewrite_rows(&mut cache.rewrite_rows);
        cache.trigger_rows = trigger_rows;
        sort_pg_trigger_rows(&mut cache.trigger_rows);
        cache.policy_rows = policy_rows;
        sort_pg_policy_rows(&mut cache.policy_rows);
        cache.publication_rows = publication_rows;
        sort_pg_publication_rows(&mut cache.publication_rows);
        cache.publication_rel_rows = publication_rel_rows;
        sort_pg_publication_rel_rows(&mut cache.publication_rel_rows);
        cache.publication_namespace_rows = publication_namespace_rows;
        sort_pg_publication_namespace_rows(&mut cache.publication_namespace_rows);
        cache.statistic_ext_rows = statistic_ext_rows;
        sort_pg_statistic_ext_rows(&mut cache.statistic_ext_rows);
        cache.statistic_ext_data_rows = statistic_ext_data_rows;
        sort_pg_statistic_ext_data_rows(&mut cache.statistic_ext_data_rows);
        cache.am_rows = am_rows;
        sort_pg_am_rows(&mut cache.am_rows);
        cache.amop_rows = amop_rows;
        sort_pg_amop_rows(&mut cache.amop_rows);
        cache.amproc_rows = amproc_rows;
        sort_pg_amproc_rows(&mut cache.amproc_rows);
        cache.authid_rows = authid_rows;
        sort_pg_authid_rows(&mut cache.authid_rows);
        cache.auth_members_rows = auth_members_rows;
        sort_pg_auth_members_rows(&mut cache.auth_members_rows);
        cache.language_rows = language_rows;
        sort_pg_language_rows(&mut cache.language_rows);
        cache.ts_parser_rows = ts_parser_rows;
        sort_pg_ts_parser_rows(&mut cache.ts_parser_rows);
        cache.ts_template_rows = ts_template_rows;
        sort_pg_ts_template_rows(&mut cache.ts_template_rows);
        cache.ts_dict_rows = ts_dict_rows;
        sort_pg_ts_dict_rows(&mut cache.ts_dict_rows);
        cache.ts_config_rows = ts_config_rows;
        sort_pg_ts_config_rows(&mut cache.ts_config_rows);
        cache.ts_config_map_rows = ts_config_map_rows;
        sort_pg_ts_config_map_rows(&mut cache.ts_config_map_rows);
        cache.constraint_rows = constraint_rows;
        sort_pg_constraint_rows(&mut cache.constraint_rows);
        cache.operator_rows = operator_rows;
        sort_pg_operator_rows(&mut cache.operator_rows);
        cache.opclass_rows = opclass_rows;
        sort_pg_opclass_rows(&mut cache.opclass_rows);
        cache.opfamily_rows = opfamily_rows;
        sort_pg_opfamily_rows(&mut cache.opfamily_rows);
        cache.partitioned_tables_by_relid = partitioned_table_rows
            .into_iter()
            .map(|row| (row.partrelid, row))
            .collect();
        cache.proc_rows = proc_rows;
        sort_pg_proc_rows(&mut cache.proc_rows);
        let mut aggregate_rows = aggregate_rows;
        sort_pg_aggregate_rows(&mut aggregate_rows);
        for row in aggregate_rows {
            cache.aggregates_by_fnoid.insert(row.aggfnoid, row);
        }
        cache.cast_rows = cast_rows;
        sort_pg_cast_rows(&mut cache.cast_rows);
        cache.collation_rows = collation_rows;
        sort_pg_collation_rows(&mut cache.collation_rows);
        cache.foreign_data_wrapper_rows = foreign_data_wrapper_rows;
        sort_pg_foreign_data_wrapper_rows(&mut cache.foreign_data_wrapper_rows);
        cache.database_rows = database_rows;
        sort_pg_database_rows(&mut cache.database_rows);
        cache.tablespace_rows = tablespace_rows;
        sort_pg_tablespace_rows(&mut cache.tablespace_rows);
        cache.statistic_rows = statistic_rows;
        cache.normalize_composite_array_types();
        cache
    }

    pub fn namespace_by_name(&self, name: &str) -> Option<&PgNamespaceRow> {
        self.namespaces_by_name
            .get(&normalize_catalog_name(name).to_ascii_lowercase())
    }

    pub fn namespace_by_name_exact(&self, name: &str) -> Option<&PgNamespaceRow> {
        self.namespaces_by_name.get(&name.to_ascii_lowercase())
    }

    pub fn namespace_by_oid(&self, oid: u32) -> Option<&PgNamespaceRow> {
        self.namespaces_by_oid.get(&oid)
    }

    pub fn class_by_name(&self, name: &str) -> Option<&PgClassRow> {
        self.classes_by_name
            .get(&normalize_catalog_name(name).to_ascii_lowercase())
    }

    pub fn class_by_name_exact(&self, name: &str) -> Option<&PgClassRow> {
        self.classes_by_name.get(&name.to_ascii_lowercase())
    }

    pub fn class_by_oid(&self, oid: u32) -> Option<&PgClassRow> {
        self.classes_by_oid.get(&oid)
    }

    pub fn attributes_by_relid(&self, relid: u32) -> Option<&[PgAttributeRow]> {
        self.attributes_by_relid.get(&relid).map(Vec::as_slice)
    }

    pub fn attrdef_by_relid_attnum(&self, relid: u32, attnum: i16) -> Option<&PgAttrdefRow> {
        self.attrdefs_by_key.get(&(relid, attnum))
    }

    pub fn type_by_name(&self, name: &str) -> Option<&PgTypeRow> {
        self.types_by_name
            .get(&normalize_catalog_name(name).to_ascii_lowercase())
    }

    pub fn type_by_oid(&self, oid: u32) -> Option<&PgTypeRow> {
        self.types_by_oid.get(&oid)
    }

    pub fn extend_type_rows(&mut self, rows: impl IntoIterator<Item = PgTypeRow>) {
        for row in rows {
            self.types_by_name
                .insert(row.typname.to_ascii_lowercase(), row.clone());
            self.types_by_oid.insert(row.oid, row);
        }
    }

    pub fn namespace_rows(&self) -> Vec<PgNamespaceRow> {
        self.namespaces_by_oid.values().cloned().collect()
    }

    pub fn class_rows(&self) -> Vec<PgClassRow> {
        self.classes_by_oid.values().cloned().collect()
    }

    pub fn attribute_rows(&self) -> Vec<PgAttributeRow> {
        self.attributes_by_relid
            .values()
            .flat_map(|rows| rows.iter().cloned())
            .collect()
    }

    pub fn type_rows(&self) -> Vec<PgTypeRow> {
        self.types_by_oid.values().cloned().collect()
    }

    pub fn attrdef_rows(&self) -> Vec<PgAttrdefRow> {
        self.attrdefs_by_key.values().cloned().collect()
    }

    pub fn depend_rows(&self) -> Vec<PgDependRow> {
        self.depend_rows.clone()
    }

    pub fn inherit_rows(&self) -> Vec<PgInheritsRow> {
        self.inherit_rows.clone()
    }

    pub fn partitioned_table_row(&self, relation_oid: u32) -> Option<&PgPartitionedTableRow> {
        self.partitioned_tables_by_relid.get(&relation_oid)
    }

    pub fn partitioned_table_rows(&self) -> Vec<PgPartitionedTableRow> {
        self.partitioned_tables_by_relid.values().cloned().collect()
    }

    pub fn index_rows(&self) -> Vec<PgIndexRow> {
        self.index_rows.clone()
    }

    pub fn rewrite_rows(&self) -> Vec<PgRewriteRow> {
        self.rewrite_rows.clone()
    }

    pub fn trigger_rows(&self) -> Vec<PgTriggerRow> {
        self.trigger_rows.clone()
    }

    pub fn publication_rows(&self) -> Vec<PgPublicationRow> {
        self.publication_rows.clone()
    }

    pub fn publication_row_by_name(&self, name: &str) -> Option<&PgPublicationRow> {
        let normalized = normalize_catalog_name(name);
        self.publication_rows
            .iter()
            .find(|row| row.pubname.eq_ignore_ascii_case(normalized))
    }

    pub fn publication_row_by_oid(&self, oid: u32) -> Option<&PgPublicationRow> {
        self.publication_rows.iter().find(|row| row.oid == oid)
    }

    pub fn publication_rel_rows(&self) -> Vec<PgPublicationRelRow> {
        self.publication_rel_rows.clone()
    }

    pub fn publication_rel_rows_for_publication(
        &self,
        publication_oid: u32,
    ) -> Vec<PgPublicationRelRow> {
        self.publication_rel_rows
            .iter()
            .filter(|row| row.prpubid == publication_oid)
            .cloned()
            .collect()
    }

    pub fn publication_rel_rows_for_relation(&self, relation_oid: u32) -> Vec<PgPublicationRelRow> {
        self.publication_rel_rows
            .iter()
            .filter(|row| row.prrelid == relation_oid)
            .cloned()
            .collect()
    }

    pub fn publication_namespace_rows(&self) -> Vec<PgPublicationNamespaceRow> {
        self.publication_namespace_rows.clone()
    }

    pub fn publication_namespace_rows_for_publication(
        &self,
        publication_oid: u32,
    ) -> Vec<PgPublicationNamespaceRow> {
        self.publication_namespace_rows
            .iter()
            .filter(|row| row.pnpubid == publication_oid)
            .cloned()
            .collect()
    }

    pub fn publication_namespace_rows_for_namespace(
        &self,
        namespace_oid: u32,
    ) -> Vec<PgPublicationNamespaceRow> {
        self.publication_namespace_rows
            .iter()
            .filter(|row| row.pnnspid == namespace_oid)
            .cloned()
            .collect()
    }

    pub fn statistic_ext_rows(&self) -> Vec<PgStatisticExtRow> {
        self.statistic_ext_rows.clone()
    }

    pub fn statistic_ext_row_by_oid(&self, oid: u32) -> Option<&PgStatisticExtRow> {
        self.statistic_ext_rows.iter().find(|row| row.oid == oid)
    }

    pub fn statistic_ext_row_by_name_namespace(
        &self,
        name: &str,
        namespace_oid: u32,
    ) -> Option<&PgStatisticExtRow> {
        let normalized = normalize_catalog_name(name);
        self.statistic_ext_rows.iter().find(|row| {
            row.stxnamespace == namespace_oid && row.stxname.eq_ignore_ascii_case(normalized)
        })
    }

    pub fn statistic_ext_rows_for_relation(&self, relation_oid: u32) -> Vec<PgStatisticExtRow> {
        let start = self
            .statistic_ext_rows
            .partition_point(|row| row.stxrelid < relation_oid);
        let end = start
            + self.statistic_ext_rows[start..].partition_point(|row| row.stxrelid == relation_oid);
        self.statistic_ext_rows[start..end].to_vec()
    }

    pub fn statistic_ext_data_rows(&self) -> Vec<PgStatisticExtDataRow> {
        self.statistic_ext_data_rows.clone()
    }

    pub fn statistic_ext_data_row(
        &self,
        stxoid: u32,
        stxdinherit: bool,
    ) -> Option<&PgStatisticExtDataRow> {
        self.statistic_ext_data_rows
            .iter()
            .find(|row| row.stxoid == stxoid && row.stxdinherit == stxdinherit)
    }

    pub fn policy_rows(&self) -> Vec<PgPolicyRow> {
        self.policy_rows.clone()
    }

    pub fn trigger_rows_for_relation(&self, relation_oid: u32) -> Vec<PgTriggerRow> {
        self.trigger_rows
            .iter()
            .filter(|row| row.tgrelid == relation_oid)
            .cloned()
            .collect()
    }

    pub fn rewrite_rows_for_relation(&self, relation_oid: u32) -> Vec<PgRewriteRow> {
        self.rewrite_rows
            .iter()
            .filter(|row| row.ev_class == relation_oid)
            .cloned()
            .collect()
    }

    pub fn policy_rows_for_relation(&self, relation_oid: u32) -> Vec<PgPolicyRow> {
        self.policy_rows
            .iter()
            .filter(|row| row.polrelid == relation_oid)
            .cloned()
            .collect()
    }

    pub fn am_rows(&self) -> Vec<PgAmRow> {
        self.am_rows.clone()
    }

    pub fn amop_rows(&self) -> Vec<PgAmopRow> {
        self.amop_rows.clone()
    }

    pub fn amproc_rows(&self) -> Vec<PgAmprocRow> {
        self.amproc_rows.clone()
    }

    pub fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        self.authid_rows.clone()
    }

    pub fn auth_members_rows(&self) -> Vec<PgAuthMembersRow> {
        self.auth_members_rows.clone()
    }

    pub fn language_rows(&self) -> Vec<PgLanguageRow> {
        self.language_rows.clone()
    }

    pub fn ts_parser_rows(&self) -> Vec<PgTsParserRow> {
        self.ts_parser_rows.clone()
    }

    pub fn ts_template_rows(&self) -> Vec<PgTsTemplateRow> {
        self.ts_template_rows.clone()
    }

    pub fn ts_dict_rows(&self) -> Vec<PgTsDictRow> {
        self.ts_dict_rows.clone()
    }

    pub fn ts_config_rows(&self) -> Vec<PgTsConfigRow> {
        self.ts_config_rows.clone()
    }

    pub fn ts_config_map_rows(&self) -> Vec<PgTsConfigMapRow> {
        self.ts_config_map_rows.clone()
    }

    pub fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        self.constraint_rows.clone()
    }

    pub fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
        self.constraint_rows
            .iter()
            .filter(|row| row.conrelid == relation_oid)
            .cloned()
            .collect()
    }

    pub fn operator_rows(&self) -> Vec<PgOperatorRow> {
        self.operator_rows.clone()
    }

    pub fn opclass_rows(&self) -> Vec<PgOpclassRow> {
        self.opclass_rows.clone()
    }

    pub fn opfamily_rows(&self) -> Vec<PgOpfamilyRow> {
        self.opfamily_rows.clone()
    }

    pub fn operator_by_name_left_right(
        &self,
        name: &str,
        left_type_oid: u32,
        right_type_oid: u32,
    ) -> Option<&PgOperatorRow> {
        let normalized = normalize_catalog_name(name);
        self.operator_rows.iter().find(|row| {
            row.oprname.eq_ignore_ascii_case(normalized)
                && row.oprleft == left_type_oid
                && row.oprright == right_type_oid
        })
    }

    pub fn proc_rows(&self) -> Vec<PgProcRow> {
        self.proc_rows.clone()
    }

    pub fn aggregate_rows(&self) -> Vec<PgAggregateRow> {
        self.aggregates_by_fnoid.values().cloned().collect()
    }

    pub fn aggregate_by_fnoid(&self, aggfnoid: u32) -> Option<&PgAggregateRow> {
        self.aggregates_by_fnoid.get(&aggfnoid)
    }

    pub fn proc_by_oid(&self, oid: u32) -> Option<&PgProcRow> {
        self.proc_rows.iter().find(|row| row.oid == oid)
    }

    pub fn proc_rows_by_name(&self, name: &str) -> Vec<&PgProcRow> {
        let normalized = normalize_catalog_name(name);
        self.proc_rows
            .iter()
            .filter(|row| row.proname.eq_ignore_ascii_case(normalized))
            .collect()
    }

    pub fn cast_rows(&self) -> Vec<PgCastRow> {
        self.cast_rows.clone()
    }

    pub fn cast_by_source_target(
        &self,
        source_type_oid: u32,
        target_type_oid: u32,
    ) -> Option<&PgCastRow> {
        self.cast_rows
            .iter()
            .find(|row| row.castsource == source_type_oid && row.casttarget == target_type_oid)
    }

    pub fn collation_rows(&self) -> Vec<PgCollationRow> {
        self.collation_rows.clone()
    }

    pub fn foreign_data_wrapper_rows(&self) -> Vec<PgForeignDataWrapperRow> {
        self.foreign_data_wrapper_rows.clone()
    }

    pub fn database_rows(&self) -> Vec<PgDatabaseRow> {
        self.database_rows.clone()
    }

    pub fn tablespace_rows(&self) -> Vec<PgTablespaceRow> {
        self.tablespace_rows.clone()
    }

    pub fn statistic_rows(&self) -> Vec<PgStatisticRow> {
        self.statistic_rows.clone()
    }
}
pub fn normalize_catalog_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
}

fn catalog_entry_sql_type_oid(catalog: &Catalog, sql_type: SqlType) -> u32 {
    if sql_type.is_array
        && matches!(sql_type.kind, SqlTypeKind::Composite | SqlTypeKind::Record)
        && sql_type.type_oid != 0
        && let Some(entry) = catalog
            .entries()
            .find_map(|(_, entry)| (entry.row_type_oid == sql_type.type_oid).then_some(entry))
        && entry.array_type_oid != 0
    {
        return entry.array_type_oid;
    }
    sql_type_oid(sql_type)
}

pub fn format_indkey(indkey: &[i16]) -> String {
    indkey
        .iter()
        .map(|attnum| attnum.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

fn catalog_object_name(name: &str) -> &str {
    name.rsplit_once('.')
        .map(|(_, object)| object)
        .unwrap_or(name)
}

pub fn sql_type_oid(sql_type: SqlType) -> u32 {
    if !sql_type.is_array && sql_type.type_oid != 0 {
        return sql_type.type_oid;
    }
    if let Some(row) = builtin_type_rows()
        .into_iter()
        .chain(bootstrap_composite_type_rows())
        .find(|row| row.sql_type == sql_type)
    {
        return row.oid;
    }
    if let Some(range_type) = range_type_ref_for_sql_type(sql_type) {
        if sql_type.is_array {
            if sql_type.type_oid != 0 && matches!(sql_type.kind, SqlTypeKind::Range) {
                return sql_type.type_oid;
            }
            if let Some(array_row) = builtin_type_rows()
                .into_iter()
                .find(|row| row.typelem == range_type.type_oid())
            {
                return array_row.oid;
            }
            unreachable!("range arrays are unsupported");
        }
        return range_type.type_oid();
    }
    if let Some(multirange_type) =
        crate::include::catalog::multirange_type_ref_for_sql_type(sql_type)
    {
        if sql_type.is_array {
            if sql_type.type_oid != 0 && matches!(sql_type.kind, SqlTypeKind::Multirange) {
                return sql_type.type_oid;
            }
            if let Some(array_row) = builtin_type_rows()
                .into_iter()
                .find(|row| row.typelem == multirange_type.type_oid())
            {
                return array_row.oid;
            }
            unreachable!("multirange arrays are unsupported");
        }
        return multirange_type.type_oid();
    }
    if !sql_type.is_array && sql_type.type_oid != 0 {
        return sql_type.type_oid;
    }
    match (sql_type.kind, sql_type.is_array) {
        (SqlTypeKind::Range, false) => sql_type.type_oid,
        (SqlTypeKind::Range, true) => sql_type.type_oid,
        (SqlTypeKind::Multirange, false) => sql_type.type_oid,
        (SqlTypeKind::Multirange, true) => sql_type.type_oid,
        (SqlTypeKind::AnyElement, false) => crate::include::catalog::ANYELEMENTOID,
        (SqlTypeKind::AnyElement, true) => unreachable!("anyelement arrays are unsupported"),
        (SqlTypeKind::AnyEnum, false) => crate::include::catalog::ANYENUMOID,
        (SqlTypeKind::AnyEnum, true) => unreachable!("anyenum arrays are unsupported"),
        (SqlTypeKind::AnyArray, false) => ANYARRAYOID,
        (SqlTypeKind::AnyArray, true) => unreachable!("anyarray arrays are unsupported"),
        (SqlTypeKind::AnyRange, false) => crate::include::catalog::ANYRANGEOID,
        (SqlTypeKind::AnyRange, true) => unreachable!("anyrange arrays are unsupported"),
        (SqlTypeKind::AnyMultirange, false) => crate::include::catalog::ANYMULTIRANGEOID,
        (SqlTypeKind::AnyMultirange, true) => unreachable!("anymultirange arrays are unsupported"),
        (SqlTypeKind::AnyCompatible, false) => crate::include::catalog::ANYCOMPATIBLEOID,
        (SqlTypeKind::AnyCompatible, true) => {
            unreachable!("anycompatible arrays are unsupported")
        }
        (SqlTypeKind::AnyCompatibleArray, false) => crate::include::catalog::ANYCOMPATIBLEARRAYOID,
        (SqlTypeKind::AnyCompatibleArray, true) => {
            unreachable!("anycompatiblearray arrays are unsupported")
        }
        (SqlTypeKind::AnyCompatibleRange, false) => crate::include::catalog::ANYCOMPATIBLERANGEOID,
        (SqlTypeKind::AnyCompatibleRange, true) => {
            unreachable!("anycompatiblerange arrays are unsupported")
        }
        (SqlTypeKind::AnyCompatibleMultirange, false) => {
            crate::include::catalog::ANYCOMPATIBLEMULTIRANGEOID
        }
        (SqlTypeKind::AnyCompatibleMultirange, true) => {
            unreachable!("anycompatiblemultirange arrays are unsupported")
        }
        (SqlTypeKind::Record, false) => sql_type.type_oid,
        (SqlTypeKind::Record, true) => crate::include::catalog::RECORD_ARRAY_TYPE_OID,
        (SqlTypeKind::Composite, false) => sql_type.type_oid,
        (SqlTypeKind::Composite, true) => crate::include::catalog::RECORD_ARRAY_TYPE_OID,
        (SqlTypeKind::Enum, false) => sql_type.type_oid,
        (SqlTypeKind::Enum, true) => sql_type.type_oid,
        (SqlTypeKind::Shell, false) => sql_type.type_oid,
        (SqlTypeKind::Shell, true) => unreachable!("shell type arrays are unsupported"),
        (SqlTypeKind::Bool, false) => BOOL_TYPE_OID,
        (SqlTypeKind::Bool, true) => BOOL_ARRAY_TYPE_OID,
        (SqlTypeKind::Bit, false) => BIT_TYPE_OID,
        (SqlTypeKind::Bit, true) => BIT_ARRAY_TYPE_OID,
        (SqlTypeKind::VarBit, false) => VARBIT_TYPE_OID,
        (SqlTypeKind::VarBit, true) => VARBIT_ARRAY_TYPE_OID,
        (SqlTypeKind::Bytea, false) => BYTEA_TYPE_OID,
        (SqlTypeKind::Bytea, true) => BYTEA_ARRAY_TYPE_OID,
        (SqlTypeKind::Uuid, false) => UUID_TYPE_OID,
        (SqlTypeKind::Uuid, true) => UUID_ARRAY_TYPE_OID,
        (SqlTypeKind::InternalChar, false) => INTERNAL_CHAR_TYPE_OID,
        (SqlTypeKind::InternalChar, true) => INTERNAL_CHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Internal, false) => crate::include::catalog::INTERNAL_TYPE_OID,
        (SqlTypeKind::Internal, true) => unreachable!("internal arrays are unsupported"),
        (SqlTypeKind::Cstring, false) => crate::include::catalog::CSTRING_TYPE_OID,
        (SqlTypeKind::Cstring, true) => crate::include::catalog::CSTRING_ARRAY_TYPE_OID,
        (SqlTypeKind::Void, false) => crate::include::catalog::VOID_TYPE_OID,
        (SqlTypeKind::Void, true) => unreachable!("void arrays are unsupported"),
        (SqlTypeKind::Trigger, false) => crate::include::catalog::TRIGGER_TYPE_OID,
        (SqlTypeKind::Trigger, true) => unreachable!("trigger arrays are unsupported"),
        (SqlTypeKind::FdwHandler, false) => crate::include::catalog::FDW_HANDLER_TYPE_OID,
        (SqlTypeKind::FdwHandler, true) => unreachable!("fdw_handler arrays are unsupported"),
        (SqlTypeKind::Int8, false) => INT8_TYPE_OID,
        (SqlTypeKind::Int8, true) => INT8_ARRAY_TYPE_OID,
        (SqlTypeKind::PgLsn, false) => crate::include::catalog::PG_LSN_TYPE_OID,
        (SqlTypeKind::PgLsn, true) => crate::include::catalog::PG_LSN_ARRAY_TYPE_OID,
        (SqlTypeKind::Name, false) => crate::include::catalog::NAME_TYPE_OID,
        (SqlTypeKind::Name, true) => crate::include::catalog::NAME_ARRAY_TYPE_OID,
        (SqlTypeKind::Int2, false) => INT2_TYPE_OID,
        (SqlTypeKind::Int2, true) => INT2_ARRAY_TYPE_OID,
        (SqlTypeKind::Int2Vector, false) => crate::include::catalog::INT2VECTOR_TYPE_OID,
        (SqlTypeKind::Int2Vector, true) => unreachable!("int2vector arrays are unsupported"),
        (SqlTypeKind::Int4, false) => INT4_TYPE_OID,
        (SqlTypeKind::Int4, true) => INT4_ARRAY_TYPE_OID,
        (SqlTypeKind::Text, false) => TEXT_TYPE_OID,
        (SqlTypeKind::Text, true) => TEXT_ARRAY_TYPE_OID,
        (SqlTypeKind::Tid, false) => TID_TYPE_OID,
        (SqlTypeKind::Tid, true) => TID_ARRAY_TYPE_OID,
        (SqlTypeKind::Xid, false) => XID_TYPE_OID,
        (SqlTypeKind::Xid, true) => XID_ARRAY_TYPE_OID,
        (SqlTypeKind::Oid, false) => OID_TYPE_OID,
        (SqlTypeKind::Oid, true) => OID_ARRAY_TYPE_OID,
        (SqlTypeKind::RegProc, false) => crate::include::catalog::REGPROC_TYPE_OID,
        (SqlTypeKind::RegProc, true) => crate::include::catalog::REGPROC_ARRAY_TYPE_OID,
        (SqlTypeKind::RegClass, false) => crate::include::catalog::REGCLASS_TYPE_OID,
        (SqlTypeKind::RegClass, true) => crate::include::catalog::REGCLASS_ARRAY_TYPE_OID,
        (SqlTypeKind::RegType, false) => crate::include::catalog::REGTYPE_TYPE_OID,
        (SqlTypeKind::RegType, true) => crate::include::catalog::REGTYPE_ARRAY_TYPE_OID,
        (SqlTypeKind::RegRole, false) => crate::include::catalog::REGROLE_TYPE_OID,
        (SqlTypeKind::RegRole, true) => crate::include::catalog::REGROLE_ARRAY_TYPE_OID,
        (SqlTypeKind::RegNamespace, false) => crate::include::catalog::REGNAMESPACE_TYPE_OID,
        (SqlTypeKind::RegNamespace, true) => crate::include::catalog::REGNAMESPACE_ARRAY_TYPE_OID,
        (SqlTypeKind::RegOper, false) => crate::include::catalog::REGOPER_TYPE_OID,
        (SqlTypeKind::RegOper, true) => crate::include::catalog::REGOPER_ARRAY_TYPE_OID,
        (SqlTypeKind::RegOperator, false) => crate::include::catalog::REGOPERATOR_TYPE_OID,
        (SqlTypeKind::RegOperator, true) => crate::include::catalog::REGOPERATOR_ARRAY_TYPE_OID,
        (SqlTypeKind::RegProcedure, false) => crate::include::catalog::REGPROCEDURE_TYPE_OID,
        (SqlTypeKind::RegProcedure, true) => crate::include::catalog::REGPROCEDURE_ARRAY_TYPE_OID,
        (SqlTypeKind::RegCollation, false) => crate::include::catalog::REGCOLLATION_TYPE_OID,
        (SqlTypeKind::RegCollation, true) => crate::include::catalog::REGCOLLATION_ARRAY_TYPE_OID,
        (SqlTypeKind::OidVector, false) => crate::include::catalog::OIDVECTOR_TYPE_OID,
        (SqlTypeKind::OidVector, true) => unreachable!("oidvector arrays are unsupported"),
        (SqlTypeKind::Float4, false) => FLOAT4_TYPE_OID,
        (SqlTypeKind::Float4, true) => FLOAT4_ARRAY_TYPE_OID,
        (SqlTypeKind::Float8, false) => FLOAT8_TYPE_OID,
        (SqlTypeKind::Float8, true) => FLOAT8_ARRAY_TYPE_OID,
        (SqlTypeKind::Money, false) => MONEY_TYPE_OID,
        (SqlTypeKind::Money, true) => MONEY_ARRAY_TYPE_OID,
        (SqlTypeKind::Inet, false) => crate::include::catalog::INET_TYPE_OID,
        (SqlTypeKind::Inet, true) => crate::include::catalog::INET_ARRAY_TYPE_OID,
        (SqlTypeKind::Cidr, false) => crate::include::catalog::CIDR_TYPE_OID,
        (SqlTypeKind::Cidr, true) => crate::include::catalog::CIDR_ARRAY_TYPE_OID,
        (SqlTypeKind::MacAddr, false) => crate::include::catalog::MACADDR_TYPE_OID,
        (SqlTypeKind::MacAddr, true) => crate::include::catalog::MACADDR_ARRAY_TYPE_OID,
        (SqlTypeKind::MacAddr8, false) => crate::include::catalog::MACADDR8_TYPE_OID,
        (SqlTypeKind::MacAddr8, true) => crate::include::catalog::MACADDR8_ARRAY_TYPE_OID,
        (SqlTypeKind::Varchar, false) => VARCHAR_TYPE_OID,
        (SqlTypeKind::Varchar, true) => VARCHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Char, false) => BPCHAR_TYPE_OID,
        (SqlTypeKind::Char, true) => BPCHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Date, false) => crate::include::catalog::DATE_TYPE_OID,
        (SqlTypeKind::Date, true) => crate::include::catalog::DATE_ARRAY_TYPE_OID,
        (SqlTypeKind::Time, false) => crate::include::catalog::TIME_TYPE_OID,
        (SqlTypeKind::Time, true) => crate::include::catalog::TIME_ARRAY_TYPE_OID,
        (SqlTypeKind::TimeTz, false) => crate::include::catalog::TIMETZ_TYPE_OID,
        (SqlTypeKind::TimeTz, true) => crate::include::catalog::TIMETZ_ARRAY_TYPE_OID,
        (SqlTypeKind::Timestamp, false) => TIMESTAMP_TYPE_OID,
        (SqlTypeKind::Timestamp, true) => TIMESTAMP_ARRAY_TYPE_OID,
        (SqlTypeKind::TimestampTz, false) => crate::include::catalog::TIMESTAMPTZ_TYPE_OID,
        (SqlTypeKind::TimestampTz, true) => crate::include::catalog::TIMESTAMPTZ_ARRAY_TYPE_OID,
        (SqlTypeKind::Interval, false) => INTERVAL_TYPE_OID,
        (SqlTypeKind::Interval, true) => INTERVAL_ARRAY_TYPE_OID,
        (SqlTypeKind::Numeric, false) => NUMERIC_TYPE_OID,
        (SqlTypeKind::Numeric, true) => NUMERIC_ARRAY_TYPE_OID,
        (SqlTypeKind::Json, false) => JSON_TYPE_OID,
        (SqlTypeKind::Json, true) => JSON_ARRAY_TYPE_OID,
        (SqlTypeKind::Jsonb, false) => JSONB_TYPE_OID,
        (SqlTypeKind::Jsonb, true) => JSONB_ARRAY_TYPE_OID,
        (SqlTypeKind::JsonPath, false) => JSONPATH_TYPE_OID,
        (SqlTypeKind::JsonPath, true) => JSONPATH_ARRAY_TYPE_OID,
        (SqlTypeKind::Xml, false) => XML_TYPE_OID,
        (SqlTypeKind::Xml, true) => XML_ARRAY_TYPE_OID,
        (SqlTypeKind::Point, false) => POINT_TYPE_OID,
        (SqlTypeKind::Point, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Lseg, false) => LSEG_TYPE_OID,
        (SqlTypeKind::Lseg, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Path, false) => PATH_TYPE_OID,
        (SqlTypeKind::Path, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Box, false) => BOX_TYPE_OID,
        (SqlTypeKind::Box, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Polygon, false) => POLYGON_TYPE_OID,
        (SqlTypeKind::Polygon, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Line, false) => LINE_TYPE_OID,
        (SqlTypeKind::Line, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::Circle, false) => CIRCLE_TYPE_OID,
        (SqlTypeKind::Circle, true) => unreachable!("geometry arrays are unsupported"),
        (SqlTypeKind::TsVector, false) => TSVECTOR_TYPE_OID,
        (SqlTypeKind::TsVector, true) => TSVECTOR_ARRAY_TYPE_OID,
        (SqlTypeKind::TsQuery, false) => TSQUERY_TYPE_OID,
        (SqlTypeKind::TsQuery, true) => TSQUERY_ARRAY_TYPE_OID,
        (SqlTypeKind::RegConfig, false) => REGCONFIG_TYPE_OID,
        (SqlTypeKind::RegConfig, true) => REGCONFIG_ARRAY_TYPE_OID,
        (SqlTypeKind::RegDictionary, false) => REGDICTIONARY_TYPE_OID,
        (SqlTypeKind::RegDictionary, true) => REGDICTIONARY_ARRAY_TYPE_OID,
        (SqlTypeKind::PgNodeTree, false) => crate::include::catalog::PG_NODE_TREE_TYPE_OID,
        (SqlTypeKind::PgNodeTree, true) => unreachable!("pg_node_tree arrays are unsupported"),
        (SqlTypeKind::Int4Range, _) => unreachable!("range handled above"),
        (SqlTypeKind::Int8Range, _) => unreachable!("range handled above"),
        (SqlTypeKind::NumericRange, _) => unreachable!("range handled above"),
        (SqlTypeKind::DateRange, _) => unreachable!("range handled above"),
        (SqlTypeKind::TimestampRange, _) => unreachable!("range handled above"),
        (SqlTypeKind::TimestampTzRange, _) => unreachable!("range handled above"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::CatalogStore;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::RelationDesc;
    use crate::include::catalog::{
        BOOL_CMP_EQ_PROC_OID, BOOTSTRAP_SUPERUSER_NAME, BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID,
        C_COLLATION_OID, CURRENT_DATABASE_NAME, DEFAULT_COLLATION_OID, DEFAULT_TABLESPACE_OID,
        DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, HEAP_TABLE_AM_OID,
        INT4_CMP_EQ_PROC_OID, INT4_TYPE_OID, JSON_TYPE_OID, OID_TYPE_OID, PG_ATTRDEF_RELATION_OID,
        PG_CLASS_RELATION_OID, PG_NAMESPACE_RELATION_OID, PG_TYPE_RELATION_OID,
        POSIX_COLLATION_OID, PUBLIC_NAMESPACE_OID, TEXT_STARTS_WITH_PROC_OID, TEXT_TYPE_OID,
        VARCHAR_TYPE_OID,
    };
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pgrust_{prefix}_{nanos}"));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn catcache_derives_pg_class_and_pg_attribute_rows() {
        let mut catalog = Catalog::default();
        let entry = catalog
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("name", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();

        let cache = CatCache::from_catalog(&catalog);
        assert_eq!(
            cache.class_by_name("people").map(|row| row.oid),
            Some(entry.relation_oid)
        );
        assert_eq!(
            cache
                .attributes_by_relid(entry.relation_oid)
                .map(|rows| rows.len()),
            Some(2)
        );
        assert_eq!(
            cache.namespace_by_name("pg_catalog").map(|row| row.oid),
            Some(11)
        );
        assert_eq!(
            cache
                .class_by_name_exact("pg_catalog.pg_class")
                .map(|row| row.oid),
            Some(PG_CLASS_RELATION_OID)
        );
        let pg_class_attrs = cache.attributes_by_relid(PG_CLASS_RELATION_OID).unwrap();
        assert_eq!(
            pg_class_attrs
                .iter()
                .find(|row| row.attname == "relname")
                .map(|row| row.attcollation),
            Some(C_COLLATION_OID)
        );
        let people_attrs = cache.attributes_by_relid(entry.relation_oid).unwrap();
        assert_eq!(
            people_attrs
                .iter()
                .find(|row| row.attname == "name")
                .map(|row| row.attcollation),
            Some(DEFAULT_COLLATION_OID)
        );
        assert_eq!(
            cache
                .namespace_by_name_exact("pg_catalog")
                .map(|row| row.oid),
            Some(11)
        );
    }

    #[test]
    fn catcache_derives_builtin_pg_type_rows() {
        let cache = CatCache::from_catalog(&Catalog::default());
        assert_eq!(
            cache.type_by_name("int4").map(|row| row.oid),
            Some(INT4_TYPE_OID)
        );
        assert_eq!(
            cache.type_by_name("pg_class").map(|row| row.typrelid),
            Some(1259)
        );
    }

    #[test]
    fn catcache_loads_rows_from_physical_catalogs() {
        let base = temp_dir("catcache_from_physical");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("name", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'anon'".into());
        let entry = store.create_table("people", desc).unwrap();
        let index = store
            .create_index("people_name_idx", "people", true, &["name".into()])
            .unwrap();

        let cache = CatCache::from_physical(&base).unwrap();
        assert_eq!(
            cache.class_by_name("people").map(|row| row.oid),
            Some(entry.relation_oid)
        );
        assert_eq!(
            cache
                .attributes_by_relid(entry.relation_oid)
                .map(|rows| rows.len()),
            Some(2)
        );
        assert_eq!(
            cache
                .type_by_oid(entry.row_type_oid)
                .map(|row| row.typrelid),
            Some(entry.relation_oid)
        );
        assert_eq!(
            cache
                .attrdef_by_relid_attnum(entry.relation_oid, 2)
                .map(|row| row.adbin.as_str()),
            Some("'anon'")
        );
        assert!(cache.depend_rows().iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == entry.relation_oid
                && row.refclassid == PG_NAMESPACE_RELATION_OID
                && row.refobjid == PUBLIC_NAMESPACE_OID
                && row.deptype == DEPENDENCY_NORMAL
        }));
        assert!(cache.depend_rows().iter().any(|row| {
            row.classid == PG_TYPE_RELATION_OID
                && row.objid == entry.row_type_oid
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.deptype == DEPENDENCY_INTERNAL
        }));
        assert!(cache.depend_rows().iter().any(|row| {
            row.classid == PG_ATTRDEF_RELATION_OID
                && row.objid == entry.desc.columns[1].attrdef_oid.unwrap()
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 2
                && row.deptype == DEPENDENCY_AUTO
        }));
        assert_eq!(
            cache
                .class_by_name("people_name_idx")
                .map(|row| row.relkind),
            Some('i')
        );
        assert_eq!(
            cache.class_by_name("people_name_idx").map(|row| row.relam),
            Some(BTREE_AM_OID)
        );
        assert_eq!(
            cache
                .class_by_name("people_name_idx")
                .map(|row| row.relpersistence),
            Some('p')
        );
        assert_eq!(
            cache.class_by_name("people").map(|row| row.relowner),
            Some(BOOTSTRAP_SUPERUSER_OID)
        );
        assert_eq!(
            cache.class_by_name("people").map(|row| row.relam),
            Some(HEAP_TABLE_AM_OID)
        );
        assert!(cache.database_rows().iter().any(|row| {
            row.oid == 1
                && row.datname == CURRENT_DATABASE_NAME
                && row.dattablespace == DEFAULT_TABLESPACE_OID
        }));
        assert!(cache.authid_rows().iter().any(|row| {
            row.oid == BOOTSTRAP_SUPERUSER_OID
                && row.rolname == BOOTSTRAP_SUPERUSER_NAME
                && row.rolsuper
        }));
        assert!(cache.auth_members_rows().is_empty());
        assert!(
            cache.language_rows().iter().any(|row| {
                row.lanname == "internal" && row.lanowner == BOOTSTRAP_SUPERUSER_OID
            })
        );
        assert!(
            cache
                .language_rows()
                .iter()
                .any(|row| { row.lanname == "sql" && row.lanpltrusted })
        );
        assert!(cache.constraint_rows().iter().any(|row| {
            row.conname == "people_id_not_null"
                && row.contype == 'n'
                && row.conrelid == entry.relation_oid
                && row.connamespace == PUBLIC_NAMESPACE_OID
                && row.convalidated
        }));
        assert!(cache.operator_rows().iter().any(|row| {
            row.oid == 91
                && row.oprname == "="
                && row.oprcode == BOOL_CMP_EQ_PROC_OID
                && row.oprcanmerge
                && row.oprcanhash
        }));
        assert!(cache.operator_rows().iter().any(|row| {
            row.oid == 96
                && row.oprname == "="
                && row.oprcode == INT4_CMP_EQ_PROC_OID
                && row.oprleft == INT4_TYPE_OID
                && row.oprright == INT4_TYPE_OID
        }));
        assert!(cache.operator_rows().iter().any(|row| {
            row.oid == 3877
                && row.oprname == "^@"
                && row.oprcode == TEXT_STARTS_WITH_PROC_OID
                && row.oprleft == TEXT_TYPE_OID
                && row.oprright == TEXT_TYPE_OID
        }));
        assert!(cache.operator_rows().iter().any(|row| {
            row.oid == 664
                && row.oprname == "<"
                && row.oprcode == crate::include::catalog::TEXT_CMP_LT_PROC_OID
                && row.oprleft == TEXT_TYPE_OID
                && row.oprright == TEXT_TYPE_OID
        }));
        assert!(cache.operator_rows().iter().any(|row| {
            row.oid == 667
                && row.oprname == ">="
                && row.oprcode == crate::include::catalog::TEXT_CMP_GE_PROC_OID
                && row.oprleft == TEXT_TYPE_OID
                && row.oprright == TEXT_TYPE_OID
        }));
        assert!(cache.operator_rows().iter().any(|row| {
            row.oid == 1784
                && row.oprname == "="
                && row.oprcode == crate::include::catalog::BIT_CMP_EQ_PROC_OID
                && row.oprleft == crate::include::catalog::BIT_TYPE_OID
                && row.oprright == crate::include::catalog::BIT_TYPE_OID
                && row.oprcanmerge
        }));
        assert!(cache.operator_rows().iter().any(|row| {
            row.oid == 1806
                && row.oprname == "<"
                && row.oprcode == crate::include::catalog::VARBIT_CMP_LT_PROC_OID
                && row.oprleft == crate::include::catalog::VARBIT_TYPE_OID
                && row.oprright == crate::include::catalog::VARBIT_TYPE_OID
        }));
        assert!(cache.operator_rows().iter().any(|row| {
            row.oid == 1955
                && row.oprname == "="
                && row.oprcode == crate::include::catalog::BYTEA_CMP_EQ_PROC_OID
                && row.oprleft == crate::include::catalog::BYTEA_TYPE_OID
                && row.oprright == crate::include::catalog::BYTEA_TYPE_OID
                && row.oprcanmerge
                && row.oprcanhash
        }));
        assert!(cache.operator_rows().iter().any(|row| {
            row.oid == 1957
                && row.oprname == "<"
                && row.oprcode == crate::include::catalog::BYTEA_CMP_LT_PROC_OID
                && row.oprleft == crate::include::catalog::BYTEA_TYPE_OID
                && row.oprright == crate::include::catalog::BYTEA_TYPE_OID
        }));
        assert!(cache.operator_rows().iter().any(|row| {
            row.oid == 3240
                && row.oprname == "="
                && row.oprcode == crate::include::catalog::JSONB_CMP_EQ_PROC_OID
                && row.oprleft == crate::include::catalog::JSONB_TYPE_OID
                && row.oprright == crate::include::catalog::JSONB_TYPE_OID
                && row.oprcanmerge
                && row.oprcanhash
        }));
        assert!(cache.proc_rows().iter().any(|row| {
            row.proname == "lower"
                && row.pronargs == 1
                && row.prorettype == TEXT_TYPE_OID
                && row.prokind == 'f'
        }));
        assert!(cache.proc_rows().iter().any(|row| {
            row.proname == "count"
                && row.pronargs == 1
                && row.prorettype == crate::include::catalog::INT8_TYPE_OID
                && row.prokind == 'a'
        }));
        assert!(cache.proc_rows().iter().any(|row| {
            row.proname == "json_array_elements" && row.proretset && row.prorettype == JSON_TYPE_OID
        }));
        assert!(cache.proc_rows().iter().any(|row| {
            row.oid == crate::include::catalog::TEXT_CMP_LT_PROC_OID
                && row.proname == "text_lt"
                && row.proargtypes == format!("{TEXT_TYPE_OID} {TEXT_TYPE_OID}")
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
        }));
        assert!(cache.proc_rows().iter().any(|row| {
            row.oid == crate::include::catalog::TEXT_CMP_GE_PROC_OID
                && row.proname == "text_ge"
                && row.proargtypes == format!("{TEXT_TYPE_OID} {TEXT_TYPE_OID}")
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
        }));
        assert!(cache.proc_rows().iter().any(|row| {
            row.oid == crate::include::catalog::BIT_CMP_EQ_PROC_OID
                && row.proname == "biteq"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::BIT_TYPE_OID,
                        crate::include::catalog::BIT_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
        }));
        assert!(cache.proc_rows().iter().any(|row| {
            row.oid == crate::include::catalog::VARBIT_CMP_LT_PROC_OID
                && row.proname == "varbitlt"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::VARBIT_TYPE_OID,
                        crate::include::catalog::VARBIT_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
        }));
        assert!(cache.proc_rows().iter().any(|row| {
            row.oid == crate::include::catalog::BYTEA_CMP_EQ_PROC_OID
                && row.proname == "byteaeq"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::BYTEA_TYPE_OID,
                        crate::include::catalog::BYTEA_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
        }));
        assert!(cache.proc_rows().iter().any(|row| {
            row.oid == crate::include::catalog::BYTEA_CMP_LT_PROC_OID
                && row.proname == "bytealt"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::BYTEA_TYPE_OID,
                        crate::include::catalog::BYTEA_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
        }));
        assert!(cache.proc_rows().iter().any(|row| {
            row.oid == crate::include::catalog::JSONB_CMP_EQ_PROC_OID
                && row.proname == "jsonb_eq"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::JSONB_TYPE_OID,
                        crate::include::catalog::JSONB_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
        }));
        assert!(cache.cast_rows().iter().any(|row| {
            row.castsource == INT4_TYPE_OID
                && row.casttarget == OID_TYPE_OID
                && row.castcontext == 'i'
                && row.castmethod == 'b'
        }));
        assert!(cache.cast_rows().iter().any(|row| {
            row.castsource == INT4_TYPE_OID
                && row.casttarget == crate::include::catalog::NUMERIC_TYPE_OID
                && row.castfunc != 0
                && row.castmethod == 'f'
        }));
        assert!(cache.cast_rows().iter().any(|row| {
            row.castsource == VARCHAR_TYPE_OID
                && row.casttarget == TEXT_TYPE_OID
                && row.castcontext == 'i'
        }));
        assert!(cache.cast_rows().iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::JSONB_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(cache.cast_rows().iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::JSONPATH_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(cache.cast_rows().iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::VARBIT_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(cache.cast_rows().iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::INT4_ARRAY_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(cache.cast_rows().iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::JSONB_ARRAY_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert_eq!(
            cache
                .collation_rows()
                .iter()
                .map(|row| (row.oid, row.collname.as_str(), row.collprovider))
                .collect::<Vec<_>>(),
            vec![
                (DEFAULT_COLLATION_OID, "default", 'd'),
                (C_COLLATION_OID, "C", 'c'),
                (POSIX_COLLATION_OID, "POSIX", 'c'),
            ]
        );
        assert!(cache.tablespace_rows().iter().any(|row| {
            row.oid == DEFAULT_TABLESPACE_OID
                && row.spcname == "pg_default"
                && row.spcowner == BOOTSTRAP_SUPERUSER_OID
        }));
        assert!(cache.index_rows().iter().any(|row| {
            row.indexrelid == index.relation_oid
                && row.indrelid == entry.relation_oid
                && row.indisunique
                && row.indkey == vec![2]
        }));
        assert!(
            cache
                .am_rows()
                .iter()
                .any(|row| row.oid == BTREE_AM_OID && row.amname == "btree")
        );
    }

    #[test]
    fn catcache_supports_direct_proc_operator_and_cast_lookups() {
        let cache = CatCache::from_catalog(&Catalog::default());

        let lower = cache.proc_rows_by_name("pg_catalog.lower");
        assert!(!lower.is_empty());
        assert!(
            lower
                .iter()
                .all(|row| row.proname == "lower" && row.pronargs == 1)
        );
        assert!(
            lower
                .iter()
                .any(|row| row.proargtypes == TEXT_TYPE_OID.to_string())
        );

        assert_eq!(
            cache
                .proc_by_oid(TEXT_STARTS_WITH_PROC_OID)
                .map(|row| row.proname.as_str()),
            Some("starts_with")
        );
        assert_eq!(
            cache
                .operator_by_name_left_right("=", INT4_TYPE_OID, INT4_TYPE_OID)
                .map(|row| row.oprcode),
            Some(INT4_CMP_EQ_PROC_OID)
        );
        assert_eq!(
            cache
                .cast_by_source_target(INT4_TYPE_OID, OID_TYPE_OID)
                .map(|row| row.castmethod),
            Some('b')
        );
        assert!(
            cache
                .proc_rows_by_name("pg_catalog.no_such_proc")
                .is_empty()
        );
        assert_eq!(
            cache.aggregate_by_fnoid(6219).map(|row| row.aggfnoid),
            Some(6219)
        );
    }
}
