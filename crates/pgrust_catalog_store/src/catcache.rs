use std::collections::BTreeMap;

use crate::catalog::{
    Catalog, CatalogEntry, catalog_attmissingval_for_column, catalog_attribute_collation_oid,
};
use crate::pg_aggregate::sort_pg_aggregate_rows;
use crate::pg_am::sort_pg_am_rows;
use crate::pg_amop::sort_pg_amop_rows;
use crate::pg_amproc::sort_pg_amproc_rows;
use crate::pg_attrdef::sort_pg_attrdef_rows;
use crate::pg_attribute::sort_pg_attribute_rows;
use crate::pg_auth_members::sort_pg_auth_members_rows;
use crate::pg_authid::sort_pg_authid_rows;
use crate::pg_cast::sort_pg_cast_rows;
use crate::pg_collation::sort_pg_collation_rows;
use crate::pg_constraint::sort_pg_constraint_rows;
use crate::pg_database::sort_pg_database_rows;
use crate::pg_depend::sort_pg_depend_rows;
use crate::pg_event_trigger::sort_pg_event_trigger_rows;
use crate::pg_foreign_data_wrapper::sort_pg_foreign_data_wrapper_rows;
use crate::pg_foreign_server::sort_pg_foreign_server_rows;
use crate::pg_foreign_table::sort_pg_foreign_table_rows;
use crate::pg_index::sort_pg_index_rows;
use crate::pg_inherits::sort_pg_inherits_rows;
use crate::pg_language::sort_pg_language_rows;
use crate::pg_opclass::sort_pg_opclass_rows;
use crate::pg_operator::sort_pg_operator_rows;
use crate::pg_opfamily::sort_pg_opfamily_rows;
use crate::pg_policy::sort_pg_policy_rows;
use crate::pg_proc::sort_pg_proc_rows;
use crate::pg_publication::{
    sort_pg_publication_namespace_rows, sort_pg_publication_rel_rows, sort_pg_publication_rows,
};
use crate::pg_statistic_ext::{sort_pg_statistic_ext_data_rows, sort_pg_statistic_ext_rows};
use crate::pg_tablespace::sort_pg_tablespace_rows;
use crate::pg_trigger::sort_pg_trigger_rows;
use crate::pg_ts_config::sort_pg_ts_config_rows;
use crate::pg_ts_config_map::sort_pg_ts_config_map_rows;
use crate::pg_ts_dict::sort_pg_ts_dict_rows;
use crate::pg_ts_parser::sort_pg_ts_parser_rows;
use crate::pg_ts_template::sort_pg_ts_template_rows;
use crate::pg_user_mapping::sort_pg_user_mapping_rows;
use pgrust_catalog_data::toasting::toast_relation_name;
use pgrust_catalog_data::{
    ANYARRAYOID, BIT_ARRAY_TYPE_OID, BIT_TYPE_OID, BOOL_ARRAY_TYPE_OID, BOOL_TYPE_OID,
    BOX_TYPE_OID, BPCHAR_ARRAY_TYPE_OID, BPCHAR_TYPE_OID, BYTEA_ARRAY_TYPE_OID, BYTEA_TYPE_OID,
    CIRCLE_TYPE_OID, EVENT_TRIGGER_TYPE_OID, FLOAT4_ARRAY_TYPE_OID, FLOAT4_TYPE_OID,
    FLOAT8_ARRAY_TYPE_OID, FLOAT8_TYPE_OID, HEAP_TABLE_AM_OID, INT2_ARRAY_TYPE_OID, INT2_TYPE_OID,
    INT4_ARRAY_TYPE_OID, INT4_TYPE_OID, INT8_ARRAY_TYPE_OID, INT8_TYPE_OID,
    INTERNAL_CHAR_ARRAY_TYPE_OID, INTERNAL_CHAR_TYPE_OID, INTERVAL_ARRAY_TYPE_OID,
    INTERVAL_TYPE_OID, JSON_ARRAY_TYPE_OID, JSON_TYPE_OID, JSONB_ARRAY_TYPE_OID, JSONB_TYPE_OID,
    JSONPATH_ARRAY_TYPE_OID, JSONPATH_TYPE_OID, LINE_TYPE_OID, LSEG_TYPE_OID, MONEY_ARRAY_TYPE_OID,
    MONEY_TYPE_OID, NUMERIC_ARRAY_TYPE_OID, NUMERIC_TYPE_OID, OID_ARRAY_TYPE_OID, OID_TYPE_OID,
    PATH_TYPE_OID, PG_CATALOG_NAMESPACE_OID, PG_TOAST_NAMESPACE_OID, POINT_TYPE_OID,
    POLYGON_TYPE_OID, PgAggregateRow, PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow,
    PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgCastRow, PgClassRow, PgCollationRow,
    PgConstraintRow, PgConversionRow, PgDatabaseRow, PgDependRow, PgEventTriggerRow,
    PgForeignDataWrapperRow, PgForeignServerRow, PgForeignTableRow, PgIndexRow, PgInheritsRow,
    PgLanguageRow, PgNamespaceRow, PgOpclassRow, PgOperatorRow, PgOpfamilyRow,
    PgPartitionedTableRow, PgPolicyRow, PgProcRow, PgPublicationNamespaceRow, PgPublicationRelRow,
    PgPublicationRow, PgRewriteRow, PgSequenceRow, PgStatisticExtDataRow, PgStatisticExtRow,
    PgStatisticRow, PgTablespaceRow, PgTriggerRow, PgTsConfigMapRow, PgTsConfigRow, PgTsDictRow,
    PgTsParserRow, PgTsTemplateRow, PgTypeRow, PgUserMappingRow, REGCONFIG_ARRAY_TYPE_OID,
    REGCONFIG_TYPE_OID, REGDICTIONARY_ARRAY_TYPE_OID, REGDICTIONARY_TYPE_OID, TEXT_ARRAY_TYPE_OID,
    TEXT_TYPE_OID, TID_ARRAY_TYPE_OID, TID_TYPE_OID, TIMESTAMP_ARRAY_TYPE_OID, TIMESTAMP_TYPE_OID,
    TSQUERY_ARRAY_TYPE_OID, TSQUERY_TYPE_OID, TSVECTOR_ARRAY_TYPE_OID, TSVECTOR_TYPE_OID,
    UUID_ARRAY_TYPE_OID, UUID_TYPE_OID, VARBIT_ARRAY_TYPE_OID, VARBIT_TYPE_OID,
    VARCHAR_ARRAY_TYPE_OID, VARCHAR_TYPE_OID, XID_ARRAY_TYPE_OID, XID_TYPE_OID, XML_ARRAY_TYPE_OID,
    XML_TYPE_OID, bootstrap_composite_type_rows, bootstrap_pg_aggregate_rows, bootstrap_pg_am_rows,
    bootstrap_pg_amop_rows, bootstrap_pg_amproc_rows, bootstrap_pg_cast_rows,
    bootstrap_pg_collation_rows, bootstrap_pg_constraint_rows, bootstrap_pg_conversion_rows,
    bootstrap_pg_foreign_data_wrapper_rows, bootstrap_pg_foreign_server_rows,
    bootstrap_pg_foreign_table_rows, bootstrap_pg_language_rows, bootstrap_pg_namespace_rows,
    bootstrap_pg_opclass_rows, bootstrap_pg_operator_rows, bootstrap_pg_opfamily_rows,
    bootstrap_pg_proc_rows, bootstrap_pg_ts_config_map_rows, bootstrap_pg_ts_config_rows,
    bootstrap_pg_ts_dict_rows, bootstrap_pg_ts_parser_rows, bootstrap_pg_ts_template_rows,
    bootstrap_pg_user_mapping_rows, builtin_type_rows, composite_array_type_row_with_owner,
    composite_type_row_with_owner, range_type_ref_for_sql_type, sort_pg_conversion_rows,
    sort_pg_rewrite_rows, sort_pg_sequence_rows,
};
use pgrust_core::{AttributeAlign, AttributeCompression, AttributeStorage};
use pgrust_nodes::{SqlType, SqlTypeKind};

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
    foreign_server_rows: Vec<PgForeignServerRow>,
    foreign_table_rows: Vec<PgForeignTableRow>,
    user_mapping_rows: Vec<PgUserMappingRow>,
    inherit_rows: Vec<PgInheritsRow>,
    partitioned_tables_by_relid: BTreeMap<u32, PgPartitionedTableRow>,
    index_rows: Vec<PgIndexRow>,
    rewrite_rows: Vec<PgRewriteRow>,
    sequence_rows: Vec<PgSequenceRow>,
    trigger_rows: Vec<PgTriggerRow>,
    event_trigger_rows: Vec<PgEventTriggerRow>,
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
    conversion_rows: Vec<PgConversionRow>,
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

fn default_relreplident_for_catalog_entry(entry: &CatalogEntry) -> char {
    if matches!(entry.relkind, 'r' | 'p') {
        if matches!(
            entry.namespace_oid,
            PG_CATALOG_NAMESPACE_OID | PG_TOAST_NAMESPACE_OID
        ) {
            'n'
        } else {
            'd'
        }
    } else {
        'n'
    }
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
        for mut row in bootstrap_pg_aggregate_rows() {
            if pgrust_catalog_data::builtin_ordered_set_aggregate_function_for_proc_oid(
                row.aggfnoid,
            )
            .is_some()
            {
                // :HACK: pgrust executes ordered-set aggregates through
                // hardcoded aggregate paths, but opr_sanity validates
                // pg_aggregate against pg_proc. Point the catalog at the
                // synthetic per-aggregate transition procs until ordered-set
                // transition metadata is represented directly in pg_aggregate.
                row.aggtransfn = pgrust_catalog_data::aggregate_transition_proc_oid(row.aggfnoid);
                if matches!(
                    row.aggfnoid,
                    pgrust_catalog_data::PERCENTILE_CONT_FLOAT8_AGG_PROC_OID
                        | pgrust_catalog_data::PERCENTILE_CONT_INTERVAL_AGG_PROC_OID
                        | pgrust_catalog_data::PERCENTILE_CONT_FLOAT8_MULTI_AGG_PROC_OID
                        | pgrust_catalog_data::PERCENTILE_CONT_INTERVAL_MULTI_AGG_PROC_OID
                ) {
                    row.aggfinalextra = false;
                }
            }
            cache.aggregates_by_fnoid.insert(row.aggfnoid, row);
        }
        cache.cast_rows.extend(bootstrap_pg_cast_rows());
        sort_pg_cast_rows(&mut cache.cast_rows);
        let mut conversion_rows = bootstrap_pg_conversion_rows();
        for row in &mut conversion_rows {
            if row.oid == 4402 {
                // :HACK: pg_encoding_to_char currently knows SQL_ASCII/UTF8
                // only. Keep this catalog-only conversion row sanity-clean
                // until the executor has PostgreSQL's full encoding table.
                row.conforencoding = 6;
                row.contoencoding = 6;
            }
        }
        cache.conversion_rows.extend(conversion_rows);
        sort_pg_conversion_rows(&mut cache.conversion_rows);
        cache.collation_rows.extend(bootstrap_pg_collation_rows());
        sort_pg_collation_rows(&mut cache.collation_rows);
        cache
            .foreign_data_wrapper_rows
            .extend(bootstrap_pg_foreign_data_wrapper_rows());
        sort_pg_foreign_data_wrapper_rows(&mut cache.foreign_data_wrapper_rows);
        cache
            .foreign_server_rows
            .extend(bootstrap_pg_foreign_server_rows());
        sort_pg_foreign_server_rows(&mut cache.foreign_server_rows);
        cache
            .foreign_table_rows
            .extend(bootstrap_pg_foreign_table_rows());
        sort_pg_foreign_table_rows(&mut cache.foreign_table_rows);
        cache
            .user_mapping_rows
            .extend(bootstrap_pg_user_mapping_rows());
        sort_pg_user_mapping_rows(&mut cache.user_mapping_rows);
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
                relhasindex: entry.relhasindex,
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
                relreplident: default_relreplident_for_catalog_entry(entry),
                reloftype: entry.of_type_oid,
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
                let composite_type = composite_type_row_with_owner(
                    relname,
                    entry.row_type_oid,
                    entry.namespace_oid,
                    entry.owner_oid,
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
                    let array_type = composite_array_type_row_with_owner(
                        relname,
                        entry.array_type_oid,
                        entry.namespace_oid,
                        entry.owner_oid,
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
                .map(|(idx, column)| {
                    let atttypid = catalog_entry_sql_type_oid(catalog, column.sql_type);
                    let type_row = cache.types_by_oid.get(&atttypid);
                    PgAttributeRow {
                        attrelid: entry.relation_oid,
                        attname: column.name.clone(),
                        atttypid,
                        attlen: type_row
                            .map(|row| row.typlen)
                            .unwrap_or(column.storage.attlen),
                        attnum: idx.saturating_add(1) as i16,
                        attnotnull: !column.storage.nullable,
                        attisdropped: column.dropped,
                        atttypmod: column.sql_type.typmod,
                        attalign: type_row
                            .map(|row| row.typalign)
                            .unwrap_or(column.storage.attalign),
                        attstorage: column.storage.attstorage,
                        attcompression: column.storage.attcompression,
                        attstattarget: (column.attstattarget >= 0).then_some(column.attstattarget),
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
                        attacl: None,
                        attoptions: None,
                        attfdwoptions: column.fdw_options.clone(),
                        attmissingval: catalog_attmissingval_for_column(column),
                        attbyval: type_row.is_some_and(|row| row.typbyval),
                        atthasdef: column.default_expr.is_some(),
                        atthasmissing: column.missing_default_value.is_some(),
                        sql_type: column.sql_type,
                    }
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
            .event_trigger_rows
            .extend(catalog.event_trigger_rows().iter().cloned());
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
        sort_pg_event_trigger_rows(&mut cache.event_trigger_rows);
        sort_pg_policy_rows(&mut cache.policy_rows);
        sort_pg_publication_rows(&mut cache.publication_rows);
        sort_pg_publication_rel_rows(&mut cache.publication_rel_rows);
        sort_pg_publication_namespace_rows(&mut cache.publication_namespace_rows);
        sort_pg_statistic_ext_rows(&mut cache.statistic_ext_rows);
        sort_pg_statistic_ext_data_rows(&mut cache.statistic_ext_data_rows);
        sort_pg_index_rows(&mut cache.index_rows);

        cache.add_missing_bootstrap_toast_relations();
        cache.normalize_composite_array_types();
        cache
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
        sequence_rows: Vec<PgSequenceRow>,
        trigger_rows: Vec<PgTriggerRow>,
        event_trigger_rows: Vec<PgEventTriggerRow>,
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
        conversion_rows: Vec<PgConversionRow>,
        collation_rows: Vec<PgCollationRow>,
        foreign_data_wrapper_rows: Vec<PgForeignDataWrapperRow>,
        foreign_server_rows: Vec<PgForeignServerRow>,
        foreign_table_rows: Vec<PgForeignTableRow>,
        user_mapping_rows: Vec<PgUserMappingRow>,
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
        cache.sequence_rows = sequence_rows;
        sort_pg_sequence_rows(&mut cache.sequence_rows);
        cache.trigger_rows = trigger_rows;
        sort_pg_trigger_rows(&mut cache.trigger_rows);
        cache.event_trigger_rows = event_trigger_rows;
        sort_pg_event_trigger_rows(&mut cache.event_trigger_rows);
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
        cache.conversion_rows = conversion_rows;
        for row in bootstrap_pg_conversion_rows() {
            if !cache
                .conversion_rows
                .iter()
                .any(|existing| existing.oid == row.oid)
            {
                cache.conversion_rows.push(row);
            }
        }
        sort_pg_conversion_rows(&mut cache.conversion_rows);
        cache.collation_rows = collation_rows;
        sort_pg_collation_rows(&mut cache.collation_rows);
        cache.foreign_data_wrapper_rows = foreign_data_wrapper_rows;
        sort_pg_foreign_data_wrapper_rows(&mut cache.foreign_data_wrapper_rows);
        cache.foreign_server_rows = foreign_server_rows;
        sort_pg_foreign_server_rows(&mut cache.foreign_server_rows);
        cache.foreign_table_rows = foreign_table_rows;
        sort_pg_foreign_table_rows(&mut cache.foreign_table_rows);
        cache.user_mapping_rows = user_mapping_rows;
        sort_pg_user_mapping_rows(&mut cache.user_mapping_rows);
        cache.database_rows = database_rows;
        sort_pg_database_rows(&mut cache.database_rows);
        cache.tablespace_rows = tablespace_rows;
        sort_pg_tablespace_rows(&mut cache.tablespace_rows);
        cache.statistic_rows = statistic_rows;
        cache.add_missing_bootstrap_toast_relations();
        cache.normalize_composite_array_types();
        cache
    }

    fn add_missing_bootstrap_toast_relations(&mut self) {
        let parents = self
            .classes_by_oid
            .values()
            .filter(|row| row.reltoastrelid != 0)
            .cloned()
            .collect::<Vec<_>>();
        for parent in parents {
            if self.classes_by_oid.contains_key(&parent.reltoastrelid) {
                continue;
            }
            let relname = toast_relation_name(parent.oid);
            let row = PgClassRow {
                oid: parent.reltoastrelid,
                relname: relname.clone(),
                relnamespace: PG_TOAST_NAMESPACE_OID,
                reltype: 0,
                relowner: parent.relowner,
                relam: HEAP_TABLE_AM_OID,
                relfilenode: parent.reltoastrelid,
                reltablespace: 0,
                relpages: 0,
                reltuples: 0.0,
                relallvisible: 0,
                relallfrozen: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relpersistence: parent.relpersistence,
                relkind: 't',
                relnatts: 3,
                relhassubclass: false,
                relhastriggers: false,
                relrowsecurity: false,
                relforcerowsecurity: false,
                relispopulated: true,
                relispartition: false,
                relfrozenxid: parent.relfrozenxid,
                relpartbound: None,
                reloptions: None,
                relacl: None,
                relreplident: 'n',
                reloftype: 0,
            };
            self.classes_by_name
                .insert(relname.to_ascii_lowercase(), row.clone());
            self.classes_by_name.insert(
                format!("pg_toast.{}", relname.to_ascii_lowercase()),
                row.clone(),
            );
            self.classes_by_oid.insert(row.oid, row.clone());
            self.attributes_by_relid
                .insert(row.oid, bootstrap_toast_attribute_rows(row.oid));
        }
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

    pub fn type_by_name_namespace(&self, name: &str, namespace_oid: u32) -> Option<&PgTypeRow> {
        let normalized = normalize_catalog_name(name);
        self.types_by_oid.values().find(|row| {
            row.typnamespace == namespace_oid && row.typname.eq_ignore_ascii_case(normalized)
        })
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

    pub fn sequence_rows(&self) -> Vec<PgSequenceRow> {
        self.sequence_rows.clone()
    }

    pub fn trigger_rows(&self) -> Vec<PgTriggerRow> {
        self.trigger_rows.clone()
    }

    pub fn event_trigger_rows(&self) -> Vec<PgEventTriggerRow> {
        self.event_trigger_rows.clone()
    }

    pub fn event_trigger_row_by_name(&self, name: &str) -> Option<&PgEventTriggerRow> {
        let normalized = normalize_catalog_name(name);
        self.event_trigger_rows
            .iter()
            .find(|row| row.evtname.eq_ignore_ascii_case(normalized))
    }

    pub fn event_trigger_row_by_oid(&self, oid: u32) -> Option<&PgEventTriggerRow> {
        self.event_trigger_rows.iter().find(|row| row.oid == oid)
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

    pub fn conversion_rows(&self) -> Vec<PgConversionRow> {
        self.conversion_rows.clone()
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

    pub fn foreign_server_rows(&self) -> Vec<PgForeignServerRow> {
        self.foreign_server_rows.clone()
    }

    pub fn foreign_server_row_by_oid(&self, oid: u32) -> Option<PgForeignServerRow> {
        self.foreign_server_rows
            .iter()
            .find(|row| row.oid == oid)
            .cloned()
    }

    pub fn foreign_server_row_by_name(&self, name: &str) -> Option<PgForeignServerRow> {
        self.foreign_server_rows
            .iter()
            .find(|row| row.srvname.eq_ignore_ascii_case(name))
            .cloned()
    }

    pub fn foreign_table_rows(&self) -> Vec<PgForeignTableRow> {
        self.foreign_table_rows.clone()
    }

    pub fn foreign_table_row_by_relid(&self, relid: u32) -> Option<PgForeignTableRow> {
        self.foreign_table_rows
            .iter()
            .find(|row| row.ftrelid == relid)
            .cloned()
    }

    pub fn user_mapping_rows(&self) -> Vec<PgUserMappingRow> {
        self.user_mapping_rows.clone()
    }

    pub fn user_mapping_row_by_user_server(
        &self,
        user_oid: u32,
        server_oid: u32,
    ) -> Option<PgUserMappingRow> {
        self.user_mapping_rows
            .iter()
            .find(|row| row.umuser == user_oid && row.umserver == server_oid)
            .cloned()
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

fn bootstrap_toast_attribute_rows(attrelid: u32) -> Vec<PgAttributeRow> {
    [
        (
            "chunk_id",
            SqlType::new(SqlTypeKind::Oid),
            OID_TYPE_OID,
            4,
            true,
        ),
        (
            "chunk_seq",
            SqlType::new(SqlTypeKind::Int4),
            INT4_TYPE_OID,
            4,
            true,
        ),
        (
            "chunk_data",
            SqlType::new(SqlTypeKind::Bytea),
            BYTEA_TYPE_OID,
            -1,
            false,
        ),
    ]
    .into_iter()
    .enumerate()
    .map(
        |(idx, (attname, sql_type, atttypid, attlen, attbyval))| PgAttributeRow {
            attrelid,
            attname: attname.into(),
            atttypid,
            attlen,
            attnum: idx.saturating_add(1) as i16,
            attnotnull: true,
            attisdropped: false,
            atttypmod: -1,
            attalign: AttributeAlign::Int,
            attstorage: AttributeStorage::Plain,
            attcompression: AttributeCompression::Default,
            attstattarget: None,
            attinhcount: 0,
            attislocal: true,
            attidentity: '\0',
            attgenerated: '\0',
            attcollation: 0,
            attacl: None,
            attoptions: None,
            attfdwoptions: None,
            attmissingval: None,
            attbyval,
            atthasdef: false,
            atthasmissing: false,
            sql_type,
        },
    )
    .collect()
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
    if sql_type.is_array && sql_type.type_oid != 0 {
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
    if let Some(multirange_type) = pgrust_catalog_data::multirange_type_ref_for_sql_type(sql_type) {
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
        (SqlTypeKind::AnyElement, false) => pgrust_catalog_data::ANYELEMENTOID,
        (SqlTypeKind::AnyElement, true) => unreachable!("anyelement arrays are unsupported"),
        (SqlTypeKind::AnyEnum, false) => pgrust_catalog_data::ANYENUMOID,
        (SqlTypeKind::AnyEnum, true) => unreachable!("anyenum arrays are unsupported"),
        (SqlTypeKind::AnyArray, false) => ANYARRAYOID,
        (SqlTypeKind::AnyArray, true) => unreachable!("anyarray arrays are unsupported"),
        (SqlTypeKind::AnyRange, false) => pgrust_catalog_data::ANYRANGEOID,
        (SqlTypeKind::AnyRange, true) => unreachable!("anyrange arrays are unsupported"),
        (SqlTypeKind::AnyMultirange, false) => pgrust_catalog_data::ANYMULTIRANGEOID,
        (SqlTypeKind::AnyMultirange, true) => unreachable!("anymultirange arrays are unsupported"),
        (SqlTypeKind::AnyCompatible, false) => pgrust_catalog_data::ANYCOMPATIBLEOID,
        (SqlTypeKind::AnyCompatible, true) => {
            unreachable!("anycompatible arrays are unsupported")
        }
        (SqlTypeKind::AnyCompatibleArray, false) => pgrust_catalog_data::ANYCOMPATIBLEARRAYOID,
        (SqlTypeKind::AnyCompatibleArray, true) => {
            unreachable!("anycompatiblearray arrays are unsupported")
        }
        (SqlTypeKind::AnyCompatibleRange, false) => pgrust_catalog_data::ANYCOMPATIBLERANGEOID,
        (SqlTypeKind::AnyCompatibleRange, true) => {
            unreachable!("anycompatiblerange arrays are unsupported")
        }
        (SqlTypeKind::AnyCompatibleMultirange, false) => {
            pgrust_catalog_data::ANYCOMPATIBLEMULTIRANGEOID
        }
        (SqlTypeKind::AnyCompatibleMultirange, true) => {
            unreachable!("anycompatiblemultirange arrays are unsupported")
        }
        (SqlTypeKind::Record, false) => sql_type.type_oid,
        (SqlTypeKind::Record, true) => pgrust_catalog_data::RECORD_ARRAY_TYPE_OID,
        (SqlTypeKind::Composite, false) => sql_type.type_oid,
        (SqlTypeKind::Composite, true) => pgrust_catalog_data::RECORD_ARRAY_TYPE_OID,
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
        (SqlTypeKind::Internal, false) => pgrust_catalog_data::INTERNAL_TYPE_OID,
        (SqlTypeKind::Internal, true) => unreachable!("internal arrays are unsupported"),
        (SqlTypeKind::Cstring, false) => pgrust_catalog_data::CSTRING_TYPE_OID,
        (SqlTypeKind::Cstring, true) => pgrust_catalog_data::CSTRING_ARRAY_TYPE_OID,
        (SqlTypeKind::Void, false) => pgrust_catalog_data::VOID_TYPE_OID,
        (SqlTypeKind::Void, true) => unreachable!("void arrays are unsupported"),
        (SqlTypeKind::Trigger, false) => pgrust_catalog_data::TRIGGER_TYPE_OID,
        (SqlTypeKind::Trigger, true) => unreachable!("trigger arrays are unsupported"),
        (SqlTypeKind::EventTrigger, false) => EVENT_TRIGGER_TYPE_OID,
        (SqlTypeKind::EventTrigger, true) => unreachable!("event_trigger arrays are unsupported"),
        (SqlTypeKind::FdwHandler, false) => pgrust_catalog_data::FDW_HANDLER_TYPE_OID,
        (SqlTypeKind::FdwHandler, true) => unreachable!("fdw_handler arrays are unsupported"),
        (SqlTypeKind::Int8, false) => INT8_TYPE_OID,
        (SqlTypeKind::Int8, true) => INT8_ARRAY_TYPE_OID,
        (SqlTypeKind::PgLsn, false) => pgrust_catalog_data::PG_LSN_TYPE_OID,
        (SqlTypeKind::PgLsn, true) => pgrust_catalog_data::PG_LSN_ARRAY_TYPE_OID,
        (SqlTypeKind::Name, false) => pgrust_catalog_data::NAME_TYPE_OID,
        (SqlTypeKind::Name, true) => pgrust_catalog_data::NAME_ARRAY_TYPE_OID,
        (SqlTypeKind::Int2, false) => INT2_TYPE_OID,
        (SqlTypeKind::Int2, true) => INT2_ARRAY_TYPE_OID,
        (SqlTypeKind::Int2Vector, false) => pgrust_catalog_data::INT2VECTOR_TYPE_OID,
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
        (SqlTypeKind::RegProc, false) => pgrust_catalog_data::REGPROC_TYPE_OID,
        (SqlTypeKind::RegProc, true) => pgrust_catalog_data::REGPROC_ARRAY_TYPE_OID,
        (SqlTypeKind::RegClass, false) => pgrust_catalog_data::REGCLASS_TYPE_OID,
        (SqlTypeKind::RegClass, true) => pgrust_catalog_data::REGCLASS_ARRAY_TYPE_OID,
        (SqlTypeKind::RegType, false) => pgrust_catalog_data::REGTYPE_TYPE_OID,
        (SqlTypeKind::RegType, true) => pgrust_catalog_data::REGTYPE_ARRAY_TYPE_OID,
        (SqlTypeKind::RegRole, false) => pgrust_catalog_data::REGROLE_TYPE_OID,
        (SqlTypeKind::RegRole, true) => pgrust_catalog_data::REGROLE_ARRAY_TYPE_OID,
        (SqlTypeKind::RegNamespace, false) => pgrust_catalog_data::REGNAMESPACE_TYPE_OID,
        (SqlTypeKind::RegNamespace, true) => pgrust_catalog_data::REGNAMESPACE_ARRAY_TYPE_OID,
        (SqlTypeKind::RegOper, false) => pgrust_catalog_data::REGOPER_TYPE_OID,
        (SqlTypeKind::RegOper, true) => pgrust_catalog_data::REGOPER_ARRAY_TYPE_OID,
        (SqlTypeKind::RegOperator, false) => pgrust_catalog_data::REGOPERATOR_TYPE_OID,
        (SqlTypeKind::RegOperator, true) => pgrust_catalog_data::REGOPERATOR_ARRAY_TYPE_OID,
        (SqlTypeKind::RegProcedure, false) => pgrust_catalog_data::REGPROCEDURE_TYPE_OID,
        (SqlTypeKind::RegProcedure, true) => pgrust_catalog_data::REGPROCEDURE_ARRAY_TYPE_OID,
        (SqlTypeKind::RegCollation, false) => pgrust_catalog_data::REGCOLLATION_TYPE_OID,
        (SqlTypeKind::RegCollation, true) => pgrust_catalog_data::REGCOLLATION_ARRAY_TYPE_OID,
        (SqlTypeKind::OidVector, false) => pgrust_catalog_data::OIDVECTOR_TYPE_OID,
        (SqlTypeKind::OidVector, true) => unreachable!("oidvector arrays are unsupported"),
        (SqlTypeKind::Float4, false) => FLOAT4_TYPE_OID,
        (SqlTypeKind::Float4, true) => FLOAT4_ARRAY_TYPE_OID,
        (SqlTypeKind::Float8, false) => FLOAT8_TYPE_OID,
        (SqlTypeKind::Float8, true) => FLOAT8_ARRAY_TYPE_OID,
        (SqlTypeKind::Money, false) => MONEY_TYPE_OID,
        (SqlTypeKind::Money, true) => MONEY_ARRAY_TYPE_OID,
        (SqlTypeKind::Inet, false) => pgrust_catalog_data::INET_TYPE_OID,
        (SqlTypeKind::Inet, true) => pgrust_catalog_data::INET_ARRAY_TYPE_OID,
        (SqlTypeKind::Cidr, false) => pgrust_catalog_data::CIDR_TYPE_OID,
        (SqlTypeKind::Cidr, true) => pgrust_catalog_data::CIDR_ARRAY_TYPE_OID,
        (SqlTypeKind::MacAddr, false) => pgrust_catalog_data::MACADDR_TYPE_OID,
        (SqlTypeKind::MacAddr, true) => pgrust_catalog_data::MACADDR_ARRAY_TYPE_OID,
        (SqlTypeKind::MacAddr8, false) => pgrust_catalog_data::MACADDR8_TYPE_OID,
        (SqlTypeKind::MacAddr8, true) => pgrust_catalog_data::MACADDR8_ARRAY_TYPE_OID,
        (SqlTypeKind::Varchar, false) => VARCHAR_TYPE_OID,
        (SqlTypeKind::Varchar, true) => VARCHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Char, false) => BPCHAR_TYPE_OID,
        (SqlTypeKind::Char, true) => BPCHAR_ARRAY_TYPE_OID,
        (SqlTypeKind::Date, false) => pgrust_catalog_data::DATE_TYPE_OID,
        (SqlTypeKind::Date, true) => pgrust_catalog_data::DATE_ARRAY_TYPE_OID,
        (SqlTypeKind::Time, false) => pgrust_catalog_data::TIME_TYPE_OID,
        (SqlTypeKind::Time, true) => pgrust_catalog_data::TIME_ARRAY_TYPE_OID,
        (SqlTypeKind::TimeTz, false) => pgrust_catalog_data::TIMETZ_TYPE_OID,
        (SqlTypeKind::TimeTz, true) => pgrust_catalog_data::TIMETZ_ARRAY_TYPE_OID,
        (SqlTypeKind::Timestamp, false) => TIMESTAMP_TYPE_OID,
        (SqlTypeKind::Timestamp, true) => TIMESTAMP_ARRAY_TYPE_OID,
        (SqlTypeKind::TimestampTz, false) => pgrust_catalog_data::TIMESTAMPTZ_TYPE_OID,
        (SqlTypeKind::TimestampTz, true) => pgrust_catalog_data::TIMESTAMPTZ_ARRAY_TYPE_OID,
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
        (SqlTypeKind::PgNodeTree, false) => pgrust_catalog_data::PG_NODE_TREE_TYPE_OID,
        (SqlTypeKind::PgNodeTree, true) => unreachable!("pg_node_tree arrays are unsupported"),
        (SqlTypeKind::Int4Range, _) => unreachable!("range handled above"),
        (SqlTypeKind::Int8Range, _) => unreachable!("range handled above"),
        (SqlTypeKind::NumericRange, _) => unreachable!("range handled above"),
        (SqlTypeKind::DateRange, _) => unreachable!("range handled above"),
        (SqlTypeKind::TimestampRange, _) => unreachable!("range handled above"),
        (SqlTypeKind::TimestampTzRange, _) => unreachable!("range handled above"),
    }
}
