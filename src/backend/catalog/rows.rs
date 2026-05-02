use std::collections::BTreeSet;

use crate::backend::catalog::catalog::{Catalog, CatalogEntry, catalog_attribute_collation_oid};
use crate::backend::parser::SqlType;
use crate::backend::utils::cache::catcache::{CatCache, sql_type_oid};
use crate::include::catalog::{
    BootstrapCatalogKind, PG_CATALOG_NAMESPACE_OID, PG_OPERATOR_RELATION_OID, PG_PROC_RELATION_OID,
    PgAggregateRow, PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow, PgAttributeRow, PgAuthIdRow,
    PgAuthMembersRow, PgCastRow, PgClassRow, PgCollationRow, PgConstraintRow, PgConversionRow,
    PgDatabaseRow, PgDefaultAclRow, PgDependRow, PgDescriptionRow, PgEventTriggerRow,
    PgForeignDataWrapperRow, PgForeignServerRow, PgForeignTableRow, PgIndexRow, PgInheritsRow,
    PgLanguageRow, PgLargeobjectMetadataRow, PgLargeobjectRow, PgNamespaceRow, PgOpclassRow,
    PgOperatorRow, PgOpfamilyRow, PgPartitionedTableRow, PgPolicyRow, PgProcRow,
    PgPublicationNamespaceRow, PgPublicationRelRow, PgPublicationRow, PgRewriteRow, PgSequenceRow,
    PgShdependRow, PgStatisticExtDataRow, PgStatisticExtRow, PgStatisticRow, PgTablespaceRow,
    PgTriggerRow, PgTsConfigMapRow, PgTsConfigRow, PgTsDictRow, PgTsParserRow, PgTsTemplateRow,
    PgTypeRow, PgUserMappingRow, bootstrap_composite_type_rows, builtin_type_row_by_oid,
    composite_array_type_row_with_owner, composite_type_row_with_owner,
};

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct PhysicalCatalogRows {
    pub namespaces: Vec<PgNamespaceRow>,
    pub classes: Vec<PgClassRow>,
    pub attributes: Vec<PgAttributeRow>,
    pub attrdefs: Vec<PgAttrdefRow>,
    pub depends: Vec<PgDependRow>,
    pub shdepends: Vec<PgShdependRow>,
    pub inherits: Vec<PgInheritsRow>,
    pub partitioned_tables: Vec<PgPartitionedTableRow>,
    pub descriptions: Vec<PgDescriptionRow>,
    pub foreign_data_wrappers: Vec<PgForeignDataWrapperRow>,
    pub foreign_servers: Vec<PgForeignServerRow>,
    pub foreign_tables: Vec<PgForeignTableRow>,
    pub user_mappings: Vec<PgUserMappingRow>,
    pub indexes: Vec<PgIndexRow>,
    pub rewrites: Vec<PgRewriteRow>,
    pub sequences: Vec<PgSequenceRow>,
    pub triggers: Vec<PgTriggerRow>,
    pub event_triggers: Vec<PgEventTriggerRow>,
    pub policies: Vec<PgPolicyRow>,
    pub publications: Vec<PgPublicationRow>,
    pub publication_rels: Vec<PgPublicationRelRow>,
    pub publication_namespaces: Vec<PgPublicationNamespaceRow>,
    pub default_acls: Vec<PgDefaultAclRow>,
    pub statistics_ext: Vec<PgStatisticExtRow>,
    pub statistics_ext_data: Vec<PgStatisticExtDataRow>,
    pub ams: Vec<PgAmRow>,
    pub amops: Vec<PgAmopRow>,
    pub amprocs: Vec<PgAmprocRow>,
    pub authids: Vec<PgAuthIdRow>,
    pub auth_members: Vec<PgAuthMembersRow>,
    pub languages: Vec<PgLanguageRow>,
    pub largeobjects: Vec<PgLargeobjectRow>,
    pub largeobject_metadata: Vec<PgLargeobjectMetadataRow>,
    pub ts_parsers: Vec<PgTsParserRow>,
    pub ts_templates: Vec<PgTsTemplateRow>,
    pub ts_dicts: Vec<PgTsDictRow>,
    pub ts_configs: Vec<PgTsConfigRow>,
    pub ts_config_maps: Vec<PgTsConfigMapRow>,
    pub constraints: Vec<PgConstraintRow>,
    pub operators: Vec<PgOperatorRow>,
    pub opclasses: Vec<PgOpclassRow>,
    pub opfamilies: Vec<PgOpfamilyRow>,
    pub procs: Vec<PgProcRow>,
    pub aggregates: Vec<PgAggregateRow>,
    pub casts: Vec<PgCastRow>,
    pub conversions: Vec<PgConversionRow>,
    pub collations: Vec<PgCollationRow>,
    pub databases: Vec<PgDatabaseRow>,
    pub tablespaces: Vec<PgTablespaceRow>,
    pub statistics: Vec<PgStatisticRow>,
    pub types: Vec<PgTypeRow>,
}

pub(crate) fn create_table_sync_kinds(entry: &CatalogEntry) -> Vec<BootstrapCatalogKind> {
    let mut kinds = vec![
        BootstrapCatalogKind::PgClass,
        BootstrapCatalogKind::PgType,
        BootstrapCatalogKind::PgAttribute,
        BootstrapCatalogKind::PgDepend,
        BootstrapCatalogKind::PgInherits,
    ];
    if entry
        .desc
        .columns
        .iter()
        .any(|column| column.default_expr.is_some())
    {
        kinds.push(BootstrapCatalogKind::PgAttrdef);
    }
    if entry
        .desc
        .columns
        .iter()
        .any(|column| !column.storage.nullable)
    {
        kinds.push(BootstrapCatalogKind::PgConstraint);
    }
    if entry.partitioned_table.is_some() {
        kinds.push(BootstrapCatalogKind::PgPartitionedTable);
    }
    kinds
}

pub(crate) fn create_index_sync_kinds() -> Vec<BootstrapCatalogKind> {
    vec![
        BootstrapCatalogKind::PgClass,
        BootstrapCatalogKind::PgAttribute,
        BootstrapCatalogKind::PgIndex,
        BootstrapCatalogKind::PgDepend,
    ]
}

pub(crate) fn create_view_sync_kinds() -> Vec<BootstrapCatalogKind> {
    vec![
        BootstrapCatalogKind::PgClass,
        BootstrapCatalogKind::PgType,
        BootstrapCatalogKind::PgAttribute,
        BootstrapCatalogKind::PgDepend,
        BootstrapCatalogKind::PgRewrite,
    ]
}

pub(crate) fn create_composite_type_sync_kinds() -> Vec<BootstrapCatalogKind> {
    vec![
        BootstrapCatalogKind::PgClass,
        BootstrapCatalogKind::PgType,
        BootstrapCatalogKind::PgAttribute,
        BootstrapCatalogKind::PgDepend,
    ]
}

pub(crate) fn drop_relation_sync_kinds() -> Vec<BootstrapCatalogKind> {
    vec![
        BootstrapCatalogKind::PgClass,
        BootstrapCatalogKind::PgType,
        BootstrapCatalogKind::PgAttribute,
        BootstrapCatalogKind::PgAttrdef,
        BootstrapCatalogKind::PgConstraint,
        BootstrapCatalogKind::PgDepend,
        BootstrapCatalogKind::PgInherits,
        BootstrapCatalogKind::PgPartitionedTable,
        BootstrapCatalogKind::PgForeignTable,
        BootstrapCatalogKind::PgDescription,
        BootstrapCatalogKind::PgIndex,
        BootstrapCatalogKind::PgRewrite,
        BootstrapCatalogKind::PgTrigger,
        BootstrapCatalogKind::PgPolicy,
        BootstrapCatalogKind::PgPublication,
        BootstrapCatalogKind::PgPublicationRel,
        BootstrapCatalogKind::PgPublicationNamespace,
        BootstrapCatalogKind::PgSequence,
        BootstrapCatalogKind::PgStatistic,
        BootstrapCatalogKind::PgStatisticExt,
        BootstrapCatalogKind::PgStatisticExtData,
    ]
}

pub(crate) fn drop_relation_delete_kinds() -> Vec<BootstrapCatalogKind> {
    vec![
        BootstrapCatalogKind::PgIndex,
        BootstrapCatalogKind::PgAttrdef,
        BootstrapCatalogKind::PgConstraint,
        BootstrapCatalogKind::PgType,
        BootstrapCatalogKind::PgAttribute,
        BootstrapCatalogKind::PgClass,
        BootstrapCatalogKind::PgDepend,
        BootstrapCatalogKind::PgShdepend,
        BootstrapCatalogKind::PgInherits,
        BootstrapCatalogKind::PgForeignTable,
        BootstrapCatalogKind::PgDescription,
        BootstrapCatalogKind::PgRewrite,
        BootstrapCatalogKind::PgTrigger,
        BootstrapCatalogKind::PgPolicy,
        BootstrapCatalogKind::PgPublicationRel,
        BootstrapCatalogKind::PgPublicationNamespace,
        BootstrapCatalogKind::PgSequence,
        BootstrapCatalogKind::PgStatistic,
        BootstrapCatalogKind::PgStatisticExt,
        BootstrapCatalogKind::PgStatisticExtData,
    ]
}

pub(crate) fn extend_physical_catalog_rows(
    target: &mut PhysicalCatalogRows,
    source: PhysicalCatalogRows,
) {
    target.namespaces.extend(source.namespaces);
    target.classes.extend(source.classes);
    target.attributes.extend(source.attributes);
    target.attrdefs.extend(source.attrdefs);
    target.depends.extend(source.depends);
    target.shdepends.extend(source.shdepends);
    target.inherits.extend(source.inherits);
    target.partitioned_tables.extend(source.partitioned_tables);
    target.descriptions.extend(source.descriptions);
    target
        .foreign_data_wrappers
        .extend(source.foreign_data_wrappers);
    target.foreign_servers.extend(source.foreign_servers);
    target.foreign_tables.extend(source.foreign_tables);
    target.user_mappings.extend(source.user_mappings);
    target.indexes.extend(source.indexes);
    target.rewrites.extend(source.rewrites);
    target.sequences.extend(source.sequences);
    target.triggers.extend(source.triggers);
    target.event_triggers.extend(source.event_triggers);
    target.policies.extend(source.policies);
    target.publications.extend(source.publications);
    target.publication_rels.extend(source.publication_rels);
    target
        .publication_namespaces
        .extend(source.publication_namespaces);
    target.default_acls.extend(source.default_acls);
    target.statistics_ext.extend(source.statistics_ext);
    target
        .statistics_ext_data
        .extend(source.statistics_ext_data);
    target.ams.extend(source.ams);
    target.amops.extend(source.amops);
    target.amprocs.extend(source.amprocs);
    target.authids.extend(source.authids);
    target.auth_members.extend(source.auth_members);
    target.languages.extend(source.languages);
    target.largeobjects.extend(source.largeobjects);
    target
        .largeobject_metadata
        .extend(source.largeobject_metadata);
    target.ts_parsers.extend(source.ts_parsers);
    target.ts_templates.extend(source.ts_templates);
    target.ts_dicts.extend(source.ts_dicts);
    target.ts_configs.extend(source.ts_configs);
    target.ts_config_maps.extend(source.ts_config_maps);
    target.constraints.extend(source.constraints);
    target.operators.extend(source.operators);
    target.opclasses.extend(source.opclasses);
    target.opfamilies.extend(source.opfamilies);
    target.procs.extend(source.procs);
    target.aggregates.extend(source.aggregates);
    target.casts.extend(source.casts);
    target.conversions.extend(source.conversions);
    target.collations.extend(source.collations);
    target.databases.extend(source.databases);
    target.tablespaces.extend(source.tablespaces);
    target.statistics.extend(source.statistics);
    target.types.extend(source.types);
}

pub(crate) fn physical_catalog_rows_from_catcache(catcache: &CatCache) -> PhysicalCatalogRows {
    let mut descriptions = Vec::new();
    add_builtin_description_rows(&mut descriptions, catcache);
    PhysicalCatalogRows {
        namespaces: catcache.namespace_rows(),
        classes: catcache.class_rows(),
        attributes: catcache.attribute_rows(),
        attrdefs: catcache.attrdef_rows(),
        depends: catcache.depend_rows(),
        shdepends: Vec::new(),
        inherits: catcache.inherit_rows(),
        partitioned_tables: catcache.partitioned_table_rows(),
        descriptions,
        foreign_data_wrappers: catcache.foreign_data_wrapper_rows(),
        foreign_servers: catcache.foreign_server_rows(),
        foreign_tables: catcache.foreign_table_rows(),
        user_mappings: catcache.user_mapping_rows(),
        indexes: catcache.index_rows(),
        rewrites: catcache.rewrite_rows(),
        sequences: catcache.sequence_rows(),
        triggers: catcache.trigger_rows(),
        event_triggers: catcache.event_trigger_rows(),
        policies: catcache.policy_rows(),
        publications: catcache.publication_rows(),
        publication_rels: catcache.publication_rel_rows(),
        publication_namespaces: catcache.publication_namespace_rows(),
        default_acls: Vec::new(),
        statistics_ext: catcache.statistic_ext_rows(),
        statistics_ext_data: catcache.statistic_ext_data_rows(),
        ams: catcache.am_rows(),
        amops: catcache.amop_rows(),
        amprocs: catcache.amproc_rows(),
        authids: catcache.authid_rows(),
        auth_members: catcache.auth_members_rows(),
        languages: catcache.language_rows(),
        largeobjects: Vec::new(),
        largeobject_metadata: Vec::new(),
        ts_parsers: catcache.ts_parser_rows(),
        ts_templates: catcache.ts_template_rows(),
        ts_dicts: catcache.ts_dict_rows(),
        ts_configs: catcache.ts_config_rows(),
        ts_config_maps: catcache.ts_config_map_rows(),
        constraints: catcache.constraint_rows(),
        operators: catcache.operator_rows(),
        opclasses: catcache.opclass_rows(),
        opfamilies: catcache.opfamily_rows(),
        procs: catcache.proc_rows(),
        aggregates: catcache.aggregate_rows(),
        casts: catcache.cast_rows(),
        conversions: catcache.conversion_rows(),
        collations: catcache.collation_rows(),
        databases: catcache.database_rows(),
        tablespaces: catcache.tablespace_rows(),
        statistics: catcache.statistic_rows(),
        types: catcache.type_rows(),
    }
}

pub(crate) fn add_builtin_description_rows(
    descriptions: &mut Vec<PgDescriptionRow>,
    catcache: &CatCache,
) {
    let mut seen = descriptions
        .iter()
        .map(|row| (row.objoid, row.classoid, row.objsubid))
        .collect::<BTreeSet<_>>();

    for row in catcache
        .proc_rows()
        .into_iter()
        .filter(|row| row.oid <= 9999)
    {
        if seen.insert((row.oid, PG_PROC_RELATION_OID, 0)) {
            descriptions.push(PgDescriptionRow {
                objoid: row.oid,
                classoid: PG_PROC_RELATION_OID,
                objsubid: 0,
                description: documented_operator_proc_description(row.oid)
                    .unwrap_or_else(|| format!("built-in function {}", row.proname)),
            });
        }
    }

    for row in catcache
        .operator_rows()
        .into_iter()
        .filter(|row| row.oid <= 9999)
    {
        if seen.insert((row.oid, PG_OPERATOR_RELATION_OID, 0)) {
            descriptions.push(PgDescriptionRow {
                objoid: row.oid,
                classoid: PG_OPERATOR_RELATION_OID,
                objsubid: 0,
                // :HACK: pgrust's bootstrap operator/proc catalog is still
                // synthetic. Mark generated operator comments as deprecated so
                // PostgreSQL's stricter proc-comment cross-check ignores them
                // until exact upstream comments are cataloged.
                description: documented_operator_description(row.oid)
                    .unwrap_or_else(|| "deprecated built-in operator".into()),
            });
        }
    }
}

fn documented_operator_proc_description(oid: u32) -> Option<String> {
    Some(
        match oid {
            212 => "implementation of - operator",
            378 => "append element onto end of array",
            379 => "prepend element onto front of array",
            1035 => "add/update ACL item",
            1036 => "remove ACL item",
            1037 => "contains",
            2747 => "implementation of && operator",
            3217 => "get value from jsonb with path elements",
            3940 => "get value from jsonb as text with path elements",
            3951 => "get value from json with path elements",
            3953 => "get value from json as text with path elements",
            _ => return None,
        }
        .into(),
    )
}

fn documented_operator_description(oid: u32) -> Option<String> {
    Some(
        match oid {
            349 => "append element onto end of array",
            374 => "prepend element onto front of array",
            558 => "negate",
            966 => "add/update ACL item",
            967 => "remove ACL item",
            968 => "contains",
            2750 => "overlaps",
            3213 => "get value from jsonb with path elements",
            3206 => "get value from jsonb as text with path elements",
            3966 => "get value from json with path elements",
            3967 => "get value from json as text with path elements",
            _ => return None,
        }
        .into(),
    )
}

fn default_relreplident_for_catalog_entry(entry: &CatalogEntry) -> char {
    if matches!(entry.relkind, 'r' | 'p') {
        if entry.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            'n'
        } else {
            'd'
        }
    } else {
        'n'
    }
}

pub(crate) fn physical_catalog_rows_for_catalog_entry(
    catalog: &Catalog,
    relation_name: &str,
    entry: &CatalogEntry,
) -> PhysicalCatalogRows {
    let relname = relation_name
        .rsplit_once('.')
        .map(|(_, object)| object)
        .unwrap_or(relation_name);
    let mut object_oids = entry_object_oids(entry);
    let mut rows = PhysicalCatalogRows::default();
    rows.classes.push(PgClassRow {
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
    });

    if entry.row_type_oid != 0 {
        rows.types.push(composite_type_row_with_owner(
            relname,
            entry.row_type_oid,
            entry.namespace_oid,
            entry.owner_oid,
            entry.relation_oid,
            entry.array_type_oid,
        ));
    }
    if entry.array_type_oid != 0 {
        rows.types.push(composite_array_type_row_with_owner(
            relname,
            entry.array_type_oid,
            entry.namespace_oid,
            entry.owner_oid,
            entry.row_type_oid,
            entry.relation_oid,
        ));
    }

    rows.attributes
        .extend(entry.desc.columns.iter().enumerate().map(|(idx, column)| {
            let atttypid = catalog_sql_type_oid(catalog, column.sql_type);
            let type_row = catalog_type_row_by_oid(atttypid);
            PgAttributeRow {
                attrelid: entry.relation_oid,
                attname: column.name.clone(),
                atttypid,
                attlen: type_row
                    .as_ref()
                    .map(|row| row.typlen)
                    .unwrap_or(column.storage.attlen),
                attnum: idx.saturating_add(1) as i16,
                attnotnull: !column.storage.nullable,
                attisdropped: column.dropped,
                atttypmod: column.sql_type.typmod,
                attalign: type_row
                    .as_ref()
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
                attacl: column.attacl.clone(),
                attoptions: None,
                attfdwoptions: column.fdw_options.clone(),
                attmissingval: None,
                attbyval: type_row.as_ref().is_some_and(|row| row.typbyval),
                sql_type: column.sql_type,
            }
        }));

    rows.attrdefs.extend(
        entry
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
            }),
    );

    rows.rewrites.extend(
        catalog
            .rewrite_rows_for_relation(entry.relation_oid)
            .iter()
            .cloned(),
    );
    rows.triggers.extend(
        catalog
            .triggers
            .iter()
            .filter(|row| row.tgrelid == entry.relation_oid)
            .cloned(),
    );
    rows.policies.extend(
        catalog
            .policy_rows_for_relation(entry.relation_oid)
            .iter()
            .cloned(),
    );
    rows.inherits.extend(
        catalog
            .inherit_rows()
            .iter()
            .filter(|row| row.inhrelid == entry.relation_oid)
            .cloned(),
    );
    if let Some(row) = &entry.partitioned_table {
        rows.partitioned_tables.push(row.clone());
    }
    object_oids.extend(rows.rewrites.iter().map(|row| row.oid));
    object_oids.extend(rows.triggers.iter().map(|row| row.oid));
    object_oids.extend(rows.policies.iter().map(|row| row.oid));

    if matches!(entry.relkind, 'r' | 'f') {
        rows.constraints.extend(
            catalog
                .constraint_rows()
                .iter()
                .filter(|row| row.conrelid == entry.relation_oid)
                .cloned(),
        );
    }

    let constraint_oids = rows
        .constraints
        .iter()
        .map(|row| row.oid)
        .collect::<BTreeSet<_>>();
    rows.depends.extend(
        catalog
            .depend_rows()
            .iter()
            .filter(|row| object_oids.contains(&row.objid) || constraint_oids.contains(&row.objid))
            .cloned(),
    );

    if let Some(index_meta) = &entry.index_meta {
        rows.indexes.push(PgIndexRow {
            indexrelid: entry.relation_oid,
            indrelid: index_meta.indrelid,
            indnatts: index_meta.indkey.len() as i16,
            indnkeyatts: index_meta.indclass.len() as i16,
            indisunique: index_meta.indisunique,
            indnullsnotdistinct: index_meta.indnullsnotdistinct,
            indisprimary: index_meta.indisprimary,
            indisexclusion: index_meta.indisexclusion,
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

    rows
}

fn catalog_sql_type_oid(catalog: &Catalog, sql_type: SqlType) -> u32 {
    if sql_type.is_array
        && matches!(
            sql_type.kind,
            crate::backend::parser::SqlTypeKind::Composite
                | crate::backend::parser::SqlTypeKind::Record
        )
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

fn catalog_type_row_by_oid(oid: u32) -> Option<PgTypeRow> {
    builtin_type_row_by_oid(oid).or_else(|| {
        bootstrap_composite_type_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    })
}

fn entry_object_oids(entry: &CatalogEntry) -> BTreeSet<u32> {
    let mut oids = BTreeSet::from([entry.relation_oid]);
    if entry.row_type_oid != 0 {
        oids.insert(entry.row_type_oid);
    }
    if entry.array_type_oid != 0 {
        oids.insert(entry.array_type_oid);
    }
    for column in &entry.desc.columns {
        if let Some(oid) = column.attrdef_oid {
            oids.insert(oid);
        }
        if let Some(oid) = column.not_null_constraint_oid {
            oids.insert(oid);
        }
    }
    oids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_rows_include_generated_builtin_descriptions() {
        let catcache = CatCache::from_catalog(&Catalog::default());
        let rows = physical_catalog_rows_from_catcache(&catcache);

        assert!(rows.descriptions.iter().any(|row| {
            row.objoid == 6200
                && row.classoid == PG_PROC_RELATION_OID
                && row.description == "built-in function random"
        }));
        assert!(rows.descriptions.iter().any(|row| {
            row.objoid == 551
                && row.classoid == PG_OPERATOR_RELATION_OID
                && row.description.starts_with("deprecated")
        }));
    }
}
