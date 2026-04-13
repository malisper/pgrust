use std::collections::BTreeSet;

use crate::backend::catalog::catalog::{Catalog, CatalogEntry};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::utils::cache::catcache::{CatCache, sql_type_oid};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, BootstrapCatalogKind, PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow,
    PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgCastRow, PgClassRow, PgCollationRow,
    PgConstraintRow, PgDatabaseRow, PgDependRow, PgDescriptionRow, PgIndexRow, PgLanguageRow,
    PgNamespaceRow, PgOpclassRow, PgOperatorRow, PgOpfamilyRow, PgProcRow, PgTablespaceRow,
    PgTsConfigMapRow, PgTsConfigRow, PgTsDictRow, PgTsParserRow, PgTsTemplateRow, PgTypeRow,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct PhysicalCatalogRows {
    pub namespaces: Vec<PgNamespaceRow>,
    pub classes: Vec<PgClassRow>,
    pub attributes: Vec<PgAttributeRow>,
    pub attrdefs: Vec<PgAttrdefRow>,
    pub depends: Vec<PgDependRow>,
    pub descriptions: Vec<PgDescriptionRow>,
    pub indexes: Vec<PgIndexRow>,
    pub ams: Vec<PgAmRow>,
    pub amops: Vec<PgAmopRow>,
    pub amprocs: Vec<PgAmprocRow>,
    pub authids: Vec<PgAuthIdRow>,
    pub auth_members: Vec<PgAuthMembersRow>,
    pub languages: Vec<PgLanguageRow>,
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
    pub casts: Vec<PgCastRow>,
    pub collations: Vec<PgCollationRow>,
    pub databases: Vec<PgDatabaseRow>,
    pub tablespaces: Vec<PgTablespaceRow>,
    pub types: Vec<PgTypeRow>,
}

pub(crate) fn create_table_sync_kinds(entry: &CatalogEntry) -> Vec<BootstrapCatalogKind> {
    let mut kinds = vec![
        BootstrapCatalogKind::PgClass,
        BootstrapCatalogKind::PgType,
        BootstrapCatalogKind::PgAttribute,
        BootstrapCatalogKind::PgDepend,
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

pub(crate) fn drop_relation_sync_kinds() -> Vec<BootstrapCatalogKind> {
    vec![
        BootstrapCatalogKind::PgClass,
        BootstrapCatalogKind::PgType,
        BootstrapCatalogKind::PgAttribute,
        BootstrapCatalogKind::PgAttrdef,
        BootstrapCatalogKind::PgConstraint,
        BootstrapCatalogKind::PgDepend,
        BootstrapCatalogKind::PgDescription,
        BootstrapCatalogKind::PgIndex,
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
        BootstrapCatalogKind::PgDescription,
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
    target.descriptions.extend(source.descriptions);
    target.indexes.extend(source.indexes);
    target.ams.extend(source.ams);
    target.amops.extend(source.amops);
    target.amprocs.extend(source.amprocs);
    target.authids.extend(source.authids);
    target.auth_members.extend(source.auth_members);
    target.languages.extend(source.languages);
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
    target.casts.extend(source.casts);
    target.collations.extend(source.collations);
    target.databases.extend(source.databases);
    target.tablespaces.extend(source.tablespaces);
    target.types.extend(source.types);
}

pub(crate) fn physical_catalog_rows_from_catcache(catcache: &CatCache) -> PhysicalCatalogRows {
    PhysicalCatalogRows {
        namespaces: catcache.namespace_rows(),
        classes: catcache.class_rows(),
        attributes: catcache.attribute_rows(),
        attrdefs: catcache.attrdef_rows(),
        depends: catcache.depend_rows(),
        descriptions: Vec::new(),
        indexes: catcache.index_rows(),
        ams: catcache.am_rows(),
        amops: catcache.amop_rows(),
        amprocs: catcache.amproc_rows(),
        authids: catcache.authid_rows(),
        auth_members: catcache.auth_members_rows(),
        languages: catcache.language_rows(),
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
        casts: catcache.cast_rows(),
        collations: catcache.collation_rows(),
        databases: catcache.database_rows(),
        tablespaces: catcache.tablespace_rows(),
        types: catcache.type_rows(),
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
    let object_oids = entry_object_oids(entry);
    let mut rows = PhysicalCatalogRows::default();
    rows.classes.push(PgClassRow {
        oid: entry.relation_oid,
        relname: relname.to_string(),
        relnamespace: entry.namespace_oid,
        reltype: entry.row_type_oid,
        relowner: BOOTSTRAP_SUPERUSER_OID,
        relam: crate::include::catalog::relam_for_relkind(entry.relkind),
        reltablespace: 0,
        relfilenode: entry.rel.rel_number,
        reltoastrelid: entry.reltoastrelid,
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relnatts: entry.desc.columns.len() as i16,
    });

    if entry.row_type_oid != 0 {
        rows.types.push(PgTypeRow {
            oid: entry.row_type_oid,
            typname: relname.to_string(),
            typnamespace: entry.namespace_oid,
            typowner: BOOTSTRAP_SUPERUSER_OID,
            typlen: -1,
            typalign: crate::include::access::htup::AttributeAlign::Double,
            typstorage: crate::include::access::htup::AttributeStorage::Extended,
            typrelid: entry.relation_oid,
            sql_type: SqlType::new(SqlTypeKind::Text),
        });
    }

    rows.attributes
        .extend(
            entry
                .desc
                .columns
                .iter()
                .enumerate()
                .map(|(idx, column)| PgAttributeRow {
                    attrelid: entry.relation_oid,
                    attname: column.name.clone(),
                    atttypid: sql_type_oid(column.sql_type),
                    attlen: column.storage.attlen,
                    attnum: idx.saturating_add(1) as i16,
                    attnotnull: !column.storage.nullable,
                    atttypmod: column.sql_type.typmod,
                    attalign: column.storage.attalign,
                    attstorage: column.storage.attstorage,
                    attcompression: column.storage.attcompression,
                    sql_type: column.sql_type,
                }),
        );

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

    if entry.relkind == 'r' {
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
            indnkeyatts: index_meta.indkey.len() as i16,
            indisunique: index_meta.indisunique,
            indnullsnotdistinct: false,
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

    rows
}

fn entry_object_oids(entry: &CatalogEntry) -> BTreeSet<u32> {
    let mut oids = BTreeSet::from([entry.relation_oid]);
    if entry.row_type_oid != 0 {
        oids.insert(entry.row_type_oid);
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
