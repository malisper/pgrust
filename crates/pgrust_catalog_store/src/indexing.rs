use crate::bootstrap::bootstrap_catalog_rel;
use crate::catalog::{Catalog, CatalogEntry, CatalogError, CatalogIndexMeta};
use pgrust_catalog_data::{
    BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID, CatalogIndexDescriptor, PG_CATALOG_NAMESPACE_OID,
    bootstrap_relation_desc, system_catalog_index_is_primary, system_catalog_indexes,
};
use pgrust_core::RelFileLocator;
use pgrust_nodes::primnodes::RelationDesc;

use crate::FROZEN_TRANSACTION_ID;
use crate::catalog::catalog_attribute_collation_oid;
use crate::relcache::IndexRelCacheEntry;

pub fn insert_bootstrap_system_indexes(catalog: &mut Catalog) {
    for descriptor in system_catalog_indexes() {
        if catalog.get_by_oid(descriptor.relation_oid).is_some() {
            continue;
        }
        let entry = system_catalog_index_entry(*descriptor);
        catalog.insert(descriptor.relation_name, entry);
    }
}

pub fn system_catalog_index_entry(descriptor: CatalogIndexDescriptor) -> CatalogEntry {
    system_catalog_index_entry_for_db(descriptor, 1)
}

pub fn system_catalog_index_entry_for_db(
    descriptor: CatalogIndexDescriptor,
    db_oid: u32,
) -> CatalogEntry {
    CatalogEntry {
        rel: system_catalog_index_rel(descriptor, db_oid),
        relation_oid: descriptor.relation_oid,
        namespace_oid: PG_CATALOG_NAMESPACE_OID,
        owner_oid: BOOTSTRAP_SUPERUSER_OID,
        relacl: None,
        reloptions: None,
        of_type_oid: 0,
        row_type_oid: 0,
        array_type_oid: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relpersistence: 'p',
        relkind: 'i',
        am_oid: BTREE_AM_OID,
        relhassubclass: false,
        relhastriggers: false,
        relispartition: false,
        relispopulated: true,
        relpartbound: None,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: FROZEN_TRANSACTION_ID,
        desc: system_catalog_index_desc(descriptor),
        partitioned_table: None,
        index_meta: Some(system_catalog_index_meta(descriptor)),
    }
}

pub fn system_catalog_index_rel_for_db(
    descriptor: CatalogIndexDescriptor,
    db_oid: u32,
) -> RelFileLocator {
    system_catalog_index_rel(descriptor, db_oid)
}

fn system_catalog_index_rel(descriptor: CatalogIndexDescriptor, db_oid: u32) -> RelFileLocator {
    let heap_rel = bootstrap_catalog_rel(descriptor.heap_kind, db_oid);
    RelFileLocator {
        spc_oid: heap_rel.spc_oid,
        db_oid: heap_rel.db_oid,
        rel_number: descriptor.relation_oid,
    }
}

pub fn system_catalog_index_desc(descriptor: CatalogIndexDescriptor) -> RelationDesc {
    let heap_desc = bootstrap_relation_desc(descriptor.heap_kind);
    let columns = descriptor
        .key_attnums
        .iter()
        .map(|attnum| {
            heap_desc
                .columns
                .get(attnum.saturating_sub(1) as usize)
                .cloned()
                .ok_or(CatalogError::Corrupt(
                    "system catalog index key out of range",
                ))
        })
        .collect::<Result<Vec<_>, _>>()
        .expect("valid system catalog index descriptors");
    RelationDesc {
        columns: columns
            .into_iter()
            .map(|mut column| {
                column.collation_oid =
                    catalog_attribute_collation_oid(descriptor.relation_oid, column.collation_oid);
                column
            })
            .collect(),
    }
}

pub fn system_catalog_index_meta(descriptor: CatalogIndexDescriptor) -> CatalogIndexMeta {
    let heap_desc = bootstrap_relation_desc(descriptor.heap_kind);
    let indcollation = descriptor
        .key_attnums
        .iter()
        .map(|attnum| {
            let column = heap_desc
                .columns
                .get(attnum.saturating_sub(1) as usize)
                .expect("valid system catalog index descriptors");
            catalog_attribute_collation_oid(
                descriptor.heap_kind.relation_oid(),
                column.collation_oid,
            )
        })
        .collect();
    CatalogIndexMeta {
        indrelid: descriptor.heap_kind.relation_oid(),
        indkey: descriptor.key_attnums.to_vec(),
        indisunique: descriptor.unique,
        indnullsnotdistinct: false,
        indisprimary: system_catalog_index_is_primary(&descriptor),
        indisexclusion: false,
        indimmediate: true,
        indisvalid: true,
        indisready: true,
        indislive: true,
        indclass: descriptor.opclass_oids.to_vec(),
        indclass_options: vec![Vec::new(); descriptor.key_attnums.len()],
        indcollation,
        indoption: vec![0; descriptor.key_attnums.len()],
        indexprs: None,
        indpred: None,
        btree_options: None,
        brin_options: None,
        gist_options: None,
        gin_options: None,
        hash_options: None,
    }
}

pub fn system_catalog_index_relcache(descriptor: CatalogIndexDescriptor) -> IndexRelCacheEntry {
    let meta = system_catalog_index_meta(descriptor);
    IndexRelCacheEntry {
        indexrelid: descriptor.relation_oid,
        indrelid: meta.indrelid,
        indnatts: meta.indkey.len() as i16,
        indnkeyatts: meta.indclass.len() as i16,
        indisunique: meta.indisunique,
        indnullsnotdistinct: false,
        indisprimary: system_catalog_index_is_primary(&descriptor),
        indisexclusion: false,
        indimmediate: true,
        indisclustered: false,
        indisvalid: true,
        indcheckxmin: false,
        indisready: true,
        indislive: true,
        indisreplident: false,
        am_oid: BTREE_AM_OID,
        am_handler_oid: None,
        indkey: meta.indkey,
        indclass: meta.indclass,
        indclass_options: meta.indclass_options,
        indcollation: meta.indcollation,
        indoption: meta.indoption,
        opfamily_oids: Vec::new(),
        opcintype_oids: Vec::new(),
        opckeytype_oids: Vec::new(),
        amop_entries: Vec::new(),
        amproc_entries: Vec::new(),
        indexprs: None,
        indpred: None,
        rd_indexprs: None,
        rd_indpred: None,
        btree_options: None,
        brin_options: None,
        gist_options: None,
        gin_options: None,
        hash_options: None,
    }
}
