use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value as JsonValue;

use crate::backend::access::heap::heapam::{heap_flush, heap_insert, heap_scan_begin, heap_scan_next};
use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::catalog::catalog::{CatalogIndexMeta, column_desc, Catalog, CatalogEntry, CatalogError};
use crate::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
use crate::backend::executor::value_io::tuple_from_values;
use crate::backend::executor::value_io::decode_value;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, RelFileLocator, StorageManager};
use crate::include::catalog::{
    BootstrapCatalogKind, PgAmRow, PgAttrdefRow, PgAttributeRow, PgAuthIdRow, PgAuthMembersRow,
    PgCastRow, PgClassRow, PgCollationRow, PgDatabaseRow, PgDependRow, PgIndexRow,
    PgLanguageRow, PgNamespaceRow, PgProcRow, PgTablespaceRow, PgTypeRow,
    bootstrap_catalog_kinds, bootstrap_composite_type_rows, bootstrap_relation_desc,
    builtin_type_rows,
};
use crate::include::nodes::datum::Value;
use crate::BufferPool;

const CONTROL_FILE_MAGIC: u32 = 0x5052_4743;
pub(crate) const DEFAULT_FIRST_REL_NUMBER: u32 = 16000;
pub(crate) const DEFAULT_FIRST_USER_OID: u32 = 16_384;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhysicalCatalogRows {
    pub namespaces: Vec<PgNamespaceRow>,
    pub classes: Vec<PgClassRow>,
    pub attributes: Vec<PgAttributeRow>,
    pub attrdefs: Vec<PgAttrdefRow>,
    pub depends: Vec<PgDependRow>,
    pub indexes: Vec<PgIndexRow>,
    pub ams: Vec<PgAmRow>,
    pub authids: Vec<PgAuthIdRow>,
    pub auth_members: Vec<PgAuthMembersRow>,
    pub languages: Vec<PgLanguageRow>,
    pub procs: Vec<PgProcRow>,
    pub casts: Vec<PgCastRow>,
    pub collations: Vec<PgCollationRow>,
    pub databases: Vec<PgDatabaseRow>,
    pub tablespaces: Vec<PgTablespaceRow>,
    pub types: Vec<PgTypeRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogStore {
    base_dir: PathBuf,
    control_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogControl {
    next_oid: u32,
    next_rel_number: u32,
    bootstrap_complete: bool,
}

impl CatalogStore {
    pub fn load(base_dir: impl Into<PathBuf>) -> Result<Self, CatalogError> {
        let base_dir = base_dir.into();
        let global_dir = base_dir.join("global");
        let control_path = global_dir.join("pg_control");
        if let Some(parent) = control_path.parent() {
            fs::create_dir_all(parent).map_err(|e| CatalogError::Io(e.to_string()))?;
        }

        let (mut catalog, control) = if control_path.exists() {
            let control = load_control_file(&control_path)?;
            let mut catalog = load_catalog_from_physical(&base_dir)?;
            catalog.next_oid = catalog.next_oid.max(control.next_oid);
            catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
            (catalog, control)
        } else {
            let catalog = Catalog::default();
            let control = CatalogControl {
                next_oid: catalog.next_oid,
                next_rel_number: catalog.next_rel_number,
                bootstrap_complete: true,
            };
            persist_control_file(&control_path, &control)?;
            (catalog, control)
        };

        catalog.next_oid = catalog.next_oid.max(control.next_oid);
        catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
        persist_control_file(
            &control_path,
            &CatalogControl {
                next_rel_number: catalog.next_rel_number,
                next_oid: catalog.next_oid.max(control.next_oid),
                bootstrap_complete: control.bootstrap_complete,
            },
        )?;
        sync_physical_catalogs(&base_dir, &catalog)?;

        Ok(Self { base_dir, control_path })
    }

    pub fn catalog_snapshot(&self) -> Result<Catalog, CatalogError> {
        self.catalog_snapshot_with_control()
    }

    pub fn relcache(&self) -> Result<RelCache, CatalogError> {
        RelCache::from_physical(&self.base_dir)
    }

    pub fn relation(&self, name: &str) -> Result<Option<RelCacheEntry>, CatalogError> {
        Ok(self.relcache()?.get_by_name(name).cloned())
    }

    pub fn visible_table_names(&self) -> Result<Vec<String>, CatalogError> {
        let mut names = self
            .relcache()?
            .entries()
            .map(|(name, _)| name.to_string())
            .filter(|name| !name.contains('.'))
            .filter(|name| !name.starts_with("pg_"))
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        Ok(names)
    }

    pub fn create_table(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
    ) -> Result<CatalogEntry, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let entry = catalog.create_table(name, desc)?;
        self.persist_catalog(&catalog)?;
        Ok(entry)
    }

    pub fn create_index(
        &mut self,
        index_name: impl Into<String>,
        table_name: &str,
        unique: bool,
        columns: &[String],
    ) -> Result<CatalogEntry, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let entry = catalog.create_index(index_name, table_name, unique, columns)?;
        self.persist_catalog(&catalog)?;
        Ok(entry)
    }

    pub fn drop_table(&mut self, name: &str) -> Result<Vec<CatalogEntry>, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let oids = drop_relation_oids(&catalog, name)?;
        let mut dropped = Vec::with_capacity(oids.len());
        for oid in oids {
            if let Some((_name, entry)) = catalog.remove_by_oid(oid) {
                dropped.push(entry);
            }
        }
        self.persist_catalog(&catalog)?;
        Ok(dropped)
    }

    fn persist_catalog(&self, catalog: &Catalog) -> Result<(), CatalogError> {
        persist_control_file(
            &self.control_path,
            &CatalogControl {
                next_oid: catalog.next_oid,
                next_rel_number: catalog.next_rel_number,
                bootstrap_complete: true,
            },
        )?;
        sync_physical_catalogs(&self.base_dir, catalog)
    }
}

fn drop_relation_oids(catalog: &Catalog, name: &str) -> Result<Vec<u32>, CatalogError> {
    let entry = catalog
        .get(name)
        .ok_or_else(|| CatalogError::UnknownTable(name.to_string()))?;
    if entry.relkind != 'r' {
        return Err(CatalogError::UnknownTable(name.to_string()));
    }
    let depend_rows = CatCache::from_catalog(catalog).depend_rows();
    let mut seen = BTreeSet::new();
    let mut order = Vec::new();
    collect_relation_drop_oids(
        catalog,
        &depend_rows,
        entry.relation_oid,
        &mut seen,
        &mut order,
    );
    Ok(order)
}

fn collect_relation_drop_oids(
    catalog: &Catalog,
    depend_rows: &[PgDependRow],
    relation_oid: u32,
    seen: &mut BTreeSet<u32>,
    order: &mut Vec<u32>,
) {
    if !seen.insert(relation_oid) {
        return;
    }

    for row in depend_rows {
        if row.refclassid != crate::include::catalog::PG_CLASS_RELATION_OID
            || row.refobjid != relation_oid
            || row.classid != crate::include::catalog::PG_CLASS_RELATION_OID
            || row.objsubid != 0
        {
            continue;
        }
        if let Some(dependent) = catalog.get_by_oid(row.objid) {
            if dependent.relkind != 'r' && dependent.relkind != 'i' {
                continue;
            }
            collect_relation_drop_oids(catalog, depend_rows, dependent.relation_oid, seen, order);
        }
    }

    order.push(relation_oid);
}

impl CatalogStore {
    fn catalog_snapshot_with_control(&self) -> Result<Catalog, CatalogError> {
        let mut catalog = load_catalog_from_physical(&self.base_dir)?;
        if self.control_path.exists() {
            let control = load_control_file(&self.control_path)?;
            catalog.next_oid = catalog.next_oid.max(control.next_oid);
            catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
        }
        Ok(catalog)
    }
}

#[cfg(test)]
pub(crate) fn sync_catalog_heaps_for_tests(
    base_dir: &Path,
    catalog: &Catalog,
) -> Result<(), CatalogError> {
    let catcache = CatCache::from_catalog(catalog);
    let rows = physical_catalog_rows_from_catcache(&catcache);
    sync_catalog_rows(base_dir, &rows, 1)
}

impl CatalogStore {
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

fn sync_physical_catalogs(base_dir: &Path, catalog: &Catalog) -> Result<(), CatalogError> {
    let catcache = CatCache::from_catalog(catalog);
    let rows = physical_catalog_rows_from_catcache(&catcache);
    sync_catalog_rows(base_dir, &rows, 1)
}

pub(crate) fn sync_catalog_rows(
    base_dir: &Path,
    rows: &PhysicalCatalogRows,
    db_oid: u32,
) -> Result<(), CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);
    for kind in bootstrap_catalog_kinds() {
        let rel = RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: kind.relation_oid(),
        };
        smgr.open(rel).map_err(|e| CatalogError::Io(e.to_string()))?;
        smgr.unlink(rel, Some(ForkNumber::Main), false);
        smgr.create(rel, ForkNumber::Main, false)
            .map_err(|e| CatalogError::Io(e.to_string()))?;
    }

    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 16);
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgNamespace.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgNamespace),
        rows.namespaces
            .iter()
            .cloned()
            .map(namespace_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgClass.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgClass),
        rows.classes.iter().cloned().map(pg_class_row_values).collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgAuthId.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAuthId),
        rows.authids
            .iter()
            .cloned()
            .map(pg_authid_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgAuthMembers.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAuthMembers),
        rows.auth_members
            .iter()
            .cloned()
            .map(pg_auth_members_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgCollation.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgCollation),
        rows.collations
            .iter()
            .cloned()
            .map(pg_collation_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgLanguage.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgLanguage),
        rows.languages
            .iter()
            .cloned()
            .map(pg_language_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgProc.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgProc),
        rows.procs.iter().cloned().map(pg_proc_row_values).collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgCast.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgCast),
        rows.casts.iter().cloned().map(pg_cast_row_values).collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgDatabase.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgDatabase),
        rows.databases
            .iter()
            .cloned()
            .map(pg_database_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgAm.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAm),
        rows.ams.iter().cloned().map(pg_am_row_values).collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgTablespace.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgTablespace),
        rows.tablespaces
            .iter()
            .cloned()
            .map(pg_tablespace_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgType.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgType),
        rows.types.iter().cloned().map(pg_type_row_values).collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgAttribute.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAttribute),
        rows.attributes
            .iter()
            .cloned()
            .map(pg_attribute_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgAttrdef.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAttrdef),
        rows.attrdefs
            .iter()
            .cloned()
            .map(pg_attrdef_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgDepend.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgDepend),
        rows.depends
            .iter()
            .cloned()
            .map(pg_depend_row_values)
            .collect(),
    )?;
    insert_catalog_rows(
        &pool,
        RelFileLocator {
            spc_oid: 0,
            db_oid,
            rel_number: BootstrapCatalogKind::PgIndex.relation_oid(),
        },
        &bootstrap_relation_desc(BootstrapCatalogKind::PgIndex),
        rows.indexes
            .iter()
            .cloned()
            .map(pg_index_row_values)
            .collect(),
    )?;
    Ok(())
}

fn physical_catalog_rows_from_catcache(catcache: &CatCache) -> PhysicalCatalogRows {
    PhysicalCatalogRows {
        namespaces: catcache.namespace_rows(),
        classes: catcache.class_rows(),
        attributes: catcache.attribute_rows(),
        attrdefs: catcache.attrdef_rows(),
        depends: catcache.depend_rows(),
        indexes: catcache.index_rows(),
        ams: catcache.am_rows(),
        authids: catcache.authid_rows(),
        auth_members: catcache.auth_members_rows(),
        languages: catcache.language_rows(),
        procs: catcache.proc_rows(),
        casts: catcache.cast_rows(),
        collations: catcache.collation_rows(),
        databases: catcache.database_rows(),
        tablespaces: catcache.tablespace_rows(),
        types: catcache.type_rows(),
    }
}

fn insert_catalog_rows(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    desc: &RelationDesc,
    rows: Vec<Vec<Value>>,
) -> Result<(), CatalogError> {
    for values in rows {
        let tuple = tuple_from_values(desc, &values)
            .map_err(|e| CatalogError::Io(format!("catalog tuple encode failed: {e:?}")))?;
        heap_insert(pool, 0, rel, &tuple)
            .map_err(|e| CatalogError::Io(format!("catalog tuple insert failed: {e:?}")))?;
    }
    let nblocks = pool
        .with_storage_mut(|s| s.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|e| CatalogError::Io(e.to_string()))?;
    for block in 0..nblocks {
        heap_flush(pool, 0, rel, block)
            .map_err(|e| CatalogError::Io(format!("catalog flush failed: {e:?}")))?;
    }
    Ok(())
}

fn namespace_row_values(row: PgNamespaceRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.nspname.into()),
        Value::Int32(row.nspowner as i32),
    ]
}

fn pg_class_row_values(row: PgClassRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.relname.into()),
        Value::Int32(row.relnamespace as i32),
        Value::Int32(row.reltype as i32),
        Value::Int32(row.relowner as i32),
        Value::Int32(row.relam as i32),
        Value::Int32(row.relfilenode as i32),
        Value::Text(row.relpersistence.to_string().into()),
        Value::Text(row.relkind.to_string().into()),
    ]
}

fn pg_am_row_values(row: PgAmRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.amname.into()),
        Value::Int32(row.amhandler as i32),
        Value::Text(row.amtype.to_string().into()),
    ]
}

fn pg_authid_row_values(row: PgAuthIdRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.rolname.into()),
        Value::Bool(row.rolsuper),
        Value::Bool(row.rolinherit),
        Value::Bool(row.rolcreaterole),
        Value::Bool(row.rolcreatedb),
        Value::Bool(row.rolcanlogin),
        Value::Bool(row.rolreplication),
        Value::Bool(row.rolbypassrls),
        Value::Int32(row.rolconnlimit),
    ]
}

fn pg_auth_members_row_values(row: PgAuthMembersRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.roleid as i32),
        Value::Int32(row.member as i32),
        Value::Int32(row.grantor as i32),
        Value::Bool(row.admin_option),
        Value::Bool(row.inherit_option),
        Value::Bool(row.set_option),
    ]
}

fn pg_collation_row_values(row: PgCollationRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.collname.into()),
        Value::Int32(row.collnamespace as i32),
        Value::Int32(row.collowner as i32),
        Value::Text(row.collprovider.to_string().into()),
        Value::Bool(row.collisdeterministic),
        Value::Int32(row.collencoding),
    ]
}

fn pg_language_row_values(row: PgLanguageRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.lanname.into()),
        Value::Int32(row.lanowner as i32),
        Value::Bool(row.lanispl),
        Value::Bool(row.lanpltrusted),
        Value::Int32(row.lanplcallfoid as i32),
        Value::Int32(row.laninline as i32),
        Value::Int32(row.lanvalidator as i32),
    ]
}

fn pg_proc_row_values(row: PgProcRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.proname.into()),
        Value::Int32(row.pronamespace as i32),
        Value::Int32(row.proowner as i32),
        Value::Int32(row.prolang as i32),
        Value::Float64(row.procost),
        Value::Float64(row.prorows),
        Value::Int32(row.provariadic as i32),
        Value::Int32(row.prosupport as i32),
        Value::Text(row.prokind.to_string().into()),
        Value::Bool(row.prosecdef),
        Value::Bool(row.proleakproof),
        Value::Bool(row.proisstrict),
        Value::Bool(row.proretset),
        Value::Text(row.provolatile.to_string().into()),
        Value::Text(row.proparallel.to_string().into()),
        Value::Int16(row.pronargs),
        Value::Int16(row.pronargdefaults),
        Value::Int32(row.prorettype as i32),
        Value::Text(row.proargtypes.into()),
        Value::Text(row.prosrc.into()),
    ]
}

fn pg_cast_row_values(row: PgCastRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.castsource as i32),
        Value::Int32(row.casttarget as i32),
        Value::Int32(row.castfunc as i32),
        Value::Text(row.castcontext.to_string().into()),
        Value::Text(row.castmethod.to_string().into()),
    ]
}

fn pg_database_row_values(row: PgDatabaseRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.datname.into()),
        Value::Int32(row.datdba as i32),
        Value::Int32(row.dattablespace as i32),
        Value::Bool(row.datistemplate),
        Value::Bool(row.datallowconn),
    ]
}

fn pg_tablespace_row_values(row: PgTablespaceRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.spcname.into()),
        Value::Int32(row.spcowner as i32),
    ]
}

fn pg_attribute_row_values(row: PgAttributeRow) -> Vec<Value> {
    vec![
        Value::Int32(row.attrelid as i32),
        Value::Text(row.attname.into()),
        Value::Int32(row.atttypid as i32),
        Value::Int16(row.attnum),
        Value::Bool(row.attnotnull),
        Value::Int32(row.atttypmod),
    ]
}

fn pg_type_row_values(row: PgTypeRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.typname.into()),
        Value::Int32(row.typnamespace as i32),
        Value::Int32(row.typowner as i32),
        Value::Int32(row.typrelid as i32),
    ]
}

fn pg_attrdef_row_values(row: PgAttrdefRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.adrelid as i32),
        Value::Int16(row.adnum),
        Value::Text(row.adbin.into()),
    ]
}

fn pg_depend_row_values(row: PgDependRow) -> Vec<Value> {
    vec![
        Value::Int32(row.classid as i32),
        Value::Int32(row.objid as i32),
        Value::Int32(row.objsubid),
        Value::Int32(row.refclassid as i32),
        Value::Int32(row.refobjid as i32),
        Value::Int32(row.refobjsubid),
        Value::Text(row.deptype.to_string().into()),
    ]
}

fn pg_index_row_values(row: PgIndexRow) -> Vec<Value> {
    vec![
        Value::Int32(row.indexrelid as i32),
        Value::Int32(row.indrelid as i32),
        Value::Int16(row.indnatts),
        Value::Int16(row.indnkeyatts),
        Value::Bool(row.indisunique),
        Value::Bool(row.indisvalid),
        Value::Bool(row.indisready),
        Value::Bool(row.indislive),
        Value::Text(row.indkey.into()),
    ]
}

fn persist_control_file(path: &Path, control: &CatalogControl) -> Result<(), CatalogError> {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&CONTROL_FILE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&control.next_oid.to_le_bytes());
    bytes.extend_from_slice(&control.next_rel_number.to_le_bytes());
    bytes.extend_from_slice(&(u32::from(control.bootstrap_complete)).to_le_bytes());
    fs::write(path, bytes).map_err(|e| CatalogError::Io(e.to_string()))
}

fn load_catalog_from_physical(base_dir: &Path) -> Result<Catalog, CatalogError> {
    let rows = load_physical_catalog_rows(base_dir)?;
    let namespace_rows = rows.namespaces;
    let type_rows = rows.types;
    let class_rows = rows.classes;
    let attribute_rows = rows.attributes;
    let attrdef_rows = rows.attrdefs;
    let _depend_rows = rows.depends;
    let index_rows = rows.indexes;
    let _am_rows = rows.ams;
    let _authid_rows = rows.authids;
    let _auth_members_rows = rows.auth_members;
    let _language_rows = rows.languages;
    let _proc_rows = rows.procs;
    let _cast_rows = rows.casts;
    let _collation_rows = rows.collations;
    let _database_rows = rows.databases;
    let _tablespace_rows = rows.tablespaces;

    let namespace_names = namespace_rows
        .iter()
        .map(|row| (row.oid, row.nspname.as_str()))
        .collect::<BTreeMap<_, _>>();
    let type_by_oid = type_rows
        .iter()
        .map(|row| (row.oid, row.sql_type))
        .collect::<BTreeMap<_, _>>();
    let mut attrs_by_relid = BTreeMap::<u32, Vec<PgAttributeRow>>::new();
    for row in attribute_rows {
        attrs_by_relid.entry(row.attrelid).or_default().push(row);
    }
    for rows in attrs_by_relid.values_mut() {
        rows.sort_by_key(|row| row.attnum);
    }
    let attrdefs_by_key = attrdef_rows
        .into_iter()
        .map(|row| ((row.adrelid, row.adnum), row))
        .collect::<BTreeMap<_, _>>();
    let indexes_by_relid = index_rows
        .into_iter()
        .map(|row| (row.indexrelid, row))
        .collect::<BTreeMap<_, _>>();
    // :HACK: Keep a one-time compatibility path for stores created before `pg_attrdef`
    // existed. Once old datadirs no longer need migration, delete this fallback and
    // require defaults to come only from `pg_attrdef`.
    let legacy_default_exprs = load_legacy_default_exprs(base_dir)?;

    let next_oid = class_rows
        .iter()
        .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
            next_oid
                .max(row.oid.saturating_add(1))
                .max(row.reltype.saturating_add(1))
        })
        .max(
            type_rows
                .iter()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        )
        .max(
            attrdefs_by_key
                .values()
                .fold(DEFAULT_FIRST_USER_OID, |next_oid, row| {
                    next_oid.max(row.oid.saturating_add(1))
                }),
        );
    let mut catalog = Catalog {
        tables: BTreeMap::new(),
        next_rel_number: DEFAULT_FIRST_REL_NUMBER,
        next_oid,
    };
    for row in class_rows {
        let attrs = attrs_by_relid.get(&row.oid).map(Vec::as_slice).unwrap_or(&[]);
        let columns = attrs
            .iter()
            .map(|attr| {
                let sql_type = *type_by_oid
                    .get(&attr.atttypid)
                    .ok_or(CatalogError::Corrupt("unknown atttypid"))?;
                let mut desc = column_desc(
                    attr.attname.clone(),
                    SqlType {
                        typmod: attr.atttypmod,
                        ..sql_type
                    },
                    !attr.attnotnull,
                );
                if let Some(attrdef) = attrdefs_by_key.get(&(row.oid, attr.attnum)) {
                    desc.attrdef_oid = Some(attrdef.oid);
                    desc.default_expr = Some(attrdef.adbin.clone());
                } else if let Some(expr) = legacy_default_exprs.get(&(row.oid, attr.attnum)) {
                    desc.default_expr = Some(expr.clone());
                    desc.attrdef_oid = Some(catalog.next_oid);
                    catalog.next_oid = catalog.next_oid.saturating_add(1);
                }
                Ok(desc)
            })
            .collect::<Result<Vec<_>, CatalogError>>()?;
        let namespace_name = namespace_names
            .get(&row.relnamespace)
            .copied()
            .unwrap_or("pg_catalog");
        let name = match namespace_name {
            "public" | "pg_catalog" => row.relname.clone(),
            other => format!("{other}.{}", row.relname),
        };
        catalog.insert(
            name,
            CatalogEntry {
                rel: RelFileLocator {
                    spc_oid: 0,
                    db_oid: 1,
                    rel_number: row.relfilenode,
                },
                relation_oid: row.oid,
                namespace_oid: row.relnamespace,
                row_type_oid: row.reltype,
                relkind: row.relkind,
                desc: RelationDesc { columns },
                index_meta: indexes_by_relid.get(&row.oid).map(|index| CatalogIndexMeta {
                    indrelid: index.indrelid,
                    indkey: parse_indkey(&index.indkey),
                    indisunique: index.indisunique,
                }),
            },
        );
        catalog.next_oid = catalog
            .next_oid
            .max(row.oid.saturating_add(1))
            .max(row.reltype.saturating_add(1));
        catalog.next_rel_number = catalog
            .next_rel_number
            .max(row.relfilenode.saturating_add(1));
    }
    Ok(catalog)
}

fn load_legacy_default_exprs(base_dir: &Path) -> Result<BTreeMap<(u32, i16), String>, CatalogError> {
    let path = base_dir.join("catalog").join("defaults.json");
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let text = fs::read_to_string(&path).map_err(|e| CatalogError::Io(e.to_string()))?;
    let json = serde_json::from_str::<JsonValue>(&text)
        .map_err(|_| CatalogError::Corrupt("invalid legacy defaults json"))?;
    let Some(entries) = json.as_array() else {
        return Err(CatalogError::Corrupt("invalid legacy defaults json root"));
    };

    let mut defaults = BTreeMap::new();
    for entry in entries {
        let relation_oid = entry
            .get("relation_oid")
            .and_then(JsonValue::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or(CatalogError::Corrupt("invalid legacy default relation oid"))?;
        let attnum = entry
            .get("attnum")
            .and_then(JsonValue::as_i64)
            .and_then(|v| i16::try_from(v).ok())
            .ok_or(CatalogError::Corrupt("invalid legacy default attnum"))?;
        let expr = entry
            .get("expr")
            .and_then(JsonValue::as_str)
            .ok_or(CatalogError::Corrupt("invalid legacy default expr"))?;
        defaults.insert((relation_oid, attnum), expr.to_string());
    }

    Ok(defaults)
}

pub(crate) fn load_physical_catalog_rows(base_dir: &Path) -> Result<PhysicalCatalogRows, CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);
    let mut rels = BTreeMap::new();
    let mut missing_attrdef = false;
    let mut missing_depend = false;
    let mut missing_index = false;
    let mut missing_am = false;
    let mut missing_authid = false;
    let mut missing_auth_members = false;
    let mut missing_language = false;
    let mut missing_proc = false;
    let mut missing_cast = false;
    let mut missing_collation = false;
    let mut missing_database = false;
    let mut missing_tablespace = false;
    for kind in bootstrap_catalog_kinds() {
        let rel = RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: kind.relation_oid(),
        };
        if !smgr.exists(rel, ForkNumber::Main) {
            if kind == BootstrapCatalogKind::PgAttrdef {
                missing_attrdef = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgDepend {
                missing_depend = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgIndex {
                missing_index = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgAm {
                missing_am = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgAuthId {
                missing_authid = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgAuthMembers {
                missing_auth_members = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgLanguage {
                missing_language = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgProc {
                missing_proc = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgCollation {
                missing_collation = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgCast {
                missing_cast = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgDatabase {
                missing_database = true;
                continue;
            }
            if kind == BootstrapCatalogKind::PgTablespace {
                missing_tablespace = true;
                continue;
            }
            return Err(CatalogError::Corrupt("missing physical bootstrap catalog"));
        }
        smgr.open(rel).map_err(|e| CatalogError::Io(e.to_string()))?;
        rels.insert(kind, rel);
    }
    let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 16);

    let namespace_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgNamespace],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgNamespace),
    )?
    .into_iter()
    .map(namespace_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let type_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgType],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgType),
    )?
    .into_iter()
    .map(pg_type_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let class_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgClass],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgClass),
    )?
    .into_iter()
    .map(pg_class_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let attribute_rows = scan_catalog_relation(
        &pool,
        rels[&BootstrapCatalogKind::PgAttribute],
        &bootstrap_relation_desc(BootstrapCatalogKind::PgAttribute),
    )?
    .into_iter()
    .map(pg_attribute_row_from_values)
    .collect::<Result<Vec<_>, _>>()?;
    let database_rows = if missing_database {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgDatabase],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgDatabase),
        )?
        .into_iter()
        .map(pg_database_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let authid_rows = if missing_authid {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgAuthId],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAuthId),
        )?
        .into_iter()
        .map(pg_authid_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let auth_members_rows = if missing_auth_members {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgAuthMembers],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAuthMembers),
        )?
        .into_iter()
        .map(pg_auth_members_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let language_rows = if missing_language {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgLanguage],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgLanguage),
        )?
        .into_iter()
        .map(pg_language_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let proc_rows = if missing_proc {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgProc],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgProc),
        )?
        .into_iter()
        .map(pg_proc_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let collation_rows = if missing_collation {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgCollation],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgCollation),
        )?
        .into_iter()
        .map(pg_collation_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let cast_rows = if missing_cast {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgCast],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgCast),
        )?
        .into_iter()
        .map(pg_cast_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let attrdef_rows = if missing_attrdef {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgAttrdef],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAttrdef),
        )?
        .into_iter()
        .map(pg_attrdef_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let depend_rows = if missing_depend {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgDepend],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgDepend),
        )?
        .into_iter()
        .map(pg_depend_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let index_rows = if missing_index {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgIndex],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgIndex),
        )?
        .into_iter()
        .map(pg_index_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let am_rows = if missing_am {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgAm],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgAm),
        )?
        .into_iter()
        .map(pg_am_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };
    let tablespace_rows = if missing_tablespace {
        Vec::new()
    } else {
        scan_catalog_relation(
            &pool,
            rels[&BootstrapCatalogKind::PgTablespace],
            &bootstrap_relation_desc(BootstrapCatalogKind::PgTablespace),
        )?
        .into_iter()
        .map(pg_tablespace_row_from_values)
        .collect::<Result<Vec<_>, _>>()?
    };

    Ok(PhysicalCatalogRows {
        namespaces: namespace_rows,
        classes: class_rows,
        attributes: attribute_rows,
        attrdefs: attrdef_rows,
        depends: depend_rows,
        indexes: index_rows,
        ams: am_rows,
        authids: authid_rows,
        auth_members: auth_members_rows,
        languages: language_rows,
        procs: proc_rows,
        casts: cast_rows,
        collations: collation_rows,
        databases: database_rows,
        tablespaces: tablespace_rows,
        types: type_rows,
    })
}

fn scan_catalog_relation(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    desc: &RelationDesc,
) -> Result<Vec<Vec<Value>>, CatalogError> {
    let mut scan = heap_scan_begin(pool, rel).map_err(|e| CatalogError::Io(format!("{e:?}")))?;
    let attr_descs = desc.attribute_descs();
    let mut rows = Vec::new();
    while let Some((_tid, tuple)) = heap_scan_next(pool, INVALID_TRANSACTION_ID, &mut scan)
        .map_err(|e| CatalogError::Io(format!("{e:?}")))?
    {
        let raw = tuple
            .deform(&attr_descs)
            .map_err(|e| CatalogError::Io(format!("{e:?}")))?;
        let row = desc
            .columns
            .iter()
            .zip(raw.into_iter())
            .map(|(column, datum)| decode_value(column, datum).map_err(|e| CatalogError::Io(format!("{e:?}"))))
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(row);
    }
    Ok(rows)
}

fn expect_oid(value: &Value) -> Result<u32, CatalogError> {
    match value {
        Value::Int64(v) => u32::try_from(*v).map_err(|_| CatalogError::Corrupt("invalid oid value")),
        Value::Int32(v) => u32::try_from(*v).map_err(|_| CatalogError::Corrupt("invalid oid value")),
        _ => Err(CatalogError::Corrupt("expected oid value")),
    }
}

fn expect_text(value: &Value) -> Result<String, CatalogError> {
    match value {
        Value::Text(text) => Ok(text.to_string()),
        _ => Err(CatalogError::Corrupt("expected text value")),
    }
}

fn parse_indkey(indkey: &str) -> Vec<i16> {
    indkey
        .split_ascii_whitespace()
        .filter_map(|value| value.parse::<i16>().ok())
        .collect()
}

fn expect_bool(value: &Value) -> Result<bool, CatalogError> {
    match value {
        Value::Bool(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected bool value")),
    }
}

fn expect_int16(value: &Value) -> Result<i16, CatalogError> {
    match value {
        Value::Int16(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected int2 value")),
    }
}

fn expect_int32(value: &Value) -> Result<i32, CatalogError> {
    match value {
        Value::Int32(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected int4 value")),
    }
}

fn expect_float64(value: &Value) -> Result<f64, CatalogError> {
    match value {
        Value::Float64(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected float value")),
    }
}

fn namespace_row_from_values(values: Vec<Value>) -> Result<PgNamespaceRow, CatalogError> {
    Ok(PgNamespaceRow {
        oid: expect_oid(&values[0])?,
        nspname: expect_text(&values[1])?,
        nspowner: expect_oid(&values[2])?,
    })
}

fn pg_class_row_from_values(values: Vec<Value>) -> Result<PgClassRow, CatalogError> {
    let relpersistence = match &values[7] {
        Value::Text(text) => text.chars().next().ok_or(CatalogError::Corrupt("empty relpersistence"))?,
        Value::InternalChar(byte) => char::from(*byte),
        _ => return Err(CatalogError::Corrupt("expected relpersistence text")),
    };
    let relkind = match &values[8] {
        Value::Text(text) => text.chars().next().ok_or(CatalogError::Corrupt("empty relkind"))?,
        Value::InternalChar(byte) => char::from(*byte),
        _ => return Err(CatalogError::Corrupt("expected relkind text")),
    };
    Ok(PgClassRow {
        oid: expect_oid(&values[0])?,
        relname: expect_text(&values[1])?,
        relnamespace: expect_oid(&values[2])?,
        reltype: expect_oid(&values[3])?,
        relowner: expect_oid(&values[4])?,
        relam: expect_oid(&values[5])?,
        relfilenode: expect_oid(&values[6])?,
        relpersistence,
        relkind,
    })
}

fn pg_am_row_from_values(values: Vec<Value>) -> Result<PgAmRow, CatalogError> {
    let amtype = match &values[3] {
        Value::Text(text) => text.chars().next().ok_or(CatalogError::Corrupt("empty amtype"))?,
        Value::InternalChar(byte) => char::from(*byte),
        _ => return Err(CatalogError::Corrupt("expected amtype text")),
    };
    Ok(PgAmRow {
        oid: expect_oid(&values[0])?,
        amname: expect_text(&values[1])?,
        amhandler: expect_oid(&values[2])?,
        amtype,
    })
}

fn pg_authid_row_from_values(values: Vec<Value>) -> Result<PgAuthIdRow, CatalogError> {
    Ok(PgAuthIdRow {
        oid: expect_oid(&values[0])?,
        rolname: expect_text(&values[1])?,
        rolsuper: expect_bool(&values[2])?,
        rolinherit: expect_bool(&values[3])?,
        rolcreaterole: expect_bool(&values[4])?,
        rolcreatedb: expect_bool(&values[5])?,
        rolcanlogin: expect_bool(&values[6])?,
        rolreplication: expect_bool(&values[7])?,
        rolbypassrls: expect_bool(&values[8])?,
        rolconnlimit: expect_int32(&values[9])?,
    })
}

fn pg_auth_members_row_from_values(values: Vec<Value>) -> Result<PgAuthMembersRow, CatalogError> {
    Ok(PgAuthMembersRow {
        oid: expect_oid(&values[0])?,
        roleid: expect_oid(&values[1])?,
        member: expect_oid(&values[2])?,
        grantor: expect_oid(&values[3])?,
        admin_option: expect_bool(&values[4])?,
        inherit_option: expect_bool(&values[5])?,
        set_option: expect_bool(&values[6])?,
    })
}

fn pg_language_row_from_values(values: Vec<Value>) -> Result<PgLanguageRow, CatalogError> {
    Ok(PgLanguageRow {
        oid: expect_oid(&values[0])?,
        lanname: expect_text(&values[1])?,
        lanowner: expect_oid(&values[2])?,
        lanispl: expect_bool(&values[3])?,
        lanpltrusted: expect_bool(&values[4])?,
        lanplcallfoid: expect_oid(&values[5])?,
        laninline: expect_oid(&values[6])?,
        lanvalidator: expect_oid(&values[7])?,
    })
}

fn pg_proc_row_from_values(values: Vec<Value>) -> Result<PgProcRow, CatalogError> {
    let prokind = match &values[9] {
        Value::Text(text) => text.chars().next().ok_or(CatalogError::Corrupt("empty prokind"))?,
        Value::InternalChar(byte) => char::from(*byte),
        _ => return Err(CatalogError::Corrupt("expected prokind text")),
    };
    let provolatile = match &values[14] {
        Value::Text(text) => text.chars().next().ok_or(CatalogError::Corrupt("empty provolatile"))?,
        Value::InternalChar(byte) => char::from(*byte),
        _ => return Err(CatalogError::Corrupt("expected provolatile text")),
    };
    let proparallel = match &values[15] {
        Value::Text(text) => text.chars().next().ok_or(CatalogError::Corrupt("empty proparallel"))?,
        Value::InternalChar(byte) => char::from(*byte),
        _ => return Err(CatalogError::Corrupt("expected proparallel text")),
    };
    Ok(PgProcRow {
        oid: expect_oid(&values[0])?,
        proname: expect_text(&values[1])?,
        pronamespace: expect_oid(&values[2])?,
        proowner: expect_oid(&values[3])?,
        prolang: expect_oid(&values[4])?,
        procost: expect_float64(&values[5])?,
        prorows: expect_float64(&values[6])?,
        provariadic: expect_oid(&values[7])?,
        prosupport: expect_oid(&values[8])?,
        prokind,
        prosecdef: expect_bool(&values[10])?,
        proleakproof: expect_bool(&values[11])?,
        proisstrict: expect_bool(&values[12])?,
        proretset: expect_bool(&values[13])?,
        provolatile,
        proparallel,
        pronargs: expect_int16(&values[16])?,
        pronargdefaults: expect_int16(&values[17])?,
        prorettype: expect_oid(&values[18])?,
        proargtypes: expect_text(&values[19])?,
        prosrc: expect_text(&values[20])?,
    })
}

fn pg_collation_row_from_values(values: Vec<Value>) -> Result<PgCollationRow, CatalogError> {
    let collprovider = match &values[4] {
        Value::Text(text) => text.chars().next().ok_or(CatalogError::Corrupt("empty collprovider"))?,
        Value::InternalChar(byte) => char::from(*byte),
        _ => return Err(CatalogError::Corrupt("expected collprovider text")),
    };
    Ok(PgCollationRow {
        oid: expect_oid(&values[0])?,
        collname: expect_text(&values[1])?,
        collnamespace: expect_oid(&values[2])?,
        collowner: expect_oid(&values[3])?,
        collprovider,
        collisdeterministic: expect_bool(&values[5])?,
        collencoding: expect_int32(&values[6])?,
    })
}

fn pg_cast_row_from_values(values: Vec<Value>) -> Result<PgCastRow, CatalogError> {
    let castcontext = match &values[4] {
        Value::Text(text) => text.chars().next().ok_or(CatalogError::Corrupt("empty castcontext"))?,
        Value::InternalChar(byte) => char::from(*byte),
        _ => return Err(CatalogError::Corrupt("expected castcontext text")),
    };
    let castmethod = match &values[5] {
        Value::Text(text) => text.chars().next().ok_or(CatalogError::Corrupt("empty castmethod"))?,
        Value::InternalChar(byte) => char::from(*byte),
        _ => return Err(CatalogError::Corrupt("expected castmethod text")),
    };
    Ok(PgCastRow {
        oid: expect_oid(&values[0])?,
        castsource: expect_oid(&values[1])?,
        casttarget: expect_oid(&values[2])?,
        castfunc: expect_oid(&values[3])?,
        castcontext,
        castmethod,
    })
}

fn pg_database_row_from_values(values: Vec<Value>) -> Result<PgDatabaseRow, CatalogError> {
    Ok(PgDatabaseRow {
        oid: expect_oid(&values[0])?,
        datname: expect_text(&values[1])?,
        datdba: expect_oid(&values[2])?,
        dattablespace: expect_oid(&values[3])?,
        datistemplate: expect_bool(&values[4])?,
        datallowconn: expect_bool(&values[5])?,
    })
}

fn pg_tablespace_row_from_values(values: Vec<Value>) -> Result<PgTablespaceRow, CatalogError> {
    Ok(PgTablespaceRow {
        oid: expect_oid(&values[0])?,
        spcname: expect_text(&values[1])?,
        spcowner: expect_oid(&values[2])?,
    })
}

fn pg_attribute_row_from_values(values: Vec<Value>) -> Result<PgAttributeRow, CatalogError> {
    Ok(PgAttributeRow {
        attrelid: expect_oid(&values[0])?,
        attname: expect_text(&values[1])?,
        atttypid: expect_oid(&values[2])?,
        attnum: expect_int16(&values[3])?,
        attnotnull: expect_bool(&values[4])?,
        atttypmod: expect_int32(&values[5])?,
        sql_type: SqlType::new(SqlTypeKind::Text),
    })
}

fn pg_attrdef_row_from_values(values: Vec<Value>) -> Result<PgAttrdefRow, CatalogError> {
    Ok(PgAttrdefRow {
        oid: expect_oid(&values[0])?,
        adrelid: expect_oid(&values[1])?,
        adnum: expect_int16(&values[2])?,
        adbin: expect_text(&values[3])?,
    })
}

fn pg_depend_row_from_values(values: Vec<Value>) -> Result<PgDependRow, CatalogError> {
    let deptype = match &values[6] {
        Value::Text(text) => text.chars().next().ok_or(CatalogError::Corrupt("empty deptype"))?,
        Value::InternalChar(byte) => char::from(*byte),
        _ => return Err(CatalogError::Corrupt("expected deptype text")),
    };
    Ok(PgDependRow {
        classid: expect_oid(&values[0])?,
        objid: expect_oid(&values[1])?,
        objsubid: expect_int32(&values[2])?,
        refclassid: expect_oid(&values[3])?,
        refobjid: expect_oid(&values[4])?,
        refobjsubid: expect_int32(&values[5])?,
        deptype,
    })
}

fn pg_index_row_from_values(values: Vec<Value>) -> Result<PgIndexRow, CatalogError> {
    Ok(PgIndexRow {
        indexrelid: expect_oid(&values[0])?,
        indrelid: expect_oid(&values[1])?,
        indnatts: expect_int16(&values[2])?,
        indnkeyatts: expect_int16(&values[3])?,
        indisunique: expect_bool(&values[4])?,
        indisvalid: expect_bool(&values[5])?,
        indisready: expect_bool(&values[6])?,
        indislive: expect_bool(&values[7])?,
        indkey: expect_text(&values[8])?,
    })
}

fn pg_type_row_from_values(values: Vec<Value>) -> Result<PgTypeRow, CatalogError> {
    let oid = expect_oid(&values[0])?;
    Ok(PgTypeRow {
        oid,
        typname: expect_text(&values[1])?,
        typnamespace: expect_oid(&values[2])?,
        typowner: expect_oid(&values[3])?,
        typrelid: expect_oid(&values[4])?,
        sql_type: decode_builtin_sql_type(oid).unwrap_or(SqlType::new(SqlTypeKind::Text)),
    })
}

fn decode_builtin_sql_type(oid: u32) -> Option<SqlType> {
    for row in builtin_type_rows().into_iter().chain(bootstrap_composite_type_rows()) {
        if row.oid == oid {
            return Some(row.sql_type);
        }
    }
    None
}

fn load_control_file(path: &Path) -> Result<CatalogControl, CatalogError> {
    let bytes = fs::read(path).map_err(|e| CatalogError::Io(e.to_string()))?;
    if bytes.len() != 16 {
        return Err(CatalogError::Corrupt("invalid control file size"));
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != CONTROL_FILE_MAGIC {
        return Err(CatalogError::Corrupt("invalid control magic"));
    }
    let next_oid = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let next_rel_number = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let bootstrap_complete = match u32::from_le_bytes(bytes[12..16].try_into().unwrap()) {
        0 => false,
        1 => true,
        _ => return Err(CatalogError::Corrupt("invalid bootstrap flag")),
    };

    Ok(CatalogControl {
        next_oid,
        next_rel_number,
        bootstrap_complete,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::storage::smgr::segment_path;
    use crate::include::catalog::{
        BOOTSTRAP_SUPERUSER_NAME, BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID, CURRENT_DATABASE_NAME,
        C_COLLATION_OID, DEFAULT_COLLATION_OID, DEFAULT_TABLESPACE_OID, DEPENDENCY_AUTO,
        DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, HEAP_TABLE_AM_OID, INT4_TYPE_OID, INT8_TYPE_OID,
        JSON_TYPE_OID, OID_TYPE_OID, PG_ATTRDEF_RELATION_OID, PG_CLASS_RELATION_OID,
        PG_LANGUAGE_INTERNAL_OID, PG_NAMESPACE_RELATION_OID, PG_TYPE_RELATION_OID,
        POSIX_COLLATION_OID, PUBLIC_NAMESPACE_OID, TEXT_TYPE_OID, VARCHAR_TYPE_OID,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("pgrust_catalog_{label}_{nanos}"))
    }

    #[test]
    fn catalog_store_roundtrips() {
        let base = temp_dir("roundtrip");
        let mut store = CatalogStore::load(&base).unwrap();
        assert!(store.catalog_snapshot().unwrap().get("pg_class").is_some());
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("name", SqlType::new(SqlTypeKind::Text), false),
                        column_desc("note", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        assert_eq!(entry.rel.rel_number, DEFAULT_FIRST_REL_NUMBER);
        assert!(entry.relation_oid >= DEFAULT_FIRST_USER_OID);

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let reopened_entry = reopened_catalog.get("people").unwrap();
        assert_eq!(reopened_entry.rel.rel_number, DEFAULT_FIRST_REL_NUMBER);
        assert_eq!(reopened_entry.desc.columns.len(), 3);
    }

    #[test]
    fn catalog_store_persists_column_defaults() {
        let base = temp_dir("defaults_roundtrip");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("b1", SqlType::with_bit_len(SqlTypeKind::Bit, 4), false),
                column_desc("b2", SqlType::with_bit_len(SqlTypeKind::VarBit, 5), true),
            ],
        };
        desc.columns[0].default_expr = Some("'1001'".into());
        desc.columns[1].default_expr = Some("B'0101'".into());
        store.create_table("bit_defaults", desc).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let relcache = reopened.relcache().unwrap();
        let entry = relcache.get_by_name("bit_defaults").unwrap();
        assert_eq!(entry.desc.columns[0].default_expr.as_deref(), Some("'1001'"));
        assert_eq!(entry.desc.columns[1].default_expr.as_deref(), Some("B'0101'"));
    }

    #[test]
    fn catalog_store_persists_pg_attrdef_rows() {
        let base = temp_dir("attrdef_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'hello'".into());
        let entry = store.create_table("notes", desc).unwrap();

        let rows = load_physical_catalog_rows(&base).unwrap();
        let attrdef = rows
            .attrdefs
            .iter()
            .find(|row| row.adrelid == entry.relation_oid && row.adnum == 2)
            .unwrap();
        assert_eq!(attrdef.adbin, "'hello'");
        assert!(attrdef.oid >= DEFAULT_FIRST_USER_OID);
    }

    #[test]
    fn catalog_store_persists_pg_depend_rows() {
        let base = temp_dir("depend_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'hello'".into());
        let entry = store.create_table("notes", desc).unwrap();
        let attrdef_oid = entry.desc.columns[1].attrdef_oid.unwrap();

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == entry.relation_oid
                && row.objsubid == 0
                && row.refclassid == PG_NAMESPACE_RELATION_OID
                && row.refobjid == PUBLIC_NAMESPACE_OID
                && row.refobjsubid == 0
                && row.deptype == DEPENDENCY_NORMAL
        }));
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_TYPE_RELATION_OID
                && row.objid == entry.row_type_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 0
                && row.deptype == DEPENDENCY_INTERNAL
        }));
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_ATTRDEF_RELATION_OID
                && row.objid == attrdef_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 2
                && row.deptype == DEPENDENCY_AUTO
        }));
    }

    #[test]
    fn catalog_store_persists_pg_index_rows() {
        let base = temp_dir("index_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let table = store
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
        let index = store
            .create_index(
                "people_name_idx",
                "people",
                true,
                &["id".into(), "name".into()],
            )
            .unwrap();

        let rows = load_physical_catalog_rows(&base).unwrap();
        let index_row = rows
            .indexes
            .iter()
            .find(|row| row.indexrelid == index.relation_oid)
            .unwrap();
        assert_eq!(index_row.indrelid, table.relation_oid);
        assert_eq!(index_row.indnatts, 2);
        assert_eq!(index_row.indnkeyatts, 2);
        assert!(index_row.indisunique);
        assert_eq!(index_row.indkey, "1 2");

        let class_row = rows
            .classes
            .iter()
            .find(|row| row.oid == index.relation_oid)
            .unwrap();
        assert_eq!(class_row.relkind, 'i');
        assert_eq!(class_row.relam, BTREE_AM_OID);
        assert_eq!(class_row.relpersistence, 'p');
        assert_eq!(class_row.relnamespace, PUBLIC_NAMESPACE_OID);
        assert_eq!(class_row.reltype, 0);

        let table_row = rows
            .classes
            .iter()
            .find(|row| row.oid == table.relation_oid)
            .unwrap();
        assert_eq!(table_row.relam, HEAP_TABLE_AM_OID);
        assert_eq!(table_row.relpersistence, 'p');

        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == index.relation_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == table.relation_oid
                && row.refobjsubid == 1
                && row.deptype == DEPENDENCY_AUTO
        }));
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == index.relation_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == table.relation_oid
                && row.refobjsubid == 2
                && row.deptype == DEPENDENCY_AUTO
        }));

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let reopened_index = reopened_catalog.get("people_name_idx").unwrap();
        assert_eq!(reopened_index.relkind, 'i');
        assert_eq!(
            reopened_index
                .index_meta
                .as_ref()
                .map(|meta| (meta.indrelid, meta.indkey.clone(), meta.indisunique)),
            Some((table.relation_oid, vec![1, 2], true))
        );
    }

    #[test]
    fn catalog_store_persists_pg_am_rows() {
        let base = temp_dir("am_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.ams.iter().any(|row| {
            row.oid == HEAP_TABLE_AM_OID
                && row.amname == "heap"
                && row.amhandler == 3
                && row.amtype == 't'
        }));
        assert!(rows.ams.iter().any(|row| {
            row.oid == BTREE_AM_OID
                && row.amname == "btree"
                && row.amhandler == 330
                && row.amtype == 'i'
        }));
    }

    #[test]
    fn catalog_store_persists_pg_authid_rows() {
        let base = temp_dir("authid_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.authids.iter().any(|row| {
            row.oid == BOOTSTRAP_SUPERUSER_OID
                && row.rolname == BOOTSTRAP_SUPERUSER_NAME
                && row.rolsuper
                && row.rolcreatedb
                && row.rolcanlogin
                && row.rolconnlimit == -1
        }));
    }

    #[test]
    fn catalog_store_persists_pg_auth_members_rows() {
        let base = temp_dir("auth_members_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.auth_members.is_empty());
    }

    #[test]
    fn catalog_store_persists_pg_language_rows() {
        let base = temp_dir("language_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.languages.iter().any(|row| {
            row.oid == PG_LANGUAGE_INTERNAL_OID
                && row.lanname == "internal"
                && row.lanowner == BOOTSTRAP_SUPERUSER_OID
        }));
        assert!(rows
            .languages
            .iter()
            .any(|row| row.lanname == "sql" && row.lanpltrusted));
    }

    #[test]
    fn catalog_store_persists_pg_proc_rows() {
        let base = temp_dir("proc_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.procs.iter().any(|row| {
            row.proname == "lower"
                && row.pronargs == 1
                && row.prorettype == TEXT_TYPE_OID
                && row.prokind == 'f'
                && row.prosrc == "lower"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "count"
                && row.pronargs == 1
                && row.prorettype == INT8_TYPE_OID
                && row.prokind == 'a'
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "numeric"
                && row.proargtypes == INT4_TYPE_OID.to_string()
                && row.prorettype == crate::include::catalog::NUMERIC_TYPE_OID
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "json_array_elements"
                && row.proretset
                && row.prorettype == JSON_TYPE_OID
        }));
    }

    #[test]
    fn catalog_store_persists_pg_collation_rows() {
        let base = temp_dir("collation_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert_eq!(
            rows.collations
                .iter()
                .map(|row| (row.oid, row.collname.as_str(), row.collprovider, row.collowner))
                .collect::<Vec<_>>(),
            vec![
                (
                    DEFAULT_COLLATION_OID,
                    "default",
                    'd',
                    BOOTSTRAP_SUPERUSER_OID,
                ),
                (C_COLLATION_OID, "C", 'c', BOOTSTRAP_SUPERUSER_OID),
                (POSIX_COLLATION_OID, "POSIX", 'c', BOOTSTRAP_SUPERUSER_OID),
            ]
        );
    }

    #[test]
    fn catalog_store_persists_pg_cast_rows() {
        let base = temp_dir("cast_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.casts.iter().any(|row| {
            row.castsource == INT4_TYPE_OID
                && row.casttarget == OID_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'i'
                && row.castmethod == 'b'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == INT4_TYPE_OID
                && row.casttarget == crate::include::catalog::NUMERIC_TYPE_OID
                && row.castfunc != 0
                && row.castcontext == 'i'
                && row.castmethod == 'f'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == VARCHAR_TYPE_OID
                && row.casttarget == TEXT_TYPE_OID
                && row.castcontext == 'i'
        }));
    }

    #[test]
    fn catalog_store_persists_pg_database_rows() {
        let base = temp_dir("database_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.databases.iter().any(|row| {
            row.oid == 1
                && row.datname == CURRENT_DATABASE_NAME
                && row.datdba == BOOTSTRAP_SUPERUSER_OID
                && row.dattablespace == DEFAULT_TABLESPACE_OID
                && !row.datistemplate
                && row.datallowconn
        }));
    }

    #[test]
    fn catalog_store_persists_pg_tablespace_rows() {
        let base = temp_dir("tablespace_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.tablespaces.iter().any(|row| {
            row.oid == DEFAULT_TABLESPACE_OID
                && row.spcname == "pg_default"
                && row.spcowner == BOOTSTRAP_SUPERUSER_OID
        }));
        assert!(rows.tablespaces.iter().any(|row| {
            row.oid == crate::include::catalog::GLOBAL_TABLESPACE_OID
                && row.spcname == "pg_global"
                && row.spcowner == BOOTSTRAP_SUPERUSER_OID
        }));
    }

    #[test]
    fn catalog_store_drop_table_cascades_indexes() {
        let base = temp_dir("drop_index_cascade");
        let mut store = CatalogStore::load(&base).unwrap();
        let table = store
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
        let index = store
            .create_index("people_name_idx", "people", false, &["name".into()])
            .unwrap();

        let dropped = store.drop_table("people").unwrap();
        assert_eq!(
            dropped.iter().map(|entry| entry.relation_oid).collect::<Vec<_>>(),
            vec![index.relation_oid, table.relation_oid]
        );
        assert_eq!(
            dropped.iter().map(|entry| entry.relkind).collect::<Vec<_>>(),
            vec!['i', 'r']
        );

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_none());
        assert!(reopened_catalog.get("people_name_idx").is_none());

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(!rows.classes.iter().any(|row| row.oid == table.relation_oid));
        assert!(!rows.classes.iter().any(|row| row.oid == index.relation_oid));
        assert!(!rows.indexes.iter().any(|row| row.indexrelid == index.relation_oid));
        assert!(!rows.depends.iter().any(|row| row.objid == index.relation_oid));
    }

    #[test]
    fn catalog_store_bootstraps_physical_core_catalog_relfiles() {
        let base = temp_dir("physical_bootstrap");
        let store = CatalogStore::load(&base).unwrap();
        let catalog = store.catalog_snapshot().unwrap();
        for name in ["pg_namespace", "pg_type", "pg_attribute", "pg_class"] {
            let entry = catalog.get(name).unwrap();
            let path = segment_path(&base, entry.rel, ForkNumber::Main, 0);
            let meta = fs::metadata(path).unwrap();
            assert!(meta.len() > 0, "{name} should have heap data");
        }

        let attrdef = catalog.get("pg_attrdef").unwrap();
        let attrdef_path = segment_path(&base, attrdef.rel, ForkNumber::Main, 0);
        assert!(attrdef_path.exists(), "pg_attrdef relfile should exist");
        let depend = catalog.get("pg_depend").unwrap();
        let depend_path = segment_path(&base, depend.rel, ForkNumber::Main, 0);
        assert!(depend_path.exists(), "pg_depend relfile should exist");
        let index = catalog.get("pg_index").unwrap();
        let index_path = segment_path(&base, index.rel, ForkNumber::Main, 0);
        assert!(index_path.exists(), "pg_index relfile should exist");
        let database = catalog.get("pg_database").unwrap();
        let database_path = segment_path(&base, database.rel, ForkNumber::Main, 0);
        assert!(database_path.exists(), "pg_database relfile should exist");
        let authid = catalog.get("pg_authid").unwrap();
        let authid_path = segment_path(&base, authid.rel, ForkNumber::Main, 0);
        assert!(authid_path.exists(), "pg_authid relfile should exist");
        let auth_members = catalog.get("pg_auth_members").unwrap();
        let auth_members_path = segment_path(&base, auth_members.rel, ForkNumber::Main, 0);
        assert!(auth_members_path.exists(), "pg_auth_members relfile should exist");
        let collation = catalog.get("pg_collation").unwrap();
        let collation_path = segment_path(&base, collation.rel, ForkNumber::Main, 0);
        assert!(collation_path.exists(), "pg_collation relfile should exist");
        let language = catalog.get("pg_language").unwrap();
        let language_path = segment_path(&base, language.rel, ForkNumber::Main, 0);
        assert!(language_path.exists(), "pg_language relfile should exist");
        let proc = catalog.get("pg_proc").unwrap();
        let proc_path = segment_path(&base, proc.rel, ForkNumber::Main, 0);
        assert!(proc_path.exists(), "pg_proc relfile should exist");
        let cast = catalog.get("pg_cast").unwrap();
        let cast_path = segment_path(&base, cast.rel, ForkNumber::Main, 0);
        assert!(cast_path.exists(), "pg_cast relfile should exist");
        let am = catalog.get("pg_am").unwrap();
        let am_path = segment_path(&base, am.rel, ForkNumber::Main, 0);
        assert!(am_path.exists(), "pg_am relfile should exist");
        let tablespace = catalog.get("pg_tablespace").unwrap();
        let tablespace_path = segment_path(&base, tablespace.rel, ForkNumber::Main, 0);
        assert!(tablespace_path.exists(), "pg_tablespace relfile should exist");
    }

    #[test]
    fn catalog_store_loads_from_physical_catalogs_without_schema_file() {
        let base = temp_dir("physical_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_table(
                "shipments",
                RelationDesc {
                    columns: vec![column_desc(
                        "tags",
                        SqlType::array_of(SqlType::new(SqlTypeKind::Varchar)),
                        true,
                    )],
                },
            )
            .unwrap();
        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let entry = reopened_catalog.get("shipments").unwrap();
        assert_eq!(
            entry.desc.columns[0].sql_type,
            SqlType::array_of(SqlType::new(SqlTypeKind::Varchar))
        );
    }

    #[test]
    fn catalog_store_roundtrips_zero_column_tables() {
        let base = temp_dir("zero_columns");
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_table(
                "zerocol",
                RelationDesc {
                    columns: Vec::new(),
                },
            )
            .unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let entry = reopened_catalog.get("zerocol").unwrap();
        assert!(entry.desc.columns.is_empty());
    }

    #[test]
    fn catalog_store_preserves_relation_allocators_across_drop_and_reload() {
        let base = temp_dir("allocator_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let first = store
            .create_table(
                "first",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();
        store.drop_table("first").unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let mut reopened = reopened;
        let second = reopened
            .create_table(
                "second",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        assert!(second.rel.rel_number > first.rel.rel_number);
        assert!(second.relation_oid > first.relation_oid);
        assert!(second.row_type_oid > first.row_type_oid);
    }

    #[test]
    fn catalog_store_migrates_legacy_defaults_json_into_pg_attrdef() {
        let base = temp_dir("legacy_defaults_migration");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'legacy'".into());
        let entry = store.create_table("notes", desc).unwrap();

        let attrdef_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgAttrdef.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&attrdef_path).unwrap();
        let legacy_dir = base.join("catalog");
        fs::create_dir_all(&legacy_dir).unwrap();
        fs::write(
            legacy_dir.join("defaults.json"),
            format!(
                r#"[{{"relation_oid":{},"attnum":2,"expr":"'legacy'"}}]"#,
                entry.relation_oid
            ),
        )
        .unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let relcache = reopened.relcache().unwrap();
        let migrated = relcache.get_by_name("notes").unwrap();
        assert_eq!(migrated.desc.columns[1].default_expr.as_deref(), Some("'legacy'"));
        assert!(migrated.desc.columns[1].attrdef_oid.is_some());

        let rows = load_physical_catalog_rows(&base).unwrap();
        let attrdef = rows
            .attrdefs
            .iter()
            .find(|row| row.adrelid == entry.relation_oid && row.adnum == 2)
            .unwrap();
        assert_eq!(attrdef.adbin, "'legacy'");
        assert!(attrdef.oid > entry.row_type_oid);
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_depend_relation() {
        let base = temp_dir("missing_depend_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let depend_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgDepend.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&depend_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(reopened.base_dir()).unwrap();
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == entry.relation_oid
                && row.refclassid == PG_NAMESPACE_RELATION_OID
                && row.refobjid == PUBLIC_NAMESPACE_OID
        }));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_index_relation() {
        let base = temp_dir("missing_index_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let index_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgIndex.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&index_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(index_path.exists(), "pg_index relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.indexes.is_empty());
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_am_relation() {
        let base = temp_dir("missing_am_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let am_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgAm.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&am_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(am_path.exists(), "pg_am relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.ams.iter().any(|row| row.oid == HEAP_TABLE_AM_OID && row.amname == "heap"));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_database_relation() {
        let base = temp_dir("missing_database_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let database_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgDatabase.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&database_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(database_path.exists(), "pg_database relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.databases.iter().any(|row| row.datname == CURRENT_DATABASE_NAME));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_authid_relation() {
        let base = temp_dir("missing_authid_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let authid_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgAuthId.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&authid_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(authid_path.exists(), "pg_authid relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.authids.iter().any(|row| {
            row.oid == BOOTSTRAP_SUPERUSER_OID && row.rolname == BOOTSTRAP_SUPERUSER_NAME
        }));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_auth_members_relation() {
        let base = temp_dir("missing_auth_members_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let auth_members_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgAuthMembers.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&auth_members_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(
            auth_members_path.exists(),
            "pg_auth_members relfile should be recreated"
        );

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.auth_members.is_empty());
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_collation_relation() {
        let base = temp_dir("missing_collation_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let collation_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgCollation.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&collation_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(collation_path.exists(), "pg_collation relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows
            .collations
            .iter()
            .any(|row| row.oid == DEFAULT_COLLATION_OID && row.collname == "default"));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_cast_relation() {
        let base = temp_dir("missing_cast_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let cast_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgCast.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&cast_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(cast_path.exists(), "pg_cast relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.casts.iter().any(|row| {
            row.castsource == INT4_TYPE_OID && row.casttarget == OID_TYPE_OID
        }));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_proc_relation() {
        let base = temp_dir("missing_proc_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let proc_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgProc.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&proc_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(proc_path.exists(), "pg_proc relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows
            .procs
            .iter()
            .any(|row| row.proname == "lower" && row.prorettype == TEXT_TYPE_OID));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_language_relation() {
        let base = temp_dir("missing_language_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let language_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgLanguage.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&language_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(language_path.exists(), "pg_language relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows
            .languages
            .iter()
            .any(|row| row.oid == PG_LANGUAGE_INTERNAL_OID && row.lanname == "internal"));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }

    #[test]
    fn catalog_store_rebuilds_missing_pg_tablespace_relation() {
        let base = temp_dir("missing_tablespace_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let tablespace_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgTablespace.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&tablespace_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_some());
        assert!(tablespace_path.exists(), "pg_tablespace relfile should be recreated");

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.tablespaces.iter().any(|row| {
            row.oid == DEFAULT_TABLESPACE_OID
                && row.spcname == "pg_default"
                && row.spcowner == BOOTSTRAP_SUPERUSER_OID
        }));
        assert!(rows.classes.iter().any(|row| row.oid == entry.relation_oid));
    }
}
