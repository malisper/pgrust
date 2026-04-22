use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::backend::catalog::catalog::scalar_type_for_sql_type;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::cache::relcache::{IndexRelCacheEntry, RelCache, RelCacheEntry};
use crate::include::access::tupdesc::AttributeDesc;
use crate::include::catalog::{BootstrapCatalogKind, CatalogScope};
use crate::include::nodes::parsenodes::SqlType;
use crate::include::nodes::primnodes::{ColumnDesc, RelationDesc};
use crate::pgrust::database::default_sequence_oid_from_default_expr;

const RELCACHE_INIT_MAGIC: u32 = 0x5052_494E;
const RELCACHE_INIT_VERSION: u32 = 5;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum RelCacheInitScopeFile {
    Shared,
    Database { db_oid: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RelCacheInitFile {
    magic: u32,
    version: u32,
    scope: RelCacheInitScopeFile,
    entries: Vec<RelCacheInitNameEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RelCacheInitNameEntry {
    name: String,
    entry: RelCacheEntryFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RelCacheEntryFile {
    rel: RelFileLocator,
    relation_oid: u32,
    namespace_oid: u32,
    owner_oid: u32,
    row_type_oid: u32,
    array_type_oid: u32,
    reltoastrelid: u32,
    relpersistence: char,
    relkind: char,
    relhastriggers: bool,
    relrowsecurity: bool,
    relforcerowsecurity: bool,
    desc: RelationDescFile,
    index: Option<IndexRelCacheEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RelationDescFile {
    columns: Vec<ColumnDescFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ColumnDescFile {
    name: String,
    storage: AttributeDesc,
    sql_type: SqlType,
    dropped: bool,
    attstattarget: i16,
    attinhcount: i16,
    attislocal: bool,
    not_null_constraint_oid: Option<u32>,
    not_null_constraint_name: Option<String>,
    not_null_constraint_validated: bool,
    not_null_constraint_is_local: bool,
    not_null_constraint_inhcount: i16,
    not_null_constraint_no_inherit: bool,
    not_null_primary_key_owned: bool,
    attrdef_oid: Option<u32>,
    default_expr: Option<String>,
}

pub(super) fn relcache_init_path_for_scope(base_dir: &Path, scope: CatalogScope) -> PathBuf {
    match scope {
        CatalogScope::Shared => base_dir.join("global").join("pg_internal.init"),
        CatalogScope::Database(db_oid) => base_dir
            .join("base")
            .join(db_oid.to_string())
            .join("pg_internal.init"),
    }
}

pub(super) fn load_relcache_init_file(base_dir: &Path, scope: CatalogScope) -> Option<RelCache> {
    let path = relcache_init_path_for_scope(base_dir, scope);
    let bytes = fs::read(&path).ok()?;
    let file = match serde_json::from_slice::<RelCacheInitFile>(&bytes) {
        Ok(file) => file,
        Err(_) => {
            let _ = fs::remove_file(&path);
            return None;
        }
    };
    if file.magic != RELCACHE_INIT_MAGIC
        || file.version != RELCACHE_INIT_VERSION
        || !scope_matches_file(scope, &file.scope)
    {
        let _ = fs::remove_file(&path);
        return None;
    }

    let mut cache = RelCache::default();
    for entry in file.entries {
        let relcache_entry = relcache_entry_from_file(entry.entry);
        if relcache_entry_belongs_in_init(&relcache_entry) {
            cache.insert(entry.name, relcache_entry);
        }
    }
    Some(cache)
}

pub(super) fn persist_relcache_init_file(
    base_dir: &Path,
    scope: CatalogScope,
    relcache: &RelCache,
) {
    let path = relcache_init_path_for_scope(base_dir, scope);
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let file = RelCacheInitFile {
        magic: RELCACHE_INIT_MAGIC,
        version: RELCACHE_INIT_VERSION,
        scope: scope_to_file(scope),
        entries: relcache
            .entries()
            .filter(|(_, entry)| relcache_entry_belongs_in_init(entry))
            .map(|(name, entry)| RelCacheInitNameEntry {
                name: name.to_string(),
                entry: relcache_entry_to_file(entry),
            })
            .collect(),
    };
    if let Ok(bytes) = serde_json::to_vec(&file) {
        let _ = fs::write(path, bytes);
    }
}

pub(super) fn invalidate_relcache_init_file(base_dir: &Path, scope: CatalogScope) {
    let path = relcache_init_path_for_scope(base_dir, scope);
    let _ = fs::remove_file(path);
}

pub(super) fn relcache_init_needs_invalidation(kinds: &[BootstrapCatalogKind]) -> bool {
    kinds.iter().any(|kind| {
        matches!(
            kind,
            BootstrapCatalogKind::PgNamespace
                | BootstrapCatalogKind::PgClass
                | BootstrapCatalogKind::PgAttribute
                | BootstrapCatalogKind::PgAttrdef
                | BootstrapCatalogKind::PgType
                | BootstrapCatalogKind::PgConstraint
                | BootstrapCatalogKind::PgInherits
                | BootstrapCatalogKind::PgIndex
        )
    })
}

fn relcache_entry_belongs_in_init(entry: &RelCacheEntry) -> bool {
    entry.relpersistence != 't'
}

fn scope_to_file(scope: CatalogScope) -> RelCacheInitScopeFile {
    match scope {
        CatalogScope::Shared => RelCacheInitScopeFile::Shared,
        CatalogScope::Database(db_oid) => RelCacheInitScopeFile::Database { db_oid },
    }
}

fn scope_matches_file(scope: CatalogScope, file_scope: &RelCacheInitScopeFile) -> bool {
    match (scope, file_scope) {
        (CatalogScope::Shared, RelCacheInitScopeFile::Shared) => true,
        (CatalogScope::Database(expected), RelCacheInitScopeFile::Database { db_oid }) => {
            expected == *db_oid
        }
        _ => false,
    }
}

fn relcache_entry_to_file(entry: &RelCacheEntry) -> RelCacheEntryFile {
    RelCacheEntryFile {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        namespace_oid: entry.namespace_oid,
        owner_oid: entry.owner_oid,
        row_type_oid: entry.row_type_oid,
        array_type_oid: entry.array_type_oid,
        reltoastrelid: entry.reltoastrelid,
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relhastriggers: entry.relhastriggers,
        relrowsecurity: entry.relrowsecurity,
        relforcerowsecurity: entry.relforcerowsecurity,
        desc: RelationDescFile {
            columns: entry.desc.columns.iter().map(column_desc_to_file).collect(),
        },
        index: entry.index.clone(),
    }
}

fn relcache_entry_from_file(entry: RelCacheEntryFile) -> RelCacheEntry {
    RelCacheEntry {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        namespace_oid: entry.namespace_oid,
        owner_oid: entry.owner_oid,
        row_type_oid: entry.row_type_oid,
        array_type_oid: entry.array_type_oid,
        reltoastrelid: entry.reltoastrelid,
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relhastriggers: entry.relhastriggers,
        relrowsecurity: entry.relrowsecurity,
        relforcerowsecurity: entry.relforcerowsecurity,
        desc: RelationDesc {
            columns: entry
                .desc
                .columns
                .into_iter()
                .map(column_desc_from_file)
                .collect(),
        },
        index: entry.index,
    }
}

fn column_desc_to_file(column: &ColumnDesc) -> ColumnDescFile {
    ColumnDescFile {
        name: column.name.clone(),
        storage: column.storage.clone(),
        sql_type: column.sql_type,
        dropped: column.dropped,
        attstattarget: column.attstattarget,
        attinhcount: column.attinhcount,
        attislocal: column.attislocal,
        not_null_constraint_oid: column.not_null_constraint_oid,
        not_null_constraint_name: column.not_null_constraint_name.clone(),
        not_null_constraint_validated: column.not_null_constraint_validated,
        not_null_constraint_is_local: column.not_null_constraint_is_local,
        not_null_constraint_inhcount: column.not_null_constraint_inhcount,
        not_null_constraint_no_inherit: column.not_null_constraint_no_inherit,
        not_null_primary_key_owned: column.not_null_primary_key_owned,
        attrdef_oid: column.attrdef_oid,
        default_expr: column.default_expr.clone(),
    }
}

fn column_desc_from_file(column: ColumnDescFile) -> ColumnDesc {
    let default_sequence_oid = column
        .default_expr
        .as_deref()
        .and_then(default_sequence_oid_from_default_expr);
    ColumnDesc {
        name: column.name,
        storage: column.storage,
        ty: scalar_type_for_sql_type(column.sql_type),
        sql_type: column.sql_type,
        dropped: column.dropped,
        attstattarget: column.attstattarget,
        attinhcount: column.attinhcount,
        attislocal: column.attislocal,
        not_null_constraint_oid: column.not_null_constraint_oid,
        not_null_constraint_name: column.not_null_constraint_name,
        not_null_constraint_validated: column.not_null_constraint_validated,
        not_null_constraint_is_local: column.not_null_constraint_is_local,
        not_null_constraint_inhcount: column.not_null_constraint_inhcount,
        not_null_constraint_no_inherit: column.not_null_constraint_no_inherit,
        not_null_primary_key_owned: column.not_null_primary_key_owned,
        attrdef_oid: column.attrdef_oid,
        default_expr: column.default_expr,
        default_sequence_oid,
        missing_default_value: None,
    }
}
