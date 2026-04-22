use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::backend::catalog::CatalogError;
use crate::backend::catalog::bootstrap::bootstrap_catalog_rel;
use crate::backend::catalog::catalog::{Catalog, CatalogEntry, column_desc};
use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlType;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::cache::catcache::{CatCache, normalize_catalog_name, sql_type_oid};
use crate::include::catalog::{
    ANYOID, CONSTRAINT_NOTNULL, CONSTRAINT_PRIMARY, PG_CATALOG_NAMESPACE_OID,
    PG_CONSTRAINT_RELATION_OID, bootstrap_catalog_kinds, system_catalog_index_by_oid,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexAmOpEntry {
    pub strategy: i16,
    pub purpose: char,
    pub lefttype: u32,
    pub righttype: u32,
    pub operator_oid: u32,
    pub operator_proc_oid: u32,
    pub sortfamily_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexAmProcEntry {
    pub procnum: i16,
    pub lefttype: u32,
    pub righttype: u32,
    pub proc_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRelCacheEntry {
    pub indexrelid: u32,
    pub indrelid: u32,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    pub am_oid: u32,
    pub am_handler_oid: Option<u32>,
    pub indkey: Vec<i16>,
    pub indclass: Vec<u32>,
    pub indcollation: Vec<u32>,
    pub indoption: Vec<i16>,
    pub opfamily_oids: Vec<u32>,
    pub opcintype_oids: Vec<u32>,
    pub opckeytype_oids: Vec<u32>,
    pub amop_entries: Vec<Vec<IndexAmOpEntry>>,
    pub amproc_entries: Vec<Vec<IndexAmProcEntry>>,
    pub indexprs: Option<String>,
    pub indpred: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ResolvedIndexSupportMetadata {
    opfamily_oids: Vec<u32>,
    opcintype_oids: Vec<u32>,
    opckeytype_oids: Vec<u32>,
    amop_entries: Vec<Vec<IndexAmOpEntry>>,
    amproc_entries: Vec<Vec<IndexAmProcEntry>>,
}

#[derive(Debug, Clone)]
struct IndexSupportLookup {
    am_rows: Vec<crate::include::catalog::PgAmRow>,
    opclass_rows: Vec<crate::include::catalog::PgOpclassRow>,
    amop_rows: Vec<crate::include::catalog::PgAmopRow>,
    amproc_rows: Vec<crate::include::catalog::PgAmprocRow>,
    operator_rows: Vec<crate::include::catalog::PgOperatorRow>,
}

impl IndexSupportLookup {
    fn from_catcache(catcache: &CatCache) -> Self {
        Self {
            am_rows: catcache.am_rows(),
            opclass_rows: catcache.opclass_rows(),
            amop_rows: catcache.amop_rows(),
            amproc_rows: catcache.amproc_rows(),
            operator_rows: catcache.operator_rows(),
        }
    }

    fn am_handler_oid(&self, am_oid: u32) -> Option<u32> {
        self.am_rows
            .iter()
            .find(|am| am.oid == am_oid)
            .map(|am| am.amhandler)
    }

    fn resolve(&self, indclass: &[u32]) -> ResolvedIndexSupportMetadata {
        let resolved_opclasses = indclass
            .iter()
            .filter_map(|oid| self.opclass_rows.iter().find(|row| row.oid == *oid))
            .collect::<Vec<_>>();
        let opfamily_oids = resolved_opclasses
            .iter()
            .map(|row| row.opcfamily)
            .collect::<Vec<_>>();
        let opcintype_oids = resolved_opclasses
            .iter()
            .map(|row| row.opcintype)
            .collect::<Vec<_>>();
        let opckeytype_oids = resolved_opclasses
            .iter()
            .map(|row| row.opckeytype)
            .collect::<Vec<_>>();
        let amop_entries = opfamily_oids
            .iter()
            .map(|family_oid| {
                self.amop_rows
                    .iter()
                    .filter(|row| row.amopfamily == *family_oid)
                    .map(|row| IndexAmOpEntry {
                        strategy: row.amopstrategy,
                        purpose: row.amoppurpose,
                        lefttype: row.amoplefttype,
                        righttype: row.amoprighttype,
                        operator_oid: row.amopopr,
                        operator_proc_oid: self
                            .operator_rows
                            .iter()
                            .find(|operator| operator.oid == row.amopopr)
                            .map(|operator| operator.oprcode)
                            .unwrap_or(0),
                        sortfamily_oid: row.amopsortfamily,
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let amproc_entries = opfamily_oids
            .iter()
            .map(|family_oid| {
                self.amproc_rows
                    .iter()
                    .filter(|row| row.amprocfamily == *family_oid)
                    .map(|row| IndexAmProcEntry {
                        procnum: row.amprocnum,
                        lefttype: row.amproclefttype,
                        righttype: row.amprocrighttype,
                        proc_oid: row.amproc,
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        ResolvedIndexSupportMetadata {
            opfamily_oids,
            opcintype_oids,
            opckeytype_oids,
            amop_entries,
            amproc_entries,
        }
    }
}

impl IndexRelCacheEntry {
    fn indexed_operand_type_oid(&self, desc: &RelationDesc, column_index: usize) -> Option<u32> {
        self.opckeytype_oids
            .get(column_index)
            .copied()
            .filter(|oid| *oid != 0)
            .or_else(|| {
                desc.columns
                    .get(column_index)
                    .map(|column| sql_type_oid(column.sql_type))
            })
    }

    fn type_match_score(
        entry_lefttype: u32,
        entry_righttype: u32,
        left_type_oid: Option<u32>,
        right_type_oid: Option<u32>,
    ) -> Option<u8> {
        fn component_score(entry_type: u32, actual_type: Option<u32>) -> Option<u8> {
            match actual_type {
                None => Some(0),
                Some(actual) if entry_type == actual => Some(2),
                Some(_) if entry_type == ANYOID => Some(1),
                Some(_) => None,
            }
        }

        Some(
            component_score(entry_lefttype, left_type_oid)?
                + component_score(entry_righttype, right_type_oid)?,
        )
    }

    pub fn amproc_oid(
        &self,
        desc: &RelationDesc,
        column_index: usize,
        procnum: i16,
    ) -> Option<u32> {
        let operand_type_oid = self.indexed_operand_type_oid(desc, column_index);
        let mut best: Option<(u8, u32)> = None;
        for entry in self.amproc_entries.get(column_index)?.iter() {
            if entry.procnum != procnum {
                continue;
            }
            let Some(score) = Self::type_match_score(
                entry.lefttype,
                entry.righttype,
                operand_type_oid,
                operand_type_oid,
            ) else {
                continue;
            };
            if best.is_none_or(|(best_score, _)| score > best_score) {
                best = Some((score, entry.proc_oid));
            }
        }
        best.map(|(_, proc_oid)| proc_oid)
    }

    pub fn amop_strategy_for_operator(
        &self,
        desc: &RelationDesc,
        column_index: usize,
        operator_oid: u32,
        right_type_oid: Option<u32>,
    ) -> Option<u16> {
        self.amop_strategy_matching(desc, column_index, right_type_oid, Some('s'), |entry| {
            entry.operator_oid == operator_oid
        })
    }

    pub fn amop_ordering_strategy_for_operator(
        &self,
        desc: &RelationDesc,
        column_index: usize,
        operator_oid: u32,
        right_type_oid: Option<u32>,
    ) -> Option<u16> {
        self.amop_strategy_matching(desc, column_index, right_type_oid, Some('o'), |entry| {
            entry.operator_oid == operator_oid
        })
    }

    pub fn amop_strategy_for_proc(
        &self,
        desc: &RelationDesc,
        column_index: usize,
        operator_proc_oid: u32,
        right_type_oid: Option<u32>,
    ) -> Option<u16> {
        self.amop_strategy_matching(desc, column_index, right_type_oid, Some('s'), |entry| {
            entry.operator_proc_oid == operator_proc_oid
        })
    }

    pub fn amop_ordering_strategy_for_proc(
        &self,
        desc: &RelationDesc,
        column_index: usize,
        operator_proc_oid: u32,
        right_type_oid: Option<u32>,
    ) -> Option<u16> {
        self.amop_strategy_matching(desc, column_index, right_type_oid, Some('o'), |entry| {
            entry.operator_proc_oid == operator_proc_oid
        })
    }

    fn amop_strategy_matching(
        &self,
        desc: &RelationDesc,
        column_index: usize,
        right_type_oid: Option<u32>,
        purpose: Option<char>,
        predicate: impl Fn(&IndexAmOpEntry) -> bool,
    ) -> Option<u16> {
        let left_type_oid = self.indexed_operand_type_oid(desc, column_index);
        let mut best: Option<(u8, i16)> = None;
        for entry in self.amop_entries.get(column_index)?.iter() {
            if purpose.is_some_and(|purpose| entry.purpose != purpose) || !predicate(entry) {
                continue;
            }
            let Some(score) = Self::type_match_score(
                entry.lefttype,
                entry.righttype,
                left_type_oid,
                right_type_oid,
            ) else {
                continue;
            };
            if best.is_none_or(|(best_score, _)| score > best_score) {
                best = Some((score, entry.strategy));
            }
        }
        best.and_then(|(_, strategy)| u16::try_from(strategy).ok())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelCacheEntry {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub namespace_oid: u32,
    pub owner_oid: u32,
    pub row_type_oid: u32,
    pub array_type_oid: u32,
    pub reltoastrelid: u32,
    pub relpersistence: char,
    pub relkind: char,
    pub relhastriggers: bool,
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    pub desc: RelationDesc,
    pub index: Option<IndexRelCacheEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct RelCache {
    by_name: BTreeMap<String, RelCacheEntry>,
    by_oid: BTreeMap<u32, RelCacheEntry>,
}

impl RelCache {
    pub fn from_catalog(catalog: &Catalog) -> Self {
        let mut cache = Self::default();
        let catcache = CatCache::from_catalog(catalog);
        let support_lookup = IndexSupportLookup::from_catcache(&catcache);
        for (name, entry) in catalog.entries() {
            let relcache_entry = from_catalog_entry(entry, &support_lookup);
            cache.by_name.insert(
                normalize_catalog_name(name).to_ascii_lowercase(),
                relcache_entry.clone(),
            );
            if let Some(namespace) = catcache.namespace_by_oid(entry.namespace_oid) {
                let relname = name.rsplit('.').next().unwrap_or(name).to_ascii_lowercase();
                cache.by_name.insert(
                    format!("{}.{}", namespace.nspname.to_ascii_lowercase(), relname),
                    relcache_entry.clone(),
                );
            }
            cache.by_oid.insert(entry.relation_oid, relcache_entry);
        }
        cache
    }

    pub fn from_physical(base_dir: &Path) -> Result<Self, CatalogError> {
        let catcache = CatCache::from_physical(base_dir)?;
        Self::from_catcache(&catcache)
    }

    pub fn from_catcache(catcache: &CatCache) -> Result<Self, CatalogError> {
        Self::from_catcache_in_db(catcache, 1)
    }

    pub fn from_catcache_in_db(
        catcache: &CatCache,
        current_db_oid: u32,
    ) -> Result<Self, CatalogError> {
        let mut cache = Self::default();
        let support_lookup = IndexSupportLookup::from_catcache(catcache);
        let index_rows = catcache.index_rows();
        let not_null_constraints = catcache
            .constraint_rows()
            .into_iter()
            .filter(|row| row.contype == CONSTRAINT_NOTNULL)
            .filter_map(|row| {
                let attnum = *row.conkey.as_ref()?.first()?;
                Some(((row.conrelid, attnum), row))
            })
            .collect::<BTreeMap<_, _>>();
        let primary_constraint_oids = catcache
            .constraint_rows()
            .into_iter()
            .filter(|row| row.contype == CONSTRAINT_PRIMARY)
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        let pk_owned_not_null = catcache
            .depend_rows()
            .into_iter()
            .filter(|row| {
                row.classid == PG_CONSTRAINT_RELATION_OID
                    && row.refclassid == PG_CONSTRAINT_RELATION_OID
                    && primary_constraint_oids.contains(&row.refobjid)
            })
            .map(|row| row.objid)
            .collect::<BTreeSet<_>>();
        for class in catcache.class_rows() {
            let attrs = catcache.attributes_by_relid(class.oid).unwrap_or(&[]);
            let columns = match attrs
                .iter()
                .map(|attr| {
                    let sql_type = catcache
                        .type_by_oid(attr.atttypid)
                        .map(|ty| ty.sql_type)
                        .ok_or(CatalogError::Corrupt("unknown atttypid"))?;
                    let mut desc = column_desc(
                        attr.attname.clone(),
                        SqlType {
                            typmod: attr.atttypmod,
                            ..sql_type
                        },
                        !attr.attnotnull,
                    );
                    desc.storage.attlen = attr.attlen;
                    desc.storage.attalign = attr.attalign;
                    desc.storage.attstorage = attr.attstorage;
                    desc.storage.attcompression = attr.attcompression;
                    desc.attstattarget = attr.attstattarget;
                    desc.attinhcount = attr.attinhcount;
                    desc.attislocal = attr.attislocal;
                    desc.dropped = attr.attisdropped;
                    if let Some(constraint) = not_null_constraints.get(&(class.oid, attr.attnum)) {
                        desc.not_null_constraint_oid = Some(constraint.oid);
                        desc.not_null_constraint_name = Some(constraint.conname.clone());
                        desc.not_null_constraint_validated = constraint.convalidated;
                        desc.not_null_constraint_is_local = constraint.conislocal;
                        desc.not_null_constraint_inhcount = constraint.coninhcount;
                        desc.not_null_constraint_no_inherit = constraint.connoinherit;
                        desc.not_null_primary_key_owned =
                            pk_owned_not_null.contains(&constraint.oid);
                    }
                    if let Some(attrdef) = catcache.attrdef_by_relid_attnum(class.oid, attr.attnum)
                    {
                        desc.attrdef_oid = Some(attrdef.oid);
                        desc.default_expr = Some(attrdef.adbin.clone());
                        desc.default_sequence_oid =
                            crate::pgrust::database::default_sequence_oid_from_default_expr(
                                &attrdef.adbin,
                            );
                        // Avoid reparsing every catalog default during relcache rebuilds.
                        // `missing_column_value` can still derive literal defaults lazily.
                        desc.missing_default_value = None;
                    }
                    Ok(desc)
                })
                .collect::<Result<Vec<_>, CatalogError>>()
            {
                Ok(columns) => columns,
                // :HACK: RelCache currently rebuilds eagerly from every relation in the
                // catalog. Skip non-system relations with dangling type refs so one broken
                // user relation cannot make the entire catalog unreadable. The PG-like end
                // state is to open relcache entries lazily and surface corruption per
                // relation instead of failing the whole cache rebuild.
                Err(CatalogError::Corrupt("unknown atttypid"))
                    if class.relnamespace != PG_CATALOG_NAMESPACE_OID =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            };
            let entry = RelCacheEntry {
                rel: relation_locator_for_class_row(class.oid, class.relfilenode, current_db_oid),
                relation_oid: class.oid,
                namespace_oid: class.relnamespace,
                owner_oid: class.relowner,
                row_type_oid: class.reltype,
                array_type_oid: catcache
                    .type_by_oid(class.reltype)
                    .map(|row| row.typarray)
                    .unwrap_or(0),
                reltoastrelid: class.reltoastrelid,
                relpersistence: class.relpersistence,
                relkind: class.relkind,
                relhastriggers: class.relhastriggers,
                relrowsecurity: class.relrowsecurity,
                relforcerowsecurity: class.relforcerowsecurity,
                desc: RelationDesc { columns },
                index: class.relkind.eq(&'i').then(|| {
                    let Some(index) = index_rows.iter().find(|row| row.indexrelid == class.oid)
                    else {
                        return IndexRelCacheEntry {
                            indexrelid: class.oid,
                            indrelid: 0,
                            indnatts: 0,
                            indnkeyatts: 0,
                            indisunique: false,
                            indnullsnotdistinct: false,
                            indisprimary: false,
                            indisexclusion: false,
                            indimmediate: false,
                            indisclustered: false,
                            indisvalid: false,
                            indcheckxmin: false,
                            indisready: false,
                            indislive: false,
                            indisreplident: false,
                            am_oid: class.relam,
                            am_handler_oid: support_lookup.am_handler_oid(class.relam),
                            indkey: Vec::new(),
                            indclass: Vec::new(),
                            indcollation: Vec::new(),
                            indoption: Vec::new(),
                            opfamily_oids: Vec::new(),
                            opcintype_oids: Vec::new(),
                            opckeytype_oids: Vec::new(),
                            amop_entries: Vec::new(),
                            amproc_entries: Vec::new(),
                            indexprs: None,
                            indpred: None,
                        };
                    };
                    let indclass = index.indclass.clone();
                    let support = support_lookup.resolve(&indclass);
                    IndexRelCacheEntry {
                        indexrelid: class.oid,
                        indrelid: index.indrelid,
                        indnatts: index.indnatts,
                        indnkeyatts: index.indnkeyatts,
                        indisunique: index.indisunique,
                        indnullsnotdistinct: index.indnullsnotdistinct,
                        indisprimary: index.indisprimary,
                        indisexclusion: index.indisexclusion,
                        indimmediate: index.indimmediate,
                        indisclustered: index.indisclustered,
                        indisvalid: index.indisvalid,
                        indcheckxmin: index.indcheckxmin,
                        indisready: index.indisready,
                        indislive: index.indislive,
                        indisreplident: index.indisreplident,
                        am_oid: class.relam,
                        am_handler_oid: support_lookup.am_handler_oid(class.relam),
                        indkey: index.indkey.clone(),
                        indclass,
                        indcollation: index.indcollation.clone(),
                        indoption: index.indoption.clone(),
                        opfamily_oids: support.opfamily_oids,
                        opcintype_oids: support.opcintype_oids,
                        opckeytype_oids: support.opckeytype_oids,
                        amop_entries: support.amop_entries,
                        amproc_entries: support.amproc_entries,
                        indexprs: index.indexprs.clone(),
                        indpred: index.indpred.clone(),
                    }
                }),
            };
            let relname = class.relname.to_ascii_lowercase();
            if class.relpersistence != 't' {
                cache.by_name.insert(relname.clone(), entry.clone());
            }
            if let Some(namespace) = catcache.namespace_by_oid(class.relnamespace) {
                let qualified = format!("{}.{}", namespace.nspname.to_ascii_lowercase(), relname);
                cache.by_name.insert(qualified, entry.clone());
            }
            cache.by_oid.insert(class.oid, entry);
        }
        Ok(cache)
    }

    pub fn get_by_name(&self, name: &str) -> Option<&RelCacheEntry> {
        self.by_name
            .get(&normalize_catalog_name(name).to_ascii_lowercase())
    }

    pub fn get_by_name_exact(&self, name: &str) -> Option<&RelCacheEntry> {
        self.by_name.get(&name.to_ascii_lowercase())
    }

    pub fn get_by_oid(&self, oid: u32) -> Option<&RelCacheEntry> {
        self.by_oid.get(&oid)
    }

    pub fn with_search_path(&self, search_path: &[String]) -> Self {
        let mut cache = Self {
            by_name: BTreeMap::new(),
            by_oid: self.by_oid.clone(),
        };

        for (name, entry) in &self.by_name {
            if name.contains('.') {
                cache.by_name.insert(name.clone(), entry.clone());
            }
        }

        for schema_name in search_path.iter().rev() {
            let prefix = format!("{}.", schema_name.to_ascii_lowercase());
            for (name, entry) in &self.by_name {
                if !name.starts_with(&prefix) {
                    continue;
                }
                if let Some((_, unqualified)) = name.rsplit_once('.') {
                    cache.by_name.insert(unqualified.to_string(), entry.clone());
                }
            }
        }

        // :HACK: `get_by_name()` still normalizes `pg_catalog.foo` to `foo`,
        // so keep catalog aliases visible even when rebuilding unqualified
        // names from the current search path.
        for (name, entry) in &self.by_name {
            if !name.contains('.') && entry.namespace_oid == PG_CATALOG_NAMESPACE_OID {
                cache.by_name.insert(name.clone(), entry.clone());
            }
        }

        cache
    }

    pub fn insert(&mut self, name: impl Into<String>, entry: RelCacheEntry) {
        self.by_name.insert(
            normalize_catalog_name(&name.into()).to_ascii_lowercase(),
            entry.clone(),
        );
        self.by_oid.insert(entry.relation_oid, entry);
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, &RelCacheEntry)> {
        self.by_name
            .iter()
            .map(|(name, entry)| (name.as_str(), entry))
    }
}

fn relation_locator_for_class_row(
    relation_oid: u32,
    relfilenode: u32,
    current_db_oid: u32,
) -> RelFileLocator {
    if let Some(kind) = bootstrap_catalog_kinds()
        .into_iter()
        .find(|kind| kind.relation_oid() == relation_oid)
    {
        return bootstrap_catalog_rel(kind, current_db_oid);
    }
    if let Some(descriptor) = system_catalog_index_by_oid(relation_oid) {
        let heap_rel = bootstrap_catalog_rel(descriptor.heap_kind, current_db_oid);
        return RelFileLocator {
            spc_oid: heap_rel.spc_oid,
            db_oid: heap_rel.db_oid,
            rel_number: relfilenode,
        };
    }
    RelFileLocator {
        spc_oid: 0,
        db_oid: current_db_oid,
        rel_number: relfilenode,
    }
}

fn from_catalog_entry(entry: &CatalogEntry, support_lookup: &IndexSupportLookup) -> RelCacheEntry {
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
        desc: entry.desc.clone(),
        index: entry.index_meta.as_ref().map(|index| {
            let support = support_lookup.resolve(&index.indclass);
            IndexRelCacheEntry {
                indexrelid: entry.relation_oid,
                indrelid: index.indrelid,
                indnatts: index.indkey.len() as i16,
                indnkeyatts: index.indkey.len() as i16,
                indisunique: index.indisunique,
                indnullsnotdistinct: false,
                indisprimary: index.indisprimary,
                indisexclusion: false,
                indimmediate: true,
                indisclustered: false,
                indisvalid: index.indisvalid,
                indcheckxmin: false,
                indisready: index.indisready,
                indislive: index.indislive,
                indisreplident: false,
                am_oid: entry.am_oid,
                am_handler_oid: support_lookup.am_handler_oid(entry.am_oid),
                indkey: index.indkey.clone(),
                indclass: index.indclass.clone(),
                indcollation: index.indcollation.clone(),
                indoption: index.indoption.clone(),
                opfamily_oids: support.opfamily_oids,
                opcintype_oids: support.opcintype_oids,
                opckeytype_oids: support.opckeytype_oids,
                amop_entries: support.amop_entries,
                amproc_entries: support.amproc_entries,
                indexprs: index.indexprs.clone(),
                indpred: index.indpred.clone(),
            }
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::CatalogStore;
    use crate::backend::catalog::catalog::{CatalogIndexBuildOptions, column_desc};
    use crate::backend::executor::RelationDesc;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::access::gist::GIST_CONSISTENT_PROC;
    use crate::include::catalog::{
        BOX_GIST_OPCLASS_OID, BOX_TYPE_OID, GIST_AM_OID, GIST_BOX_CONSISTENT_PROC_OID,
        INT4_TYPE_OID, INT4RANGE_TYPE_OID, RANGE_GIST_CONSISTENT_PROC_OID, RANGE_GIST_OPCLASS_OID,
        bootstrap_pg_operator_rows,
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
    fn relcache_indexes_relations_by_name_and_oid() {
        let mut catalog = Catalog::default();
        let entry = catalog
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let cache = RelCache::from_catalog(&catalog);
        assert_eq!(
            cache
                .get_by_name("people")
                .map(|entry| entry.rel.rel_number),
            Some(entry.rel.rel_number)
        );
        assert_eq!(
            cache
                .get_by_oid(entry.relation_oid)
                .map(|entry| entry.rel.rel_number),
            Some(entry.rel.rel_number)
        );
    }

    #[test]
    fn relcache_from_catalog_populates_gist_support_metadata() {
        let mut catalog = Catalog::default();
        let table = catalog
            .create_table(
                "boxes",
                RelationDesc {
                    columns: vec![column_desc("b", SqlType::new(SqlTypeKind::Box), true)],
                },
            )
            .unwrap();

        catalog
            .create_index_for_relation_with_options(
                "boxes_b_gist",
                table.relation_oid,
                false,
                &["b".into()],
                &CatalogIndexBuildOptions {
                    am_oid: GIST_AM_OID,
                    indclass: vec![BOX_GIST_OPCLASS_OID],
                    indcollation: vec![0],
                    indoption: vec![0],
                },
            )
            .unwrap();

        let cache = RelCache::from_catalog(&catalog);
        let index = cache
            .get_by_name("boxes_b_gist")
            .and_then(|entry| entry.index.as_ref())
            .expect("GiST index entry should be present");

        assert_eq!(index.am_oid, GIST_AM_OID);
        assert!(index.am_handler_oid.is_some());
        assert_eq!(
            index.amproc_oid(
                &cache.get_by_name("boxes_b_gist").unwrap().desc,
                0,
                GIST_CONSISTENT_PROC
            ),
            Some(GIST_BOX_CONSISTENT_PROC_OID)
        );
        assert!(!index.amop_entries.is_empty());
    }

    #[test]
    fn relcache_range_strategy_lookup_uses_argument_type() {
        let support_lookup =
            IndexSupportLookup::from_catcache(&CatCache::from_catalog(&Catalog::default()));
        let support = support_lookup.resolve(&[RANGE_GIST_OPCLASS_OID]);
        let desc = RelationDesc {
            columns: vec![column_desc(
                "span",
                SqlType::new(SqlTypeKind::Int4Range),
                true,
            )],
        };
        let index = IndexRelCacheEntry {
            indexrelid: 42,
            indrelid: 41,
            indnatts: 1,
            indnkeyatts: 1,
            indisunique: false,
            indnullsnotdistinct: false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: false,
            indisclustered: false,
            indisvalid: true,
            indcheckxmin: false,
            indisready: true,
            indislive: true,
            indisreplident: false,
            am_oid: GIST_AM_OID,
            am_handler_oid: support_lookup.am_handler_oid(GIST_AM_OID),
            indkey: vec![1],
            indclass: vec![RANGE_GIST_OPCLASS_OID],
            indcollation: vec![0],
            indoption: vec![0],
            opfamily_oids: support.opfamily_oids,
            opcintype_oids: support.opcintype_oids,
            opckeytype_oids: support.opckeytype_oids,
            amop_entries: support.amop_entries,
            amproc_entries: support.amproc_entries,
            indexprs: None,
            indpred: None,
        };
        let contains_proc_oid = bootstrap_pg_operator_rows()
            .into_iter()
            .find(|row| {
                row.oprname == "@>"
                    && row.oprleft == INT4RANGE_TYPE_OID
                    && row.oprright == INT4RANGE_TYPE_OID
            })
            .map(|row| row.oprcode)
            .expect("int4range contains operator proc oid");

        assert_eq!(
            index.amproc_oid(&desc, 0, GIST_CONSISTENT_PROC),
            Some(RANGE_GIST_CONSISTENT_PROC_OID)
        );
        assert_eq!(
            index.amop_strategy_for_proc(&desc, 0, contains_proc_oid, Some(INT4RANGE_TYPE_OID),),
            Some(7)
        );
        assert_eq!(
            index.amop_strategy_for_proc(&desc, 0, contains_proc_oid, Some(INT4_TYPE_OID),),
            Some(16)
        );
    }

    #[test]
    fn relcache_distinguishes_gist_search_and_ordering_rows() {
        let support_lookup =
            IndexSupportLookup::from_catcache(&CatCache::from_catalog(&Catalog::default()));
        let support = support_lookup.resolve(&[BOX_GIST_OPCLASS_OID]);
        let desc = RelationDesc {
            columns: vec![column_desc("b", SqlType::new(SqlTypeKind::Box), true)],
        };
        let index = IndexRelCacheEntry {
            indexrelid: 52,
            indrelid: 51,
            indnatts: 1,
            indnkeyatts: 1,
            indisunique: false,
            indnullsnotdistinct: false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: false,
            indisclustered: false,
            indisvalid: true,
            indcheckxmin: false,
            indisready: true,
            indislive: true,
            indisreplident: false,
            am_oid: GIST_AM_OID,
            am_handler_oid: support_lookup.am_handler_oid(GIST_AM_OID),
            indkey: vec![1],
            indclass: vec![BOX_GIST_OPCLASS_OID],
            indcollation: vec![0],
            indoption: vec![0],
            opfamily_oids: support.opfamily_oids,
            opcintype_oids: support.opcintype_oids,
            opckeytype_oids: support.opckeytype_oids,
            amop_entries: support.amop_entries,
            amproc_entries: support.amproc_entries,
            indexprs: None,
            indpred: None,
        };
        let distance_operator = bootstrap_pg_operator_rows()
            .into_iter()
            .find(|row| {
                row.oprname == "<->" && row.oprleft == BOX_TYPE_OID && row.oprright == BOX_TYPE_OID
            })
            .expect("box distance operator row");

        assert_eq!(
            index.amop_strategy_for_operator(&desc, 0, distance_operator.oid, Some(BOX_TYPE_OID)),
            None
        );
        assert_eq!(
            index.amop_ordering_strategy_for_operator(
                &desc,
                0,
                distance_operator.oid,
                Some(BOX_TYPE_OID),
            ),
            Some(1)
        );
        assert_eq!(
            index.amop_ordering_strategy_for_proc(
                &desc,
                0,
                distance_operator.oprcode,
                Some(BOX_TYPE_OID),
            ),
            Some(1)
        );
    }

    #[test]
    fn relcache_loads_relations_from_physical_catalogs() {
        let base = temp_dir("relcache_from_physical");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let cache = RelCache::from_physical(&base).unwrap();
        assert_eq!(
            cache.get_by_name("people").map(|rel| rel.rel.rel_number),
            Some(entry.rel.rel_number)
        );
        assert_eq!(
            cache
                .get_by_oid(entry.relation_oid)
                .map(|rel| rel.desc.columns.len()),
            Some(1)
        );
    }

    #[test]
    fn relcache_loads_zero_column_relations_from_physical_catalogs() {
        let base = temp_dir("relcache_zero_column");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "zerocol",
                RelationDesc {
                    columns: Vec::new(),
                },
            )
            .unwrap();

        let cache = RelCache::from_physical(&base).unwrap();
        assert_eq!(
            cache
                .get_by_oid(entry.relation_oid)
                .map(|rel| rel.desc.columns.len()),
            Some(0)
        );
        assert!(cache.get_by_name("zerocol").is_some());
    }

    #[test]
    fn relcache_skips_user_relations_with_dangling_type_oids() {
        let base = temp_dir("relcache_dangling_type");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let mut rows = crate::backend::catalog::rows::physical_catalog_rows_from_catcache(
            &store.catcache().unwrap(),
        );
        rows.attributes
            .iter_mut()
            .find(|row| row.attrelid == entry.relation_oid && row.attname == "id")
            .unwrap()
            .atttypid = 999_999;
        let broken = crate::backend::utils::cache::catcache::CatCache::from_rows(
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
            rows.procs,
            rows.aggregates,
            rows.casts,
            rows.collations,
            rows.foreign_data_wrappers,
            rows.databases,
            rows.tablespaces,
            rows.statistics,
            rows.types,
        );

        let cache = RelCache::from_catcache_in_db(&broken, 1).unwrap();
        assert!(cache.get_by_name("people").is_none());
        assert!(cache.get_by_name("pg_namespace").is_some());
    }

    #[test]
    fn relcache_preserves_exact_pg_catalog_qualified_names() {
        let cache = RelCache::from_catalog(&Catalog::default());
        assert_eq!(
            cache
                .get_by_name_exact("pg_catalog.pg_class")
                .map(|entry| entry.relation_oid),
            Some(crate::include::catalog::PG_CLASS_RELATION_OID)
        );
        assert_eq!(
            cache
                .get_by_name("pg_catalog.pg_class")
                .map(|entry| entry.relation_oid),
            Some(crate::include::catalog::PG_CLASS_RELATION_OID)
        );
    }
}
