use std::collections::{BTreeMap, BTreeSet};

use crate::CatalogError;
use crate::bootstrap::bootstrap_catalog_rel;
use crate::catalog::{Catalog, CatalogEntry, column_desc};
use crate::catcache::{CatCache, normalize_catalog_name, sql_type_oid};
use pgrust_catalog_data::PgTypeRow;
use pgrust_catalog_data::toasting::toast_relation_name;
use pgrust_catalog_data::{
    ANYELEMENTOID, ANYMULTIRANGEOID, ANYOID, ANYRANGEOID, BIT_TYPE_OID, CIDR_TYPE_OID,
    CONSTRAINT_NOTNULL, CONSTRAINT_PRIMARY, INET_TYPE_OID, PG_CATALOG_NAMESPACE_OID,
    PG_CONSTRAINT_RELATION_OID, PG_TOAST_NAMESPACE_OID, TIMESTAMP_TYPE_OID, TIMESTAMPTZ_TYPE_OID,
    VARBIT_TYPE_OID, bootstrap_catalog_kinds, builtin_range_spec_by_multirange_oid,
    builtin_range_spec_by_oid, builtin_scalar_function_for_proc_oid, system_catalog_index_by_oid,
};
use pgrust_core::RelFileLocator;
use pgrust_nodes::primnodes::RelationDesc;
pub use pgrust_nodes::relcache::{
    IndexAmOpEntry, IndexAmProcEntry, IndexRelCacheEntry, RelCacheEntry,
};
use pgrust_nodes::{SqlType, SqlTypeKind};

pub fn default_sequence_oid_from_default_expr(default_expr: &str) -> Option<u32> {
    let expr = default_expr.trim();
    let rest = expr.strip_prefix("nextval(")?;
    if let Some(oid_end) = rest.find("::oid)") {
        return rest[..oid_end].trim().parse::<u32>().ok();
    }
    let oid_end = rest.find(')')?;
    rest[..oid_end].trim().parse::<u32>().ok()
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
    am_rows: Vec<pgrust_catalog_data::PgAmRow>,
    opclass_rows: Vec<pgrust_catalog_data::PgOpclassRow>,
    amop_rows: Vec<pgrust_catalog_data::PgAmopRow>,
    amproc_rows: Vec<pgrust_catalog_data::PgAmprocRow>,
    operator_rows: Vec<pgrust_catalog_data::PgOperatorRow>,
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

fn index_indexed_operator_type_oid(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
) -> Option<u32> {
    index
        .opcintype_oids
        .get(column_index)
        .copied()
        .filter(|oid| *oid != 0)
        .filter(|oid| {
            !matches!(
                *oid,
                pgrust_catalog_data::ANYOID
                    | pgrust_catalog_data::ANYARRAYOID
                    | pgrust_catalog_data::ANYRANGEOID
                    | pgrust_catalog_data::ANYMULTIRANGEOID
            )
        })
        .or_else(|| {
            desc.columns
                .get(column_index)
                .map(|column| sql_type_oid(column.sql_type))
        })
}

fn index_indexed_operand_type_oid(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
) -> Option<u32> {
    index
        .opckeytype_oids
        .get(column_index)
        .copied()
        .filter(|oid| *oid != 0)
        .or_else(|| {
            desc.columns
                .get(column_index)
                .map(|column| sql_type_oid(column.sql_type))
        })
}

fn index_type_match_score(
    entry_lefttype: u32,
    entry_righttype: u32,
    left_type_oid: Option<u32>,
    right_type_oid: Option<u32>,
) -> Option<u8> {
    fn same_index_type_family(entry_type: u32, actual_type: u32) -> bool {
        matches!(
            (entry_type, actual_type),
            (INET_TYPE_OID | CIDR_TYPE_OID, INET_TYPE_OID | CIDR_TYPE_OID)
                | (
                    BIT_TYPE_OID | VARBIT_TYPE_OID,
                    BIT_TYPE_OID | VARBIT_TYPE_OID
                )
                | (
                    TIMESTAMP_TYPE_OID | TIMESTAMPTZ_TYPE_OID,
                    TIMESTAMP_TYPE_OID | TIMESTAMPTZ_TYPE_OID
                )
        )
    }

    fn component_score(entry_type: u32, actual_type: Option<u32>) -> Option<u8> {
        match actual_type {
            None => Some(0),
            Some(actual) if entry_type == actual => Some(4),
            Some(actual) if same_index_type_family(entry_type, actual) => Some(3),
            Some(_) if entry_type == ANYOID => Some(1),
            Some(actual)
                if entry_type == ANYRANGEOID && builtin_range_spec_by_oid(actual).is_some() =>
            {
                Some(2)
            }
            Some(actual)
                if entry_type == ANYMULTIRANGEOID
                    && builtin_range_spec_by_multirange_oid(actual).is_some() =>
            {
                Some(2)
            }
            Some(_) if entry_type == ANYELEMENTOID => Some(1),
            Some(_) => None,
        }
    }

    Some(
        component_score(entry_lefttype, left_type_oid)?
            + component_score(entry_righttype, right_type_oid)?,
    )
}

pub fn index_amproc_oid(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    procnum: i16,
) -> Option<u32> {
    let operand_type_oid = index_indexed_operand_type_oid(index, desc, column_index);
    let operator_type_oid = index_indexed_operator_type_oid(index, desc, column_index);
    let mut best: Option<(u8, u32)> = None;
    for entry in index.amproc_entries.get(column_index)?.iter() {
        if entry.procnum != procnum {
            continue;
        }
        let operand_score = index_type_match_score(
            entry.lefttype,
            entry.righttype,
            operand_type_oid,
            operand_type_oid,
        );
        let operator_score = index_type_match_score(
            entry.lefttype,
            entry.righttype,
            operator_type_oid,
            operator_type_oid,
        );
        let Some(score) = operand_score.or(operator_score) else {
            continue;
        };
        if best.is_none_or(|(best_score, _)| score > best_score) {
            best = Some((score, entry.proc_oid));
        }
    }
    best.map(|(_, proc_oid)| proc_oid)
}

pub fn index_amop_strategy_for_operator(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    operator_oid: u32,
    right_type_oid: Option<u32>,
) -> Option<u16> {
    index_amop_strategy_matching(
        index,
        desc,
        column_index,
        right_type_oid,
        Some('s'),
        |entry| entry.operator_oid == operator_oid,
    )
}

pub fn index_amop_ordering_strategy_for_operator(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    operator_oid: u32,
    right_type_oid: Option<u32>,
) -> Option<u16> {
    index_amop_strategy_matching(
        index,
        desc,
        column_index,
        right_type_oid,
        Some('o'),
        |entry| entry.operator_oid == operator_oid,
    )
    .map(normalize_ordering_strategy)
}

pub fn index_amop_strategy_for_proc(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    operator_proc_oid: u32,
    right_type_oid: Option<u32>,
) -> Option<u16> {
    index_amop_strategy_matching(
        index,
        desc,
        column_index,
        right_type_oid,
        Some('s'),
        |entry| proc_oids_match(entry.operator_proc_oid, operator_proc_oid),
    )
}

pub fn index_amop_ordering_strategy_for_proc(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    operator_proc_oid: u32,
    right_type_oid: Option<u32>,
) -> Option<u16> {
    index_amop_strategy_matching(
        index,
        desc,
        column_index,
        right_type_oid,
        Some('o'),
        |entry| proc_oids_match(entry.operator_proc_oid, operator_proc_oid),
    )
    .map(normalize_ordering_strategy)
}

fn index_amop_strategy_matching(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    right_type_oid: Option<u32>,
    purpose: Option<char>,
    predicate: impl Fn(&IndexAmOpEntry) -> bool,
) -> Option<u16> {
    let left_type_oid = index_indexed_operator_type_oid(index, desc, column_index);
    let mut best: Option<(u8, i16)> = None;
    for entry in index.amop_entries.get(column_index)?.iter() {
        if purpose.is_some_and(|purpose| entry.purpose != purpose) || !predicate(entry) {
            continue;
        }
        let Some(score) = index_type_match_score(
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

fn proc_oids_match(left: u32, right: u32) -> bool {
    left == right
        || builtin_scalar_function_for_proc_oid(left)
            .zip(builtin_scalar_function_for_proc_oid(right))
            .is_some_and(|(left, right)| left == right)
}

fn normalize_ordering_strategy(strategy: u16) -> u16 {
    if strategy == 15 { 1 } else { strategy }
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
            cache
                .by_oid
                .insert(entry.relation_oid, relcache_entry.clone());
            if entry.reltoastrelid != 0 {
                let toast_entry = bootstrap_toast_relcache_entry(&relcache_entry);
                cache.by_name.insert(
                    format!("pg_toast.{}", toast_relation_name(entry.relation_oid)),
                    toast_entry.clone(),
                );
                cache.by_oid.insert(toast_entry.relation_oid, toast_entry);
            }
        }
        cache
    }

    pub fn from_catcache(catcache: &CatCache) -> Result<Self, CatalogError> {
        Self::from_catcache_in_db(catcache, 1)
    }

    pub fn from_catcache_in_db(
        catcache: &CatCache,
        current_db_oid: u32,
    ) -> Result<Self, CatalogError> {
        Self::from_catcache_in_db_with_extra_type_rows(catcache, current_db_oid, &[])
    }

    pub fn from_catcache_in_db_with_extra_type_rows(
        catcache: &CatCache,
        current_db_oid: u32,
        extra_type_rows: &[PgTypeRow],
    ) -> Result<Self, CatalogError> {
        let mut cache = Self::default();
        let support_lookup = IndexSupportLookup::from_catcache(catcache);
        let extra_types_by_oid = extra_type_rows
            .iter()
            .map(|row| (row.oid, row.sql_type))
            .collect::<BTreeMap<_, _>>();
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
                    let sql_type = extra_types_by_oid
                        .get(&attr.atttypid)
                        .copied()
                        .or_else(|| catcache.type_by_oid(attr.atttypid).map(|ty| ty.sql_type))
                        .or_else(|| attr.attisdropped.then_some(SqlType::new(SqlTypeKind::Int4)))
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
                    desc.attstattarget = attr.attstattarget.unwrap_or(-1);
                    desc.attinhcount = attr.attinhcount;
                    desc.attislocal = attr.attislocal;
                    desc.attacl = attr.attacl.clone();
                    desc.collation_oid = attr.attcollation;
                    desc.fdw_options = attr.attfdwoptions.clone();
                    desc.identity = pgrust_nodes::parsenodes::ColumnIdentityKind::from_catalog_char(
                        attr.attidentity,
                    );
                    desc.generated =
                        pgrust_nodes::parsenodes::ColumnGeneratedKind::from_catalog_char(
                            attr.attgenerated,
                        );
                    desc.dropped = attr.attisdropped;
                    desc.missing_default_value = attr
                        .attmissingval
                        .as_ref()
                        .and_then(|values| values.first().cloned())
                        .map(|value| {
                            crate::catalog::missing_default_value_from_attmissingval(
                                value,
                                desc.sql_type,
                            )
                        });
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
                            crate::relcache::default_sequence_oid_from_default_expr(&attrdef.adbin);
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
                rel: relation_locator_for_class_row(
                    class.oid,
                    class.relfilenode,
                    class.reltablespace,
                    current_db_oid,
                ),
                relation_oid: class.oid,
                namespace_oid: class.relnamespace,
                owner_oid: class.relowner,
                of_type_oid: class.reloftype,
                row_type_oid: class.reltype,
                array_type_oid: catcache
                    .type_by_oid(class.reltype)
                    .map(|row| row.typarray)
                    .unwrap_or(0),
                reltoastrelid: class.reltoastrelid,
                relhasindex: class.relhasindex,
                relpersistence: class.relpersistence,
                relkind: class.relkind,
                relispartition: class.relispartition,
                relispopulated: class.relispopulated,
                relpartbound: class.relpartbound.clone(),
                relhastriggers: class.relhastriggers,
                relrowsecurity: class.relrowsecurity,
                relforcerowsecurity: class.relforcerowsecurity,
                desc: RelationDesc { columns },
                partitioned_table: catcache.partitioned_table_row(class.oid).cloned(),
                partition_spec: None,
                index: matches!(class.relkind, 'i' | 'I').then(|| {
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
                            indclass_options: Vec::new(),
                            indcollation: Vec::new(),
                            indoption: Vec::new(),
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
                        indclass_options: crate::state::index_opclass_options_from_reloptions(
                            class.reloptions.as_deref(),
                        ),
                        indcollation: index.indcollation.clone(),
                        indoption: index.indoption.clone(),
                        opfamily_oids: support.opfamily_oids,
                        opcintype_oids: support.opcintype_oids,
                        opckeytype_oids: support.opckeytype_oids,
                        amop_entries: support.amop_entries,
                        amproc_entries: support.amproc_entries,
                        indexprs: index.indexprs.clone(),
                        indpred: index.indpred.clone(),
                        rd_indexprs: None,
                        rd_indpred: None,
                        btree_options: None,
                        brin_options: None,
                        gist_options: None,
                        gin_options: None,
                        hash_options: None,
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
            if class.reltoastrelid != 0 {
                let toast_entry =
                    bootstrap_toast_relcache_entry(cache.by_oid.get(&class.oid).unwrap());
                cache.by_name.insert(
                    format!("pg_toast.{}", toast_relation_name(class.oid)),
                    toast_entry.clone(),
                );
                cache.by_oid.insert(toast_entry.relation_oid, toast_entry);
            }
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

    pub fn relation_get_index_list(&self, relation_oid: u32) -> Vec<u32> {
        self.by_oid
            .values()
            .filter_map(|entry| {
                let index = entry.index.as_ref()?;
                (index.indrelid == relation_oid && index.indislive).then_some(entry.relation_oid)
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn relation_name_by_oid(&self, relation_oid: u32) -> Option<String> {
        self.by_name
            .iter()
            .find(|(name, entry)| entry.relation_oid == relation_oid && !name.contains('.'))
            .or_else(|| {
                self.by_name
                    .iter()
                    .find(|(_, entry)| entry.relation_oid == relation_oid)
            })
            .map(|(name, _)| name.rsplit('.').next().unwrap_or(name).to_string())
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

fn bootstrap_toast_relation_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("chunk_id", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("chunk_seq", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("chunk_data", SqlType::new(SqlTypeKind::Bytea), false),
        ],
    }
}

fn bootstrap_toast_relcache_entry(parent: &RelCacheEntry) -> RelCacheEntry {
    RelCacheEntry {
        rel: RelFileLocator {
            spc_oid: parent.rel.spc_oid,
            db_oid: parent.rel.db_oid,
            rel_number: parent.reltoastrelid,
        },
        relation_oid: parent.reltoastrelid,
        namespace_oid: PG_TOAST_NAMESPACE_OID,
        owner_oid: parent.owner_oid,
        of_type_oid: 0,
        row_type_oid: 0,
        array_type_oid: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relpersistence: parent.relpersistence,
        relkind: 't',
        relispartition: false,
        relispopulated: true,
        relpartbound: None,
        relhastriggers: false,
        relrowsecurity: false,
        relforcerowsecurity: false,
        desc: bootstrap_toast_relation_desc(),
        partitioned_table: None,
        partition_spec: None,
        index: None,
    }
}

pub fn relation_locator_for_class_row(
    relation_oid: u32,
    relfilenode: u32,
    reltablespace: u32,
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
        spc_oid: reltablespace,
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
        of_type_oid: entry.of_type_oid,
        row_type_oid: entry.row_type_oid,
        array_type_oid: entry.array_type_oid,
        reltoastrelid: entry.reltoastrelid,
        relhasindex: entry.relhasindex,
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relispartition: entry.relispartition,
        relispopulated: entry.relispopulated,
        relpartbound: entry.relpartbound.clone(),
        relhastriggers: entry.relhastriggers,
        relrowsecurity: entry.relrowsecurity,
        relforcerowsecurity: entry.relforcerowsecurity,
        desc: entry.desc.clone(),
        partitioned_table: entry.partitioned_table.clone(),
        partition_spec: None,
        index: entry.index_meta.as_ref().map(|index| {
            let support = support_lookup.resolve(&index.indclass);
            IndexRelCacheEntry {
                indexrelid: entry.relation_oid,
                indrelid: index.indrelid,
                indnatts: index.indkey.len() as i16,
                indnkeyatts: index.indclass.len() as i16,
                indisunique: index.indisunique,
                indnullsnotdistinct: index.indnullsnotdistinct,
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
                indclass_options: index.indclass_options.clone(),
                indcollation: index.indcollation.clone(),
                indoption: index.indoption.clone(),
                opfamily_oids: support.opfamily_oids,
                opcintype_oids: support.opcintype_oids,
                opckeytype_oids: support.opckeytype_oids,
                amop_entries: support.amop_entries,
                amproc_entries: support.amproc_entries,
                indexprs: index.indexprs.clone(),
                indpred: index.indpred.clone(),
                rd_indexprs: None,
                rd_indpred: None,
                btree_options: index.btree_options,
                brin_options: index.brin_options.clone(),
                gist_options: index.gist_options,
                gin_options: index.gin_options.clone(),
                hash_options: index.hash_options,
            }
        }),
    }
}
