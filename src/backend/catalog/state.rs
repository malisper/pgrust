use std::collections::{BTreeMap, BTreeSet};

use crate::backend::catalog::bootstrap::{bootstrap_catalog_entry, bootstrap_catalog_kinds};
use crate::backend::catalog::catalog::allocate_relation_object_oids;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::catalog::indexing::insert_bootstrap_system_indexes;
use crate::backend::catalog::pg_constraint::{derived_pg_constraint_rows, sort_pg_constraint_rows};
use crate::backend::catalog::pg_depend::{
    derived_pg_depend_rows, foreign_key_constraint_depend_rows,
    index_backed_constraint_depend_rows, inheritance_depend_rows,
    primary_key_owned_not_null_depend_rows, relation_constraint_depend_rows, sort_pg_depend_rows,
    trigger_depend_rows,
};
use crate::backend::catalog::pg_inherits::sort_pg_inherits_rows;
use crate::backend::catalog::pg_partitioned_table::sort_pg_partitioned_table_rows;
use crate::backend::catalog::pg_policy::sort_pg_policy_rows;
use crate::backend::catalog::store::{DEFAULT_FIRST_REL_NUMBER, DEFAULT_FIRST_USER_OID};
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::backend::utils::misc::interrupts::InterruptReason;
use crate::include::access::brin::BrinOptions;
use crate::include::access::gin::GinOptions;
use crate::include::access::hash::HashOptions;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, CONSTRAINT_NOTNULL, CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE,
    DEPENDENCY_INTERNAL, PG_CONSTRAINT_RELATION_OID, PUBLIC_NAMESPACE_OID, PgAuthIdRow,
    PgAuthMembersRow, PgConstraintRow, PgDatabaseRow, PgDependRow, PgInheritsRow,
    PgPartitionedTableRow, PgPolicyRow, PgPublicationNamespaceRow, PgPublicationRelRow,
    PgPublicationRow, PgRewriteRow, PgStatisticExtDataRow, PgStatisticExtRow, PgTablespaceRow,
    PgTriggerRow, bootstrap_pg_auth_members_rows, bootstrap_pg_authid_rows,
    bootstrap_pg_database_rows, bootstrap_pg_opclass_rows, bootstrap_pg_tablespace_rows,
    builtin_range_name_for_sql_type, builtin_type_name_for_oid, builtin_type_row_by_oid,
    builtin_type_rows, relkind_has_storage, sort_pg_rewrite_rows,
};

const DEFAULT_SPC_OID: u32 = 0;
const DEFAULT_DB_OID: u32 = 1;

fn dropped_column_name(attnum: usize) -> String {
    format!("........pg.dropped.{attnum}........")
}

fn build_catalog_index_entry(
    table: &CatalogEntry,
    unique: bool,
    primary: bool,
    columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    options: &CatalogIndexBuildOptions,
    predicate_sql: Option<&str>,
    relation_oid: u32,
    rel_number: u32,
    relkind: char,
    indisready: bool,
    indisvalid: bool,
) -> Result<CatalogEntry, CatalogError> {
    let mut indkey = Vec::with_capacity(columns.len());
    let mut index_columns = Vec::with_capacity(columns.len());
    let mut used_index_column_names = BTreeSet::new();
    let mut expr_sqls = Vec::new();
    for (position, column_name) in columns.iter().enumerate() {
        let opckey_sql_type = index_column_opckey_sql_type(position, options);
        if let Some(expr_sql) = column_name.expr_sql.as_deref() {
            indkey.push(0);
            expr_sqls.push(expr_sql.to_string());
            let expr_type = column_name
                .expr_type
                .ok_or(CatalogError::Corrupt("missing expression index sql type"))?;
            push_unique_index_column(
                &mut index_columns,
                &mut used_index_column_names,
                column_desc(
                    format!("expr{}", position + 1),
                    opckey_sql_type.unwrap_or(expr_type),
                    true,
                ),
            );
            continue;
        }

        let (attnum, column) = table
            .desc
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name.eq_ignore_ascii_case(&column_name.name))
            .ok_or_else(|| CatalogError::UnknownColumn(column_name.name.clone()))?;
        indkey.push(attnum.saturating_add(1) as i16);
        let mut column = column.clone();
        if let Some(opckey_sql_type) = opckey_sql_type {
            column = column_desc(
                column.name.clone(),
                opckey_sql_type,
                column.storage.nullable,
            );
        }
        column.not_null_constraint_oid = None;
        column.not_null_constraint_name = None;
        column.not_null_constraint_validated = false;
        column.not_null_primary_key_owned = false;
        column.attrdef_oid = None;
        column.default_expr = None;
        push_unique_index_column(&mut index_columns, &mut used_index_column_names, column);
    }
    let key_count = options.indclass.len();
    if key_count > columns.len()
        || options.indcollation.len() != key_count
        || options.indoption.len() != key_count
    {
        return Err(CatalogError::Corrupt("index build options length mismatch"));
    }

    Ok(CatalogEntry {
        rel: RelFileLocator {
            spc_oid: DEFAULT_SPC_OID,
            db_oid: table.rel.db_oid,
            rel_number,
        },
        relation_oid,
        namespace_oid: table.namespace_oid,
        owner_oid: table.owner_oid,
        relacl: None,
        reloptions: None,
        of_type_oid: 0,
        row_type_oid: 0,
        array_type_oid: 0,
        reltoastrelid: 0,
        relpersistence: table.relpersistence,
        relkind,
        am_oid: options.am_oid,
        relhassubclass: false,
        relhastriggers: false,
        relispartition: false,
        relispopulated: true,
        relpartbound: None,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: if relkind_has_storage(relkind) {
            -1.0
        } else {
            0.0
        },
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
        desc: RelationDesc {
            columns: index_columns,
        },
        partitioned_table: None,
        index_meta: Some(CatalogIndexMeta {
            indrelid: table.relation_oid,
            indkey,
            indisunique: unique,
            indnullsnotdistinct: options.indnullsnotdistinct,
            indisprimary: primary,
            indisexclusion: options.indisexclusion,
            indimmediate: options.indimmediate,
            indisvalid,
            indisready,
            indislive: true,
            indclass: options.indclass.clone(),
            indcollation: options.indcollation.clone(),
            indoption: options.indoption.clone(),
            indexprs: (!expr_sqls.is_empty())
                .then(|| serde_json::to_string(&expr_sqls))
                .transpose()
                .map_err(|_| CatalogError::Corrupt("invalid index expression metadata"))?,
            indpred: predicate_sql
                .map(str::trim)
                .filter(|pred| !pred.is_empty())
                .map(str::to_string),
            brin_options: options.brin_options.clone(),
            gin_options: options.gin_options.clone(),
            hash_options: options.hash_options,
        }),
    })
}

fn index_column_opckey_sql_type(
    position: usize,
    options: &CatalogIndexBuildOptions,
) -> Option<SqlType> {
    let opclass_oid = *options.indclass.get(position)?;
    let opclass = bootstrap_pg_opclass_rows()
        .into_iter()
        .find(|row| row.oid == opclass_oid)?;
    if opclass.opcmethod != crate::include::catalog::GIST_AM_OID || opclass.opckeytype == 0 {
        return None;
    }
    builtin_type_row_by_oid(opclass.opckeytype).map(|row| row.sql_type)
}

fn push_unique_index_column(
    columns: &mut Vec<crate::backend::executor::ColumnDesc>,
    used_names: &mut BTreeSet<String>,
    mut column: crate::backend::executor::ColumnDesc,
) {
    let base = column.name.clone();
    let mut candidate = base.clone();
    let mut suffix = 1usize;
    while !used_names.insert(candidate.to_ascii_lowercase()) {
        candidate = format!("{base}{suffix}");
        suffix = suffix.saturating_add(1);
    }
    column.name = candidate;
    columns.push(column);
}

fn index_constraint_key_attnums(meta: &CatalogIndexMeta) -> Vec<i16> {
    meta.indkey
        .iter()
        .take(meta.indclass.len())
        .copied()
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogIndexMeta {
    pub indrelid: u32,
    pub indkey: Vec<i16>,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisvalid: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indclass: Vec<u32>,
    pub indcollation: Vec<u32>,
    pub indoption: Vec<i16>,
    pub indexprs: Option<String>,
    pub indpred: Option<String>,
    pub brin_options: Option<BrinOptions>,
    pub gin_options: Option<GinOptions>,
    pub hash_options: Option<HashOptions>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogIndexBuildOptions {
    pub am_oid: u32,
    pub indclass: Vec<u32>,
    pub indcollation: Vec<u32>,
    pub indoption: Vec<i16>,
    pub indnullsnotdistinct: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub brin_options: Option<BrinOptions>,
    pub gin_options: Option<GinOptions>,
    pub hash_options: Option<HashOptions>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogEntry {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub namespace_oid: u32,
    pub owner_oid: u32,
    pub relacl: Option<Vec<String>>,
    pub reloptions: Option<Vec<String>>,
    pub of_type_oid: u32,
    pub row_type_oid: u32,
    pub array_type_oid: u32,
    pub reltoastrelid: u32,
    pub relpersistence: char,
    pub relkind: char,
    pub am_oid: u32,
    pub relhassubclass: bool,
    pub relhastriggers: bool,
    pub relispartition: bool,
    pub relispopulated: bool,
    pub relpartbound: Option<String>,
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    pub relpages: i32,
    pub reltuples: f64,
    pub relallvisible: i32,
    pub relallfrozen: i32,
    pub relfrozenxid: u32,
    pub desc: RelationDesc,
    pub partitioned_table: Option<PgPartitionedTableRow>,
    pub index_meta: Option<CatalogIndexMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    Io(String),
    Corrupt(&'static str),
    TableAlreadyExists(String),
    TypeAlreadyExists(String),
    UnknownTable(String),
    UnknownColumn(String),
    UnknownType(String),
    UniqueViolation(String),
    Interrupted(InterruptReason),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    pub(crate) tables: BTreeMap<String, CatalogEntry>,
    pub(crate) constraints: Vec<PgConstraintRow>,
    pub(crate) depends: Vec<PgDependRow>,
    pub(crate) inherits: Vec<PgInheritsRow>,
    pub(crate) partitioned_tables: Vec<PgPartitionedTableRow>,
    pub(crate) rewrites: Vec<PgRewriteRow>,
    pub(crate) triggers: Vec<crate::include::catalog::PgTriggerRow>,
    pub(crate) policies: Vec<PgPolicyRow>,
    pub(crate) publications: Vec<PgPublicationRow>,
    pub(crate) publication_rels: Vec<PgPublicationRelRow>,
    pub(crate) publication_namespaces: Vec<PgPublicationNamespaceRow>,
    pub(crate) statistics_ext: Vec<PgStatisticExtRow>,
    pub(crate) statistics_ext_data: Vec<PgStatisticExtDataRow>,
    pub(crate) authids: Vec<PgAuthIdRow>,
    pub(crate) auth_members: Vec<PgAuthMembersRow>,
    pub(crate) databases: Vec<PgDatabaseRow>,
    pub(crate) tablespaces: Vec<PgTablespaceRow>,
    pub(crate) next_rel_number: u32,
    pub(crate) next_oid: u32,
}

impl Default for Catalog {
    fn default() -> Self {
        let mut catalog = Self {
            tables: BTreeMap::new(),
            constraints: Vec::new(),
            depends: Vec::new(),
            inherits: Vec::new(),
            partitioned_tables: Vec::new(),
            rewrites: Vec::new(),
            triggers: Vec::new(),
            policies: Vec::new(),
            publications: Vec::new(),
            publication_rels: Vec::new(),
            publication_namespaces: Vec::new(),
            statistics_ext: Vec::new(),
            statistics_ext_data: Vec::new(),
            authids: bootstrap_pg_authid_rows(),
            auth_members: bootstrap_pg_auth_members_rows().into(),
            databases: bootstrap_pg_database_rows().into(),
            tablespaces: bootstrap_pg_tablespace_rows().into(),
            next_rel_number: DEFAULT_FIRST_REL_NUMBER,
            next_oid: DEFAULT_FIRST_USER_OID,
        };
        catalog.insert_bootstrap_relations();
        catalog
    }
}

impl Catalog {
    fn insert_bootstrap_relations(&mut self) {
        for kind in bootstrap_catalog_kinds() {
            let entry = bootstrap_catalog_entry(kind);
            self.insert(kind.relation_name(), entry);
        }
        insert_bootstrap_system_indexes(self);
    }

    pub fn insert(&mut self, name: impl Into<String>, entry: CatalogEntry) {
        let name = name.into().to_ascii_lowercase();
        self.next_rel_number = self
            .next_rel_number
            .max(entry.rel.rel_number.saturating_add(1));
        let next_attrdef_oid = entry
            .desc
            .columns
            .iter()
            .filter_map(|column| column.attrdef_oid)
            .max()
            .map(|oid| oid.saturating_add(1))
            .unwrap_or(self.next_oid);
        let next_constraint_oid = entry
            .desc
            .columns
            .iter()
            .filter_map(|column| column.not_null_constraint_oid)
            .max()
            .map(|oid| oid.saturating_add(1))
            .unwrap_or(self.next_oid);
        self.next_oid = self
            .next_oid
            .max(entry.relation_oid.saturating_add(1))
            .max(entry.row_type_oid.saturating_add(1))
            .max(entry.array_type_oid.saturating_add(1))
            .max(next_attrdef_oid)
            .max(next_constraint_oid);
        self.replace_constraint_rows_for_entry(&name, &entry);
        self.replace_depend_rows_for_entry(&entry);
        self.tables.insert(name, entry);
    }

    pub fn get(&self, name: &str) -> Option<&CatalogEntry> {
        self.tables.get(&name.to_ascii_lowercase())
    }

    pub fn get_by_oid(&self, relation_oid: u32) -> Option<&CatalogEntry> {
        self.tables
            .values()
            .find(|entry| entry.relation_oid == relation_oid)
    }

    pub fn relation_name_by_oid(&self, relation_oid: u32) -> Option<&str> {
        self.tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.as_str())
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, &CatalogEntry)> {
        self.tables
            .iter()
            .map(|(name, entry)| (name.as_str(), entry))
    }

    pub fn constraint_rows(&self) -> &[PgConstraintRow] {
        &self.constraints
    }

    pub fn depend_rows(&self) -> &[PgDependRow] {
        &self.depends
    }

    pub fn inherit_rows(&self) -> &[PgInheritsRow] {
        &self.inherits
    }

    pub fn partitioned_table_rows(&self) -> &[PgPartitionedTableRow] {
        &self.partitioned_tables
    }

    pub fn partitioned_table_row(&self, relation_oid: u32) -> Option<&PgPartitionedTableRow> {
        self.partitioned_tables
            .iter()
            .find(|row| row.partrelid == relation_oid)
    }

    pub fn inheritance_parents(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        self.inherits
            .iter()
            .filter(|row| row.inhrelid == relation_oid)
            .cloned()
            .collect()
    }

    pub fn inheritance_children(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        self.inherits
            .iter()
            .filter(|row| row.inhparent == relation_oid)
            .cloned()
            .collect()
    }

    pub fn find_all_inheritors(&self, relation_oid: u32) -> Vec<u32> {
        fn walk(catalog: &Catalog, relation_oid: u32, out: &mut Vec<u32>) {
            let mut child_oids = catalog
                .inheritance_children(relation_oid)
                .into_iter()
                .filter(|row| !row.inhdetachpending)
                .map(|row| row.inhrelid)
                .collect::<Vec<_>>();
            child_oids.sort_unstable();
            child_oids.dedup();
            for child_oid in child_oids {
                if out.contains(&child_oid) {
                    continue;
                }
                out.push(child_oid);
                walk(catalog, child_oid, out);
            }
        }

        let mut out = vec![relation_oid];
        walk(self, relation_oid, &mut out);
        out.sort_unstable();
        out
    }

    pub fn has_subclass(&self, relation_oid: u32) -> bool {
        self.tables
            .values()
            .find(|entry| entry.relation_oid == relation_oid)
            .map(|entry| entry.relhassubclass)
            .unwrap_or_else(|| !self.inheritance_children(relation_oid).is_empty())
    }

    pub fn rewrite_rows(&self) -> &[PgRewriteRow] {
        &self.rewrites
    }

    pub fn rewrite_rows_for_relation(&self, relation_oid: u32) -> &[PgRewriteRow] {
        let start = self
            .rewrites
            .partition_point(|row| row.ev_class < relation_oid);
        let end =
            start + self.rewrites[start..].partition_point(|row| row.ev_class == relation_oid);
        &self.rewrites[start..end]
    }

    pub fn trigger_rows(&self) -> &[PgTriggerRow] {
        &self.triggers
    }

    pub fn trigger_rows_for_relation(&self, relation_oid: u32) -> &[PgTriggerRow] {
        let start = self
            .triggers
            .partition_point(|row| row.tgrelid < relation_oid);
        let end = start + self.triggers[start..].partition_point(|row| row.tgrelid == relation_oid);
        &self.triggers[start..end]
    }

    pub fn policy_rows(&self) -> &[PgPolicyRow] {
        &self.policies
    }

    pub fn policy_rows_for_relation(&self, relation_oid: u32) -> &[PgPolicyRow] {
        let start = self
            .policies
            .partition_point(|row| row.polrelid < relation_oid);
        let end =
            start + self.policies[start..].partition_point(|row| row.polrelid == relation_oid);
        &self.policies[start..end]
    }

    pub fn publication_rows(&self) -> &[PgPublicationRow] {
        &self.publications
    }

    pub fn publication_rel_rows(&self) -> &[PgPublicationRelRow] {
        &self.publication_rels
    }

    pub fn publication_namespace_rows(&self) -> &[PgPublicationNamespaceRow] {
        &self.publication_namespaces
    }

    pub fn statistic_ext_rows(&self) -> &[PgStatisticExtRow] {
        &self.statistics_ext
    }

    pub fn statistic_ext_data_rows(&self) -> &[PgStatisticExtDataRow] {
        &self.statistics_ext_data
    }

    pub fn next_oid(&self) -> u32 {
        self.next_oid
    }

    pub fn authid_rows(&self) -> &[PgAuthIdRow] {
        &self.authids
    }

    pub fn auth_members_rows(&self) -> &[PgAuthMembersRow] {
        &self.auth_members
    }

    pub fn database_rows(&self) -> &[PgDatabaseRow] {
        &self.databases
    }

    pub fn tablespace_rows(&self) -> &[PgTablespaceRow] {
        &self.tablespaces
    }

    pub fn create_table(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
    ) -> Result<CatalogEntry, CatalogError> {
        self.create_table_with_options(
            name,
            desc,
            PUBLIC_NAMESPACE_OID,
            DEFAULT_DB_OID,
            'p',
            BOOTSTRAP_SUPERUSER_OID,
        )
    }

    pub fn create_table_with_options(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        namespace_oid: u32,
        db_oid: u32,
        relpersistence: char,
        owner_oid: u32,
    ) -> Result<CatalogEntry, CatalogError> {
        self.create_table_with_relkind(
            name,
            desc,
            namespace_oid,
            db_oid,
            relpersistence,
            'r',
            owner_oid,
        )
    }

    pub(crate) fn create_table_with_relkind(
        &mut self,
        name: impl Into<String>,
        mut desc: RelationDesc,
        namespace_oid: u32,
        db_oid: u32,
        relpersistence: char,
        relkind: char,
        owner_oid: u32,
    ) -> Result<CatalogEntry, CatalogError> {
        let name = name.into().to_ascii_lowercase();
        if self.tables.contains_key(&name) {
            return Err(CatalogError::TableAlreadyExists(name));
        }
        validate_builtin_type_rows(&desc)?;

        let relation_oid = self.next_oid;
        let row_type_oid = relation_oid.saturating_add(1);
        let array_type_oid = if row_type_oid != 0 {
            row_type_oid.saturating_add(1)
        } else {
            0
        };
        let mut next_oid = array_type_oid.saturating_add(1);
        if matches!(relkind, 'r' | 'p') {
            allocate_relation_object_oids(&mut desc, &mut next_oid);
        }
        let rel_number = if relkind_has_storage(relkind) {
            self.next_rel_number
        } else {
            0
        };
        let (relpages, reltuples) = if relkind == 'S' {
            (1, 1.0)
        } else if relkind_has_storage(relkind) {
            (0, -1.0)
        } else {
            (0, 0.0)
        };

        let entry = CatalogEntry {
            rel: RelFileLocator {
                spc_oid: DEFAULT_SPC_OID,
                db_oid,
                rel_number,
            },
            relation_oid,
            namespace_oid,
            owner_oid,
            relacl: None,
            reloptions: None,
            of_type_oid: 0,
            row_type_oid,
            array_type_oid,
            reltoastrelid: 0,
            relpersistence,
            relkind,
            am_oid: crate::include::catalog::relam_for_relkind(relkind),
            relhassubclass: false,
            relhastriggers: false,
            relispartition: false,
            relispopulated: true,
            relpartbound: None,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relpages,
            reltuples,
            relallvisible: 0,
            relallfrozen: 0,
            relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
            desc,
            partitioned_table: None,
            index_meta: None,
        };
        if relkind_has_storage(relkind) {
            self.next_rel_number = self.next_rel_number.saturating_add(1);
        }
        self.next_oid = next_oid;
        self.replace_constraint_rows_for_entry(&name, &entry);
        self.replace_depend_rows_for_entry(&entry);
        self.tables.insert(name, entry.clone());
        Ok(entry)
    }

    pub fn create_index(
        &mut self,
        index_name: impl Into<String>,
        table_name: &str,
        unique: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    ) -> Result<CatalogEntry, CatalogError> {
        let table = self
            .get(table_name)
            .ok_or_else(|| CatalogError::UnknownTable(table_name.to_string()))?;
        self.create_index_for_relation(index_name, table.relation_oid, unique, columns)
    }

    pub fn create_index_for_relation(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    ) -> Result<CatalogEntry, CatalogError> {
        self.create_index_for_relation_with_flags(index_name, relation_oid, unique, false, columns)
    }

    pub fn create_index_for_relation_with_flags(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        primary: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    ) -> Result<CatalogEntry, CatalogError> {
        let options = self.default_index_build_options(relation_oid, columns)?;
        self.create_index_for_relation_with_options_and_flags(
            index_name,
            relation_oid,
            unique,
            primary,
            columns,
            &options,
            None,
        )
    }

    pub fn create_index_for_relation_with_options(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
        options: &CatalogIndexBuildOptions,
    ) -> Result<CatalogEntry, CatalogError> {
        self.create_index_for_relation_with_options_and_flags(
            index_name,
            relation_oid,
            unique,
            false,
            columns,
            options,
            None,
        )
    }

    pub fn create_index_for_relation_with_options_and_flags(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        primary: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
        options: &CatalogIndexBuildOptions,
        predicate_sql: Option<&str>,
    ) -> Result<CatalogEntry, CatalogError> {
        let index_name = index_name.into().to_ascii_lowercase();
        if self.tables.contains_key(&index_name) {
            return Err(CatalogError::TableAlreadyExists(index_name));
        }

        let table = self
            .get_by_oid(relation_oid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'r' && table.relkind != 't' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let entry = build_catalog_index_entry(
            &table,
            unique,
            primary,
            columns,
            options,
            predicate_sql,
            self.next_oid,
            self.next_rel_number,
            'i',
            false,
            false,
        )?;
        self.next_rel_number = self
            .next_rel_number
            .saturating_add(u32::from(relkind_has_storage(entry.relkind)));
        self.next_oid = self.next_oid.saturating_add(1);
        self.replace_depend_rows_for_entry(&entry);
        self.tables.insert(index_name, entry.clone());
        Ok(entry)
    }

    pub fn create_partitioned_index_for_relation_with_options_and_flags(
        &mut self,
        index_name: impl Into<String>,
        relation_oid: u32,
        unique: bool,
        primary: bool,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
        options: &CatalogIndexBuildOptions,
        predicate_sql: Option<&str>,
    ) -> Result<CatalogEntry, CatalogError> {
        let index_name = index_name.into().to_ascii_lowercase();
        if self.tables.contains_key(&index_name) {
            return Err(CatalogError::TableAlreadyExists(index_name));
        }

        let table = self
            .get_by_oid(relation_oid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'p' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let entry = build_catalog_index_entry(
            &table,
            unique,
            primary,
            columns,
            options,
            predicate_sql,
            self.next_oid,
            0,
            'I',
            true,
            true,
        )?;
        self.next_oid = self.next_oid.saturating_add(1);
        self.replace_depend_rows_for_entry(&entry);
        self.tables.insert(index_name, entry.clone());
        Ok(entry)
    }

    pub fn create_index_backed_constraint(
        &mut self,
        relation_oid: u32,
        index_oid: u32,
        conname: impl Into<String>,
        contype: char,
        primary_key_owned_not_null_oids: &[u32],
    ) -> Result<PgConstraintRow, CatalogError> {
        self.create_index_backed_constraint_with_inheritance(
            relation_oid,
            index_oid,
            conname,
            contype,
            primary_key_owned_not_null_oids,
            0,
            true,
            0,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_index_backed_constraint_with_inheritance(
        &mut self,
        relation_oid: u32,
        index_oid: u32,
        conname: impl Into<String>,
        contype: char,
        primary_key_owned_not_null_oids: &[u32],
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        connoinherit: bool,
    ) -> Result<PgConstraintRow, CatalogError> {
        self.create_index_backed_constraint_with_inheritance_and_period(
            relation_oid,
            index_oid,
            conname,
            contype,
            primary_key_owned_not_null_oids,
            conparentid,
            conislocal,
            coninhcount,
            connoinherit,
            false,
            None,
            false,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_index_backed_constraint_with_inheritance_and_period(
        &mut self,
        relation_oid: u32,
        index_oid: u32,
        conname: impl Into<String>,
        contype: char,
        primary_key_owned_not_null_oids: &[u32],
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        connoinherit: bool,
        conperiod: bool,
        conexclop: Option<Vec<u32>>,
        deferrable: bool,
        initially_deferred: bool,
    ) -> Result<PgConstraintRow, CatalogError> {
        let table = self
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'r' && table.relkind != 'p' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let table_namespace_oid = table.namespace_oid;
        let index = self
            .get_by_oid(index_oid)
            .ok_or_else(|| CatalogError::UnknownTable(index_oid.to_string()))?;
        if index.relkind != 'i' && index.relkind != 'I' {
            return Err(CatalogError::UnknownTable(index_oid.to_string()));
        }

        let conname = conname.into();
        if self.constraints.iter().any(|row| {
            row.conrelid == relation_oid
                && row.contype == contype
                && row.conname.eq_ignore_ascii_case(&conname)
        }) {
            return Err(CatalogError::TableAlreadyExists(conname));
        }

        let row = PgConstraintRow {
            oid: self.next_oid,
            conname,
            connamespace: table_namespace_oid,
            contype,
            condeferrable: deferrable,
            condeferred: initially_deferred,
            conenforced: true,
            convalidated: true,
            conrelid: relation_oid,
            contypid: 0,
            conindid: index_oid,
            conparentid,
            confrelid: 0,
            confupdtype: ' ',
            confdeltype: ' ',
            confmatchtype: ' ',
            conkey: index.index_meta.as_ref().map(index_constraint_key_attnums),
            confkey: None,
            conpfeqop: None,
            conppeqop: None,
            conffeqop: None,
            confdelsetcols: None,
            conexclop,
            conbin: None,
            conislocal,
            coninhcount,
            connoinherit,
            conperiod,
        };
        self.next_oid = self.next_oid.saturating_add(1);
        self.constraints.push(row.clone());
        sort_pg_constraint_rows(&mut self.constraints);
        self.depends.extend(index_backed_constraint_depend_rows(
            row.oid,
            relation_oid,
            index_oid,
        ));
        if contype == CONSTRAINT_PRIMARY {
            for &not_null_constraint_oid in primary_key_owned_not_null_oids {
                self.depends.extend(primary_key_owned_not_null_depend_rows(
                    not_null_constraint_oid,
                    row.oid,
                ));
            }
        }
        sort_pg_depend_rows(&mut self.depends);
        Ok(row)
    }

    pub fn update_index_backed_constraint_inheritance(
        &mut self,
        relation_oid: u32,
        constraint_oid: u32,
        conparentid: u32,
        conislocal: bool,
        coninhcount: i16,
        connoinherit: bool,
    ) -> Result<PgConstraintRow, CatalogError> {
        let position = self
            .constraints
            .iter()
            .position(|row| row.oid == constraint_oid && row.conrelid == relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(constraint_oid.to_string()))?;
        let mut row = self.constraints[position].clone();
        if !matches!(row.contype, CONSTRAINT_PRIMARY | CONSTRAINT_UNIQUE) || row.conindid == 0 {
            return Err(CatalogError::UnknownTable(constraint_oid.to_string()));
        }
        let primary_key_owned_not_null_oids = self
            .depends
            .iter()
            .filter(|depend| {
                depend.classid == PG_CONSTRAINT_RELATION_OID
                    && depend.refclassid == PG_CONSTRAINT_RELATION_OID
                    && depend.refobjid == constraint_oid
                    && depend.deptype == DEPENDENCY_INTERNAL
            })
            .map(|depend| depend.objid)
            .collect::<Vec<_>>();
        row.conparentid = conparentid;
        row.conislocal = conislocal;
        row.coninhcount = coninhcount;
        row.connoinherit = connoinherit;
        self.constraints[position] = row.clone();
        sort_pg_constraint_rows(&mut self.constraints);
        self.depends
            .retain(|depend| depend.objid != constraint_oid && depend.refobjid != constraint_oid);
        self.depends.extend(index_backed_constraint_depend_rows(
            row.oid,
            row.conrelid,
            row.conindid,
        ));
        if row.contype == CONSTRAINT_PRIMARY {
            for not_null_constraint_oid in primary_key_owned_not_null_oids {
                self.depends.extend(primary_key_owned_not_null_depend_rows(
                    not_null_constraint_oid,
                    row.oid,
                ));
            }
        }
        sort_pg_depend_rows(&mut self.depends);
        Ok(row)
    }

    pub fn create_check_constraint(
        &mut self,
        relation_oid: u32,
        conname: impl Into<String>,
        conenforced: bool,
        convalidated: bool,
        conbin: impl Into<String>,
    ) -> Result<PgConstraintRow, CatalogError> {
        let table = self
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }

        let conname = conname.into();
        if self
            .constraints
            .iter()
            .any(|row| row.conrelid == relation_oid && row.conname.eq_ignore_ascii_case(&conname))
        {
            return Err(CatalogError::TableAlreadyExists(conname));
        }

        let row = PgConstraintRow {
            oid: self.next_oid,
            conname,
            connamespace: table.namespace_oid,
            contype: crate::include::catalog::CONSTRAINT_CHECK,
            condeferrable: false,
            condeferred: false,
            conenforced,
            convalidated,
            conrelid: relation_oid,
            contypid: 0,
            conindid: 0,
            conparentid: 0,
            confrelid: 0,
            confupdtype: ' ',
            confdeltype: ' ',
            confmatchtype: ' ',
            conkey: None,
            confkey: None,
            conpfeqop: None,
            conppeqop: None,
            conffeqop: None,
            confdelsetcols: None,
            conexclop: None,
            conbin: Some(conbin.into()),
            conislocal: true,
            coninhcount: 0,
            connoinherit: false,
            conperiod: false,
        };
        self.next_oid = self.next_oid.saturating_add(1);
        self.constraints.push(row.clone());
        sort_pg_constraint_rows(&mut self.constraints);
        self.depends
            .extend(relation_constraint_depend_rows(row.oid, relation_oid));
        sort_pg_depend_rows(&mut self.depends);
        Ok(row)
    }

    pub fn create_foreign_key_constraint(
        &mut self,
        relation_oid: u32,
        conname: impl Into<String>,
        deferrable: bool,
        initially_deferred: bool,
        conenforced: bool,
        convalidated: bool,
        local_attnums: &[i16],
        referenced_relation_oid: u32,
        referenced_index_oid: u32,
        referenced_attnums: &[i16],
        confupdtype: char,
        confdeltype: char,
        confmatchtype: char,
        confdelsetcols: Option<&[i16]>,
        conperiod: bool,
    ) -> Result<PgConstraintRow, CatalogError> {
        let table = self
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let referenced_table = self
            .get_by_oid(referenced_relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(referenced_relation_oid.to_string()))?;
        if referenced_table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(
                referenced_relation_oid.to_string(),
            ));
        }
        let referenced_index = self
            .get_by_oid(referenced_index_oid)
            .ok_or_else(|| CatalogError::UnknownTable(referenced_index_oid.to_string()))?;
        if referenced_index.relkind != 'i' {
            return Err(CatalogError::UnknownTable(referenced_index_oid.to_string()));
        }

        let conname = conname.into();
        if self
            .constraints
            .iter()
            .any(|row| row.conrelid == relation_oid && row.conname.eq_ignore_ascii_case(&conname))
        {
            return Err(CatalogError::TableAlreadyExists(conname));
        }

        let equality_ops = referenced_index
            .index_meta
            .as_ref()
            .and_then(|meta| foreign_key_equality_operators(&meta.indclass));
        let row = PgConstraintRow {
            oid: self.next_oid,
            conname,
            connamespace: table.namespace_oid,
            contype: crate::include::catalog::CONSTRAINT_FOREIGN,
            condeferrable: deferrable,
            condeferred: initially_deferred,
            conenforced,
            convalidated,
            conrelid: relation_oid,
            contypid: 0,
            conindid: referenced_index_oid,
            conparentid: 0,
            confrelid: referenced_relation_oid,
            confupdtype,
            confdeltype,
            confmatchtype,
            conkey: Some(local_attnums.to_vec()),
            confkey: Some(referenced_attnums.to_vec()),
            conpfeqop: equality_ops.clone(),
            conppeqop: equality_ops.clone(),
            conffeqop: equality_ops,
            confdelsetcols: confdelsetcols.map(<[i16]>::to_vec),
            conexclop: None,
            conbin: None,
            conislocal: true,
            coninhcount: 0,
            connoinherit: false,
            conperiod,
        };
        self.next_oid = self.next_oid.saturating_add(1);
        self.constraints.push(row.clone());
        sort_pg_constraint_rows(&mut self.constraints);
        self.depends.extend(foreign_key_constraint_depend_rows(
            row.oid,
            relation_oid,
            referenced_relation_oid,
            referenced_index_oid,
        ));
        sort_pg_depend_rows(&mut self.depends);
        Ok(row)
    }

    pub fn drop_relation_entry_by_oid(
        &mut self,
        relation_oid: u32,
    ) -> Result<(String, CatalogEntry), CatalogError> {
        self.remove_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))
    }

    pub fn set_column_not_null(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        constraint_name: impl Into<String>,
        validated: bool,
        no_inherit: bool,
        primary_key_owned: bool,
    ) -> Result<(u32, String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self.relation_name_for_oid(relation_oid)?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if !matches!(old_entry.relkind, 'r' | 'p') {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }

        let column_index = relation_column_index(&old_entry.desc, column_name)?;
        let mut new_entry = old_entry.clone();
        let column = &mut new_entry.desc.columns[column_index];
        column.storage.nullable = false;
        if column.not_null_constraint_oid.is_none() {
            column.not_null_constraint_oid = Some(self.next_oid);
            self.next_oid = self.next_oid.saturating_add(1);
        }
        let constraint_oid = column
            .not_null_constraint_oid
            .expect("not-null constraint oid");
        column.not_null_constraint_name = Some(constraint_name.into());
        column.not_null_constraint_validated = validated;
        column.not_null_constraint_no_inherit = no_inherit;
        column.not_null_primary_key_owned = primary_key_owned;

        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *entry = new_entry.clone();
        self.replace_constraint_rows_for_entry(&name, &new_entry);
        self.replace_depend_rows_for_entry(&new_entry);
        Ok((constraint_oid, name, old_entry, new_entry))
    }

    pub fn drop_column_not_null(
        &mut self,
        relation_oid: u32,
        column_name: &str,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self.relation_name_for_oid(relation_oid)?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if !matches!(old_entry.relkind, 'r' | 'p') {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }

        let column_index = relation_column_index(&old_entry.desc, column_name)?;
        let mut new_entry = old_entry.clone();
        let column = &mut new_entry.desc.columns[column_index];
        column.storage.nullable = true;
        column.not_null_constraint_oid = None;
        column.not_null_constraint_name = None;
        column.not_null_constraint_validated = false;
        column.not_null_constraint_no_inherit = false;
        column.not_null_primary_key_owned = false;

        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *entry = new_entry.clone();
        self.replace_constraint_rows_for_entry(&name, &new_entry);
        self.replace_depend_rows_for_entry(&new_entry);
        Ok((name, old_entry, new_entry))
    }

    pub fn validate_not_null_constraint(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self.relation_name_for_oid(relation_oid)?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if !matches!(old_entry.relkind, 'r' | 'p') {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }

        let column_index = not_null_constraint_column_index(&old_entry.desc, constraint_name)?;
        let mut new_entry = old_entry.clone();
        new_entry.desc.columns[column_index].not_null_constraint_validated = true;

        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *entry = new_entry.clone();
        self.replace_constraint_rows_for_entry(&name, &new_entry);
        self.replace_depend_rows_for_entry(&new_entry);
        Ok((name, old_entry, new_entry))
    }

    pub fn validate_check_constraint(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
    ) -> Result<(PgConstraintRow, PgConstraintRow), CatalogError> {
        let row = self
            .constraints
            .iter_mut()
            .find(|row| {
                row.conrelid == relation_oid
                    && row.contype == crate::include::catalog::CONSTRAINT_CHECK
                    && row.conname.eq_ignore_ascii_case(constraint_name)
            })
            .ok_or_else(|| CatalogError::UnknownTable(constraint_name.to_string()))?;
        let old_row = row.clone();
        row.convalidated = true;
        let new_row = row.clone();
        Ok((old_row, new_row))
    }

    pub fn validate_foreign_key_constraint(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
    ) -> Result<(PgConstraintRow, PgConstraintRow), CatalogError> {
        let row = self
            .constraints
            .iter_mut()
            .find(|row| {
                row.conrelid == relation_oid
                    && row.contype == crate::include::catalog::CONSTRAINT_FOREIGN
                    && row.conname.eq_ignore_ascii_case(constraint_name)
            })
            .ok_or_else(|| CatalogError::UnknownTable(constraint_name.to_string()))?;
        let old_row = row.clone();
        row.convalidated = true;
        let new_row = row.clone();
        Ok((old_row, new_row))
    }

    pub fn alter_foreign_key_constraint_attributes(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
        deferrable: bool,
        initially_deferred: bool,
        enforced: bool,
        validated: bool,
    ) -> Result<(PgConstraintRow, PgConstraintRow), CatalogError> {
        let row = self
            .constraints
            .iter_mut()
            .find(|row| {
                row.conrelid == relation_oid
                    && row.contype == crate::include::catalog::CONSTRAINT_FOREIGN
                    && row.conname.eq_ignore_ascii_case(constraint_name)
            })
            .ok_or_else(|| CatalogError::UnknownTable(constraint_name.to_string()))?;
        let old_row = row.clone();
        row.condeferrable = deferrable;
        row.condeferred = initially_deferred;
        row.conenforced = enforced;
        row.convalidated = validated;
        let new_row = row.clone();
        Ok((old_row, new_row))
    }

    pub fn drop_relation_constraint(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
    ) -> Result<PgConstraintRow, CatalogError> {
        let index = self
            .constraints
            .iter()
            .position(|row| {
                row.conrelid == relation_oid && row.conname.eq_ignore_ascii_case(constraint_name)
            })
            .ok_or_else(|| CatalogError::UnknownTable(constraint_name.to_string()))?;
        let removed = self.constraints.remove(index);
        self.depends
            .retain(|row| row.objid != removed.oid && row.refobjid != removed.oid);
        sort_pg_constraint_rows(&mut self.constraints);
        sort_pg_depend_rows(&mut self.depends);
        Ok(removed)
    }

    pub fn rename_relation_constraint(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
        new_constraint_name: &str,
    ) -> Result<
        (
            PgConstraintRow,
            PgConstraintRow,
            Option<(String, CatalogEntry, String, CatalogEntry)>,
        ),
        CatalogError,
    > {
        let new_constraint_name = new_constraint_name.to_ascii_lowercase();
        if self.constraints.iter().any(|row| {
            row.conrelid == relation_oid && row.conname.eq_ignore_ascii_case(&new_constraint_name)
        }) {
            return Err(CatalogError::TableAlreadyExists(
                new_constraint_name.clone(),
            ));
        }

        let row_index = self
            .constraints
            .iter()
            .position(|row| {
                row.conrelid == relation_oid && row.conname.eq_ignore_ascii_case(constraint_name)
            })
            .ok_or_else(|| CatalogError::UnknownTable(constraint_name.to_string()))?;

        let old_row = self.constraints[row_index].clone();
        let index_rename = if old_row.conindid != 0 {
            Some(self.rename_index_relation(old_row.conindid, &new_constraint_name)?)
        } else {
            None
        };

        self.constraints[row_index].conname = new_constraint_name.clone();
        let new_row = self.constraints[row_index].clone();
        sort_pg_constraint_rows(&mut self.constraints);

        if old_row.contype == CONSTRAINT_NOTNULL {
            self.rename_not_null_constraint_in_desc(
                relation_oid,
                constraint_name,
                &new_constraint_name,
            )?;
        }

        Ok((old_row, new_row, index_rename))
    }

    fn rename_index_relation(
        &mut self,
        relation_oid: u32,
        new_name: &str,
    ) -> Result<(String, CatalogEntry, String, CatalogEntry), CatalogError> {
        self.rename_relation_with_relkind(relation_oid, new_name, 'i')
    }

    fn rename_relation_with_relkind(
        &mut self,
        relation_oid: u32,
        new_name: &str,
        relkind: char,
    ) -> Result<(String, CatalogEntry, String, CatalogEntry), CatalogError> {
        let old_name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&old_name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if old_entry.relkind != relkind {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }

        let new_relname = new_name.to_ascii_lowercase();
        let qualified_new_name = old_name
            .rsplit_once('.')
            .map(|(schema, _)| format!("{schema}.{new_relname}"))
            .unwrap_or_else(|| new_relname.clone());
        if qualified_new_name != old_name && self.tables.contains_key(&qualified_new_name) {
            return Err(CatalogError::TableAlreadyExists(new_relname));
        }

        let entry = self
            .tables
            .remove(&old_name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        self.tables
            .insert(qualified_new_name.clone(), entry.clone());
        self.replace_depend_rows_for_entry(&entry);
        Ok((old_name, old_entry, qualified_new_name, entry))
    }

    fn rename_not_null_constraint_in_desc(
        &mut self,
        relation_oid: u32,
        constraint_name: &str,
        new_constraint_name: &str,
    ) -> Result<(), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let mut entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let column_index = not_null_constraint_column_index(&entry.desc, constraint_name)?;
        entry.desc.columns[column_index].not_null_constraint_name =
            Some(new_constraint_name.to_string());
        let slot = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *slot = entry.clone();
        self.replace_constraint_rows_for_entry(&name, &entry);
        self.replace_depend_rows_for_entry(&entry);
        Ok(())
    }

    fn default_index_build_options(
        &self,
        relation_oid: u32,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    ) -> Result<CatalogIndexBuildOptions, CatalogError> {
        let table = self
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let mut indclass = Vec::with_capacity(columns.len());
        let mut indcollation = Vec::with_capacity(columns.len());
        let mut indoption = Vec::with_capacity(columns.len());
        for column_name in columns {
            let column = table
                .desc
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&column_name.name))
                .ok_or_else(|| CatalogError::UnknownColumn(column_name.name.clone()))?;
            let type_oid = sql_type_oid(column.sql_type);
            if type_oid == 0 {
                return Err(CatalogError::UnknownType("index column type".into()));
            }
            let opclass_oid = if matches!(
                column.sql_type.element_type().kind,
                crate::backend::parser::SqlTypeKind::Enum
            ) {
                crate::include::catalog::ENUM_BTREE_OPCLASS_OID
            } else {
                crate::include::catalog::default_btree_opclass_oid(type_oid)
                    .ok_or_else(|| CatalogError::UnknownType("index column type".into()))?
            };
            indclass.push(opclass_oid);
            indcollation.push(0);
            let mut option = 0i16;
            if column_name.descending {
                option |= 0x0001;
            }
            if column_name.nulls_first.unwrap_or(column_name.descending) {
                option |= 0x0002;
            }
            indoption.push(option);
        }
        Ok(CatalogIndexBuildOptions {
            am_oid: crate::include::catalog::BTREE_AM_OID,
            indclass,
            indcollation,
            indoption,
            indnullsnotdistinct: false,
            indisexclusion: false,
            indimmediate: true,
            brin_options: None,
            gin_options: None,
            hash_options: None,
        })
    }

    pub fn drop_table(&mut self, name: &str) -> Result<CatalogEntry, CatalogError> {
        match self.tables.get(&name.to_ascii_lowercase()) {
            Some(entry) if entry.relkind == 'r' => {}
            _ => return Err(CatalogError::UnknownTable(name.to_string())),
        }
        let entry = self
            .tables
            .remove(&name.to_ascii_lowercase())
            .ok_or_else(|| CatalogError::UnknownTable(name.to_string()))?;
        self.constraints
            .retain(|row| row.conrelid != entry.relation_oid);
        self.depends
            .retain(|row| row.objid != entry.relation_oid && row.refobjid != entry.relation_oid);
        Ok(entry)
    }

    pub fn alter_table_add_column(
        &mut self,
        relation_oid: u32,
        column: crate::backend::executor::ColumnDesc,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if !matches!(old_entry.relkind, 'r' | 'p' | 'i' | 'I') {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        if old_entry
            .desc
            .columns
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(&column.name))
        {
            return Err(CatalogError::TableAlreadyExists(column.name));
        }

        let mut new_entry = old_entry.clone();
        new_entry.desc.columns.push(column);
        allocate_relation_object_oids(&mut new_entry.desc, &mut self.next_oid);
        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *entry = new_entry.clone();
        self.replace_constraint_rows_for_entry(&name, &new_entry);
        self.replace_depend_rows_for_entry(&new_entry);
        Ok((name, old_entry, new_entry))
    }

    pub fn alter_table_set_column_default(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        default_expr: Option<String>,
        default_sequence_oid: Option<u32>,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if !matches!(old_entry.relkind, 'r' | 'p') {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let column_index = relation_column_index(&old_entry.desc, column_name)?;

        let mut new_entry = old_entry.clone();
        let column = &mut new_entry.desc.columns[column_index];
        column.default_expr = default_expr;
        column.default_sequence_oid = default_sequence_oid;
        if column.default_expr.is_some() {
            if column.attrdef_oid.is_none() {
                column.attrdef_oid = Some(self.next_oid);
                self.next_oid = self.next_oid.saturating_add(1);
            }
        } else {
            column.attrdef_oid = None;
            column.missing_default_value = None;
        }

        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *entry = new_entry.clone();
        self.replace_constraint_rows_for_entry(&name, &new_entry);
        self.replace_depend_rows_for_entry(&new_entry);
        Ok((name, old_entry, new_entry))
    }

    pub fn alter_table_set_column_generation(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        default_expr: Option<String>,
        generated: Option<crate::include::nodes::parsenodes::ColumnGeneratedKind>,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if old_entry.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let column_index = relation_column_index(&old_entry.desc, column_name)?;

        let mut new_entry = old_entry.clone();
        let column = &mut new_entry.desc.columns[column_index];
        column.default_expr = default_expr;
        column.default_sequence_oid = None;
        column.generated = generated;
        if column.default_expr.is_some() {
            if column.attrdef_oid.is_none() {
                column.attrdef_oid = Some(self.next_oid);
                self.next_oid = self.next_oid.saturating_add(1);
            }
        } else {
            column.attrdef_oid = None;
            column.missing_default_value = None;
        }

        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *entry = new_entry.clone();
        self.replace_constraint_rows_for_entry(&name, &new_entry);
        self.replace_depend_rows_for_entry(&new_entry);
        Ok((name, old_entry, new_entry))
    }

    pub fn alter_table_set_column_statistics(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        statistics_target: i16,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        self.set_relation_column_statistics(relation_oid, column_name, statistics_target, &['r'])
    }

    pub fn alter_index_set_column_statistics(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        statistics_target: i16,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        self.set_relation_column_statistics(relation_oid, column_name, statistics_target, &['i'])
    }

    fn set_relation_column_statistics(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        statistics_target: i16,
        allowed_relkinds: &[char],
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if !allowed_relkinds.contains(&old_entry.relkind) {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let column_index = relation_column_index(&old_entry.desc, column_name)?;

        let mut new_entry = old_entry.clone();
        new_entry.desc.columns[column_index].attstattarget = statistics_target;

        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *entry = new_entry.clone();
        Ok((name, old_entry, new_entry))
    }

    pub fn alter_table_set_column_storage(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        storage: crate::include::access::htup::AttributeStorage,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if old_entry.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let column_index = relation_column_index(&old_entry.desc, column_name)?;

        let mut new_entry = old_entry.clone();
        new_entry.desc.columns[column_index].storage.attstorage = storage;

        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *entry = new_entry.clone();
        Ok((name, old_entry, new_entry))
    }

    pub fn alter_table_drop_column(
        &mut self,
        relation_oid: u32,
        column_name: &str,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if old_entry.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let column_index = old_entry
            .desc
            .columns
            .iter()
            .enumerate()
            .find_map(|(index, column)| {
                (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
            })
            .ok_or_else(|| CatalogError::UnknownColumn(column_name.to_string()))?;

        let mut new_entry = old_entry.clone();
        let attnum = column_index + 1;
        let column = &mut new_entry.desc.columns[column_index];
        column.name = dropped_column_name(attnum);
        column.storage.name = column.name.clone();
        column.storage.nullable = true;
        column.dropped = true;
        column.attstattarget = -1;
        column.not_null_constraint_oid = None;
        column.not_null_constraint_name = None;
        column.not_null_constraint_validated = false;
        column.not_null_primary_key_owned = false;
        column.attrdef_oid = None;
        column.default_expr = None;
        column.generated = None;
        column.missing_default_value = None;

        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *entry = new_entry.clone();
        self.replace_constraint_rows_for_entry(&name, &new_entry);
        self.replace_depend_rows_for_entry(&new_entry);
        Ok((name, old_entry, new_entry))
    }

    pub fn alter_table_alter_column_type(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        new_column: crate::backend::executor::ColumnDesc,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if old_entry.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let column_index = old_entry
            .desc
            .columns
            .iter()
            .enumerate()
            .find_map(|(index, column)| {
                (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
            })
            .ok_or_else(|| CatalogError::UnknownColumn(column_name.to_string()))?;

        let mut new_entry = old_entry.clone();
        new_entry.desc.columns[column_index] = new_column;

        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *entry = new_entry.clone();
        self.replace_constraint_rows_for_entry(&name, &new_entry);
        self.replace_depend_rows_for_entry(&new_entry);
        Ok((name, old_entry, new_entry))
    }

    pub fn alter_table_rename_column(
        &mut self,
        relation_oid: u32,
        column_name: &str,
        new_column_name: &str,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if old_entry.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        if old_entry.desc.columns.iter().any(|column| {
            !column.dropped
                && !column.name.eq_ignore_ascii_case(column_name)
                && column.name.eq_ignore_ascii_case(new_column_name)
        }) {
            return Err(CatalogError::TableAlreadyExists(
                new_column_name.to_string(),
            ));
        }
        let column_index = old_entry
            .desc
            .columns
            .iter()
            .enumerate()
            .find_map(|(index, column)| {
                (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
            })
            .ok_or_else(|| CatalogError::UnknownColumn(column_name.to_string()))?;

        let mut new_entry = old_entry.clone();
        let column = &mut new_entry.desc.columns[column_index];
        column.name = new_column_name.to_string();
        column.storage.name = column.name.clone();

        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        *entry = new_entry.clone();
        self.replace_constraint_rows_for_entry(&name, &new_entry);
        self.replace_depend_rows_for_entry(&new_entry);
        Ok((name, old_entry, new_entry))
    }

    pub fn rename_relation(
        &mut self,
        relation_oid: u32,
        new_name: &str,
    ) -> Result<(String, CatalogEntry, String, CatalogEntry), CatalogError> {
        let old_name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&old_name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if old_entry.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }

        let new_relname = new_name.to_ascii_lowercase();
        let qualified_new_name = old_name
            .rsplit_once('.')
            .map(|(schema, _)| format!("{schema}.{new_relname}"))
            .unwrap_or_else(|| new_relname.clone());
        if qualified_new_name != old_name && self.tables.contains_key(&qualified_new_name) {
            return Err(CatalogError::TableAlreadyExists(new_relname));
        }

        let entry = self
            .tables
            .remove(&old_name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        self.tables
            .insert(qualified_new_name.clone(), entry.clone());
        self.replace_constraint_rows_for_entry(&qualified_new_name, &entry);
        self.replace_depend_rows_for_entry(&entry);
        Ok((old_name, old_entry, qualified_new_name, entry))
    }

    pub fn rewrite_relation_storage(
        &mut self,
        relation_oids: &[u32],
    ) -> Result<Vec<(String, CatalogEntry, CatalogEntry)>, CatalogError> {
        let mut rewritten = Vec::with_capacity(relation_oids.len());
        for &relation_oid in relation_oids {
            let name = self
                .tables
                .iter()
                .find(|(_, entry)| entry.relation_oid == relation_oid)
                .map(|(name, _)| name.clone())
                .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
            let old_entry = self
                .tables
                .get(&name)
                .cloned()
                .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
            if !matches!(old_entry.relkind, 'r' | 't' | 'i') {
                return Err(CatalogError::UnknownTable(relation_oid.to_string()));
            }

            let mut new_entry = old_entry.clone();
            new_entry.rel.rel_number = self.next_rel_number;
            self.next_rel_number = self.next_rel_number.saturating_add(1);

            let entry = self
                .tables
                .get_mut(&name)
                .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
            *entry = new_entry.clone();
            self.replace_constraint_rows_for_entry(&name, &new_entry);
            self.replace_depend_rows_for_entry(&new_entry);
            rewritten.push((name, old_entry, new_entry));
        }
        Ok(rewritten)
    }

    pub fn alter_relation_owner(
        &mut self,
        relation_oid: u32,
        new_owner_oid: u32,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        entry.owner_oid = new_owner_oid;
        let new_entry = entry.clone();
        Ok((name, old_entry, new_entry))
    }

    pub fn relation_owner_oid(&self, relation_oid: u32) -> Option<u32> {
        self.tables
            .values()
            .find(|entry| entry.relation_oid == relation_oid)
            .map(|entry| entry.owner_oid)
    }

    pub fn set_index_ready_valid(
        &mut self,
        relation_oid: u32,
        indisready: bool,
        indisvalid: bool,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if !matches!(old_entry.relkind, 'i' | 'I') {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let index_meta = entry.index_meta.as_mut().ok_or(CatalogError::Corrupt(
            "index relation missing index metadata",
        ))?;
        index_meta.indisready = indisready;
        index_meta.indisvalid = indisvalid;
        let new_entry = entry.clone();
        self.replace_depend_rows_for_entry(&new_entry);
        Ok((name, old_entry, new_entry))
    }

    pub fn set_relation_toast_relid(
        &mut self,
        relation_oid: u32,
        reltoastrelid: u32,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if old_entry.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        entry.reltoastrelid = reltoastrelid;
        let new_entry = entry.clone();
        Ok((name, old_entry, new_entry))
    }

    pub fn set_relation_stats(
        &mut self,
        relation_oid: u32,
        relpages: i32,
        reltuples: f64,
    ) -> Result<(String, CatalogEntry, CatalogEntry), CatalogError> {
        let name = self
            .tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let old_entry = self
            .tables
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let entry = self
            .tables
            .get_mut(&name)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        entry.relpages = relpages;
        entry.reltuples = reltuples;
        let new_entry = entry.clone();
        Ok((name, old_entry, new_entry))
    }

    pub fn remove_by_oid(&mut self, relation_oid: u32) -> Option<(String, CatalogEntry)> {
        let name = self
            .tables
            .iter()
            .find_map(|(name, entry)| (entry.relation_oid == relation_oid).then(|| name.clone()))?;
        let entry = self.tables.remove(&name)?;
        let rewrite_oids = self
            .remove_rewrite_rows_for_relation(relation_oid)
            .into_iter()
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        self.constraints.retain(|row| row.conrelid != relation_oid);
        self.depends.retain(|row| {
            row.objid != relation_oid
                && row.refobjid != relation_oid
                && !rewrite_oids.contains(&row.objid)
                && !rewrite_oids.contains(&row.refobjid)
        });
        Some((name, entry))
    }

    pub fn add_depend_row(&mut self, row: PgDependRow) {
        if self.depends.iter().any(|existing| existing == &row) {
            return;
        }
        self.depends.push(row);
        sort_pg_depend_rows(&mut self.depends);
    }

    pub fn add_inherit_row(&mut self, row: PgInheritsRow) {
        if self.inherits.iter().any(|existing| existing == &row) {
            return;
        }
        self.inherits.push(row);
        sort_pg_inherits_rows(&mut self.inherits);
    }

    pub fn attach_inheritance(
        &mut self,
        relation_oid: u32,
        parent_oids: &[u32],
    ) -> Result<(), CatalogError> {
        let child_name = self
            .relation_name_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?
            .to_string();
        for (index, parent_oid) in parent_oids.iter().copied().enumerate() {
            let parent_name = self
                .relation_name_by_oid(parent_oid)
                .ok_or_else(|| CatalogError::UnknownTable(parent_oid.to_string()))?
                .to_string();
            let Some(parent) = self.tables.get_mut(&parent_name) else {
                return Err(CatalogError::UnknownTable(parent_oid.to_string()));
            };
            parent.relhassubclass = true;
            self.add_inherit_row(PgInheritsRow {
                inhrelid: relation_oid,
                inhparent: parent_oid,
                inhseqno: index.saturating_add(1) as i32,
                inhdetachpending: false,
            });
        }
        let child = self
            .tables
            .get(&child_name)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        self.replace_depend_rows_for_entry(&child);
        Ok(())
    }

    pub fn detach_inheritance(&mut self, relation_oid: u32) -> Result<Vec<u32>, CatalogError> {
        if self.get_by_oid(relation_oid).is_none() {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let parent_oids = self
            .inheritance_parents(relation_oid)
            .into_iter()
            .map(|row| row.inhparent)
            .collect::<Vec<_>>();
        self.inherits.retain(|row| row.inhrelid != relation_oid);
        for parent_oid in &parent_oids {
            if let Some(parent_name) = self.relation_name_by_oid(*parent_oid).map(str::to_string)
                && let Some(parent) = self.tables.get_mut(&parent_name)
            {
                parent.relhassubclass =
                    self.inherits.iter().any(|row| row.inhparent == *parent_oid);
            }
        }
        if let Some(child_name) = self.relation_name_by_oid(relation_oid).map(str::to_string)
            && let Some(child) = self.tables.get(&child_name).cloned()
        {
            self.replace_depend_rows_for_entry(&child);
        }
        Ok(parent_oids)
    }

    pub fn add_rewrite_row(&mut self, row: PgRewriteRow) {
        if self.rewrites.iter().any(|existing| existing == &row) {
            return;
        }
        self.next_oid = self.next_oid.max(row.oid.saturating_add(1));
        self.rewrites.push(row);
        sort_pg_rewrite_rows(&mut self.rewrites);
    }

    pub fn remove_rewrite_rows_for_relation(&mut self, relation_oid: u32) -> Vec<PgRewriteRow> {
        let mut removed = Vec::new();
        self.rewrites.retain(|row| {
            if row.ev_class == relation_oid {
                removed.push(row.clone());
                false
            } else {
                true
            }
        });
        removed
    }

    pub fn add_policy_row(&mut self, row: PgPolicyRow) {
        if self.policies.iter().any(|existing| existing == &row) {
            return;
        }
        self.next_oid = self.next_oid.max(row.oid.saturating_add(1));
        self.policies.push(row);
        sort_pg_policy_rows(&mut self.policies);
    }

    pub fn remove_policy_rows_for_relation(&mut self, relation_oid: u32) -> Vec<PgPolicyRow> {
        let mut removed = Vec::new();
        self.policies.retain(|row| {
            if row.polrelid == relation_oid {
                removed.push(row.clone());
                false
            } else {
                true
            }
        });
        removed
    }

    pub fn create_trigger(&mut self, mut row: PgTriggerRow) -> Result<PgTriggerRow, CatalogError> {
        let relation_name = self.relation_name_for_oid(row.tgrelid)?;
        let Some(entry) = self.tables.get(&relation_name) else {
            return Err(CatalogError::UnknownTable(row.tgrelid.to_string()));
        };
        if self
            .trigger_rows_for_relation(row.tgrelid)
            .iter()
            .any(|existing| existing.tgname.eq_ignore_ascii_case(&row.tgname))
        {
            return Err(CatalogError::UniqueViolation(
                "pg_trigger_tgrelid_tgname_index".into(),
            ));
        }
        if row.oid == 0 {
            row.oid = self.next_oid;
        }
        self.next_oid = self.next_oid.max(row.oid.saturating_add(1));
        self.triggers.push(row.clone());
        crate::include::catalog::sort_pg_trigger_rows(&mut self.triggers);
        self.depends.extend(trigger_depend_rows(
            row.oid,
            row.tgrelid,
            row.tgfoid,
            &row.tgattr,
        ));
        sort_pg_depend_rows(&mut self.depends);
        if !entry.relhastriggers {
            let mut new_entry = entry.clone();
            new_entry.relhastriggers = true;
            self.tables.insert(relation_name, new_entry);
        }
        Ok(row)
    }

    pub fn replace_trigger(
        &mut self,
        relation_oid: u32,
        trigger_name: &str,
        row: PgTriggerRow,
    ) -> Result<PgTriggerRow, CatalogError> {
        let old = self.drop_trigger(relation_oid, trigger_name)?;
        let mut row = row;
        row.oid = old.oid;
        self.create_trigger(row)
    }

    pub fn drop_trigger(
        &mut self,
        relation_oid: u32,
        trigger_name: &str,
    ) -> Result<PgTriggerRow, CatalogError> {
        let relation_name = self.relation_name_for_oid(relation_oid)?;
        let index = self
            .triggers
            .iter()
            .position(|row| {
                row.tgrelid == relation_oid && row.tgname.eq_ignore_ascii_case(trigger_name)
            })
            .ok_or_else(|| CatalogError::UnknownTable(trigger_name.to_string()))?;
        let removed = self.triggers.remove(index);
        self.depends
            .retain(|row| row.objid != removed.oid && row.refobjid != removed.oid);
        let has_remaining = self.triggers.iter().any(|row| row.tgrelid == relation_oid);
        if let Some(entry) = self.tables.get(&relation_name).cloned()
            && entry.relhastriggers != has_remaining
        {
            let mut new_entry = entry;
            new_entry.relhastriggers = has_remaining;
            self.tables.insert(relation_name, new_entry);
        }
        Ok(removed)
    }

    pub fn remove_rewrite_row_by_oid(&mut self, rewrite_oid: u32) -> Option<PgRewriteRow> {
        let position = self
            .rewrites
            .iter()
            .position(|row| row.oid == rewrite_oid)?;
        Some(self.rewrites.remove(position))
    }

    fn replace_constraint_rows_for_entry(&mut self, relation_name: &str, entry: &CatalogEntry) {
        self.constraints.retain(|row| {
            !(row.conrelid == entry.relation_oid && row.contype == CONSTRAINT_NOTNULL)
        });
        if !matches!(entry.relkind, 'r' | 'p') {
            return;
        }
        let relname = relation_name.rsplit('.').next().unwrap_or(relation_name);
        self.constraints.extend(derived_pg_constraint_rows(
            entry.relation_oid,
            relname,
            entry.namespace_oid,
            &entry.desc,
        ));
        sort_pg_constraint_rows(&mut self.constraints);
    }

    fn replace_depend_rows_for_entry(&mut self, entry: &CatalogEntry) {
        self.replace_partitioned_table_rows_for_entry(entry);
        let entry_object_oids = entry_owned_object_oids(entry);
        self.depends
            .retain(|row| !entry_object_oids.contains(&row.objid));
        if entry.relation_oid < DEFAULT_FIRST_USER_OID {
            return;
        }
        self.depends.extend(derived_pg_depend_rows(entry));
        if matches!(entry.relkind, 'r' | 'p')
            && let Some(primary_constraint_oid) =
                self.primary_constraint_oid_for_relation(entry.relation_oid)
        {
            for column in &entry.desc.columns {
                if column.not_null_primary_key_owned
                    && let Some(not_null_constraint_oid) = column.not_null_constraint_oid
                {
                    self.depends.extend(primary_key_owned_not_null_depend_rows(
                        not_null_constraint_oid,
                        primary_constraint_oid,
                    ));
                }
            }
        }
        self.depends.extend(inheritance_depend_rows(
            entry.relation_oid,
            &self
                .inheritance_parents(entry.relation_oid)
                .into_iter()
                .map(|row| row.inhparent)
                .collect::<Vec<_>>(),
        ));
        sort_pg_depend_rows(&mut self.depends);
    }

    fn replace_partitioned_table_rows_for_entry(&mut self, entry: &CatalogEntry) {
        self.partitioned_tables
            .retain(|row| row.partrelid != entry.relation_oid);
        if let Some(row) = &entry.partitioned_table {
            self.partitioned_tables.push(row.clone());
            sort_pg_partitioned_table_rows(&mut self.partitioned_tables);
        }
    }

    fn relation_name_for_oid(&self, relation_oid: u32) -> Result<String, CatalogError> {
        self.tables
            .iter()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
            .map(|(name, _)| name.clone())
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))
    }

    fn primary_constraint_oid_for_relation(&self, relation_oid: u32) -> Option<u32> {
        self.constraints
            .iter()
            .find(|row| {
                row.conrelid == relation_oid
                    && row.contype == crate::include::catalog::CONSTRAINT_PRIMARY
            })
            .map(|row| row.oid)
    }
}

fn foreign_key_equality_operators(indclass: &[u32]) -> Option<Vec<u32>> {
    let opclasses = crate::include::catalog::bootstrap_pg_opclass_rows();
    let amops = crate::include::catalog::bootstrap_pg_amop_rows();
    indclass
        .iter()
        .map(|opclass_oid| {
            let family = opclasses
                .iter()
                .find(|row| row.oid == *opclass_oid)?
                .opcfamily;
            amops
                .iter()
                .find(|row| row.amopfamily == family && row.amopstrategy == 3)
                .map(|row| row.amopopr)
        })
        .collect()
}

fn entry_owned_object_oids(entry: &CatalogEntry) -> BTreeSet<u32> {
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

fn relation_column_index(desc: &RelationDesc, column_name: &str) -> Result<usize, CatalogError> {
    desc.columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
        })
        .ok_or_else(|| CatalogError::UnknownColumn(column_name.to_string()))
}

fn not_null_constraint_column_index(
    desc: &RelationDesc,
    constraint_name: &str,
) -> Result<usize, CatalogError> {
    desc.columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped
                && column
                    .not_null_constraint_name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(constraint_name)))
            .then_some(index)
        })
        .ok_or_else(|| CatalogError::UnknownTable(constraint_name.to_string()))
}

pub(crate) fn validate_builtin_type_rows(desc: &RelationDesc) -> Result<(), CatalogError> {
    let builtin_rows = builtin_type_rows();
    for column in &desc.columns {
        if matches!(column.sql_type.kind, SqlTypeKind::Trigger) {
            return Err(CatalogError::UnknownType("trigger".into()));
        }
        if !column.sql_type.is_array && column.sql_type.type_oid != 0 {
            continue;
        }
        let present = builtin_rows.iter().any(|row| {
            row.sql_type.kind == column.sql_type.kind
                && row.sql_type.is_array == column.sql_type.is_array
        }) || (column.sql_type.is_array
            && matches!(column.sql_type.kind, SqlTypeKind::Composite));
        if !present {
            return Err(CatalogError::UnknownType(format!(
                "{} (missing builtin pg_type row)",
                format_sql_type_name(column.sql_type)
            )));
        }
    }
    Ok(())
}

fn format_sql_type_name(sql_type: SqlType) -> String {
    if !sql_type.is_array
        && sql_type.type_oid != 0
        && let Some(name) = builtin_type_name_for_oid(sql_type.type_oid)
    {
        return name;
    }
    if sql_type.is_array {
        if sql_type.element_type().is_range() {
            return "unsupported array".into();
        }
        if sql_type.element_type().is_multirange() {
            return "unsupported array".into();
        }
        return match sql_type.kind {
            SqlTypeKind::AnyElement
            | SqlTypeKind::AnyRange
            | SqlTypeKind::AnyMultirange
            | SqlTypeKind::AnyCompatible
            | SqlTypeKind::AnyCompatibleArray
            | SqlTypeKind::AnyCompatibleRange
            | SqlTypeKind::AnyCompatibleMultirange
            | SqlTypeKind::AnyEnum => "unsupported array",
            SqlTypeKind::AnyArray => "anyarray",
            SqlTypeKind::Enum => "unsupported array",
            SqlTypeKind::Record => "unsupported array",
            SqlTypeKind::Composite => "_record",
            SqlTypeKind::Shell => "unsupported array",
            SqlTypeKind::Internal => "unsupported array",
            SqlTypeKind::Cstring => "_cstring",
            SqlTypeKind::Void => "unsupported array",
            SqlTypeKind::Trigger => "unsupported array",
            SqlTypeKind::FdwHandler => "unsupported array",
            SqlTypeKind::Bool => "_bool",
            SqlTypeKind::Bit => "_bit",
            SqlTypeKind::VarBit => "_varbit",
            SqlTypeKind::Bytea => "_bytea",
            SqlTypeKind::Uuid => "_uuid",
            SqlTypeKind::Inet => "_inet",
            SqlTypeKind::Cidr => "_cidr",
            SqlTypeKind::MacAddr => "_macaddr",
            SqlTypeKind::MacAddr8 => "_macaddr8",
            SqlTypeKind::InternalChar => "_char",
            SqlTypeKind::Int8 => "_int8",
            SqlTypeKind::Name => "_name",
            SqlTypeKind::Int2 => "_int2",
            SqlTypeKind::Int4 => "_int4",
            SqlTypeKind::Text => "_text",
            SqlTypeKind::Oid => "_oid",
            SqlTypeKind::RegProc => "_regproc",
            SqlTypeKind::RegClass => "_regclass",
            SqlTypeKind::RegType => "_regtype",
            SqlTypeKind::RegRole => "_regrole",
            SqlTypeKind::RegNamespace => "_regnamespace",
            SqlTypeKind::RegOper => "_regoper",
            SqlTypeKind::RegOperator => "_regoperator",
            SqlTypeKind::RegProcedure => "_regprocedure",
            SqlTypeKind::RegCollation => "_regcollation",
            SqlTypeKind::Tid => "_tid",
            SqlTypeKind::Xid => "_xid",
            SqlTypeKind::Float4 => "_float4",
            SqlTypeKind::Float8 => "_float8",
            SqlTypeKind::Money => "_money",
            SqlTypeKind::Varchar => "_varchar",
            SqlTypeKind::Char => "_bpchar",
            SqlTypeKind::Date => "_date",
            SqlTypeKind::Time => "_time",
            SqlTypeKind::TimeTz => "_timetz",
            SqlTypeKind::Interval => "_interval",
            SqlTypeKind::Timestamp => "_timestamp",
            SqlTypeKind::TimestampTz => "_timestamptz",
            SqlTypeKind::Numeric => "_numeric",
            SqlTypeKind::Json => "_json",
            SqlTypeKind::Jsonb => "_jsonb",
            SqlTypeKind::JsonPath => "_jsonpath",
            SqlTypeKind::Xml => "_xml",
            SqlTypeKind::TsVector => "_tsvector",
            SqlTypeKind::TsQuery => "_tsquery",
            SqlTypeKind::PgLsn => "_pg_lsn",
            SqlTypeKind::RegConfig => "_regconfig",
            SqlTypeKind::RegDictionary => "_regdictionary",
            SqlTypeKind::Int2Vector
            | SqlTypeKind::OidVector
            | SqlTypeKind::Point
            | SqlTypeKind::Lseg
            | SqlTypeKind::Path
            | SqlTypeKind::Line
            | SqlTypeKind::Box
            | SqlTypeKind::Polygon
            | SqlTypeKind::Circle
            | SqlTypeKind::PgNodeTree => "unsupported array",
            SqlTypeKind::Range
            | SqlTypeKind::Int4Range
            | SqlTypeKind::Int8Range
            | SqlTypeKind::NumericRange
            | SqlTypeKind::DateRange
            | SqlTypeKind::TimestampRange
            | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
            SqlTypeKind::Multirange => unreachable!("multirange handled above"),
        }
        .into();
    }

    if sql_type.is_range() {
        return builtin_range_name_for_sql_type(sql_type)
            .unwrap_or("range")
            .into();
    }
    if sql_type.is_multirange() {
        return crate::include::catalog::builtin_multirange_name_for_sql_type(sql_type)
            .unwrap_or("multirange")
            .into();
    }

    match sql_type.kind {
        SqlTypeKind::AnyElement => "anyelement",
        SqlTypeKind::AnyArray => "anyarray",
        SqlTypeKind::AnyRange => "anyrange",
        SqlTypeKind::AnyMultirange => "anymultirange",
        SqlTypeKind::AnyCompatible => "anycompatible",
        SqlTypeKind::AnyCompatibleArray => "anycompatiblearray",
        SqlTypeKind::AnyCompatibleRange => "anycompatiblerange",
        SqlTypeKind::AnyCompatibleMultirange => "anycompatiblemultirange",
        SqlTypeKind::AnyEnum => "anyenum",
        SqlTypeKind::Enum => return sql_type.type_oid.to_string(),
        SqlTypeKind::Record => "record",
        SqlTypeKind::Composite => "record",
        SqlTypeKind::Shell => "shell",
        SqlTypeKind::Internal => "internal",
        SqlTypeKind::Cstring => "cstring",
        SqlTypeKind::Void => "void",
        SqlTypeKind::Trigger => "trigger",
        SqlTypeKind::FdwHandler => "fdw_handler",
        SqlTypeKind::Bool => "bool",
        SqlTypeKind::Bit => "bit",
        SqlTypeKind::VarBit => "varbit",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::Uuid => "uuid",
        SqlTypeKind::Inet => "inet",
        SqlTypeKind::Cidr => "cidr",
        SqlTypeKind::MacAddr => "macaddr",
        SqlTypeKind::MacAddr8 => "macaddr8",
        SqlTypeKind::InternalChar => "\"char\"",
        SqlTypeKind::Int8 => "int8",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Int2 => "int2",
        SqlTypeKind::Int2Vector => "int2vector",
        SqlTypeKind::Int4 => "int4",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::RegProc => "regproc",
        SqlTypeKind::RegClass => "regclass",
        SqlTypeKind::RegType => "regtype",
        SqlTypeKind::RegRole => "regrole",
        SqlTypeKind::RegNamespace => "regnamespace",
        SqlTypeKind::RegOper => "regoper",
        SqlTypeKind::RegOperator => "regoperator",
        SqlTypeKind::RegProcedure => "regprocedure",
        SqlTypeKind::RegCollation => "regcollation",
        SqlTypeKind::Tid => "tid",
        SqlTypeKind::Xid => "xid",
        SqlTypeKind::OidVector => "oidvector",
        SqlTypeKind::Float4 => "float4",
        SqlTypeKind::Float8 => "float8",
        SqlTypeKind::Money => "money",
        SqlTypeKind::Varchar => "varchar",
        SqlTypeKind::Char => "bpchar",
        SqlTypeKind::Date => "date",
        SqlTypeKind::Time => "time",
        SqlTypeKind::TimeTz => "timetz",
        SqlTypeKind::Interval => "interval",
        SqlTypeKind::Timestamp => "timestamp",
        SqlTypeKind::TimestampTz => "timestamptz",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
        SqlTypeKind::Xml => "xml",
        SqlTypeKind::Point => "point",
        SqlTypeKind::Lseg => "lseg",
        SqlTypeKind::Path => "path",
        SqlTypeKind::Line => "line",
        SqlTypeKind::Box => "box",
        SqlTypeKind::Polygon => "polygon",
        SqlTypeKind::Circle => "circle",
        SqlTypeKind::TsVector => "tsvector",
        SqlTypeKind::TsQuery => "tsquery",
        SqlTypeKind::PgLsn => "pg_lsn",
        SqlTypeKind::RegConfig => "regconfig",
        SqlTypeKind::RegDictionary => "regdictionary",
        SqlTypeKind::PgNodeTree => "pg_node_tree",
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
        SqlTypeKind::Multirange => unreachable!("multirange handled above"),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;

    #[test]
    fn create_table_accepts_datetime_types_with_bootstrap_rows() {
        let mut catalog = Catalog::default();
        let desc = RelationDesc {
            columns: vec![
                column_desc("d", SqlType::new(SqlTypeKind::Date), true),
                column_desc("t", SqlType::new(SqlTypeKind::Time), true),
                column_desc("tz", SqlType::new(SqlTypeKind::TimeTz), true),
                column_desc("ts", SqlType::new(SqlTypeKind::Timestamp), true),
                column_desc("tstz", SqlType::new(SqlTypeKind::TimestampTz), true),
            ],
        };
        let entry = catalog
            .create_table("dt_test", desc)
            .expect("datetime create table");
        assert_eq!(entry.desc.columns.len(), 5);
    }
}
