use std::collections::{BTreeMap, BTreeSet};

use crate::backend::catalog::bootstrap::{bootstrap_catalog_entry, bootstrap_catalog_kinds};
use crate::backend::catalog::catalog::allocate_relation_object_oids;
use crate::backend::catalog::indexing::insert_bootstrap_system_indexes;
use crate::backend::catalog::pg_constraint::{derived_pg_constraint_rows, sort_pg_constraint_rows};
use crate::backend::catalog::pg_depend::{
    derived_pg_depend_rows, index_backed_constraint_depend_rows, sort_pg_depend_rows,
};
use crate::backend::catalog::store::{DEFAULT_FIRST_REL_NUMBER, DEFAULT_FIRST_USER_OID};
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::storage::smgr::RelFileLocator;
use crate::include::catalog::{
    CONSTRAINT_NOTNULL, PUBLIC_NAMESPACE_OID, PgConstraintRow, PgDependRow, PgRewriteRow,
    builtin_type_rows, sort_pg_rewrite_rows,
};

const DEFAULT_SPC_OID: u32 = 0;
const DEFAULT_DB_OID: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogIndexMeta {
    pub indrelid: u32,
    pub indkey: Vec<i16>,
    pub indisunique: bool,
    pub indisprimary: bool,
    pub indisvalid: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indclass: Vec<u32>,
    pub indcollation: Vec<u32>,
    pub indoption: Vec<i16>,
    pub indexprs: Option<String>,
    pub indpred: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogIndexBuildOptions {
    pub am_oid: u32,
    pub indclass: Vec<u32>,
    pub indcollation: Vec<u32>,
    pub indoption: Vec<i16>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogEntry {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub namespace_oid: u32,
    pub row_type_oid: u32,
    pub reltoastrelid: u32,
    pub relpersistence: char,
    pub relkind: char,
    pub relpages: i32,
    pub reltuples: f64,
    pub desc: RelationDesc,
    pub index_meta: Option<CatalogIndexMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    Io(String),
    Corrupt(&'static str),
    TableAlreadyExists(String),
    UnknownTable(String),
    UnknownColumn(String),
    UnknownType(String),
    UniqueViolation(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    pub(crate) tables: BTreeMap<String, CatalogEntry>,
    pub(crate) constraints: Vec<PgConstraintRow>,
    pub(crate) depends: Vec<PgDependRow>,
    pub(crate) rewrites: Vec<PgRewriteRow>,
    pub(crate) next_rel_number: u32,
    pub(crate) next_oid: u32,
}

impl Default for Catalog {
    fn default() -> Self {
        let mut catalog = Self {
            tables: BTreeMap::new(),
            constraints: Vec::new(),
            depends: Vec::new(),
            rewrites: Vec::new(),
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

    pub fn next_oid(&self) -> u32 {
        self.next_oid
    }

    pub fn create_table(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
    ) -> Result<CatalogEntry, CatalogError> {
        self.create_table_with_options(name, desc, PUBLIC_NAMESPACE_OID, DEFAULT_DB_OID, 'p')
    }

    pub fn create_table_with_options(
        &mut self,
        name: impl Into<String>,
        desc: RelationDesc,
        namespace_oid: u32,
        db_oid: u32,
        relpersistence: char,
    ) -> Result<CatalogEntry, CatalogError> {
        self.create_table_with_relkind(name, desc, namespace_oid, db_oid, relpersistence, 'r')
    }

    pub(crate) fn create_table_with_relkind(
        &mut self,
        name: impl Into<String>,
        mut desc: RelationDesc,
        namespace_oid: u32,
        db_oid: u32,
        relpersistence: char,
        relkind: char,
    ) -> Result<CatalogEntry, CatalogError> {
        let name = name.into().to_ascii_lowercase();
        if self.tables.contains_key(&name) {
            return Err(CatalogError::TableAlreadyExists(name));
        }
        validate_builtin_type_rows(&desc)?;

        let relation_oid = self.next_oid;
        let row_type_oid = relation_oid.saturating_add(1);
        let mut next_oid = row_type_oid.saturating_add(1);
        if relkind == 'r' {
            allocate_relation_object_oids(&mut desc, &mut next_oid);
        }
        let rel_number = if relkind == 'v' {
            0
        } else {
            self.next_rel_number
        };

        let entry = CatalogEntry {
            rel: RelFileLocator {
                spc_oid: DEFAULT_SPC_OID,
                db_oid,
                rel_number,
            },
            relation_oid,
            namespace_oid,
            row_type_oid,
            reltoastrelid: 0,
            relpersistence,
            relkind,
            relpages: 0,
            reltuples: 0.0,
            desc,
            index_meta: None,
        };
        if relkind != 'v' {
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
        let mut indkey = Vec::with_capacity(columns.len());
        let mut index_columns = Vec::with_capacity(columns.len());
        for column_name in columns {
            let (attnum, column) = table
                .desc
                .columns
                .iter()
                .enumerate()
                .find(|(_, column)| column.name.eq_ignore_ascii_case(&column_name.name))
                .ok_or_else(|| CatalogError::UnknownColumn(column_name.name.clone()))?;
            indkey.push(attnum.saturating_add(1) as i16);
            let mut column = column.clone();
            column.not_null_constraint_oid = None;
            column.attrdef_oid = None;
            column.default_expr = None;
            index_columns.push(column);
        }
        if options.indclass.len() != columns.len()
            || options.indcollation.len() != columns.len()
            || options.indoption.len() != columns.len()
        {
            return Err(CatalogError::Corrupt("index build options length mismatch"));
        }

        let entry = CatalogEntry {
            rel: RelFileLocator {
                spc_oid: DEFAULT_SPC_OID,
                db_oid: DEFAULT_DB_OID,
                rel_number: self.next_rel_number,
            },
            relation_oid: self.next_oid,
            namespace_oid: table.namespace_oid,
            row_type_oid: 0,
            reltoastrelid: 0,
            relpersistence: table.relpersistence,
            relkind: 'i',
            relpages: 0,
            reltuples: 0.0,
            desc: RelationDesc {
                columns: index_columns,
            },
            index_meta: Some(CatalogIndexMeta {
                indrelid: table.relation_oid,
                indkey,
                indisunique: unique,
                indisprimary: primary,
                indisvalid: false,
                indisready: false,
                indislive: true,
                indclass: options.indclass.clone(),
                indcollation: options.indcollation.clone(),
                indoption: options.indoption.clone(),
                indexprs: None,
                indpred: None,
            }),
        };
        self.next_rel_number = self.next_rel_number.saturating_add(1);
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
    ) -> Result<PgConstraintRow, CatalogError> {
        let table = self
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let index = self
            .get_by_oid(index_oid)
            .ok_or_else(|| CatalogError::UnknownTable(index_oid.to_string()))?;
        if index.relkind != 'i' {
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
            connamespace: table.namespace_oid,
            contype,
            condeferrable: false,
            condeferred: false,
            conenforced: true,
            convalidated: true,
            conrelid: relation_oid,
            contypid: 0,
            conindid: index_oid,
            conparentid: 0,
            confrelid: 0,
            confupdtype: ' ',
            confdeltype: ' ',
            confmatchtype: ' ',
            conislocal: true,
            coninhcount: 0,
            connoinherit: false,
            conperiod: false,
        };
        self.next_oid = self.next_oid.saturating_add(1);
        self.constraints.push(row.clone());
        sort_pg_constraint_rows(&mut self.constraints);
        self.depends.extend(index_backed_constraint_depend_rows(
            row.oid,
            relation_oid,
            index_oid,
        ));
        sort_pg_depend_rows(&mut self.depends);
        Ok(row)
    }

    fn default_index_build_options(
        &self,
        relation_oid: u32,
        columns: &[crate::include::nodes::parsenodes::IndexColumnDef],
    ) -> Result<CatalogIndexBuildOptions, CatalogError> {
        let table = self
            .get_by_oid(relation_oid)
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        let type_rows = crate::include::catalog::builtin_type_rows();
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
            let type_oid = type_rows
                .iter()
                .find(|row| row.sql_type == column.sql_type)
                .map(|row| row.oid)
                .ok_or_else(|| CatalogError::UnknownType("index column type".into()))?;
            let opclass_oid = crate::include::catalog::default_btree_opclass_oid(type_oid)
                .ok_or_else(|| CatalogError::UnknownType("index column type".into()))?;
            indclass.push(opclass_oid);
            indcollation.push(0);
            let mut option = 0i16;
            if column_name.descending {
                option |= 0x0001;
            }
            if column_name.nulls_first.unwrap_or(false) {
                option |= 0x0002;
            }
            indoption.push(option);
        }
        Ok(CatalogIndexBuildOptions {
            am_oid: crate::include::catalog::BTREE_AM_OID,
            indclass,
            indcollation,
            indoption,
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
        if old_entry.relkind != 'r' {
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
        self.tables.insert(qualified_new_name.clone(), entry.clone());
        self.replace_constraint_rows_for_entry(&qualified_new_name, &entry);
        self.replace_depend_rows_for_entry(&entry);
        Ok((old_name, old_entry, qualified_new_name, entry))
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
        if old_entry.relkind != 'i' {
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

    fn replace_constraint_rows_for_entry(&mut self, relation_name: &str, entry: &CatalogEntry) {
        self.constraints.retain(|row| {
            !(row.conrelid == entry.relation_oid && row.contype == CONSTRAINT_NOTNULL)
        });
        if entry.relkind != 'r' {
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
        let entry_object_oids = entry_owned_object_oids(entry);
        self.depends
            .retain(|row| !entry_object_oids.contains(&row.objid));
        if entry.relation_oid < DEFAULT_FIRST_USER_OID {
            return;
        }
        self.depends.extend(derived_pg_depend_rows(entry));
        sort_pg_depend_rows(&mut self.depends);
    }
}

fn entry_owned_object_oids(entry: &CatalogEntry) -> BTreeSet<u32> {
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

fn validate_builtin_type_rows(desc: &RelationDesc) -> Result<(), CatalogError> {
    let builtin_rows = builtin_type_rows();
    for column in &desc.columns {
        let present = builtin_rows.iter().any(|row| {
            row.sql_type.kind == column.sql_type.kind
                && row.sql_type.is_array == column.sql_type.is_array
        });
        if !present {
            return Err(CatalogError::UnknownType(format!(
                "{} (missing builtin pg_type row)",
                format_sql_type_name(column.sql_type)
            )));
        }
    }
    Ok(())
}

fn format_sql_type_name(sql_type: SqlType) -> &'static str {
    if sql_type.is_array {
        return match sql_type.kind {
            SqlTypeKind::AnyArray => "anyarray",
            SqlTypeKind::Bool => "_bool",
            SqlTypeKind::Bit => "_bit",
            SqlTypeKind::VarBit => "_varbit",
            SqlTypeKind::Bytea => "_bytea",
            SqlTypeKind::InternalChar => "_char",
            SqlTypeKind::Int8 => "_int8",
            SqlTypeKind::Name => "_name",
            SqlTypeKind::Int2 => "_int2",
            SqlTypeKind::Int4 => "_int4",
            SqlTypeKind::Text => "_text",
            SqlTypeKind::Oid => "_oid",
            SqlTypeKind::Float4 => "_float4",
            SqlTypeKind::Float8 => "_float8",
            SqlTypeKind::Varchar => "_varchar",
            SqlTypeKind::Char => "_bpchar",
            SqlTypeKind::Date => "_date",
            SqlTypeKind::Time => "_time",
            SqlTypeKind::TimeTz => "_timetz",
            SqlTypeKind::Timestamp => "_timestamp",
            SqlTypeKind::TimestampTz => "_timestamptz",
            SqlTypeKind::Numeric => "_numeric",
            SqlTypeKind::Json => "_json",
            SqlTypeKind::Jsonb => "_jsonb",
            SqlTypeKind::JsonPath => "_jsonpath",
            SqlTypeKind::TsVector => "_tsvector",
            SqlTypeKind::TsQuery => "_tsquery",
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
        };
    }

    match sql_type.kind {
        SqlTypeKind::AnyArray => "anyarray",
        SqlTypeKind::Bool => "bool",
        SqlTypeKind::Bit => "bit",
        SqlTypeKind::VarBit => "varbit",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::InternalChar => "\"char\"",
        SqlTypeKind::Int8 => "int8",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Int2 => "int2",
        SqlTypeKind::Int2Vector => "int2vector",
        SqlTypeKind::Int4 => "int4",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::OidVector => "oidvector",
        SqlTypeKind::Float4 => "float4",
        SqlTypeKind::Float8 => "float8",
        SqlTypeKind::Varchar => "varchar",
        SqlTypeKind::Char => "bpchar",
        SqlTypeKind::Date => "date",
        SqlTypeKind::Time => "time",
        SqlTypeKind::TimeTz => "timetz",
        SqlTypeKind::Timestamp => "timestamp",
        SqlTypeKind::TimestampTz => "timestamptz",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
        SqlTypeKind::Point => "point",
        SqlTypeKind::Lseg => "lseg",
        SqlTypeKind::Path => "path",
        SqlTypeKind::Line => "line",
        SqlTypeKind::Box => "box",
        SqlTypeKind::Polygon => "polygon",
        SqlTypeKind::Circle => "circle",
        SqlTypeKind::TsVector => "tsvector",
        SqlTypeKind::TsQuery => "tsquery",
        SqlTypeKind::RegConfig => "regconfig",
        SqlTypeKind::RegDictionary => "regdictionary",
        SqlTypeKind::PgNodeTree => "pg_node_tree",
    }
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
