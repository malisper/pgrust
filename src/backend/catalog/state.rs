use std::collections::BTreeMap;

use crate::backend::catalog::bootstrap::{bootstrap_catalog_entry, bootstrap_catalog_kinds};
use crate::backend::catalog::catalog::allocate_relation_object_oids;
use crate::backend::catalog::pg_constraint::{derived_pg_constraint_rows, sort_pg_constraint_rows};
use crate::backend::catalog::pg_depend::{derived_pg_depend_rows, sort_pg_depend_rows};
use crate::backend::catalog::store::{DEFAULT_FIRST_REL_NUMBER, DEFAULT_FIRST_USER_OID};
use crate::backend::executor::RelationDesc;
use crate::backend::storage::smgr::RelFileLocator;
use crate::include::catalog::{PUBLIC_NAMESPACE_OID, PgConstraintRow, PgDependRow};

const DEFAULT_SPC_OID: u32 = 0;
const DEFAULT_DB_OID: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogIndexMeta {
    pub indrelid: u32,
    pub indkey: Vec<i16>,
    pub indisunique: bool,
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
pub struct CatalogEntry {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub namespace_oid: u32,
    pub row_type_oid: u32,
    pub relpersistence: char,
    pub relkind: char,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    pub(crate) tables: BTreeMap<String, CatalogEntry>,
    pub(crate) constraints: Vec<PgConstraintRow>,
    pub(crate) depends: Vec<PgDependRow>,
    pub(crate) next_rel_number: u32,
    pub(crate) next_oid: u32,
}

impl Default for Catalog {
    fn default() -> Self {
        let mut catalog = Self {
            tables: BTreeMap::new(),
            constraints: Vec::new(),
            depends: Vec::new(),
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
        mut desc: RelationDesc,
        namespace_oid: u32,
        db_oid: u32,
        relpersistence: char,
    ) -> Result<CatalogEntry, CatalogError> {
        let name = name.into().to_ascii_lowercase();
        if self.tables.contains_key(&name) {
            return Err(CatalogError::TableAlreadyExists(name));
        }

        let relation_oid = self.next_oid;
        let row_type_oid = relation_oid.saturating_add(1);
        let mut next_oid = row_type_oid.saturating_add(1);
        allocate_relation_object_oids(&mut desc, &mut next_oid);

        let entry = CatalogEntry {
            rel: RelFileLocator {
                spc_oid: DEFAULT_SPC_OID,
                db_oid,
                rel_number: self.next_rel_number,
            },
            relation_oid,
            namespace_oid,
            row_type_oid,
            relpersistence,
            relkind: 'r',
            desc,
            index_meta: None,
        };
        self.next_rel_number = self.next_rel_number.saturating_add(1);
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
        let index_name = index_name.into().to_ascii_lowercase();
        if self.tables.contains_key(&index_name) {
            return Err(CatalogError::TableAlreadyExists(index_name));
        }

        let table = self
            .get_by_oid(relation_oid)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(relation_oid.to_string()))?;
        if table.relkind != 'r' {
            return Err(CatalogError::UnknownTable(relation_oid.to_string()));
        }
        let type_rows = crate::include::catalog::builtin_type_rows();
        let mut indkey = Vec::with_capacity(columns.len());
        let mut index_columns = Vec::with_capacity(columns.len());
        let mut indclass = Vec::with_capacity(columns.len());
        let mut indcollation = Vec::with_capacity(columns.len());
        let mut indoption = Vec::with_capacity(columns.len());
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
            let column_sql_type = column.sql_type;
            index_columns.push(column);
            let type_oid = type_rows
                .iter()
                .find(|row| row.sql_type == column_sql_type)
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

        let entry = CatalogEntry {
            rel: RelFileLocator {
                spc_oid: DEFAULT_SPC_OID,
                db_oid: DEFAULT_DB_OID,
                rel_number: self.next_rel_number,
            },
            relation_oid: self.next_oid,
            namespace_oid: table.namespace_oid,
            row_type_oid: 0,
            relpersistence: table.relpersistence,
            relkind: 'i',
            desc: RelationDesc {
                columns: index_columns,
            },
            index_meta: Some(CatalogIndexMeta {
                indrelid: table.relation_oid,
                indkey,
                indisunique: unique,
                indisvalid: false,
                indisready: false,
                indislive: true,
                indclass,
                indcollation,
                indoption,
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

    pub fn drop_table(&mut self, name: &str) -> Result<CatalogEntry, CatalogError> {
        match self.tables.get(&name.to_ascii_lowercase()) {
            Some(entry) if entry.relkind == 'r' => {}
            _ => return Err(CatalogError::UnknownTable(name.to_string())),
        }
        let entry = self
            .tables
            .remove(&name.to_ascii_lowercase())
            .ok_or_else(|| CatalogError::UnknownTable(name.to_string()))?;
        self.constraints.retain(|row| row.conrelid != entry.relation_oid);
        self.depends
            .retain(|row| row.objid != entry.relation_oid && row.refobjid != entry.relation_oid);
        Ok(entry)
    }

    pub fn remove_by_oid(&mut self, relation_oid: u32) -> Option<(String, CatalogEntry)> {
        let name = self
            .tables
            .iter()
            .find_map(|(name, entry)| (entry.relation_oid == relation_oid).then(|| name.clone()))?;
        let entry = self.tables.remove(&name)?;
        self.constraints.retain(|row| row.conrelid != relation_oid);
        self.depends
            .retain(|row| row.objid != relation_oid && row.refobjid != relation_oid);
        Some((name, entry))
    }

    fn replace_constraint_rows_for_entry(&mut self, relation_name: &str, entry: &CatalogEntry) {
        self.constraints.retain(|row| row.conrelid != entry.relation_oid);
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
        self.depends
            .retain(|row| row.objid != entry.relation_oid && row.refobjid != entry.relation_oid);
        if entry.relation_oid < DEFAULT_FIRST_USER_OID {
            return;
        }
        self.depends.extend(derived_pg_depend_rows(entry));
        sort_pg_depend_rows(&mut self.depends);
    }
}
