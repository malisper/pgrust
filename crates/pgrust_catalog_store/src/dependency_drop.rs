use std::collections::BTreeMap;

use pgrust_catalog_data::{PG_CLASS_RELATION_OID, PgDependRow, PgInheritsRow};

use crate::CatCache;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectAddress {
    pub classid: u32,
    pub objid: u32,
    pub objsubid: i32,
}

impl ObjectAddress {
    pub fn new(classid: u32, objid: u32, objsubid: i32) -> Self {
        Self {
            classid,
            objid,
            objsubid,
        }
    }

    pub fn relation(relation_oid: u32) -> Self {
        Self::new(PG_CLASS_RELATION_OID, relation_oid, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropBehavior {
    Restrict,
    Cascade,
}

impl DropBehavior {
    pub fn from_cascade(cascade: bool) -> Self {
        if cascade {
            Self::Cascade
        } else {
            Self::Restrict
        }
    }

    pub fn is_cascade(self) -> bool {
        matches!(self, Self::Cascade)
    }
}

#[derive(Debug)]
pub struct CatalogDependencyGraph {
    dependents_by_ref: BTreeMap<ObjectAddress, Vec<PgDependRow>>,
    inherits_by_parent: BTreeMap<u32, Vec<PgInheritsRow>>,
}

impl CatalogDependencyGraph {
    pub fn new(catcache: &CatCache) -> Self {
        Self::from_rows(catcache.depend_rows(), catcache.inherit_rows())
    }

    pub fn from_rows(depend_rows: Vec<PgDependRow>, inherit_rows: Vec<PgInheritsRow>) -> Self {
        let mut dependents_by_ref: BTreeMap<ObjectAddress, Vec<PgDependRow>> = BTreeMap::new();
        for row in depend_rows {
            dependents_by_ref
                .entry(ObjectAddress::new(
                    row.refclassid,
                    row.refobjid,
                    row.refobjsubid,
                ))
                .or_default()
                .push(row);
        }
        for rows in dependents_by_ref.values_mut() {
            rows.sort_by_key(|row| {
                (
                    row.classid,
                    row.objid,
                    row.objsubid,
                    row.deptype as u32,
                    row.refclassid,
                    row.refobjid,
                    row.refobjsubid,
                )
            });
        }

        let mut inherits_by_parent: BTreeMap<u32, Vec<PgInheritsRow>> = BTreeMap::new();
        for row in inherit_rows {
            inherits_by_parent
                .entry(row.inhparent)
                .or_default()
                .push(row);
        }
        for rows in inherits_by_parent.values_mut() {
            rows.sort_by_key(|row| (row.inhseqno, row.inhrelid));
        }

        Self {
            dependents_by_ref,
            inherits_by_parent,
        }
    }

    pub fn dependents(&self, referenced: ObjectAddress) -> &[PgDependRow] {
        self.dependents_by_ref
            .get(&referenced)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn inheritance_children(&self, parent_oid: u32) -> &[PgInheritsRow] {
        self.inherits_by_parent
            .get(&parent_oid)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}
