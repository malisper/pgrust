use std::collections::BTreeMap;

use crate::backend::utils::cache::catcache::CatCache;
use crate::include::catalog::{PG_CLASS_RELATION_OID, PgDependRow, PgInheritsRow};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ObjectAddress {
    pub classid: u32,
    pub objid: u32,
    pub objsubid: i32,
}

impl ObjectAddress {
    pub(super) fn new(classid: u32, objid: u32, objsubid: i32) -> Self {
        Self {
            classid,
            objid,
            objsubid,
        }
    }

    pub(super) fn relation(relation_oid: u32) -> Self {
        Self::new(PG_CLASS_RELATION_OID, relation_oid, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DropBehavior {
    Restrict,
    Cascade,
}

impl DropBehavior {
    pub(super) fn from_cascade(cascade: bool) -> Self {
        if cascade {
            Self::Cascade
        } else {
            Self::Restrict
        }
    }

    pub(super) fn is_cascade(self) -> bool {
        matches!(self, Self::Cascade)
    }
}

#[derive(Debug)]
pub(super) struct CatalogDependencyGraph {
    dependents_by_ref: BTreeMap<ObjectAddress, Vec<PgDependRow>>,
    inherits_by_parent: BTreeMap<u32, Vec<PgInheritsRow>>,
}

impl CatalogDependencyGraph {
    pub(super) fn new(catcache: &CatCache) -> Self {
        let mut dependents_by_ref: BTreeMap<ObjectAddress, Vec<PgDependRow>> = BTreeMap::new();
        for row in catcache.depend_rows() {
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
        for row in catcache.inherit_rows() {
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

    pub(super) fn dependents(&self, referenced: ObjectAddress) -> &[PgDependRow] {
        self.dependents_by_ref
            .get(&referenced)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(super) fn inheritance_children(&self, parent_oid: u32) -> &[PgInheritsRow] {
        self.inherits_by_parent
            .get(&parent_oid)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}
