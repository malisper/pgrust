//! ABI types for the publication catalogs (`pg_publication`,
//! `pg_publication_rel`, `pg_publication_namespace`) and the in-memory
//! `Publication` / `PublicationRelInfo` runtime structs.
//!
//! These mirror, with identical `repr(C)` layout and field order, the C
//! definitions in:
//! - `catalog/pg_publication.h` — `FormData_pg_publication`, `Publication`,
//!   `PublicationActions`, `PublicationRelInfo`, `PublicationPartOpt`,
//!   `PublishGencolsType`
//! - `catalog/pg_publication_rel.h` — `FormData_pg_publication_rel`
//! - `catalog/pg_publication_namespace.h` — `FormData_pg_publication_namespace`
//!
//! The index/relation OIDs and `Anum_*`/`Natts_*` column numbers come from the
//! generated `pg_publication{,_rel,_namespace}_d.h` headers.
//!
//! This module is referenced by path (`pg_ffi_fgram::publication::*`) and is
//! deliberately NOT in the crate-root glob, to avoid ambiguous-glob collisions
//! with the widely-named `List`/`Node`/`Relation`/`Bitmapset` types it uses.

use core::ffi::c_char;

use crate::types::{AttrNumber, Oid};

/* ---------------------------------------------------------------------------
 * Relation OIDs (pg_publication{,_rel,_namespace}_d.h).
 *
 * NB: PublicationRelationId / PublicationRelRelationId /
 * PublicationNamespaceRelationId already exist in `catalog.rs`; re-export them
 * here under their C names for convenience.
 * ------------------------------------------------------------------------- */

pub use crate::catalog::{
    PublicationRelRelationId, PUBLICATION_NAMESPACE_RELATION_ID as PublicationNamespaceRelationId,
    PUBLICATION_RELATION_ID as PublicationRelationId,
};

/* ---------------------------------------------------------------------------
 * Index OIDs (pg_publication{,_rel,_namespace}_d.h).
 * ------------------------------------------------------------------------- */

/// `PublicationObjectIndexId` — `pg_publication_oid_index` (pg_publication_d.h).
pub const PublicationObjectIndexId: Oid = 6110;
/// `PublicationNameIndexId` — `pg_publication_pubname_index`.
pub const PublicationNameIndexId: Oid = 6111;
/// `PublicationRelObjectIndexId` — `pg_publication_rel_oid_index`.
pub const PublicationRelObjectIndexId: Oid = 6112;
/// `PublicationRelPrrelidPrpubidIndexId` — unique (prrelid, prpubid) index.
pub const PublicationRelPrrelidPrpubidIndexId: Oid = 6113;
/// `PublicationRelPrpubidIndexId` — index on prpubid.
pub const PublicationRelPrpubidIndexId: Oid = 6116;
/// `PublicationNamespaceObjectIndexId` — `pg_publication_namespace_oid_index`.
pub const PublicationNamespaceObjectIndexId: Oid = 6238;
/// `PublicationNamespacePnnspidPnpubidIndexId` — unique (pnnspid, pnpubid) index.
pub const PublicationNamespacePnnspidPnpubidIndexId: Oid = 6239;

/* ---------------------------------------------------------------------------
 * pg_publication column numbers (pg_publication_d.h).
 * ------------------------------------------------------------------------- */

pub const Anum_pg_publication_oid: AttrNumber = 1;
pub const Anum_pg_publication_pubname: AttrNumber = 2;
pub const Anum_pg_publication_pubowner: AttrNumber = 3;
pub const Anum_pg_publication_puballtables: AttrNumber = 4;
pub const Anum_pg_publication_pubinsert: AttrNumber = 5;
pub const Anum_pg_publication_pubupdate: AttrNumber = 6;
pub const Anum_pg_publication_pubdelete: AttrNumber = 7;
pub const Anum_pg_publication_pubtruncate: AttrNumber = 8;
pub const Anum_pg_publication_pubviaroot: AttrNumber = 9;
pub const Anum_pg_publication_pubgencols: AttrNumber = 10;
pub const Natts_pg_publication: usize = 10;

/* pg_publication_rel column numbers (pg_publication_rel_d.h). */
pub const Anum_pg_publication_rel_oid: AttrNumber = 1;
pub const Anum_pg_publication_rel_prpubid: AttrNumber = 2;
pub const Anum_pg_publication_rel_prrelid: AttrNumber = 3;
pub const Anum_pg_publication_rel_prqual: AttrNumber = 4;
pub const Anum_pg_publication_rel_prattrs: AttrNumber = 5;
pub const Natts_pg_publication_rel: usize = 5;

/* pg_publication_namespace column numbers (pg_publication_namespace_d.h). */
pub const Anum_pg_publication_namespace_oid: AttrNumber = 1;
pub const Anum_pg_publication_namespace_pnpubid: AttrNumber = 2;
pub const Anum_pg_publication_namespace_pnnspid: AttrNumber = 3;
pub const Natts_pg_publication_namespace: usize = 3;

/* ---------------------------------------------------------------------------
 * FormData_pg_publication_rel / FormData_pg_publication_namespace.
 *
 * Only the fixed-length (non-CATALOG_VARLEN) columns are part of the C struct;
 * `prqual` (pg_node_tree) and `prattrs` (int2vector) live past the struct and
 * are read via SysCacheGetAttr, never as struct fields.
 * ------------------------------------------------------------------------- */

/// `FormData_pg_publication_rel` — fixed-length prefix of a pg_publication_rel
/// row (`catalog/pg_publication_rel.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FormData_pg_publication_rel {
    /// `oid` — row OID.
    pub oid: Oid,
    /// `prpubid` — OID of the publication.
    pub prpubid: Oid,
    /// `prrelid` — OID of the relation.
    pub prrelid: Oid,
}

pub type Form_pg_publication_rel = *mut FormData_pg_publication_rel;

/// `FormData_pg_publication_namespace` — a pg_publication_namespace row
/// (`catalog/pg_publication_namespace.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FormData_pg_publication_namespace {
    /// `oid` — row OID.
    pub oid: Oid,
    /// `pnpubid` — OID of the publication.
    pub pnpubid: Oid,
    /// `pnnspid` — OID of the schema.
    pub pnnspid: Oid,
}

pub type Form_pg_publication_namespace = *mut FormData_pg_publication_namespace;

/* ---------------------------------------------------------------------------
 * PublishGencolsType / PublicationActions / Publication / PublicationRelInfo /
 * PublicationPartOpt (pg_publication.h).
 * ------------------------------------------------------------------------- */

/// `PublishGencolsType` — whether generated columns are replicated
/// (`pg_publication.h`).  Stored in pg_publication.pubgencols as a `char`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishGencolsType(pub c_char);

/// `PUBLISH_GENCOLS_NONE = 'n'` — generated columns should not be replicated.
pub const PUBLISH_GENCOLS_NONE: PublishGencolsType = PublishGencolsType(b'n' as c_char);
/// `PUBLISH_GENCOLS_STORED = 's'` — stored generated columns should be replicated.
pub const PUBLISH_GENCOLS_STORED: PublishGencolsType = PublishGencolsType(b's' as c_char);

/// `typedef struct PublicationActions` (`pg_publication.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub struct PublicationActions {
    pub pubinsert: bool,
    pub pubupdate: bool,
    pub pubdelete: bool,
    pub pubtruncate: bool,
}

/// `typedef struct Publication` (`pg_publication.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Publication {
    pub oid: Oid,
    /// `char *name` — the palloc'd publication name.
    pub name: *mut c_char,
    pub alltables: bool,
    pub pubviaroot: bool,
    pub pubgencols_type: PublishGencolsType,
    pub pubactions: PublicationActions,
}

/// `PublicationPartOpt` — which partitions of partitioned tables to expand
/// (`pg_publication.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicationPartOpt(pub i32);

pub const PUBLICATION_PART_ROOT: PublicationPartOpt = PublicationPartOpt(0);
pub const PUBLICATION_PART_LEAF: PublicationPartOpt = PublicationPartOpt(1);
pub const PUBLICATION_PART_ALL: PublicationPartOpt = PublicationPartOpt(2);

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn publication_form_layout() {
        // FormData_pg_publication_rel: three Oids, tightly packed.
        assert_eq!(size_of::<FormData_pg_publication_rel>(), 12);
        assert_eq!(align_of::<FormData_pg_publication_rel>(), 4);
        assert_eq!(offset_of!(FormData_pg_publication_rel, oid), 0);
        assert_eq!(offset_of!(FormData_pg_publication_rel, prpubid), 4);
        assert_eq!(offset_of!(FormData_pg_publication_rel, prrelid), 8);

        assert_eq!(size_of::<FormData_pg_publication_namespace>(), 12);
        assert_eq!(offset_of!(FormData_pg_publication_namespace, oid), 0);
        assert_eq!(offset_of!(FormData_pg_publication_namespace, pnpubid), 4);
        assert_eq!(offset_of!(FormData_pg_publication_namespace, pnnspid), 8);
    }

    #[test]
    fn gencols_values() {
        assert_eq!(PUBLISH_GENCOLS_NONE.0, b'n' as c_char);
        assert_eq!(PUBLISH_GENCOLS_STORED.0, b's' as c_char);
    }

    #[test]
    fn partopt_values() {
        assert_eq!(PUBLICATION_PART_ROOT.0, 0);
        assert_eq!(PUBLICATION_PART_LEAF.0, 1);
        assert_eq!(PUBLICATION_PART_ALL.0, 2);
    }
}
