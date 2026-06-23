//! Publication vocabulary (`catalog/pg_publication.h`), trimmed to the items
//! the logical-replication protocol consumes.

/// `PublishGencolsType` (`catalog/pg_publication.h`): how generated columns
/// are handled in a publication when there is no column list. The values are
/// the catalog characters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PublishGencolsType {
    /// `PUBLISH_GENCOLS_NONE = 'n'` — generated columns are published only
    /// when present in a publication column list.
    None = b'n',
    /// `PUBLISH_GENCOLS_STORED = 's'` — stored generated columns are
    /// published even when there is no column list.
    Stored = b's',
}

impl PublishGencolsType {
    /// Decode the `pubgencols` catalog character (`'n'`/`'s'`) into the enum.
    /// `GetPublication` reads `pubform->pubgencols` directly into
    /// `pub->pubgencols_type` (`pg_publication.c`); any other value would be a
    /// corrupt catalog.
    pub fn from_char(c: i8) -> Self {
        match c as u8 {
            b's' => PublishGencolsType::Stored,
            _ => PublishGencolsType::None,
        }
    }

    /// The catalog character (`'n'`/`'s'`).
    pub fn as_char(self) -> i8 {
        self as u8 as i8
    }
}

use mcx::{PgString, PgVec};
use ::types_core::primitive::Oid;

/* ==========================================================================
 * Relation / index OIDs (`pg_publication.h`, `pg_publication_rel.h`,
 * `pg_publication_namespace.h` CATALOG / DECLARE_*_INDEX lines).
 * ========================================================================== */

/// `PublicationRelationId` — `pg_publication`.
pub const PublicationRelationId: Oid = 6104;
/// `PublicationRelRelationId` — `pg_publication_rel`.
pub const PublicationRelRelationId: Oid = 6106;
/// `PublicationNamespaceRelationId` — `pg_publication_namespace`.
pub const PublicationNamespaceRelationId: Oid = 6237;

/// `PublicationObjectIndexId` — `pg_publication_oid_index` (6110).
pub const PublicationObjectIndexId: Oid = 6110;
/// `PublicationNameIndexId` — `pg_publication_pubname_index` (6111).
pub const PublicationNameIndexId: Oid = 6111;

/// `PublicationRelObjectIndexId` — `pg_publication_rel_oid_index` (6112).
pub const PublicationRelObjectIndexId: Oid = 6112;
/// `PublicationRelPrrelidPrpubidIndexId` —
/// `pg_publication_rel_prrelid_prpubid_index` (6113).
pub const PublicationRelPrrelidPrpubidIndexId: Oid = 6113;
/// `PublicationRelPrpubidIndexId` — `pg_publication_rel_prpubid_index` (6116).
pub const PublicationRelPrpubidIndexId: Oid = 6116;

/// `PublicationNamespaceObjectIndexId` —
/// `pg_publication_namespace_oid_index` (6238).
pub const PublicationNamespaceObjectIndexId: Oid = 6238;
/// `PublicationNamespacePnnspidPnpubidIndexId` —
/// `pg_publication_namespace_pnnspid_pnpubid_index` (6239).
pub const PublicationNamespacePnnspidPnpubidIndexId: Oid = 6239;

/* ==========================================================================
 * `pg_publication` attribute numbers (`FormData_pg_publication`, in order).
 * ========================================================================== */

/// `Anum_pg_publication_oid` = 1.
pub const Anum_pg_publication_oid: i32 = 1;
/// `Anum_pg_publication_pubname` = 2.
pub const Anum_pg_publication_pubname: i32 = 2;
/// `Anum_pg_publication_pubowner` = 3.
pub const Anum_pg_publication_pubowner: i32 = 3;
/// `Anum_pg_publication_puballtables` = 4.
pub const Anum_pg_publication_puballtables: i32 = 4;
/// `Anum_pg_publication_pubinsert` = 5.
pub const Anum_pg_publication_pubinsert: i32 = 5;
/// `Anum_pg_publication_pubupdate` = 6.
pub const Anum_pg_publication_pubupdate: i32 = 6;
/// `Anum_pg_publication_pubdelete` = 7.
pub const Anum_pg_publication_pubdelete: i32 = 7;
/// `Anum_pg_publication_pubtruncate` = 8.
pub const Anum_pg_publication_pubtruncate: i32 = 8;
/// `Anum_pg_publication_pubviaroot` = 9.
pub const Anum_pg_publication_pubviaroot: i32 = 9;
/// `Anum_pg_publication_pubgencols` = 10.
pub const Anum_pg_publication_pubgencols: i32 = 10;
/// `Natts_pg_publication` = 10.
pub const Natts_pg_publication: usize = 10;

/* ==========================================================================
 * `pg_publication_rel` attribute numbers (`FormData_pg_publication_rel`).
 * ========================================================================== */

/// `Anum_pg_publication_rel_oid` = 1.
pub const Anum_pg_publication_rel_oid: i32 = 1;
/// `Anum_pg_publication_rel_prpubid` = 2.
pub const Anum_pg_publication_rel_prpubid: i32 = 2;
/// `Anum_pg_publication_rel_prrelid` = 3.
pub const Anum_pg_publication_rel_prrelid: i32 = 3;
/// `Anum_pg_publication_rel_prqual` = 4 (`pg_node_tree`, varlen).
pub const Anum_pg_publication_rel_prqual: i32 = 4;
/// `Anum_pg_publication_rel_prattrs` = 5 (`int2vector`, varlen).
pub const Anum_pg_publication_rel_prattrs: i32 = 5;
/// `Natts_pg_publication_rel` = 5.
pub const Natts_pg_publication_rel: usize = 5;

/* ==========================================================================
 * `pg_publication_namespace` attribute numbers
 * (`FormData_pg_publication_namespace`).
 * ========================================================================== */

/// `Anum_pg_publication_namespace_oid` = 1.
pub const Anum_pg_publication_namespace_oid: i32 = 1;
/// `Anum_pg_publication_namespace_pnpubid` = 2.
pub const Anum_pg_publication_namespace_pnpubid: i32 = 2;
/// `Anum_pg_publication_namespace_pnnspid` = 3.
pub const Anum_pg_publication_namespace_pnnspid: i32 = 3;
/// `Natts_pg_publication_namespace` = 3.
pub const Natts_pg_publication_namespace: usize = 3;

/* ==========================================================================
 * `PublicationPartOpt` (`pg_publication.h`): which partitions of partitioned
 * tables a caller expects to see.
 * ========================================================================== */

/// `enum PublicationPartOpt`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationPartOpt {
    /// `PUBLICATION_PART_ROOT` — only the table explicitly mentioned.
    Root,
    /// `PUBLICATION_PART_LEAF` — only leaf partitions in the tree.
    Leaf,
    /// `PUBLICATION_PART_ALL` — all partitions in the tree.
    All,
}

/* ==========================================================================
 * `PublicationActions` / `Publication` (`pg_publication.h`).
 * ========================================================================== */

/// `PublicationActions` (`pg_publication.h`): the per-DML publish flags.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PublicationActions {
    pub pubinsert: bool,
    pub pubupdate: bool,
    pub pubdelete: bool,
    pub pubtruncate: bool,
}

/// `PublicationDesc` (`pg_publication.h`): the per-relation summary the
/// relcache caches in `rd_pubdesc` (built by `RelationBuildPublicationDesc`),
/// recording the relation's combined publish actions plus whether its row
/// filters, column lists, and generated columns are valid for UPDATE/DELETE
/// (i.e. fully covered by the replica identity).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PublicationDesc {
    /// `pubactions` — the OR of all publications' publish actions for this rel.
    pub pubactions: PublicationActions,
    /// true if the columns referenced in row filters used for UPDATE are part
    /// of the replica identity (or UPDATE is not published).
    pub rf_valid_for_update: bool,
    /// row-filter validity for DELETE.
    pub rf_valid_for_delete: bool,
    /// true if the column list covers the replica identity for UPDATE (or
    /// UPDATE is not published).
    pub cols_valid_for_update: bool,
    /// column-list validity for DELETE.
    pub cols_valid_for_delete: bool,
    /// true if all generated columns that are part of the replica identity are
    /// published for UPDATE (or UPDATE is not published).
    pub gencols_valid_for_update: bool,
    /// generated-column validity for DELETE.
    pub gencols_valid_for_delete: bool,
}

/// `Publication` (`pg_publication.h`): the decoded, palloc'd publication that
/// `GetPublication` returns. The `name` is owned in `'mcx`.
#[derive(Debug)]
pub struct Publication<'mcx> {
    pub oid: Oid,
    pub name: PgString<'mcx>,
    pub alltables: bool,
    pub pubviaroot: bool,
    pub pubgencols_type: PublishGencolsType,
    pub pubactions: PublicationActions,
}

/// `published_rel` (`pg_publication.c`, file-local): the (relid, pubid) pair the
/// SRF accumulates. Public here so the SRF helper carrier can name it.
#[derive(Clone, Copy, Debug)]
pub struct PublishedRel {
    pub relid: Oid,
    pub pubid: Oid,
}

/// One result row of `pg_get_publication_tables` (the SRF):
/// `(pubid, relid, attrs int2vector, qual pg_node_tree)`. The two varlen
/// columns travel as their raw varlena byte image (or `None` for SQL NULL).
#[derive(Clone, Debug)]
pub struct PublicationTableRow<'mcx> {
    pub pubid: Oid,
    pub relid: Oid,
    /// `attrs` — the `int2vector` varlena bytes, or `None` (SQL NULL).
    pub attrs: Option<PgVec<'mcx, u8>>,
    /// `qual` — the `pg_node_tree` (text) varlena bytes, or `None` (SQL NULL).
    pub qual: Option<PgVec<'mcx, u8>>,
}

/// A projected `pg_class` row, as the publication full-catalog scans read it
/// (`is_publishable_class` needs `oid`, `relkind`, `relpersistence`,
/// `relispartition`, `relnamespace`).
#[derive(Clone, Copy, Debug)]
pub struct PgClassRow {
    pub oid: Oid,
    pub relkind: u8,
    pub relpersistence: u8,
    pub relispartition: bool,
    pub relnamespace: Oid,
}
