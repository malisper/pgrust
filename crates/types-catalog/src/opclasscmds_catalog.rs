//! Catalog vocabulary for the operator-class / operator-family commands
//! (`commands/opclasscmds.c`): the genbki index OIDs, attribute numbers, and
//! deformed-row projections for `pg_opfamily`, `pg_opclass`, `pg_amop`, and
//! `pg_amproc`.
//!
//! The `FormData_*` structs are the caller-shaped deformed rows that
//! `opclasscmds` hands to the `catalog/indexing.c` insert seams (the
//! `FormData_pg_depend` precedent): the indexing owner assigns the row's OID
//! via `GetNewOidWithIndex`, forms the heap tuple against the catalog's
//! descriptor, and runs `CatalogTupleInsert`.

use types_core::primitive::{AttrNumber, Oid};

// ---------------------------------------------------------------------------
// pg_opfamily
// ---------------------------------------------------------------------------

/// `OpfamilyOidIndexId` — `pg_opfamily_oid_index` (`pg_opfamily_d.h`).
pub const OpfamilyOidIndexId: Oid = 2755;

pub const Anum_pg_opfamily_oid: AttrNumber = 1;
pub const Anum_pg_opfamily_opfmethod: AttrNumber = 2;
pub const Anum_pg_opfamily_opfname: AttrNumber = 3;
pub const Anum_pg_opfamily_opfnamespace: AttrNumber = 4;
pub const Anum_pg_opfamily_opfowner: AttrNumber = 5;
/// `Natts_pg_opfamily` (`pg_opfamily_d.h`).
pub const Natts_pg_opfamily: usize = 5;

/// Deformed `pg_opfamily` row (`opfoid` assigned by the indexing owner).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormData_pg_opfamily {
    pub opfmethod: Oid,
    /// `NameData` opfname.
    pub opfname: String,
    pub opfnamespace: Oid,
    pub opfowner: Oid,
}

// ---------------------------------------------------------------------------
// pg_opclass
// ---------------------------------------------------------------------------

/// `OperatorClassRelationId` — `pg_opclass` (`pg_opclass_d.h`).
pub const OperatorClassRelationId: Oid = 2616;
/// `OpclassOidIndexId` — `pg_opclass_oid_index` (`pg_opclass_d.h`).
pub const OpclassOidIndexId: Oid = 2687;
/// `OpclassAmNameNspIndexId` — `pg_opclass_am_name_nsp_index`
/// (`pg_opclass_d.h`).
pub const OpclassAmNameNspIndexId: Oid = 2686;

pub const Anum_pg_opclass_oid: AttrNumber = 1;
pub const Anum_pg_opclass_opcmethod: AttrNumber = 2;
pub const Anum_pg_opclass_opcname: AttrNumber = 3;
pub const Anum_pg_opclass_opcnamespace: AttrNumber = 4;
pub const Anum_pg_opclass_opcowner: AttrNumber = 5;
pub const Anum_pg_opclass_opcfamily: AttrNumber = 6;
pub const Anum_pg_opclass_opcintype: AttrNumber = 7;
pub const Anum_pg_opclass_opcdefault: AttrNumber = 8;
pub const Anum_pg_opclass_opckeytype: AttrNumber = 9;
/// `Natts_pg_opclass` (`pg_opclass_d.h`).
pub const Natts_pg_opclass: usize = 9;

/// Deformed `pg_opclass` row (`opcoid` assigned by the indexing owner). Also
/// the `Form_pg_opclass` fields the default-opclass scan reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormData_pg_opclass {
    pub opcmethod: Oid,
    /// `NameData` opcname.
    pub opcname: String,
    pub opcnamespace: Oid,
    pub opcowner: Oid,
    pub opcfamily: Oid,
    pub opcintype: Oid,
    pub opcdefault: bool,
    pub opckeytype: Oid,
}

// ---------------------------------------------------------------------------
// pg_amop
// ---------------------------------------------------------------------------

/// `AccessMethodOperatorOidIndexId` — `pg_amop_oid_index` (`pg_amop_d.h`).
pub const AccessMethodOperatorOidIndexId: Oid = 2756;

pub const Anum_pg_amop_oid: AttrNumber = 1;
pub const Anum_pg_amop_amopfamily: AttrNumber = 2;
pub const Anum_pg_amop_amoplefttype: AttrNumber = 3;
pub const Anum_pg_amop_amoprighttype: AttrNumber = 4;
pub const Anum_pg_amop_amopstrategy: AttrNumber = 5;
pub const Anum_pg_amop_amoppurpose: AttrNumber = 6;
pub const Anum_pg_amop_amopopr: AttrNumber = 7;
pub const Anum_pg_amop_amopmethod: AttrNumber = 8;
pub const Anum_pg_amop_amopsortfamily: AttrNumber = 9;
/// `Natts_pg_amop` (`pg_amop_d.h`).
pub const Natts_pg_amop: usize = 9;

/// Deformed `pg_amop` row (`oid` assigned by the indexing owner).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FormData_pg_amop {
    pub amopfamily: Oid,
    pub amoplefttype: Oid,
    pub amoprighttype: Oid,
    pub amopstrategy: i16,
    /// `AMOP_SEARCH` (`'s'`) / `AMOP_ORDER` (`'o'`).
    pub amoppurpose: i8,
    pub amopopr: Oid,
    pub amopmethod: Oid,
    pub amopsortfamily: Oid,
}

// ---------------------------------------------------------------------------
// pg_amproc
// ---------------------------------------------------------------------------

/// `AccessMethodProcedureRelationId` — `pg_amproc` (`pg_amproc_d.h`).
pub const AccessMethodProcedureRelationId: Oid = 2603;
/// `AccessMethodProcedureIndexId` — `pg_amproc_fam_proc_index`
/// (`pg_amproc_d.h`).
pub const AccessMethodProcedureIndexId: Oid = 2655;
/// `AccessMethodProcedureOidIndexId` — `pg_amproc_oid_index`
/// (`pg_amproc_d.h`).
pub const AccessMethodProcedureOidIndexId: Oid = 2757;

pub const Anum_pg_amproc_oid: AttrNumber = 1;
pub const Anum_pg_amproc_amprocfamily: AttrNumber = 2;
pub const Anum_pg_amproc_amproclefttype: AttrNumber = 3;
pub const Anum_pg_amproc_amprocrighttype: AttrNumber = 4;
pub const Anum_pg_amproc_amprocnum: AttrNumber = 5;
pub const Anum_pg_amproc_amproc: AttrNumber = 6;
/// `Natts_pg_amproc` (`pg_amproc_d.h`).
pub const Natts_pg_amproc: usize = 6;

/// Deformed `pg_amproc` row (`oid` assigned by the indexing owner).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FormData_pg_amproc {
    pub amprocfamily: Oid,
    pub amproclefttype: Oid,
    pub amprocrighttype: Oid,
    pub amprocnum: i16,
    pub amproc: Oid,
}
