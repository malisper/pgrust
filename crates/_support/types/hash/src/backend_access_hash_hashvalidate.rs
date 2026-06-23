//! Signature value-types for the `backend/access/hash/hashvalidate.c` unit's
//! seams.
//!
//! Plain owned mirrors of the catalog `Form_pg_opclass` / `Form_pg_amproc` /
//! `Form_pg_amop` rows and the `amapi.h` `OpFamilyMember` dependency-adjustment
//! record the hash opclass validator (`hashvalidate`/`hashadjustmembers`)
//! consults. These are validator-local shapes (they differ from the same-named
//! records of the other AM validators and the parser-side `OpFamilyMember`),
//! so they live under this module's own C-path. The cross-validator
//! `OpFamilyOpFuncGroup` is types-amvalidate's shared definition.

use ::mcx::PgString;
use ::types_core::Oid;

/// `Form_pg_opclass` fields read by `hashvalidate` (the result of
/// `SearchSysCache1(CLAOID, opclassoid)` projected to what the validator uses).
/// The name is context-allocated, so the form carries the allocator lifetime.
#[derive(Debug, PartialEq, Eq)]
pub struct OpclassForm<'mcx> {
    /// `opcfamily` — the opclass's opfamily OID.
    pub opcfamily: Oid,
    /// `opcintype` — the opclass's input data type OID.
    pub opcintype: Oid,
    /// `opckeytype` — the type actually stored in the index, or `InvalidOid`
    /// (0) when it equals `opcintype`. `hashvalidate` does not consult this, but
    /// `ginvalidate` does (the GIN `compare`/`comparePartial` signature checks
    /// run against `opckeytype`, falling back to `opcintype` when unset).
    pub opckeytype: Oid,
    /// `NameStr(opcname)` — the opclass name (for the missing-operators message).
    pub opcname: PgString<'mcx>,
}

/// One `Form_pg_amproc` member row (a member of the `AMPROCNUM` cat-list for the
/// opfamily).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AmprocRow {
    /// `amproclefttype`.
    pub amproclefttype: Oid,
    /// `amprocrighttype`.
    pub amprocrighttype: Oid,
    /// `amprocnum`.
    pub amprocnum: i16,
    /// `amproc` (the support function's OID / `RegProcedure`).
    pub amproc: Oid,
}

/// One `Form_pg_amop` member row (a member of the `AMOPSTRATEGY` cat-list for the
/// opfamily).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AmopRow {
    /// `amopstrategy`.
    pub amopstrategy: i16,
    /// `amoppurpose` (`'s'` search / `'o'` order-by).
    pub amoppurpose: i8,
    /// `amopopr` (the operator's OID).
    pub amopopr: Oid,
    /// `amopsortfamily`.
    pub amopsortfamily: Oid,
    /// `amoplefttype`.
    pub amoplefttype: Oid,
    /// `amoprighttype`.
    pub amoprighttype: Oid,
}

// `OpFamilyOpFuncGroup` (amvalidate.h) is the canonical shared definition in
// types-amvalidate, re-exported here for the hashvalidate seam signatures.
pub use types_amvalidate::index_amvalidate::OpFamilyOpFuncGroup;

/// `OpFamilyMember` (amapi.h) — the dependency-adjustment record mutated by
/// `hashadjustmembers`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpFamilyMember {
    /// `is_func` — true for a support function, false for an operator.
    pub is_func: bool,
    /// `number` — the support function number (for `functions`) or the strategy
    /// number (for `operators`).
    pub number: i16,
    /// `lefttype`.
    pub lefttype: Oid,
    /// `righttype`.
    pub righttype: Oid,
    /// `ref_is_hard`.
    pub ref_is_hard: bool,
    /// `ref_is_family`.
    pub ref_is_family: bool,
    /// `refobjid`.
    pub refobjid: Oid,
}
