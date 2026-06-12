//! Signature value-types for the `backend/access/hash/hashvalidate.c` unit's
//! seams.
//!
//! Plain owned mirrors of the catalog `Form_pg_opclass` / `Form_pg_amproc` /
//! `Form_pg_amop` rows, the `amvalidate.h` `OpFamilyOpFuncGroup` group
//! descriptor, and the `amapi.h` `OpFamilyMember` dependency-adjustment record
//! the hash opclass validator (`hashvalidate`/`hashadjustmembers`) consults.
//! These are validator-local shapes (they differ from the same-named records of
//! the other AM validators and the parser-side `OpFamilyMember`), so they live
//! under this module's own C-path.

use types_core::Oid;

/// `Form_pg_opclass` fields read by `hashvalidate` (the result of
/// `SearchSysCache1(CLAOID, opclassoid)` projected to what the validator uses).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OpclassForm {
    /// `opcfamily` — the opclass's opfamily OID.
    pub opcfamily: Oid,
    /// `opcintype` — the opclass's input data type OID.
    pub opcintype: Oid,
    /// `NameStr(opcname)` — the opclass name (for the missing-operators message).
    pub opcname: alloc::string::String,
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

/// `OpFamilyOpFuncGroup` (amvalidate.h) — one datatype-pair group with its
/// operator and function presence bitmaps, as produced by
/// `identify_opfamily_groups`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpFamilyOpFuncGroup {
    /// `lefttype`.
    pub lefttype: Oid,
    /// `righttype`.
    pub righttype: Oid,
    /// `operatorset` — bitmask of present strategy operators.
    pub operatorset: u64,
    /// `functionset` — bitmask of present support functions.
    pub functionset: u64,
}

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
