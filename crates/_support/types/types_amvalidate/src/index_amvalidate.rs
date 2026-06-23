//! Row and group records consumed/produced by `identify_opfamily_groups`
//! (`access/index/amvalidate.c`).
//!
//! The C function reads the member tuples of an opfamily's `AMOPSTRATEGY` /
//! `AMPROCNUM` `CatCList`s via `GETSTRUCT`; here the fields it reads are owned
//! rows. Each AM validator projects its (possibly richer) catalog mirrors into
//! these before calling.

use types_core::Oid;

/// One `pg_amop` member row consumed by `identify_opfamily_groups`
/// (`pg_amop.h`): the datatype pair and the strategy number.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AmopRow {
    /// `amoplefttype`.
    pub amoplefttype: Oid,
    /// `amoprighttype`.
    pub amoprighttype: Oid,
    /// `amopstrategy`.
    pub amopstrategy: i16,
}

/// One `pg_amproc` member row consumed by `identify_opfamily_groups`
/// (`pg_amproc.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AmprocRow {
    /// `amproclefttype`.
    pub amproclefttype: Oid,
    /// `amprocrighttype`.
    pub amprocrighttype: Oid,
    /// `amprocnum`.
    pub amprocnum: i16,
}

/// `OpFamilyOpFuncGroup` (`access/amvalidate.h`) — one datatype-pair group with
/// its operator and support-function presence bitmaps. With `uint64` fields we
/// can handle operator and function numbers up to 63, which is plenty.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpFamilyOpFuncGroup {
    /// `lefttype` — `amoplefttype`/`amproclefttype`.
    pub lefttype: Oid,
    /// `righttype` — `amoprighttype`/`amprocrighttype`.
    pub righttype: Oid,
    /// `operatorset` — bitmask of operators with these types.
    pub operatorset: u64,
    /// `functionset` — bitmask of support funcs with these types.
    pub functionset: u64,
}
