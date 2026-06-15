//! Index access-method API vocabulary (`access/amapi.h`) and the
//! `CompareType` enum (`nodes/primnodes.h`).
//!
//! `IndexAmRoutine` is the dispatch struct an AM handler (`bthandler`,
//! `hashhandler`, …) assembles and returns. It is trimmed to the fields the
//! ported AM handlers populate; the C struct's `am*` function-pointer members
//! that are reached by safe Rust signatures (rather than the raw fmgr ABI)
//! are not stored here — the AM crate exposes them by name. The two pure
//! translate callbacks are kept as Rust fn pointers (their C signatures take
//! no relation/allocate nothing).

#![allow(non_upper_case_globals)]

use types_core::Oid;
use types_nodes::nodes::NodeTag;
use types_scan::scankey::StrategyNumber;

/// `T_IndexAmRoutine` (`nodes/nodetags.h`).
pub const T_IndexAmRoutine: NodeTag = NodeTag(438);

/// `CompareType` (`access/cmptype.h`) — the AM-independent comparison
/// operator categories an opclass can expose. Values verified against the C
/// header (PostgreSQL 18.3).
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CompareType {
    COMPARE_INVALID = 0,
    /// `BTLessStrategyNumber`
    COMPARE_LT = 1,
    /// `BTLessEqualStrategyNumber`
    COMPARE_LE = 2,
    /// `BTEqualStrategyNumber`
    COMPARE_EQ = 3,
    /// `BTGreaterEqualStrategyNumber`
    COMPARE_GE = 4,
    /// `BTGreaterStrategyNumber`
    COMPARE_GT = 5,
    /// no such btree strategy
    COMPARE_NE = 6,
    COMPARE_OVERLAP = 7,
    COMPARE_CONTAINED_BY = 8,
}

pub use CompareType::{
    COMPARE_CONTAINED_BY, COMPARE_EQ, COMPARE_GE, COMPARE_GT, COMPARE_INVALID, COMPARE_LE,
    COMPARE_LT, COMPARE_NE, COMPARE_OVERLAP,
};

/// `IndexBuildResult` (`access/genam.h`) — statistics returned by `ambuild`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct IndexBuildResult {
    /// `double heap_tuples` — # of heap tuples scanned.
    pub heap_tuples: f64,
    /// `double index_tuples` — # of index tuples created.
    pub index_tuples: f64,
}

/// `amtranslate_strategy` callback (`access/amapi.h`).
pub type IndexAmTranslateStrategy = fn(StrategyNumber, Oid) -> CompareType;
/// `amtranslate_cmptype` callback (`access/amapi.h`).
pub type IndexAmTranslateCompareType = fn(CompareType, Oid) -> StrategyNumber;
/// `amvalidate` callback (`access/amapi.h`), the raw `fn(Oid) -> bool` ABI
/// shape. Validators that return a soft-error result instead are reached by
/// name from their AM crate, not stored here.
pub type IndexAmValidate = fn(Oid) -> bool;

/// `IndexAmRoutine` (`access/amapi.h`) — AM parameters plus callbacks. Trimmed
/// to the parameter fields and the two pure translate callbacks; the
/// non-pure AM-method callbacks are reached by name from the owning AM crate.
#[derive(Clone, Debug)]
pub struct IndexAmRoutine {
    pub type_: NodeTag,
    /// total number of strategies (operators) by which we can traverse/search
    pub amstrategies: u16,
    /// total number of support functions that this AM uses
    pub amsupport: u16,
    /// opclass options support function number or 0
    pub amoptsprocnum: u16,
    /// does AM support ORDER BY indexed column's value?
    pub amcanorder: bool,
    /// does AM support ORDER BY result of an operator on indexed column?
    pub amcanorderbyop: bool,
    /// does AM support hashing of indexed column?
    pub amcanhash: bool,
    /// does AM consider opclasses with the same equality semantics equivalent?
    pub amconsistentequality: bool,
    /// does AM consider opclasses with the same ordering semantics equivalent?
    pub amconsistentordering: bool,
    /// can AM be used by backwards scan?
    pub amcanbackward: bool,
    /// does AM support UNIQUE indexes?
    pub amcanunique: bool,
    /// does AM support multi-column indexes?
    pub amcanmulticol: bool,
    /// can query omit key for the first column?
    pub amoptionalkey: bool,
    /// can AM handle ScalarArrayOpExpr quals?
    pub amsearcharray: bool,
    /// can AM handle IS NULL/NOT NULL quals?
    pub amsearchnulls: bool,
    /// can storage type differ from column type?
    pub amstorage: bool,
    /// can index be clustered on this AM?
    pub amclusterable: bool,
    /// does AM handle predicate locks?
    pub ampredlocks: bool,
    /// does AM support parallel scan?
    pub amcanparallel: bool,
    /// does AM support parallel build?
    pub amcanbuildparallel: bool,
    /// does AM support columns included with clause INCLUDE?
    pub amcaninclude: bool,
    /// does AM use maintenance_work_mem?
    pub amusemaintenanceworkmem: bool,
    /// does AM store tuple information only at block granularity?
    pub amsummarizing: bool,
    /// OR of parallel vacuum flags
    pub amparallelvacuumoptions: u8,
    /// type of data stored in index, or InvalidOid if variable
    pub amkeytype: Oid,
    pub amtranslatestrategy: Option<IndexAmTranslateStrategy>,
    pub amtranslatecmptype: Option<IndexAmTranslateCompareType>,
    pub amvalidate: Option<IndexAmValidate>,
}
