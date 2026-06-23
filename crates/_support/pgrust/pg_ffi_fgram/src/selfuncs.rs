//! ABI vocabulary specific to `backend/utils/adt/selfuncs.c` —
//! `src/include/utils/selfuncs.h`.
//!
//! These three `#[repr(C)]` structs are the public interface of the
//! selectivity / index-cost estimator and are produced/consumed across the
//! crate boundary (the planner fills `EstimationInfo`, `examine_variable` fills
//! `VariableStatData`, and the AM cost estimators fill `GenericCosts`).  They
//! are not glob-re-exported from the crate root (to avoid ambiguous-glob
//! collisions with the optimizer ABI modules); reach them by path, e.g.
//! `pg_ffi_fgram::selfuncs::VariableStatData`.

use core::ffi::c_void;

use crate::heaptuple::HeapTuple;
use crate::pathnodes::RelOptInfo;
use crate::types::{Cost, Oid, Selectivity};

/* ----------------------------------------------------------------------------
 * Default selectivity / cardinality estimates (selfuncs.h #defines).
 * -------------------------------------------------------------------------- */

/// `DEFAULT_EQ_SEL` — default selectivity for equalities such as "A = b".
pub const DEFAULT_EQ_SEL: f64 = 0.005;

/// `DEFAULT_INEQ_SEL` — default selectivity for inequalities such as "A < b".
pub const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;

/// `DEFAULT_RANGE_INEQ_SEL` — default selectivity for range inequalities
/// "A > b AND A < c".
pub const DEFAULT_RANGE_INEQ_SEL: f64 = 0.005;

/// `DEFAULT_MULTIRANGE_INEQ_SEL` — default selectivity for multirange
/// inequalities "A > b AND A < c".
pub const DEFAULT_MULTIRANGE_INEQ_SEL: f64 = 0.005;

/// `DEFAULT_MATCH_SEL` — default selectivity for pattern-match operators (LIKE).
pub const DEFAULT_MATCH_SEL: f64 = 0.005;

/// `DEFAULT_MATCHING_SEL` — default selectivity for other matching operators.
pub const DEFAULT_MATCHING_SEL: f64 = 0.010;

/// `DEFAULT_NUM_DISTINCT` — default number of distinct values in a table.
pub const DEFAULT_NUM_DISTINCT: f64 = 200.0;

/// `DEFAULT_UNK_SEL` — default selectivity for boolean and null test nodes.
pub const DEFAULT_UNK_SEL: f64 = 0.005;

/// `DEFAULT_NOT_UNK_SEL` — `1.0 - DEFAULT_UNK_SEL`.
pub const DEFAULT_NOT_UNK_SEL: f64 = 1.0 - DEFAULT_UNK_SEL;

/// `SELFLAG_USED_DEFAULT` — estimation fell back on one of the DEFAULTs.
pub const SELFLAG_USED_DEFAULT: u32 = 1 << 0;

/// `DEFAULT_PAGE_CPU_MULTIPLIER` (selfuncs.c local #define).
pub const DEFAULT_PAGE_CPU_MULTIPLIER: f64 = 50.0;

/* ----------------------------------------------------------------------------
 * EstimationInfo (selfuncs.h)
 * -------------------------------------------------------------------------- */

/// `EstimationInfo` — flags some selectivity estimators pass back to callers to
/// describe assumptions made during estimation.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct EstimationInfo {
    /// Flags (see `SELFLAG_USED_DEFAULT`).
    pub flags: u32,
}

/* ----------------------------------------------------------------------------
 * VariableStatData (selfuncs.h)
 * -------------------------------------------------------------------------- */

/// `void (*freefunc)(HeapTuple tuple)` — how to free `statsTuple`.
pub type VariableStatDataFreeFunc = Option<unsafe extern "C" fn(HeapTuple)>;

/// `VariableStatData` — return data from `examine_variable` and friends.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct VariableStatData {
    /// `Node *var` — the Var or expression tree.
    pub var: *mut c_void,
    /// `RelOptInfo *rel` — relation, or NULL if not identifiable.
    pub rel: *mut RelOptInfo,
    /// `HeapTuple statsTuple` — pg_statistic tuple, or NULL if none.
    /// NB: if non-NULL, it must be freed when caller is done.
    pub statsTuple: HeapTuple,
    /// `void (*freefunc)(HeapTuple)` — how to free `statsTuple`.
    pub freefunc: VariableStatDataFreeFunc,
    /// `Oid vartype` — exposed type of expression.
    pub vartype: Oid,
    /// `Oid atttype` — actual type (after stripping relabel).
    pub atttype: Oid,
    /// `int32 atttypmod` — actual typmod (after stripping relabel).
    pub atttypmod: i32,
    /// `bool isunique` — matches unique index, DISTINCT or GROUP-BY clause.
    pub isunique: bool,
    /// `bool acl_ok` — true if user has SELECT privilege on all rows from the
    /// table or column.
    pub acl_ok: bool,
}

impl Default for VariableStatData {
    fn default() -> Self {
        VariableStatData {
            var: core::ptr::null_mut(),
            rel: core::ptr::null_mut(),
            statsTuple: core::ptr::null_mut(),
            freefunc: None,
            vartype: 0,
            atttype: 0,
            atttypmod: 0,
            isunique: false,
            acl_ok: false,
        }
    }
}

/* ----------------------------------------------------------------------------
 * GenericCosts (selfuncs.h)
 * -------------------------------------------------------------------------- */

/// `GenericCosts` — intermediate and final values returned by
/// `genericcostestimate` to the per-AM cost estimators.
///
/// Callers should initialize all fields to zero; they may then set
/// `numIndexTuples` to a positive value and `num_sa_scans` to a value >= 1
/// before calling `genericcostestimate`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct GenericCosts {
    /// `Cost indexStartupCost` — index-related startup cost.
    pub indexStartupCost: Cost,
    /// `Cost indexTotalCost` — total index-related scan cost.
    pub indexTotalCost: Cost,
    /// `Selectivity indexSelectivity` — selectivity of index.
    pub indexSelectivity: Selectivity,
    /// `double indexCorrelation` — order correlation of index.
    pub indexCorrelation: f64,
    /// `double numIndexPages` — number of leaf pages visited.
    pub numIndexPages: f64,
    /// `double numIndexTuples` — number of leaf tuples visited.
    pub numIndexTuples: f64,
    /// `double spc_random_page_cost` — relevant random_page_cost value.
    pub spc_random_page_cost: f64,
    /// `double num_sa_scans` — # indexscans from ScalarArrayOpExprs.
    pub num_sa_scans: f64,
}
