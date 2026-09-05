//! FdwRoutine (foreign/fdwapi.h) vocabulary.
//!
//! C: the FDW handler function returns a palloc'd `FdwRoutine` full of
//! callback pointers, consumed by the planner (GetForeignRelSize/Paths/Plan)
//! and the executor (Begin/Iterate/ReScan/End ForeignScan). In pgrust the
//! provider set is in-tree and closed, so the routine collapses to a tagged
//! provider id; each consuming layer owns the half of the callback table
//! whose types it can name (planner: `FdwPlanRoutine`; executor:
//! `FdwExecRoutine`), installed per-provider at `init_seams()` time and
//! looked up by [`FdwKind`].

use crate::tags::NodeTag;

/// The closed set of in-tree FDW providers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum FdwKind {
    FileFdw = 0,
    PostgresFdw = 1,
}

pub const NUM_FDW_KINDS: usize = 2;

mcx::forget_safe_nodrop!(FdwKind);

impl FdwKind {
    #[inline]
    pub fn index(self) -> usize {
        self as usize
    }
}

/// `FdwRoutine` — what an FDW handler function returns, trimmed to the
/// provider id (the callback tables are layer-owned; see module doc). The
/// handler returns a pointer datum to a `&'static FdwRoutine`; `tag` is the
/// C `IsA(routine, FdwRoutine)` check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FdwRoutine {
    pub tag: NodeTag,
    pub kind: FdwKind,
}

impl FdwRoutine {
    pub const fn new(kind: FdwKind) -> Self {
        FdwRoutine { tag: NodeTag::T_FdwRoutine, kind }
    }
}

/// One `ExplainForeignScan` property. Divergence from C: the FDW's explain
/// callback cannot take an ExplainState (crate cycle), so properties cross
/// as (label, value) pairs and the explain layer maps them onto
/// `ExplainPropertyText` / `ExplainPropertyInteger`.
#[derive(Clone, Copy, Debug)]
pub enum FdwExplainProp<'a> {
    Text(&'a str),
    Integer { value: i64, unit: &'static str },
}

/// The ExplainState bits C's ExplainForeignScan hooks read (same crate-cycle
/// marshal as FdwExplainProp): file_fdw gates "Foreign File Size" on
/// es->costs; postgres_fdw gates "Remote SQL" on es->verbose and names the
/// relations of a pushed-down join/aggregate through es->rtable_names.
#[derive(Clone, Copy, Debug)]
pub struct FdwExplainFlags<'a> {
    pub costs: bool,
    pub verbose: bool,
    /// es->analyze: the plan was executed, so executor-side state (an FDW's
    /// ri_FdwState-derived values such as postgres_fdw's clamped batch size)
    /// exists and C's explain hooks read it.
    pub analyze: bool,
    /// es->rtable_names (explain.c select_rtable_names_for_explain), indexed
    /// by rti - 1: EXPLAIN's deduplicated reference names ("pagg_1" for the
    /// second child scanned under the alias "pagg"). None (or out of range)
    /// = fall back to the RTE's eref aliasname, as C.
    pub rtable_names: &'a [Option<&'a str>],
}
