//! Work-table-scan plan-node and state vocabulary (nodes/plannodes.h /
//! executor/execnodes.h), plus the ancestor `RecursiveUnion` executor state the
//! work-table scan reaches into.
//!
//! Trimmed to the fields the `nodeWorktablescan.c` port consumes. The
//! `RecursiveUnionStateData` mirror is included here because a
//! [`WorkTableScanStateData`] holds a `RecursiveUnionState *rustate` back-link;
//! it is the real owned struct that `nodeRecursiveunion.c` will populate when it
//! lands (the work-table scan only ever reads it through seams into that
//! unported owner).

use alloc::boxed::Box;
use alloc::vec::Vec;

use mcx::MemoryContext;
use types_core::fmgr::FmgrInfo;
use types_core::primitive::Oid;

use crate::execnodes::{PlanStateData, ScanStateData};
use crate::funcapi::Tuplestorestate;
use crate::nodeagg::TupleHashTable;
use crate::nodeindexscan::Scan;

/// `WorkTableScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct WorkTableScan {
///     Scan        scan;
///     int         wtParam;    /* ID of Param representing work table */
/// } WorkTableScan;
/// ```
#[derive(Debug, Default)]
pub struct WorkTableScan<'mcx> {
    /// `Scan scan` — the abstract scan-plan base (embeds `Plan plan`).
    pub scan: Scan<'mcx>,
    /// `int wtParam` — ID of the `Param` representing the work table, indexing
    /// `EState.es_param_exec_vals`.
    pub wtParam: i32,
}

/// `WorkTableScanState` (execnodes.h):
///
/// ```c
/// typedef struct WorkTableScanState {
///     ScanState   ss;             /* its first field is NodeTag */
///     RecursiveUnionState *rustate;
/// } WorkTableScanState;
/// ```
#[derive(Debug, Default)]
pub struct WorkTableScanStateData<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `RecursiveUnionState *rustate` — the ancestor `RecursiveUnion`'s executor
    /// state, owning the work-table tuplestore. `None` (C `NULL`) until the
    /// first `ExecWorkTableScan` call resolves it from the work-table `Param`
    /// slot.
    pub rustate: Option<Box<RecursiveUnionStateData<'mcx>>>,
}

/// `RecursiveUnionState` (execnodes.h):
///
/// ```c
/// typedef struct RecursiveUnionState
/// {
///     PlanState   ps;             /* its first field is NodeTag */
///     bool        recursing;
///     bool        intermediate_empty;
///     Tuplestorestate *working_table;
///     Tuplestorestate *intermediate_table;
///     Oid        *eqfuncoids;     /* per-grouping-field equality fns */
///     FmgrInfo   *hashfunctions;  /* per-grouping-field hash fns */
///     MemoryContext tempContext;  /* short-term context for comparisons */
///     TupleHashTable hashtable;   /* hash table for tuples already seen */
///     MemoryContext tableContext; /* memory context containing hash table */
/// } RecursiveUnionState;
/// ```
///
/// The real owned struct. `nodeRecursiveunion.c` (not yet ported) owns its
/// construction and mutation; the work-table scan only reads `working_table`
/// through its seams into that owner.
#[derive(Debug, Default)]
pub struct RecursiveUnionStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `bool recursing` — are we in the recursive (phase-2) loop yet?
    pub recursing: bool,
    /// `bool intermediate_empty` — nothing stashed in the intermediate table?
    pub intermediate_empty: bool,
    /// `Tuplestorestate *working_table` — the current working table (WT).
    pub working_table: Option<Box<Tuplestorestate<'mcx>>>,
    /// `Tuplestorestate *intermediate_table` — accumulates this iteration's rows.
    pub intermediate_table: Option<Box<Tuplestorestate<'mcx>>>,
    /// `Oid *eqfuncoids` — per-grouping-field equality functions (UNION only).
    pub eqfuncoids: Vec<Oid>,
    /// `FmgrInfo *hashfunctions` — per-grouping-field hash functions (UNION only).
    pub hashfunctions: Vec<FmgrInfo>,
    /// `MemoryContext tempContext` — short-term context for comparisons.
    pub temp_context: Option<MemoryContext>,
    /// `TupleHashTable hashtable` — hash table for tuples already seen.
    pub hashtable: Option<Box<TupleHashTable<'mcx>>>,
    /// `MemoryContext tableContext` — memory context containing the hash table.
    pub table_context: Option<MemoryContext>,
}
