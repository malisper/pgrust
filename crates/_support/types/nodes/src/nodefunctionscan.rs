//! Function-scan plan-node vocabulary (`nodes/plannodes.h` `FunctionScan`).
//!
//! `FunctionScan` is the plan node for an `RTE_FUNCTION` range-table entry
//! (a set-returning function in the FROM clause). The leading `Scan` base
//! reuses [`crate::nodeindexscan::Scan`]; the `functions` list carries the
//! per-function descriptors as [`RangeTblFunction`] values.

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use ::types_error::PgResult;

use crate::execexpr::SetExprState;
use crate::execnodes::ScanStateData;
use crate::funcapi::Tuplestorestate;
use crate::nodeindexscan::Scan;
use crate::nodes::NodeTag;
use crate::rawnodes::RangeTblFunction;
use crate::SlotId;

pub use crate::nodes::T_FunctionScan;

/// `T_FunctionScanState` (nodes/nodetags.h) — the executor-state node tag for a
/// FunctionScan node. Verified against PostgreSQL 18.3 (`T_FunctionScanState =
/// 412`).
pub const T_FunctionScanState: NodeTag = NodeTag(412);

/// `FunctionScan` plan node (nodes/plannodes.h):
///
/// ```c
/// typedef struct FunctionScan
/// {
///     Scan        scan;
///     List       *functions;       /* list of RangeTblFunction nodes */
///     bool        funcordinality;  /* WITH ORDINALITY */
/// } FunctionScan;
/// ```
#[derive(Debug, Default)]
pub struct FunctionScan<'mcx> {
    /// `Scan scan` — the abstract scan-plan base (embeds `Plan plan`).
    pub scan: Scan<'mcx>,
    /// `List *functions` — list of `RangeTblFunction` nodes. `None` = the C
    /// `NIL`.
    pub functions: Option<PgVec<'mcx, RangeTblFunction<'mcx>>>,
    /// `bool funcordinality` — `WITH ORDINALITY` requested?
    pub funcordinality: bool,
}

impl FunctionScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<FunctionScan<'b>> {
        let functions = match &self.functions {
            Some(list) => {
                let mut out = vec_with_capacity_in(mcx, list.len())?;
                for f in list.iter() {
                    out.push(f.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        Ok(FunctionScan {
            scan: self.scan.clone_in(mcx)?,
            functions,
            funcordinality: self.funcordinality,
        })
    }
}

/// `FunctionScanPerFuncState` (nodeFunctionscan.c) — runtime data for each
/// function being scanned:
///
/// ```c
/// typedef struct FunctionScanPerFuncState
/// {
///     SetExprState *setexpr;      /* state of the expression being evaluated */
///     TupleDesc   tupdesc;        /* desc of the function result type */
///     int         colcount;       /* expected number of result columns */
///     Tuplestorestate *tstore;    /* holds the function result set */
///     int64       rowcount;       /* # of rows in result set, -1 if not known */
///     TupleTableSlot *func_slot;  /* function result slot (or NULL) */
/// } FunctionScanPerFuncState;
/// ```
///
/// In the owned tree `setexpr` is the owned compiled [`SetExprState`]
/// (`ExecInitTableFunctionResult` output, allocated in the per-query context);
/// `tstore` is the owned materialized-result tuplestore carrier (`None` = the C
/// `NULL`, i.e. the function has not been called yet); `func_slot` is the id of
/// the per-function result slot in the `EState` slot pool (`None` is the C
/// `NULL`, used in the simple single-function-no-ordinality case where results
/// are fetched straight into the scan slot).
#[derive(Debug)]
pub struct FunctionScanPerFuncState<'mcx> {
    /// `SetExprState *setexpr` — state of the expression being evaluated.
    pub setexpr: Option<PgBox<'mcx, SetExprState<'mcx>>>,
    /// `TupleDesc tupdesc` — desc of the function result type (`None` is the C
    /// `NULL`).
    pub tupdesc: types_tuple::heaptuple::TupleDesc<'mcx>,
    /// `int colcount` — expected number of result columns.
    pub colcount: i32,
    /// `Tuplestorestate *tstore` — holds the function result set. `None` is the
    /// C `NULL` (function not yet called, or invalidated by a rescan).
    pub tstore: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
    /// `int64 rowcount` — # of rows in result set, `-1` if not known.
    pub rowcount: i64,
    /// `TupleTableSlot *func_slot` — function result slot (id into the EState
    /// slot pool), or `None` (the C `NULL`).
    pub func_slot: Option<SlotId>,
}

impl<'mcx> FunctionScanPerFuncState<'mcx> {
    /// A zeroed per-function state (the C palloc cell), with `setexpr`/`tstore`
    /// the C `NULL` and `rowcount = -1` (set in `ExecInitFunctionScan`).
    pub fn new() -> Self {
        FunctionScanPerFuncState {
            setexpr: None,
            tupdesc: None,
            colcount: 0,
            tstore: None,
            rowcount: -1,
            func_slot: None,
        }
    }
}

impl Default for FunctionScanPerFuncState<'_> {
    fn default() -> Self {
        Self::new()
    }
}

/// `FunctionScanState` (execnodes.h):
///
/// ```c
/// typedef struct FunctionScanState
/// {
///     ScanState   ss;             /* its first field is NodeTag */
///     int         eflags;
///     bool        ordinality;
///     bool        simple;
///     int64       ordinal;
///     int         nfuncs;
///     struct FunctionScanPerFuncState *funcstates;    /* array of nfuncs */
///     MemoryContext argcontext;
/// } FunctionScanState;
/// ```
///
/// In the owned tree the `funcstates` C array is the owned `funcstates`
/// `PgVec`, allocated in the EState's per-query context; `argcontext` is a real
/// owned child memory context (the C `AllocSetContextCreate` "Table function
/// arguments" context that `ExecMakeTableFunctionResult` evaluates function
/// arguments in). `ss` begins with the `PlanState` head whose `NodeTag` is
/// `T_FunctionScanState`.
#[derive(Debug)]
pub struct FunctionScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `int eflags` — the exec flags this node was initialized with (saved for
    /// the `EXEC_FLAG_BACKWARD` test in `FunctionNext`).
    pub eflags: i32,
    /// `bool ordinality` — are we adding an ordinality column?
    pub ordinality: bool,
    /// `bool simple` — fast-path: a single function, no ordinality, function
    /// result type == scan result type.
    pub simple: bool,
    /// `int64 ordinal` — current ordinal position (`0` = "before the first
    /// row").
    pub ordinal: i64,
    /// `int nfuncs` — number of functions being scanned.
    pub nfuncs: i32,
    /// `struct FunctionScanPerFuncState *funcstates` — per-function runtime
    /// data (`nfuncs` long).
    pub funcstates: PgVec<'mcx, FunctionScanPerFuncState<'mcx>>,
    /// `MemoryContext argcontext` — context `ExecMakeTableFunctionResult` uses
    /// to evaluate function arguments in. A real owned child context (`None`
    /// until set in init).
    pub argcontext: Option<::mcx::MemoryContext>,
}

impl<'mcx> FunctionScanState<'mcx> {
    /// `makeNode(FunctionScanState)`-shaped construction: a palloc0 state with
    /// the `funcstates` array empty, allocated in `mcx`.
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        FunctionScanState {
            ss: ScanStateData::default(),
            eflags: 0,
            ordinality: false,
            simple: false,
            ordinal: 0,
            nfuncs: 0,
            funcstates: PgVec::new_in(mcx),
            argcontext: None,
        }
    }
}
