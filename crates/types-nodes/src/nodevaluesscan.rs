//! `ValuesScan` node vocabulary (nodes/plannodes.h / executor/execnodes.h).
//!
//! `ValuesScan` scans a `VALUES (...), (...), ...` list appearing in the range
//! table.

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::PgResult;

use crate::execexpr::ExprState;
use crate::execnodes::{EcxtId, ScanStateData};
use crate::nodeindexscan::Scan;
use crate::nodes::NodeTag;
use crate::primnodes::Expr;

/// `T_ValuesScanState` (nodes/nodetags.h) — the executor-state node tag for a
/// ValuesScan node. Verified against PostgreSQL 18.3 (`T_ValuesScanState =
/// 413`).
pub const T_ValuesScanState: NodeTag = NodeTag(413);

/// `ValuesScan` (nodes/plannodes.h) — VALUES scan node:
///
/// ```c
/// typedef struct ValuesScan { Scan scan; List *values_lists; } ValuesScan;
/// ```
#[derive(Debug)]
pub struct ValuesScan<'mcx> {
    /// `Scan scan` — the abstract scan base (`scan.scanrelid` is the RT index;
    /// `scan.plan.qual` is the residual qual).
    pub scan: Scan<'mcx>,
    /// `List *values_lists` — list of expression lists. Each element is one
    /// VALUES row: a list of the row's column expressions.
    pub values_lists: PgVec<'mcx, PgVec<'mcx, Expr>>,
}

impl<'mcx> ValuesScan<'mcx> {
    /// Deep copy of the node (and its plan subtree) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ValuesScan<'b>> {
        let mut values_lists = vec_with_capacity_in(mcx, self.values_lists.len())?;
        for sublist in self.values_lists.iter() {
            let mut row = vec_with_capacity_in(mcx, sublist.len())?;
            for e in sublist.iter() {
                // Deep-copy via `clone_in`, not the derived `Expr::clone`
                // (which panics on a `SubPlan` arm).
                row.push(e.clone_in(mcx)?);
            }
            values_lists.push(row);
        }
        Ok(ValuesScan {
            scan: self.scan.clone_in(mcx)?,
            values_lists,
        })
    }
}

/// `ValuesScanState` (execnodes.h):
///
/// ```c
/// typedef struct ValuesScanState
/// {
///     ScanState   ss;             /* its first field is NodeTag */
///     ExprContext *rowcontext;
///     List      **exprlists;
///     List      **exprstatelists;
///     int         array_len;
///     int         curr_idx;
/// } ValuesScanState;
/// ```
///
/// In the owned tree `rowcontext` is the id of the per-sublist expression
/// context (created via `ExecAssignExprContext`, captured from
/// `ps_ExprContext` before the second one is built). The two runtime arrays
/// (`exprlists`/`exprstatelists`, `palloc`/`palloc0` in C) are owned `PgVec`s
/// allocated in the EState's per-query context: `exprlists[i]` is the row's
/// expression list, `exprstatelists[i]` the row's compiled `ExprState`s (built
/// eagerly only for SubPlan-bearing rows, else `None` cells until `ValuesNext`
/// fills them).
#[derive(Debug)]
pub struct ValuesScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `ExprContext *rowcontext` — per-expression-list context (id into the
    /// EState pool). `None` until init.
    pub rowcontext: Option<EcxtId>,
    /// `List **exprlists` — array (`array_len` long) of per-row expression
    /// lists being evaluated.
    pub exprlists: PgVec<'mcx, PgVec<'mcx, Expr>>,
    /// `List **exprstatelists` — array (`array_len` long) of per-row expression
    /// state lists. A row's cell is empty (the C `NULL`) until built (eagerly
    /// for SubPlan rows in init, lazily for the rest in `ValuesNext`).
    pub exprstatelists: PgVec<'mcx, PgVec<'mcx, Option<ExprState<'mcx>>>>,
    /// `int array_len` — size of the above arrays.
    pub array_len: i32,
    /// `int curr_idx` — current array index (0-based).
    pub curr_idx: i32,
}

impl<'mcx> ValuesScanState<'mcx> {
    /// A zeroed node (the C `makeNode(ValuesScanState)` `palloc0`), with its two
    /// runtime-array spines empty in `mcx`.
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        ValuesScanState {
            ss: ScanStateData::default(),
            rowcontext: None,
            exprlists: PgVec::new_in(mcx),
            exprstatelists: PgVec::new_in(mcx),
            array_len: 0,
            curr_idx: 0,
        }
    }
}
