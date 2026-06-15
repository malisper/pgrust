//! Function-scan plan-node vocabulary (`nodes/plannodes.h` `FunctionScan`).
//!
//! `FunctionScan` is the plan node for an `RTE_FUNCTION` range-table entry
//! (a set-returning function in the FROM clause). The leading `Scan` base
//! reuses [`crate::nodeindexscan::Scan`]; the `functions` list carries the
//! per-function descriptors as [`RangeTblFunction`] values.

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::PgResult;

use crate::nodeindexscan::Scan;
use crate::rawnodes::RangeTblFunction;

pub use crate::nodes::T_FunctionScan;

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
