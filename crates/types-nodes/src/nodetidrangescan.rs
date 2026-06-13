//! `TidRangeScan` plan-node vocabulary (nodes/plannodes.h), trimmed.

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::PgResult;

use crate::nodeindexscan::Scan;
use crate::primnodes::Expr;

/// `TidRangeScan` (nodes/plannodes.h) — TID range scan node. The `tidrangequals`
/// list holds the qual(s) involving `CTID op something`.
#[derive(Debug, Default)]
pub struct TidRangeScan<'mcx> {
    /// `Scan scan` — the abstract scan base (`scan.scanrelid` is the RT index;
    /// `scan.plan.qual` is the residual qual).
    pub scan: Scan<'mcx>,
    /// `List *tidrangequals` — qual(s) involving CTID op something. `None` is
    /// the C `NIL`.
    pub tidrangequals: Option<PgVec<'mcx, Expr>>,
}

impl TidRangeScan<'_> {
    /// Deep copy of the node (and its plan subtree) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TidRangeScan<'b>> {
        let tidrangequals = match &self.tidrangequals {
            Some(quals) => {
                let mut out = vec_with_capacity_in(mcx, quals.len())?;
                for q in quals.iter() {
                    out.push(q.clone());
                }
                Some(out)
            }
            None => None,
        };
        Ok(TidRangeScan {
            scan: self.scan.clone_in(mcx)?,
            tidrangequals,
        })
    }
}
