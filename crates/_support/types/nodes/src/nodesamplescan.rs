//! `SampleScan` plan-node and `TableSampleClause` vocabulary
//! (nodes/plannodes.h / nodes/parsenodes.h), trimmed to the fields the
//! `nodeSamplescan.c` port consumes.

use alloc::boxed::Box;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::primitive::Oid;
use ::types_error::PgResult;

use crate::nodeindexscan::Scan;
use crate::nodes::NodeTag;
use crate::primnodes::Expr;

/// `T_TableSampleClause` (nodes/nodetags.h).
pub const T_TableSampleClause: NodeTag = NodeTag(104);
/// `T_SampleScan` (nodes/nodetags.h).
pub const T_SampleScan: NodeTag = NodeTag(340);

/// `TableSampleClause` (nodes/parsenodes.h): the parsed `TABLESAMPLE` clause the
/// `SampleScan` plan node points at.
///
/// ```c
/// typedef struct TableSampleClause {
///     NodeTag  type;
///     Oid      tsmhandler;   /* OID of the tablesample handler function */
///     List    *args;         /* tablesample argument expression(s) */
///     Expr    *repeatable;   /* REPEATABLE expression, or NULL if none */
/// } TableSampleClause;
/// ```
#[derive(Debug)]
pub struct TableSampleClause<'mcx> {
    /// `NodeTag type`.
    pub type_: NodeTag,
    /// `Oid tsmhandler` — OID of the tablesample handler function.
    pub tsmhandler: Oid,
    /// `List *args` — tablesample argument expression(s). `None` = the C `NIL`.
    pub args: Option<PgVec<'mcx, Expr<'mcx>>>,
    /// `Expr *repeatable` — REPEATABLE expression, or `None` if none.
    pub repeatable: Option<Box<Expr<'mcx>>>,
}

impl Default for TableSampleClause<'_> {
    fn default() -> Self {
        TableSampleClause {
            type_: T_TableSampleClause,
            tsmhandler: 0,
            args: None,
            repeatable: None,
        }
    }
}

impl TableSampleClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TableSampleClause<'b>> {
        let args = match &self.args {
            Some(list) => {
                let mut out = vec_with_capacity_in(mcx, list.len())?;
                for a in list.iter() {
                    // Deep-copy via `clone_in`, not the derived `Expr::clone`
                    // (which panics on a `SubPlan` arm).
                    out.push(a.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        Ok(TableSampleClause {
            type_: self.type_,
            tsmhandler: self.tsmhandler,
            args,
            repeatable: match &self.repeatable {
                Some(r) => Some(alloc::boxed::Box::new(r.clone_in(mcx)?)),
                None => None,
            },
        })
    }
}

/// `SampleScan` plan node (nodes/plannodes.h):
///
/// ```c
/// typedef struct SampleScan {
///     Scan                scan;
///     struct TableSampleClause *tablesample;
/// } SampleScan;
/// ```
#[derive(Debug, Default)]
pub struct SampleScan<'mcx> {
    /// `Scan scan` — the abstract scan base (which embeds `Plan plan` first;
    /// `scan.scanrelid` is the RT index, `scan.plan.qual` the residual qual).
    pub scan: Scan<'mcx>,
    /// `struct TableSampleClause *tablesample` — the parsed `TABLESAMPLE`
    /// clause. Typed directly: in C the link is always a `TableSampleClause *`.
    pub tablesample: Option<Box<TableSampleClause<'mcx>>>,
}

impl SampleScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SampleScan<'b>> {
        let tablesample = match &self.tablesample {
            Some(ts) => Some(Box::new(ts.clone_in(mcx)?)),
            None => None,
        };
        Ok(SampleScan {
            scan: self.scan.clone_in(mcx)?,
            tablesample,
        })
    }
}
