//! Join-type vocabulary (`nodes/nodes.h` `JoinType`) and the `Join` plan-node
//! base (`nodes/plannodes.h`), trimmed to what the join executor nodes consume.

use mcx::{Mcx, PgBox, PgVec, vec_with_capacity_in};
use types_error::PgResult;

use crate::nodeindexscan::Plan;
use crate::primnodes::Expr;

/// `JoinType` (nodes/nodes.h) — values verified against PostgreSQL 18.3's
/// generated enumeration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JoinType {
    /// `JOIN_INNER` — matching tuple pairs only.
    JOIN_INNER = 0,
    /// `JOIN_LEFT` — pairs + unmatched LHS tuples.
    JOIN_LEFT = 1,
    /// `JOIN_FULL` — pairs + unmatched LHS + unmatched RHS.
    JOIN_FULL = 2,
    /// `JOIN_RIGHT` — pairs + unmatched RHS tuples.
    JOIN_RIGHT = 3,
    /// `JOIN_SEMI` — 1 copy of each LHS row that has any match.
    JOIN_SEMI = 4,
    /// `JOIN_ANTI` — 1 copy of each LHS row that has no match.
    JOIN_ANTI = 5,
    /// `JOIN_RIGHT_SEMI` — 1 copy of each RHS row that has any match.
    JOIN_RIGHT_SEMI = 6,
    /// `JOIN_RIGHT_ANTI` — 1 copy of each RHS row that has no match.
    JOIN_RIGHT_ANTI = 7,
    /// `JOIN_UNIQUE_OUTER` — LHS path must be made unique.
    JOIN_UNIQUE_OUTER = 8,
    /// `JOIN_UNIQUE_INNER` — RHS path must be made unique.
    JOIN_UNIQUE_INNER = 9,
}

pub use JoinType::{
    JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_RIGHT_ANTI, JOIN_RIGHT_SEMI,
    JOIN_SEMI, JOIN_UNIQUE_INNER, JOIN_UNIQUE_OUTER,
};

/// `Join` plan node (nodes/plannodes.h) — the abstract base every join plan
/// node embeds first, trimmed to the fields the executor consumes:
///
/// ```c
/// typedef struct Join
/// {
///     Plan        plan;
///     JoinType    jointype;
///     bool        inner_unique;
///     List       *joinqual;   /* JOIN quals (in addition to plan.qual) */
/// } Join;
/// ```
#[derive(Debug)]
pub struct Join<'mcx> {
    /// `Plan plan` — its first field starts with the `NodeTag`.
    pub plan: Plan<'mcx>,
    /// `JoinType jointype`.
    pub jointype: JoinType,
    /// `bool inner_unique` — each outer tuple provably matches no more than one
    /// inner tuple.
    pub inner_unique: bool,
    /// `List *joinqual` — JOIN quals (in addition to plan.qual). `None` = the C
    /// `NIL`.
    pub joinqual: Option<PgVec<'mcx, Expr>>,
}

impl Default for Join<'_> {
    fn default() -> Self {
        Join {
            plan: Plan::default(),
            jointype: JoinType::JOIN_INNER,
            inner_unique: false,
            joinqual: None,
        }
    }
}

impl Join<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Join<'b>> {
        let joinqual = match &self.joinqual {
            Some(q) => {
                let mut out = vec_with_capacity_in(mcx, q.len())?;
                for e in q.iter() {
                    out.push(e.clone());
                }
                Some(out)
            }
            None => None,
        };
        Ok(Join {
            plan: self.plan.clone_in(mcx)?,
            jointype: self.jointype,
            inner_unique: self.inner_unique,
            joinqual,
        })
    }
}

/// `JoinState` head (executor/execnodes.h) — the embedded state base every join
/// executor node begins with:
///
/// ```c
/// typedef struct JoinState
/// {
///     PlanState   ps;
///     JoinType    jointype;
///     bool        single_match;   /* True if we should skip to next outer
///                                  * tuple after finding one inner match */
///     ExprState  *joinqual;       /* JOIN quals (in addition to ps.qual) */
/// } JoinState;
/// ```
#[derive(Debug, Default)]
pub struct JoinStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: crate::execnodes::PlanStateData<'mcx>,
    /// `JoinType jointype`.
    pub jointype: JoinType,
    /// `bool single_match` — skip to next outer tuple after one inner match.
    pub single_match: bool,
    /// `ExprState *joinqual` — compiled JOIN quals (in addition to `ps.qual`).
    /// `None` = the C `NULL`.
    pub joinqual: Option<PgBox<'mcx, crate::execexpr::ExprState<'mcx>>>,
}

impl Default for JoinType {
    fn default() -> Self {
        JoinType::JOIN_INNER
    }
}
