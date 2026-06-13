//! Material node vocabulary (nodes/plannodes.h / executor/execnodes.h).
//!
//! src-idiomatic hosts `Material` / `MaterialState` in this module; the name
//! is preserved.

use mcx::{Mcx, PgBox};
use types_error::PgResult;

use crate::execnodes::{PlanStateData, ScanStateData};
use crate::funcapi::Tuplestorestate;

/// `Material` plan node (plannodes.h):
///
/// ```c
/// typedef struct Material { Plan plan; } Material;
/// ```
#[derive(Debug, Default)]
pub struct Material<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: crate::nodeindexscan::Plan<'mcx>,
}

impl Material<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Material<'b>> {
        Ok(Material {
            plan: self.plan.clone_in(mcx)?,
        })
    }
}

/// `MaterialState` (execnodes.h):
///
/// ```c
/// typedef struct MaterialState {
///     ScanState   ss;                 /* its first field is NodeTag */
///     int         eflags;             /* capability flags to pass to tuplestore */
///     bool        eof_underlying;     /* reached end of underlying plan? */
///     Tuplestorestate *tuplestorestate;
/// } MaterialState;
/// ```
#[derive(Debug, Default)]
pub struct MaterialState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `int eflags` — capability flags to pass to the tuplestore.
    pub eflags: i32,
    /// `bool eof_underlying` — reached end of underlying plan?
    pub eof_underlying: bool,
    /// `Tuplestorestate *tuplestorestate` — the materialized rows. The box is
    /// context-allocated (C: `tuplestore_begin_heap` pallocs the state in the
    /// caller's current context).
    pub tuplestorestate: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
}

impl<'mcx> MaterialState<'mcx> {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData<'mcx> {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData<'mcx> {
        &mut self.ss.ps
    }
}
