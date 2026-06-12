//! Material node vocabulary (nodes/plannodes.h / executor/execnodes.h).
//!
//! src-idiomatic hosts `Material` / `MaterialState` in this module; the name
//! is preserved.

use alloc::boxed::Box;

use crate::execnodes::{PlanStateData, ScanStateData};
use crate::funcapi::Tuplestorestate;

/// `Material` plan node (plannodes.h):
///
/// ```c
/// typedef struct Material { Plan plan; } Material;
/// ```
#[derive(Clone, Debug, Default)]
pub struct Material {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: crate::nodeindexscan::Plan,
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
pub struct MaterialState {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `int eflags` — capability flags to pass to the tuplestore.
    pub eflags: i32,
    /// `bool eof_underlying` — reached end of underlying plan?
    pub eof_underlying: bool,
    /// `Tuplestorestate *tuplestorestate` — the materialized rows.
    pub tuplestorestate: Option<Box<Tuplestorestate>>,
}

impl MaterialState {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData {
        &mut self.ss.ps
    }
}
