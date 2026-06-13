//! Seam declarations for the `backend-optimizer-util-var` unit
//! (`optimizer/util/var.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `pull_varattnos(node, varno, &varattnos)` (var.c): collect, into the
    /// returned bitmapset, the attribute numbers (offset by
    /// `FirstLowInvalidHeapAttributeNumber`) of all `Var`s in `node` that
    /// reference range-table entry `varno`. The C accumulates into a caller
    /// `Bitmapset *` initialized to `NULL`; the owned model returns the
    /// resulting set (allocated in `mcx`). `None` is the C empty/NULL set.
    /// Walking the tree can `ereport(ERROR)` on an unexpected node.
    pub fn pull_varattnos<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        node: &types_nodes::primnodes::Expr,
        varno: u32,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);
