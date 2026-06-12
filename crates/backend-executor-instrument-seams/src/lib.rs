//! Seam declarations for the `backend-executor-instrument` unit
//! (`executor/instrument.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `InstrEndLoop(instr)` (instrument.c): finish one cycle — fold the
    /// per-cycle counters into the totals and reset them. Errors with the C
    /// `elog(ERROR, "InstrEndLoop called on running node")` when the node's
    /// timer is still running.
    pub fn instr_end_loop(
        instr: &mut types_nodes::Instrumentation,
    ) -> types_error::PgResult<()>
);
