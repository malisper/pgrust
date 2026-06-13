//! Seam declarations for the `backend-executor-instrument` unit
//! (`executor/instrument.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `InstrStartNode(instr)` (instrument.c): mark the start of a node
    /// execution — read the start timestamp (when `need_timer`) and snapshot
    /// the buffer/WAL usage totals. Errors with the C `elog(ERROR,
    /// "InstrStartNode called twice in a row")` when the timer is already
    /// running.
    pub fn instr_start_node(
        instr: &mut types_nodes::Instrumentation,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InstrStopNode(instr, nTuples)` (instrument.c): mark the end of a node
    /// execution — accumulate elapsed time and buffer/WAL usage, bump the
    /// per-tuple counters by `n_tuples`. Errors with the C `elog(ERROR,
    /// "InstrStopNode called without start")` when the timer was not running.
    pub fn instr_stop_node(
        instr: &mut types_nodes::Instrumentation,
        n_tuples: f64,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InstrEndLoop(instr)` (instrument.c): finish one cycle — fold the
    /// per-cycle counters into the totals and reset them. Errors with the C
    /// `elog(ERROR, "InstrEndLoop called on running node")` when the node's
    /// timer is still running.
    pub fn instr_end_loop(
        instr: &mut types_nodes::Instrumentation,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InstrUpdateTupleCount(instr, nTuples)` (instrument.c): add `n_tuples`
    /// to the node's running tuple count. Infallible.
    pub fn instr_update_tuple_count(
        instr: &mut types_nodes::Instrumentation,
        n_tuples: f64,
    )
);
