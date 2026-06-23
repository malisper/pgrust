//! Seam declarations for the `backend-executor-instrument` unit
//! (`executor/instrument.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `InstrAlloc(n, instrument_options, async_mode)` (instrument.c): allocate
    /// `n` zeroed [`Instrumentation`](nodes::Instrumentation) structures
    /// in the caller's current memory context, filling the `need_timer` /
    /// `need_bufusage` / `need_walusage` / `async_mode` flags from the option
    /// bits. `instrument_options` is the C `int` `InstrumentOption` bitmask. A
    /// negative `n` sign-extends to a huge request that palloc's MaxAllocSize
    /// gate rejects with a recoverable error.
    pub fn instr_alloc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        n: i32,
        instrument_options: i32,
        async_mode: bool,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, nodes::Instrumentation>>
);

seam_core::seam!(
    /// `InstrStartNode(instr)` (instrument.c): mark the start of a node
    /// execution — read the start timestamp (when `need_timer`) and snapshot
    /// the buffer/WAL usage totals. Errors with the C `elog(ERROR,
    /// "InstrStartNode called twice in a row")` when the timer is already
    /// running.
    pub fn instr_start_node(
        instr: &mut nodes::Instrumentation,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InstrStopNode(instr, nTuples)` (instrument.c): mark the end of a node
    /// execution — accumulate elapsed time and buffer/WAL usage, bump the
    /// per-tuple counters by `n_tuples`. Errors with the C `elog(ERROR,
    /// "InstrStopNode called without start")` when the timer was not running.
    pub fn instr_stop_node(
        instr: &mut nodes::Instrumentation,
        n_tuples: f64,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InstrEndLoop(instr)` (instrument.c): finish one cycle — fold the
    /// per-cycle counters into the totals and reset them. Errors with the C
    /// `elog(ERROR, "InstrEndLoop called on running node")` when the node's
    /// timer is still running.
    pub fn instr_end_loop(
        instr: &mut nodes::Instrumentation,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InstrUpdateTupleCount(instr, nTuples)` (instrument.c): add `n_tuples`
    /// to the node's running tuple count. Infallible.
    pub fn instr_update_tuple_count(
        instr: &mut nodes::Instrumentation,
        n_tuples: f64,
    )
);

seam_core::seam!(
    /// `InstrAggNode(dst, add)` (instrument.c): fold the per-node statistics in
    /// `add` into `dst` (the parallel-executor leader-side accumulation of each
    /// worker's `Instrumentation` into the leader's). `add` is passed by value
    /// (the C struct is trivially copyable). Infallible.
    pub fn instr_agg_node(
        dst: &mut nodes::Instrumentation,
        add: nodes::Instrumentation,
    )
);

seam_core::seam!(
    /// `pgWalUsage.wal_bytes += ...; wal_records++; wal_fpi += num_fpi`
    /// (xlog.c:1105, the `XLogInsertRecord` "Report WAL traffic to the
    /// instrumentation" block). The `pgWalUsage` global lives in instrument.c;
    /// this seam lets the WAL-insertion core (xlog) report the just-inserted
    /// record's traffic without depending on the instrument unit. Infallible.
    pub fn report_wal_usage(wal_bytes: u64, wal_records: i64, wal_fpi: i64)
);
