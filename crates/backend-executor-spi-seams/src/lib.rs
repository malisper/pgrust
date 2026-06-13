//! Seam declarations for the Server Programming Interface (`executor/spi.c`)
//! calls `ri_triggers.c` makes to plan and run its FK enforcement queries.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. SPI plans are the opaque `SPIPlanPtr`
//! (`struct _SPI_plan *`); snapshots cross as owned `SnapshotData` values.

use mcx::{Mcx, PgString, PgVec};
use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_ri_triggers::{ResultColumn, SpiExecResult, SpiPlanPtr};

seam_core::seam!(
    /// `SPI_connect()`. Can `ereport(ERROR)` (nesting limit / OOM), carried on
    /// `Err`.
    pub fn spi_connect() -> PgResult<()>
);
seam_core::seam!(
    /// `SPI_finish()`; returns the SPI code (C checks `!= SPI_OK_FINISH`).
    pub fn spi_finish() -> PgResult<i32>
);
seam_core::seam!(
    /// `SPI_prepare(querystr, nargs, argtypes)`; `Ok(None)` for a NULL plan (C
    /// then `elog(ERROR)`s with `SPI_result_code_string(SPI_result)`). The
    /// query bytes are the raw server-encoded query text passed straight to
    /// the parser. Planning can `ereport(ERROR)`, carried on `Err`.
    pub fn spi_prepare(querystr: &[u8], argtypes: &[Oid]) -> PgResult<Option<SpiPlanPtr>>
);
seam_core::seam!(
    /// `SPI_keepplan(plan)`: move the plan to long-lived SPI memory. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn spi_keepplan(plan: SpiPlanPtr) -> PgResult<()>
);
seam_core::seam!(
    /// `SPI_freeplan(plan)`.
    pub fn spi_freeplan(plan: SpiPlanPtr) -> PgResult<()>
);
seam_core::seam!(
    /// `SPI_plan_is_valid(plan)`.
    pub fn spi_plan_is_valid(plan: SpiPlanPtr) -> bool
);
seam_core::seam!(
    /// `SPI_execute_snapshot(plan, vals, nulls, snapshot, crosscheck,
    /// read_only, fire_triggers, tcount)`. `nulls[i]` is `true` when the C
    /// `nulls[i] == 'n'`. `None` snapshots are C's `InvalidSnapshot`. Returns
    /// the SPI code + `SPI_processed`; can `ereport(ERROR)`, carried on `Err`.
    pub fn spi_execute_snapshot(
        plan: SpiPlanPtr,
        vals: &[Datum],
        nulls: &[bool],
        snapshot: Option<types_snapshot::SnapshotData>,
        crosscheck: Option<types_snapshot::SnapshotData>,
        read_only: bool,
        fire_triggers: bool,
        tcount: i64,
    ) -> PgResult<SpiExecResult>
);
seam_core::seam!(
    /// `SPI_result_code_string(code)` — the human-readable code name for elog
    /// messages, copied into `mcx`.
    pub fn spi_result_code_string<'mcx>(mcx: Mcx<'mcx>, code: i32) -> PgResult<PgString<'mcx>>
);
seam_core::seam!(
    /// After a `SPI_OK_SELECT` with rows, build the [`ResultColumn`]s for the
    /// first result tuple (`SPI_tuptable->vals[0]`) over the given (1-based)
    /// result-tuple `attnums`, rendering each value via `getTypeOutputInfo` +
    /// `OidOutputFunctionCall`. Allocated into `mcx`. Can `ereport(ERROR)`.
    pub fn spi_first_row_columns<'mcx>(
        mcx: Mcx<'mcx>,
        attnums: &[i16],
    ) -> PgResult<PgVec<'mcx, ResultColumn<'mcx>>>
);
