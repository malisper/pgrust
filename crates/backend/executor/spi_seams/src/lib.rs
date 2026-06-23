//! Seam declarations for the `backend-executor-spi` unit (`executor/spi.c`),
//! including the SPI calls `ri_triggers.c` makes to plan and run its FK
//! enforcement queries.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. SPI plans are the opaque `SPIPlanPtr`
//! (`struct _SPI_plan *`); snapshots cross as owned `SnapshotData` values.

use mcx::{Mcx, PgString, PgVec};
use types_core::{Oid, SubTransactionId};
use ::types_error::PgResult;
use ::types_tuple::Datum;
use types_ri_triggers::{ResultColumn, SpiExecResult, SpiPlanPtr};

/// One row of the `ts_rewrite(query, text)` SPI result: the `target`
/// (column 1) and `substitute` (column 2) `tsquery` datums.
///
/// Each is `None` when the corresponding `SPI_getbinval(..., &isnull)` reported
/// SQL NULL (C's `if (isnull) continue;` for column 1, and the `if (!isnull)`
/// guard for column 2), and otherwise the raw, fully-detoasted `tsquery`
/// varlena bytes (`DatumGetTSQuery(...)`).
pub type TsRewriteRow = (Option<Vec<u8>>, Option<Vec<u8>>);

/// The full result of running the rewrite command through SPI.
///
/// `natts` / `col1_type` / `col2_type` reproduce the `SPI_tuptable->tupdesc`
/// shape the consumer's type check inspects (`tupdesc->natts`,
/// `SPI_gettypeid(tupdesc, 1)`, `SPI_gettypeid(tupdesc, 2)`); they are reported
/// even for an empty result because C performs the type check after the first
/// `SPI_cursor_fetch` regardless of `SPI_processed`.
///
/// `batches` holds the fetched rows in cursor order, grouped exactly as the
/// successive `SPI_cursor_fetch(portal, true, 100)` calls returned them (one
/// inner `Vec` per fetch of up to 100 rows).
pub struct TsRewriteResult {
    /// `SPI_tuptable->tupdesc->natts`.
    pub natts: i32,
    /// `SPI_gettypeid(SPI_tuptable->tupdesc, 1)`.
    pub col1_type: Oid,
    /// `SPI_gettypeid(SPI_tuptable->tupdesc, 2)`.
    pub col2_type: Oid,
    /// The fetched rows, grouped per `SPI_cursor_fetch` batch.
    pub batches: Vec<Vec<TsRewriteRow>>,
}

seam_core::seam!(
    /// The `SPI_connect` … `SPI_finish` execution of the
    /// `ts_rewrite(query, text)` variant (`tsquery_rewrite_query`).
    ///
    /// C (`tsquery_rewrite.c`): `SPI_connect()`,
    /// `SPI_prepare(command, 0, NULL)`,
    /// `SPI_cursor_open(NULL, plan, NULL, NULL, true)`, the
    /// `SPI_cursor_fetch(portal, true, 100)` loop reading `SPI_tuptable` /
    /// `SPI_processed` and the per-column `SPI_getbinval(..., &isnull)`, then
    /// `SPI_cursor_close` / `SPI_freeplan` / `SPI_finish`. This is SPI's
    /// execution capability, so the declaration lives in SPI's seam crate; the
    /// caller keeps the `ERRCODE_INVALID_PARAMETER_VALUE` two-`tsquery`-column
    /// type-check decision and the rewrite algorithm in-crate.
    ///
    /// `command` is the `text_to_cstring(in)` query text. The provider gathers
    /// the type-check data only (it reports `natts` and the two column type
    /// OIDs).
    pub fn tsquery_rewrite_run(command: String) -> PgResult<TsRewriteResult>
);

seam_core::seam!(
    /// `CreateDestReceiver(DestSPI)` (spi.c `spi_printtupDR`): build the DestSPI
    /// receiver and register its `spi_dest_startup` / `spi_printtup` vtable into
    /// the `backend-tcop-dest` router, returning the handle that names it. The
    /// router's `CreateDestReceiver(Spi)` arm calls this, mirroring how it
    /// reaches printtup's `printtup_create_dr` and copyto's
    /// `create_copy_dest_receiver`.
    pub fn create_spi_dest_receiver() -> nodes::parsestmt::DestReceiverHandle
);

seam_core::seam!(
    /// `AtEOXact_SPI(isCommit)` — clean up SPI state; WARNs about leaked
    /// connections at commit.
    pub fn at_eoxact_spi(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `AtEOSubXact_SPI(isCommit, mySubid)`.
    pub fn at_eosubxact_spi(is_commit: bool, my_subid: SubTransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `SPI_inside_nonatomic_context()` — true when running inside a
    /// nonatomic SPI context (procedures).
    pub fn spi_inside_nonatomic_context() -> bool
);

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
    pub fn spi_execute_snapshot<'mcx>(
        plan: SpiPlanPtr,
        vals: &[Datum<'mcx>],
        nulls: &[bool],
        snapshot: Option<snapshot::SnapshotData>,
        crosscheck: Option<snapshot::SnapshotData>,
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
