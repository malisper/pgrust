//! Seam declarations for the `backend-utils-adt-ts-small` unit
//! (`tsquery_cleanup.c` / `tsquery_rewrite.c` / `tsquery_util.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The only genuine external that this unit cannot host in-crate is the
//! `ts_rewrite(query, text)` SPI execution: the
//! `SPI_connect` … `SPI_cursor_fetch` … `SPI_finish` sequence of
//! `tsquery_rewrite_query` runs an arbitrary user command and reads back the
//! `(target, substitute)` `tsquery` row pairs. SPI is a genuinely-external
//! subsystem (it executes arbitrary SQL), so the execution is funneled through
//! one seam; the rewrite algorithm and the two-`tsquery`-column type-check
//! decision stay in the owning crate. (`check_stack_depth` /
//! `CHECK_FOR_INTERRUPTS` are owned by `tcop/postgres.c` and routed through
//! `backend-tcop-postgres-seams`, not here.)

use types_core::Oid;
use types_error::PgResult;

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
/// shape the C type check inspects (`tupdesc->natts`, `SPI_gettypeid(tupdesc,
/// 1)`, `SPI_gettypeid(tupdesc, 2)`); they are reported even for an empty
/// result because C performs the type check after the first
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
    /// C: `SPI_connect()`, `SPI_prepare(command, 0, NULL)`,
    /// `SPI_cursor_open(NULL, plan, NULL, NULL, true)`, the
    /// `SPI_cursor_fetch(portal, true, 100)` loop reading `SPI_tuptable` /
    /// `SPI_processed` and the per-column `SPI_getbinval(..., &isnull)`, then
    /// `SPI_cursor_close` / `SPI_freeplan` / `SPI_finish`.
    ///
    /// `command` is the `text_to_cstring(in)` query text. The provider gathers
    /// the type-check data only (it reports `natts` and the two column type
    /// OIDs); the `ERRCODE_INVALID_PARAMETER_VALUE` decision stays in-crate.
    pub fn tsquery_rewrite_run(command: String) -> PgResult<TsRewriteResult>
);
