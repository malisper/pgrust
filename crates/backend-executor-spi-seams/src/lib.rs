//! Seam declarations for the `backend-executor-spi` unit
//! (`executor/spi.c`). The owning unit installs these from its `init_seams()`
//! when it lands; until then a call panics loudly.

use types_core::{Oid, SubTransactionId};
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
