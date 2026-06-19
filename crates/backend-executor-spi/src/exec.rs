//! The remaining SPI cursor-driver leg (`ts_rewrite`'s SPI cursor walk).
//!
//! # Port status
//!
//! The prepared-plan execution surface (`SPI_prepare` / `SPI_keepplan` /
//! `SPI_execute_snapshot` / `spi_first_row_columns`) the RI triggers and
//! PL/pgSQL use is now a real body in [`crate::prepare`]. What remains
//! seam-and-panic here is the `ts_rewrite(query, text)` driver, which walks an
//! SPI **cursor** (`SPI_cursor_open` / `SPI_cursor_fetch` / `SPI_cursor_move`
//! over a prepared plan) rather than a single execute; that needs the cursor
//! leg (`PortalRunFetch` over a prepared-plan portal) which is the follow-up
//! cursor surface, so it stays an honest `panic!` (never `todo!`).

use backend_executor_spi_seams::TsRewriteResult;
use types_error::PgResult;

const NEED_CURSOR: &str = "backend-executor-spi: ts_rewrite SPI-cursor leg is not yet wired — \
    needs SPI_cursor_open/SPI_cursor_fetch over a prepared plan (the prepared-plan execute path \
    is landed; the prepared-plan *cursor* path is the follow-up cursor surface).";

/// `tsquery_rewrite_run(command)` seam body (the `ts_rewrite(query, text)` SPI
/// driver homed here per the seam contract; consumed by
/// `backend-utils-adt-ts-small`).
pub(crate) fn tsquery_rewrite_run_seam(_command: String) -> PgResult<TsRewriteResult> {
    panic!("{NEED_CURSOR}");
}
