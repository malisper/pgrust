//! The one-shot, no-parameter SPI execution entry points `SPI_exec` /
//! `SPI_execute` (`spi.c` 595-635) and the `SPI_tuptable`-over-`SPI_getvalue`
//! first-value read these drivers expose.
//!
//! `SPI_execute(src, read_only, tcount)` builds a `_SPI_plan` with
//! `parse_mode = RAW_PARSE_DEFAULT` and `cursor_options = CURSOR_OPT_PARALLEL_OK`,
//! `_SPI_prepare_oneshot_plan`s it (no parser hooks, no fixed params), then runs
//! `_SPI_execute_plan` with `read_only` / `tcount`. The result code is the last
//! statement's classification; `SPI_processed` is set to its row count.
//!
//! The whole prepare → plan → execute pipeline for a parameter-less query is
//! exactly what [`crate::execsql::spi_execsql_dynamic`] runs (the dynamic
//! `EXECUTE` path: `RAW_PARSE_DEFAULT`, no PL/pgSQL hooks, every command type,
//! sets `SPI_processed`), so these entry points delegate to it with an empty
//! `USING` parameter list, then render the first result row's first column into
//! the `SPI_tuptable` first-value slot [`SPI_getvalue`] reads.

use std::cell::RefCell;

use mcx::MemoryContext;
use types_error::PgResult;

use crate::execsql::{spi_execsql_dynamic, ExecsqlColumn};
use crate::result_code::SPI_ERROR_ARGUMENT;

thread_local! {
    /// The first column of the first row of the most recent row-returning
    /// `SPI_execute` (`SPI_tuptable->vals[0]` col 1, already rendered to its
    /// text image via `getTypeOutputInfo` + `OidOutputFunctionCall`). `None`
    /// when the last execute returned no tuple table or no rows.
    static SPI_FIRST_VALUE: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// `SPI_execute(const char *src, bool read_only, long tcount)` (spi.c:595). Runs
/// `src` as a single one-shot, no-parameter command of any type, returning the
/// SPI result code and setting `SPI_processed`. A row-returning command leaves
/// its first row's first column in the `SPI_tuptable` first-value slot.
pub fn SPI_execute(src: &str, read_only: bool, tcount: i64) -> PgResult<i32> {
    // C: `if (src == NULL || tcount < 0) return SPI_ERROR_ARGUMENT;`
    if tcount < 0 {
        return Ok(SPI_ERROR_ARGUMENT);
    }

    // `into = true` collects the first result row (for `SPI_getvalue`'s
    // `SPI_tuptable->vals[0]`); `collect_all = false` (we only need the first).
    let result = spi_execsql_dynamic(src, &[], read_only, true, false, tcount, false)?;

    // Render and stash the first row's first column for `SPI_getvalue_first`,
    // mirroring `SPI_tuptable->vals[0]` after a row-returning command. A
    // non-row-returning command (DML without RETURNING, utility) leaves no
    // tuple table → clear the slot.
    let first = if result.returned_tuptable {
        render_first_column(result.first_row.first())?
    } else {
        None
    };
    set_first_value(first);

    Ok(result.code)
}

/// `SPI_exec(const char *src, long tcount)` (spi.c:629) — `SPI_execute` with
/// `read_only = false`.
pub fn SPI_exec(src: &str, tcount: i64) -> PgResult<i32> {
    SPI_execute(src, false, tcount)
}

/// Render one collected result column into its text image
/// (`getTypeOutputInfo(typoid)` + `OidOutputFunctionCall(foutoid, val)` — the
/// body of `SPI_getvalue`), in a throwaway context. A SQL NULL renders to
/// `None`.
fn render_first_column(col: Option<&ExecsqlColumn>) -> PgResult<Option<String>> {
    let col = match col {
        Some(c) => c,
        None => return Ok(None),
    };
    if col.isnull {
        return Ok(None);
    }

    let cxt = MemoryContext::new("SPI getvalue");
    let mcx = cxt.mcx();

    // Reconstitute the column's `Datum` from the collected bare word / by-ref
    // byte image (the same rebuild the dynamic-param path uses).
    let value = match &col.byref {
        Some(bytes) => types_tuple::Datum::from_byref_bytes_in(mcx, bytes)?,
        None => types_tuple::Datum::from_usize(col.value),
    };

    // getTypeOutputInfo(typoid, &foutoid, &typisvarlena);
    let (foutoid, _typisvarlena) =
        backend_utils_cache_lsyscache_seams::get_type_output_info::call(col.typeid)?;
    // OidOutputFunctionCall(foutoid, val) -> text image.
    let bytes = backend_utils_fmgr_fmgr_seams::oid_output_function_call::call(mcx, foutoid, &value)?;
    Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
}

fn set_first_value(v: Option<String>) {
    SPI_FIRST_VALUE.with(|s| *s.borrow_mut() = v);
}

/// `SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1)` — the text
/// rendering of the first column of the first row left by the last
/// row-returning [`SPI_execute`]. Returns the empty string when there is no
/// such value (matching C, where `refresh_by_match_merge` reaches this only
/// after a `SPI_processed > 0` SELECT).
pub fn SPI_getvalue_first() -> String {
    SPI_FIRST_VALUE.with(|s| s.borrow().clone().unwrap_or_default())
}

/// Seam-shaped `SPI_exec(query, 0)` for matview's `refresh_by_match_merge`.
pub(crate) fn spi_exec_seam(query: String) -> PgResult<i32> {
    SPI_exec(&query, 0)
}

/// Seam-shaped `SPI_execute(query, read_only, tcount)`.
pub(crate) fn spi_execute_seam(query: String, read_only: bool, tcount: i64) -> PgResult<i32> {
    SPI_execute(&query, read_only, tcount)
}

/// Seam-shaped `SPI_processed`.
pub(crate) fn spi_processed_seam() -> PgResult<u64> {
    Ok(crate::backbone::SPI_processed())
}

/// Seam-shaped `SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1)`.
pub(crate) fn spi_getvalue_first_seam() -> PgResult<String> {
    Ok(SPI_getvalue_first())
}
