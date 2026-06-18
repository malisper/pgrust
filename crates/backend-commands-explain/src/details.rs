//! Structural detail helpers of `commands/explain.c` whose inputs are plain
//! values or `Instrumentation` (not unmodeled executor-state structs): WAL /
//! memory / storage / buffer-peek counters, the query-text / query-parameter
//! nodes, `elapsed_time`, and `show_instrumentation_count`.
//!
//! The per-node `show_*_info` helpers (hash / sort / material / memoize /
//! hashagg / tidbitmap / windowagg / ...) read executor-state carriers
//! (`HashState.hinstrument`, `MaterialState.tuplestorestate`,
//! `IndexScanState.iss_Instrument`, ...) that the trimmed `PlanStateNode` does
//! not carry; they are ANALYZE-only and not reachable on the structural slice,
//! so they cannot be expressed until those carriers are modelled. The pure
//! value-input pieces they share (`show_storage_info`) live here.

extern crate alloc;

use alloc::format;

use types_core::instrument::{Instrumentation, WalUsage};
use types_error::PgResult;
use types_explain::{ExplainFormat, ExplainState};

use backend_commands_explain_format as fmt;

/// `BYTES_TO_KILOBYTES(b)` — `(b + 1023) / 1024` (memutils.h).
fn bytes_to_kilobytes(b: i64) -> i64 {
    (b + (1024 - 1)) / 1024
}

/// `elapsed_time(starttime)` (explain.c:1163) — seconds since `starttime`.
pub fn elapsed_time(starttime: &types_core::instrument::instr_time) -> f64 {
    let mut endtime = types_core::instrument::instr_time::default();
    portability_instr_time::instr_time_set_current(&mut endtime);
    endtime.subtract(*starttime);
    endtime.get_double()
}

/// `ExplainQueryText(es, queryDesc)` (explain.c:1059) — add the "Query Text"
/// node. The driver passes the already-resolved source text (the C
/// `queryDesc->sourceText`); `None` is the C `NULL` guard.
pub fn ExplainQueryText(es: &mut ExplainState<'_>, source_text: Option<&str>) -> PgResult<()> {
    if let Some(text) = source_text {
        fmt::ExplainPropertyText("Query Text", text, es)?;
    }
    Ok(())
}

/// `ExplainQueryParameters(es, params, maxlen)` (explain.c:1074) — add the
/// "Query Parameters" node. The driver passes the already-built parameter log
/// string (`BuildParamLogString`); we emit it when non-empty, mirroring the
/// `params == NULL || numParams <= 0 || maxlen == 0` guard handled by the caller
/// producing `None`/empty.
pub fn ExplainQueryParameters(es: &mut ExplainState<'_>, param_str: Option<&str>) -> PgResult<()> {
    if let Some(s) = param_str {
        if !s.is_empty() {
            fmt::ExplainPropertyText("Query Parameters", s, es)?;
        }
    }
    Ok(())
}

/// `peek_buffer_usage(es, usage)` (explain.c:4046) — whether `show_buffer_usage`
/// would print anything for this `usage`. In non-text formats it always would
/// (zeros are printed); in text only positive counters print.
pub fn peek_buffer_usage(
    es: &ExplainState<'_>,
    usage: Option<&types_core::instrument::BufferUsage>,
) -> bool {
    let usage = match usage {
        Some(u) => u,
        None => return false,
    };
    if es.format != ExplainFormat::EXPLAIN_FORMAT_TEXT {
        return true;
    }
    let has_shared = usage.shared_blks_hit > 0
        || usage.shared_blks_read > 0
        || usage.shared_blks_dirtied > 0
        || usage.shared_blks_written > 0;
    let has_local = usage.local_blks_hit > 0
        || usage.local_blks_read > 0
        || usage.local_blks_dirtied > 0
        || usage.local_blks_written > 0;
    let has_temp = usage.temp_blks_read > 0 || usage.temp_blks_written > 0;
    let has_shared_timing =
        !usage.shared_blk_read_time.is_zero() || !usage.shared_blk_write_time.is_zero();
    let has_local_timing =
        !usage.local_blk_read_time.is_zero() || !usage.local_blk_write_time.is_zero();
    let has_temp_timing =
        !usage.temp_blk_read_time.is_zero() || !usage.temp_blk_write_time.is_zero();
    has_shared
        || has_local
        || has_temp
        || has_shared_timing
        || has_local_timing
        || has_temp_timing
}

/// `show_wal_usage(es, usage)` (explain.c:4255) — show WAL usage details.
pub fn show_wal_usage(es: &mut ExplainState<'_>, usage: &WalUsage) -> PgResult<()> {
    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        if usage.wal_records > 0
            || usage.wal_fpi > 0
            || usage.wal_bytes > 0
            || usage.wal_buffers_full > 0
        {
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str("WAL:")?;
            if usage.wal_records > 0 {
                es.str.try_push_str(&format!(" records={}", usage.wal_records))?;
            }
            if usage.wal_fpi > 0 {
                es.str.try_push_str(&format!(" fpi={}", usage.wal_fpi))?;
            }
            if usage.wal_bytes > 0 {
                es.str.try_push_str(&format!(" bytes={}", usage.wal_bytes))?;
            }
            if usage.wal_buffers_full > 0 {
                es.str
                    .try_push_str(&format!(" buffers full={}", usage.wal_buffers_full))?;
            }
            es.str.try_push('\n')?;
        }
    } else {
        fmt::ExplainPropertyInteger("WAL Records", None, usage.wal_records, es)?;
        fmt::ExplainPropertyInteger("WAL FPI", None, usage.wal_fpi, es)?;
        fmt::ExplainPropertyUInteger("WAL Bytes", None, usage.wal_bytes, es)?;
        fmt::ExplainPropertyInteger("WAL Buffers Full", None, usage.wal_buffers_full, es)?;
    }
    Ok(())
}

/// `show_memory_counters(es, mem_counters)` (explain.c:4298) — show memory usage
/// details from a planner `MemoryContextCounters`.
pub fn show_memory_counters(
    es: &mut ExplainState<'_>,
    totalspace: i64,
    freespace: i64,
) -> PgResult<()> {
    let mem_used_kb = bytes_to_kilobytes(totalspace - freespace);
    let mem_allocated_kb = bytes_to_kilobytes(totalspace);

    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        fmt::ExplainIndentText(es)?;
        es.str.try_push_str(&format!(
            "Memory: used={mem_used_kb}kB  allocated={mem_allocated_kb}kB"
        ))?;
        es.str.try_push('\n')?;
    } else {
        fmt::ExplainPropertyInteger("Memory Used", Some("kB"), mem_used_kb, es)?;
        fmt::ExplainPropertyInteger("Memory Allocated", Some("kB"), mem_allocated_kb, es)?;
    }
    Ok(())
}

/// `show_storage_info(maxStorageType, maxSpaceUsed, es)` (explain.c:2995) —
/// storage method + maximum memory/disk space used (shared by `show_sort_info`
/// / `show_material_info` / `show_windowagg_info`).
pub fn show_storage_info(
    es: &mut ExplainState<'_>,
    max_storage_type: &str,
    max_space_used: i64,
) -> PgResult<()> {
    let max_space_used_kb = bytes_to_kilobytes(max_space_used);
    if es.format != ExplainFormat::EXPLAIN_FORMAT_TEXT {
        fmt::ExplainPropertyText("Storage", max_storage_type, es)?;
        fmt::ExplainPropertyInteger("Maximum Storage", Some("kB"), max_space_used_kb, es)?;
    } else {
        fmt::ExplainIndentText(es)?;
        es.str.try_push_str(&format!(
            "Storage: {max_storage_type}  Maximum Storage: {max_space_used_kb}kB\n"
        ))?;
    }
    Ok(())
}

/// `show_instrumentation_count(qlabel, which, planstate, es)` (explain.c:3965) —
/// show a filtered-rows count. The driver supplies the node's `Instrumentation`
/// directly (the C `planstate->instrument`); a `None` instrument or
/// non-ANALYZE caller is the C `return`.
pub fn show_instrumentation_count(
    es: &mut ExplainState<'_>,
    qlabel: &str,
    which: i32,
    instrument: Option<&Instrumentation>,
) -> PgResult<()> {
    if !es.analyze {
        return Ok(());
    }
    let instr = match instrument {
        Some(i) => i,
        None => return Ok(()),
    };
    let nfiltered = if which == 2 {
        instr.nfiltered2
    } else {
        instr.nfiltered1
    };
    let nloops = instr.nloops;

    // In text mode, suppress zero counts.
    if nfiltered > 0.0 || es.format != ExplainFormat::EXPLAIN_FORMAT_TEXT {
        if nloops > 0.0 {
            fmt::ExplainPropertyFloat(qlabel, None, nfiltered / nloops, 0, es)?;
        } else {
            fmt::ExplainPropertyFloat(qlabel, None, 0.0, 0, es)?;
        }
    }
    Ok(())
}
