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

use alloc::vec::Vec;

use types_core::instrument::{Instrumentation, WalUsage};
use types_error::PgResult;
use types_explain::{ExplainFormat, ExplainState};

use types_nodes::nodehash::HashState;
use types_nodes::nodeincrementalsort::{
    IncrementalSortGroupInfo, IncrementalSortStateData, SharedIncrementalSortInfo,
};
use types_nodes::nodesort::{SharedSortInfo, SortStateData, TuplesortMethod, TuplesortSpaceType};

use backend_commands_explain_format as fmt;
use backend_utils_sort_tuplesort::{tuplesort_method_name, tuplesort_space_type_name};

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

/// `show_hash_info(hashstate, es)` (commands/explain.c:3375) — under EXPLAIN
/// ANALYZE, emit the Hash node's bucket/batch/peak-memory instrumentation (the
/// "Buckets: N (originally M)  Batches: N (originally M)  Memory Usage: NkB"
/// line in TEXT format, or the discrete `Hash Buckets`/`Original Hash
/// Buckets`/`Hash Batches`/`Original Hash Batches`/`Peak Memory Usage`
/// properties otherwise).
pub fn show_hash_info(
    hashstate: &HashState<'_>,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    // HashInstrumentation hinstrument = {0};
    // ... collect local + merge worker maxima ... (collect_hash_instrumentation)
    let hinstrument = match hashstate.collect_hash_instrumentation() {
        Some(h) => h,
        None => return Ok(()),
    };

    // if (hinstrument.nbatch > 0)
    if hinstrument.nbatch > 0 {
        // uint64 spacePeakKb = BYTES_TO_KILOBYTES(hinstrument.space_peak);
        let space_peak_kb = bytes_to_kilobytes(hinstrument.space_peak as i64) as u64;

        if es.format != ExplainFormat::EXPLAIN_FORMAT_TEXT {
            fmt::ExplainPropertyInteger("Hash Buckets", None, hinstrument.nbuckets as i64, es)?;
            fmt::ExplainPropertyInteger(
                "Original Hash Buckets",
                None,
                hinstrument.nbuckets_original as i64,
                es,
            )?;
            fmt::ExplainPropertyInteger("Hash Batches", None, hinstrument.nbatch as i64, es)?;
            fmt::ExplainPropertyInteger(
                "Original Hash Batches",
                None,
                hinstrument.nbatch_original as i64,
                es,
            )?;
            fmt::ExplainPropertyUInteger("Peak Memory Usage", Some("kB"), space_peak_kb, es)?;
        } else if hinstrument.nbatch_original != hinstrument.nbatch
            || hinstrument.nbuckets_original != hinstrument.nbuckets
        {
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str(&format!(
                "Buckets: {} (originally {})  Batches: {} (originally {})  Memory Usage: {}kB\n",
                hinstrument.nbuckets,
                hinstrument.nbuckets_original,
                hinstrument.nbatch,
                hinstrument.nbatch_original,
                space_peak_kb
            ))?;
        } else {
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str(&format!(
                "Buckets: {}  Batches: {}  Memory Usage: {}kB\n",
                hinstrument.nbuckets, hinstrument.nbatch, space_peak_kb
            ))?;
        }
    }

    Ok(())
}

/// `show_hashagg_info(aggstate, es)` (commands/explain.c:3445) — for a
/// hashed/mixed `Agg` node, emit the planned-partition count and, under EXPLAIN
/// ANALYZE, the node-level `Batches`/`Memory Usage`/`Disk Usage` plus one line
/// per parallel worker. `info` is the carrier snapshot (the EXPLAIN crate cannot
/// name `AggStateData`); `None` means the strategy was not hashed/mixed and C
/// returned early.
pub fn show_hashagg_info(
    info: &types_nodes::aggstate_carrier::HashAggExplainInfo,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    // int64 memPeakKb = BYTES_TO_KILOBYTES(aggstate->hash_mem_peak);
    let mem_peak_kb = bytes_to_kilobytes(info.node.hash_mem_peak as i64);

    if es.format != ExplainFormat::EXPLAIN_FORMAT_TEXT {
        if es.costs {
            fmt::ExplainPropertyInteger(
                "Planned Partitions",
                None,
                info.hash_planned_partitions as i64,
                es,
            )?;
        }

        // During parallel query the leader may have not helped out; detect that
        // by checking how much memory it used.
        if es.analyze && info.node.hash_mem_peak > 0 {
            fmt::ExplainPropertyInteger(
                "HashAgg Batches",
                None,
                info.node.hash_batches_used as i64,
                es,
            )?;
            fmt::ExplainPropertyInteger("Peak Memory Usage", Some("kB"), mem_peak_kb, es)?;
            fmt::ExplainPropertyInteger(
                "Disk Usage",
                Some("kB"),
                info.node.hash_disk_used as i64,
                es,
            )?;
        }
    } else {
        let mut gotone = false;

        if es.costs && info.hash_planned_partitions > 0 {
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str(&format!(
                "Planned Partitions: {}",
                info.hash_planned_partitions
            ))?;
            gotone = true;
        }

        if es.analyze && info.node.hash_mem_peak > 0 {
            if !gotone {
                fmt::ExplainIndentText(es)?;
            } else {
                es.str.try_push_str("  ")?;
            }

            es.str.try_push_str(&format!(
                "Batches: {}  Memory Usage: {}kB",
                info.node.hash_batches_used, mem_peak_kb
            ))?;
            gotone = true;

            // Only display disk usage if we spilled to disk.
            if info.node.hash_batches_used > 1 {
                es.str.try_push_str(&format!(
                    "  Disk Usage: {}kB",
                    info.node.hash_disk_used
                ))?;
            }
        }

        if gotone {
            es.str.try_push('\n')?;
        }
    }

    // Display stats for each parallel worker.
    if es.analyze {
        for sinstrument in info.worker_instrument.iter() {
            // Skip workers that didn't do anything.
            if sinstrument.hash_mem_peak == 0 {
                continue;
            }
            let hash_disk_used = sinstrument.hash_disk_used;
            let hash_batches_used = sinstrument.hash_batches_used;
            let mem_peak_kb = bytes_to_kilobytes(sinstrument.hash_mem_peak as i64);

            // es->workers_state is unmodelled on the structural slice (the
            // ExplainOpenWorker/CloseWorker formatting is unmodelled), so worker
            // data appears as top-level data — matching C's hide_workers
            // fallback behaviour (cf. show_sort_info).
            if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                fmt::ExplainIndentText(es)?;
                es.str.try_push_str(&format!(
                    "Batches: {}  Memory Usage: {}kB",
                    hash_batches_used, mem_peak_kb
                ))?;
                if hash_batches_used > 1 {
                    es.str
                        .try_push_str(&format!("  Disk Usage: {}kB", hash_disk_used))?;
                }
                es.str.try_push('\n')?;
            } else {
                fmt::ExplainPropertyInteger(
                    "HashAgg Batches",
                    None,
                    hash_batches_used as i64,
                    es,
                )?;
                fmt::ExplainPropertyInteger("Peak Memory Usage", Some("kB"), mem_peak_kb, es)?;
                fmt::ExplainPropertyInteger("Disk Usage", Some("kB"), hash_disk_used as i64, es)?;
            }
        }
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
    // C calls `InstrEndLoop(planstate->instrument)` once at the top of
    // `ExplainNode`, folding the current in-progress cycle into the totals
    // (`nloops += 1` for a node that produced rows but was never loop-ended —
    // the common single-scan EXPLAIN ANALYZE case) *before* reading any
    // counter. Our owned model borrows the instrument immutably, so the
    // cost block (walk.rs) replicates that fold locally; replicate the same
    // `nloops` fold here so the divisor matches (otherwise `nloops == 0` makes
    // `Rows Removed by Filter` print 0 even when rows were filtered).
    let nloops = if instr.running {
        instr.nloops + 1.0
    } else {
        instr.nloops
    };

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

/// `NUM_TUPLESORTMETHODS` (tuplesort.h) — number of single-bit `TuplesortMethod`
/// values (top-N heapsort, quicksort, external sort, external merge).
const NUM_TUPLESORTMETHODS: i32 = 4;

/// `show_incremental_sort_group_info(groupInfo, groupLabel, indent, es)`
/// (explain.c) — emit one group's sort-method list and average/peak space usage.
pub fn show_incremental_sort_group_info(
    group_info: &IncrementalSortGroupInfo,
    group_label: &str,
    indent: bool,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    // Generate a list of sort methods used across all groups.
    //   for (bit = 0; bit < NUM_TUPLESORTMETHODS; bit++) { sortMethod = 1<<bit;
    //       if (groupInfo->sortMethods & sortMethod)
    //           methodNames = lappend(methodNames, tuplesort_method_name(...)); }
    let mut method_names: Vec<&'static str> = Vec::new();
    for bit in 0..NUM_TUPLESORTMETHODS {
        let sort_method_bit: u32 = 1u32 << bit;
        if group_info.sortMethods & sort_method_bit != 0 {
            // (1 << bit) is one of the single-bit TuplesortMethod values.
            let sort_method = match sort_method_bit {
                x if x == TuplesortMethod::SORT_TYPE_TOP_N_HEAPSORT as u32 => {
                    TuplesortMethod::SORT_TYPE_TOP_N_HEAPSORT
                }
                x if x == TuplesortMethod::SORT_TYPE_QUICKSORT as u32 => {
                    TuplesortMethod::SORT_TYPE_QUICKSORT
                }
                x if x == TuplesortMethod::SORT_TYPE_EXTERNAL_SORT as u32 => {
                    TuplesortMethod::SORT_TYPE_EXTERNAL_SORT
                }
                _ => TuplesortMethod::SORT_TYPE_EXTERNAL_MERGE,
            };
            method_names.push(tuplesort_method_name(sort_method));
        }
    }

    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        // if (indent) appendStringInfoSpaces(es->str, es->indent * 2);
        if indent {
            for _ in 0..(es.indent * 2) {
                es.str.try_push(' ')?;
            }
        }
        // appendStringInfo(es->str, "%s Groups: %ld  Sort Method", groupLabel, groupCount);
        es.str.try_push_str(group_label)?;
        es.str.try_push_str(" Groups: ")?;
        es.str.try_push_str(&format!("{}", group_info.groupCount))?;
        es.str.try_push_str("  Sort Method")?;

        // plural/singular based on methodNames size.
        if method_names.len() > 1 {
            es.str.try_push_str("s: ")?;
        } else {
            es.str.try_push_str(": ")?;
        }
        // foreach: comma-separated method names.
        for (i, name) in method_names.iter().enumerate() {
            es.str.try_push_str(name)?;
            if i < method_names.len() - 1 {
                es.str.try_push_str(", ")?;
            }
        }

        if group_info.maxMemorySpaceUsed > 0 {
            let avg_space = group_info.totalMemorySpaceUsed / group_info.groupCount;
            let space_type_name =
                tuplesort_space_type_name(TuplesortSpaceType::SORT_SPACE_TYPE_MEMORY);
            es.str.try_push_str(&format!(
                "  Average {space_type_name}: {avg_space}kB  Peak {space_type_name}: {}kB",
                group_info.maxMemorySpaceUsed
            ))?;
        }

        if group_info.maxDiskSpaceUsed > 0 {
            let avg_space = group_info.totalDiskSpaceUsed / group_info.groupCount;
            let space_type_name =
                tuplesort_space_type_name(TuplesortSpaceType::SORT_SPACE_TYPE_DISK);
            es.str.try_push_str(&format!(
                "  Average {space_type_name}: {avg_space}kB  Peak {space_type_name}: {}kB",
                group_info.maxDiskSpaceUsed
            ))?;
        }
    } else {
        // Non-text: structured group/property output.
        let group_name = format!("{group_label} Groups");
        fmt::ExplainOpenGroup("Incremental Sort Groups", Some(&group_name), true, es)?;
        fmt::ExplainPropertyInteger("Group Count", None, group_info.groupCount, es)?;

        fmt::ExplainPropertyList("Sort Methods Used", &method_names, es)?;

        if group_info.maxMemorySpaceUsed > 0 {
            let avg_space = group_info.totalMemorySpaceUsed / group_info.groupCount;
            let space_type_name =
                tuplesort_space_type_name(TuplesortSpaceType::SORT_SPACE_TYPE_MEMORY);
            let memory_name = format!("Sort Space {space_type_name}");
            fmt::ExplainOpenGroup("Sort Space", Some(&memory_name), true, es)?;
            fmt::ExplainPropertyInteger("Average Sort Space Used", Some("kB"), avg_space, es)?;
            fmt::ExplainPropertyInteger(
                "Peak Sort Space Used",
                Some("kB"),
                group_info.maxMemorySpaceUsed,
                es,
            )?;
            fmt::ExplainCloseGroup("Sort Space", Some(&memory_name), true, es)?;
        }
        if group_info.maxDiskSpaceUsed > 0 {
            let avg_space = group_info.totalDiskSpaceUsed / group_info.groupCount;
            let space_type_name =
                tuplesort_space_type_name(TuplesortSpaceType::SORT_SPACE_TYPE_DISK);
            let disk_name = format!("Sort Space {space_type_name}");
            fmt::ExplainOpenGroup("Sort Space", Some(&disk_name), true, es)?;
            fmt::ExplainPropertyInteger("Average Sort Space Used", Some("kB"), avg_space, es)?;
            fmt::ExplainPropertyInteger(
                "Peak Sort Space Used",
                Some("kB"),
                group_info.maxDiskSpaceUsed,
                es,
            )?;
            fmt::ExplainCloseGroup("Sort Space", Some(&disk_name), true, es)?;
        }

        fmt::ExplainCloseGroup("Incremental Sort Groups", Some(&group_name), true, es)?;
    }
    Ok(())
}

/// `show_incremental_sort_info(incrsortstate, es)` (explain.c) — capture sort
/// statistics for the incremental-sort node and emit the "Full-sort Groups:" /
/// "Pre-sorted Groups:" detail lines under EXPLAIN ANALYZE.
pub fn show_incremental_sort_info(
    incrsortstate: &IncrementalSortStateData<'_>,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    let fullsort_group_info = &incrsortstate.incsort_info.fullsortGroupInfo;

    if !es.analyze {
        return Ok(());
    }

    // Since we never have any prefix groups unless we've first sorted a full
    // group and transitioned modes, we don't need to do anything if there were 0
    // full groups. We still continue after this block (workers may have done
    // real work even if the leader didn't participate).
    if fullsort_group_info.groupCount > 0 {
        show_incremental_sort_group_info(fullsort_group_info, "Full-sort", true, es)?;
        let prefixsort_group_info = &incrsortstate.incsort_info.prefixsortGroupInfo;
        if prefixsort_group_info.groupCount > 0 {
            if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                es.str.try_push('\n')?;
            }
            show_incremental_sort_group_info(prefixsort_group_info, "Pre-sorted", true, es)?;
        }
        if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
            es.str.try_push('\n')?;
        }
    }

    // Per-worker shared_info path: at EXPLAIN ANALYZE time the leader's
    // `shared_info` has been snapshotted into the backend-local `Local` arm by
    // `ExecIncrementalSortRetrieveInstrumentation` (the DSM segment is already
    // detached). Mirror C's loop over `shared_info->sinfo[0..num_workers]`.
    if let Some(SharedIncrementalSortInfo::Local {
        num_workers,
        sinfo,
    }) = incrsortstate.shared_info.as_ref()
    {
        for n in 0..*num_workers {
            let incsort_info = match sinfo.get(n as usize) {
                Some(s) => s,
                None => break,
            };

            // Exclude workers that processed no sort groups.
            let worker_fullsort = &incsort_info.fullsortGroupInfo;
            if worker_fullsort.groupCount == 0 {
                continue;
            }

            // es->workers_state is unmodelled on the structural slice (the trimmed
            // ExplainState carries no worker formatting state); the indent-first
            // decision (workers_state == NULL || verbose) is thus always "true".
            let indent_first_line = es.workers_state.is_none() || es.verbose;
            show_incremental_sort_group_info(
                worker_fullsort,
                "Full-sort",
                indent_first_line,
                es,
            )?;
            let worker_prefixsort = &incsort_info.prefixsortGroupInfo;
            if worker_prefixsort.groupCount > 0 {
                if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                    es.str.try_push('\n')?;
                }
                show_incremental_sort_group_info(worker_prefixsort, "Pre-sorted", true, es)?;
            }
            if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                es.str.try_push('\n')?;
            }
        }
    }

    Ok(())
}

/// `show_sort_info(sortstate, es)` (explain.c:3084) — under EXPLAIN ANALYZE,
/// emit the sort algorithm + space (Memory/Disk) used by a plain `Sort` node
/// (the "Sort Method: quicksort  Memory: NNkB" line). Reads the node's completed
/// `Tuplesortstate` via the `tuplesort_get_stats` seam.
pub fn show_sort_info(
    sortstate: &SortStateData<'_>,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    if !es.analyze {
        return Ok(());
    }

    // if (sortstate->sort_Done && sortstate->tuplesortstate != NULL)
    if sortstate.sort_Done {
        if let Some(state) = sortstate.tuplesortstate.as_deref() {
            let stats = backend_utils_sort_tuplesort_seams::tuplesort_get_stats::call(state);
            let sort_method = tuplesort_method_name(stats.sortMethod);
            let space_type = tuplesort_space_type_name(stats.spaceType);
            let space_used = stats.spaceUsed;

            if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                fmt::ExplainIndentText(es)?;
                es.str.try_push_str(&format!(
                    "Sort Method: {sort_method}  {space_type}: {space_used}kB\n"
                ))?;
            } else {
                fmt::ExplainPropertyText("Sort Method", sort_method, es)?;
                fmt::ExplainPropertyInteger("Sort Space Used", Some("kB"), space_used, es)?;
                fmt::ExplainPropertyText("Sort Space Type", space_type, es)?;
            }
        }
    }

    // Per-worker shared_info (parallel sort): emit each filled slot. The
    // workers_state/OpenWorker/CloseWorker formatting is unmodelled on the
    // structural slice, so worker 0's data appears as top-level data (matching
    // C's hide_workers fallback behaviour).
    // At EXPLAIN ANALYZE time the leader's `shared_info` has been snapshotted
    // into the backend-local `Local` arm by `ExecSortRetrieveInstrumentation`
    // (the DSM segment is already detached). Mirror C's loop over
    // `shared_info->sinstrument[0..num_workers]`.
    if let Some(SharedSortInfo::Local {
        num_workers,
        sinstrument,
    }) = sortstate.shared_info.as_ref()
    {
        for n in 0..*num_workers {
            let sinstrument = match sinstrument.get(n as usize) {
                Some(s) => s,
                None => break,
            };
            // ignore any unfilled slots
            if sinstrument.sortMethod == TuplesortMethod::SORT_TYPE_STILL_IN_PROGRESS {
                continue;
            }
            let sort_method = tuplesort_method_name(sinstrument.sortMethod);
            let space_type = tuplesort_space_type_name(sinstrument.spaceType);
            let space_used = sinstrument.spaceUsed;

            if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                fmt::ExplainIndentText(es)?;
                es.str.try_push_str(&format!(
                    "Sort Method: {sort_method}  {space_type}: {space_used}kB\n"
                ))?;
            } else {
                fmt::ExplainPropertyText("Sort Method", sort_method, es)?;
                fmt::ExplainPropertyInteger("Sort Space Used", Some("kB"), space_used, es)?;
                fmt::ExplainPropertyText("Sort Space Type", space_type, es)?;
            }
        }
    }

    Ok(())
}
