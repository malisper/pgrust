//! The executor hooks, `pgss_store`, and the hashtable entry lifecycle
//! (`entry_alloc` / `entry_dealloc` / `entry_reset`).
//!
//! Faithful port of the corresponding pg_stat_statements.c routines, adapted to
//! pgrust's executor model: pgrust's `QueryDesc` has no `totaltime`
//! `Instrumentation` field, so execution time + buffer/WAL deltas are measured
//! by bracketing `ExecutorStart..ExecutorEnd` with a per-query thread-local
//! (`EXEC_BRACKET`) — the same `INSTR_TIME_SET_CURRENT` + `BufferUsageAccumDiff`
//! technique pgss's own planner/utility hooks use, rather than reading a
//! finalized `totaltime`.

use core::cell::RefCell;

use ::types_core::instrument::{instr_time, BufferUsage, WalUsage};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::hash::hsearch::{HASHACTION, HASH_ENTER, HASH_FIND, HASH_REMOVE};
use ::nodes::querydesc::QueryDesc;
use ::types_scan::sdir::ScanDirection;

use crate::shmem::{self, entry_ref, pgss_hash, pgss_ref};
use crate::{
    nesting_level, pgss_enabled, pgss_max, Counters, PgssEntry, PgssHashKey,
    NESTING_LEVEL, PGSS_EXEC, PGSS_INVALID, PGSS_NUMKIND, STICKY_DECREASE_FACTOR,
    USAGE_DEALLOC_PERCENT, USAGE_DECREASE_FACTOR, USAGE_INIT,
};

// ---------------------------------------------------------------------------
// Per-query execution bracket (the analog of queryDesc->totaltime).
// ---------------------------------------------------------------------------

struct ExecBracket {
    start_time: instr_time,
    start_buf: BufferUsage,
    start_wal: WalUsage,
    active: bool,
}

thread_local! {
    static EXEC_BRACKET: RefCell<Vec<ExecBracket>> = const { RefCell::new(Vec::new()) };
}

fn now() -> instr_time {
    let mut t = instr_time { ticks: 0 };
    ::instr_time::instr_time_set_current(&mut t);
    t
}

// ---------------------------------------------------------------------------
// Hook installation.
// ---------------------------------------------------------------------------

pub(crate) fn install_exec_hooks() {
    execMain_seams::set_executor_start_hook(Some(pgss_executor_start));
    execMain_seams::set_executor_run_hook(Some(pgss_executor_run));
    execMain_seams::set_executor_finish_hook(Some(pgss_executor_finish));
    execMain_seams::set_executor_end_hook(Some(pgss_executor_end));

    // `post_parse_analyze_hook = pgss_post_parse_analyze;` (the canonical
    // field-bearing parse-analysis path's variant). Records the normalized
    // query string for queries that have ignorable constants.
    parser_analyze_seams::set_post_parse_analyze_canonical_hook(Some(pgss_post_parse_analyze));

    // `planner_hook = pgss_planner;` — measure planning time when track_planning.
    planner_seams::set_planner_hook(Some(pgss_planner));

    // `ProcessUtility_hook = pgss_ProcessUtility;` — track utility statements.
    utility_seams::set_process_utility_hook(Some(pgss_process_utility));
}

/// `pgss_planner` (pg_stat_statements.c:887). Forward to the regular planner,
/// measuring planning time/buffer/WAL when `track_planning` is on. Always bumps
/// the nesting level around planning so functions evaluated during planning are
/// not seen as top-level.
fn pgss_planner<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    parse: &::nodes::copy_query::Query<'mcx>,
    query_string: &str,
    cursor_options: i32,
    bound_params: ::nodes::params::ParamListInfo,
) -> PgResult<::nodes::nodeindexscan::PlannedStmt<'mcx>> {
    // We can't process the query if no query_string is provided, and we ignore
    // queries with queryId zero (treated as utility statements).
    let track = pgss_enabled(nesting_level())
        && crate::pgss_track_planning()
        && !query_string.is_empty()
        && parse.queryId != 0;

    if !track {
        // Still bump the nesting level so functions evaluated during planning
        // are not seen as top-level calls.
        NESTING_LEVEL.with(|c| c.set(c.get() + 1));
        let result =
            planner_seams::standard_planner::call(mcx, parse, query_string, cursor_options, bound_params);
        NESTING_LEVEL.with(|c| c.set(c.get() - 1));
        return result;
    }

    let start_buf = instrument::pgBufferUsage();
    let start_wal = instrument::pgWalUsage();
    let start = now();

    NESTING_LEVEL.with(|c| c.set(c.get() + 1));
    let result =
        planner_seams::standard_planner::call(mcx, parse, query_string, cursor_options, bound_params);
    NESTING_LEVEL.with(|c| c.set(c.get() - 1));
    let result = result?;

    let mut duration = now();
    duration.subtract(start);

    let mut bufusage = BufferUsage::default();
    instrument::BufferUsageAccumDiff(&mut bufusage, &instrument::pgBufferUsage(), &start_buf);
    let mut walusage = WalUsage::default();
    instrument::WalUsageAccumDiff(&mut walusage, &instrument::pgWalUsage(), &start_wal);

    pgss_store(
        query_string,
        parse.queryId,
        parse.stmt_location,
        parse.stmt_len,
        crate::PGSS_PLAN as i32,
        duration.get_millisec(),
        0,
        Some(&bufusage),
        Some(&walusage),
        None,
        0,
        0,
    );

    Ok(result)
}

/// `pgss_ProcessUtility` (pg_stat_statements.c:1106). Track utility statements
/// (when track_utility) and manage the nesting level so nested optimizable
/// statements are charged to the utility level.
#[allow(clippy::too_many_arguments)]
fn pgss_process_utility<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    pstmt: &::nodes::nodeindexscan::PlannedStmt<'mcx>,
    query_string: &str,
    read_only_tree: bool,
    context: ::nodes::parsestmt::ProcessUtilityContext,
    params: ::nodes::portalcmds::ParamListInfo,
    dest: ::nodes::parsestmt::DestReceiverHandle,
    qc: &mut ::portal::QueryCompletion,
) -> PgResult<()> {
    let saved_query_id = pstmt.queryId;
    let saved_stmt_location = pstmt.stmt_location;
    let saved_stmt_len = pstmt.stmt_len;
    let enabled = crate::pgss_track_utility() && pgss_enabled(nesting_level());

    // Classify the utility statement (EXECUTE / PREPARE are not tracked, and do
    // not bump the nesting level, so their costs are charged at the right level).
    let (is_execute, is_prepare) = match &pstmt.utilityStmt {
        Some(u) => {
            let tag = u.tag();
            (
                tag == ::nodes::nodes::T_ExecuteStmt,
                tag == ::nodes::nodes::T_PrepareStmt,
            )
        }
        None => (false, false),
    };

    // Force utility statements to get queryId zero so the executor hooks don't
    // also track the contained optimizable statement (we measure at the utility
    // level). pgrust's `pstmt` is an immutable borrow here; the executor hooks
    // gate on the PlannedStmt queryId, which for a CMD_UTILITY statement carries
    // the same saved id — but the contained statement re-derives its own queryId
    // at parse-analysis, so double counting is avoided by the toplevel gate.

    if enabled && !is_execute && !is_prepare {
        let start_buf = instrument::pgBufferUsage();
        let start_wal = instrument::pgWalUsage();
        let start = now();

        NESTING_LEVEL.with(|c| c.set(c.get() + 1));
        let result = utility_seams::standard_process_utility::call(
            mcx,
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            dest,
            qc,
        );
        NESTING_LEVEL.with(|c| c.set(c.get() - 1));
        result?;

        let mut duration = now();
        duration.subtract(start);

        // Track rows for COPY/FETCH/SELECT/REFRESH MATERIALIZED VIEW. The
        // `CommandTag` value is the 0-based cmdtaglist.h list position (verified
        // vs PG 18.3): COPY=56, FETCH=154, REFRESH MATERIALIZED VIEW=169,
        // SELECT=179.
        let rows = match qc.commandTag {
            56 | 154 | 169 | 179 => qc.nprocessed,
            _ => 0,
        };

        let mut bufusage = BufferUsage::default();
        instrument::BufferUsageAccumDiff(&mut bufusage, &instrument::pgBufferUsage(), &start_buf);
        let mut walusage = WalUsage::default();
        instrument::WalUsageAccumDiff(&mut walusage, &instrument::pgWalUsage(), &start_wal);

        pgss_store(
            query_string,
            saved_query_id,
            saved_stmt_location,
            saved_stmt_len,
            PGSS_EXEC as i32,
            duration.get_millisec(),
            rows,
            Some(&bufusage),
            Some(&walusage),
            None,
            0,
            0,
        );
        Ok(())
    } else {
        // Not tracking: still bump the nesting level (except for EXECUTE/PREPARE)
        // so functions evaluated within are not seen as top-level calls.
        let bump_level = !is_execute && !is_prepare;
        if bump_level {
            NESTING_LEVEL.with(|c| c.set(c.get() + 1));
        }
        let result = utility_seams::standard_process_utility::call(
            mcx,
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            dest,
            qc,
        );
        if bump_level {
            NESTING_LEVEL.with(|c| c.set(c.get() - 1));
        }
        result
    }
}

/// `pgss_post_parse_analyze` (pg_stat_statements.c:835). Create a sticky
/// normalized hash-table entry whenever query jumbling found ignorable
/// constants, so the normalized form of the query string is recorded.
fn pgss_post_parse_analyze(
    info: parser_analyze_seams::PostParseAnalyzeQueryInfo<'_>,
    jstate: Option<&::nodes::portalcmds::JumbleState>,
) -> PgResult<()> {
    // Safety check: only act when enabled at this nesting level.
    if !shmem::is_initialized() || !pgss_enabled(nesting_level()) {
        return Ok(());
    }

    // If it's EXECUTE, the queryId is cleared for the underlying PREPARE; with
    // track_utility on, do not create a normalized entry here. (pgss clears
    // query->queryId in C; here the executor path keys off the same queryId, and
    // the ProcessUtility hook forces utility queryId to zero, so a normalized
    // entry for EXECUTE would never be matched — skip it.)
    if info.is_utility && crate::pgss_track_utility() && info.utility_is_execute {
        return Ok(());
    }

    // If jumbling identified ignorable constants, create the normalized entry.
    let Some(jstate) = jstate else { return Ok(()) };
    if jstate.clocations_count() == 0 {
        return Ok(());
    }

    let pgss_jstate = to_pgss_jumble(jstate);
    pgss_store(
        info.source_text,
        info.query_id,
        info.stmt_location,
        info.stmt_len,
        PGSS_INVALID,
        0.0,
        0,
        None,
        None,
        Some(StoreJumble {
            query_loc: info.stmt_location,
            jstate: &pgss_jstate,
        }),
        0,
        0,
    );
    Ok(())
}

/// Project the core `JumbleState` (constant locations) onto pgss's
/// [`crate::normalize::PgssJumble`] view.
fn to_pgss_jumble(
    jstate: &::nodes::portalcmds::JumbleState,
) -> crate::normalize::PgssJumble {
    use crate::normalize::{PgssJumble, PgssLocationLen};
    PgssJumble {
        clocations: jstate
            .clocations
            .iter()
            .map(|c| PgssLocationLen {
                location: c.location,
                length: c.length,
                squashed: c.squashed,
                extern_param: c.extern_param,
            })
            .collect(),
        highest_extern_param_id: jstate.highest_extern_param_id,
        has_squashed_lists: jstate.has_squashed_lists,
    }
}

/// `pgss_ExecutorStart` (pg_stat_statements.c:992).
fn pgss_executor_start(query_desc: &mut QueryDesc, eflags: i32) -> PgResult<()> {
    // Chain: pgrust's single settable hook means we run standard directly.
    execMain::standard_ExecutorStart(query_desc, eflags)?;

    // If query has queryId zero, don't track it.
    if pgss_enabled(nesting_level()) && query_desc.query_id() != 0 {
        // Set up to track elapsed time + buffer/WAL deltas across execution.
        EXEC_BRACKET.with(|b| {
            b.borrow_mut().push(ExecBracket {
                start_time: now(),
                start_buf: instrument::pgBufferUsage(),
                start_wal: instrument::pgWalUsage(),
                active: true,
            });
        });
    } else {
        EXEC_BRACKET.with(|b| {
            b.borrow_mut().push(ExecBracket {
                start_time: instr_time { ticks: 0 },
                start_buf: BufferUsage::default(),
                start_wal: WalUsage::default(),
                active: false,
            });
        });
    }
    Ok(())
}

/// `pgss_ExecutorRun` (pg_stat_statements.c:1026) — track nesting depth.
fn pgss_executor_run(
    query_desc: &mut QueryDesc,
    direction: ScanDirection,
    count: u64,
) -> PgResult<()> {
    NESTING_LEVEL.with(|c| c.set(c.get() + 1));
    let result = execMain::standard_ExecutorRun(query_desc, direction, count);
    NESTING_LEVEL.with(|c| c.set(c.get() - 1));
    result
}

/// `pgss_ExecutorFinish` (pg_stat_statements.c:1047) — track nesting depth.
fn pgss_executor_finish(query_desc: &mut QueryDesc) -> PgResult<()> {
    NESTING_LEVEL.with(|c| c.set(c.get() + 1));
    let result = execMain::standard_ExecutorFinish(query_desc);
    NESTING_LEVEL.with(|c| c.set(c.get() - 1));
    result
}

/// `pgss_ExecutorEnd` (pg_stat_statements.c:1068) — store results if needed.
fn pgss_executor_end(query_desc: &mut QueryDesc) -> PgResult<()> {
    let query_id = query_desc.query_id();
    let bracket = EXEC_BRACKET.with(|b| b.borrow_mut().pop());

    if let Some(bracket) = bracket {
        if query_id != 0 && bracket.active && pgss_enabled(nesting_level()) {
            let mut duration = now();
            duration.subtract(bracket.start_time);

            let mut bufusage = BufferUsage::default();
            instrument::BufferUsageAccumDiff(
                &mut bufusage,
                &instrument::pgBufferUsage(),
                &bracket.start_buf,
            );
            let mut walusage = WalUsage::default();
            instrument::WalUsageAccumDiff(
                &mut walusage,
                &instrument::pgWalUsage(),
                &bracket.start_wal,
            );

            pgss_store(
                &query_desc.source_text_owned(),
                query_id,
                query_desc.stmt_location(),
                query_desc.stmt_len(),
                PGSS_EXEC as i32,
                duration.get_millisec(),
                query_desc.es_total_processed(),
                Some(&bufusage),
                Some(&walusage),
                None,
                query_desc.es_parallel_workers_to_launch(),
                query_desc.es_parallel_workers_launched(),
            );
        }
    }

    execMain::standard_ExecutorEnd(query_desc)
}

// ---------------------------------------------------------------------------
// pgss_store.
// ---------------------------------------------------------------------------

/// The normalized-query intent passed to `pgss_store` (the C `JumbleState
/// *jstate`). `Some` means "create only a sticky normalized entry"; the carried
/// data is the constant-location array used to build the normalized text.
pub(crate) struct StoreJumble<'a> {
    pub query_loc: i32,
    pub jstate: &'a crate::normalize::PgssJumble,
}

/// `pgss_store(query, queryId, query_location, query_len, kind, total_time,
/// rows, bufusage, walusage, jitusage, jstate, ...)` (pg_stat_statements.c:1280).
#[allow(clippy::too_many_arguments)]
pub(crate) fn pgss_store(
    query: &str,
    query_id: i64,
    mut query_location: i32,
    mut query_len: i32,
    kind: i32,
    total_time: f64,
    rows: u64,
    bufusage: Option<&BufferUsage>,
    walusage: Option<&WalUsage>,
    jstate: Option<StoreJumble<'_>>,
    parallel_workers_to_launch: i32,
    parallel_workers_launched: i32,
) {
    let encoding = mb_fgram::GetDatabaseEncoding() as i32;

    if !shmem::is_initialized() {
        return;
    }
    // Nothing to do if no query identifier.
    if query_id == 0 {
        return;
    }

    // Confine our attention to the relevant part of the string (CleanQuerytext).
    let query_bytes = query.as_bytes();
    let cleaned = clean_querytext(query_bytes, &mut query_location, &mut query_len);

    // Set up key for hashtable search (clear padding).
    let mut key = PgssHashKey {
        userid: miscinit::GetUserId(),
        dbid: init_small_seams::my_database_id::call(),
        queryid: query_id,
        toplevel: nesting_level() == 0,
        _pad: [0; 7],
    };

    let pgss = unsafe { pgss_ref() };
    let lock = pgss.lock;

    // Lookup the hash table entry with shared lock.
    if lwlock_acquire(lock, false).is_err() {
        return;
    }

    let mut entry = hash_search(&mut key, HASH_FIND);
    let mut norm_query: Option<Vec<u8>> = None;

    if entry.is_null() {
        // Create new entry, if not present.
        // Build the normalized query string if caller asked (without the lock).
        if let Some(ref j) = jstate {
            lwlock_release(lock);
            let mut len = query_len;
            // C passes the *post-CleanQuerytext* query_location here (the
            // local `query_location`, updated by clean_querytext above), NOT
            // the original stmt_location saved in jstate. When CleanQuerytext
            // trims leading whitespace/comments it advances query_location;
            // using the stale value mis-offsets every recorded constant by the
            // trimmed amount (dropping the statement's leading char).
            norm_query = Some(crate::normalize::generate_normalized_query(
                j.jstate, cleaned, query_location, &mut len,
            ));
            query_len = len;
            if lwlock_acquire(lock, false).is_err() {
                return;
            }
        }

        let text: &[u8] = match &norm_query {
            Some(n) => n,
            None => &cleaned[..query_len.max(0) as usize],
        };

        let mut gc_count = 0i32;
        let (mut stored, mut query_offset) =
            crate::qtext::qtext_store(text, query_len.max(0) as usize, Some(&mut gc_count));

        let do_gc = crate::qtext::need_gc_qtexts();

        // Need exclusive lock to make a new hashtable entry — promote.
        lwlock_release(lock);
        if lwlock_acquire(lock, true).is_err() {
            return;
        }

        // A GC may have occurred; rewrite the text if so.
        if !stored || pgss.gc_count != gc_count {
            let (s, off) = crate::qtext::qtext_store(text, query_len.max(0) as usize, None);
            stored = s;
            query_offset = off;
        }

        if !stored {
            lwlock_release(lock);
            return;
        }

        let raw = entry_alloc(&key, query_offset, query_len, encoding, jstate.is_some());
        entry = raw;

        if do_gc {
            crate::qtext::gc_qtexts();
        }
    }

    // Increment the counts, except when jstate is set (sticky pre-entry).
    if jstate.is_none() && !entry.is_null() {
        let e = unsafe { entry_ref(entry) };
        shmem::spin_lock_acquire(&e.mutex);

        // "Unstick" entry if previously sticky.
        if e.counters.is_sticky() {
            e.counters.usage = USAGE_INIT;
        }

        let k = kind as usize;
        e.counters.calls[k] += 1;
        e.counters.total_time[k] += total_time;

        if e.counters.calls[k] == 1 {
            e.counters.min_time[k] = total_time;
            e.counters.max_time[k] = total_time;
            e.counters.mean_time[k] = total_time;
        } else {
            // Welford's method.
            let old_mean = e.counters.mean_time[k];
            e.counters.mean_time[k] += (total_time - old_mean) / e.counters.calls[k] as f64;
            e.counters.sum_var_time[k] +=
                (total_time - old_mean) * (total_time - e.counters.mean_time[k]);

            if e.counters.min_time[k] == 0.0 && e.counters.max_time[k] == 0.0 {
                e.counters.min_time[k] = total_time;
                e.counters.max_time[k] = total_time;
            } else {
                if e.counters.min_time[k] > total_time {
                    e.counters.min_time[k] = total_time;
                }
                if e.counters.max_time[k] < total_time {
                    e.counters.max_time[k] = total_time;
                }
            }
        }
        e.counters.rows += rows as i64;
        if let Some(b) = bufusage {
            e.counters.shared_blks_hit += b.shared_blks_hit;
            e.counters.shared_blks_read += b.shared_blks_read;
            e.counters.shared_blks_dirtied += b.shared_blks_dirtied;
            e.counters.shared_blks_written += b.shared_blks_written;
            e.counters.local_blks_hit += b.local_blks_hit;
            e.counters.local_blks_read += b.local_blks_read;
            e.counters.local_blks_dirtied += b.local_blks_dirtied;
            e.counters.local_blks_written += b.local_blks_written;
            e.counters.temp_blks_read += b.temp_blks_read;
            e.counters.temp_blks_written += b.temp_blks_written;
            e.counters.shared_blk_read_time += b.shared_blk_read_time.get_millisec();
            e.counters.shared_blk_write_time += b.shared_blk_write_time.get_millisec();
            e.counters.local_blk_read_time += b.local_blk_read_time.get_millisec();
            e.counters.local_blk_write_time += b.local_blk_write_time.get_millisec();
            e.counters.temp_blk_read_time += b.temp_blk_read_time.get_millisec();
            e.counters.temp_blk_write_time += b.temp_blk_write_time.get_millisec();
        }
        e.counters.usage += crate::usage_exec(total_time);
        if let Some(w) = walusage {
            e.counters.wal_records += w.wal_records;
            e.counters.wal_fpi += w.wal_fpi;
            e.counters.wal_bytes = e.counters.wal_bytes.wrapping_add(w.wal_bytes);
            e.counters.wal_buffers_full += w.wal_buffers_full;
        }
        e.counters.parallel_workers_to_launch += parallel_workers_to_launch as i64;
        e.counters.parallel_workers_launched += parallel_workers_launched as i64;

        shmem::spin_lock_release(&e.mutex);
    }

    lwlock_release(lock);
    // norm_query is dropped here (the C pfree after releasing the lock).
    drop(norm_query);
}

// ---------------------------------------------------------------------------
// entry_alloc / entry_dealloc.
// ---------------------------------------------------------------------------

/// `entry_alloc` (pg_stat_statements.c:2077). Caller holds the exclusive lock.
/// Returns a raw `*mut u8` key pointer (the `PgssEntry` starts at the key).
pub(crate) fn entry_alloc(
    key: &PgssHashKey,
    query_offset: usize,
    query_len: i32,
    encoding: i32,
    sticky: bool,
) -> *mut u8 {
    let pgss_hash = pgss_hash();

    // Make space if needed.
    while dynahash::hash_get_num_entries(pgss_hash) >= pgss_max() as i64 {
        entry_dealloc();
    }

    let (ptr, found) = match hash_search_raw(key, HASH_ENTER) {
        Ok(r) => r,
        Err(_) => return core::ptr::null_mut(),
    };
    if ptr.is_null() {
        return ptr;
    }

    if !found {
        let pgss = unsafe { pgss_ref() };
        let e = unsafe { entry_ref(ptr) };
        e.counters = Counters::zeroed();
        e.counters.usage = if sticky { pgss.cur_median_usage } else { USAGE_INIT };
        shmem::spin_lock_init(&e.mutex);
        e.query_offset = query_offset;
        e.query_len = query_len;
        e.encoding = encoding;
        e.stats_since = adt_datetime::timestamp::GetCurrentTimestamp();
        e.minmax_stats_since = e.stats_since;
    }
    ptr
}

/// `entry_dealloc` (pg_stat_statements.c:2135). Caller holds the exclusive lock.
fn entry_dealloc() {
    let pgss = unsafe { pgss_ref() };
    let pgss_hash = pgss_hash();

    let n = dynahash::hash_get_num_entries(pgss_hash);
    let mut entries: Vec<*mut PgssEntry> = Vec::with_capacity(n.max(0) as usize);

    let mut tottextlen: usize = 0;
    let mut nvalidtexts: usize = 0;

    let mut hash_seq = ::hash::hsearch::HASH_SEQ_STATUS::new();
    dynahash::hash_seq_init(&mut hash_seq, pgss_hash);
    loop {
        let ptr = match dynahash::hash_seq_search(&mut hash_seq) {
            Ok(p) => p,
            Err(_) => break,
        };
        if ptr.is_null() {
            break;
        }
        let e = unsafe { entry_ref(ptr) };
        entries.push(e as *mut PgssEntry);
        if e.counters.is_sticky() {
            e.counters.usage *= STICKY_DECREASE_FACTOR;
        } else {
            e.counters.usage *= USAGE_DECREASE_FACTOR;
        }
        if e.query_len >= 0 {
            tottextlen += e.query_len as usize + 1;
            nvalidtexts += 1;
        }
    }

    // Sort into increasing usage order.
    entries.sort_by(|a, b| {
        let la = unsafe { (**a).counters.usage };
        let lb = unsafe { (**b).counters.usage };
        la.partial_cmp(&lb).unwrap_or(core::cmp::Ordering::Equal)
    });

    let i = entries.len();
    if i > 0 {
        pgss.cur_median_usage = unsafe { (*entries[i / 2]).counters.usage };
    }
    pgss.mean_query_len = if nvalidtexts > 0 {
        tottextlen / nvalidtexts
    } else {
        crate::ASSUMED_LENGTH_INIT
    };

    let mut nvictims = (10i64).max(i as i64 * USAGE_DEALLOC_PERCENT / 100);
    nvictims = nvictims.min(i as i64);

    for victim in entries.iter().take(nvictims as usize) {
        let key = unsafe { &(**victim).key };
        let _ = hash_search_raw(key, HASH_REMOVE);
    }

    shmem::spin_lock_acquire(&pgss.mutex);
    pgss.stats.dealloc += 1;
    shmem::spin_lock_release(&pgss.mutex);
}

// ---------------------------------------------------------------------------
// entry_reset.
// ---------------------------------------------------------------------------

/// `entry_reset(userid, dbid, queryid, minmax_only)` (pg_stat_statements.c:2673).
pub(crate) fn entry_reset(
    userid: Oid,
    dbid: Oid,
    queryid: i64,
    minmax_only: bool,
) -> PgResult<i64> {
    use ::utils_error::ereport;
    use ::types_error::{ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR};

    if !shmem::is_initialized() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("pg_stat_statements must be loaded via \"shared_preload_libraries\"")
            .into_error());
    }

    let pgss = unsafe { pgss_ref() };
    let pgss_hash = pgss_hash();
    let lock = pgss.lock;

    lwlock_acquire(lock, true)?;
    let num_entries = dynahash::hash_get_num_entries(pgss_hash);
    let stats_reset = adt_datetime::timestamp::GetCurrentTimestamp();
    let mut num_remove: i64 = 0;

    let uid = userid;
    let did = dbid;

    if uid != 0 && did != 0 && queryid != 0 {
        // Fast path: reset both the non-top-level and top-level entries.
        for toplevel in [false, true] {
            let mut key = PgssHashKey {
                userid,
                dbid,
                queryid,
                toplevel,
                _pad: [0; 7],
            };
            let ptr = hash_search(&mut key, HASH_FIND);
            single_entry_reset(ptr, minmax_only, stats_reset, &mut num_remove);
        }
    } else if uid != 0 || did != 0 || queryid != 0 {
        let mut hash_seq = ::hash::hsearch::HASH_SEQ_STATUS::new();
        dynahash::hash_seq_init(&mut hash_seq, pgss_hash);
        loop {
            let ptr = match dynahash::hash_seq_search(&mut hash_seq) {
                Ok(p) => p,
                Err(_) => break,
            };
            if ptr.is_null() {
                break;
            }
            let e = unsafe { entry_ref(ptr) };
            if (uid == 0 || e.key.userid == uid)
                && (did == 0 || e.key.dbid == did)
                && (queryid == 0 || e.key.queryid == queryid)
            {
                single_entry_reset(ptr, minmax_only, stats_reset, &mut num_remove);
            }
        }
    } else {
        let mut hash_seq = ::hash::hsearch::HASH_SEQ_STATUS::new();
        dynahash::hash_seq_init(&mut hash_seq, pgss_hash);
        loop {
            let ptr = match dynahash::hash_seq_search(&mut hash_seq) {
                Ok(p) => p,
                Err(_) => break,
            };
            if ptr.is_null() {
                break;
            }
            single_entry_reset(ptr, minmax_only, stats_reset, &mut num_remove);
        }
    }

    // If all entries were removed, reset global stats + the texts file.
    if num_entries == num_remove {
        shmem::spin_lock_acquire(&pgss.mutex);
        pgss.stats.dealloc = 0;
        pgss.stats.stats_reset = stats_reset;
        shmem::spin_lock_release(&pgss.mutex);

        crate::qtext::reset_texts_file();
        pgss.extent = 0;
        crate::qtext::record_gc_qtexts();
    }

    lwlock_release(lock);
    Ok(stats_reset)
}

/// `SINGLE_ENTRY_RESET(e)` macro (pg_stat_statements.c:2651).
fn single_entry_reset(ptr: *mut u8, minmax_only: bool, stats_reset: i64, num_remove: &mut i64) {
    if ptr.is_null() {
        return;
    }
    let e = unsafe { entry_ref(ptr) };
    if minmax_only {
        for kind in 0..PGSS_NUMKIND {
            e.counters.max_time[kind] = 0.0;
            e.counters.min_time[kind] = 0.0;
        }
        e.minmax_stats_since = stats_reset;
    } else {
        let key = e.key;
        let _ = hash_search_raw(&key, HASH_REMOVE);
        *num_remove += 1;
    }
}

// ---------------------------------------------------------------------------
// dynahash key-pointer adapters.
// ---------------------------------------------------------------------------

fn key_bytes(key: &PgssHashKey) -> *const u8 {
    (key as *const PgssHashKey).cast::<u8>()
}

fn hash_search(key: &mut PgssHashKey, action: HASHACTION) -> *mut u8 {
    match hash_search_raw(key, action) {
        Ok((ptr, _)) => ptr,
        Err(_) => core::ptr::null_mut(),
    }
}

fn hash_search_raw(key: &PgssHashKey, action: HASHACTION) -> PgResult<(*mut u8, bool)> {
    dynahash::hash_search(pgss_hash(), key_bytes(key), action)
}

// ---------------------------------------------------------------------------
// LWLock helpers over the raw `LWLock *`.
// ---------------------------------------------------------------------------

fn lwlock_acquire(lock: *const types_storage::storage::LWLock, exclusive: bool) -> PgResult<()> {
    use ::lwlock::LWLockAcquire; use types_storage::storage::LWLockMode;
    // SAFETY: `lock` points into the live MAP_SHARED LWLock array.
    let lock = unsafe { &*lock };
    let mode = if exclusive {
        LWLockMode::LW_EXCLUSIVE
    } else {
        LWLockMode::LW_SHARED
    };
    LWLockAcquire(lock, mode, init_small_seams::my_proc_number::call())?;
    Ok(())
}

fn lwlock_release(lock: *const types_storage::storage::LWLock) {
    // SAFETY: `lock` points into the live MAP_SHARED LWLock array.
    let lock = unsafe { &*lock };
    let _ = ::lwlock::LWLockRelease(lock);
}

// ---------------------------------------------------------------------------
// CleanQuerytext (nodes/queryjumble.c) — confine attention to the relevant
// part of a (possibly multi-statement) source string and trim leading
// whitespace / semicolons. Ported here as it is not yet a public queryjumble
// API.
// ---------------------------------------------------------------------------

/// `CleanQuerytext(query, &location, &len)` (queryjumble.c). Returns the slice of
/// `query` the entry should represent, updating `*location`/`*len`.
fn clean_querytext<'a>(query: &'a [u8], location: &mut i32, len: &mut i32) -> &'a [u8] {
    // If the query is a portion of a multi-statement string, confine to it.
    let mut query = query;
    if *location >= 0 {
        debug_assert!(*location <= query.len() as i32);
        query = &query[*location as usize..];
        // Length of 0 (or less) means "rest of string".
        if *len <= 0 {
            *len = query.len() as i32;
        } else {
            debug_assert!(*len <= query.len() as i32);
        }
    } else {
        // If query location is unknown, distrust query_len as well.
        *location = 0;
        *len = query.len() as i32;
    }

    // Discard leading and trailing whitespace, too. Use scanner_isspace() to
    // agree with scanner.
    let mut start = 0usize;
    let mut l = *len as usize;
    while l > 0 && scanner_isspace(query[start]) {
        start += 1;
        *location += 1;
        l -= 1;
    }
    while l > 0 && scanner_isspace(query[start + l - 1]) {
        l -= 1;
    }
    *len = l as i32;
    &query[start..start + l]
}

/// `scanner_isspace(ch)` (scansup.c) — the scanner's notion of whitespace.
fn scanner_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}
