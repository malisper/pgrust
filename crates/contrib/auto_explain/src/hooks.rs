//! The executor hook bodies (auto_explain.c). Enter/leave pairs replace C's
//! PG_TRY/PG_FINALLY nesting bookkeeping, exactly like pg_stat_statements.

use std::cell::Cell;

use types_core::instrument::{
    Instrumentation, INSTRUMENT_ALL, INSTRUMENT_BUFFERS, INSTRUMENT_ROWS, INSTRUMENT_TIMER,
    INSTRUMENT_WAL,
};
use types_error::{ErrorLevel, ErrorLocation, WARNING};
use types_portal::{ParamListHandle, QueryDescHandle};
use types_slot::EXEC_FLAG_EXPLAIN_ONLY;

use crate::gucs;

thread_local! {
    /// C: static int nesting_level (ExecutorRun/Finish depth).
    static NESTING_LEVEL: Cell<i32> = const { Cell::new(0) };
    /// C: static bool current_query_sampled.
    static CURRENT_QUERY_SAMPLED: Cell<bool> = const { Cell::new(false) };
}

fn nesting_level() -> i32 {
    NESTING_LEVEL.get()
}

fn nesting_add(d: i32) {
    NESTING_LEVEL.set(NESTING_LEVEL.get() + d);
}

/// C macro `auto_explain_enabled()`.
fn auto_explain_enabled() -> bool {
    gucs::log_min_duration() >= 0
        && (nesting_level() == 0 || gucs::log_nested_statements())
        && CURRENT_QUERY_SAMPLED.get()
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

/// `explain_ExecutorStart`.
pub(crate) fn explain_executor_start(h: QueryDescHandle) {
    // At the start of each top-level statement, decide whether to sample it.
    // In a parallel worker do nothing (the leader reports).
    if nesting_level() == 0 {
        let sampled = if gucs::log_min_duration() >= 0 && !parallel::IsParallelWorker() {
            pg_prng::global_prng(|p| p.next_f64()) < gucs::sample_rate()
        } else {
            false
        };
        CURRENT_QUERY_SAMPLED.set(sampled);
    }

    if !auto_explain_enabled() {
        return;
    }

    execmain::with_qd(h, |qd| {
        // Enable per-node instrumentation iff log_analyze is required.
        // C skips this under EXEC_FLAG_EXPLAIN_ONLY; eflags are not visible
        // pre-start here, so an EXPLAIN(-only) statement allocates unused
        // node instrumentation — output parity is restored at log time by the
        // es_top_eflags check in explain_executor_end.
        if gucs::log_analyze() {
            if gucs::log_timing() {
                qd.instrument_options |= INSTRUMENT_TIMER;
            } else {
                qd.instrument_options |= INSTRUMENT_ROWS;
            }
            if gucs::log_buffers() {
                qd.instrument_options |= INSTRUMENT_BUFFERS;
            }
            if gucs::log_wal() {
                qd.instrument_options |= INSTRUMENT_WAL;
            }
        }

        // Track total elapsed time (C arms after standard_ExecutorStart; the
        // executor reads totaltime only from ExecutorRun on, so pre-start is
        // equivalent). Always a fresh Instrumentation: a rearmed parked
        // portal reuses the QueryDesc and must not accumulate onto the
        // previous execution. pg_stat_statements' hook may replace this with
        // its own fresh instance (both are INSTRUMENT_ALL) — same sharing as
        // C's if-NULL alloc, fresh either way.
        let mut instr = Box::new(Instrumentation::default());
        instrument::instr_init(&mut instr, INSTRUMENT_ALL);
        qd.totaltime = Some(instr);
    });
}

/// `explain_ExecutorRun` (enter half).
pub(crate) fn explain_executor_run(_h: QueryDescHandle) {
    nesting_add(1);
}

/// `explain_ExecutorRun` (PG_FINALLY half).
pub(crate) fn explain_executor_run_leave(_h: QueryDescHandle) {
    nesting_add(-1);
}

/// `explain_ExecutorFinish` (enter half).
pub(crate) fn explain_executor_finish(_h: QueryDescHandle) {
    nesting_add(1);
}

/// `explain_ExecutorFinish` (PG_FINALLY half).
pub(crate) fn explain_executor_finish_leave(_h: QueryDescHandle) {
    nesting_add(-1);
}

struct LogJob {
    msec: f64,
    source_text: &'static str,
    instrument_options: i32,
    explain_only: bool,
    params: ParamListHandle,
}

/// `explain_ExecutorEnd`: log the plan if the duration threshold was crossed.
/// Split in two phases: the decision + snapshot runs inside `with_qd`, the
/// EXPLAIN walk runs outside it (ExplainPrintPlan re-enters the QueryDesc
/// registry through `es.qd`, which would double-borrow).
pub(crate) fn explain_executor_end(h: QueryDescHandle) {
    let job = execmain::with_qd(h, |qd| -> Option<LogJob> {
        if qd.totaltime.is_none() || !auto_explain_enabled() {
            return None;
        }

        // Make sure stats accumulation is done (several hook levels may all
        // do this; instr_end_loop on a stopped node is idempotent).
        let t = qd.totaltime.as_deref_mut().expect("checked above");
        instrument::instr_end_loop(t);
        let msec = t.total * 1000.0;
        if msec < f64::from(gucs::log_min_duration()) {
            return None;
        }

        let explain_only = match qd.exec.as_ref() {
            Some(exec) => exec.with(|d| d.estate.es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY != 0),
            None => false,
        };

        Some(LogJob {
            msec,
            source_text: qd.source_text(),
            instrument_options: qd.instrument_options,
            explain_only,
            params: qd.params,
        })
    });

    if let Some(job) = job {
        // Explain-build failures must not take down the query at ExecutorEnd:
        // the tap has no error channel (C would abort the statement here; a
        // failure in plan rendering is the only divergence surface).
        if let Err(e) = log_plan(h, &job) {
            let _ = elog::ereport(WARNING)
                .errmsg(format!("auto_explain: could not log plan: {}", e.message()))
                .finish(loc("explain_ExecutorEnd"));
        }
    }
}

fn log_plan(h: QueryDescHandle, job: &LogJob) -> types_error::PgResult<()> {
    use explain::{
        ExplainBeginOutput, ExplainCloseGroup, ExplainEndOutput, ExplainOpenGroup,
        ExplainPropertyText,
    };

    // C switches to the per-query context; a private context scoped to this
    // call gives the same discard-at-end guarantee.
    let ctx = mcx::MemoryContext::new("auto_explain log");
    let mcx = ctx.mcx();

    let mut es = explain::NewExplainState(mcx)?;
    // C: es->analyze = (queryDesc->instrument_options && auto_explain_log_analyze).
    // The explain_only leg reproduces C's EXEC_FLAG_EXPLAIN_ONLY guard at
    // ExecutorStart (see explain_executor_start).
    es.analyze = job.instrument_options != 0 && gucs::log_analyze() && !job.explain_only;
    es.verbose = gucs::log_verbose();
    es.buffers = es.analyze && gucs::log_buffers();
    es.wal = es.analyze && gucs::log_wal();
    es.timing = es.analyze && gucs::log_timing();
    es.summary = es.analyze;
    es.format = match gucs::log_format() {
        guc_tables::consts::EXPLAIN_FORMAT_XML => explain::EXPLAIN_FORMAT_XML,
        guc_tables::consts::EXPLAIN_FORMAT_JSON => explain::EXPLAIN_FORMAT_JSON,
        guc_tables::consts::EXPLAIN_FORMAT_YAML => explain::EXPLAIN_FORMAT_YAML,
        _ => explain::EXPLAIN_FORMAT_TEXT,
    };
    es.settings = gucs::log_settings();

    ExplainBeginOutput(&mut es);

    // ExplainQueryText (explain.c): the source text property.
    ExplainPropertyText("Query Text", job.source_text, &mut es);

    // ExplainQueryParameters (explain.c). The fetch-hooked bail-out is C's
    // params.c BuildParamLogString: "No work if the param fetch hook is in
    // use" — PL-driven nested statements (plpgsql/sql-function variables)
    // never log parameters.
    let maxlen = gucs::log_parameter_max_length();
    if !job.params.is_null()
        && !types_portal::params::is_fetch_hooked(job.params)
        && types_portal::params::num_params(job.params) > 0
        && maxlen != 0
    {
        let s = types_portal::params::with(job.params, |p| {
            nodes_params::build_param_log_string(mcx, p, None, maxlen)
        })?;
        if let Some(s) = s {
            ExplainPropertyText("Query Parameters", &s, &mut es);
        }
    }

    // SAFETY: plannedstmt() is 'static-viewed under create_query_desc's
    // retention contract (live until free_query_desc; the end tap precedes
    // standard_executor_end). PlannedStmt is invariant in its lifetime
    // (interior Cells), so the shrink to this frame's 'mcx needs an explicit
    // transmute (execmain's unify_call_lifetime precedent).
    let pstmt: &types_nodes::plannodes::PlannedStmt<'_> =
        unsafe { core::mem::transmute(execmain::with_qd(h, |qd| qd.plannedstmt())) };
    es.qd = h;
    let r = explain::ExplainPrintPlan(mcx, &mut es, pstmt);
    es.qd = QueryDescHandle::NULL;
    r?;

    if es.analyze && gucs::log_triggers() {
        // ExplainPrintTriggers: same shape core EXPLAIN uses — no CREATE
        // TRIGGER path exists, so report_triggers output is provably empty;
        // non-text formats still print the empty Triggers group (as C does
        // for a query that fired no triggers).
        ExplainOpenGroup("Triggers", Some("Triggers"), false, &mut es);
        ExplainCloseGroup("Triggers", Some("Triggers"), false, &mut es);
    }

    // ExplainPrintJITSummary is skipped: provably output-free in this build
    // (no LLVM provider; ExplainPrintJIT bails with zero created functions),
    // matching C run without the llvmjit package.

    ExplainEndOutput(&mut es);

    let mut text = String::from_utf8_lossy(es.str.as_bytes()).into_owned();
    // Remove last line break (C: es->str->data[--len] = '\0').
    if text.ends_with('\n') {
        text.pop();
    }
    // Fix JSON to output an object (C flips the array brackets in place).
    if es.format == explain::EXPLAIN_FORMAT_JSON {
        // SAFETY-free byte patch: '[' and ']' are single-byte ASCII.
        let bytes = unsafe { text.as_bytes_mut() };
        if bytes.first() == Some(&b'[') {
            bytes[0] = b'{';
        }
        let last = bytes.len() - 1;
        if bytes.get(last) == Some(&b']') {
            bytes[last] = b'}';
        }
    }

    // C relies on log_line context to identify the statement; errhidestmt
    // avoids duplicating the query text.
    elog::ereport(ErrorLevel(gucs::log_level()))
        .errmsg(format!("duration: {:.3} ms  plan:\n{}", job.msec, text))
        .errhidestmt(true)
        .finish(loc("explain_ExecutorEnd"))
}
