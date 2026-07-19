//! The hook bodies pg_stat_statements installs on the tap seams. C wraps the
//! standard calls in PG_TRY/PG_FINALLY; the enter/leave tap pairs carry the
//! same guarantee (leave fires on the error path with ok=false).

use std::cell::RefCell;

use types_core::instrument::{
    instr_time, BufferUsage, Instrumentation, WalUsage, INSTRUMENT_ALL,
};
use types_nodes::parsenodes::Query;
use types_nodes::plannodes::PlannedStmt;
use types_nodes::NodeTag;
use types_portal::{QueryCompletion, QueryDescHandle};
use utility::consts::{
    CMDTAG_COPY, CMDTAG_FETCH, CMDTAG_REFRESH_MATERIALIZED_VIEW, CMDTAG_SELECT,
};

use crate::store::pgss_store;
use crate::{gucs, nesting_add, nesting_level, pgss_enabled, PGSS_EXEC, PGSS_PLAN};

struct Snapshot {
    start: instr_time,
    bufusage_start: BufferUsage,
    walusage_start: WalUsage,
}

impl Snapshot {
    fn take() -> Self {
        Snapshot {
            bufusage_start: instrument::pg_buffer_usage(),
            walusage_start: instrument::pg_wal_usage(),
            start: instrument::instr_time_current(),
        }
    }

    fn elapsed_ms(&self) -> f64 {
        (instrument::instr_time_current().ticks - self.start.ticks) as f64 / 1_000_000.0
    }

    fn buf_diff(&self) -> BufferUsage {
        let mut d = BufferUsage::default();
        instrument::buffer_usage_accum_diff(&mut d, &instrument::pg_buffer_usage(), &self.bufusage_start);
        d
    }

    fn wal_diff(&self) -> WalUsage {
        let mut d = WalUsage::default();
        instrument::wal_usage_accum_diff(&mut d, &instrument::pg_wal_usage(), &self.walusage_start);
        d
    }
}

struct PlanFrame {
    tracked: bool,
    snap: Option<Snapshot>,
}

struct UtilFrame {
    tracked: bool,
    bump: bool,
    saved_query_id: i64,
    stmt_location: i32,
    stmt_len: i32,
    snap: Option<Snapshot>,
}

thread_local! {
    static PLAN_FRAMES: RefCell<Vec<PlanFrame>> = const { RefCell::new(Vec::new()) };
    static UTIL_FRAMES: RefCell<Vec<UtilFrame>> = const { RefCell::new(Vec::new()) };
}

/// `pgss_post_parse_analyze`.
pub(crate) fn pgss_post_parse_analyze(
    query: &mut Query<'_>,
    jstate: &queryjumble::JumbleState<'_>,
    source_text: &str,
) {
    if !pgss_enabled(nesting_level()) {
        return;
    }

    // EXECUTE accumulates onto the underlying PREPARE; clear its queryId
    // (only while tracking utility statements, as in C).
    if let Some(u) = query.utilityStmt {
        if gucs::pgss_track_utility() && u.node_tag() == NodeTag::T_ExecuteStmt {
            query.queryId = 0;
            return;
        }
    }

    if !jstate.clocations.is_empty() {
        pgss_store(
            source_text,
            query.queryId,
            query.stmt_location,
            query.stmt_len,
            None,
            0.0,
            0,
            &BufferUsage::default(),
            &WalUsage::default(),
            Some(jstate),
            0,
            0,
        );
    }
}

/// `pgss_planner` (enter half).
pub(crate) fn pgss_planner_enter(query_id: i64) {
    let tracked =
        pgss_enabled(nesting_level()) && gucs::pgss_track_planning() && query_id != 0;
    PLAN_FRAMES.with(|f| {
        f.borrow_mut().push(PlanFrame { tracked, snap: tracked.then(Snapshot::take) })
    });
    nesting_add(1);
}

/// `pgss_planner` (leave half; ok=false is C's PG_FINALLY error path).
pub(crate) fn pgss_planner_leave(
    ok: bool,
    query_id: i64,
    stmt_location: i32,
    stmt_len: i32,
    query_string: &str,
) {
    nesting_add(-1);
    let frame = PLAN_FRAMES
        .with(|f| f.borrow_mut().pop())
        .expect("pgss_planner_leave without enter");
    if !(frame.tracked && ok) {
        return;
    }
    let snap = frame.snap.expect("tracked plan frame carries a snapshot");
    pgss_store(
        query_string,
        query_id,
        stmt_location,
        stmt_len,
        Some(PGSS_PLAN),
        snap.elapsed_ms(),
        0,
        &snap.buf_diff(),
        &snap.wal_diff(),
        None,
        0,
        0,
    );
}

/// `pgss_ExecutorStart`: arm queryDesc->totaltime for tracked statements.
pub(crate) fn pgss_executor_start(h: QueryDescHandle) {
    execmain::with_qd(h, |qd| {
        if pgss_enabled(nesting_level()) && qd.plannedstmt().queryId.get() != 0 {
            // Fresh per execution: a rearmed parked portal reuses the
            // QueryDesc, so overwrite rather than C's if-NULL alloc.
            let mut instr = Box::new(Instrumentation::default());
            instrument::instr_init(&mut instr, INSTRUMENT_ALL);
            qd.totaltime = Some(instr);
        }
        // Not tracked: leave qd.totaltime alone. C's if-NULL alloc never
        // clears, and another exec_hooks consumer (auto_explain) may have
        // armed it this execution; a stale rearm leftover is harmless — every
        // consumer's end hook re-checks its own enablement.
    });
}

pub(crate) fn pgss_executor_run(_h: QueryDescHandle) {
    nesting_add(1);
}

pub(crate) fn pgss_executor_run_leave(_h: QueryDescHandle) {
    nesting_add(-1);
}

pub(crate) fn pgss_executor_finish(_h: QueryDescHandle) {
    nesting_add(1);
}

pub(crate) fn pgss_executor_finish_leave(_h: QueryDescHandle) {
    nesting_add(-1);
}

/// `pgss_ExecutorEnd`.
pub(crate) fn pgss_executor_end(h: QueryDescHandle) {
    execmain::with_qd(h, |qd| {
        let pstmt = qd.plannedstmt();
        let query_id = pstmt.queryId.get();
        if query_id == 0 || qd.totaltime.is_none() || !pgss_enabled(nesting_level()) {
            return;
        }
        let (stmt_location, stmt_len) = (pstmt.stmt_location, pstmt.stmt_len);
        let source_text = qd.source_text();
        let (rows, pw_to_launch, pw_launched) = match qd.exec.as_ref() {
            Some(exec) => exec.with(|d| {
                (
                    d.estate.es_total_processed,
                    d.estate.es_parallel_workers_to_launch,
                    d.estate.es_parallel_workers_launched,
                )
            }),
            None => (0, 0, 0),
        };
        let t = qd.totaltime.as_deref_mut().expect("checked above");
        instrument::instr_end_loop(t);
        pgss_store(
            source_text,
            query_id,
            stmt_location,
            stmt_len,
            Some(PGSS_EXEC),
            t.total * 1000.0,
            rows,
            &t.bufusage,
            &t.walusage,
            None,
            i64::from(pw_to_launch),
            i64::from(pw_launched),
        );
    });
}

/// `pgss_ProcessUtility` (enter half): zero the pstmt queryId in place and
/// open the timing frame.
pub(crate) fn pgss_process_utility_enter(pstmt: &PlannedStmt<'_>) {
    let saved_query_id = pstmt.queryId.get();
    let enabled = gucs::pgss_track_utility() && pgss_enabled(nesting_level());
    if enabled {
        pstmt.queryId.set(0);
    }
    let tag = pstmt.utilityStmt.map(|u| u.node_tag());
    let exempt = matches!(tag, Some(NodeTag::T_ExecuteStmt) | Some(NodeTag::T_PrepareStmt));
    let tracked = enabled && !exempt;
    UTIL_FRAMES.with(|f| {
        f.borrow_mut().push(UtilFrame {
            tracked,
            bump: !exempt,
            saved_query_id,
            stmt_location: pstmt.stmt_location,
            stmt_len: pstmt.stmt_len,
            snap: tracked.then(Snapshot::take),
        })
    });
    if !exempt {
        nesting_add(1);
    }
}

/// `pgss_ProcessUtility` (leave half). Must not touch pstmt (C: the tree may
/// have been freed by ROLLBACK); everything needed was saved at enter.
pub(crate) fn pgss_process_utility_leave(
    ok: bool,
    source_text: &str,
    qc: Option<&QueryCompletion>,
) {
    let frame = UTIL_FRAMES
        .with(|f| f.borrow_mut().pop())
        .expect("pgss_process_utility_leave without enter");
    if frame.bump {
        nesting_add(-1);
    }
    if !(frame.tracked && ok) {
        return;
    }
    let rows = match qc {
        Some(qc)
            if qc.commandTag == CMDTAG_COPY
                || qc.commandTag == CMDTAG_FETCH
                || qc.commandTag == CMDTAG_SELECT
                || qc.commandTag == CMDTAG_REFRESH_MATERIALIZED_VIEW =>
        {
            qc.nprocessed
        }
        _ => 0,
    };
    let snap = frame.snap.expect("tracked utility frame carries a snapshot");
    pgss_store(
        source_text,
        frame.saved_query_id,
        frame.stmt_location,
        frame.stmt_len,
        Some(PGSS_EXEC),
        snap.elapsed_ms(),
        rows,
        &snap.buf_diff(),
        &snap.wal_diff(),
        None,
        0,
        0,
    );
}
