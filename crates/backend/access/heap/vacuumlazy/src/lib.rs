//! vacuumlazy.c phases I (scan/prune/freeze), II (index vacuum via
//! ambulkdelete), III (mark LP_UNUSED), and end-of-vacuum rel truncation,
//! single-table lane. Loud named panics: eager scanning. C divergences
//! (recorded): the read stream is collapsed to sync per-block reads (bitmap
//! precedent).
//!
//! Phase-I MORSELIZATION (docs/design/vacuum-morsels.md, inc-2): behind
//! the morsel arm (default ON since train-21; `PGRUST_RUNTIME_VACUUM=0` kills, requires the runtime master switch) the
//! heap scan runs as SCAN task sets on the morsel runtime — see [`morsels`].
//! The per-block bodies below are shared verbatim between the serial arm and
//! the morsel workers: they take a read-only [`ScanEnv`] + the order-free
//! fold block [`ScanFolds`] + a dead-TID sink instead of the whole
//! LVRelState, so the same code folds into the serial state or a worker's
//! `VacScanLocal` (anti-goal zero: no behavior change to WHAT vacuum does).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::commands_vacuum::{
    vac_bulkdel_one_index, vac_cleanup_one_index, vac_close_indexes, vac_estimate_reltuples,
    vac_open_indexes, vacuum_delay_point, vacuum_get_cutoffs, vacuum_xid_failsafe_check,
    SetVacuumFailsafeActive, VacuumFailsafeActive,
};
use ::nbtree::IndexVacuumInfo;
use ::types_nbtree::IndexBulkDeleteResult;
use ::types_rel::lock::{NoLock, RowExclusiveLock};
use ::types_rel::Relation;
use ::mcx::{Mcx, PgVec};
use ::tableam_vocab::{
    VacOptValue, VacuumCutoffs, VacuumParams, VACOPT_DISABLE_PAGE_SKIPPING, VACOPT_VERBOSE,
};
use ::types_core::xact::{
    InvalidTransactionId, TransactionIdIsNormal, TransactionIdIsValid, TransactionIdPrecedes,
};
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, GlobalVisStateHandle, InvalidBlockNumber, OffsetNumber, Size,
    TransactionId, BLCKSZ,
};
use ::types_error::PgResult;
use ::types_rel::RelationData;
use ::types_snapshot::HTSV_Result;
use ::types_storage::buf::BufferAccessStrategy;
use ::types_storage::bufpage::{
    MaxHeapTuplesPerPage, PageMut, PageRef, SizeOfPageHeaderData,
};
use ::types_storage::ReadBufferMode;
use ::tidstore::TidStore;
use ::types_tuple::{
    FirstOffsetNumber, HeapTupleData, InvalidOffsetNumber, ItemPointerData, MaxOffsetNumber,
};

use ::backend_progress::progress::*;
use ::backend_progress::{
    pgstat_progress_end_command, pgstat_progress_start_command,
    pgstat_progress_update_multi_param, pgstat_progress_update_param, PROGRESS_COMMAND_VACUUM,
};
use ::bufmgr_seams::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};
use ::pruneheap::{
    heap_page_prune_and_freeze, log_heap_prune_and_freeze, PruneFreezeResult, PruneReason,
    HEAP_PAGE_PRUNE_FREEZE, HEAP_PAGE_PRUNE_MARK_UNUSED_NOW,
};
use ::visibilitymap::{
    visibilitymap_clear, visibilitymap_count, visibilitymap_get_status, visibilitymap_pin,
    visibilitymap_set, vm_all_frozen, VmBuffer, VISIBILITYMAP_ALL_FROZEN,
    VISIBILITYMAP_ALL_VISIBLE, VISIBILITYMAP_VALID_BITS,
};

const SKIP_PAGES_THRESHOLD: BlockNumber = 32;
const FAILSAFE_EVERY_PAGES: BlockNumber = ((4u64 * 1024 * 1024 * 1024) / BLCKSZ as u64) as BlockNumber;
const VACUUM_FSM_EVERY_PAGES: BlockNumber = ((8u64 * 1024 * 1024 * 1024) / BLCKSZ as u64) as BlockNumber;
const REL_TRUNCATE_MINIMUM: BlockNumber = 1000;
const REL_TRUNCATE_FRACTION: BlockNumber = 16;
const BYPASS_THRESHOLD_PAGES: f64 = 0.02;

/// Read-only inputs of the phase-I per-block bodies. One per scanning
/// participant: the serial arm builds it from LVRelState, each morsel worker
/// from its own opened relation + the generation's published cutoffs
/// (workers derive their OWN vistest at bind — doc §5.1; every worker
/// horizon >= the leader's, within C's envelope).
pub(crate) struct ScanEnv<'a, 'mcx> {
    pub(crate) rel: &'a RelationData<'mcx>,
    pub(crate) cutoffs: &'a VacuumCutoffs,
    pub(crate) vistest: GlobalVisStateHandle,
    pub(crate) aggressive: bool,
    pub(crate) nindexes: usize,
}

/// The phase-I fold block: the LVRelState counters the per-block bodies
/// write, every one an order-insensitive-exact fold (sum / XID-min / max —
/// doc §3.2), homed on the gated vacuum_morsels::ScanCounters unit so the
/// serial arm and the morsel workers fold IDENTICAL state. `offnum` is C's
/// error-context bookkeeping (vacrel->offnum), participant-local.
pub(crate) struct ScanFolds {
    pub(crate) counters: ::vacuum_morsels::ScanCounters,
    pub(crate) offnum: OffsetNumber,
}

/// Dead-TID sink of the per-block bodies: the serial arm feeds the round
/// TidStore (dead_items_add), morsel workers append VacScanLocal runs +
/// shared byte accounting (doc §3.2).
pub(crate) type DeadSink<'x> = &'x mut dyn FnMut(BlockNumber, &[OffsetNumber]) -> PgResult<()>;

pub struct LVRelState<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    rel: &'a RelationData<'mcx>,
    indrels: ::mcx::PgVec<'mcx, Relation<'mcx>>,
    indstats: ::mcx::PgVec<'mcx, Option<IndexBulkDeleteResult>>,
    nindexes: usize,
    bstrategy: BufferAccessStrategy,

    aggressive: bool,
    skipwithvm: bool,
    // VACUUM VERBOSE: client-visible INFO instead of DEBUG2/LOG.
    verbose: bool,
    consider_bypass_optimization: bool,
    do_index_vacuuming: bool,
    do_index_cleanup: bool,
    do_rel_truncate: bool,

    cutoffs: VacuumCutoffs,
    vistest: GlobalVisStateHandle,
    skippedallvis: bool,
    /// §5.2 fail-closed coverage guard (morsel arm only): a scan hole was
    /// detected — suppress relfrozenxid/relminmxid advancement.
    coverage_hole: bool,

    rel_pages: BlockNumber,
    // VACUUM VERBOSE / autovacuum-log truncation accounting (phase IV is
    // serial; not part of the order-free folds).
    removed_pages: BlockNumber,

    /// The order-free phase-I folds (incl. NewRelfrozenXid/NewRelminMxid
    /// trackers and vacrel->offnum) — see [`ScanFolds`].
    folds: ScanFolds,

    // Option only so phase III can split the borrow (take/put-back); always
    // Some between dead_items_alloc and dead_items_cleanup.
    dead_items: Option<TidStore>,
    dead_items_info: VacDeadItemsInfo,
    pvs: Option<vacuumparallel::ParallelVacuumState>,

    num_index_scans: i64,

    new_rel_tuples: f64,
    new_live_tuples: f64,

    current_block: BlockNumber,
    next_unskippable_block: BlockNumber,
    next_unskippable_allvis: bool,
    next_unskippable_vmbuffer: VmBuffer,
}

/// GL-M41-2 per-phase wall clocks (trace-gated, PGRUST_VACUUM_TRACE=1):
/// leader-thread TLS accumulators, reset per heap_vacuum_rel, emitted as ONE
/// `vacuum-phases:` summary line at the end. Every phase also snapshots the
/// leader's TLS WalUsage so the summary carries leader-side WAL per phase.
/// Observability only — never consulted behaviorally (DST: wall Instants,
/// the ScanPhaseClock precedent).
pub(crate) mod phase_trace {
    use std::cell::RefCell;

    pub const SCAN: usize = 0;
    pub const IDXBULK: usize = 1;
    pub const REAP: usize = 2;
    pub const IDXCLEAN: usize = 3;
    pub const FSM: usize = 4;
    pub const PVEND: usize = 5;
    pub const TRUNC: usize = 6;
    pub const N: usize = 7;

    #[derive(Clone, Copy, Default)]
    pub struct Acc {
        pub ns: u64,
        pub calls: u32,
        pub wal_records: i64,
        pub wal_bytes: u64,
    }

    thread_local! {
        static ACC: RefCell<[Acc; N]> = const { RefCell::new([Acc { ns: 0, calls: 0, wal_records: 0, wal_bytes: 0 }; N]) };
    }

    pub fn reset() {
        ACC.with(|a| *a.borrow_mut() = [Acc::default(); N]);
    }

    pub fn time<T>(ph: usize, f: impl FnOnce() -> T) -> T {
        let t0 = std::time::Instant::now();
        let w0 = ::instrument::pg_wal_usage();
        let r = f();
        let w1 = ::instrument::pg_wal_usage();
        let ns = t0.elapsed().as_nanos() as u64;
        ACC.with(|a| {
            let mut a = a.borrow_mut();
            a[ph].ns += ns;
            a[ph].calls += 1;
            a[ph].wal_records += w1.wal_records - w0.wal_records;
            a[ph].wal_bytes += w1.wal_bytes.wrapping_sub(w0.wal_bytes);
        });
        r
    }

    pub fn get(ph: usize) -> Acc {
        ACC.with(|a| a.borrow()[ph])
    }
}

pub fn heap_vacuum_rel<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'mcx>,
    params: &VacuumParams,
    bstrategy: BufferAccessStrategy,
) -> PgResult<()> {
    let trace_t0 = std::time::Instant::now();
    phase_trace::reset();
    let verbose = params.options & VACOPT_VERBOSE != 0;
    let instrument_vac = verbose
        || (miscinit::GetMyBackendType() == ::types_core::BackendType::AutovacWorker
            && params.log_min_duration >= 0);
    let ru0 = if instrument_vac { Some(pg_rusage::pg_rusage_init()) } else { None };
    let startwalusage = ::instrument::pg_wal_usage();
    let startbufferusage = ::instrument::pg_buffer_usage();
    debug_assert!(params.index_cleanup != VacOptValue::Unspecified);
    debug_assert!(!matches!(params.truncate, VacOptValue::Unspecified | VacOptValue::Auto));

    let starttime = timestamp_seams::get_current_timestamp::call();

    pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM, rel.rd_id);

    let indrels = vac_open_indexes(mcx, rel, RowExclusiveLock)?;
    let nindexes = indrels.len();
    let mut indstats = ::mcx::PgVec::with_capacity_in(nindexes, mcx);
    for _ in 0..nindexes {
        indstats.push(None);
    }

    SetVacuumFailsafeActive(false);
    let mut do_index_vacuuming = true;
    let mut do_index_cleanup = true;
    let mut consider_bypass_optimization = true;
    match params.index_cleanup {
        VacOptValue::Disabled => {
            do_index_vacuuming = false;
            do_index_cleanup = false;
        }
        VacOptValue::Enabled => consider_bypass_optimization = false,
        _ => debug_assert!(params.index_cleanup == VacOptValue::Auto),
    }

    let (mut aggressive, cutoffs) = vacuum_get_cutoffs(rel, params)?;
    let rel_pages =
        bufmgr_seams::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)?;
    let orig_rel_pages = rel_pages;
    let vistest = procarray_seams::global_vis_test_for::call(rel);

    let mut skipwithvm = true;
    if params.options & VACOPT_DISABLE_PAGE_SKIPPING != 0 {
        aggressive = true;
        skipwithvm = false;
    }

    // C divergence (recorded): heap_vacuum_eager_scan_setup is elided; eager
    // scanning stays disabled (find_next_unskippable_block skips all-visible
    // pages exactly as a normal vacuum with the failure cap exhausted).

    let mut vacrel = LVRelState {
        mcx,
        rel,
        indrels,
        indstats,
        nindexes,
        bstrategy,
        aggressive,
        skipwithvm,
        verbose,
        consider_bypass_optimization,
        do_index_vacuuming,
        do_index_cleanup,
        do_rel_truncate: params.truncate != VacOptValue::Disabled,
        cutoffs,
        vistest,
        skippedallvis: false,
        coverage_hole: false,
        rel_pages,
        removed_pages: 0,
        folds: ScanFolds {
            // Trackers start at the removal cutoffs; pruning ratchets them
            // back to the oldest extant XID/MXID.
            counters: ::vacuum_morsels::ScanCounters::seed(cutoffs.OldestXmin, cutoffs.OldestMxact),
            offnum: InvalidOffsetNumber,
        },
        dead_items: None,
        dead_items_info: VacDeadItemsInfo { max_bytes: 0, num_items: 0 },
        pvs: None,
        num_index_scans: 0,
        new_rel_tuples: 0.0,
        new_live_tuples: 0.0,
        current_block: InvalidBlockNumber,
        next_unskippable_block: InvalidBlockNumber,
        next_unskippable_allvis: false,
        next_unskippable_vmbuffer: VmBuffer::new(),
    };

    // C snapshots db/namespace/rel names up front for instrumentation (the
    // error-context callback itself is elided in this port).
    let (dbname, relnamespace, relname) = if instrument_vac {
        let dbname = dbcommands_seams::get_database_name::call(
            init_small::globals::MyDatabaseId(),
        )?
        .unwrap_or_default();
        let nspname = syscache_seams::pg_namespace_nspname::call(rel.rd_rel.relnamespace)?
            .map(|n| String::from_utf8_lossy(n.name_str()).into_owned())
            .unwrap_or_default();
        (dbname, nspname, rel.name().to_string())
    } else {
        (String::new(), String::new(), String::new())
    };

    if verbose {
        // C: aggressiveness gets its own dedicated VACUUM VERBOSE ereport.
        elog::ereport(::types_error::INFO)
            .errmsg(format!(
                "{} \"{}.{}.{}\"",
                if vacrel.aggressive { "aggressively vacuuming" } else { "vacuuming" },
                dbname, relnamespace, relname
            ))
            .finish(::types_error::ErrorLocation::new(
                "src/backend/access/heap/vacuumlazy.c",
                815,
                "heap_vacuum_rel",
            ))?;
    }

    lazy_check_wraparound_failsafe(&mut vacrel)?;
    dead_items_alloc(&mut vacrel, params.nworkers)?;

    let trace_setup_ns = trace_t0.elapsed().as_nanos() as u64;
    phase_trace::time(phase_trace::SCAN, || lazy_scan_heap(&mut vacrel, mcx, params.nworkers))?;

    // dead_items_cleanup: ends parallel mode (copying worker stats out first),
    // then drops the tidstore.
    if let Some(pvs) = vacrel.pvs.take() {
        phase_trace::time(phase_trace::PVEND, || {
            vacuumparallel::parallel_vacuum_end(pvs, &mut vacrel.indstats)
        })?;
    }
    debug_assert!(!xact::IsInParallelMode());
    vacrel.dead_items = None;

    if vacrel.do_index_cleanup {
        update_relstats_all_indexes(&mut vacrel)?;
    }

    let indrels = core::mem::replace(&mut vacrel.indrels, ::mcx::PgVec::new_in(mcx));
    vac_close_indexes(indrels, NoLock)?;

    if should_attempt_truncation(&vacrel) {
        phase_trace::time(phase_trace::TRUNC, || lazy_truncate_heap(&mut vacrel))?;
    }

    pgstat_progress_update_param(PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_FINAL_CLEANUP);

    // Aggressive VACUUMs must reach FreezeLimit/MultiXactCutoff.
    debug_assert!(
        vacrel.folds.counters.NewRelfrozenXid == vacrel.cutoffs.OldestXmin
            || ::types_core::xact::TransactionIdPrecedesOrEquals(
                if vacrel.aggressive { vacrel.cutoffs.FreezeLimit } else { vacrel.cutoffs.relfrozenxid },
                vacrel.folds.counters.NewRelfrozenXid
            )
    );
    debug_assert!(
        vacrel.folds.counters.NewRelminMxid == vacrel.cutoffs.OldestMxact
            || ::types_core::xact::MultiXactIdPrecedesOrEquals(
                if vacrel.aggressive { vacrel.cutoffs.MultiXactCutoff } else { vacrel.cutoffs.relminmxid },
                vacrel.folds.counters.NewRelminMxid
            )
    );
    if vacrel.skippedallvis {
        // Skipped all-visible ranges may hold unfrozen XIDs the trackers missed.
        debug_assert!(!vacrel.aggressive);
    }
    if vacrel.skippedallvis || vacrel.coverage_hole {
        // coverage_hole: the §5.2 fail-closed guard — a morsel round's fold
        // coverage could not be verified, so advancement is suppressed
        // (always-safe, C's skippedallvis behavior; fires only under fault
        // injection — the gate record tracks that).
        vacrel.folds.counters.NewRelfrozenXid = InvalidTransactionId;
        vacrel.folds.counters.NewRelminMxid = 0;
    }

    let new_rel_pages = vacrel.rel_pages;
    let (mut new_rel_allvisible, mut new_rel_allfrozen) = visibilitymap_count(rel)?;
    if new_rel_allvisible > new_rel_pages {
        new_rel_allvisible = new_rel_pages;
    }
    if new_rel_allfrozen > new_rel_allvisible {
        new_rel_allfrozen = new_rel_allvisible;
    }
    let (frozenxid_updated, minmulti_updated) = vacuum_seams::vac_update_relstats::call(
        rel,
        new_rel_pages,
        vacrel.new_live_tuples,
        new_rel_allvisible,
        new_rel_allfrozen,
        vacrel.nindexes > 0,
        vacrel.folds.counters.NewRelfrozenXid,
        vacrel.folds.counters.NewRelminMxid,
        false,
    )?;

    pgstat::pgstat_report_vacuum(
        rel.rd_id,
        rel.rd_rel.relisshared,
        vacrel.new_live_tuples.max(0.0) as i64,
        (vacrel.folds.counters.recently_dead_tuples + vacrel.folds.counters.missed_dead_tuples)
            as i64,
        starttime,
    );
    pgstat_progress_end_command();

    if morsels::vtrace_enabled() {
        // GL-M41-2 attribution line. scan includes the nested idxbulk/reap/
        // idxclean/fsm windows (they run inside lazy_scan_heap); scan_only
        // subtracts them. tail = everything after truncate (relstats,
        // visibilitymap_count, pgstat report). WAL figures are the LEADER's
        // TLS WalUsage only (pool/launched helpers accumulate their own).
        let total_ns = trace_t0.elapsed().as_nanos() as u64;
        let scan = phase_trace::get(phase_trace::SCAN);
        let idxbulk = phase_trace::get(phase_trace::IDXBULK);
        let reap = phase_trace::get(phase_trace::REAP);
        let idxclean = phase_trace::get(phase_trace::IDXCLEAN);
        let fsm = phase_trace::get(phase_trace::FSM);
        let pvend = phase_trace::get(phase_trace::PVEND);
        let trunc = phase_trace::get(phase_trace::TRUNC);
        let mut walusage = ::types_core::instrument::WalUsage::default();
        ::instrument::wal_usage_accum_diff(
            &mut walusage,
            &::instrument::pg_wal_usage(),
            &startwalusage,
        );
        let nested_ns = idxbulk.ns + reap.ns + idxclean.ns + fsm.ns;
        let tail_ns = total_ns
            .saturating_sub(trace_setup_ns + scan.ns + pvend.ns + trunc.ns);
        morsels::vtrace(&format!(
            "vacuum-phases: rel={} total_ms={} setup_ms={} scan_ms={} scan_only_ms={} \
             idxbulk_ms={} idxbulk_n={} reap_ms={} reap_n={} idxclean_ms={} fsm_ms={} \
             pvend_ms={} trunc_ms={} tail_ms={} \
             leader_wal_recs={} leader_wal_bytes={} leader_wal_fpi={} leader_wal_buf_full={} \
             idxbulk_lwal_recs={} idxbulk_lwal_bytes={} reap_lwal_recs={} reap_lwal_bytes={}",
            vacrel.rel.name(),
            total_ns / 1_000_000,
            trace_setup_ns / 1_000_000,
            scan.ns / 1_000_000,
            scan.ns.saturating_sub(nested_ns) / 1_000_000,
            idxbulk.ns / 1_000_000,
            idxbulk.calls,
            reap.ns / 1_000_000,
            reap.calls,
            idxclean.ns / 1_000_000,
            fsm.ns / 1_000_000,
            pvend.ns / 1_000_000,
            trunc.ns / 1_000_000,
            tail_ns / 1_000_000,
            walusage.wal_records,
            walusage.wal_bytes,
            walusage.wal_fpi,
            walusage.wal_buffers_full,
            idxbulk.wal_records,
            idxbulk.wal_bytes,
            reap.wal_records,
            reap.wal_bytes,
        ));
    }

    if instrument_vac {
        let endtime = timestamp_seams::get_current_timestamp::call();
        if verbose
            || params.log_min_duration == 0
            || adt_timestamp::TimestampDifferenceExceeds(
                starttime,
                endtime,
                params.log_min_duration,
            )
        {
            vacuum_instrument_report(
                &vacrel,
                params,
                verbose,
                &dbname,
                &relnamespace,
                &relname,
                starttime,
                endtime,
                ru0.as_ref().expect("instrument implies ru0"),
                &startwalusage,
                &startbufferusage,
                orig_rel_pages,
                new_rel_pages,
                frozenxid_updated,
                minmulti_updated,
            )?;
        }
    }
    Ok(())
}

// The `instrument` report tail of heap_vacuum_rel (vacuumlazy.c:946): the
// "finished vacuuming"/"automatic vacuum of table" multi-line summary at INFO
// (VERBOSE) or LOG (autovacuum log_min_duration). Divergences recorded inline:
// eager scanning is elided (always 0 eagerly scanned); the delay-time line
// needs the vacuum-delay progress param (track_cost_delay_timing defaults
// off); I/O timings come from the BufferUsage diff (the pgstat block-time
// globals it mirrors).
#[allow(clippy::too_many_arguments)]
fn vacuum_instrument_report(
    vacrel: &LVRelState<'_, '_>,
    params: &VacuumParams,
    verbose: bool,
    dbname: &str,
    relnamespace: &str,
    relname: &str,
    starttime: ::types_core::TimestampTz,
    endtime: ::types_core::TimestampTz,
    ru0: &pg_rusage::PgRUsage,
    startwalusage: &::types_core::instrument::WalUsage,
    startbufferusage: &::types_core::instrument::BufferUsage,
    orig_rel_pages: BlockNumber,
    new_rel_pages: BlockNumber,
    frozenxid_updated: bool,
    minmulti_updated: bool,
) -> PgResult<()> {
    use std::fmt::Write as _;

    let (secs_dur, usecs_dur) = adt_timestamp::TimestampDifference(starttime, endtime);

    let mut walusage = ::types_core::instrument::WalUsage::default();
    ::instrument::wal_usage_accum_diff(&mut walusage, &::instrument::pg_wal_usage(), startwalusage);
    let mut bufferusage = ::types_core::instrument::BufferUsage::default();
    ::instrument::buffer_usage_accum_diff(
        &mut bufferusage,
        &::instrument::pg_buffer_usage(),
        startbufferusage,
    );

    let total_blks_hit = bufferusage.shared_blks_hit + bufferusage.local_blks_hit;
    let total_blks_read = bufferusage.shared_blks_read + bufferusage.local_blks_read;
    let total_blks_dirtied = bufferusage.shared_blks_dirtied + bufferusage.local_blks_dirtied;

    let mut buf = String::new();
    if verbose {
        debug_assert!(!params.is_wraparound);
        let _ = writeln!(
            buf,
            "finished vacuuming \"{}.{}.{}\": index scans: {}",
            dbname, relnamespace, relname, vacrel.num_index_scans
        );
    } else if params.is_wraparound {
        let msg = if vacrel.aggressive {
            "automatic aggressive vacuum to prevent wraparound of table"
        } else {
            "automatic vacuum to prevent wraparound of table"
        };
        let _ = writeln!(
            buf,
            "{} \"{}.{}.{}\": index scans: {}",
            msg, dbname, relnamespace, relname, vacrel.num_index_scans
        );
    } else {
        let msg = if vacrel.aggressive {
            "automatic aggressive vacuum of table"
        } else {
            "automatic vacuum of table"
        };
        let _ = writeln!(
            buf,
            "{} \"{}.{}.{}\": index scans: {}",
            msg, dbname, relnamespace, relname, vacrel.num_index_scans
        );
    }
    let pct = |part: u64| {
        if orig_rel_pages == 0 { 100.0 } else { 100.0 * part as f64 / orig_rel_pages as f64 }
    };
    let _ = writeln!(
        buf,
        "pages: {} removed, {} remain, {} scanned ({:.2}% of total), {} eagerly scanned",
        vacrel.removed_pages,
        new_rel_pages,
        vacrel.folds.counters.scanned_pages,
        pct(vacrel.folds.counters.scanned_pages),
        0, // eager scanning elided in this port (heap_vacuum_eager_scan_setup)
    );
    let _ = writeln!(
        buf,
        "tuples: {} removed, {} remain, {} are dead but not yet removable",
        vacrel.folds.counters.tuples_deleted, vacrel.new_rel_tuples as i64, vacrel.folds.counters.recently_dead_tuples
    );
    if vacrel.folds.counters.missed_dead_tuples > 0 {
        let _ = writeln!(
            buf,
            "tuples missed: {} dead from {} pages not removed due to cleanup lock contention",
            vacrel.folds.counters.missed_dead_tuples, vacrel.folds.counters.missed_dead_pages
        );
    }
    let next_xid = varsup::ReadNextTransactionId()?;
    let diff = next_xid.wrapping_sub(vacrel.cutoffs.OldestXmin) as i32;
    let _ = writeln!(
        buf,
        "removable cutoff: {}, which was {} XIDs old when operation ended",
        vacrel.cutoffs.OldestXmin, diff
    );
    if frozenxid_updated {
        let diff =
            vacrel.folds.counters.NewRelfrozenXid.wrapping_sub(vacrel.cutoffs.relfrozenxid) as i32;
        let _ = writeln!(
            buf,
            "new relfrozenxid: {}, which is {} XIDs ahead of previous value",
            vacrel.folds.counters.NewRelfrozenXid, diff
        );
    }
    if minmulti_updated {
        let diff =
            vacrel.folds.counters.NewRelminMxid.wrapping_sub(vacrel.cutoffs.relminmxid) as i32;
        let _ = writeln!(
            buf,
            "new relminmxid: {}, which is {} MXIDs ahead of previous value",
            vacrel.folds.counters.NewRelminMxid, diff
        );
    }
    let _ = writeln!(
        buf,
        "frozen: {} pages from table ({:.2}% of total) had {} tuples frozen",
        vacrel.folds.counters.new_frozen_tuple_pages,
        pct(vacrel.folds.counters.new_frozen_tuple_pages),
        vacrel.folds.counters.tuples_frozen
    );
    let _ = writeln!(
        buf,
        "visibility map: {} pages set all-visible, {} pages set all-frozen ({} were all-visible)",
        vacrel.folds.counters.vm_new_visible_pages,
        vacrel.folds.counters.vm_new_visible_frozen_pages + vacrel.folds.counters.vm_new_frozen_pages,
        vacrel.folds.counters.vm_new_frozen_pages
    );
    if vacrel.do_index_vacuuming {
        if vacrel.nindexes == 0 || vacrel.num_index_scans == 0 {
            buf.push_str("index scan not needed: ");
        } else {
            buf.push_str("index scan needed: ");
        }
        let _ = writeln!(
            buf,
            "{} pages from table ({:.2}% of total) had {} dead item identifiers removed",
            vacrel.folds.counters.lpdead_item_pages,
            pct(vacrel.folds.counters.lpdead_item_pages),
            vacrel.folds.counters.lpdead_items
        );
    } else {
        if !VacuumFailsafeActive() {
            buf.push_str("index scan bypassed: ");
        } else {
            buf.push_str("index scan bypassed by failsafe: ");
        }
        let _ = writeln!(
            buf,
            "{} pages from table ({:.2}% of total) have {} dead item identifiers",
            vacrel.folds.counters.lpdead_item_pages,
            pct(vacrel.folds.counters.lpdead_item_pages),
            vacrel.folds.counters.lpdead_items
        );
    }
    for (i, istat) in vacrel.indstats.iter().enumerate() {
        let Some(istat) = istat else { continue };
        let _ = writeln!(
            buf,
            "index \"{}\": pages: {} in total, {} newly deleted, {} currently deleted, {} reusable",
            vacrel.indrels.get(i).map(|r| r.name()).unwrap_or(""),
            istat.num_pages,
            istat.pages_newly_deleted,
            istat.pages_deleted,
            istat.pages_free
        );
    }
    if guc_tables::vars::track_io_timing.read() {
        let read_ms = bufferusage.shared_blk_read_time.get_millisec()
            + bufferusage.local_blk_read_time.get_millisec();
        let write_ms = bufferusage.shared_blk_write_time.get_millisec()
            + bufferusage.local_blk_write_time.get_millisec();
        let _ = writeln!(buf, "I/O timings: read: {:.3} ms, write: {:.3} ms", read_ms, write_ms);
    }
    let (mut read_rate, mut write_rate) = (0.0f64, 0.0f64);
    if secs_dur > 0 || usecs_dur > 0 {
        let dur = secs_dur as f64 + usecs_dur as f64 / 1_000_000.0;
        read_rate = BLCKSZ as f64 * total_blks_read as f64 / (1024.0 * 1024.0) / dur;
        write_rate = BLCKSZ as f64 * total_blks_dirtied as f64 / (1024.0 * 1024.0) / dur;
    }
    let _ = writeln!(
        buf,
        "avg read rate: {:.3} MB/s, avg write rate: {:.3} MB/s",
        read_rate, write_rate
    );
    let _ = writeln!(
        buf,
        "buffer usage: {} hits, {} reads, {} dirtied",
        total_blks_hit, total_blks_read, total_blks_dirtied
    );
    let _ = writeln!(
        buf,
        "WAL usage: {} records, {} full page images, {} bytes, {} buffers full",
        walusage.wal_records, walusage.wal_fpi, walusage.wal_bytes, walusage.wal_buffers_full
    );
    let _ = write!(buf, "system usage: {}", pg_rusage::pg_rusage_show(ru0).as_str());

    elog::ereport(if verbose { ::types_error::INFO } else { ::types_error::LOG })
        .errmsg_internal(buf)
        .finish(::types_error::ErrorLocation::new(
            "src/backend/access/heap/vacuumlazy.c",
            1146,
            "heap_vacuum_rel",
        ))
}

// C VacDeadItemsInfo (vacuum.h); DSM-resident under parallel vacuum.
struct VacDeadItemsInfo {
    max_bytes: usize,
    num_items: i64,
}

fn dead_items_alloc(vacrel: &mut LVRelState<'_, '_>, nworkers: i32) -> PgResult<()> {
    let vac_work_mem = if miscinit::GetMyBackendType() == ::types_core::BackendType::AutovacWorker
        && guc_tables::vars::autovacuum_work_mem.read() != -1
    {
        guc_tables::vars::autovacuum_work_mem.read()
    } else {
        init_small::globals::maintenance_work_mem()
    };

    // C tries parallel_vacuum_init whenever nworkers >= 0 (index-size gating
    // inside decides). The leader-local tidstore below stays the accumulation
    // buffer either way; a flat snapshot crosses to workers per pass.
    if nworkers >= 0 && vacrel.nindexes > 1 && vacrel.do_index_vacuuming {
        if vacrel.rel.uses_local_buffers() {
            if nworkers > 0 {
                elog::ereport(::types_error::WARNING)
                    .errmsg(format!(
                        "disabling parallel option of vacuum on \"{}\" --- cannot vacuum temporary tables in parallel",
                        vacrel.rel.name()
                    ))
                    .finish(::types_error::ErrorLocation::new(
                        "vacuumlazy.c",
                        3499,
                        "dead_items_alloc",
                    ))?;
            }
        } else {
            vacrel.pvs = vacuumparallel::parallel_vacuum_init(
                &vacrel.indrels,
                nworkers,
                vac_work_mem,
                &vacrel.bstrategy,
                vacrel.rel.rd_id,
            )?;
        }
    }

    vacrel.dead_items_info =
        VacDeadItemsInfo { max_bytes: vac_work_mem as usize * 1024, num_items: 0 };
    vacrel.dead_items = Some(TidStore::create_local(
        vacrel.mcx,
        vacrel.dead_items_info.max_bytes,
        true,
    )?);
    Ok(())
}

fn dead_items_add(
    dead_items: &mut TidStore,
    dead_items_info: &mut VacDeadItemsInfo,
    blkno: BlockNumber,
    offsets: &[OffsetNumber],
) -> PgResult<()> {
    dead_items.set_block_offsets(blkno, offsets)?;
    dead_items_info.num_items += offsets.len() as i64;

    pgstat_progress_update_multi_param(
        &[PROGRESS_VACUUM_NUM_DEAD_ITEM_IDS, PROGRESS_VACUUM_DEAD_TUPLE_BYTES],
        &[dead_items_info.num_items, dead_items.memory_usage() as i64],
    );
    Ok(())
}

// C dead_items_reset: recreate the tidstore with the same max_bytes.
fn dead_items_reset(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    vacrel.dead_items = None;
    vacrel.dead_items = Some(TidStore::create_local(
        vacrel.mcx,
        vacrel.dead_items_info.max_bytes,
        true,
    )?);
    vacrel.dead_items_info.num_items = 0;
    Ok(())
}

// indexam's reap check binary-searches a sorted dead-TID slice (recorded
// divergence); materialized per round until that lane adopts TidStoreIsMember.
fn collect_dead_tids<'mcx>(vacrel: &LVRelState<'_, 'mcx>) -> PgVec<'mcx, ItemPointerData> {
    let mut tids: PgVec<'mcx, ItemPointerData> =
        PgVec::with_capacity_in(vacrel.dead_items_info.num_items as usize, vacrel.mcx);
    let mut iter = vacrel.dead_items.as_ref().unwrap().begin_iterate();
    let mut offsets = [InvalidOffsetNumber; MaxOffsetNumber as usize];
    while let Some(res) = iter.next() {
        let n = res.block_offsets(&mut offsets);
        debug_assert!(n <= offsets.len());
        for &off in &offsets[..n] {
            tids.push(ItemPointerData::new(res.blkno, off));
        }
    }
    debug_assert_eq!(tids.len() as i64, vacrel.dead_items_info.num_items);
    tids
}

fn lazy_scan_heap(vacrel: &mut LVRelState<'_, '_>, mcx: Mcx<'_>, nrequested: i32) -> PgResult<()> {
    let _ = mcx;
    let rel_pages = vacrel.rel_pages;
    let mut next_fsm_block_to_vacuum: BlockNumber = 0;
    let mut vmbuffer = VmBuffer::new();

    pgstat_progress_update_multi_param(
        &[
            PROGRESS_VACUUM_PHASE,
            PROGRESS_VACUUM_TOTAL_HEAP_BLKS,
            PROGRESS_VACUUM_MAX_DEAD_TUPLE_BYTES,
        ],
        &[
            PROGRESS_VACUUM_PHASE_SCAN_HEAP,
            rel_pages as i64,
            vacrel.dead_items_info.max_bytes as i64,
        ],
    );

    // Morsel arm (doc §3, inc-2): behind PGRUST_RUNTIME_VACUUM=1 the heap
    // scan runs as RG-per-round SCAN task sets; INDEX/REAP stay the ported
    // serial paths driven from lazy_vacuum between rounds. Every refusal —
    // and every handoff (quiesce fallback, failsafe fire, small tail) — is
    // today's serial scan, resumed at `resume_block` with C's own cursor
    // machinery.
    let mut resume_block: BlockNumber = 0;
    if let Some(k) = morsels::admit(vacrel, nrequested) {
        match morsels::scan_rounds(vacrel, k)? {
            morsels::ScanHandoff::Refused => {}
            morsels::ScanHandoff::Resume { block, next_fsm } => {
                resume_block = block;
                next_fsm_block_to_vacuum = next_fsm;
            }
        }
    }
    let mut blkno: BlockNumber = resume_block;

    if resume_block == 0 {
        vacrel.current_block = InvalidBlockNumber;
        vacrel.next_unskippable_block = InvalidBlockNumber;
    } else {
        // Serial resume at `resume_block` (morsel handoff): the cursor
        // machine's next step scans/decides from exactly that block.
        vacrel.current_block = resume_block.wrapping_sub(1);
        vacrel.next_unskippable_block = resume_block.wrapping_sub(1);
    }
    vacrel.next_unskippable_allvis = false;

    loop {
        vacuum_delay_point(false)?;

        if vacrel.folds.counters.scanned_pages > 0
            && vacrel.folds.counters.scanned_pages % FAILSAFE_EVERY_PAGES as u64 == 0
        {
            lazy_check_wraparound_failsafe(vacrel)?;
        }

        if vacrel.dead_items_info.num_items > 0
            && vacrel.dead_items.as_ref().unwrap().memory_usage()
                > vacrel.dead_items_info.max_bytes
        {
            vmbuffer.release();
            vacrel.consider_bypass_optimization = false;
            lazy_vacuum(vacrel)?;
            phase_trace::time(phase_trace::FSM, || {
                freespace::FreeSpaceMapVacuumRange(vacrel.rel, next_fsm_block_to_vacuum, blkno + 1)
            })?;
            next_fsm_block_to_vacuum = blkno;

            pgstat_progress_update_param(PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_SCAN_HEAP);
        }

        let Some((next_blkno, all_visible_according_to_vm)) = heap_vac_scan_next_block(vacrel)?
        else {
            break;
        };
        blkno = next_blkno;

        let buf = bufmgr_seams::read_buffer_extended::call(
            vacrel.rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            vacrel.bstrategy.clone(),
        )?;
        vacrel.folds.counters.scanned_pages += 1;

        pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, blkno as i64);

        visibilitymap_pin(vacrel.rel, blkno, &mut vmbuffer)?;

        let mut got_cleanup_lock =
            bufmgr_seams::conditional_lock_buffer_for_cleanup::call(buf)?;
        if !got_cleanup_lock {
            bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;
        }

        // SAFETY: buffer pinned + at least share-locked above.
        let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };

        // Split borrows: the per-block bodies take the read-only env + the
        // fold block + the dead-TID sink (the round TidStore on this arm).
        let LVRelState {
            rel,
            cutoffs,
            vistest,
            aggressive,
            nindexes,
            do_index_vacuuming,
            folds,
            dead_items,
            dead_items_info,
            ..
        } = &mut *vacrel;
        let env = ScanEnv {
            rel: *rel,
            cutoffs: &*cutoffs,
            vistest: *vistest,
            aggressive: *aggressive,
            nindexes: *nindexes,
        };
        let mut sink = |blkno: BlockNumber, offsets: &[OffsetNumber]| {
            dead_items_add(dead_items.as_mut().unwrap(), dead_items_info, blkno, offsets)
        };

        if lazy_scan_new_or_empty(&env, folds, buf, blkno, page, !got_cleanup_lock, &vmbuffer)? {
            continue;
        }

        let mut has_lpdead_items = false;
        if !got_cleanup_lock
            && !lazy_scan_noprune(&env, folds, &mut sink, buf, blkno, page, &mut has_lpdead_items)?
        {
            debug_assert!(env.aggressive);
            bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            bufmgr_seams::lock_buffer_for_cleanup::call(buf)?;
            got_cleanup_lock = true;
        }

        let mut ndeleted = 0;
        if got_cleanup_lock {
            ndeleted = lazy_scan_prune(
                &env,
                folds,
                &mut sink,
                buf,
                blkno,
                page,
                &mut vmbuffer,
                all_visible_according_to_vm,
                &mut has_lpdead_items,
            )?;
        }

        if env.nindexes == 0 || !*do_index_vacuuming || !has_lpdead_items {
            let freespace = page.heap_free_space();
            bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            bufmgr_seams::release_buffer::call(buf)?;
            freespace::RecordPageWithFreeSpace(env.rel, blkno, freespace)?;

            if got_cleanup_lock
                && env.nindexes == 0
                && ndeleted > 0
                && blkno - next_fsm_block_to_vacuum >= VACUUM_FSM_EVERY_PAGES
            {
                freespace::FreeSpaceMapVacuumRange(env.rel, next_fsm_block_to_vacuum, blkno)?;
                next_fsm_block_to_vacuum = blkno;
            }
        } else {
            bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            bufmgr_seams::release_buffer::call(buf)?;
        }
    }

    vacrel.current_block = InvalidBlockNumber;
    vmbuffer.release();
    vacrel.next_unskippable_vmbuffer.release();

    pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, rel_pages as i64);

    vacrel.new_live_tuples = vac_estimate_reltuples(
        vacrel.rel,
        rel_pages,
        vacrel.folds.counters.scanned_pages as BlockNumber,
        vacrel.folds.counters.live_tuples as f64,
    );
    vacrel.new_rel_tuples = vacrel.new_live_tuples.max(0.0)
        + vacrel.folds.counters.recently_dead_tuples as f64
        + vacrel.folds.counters.missed_dead_tuples as f64;

    if vacrel.dead_items_info.num_items > 0 {
        lazy_vacuum(vacrel)?;
    }

    if rel_pages > next_fsm_block_to_vacuum {
        phase_trace::time(phase_trace::FSM, || {
            freespace::FreeSpaceMapVacuumRange(vacrel.rel, next_fsm_block_to_vacuum, rel_pages)
        })?;
    }

    pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, rel_pages as i64);

    if vacrel.nindexes > 0 && vacrel.do_index_cleanup {
        phase_trace::time(phase_trace::IDXCLEAN, || lazy_cleanup_all_indexes(vacrel))?;
    }
    Ok(())
}

/// The read-stream callback collapsed to a direct call: returns the next
/// block to scan and its VM status, or None at end of relation.
fn heap_vac_scan_next_block(
    vacrel: &mut LVRelState<'_, '_>,
) -> PgResult<Option<(BlockNumber, bool)>> {
    let mut next_block = vacrel.current_block.wrapping_add(1);

    if next_block >= vacrel.rel_pages {
        vacrel.next_unskippable_vmbuffer.release();
        return Ok(None);
    }

    if vacrel.next_unskippable_block == InvalidBlockNumber
        || next_block > vacrel.next_unskippable_block
    {
        let skipsallvis = find_next_unskippable_block(vacrel)?;
        if vacrel.next_unskippable_block - next_block >= SKIP_PAGES_THRESHOLD {
            next_block = vacrel.next_unskippable_block;
            if skipsallvis {
                vacrel.skippedallvis = true;
            }
        }
    }

    if next_block < vacrel.next_unskippable_block {
        vacrel.current_block = next_block;
        Ok(Some((next_block, true)))
    } else {
        debug_assert!(next_block == vacrel.next_unskippable_block);
        vacrel.current_block = next_block;
        Ok(Some((next_block, vacrel.next_unskippable_allvis)))
    }
}

fn find_next_unskippable_block(vacrel: &mut LVRelState<'_, '_>) -> PgResult<bool> {
    let rel_pages = vacrel.rel_pages;
    let mut next_unskippable_block = vacrel.next_unskippable_block.wrapping_add(1);
    let mut skipsallvis = false;

    loop {
        let mapbits = visibilitymap_get_status(
            vacrel.rel,
            next_unskippable_block,
            &mut vacrel.next_unskippable_vmbuffer,
        )?;
        let next_unskippable_allvis = mapbits & VISIBILITYMAP_ALL_VISIBLE != 0;

        if !next_unskippable_allvis {
            debug_assert!(mapbits & VISIBILITYMAP_ALL_FROZEN == 0);
            vacrel.next_unskippable_allvis = false;
            break;
        }
        // The last block is always scanned (truncation opportunity check).
        if next_unskippable_block == rel_pages - 1 {
            vacrel.next_unskippable_allvis = true;
            break;
        }
        if !vacrel.skipwithvm {
            vacrel.next_unskippable_allvis = true;
            break;
        }
        if mapbits & VISIBILITYMAP_ALL_FROZEN != 0 {
            next_unskippable_block += 1;
            continue;
        }
        if vacrel.aggressive {
            vacrel.next_unskippable_allvis = true;
            break;
        }
        skipsallvis = true;
        next_unskippable_block += 1;
    }

    vacrel.next_unskippable_block = next_unskippable_block;
    Ok(skipsallvis)
}

fn page_is_empty(page: PageRef<'_>) -> bool {
    (page.pd_lower() as usize) <= SizeOfPageHeaderData
}

fn lazy_scan_new_or_empty(
    env: &ScanEnv<'_, '_>,
    folds: &mut ScanFolds,
    buf: Buffer,
    blkno: BlockNumber,
    page: PageRef<'_>,
    sharelock: bool,
    vmbuffer: &VmBuffer,
) -> PgResult<bool> {
    if page.is_new() {
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::release_buffer::call(buf)?;
        if freespace::GetRecordedFreeSpace(env.rel, blkno)? == 0 {
            let freespace: Size = BLCKSZ - SizeOfPageHeaderData;
            freespace::RecordPageWithFreeSpace(env.rel, blkno, freespace)?;
        }
        return Ok(true);
    }

    if page_is_empty(page) {
        // A share lock does not suffice to set all-visible: escalate to
        // exclusive (still no cleanup lock needed), rechecking emptiness.
        if sharelock {
            bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
            if !page_is_empty(page) {
                return Ok(false);
            }
        }

        if !page.is_all_visible() {
            bufmgr_seams::mark_buffer_dirty::call(buf)?;

            if relation_needs_wal(env.rel)
                && bufmgr_seams::buffer_page_get_lsn::call(buf) == 0
            {
                xloginsert_seams::log_newpage_buffer::call(buf, true)?;
            }

            // SAFETY: pinned + exclusive-or-cleanup-locked above.
            let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
            pm.set_all_visible();
            visibilitymap_set(
                env.rel,
                blkno,
                buf,
                0,
                vmbuffer,
                InvalidTransactionId,
                VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN,
            )?;
            folds.counters.vm_new_visible_pages += 1;
            folds.counters.vm_new_visible_frozen_pages += 1;
        }

        let freespace = page.heap_free_space();
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::release_buffer::call(buf)?;
        freespace::RecordPageWithFreeSpace(env.rel, blkno, freespace)?;
        return Ok(true);
    }

    Ok(false)
}

/// Share-lock fallback for lazy_scan_prune: no pruning or freezing. False
/// (page unprocessed) only when an aggressive VACUUM must freeze this page.
fn lazy_scan_noprune(
    env: &ScanEnv<'_, '_>,
    folds: &mut ScanFolds,
    dead_sink: DeadSink<'_>,
    buf: Buffer,
    blkno: BlockNumber,
    page: PageRef<'_>,
    has_lpdead_items: &mut bool,
) -> PgResult<bool> {
    debug_assert!(bufmgr_seams::buffer_get_block_number::call(buf) == blkno);

    let mut hastup = false;
    let mut lpdead_items = 0usize;
    let mut live_tuples: u64 = 0;
    let mut recently_dead_tuples: u64 = 0;
    let mut missed_dead_tuples: u64 = 0;
    let mut NoFreezePageRelfrozenXid = folds.counters.NewRelfrozenXid;
    let mut NoFreezePageRelminMxid = folds.counters.NewRelminMxid;
    let mut deadoffsets = [InvalidOffsetNumber; MaxHeapTuplesPerPage];

    let maxoff = page.max_offset_number();
    let mut offnum = FirstOffsetNumber;
    while offnum <= maxoff {
        folds.offnum = offnum;
        let itemid = page.item_id(offnum);

        if !itemid.is_used() {
            offnum += 1;
            continue;
        }

        if itemid.is_redirected() {
            hastup = true;
            offnum += 1;
            continue;
        }

        if itemid.is_dead() {
            // Deliberately no hastup here, as C (see lazy_scan_prune).
            deadoffsets[lpdead_items] = offnum;
            lpdead_items += 1;
            offnum += 1;
            continue;
        }

        hastup = true;
        // SAFETY: LP_NORMAL item within the pinned, share-locked page image.
        let (ptr, len) = unsafe { page.item_raw_unchecked(itemid) };
        // SAFETY: in-page tuple image; the pin outlives this scope.
        let mut tuple = unsafe {
            HeapTupleData::from_raw_parts(
                ptr,
                len,
                ItemPointerData::new(blkno, offnum),
                env.rel.rd_id,
            )
        };

        if heapam::freeze::heap_tuple_should_freeze(
            tuple.t_data(),
            env.cutoffs,
            &mut NoFreezePageRelfrozenXid,
            &mut NoFreezePageRelminMxid,
        )? && env.aggressive
        {
            // Aggressive VACUUM must advance relfrozenxid past FreezeLimit:
            // only lazy_scan_prune under a cleanup lock can freeze this page.
            folds.offnum = InvalidOffsetNumber;
            return Ok(false);
        }

        match heapam_visibility_seams::heap_tuple_satisfies_vacuum::call(
            &mut tuple,
            env.cutoffs.OldestXmin,
            buf,
        )? {
            HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS | HTSV_Result::HEAPTUPLE_LIVE => {
                live_tuples += 1;
            }
            HTSV_Result::HEAPTUPLE_DEAD => {
                missed_dead_tuples += 1;
            }
            HTSV_Result::HEAPTUPLE_RECENTLY_DEAD => {
                recently_dead_tuples += 1;
            }
            HTSV_Result::HEAPTUPLE_INSERT_IN_PROGRESS => {}
        }
        offnum += 1;
    }

    folds.offnum = InvalidOffsetNumber;

    // Freezing/pruning is deferred to the next VACUUM; ratchet the trackers
    // last (lazy_scan_prune expects a clean slate).
    folds.counters.NewRelfrozenXid = NoFreezePageRelfrozenXid;
    folds.counters.NewRelminMxid = NoFreezePageRelminMxid;

    if env.nindexes == 0 {
        if lpdead_items > 0 {
            // One-pass strategy without a cleanup lock: count LP_DEAD items
            // as missed instead of maintaining a dedicated reap lane, as C.
            hastup = true;
            missed_dead_tuples += lpdead_items as u64;
        }
    } else if lpdead_items > 0 {
        folds.counters.lpdead_item_pages += 1;
        dead_sink(blkno, &deadoffsets[..lpdead_items])?;
        folds.counters.lpdead_items += lpdead_items as u64;
    }

    folds.counters.live_tuples += live_tuples;
    folds.counters.recently_dead_tuples += recently_dead_tuples;
    folds.counters.missed_dead_tuples += missed_dead_tuples;
    if missed_dead_tuples > 0 {
        folds.counters.missed_dead_pages += 1;
    }

    if hastup {
        folds.counters.nonempty_pages = folds.counters.nonempty_pages.max(blkno + 1);
    }

    *has_lpdead_items = lpdead_items > 0;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn lazy_scan_prune(
    env: &ScanEnv<'_, '_>,
    folds: &mut ScanFolds,
    dead_sink: DeadSink<'_>,
    buf: Buffer,
    blkno: BlockNumber,
    page: PageRef<'_>,
    vmbuffer: &mut VmBuffer,
    all_visible_according_to_vm: bool,
    has_lpdead_items: &mut bool,
) -> PgResult<i32> {
    let mut prune_options = HEAP_PAGE_PRUNE_FREEZE;
    if env.nindexes == 0 {
        prune_options |= HEAP_PAGE_PRUNE_MARK_UNUSED_NOW;
    }

    let mut presult = PruneFreezeResult::default();
    let mut new_relfrozen_xid = folds.counters.NewRelfrozenXid;
    let mut new_relmin_mxid = folds.counters.NewRelminMxid;
    heap_page_prune_and_freeze(
        env.rel,
        buf,
        env.vistest,
        prune_options,
        Some(env.cutoffs),
        &mut presult,
        PruneReason::PruneVacuumScan,
        &mut folds.offnum,
        Some(&mut new_relfrozen_xid),
        Some(&mut new_relmin_mxid),
    )?;
    folds.counters.NewRelfrozenXid = new_relfrozen_xid;
    folds.counters.NewRelminMxid = new_relmin_mxid;
    debug_assert!(folds.counters.NewRelminMxid != 0);
    debug_assert!(TransactionIdIsValid(folds.counters.NewRelfrozenXid));

    if presult.nfrozen > 0 {
        // Counts pages with newly frozen tuples, not pages newly all-frozen
        // in the VM.
        folds.counters.new_frozen_tuple_pages += 1;
    }

    // Prune-time visibility must agree with heap_page_is_all_visible (C's
    // USE_ASSERT_CHECKING cross-check).
    #[cfg(debug_assertions)]
    if presult.all_visible {
        debug_assert!(presult.lpdead_items == 0);
        let (dbg_av, dbg_af, dbg_cutoff) =
            heap_page_is_all_visible(env.rel, env.cutoffs.OldestXmin, &mut folds.offnum, buf)?;
        debug_assert!(dbg_av);
        debug_assert!(presult.all_frozen == dbg_af);
        debug_assert!(!TransactionIdIsValid(dbg_cutoff) || dbg_cutoff == presult.vm_conflict_horizon);
    }

    if presult.lpdead_items > 0 {
        folds.counters.lpdead_item_pages += 1;
        let deadoffsets = &mut presult.deadoffsets[..presult.lpdead_items as usize];
        deadoffsets.sort_unstable();
        dead_sink(blkno, deadoffsets)?;
    }

    folds.counters.tuples_deleted += presult.ndeleted as u64;
    folds.counters.tuples_frozen += presult.nfrozen as u64;
    folds.counters.lpdead_items += presult.lpdead_items as u64;
    folds.counters.live_tuples += presult.live_tuples as u64;
    folds.counters.recently_dead_tuples += presult.recently_dead_tuples as u64;

    if presult.hastup {
        folds.counters.nonempty_pages = folds.counters.nonempty_pages.max(blkno + 1);
    }

    *has_lpdead_items = presult.lpdead_items > 0;
    debug_assert!(!presult.all_visible || presult.lpdead_items == 0);

    let (all_visible, all_frozen, vm_conflict_horizon) = (
        presult.all_visible,
        presult.all_frozen,
        presult.vm_conflict_horizon,
    );

    if !all_visible_according_to_vm && all_visible {
        let mut flags = VISIBILITYMAP_ALL_VISIBLE;
        if all_frozen {
            debug_assert!(!TransactionIdIsValid(vm_conflict_horizon));
            flags |= VISIBILITYMAP_ALL_FROZEN;
        }

        // PD_ALL_VISIBLE before the VM bit, as C (the reverse is corruption).
        // SAFETY: pinned + cleanup-locked by the scan loop.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
        pm.set_all_visible();
        bufmgr_seams::mark_buffer_dirty::call(buf)?;
        let old_vmbits =
            visibilitymap_set(env.rel, blkno, buf, 0, vmbuffer, vm_conflict_horizon, flags)?;

        if old_vmbits & VISIBILITYMAP_ALL_VISIBLE == 0 {
            folds.counters.vm_new_visible_pages += 1;
            if all_frozen {
                folds.counters.vm_new_visible_frozen_pages += 1;
            }
        } else if old_vmbits & VISIBILITYMAP_ALL_FROZEN == 0 && all_frozen {
            folds.counters.vm_new_frozen_pages += 1;
        }
    } else if all_visible_according_to_vm
        && !page.is_all_visible()
        && visibilitymap_get_status(env.rel, blkno, vmbuffer)? != 0
    {
        // VM bit set while the page-level bit is clear: repair, as C (WARNING
        // elided).
        visibilitymap_clear(env.rel, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS)?;
    } else if presult.lpdead_items > 0 && page.is_all_visible() {
        // SAFETY: pinned + cleanup-locked by the scan loop.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
        pm.clear_all_visible();
        bufmgr_seams::mark_buffer_dirty::call(buf)?;
        visibilitymap_clear(env.rel, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS)?;
    } else if all_visible_according_to_vm
        && all_visible
        && all_frozen
        && !vm_all_frozen(env.rel, blkno, vmbuffer)?
    {
        if !page.is_all_visible() {
            // SAFETY: pinned + cleanup-locked by the scan loop.
            let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
            pm.set_all_visible();
            bufmgr_seams::mark_buffer_dirty::call(buf)?;
        }
        debug_assert!(!TransactionIdIsValid(vm_conflict_horizon));
        let old_vmbits = visibilitymap_set(
            env.rel,
            blkno,
            buf,
            0,
            vmbuffer,
            InvalidTransactionId,
            VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN,
        )?;
        if old_vmbits & VISIBILITYMAP_ALL_VISIBLE == 0 {
            folds.counters.vm_new_visible_pages += 1;
            folds.counters.vm_new_visible_frozen_pages += 1;
        } else {
            folds.counters.vm_new_frozen_pages += 1;
        }
    }

    Ok(presult.ndeleted)
}

fn lazy_vacuum(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    debug_assert!(vacrel.nindexes > 0);
    debug_assert!(vacrel.folds.counters.lpdead_item_pages > 0);

    if !vacrel.do_index_vacuuming {
        debug_assert!(!vacrel.do_index_cleanup);
        dead_items_reset(vacrel)?;
        return Ok(());
    }

    let mut bypass = false;
    if vacrel.consider_bypass_optimization && vacrel.rel_pages > 0 {
        debug_assert!(vacrel.num_index_scans == 0);
        debug_assert!(vacrel.folds.counters.lpdead_items == vacrel.dead_items_info.num_items as u64);
        let threshold = vacrel.rel_pages as f64 * BYPASS_THRESHOLD_PAGES;
        bypass = (vacrel.folds.counters.lpdead_item_pages as f64) < threshold
            && vacrel.dead_items.as_ref().unwrap().memory_usage() < 32 * 1024 * 1024;
    }

    if bypass {
        vacrel.do_index_vacuuming = false;
    } else if phase_trace::time(phase_trace::IDXBULK, || lazy_vacuum_all_indexes(vacrel))? {
        phase_trace::time(phase_trace::REAP, || lazy_vacuum_heap_rel(vacrel))?;
    } else {
        debug_assert!(VacuumFailsafeActive());
    }

    dead_items_reset(vacrel)?;
    Ok(())
}

/// lazy_vacuum_all_indexes: one ambulkdelete round over every index. `false`
/// only in the wraparound-failsafe case.
fn lazy_vacuum_all_indexes(vacrel: &mut LVRelState<'_, '_>) -> PgResult<bool> {
    let mut allindexes = true;
    let old_live_tuples = vacrel.rel.rd_rel.reltuples as f64;

    debug_assert!(vacrel.nindexes > 0);
    debug_assert!(vacrel.do_index_vacuuming);
    debug_assert!(vacrel.do_index_cleanup);

    if lazy_check_wraparound_failsafe(vacrel)? {
        return Ok(false);
    }

    pgstat_progress_update_multi_param(
        &[PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_INDEXES_TOTAL],
        &[PROGRESS_VACUUM_PHASE_VACUUM_INDEX, vacrel.nindexes as i64],
    );

    let dead_tids = collect_dead_tids(vacrel);

    // W1 forensics (trace-gated): the snapshot the index-side reap-membership
    // binary search (nbtree tid_is_member) consumes MUST be strictly
    // ascending under ItemPointerCompare; report length + the first
    // violation so a phantom-index-entry recurrence self-classifies
    // (family A: store short of the lpdead fold = worker collection miss;
    // family B: full-but-unsorted snapshot or a per-index removed shortfall
    // = snapshot/search miss).
    if morsels::vtrace_enabled() {
        let mut first_bad: i64 = -1;
        for i in 1..dead_tids.len() {
            if ::types_tuple::itemptr::ItemPointerCompare(&dead_tids[i - 1], &dead_tids[i]) >= 0 {
                first_bad = i as i64;
                break;
            }
        }
        morsels::vtrace(&format!(
            "w1 index snapshot n={} store_items={} num_index_scans={} strict_sorted={} first_bad={}",
            dead_tids.len(),
            vacrel.dead_items_info.num_items,
            vacrel.num_index_scans,
            first_bad < 0,
            first_bad,
        ));
    }

    if vacrel.pvs.is_none() {
        for idx in 0..vacrel.nindexes {
            let istat = vacrel.indstats[idx].take();
            let new_istat = {
                let ivinfo = IndexVacuumInfo {
                    index: &vacrel.indrels[idx],
                    heaprel: vacrel.rel,
                    analyze_only: false,
                    estimated_count: true,
                    num_heap_tuples: old_live_tuples,
                    strategy: vacrel.bstrategy.clone(),
                };
                vac_bulkdel_one_index(vacrel.mcx, &ivinfo, istat, &dead_tids)?
            };
            if morsels::vtrace_enabled() {
                morsels::vtrace(&format!(
                    "w1 index bulkdel idx={} rel={} tuples_removed={} num_index_tuples={}",
                    idx,
                    vacrel.indrels[idx].name(),
                    new_istat.tuples_removed,
                    new_istat.num_index_tuples,
                ));
            }
            vacrel.indstats[idx] = Some(new_istat);

            pgstat_progress_update_param(PROGRESS_VACUUM_INDEXES_PROCESSED, (idx + 1) as i64);

            if lazy_check_wraparound_failsafe(vacrel)? {
                allindexes = false;
                break;
            }
        }
    } else {
        vacuumparallel::parallel_vacuum_bulkdel_all_indexes(
            vacrel.pvs.as_mut().expect("checked is_some"),
            vacrel.mcx,
            vacrel.rel,
            &vacrel.indrels,
            &vacrel.bstrategy,
            &dead_tids,
            old_live_tuples,
            vacrel.num_index_scans as i32,
        )?;
        // Parallel VACUUM gets only the precheck and this postcheck.
        if lazy_check_wraparound_failsafe(vacrel)? {
            allindexes = false;
        }
    }

    debug_assert!(
        vacrel.num_index_scans > 0
            || vacrel.dead_items_info.num_items as u64 == vacrel.folds.counters.lpdead_items
    );
    debug_assert!(allindexes || VacuumFailsafeActive());

    vacrel.num_index_scans += 1;
    pgstat_progress_update_multi_param(
        &[
            PROGRESS_VACUUM_INDEXES_TOTAL,
            PROGRESS_VACUUM_INDEXES_PROCESSED,
            PROGRESS_VACUUM_NUM_INDEX_VACUUMS,
        ],
        &[0, 0, vacrel.num_index_scans],
    );
    Ok(allindexes)
}

/// lazy_cleanup_all_indexes: amvacuumcleanup for every index.
fn lazy_cleanup_all_indexes(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    debug_assert!(vacrel.do_index_cleanup);
    debug_assert!(vacrel.nindexes > 0);

    let reltuples = vacrel.new_rel_tuples;
    let estimated_count = vacrel.folds.counters.scanned_pages < vacrel.rel_pages as u64;

    pgstat_progress_update_multi_param(
        &[PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_INDEXES_TOTAL],
        &[PROGRESS_VACUUM_PHASE_INDEX_CLEANUP, vacrel.nindexes as i64],
    );

    if vacrel.pvs.is_none() {
        for idx in 0..vacrel.nindexes {
            let istat = vacrel.indstats[idx].take();
            let new_istat = {
                let ivinfo = IndexVacuumInfo {
                    index: &vacrel.indrels[idx],
                    heaprel: vacrel.rel,
                    analyze_only: false,
                    estimated_count,
                    num_heap_tuples: reltuples,
                    strategy: vacrel.bstrategy.clone(),
                };
                vac_cleanup_one_index(vacrel.mcx, &ivinfo, istat)?
            };
            vacrel.indstats[idx] = new_istat;

            pgstat_progress_update_param(PROGRESS_VACUUM_INDEXES_PROCESSED, (idx + 1) as i64);
        }
    } else {
        vacuumparallel::parallel_vacuum_cleanup_all_indexes(
            vacrel.pvs.as_mut().expect("checked is_some"),
            vacrel.mcx,
            vacrel.rel,
            &vacrel.indrels,
            &vacrel.bstrategy,
            reltuples,
            vacrel.num_index_scans as i32,
            estimated_count,
        )?;
    }

    pgstat_progress_update_multi_param(
        &[PROGRESS_VACUUM_INDEXES_TOTAL, PROGRESS_VACUUM_INDEXES_PROCESSED],
        &[0, 0],
    );
    Ok(())
}

/// update_relstats_all_indexes: index pg_class stats where accurate.
fn update_relstats_all_indexes(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    debug_assert!(vacrel.do_index_cleanup);
    for idx in 0..vacrel.nindexes {
        let Some(istat) = &vacrel.indstats[idx] else {
            continue;
        };
        if istat.estimated_count {
            continue;
        }
        vacuum_seams::vac_update_relstats::call(
            &vacrel.indrels[idx],
            istat.num_pages,
            istat.num_index_tuples,
            0,
            0,
            false,
            InvalidTransactionId,
            0,
            false,
        )?;
    }
    Ok(())
}

/// Phase III driver (C lazy_vacuum_heap_rel): reap the collected LP_DEAD tids
/// block by block. Reached from lazy_vacuum only after index vacuuming (loud
/// today); exercised directly by tests.
pub fn lazy_vacuum_heap_rel(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    let mut vacuumed_pages: BlockNumber = 0;
    let mut vacuumed_items: u64 = 0; // W1 forensics (trace-gated report)
    let mut vmbuffer = VmBuffer::new();

    pgstat_progress_update_param(PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_VACUUM_HEAP);

    let dead_items = vacrel.dead_items.take().unwrap();
    let mut iter = dead_items.begin_iterate();
    while let Some(iter_result) = iter.next() {
        vacuum_delay_point(false)?;

        let blkno = iter_result.blkno;
        let mut offsets = [InvalidOffsetNumber; MaxOffsetNumber as usize];
        let num_offsets = iter_result.block_offsets(&mut offsets);
        debug_assert!(num_offsets <= offsets.len());

        let buf = bufmgr_seams::read_buffer_extended::call(
            vacrel.rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            vacrel.bstrategy.clone(),
        )?;
        visibilitymap_pin(vacrel.rel, blkno, &mut vmbuffer)?;
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
        lazy_vacuum_heap_page(vacrel, blkno, buf, &offsets[..num_offsets], &vmbuffer)?;

        // SAFETY: pinned; freespace read before unlock, as C.
        let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
        let freespace = page.heap_free_space();
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::release_buffer::call(buf)?;
        freespace::RecordPageWithFreeSpace(vacrel.rel, blkno, freespace)?;
        vacuumed_pages += 1;
        vacuumed_items += num_offsets as u64;
    }
    drop(iter);
    vacrel.dead_items = Some(dead_items);
    if morsels::vtrace_enabled() {
        morsels::vtrace(&format!(
            "w1 reap done rel={} pages={} items={} store_items={}",
            vacrel.rel.name(),
            vacuumed_pages,
            vacuumed_items,
            vacrel.dead_items_info.num_items,
        ));
    }
    debug_assert!(
        vacrel.num_index_scans > 1
            || (vacrel.dead_items_info.num_items as u64 == vacrel.folds.counters.lpdead_items
                && vacuumed_pages as u64 == vacrel.folds.counters.lpdead_item_pages)
    );

    vmbuffer.release();
    Ok(())
}

fn lazy_vacuum_heap_page(
    vacrel: &mut LVRelState<'_, '_>,
    blkno: BlockNumber,
    buffer: Buffer,
    deadoffsets: &[OffsetNumber],
    vmbuffer: &VmBuffer,
) -> PgResult<()> {
    pgstat_progress_update_param(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blkno as i64);

    // SAFETY: caller holds pin + exclusive content lock.
    let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) };
    let mut unused = [InvalidOffsetNumber; MaxHeapTuplesPerPage];
    let mut nunused = 0usize;

    for &toff in deadoffsets {
        let mut itemid = pm.as_ref().item_id(toff);
        debug_assert!(itemid.is_dead() && !itemid.has_storage());
        itemid.set_unused();
        pm.set_item_id(toff, itemid);
        unused[nunused] = toff;
        nunused += 1;
    }
    debug_assert!(nunused > 0);

    pm.truncate_line_pointer_array();

    bufmgr_seams::mark_buffer_dirty::call(buffer)?;

    if relation_needs_wal(vacrel.rel) {
        log_heap_prune_and_freeze(
            vacrel.rel,
            buffer,
            InvalidTransactionId,
            false,
            PruneReason::PruneVacuumCleanup,
            &mut [],
            &[],
            &[],
            &unused[..nunused],
        )?;
    }

    debug_assert!(!pm.as_ref().is_all_visible());
    let (all_visible, all_frozen, visibility_cutoff_xid) = heap_page_is_all_visible(
        vacrel.rel,
        vacrel.cutoffs.OldestXmin,
        &mut vacrel.folds.offnum,
        buffer,
    )?;
    if all_visible {
        let mut flags = VISIBILITYMAP_ALL_VISIBLE;
        if all_frozen {
            debug_assert!(!TransactionIdIsValid(visibility_cutoff_xid));
            flags |= VISIBILITYMAP_ALL_FROZEN;
        }
        pm.set_all_visible();
        visibilitymap_set(
            vacrel.rel,
            blkno,
            buffer,
            0,
            vmbuffer,
            visibility_cutoff_xid,
            flags,
        )?;
        vacrel.folds.counters.vm_new_visible_pages += 1;
        if all_frozen {
            vacrel.folds.counters.vm_new_visible_frozen_pages += 1;
        }
    }
    Ok(())
}

/// Returns (all_visible, all_frozen, visibility_cutoff_xid). `offnum` is the
/// caller's error-context slot (C's vacrel->offnum) — participant-local.
fn heap_page_is_all_visible(
    rel: &RelationData<'_>,
    oldest_xmin: TransactionId,
    offnum_cx: &mut OffsetNumber,
    buf: Buffer,
) -> PgResult<(bool, bool, TransactionId)> {
    // SAFETY: caller holds pin + content lock; HTSV hint-bit stores land in
    // the page, as C.
    let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
    let blockno = bufmgr_seams::buffer_get_block_number::call(buf);
    let mut visibility_cutoff_xid = InvalidTransactionId;
    let mut all_frozen = true;
    let mut all_visible = true;

    let maxoff = page.max_offset_number();
    let mut offnum = FirstOffsetNumber;
    while offnum <= maxoff && all_visible {
        *offnum_cx = offnum;
        let itemid = page.item_id(offnum);

        if !itemid.is_used() || itemid.is_redirected() {
            offnum += 1;
            continue;
        }

        if itemid.is_dead() {
            all_visible = false;
            all_frozen = false;
            break;
        }

        debug_assert!(itemid.is_normal());
        // SAFETY: LP_NORMAL item within the locked page image.
        let (ptr, len) = unsafe { page.item_raw_unchecked(itemid) };
        // SAFETY: in-page tuple image; the pin outlives this scope.
        let mut tuple = unsafe {
            HeapTupleData::from_raw_parts(
                ptr,
                len,
                ItemPointerData::new(blockno, offnum),
                rel.rd_id,
            )
        };

        match heapam_visibility_seams::heap_tuple_satisfies_vacuum::call(
            &mut tuple,
            oldest_xmin,
            buf,
        )? {
            HTSV_Result::HEAPTUPLE_LIVE => {
                let hdr = tuple.t_data();
                if !hdr.xmin_committed() {
                    all_visible = false;
                    all_frozen = false;
                    break;
                }
                let xmin = hdr.xmin();
                if !TransactionIdPrecedes(xmin, oldest_xmin) {
                    all_visible = false;
                    all_frozen = false;
                    break;
                }
                if TransactionIdIsNormal(xmin)
                    && (visibility_cutoff_xid == InvalidTransactionId
                        || TransactionIdPrecedes(visibility_cutoff_xid, xmin))
                {
                    visibility_cutoff_xid = xmin;
                }
                if all_frozen && heapam::heap_tuple_needs_eventual_freeze(hdr) {
                    all_frozen = false;
                }
            }
            HTSV_Result::HEAPTUPLE_DEAD
            | HTSV_Result::HEAPTUPLE_RECENTLY_DEAD
            | HTSV_Result::HEAPTUPLE_INSERT_IN_PROGRESS
            | HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS => {
                all_visible = false;
                all_frozen = false;
            }
        }
        offnum += 1;
    }

    *offnum_cx = InvalidOffsetNumber;
    Ok((all_visible, all_frozen, visibility_cutoff_xid))
}

/// C lazy_check_wraparound_failsafe (REL_18_3 vacuumlazy.c:2949): one-shot
/// latch; on fire, abandon the buffer strategy, disable index vacuuming /
/// cleanup / truncation, reset the index progress counters, warn, and stop
/// applying cost limits. (Previously loud here; ported by the vacuum-morsels
/// lane — §5.5 — because the morsel arm's failsafe path completes serially
/// through this body, and it is reachable today without morsels.)
fn lazy_check_wraparound_failsafe(vacrel: &mut LVRelState<'_, '_>) -> PgResult<bool> {
    // Don't warn more than once per VACUUM.
    if VacuumFailsafeActive() {
        return Ok(true);
    }
    if !vacuum_xid_failsafe_check(&vacrel.cutoffs)? {
        return Ok(false);
    }
    apply_failsafe(vacrel)?;
    Ok(true)
}

/// The failsafe FIRE body (shared by the serial check above and the morsel
/// arm's leader, which observes the fire from its park loop and applies the
/// state change after the round quiesces — doc §5.5).
fn apply_failsafe(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    SetVacuumFailsafeActive(true);

    // Abandon use of a buffer access strategy to allow use of all of shared
    // buffers (C assumes the allocating caller frees the object).
    vacrel.bstrategy = types_storage::buf::buffer_access_strategy_none();

    // Disable index vacuuming, index cleanup, and heap rel truncation.
    vacrel.do_index_vacuuming = false;
    vacrel.do_index_cleanup = false;
    vacrel.do_rel_truncate = false;

    // Reset the progress counters.
    pgstat_progress_update_multi_param(
        &[PROGRESS_VACUUM_INDEXES_TOTAL, PROGRESS_VACUUM_INDEXES_PROCESSED],
        &[0, 0],
    );

    // C names the table db.schema.relname; the port has the relname (recorded
    // divergence: message prefix elided with the rest of the logging lane).
    elog::ereport(::types_error::WARNING)
        .errmsg(format!(
            "bypassing nonessential maintenance of table \"{}\" as a failsafe after {} index scans",
            vacrel.rel.name(),
            vacrel.num_index_scans
        ))
        .errdetail("The table's relfrozenxid or relminmxid is too far in the past.")
        .errhint(
            "Consider increasing configuration parameter \"maintenance_work_mem\" or \"autovacuum_work_mem\".\nYou might also need to consider other ways for VACUUM to keep up with the allocation of transaction IDs.",
        )
        .finish(::types_error::ErrorLocation::new(
            "vacuumlazy.c",
            2949,
            "lazy_check_wraparound_failsafe",
        ))?;

    // Stop applying cost limits from this point on.
    init_small::globals::SetVacuumCostActive(false);
    init_small::globals::SetVacuumCostBalance(0);
    Ok(())
}

const VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL_MS: u64 = 50;
const VACUUM_TRUNCATE_LOCK_TIMEOUT_MS: u64 = 5000;
const VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL_MS: i64 = 20;

// lazy_truncate_heap (vacuumlazy.c). The "stopping/suspending truncate" and
// "truncated N to M pages" messages are DEBUG2 without VERBOSE (loud
// upstream), so none are emitted.
fn lazy_truncate_heap(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    pgstat_progress_update_param(PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_TRUNCATE);

    let mut orig_rel_pages = vacrel.rel_pages;
    loop {
        let mut lock_retry = 0u64;
        loop {
            if lmgr::ConditionalLockRelation(vacrel.rel, types_rel::lock::AccessExclusiveLock)? {
                break;
            }
            postgres_seams::check_for_interrupts::call()?;
            lock_retry += 1;
            if lock_retry
                > VACUUM_TRUNCATE_LOCK_TIMEOUT_MS / VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL_MS
            {
                vacuum_verbose_msg(
                    vacrel,
                    format!(
                        "\"{}\": stopping truncate due to conflicting lock request",
                        vacrel.rel.name()
                    ),
                    3247,
                )?;
                return Ok(());
            }
            // C: WaitLatch(MyLatch, WL_TIMEOUT, 50ms) — no latch wakeups
            // here, so a plain timed sleep (worst case the same 50ms).
            std::thread::sleep(std::time::Duration::from_millis(
                VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL_MS,
            ));
        }

        // If the rel grew while we vacuumed under a weaker lock, the new
        // pages presumably hold live tuples: give up without updating
        // rel_pages (the old density estimate stays).
        let new_rel_pages = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
            vacrel.rel,
            ForkNumber::MAIN_FORKNUM,
        )?;
        if new_rel_pages != orig_rel_pages {
            lmgr::UnlockRelation(vacrel.rel, types_rel::lock::AccessExclusiveLock)?;
            return Ok(());
        }

        let mut lock_waiter_detected = false;
        let new_rel_pages = count_nondeletable_pages(vacrel, &mut lock_waiter_detected)?;
        vacrel.current_block = new_rel_pages;

        if new_rel_pages >= orig_rel_pages {
            lmgr::UnlockRelation(vacrel.rel, types_rel::lock::AccessExclusiveLock)?;
            return Ok(());
        }

        catalog_storage::RelationTruncate(vacrel.rel, new_rel_pages)?;

        // Other backends can't touch the rel until they process the smgr
        // inval smgrtruncate sent, which happens once they take their lock.
        lmgr::UnlockRelation(vacrel.rel, types_rel::lock::AccessExclusiveLock)?;

        // rel_pages shrinks without touching reltuples: the truncated pages
        // held no tuples.
        vacrel.removed_pages += orig_rel_pages - new_rel_pages;
        vacrel.rel_pages = new_rel_pages;

        vacuum_verbose_msg(
            vacrel,
            format!(
                "table \"{}\": truncated {} to {} pages",
                vacrel.rel.name(),
                orig_rel_pages,
                new_rel_pages
            ),
            3317,
        )?;
        orig_rel_pages = new_rel_pages;

        if !(new_rel_pages > vacrel.folds.counters.nonempty_pages && lock_waiter_detected) {
            return Ok(());
        }
    }
}

// count_nondeletable_pages (vacuumlazy.c). C's OS-readahead prefetch loop is
// skipped (no PrefetchBuffer surface); advisory only.
fn count_nondeletable_pages(
    vacrel: &mut LVRelState<'_, '_>,
    lock_waiter_detected: &mut bool,
) -> PgResult<BlockNumber> {
    // DST P2 (contract §1.3): truncate lock-waiter recheck on pg_clock.
    let mut starttime = pg_clock::MonoStamp::now();
    let mut blkno = vacrel.rel_pages;
    while blkno > vacrel.folds.counters.nonempty_pages {
        // Waiters queue behind our AccessExclusiveLock; probe at most every
        // VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL, checked once per 32 blocks.
        if blkno % 32 == 0 {
            let currenttime = pg_clock::MonoStamp::now();
            if currenttime.since_ns(starttime) as i64 / 1_000_000
                >= VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL_MS
            {
                if lmgr::LockHasWaitersRelation(
                    vacrel.rel,
                    types_rel::lock::AccessExclusiveLock,
                )? {
                    let msg = format!(
                        "table \"{}\": suspending truncate due to conflicting lock request",
                        vacrel.rel.name()
                    );
                    vacuum_verbose_msg(vacrel, msg, 3379)?;
                    *lock_waiter_detected = true;
                    return Ok(blkno);
                }
                starttime = currenttime;
            }
        }

        // No vacuum delay point under the exclusive lock; interrupts only.
        postgres_seams::check_for_interrupts::call()?;

        blkno -= 1;

        let buf = bufmgr_seams::read_buffer_extended::call(
            vacrel.rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            vacrel.bstrategy.clone(),
        )?;
        bufmgr_seams::lock_buffer::call(buf, ::bufmgr_seams::BUFFER_LOCK_SHARE)?;
        // SAFETY: buffer pinned + share-locked above.
        let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };

        if page.is_new() || page_is_empty(page) {
            bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            bufmgr_seams::release_buffer::call(buf)?;
            continue;
        }

        let mut hastup = false;
        let maxoff = page.max_offset_number();
        for offnum in FirstOffsetNumber..=maxoff {
            // Any non-unused item keeps the page: even LP_DEAD makes
            // truncation unsafe, its index entries may not be cleaned out.
            if page.item_id(offnum).is_used() {
                hastup = true;
                break;
            }
        }
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::release_buffer::call(buf)?;

        if hastup {
            return Ok(blkno + 1);
        }
    }
    Ok(vacrel.folds.counters.nonempty_pages)
}

fn should_attempt_truncation(vacrel: &LVRelState<'_, '_>) -> bool {
    if !vacrel.do_rel_truncate || VacuumFailsafeActive() {
        return false;
    }
    let possibly_freeable = vacrel.rel_pages - vacrel.folds.counters.nonempty_pages;
    possibly_freeable > 0
        && (possibly_freeable >= REL_TRUNCATE_MINIMUM
            || possibly_freeable >= vacrel.rel_pages / REL_TRUNCATE_FRACTION)
}


// RelationNeedsWAL (rel.h), including the wal_level=minimal skip-WAL clause.
fn relation_needs_wal(rel: &RelationData<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId))
}

pub fn init_seams() {
    // rd_tableam->relation_vacuum: installed here, not by tableam — heap is
    // the only AM and a tableam-side install cycles through heapam_handler.
    tableam_seams::table_relation_vacuum::set(|mcx, rel, params, bstrategy| {
        // pgrcolumnar: immutable append-only row groups — VACUUM is a no-op
        // (docs/design/pgrcolumnar-impl.md §7.2); autovacuum must never walk
        // pgrcolumnar bytes as heap pages.
        if tableam_vocab::is_pgrcolumnar_am_oid(rel.rd_rel.relam) {
            return Ok(());
        }
        heap_vacuum_rel(mcx, rel, params, bstrategy)
    });
}

// ereport(vacrel->verbose ? INFO : DEBUG2, ...) sites in vacuumlazy.c.
fn vacuum_verbose_msg(
    vacrel: &LVRelState<'_, '_>,
    msg: String,
    line: i32,
) -> PgResult<()> {
    elog::ereport(if vacrel.verbose { ::types_error::INFO } else { ::types_error::DEBUG2 })
        .errmsg(msg)
        .finish(::types_error::ErrorLocation::new(
            "src/backend/access/heap/vacuumlazy.c",
            line,
            "lazy_truncate_heap",
        ))
}

mod morsels;

#[cfg(test)]
mod tests;
