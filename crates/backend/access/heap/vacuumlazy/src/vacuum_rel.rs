//! Entry point and eager-scan setup (`vacuumlazy.c`).
//!
//!   * [`heap_vacuum_rel`] (vacuumlazy.c:615) — the public entry: VACUUM for one
//!     heap relation. Sets things up, calls `lazy_scan_heap`, then finalizes
//!     (truncation + pg_class updates + logging).
//!   * [`heap_vacuum_eager_scan_setup`] (vacuumlazy.c:488) — initialize the eager
//!     scan management fields of the [`LVRelState`].

use utils_error::{ereport};
use types_error::{ErrorLocation, INFO, LOG};
use types_vacuum::vacuumparallel::IndexBulkDeleteResult;
use types_core::{BlockNumber, BLCKSZ};
use types_error::PgResult;
use types_vacuum::vacuum::{VacOptValue, VacuumParams};

use crate::consts::{
    multi_xact_id_is_valid, multi_xact_id_precedes, multi_xact_id_precedes_or_equals,
    transaction_id_is_normal, transaction_id_precedes, transaction_id_precedes_or_equals,
    InvalidBlockNumber, InvalidMultiXactId, InvalidTransactionId, PROGRESS_COMMAND_VACUUM,
    PROGRESS_VACUUM_DELAY_TIME, PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_FINAL_CLEANUP,
    VACOPT_DISABLE_PAGE_SKIPPING, VACOPT_VERBOSE,
};
use crate::core::{LVRelState, VacErrPhase, EAGER_SCAN_REGION_SIZE, MAX_EAGER_FREEZE_SUCCESS_RATE};

use vacuumlazy_seams as vl;

fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("vacuumlazy.c", 0, funcname)
}

#[inline]
fn fmax(a: f64, b: f64) -> f64 {
    if a > b {
        a
    } else {
        b
    }
}

/// `heap_vacuum_eager_scan_setup()` (vacuumlazy.c:488) — set up the eager
/// scanning state for vacuuming a single relation.
pub fn heap_vacuum_eager_scan_setup<'mcx>(
    vacrel: &mut LVRelState<'mcx>,
    params: &VacuumParams,
) -> PgResult<()> {
    /* Initialize eager scan management fields to their disabled values. */
    vacrel.next_eager_scan_region_start = InvalidBlockNumber;
    vacrel.eager_scan_max_fails_per_region = 0;
    vacrel.eager_scan_remaining_fails = 0;
    vacrel.eager_scan_remaining_successes = 0;

    /* If eager scanning is explicitly disabled, just return. */
    if params.max_eager_freeze_failure_rate == 0.0 {
        return Ok(());
    }

    /* Aggressive vacuums do not eager scan. */
    if vacrel.aggressive {
        return Ok(());
    }

    /* Aggressively vacuuming a small relation isn't worth amortizing. */
    if vacrel.rel_pages < 2 * EAGER_SCAN_REGION_SIZE {
        return Ok(());
    }

    /*
     * Only enable eager scanning if we are likely to be able to freeze some
     * pages: wait until FreezeLimit has advanced past relfrozenxid or
     * MultiXactCutoff past relminmxid.
     */
    let mut oldest_unfrozen_before_cutoff = false;
    if transaction_id_is_normal(vacrel.cutoffs.relfrozenxid)
        && transaction_id_precedes(vacrel.cutoffs.relfrozenxid, vacrel.cutoffs.FreezeLimit)
    {
        oldest_unfrozen_before_cutoff = true;
    }

    if !oldest_unfrozen_before_cutoff
        && multi_xact_id_is_valid(vacrel.cutoffs.relminmxid)
        && multi_xact_id_precedes(vacrel.cutoffs.relminmxid, vacrel.cutoffs.MultiXactCutoff)
    {
        oldest_unfrozen_before_cutoff = true;
    }

    if !oldest_unfrozen_before_cutoff {
        return Ok(());
    }

    /* We have met the criteria to eagerly scan some pages. */

    /*
     * Our success cap is MAX_EAGER_FREEZE_SUCCESS_RATE of the all-visible but
     * not all-frozen blocks in the relation.
     */
    let (allvisible, allfrozen) = vl::visibilitymap_count::call(&vacrel.rel)?;

    vacrel.eager_scan_remaining_successes =
        (MAX_EAGER_FREEZE_SUCCESS_RATE * allvisible.wrapping_sub(allfrozen) as f64) as BlockNumber;

    /* If every all-visible page is frozen, eager scanning is disabled. */
    if vacrel.eager_scan_remaining_successes == 0 {
        return Ok(());
    }

    /*
     * Calculate the bounds of the first eager scan region: a random spot in the
     * first EAGER_SCAN_REGION_SIZE blocks.
     */
    let randseed = prng_seams::pg_global_prng_uint32::call();

    vacrel.next_eager_scan_region_start = randseed % EAGER_SCAN_REGION_SIZE;

    debug_assert!(
        params.max_eager_freeze_failure_rate > 0.0
            && params.max_eager_freeze_failure_rate <= 1.0
    );

    vacrel.eager_scan_max_fails_per_region =
        (params.max_eager_freeze_failure_rate * EAGER_SCAN_REGION_SIZE as f64) as BlockNumber;

    /* The first region is smaller; adjust the eager freeze failures tolerated. */
    let first_region_ratio =
        1.0 - vacrel.next_eager_scan_region_start as f32 / EAGER_SCAN_REGION_SIZE as f32;

    vacrel.eager_scan_remaining_fails =
        (vacrel.eager_scan_max_fails_per_region as f32 * first_region_ratio) as BlockNumber;

    Ok(())
}

/// `heap_vacuum_rel()` (vacuumlazy.c:615) — perform VACUUM for one heap relation.
///
/// At entry, the caller has already established a transaction and opened and
/// locked the relation.
pub fn heap_vacuum_rel<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rel: rel::Relation<'mcx>,
    params: &VacuumParams,
    bstrategy: types_storage::buf::BufferAccessStrategy,
) -> PgResult<()> {
    let mut frozenxid_updated;
    let mut minmulti_updated;
    let orig_rel_pages: BlockNumber;
    let new_rel_pages: BlockNumber;
    let mut new_rel_allvisible: BlockNumber;
    let mut new_rel_allfrozen: BlockNumber;
    let starttime: types_core::TimestampTz;
    let mut startreadtime: i64 = 0;
    let mut startwritetime: i64 = 0;
    let startwalusage = vl::pg_wal_usage::call()?;
    let startbufferusage = vl::pg_buffer_usage::call()?;
    let mut indnames: Vec<String> = Vec::new();
    /* PGRUsage ru0; — captured under `instrument`, formatted at the end. */
    let mut ru0: Option<rusage::PgRUsage> = None;

    let verbose = (params.options & VACOPT_VERBOSE) != 0;
    let instrument =
        verbose || (vl::am_autovacuum_worker_process::call()? && params.log_min_duration >= 0);
    if instrument {
        ru0 = Some(rusage_seams::pg_rusage_init::call());
    }
    if instrument && vl::track_io_timing::call()? {
        startreadtime = vl::pgstat_block_read_time::call()?;
        startwritetime = vl::pgstat_block_write_time::call()?;
    }

    /* Used for instrumentation and stats report. */
    starttime = vl::get_current_timestamp::call()?;

    let relid = rel.rd_id;
    vl::pgstat_progress_start_command::call(PROGRESS_COMMAND_VACUUM, relid)?;

    /*
     * Set up the central working state. Copy the rel names into local memory for
     * error reporting. The run owns `mcx` and holds the live open `rel` for the
     * whole scan (C: `vacrel->rel = rel`).
     */
    let mut vr = LVRelState::new_zeroed(mcx, rel);

    vr.dbname = vl::get_database_name::call(vl::my_database_id::call()?)?;
    vr.relnamespace = vl::get_namespace_name::call(vl::relation_get_namespace::call(&vr.rel)?)?;
    vr.relname = vl::relation_get_relation_name::call(&vr.rel)?;
    vr.indname = None;
    vr.phase = VacErrPhase::Unknown;
    vr.verbose = verbose;
    vl::push_error_context::call()?;

    /* Set up high level stuff about rel and its indexes. */
    vr.indrels = vl::vac_open_indexes::call(vr.mcx, &vr.rel)?;
    vr.nindexes = vr.indrels.len() as i32;
    vr.bstrategy = bstrategy;
    if instrument && vr.nindexes > 0 {
        for i in 0..vr.nindexes as usize {
            indnames.push(vl::relation_get_relation_name::call(&vr.indrels[i])?);
        }
    }

    /*
     * The index_cleanup param either disables index vacuuming and cleanup or
     * forces it. The truncate param allows the user to avoid truncation.
     */
    debug_assert!(params.index_cleanup != VacOptValue::VACOPTVALUE_UNSPECIFIED);
    debug_assert!(
        params.truncate != VacOptValue::VACOPTVALUE_UNSPECIFIED
            && params.truncate != VacOptValue::VACOPTVALUE_AUTO
    );

    vl::set_vacuum_failsafe_active::call(false)?;
    vr.consider_bypass_optimization = true;
    vr.do_index_vacuuming = true;
    vr.do_index_cleanup = true;
    vr.do_rel_truncate = params.truncate != VacOptValue::VACOPTVALUE_DISABLED;
    if params.index_cleanup == VacOptValue::VACOPTVALUE_DISABLED {
        vr.do_index_vacuuming = false;
        vr.do_index_cleanup = false;
    } else if params.index_cleanup == VacOptValue::VACOPTVALUE_ENABLED {
        vr.consider_bypass_optimization = false;
    } else {
        debug_assert!(params.index_cleanup == VacOptValue::VACOPTVALUE_AUTO);
    }

    /* Counters were zeroed by LVRelState::new_zeroed(); be tidy is implicit. */

    /* Allocate/initialize output statistics state. */
    vr.indstats = vec![None::<IndexBulkDeleteResult>; vr.nindexes as usize];

    /*
     * Get cutoffs, then determine the extent of the blocks we'll scan. Then
     * acquire vistest, used in pruning.
     */
    vr.aggressive = vl::vacuum_get_cutoffs::call(&vr.rel, *params, &mut vr.cutoffs)?;
    orig_rel_pages = vl::relation_get_number_of_blocks::call(&vr.rel)?;
    vr.rel_pages = orig_rel_pages;
    vr.vistest = vl::global_vis_test_for::call(&vr.rel)?;

    /* Initialize state used to track oldest extant XID/MXID. */
    vr.new_relfrozen_xid = vr.cutoffs.OldestXmin;
    vr.new_relmin_mxid = vr.cutoffs.OldestMxact;

    /* Initialize all-visible page skipping state. */
    vr.skippedallvis = false;
    let skipwithvm = if params.options & VACOPT_DISABLE_PAGE_SKIPPING != 0 {
        /* Force aggressive mode and disable VM-based skipping. */
        vr.aggressive = true;
        false
    } else {
        true
    };
    vr.skipwithvm = skipwithvm;

    /* Set up eager scan tracking state (must be after the aggressive decision). */
    heap_vacuum_eager_scan_setup(&mut vr, params)?;

    if verbose {
        if vr.aggressive {
            ereport(INFO)
                .errmsg(format!(
                    "aggressively vacuuming \"{}.{}.{}\"",
                    vr.dbname, vr.relnamespace, vr.relname
                ))
                .finish(here("heap_vacuum_rel"))?;
        } else {
            ereport(INFO)
                .errmsg(format!(
                    "vacuuming \"{}.{}.{}\"",
                    vr.dbname, vr.relnamespace, vr.relname
                ))
                .finish(here("heap_vacuum_rel"))?;
        }
    }

    /*
     * Allocate dead_items memory. Do a failsafe precheck first so parallel
     * VACUUM won't be attempted when relfrozenxid is already dangerously old.
     */
    crate::vacuum_phase::lazy_check_wraparound_failsafe(&mut vr)?;
    crate::dead_items::dead_items_alloc(&mut vr, params.nworkers)?;

    /*
     * Call lazy_scan_heap to perform all required heap pruning, index vacuuming,
     * and heap vacuuming (plus related processing).
     */
    crate::scan::lazy_scan_heap(&mut vr)?;

    /* Free resources managed by dead_items_alloc. */
    crate::dead_items::dead_items_cleanup(&mut vr)?;

    /* Update pg_class entries for each of rel's indexes where appropriate. */
    if vr.do_index_cleanup {
        crate::index::update_relstats_all_indexes(&mut vr)?;
    }

    /* Done with rel's indexes. */
    vl::vac_close_indexes::call(core::mem::take(&mut vr.indrels))?;

    /* Optionally truncate rel. */
    if crate::truncate::should_attempt_truncation(&mut vr)? {
        crate::truncate::lazy_truncate_heap(&mut vr)?;
    }

    /* Pop the error context stack. */
    vl::pop_error_context::call()?;

    /* Report that we are now doing final cleanup. */
    vl::pgstat_progress_update_param::call(PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_FINAL_CLEANUP)?;

    /*
     * Prepare to update rel's pg_class entry.
     */
    debug_assert!(
        vr.new_relfrozen_xid == vr.cutoffs.OldestXmin
            || transaction_id_precedes_or_equals(
                if vr.aggressive {
                    vr.cutoffs.FreezeLimit
                } else {
                    vr.cutoffs.relfrozenxid
                },
                vr.new_relfrozen_xid
            )
    );
    debug_assert!(
        vr.new_relmin_mxid == vr.cutoffs.OldestMxact
            || multi_xact_id_precedes_or_equals(
                if vr.aggressive {
                    vr.cutoffs.MultiXactCutoff
                } else {
                    vr.cutoffs.relminmxid
                },
                vr.new_relmin_mxid
            )
    );
    if vr.skippedallvis {
        /*
         * Must keep original relfrozenxid in a non-aggressive VACUUM that skipped
         * an all-visible page range.
         */
        debug_assert!(!vr.aggressive);
        vr.new_relfrozen_xid = InvalidTransactionId;
        vr.new_relmin_mxid = InvalidMultiXactId;
    }

    /* Clamp relallvisible to be not more than pg_class.relpages. */
    new_rel_pages = vr.rel_pages; /* After possible rel truncation. */
    let (av, af) = vl::visibilitymap_count::call(&vr.rel)?;
    new_rel_allvisible = av;
    new_rel_allfrozen = af;
    if new_rel_allvisible > new_rel_pages {
        new_rel_allvisible = new_rel_pages;
    }

    /* An all-frozen block must be all-visible; clamp all-frozen to all-visible. */
    if new_rel_allfrozen > new_rel_allvisible {
        new_rel_allfrozen = new_rel_allvisible;
    }

    /* Now actually update rel's pg_class entry. */
    let (fz_updated, mm_updated) = vl::vac_update_relstats::call(types_vacuum::vacuumlazy::UpdateRelStatsArgs {
        relation: vr.rel.rd_id,
        num_pages: new_rel_pages,
        num_tuples: vr.new_live_tuples,
        num_all_visible_pages: new_rel_allvisible,
        num_all_frozen_pages: new_rel_allfrozen,
        hasindex: vr.nindexes > 0,
        frozenxid: vr.new_relfrozen_xid,
        minmulti: vr.new_relmin_mxid,
        in_outer_xact: false,
    })?;
    frozenxid_updated = fz_updated;
    minmulti_updated = mm_updated;

    /* Report results to the cumulative stats system. */
    vl::pgstat_report_vacuum::call(
        relid,
        vl::relation_is_shared::call(&vr.rel)?,
        fmax(vr.new_live_tuples, 0.0) as i64,
        vr.recently_dead_tuples + vr.missed_dead_tuples,
        starttime,
    )?;
    vl::pgstat_progress_end_command::call()?;

    if instrument {
        let endtime = vl::get_current_timestamp::call()?;

        if verbose
            || params.log_min_duration == 0
            || vl::timestamp_difference_exceeds::call(starttime, endtime, params.log_min_duration)?
        {
            emit_verbose_log(
                &mut vr,
                params,
                verbose,
                orig_rel_pages,
                new_rel_pages,
                starttime,
                endtime,
                startreadtime,
                startwritetime,
                startwalusage,
                startbufferusage,
                &indnames,
                // ru0 is always Some here: it is captured under the same
                // `if instrument` guard that reaches this emit path.
                ru0.expect("ru0 captured when instrument"),
                &mut frozenxid_updated,
                &mut minmulti_updated,
            )?;
        }
    }

    Ok(())
}

/// The `instrument` completion-log block of `heap_vacuum_rel()` (the
/// `appendStringInfo` cascade), factored out to keep the entry readable.
#[allow(clippy::too_many_arguments)]
fn emit_verbose_log<'mcx>(
    vr: &mut LVRelState<'mcx>,
    params: &VacuumParams,
    verbose: bool,
    orig_rel_pages: BlockNumber,
    new_rel_pages: BlockNumber,
    starttime: types_core::TimestampTz,
    endtime: types_core::TimestampTz,
    startreadtime: i64,
    startwritetime: i64,
    startwalusage: (i64, i64, u64, i64),
    startbufferusage: (i64, i64, i64, i64, i64, i64),
    indnames: &[String],
    ru0: rusage::PgRUsage,
    frozenxid_updated: &mut bool,
    minmulti_updated: &mut bool,
) -> PgResult<()> {
    let (secs_dur, usecs_dur) = vl::timestamp_difference::call(starttime, endtime)?;

    /* WalUsageAccumDiff(&walusage, &pgWalUsage, &startwalusage). */
    let now_wal = vl::pg_wal_usage::call()?;
    let wal_records = now_wal.0 - startwalusage.0;
    let wal_fpi = now_wal.1 - startwalusage.1;
    let wal_bytes = now_wal.2.wrapping_sub(startwalusage.2);
    let wal_buffers_full = now_wal.3 - startwalusage.3;

    /* BufferUsageAccumDiff(&bufferusage, &pgBufferUsage, &startbufferusage). */
    let now_buf = vl::pg_buffer_usage::call()?;
    let shared_blks_hit = now_buf.0 - startbufferusage.0;
    let shared_blks_read = now_buf.1 - startbufferusage.1;
    let shared_blks_dirtied = now_buf.2 - startbufferusage.2;
    let local_blks_hit = now_buf.3 - startbufferusage.3;
    let local_blks_read = now_buf.4 - startbufferusage.4;
    let local_blks_dirtied = now_buf.5 - startbufferusage.5;

    let total_blks_hit = shared_blks_hit + local_blks_hit;
    let total_blks_read = shared_blks_read + local_blks_read;
    let total_blks_dirtied = shared_blks_dirtied + local_blks_dirtied;

    let mut buf = String::new();

    if verbose {
        debug_assert!(!params.is_wraparound);
        buf.push_str(&format!(
            "finished vacuuming \"{}.{}.{}\": index scans: {}\n",
            vr.dbname, vr.relnamespace, vr.relname, vr.num_index_scans
        ));
    } else if params.is_wraparound {
        if vr.aggressive {
            buf.push_str(&format!(
                "automatic aggressive vacuum to prevent wraparound of table \"{}.{}.{}\": index scans: {}\n",
                vr.dbname, vr.relnamespace, vr.relname, vr.num_index_scans
            ));
        } else {
            buf.push_str(&format!(
                "automatic vacuum to prevent wraparound of table \"{}.{}.{}\": index scans: {}\n",
                vr.dbname, vr.relnamespace, vr.relname, vr.num_index_scans
            ));
        }
    } else if vr.aggressive {
        buf.push_str(&format!(
            "automatic aggressive vacuum of table \"{}.{}.{}\": index scans: {}\n",
            vr.dbname, vr.relnamespace, vr.relname, vr.num_index_scans
        ));
    } else {
        buf.push_str(&format!(
            "automatic vacuum of table \"{}.{}.{}\": index scans: {}\n",
            vr.dbname, vr.relnamespace, vr.relname, vr.num_index_scans
        ));
    }

    buf.push_str(&format!(
        "pages: {} removed, {} remain, {} scanned ({:.2}% of total), {} eagerly scanned\n",
        vr.removed_pages,
        new_rel_pages,
        vr.scanned_pages,
        if orig_rel_pages == 0 {
            100.0
        } else {
            100.0 * vr.scanned_pages as f64 / orig_rel_pages as f64
        },
        vr.eager_scanned_pages
    ));
    buf.push_str(&format!(
        "tuples: {} removed, {} remain, {} are dead but not yet removable\n",
        vr.tuples_deleted, vr.new_rel_tuples as i64, vr.recently_dead_tuples
    ));
    if vr.missed_dead_tuples > 0 {
        buf.push_str(&format!(
            "tuples missed: {} dead from {} pages not removed due to cleanup lock contention\n",
            vr.missed_dead_tuples, vr.missed_dead_pages
        ));
    }
    let mut diff =
        vl::read_next_transaction_id::call()?.wrapping_sub(vr.cutoffs.OldestXmin) as i32;
    buf.push_str(&format!(
        "removable cutoff: {}, which was {} XIDs old when operation ended\n",
        vr.cutoffs.OldestXmin, diff
    ));
    if *frozenxid_updated {
        diff = vr.new_relfrozen_xid.wrapping_sub(vr.cutoffs.relfrozenxid) as i32;
        buf.push_str(&format!(
            "new relfrozenxid: {}, which is {} XIDs ahead of previous value\n",
            vr.new_relfrozen_xid, diff
        ));
    }
    if *minmulti_updated {
        diff = (vr.new_relmin_mxid as i64 - vr.cutoffs.relminmxid as i64) as i32;
        buf.push_str(&format!(
            "new relminmxid: {}, which is {} MXIDs ahead of previous value\n",
            vr.new_relmin_mxid, diff
        ));
    }
    buf.push_str(&format!(
        "frozen: {} pages from table ({:.2}% of total) had {} tuples frozen\n",
        vr.new_frozen_tuple_pages,
        if orig_rel_pages == 0 {
            100.0
        } else {
            100.0 * vr.new_frozen_tuple_pages as f64 / orig_rel_pages as f64
        },
        vr.tuples_frozen
    ));
    buf.push_str(&format!(
        "visibility map: {} pages set all-visible, {} pages set all-frozen ({} were all-visible)\n",
        vr.vm_new_visible_pages,
        vr.vm_new_visible_frozen_pages + vr.vm_new_frozen_pages,
        vr.vm_new_frozen_pages
    ));
    let removed: bool;
    if vr.do_index_vacuuming {
        if vr.nindexes == 0 || vr.num_index_scans == 0 {
            buf.push_str("index scan not needed: ");
        } else {
            buf.push_str("index scan needed: ");
        }
        removed = true;
    } else {
        if !vl::vacuum_failsafe_active::call()? {
            buf.push_str("index scan bypassed: ");
        } else {
            buf.push_str("index scan bypassed by failsafe: ");
        }
        removed = false;
    }
    {
        let pct = if orig_rel_pages == 0 {
            100.0
        } else {
            100.0 * vr.lpdead_item_pages as f64 / orig_rel_pages as f64
        };
        if removed {
            buf.push_str(&format!(
                "{} pages from table ({:.2}% of total) had {} dead item identifiers removed\n",
                vr.lpdead_item_pages, pct, vr.lpdead_items
            ));
        } else {
            buf.push_str(&format!(
                "{} pages from table ({:.2}% of total) have {} dead item identifiers\n",
                vr.lpdead_item_pages, pct, vr.lpdead_items
            ));
        }
    }
    for (i, istat) in vr.indstats.iter().enumerate() {
        let s = match istat {
            Some(s) => s,
            None => continue,
        };
        let name = indnames.get(i).map(|s| s.as_str()).unwrap_or("");
        buf.push_str(&format!(
            "index \"{}\": pages: {} in total, {} newly deleted, {} currently deleted, {} reusable\n",
            name, s.num_pages, s.pages_newly_deleted, s.pages_deleted, s.pages_free
        ));
    }
    if vl::track_cost_delay_timing::call()? {
        buf.push_str(&format!(
            "delay time: {:.3} ms\n",
            vl::my_be_entry_progress_param::call(PROGRESS_VACUUM_DELAY_TIME)? as f64 / 1_000_000.0
        ));
    }
    if vl::track_io_timing::call()? {
        let read_ms = (vl::pgstat_block_read_time::call()? - startreadtime) as f64 / 1000.0;
        let write_ms = (vl::pgstat_block_write_time::call()? - startwritetime) as f64 / 1000.0;
        buf.push_str(&format!(
            "I/O timings: read: {:.3} ms, write: {:.3} ms\n",
            read_ms, write_ms
        ));
    }
    let mut read_rate = 0.0;
    let mut write_rate = 0.0;
    if secs_dur > 0 || usecs_dur > 0 {
        let denom = secs_dur as f64 + usecs_dur as f64 / 1_000_000.0;
        read_rate = BLCKSZ as f64 * total_blks_read as f64 / (1024.0 * 1024.0) / denom;
        write_rate = BLCKSZ as f64 * total_blks_dirtied as f64 / (1024.0 * 1024.0) / denom;
    }
    buf.push_str(&format!(
        "avg read rate: {:.3} MB/s, avg write rate: {:.3} MB/s\n",
        read_rate, write_rate
    ));
    buf.push_str(&format!(
        "buffer usage: {} hits, {} reads, {} dirtied\n",
        total_blks_hit, total_blks_read, total_blks_dirtied
    ));
    buf.push_str(&format!(
        "WAL usage: {} records, {} full page images, {} bytes, {} buffers full\n",
        wal_records, wal_fpi, wal_bytes, wal_buffers_full
    ));
    buf.push_str(&format!(
        "system usage: {}",
        rusage_seams::pg_rusage_show::call(ru0)
    ));

    ereport(if verbose { INFO } else { LOG })
        .errmsg_internal(buf)
        .finish(here("heap_vacuum_rel"))?;

    Ok(())
}
