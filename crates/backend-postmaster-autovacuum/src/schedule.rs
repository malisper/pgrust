//! Per-table scheduling/execution (`autovacuum.c` lines 2604-3280): the
//! `table_recheck_autovac` recheck path, the `relation_needs_vacanalyze`
//! decision math, work-item processing, and the ps-display/activity reporting
//! helpers.

extern crate alloc;
use alloc::format;
use alloc::string::String;

use backend_utils_error::{elog, PgResult};
use types_error::{DEBUG3, WARNING};

use types_core::{bits32, MultiXactId, Oid, TransactionId};
use types_reloptions::AutoVacOpts;
use types_vacuum::vacuum::{
    VacOptValue, VacuumParams, VACOPT_ANALYZE, VACOPT_PROCESS_MAIN, VACOPT_SKIP_DATABASE_STATS,
    VACOPT_SKIP_LOCKED, VACOPT_VACUUM,
};
use types_autovacuum::{AutovacTable, AvRelation, RecheckClassRow, TabStatEntry};

use crate::core::{
    self, BlockNumberIsValid, FirstMultiXactId, FirstNormalTransactionId, MultiXactIdIsValid,
    MultiXactIdPrecedes, TransactionIdIsNormal, TransactionIdPrecedes, AVW_BRINSummarizeRange,
    RELKIND_TOASTVALUE, StatisticRelationId,
};
use crate::shmem::AutoVacuumingActive;
use backend_postmaster_autovacuum_ext_seams as seam;

/// `static autovac_table *table_recheck_autovac(...)` (`autovacuum.c` lines
/// 2748-2889).
///
/// Recheck whether a table still needs vacuum or analyze. Returns `Some` if it
/// does, `None` otherwise. The returned [`AutovacTable`] does not have the name
/// fields set.
pub fn table_recheck_autovac(
    relid: Oid,
    table_toast_map: &[AvRelation],
    effective_multixact_freeze_max_age: i32,
) -> PgResult<Option<AutovacTable>> {
    /* fetch the relation's relcache entry */
    let class_row: RecheckClassRow = match seam::recheck_fetch_class_row::call(relid) {
        Some(r) => r,
        None => return Ok(None),
    };

    /*
     * Get the applicable reloptions.  If it is a TOAST table, try to get the
     * main table reloptions if the toast table itself doesn't have.
     */
    let mut avopts: Option<AutoVacOpts> =
        core::extract_autovac_opts(class_row.relkind, class_row.relopts);
    if avopts.is_none() && class_row.relkind == RELKIND_TOASTVALUE {
        if let Some(hentry) = table_toast_map.iter().find(|h| h.ar_toastrelid == relid) {
            if hentry.ar_hasrelopts {
                avopts = Some(hentry.ar_reloptions);
            }
        }
    }

    let (dovacuum, doanalyze, wraparound) = recheck_relation_needs_vacanalyze(
        relid,
        avopts.as_ref(),
        &class_row,
        effective_multixact_freeze_max_age,
    );

    let mut tab: Option<AutovacTable> = None;

    /* OK, it needs something done */
    if doanalyze || dovacuum {
        let av = avopts.as_ref();

        /* -1 in autovac setting means use log_autovacuum_min_duration */
        let log_min_duration = if av.is_some_and(|a| a.log_min_duration >= 0) {
            av.unwrap().log_min_duration
        } else {
            core::Log_autovacuum_min_duration()
        };

        /* these do not have autovacuum-specific settings */
        let freeze_min_age = if av.is_some_and(|a| a.freeze_min_age >= 0) {
            av.unwrap().freeze_min_age
        } else {
            core::default_freeze_min_age()
        };

        let freeze_table_age = if av.is_some_and(|a| a.freeze_table_age >= 0) {
            av.unwrap().freeze_table_age
        } else {
            core::default_freeze_table_age()
        };

        let multixact_freeze_min_age = if av.is_some_and(|a| a.multixact_freeze_min_age >= 0) {
            av.unwrap().multixact_freeze_min_age
        } else {
            core::default_multixact_freeze_min_age()
        };

        let multixact_freeze_table_age = if av.is_some_and(|a| a.multixact_freeze_table_age >= 0) {
            av.unwrap().multixact_freeze_table_age
        } else {
            core::default_multixact_freeze_table_age()
        };

        let mut at = AutovacTable {
            at_relid: relid,
            at_sharedrel: class_row.relisshared,
            ..AutovacTable::default()
        };

        /*
         * Select VACUUM options.  Note we don't say VACOPT_PROCESS_TOAST, so
         * that vacuum() skips toast relations.  Also note we tell vacuum() to
         * skip vac_update_datfrozenxid(); we'll do that separately.
         */
        let options: bits32 = (if dovacuum {
            VACOPT_VACUUM | VACOPT_PROCESS_MAIN | VACOPT_SKIP_DATABASE_STATS
        } else {
            0
        }) | (if doanalyze { VACOPT_ANALYZE } else { 0 })
            | (if !wraparound { VACOPT_SKIP_LOCKED } else { 0 });

        at.at_params = VacuumParams {
            options,
            index_cleanup: VacOptValue::VACOPTVALUE_UNSPECIFIED,
            truncate: VacOptValue::VACOPTVALUE_UNSPECIFIED,
            /* As of now, we don't support parallel vacuum for autovacuum */
            nworkers: -1,
            freeze_min_age,
            freeze_table_age,
            multixact_freeze_min_age,
            multixact_freeze_table_age,
            is_wraparound: wraparound,
            log_min_duration,
            toast_parent: types_core::InvalidOid,
            /*
             * Later, in vacuum_rel(), we check reloptions for any
             * vacuum_max_eager_freeze_failure_rate override.
             */
            max_eager_freeze_failure_rate: seam::vacuum_max_eager_freeze_failure_rate::call(),
        };

        at.at_storage_param_vac_cost_limit = av.map(|a| a.vacuum_cost_limit).unwrap_or(0);
        at.at_storage_param_vac_cost_delay = av.map(|a| a.vacuum_cost_delay).unwrap_or(-1.0);
        at.at_relname = None;
        at.at_nspname = None;
        at.at_datname = None;

        /*
         * If any of the cost delay parameters has been set individually for
         * this table, disable the balancing algorithm.
         */
        at.at_dobalance =
            !av.is_some_and(|a| a.vacuum_cost_limit > 0 || a.vacuum_cost_delay >= 0.0);

        tab = Some(at);
    }

    Ok(tab)
}

/// `static void recheck_relation_needs_vacanalyze(...)` (`autovacuum.c` lines
/// 2899-2925).
///
/// Subroutine for `table_recheck_autovac`. Fetch the pgstat of a relation and
/// recheck whether it needs to be vacuumed or analyzed.
pub fn recheck_relation_needs_vacanalyze(
    relid: Oid,
    avopts: Option<&AutoVacOpts>,
    class_row: &RecheckClassRow,
    effective_multixact_freeze_max_age: i32,
) -> (bool, bool, bool) {
    /* fetch the pgstat table entry */
    let tabentry = seam::pgstat_fetch_stat_tabentry::call(class_row.relisshared, relid);

    let (dovacuum, mut doanalyze, wraparound) = relation_needs_vacanalyze(
        relid,
        avopts,
        class_row.relkind,
        class_row.relfrozenxid,
        class_row.relminmxid,
        class_row.reltuples,
        class_row.relpages,
        class_row.relallfrozen,
        &class_row.relname,
        tabentry,
        effective_multixact_freeze_max_age,
    );

    /* ignore ANALYZE for toast tables */
    if class_row.relkind == RELKIND_TOASTVALUE {
        doanalyze = false;
    }

    (dovacuum, doanalyze, wraparound)
}

/// `static void relation_needs_vacanalyze(...)` (`autovacuum.c` lines
/// 2966-3163).
///
/// Decide whether a relation needs vacuum/analyze. Returns
/// `(dovacuum, doanalyze, wraparound)`.
#[allow(clippy::too_many_arguments)]
pub fn relation_needs_vacanalyze(
    relid: Oid,
    relopts: Option<&AutoVacOpts>,
    _relkind: u8,
    relfrozenxid: TransactionId,
    relminmxid: MultiXactId,
    reltuples_in: f32,
    relpages: i32,
    relallfrozen_in: i32,
    relname: &str,
    tabentry: Option<TabStatEntry>,
    effective_multixact_freeze_max_age: i32,
) -> (bool, bool, bool) {
    /*
     * Determine vacuum/analyze equation parameters.  We have two possible
     * sources: the passed reloptions, or the autovacuum GUC variables.
     */

    /* -1 in autovac setting means use plain vacuum_scale_factor */
    let vac_scale_factor: f32 = if relopts.is_some_and(|r| r.vacuum_scale_factor >= 0.0) {
        relopts.unwrap().vacuum_scale_factor as f32
    } else {
        core::autovacuum_vac_scale() as f32
    };

    let vac_base_thresh: i32 = if relopts.is_some_and(|r| r.vacuum_threshold >= 0) {
        relopts.unwrap().vacuum_threshold
    } else {
        core::autovacuum_vac_thresh()
    };

    /* -1 is used to disable max threshold */
    let vac_max_thresh: i32 = if relopts.is_some_and(|r| r.vacuum_max_threshold >= -1) {
        relopts.unwrap().vacuum_max_threshold
    } else {
        core::autovacuum_vac_max_thresh()
    };

    let vac_ins_scale_factor: f32 = if relopts.is_some_and(|r| r.vacuum_ins_scale_factor >= 0.0) {
        relopts.unwrap().vacuum_ins_scale_factor as f32
    } else {
        core::autovacuum_vac_ins_scale() as f32
    };

    /* -1 is used to disable insert vacuums */
    let vac_ins_base_thresh: i32 = if relopts.is_some_and(|r| r.vacuum_ins_threshold >= -1) {
        relopts.unwrap().vacuum_ins_threshold
    } else {
        core::autovacuum_vac_ins_thresh()
    };

    let anl_scale_factor: f32 = if relopts.is_some_and(|r| r.analyze_scale_factor >= 0.0) {
        relopts.unwrap().analyze_scale_factor as f32
    } else {
        core::autovacuum_anl_scale() as f32
    };

    let anl_base_thresh: i32 = if relopts.is_some_and(|r| r.analyze_threshold >= 0) {
        relopts.unwrap().analyze_threshold
    } else {
        core::autovacuum_anl_thresh()
    };

    let freeze_max_age: i32 = if relopts.is_some_and(|r| r.freeze_max_age >= 0) {
        ::core::cmp::min(relopts.unwrap().freeze_max_age, core::autovacuum_freeze_max_age())
    } else {
        core::autovacuum_freeze_max_age()
    };

    let multixact_freeze_max_age: i32 = if relopts.is_some_and(|r| r.multixact_freeze_max_age >= 0) {
        ::core::cmp::min(
            relopts.unwrap().multixact_freeze_max_age,
            effective_multixact_freeze_max_age,
        )
    } else {
        effective_multixact_freeze_max_age
    };

    let av_enabled: bool = relopts.map(|r| r.enabled).unwrap_or(true);

    /* Force vacuum if table is at risk of wraparound */
    let mut xid_force_limit = core::recentXid().wrapping_sub(freeze_max_age as u32);
    if xid_force_limit < FirstNormalTransactionId {
        xid_force_limit = xid_force_limit.wrapping_sub(FirstNormalTransactionId);
    }
    let mut force_vacuum: bool =
        TransactionIdIsNormal(relfrozenxid) && TransactionIdPrecedes(relfrozenxid, xid_force_limit);
    if !force_vacuum {
        let mut multi_force_limit =
            core::recentMulti().wrapping_sub(multixact_freeze_max_age as u32);
        if multi_force_limit < FirstMultiXactId {
            multi_force_limit = multi_force_limit.wrapping_sub(FirstMultiXactId);
        }
        force_vacuum =
            MultiXactIdIsValid(relminmxid) && MultiXactIdPrecedes(relminmxid, multi_force_limit);
    }
    let wraparound = force_vacuum;

    /* User disabled it in pg_class.reloptions?  (But ignore if at risk) */
    if !av_enabled && !force_vacuum {
        return (false, false, wraparound);
    }

    let dovacuum;
    let doanalyze;

    /*
     * If we found stats for the table, and autovacuum is currently enabled,
     * make a threshold-based decision whether to vacuum and/or analyze.  If
     * autovacuum is currently disabled, we must be here for anti-wraparound
     * vacuuming only, so don't vacuum (or analyze) anything that's not being
     * forced.
     */
    if let Some(tabentry) = tabentry.filter(|_| AutoVacuumingActive()) {
        let mut pcnt_unfrozen: f32 = 1.0;
        let mut reltuples: f32 = reltuples_in;
        let mut relallfrozen: i32 = relallfrozen_in;

        let vactuples: f32 = tabentry.dead_tuples;
        let instuples: f32 = tabentry.ins_since_vacuum;
        let anltuples: f32 = tabentry.mod_since_analyze;

        /* If the table hasn't yet been vacuumed, take reltuples as zero */
        if reltuples < 0.0 {
            reltuples = 0.0;
        }

        /*
         * If we have data for relallfrozen, calculate the unfrozen percentage
         * of the table to modify insert scale factor.
         */
        if relpages > 0 && relallfrozen > 0 {
            /*
             * It could be the stats were updated manually and relallfrozen >
             * relpages. Clamp relallfrozen to relpages to avoid nonsensical
             * calculations.
             */
            relallfrozen = ::core::cmp::min(relallfrozen, relpages);
            pcnt_unfrozen = 1.0 - (relallfrozen as f32 / relpages as f32);
        }

        let mut vacthresh: f32 = vac_base_thresh as f32 + vac_scale_factor * reltuples;
        if vac_max_thresh >= 0 && vacthresh > vac_max_thresh as f32 {
            vacthresh = vac_max_thresh as f32;
        }

        let vacinsthresh: f32 =
            vac_ins_base_thresh as f32 + vac_ins_scale_factor * reltuples * pcnt_unfrozen;
        let anlthresh: f32 = anl_base_thresh as f32 + anl_scale_factor * reltuples;

        /*
         * Note that we don't need to take special consideration for stat
         * reset, because if that happens, the last vacuum and analyze counts
         * will be reset too.
         */
        if vac_ins_base_thresh >= 0 {
            elog(
                DEBUG3,
                format!(
                    "{relname}: vac: {vactuples:.0} (threshold {vacthresh:.0}), ins: {instuples:.0} (threshold {vacinsthresh:.0}), anl: {anltuples:.0} (threshold {anlthresh:.0})"
                ),
            )
            .ok();
        } else {
            elog(
                DEBUG3,
                format!(
                    "{relname}: vac: {vactuples:.0} (threshold {vacthresh:.0}), ins: (disabled), anl: {anltuples:.0} (threshold {anlthresh:.0})"
                ),
            )
            .ok();
        }

        /* Determine if this table needs vacuum or analyze. */
        dovacuum = force_vacuum
            || (vactuples > vacthresh)
            || (vac_ins_base_thresh >= 0 && instuples > vacinsthresh);
        doanalyze = anltuples > anlthresh;
    } else {
        /*
         * Skip a table not found in stat hash, unless we have to force vacuum
         * for anti-wrap purposes.
         */
        dovacuum = force_vacuum;
        doanalyze = false;
    }

    /* ANALYZE refuses to work with pg_statistic */
    let doanalyze = if relid == StatisticRelationId {
        false
    } else {
        doanalyze
    };

    (dovacuum, doanalyze, wraparound)
}

/// `static void perform_work_item(AutoVacuumWorkItem *workitem)` (`autovacuum.c`
/// lines 2604-2705).
///
/// Execute a previously registered work item, addressed by its array index `i`.
pub fn perform_work_item(i: i32) -> PgResult<()> {
    /*
     * Save the relation name for a possible error message, to avoid a catalog
     * lookup in case of an error.  If any of these return NULL, then the
     * relation has been dropped since last we checked; skip it.
     */
    let avw_relation = seam::workitem_get_relation::call(i);
    let cur_relname: Option<String> = seam::get_rel_name::call(avw_relation);
    let cur_nspname: Option<String> = seam::get_rel_namespace_name::call(avw_relation);
    let cur_datname: Option<String> = seam::get_database_name::call(seam::my_database_id::call());

    if cur_relname.is_none() || cur_nspname.is_none() || cur_datname.is_none() {
        /* deleted2: */
        return Ok(());
    }
    let cur_datname = cur_datname.unwrap();
    let cur_nspname = cur_nspname.unwrap();
    let cur_relname = cur_relname.unwrap();

    autovac_report_workitem(i, &cur_nspname, &cur_relname);

    /* clean up memory before each work item */
    seam::portal_context_reset::call();

    /*
     * We will abort the current work item if something errors out, and
     * continue with the next one; in particular, this happens if we are
     * interrupted with SIGINT.  Note that this means that the work item list
     * can be lossy.
     *
     * The C body runs the dispatch in PG_TRY; on error PG_CATCH adorns the
     * in-flight error with autovacuum's errcontext line, emits it, aborts and
     * restarts the transaction, and proceeds with the next work item.  Here
     * the closure plays the PG_TRY role and the Err arm plays PG_CATCH (the
     * HOLD_INTERRUPTS/EmitErrorReport/AbortOutOfAnyTransaction/FlushErrorState/
     * MemoryContextReset(PortalContext)/StartTransactionCommand/RESUME_INTERRUPTS
     * sequence is the foreign seam body).
     */
    let result: PgResult<()> = (|| {
        /*
         * Have at it.  Functions called here are responsible for any required
         * user switch and sandbox.
         */
        match seam::workitem_get_type::call(i) {
            x if x == AVW_BRINSummarizeRange => {
                let blkno = seam::workitem_get_block_number::call(i);
                seam::perform_brin_summarize_range::call(avw_relation, blkno)?;
            }
            other => {
                elog(WARNING, format!("unrecognized work item found: type {other}")).ok();
            }
        }
        Ok(())
    })();

    match result {
        Ok(()) => {
            /*
             * Clear a possible query-cancel signal, to avoid a late reaction to
             * an automatically-sent signal because of vacuuming the current
             * table (we're done with it, so it would make no sense to cancel at
             * this point.)
             */
            seam::set_query_cancel_pending::call(false);
        }
        Err(mut err) => {
            /*
             * Abort the transaction, start a new one, and proceed with the next
             * work item; adorn the in-flight error with autovacuum's errcontext
             * line first.
             */
            err.add_context_line(format!(
                "processing work entry for relation \"{}.{}.{}\"",
                cur_datname, cur_nspname, cur_relname
            ));
            seam::emit_report_and_restart_after_table_error::call(err);
        }
    }

    /* Make sure we're back in AutovacMemCxt */
    seam::switch_to_autovac_mem_cxt::call();

    /* We intentionally do not set did_vacuum here */

    Ok(())
}

/// `MAX_AUTOVAC_ACTIV_LEN` (`autovacuum.c` line 3215) — `NAMEDATALEN * 2 + 56`.
const NAMEDATALEN: usize = 64;
pub(crate) const MAX_AUTOVAC_ACTIV_LEN: usize = NAMEDATALEN * 2 + 56;

/// `static void autovac_report_activity(autovac_table *tab)` (`autovacuum.c`
/// lines 3212-3241).
///
/// Report to pgstat what autovacuum is doing — a SQL string mirroring the
/// equivalent manual command.
pub fn autovac_report_activity(tab: &AutovacTable) {
    /* Report the command and possible options */
    let mut activity = if tab.at_params.options & VACOPT_VACUUM != 0 {
        format!(
            "autovacuum: VACUUM{}",
            if tab.at_params.options & VACOPT_ANALYZE != 0 {
                " ANALYZE"
            } else {
                ""
            }
        )
    } else {
        String::from("autovacuum: ANALYZE")
    };

    /*
     * Report the qualified name of the relation.
     *
     * C does `snprintf(activity + len, MAX_AUTOVAC_ACTIV_LEN - len, ...)`, so
     * the suffix is bounded to keep the whole string within
     * `MAX_AUTOVAC_ACTIV_LEN - 1` (the trailing NUL).
     */
    snprintf_append(
        &mut activity,
        MAX_AUTOVAC_ACTIV_LEN,
        &format!(
            " {}.{}{}",
            tab.at_nspname.as_deref().unwrap_or(""),
            tab.at_relname.as_deref().unwrap_or(""),
            if tab.at_params.is_wraparound {
                " (to prevent wraparound)"
            } else {
                ""
            }
        ),
    );

    /* Set statement_timestamp() to current time, then report (via the seam). */
    seam::pgstat_report_activity_running::call(activity);
}

/// `static void autovac_report_workitem(...)` (`autovacuum.c` lines 3247-3280).
///
/// Report to pgstat that autovacuum is processing a work item, addressed by its
/// array index `i`.
pub fn autovac_report_workitem(i: i32, nspname: &str, relname: &str) {
    let mut activity = match seam::workitem_get_type::call(i) {
        x if x == AVW_BRINSummarizeRange => String::from("autovacuum: BRIN summarize"),
        _ => String::new(),
    };

    /*
     * Report the qualified name of the relation, and the block number if any
     */
    let avw_block_number = seam::workitem_get_block_number::call(i);
    let blk = if BlockNumberIsValid(avw_block_number) {
        format!(" {avw_block_number}")
    } else {
        String::new()
    };

    /*
     * C does `snprintf(activity + len, MAX_AUTOVAC_ACTIV_LEN - len, ...)`: the
     * prefix is first capped to MAX (via its own snprintf), then the suffix is
     * bounded to keep the total within `MAX_AUTOVAC_ACTIV_LEN - 1`.  The
     * oversized `activity[MAX + 12 + 2]` buffer never matters because the
     * second snprintf's capacity is `MAX - len`.
     */
    snprintf_append(&mut activity, MAX_AUTOVAC_ACTIV_LEN, &format!(" {nspname}.{relname}{blk}"));

    /* Set statement_timestamp() to current time, then report (via the seam). */
    seam::pgstat_report_activity_running::call(activity);
}

/// Mirror C's `snprintf(activity + len, cap - len, "%s...", suffix)`: append
/// `suffix` to `s`, but bound the total length so that the C buffer of `cap`
/// bytes (including its trailing NUL) would not overflow.  C stores at most
/// `cap - 1` content bytes, so the result is truncated to `cap - 1` bytes (on a
/// char boundary).  If `s` is already at/over the bound, nothing is appended.
pub(crate) fn snprintf_append(s: &mut String, cap: usize, suffix: &str) {
    let limit = cap.saturating_sub(1);
    if s.len() >= limit {
        s.truncate(char_boundary_floor(s, limit));
        return;
    }
    let room = limit - s.len();
    if suffix.len() <= room {
        s.push_str(suffix);
    } else {
        s.push_str(&suffix[..char_boundary_floor(suffix, room)]);
    }
}

/// Largest index `<= n` that is a char boundary of `s`.
fn char_boundary_floor(s: &str, n: usize) -> usize {
    let mut end = n.min(s.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    end
}
