//! Tests for the seam-free owned algorithms: the `relation_needs_vacanalyze`
//! threshold/wraparound decision math, the `check_autovacuum_work_mem` GUC
//! clamp, and `AutoVacuumingActive`. The cross-subsystem paths are exercised by
//! the unit's smoke test, not here.

use crate::core::{self, RELKIND_RELATION, RELKIND_TOASTVALUE};
use crate::schedule::relation_needs_vacanalyze;
use crate::shmem::{check_autovacuum_work_mem, AutoVacuumingActive};
use types_autovacuum::TabStatEntry;
use types_reloptions::AutoVacOpts;

/// Seed the GUC globals to PostgreSQL's defaults and clear the worker/wrap
/// state so the threshold math runs deterministically.
fn seed_default_gucs() {
    install_track_counts_seam();
    core::set_autovacuum_start_daemon(true);
    core::set_pgstat_track_counts(true);
    core::set_autovacuum_vac_thresh(50);
    core::set_autovacuum_vac_max_thresh(100_000_000);
    core::set_autovacuum_vac_scale(0.2);
    core::set_autovacuum_vac_ins_thresh(1000);
    core::set_autovacuum_vac_ins_scale(0.2);
    core::set_autovacuum_anl_thresh(50);
    core::set_autovacuum_anl_scale(0.1);
    core::set_autovacuum_freeze_max_age(200_000_000);
    core::set_autovacuum_multixact_freeze_max_age(400_000_000);
    // No wraparound pressure: recent xid/multi just past the normal floor.
    core::set_recentXid(1000);
    core::set_recentMulti(1000);
}

fn tabentry(dead: f32, ins: f32, modified: f32) -> Option<TabStatEntry> {
    Some(TabStatEntry {
        dead_tuples: dead,
        ins_since_vacuum: ins,
        mod_since_analyze: modified,
    })
}

#[test]
fn rnv_no_stats_no_force_skips() {
    seed_default_gucs();
    // No tabentry => skip unless forced for wraparound (not forced here).
    let (dovacuum, doanalyze, wraparound) = relation_needs_vacanalyze(
        16384, None, RELKIND_RELATION, 500, 1, 1000.0, 10, 0, "t", None, 400_000_000,
    );
    assert!(!dovacuum);
    assert!(!doanalyze);
    assert!(!wraparound);
}

#[test]
fn rnv_dead_tuples_over_threshold_triggers_vacuum() {
    seed_default_gucs();
    // reltuples = 1000 => vacthresh = 50 + 0.2*1000 = 250; dead = 300 > 250.
    let (dovacuum, doanalyze, wraparound) = relation_needs_vacanalyze(
        16384, None, RELKIND_RELATION, 500, 1, 1000.0, 10, 0, "t",
        tabentry(300.0, 0.0, 0.0), 400_000_000,
    );
    assert!(dovacuum);
    assert!(!doanalyze);
    assert!(!wraparound);
}

#[test]
fn rnv_dead_tuples_under_threshold_no_vacuum() {
    seed_default_gucs();
    // dead = 100 < 250 => no vacuum; anlthresh = 150, mod 0 < 150.
    let (dovacuum, doanalyze, _w) = relation_needs_vacanalyze(
        16384, None, RELKIND_RELATION, 500, 1, 1000.0, 10, 0, "t",
        tabentry(100.0, 0.0, 0.0), 400_000_000,
    );
    assert!(!dovacuum);
    assert!(!doanalyze);
}

#[test]
fn rnv_modified_over_threshold_triggers_analyze() {
    seed_default_gucs();
    // anlthresh = 50 + 0.1*1000 = 150; mod = 200 > 150 => doanalyze.
    let (dovacuum, doanalyze, _w) = relation_needs_vacanalyze(
        16384, None, RELKIND_RELATION, 500, 1, 1000.0, 10, 0, "t",
        tabentry(0.0, 0.0, 200.0), 400_000_000,
    );
    assert!(!dovacuum);
    assert!(doanalyze);
}

#[test]
fn rnv_inserts_over_threshold_triggers_vacuum() {
    seed_default_gucs();
    // relpages 0 => pcnt_unfrozen stays 1.0.
    // vacinsthresh = 1000 + 0.2*1000*1.0 = 1200; ins = 1500 > 1200 => dovacuum.
    let (dovacuum, _doanalyze, _w) = relation_needs_vacanalyze(
        16384, None, RELKIND_RELATION, 500, 1, 1000.0, 0, 0, "t",
        tabentry(0.0, 1500.0, 0.0), 400_000_000,
    );
    assert!(dovacuum);
}

#[test]
fn rnv_wraparound_forces_vacuum_without_stats() {
    seed_default_gucs();
    // Put recentXid well ahead so xid_force_limit = 300M - 200M = 100M is a
    // normal xid; a relfrozenxid of 50 precedes it => force_vacuum, even with
    // no stats entry.
    core::set_recentXid(300_000_000);
    let (dovacuum, _doanalyze, wraparound) = relation_needs_vacanalyze(
        16384, None, RELKIND_RELATION, 50, 1, 1000.0, 10, 0, "t", None, 400_000_000,
    );
    assert!(dovacuum);
    assert!(wraparound);
}

#[test]
fn rnv_disabled_reloption_skips_unless_forced() {
    seed_default_gucs();
    let opts = AutoVacOpts {
        enabled: false,
        vacuum_threshold: -1,
        vacuum_max_threshold: -1,
        vacuum_ins_threshold: -1,
        analyze_threshold: -1,
        vacuum_cost_limit: 0,
        freeze_min_age: -1,
        freeze_max_age: -1,
        freeze_table_age: -1,
        multixact_freeze_min_age: -1,
        multixact_freeze_max_age: -1,
        multixact_freeze_table_age: -1,
        log_min_duration: -1,
        vacuum_cost_delay: -1.0,
        vacuum_scale_factor: -1.0,
        vacuum_ins_scale_factor: -1.0,
        analyze_scale_factor: -1.0,
    };
    // av_enabled = false, not at risk => (false, false, false) regardless of
    // huge dead-tuple count.
    let (dovacuum, doanalyze, wraparound) = relation_needs_vacanalyze(
        16384, Some(&opts), RELKIND_RELATION, 500, 1, 1000.0, 10, 0, "t",
        tabentry(1_000_000.0, 0.0, 0.0), 400_000_000,
    );
    assert!(!dovacuum);
    assert!(!doanalyze);
    assert!(!wraparound);
}

#[test]
fn rnv_pg_statistic_never_analyzed() {
    seed_default_gucs();
    // relid == StatisticRelationId (2619) => doanalyze forced false even though
    // mod is over threshold.
    let (_dovacuum, doanalyze, _w) = relation_needs_vacanalyze(
        2619, None, RELKIND_RELATION, 500, 1, 1000.0, 10, 0, "pg_statistic",
        tabentry(0.0, 0.0, 1000.0), 400_000_000,
    );
    assert!(!doanalyze);
}

#[test]
fn rnv_toast_relkind_decision_math_runs() {
    // relkind passed in is ignored by relation_needs_vacanalyze itself (the
    // ANALYZE-suppression for toast happens in recheck_relation_needs_vacanalyze);
    // confirm the math still produces a vacuum decision for a toast relkind.
    seed_default_gucs();
    let (dovacuum, _doanalyze, _w) = relation_needs_vacanalyze(
        16384, None, RELKIND_TOASTVALUE, 500, 1, 1000.0, 10, 0, "t",
        tabentry(300.0, 0.0, 0.0), 400_000_000,
    );
    assert!(dovacuum);
}

#[test]
fn work_mem_clamp() {
    assert_eq!(check_autovacuum_work_mem(-1), (-1, true)); // fallback untouched
    assert_eq!(check_autovacuum_work_mem(32), (64, true)); // clamped up to 64kB
    assert_eq!(check_autovacuum_work_mem(64), (64, true));
    assert_eq!(check_autovacuum_work_mem(4096), (4096, true));
}

#[test]
fn extract_autovac_opts_projects_autovacuum_subfield() {
    use types_reloptions::StdRdOptions;

    // C: relopts == NULL -> NULL.
    assert_eq!(core::extract_autovac_opts(RELKIND_RELATION, None), None);

    // C: memcpy(av, &((StdRdOptions *) relopts)->autovacuum, ...) — only the
    // .autovacuum sub-struct is projected out, not the surrounding fields.
    let av = AutoVacOpts {
        enabled: false,
        vacuum_threshold: 42,
        vacuum_cost_delay: 7.5,
        ..AutoVacOpts::default()
    };
    let opts = StdRdOptions {
        fillfactor: 90,
        autovacuum: av,
        ..StdRdOptions::default()
    };
    assert_eq!(
        core::extract_autovac_opts(RELKIND_RELATION, Some(opts)),
        Some(av)
    );
}

#[test]
fn snprintf_append_bounds_total_like_c() {
    use crate::schedule::{snprintf_append, MAX_AUTOVAC_ACTIV_LEN};

    // Suffix fits within cap-1: appended verbatim.
    let mut s = alloc::string::String::from("abc");
    snprintf_append(&mut s, 8, " def");
    assert_eq!(s, "abc def");

    // Total would exceed cap-1: suffix is truncated so len == cap-1.
    let mut s = alloc::string::String::from("abc");
    snprintf_append(&mut s, 6, " defghi");
    assert_eq!(s, "abc d"); // 5 bytes == cap (6) - 1

    // The autovac_report_workitem prefix never overflows the real cap; a long
    // nsp.rel suffix gets bounded to MAX_AUTOVAC_ACTIV_LEN - 1.
    let mut s = alloc::string::String::from("autovacuum: BRIN summarize");
    let long = alloc::format!(" {}.{} 0", "n".repeat(200), "r".repeat(200));
    snprintf_append(&mut s, MAX_AUTOVAC_ACTIV_LEN, &long);
    assert_eq!(s.len(), MAX_AUTOVAC_ACTIV_LEN - 1);
}

/// Install the `pgstat_track_counts` ext-seam so `AutoVacuumingActive()` reads
/// the test-controllable local cell (production installs it against pgstat's
/// live GUC value from `seams-init`). Idempotent — `set` overwrites.
fn install_track_counts_seam() {
    if !autovacuum_ext_seams::pgstat_track_counts::is_installed() {
        autovacuum_ext_seams::pgstat_track_counts::set(core::pgstat_track_counts);
    }
}

#[test]
fn autovacuuming_active_requires_both_gucs() {
    install_track_counts_seam();
    core::set_autovacuum_start_daemon(false);
    core::set_pgstat_track_counts(false);
    assert!(!AutoVacuumingActive());

    core::set_autovacuum_start_daemon(true);
    core::set_pgstat_track_counts(false);
    assert!(!AutoVacuumingActive());

    core::set_autovacuum_start_daemon(true);
    core::set_pgstat_track_counts(true);
    assert!(AutoVacuumingActive());
}
