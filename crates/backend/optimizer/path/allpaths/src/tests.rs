use super::*;
use std::sync::Mutex;
use types_pathnodes::RELOPT_OTHER_MEMBER_REL;

// GUCs are process-global; every test takes the lock.
fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn with_rel<R>(f: impl FnOnce(&mut RelOptInfo<'_>) -> R) -> R {
    let cx = mcx::MemoryContext::new("allpaths-test");
    let mut rel = RelOptInfo::new(cx.mcx());
    rel.rel_parallel_workers = -1;
    f(&mut rel)
}

// The log3-rule tests below verify compute_parallel_worker's ALGORITHM against
// C's documented behavior, which is keyed to C's threshold defaults (1024
// table / 64 index pages). pgrust ships LOWER defaults (8MB->1MB, 512KB->64KB;
// docs/design/jit-parallel-defaults.md), so pin C's reference thresholds here
// to keep exercising the algorithm rather than the default value. Save/restore
// keeps the process-global GUC clean for sibling tests.
fn pin_c_thresholds() -> (i32, i32) {
    let save = (gucs::min_parallel_table_scan_size(), gucs::min_parallel_index_scan_size());
    gucs::set_min_parallel_table_scan_size(1024);
    gucs::set_min_parallel_index_scan_size(64);
    save
}

fn restore_thresholds((table, index): (i32, i32)) {
    gucs::set_min_parallel_table_scan_size(table);
    gucs::set_min_parallel_index_scan_size(index);
}

#[test]
fn heap_pages_log3_rule_matches_c() {
    let _g = test_lock();
    let save = pin_c_thresholds();
    with_rel(|rel| {
        for (pages, want) in [
            (0.0, 0),
            (1023.0, 0),
            (1024.0, 1),
            (3071.0, 1),
            (3072.0, 2),
            (9215.0, 2),
            (9216.0, 3),
            (27648.0, 4),
        ] {
            assert_eq!(
                compute_parallel_worker(rel, pages, -1.0, 8),
                want,
                "heap_pages={pages}"
            );
        }
    });
    restore_thresholds(save);
}

#[test]
fn index_pages_use_index_threshold() {
    let _g = test_lock();
    let save = pin_c_thresholds();
    with_rel(|rel| {
        assert_eq!(compute_parallel_worker(rel, -1.0, 63.0, 8), 0);
        assert_eq!(compute_parallel_worker(rel, -1.0, 64.0, 8), 1);
        assert_eq!(compute_parallel_worker(rel, -1.0, 191.0, 8), 1);
        assert_eq!(compute_parallel_worker(rel, -1.0, 192.0, 8), 2);
        assert_eq!(compute_parallel_worker(rel, -1.0, 576.0, 8), 3);
    });
    restore_thresholds(save);
}

#[test]
fn both_set_takes_min() {
    let _g = test_lock();
    let save = pin_c_thresholds();
    with_rel(|rel| {
        assert_eq!(compute_parallel_worker(rel, 9216.0, 64.0, 8), 1);
        assert_eq!(compute_parallel_worker(rel, 1024.0, 576.0, 8), 1);
        assert_eq!(compute_parallel_worker(rel, 9216.0, 576.0, 8), 3);
    });
    restore_thresholds(save);
}

#[test]
fn reloption_overrides_and_max_workers_clamps() {
    let _g = test_lock();
    with_rel(|rel| {
        rel.rel_parallel_workers = 5;
        assert_eq!(compute_parallel_worker(rel, 10.0, -1.0, 8), 5);
        assert_eq!(compute_parallel_worker(rel, 10.0, -1.0, 2), 2);
        rel.rel_parallel_workers = 0;
        assert_eq!(compute_parallel_worker(rel, 1_000_000.0, -1.0, 8), 0);
        rel.rel_parallel_workers = -1;
        assert_eq!(compute_parallel_worker(rel, 9216.0, -1.0, 2), 2);
        assert_eq!(compute_parallel_worker(rel, 9216.0, -1.0, 0), 0);
    });
}

#[test]
fn small_rel_gate_skipped_for_non_baserel() {
    let _g = test_lock();
    let save = pin_c_thresholds();
    with_rel(|rel| {
        assert_eq!(compute_parallel_worker(rel, 10.0, -1.0, 8), 0);
        assert_eq!(compute_parallel_worker(rel, 1024.0, 63.0, 8), 0);
        rel.reloptkind = RELOPT_OTHER_MEMBER_REL;
        assert_eq!(compute_parallel_worker(rel, 10.0, -1.0, 8), 1);
        assert_eq!(compute_parallel_worker(rel, -1.0, 10.0, 8), 1);
    });
    restore_thresholds(save);
}

#[test]
fn pgrcolumnar_rg_sizing_scales_linearly() {
    let _g = test_lock();
    with_rel(|rel| {
        // Below 4 RGs (~262k rows) a plain baserel plans serial.
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 0.0, 16), 0);
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 65_536.0, 16), 0);
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 196_608.0, 16), 0);
        // 4 RGs = 1 worker; linear in claim units from there.
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 262_144.0, 16), 1);
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 1_000_000.0, 16), 4);
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 4_000_000.0, 16), 15);
        // 10M rows = 153 RGs -> 38 pre-clamp: machine-sized on big banks.
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 10_000_000.0, 16), 16);
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 10_000_000.0, 64), 38);
    });
}

#[test]
fn pgrcolumnar_reloption_overrides_and_child_skips_gate() {
    let _g = test_lock();
    with_rel(|rel| {
        rel.rel_parallel_workers = 3;
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 10_000_000.0, 16), 3);
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 10_000_000.0, 2), 2);
        rel.rel_parallel_workers = 0;
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 10_000_000.0, 16), 0);
        rel.rel_parallel_workers = -1;
        // Inheritance children skip the small-rel gate (C parity).
        rel.reloptkind = RELOPT_OTHER_MEMBER_REL;
        assert_eq!(compute_pgrcolumnar_parallel_worker(rel, 65_536.0, 16), 1);
    });
}

#[test]
fn guc_changes_move_thresholds() {
    let _g = test_lock();
    let save = gucs::min_parallel_table_scan_size();
    gucs::set_min_parallel_table_scan_size(0);
    with_rel(|rel| {
        assert_eq!(compute_parallel_worker(rel, 2.0, -1.0, 8), 1);
        assert_eq!(compute_parallel_worker(rel, 3.0, -1.0, 8), 2);
        assert_eq!(compute_parallel_worker(rel, 9.0, -1.0, 8), 3);
    });
    gucs::set_min_parallel_table_scan_size(save);
}
