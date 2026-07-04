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

#[test]
fn heap_pages_log3_rule_matches_c() {
    let _g = test_lock();
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
}

#[test]
fn index_pages_use_index_threshold() {
    let _g = test_lock();
    with_rel(|rel| {
        assert_eq!(compute_parallel_worker(rel, -1.0, 63.0, 8), 0);
        assert_eq!(compute_parallel_worker(rel, -1.0, 64.0, 8), 1);
        assert_eq!(compute_parallel_worker(rel, -1.0, 191.0, 8), 1);
        assert_eq!(compute_parallel_worker(rel, -1.0, 192.0, 8), 2);
        assert_eq!(compute_parallel_worker(rel, -1.0, 576.0, 8), 3);
    });
}

#[test]
fn both_set_takes_min() {
    let _g = test_lock();
    with_rel(|rel| {
        assert_eq!(compute_parallel_worker(rel, 9216.0, 64.0, 8), 1);
        assert_eq!(compute_parallel_worker(rel, 1024.0, 576.0, 8), 1);
        assert_eq!(compute_parallel_worker(rel, 9216.0, 576.0, 8), 3);
    });
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
    with_rel(|rel| {
        assert_eq!(compute_parallel_worker(rel, 10.0, -1.0, 8), 0);
        assert_eq!(compute_parallel_worker(rel, 1024.0, 63.0, 8), 0);
        rel.reloptkind = RELOPT_OTHER_MEMBER_REL;
        assert_eq!(compute_parallel_worker(rel, 10.0, -1.0, 8), 1);
        assert_eq!(compute_parallel_worker(rel, -1.0, 10.0, 8), 1);
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
