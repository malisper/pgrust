use super::*;

#[test]
fn workspace_preallocates_c_sizes_and_seams_install() {
    init_seams();
    init_small::globals::SetMaxBackends(16);
    deadlock_seams::init_dead_lock_checking::call().unwrap();
    WORKSPACE.with(|w| {
        let w = w.borrow();
        let ws = w.as_ref().unwrap();
        assert!(ws.visitedProcs.len() >= 16);
        assert!(ws.waitOrders.len() >= 8);
        assert!(ws.possibleConstraints.len() >= 64);
        assert_eq!(ws.maxCurConstraints, 16);
    });
    assert!(deadlock_seams::dead_lock_check::is_installed());
    assert!(deadlock_seams::dead_lock_report::is_installed());
    assert!(deadlock_seams::remember_simple_deadlock::is_installed());
    assert!(deadlock_seams::get_blocking_autovacuum_procno::is_installed());
}
