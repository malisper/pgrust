use super::*;
use types_nodes::plannodes::PlannedStmt;

fn select_stmt() -> PlannedStmt<'static> {
    PlannedStmt {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        ..PlannedStmt::default()
    }
}

#[test]
fn choose_strategy_one_select() {
    let stmts = [select_stmt()];
    assert_eq!(ChoosePortalStrategy(&stmts), PORTAL_ONE_SELECT);
}

#[test]
fn choose_strategy_mod_with() {
    let mut s = select_stmt();
    s.hasModifyingCTE = true;
    assert_eq!(ChoosePortalStrategy(&[s]), PORTAL_ONE_MOD_WITH);
}

#[test]
fn choose_strategy_empty_is_multi() {
    assert_eq!(ChoosePortalStrategy(&[]), PORTAL_MULTI_QUERY);
}

#[test]
fn choose_strategy_returning() {
    let ins = PlannedStmt {
        commandType: CmdType::CMD_INSERT,
        canSetTag: true,
        hasReturning: true,
        ..PlannedStmt::default()
    };
    assert_eq!(ChoosePortalStrategy(&[ins]), PORTAL_ONE_RETURNING);
    // Two canSetTag statements collapse to MULTI.
    let two = [
        PlannedStmt {
            commandType: CmdType::CMD_INSERT,
            canSetTag: true,
            hasReturning: true,
            ..PlannedStmt::default()
        },
        PlannedStmt {
            commandType: CmdType::CMD_INSERT,
            canSetTag: true,
            hasReturning: true,
            ..PlannedStmt::default()
        },
    ];
    assert_eq!(ChoosePortalStrategy(&two), PORTAL_MULTI_QUERY);
    // A canSetTag stmt without RETURNING collapses too.
    let no_ret = [PlannedStmt {
        commandType: CmdType::CMD_UPDATE,
        canSetTag: true,
        ..PlannedStmt::default()
    }];
    assert_eq!(ChoosePortalStrategy(&no_ret), PORTAL_MULTI_QUERY);
}

#[test]
fn primary_stmt_is_can_set_tag() {
    let aux = PlannedStmt {
        commandType: CmdType::CMD_INSERT,
        canSetTag: false,
        ..PlannedStmt::default()
    };
    let stmts = [aux, select_stmt()];
    assert_eq!(PortalGetPrimaryStmt(&stmts), Some(1));
    assert_eq!(PortalGetPrimaryStmt(&[]), None);
}

#[test]
fn planned_stmt_requires_snapshot_non_utility() {
    assert!(PlannedStmtRequiresSnapshot(&select_stmt()));
}

#[test]
fn stmt_list_roundtrip_and_staleness() {
    let stmts = vec![select_stmt()];
    // SAFETY: `stmts` outlives the handle; freed below before drop.
    let h = unsafe { stmt_list::register(&stmts) };
    assert!(stmt_list::is_live(h));
    let n = stmt_list::with(h, |s| s.len());
    assert_eq!(n, 1);
    // Re-entrant access must not deadlock/panic.
    stmt_list::with(h, |_| stmt_list::with(h, |s| assert!(s[0].canSetTag)));
    stmt_list::free(h);
    assert!(!stmt_list::is_live(h));
    let stale = std::panic::catch_unwind(|| stmt_list::with(h, |s| s.len()));
    assert!(stale.is_err());
    assert!(!stmt_list::is_live(types_portal::StmtListHandle::NULL));
}

#[test]
fn stmt_list_reset_all_clears() {
    let stmts = vec![select_stmt()];
    let h = unsafe { stmt_list::register(&stmts) };
    stmt_list::reset_all();
    assert!(!stmt_list::is_live(h));
}
