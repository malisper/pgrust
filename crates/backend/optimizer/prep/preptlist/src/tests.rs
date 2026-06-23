//! Unit tests for the `preptlist.c` SELECT-core port.
//!
//! These exercise `preprocess_targetlist` over a hand-built owned `Query<'mcx>`
//! with a SELECT targetlist of simple `Var` TLEs, asserting the resulting
//! `root.processed_tlist` carries faithful deep-cloned `TargetEntry` handles
//! into `root.node_arena`. They also cover the standalone `get_plan_rowmark` /
//! `extract_update_targetlist_colnos` helpers.

extern crate alloc;
use alloc::string::String;

use ::mcx::{alloc_in, MemoryContext};
use ::nodes::copy_query::Query;
use ::nodes::nodes::CmdType;
use ::nodes::primnodes::{Expr, TargetEntry, Var};
use ::pathnodes::{PlannerInfo, TargetEntryNode};

use crate::{extract_update_targetlist_colnos, get_plan_rowmark, preprocess_targetlist};

/// A simple `Var` expr leaf with the given `varattno`.
fn a_var(varattno: i16) -> Expr {
    Expr::Var(Var {
        varno: 1,
        varattno,
        vartype: 23, // INT4OID
        vartypmod: -1,
        location: -1,
        ..Default::default()
    })
}

/// Build a SELECT `TargetEntry` for `varattno` at result position `resno`.
fn a_tle<'mcx>(mcx: ::mcx::Mcx<'mcx>, varattno: i16, resno: i16, resjunk: bool) -> TargetEntry<'mcx> {
    TargetEntry {
        expr: Some(alloc_in(mcx, a_var(varattno)).unwrap()),
        resno,
        resname: None,
        ressortgroupref: 0,
        resorigtbl: 0,
        resorigcol: 0,
        resjunk,
    }
}

#[test]
fn select_targetlist_is_cloned_into_processed_tlist() {
    let cx = MemoryContext::new("preptlist-test");
    let mcx = cx.mcx();

    // SELECT with two output columns (Vars), one resname'd.
    let mut parse = Query::new(mcx);
    parse.commandType = CmdType::CMD_SELECT;
    parse.resultRelation = 0;
    let mut t0 = a_tle(mcx, 1, 1, false);
    t0.resname = Some(::mcx::PgString::from_str_in("a", mcx).unwrap());
    parse.targetList.push(t0);
    parse.targetList.push(a_tle(mcx, 2, 2, false));

    let mut root = PlannerInfo::default();

    preprocess_targetlist(mcx, &mut root, &mut parse, &[]).unwrap();

    // processed_tlist carries one NodeId handle per source TLE.
    assert_eq!(root.processed_tlist.len(), 2);

    // Each handle resolves to a TargetEntryNode in the arena with the source
    // TLE's scalar fields preserved and its expr deep-cloned (the expr handle
    // resolves to the cloned Var).
    let id0 = root.processed_tlist[0];
    let te0 = root.targetentry(id0);
    assert_eq!(te0.resno, 1);
    assert_eq!(te0.resname.as_deref(), Some("a"));
    assert!(!te0.resjunk);
    match root.node(te0.expr) {
        Expr::Var(v) => assert_eq!(v.varattno, 1),
        other => panic!("expected cloned Var, got {other:?}"),
    }

    let id1 = root.processed_tlist[1];
    let te1 = root.targetentry(id1);
    assert_eq!(te1.resno, 2);
    assert_eq!(te1.resname, None);
    match root.node(te1.expr) {
        Expr::Var(v) => assert_eq!(v.varattno, 2),
        other => panic!("expected cloned Var, got {other:?}"),
    }
}

#[test]
fn empty_select_targetlist_yields_empty_processed_tlist() {
    let cx = MemoryContext::new("preptlist-test");
    let mcx = cx.mcx();

    let mut parse = Query::new(mcx);
    parse.commandType = CmdType::CMD_SELECT;
    let mut root = PlannerInfo::default();

    preprocess_targetlist(mcx, &mut root, &mut parse, &[]).unwrap();
    assert!(root.processed_tlist.is_empty());
}

#[test]
fn get_plan_rowmark_resolves_rti() {
    let cx = MemoryContext::new("preptlist-test");
    let mcx = cx.mcx();
    let mut run = ::pathnodes::planner_run::PlannerRun::new(mcx);

    // Empty rowMarks (the plain SELECT path) finds nothing.
    let empty: alloc::vec::Vec<::pathnodes::PlanRowMarkId> = alloc::vec::Vec::new();
    assert!(get_plan_rowmark(&run, &empty, 1).is_none());

    // A FOR-UPDATE/SHARE rowmark for rti=3 is interned as a handle in
    // root.rowMarks; the lookup resolves the handle to compare rc->rti.
    let mk = |rti: u32| ::nodes::nodelockrows::PlanRowMark {
        type_: ::nodes::nodelockrows::T_PlanRowMark,
        rti: rti as types_core::Index,
        prti: rti as types_core::Index,
        rowmarkId: rti,
        markType: ::nodes::nodelockrows::ROW_MARK_REFERENCE,
        allMarkTypes: 1 << ::nodes::nodelockrows::ROW_MARK_REFERENCE,
        strength: ::nodes::rawnodes::LockClauseStrength::LCS_NONE as u32 as i32,
        waitPolicy: ::nodes::rawnodes::LockWaitPolicy::LockWaitBlock as u32 as i32,
        isParent: false,
    };
    let marks = alloc::vec![run.intern_rowmark(mk(3)), run.intern_rowmark(mk(5))];
    assert_eq!(get_plan_rowmark(&run, &marks, 3), Some(marks[0]));
    assert_eq!(get_plan_rowmark(&run, &marks, 5), Some(marks[1]));
    assert!(get_plan_rowmark(&run, &marks, 2).is_none());
}

#[test]
fn extract_update_colnos_collects_and_renumbers() {
    // Arena-level helper: non-resjunk resnos are collected as target colnos and
    // every TLE is renumbered to a consecutive 1..n.
    let mut root = PlannerInfo::default();
    // resno=3 (col 3), resjunk; resno=2 (col 2); resno=5 (col 5).
    let ids = alloc::vec![
        root.alloc_targetentry(TargetEntryNode {
            expr: ::pathnodes::NodeId(0),
            resno: 3,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        }),
        root.alloc_targetentry(TargetEntryNode {
            expr: ::pathnodes::NodeId(0),
            resno: 2,
            resname: Some(String::from("j")),
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: true,
        }),
        root.alloc_targetentry(TargetEntryNode {
            expr: ::pathnodes::NodeId(0),
            resno: 5,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        }),
    ];

    let colnos = extract_update_targetlist_colnos(&mut root, &ids);
    // Only the two non-resjunk source resnos, in order.
    assert_eq!(colnos, alloc::vec![3, 5]);
    // All renumbered 1,2,3.
    assert_eq!(root.targetentry(ids[0]).resno, 1);
    assert_eq!(root.targetentry(ids[1]).resno, 2);
    assert_eq!(root.targetentry(ids[2]).resno, 3);
}
