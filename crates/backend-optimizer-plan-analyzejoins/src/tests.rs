//! Unit GATE for the #295 keystone: the planner relid-replacement walker
//! ([`crate::change_relids`]) that makes a planner `RestrictInfo` / arena clause
//! walkable for a change of RT index, mirroring C's
//! `ChangeVarNodesExtended((Node *) rinfo, relid, subst, 0, replace_relid_callback)`.

extern crate std;

use crate::change_relids::{
    change_relids_in_em, change_relids_in_rinfo, ReplaceRelidContext,
};
use crate::relids as r;

use types_nodes::primnodes::{Expr, Var};
use types_pathnodes::{
    Bitmapset, EquivalenceMember, PlannerInfo, RestrictInfo, RinfoId, VOLATILITY_UNKNOWN,
};

fn singleton(x: i32) -> types_pathnodes::Relids {
    let wn = (x / 64) as usize;
    let mut w = std::vec![0u64; wn + 1];
    w[wn] = 1u64 << (x % 64);
    Some(std::boxed::Box::new(Bitmapset { words: w }))
}

/// Intern a `Var(varno = vno)` clause node and a RestrictInfo over it whose
/// relid sets are exactly `{vno}`.
fn mk_var_rinfo(root: &mut PlannerInfo, vno: i32) -> RinfoId {
    let clause = root.alloc_node(Expr::Var(Var {
        varno: vno,
        varattno: 1,
        ..Var::default()
    }));
    let ri = RestrictInfo {
        clause,
        orclause: None,
        is_pushed_down: false,
        pseudoconstant: false,
        has_clone: false,
        is_clone: false,
        can_join: false,
        leakproof: false,
        has_volatile: VOLATILITY_UNKNOWN,
        security_level: 0,
        num_base_rels: 1,
        clause_relids: singleton(vno),
        required_relids: singleton(vno),
        incompatible_relids: None,
        outer_relids: None,
        left_relids: singleton(vno),
        right_relids: None,
        rinfo_serial: 0,
        parent_ec: None,
        eval_cost: types_pathnodes::QualCost::default(),
        norm_selec: -1.0,
        outer_selec: -1.0,
        mergeopfamilies: std::vec::Vec::new(),
        left_ec: None,
        right_ec: None,
        left_em: None,
        right_em: None,
        scansel_cache: std::vec::Vec::new(),
        outer_is_left: false,
        hashjoinoperator: 0,
        left_bucketsize: -1.0,
        right_bucketsize: -1.0,
        left_mcvfreq: -1.0,
        right_mcvfreq: -1.0,
        left_hasheqoperator: 0,
        right_hasheqoperator: 0,
    };
    root.alloc_rinfo(ri)
}

/// KEYSTONE GATE: changing rel 2 → 1 must (a) rewrite the clause Var's varno from
/// 2 to 1 (the arena clause is walkable through the bridge), and (b) adjust the
/// RestrictInfo's clause_relids / required_relids / left_relids from {2} to {1}.
#[test]
fn change_relids_rewrites_rinfo_and_clause() {
    let cx = mcx::MemoryContext::new("change_relids test");
    let mcx = cx.mcx();
    let mut root = PlannerInfo::default();
    let rid = mk_var_rinfo(&mut root, 2);

    change_relids_in_rinfo(
        mcx,
        &mut root,
        rid,
        ReplaceRelidContext {
            rt_index: 2,
            new_index: 1,
        },
    )
    .expect("change_relids_in_rinfo");

    // (a) the clause Var was rewritten in place.
    let clause_id = root.rinfo(rid).clause;
    match root.node(clause_id) {
        Expr::Var(v) => assert_eq!(v.varno, 1, "clause Var.varno should be rewritten 2->1"),
        other => panic!("clause should still be a Var, got {other:?}"),
    }

    // (b) the relid sets were adjusted.
    let rinfo = root.rinfo(rid);
    assert!(r::is_member(1, &rinfo.clause_relids), "clause_relids should contain 1");
    assert!(!r::is_member(2, &rinfo.clause_relids), "clause_relids should not contain 2");
    assert!(r::is_member(1, &rinfo.required_relids), "required_relids 2->1");
    assert!(r::is_member(1, &rinfo.left_relids), "left_relids 2->1");
    assert_eq!(rinfo.num_base_rels, 1, "num_base_rels unchanged (still 1 base rel)");
}

/// Deleting a relid (new_index < 0, the left-join `subst == -1` case) should drop
/// the member from the sets without adding a replacement.
#[test]
fn change_relids_delete_only() {
    let cx = mcx::MemoryContext::new("change_relids test");
    let mcx = cx.mcx();
    let mut root = PlannerInfo::default();
    let rid = mk_var_rinfo(&mut root, 3);

    change_relids_in_rinfo(
        mcx,
        &mut root,
        rid,
        ReplaceRelidContext {
            rt_index: 3,
            new_index: -1,
        },
    )
    .expect("change_relids_in_rinfo");

    let rinfo = root.rinfo(rid);
    assert!(r::is_empty(&rinfo.clause_relids), "clause_relids should be emptied");
    assert!(r::is_empty(&rinfo.required_relids), "required_relids should be emptied");
}

/// KEYSTONE GATE: an EquivalenceMember's `em_expr` clause is walkable too.
#[test]
fn change_relids_rewrites_em_expr() {
    let mut root = PlannerInfo::default();
    let em_expr = root.alloc_node(Expr::Var(Var {
        varno: 5,
        varattno: 1,
        ..Var::default()
    }));
    let em = EquivalenceMember {
        em_expr,
        em_relids: singleton(5),
        ..EquivalenceMember::default()
    };
    let emid = root.alloc_em(em);

    let cx = mcx::MemoryContext::new("change_relids test");
    let mcx = cx.mcx();
    change_relids_in_em(
        mcx,
        &mut root,
        emid,
        ReplaceRelidContext {
            rt_index: 5,
            new_index: 4,
        },
    )
    .expect("change_relids_in_em");

    let expr_id = root.em(emid).em_expr;
    match root.node(expr_id) {
        Expr::Var(v) => assert_eq!(v.varno, 4, "em_expr Var.varno should be rewritten 5->4"),
        other => panic!("em_expr should still be a Var, got {other:?}"),
    }
}
