//! Unit tests for the paramassign PARAM_EXEC slot management.

use super::*;
use alloc::boxed::Box;
use ::pathnodes::{PlannerGlobal, PlannerInfo};

fn empty_root() -> PlannerInfo {
    let mut root = PlannerInfo::default();
    root.glob = Some(Box::new(PlannerGlobal::default()));
    root.query_level = 1;
    root
}

#[test]
fn assign_special_exec_param_appends_invalid_oid_and_returns_index() {
    let mut root = empty_root();
    assert_eq!(assign_special_exec_param(&mut root).unwrap(), 0);
    assert_eq!(assign_special_exec_param(&mut root).unwrap(), 1);
    let glob = root.glob.as_ref().unwrap();
    assert_eq!(glob.param_exec_types, alloc::vec![InvalidOid, InvalidOid]);
}

#[test]
fn generate_new_exec_param_records_type_and_builds_param() {
    let mut root = empty_root();
    let p = generate_new_exec_param(&mut root, 23, -1, 0).unwrap();
    assert_eq!(p.paramkind, ParamKind::PARAM_EXEC);
    assert_eq!(p.paramid, 0);
    assert_eq!(p.paramtype, 23);
    assert_eq!(p.paramtypmod, -1);
    assert_eq!(p.location, -1);
    assert_eq!(root.glob.as_ref().unwrap().param_exec_types, alloc::vec![23]);
}

#[test]
fn replace_nestloop_param_var_new_then_reuse() {
    // The reuse path compares a Var against the recorded paramval via the
    // equal_expr seam; install it (owned by equalfuncs) for this test.
    equalfuncs::init_seams();

    let mut root = empty_root();
    let var = Var {
        varno: 1,
        varattno: 2,
        vartype: 23,
        vartypmod: -1,
        varcollid: 0,
        ..Var::default()
    };

    // First call: assigns a fresh slot 0 and records one curOuterParams entry.
    let p1 = replace_nestloop_param_var(&mut root, &var).unwrap();
    assert_eq!(p1.paramid, 0);
    assert_eq!(p1.paramtype, 23);
    assert_eq!(root.curOuterParams.len(), 1);
    assert_eq!(root.glob.as_ref().unwrap().param_exec_types.len(), 1);

    // Second call with an equal Var: reuses the same NLP slot, adds nothing.
    let p2 = replace_nestloop_param_var(&mut root, &var).unwrap();
    assert_eq!(p2.paramid, 0);
    assert_eq!(root.curOuterParams.len(), 1);
    assert_eq!(root.glob.as_ref().unwrap().param_exec_types.len(), 1);
}
