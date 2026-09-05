use super::*;
use types_nodes::primnodes::{TargetEntry, Var};
use types_nodes::NodeList;
use types_pathnodes::JoinDomain;

// equivclass.c:2618-2630: no enclosed JoinDomain is elog(ERROR) "failed to
// find appropriate JoinDomain" (XX000, catchable), never a panic; the first
// enclosed domain is returned otherwise.
#[test]
fn find_join_domain_reports_missing_domain() {
    let ctx = mcx::MemoryContext::new_bump("equivclass-test");
    let mcx = ctx.mcx();
    let mut run = PlannerRun::new(mcx);
    let relids = relids_add_member(mcx, &relids_empty(), 1);
    let err = find_join_domain(&run, &relids).expect_err("C elog(ERROR)s without a domain");
    assert_eq!(err.message(), "failed to find appropriate JoinDomain");
    run.root.join_domains.push(JoinDomain { jd_relids: relids_add_member(mcx, &relids_empty(), 2) });
    run.root.join_domains.push(JoinDomain { jd_relids: relids_empty() });
    assert_eq!(find_join_domain(&run, &relids).unwrap(), 1);
}

// equivclass.c:3099-3100: a non-resjunk child target without a pathkey is
// elog(ERROR) "too few pathkeys for set operation" (XX000, catchable).
#[test]
fn add_setop_child_rel_equivalences_reports_too_few_pathkeys() {
    let ctx = mcx::MemoryContext::new_bump("equivclass-test");
    let mcx = ctx.mcx();
    let mut run = PlannerRun::new(mcx);
    let expr = Node::mk(mcx, Var { varno: 1, varattno: 1, vartype: 23, ..Default::default() }).unwrap();
    let tle = Node::mk(
        mcx,
        TargetEntry {
            expr,
            resno: 1,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        },
    )
    .unwrap();
    let tlist = NodeList::make1(mcx, tle).unwrap();
    let err = add_setop_child_rel_equivalences(&mut run, RelId(0), &tlist, &[])
        .expect_err("C elog(ERROR)s with too few pathkeys");
    assert_eq!(err.message(), "too few pathkeys for set operation");
}
