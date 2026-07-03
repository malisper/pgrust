use ::datum::Datum;
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, MemoryContext};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Limit;
use ::types_scan::sdir::BackwardScanDirection;

use crate::*;

const INT8OID: u32 = 20;

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("nodelimit-test")));
    m.mcx()
}

fn mk_i64_const(mcx: Mcx<'static>, v: i64) -> Node<'static> {
    Node::mk_const(mcx, INT8OID, -1, 0, 8, Datum::from_i64(v), false, true).unwrap()
}

fn mk_null_i64_const(mcx: Mcx<'static>) -> Node<'static> {
    Node::mk_const(mcx, INT8OID, -1, 0, 8, Datum::null(), true, true).unwrap()
}

fn mk_limit_plan(
    mcx: Mcx<'static>,
    offset: Option<Node<'static>>,
    count: Option<Node<'static>>,
) -> &'static Limit<'static> {
    let mut limit = Node::build::<Limit>(mcx).unwrap();
    limit.limitOffset = offset;
    limit.limitCount = count;
    limit.seal().as_limit().unwrap()
}

// Child yielding 0..n as positions in a single reused slot id; supports
// backward stepping like a materialized node would.
struct Counter {
    n: i64,
    pos: i64,
    slot: ExecSlotId,
    bound: Option<i64>,
    forward: bool,
}

impl<'mcx> LimitChild<'mcx> for Counter {
    fn exec_proc(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        if self.forward {
            if self.pos >= self.n {
                return Ok(None);
            }
            self.pos += 1;
        } else {
            if self.pos <= 1 {
                return Ok(None);
            }
            self.pos -= 1;
        }
        Ok(Some(self.slot))
    }

    fn set_tuple_bound(&mut self, tuples_needed: i64) {
        self.bound = Some(tuples_needed);
    }
}

fn setup(
    offset: Option<i64>,
    count: Option<i64>,
    n: i64,
) -> (LimitState<'static>, Counter, EStateData<'static>) {
    let mcx = leaked_mcx();
    let mut estate = EStateData::new_in(mcx);
    let plan = mk_limit_plan(
        mcx,
        offset.map(|v| mk_i64_const(mcx, v)),
        count.map(|v| mk_i64_const(mcx, v)),
    );
    let node = exec_init_limit(plan, &mut estate, 0).unwrap();
    let child = Counter { n, pos: 0, slot: ExecSlotId(0), bound: None, forward: true };
    (node, child, estate)
}

fn drain(
    node: &mut LimitState<'static>,
    child: &mut Counter,
    estate: &mut EStateData<'static>,
) -> Vec<i64> {
    let mut out = Vec::new();
    while exec_limit(node, child, estate).unwrap().is_some() {
        out.push(child.pos);
    }
    out
}

#[test]
fn limit_and_offset_window() {
    let (mut node, mut child, mut estate) = setup(Some(2), Some(3), 10);
    let out = drain(&mut node, &mut child, &mut estate);
    assert_eq!(out, vec![3, 4, 5]);
    assert_eq!(child.bound, Some(5));
    assert_eq!(node.lstate, LimitStateCond::LIMIT_WINDOWEND);
    // Still EOF on further pulls.
    assert!(exec_limit(&mut node, &mut child, &mut estate).unwrap().is_none());
}

#[test]
fn no_count_returns_all_after_offset() {
    let (mut node, mut child, mut estate) = setup(Some(7), None, 10);
    let out = drain(&mut node, &mut child, &mut estate);
    assert_eq!(out, vec![8, 9, 10]);
    assert_eq!(child.bound, Some(-1));
    assert_eq!(node.lstate, LimitStateCond::LIMIT_SUBPLANEOF);
}

#[test]
fn zero_count_is_empty_without_touching_subplan() {
    let (mut node, mut child, mut estate) = setup(None, Some(0), 10);
    assert!(exec_limit(&mut node, &mut child, &mut estate).unwrap().is_none());
    assert_eq!(node.lstate, LimitStateCond::LIMIT_EMPTY);
    assert_eq!(child.pos, 0);
}

#[test]
fn subplan_shorter_than_offset_is_empty() {
    let (mut node, mut child, mut estate) = setup(Some(5), Some(2), 3);
    assert!(exec_limit(&mut node, &mut child, &mut estate).unwrap().is_none());
    assert_eq!(node.lstate, LimitStateCond::LIMIT_EMPTY);
}

#[test]
fn null_count_means_limit_all() {
    let mcx = leaked_mcx();
    let mut estate = EStateData::new_in(mcx);
    let plan = mk_limit_plan(mcx, None, Some(mk_null_i64_const(mcx)));
    let mut node = exec_init_limit(plan, &mut estate, 0).unwrap();
    let mut child = Counter { n: 4, pos: 0, slot: ExecSlotId(0), bound: None, forward: true };
    let out = drain(&mut node, &mut child, &mut estate);
    assert_eq!(out, vec![1, 2, 3, 4]);
    assert_eq!(child.bound, Some(-1));
}

#[test]
fn negative_limit_and_offset_error() {
    let (mut node, mut child, mut estate) = setup(None, Some(-1), 10);
    let err = exec_limit(&mut node, &mut child, &mut estate).unwrap_err();
    assert_eq!(err.message, "LIMIT must not be negative");

    let (mut node, mut child, mut estate) = setup(Some(-1), Some(1), 10);
    let err = exec_limit(&mut node, &mut child, &mut estate).unwrap_err();
    assert_eq!(err.message, "OFFSET must not be negative");
}

#[test]
fn rescan_recomputes_and_replays() {
    let (mut node, mut child, mut estate) = setup(Some(1), Some(2), 10);
    assert_eq!(drain(&mut node, &mut child, &mut estate), vec![2, 3]);
    exec_rescan_limit(&mut node, &mut child, &mut estate).unwrap();
    assert_eq!(node.lstate, LimitStateCond::LIMIT_RESCAN);
    child.pos = 0;
    assert_eq!(drain(&mut node, &mut child, &mut estate), vec![2, 3]);
}

#[test]
fn backward_within_window_and_windowstart() {
    let (mut node, mut child, mut estate) = setup(Some(2), Some(3), 10);
    assert_eq!(drain(&mut node, &mut child, &mut estate), vec![3, 4, 5]);
    // Back up: WINDOWEND re-returns last tuple, then in-window backward.
    estate.es_direction = BackwardScanDirection;
    child.forward = false;
    assert!(exec_limit(&mut node, &mut child, &mut estate).unwrap().is_some());
    assert_eq!(child.pos, 5);
    assert!(exec_limit(&mut node, &mut child, &mut estate).unwrap().is_some());
    assert_eq!(child.pos, 4);
    assert!(exec_limit(&mut node, &mut child, &mut estate).unwrap().is_some());
    assert_eq!(child.pos, 3);
    assert!(exec_limit(&mut node, &mut child, &mut estate).unwrap().is_none());
    assert_eq!(node.lstate, LimitStateCond::LIMIT_WINDOWSTART);
}

#[test]
#[should_panic(expected = "WITH TIES")]
fn with_ties_panics_at_init() {
    let mcx = leaked_mcx();
    let mut estate = EStateData::new_in(mcx);
    let mut limit = Node::build::<Limit>(mcx).unwrap();
    limit.limitCount = Some(mk_i64_const(mcx, 1));
    limit.limitOption = ::types_nodes::LimitOption::LIMIT_OPTION_WITH_TIES;
    let plan = limit.seal().as_limit().unwrap();
    let _ = exec_init_limit(plan, &mut estate, 0);
}
