use ::datum::Datum;
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, MemoryContext};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Limit;

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

// Child yielding 0..n as positions in a single reused slot id. Forward-only
// (backward-execution wave B4: exec_limit's backward legs are deleted; the
// run seam refuses backward entry since deletion-prep B1).
struct Counter {
    n: i64,
    pos: i64,
    slot: ExecSlotId,
    bound: Option<i64>,
}

impl<'mcx> LimitChild<'mcx> for Counter {
    fn exec_proc(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
        if self.pos >= self.n {
            return Ok(None);
        }
        self.pos += 1;
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
    let node = exec_init_limit(plan, &mut estate, 0, None).unwrap();
    let child = Counter { n, pos: 0, slot: ExecSlotId(0), bound: None };
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
    let mut node = exec_init_limit(plan, &mut estate, 0, None).unwrap();
    let mut child = Counter { n: 4, pos: 0, slot: ExecSlotId(0), bound: None };
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

// (backward_within_window_and_windowstart retired with the B4 deletion: the
// backward walk it pinned — WINDOWEND re-return, in-window backward steps,
// the LIMIT_WINDOWSTART parking state — is unreachable behind the forward-
// only run seam, deletion-prep B1. The default-world cursor reads it modeled
// are store-served and pinned by the portalcmds/pquery cursor suites.)

// WITH TIES over sorted int4 rows: the window extends across duplicates of
// the boundary tuple's key.
mod with_ties {
    use std::rc::Rc;
    use std::sync::Once;

    use ::datum::Datum;
    use ::executils::{create_executor_state, EStateData, ExecSlotId};
    use ::mcx::{Mcx, MemoryContext, PgVec};
    use ::types_core::INT4OID;
    use ::types_error::PgResult;
    use ::types_nodes::node_tree::Node;
    use ::types_nodes::plannodes::Limit;
    use ::types_slot::TupleSlotKind;
    use ::types_tuple::{
        CompactAttribute, FormData_pg_attribute, PgTypeShape, TupleDescData, TYPALIGN_INT,
        TYPSTORAGE_PLAIN,
    };

    use crate::{exec_init_limit, exec_limit, LimitChild, LimitState};

    const INT4_EQ: u32 = 96;
    const F_INT4EQ: u32 = 65;
    const INT8OID: u32 = 20;

    static SEAMS: Once = Once::new();

    fn install_seams() {
        SEAMS.call_once(|| {
            miscinit_seams::get_user_id::set(|| 10);
            aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
            syscache_seams::lookup_pg_type_shape::set(|typid| {
                Ok((typid == INT4OID).then_some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }))
            });
            syscache_seams::lookup_pg_operator_shape::set(|opno| {
                Ok((opno == INT4_EQ).then_some(syscache_seams::PgOperatorShape {
                    oprnamespace: 11,
                    oprleft: INT4OID,
                    oprright: INT4OID,
                    oprresult: 16,
                    oprcom: INT4_EQ,
                    oprnegate: 518,
                    oprcode: F_INT4EQ,
                    oprrest: 101,
                    oprjoin: 105,
                    oprcanmerge: true,
                    oprcanhash: true,
                }))
            });
        });
    }

    fn leaked_mcx() -> Mcx<'static> {
        let m: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("nodelimit-ties-test")));
        m.mcx()
    }

    fn one_col_desc(mcx: Mcx<'static>) -> Rc<TupleDescData<'static>> {
        let att = FormData_pg_attribute {
            attnum: 1,
            atttypid: INT4OID,
            atttypmod: -1,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
        Rc::new(TupleDescData {
            natts: 1,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        })
    }

    fn mk_ties_limit(mcx: Mcx<'static>, count: i64) -> &'static Limit<'static> {
        let mut limit = Node::build::<Limit>(mcx).unwrap();
        limit.limitCount = Some(
            Node::mk_const(mcx, INT8OID, -1, 0, 8, Datum::from_i64(count), false, true).unwrap(),
        );
        limit.limitOption = ::types_nodes::LimitOption::LIMIT_OPTION_WITH_TIES;
        limit.uniqNumCols = 1;
        limit.uniqColIdx = mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
        limit.uniqOperators = mcx::slice_borrow_in(mcx, &[INT4_EQ]).unwrap();
        limit.uniqCollations = mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
        limit.seal().as_limit().unwrap()
    }

    struct RowFeeder {
        rows: &'static [i32],
        pos: usize,
        slot: ExecSlotId,
        bound: Option<i64>,
    }

    impl<'mcx> LimitChild<'mcx> for RowFeeder {
        fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>> {
            if self.pos >= self.rows.len() {
                return Ok(None);
            }
            let mcx = estate.es_query_cxt;
            let slot = estate.slot_mut(self.slot);
            exectuples::exec_clear_tuple(slot, mcx);
            slot.base_mut().tts_values[0] = Datum::from_i32(self.rows[self.pos]);
            slot.base_mut().tts_isnull[0] = false;
            exectuples::exec_store_virtual_tuple(slot);
            self.pos += 1;
            Ok(Some(self.slot))
        }

        fn set_tuple_bound(&mut self, tuples_needed: i64) {
            self.bound = Some(tuples_needed);
        }
    }

    // SAFETY: caller keeps the referent alive ('static leak in these tests).
    unsafe fn shorten<'a>(l: &Limit<'_>) -> &'a Limit<'a> {
        unsafe { core::mem::transmute::<&Limit<'_>, &'a Limit<'a>>(l) }
    }

    fn run_ties(rows: &'static [i32], count: i64) -> Vec<i32> {
        install_seams();
        let plan = mk_ties_limit(leaked_mcx(), count);
        let mut estate_owner =
            create_executor_state(Box::leak(Box::new(MemoryContext::new("q")))).unwrap();
        estate_owner.with_mut(|estate| {
            // SAFETY: plan is leaked ('static) and read-only.
            let plan = unsafe { shorten(plan) };
            let outer_desc = one_col_desc(leaked_mcx());
            let outer_id = estate
                .exec_init_extra_tuple_slot(Some(outer_desc.clone()), TupleSlotKind::Virtual);
            let mut state: LimitState<'_> =
                exec_init_limit(plan, estate, 0, Some(&outer_desc)).unwrap();
            let mut child = RowFeeder { rows, pos: 0, slot: outer_id, bound: None };
            let mut got = Vec::new();
            while let Some(slot_id) = exec_limit(&mut state, &mut child, estate).unwrap() {
                let slot = estate.slot_mut(slot_id);
                exectuples::slot_getallattrs(slot);
                got.push(slot.base().tts_values[0].as_i32());
            }
            // WITH TIES disables the tuple bound pushdown.
            assert_eq!(child.bound, Some(-1));
            got
        })
    }

    #[test]
    fn ties_extend_window_past_count() {
        assert_eq!(run_ties(&[0, 0, 0, 1, 2], 2), vec![0, 0, 0]);
    }

    #[test]
    fn no_ties_stops_at_count() {
        assert_eq!(run_ties(&[0, 1, 2, 3], 2), vec![0, 1]);
    }

    #[test]
    fn ties_at_subplan_eof() {
        assert_eq!(run_ties(&[5, 5], 1), vec![5, 5]);
    }
}
