use std::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::MergeJoin;
use ::types_nodes::primnodes::{OpExpr, INNER_VAR, OUTER_VAR};
use ::types_nodes::JoinType;
use ::types_slot::TupleSlotKind;
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, PgTypeShape, TupleDescData, TYPALIGN_INT,
    TYPSTORAGE_PLAIN,
};

use crate::*;

const INT4OID: u32 = 23;
const INT4_EQ: u32 = 96;
const F_INT4EQ: u32 = 65;
const INTEGER_BTREE_FAM: u32 = 1976;
const BTREE_EQ_STRATEGY: i16 = 3;
const F_BTINT4SORTSUPPORT: u32 = 3130;

static SEAMS: Once = Once::new();

fn install_seams() {
    SEAMS.call_once(|| {
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok((typid == INT4OID).then_some(PgTypeShape {
                typlen: 4,
                typbyval: true,
                typalign: TYPALIGN_INT,
                typstorage: TYPSTORAGE_PLAIN,
                typcollation: 0,
            }))
        });
        syscache_seams::lookup_pg_amop_by_operator::set(|opno, _purpose, opfamily| {
            assert_eq!((opno, opfamily), (INT4_EQ, INTEGER_BTREE_FAM));
            Ok(Some(syscache_seams::PgAmopShape {
                amopstrategy: BTREE_EQ_STRATEGY,
                amopsortfamily: 0,
                amoplefttype: INT4OID,
                amoprighttype: INT4OID,
            }))
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, left, right, procnum| {
            assert_eq!((opfamily, left, right, procnum), (INTEGER_BTREE_FAM, INT4OID, INT4OID, 2));
            Ok(F_BTINT4SORTSUPPORT)
        });
    });
}

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("nodemergejoin-test")));
    m.mcx()
}

fn int4_desc(mcx: Mcx<'static>, natts: i32) -> Rc<TupleDescData<'static>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for i in 0..natts {
        let att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
            atttypid: INT4OID,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

// `o <jointype> JOIN i ON o.a = i.b` over single-int4 sides, projecting
// (o.a, i.b). (WS-MJ1 band-99001 generalization of the RIGHT-only builder.)
fn mk_join_plan(mcx: Mcx<'static>, jointype: JoinType) -> &'static MergeJoin<'static> {
    let outer_var = || Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let inner_var = || Node::mk_var(mcx, INNER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let clause = Node::mk(
        mcx,
        OpExpr {
            opno: INT4_EQ,
            opfuncid: F_INT4EQ,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, outer_var(), inner_var()).unwrap(),
            location: -1,
        },
    )
    .unwrap();

    let tle1 = Node::mk_target_entry(mcx, outer_var(), 1, Some("a"), false).unwrap();
    let tle2 = Node::mk_target_entry(mcx, inner_var(), 2, Some("b"), false).unwrap();
    let mut mj = Node::build::<MergeJoin>(mcx).unwrap();
    mj.join.jointype = jointype;
    mj.join.plan.targetlist = NodeList::make2(mcx, tle1, tle2).unwrap();
    mj.mergeclauses = NodeList::make1(mcx, clause).unwrap();
    mj.mergeFamilies = ::mcx::slice_borrow_in(mcx, &[INTEGER_BTREE_FAM]).unwrap();
    mj.mergeCollations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    mj.mergeReversals = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();
    mj.mergeNullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();
    mj.seal().as_merge_join().unwrap()
}

struct Feed {
    slot: ExecSlotId,
    // None = a NULL merge key in that row (WS-MJ1 band-99001 NULL pins).
    rows: Vec<Option<i32>>,
    next: usize,
    marked: usize,
}

impl Feed {
    fn fetch(
        &mut self,
        estate: &mut EStateData<'static>,
    ) -> ::types_error::PgResult<Option<ExecSlotId>> {
        if self.next >= self.rows.len() {
            return Ok(None);
        }
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(self.slot);
        exectuples::exec_clear_tuple(slot, mcx);
        let base = slot.base_mut();
        match self.rows[self.next] {
            Some(v) => {
                base.tts_values[0] = Datum::from_i32(v);
                base.tts_isnull[0] = false;
            }
            None => {
                base.tts_values[0] = Datum::null();
                base.tts_isnull[0] = true;
            }
        }
        exectuples::exec_store_virtual_tuple(slot);
        self.next += 1;
        Ok(Some(self.slot))
    }
}

impl MergeJoinOuter<'static> for Feed {
    fn exec_proc(
        &mut self,
        estate: &mut EStateData<'static>,
    ) -> ::types_error::PgResult<Option<ExecSlotId>> {
        self.fetch(estate)
    }
    fn rescan(&mut self, _estate: &mut EStateData<'static>) -> ::types_error::PgResult<()> {
        self.next = 0;
        Ok(())
    }
}

impl MergeJoinInner<'static> for Feed {
    fn exec_proc(
        &mut self,
        estate: &mut EStateData<'static>,
    ) -> ::types_error::PgResult<Option<ExecSlotId>> {
        self.fetch(estate)
    }
    fn rescan(&mut self, _estate: &mut EStateData<'static>) -> ::types_error::PgResult<()> {
        self.next = 0;
        Ok(())
    }
    fn mark_pos(&mut self, _estate: &mut EStateData<'static>) -> ::types_error::PgResult<()> {
        // C marks the position of the tuple just returned.
        self.marked = self.next - 1;
        Ok(())
    }
    fn restr_pos(&mut self, _estate: &mut EStateData<'static>) -> ::types_error::PgResult<()> {
        self.next = self.marked;
        Ok(())
    }
}

fn run_join(
    jointype: JoinType,
    outer_rows: Vec<Option<i32>>,
    inner_rows: Vec<Option<i32>>,
) -> ::types_error::PgResult<Vec<(Option<i32>, Option<i32>)>> {
    install_seams();
    let mcx = leaked_mcx();
    let one_col = int4_desc(mcx, 1);
    let result_desc = int4_desc(mcx, 2);
    let mut estate = EStateData::new_in(mcx);
    let outer_slot = estate.exec_init_extra_tuple_slot(Some(one_col.clone()), TupleSlotKind::Virtual);
    let inner_slot = estate.exec_init_extra_tuple_slot(Some(one_col.clone()), TupleSlotKind::Virtual);
    let plan = mk_join_plan(mcx, jointype);
    let mut node =
        exec_init_merge_join(plan, &mut estate, 0, &one_col, &one_col, result_desc, false).unwrap();
    let mut outer = Feed { slot: outer_slot, rows: outer_rows, next: 0, marked: 0 };
    let mut inner = Feed { slot: inner_slot, rows: inner_rows, next: 0, marked: 0 };

    let mut out = Vec::new();
    while let Some(id) = exec_merge_join(&mut node, &mut outer, &mut inner, &mut estate)? {
        let slot = estate.slot_mut(id);
        let mut row = (None, None);
        let mut isnull = false;
        let a = exectuples::slot_getattr(slot, 1, &mut isnull);
        row.0 = (!isnull).then(|| a.as_i32());
        let b = exectuples::slot_getattr(slot, 2, &mut isnull);
        row.1 = (!isnull).then(|| b.as_i32());
        out.push(row);
    }
    Ok(out)
}

fn run_right_join(
    outer_rows: Vec<i32>,
    inner_rows: Vec<i32>,
) -> Vec<(Option<i32>, Option<i32>)> {
    run_join(
        JoinType::JOIN_RIGHT,
        outer_rows.into_iter().map(Some).collect(),
        inner_rows.into_iter().map(Some).collect(),
    )
    .unwrap()
}

// C gold (18.3): `select * from o right join i on a=b` with o empty,
// i=(1),(2) fills every inner row. Before the INITIALIZE_OUTER
// ENDOFJOIN->ENDOUTER arm set MatchedInner, this panicked "inner slot set"
// (ENDOUTER filled before any inner tuple was fetched).
#[test]
fn right_join_empty_outer_fills_all_inners() {
    let rows = run_right_join(vec![], vec![1, 2]);
    assert_eq!(rows, vec![(None, Some(1)), (None, Some(2))]);
}

// C gold (18.3): o=(2), i=(1),(2),(3) -> (NULL,1),(2,2),(NULL,3).
#[test]
fn right_join_fills_unmatched_inners_around_match() {
    let rows = run_right_join(vec![2], vec![1, 2, 3]);
    assert_eq!(rows, vec![(None, Some(1)), (Some(2), Some(2)), (None, Some(3))]);
}

// ============================================================================
// WS-MJ1 band-99001 FSM-level pins (LANE-MERGEJOIN contract §2; worklog
// notes/mergejoin-ws-mj1.md). These pin the ported state machine's trap
// semantics DIRECTLY (mock feeds / direct comparator calls); the lane-surface
// pins (band 99101+) live in execmain/src/tests.rs.
// ============================================================================

/// §2.2/§8.1 — the nulleqnull rule, pinned by a DIRECT comparator unit (not
/// via the state machine: every FSM call site is reachable only with
/// MATCHABLE verdicts on both sides, so the arm is defensively dead — port
/// it anyway; forbidden to "optimize away" on a reachability argument).
/// C: MJCompare (nodeMergejoin.c): "we do not want to report that the
/// tuples are equal ... This will result in advancing the inner side" —
/// NULL-vs-NULL on any clause with an otherwise-tied compare forces +1.
/// Also pins the mj_ConstFalseJoin coupling: a constant-false joinqual
/// forces non-equality "as part of the mergequals, else the rescan logic
/// will do the wrong thing".
#[test]
fn mj99001_nulleqnull_comparator_forces_advance_inner() {
    install_seams();
    let mcx = leaked_mcx();
    let one_col = int4_desc(mcx, 1);
    let result_desc = int4_desc(mcx, 2);
    let mut estate = EStateData::new_in(mcx);
    let plan = mk_join_plan(mcx, JoinType::JOIN_INNER);
    let mut node =
        exec_init_merge_join(plan, &mut estate, 0, &one_col, &one_col, result_desc, false)
            .unwrap();

    // Control: equal non-null keys compare 0.
    node.clauses[0].ldatum = Datum::from_i32(7);
    node.clauses[0].lisnull = false;
    node.clauses[0].rdatum = Datum::from_i32(7);
    node.clauses[0].risnull = false;
    assert_eq!(mj_compare(&mut node, &mut estate), 0);

    // NULL vs NULL: the per-clause compare is SKIPPED and the tie is
    // broken as +1 (advance the inner side).
    node.clauses[0].ldatum = Datum::null();
    node.clauses[0].lisnull = true;
    node.clauses[0].rdatum = Datum::null();
    node.clauses[0].risnull = true;
    assert_eq!(mj_compare(&mut node, &mut estate), 1, "nulleqnull must force +1");

    // One-sided NULL is NOT the nulleqnull arm: ApplySortComparator's
    // nulls-last rule decides (NULL sorts after non-NULL here).
    node.clauses[0].lisnull = false;
    node.clauses[0].ldatum = Datum::from_i32(7);
    assert!(mj_compare(&mut node, &mut estate) < 0, "non-NULL vs NULL, nulls-last");

    // mj_ConstFalseJoin coupling: equal keys + constant-false joinqual
    // still report unequal (+1).
    node.clauses[0].rdatum = Datum::from_i32(7);
    node.clauses[0].risnull = false;
    node.mj_ConstFalseJoin = true;
    assert_eq!(mj_compare(&mut node, &mut estate), 1, "const-false joinqual forces +1");
    node.mj_ConstFalseJoin = false;
    assert_eq!(mj_compare(&mut node, &mut estate), 0);
}

/// §2.1 — the two sort-order guards are ERRORS, not panics (user-reachable
/// on misdeclared collations/opfamilies; nodeMergejoin.c:902 NEXTINNER
/// compare>0 arm exercised here with an out-of-order inner).
#[test]
fn mj99002_out_of_order_inner_is_error_not_panic() {
    let err = run_join(
        JoinType::JOIN_INNER,
        vec![Some(2), Some(2)],
        vec![Some(2), Some(1)], // out of order behind the first match
    )
    .expect_err("misordered inner must be an ERROR");
    assert!(
        err.to_string().contains("mergejoin input data is out of order"),
        "got: {err}"
    );
}

/// §2.5 — the INITIALIZE_INNER ENDOFJOIN asymmetry: mj_MatchedOuter is
/// FORCED false so ENDINNER emits the already-fetched first outer before
/// advancing (C: "to force the ENDINNER state to emit first tuple before
/// advancing"). LEFT join, empty inner: every outer null-extends, INCLUDING
/// the first one. (The ENDOUTER half — mj_MatchedInner forced TRUE at
/// INITIALIZE_OUTER ENDOFJOIN, "to force the ENDOUTER state to advance
/// inner" — is pinned by right_join_empty_outer_fills_all_inners above.)
#[test]
fn mj99003_initialize_inner_endofjoin_matchedouter_asymmetry() {
    let rows = run_join(JoinType::JOIN_LEFT, vec![Some(1), Some(2)], vec![]).unwrap();
    assert_eq!(rows, vec![(Some(1), None), (Some(2), None)]);
}

/// §2.2 — FIRST-column NULL under nulls-last = effective end of input, but
/// ONLY because the corresponding fill mode is off (INNER join here; in
/// fill mode every tuple must still be visited — both halves of the
/// condition are load-bearing). Outer-side NULL ends the join early.
#[test]
fn mj99004_first_key_null_nulls_last_outer_effective_endofjoin() {
    let rows =
        run_join(JoinType::JOIN_INNER, vec![Some(1), None, Some(3)], vec![Some(1), Some(3)])
            .unwrap();
    // The NULL-keyed outer ends the join: key 3 is NEVER reached.
    assert_eq!(rows, vec![(Some(1), Some(1))]);
}

/// §2.2 — the NEXTINNER ENDOFJOIN trap, ported with its hack: an inner
/// first-column NULL (nulls-last, no fill) is only EFFECTIVE end of inner,
/// so the FSM forces mj_InnerTupleSlot to null ("We need this hack because
/// we are not transiting to a state where the inner plan is assumed to be
/// exhausted"). The subsequent TESTOUTER cmp>0 arm re-evaluates the current
/// (forced-null) inner and terminates cleanly instead of replaying it.
#[test]
fn mj99005_nextinner_effective_endofjoin_forces_null_inner_slot() {
    let rows =
        run_join(JoinType::JOIN_INNER, vec![Some(1), Some(2)], vec![Some(1), None, Some(2)])
            .unwrap();
    // Inner NULL after the key-1 run ends the inner side; outer 2 finds
    // EndOfJoin at TESTOUTER's re-evaluation, emits nothing.
    assert_eq!(rows, vec![(Some(1), Some(1))]);
}

/// §2.2 fill-mode half of the first-key-NULL condition: under RIGHT
/// (FillInner) an inner NULL key must STILL be visited (null-extended),
/// not treated as end of input.
#[test]
fn mj99006_first_key_null_still_visited_in_fill_mode() {
    let rows =
        run_join(JoinType::JOIN_RIGHT, vec![Some(1)], vec![Some(1), None]).unwrap();
    assert_eq!(rows, vec![(Some(1), Some(1)), (None, None)]);
}
