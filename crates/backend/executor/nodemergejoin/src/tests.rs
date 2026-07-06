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

// `o RIGHT JOIN i ON o.a = i.b` over single-int4 sides, projecting (o.a, i.b).
fn mk_right_join_plan(mcx: Mcx<'static>) -> &'static MergeJoin<'static> {
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
    mj.join.jointype = JoinType::JOIN_RIGHT;
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
    rows: Vec<i32>,
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
        base.tts_values[0] = Datum::from_i32(self.rows[self.next]);
        base.tts_isnull[0] = false;
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

fn run_right_join(
    outer_rows: Vec<i32>,
    inner_rows: Vec<i32>,
) -> Vec<(Option<i32>, Option<i32>)> {
    install_seams();
    let mcx = leaked_mcx();
    let one_col = int4_desc(mcx, 1);
    let result_desc = int4_desc(mcx, 2);
    let mut estate = EStateData::new_in(mcx);
    let outer_slot = estate.exec_init_extra_tuple_slot(Some(one_col.clone()), TupleSlotKind::Virtual);
    let inner_slot = estate.exec_init_extra_tuple_slot(Some(one_col.clone()), TupleSlotKind::Virtual);
    let plan = mk_right_join_plan(mcx);
    let mut node =
        exec_init_merge_join(plan, &mut estate, 0, &one_col, &one_col, result_desc, false).unwrap();
    let mut outer = Feed { slot: outer_slot, rows: outer_rows, next: 0, marked: 0 };
    let mut inner = Feed { slot: inner_slot, rows: inner_rows, next: 0, marked: 0 };

    let mut out = Vec::new();
    while let Some(id) = exec_merge_join(&mut node, &mut outer, &mut inner, &mut estate).unwrap() {
        let slot = estate.slot_mut(id);
        let mut row = (None, None);
        let mut isnull = false;
        let a = exectuples::slot_getattr(slot, 1, &mut isnull);
        row.0 = (!isnull).then(|| a.as_i32());
        let b = exectuples::slot_getattr(slot, 2, &mut isnull);
        row.1 = (!isnull).then(|| b.as_i32());
        out.push(row);
    }
    out
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
