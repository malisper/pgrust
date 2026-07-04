use std::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::IncrementalSort;
use ::types_slot::TupleSlotKind;
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, PgTypeShape, TupleDescData, TYPALIGN_INT,
    TYPSTORAGE_PLAIN,
};

use crate::*;

const INT4OID: u32 = 23;
const INT4_LT: u32 = 97;
const INT4_EQ: u32 = 96;
const F_INT4EQ: u32 = 65;
const INTEGER_BTREE_FAM: u32 = 1976;
const BTREE_AM: u32 = 403;
const F_BTINT4SORTSUPPORT: u32 = 3130;
const BT_EQUAL_STRATEGY: i16 = 3;

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
        syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
            assert_eq!(opno, INT4_LT);
            let mut v = PgVec::new_in(mcx);
            v.push(syscache_seams::PgAmopMemberShape {
                amopfamily: INTEGER_BTREE_FAM,
                amoplefttype: INT4OID,
                amoprighttype: INT4OID,
                amopstrategy: 1,
                amopmethod: BTREE_AM,
            });
            Ok(v)
        });
        syscache_seams::lookup_pg_opfamily_shape::set(|opfid| {
            Ok((opfid == INTEGER_BTREE_FAM).then(|| syscache_seams::PgOpfamilyShape {
                opfmethod: BTREE_AM,
                opfname: ::types_tuple::NameData::default(),
            }))
        });
        syscache_seams::lookup_pg_amop_by_strategy::set(|opfamily, left, right, strategy| {
            assert_eq!(
                (opfamily, left, right, strategy),
                (INTEGER_BTREE_FAM, INT4OID, INT4OID, BT_EQUAL_STRATEGY)
            );
            Ok(INT4_EQ)
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            Ok((opno == INT4_EQ).then_some(syscache_seams::PgOperatorShape { oprnamespace: 11,
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
        syscache_seams::lookup_pg_amproc::set(|opfamily, left, right, procnum| {
            assert_eq!((opfamily, left, right, procnum), (INTEGER_BTREE_FAM, INT4OID, INT4OID, 2));
            Ok(F_BTINT4SORTSUPPORT)
        });
    });
}

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("incrsort-test")));
    m.mcx()
}

fn int4_desc(mcx: Mcx<'static>, natts: i32) -> Rc<TupleDescData<'static>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for i in 0..natts {
        let att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
            atttypid: INT4OID,
            atttypmod: -1,
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

fn mk_plan(mcx: Mcx<'static>, n_presorted: i32) -> &'static IncrementalSort<'static> {
    let mut plan = Node::build::<IncrementalSort>(mcx).unwrap();
    plan.sort.numCols = 2;
    plan.sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16, 2]).unwrap();
    plan.sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT, INT4_LT]).unwrap();
    plan.sort.collations = ::mcx::slice_borrow_in(mcx, &[0u32, 0]).unwrap();
    plan.sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false, false]).unwrap();
    plan.nPresortedCols = n_presorted;
    plan.seal().as_incremental_sort().unwrap()
}

struct Feed {
    slot: ExecSlotId,
    rows: Vec<(Option<i32>, i32)>,
    next: usize,
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
        let (a, b) = self.rows[self.next];
        let base = slot.base_mut();
        base.tts_values[0] = a.map_or(Datum::null(), Datum::from_i32);
        base.tts_isnull[0] = a.is_none();
        base.tts_values[1] = Datum::from_i32(b);
        base.tts_isnull[1] = false;
        exectuples::exec_store_virtual_tuple(slot);
        self.next += 1;
        Ok(Some(self.slot))
    }
}

fn setup(
    rows: Vec<(Option<i32>, i32)>,
) -> (IncrementalSortState<'static>, EStateData<'static>, Feed) {
    install_seams();
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 2);
    let mut estate = EStateData::new_in(mcx);
    let in_slot = estate.exec_init_extra_tuple_slot(Some(desc.clone()), TupleSlotKind::Virtual);
    let plan = mk_plan(mcx, 1);
    let node = exec_init_incremental_sort(plan, &mut estate, 0, &desc, desc.clone());
    let feed = Feed { slot: in_slot, rows, next: 0 };
    (node, estate, feed)
}

fn drain(
    node: &mut IncrementalSortState<'static>,
    estate: &mut EStateData<'static>,
    feed: &mut Feed,
    limit: Option<usize>,
) -> Vec<(Option<i32>, i32)> {
    let mut out = Vec::new();
    loop {
        let got = exec_incremental_sort(node, estate, |es| feed.fetch(es)).unwrap();
        let Some(id) = got else { break };
        let slot = estate.slot_mut(id);
        let mut n1 = false;
        let mut n2 = false;
        let a = exectuples::slot_getattr(slot, 1, &mut n1);
        let b = exectuples::slot_getattr(slot, 2, &mut n2);
        assert!(!n2);
        out.push((if n1 { None } else { Some(a.as_i32()) }, b.as_i32()));
        if limit.is_some_and(|l| out.len() >= l) {
            break;
        }
    }
    out
}

fn expected_sorted(mut rows: Vec<(Option<i32>, i32)>) -> Vec<(Option<i32>, i32)> {
    // NULLS LAST on the prefix column.
    rows.sort_by_key(|&(a, b)| (a.is_none(), a, b));
    rows
}

#[test]
fn small_groups_full_sort_only() {
    let rows = vec![
        (Some(1), 5),
        (Some(1), 2),
        (Some(2), 9),
        (Some(2), 1),
        (Some(2), 5),
        (Some(3), 3),
        (Some(3), 7),
        (None, 4),
        (None, 0),
    ];
    let (mut node, mut estate, mut feed) = setup(rows.clone());
    let out = drain(&mut node, &mut estate, &mut feed, None);
    assert_eq!(out, expected_sorted(rows));
    let info = estate.es_incsort_instrumentation[0].1;
    assert_eq!(info.fullsortGroupInfo.groupCount, 1);
    assert_eq!(info.prefixsortGroupInfo.groupCount, 0);
}

#[test]
fn large_group_switches_to_prefix_mode() {
    let mut rows: Vec<(Option<i32>, i32)> = (0..200).map(|i| (Some(1), (i * 37) % 200)).collect();
    rows.extend((0..50).map(|i| (Some(2), 49 - i)));
    let (mut node, mut estate, mut feed) = setup(rows.clone());
    let out = drain(&mut node, &mut estate, &mut feed, None);
    assert_eq!(out, expected_sorted(rows));
    let info = estate.es_incsort_instrumentation[0].1;
    // Group a=1: one fullsort batch (65 rows) drained into one prefix batch;
    // group a=2 fits a single fullsort batch.
    assert_eq!(info.fullsortGroupInfo.groupCount, 2);
    assert_eq!(info.prefixsortGroupInfo.groupCount, 1);
}

#[test]
fn multiple_prefix_groups_inside_full_sort_batch() {
    // >64 tuples spanning several small groups: the transfer loop must carry
    // group openers across batches.
    let mut rows: Vec<(Option<i32>, i32)> = Vec::new();
    for g in 0..10 {
        for i in 0..9 {
            rows.push((Some(g), (i * 53) % 9));
        }
    }
    let (mut node, mut estate, mut feed) = setup(rows.clone());
    let out = drain(&mut node, &mut estate, &mut feed, None);
    assert_eq!(out, expected_sorted(rows));
}

#[test]
fn bounded_returns_top_n() {
    let mut rows: Vec<(Option<i32>, i32)> = (0..200).map(|i| (Some(1), 199 - i)).collect();
    rows.extend((0..100).map(|i| (Some(2), 99 - i)));
    let (mut node, mut estate, mut feed) = setup(rows.clone());
    incremental_sort_set_tuple_bound(&mut node, 5);
    assert!(node.bounded && node.bound == 5);
    let out = drain(&mut node, &mut estate, &mut feed, Some(5));
    assert_eq!(out, expected_sorted(rows)[..5].to_vec());
}

#[test]
fn rescan_resorts_from_scratch() {
    let rows = vec![(Some(1), 2), (Some(1), 1), (Some(2), 4), (Some(2), 3)];
    let (mut node, mut estate, mut feed) = setup(rows.clone());
    let out = drain(&mut node, &mut estate, &mut feed, None);
    assert_eq!(out, expected_sorted(rows.clone()));
    exec_rescan_incremental_sort(&mut node, &mut estate);
    feed.next = 0;
    let out = drain(&mut node, &mut estate, &mut feed, None);
    assert_eq!(out, expected_sorted(rows));
}
