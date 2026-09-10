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
const F_INT4LE: u32 = 149;
const INTEGER_BTREE_FAM: u32 = 1976;
const BTREE_AM: u32 = 403;
const F_BTINT4SORTSUPPORT: u32 = 3130;
const BT_EQUAL_STRATEGY: i16 = 3;

// Synthetic btree opfamilies over int4 (pathkeys-valid: strategy 1 plus a
// sort-support proc), differing only in their equality member:
//   ASYM:   equality operator's function is int4le — eq(a, b) := a <= b, so
//           the operand order of isCurrentGroup is observable;
//   NOEQ:   no equality member (preparePresortedCols's first elog(ERROR));
//   NOFUNC: equality operator whose oprcode is InvalidOid (its second).
const ASYM_LT: u32 = 90001;
const ASYM_EQ: u32 = 90002;
const ASYM_FAM: u32 = 90100;
const NOEQ_LT: u32 = 90003;
const NOEQ_FAM: u32 = 90200;
const NOFUNC_LT: u32 = 90005;
const NOFUNC_EQ: u32 = 90006;
const NOFUNC_FAM: u32 = 90300;

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
        syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
            let amopfamily = match opno {
                INT4_LT => INTEGER_BTREE_FAM,
                ASYM_LT => ASYM_FAM,
                NOEQ_LT => NOEQ_FAM,
                NOFUNC_LT => NOFUNC_FAM,
                other => panic!("unexpected operator {other}"),
            };
            let mut v = PgVec::new_in(mcx);
            v.push(syscache_seams::PgAmopMemberShape {
                amopfamily,
                amoplefttype: INT4OID,
                amoprighttype: INT4OID,
                amopstrategy: 1,
                amopmethod: BTREE_AM,
            });
            Ok(v)
        });
        syscache_seams::lookup_pg_opfamily_shape::set(|opfid| {
            Ok(matches!(opfid, INTEGER_BTREE_FAM | ASYM_FAM | NOEQ_FAM | NOFUNC_FAM).then(|| {
                syscache_seams::PgOpfamilyShape {
                    opfmethod: BTREE_AM,
                    opfname: ::types_tuple::NameData::default(),
                }
            }))
        });
        syscache_seams::lookup_pg_amop_by_strategy::set(|opfamily, left, right, strategy| {
            assert_eq!((left, right, strategy), (INT4OID, INT4OID, BT_EQUAL_STRATEGY));
            Ok(match opfamily {
                INTEGER_BTREE_FAM => INT4_EQ,
                ASYM_FAM => ASYM_EQ,
                NOEQ_FAM => 0,
                NOFUNC_FAM => NOFUNC_EQ,
                other => panic!("unexpected opfamily {other}"),
            })
        });
        syscache_seams::lookup_pg_operator_shape::set(|opno| {
            let oprcode = match opno {
                INT4_EQ => F_INT4EQ,
                ASYM_EQ => F_INT4LE,
                NOFUNC_EQ => 0,
                _ => return Ok(None),
            };
            Ok(Some(syscache_seams::PgOperatorShape {
                oprnamespace: 11,
                oprleft: INT4OID,
                oprright: INT4OID,
                oprresult: 16,
                oprcom: opno,
                oprnegate: 518,
                oprcode,
                oprrest: 101,
                oprjoin: 105,
                oprcanmerge: true,
                oprcanhash: true,
            }))
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, left, right, procnum| {
            assert!(matches!(opfamily, INTEGER_BTREE_FAM | ASYM_FAM | NOEQ_FAM | NOFUNC_FAM));
            assert_eq!((left, right, procnum), (INT4OID, INT4OID, 2));
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

fn mk_plan(
    mcx: Mcx<'static>,
    n_presorted: i32,
    prefix_sortop: u32,
) -> &'static IncrementalSort<'static> {
    let mut plan = Node::build::<IncrementalSort>(mcx).unwrap();
    plan.sort.numCols = 2;
    plan.sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16, 2]).unwrap();
    plan.sort.sortOperators = ::mcx::slice_borrow_in(mcx, &[prefix_sortop, INT4_LT]).unwrap();
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
    setup_with_sortop(rows, INT4_LT)
}

fn setup_with_sortop(
    rows: Vec<(Option<i32>, i32)>,
    prefix_sortop: u32,
) -> (IncrementalSortState<'static>, EStateData<'static>, Feed) {
    install_seams();
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 2);
    let mut estate = EStateData::new_in(mcx);
    let in_slot = estate.exec_init_extra_tuple_slot(Some(desc.clone()), TupleSlotKind::Virtual);
    let plan = mk_plan(mcx, 1, prefix_sortop);
    let node = exec_init_incremental_sort(plan, &mut estate, 0, &desc, desc.clone()).unwrap();
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
    exec_rescan_incremental_sort(&mut node, &mut estate).unwrap();
    feed.next = 0;
    let out = drain(&mut node, &mut estate, &mut feed, None);
    assert_eq!(out, expected_sorted(rows));
}

// Query-context bytes after sorting `n` rows whose prefix key changes on
// every row (a pivot copy at every group boundary): (self, subtree).
fn query_ctx_used_after(n: i32) -> (usize, usize) {
    let rows: Vec<(Option<i32>, i32)> = (0..n).map(|i| (Some(i), 0)).collect();
    let (mut node, mut estate, mut feed) = setup(rows);
    let out = drain(&mut node, &mut estate, &mut feed, None);
    assert_eq!(out.len(), n as usize);
    let ctx = estate.es_query_cxt.context();
    (ctx.used(), ctx.subtree_used())
}

// The retained group pivot must not accumulate in the query context: 50k
// group boundaries cost the same query-context bytes as 2k, to within one
// arena block.
#[test]
fn retained_group_pivot_does_not_grow_query_context() {
    let (small_self, small_tree) = query_ctx_used_after(2_000);
    let (big_self, big_tree) = query_ctx_used_after(50_000);
    const SLACK: usize = 64 * 1024;
    assert!(
        big_self <= small_self + SLACK,
        "query context grew with group count: {small_self} -> {big_self}"
    );
    assert!(
        big_tree <= small_tree + SLACK,
        "query context subtree grew with group count: {small_tree} -> {big_tree}"
    );
}

// nodeIncrementalSort.c:248-249: fcinfo->args[0] = pivot, args[1] = tuple.
// With eq(a, b) := a <= b over distinct ascending prefix keys, C's
// eq(pivot, tuple) is always true — one group, sorted by the suffix key
// only — whereas eq(tuple, pivot) is true only for equal keys.
#[test]
fn is_current_group_passes_pivot_then_tuple() {
    let rows: Vec<(Option<i32>, i32)> = (0..100).map(|i| (Some(i), (i * 37) % 101)).collect();
    let (mut node, mut estate, mut feed) = setup_with_sortop(rows.clone(), ASYM_LT);
    let out = drain(&mut node, &mut estate, &mut feed, None);
    let mut expected = rows;
    expected.sort_by_key(|&(_, b)| b);
    assert_eq!(out, expected);
}

// nodeIncrementalSort.c:185: elog(ERROR), not a process panic, when the
// ordering operator's opfamily has no equality member.
#[test]
fn missing_equality_operator_is_an_error() {
    let rows: Vec<(Option<i32>, i32)> = (0..40).map(|i| (Some(i), i)).collect();
    let (mut node, mut estate, mut feed) = setup_with_sortop(rows, NOEQ_LT);
    let err = exec_incremental_sort(&mut node, &mut estate, |es| feed.fetch(es)).unwrap_err();
    assert_eq!(
        err.message(),
        format!("missing equality operator for ordering operator {NOEQ_LT}")
    );
}

// nodeIncrementalSort.c:190: the equality operator's oprcode is checked
// before any function lookup.
#[test]
fn missing_operator_function_is_an_error() {
    let rows: Vec<(Option<i32>, i32)> = (0..40).map(|i| (Some(i), i)).collect();
    let (mut node, mut estate, mut feed) = setup_with_sortop(rows, NOFUNC_LT);
    let err = exec_incremental_sort(&mut node, &mut estate, |es| feed.fetch(es)).unwrap_err();
    assert_eq!(err.message(), format!("missing function for operator {NOFUNC_EQ}"));
}
