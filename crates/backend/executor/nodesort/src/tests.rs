use std::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Sort;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_REWIND};
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, PgTypeShape, TupleDescData, TYPALIGN_INT,
    TYPSTORAGE_PLAIN,
};

use crate::*;

const INT4OID: u32 = 23;
const INT4_LT: u32 = 97;
const INTEGER_BTREE_FAM: u32 = 1976;
const BTREE_AM: u32 = 403;
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
        syscache_seams::lookup_pg_amproc::set(|opfamily, left, right, procnum| {
            assert_eq!((opfamily, left, right, procnum), (INTEGER_BTREE_FAM, INT4OID, INT4OID, 2));
            Ok(F_BTINT4SORTSUPPORT)
        });
    });
}

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("nodesort-test")));
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

fn mk_sort_plan(mcx: Mcx<'static>, ncols: usize) -> &'static Sort<'static> {
    let mut sort = Node::build::<Sort>(mcx).unwrap();
    sort.numCols = ncols as i32;
    sort.sortColIdx = ::mcx::slice_borrow_in(mcx, &(1..=ncols as i16).collect::<Vec<_>>()).unwrap();
    sort.sortOperators = ::mcx::slice_borrow_in(mcx, &vec![INT4_LT; ncols]).unwrap();
    sort.collations = ::mcx::slice_borrow_in(mcx, &vec![0u32; ncols]).unwrap();
    sort.nullsFirst = ::mcx::slice_borrow_in(mcx, &vec![false; ncols]).unwrap();
    sort.seal().as_sort().unwrap()
}

struct Feed {
    slot: ExecSlotId,
    rows: Vec<Vec<Option<i32>>>,
    next: usize,
}

impl Feed {
    fn fetch(&mut self, estate: &mut EStateData<'static>) -> ::types_error::PgResult<Option<ExecSlotId>> {
        if self.next >= self.rows.len() {
            return Ok(None);
        }
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(self.slot);
        exectuples::exec_clear_tuple(slot, mcx);
        let base = slot.base_mut();
        for (i, v) in self.rows[self.next].iter().enumerate() {
            base.tts_values[i] = v.map_or(Datum::null(), Datum::from_i32);
            base.tts_isnull[i] = v.is_none();
        }
        exectuples::exec_store_virtual_tuple(slot);
        self.next += 1;
        Ok(Some(self.slot))
    }
}

fn drain(
    node: &mut SortState<'static>,
    estate: &mut EStateData<'static>,
    outer_desc: &Rc<TupleDescData<'static>>,
    feed: &mut Feed,
) -> Vec<Vec<Option<i32>>> {
    let natts = outer_desc.natts;
    let mut out = Vec::new();
    loop {
        let got = exec_sort(node, estate, outer_desc.clone(), |es| feed.fetch(es)).unwrap();
        let Some(id) = got else { break };
        let slot = estate.slot_mut(id);
        let mut row = Vec::new();
        for a in 1..=natts {
            let mut isnull = false;
            let v = exectuples::slot_getattr(slot, a, &mut isnull);
            row.push(if isnull { None } else { Some(v.as_i32()) });
        }
        out.push(row);
    }
    out
}

fn setup(
    ncols: usize,
    rows: Vec<Vec<Option<i32>>>,
    eflags: i32,
) -> (SortState<'static>, EStateData<'static>, Rc<TupleDescData<'static>>, Feed) {
    install_seams();
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, ncols as i32);
    let mut estate = EStateData::new_in(mcx);
    let in_slot = estate.exec_init_extra_tuple_slot(Some(desc.clone()), TupleSlotKind::Virtual);
    let plan = mk_sort_plan(mcx, ncols);
    let node = exec_init_sort(plan, &mut estate, eflags, &desc, desc.clone()).unwrap();
    let feed = Feed { slot: in_slot, rows, next: 0 };
    (node, estate, desc, feed)
}

#[test]
fn datum_sort_lane_single_column() {
    let rows: Vec<Vec<Option<i32>>> =
        vec![vec![Some(3)], vec![None], vec![Some(1)], vec![Some(2)], vec![None]];
    let (mut node, mut estate, desc, mut feed) = setup(1, rows, 0);
    let out = drain(&mut node, &mut estate, &desc, &mut feed);
    assert_eq!(
        out,
        vec![vec![Some(1)], vec![Some(2)], vec![Some(3)], vec![None], vec![None]]
    );
}

#[test]
fn heap_sort_lane_two_columns() {
    let rows = vec![
        vec![Some(2), Some(9)],
        vec![Some(1), Some(8)],
        vec![Some(2), Some(1)],
        vec![Some(1), Some(3)],
    ];
    let (mut node, mut estate, desc, mut feed) = setup(2, rows, 0);
    let out = drain(&mut node, &mut estate, &desc, &mut feed);
    assert_eq!(
        out,
        vec![
            vec![Some(1), Some(3)],
            vec![Some(1), Some(8)],
            vec![Some(2), Some(1)],
            vec![Some(2), Some(9)],
        ]
    );
}

#[test]
fn rescan_with_random_access_replays_without_resort() {
    let rows = vec![vec![Some(2)], vec![Some(1)]];
    let (mut node, mut estate, desc, mut feed) = setup(1, rows, EXEC_FLAG_REWIND);
    let out = drain(&mut node, &mut estate, &desc, &mut feed);
    assert_eq!(out, vec![vec![Some(1)], vec![Some(2)]]);
    let need_outer = exec_rescan_sort(&mut node, &mut estate).unwrap();
    assert!(!need_outer);
    let out = drain(&mut node, &mut estate, &desc, &mut feed);
    assert_eq!(out, vec![vec![Some(1)], vec![Some(2)]]);
}

#[test]
fn rescan_without_random_access_resorts() {
    let rows = vec![vec![Some(2)], vec![Some(1)]];
    let (mut node, mut estate, desc, mut feed) = setup(1, rows.clone(), 0);
    let out = drain(&mut node, &mut estate, &desc, &mut feed);
    assert_eq!(out.len(), 2);
    let need_outer = exec_rescan_sort(&mut node, &mut estate).unwrap();
    assert!(need_outer);
    feed.next = 0;
    let out = drain(&mut node, &mut estate, &desc, &mut feed);
    assert_eq!(out, vec![vec![Some(1)], vec![Some(2)]]);
}

struct KeyFeed {
    slot: ExecSlotId,
    rows: Vec<Option<i32>>,
    batch: usize,
    base: usize,
    n: u32,
    // Rows whose index % 3 == 2 report as fallback and take the emit path.
    direct: bool,
}

impl SortFeedSource<'static> for KeyFeed {
    fn next_batch(&mut self, _estate: &mut EStateData<'static>) -> ::types_error::PgResult<u32> {
        self.base += self.n as usize;
        self.n = self.batch.min(self.rows.len() - self.base) as u32;
        Ok(self.n)
    }

    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'static>,
    ) -> ::types_error::PgResult<Option<ExecSlotId>> {
        let v = self.rows[self.base + i as usize];
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(self.slot);
        exectuples::exec_clear_tuple(slot, mcx);
        let base = slot.base_mut();
        base.tts_values[0] = v.map_or(Datum::null(), Datum::from_i32);
        base.tts_isnull[0] = v.is_none();
        exectuples::exec_store_virtual_tuple(slot);
        Ok(Some(self.slot))
    }

    fn key_direct(&mut self, _estate: &mut EStateData<'static>) -> bool {
        self.direct
    }

    fn emit_key(&mut self, i: u32) -> Option<(Datum, bool)> {
        let idx = self.base + i as usize;
        if idx % 3 == 2 {
            return None;
        }
        let v = self.rows[idx];
        Some((v.map_or(Datum::null(), Datum::from_i32), v.is_none()))
    }
}

fn drain_batched(
    node: &mut SortState<'static>,
    estate: &mut EStateData<'static>,
    outer_desc: &Rc<TupleDescData<'static>>,
    feed: &mut KeyFeed,
    limit: usize,
) -> Vec<Option<i32>> {
    let mut out = Vec::new();
    while out.len() < limit {
        let got = exec_sort_batched(node, estate, outer_desc.clone(), &mut *feed).unwrap();
        let Some(id) = got else { break };
        let mut isnull = false;
        let v = exectuples::slot_getattr(estate.slot_mut(id), 1, &mut isnull);
        out.push(if isnull { None } else { Some(v.as_i32()) });
    }
    out
}

impl<'mcx, S: SortFeedSource<'mcx>> SortFeedSource<'mcx> for &mut S {
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> ::types_error::PgResult<u32> {
        (**self).next_batch(estate)
    }
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> ::types_error::PgResult<Option<ExecSlotId>> {
        (**self).emit(i, estate)
    }
    fn key_direct(&mut self, estate: &mut EStateData<'mcx>) -> bool {
        (**self).key_direct(estate)
    }
    fn emit_key(&mut self, i: u32) -> Option<(Datum, bool)> {
        (**self).emit_key(i)
    }
}

#[test]
fn datum_sort_direct_key_matches_emit_path() {
    let rows: Vec<Option<i32>> = (0..1000)
        .map(|i| if i % 7 == 0 { None } else { Some((i * 48271) % 997) })
        .collect();
    for bound in [None, Some(10)] {
        let mut outs = Vec::new();
        for direct in [false, true] {
            install_seams();
            let mcx = leaked_mcx();
            let desc = int4_desc(mcx, 1);
            let mut estate = EStateData::new_in(mcx);
            let in_slot =
                estate.exec_init_extra_tuple_slot(Some(desc.clone()), TupleSlotKind::Virtual);
            let plan = mk_sort_plan(mcx, 1);
            let mut node = exec_init_sort(plan, &mut estate, 0, &desc, desc.clone()).unwrap();
            if let Some(b) = bound {
                sort_set_tuple_bound(&mut node, b);
            }
            let mut feed =
                KeyFeed { slot: in_slot, rows: rows.clone(), batch: 96, base: 0, n: 0, direct };
            let out = drain_batched(
                &mut node,
                &mut estate,
                &desc,
                &mut feed,
                bound.map_or(usize::MAX, |b| b as usize),
            );
            outs.push(out);
        }
        assert_eq!(outs[0], outs[1], "bound={bound:?}");
        assert_eq!(outs[0].len(), bound.map_or(1000, |b| b as usize));
    }
}

#[test]
fn bound_pushdown_uses_bounded_sort() {
    let rows: Vec<Vec<Option<i32>>> = (0..500).rev().map(|i| vec![Some(i)]).collect();
    let (mut node, mut estate, desc, mut feed) = setup(1, rows, 0);
    sort_set_tuple_bound(&mut node, 3);
    assert!(node.bounded && node.bound == 3);
    let mut out = Vec::new();
    for _ in 0..3 {
        let id = exec_sort(&mut node, &mut estate, desc.clone(), |es| feed.fetch(es))
            .unwrap()
            .unwrap();
        let mut isnull = false;
        out.push(exectuples::slot_getattr(estate.slot_mut(id), 1, &mut isnull).as_i32());
    }
    assert_eq!(out, vec![0, 1, 2]);
}
