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

/// Lane batch feed with a direct-key face: every third row falls back to the
/// full emit path, mirroring `KeyFeed`'s coverage pattern.
struct LaneKeyFeed {
    slot: ExecSlotId,
    rows: Vec<Option<i32>>,
}

impl SortLaneBatchFeed<'static> for LaneKeyFeed {
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'static>,
    ) -> ::types_error::PgResult<Option<ExecSlotId>> {
        let v = self.rows[i as usize];
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(self.slot);
        exectuples::exec_clear_tuple(slot, mcx);
        let base = slot.base_mut();
        base.tts_values[0] = v.map_or(Datum::null(), Datum::from_i32);
        base.tts_isnull[0] = v.is_none();
        exectuples::exec_store_virtual_tuple(slot);
        Ok(Some(self.slot))
    }

    fn emit_key(&mut self, i: u32) -> Option<(Datum, bool)> {
        let idx = i as usize;
        if idx % 3 == 2 {
            return None;
        }
        let v = self.rows[idx];
        Some((v.map_or(Datum::null(), Datum::from_i32), v.is_none()))
    }
}

/// A/B the lane sort feed's direct-key arm against its full emit path (the
/// lane mirror of `datum_sort_direct_key_matches_emit_path`): same rows, same
/// order, direct off vs on — identical sorted output.
#[test]
fn lane_datum_sort_direct_key_matches_emit_path() {
    let rows: Vec<Option<i32>> = (0..1000)
        .map(|i| if i % 7 == 0 { None } else { Some((i * 48271) % 997) })
        .collect();
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
        assert!(sort_lane_is_datum(&node));
        sort_lane_begin(&mut node, desc.clone()).unwrap();
        let mut feed = LaneKeyFeed { slot: in_slot, rows: rows.clone() };
        // Two batches, split mid-stream, exercising pos..n ranges.
        let n = rows.len() as u32;
        sort_lane_put_batch(&mut node, &mut estate, 0, n / 2, direct, &mut feed).unwrap();
        sort_lane_put_batch(&mut node, &mut estate, n / 2, n, direct, &mut feed).unwrap();
        sort_lane_finish(&mut node, &mut estate).unwrap();
        let mut out = Vec::new();
        while let Some(id) = sort_lane_next(&mut node, &mut estate).unwrap() {
            let mut isnull = false;
            let v = exectuples::slot_getattr(estate.slot_mut(id), 1, &mut isnull);
            out.push(if isnull { None } else { Some(v.as_i32()) });
        }
        assert_eq!(out.len(), rows.len());
        outs.push(out);
    }
    assert_eq!(outs[0], outs[1]);
}

// ---------------------------------------------------------------------------
// Lane refsort (late-materialization top-N) seams.
// ---------------------------------------------------------------------------

#[test]
fn refsort_ref_encode_roundtrip() {
    for (rg, row) in [
        (0u32, 0u32),
        (0, 1),
        (1, 0),
        (7, 12345),
        (u32::MAX, 0),
        (0, u32::MAX),
        (u32::MAX, u32::MAX),
        (0x8000_0000, 0x8000_0000),
    ] {
        let r = refsort_encode(rg, row);
        assert_eq!(refsort_decode(r), (rg, row), "ref {r:#x}");
        // Round-trips through the Datum currency the tuplesort carries.
        assert_eq!(Datum::from_i64(r).as_i64(), r);
    }
    // Refs order within one row group follows the row index (not load-bearing
    // for correctness — the sort orders by key — but documents the packing).
    assert!(refsort_encode(3, 10) < refsort_encode(3, 11));
}

/// 2-col synthetic (int4 key, int8 ref) desc, hand-built like `int4_desc`.
fn refsort_key_desc(mcx: Mcx<'static>) -> Rc<TupleDescData<'static>> {
    use ::types_tuple::TYPALIGN_DOUBLE;
    let key = FormData_pg_attribute {
        attnum: 1,
        atttypid: INT4OID,
        attlen: 4,
        attbyval: true,
        attalign: TYPALIGN_INT,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let refatt = FormData_pg_attribute {
        attnum: 2,
        atttypid: 20, // INT8OID
        atttypmod: -1,
        attlen: 8,
        attbyval: true,
        attalign: TYPALIGN_DOUBLE,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for att in [key, refatt] {
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts: 2,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

/// Full refsort node cycle: narrow bounded feed -> sorted winner refs ->
/// buffered winners served (in order) by BOTH emit faces (`sort_lane_next`
/// and the `exec_sort` fallback drain) -> rescan clears everything.
#[test]
fn refsort_feed_gather_emit_and_rescan() {
    // Outer shape: 2 int4 columns (so !datumSort), leading key = col 1.
    let (mut node, mut estate, desc, _feed) = setup(2, vec![], 0);
    sort_set_tuple_bound(&mut node, 2);
    assert!(node.bounded && node.bound == 2);
    let mcx = estate.es_query_cxt;

    let kdesc = refsort_key_desc(mcx);
    sort_lane_begin_refsort(&mut node, kdesc.clone()).unwrap();
    assert_eq!(
        sort_lane_refsort_key_desc(&node).unwrap().natts,
        2,
        "key desc memoized on the node"
    );
    // Keys 5, 1, 3, 2 at refs (rg 7, rows 100..104): bound 2 keeps 1 and 2.
    for (i, key) in [5i32, 1, 3, 2].into_iter().enumerate() {
        sort_lane_put_refsort(
            &mut node,
            Datum::from_i32(key),
            false,
            refsort_encode(7, 100 + i as u32),
        )
        .unwrap();
    }
    sort_lane_finish(&mut node, &mut estate).unwrap();
    assert!(node.sort_done());

    // Winner refs come back in sorted key order: key 1 (row 101), then key 2
    // (row 103). The caller must read AT MOST `bound` refs: a bounded
    // tuplesort ERRORS when read past its bound ("retrieved too many tuples
    // in a bounded sort"), exactly like C -- the production gather loop caps
    // at bound for this reason.
    assert_eq!(sort_lane_refsort_next_ref(&mut node).unwrap(), Some((7, 101))); // key 1
    assert_eq!(sort_lane_refsort_next_ref(&mut node).unwrap(), Some((7, 103))); // key 2

    // Buffer the gathered winners (outer format: key, payload).
    sort_lane_refsort_push_winner(
        &mut node,
        mcx,
        &[Datum::from_i32(1), Datum::from_i32(11)],
        &[false, false],
    )
    .unwrap();
    sort_lane_refsort_push_winner(
        &mut node,
        mcx,
        &[Datum::from_i32(2), Datum::from_i32(22)],
        &[false, false],
    )
    .unwrap();
    assert_eq!(sort_lane_refsort_winners(&node), 2);

    // Emit face 1: sort_lane_next pops the buffer, never the narrow sort.
    let id = sort_lane_next(&mut node, &mut estate).unwrap().unwrap();
    let mut isnull = false;
    assert_eq!(exectuples::slot_getattr(estate.slot_mut(id), 1, &mut isnull).as_i32(), 1);
    assert_eq!(exectuples::slot_getattr(estate.slot_mut(id), 2, &mut isnull).as_i32(), 11);

    // Emit face 2 (fallback safety): a mid-stream fall back to `exec_sort`'s
    // drain leg serves the SAME buffer — the outer fetch must never run
    // (sort_Done is set), and the narrow tuplesort is never read as output.
    let got = exec_sort(&mut node, &mut estate, desc.clone(), |_| {
        panic!("outer fetched after sort_Done")
    })
    .unwrap()
    .unwrap();
    assert_eq!(exectuples::slot_getattr(estate.slot_mut(got), 1, &mut isnull).as_i32(), 2);
    assert_eq!(exectuples::slot_getattr(estate.slot_mut(got), 2, &mut isnull).as_i32(), 22);

    // Drained: EOF from both faces.
    assert!(sort_lane_next(&mut node, &mut estate).unwrap().is_none());

    // Rescan (no randomAccess): refs/winners never cross a rescan.
    let need_outer = exec_rescan_sort(&mut node, &mut estate).unwrap();
    assert!(need_outer);
    assert!(!node.sort_done());
    assert_eq!(sort_lane_refsort_winners(&node), 0);
    // The node re-feeds through the ORDINARY begin afterwards (the demote /
    // non-refsort path): byte-safe legacy feed over the same node state.
    sort_lane_begin(&mut node, desc.clone()).unwrap();
    sort_lane_finish(&mut node, &mut estate).unwrap();
    assert!(sort_lane_next(&mut node, &mut estate).unwrap().is_none());
}

/// The demote reset (`sort_lane_reset_for_refeed`) drops the narrow sort,
/// the marker, and the buffer; the sticky refusal flag survives.
#[test]
fn refsort_reset_for_refeed_clears_state_and_refusal_sticks() {
    let (mut node, mut estate, desc, _feed) = setup(2, vec![], 0);
    sort_set_tuple_bound(&mut node, 4);
    let mcx = estate.es_query_cxt;
    sort_lane_begin_refsort(&mut node, refsort_key_desc(mcx)).unwrap();
    sort_lane_put_refsort(&mut node, Datum::from_i32(9), false, refsort_encode(1, 2)).unwrap();
    assert!(!sort_lane_refsort_refused(&node));
    sort_lane_refsort_refuse(&mut node);
    sort_lane_reset_for_refeed(&mut node);
    assert!(!node.sort_done());
    assert_eq!(sort_lane_refsort_winners(&node), 0);
    assert!(sort_lane_refsort_refused(&node), "demote refusal is sticky");
    // Legacy re-feed over the same node state works.
    sort_lane_begin(&mut node, desc.clone()).unwrap();
    sort_lane_finish(&mut node, &mut estate).unwrap();
    assert!(sort_lane_next(&mut node, &mut estate).unwrap().is_none());
}
