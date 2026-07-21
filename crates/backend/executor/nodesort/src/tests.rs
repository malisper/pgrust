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
const INT8OID: u32 = 20;
const INT4_LT: u32 = 97;
const INT8_LT: u32 = 412;
const INTEGER_BTREE_FAM: u32 = 1976;
const BTREE_AM: u32 = 403;
const F_BTINT4SORTSUPPORT: u32 = 3130;
const F_BTINT8SORTSUPPORT: u32 = 3131;

static SEAMS: Once = Once::new();

fn install_seams() {
    SEAMS.call_once(|| {
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                INT8OID => Some(PgTypeShape {
                    typlen: 8,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
            // int4 `<` (the plans' key-0) and int8 `<` (the refsort rule-2
            // ref tie-break column).
            let ty = match opno {
                INT4_LT => INT4OID,
                INT8_LT => INT8OID,
                other => panic!("unexpected operator lookup {other}"),
            };
            let mut v = PgVec::new_in(mcx);
            v.push(syscache_seams::PgAmopMemberShape {
                amopfamily: INTEGER_BTREE_FAM,
                amoplefttype: ty,
                amoprighttype: ty,
                amopstrategy: 1,
                amopmethod: BTREE_AM,
            });
            Ok(v)
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, left, right, procnum| {
            assert_eq!((opfamily, procnum), (INTEGER_BTREE_FAM, 2));
            Ok(match (left, right) {
                (INT4OID, INT4OID) => F_BTINT4SORTSUPPORT,
                (INT8OID, INT8OID) => F_BTINT8SORTSUPPORT,
                other => panic!("unexpected amproc lookup {other:?}"),
            })
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
/// full emit path (the deleted fused `KeyFeed` double's coverage pattern).
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
    sort_lane_begin_refsort(&mut node, kdesc.clone(), false).unwrap();
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

/// Rule-2 refsort (lazytopn): the ref column joins the comparator — the
/// bounded selection is the (key, ref-ascending) TOTAL ORDER, so full-key
/// ties at the LIMIT cut select the physically-earliest refs and emit in
/// ref order, independent of put order. Under the plain single-key
/// comparator the same feed's tie survivors are heap-shape arbitrary (the
/// sorted-limit-walk rule-2 landing's KEY FACT) — this pins the rule-2 leg exactly.
#[test]
fn refsort_rule2_ties_select_earliest_refs_in_ref_order() {
    let (mut node, mut estate, _desc, _feed) = setup(2, vec![], 0);
    sort_set_tuple_bound(&mut node, 2);
    let mcx = estate.es_query_cxt;
    sort_lane_begin_refsort(&mut node, refsort_key_desc(mcx), true).unwrap();
    // Three full-key ties (key 1) put in NON-ref order + one worse key.
    // Rule-2 order: (1,100) < (1,102) < (1,103) < (2,101); bound 2 keeps
    // the two physically-earliest tie members.
    for (key, rg, row) in [(1i32, 7u32, 103u32), (2, 7, 101), (1, 7, 100), (1, 7, 102)] {
        sort_lane_put_refsort(&mut node, Datum::from_i32(key), false, refsort_encode(rg, row))
            .unwrap();
    }
    sort_lane_finish(&mut node, &mut estate).unwrap();
    assert_eq!(sort_lane_refsort_next_ref(&mut node).unwrap(), Some((7, 100)));
    assert_eq!(sort_lane_refsort_next_ref(&mut node).unwrap(), Some((7, 102)));
}

/// The demote reset (`sort_lane_reset_for_refeed`) drops the narrow sort,
/// the marker, and the buffer; the sticky refusal flag survives.
#[test]
fn refsort_reset_for_refeed_clears_state_and_refusal_sticks() {
    let (mut node, mut estate, desc, _feed) = setup(2, vec![], 0);
    sort_set_tuple_bound(&mut node, 4);
    let mcx = estate.es_query_cxt;
    sort_lane_begin_refsort(&mut node, refsort_key_desc(mcx), false).unwrap();
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

// ---------------------------------------------------------------------------
// for_each_put: the skip-mask put iterator must visit EXACTLY the positions
// the plain pos..n loop would offer to an emit that answers None on
// bit-cleared rows — same positions, same order — for arbitrary masks,
// pos offsets and ragged tails. (The sort feed's put-stream identity.)
// ---------------------------------------------------------------------------
#[test]
fn for_each_put_matches_plain_loop_on_set_bits() {
    // Deterministic xorshift; no external deps.
    let mut seed = 0x9E3779B97F4A7C15u64;
    let mut rng = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for case in 0..500 {
        let n: u32 = match case % 5 {
            0 => 1,
            1 => 63,
            2 => 64,
            3 => 65,
            _ => (rng() % (64 * exectuples::SOA_BM_WORDS as u64 - 1) + 1) as u32,
        };
        let pos: u32 = (rng() % (n as u64 + 1)) as u32;
        let mut words = [0u64; exectuples::SOA_BM_WORDS];
        for w in words.iter_mut() {
            *w = match case % 4 {
                0 => 0,
                1 => !0,
                _ => rng(),
            };
        }
        // Producer contract: bits at/past n are zero.
        for i in n..(64 * exectuples::SOA_BM_WORDS as u32) {
            words[(i / 64) as usize] &= !(1u64 << (i % 64));
        }
        let expected: Vec<u32> = (pos..n)
            .filter(|&i| words[(i / 64) as usize] & (1u64 << (i % 64)) != 0)
            .collect();
        let mut got = Vec::new();
        for_each_put(Some(&words), pos, n, |i| {
            got.push(i);
            Ok(())
        })
        .unwrap();
        assert_eq!(got, expected, "case {case} n={n} pos={pos}");
        // None mask = the plain loop.
        let mut all = Vec::new();
        for_each_put(None, pos, n, |i| {
            all.push(i);
            Ok(())
        })
        .unwrap();
        assert_eq!(all, (pos..n).collect::<Vec<_>>());
    }
}

// --- WS-AD wave-8: randomAccess lane-feed delegation units -----------------
// Acceptance ladder 2 (the delegation unit): a lane-leg feed
// (sort_lane_begin/put/finish) of a randomAccess node leaves the ROW-PATH
// Tuplesort as the ONE read-back face (`sort_lane_readback_delegated`), and
// every random-access read — rescan replay, mark/restore, backward — runs
// on it interchangeably with `exec_sort` over the same node state (the
// production fallback the direction gate lands on). The backward leg is
// proven differentially against a pure-`exec_sort` control node driven by
// the identical read script, so no read semantics are assumed.

/// The lane feed legs, exactly as the breaker drives them: begin → per-row
/// put → finish (performsort + phase flip).
fn lane_feed(
    node: &mut SortState<'static>,
    estate: &mut EStateData<'static>,
    desc: &Rc<TupleDescData<'static>>,
    feed: &mut Feed,
) {
    sort_lane_begin(node, desc.clone()).unwrap();
    while let Some(id) = feed.fetch(estate).unwrap() {
        sort_lane_put(node, estate, id).unwrap();
    }
    sort_lane_finish(node, estate).unwrap();
}

fn slot_row(
    estate: &mut EStateData<'static>,
    id: ExecSlotId,
    natts: i32,
) -> Vec<Option<i32>> {
    let slot = estate.slot_mut(id);
    (1..=natts)
        .map(|a| {
            let mut isnull = false;
            let v = exectuples::slot_getattr(slot, a, &mut isnull);
            if isnull { None } else { Some(v.as_i32()) }
        })
        .collect()
}

/// One forward read on the LANE emit face (`sort_lane_next` — what the
/// breaker's Source serves per pull).
fn lane_next_row(
    node: &mut SortState<'static>,
    estate: &mut EStateData<'static>,
    natts: i32,
) -> Option<Vec<Option<i32>>> {
    sort_lane_next(node, estate)
        .unwrap()
        .map(|id| slot_row(estate, id, natts))
}

/// One read on the ROW-PATH face (`exec_sort` over a drained feed closure —
/// the fallback every non-forward pull takes), in direction `dir`.
fn row_path_read(
    node: &mut SortState<'static>,
    estate: &mut EStateData<'static>,
    desc: &Rc<TupleDescData<'static>>,
    dir: ::types_scan::sdir::ScanDirection,
    natts: i32,
) -> Option<Vec<Option<i32>>> {
    estate.es_direction = dir;
    let got = exec_sort(node, estate, desc.clone(), |_| Ok(None)).unwrap();
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    got.map(|id| slot_row(estate, id, natts))
}

#[test]
fn lane_feed_random_access_delegates_and_rescan_replays() {
    let rows = vec![vec![Some(3)], vec![Some(1)], vec![Some(2)]];
    let (mut node, mut estate, desc, mut feed) = setup(1, rows, EXEC_FLAG_REWIND);
    assert!(node.randomAccess);
    // RA side-memo roundtrip (the bare-hook verdict store).
    assert_eq!(sort_lane_ra_fusible(&node), None);
    sort_lane_ra_fusible_set(&mut node, true);
    assert_eq!(sort_lane_ra_fusible(&node), Some(true));

    lane_feed(&mut node, &mut estate, &desc, &mut feed);
    // The one read-back face is the row-path Tuplesort (no substituted
    // lane emit face) — randomAccess reads are sound exactly here.
    assert!(sort_lane_readback_delegated(&node));

    // Forward drain on the lane face.
    let sorted = vec![vec![Some(1)], vec![Some(2)], vec![Some(3)]];
    let mut out = Vec::new();
    while let Some(r) = lane_next_row(&mut node, &mut estate, 1) {
        out.push(r);
    }
    assert_eq!(out, sorted);

    // Rewind: the randomAccess arm preserves the tuplesort (no re-sort, no
    // child rescan) and the ROW-PATH drain replays it — cross-face
    // byte-identity over the same node state.
    let need_outer = exec_rescan_sort(&mut node, &mut estate).unwrap();
    assert!(!need_outer);
    assert!(sort_lane_readback_delegated(&node));
    let replay = drain(&mut node, &mut estate, &desc, &mut feed);
    assert_eq!(replay, sorted);
}

#[test]
fn lane_feed_random_access_mark_restore_delegates() {
    let rows = vec![vec![Some(2)], vec![Some(3)], vec![Some(1)]];
    let (mut node, mut estate, desc, mut feed) =
        setup(1, rows, ::types_slot::EXEC_FLAG_MARK);
    assert!(node.randomAccess);
    lane_feed(&mut node, &mut estate, &desc, &mut feed);
    assert!(sort_lane_readback_delegated(&node));

    // Read 1 on the lane face, mark, read 2 and 3, restore: the mark/
    // restore protocol operates on the delegated tuplesort directly, and
    // the post-restore read resumes at the marked position on the ROW-PATH
    // face (cross-face), then the lane face continues in step.
    assert_eq!(lane_next_row(&mut node, &mut estate, 1), Some(vec![Some(1)]));
    exec_sort_mark_pos(&mut node).unwrap();
    assert_eq!(lane_next_row(&mut node, &mut estate, 1), Some(vec![Some(2)]));
    assert_eq!(lane_next_row(&mut node, &mut estate, 1), Some(vec![Some(3)]));
    exec_sort_restr_pos(&mut node).unwrap();
    assert_eq!(
        row_path_read(&mut node, &mut estate, &desc, ::types_scan::sdir::ForwardScanDirection, 1),
        Some(vec![Some(2)])
    );
    assert_eq!(lane_next_row(&mut node, &mut estate, 1), Some(vec![Some(3)]));
    assert_eq!(lane_next_row(&mut node, &mut estate, 1), None);
}

// (lane_feed_random_access_backward_matches_row_path retired with the
// backward-execution wave B6: it pinned the mixed-face F F F B B F script
// where BACKWARD reads fell to the row-path drain — that drain is deleted;
// the run seam refuses backward entry since deletion-prep B1, and backward
// cursor reads are served by the portal tuplestore. Forward lane/row-path
// parity, rescan replay, and mark/restore delegation keep their own pins
// above.)
