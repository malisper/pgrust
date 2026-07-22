use std::rc::Rc;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::{CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN};

use crate::*;

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("tuplesort-test")));
    m.mcx()
}

fn int4_desc(mcx: Mcx<'static>, natts: i32) -> Rc<TupleDescData<'static>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for i in 0..natts {
        let att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
            atttypid: 23,
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

fn int32_key(attno: i16, nulls_first: bool, reverse: bool) -> SortSupport {
    SortSupport {
        ssup_collation: 0,
        ssup_reverse: reverse,
        ssup_nulls_first: nulls_first,
        ssup_attno: attno,
        comparator: SortComparator::Int32,
    }
}

// Deterministic pseudo-random stream (LCG); varied inputs, stable tests.
fn lcg(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

fn datum_sort_oracle(mut input: Vec<Option<i32>>, nulls_first: bool, reverse: bool) -> Vec<Option<i32>> {
    input.sort_by(|a, b| {
        use std::cmp::Ordering;
        match (a, b) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => if nulls_first { Ordering::Less } else { Ordering::Greater },
            (Some(_), None) => if nulls_first { Ordering::Greater } else { Ordering::Less },
            (Some(x), Some(y)) => if reverse { y.cmp(x) } else { x.cmp(y) },
        }
    });
    input
}

fn run_datum_sort(
    input: &[Option<i32>],
    nulls_first: bool,
    reverse: bool,
    sortopt: i32,
    bound: Option<i64>,
) -> (Tuplesort, Vec<Option<i32>>) {
    let mut ts = Tuplesort::begin_datum_with_key(int32_key(1, nulls_first, reverse), 1024, sortopt);
    if let Some(b) = bound {
        ts.set_bound(b);
    }
    for v in input {
        ts.putdatum(v.map_or(Datum::null(), Datum::from_i32), v.is_none()).unwrap();
    }
    ts.performsort().unwrap();
    let mut out = Vec::new();
    // C forbids draining a bounded sort past its bound.
    let limit = bound.map_or(usize::MAX, |b| b as usize);
    while out.len() < limit {
        let Some(nd) = ts.getdatum(true).unwrap() else {
            break;
        };
        out.push(if nd.isnull { None } else { Some(nd.value.as_i32()) });
    }
    (ts, out)
}

#[test]
fn datum_sort_matches_oracle_all_orderings() {
    let mut seed = 42u64;
    let mut input: Vec<Option<i32>> = (0..5000)
        .map(|_| {
            let r = lcg(&mut seed);
            if r % 17 == 0 { None } else { Some((r % 1000) as i32 - 500) }
        })
        .collect();
    input.push(Some(i32::MAX));
    input.push(Some(i32::MIN));
    for (nulls_first, reverse) in [(false, false), (true, false), (false, true), (true, true)] {
        let (_ts, got) = run_datum_sort(&input, nulls_first, reverse, TUPLESORT_NONE, None);
        assert_eq!(got, datum_sort_oracle(input.clone(), nulls_first, reverse));
    }
}

#[test]
fn datum_sort_empty_and_single() {
    let (_ts, got) = run_datum_sort(&[], false, false, TUPLESORT_NONE, None);
    assert!(got.is_empty());
    let (_ts, got) = run_datum_sort(&[Some(7)], false, false, TUPLESORT_NONE, None);
    assert_eq!(got, vec![Some(7)]);
}

#[test]
fn datum_sort_grows_memtuples_past_initial_size() {
    let mut seed = 7u64;
    let input: Vec<Option<i32>> = (0..20_000).map(|_| Some(lcg(&mut seed) as i32)).collect();
    let (_ts, got) = run_datum_sort(&input, false, false, TUPLESORT_NONE, None);
    assert_eq!(got, datum_sort_oracle(input, false, false));
}

fn run_datum_sort_batched(
    input: &[Option<i32>],
    sortopt: i32,
    bound: Option<i64>,
) -> (Tuplesort, Vec<Option<i32>>) {
    let mut ts = Tuplesort::begin_datum_with_key(int32_key(1, false, false), 1024, sortopt);
    if let Some(b) = bound {
        ts.set_bound(b);
    }
    for chunk in input.chunks(777) {
        ts.putdatum_batch(|p| {
            for v in chunk {
                p.put(v.map_or(Datum::null(), Datum::from_i32), v.is_none())?;
            }
            Ok(())
        })
        .unwrap();
        ts.putdatum(Datum::from_i32(-1), false).unwrap();
    }
    ts.performsort().unwrap();
    let mut out = Vec::new();
    let limit = bound.map_or(usize::MAX, |b| b as usize);
    while out.len() < limit {
        let Some(nd) = ts.getdatum(true).unwrap() else { break };
        out.push(if nd.isnull { None } else { Some(nd.value.as_i32()) });
    }
    (ts, out)
}

#[test]
fn batched_putdatum_matches_oracle_across_grow_and_bounds() {
    let mut seed = 11u64;
    let input: Vec<Option<i32>> = (0..20_000)
        .map(|_| {
            let r = lcg(&mut seed);
            if r % 13 == 0 { None } else { Some(r as i32) }
        })
        .collect();
    let mut expected: Vec<Option<i32>> = input.clone();
    expected.extend(std::iter::repeat(Some(-1)).take(input.chunks(777).count()));
    let oracle = datum_sort_oracle(expected.clone(), false, false);

    let (_ts, got) = run_datum_sort_batched(&input, TUPLESORT_NONE, None);
    assert_eq!(got, oracle);

    let (ts, got) = run_datum_sort_batched(&input, TUPLESORT_ALLOWBOUNDED, Some(50));
    assert!(ts.used_bound());
    assert_eq!(got, oracle[..50]);
}

#[test]
fn batched_putdatum_small_batches_and_empty() {
    let mut ts = Tuplesort::begin_datum_with_key(int32_key(1, false, false), 1024, TUPLESORT_NONE);
    ts.putdatum_batch(|_| Ok(())).unwrap();
    ts.putdatum_batch(|p| p.put(Datum::from_i32(3), false)).unwrap();
    ts.putdatum_batch(|p| {
        p.put(Datum::from_i32(1), false)?;
        p.put(Datum::null(), true)?;
        p.put(Datum::from_i32(2), false)
    })
    .unwrap();
    ts.performsort().unwrap();
    let mut out = Vec::new();
    while let Some(nd) = ts.getdatum(true).unwrap() {
        out.push(if nd.isnull { None } else { Some(nd.value.as_i32()) });
    }
    assert_eq!(out, vec![Some(1), Some(2), Some(3), None]);
}

#[test]
fn bounded_top_n_heapsort_used_and_correct() {
    let mut seed = 99u64;
    let input: Vec<Option<i32>> = (0..10_000)
        .map(|_| if lcg(&mut seed) % 31 == 0 { None } else { Some(lcg(&mut seed) as i32) })
        .collect();
    for (nulls_first, reverse) in [(false, false), (true, true)] {
        let (ts, got) =
            run_datum_sort(&input, nulls_first, reverse, TUPLESORT_ALLOWBOUNDED, Some(100));
        assert!(ts.used_bound());
        assert_eq!(got.len(), 100);
        let oracle = datum_sort_oracle(input.clone(), nulls_first, reverse);
        assert_eq!(got, oracle[..100]);
    }
}

// Lane top-k cutoff boundary accessor: None until the bounded heap fills
// (TSS_BOUNDED), then always the WORST surviving top-k member (the k-th
// boundary), monotonically tightening — and every value strictly worse than
// the boundary on the (only) key is exactly what puttuple_bounded discards.
#[test]
fn topk_boundary_tracks_kth_worst_and_tightens() {
    let mut ts =
        Tuplesort::begin_datum_with_key(int32_key(1, false, false), 1024, TUPLESORT_ALLOWBOUNDED);
    ts.set_bound(3);
    assert_eq!(ts.topk_boundary(), None, "no boundary before any put");
    let mut kept: Vec<i32> = Vec::new();
    let mut seed = 1234u64;
    let mut last_boundary: Option<i32> = None;
    for i in 0..5000 {
        let v = (lcg(&mut seed) % 100_000) as i32 - 50_000;
        ts.putdatum(Datum::from_i32(v), false).unwrap();
        kept.push(v);
        kept.sort_unstable();
        kept.truncate(3);
        match ts.topk_boundary() {
            None => {
                // The heap-mode transition happens once memtuples outgrows
                // 2*bound; before that the boundary is unavailable and the
                // pre-filter must stay disengaged.
                assert!(i < 16, "boundary still None after the bounded transition");
            }
            Some((d, isnull)) => {
                assert!(!isnull);
                let b = d.as_i32();
                assert_eq!(b, kept[2], "boundary = current 3rd-best (worst survivor)");
                if let Some(prev) = last_boundary {
                    assert!(b <= prev, "ASC boundary only tightens");
                }
                last_boundary = Some(b);
            }
        }
    }
    ts.performsort().unwrap();
    let mut out = Vec::new();
    while out.len() < 3 {
        let Some(nd) = ts.getdatum(true).unwrap() else { break };
        out.push(nd.value.as_i32());
    }
    assert_eq!(out, kept);
}

#[test]
fn bounded_larger_than_input_falls_back_to_quicksort() {
    let input: Vec<Option<i32>> = vec![Some(3), Some(1), Some(2)];
    let (ts, got) = run_datum_sort(&input, false, false, TUPLESORT_ALLOWBOUNDED, Some(100));
    assert!(!ts.used_bound());
    assert_eq!(got, vec![Some(1), Some(2), Some(3)]);
}

#[test]
fn random_access_backward_rescan_markpos() {
    let input: Vec<Option<i32>> = vec![Some(5), Some(1), Some(9), Some(3)];
    let mut ts =
        Tuplesort::begin_datum_with_key(int32_key(1, false, false), 1024, TUPLESORT_RANDOMACCESS);
    for v in &input {
        ts.putdatum(Datum::from_i32(v.unwrap()), false).unwrap();
    }
    ts.performsort().unwrap();
    assert_eq!(ts.getdatum(true).unwrap().unwrap().value.as_i32(), 1);
    assert_eq!(ts.getdatum(true).unwrap().unwrap().value.as_i32(), 3);
    ts.markpos();
    assert_eq!(ts.getdatum(true).unwrap().unwrap().value.as_i32(), 5);
    // Backward: returns the tuple before the last-returned one.
    assert_eq!(ts.getdatum(false).unwrap().unwrap().value.as_i32(), 3);
    ts.restorepos();
    assert_eq!(ts.getdatum(true).unwrap().unwrap().value.as_i32(), 5);
    assert_eq!(ts.getdatum(true).unwrap().unwrap().value.as_i32(), 9);
    assert!(ts.getdatum(true).unwrap().is_none());
    // Backward off EOF re-returns the last tuple.
    assert_eq!(ts.getdatum(false).unwrap().unwrap().value.as_i32(), 9);
    ts.rescan();
    assert_eq!(ts.getdatum(true).unwrap().unwrap().value.as_i32(), 1);
    ts.end();
}

fn store_row(slot: &mut SlotData<'static>, mcx: Mcx<'static>, vals: &[Option<i32>]) {
    exectuples::exec_clear_tuple(slot, mcx);
    let base = slot.base_mut();
    for (i, v) in vals.iter().enumerate() {
        base.tts_values[i] = v.map_or(Datum::null(), Datum::from_i32);
        base.tts_isnull[i] = v.is_none();
    }
    exectuples::exec_store_virtual_tuple(slot);
}

#[test]
fn heap_sort_two_keys_with_tiebreak() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 2);
    let mut in_slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
    let mut out_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));

    let keys = [int32_key(1, false, false), int32_key(2, false, true)];
    let mut ts = Tuplesort::begin_heap_with_keys(desc.clone(), &keys, 1024, TUPLESORT_NONE);

    let mut seed = 5u64;
    let mut rows: Vec<(Option<i32>, Option<i32>)> = (0..3000)
        .map(|_| {
            let a = lcg(&mut seed) % 20;
            let b = lcg(&mut seed);
            (Some(a as i32), if b % 13 == 0 { None } else { Some((b % 50) as i32) })
        })
        .collect();
    for (a, b) in &rows {
        store_row(&mut in_slot, mcx, &[*a, *b]);
        ts.puttupleslot(&mut in_slot, mcx).unwrap();
    }
    ts.performsort().unwrap();

    // Oracle: key1 ASC NULLS LAST, key2 DESC NULLS LAST (ssup_reverse does
    // not affect null ordering; only ssup_nulls_first does).
    rows.sort_by(|x, y| {
        let k1 = match (x.0, y.0) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, _) => std::cmp::Ordering::Greater,
            (_, None) => std::cmp::Ordering::Less,
            (Some(a), Some(b)) => a.cmp(&b),
        };
        k1.then_with(|| match (x.1, y.1) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, _) => std::cmp::Ordering::Greater,
            (_, None) => std::cmp::Ordering::Less,
            (Some(a), Some(b)) => b.cmp(&a),
        })
    });

    let mut got = Vec::new();
    while ts.gettupleslot(true, false, &mut out_slot, mcx).unwrap() {
        let mut n1 = false;
        let mut n2 = false;
        let v1 = exectuples::slot_getattr(&mut out_slot, 1, &mut n1);
        let v2 = exectuples::slot_getattr(&mut out_slot, 2, &mut n2);
        got.push((
            if n1 { None } else { Some(v1.as_i32()) },
            if n2 { None } else { Some(v2.as_i32()) },
        ));
    }
    assert_eq!(got.len(), rows.len());
    for (g, o) in got.iter().zip(rows.iter()) {
        assert_eq!(g.0, o.0);
        assert_eq!(g.1, o.1);
    }
    assert!(out_slot.base().is_empty());
    ts.end();
}

#[test]
fn heap_sort_gettupleslot_copy_survives() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut in_slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
    let mut out_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));

    let mut ts = Tuplesort::begin_heap_with_keys(
        desc.clone(),
        &[int32_key(1, false, false)],
        1024,
        TUPLESORT_NONE,
    );
    for v in [3, 1, 2] {
        store_row(&mut in_slot, mcx, &[Some(v)]);
        ts.puttupleslot(&mut in_slot, mcx).unwrap();
    }
    ts.performsort().unwrap();
    assert!(ts.gettupleslot(true, true, &mut out_slot, mcx).unwrap());
    ts.end();
    let mut isnull = false;
    assert_eq!(exectuples::slot_getattr(&mut out_slot, 1, &mut isnull).as_i32(), 1);
    assert!(!isnull);
}

#[test]
fn unsigned_and_signed_comparator_arms() {
    for (cmp, vals, expect) in [
        (
            SortComparator::SignedI64,
            vec![Datum::from_i64(-1), Datum::from_i64(5), Datum::from_i64(i64::MIN)],
            vec![i64::MIN, -1, 5],
        ),
        (
            SortComparator::Unsigned,
            vec![Datum::from_u64(u64::MAX), Datum::from_u64(0), Datum::from_u64(7)],
            vec![0, 7, u64::MAX as i64],
        ),
    ] {
        let key = SortSupport {
            ssup_collation: 0,
            ssup_reverse: false,
            ssup_nulls_first: false,
            ssup_attno: 1,
            comparator: cmp,
        };
        let mut ts = Tuplesort::begin_datum_with_key(key, 1024, TUPLESORT_NONE);
        for v in &vals {
            ts.putdatum(*v, false).unwrap();
        }
        ts.performsort().unwrap();
        let mut got = Vec::new();
        while let Some(nd) = ts.getdatum(true).unwrap() {
            got.push(nd.value.as_u64() as i64);
        }
        assert_eq!(got, expect);
    }
}

// Miri-scale coverage of every unsafe path: qsort med3-of-9 + partition
// (n > 40), bounded heap ops, tiebreak minimal_getattr, borrowed-slot store.
#[test]
fn miri_scale_unsafe_paths() {
    let mut seed = 3u64;
    let input: Vec<Option<i32>> = (0..120)
        .map(|_| {
            let r = lcg(&mut seed);
            if r % 11 == 0 { None } else { Some((r % 8) as i32) }
        })
        .collect();
    let (_ts, got) = run_datum_sort(&input, false, false, TUPLESORT_NONE, None);
    assert_eq!(got, datum_sort_oracle(input.clone(), false, false));

    let (ts, got) = run_datum_sort(&input, true, true, TUPLESORT_ALLOWBOUNDED, Some(15));
    assert!(ts.used_bound());
    assert_eq!(got, datum_sort_oracle(input.clone(), true, true)[..15]);

    let (ts, got) = run_datum_sort_batched(&input, TUPLESORT_ALLOWBOUNDED, Some(15));
    assert!(ts.used_bound());
    let mut expected = input.clone();
    expected.extend(std::iter::repeat(Some(-1)).take(input.chunks(777).count()));
    assert_eq!(got, datum_sort_oracle(expected, false, false)[..15]);

    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 2);
    let mut in_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
    let mut out_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));
    let keys = [int32_key(1, false, false), int32_key(2, true, false)];
    let mut ts = Tuplesort::begin_heap_with_keys(desc, &keys, 1024, TUPLESORT_NONE);
    let mut seed = 9u64;
    for _ in 0..60 {
        let a = (lcg(&mut seed) % 4) as i32;
        let b = lcg(&mut seed);
        store_row(
            &mut in_slot,
            mcx,
            &[Some(a), if b % 5 == 0 { None } else { Some((b % 9) as i32) }],
        );
        ts.puttupleslot(&mut in_slot, mcx).unwrap();
    }
    ts.performsort().unwrap();
    let mut prev: Option<(Option<i32>, Option<i32>)> = None;
    while ts.gettupleslot(true, false, &mut out_slot, mcx).unwrap() {
        let (mut n1, mut n2) = (false, false);
        let v1 = exectuples::slot_getattr(&mut out_slot, 1, &mut n1);
        let v2 = exectuples::slot_getattr(&mut out_slot, 2, &mut n2);
        let cur = (
            if n1 { None } else { Some(v1.as_i32()) },
            if n2 { None } else { Some(v2.as_i32()) },
        );
        if let Some(p) = prev {
            // key1 ASC NULLS LAST, key2 ASC NULLS FIRST.
            let ord = |x: Option<i32>| x.map_or(i64::MAX, |v| v as i64);
            let ord2 = |x: Option<i32>| x.map_or(i64::MIN, |v| v as i64);
            assert!(ord(p.0) < ord(cur.0) || (p.0 == cur.0 && ord2(p.1) <= ord2(cur.1)));
        }
        prev = Some(cur);
    }
    ts.end();
}

#[test]
fn reset_recycles_batch_keeps_keys_and_max_stats() {
    let key = int32_key(1, false, false);
    let mut ts = Tuplesort::begin_datum_with_key(key, 1024, TUPLESORT_NONE);
    for v in [3i32, 1, 2] {
        ts.putdatum(Datum::from_i32(v), false).unwrap();
    }
    ts.performsort().unwrap();
    let mut out = Vec::new();
    while let Some(nd) = ts.getdatum(true).unwrap() {
        out.push(nd.value.as_i32());
    }
    assert_eq!(out, [1, 2, 3]);
    let first = ts.get_stats();
    assert_eq!(first.sortMethod, TuplesortMethod::Quicksort);

    ts.reset();
    for v in [9i32, 7, 8, 6] {
        ts.putdatum(Datum::from_i32(v), false).unwrap();
    }
    ts.performsort().unwrap();
    let mut out = Vec::new();
    while let Some(nd) = ts.getdatum(true).unwrap() {
        out.push(nd.value.as_i32());
    }
    assert_eq!(out, [6, 7, 8, 9]);
    // spaceUsed is the max across batches (C tuplesort_updatemax).
    assert!(ts.get_stats().spaceUsed >= first.spaceUsed);

    // Bound state does not leak across reset.
    ts.reset();
    assert!(!ts.used_bound());
}

fn tid(blk: u32, pos: u16) -> ::types_tuple::itemptr::ItemPointerData {
    ::types_tuple::itemptr::ItemPointerData {
        ip_blkid: ::types_tuple::itemptr::BlockIdData {
            bi_hi: (blk >> 16) as u16,
            bi_lo: (blk & 0xffff) as u16,
        },
        ip_posid: pos,
    }
}

fn drain_index(ts: &mut Tuplesort, desc: &TupleDescData<'_>, nkeys: usize) -> Vec<(Vec<Option<i64>>, (u32, u16))> {
    let mut out = Vec::new();
    while let Some(itup) = ts.getindextuple(true).unwrap() {
        let mut keys = Vec::new();
        for k in 1..=nkeys {
            let mut isnull = false;
            // SAFETY: live sorted image under desc.
            let d = unsafe { nbtree::itup::index_getattr(itup, k as i16, desc, &mut isnull) };
            keys.push(if isnull { None } else { Some(d.as_i32() as i64) });
        }
        // SAFETY: live image.
        let t = unsafe { nbtree::itup::t_tid(itup) };
        out.push((keys, (((t.ip_blkid.bi_hi as u32) << 16) | t.ip_blkid.bi_lo as u32, t.ip_posid)));
    }
    out
}

#[test]
fn index_sort_int4_key_then_tid_with_nulls() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplesort::begin_index_with_keys(
        desc.clone(), &[int32_key(1, false, false)], 1, false, false, "t_a_idx", None, 1024,
        TUPLESORT_NONE,
    );
    let mut seed = 3u64;
    let mut oracle: Vec<(Option<i64>, (u32, u16))> = Vec::new();
    for i in 0..400u32 {
        let r = lcg(&mut seed);
        let key = if r % 19 == 0 { None } else { Some((r % 40) as i32) };
        let t = tid(i / 100, (i % 100 + 1) as u16);
        ts.putindextuplevalues(
            t,
            &[key.map_or(Datum::null(), Datum::from_i32)],
            &[key.is_none()],
        )
        .unwrap();
        oracle.push((key.map(|k| k as i64), (i / 100, (i % 100 + 1) as u16)));
    }
    // ASC NULLS LAST, then heap TID.
    oracle.sort_by_key(|(k, t)| (k.map_or(i64::MAX, |v| v), *t));
    ts.performsort().unwrap();
    let got = drain_index(&mut ts, &desc, 1);
    let got: Vec<(Option<i64>, (u32, u16))> = got.into_iter().map(|(k, t)| (k[0], t)).collect();
    assert_eq!(got, oracle);
    ts.end();
}

#[test]
fn index_sort_two_keys_then_tid() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 2);
    let keys = [int32_key(1, false, false), int32_key(2, false, false)];
    let mut ts = Tuplesort::begin_index_with_keys(
        desc.clone(), &keys, 2, false, false, "t_ab_idx", None, 1024, TUPLESORT_NONE,
    );
    let mut seed = 9u64;
    let mut oracle = Vec::new();
    for i in 0..300u32 {
        let (a, b) = ((lcg(&mut seed) % 5) as i32, (lcg(&mut seed) % 7) as i32);
        let t = tid(i, 1);
        ts.putindextuplevalues(t, &[Datum::from_i32(a), Datum::from_i32(b)], &[false, false])
            .unwrap();
        oracle.push((vec![Some(a as i64), Some(b as i64)], (i, 1u16)));
    }
    oracle.sort_by(|x, y| x.0.cmp(&y.0).then(x.1.cmp(&y.1)));
    ts.performsort().unwrap();
    assert_eq!(drain_index(&mut ts, &desc, 2), oracle);
    ts.end();
}

/// M4.2 feed-entry equivalence: `put_index_tuple_image` (the pool arm's
/// pre-formed-image entry) must yield a sorted stream identical to
/// `putindextuplevalues` over the same logical tuples — INCLUDING under a
/// different arrival order, because the index comparator is total (key
/// then TID): the parallel scan's nondeterministic interleave cannot
/// change the sorted sequence.
#[test]
fn index_sort_image_feed_matches_values_feed() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mk = || {
        Tuplesort::begin_index_with_keys(
            desc.clone(),
            &[int32_key(1, false, false)],
            1,
            false,
            false,
            "t_a_idx",
            None,
            1024,
            TUPLESORT_NONE,
        )
    };
    let mut seed = 41u64;
    let mut rows: Vec<(i32, (u32, u16))> = Vec::new();
    for i in 0..500u32 {
        rows.push(((lcg(&mut seed) % 60) as i32, (i / 90, (i % 90 + 1) as u16)));
    }

    let mut by_values = mk();
    for (k, (blk, pos)) in &rows {
        by_values
            .putindextuplevalues(tid(*blk, *pos), &[Datum::from_i32(*k)], &[false])
            .unwrap();
    }

    // Image feed, arrival order REVERSED (the pool interleave stand-in):
    // form each tuple exactly as a worker does (index_form_tuple + t_tid
    // write), ship the raw bytes.
    let scratch = mcx::MemoryContext::new("image feed scratch");
    let mut by_image = mk();
    for (k, (blk, pos)) in rows.iter().rev() {
        let mut buf = nbtree::itup::index_form_tuple(
            scratch.mcx(),
            &desc,
            &[Datum::from_i32(*k)],
            &[false],
        )
        .unwrap();
        let t = tid(*blk, *pos);
        // SAFETY: t_tid = first 6 bytes of the owned image (itup.h).
        unsafe {
            buf.as_mut_ptr()
                .cast::<::types_tuple::itemptr::ItemPointerData>()
                .write_unaligned(t);
        }
        let len = buf.size();
        // SAFETY: freshly formed live image of `len` bytes.
        let image = unsafe { core::slice::from_raw_parts(buf.as_ptr(), len) };
        by_image.put_index_tuple_image(image).unwrap();
    }

    by_values.performsort().unwrap();
    by_image.performsort().unwrap();
    let a = drain_index(&mut by_values, &desc, 1);
    let b = drain_index(&mut by_image, &desc, 1);
    assert_eq!(a, b, "sorted stream must be entry-point- and arrival-order-independent");
    by_values.end();
    by_image.end();
}

#[test]
fn index_sort_unique_violation_is_23505() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplesort::begin_index_with_keys(
        desc, &[int32_key(1, false, false)], 1, true, false, "t_a_key", None, 1024, TUPLESORT_NONE,
    );
    for i in 0..10u16 {
        ts.putindextuplevalues(tid(0, i + 1), &[Datum::from_i32((i % 9) as i32)], &[false])
            .unwrap();
    }
    let err = ts.performsort().unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNIQUE_VIOLATION);
    assert!(err.message().contains("could not create unique index \"t_a_key\""),
        "message: {}", err.message());
}

#[test]
fn index_sort_unique_null_keys_do_not_collide() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplesort::begin_index_with_keys(
        desc.clone(), &[int32_key(1, false, false)], 1, true, false, "t_a_key", None, 1024,
        TUPLESORT_NONE,
    );
    for i in 0..8u16 {
        ts.putindextuplevalues(tid(0, i + 1), &[Datum::null()], &[true]).unwrap();
    }
    ts.performsort().unwrap();
    assert_eq!(drain_index(&mut ts, &desc, 1).len(), 8);
    ts.end();
}

fn text_desc(mcx: Mcx<'static>) -> Rc<TupleDescData<'static>> {
    use ::types_tuple::TYPSTORAGE_EXTENDED;
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: 25,
        attlen: -1,
        attbyval: false,
        attalign: TYPALIGN_INT,
        attstorage: TYPSTORAGE_EXTENDED,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 2249,
        tdtypmod: -1,
        constr: None,
        tdrefcount: -1,
        compact_attrs: compact,
        attrs,
    })
}

#[test]
fn index_sort_text_c_collation_memcmp_order() {
    let mcx = leaked_mcx();
    let desc = text_desc(mcx);
    let key = SortSupport {
        ssup_collation: 950,
        ssup_reverse: false,
        ssup_nulls_first: false,
        ssup_attno: 1,
        comparator: SortComparator::TextC,
    };
    let mut ts = Tuplesort::begin_index_with_keys(
        desc.clone(), &[key], 1, false, false, "t_txt_idx", None, 1024, TUPLESORT_NONE,
    );
    let words: Vec<&[u8]> = vec![
        b"pear", b"apple", b"Banana", b"apples", b"app", b"zebra", b"", b"apple", b"\xc3\xa9clair",
    ];
    let mut images = Vec::new();
    for w in &words {
        images.push(varlena::cstring_to_text(mcx, w).unwrap());
    }
    for (i, img) in images.iter().enumerate() {
        let d = Datum::from_usize(img.as_bytes().as_ptr() as usize);
        ts.putindextuplevalues(tid(0, (i + 1) as u16), &[d], &[false]).unwrap();
    }
    ts.performsort().unwrap();
    let mut got = Vec::new();
    while let Some(itup) = ts.getindextuple(true).unwrap() {
        let mut isnull = false;
        // SAFETY: live sorted image under desc.
        let d = unsafe { nbtree::itup::index_getattr(itup, 1, &desc, &mut isnull) };
        let p = d.as_usize() as *const u8;
        // SAFETY: datum points into the live image; short or 4B varlena.
        let payload = unsafe {
            use ::types_tuple::varatt::{varatt_is_1b, varsize_1b, varsize_4b};
            if varatt_is_1b(p) {
                std::slice::from_raw_parts(p.add(1), varsize_1b(p) - 1)
            } else {
                std::slice::from_raw_parts(p.add(4), varsize_4b(p) - 4)
            }
        };
        got.push(payload.to_vec());
    }
    let mut oracle: Vec<Vec<u8>> = words.iter().map(|w| w.to_vec()).collect();
    oracle.sort();
    assert_eq!(got, oracle);
    ts.end();
}

// ---- abbreviated keys ----

fn text_blob(payload: &[u8]) -> Box<[u64]> {
    let total = 4 + payload.len();
    let mut blob = vec![0u64; total.div_ceil(8)].into_boxed_slice();
    let base = blob.as_mut_ptr().cast::<u8>();
    // SAFETY: fresh buffer of >= total bytes.
    unsafe {
        let hdr = ::datum::varlena::set_varsize_4b(total);
        std::ptr::copy_nonoverlapping(hdr.as_ptr(), base, 4);
        std::ptr::copy_nonoverlapping(payload.as_ptr(), base.add(4), payload.len());
    }
    blob
}

fn text_key(nulls_first: bool, reverse: bool) -> SortSupport {
    SortSupport {
        ssup_collation: 0,
        ssup_reverse: reverse,
        ssup_nulls_first: nulls_first,
        ssup_attno: 1,
        comparator: SortComparator::Unsigned,
    }
}

fn text_abbrev_arm() -> AbbrevArm {
    AbbrevArm { kind: AbbrevKind::VarStrC, full_comparator: SortComparator::TextC }
}

fn begin_text_datum_abbrev(sortopt: i32) -> Tuplesort {
    Tuplesort::begin_common(
        1024,
        sortopt,
        &[text_key(false, false)],
        false,
        Some(Box::new(AbbrevState::new(text_abbrev_arm()))),
        SortVariant::Datum { byref_typlen: -1 },
    )
}

fn random_texts(n: usize, seed: u64, shared_prefix: &[u8]) -> Vec<Option<Vec<u8>>> {
    let mut s = seed;
    (0..n)
        .map(|_| {
            if lcg(&mut s) % 17 == 0 {
                return None;
            }
            let len = (lcg(&mut s) % 24) as usize;
            let mut v = shared_prefix.to_vec();
            for _ in 0..len {
                v.push(match lcg(&mut s) % 5 {
                    0 => 0u8,
                    1 => b'a',
                    2 => b'b',
                    3 => 0xff,
                    _ => b' ',
                });
            }
            Some(v)
        })
        .collect()
}

fn text_oracle(mut vals: Vec<Option<Vec<u8>>>, nulls_first: bool) -> Vec<Option<Vec<u8>>> {
    vals.sort_by(|a, b| {
        use std::cmp::Ordering;
        match (a, b) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => if nulls_first { Ordering::Less } else { Ordering::Greater },
            (Some(_), None) => if nulls_first { Ordering::Greater } else { Ordering::Less },
            (Some(x), Some(y)) => x.cmp(y),
        }
    });
    vals
}

fn drain_text_datums(ts: &mut Tuplesort) -> Vec<Option<Vec<u8>>> {
    let mut got = Vec::new();
    while let Some(nd) = ts.getdatum(true).unwrap() {
        if nd.isnull {
            got.push(None);
            continue;
        }
        let p = nd.value.as_usize() as *const u8;
        // SAFETY: getdatum returns the sort-owned datumCopy image (the
        // ORIGINAL, never the abbreviated word).
        let payload = unsafe {
            use ::types_tuple::varatt::{varatt_is_1b, varsize_1b, varsize_4b};
            if varatt_is_1b(p) {
                std::slice::from_raw_parts(p.add(1), varsize_1b(p) - 1)
            } else {
                std::slice::from_raw_parts(p.add(4), varsize_4b(p) - 4)
            }
        };
        got.push(Some(payload.to_vec()));
    }
    got
}

#[test]
fn abbrev_text_datum_sort_returns_originals_in_full_cmp_order() {
    let vals = random_texts(700, 0xabcd, b"");
    let mut ts = begin_text_datum_abbrev(TUPLESORT_NONE);
    let blobs: Vec<Option<Box<[u64]>>> =
        vals.iter().map(|v| v.as_ref().map(|p| text_blob(p))).collect();
    for b in &blobs {
        match b {
            Some(blob) => ts
                .putdatum(Datum::from_usize(blob.as_ptr() as usize), false)
                .unwrap(),
            None => ts.putdatum(Datum::null(), true).unwrap(),
        }
    }
    ts.performsort().unwrap();
    assert_eq!(drain_text_datums(&mut ts), text_oracle(vals, false));
    ts.end();
}

#[test]
fn abbrev_abort_low_cardinality_prefix_still_sorts() {
    // 8-byte shared prefix: every abbrev word equal, full keys distinct ->
    // varstr_abbrev_abort fires at an abbrevNext checkpoint; REMOVEABBREV
    // must restore original datum1 for already-stored tuples.
    let vals = random_texts(4000, 0x77, b"zzzzzzzz");
    let mut ts = begin_text_datum_abbrev(TUPLESORT_NONE);
    let blobs: Vec<Option<Box<[u64]>>> =
        vals.iter().map(|v| v.as_ref().map(|p| text_blob(p))).collect();
    for b in &blobs {
        match b {
            Some(blob) => ts
                .putdatum(Datum::from_usize(blob.as_ptr() as usize), false)
                .unwrap(),
            None => ts.putdatum(Datum::null(), true).unwrap(),
        }
    }
    ts.0.with(|st| assert!(st.abbrev.is_none(), "abort should have fired"));
    ts.performsort().unwrap();
    assert_eq!(drain_text_datums(&mut ts), text_oracle(vals, false));
    ts.end();
}

#[test]
fn abbrev_bounded_text_sort_top_n() {
    let vals = random_texts(2000, 0x1234, b"");
    let mut ts = begin_text_datum_abbrev(TUPLESORT_ALLOWBOUNDED);
    ts.set_bound(25);
    // C disarms abbreviation in tuplesort_set_bound.
    ts.0.with(|st| {
        assert!(st.abbrev.is_none());
        assert!(matches!(st.sort_keys[0].comparator, SortComparator::TextC));
    });
    let blobs: Vec<Option<Box<[u64]>>> =
        vals.iter().map(|v| v.as_ref().map(|p| text_blob(p))).collect();
    for b in &blobs {
        match b {
            Some(blob) => ts
                .putdatum(Datum::from_usize(blob.as_ptr() as usize), false)
                .unwrap(),
            None => ts.putdatum(Datum::null(), true).unwrap(),
        }
    }
    ts.performsort().unwrap();
    assert!(ts.used_bound());
    // Bounded contract: fetching past the bound is an error (C elog), so
    // drain exactly `bound` rows as Limit does.
    let mut got = Vec::new();
    for _ in 0..25 {
        let nd = ts.getdatum(true).unwrap().expect("bound rows present");
        got.push(if nd.isnull {
            None
        } else {
            let p = nd.value.as_usize() as *const u8;
            // SAFETY: sort-owned datumCopy image.
            Some(unsafe {
                use ::types_tuple::varatt::{varatt_is_1b, varsize_1b, varsize_4b};
                if varatt_is_1b(p) {
                    std::slice::from_raw_parts(p.add(1), varsize_1b(p) - 1).to_vec()
                } else {
                    std::slice::from_raw_parts(p.add(4), varsize_4b(p) - 4).to_vec()
                }
            })
        });
    }
    assert_eq!(got[..], text_oracle(vals, false)[..25]);
    ts.end();
}

#[test]
fn abbrev_uuid_datum_sort() {
    let mut s = 5u64;
    let uuids: Vec<[u8; 16]> = (0..500)
        .map(|_| {
            let mut u = [0u8; 16];
            let hi = lcg(&mut s) % 4; // force abbrev ties
            u[..8].copy_from_slice(&hi.to_be_bytes());
            u[8..].copy_from_slice(&lcg(&mut s).to_be_bytes());
            u
        })
        .collect();
    let key = SortSupport {
        ssup_collation: 0,
        ssup_reverse: false,
        ssup_nulls_first: false,
        ssup_attno: 1,
        comparator: SortComparator::Unsigned,
    };
    let arm = AbbrevArm { kind: AbbrevKind::Uuid, full_comparator: SortComparator::Uuid };
    let mut ts = Tuplesort::begin_common(
        1024,
        TUPLESORT_NONE,
        &[key],
        false,
        Some(Box::new(AbbrevState::new(arm))),
        SortVariant::Datum { byref_typlen: 16 },
    );
    for u in &uuids {
        ts.putdatum(Datum::from_usize(u.as_ptr() as usize), false).unwrap();
    }
    ts.performsort().unwrap();
    let mut got = Vec::new();
    while let Some(nd) = ts.getdatum(true).unwrap() {
        // SAFETY: sort-owned 16-byte datumCopy image.
        got.push(unsafe { *(nd.value.as_usize() as *const [u8; 16]) });
    }
    let mut oracle = uuids.clone();
    oracle.sort();
    assert_eq!(got, oracle);
    ts.end();
}

#[test]
fn abbrev_heap_text_sort_with_tiebreak_key() {
    // Leading text key (abbreviated) + int4 second key.
    let mcx = leaked_mcx();
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    let text_att = FormData_pg_attribute {
        attnum: 1,
        atttypid: 25,
        attlen: -1,
        attbyval: false,
        attalign: TYPALIGN_INT,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    compact.push(CompactAttribute::populate_from(&text_att));
    attrs.push(text_att);
    let int_att = FormData_pg_attribute {
        attnum: 2,
        atttypid: 23,
        attlen: 4,
        attbyval: true,
        attalign: TYPALIGN_INT,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    compact.push(CompactAttribute::populate_from(&int_att));
    attrs.push(int_att);
    let desc = Rc::new(TupleDescData {
        natts: 2,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    });

    let keys = [text_key(false, false), int32_key(2, false, false)];
    let mut ts = Tuplesort::begin_common(
        1024,
        TUPLESORT_NONE,
        &keys,
        false,
        Some(Box::new(AbbrevState::new(text_abbrev_arm()))),
        SortVariant::Heap { tup_desc: desc.clone() },
    );

    let texts = random_texts(400, 0x99, b"pfx_");
    let mut s = 11u64;
    let rows: Vec<(Option<Vec<u8>>, i32)> = texts
        .into_iter()
        .map(|t| (t, (lcg(&mut s) % 7) as i32))
        .collect();
    let blobs: Vec<(Option<Box<[u64]>>, i32)> = rows
        .iter()
        .map(|(t, i)| (t.as_ref().map(|p| text_blob(p)), *i))
        .collect();

    let mut in_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
    for (t, i) in &blobs {
        exectuples::exec_clear_tuple(&mut in_slot, mcx);
        let base = in_slot.base_mut();
        match t {
            Some(blob) => {
                base.tts_values[0] = Datum::from_usize(blob.as_ptr() as usize);
                base.tts_isnull[0] = false;
            }
            None => {
                base.tts_values[0] = Datum::null();
                base.tts_isnull[0] = true;
            }
        }
        base.tts_values[1] = Datum::from_i32(*i);
        base.tts_isnull[1] = false;
        exectuples::exec_store_virtual_tuple(&mut in_slot);
        ts.puttupleslot(&mut in_slot, mcx).unwrap();
    }
    ts.performsort().unwrap();

    let mut got = Vec::new();
    let mut out_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));
    while ts.gettupleslot(true, false, &mut out_slot, mcx).unwrap() {
        let mut n1 = false;
        let mut n2 = false;
        let d1 = exectuples::slot_getattr(&mut out_slot, 1, &mut n1);
        let d2 = exectuples::slot_getattr(&mut out_slot, 2, &mut n2);
        let t = if n1 {
            None
        } else {
            let p = d1.as_usize() as *const u8;
            // SAFETY: live minimal-tuple varlena attr.
            Some(unsafe {
                use ::types_tuple::varatt::{varatt_is_1b, varsize_1b, varsize_4b};
                if varatt_is_1b(p) {
                    std::slice::from_raw_parts(p.add(1), varsize_1b(p) - 1).to_vec()
                } else {
                    std::slice::from_raw_parts(p.add(4), varsize_4b(p) - 4).to_vec()
                }
            })
        };
        got.push((t, d2.as_i32()));
    }
    let mut oracle = rows.clone();
    oracle.sort_by(|(t1, i1), (t2, i2)| {
        use std::cmp::Ordering;
        let c = match (t1, t2) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(x), Some(y)) => x.cmp(y),
        };
        c.then(i1.cmp(i2))
    });
    assert_eq!(got, oracle);
    ts.end();
}

fn heap_sort_rows(
    mcx: Mcx<'static>,
    ncols: i32,
    keys: &[SortSupport],
    rows: &[Vec<Option<i32>>],
) -> Vec<Vec<Option<i32>>> {
    let desc = int4_desc(mcx, ncols);
    let mut in_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
    let mut out_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));
    let mut ts = Tuplesort::begin_heap_with_keys(desc, keys, 1024, TUPLESORT_NONE);
    for r in rows {
        store_row(&mut in_slot, mcx, r);
        ts.puttupleslot(&mut in_slot, mcx).unwrap();
    }
    ts.performsort().unwrap();
    let mut got = Vec::new();
    while ts.gettupleslot(true, false, &mut out_slot, mcx).unwrap() {
        let mut row = Vec::new();
        for a in 1..=ncols {
            let mut isnull = false;
            let v = exectuples::slot_getattr(&mut out_slot, a, &mut isnull);
            row.push(if isnull { None } else { Some(v.as_i32()) });
        }
        got.push(row);
    }
    ts.end();
    got
}

// Unique full key set => unique sorted permutation => exact oracle equality.
#[test]
fn mksort_two_key_unique() {
    let mcx = leaked_mcx();
    let keys = [int32_key(1, false, false), int32_key(2, false, false)];
    let mut seed = 11u64;
    let mut order: Vec<i32> = (0..1000).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, (lcg(&mut seed) % (i as u64 + 1)) as usize);
    }
    let rows: Vec<Vec<Option<i32>>> =
        order.iter().map(|&i| vec![Some(i % 25), Some(i)]).collect();
    let got = heap_sort_rows(mcx, 2, &keys, &rows);
    let mut oracle = rows.clone();
    oracle.sort();
    assert_eq!(got, oracle);
}

// Six keys, duplicated prefixes, unique last key: per-key segment recursion.
#[test]
fn mksort_six_key_unique() {
    let mcx = leaked_mcx();
    let keys: Vec<SortSupport> = (1..=6).map(|a| int32_key(a, false, false)).collect();
    let mut seed = 17u64;
    let mut order: Vec<i32> = (0..2000).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, (lcg(&mut seed) % (i as u64 + 1)) as usize);
    }
    let rows: Vec<Vec<Option<i32>>> = order
        .iter()
        .map(|&i| {
            vec![Some(i % 3), Some(i % 4), Some(i % 5), Some(i % 7), Some(i % 11), Some(i)]
        })
        .collect();
    let got = heap_sort_rows(mcx, 6, &keys, &rows);
    let mut oracle = rows.clone();
    oracle.sort();
    assert_eq!(got, oracle);
}

// Full-key duplicates (col 3 is payload, not a key): the tie fallback must
// reproduce pg_qsort's tie permutation bit-for-bit.
#[test]
fn mksort_ties_match_pgqsort_order() {
    let mcx = leaked_mcx();
    let keys = [int32_key(1, false, false), int32_key(2, true, true)];
    let mut seed = 23u64;
    let rows: Vec<Vec<Option<i32>>> = (0..700)
        .map(|i| {
            let k1 = (lcg(&mut seed) % 5) as i32;
            let k2 = lcg(&mut seed) % 4;
            vec![Some(k1), if k2 == 0 { None } else { Some(k2 as i32) }, Some(i)]
        })
        .collect();
    let got_mksort = heap_sort_rows(mcx, 3, &keys, &rows);
    crate::testhooks::MKSORT_DISABLE.with(|c| c.set(true));
    let got_pgqsort = heap_sort_rows(mcx, 3, &keys, &rows);
    crate::testhooks::MKSORT_DISABLE.with(|c| c.set(false));
    assert_eq!(got_mksort, got_pgqsort);
}

// Nulls in the leading key: notnull spec must not engage; NULLS LAST holds.
#[test]
fn mksort_null_leading_key() {
    let mcx = leaked_mcx();
    let keys = [int32_key(1, false, false), int32_key(2, false, false)];
    let mut seed = 31u64;
    let rows: Vec<Vec<Option<i32>>> = (0..500)
        .map(|i| {
            let k1 = lcg(&mut seed) % 10;
            vec![if k1 == 0 { None } else { Some(k1 as i32) }, Some(i)]
        })
        .collect();
    let got = heap_sort_rows(mcx, 2, &keys, &rows);
    let mut oracle = rows.clone();
    oracle.sort_by(|x, y| {
        use std::cmp::Ordering;
        let c = match (x[0], y[0]) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(a), Some(b)) => a.cmp(&b),
        };
        c.then(x[1].cmp(&y[1]))
    });
    assert_eq!(got, oracle);
}

// ---- numeric sortsupport / abbreviation ----

fn numeric_blob(s: &str) -> Box<[u64]> {
    let img = match s {
        "NaN" => ::adt_numeric::NumericImage::nan(),
        "Infinity" => ::adt_numeric::NumericImage::pinf(),
        "-Infinity" => ::adt_numeric::NumericImage::ninf(),
        _ => ::adt_numeric::numeric_in(s, -1, None).unwrap().unwrap(),
    };
    let bytes = img.as_bytes();
    let mut blob = vec![0u64; bytes.len().div_ceil(8)].into_boxed_slice();
    // SAFETY: fresh buffer of >= bytes.len() bytes.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), blob.as_mut_ptr().cast::<u8>(), bytes.len());
    }
    blob
}

fn numeric_key(nulls_first: bool, reverse: bool) -> SortSupport {
    SortSupport {
        ssup_collation: 0,
        ssup_reverse: reverse,
        ssup_nulls_first: nulls_first,
        ssup_attno: 1,
        comparator: SortComparator::NumericAbbrev,
    }
}

fn numeric_abbrev_arm() -> AbbrevArm {
    AbbrevArm { kind: AbbrevKind::Numeric, full_comparator: SortComparator::Numeric }
}

fn numeric_corpus(n: usize, seed: u64) -> Vec<Option<String>> {
    let mut s = seed;
    (0..n)
        .map(|_| {
            let r = lcg(&mut s) % 23;
            Some(match r {
                0 => return None,
                1 => "NaN".to_string(),
                2 => "Infinity".to_string(),
                3 => "-Infinity".to_string(),
                4 => "0".to_string(),
                5 => format!("1e{}", 80 + (lcg(&mut s) % 30)),
                6 => format!("-1e{}", 80 + (lcg(&mut s) % 30)),
                7 => format!("1e-{}", 40 + (lcg(&mut s) % 20)),
                // Same 7-word prefix, differing tail: abbrev tie, full-cmp
                // decides (exceeds the 4 packed digit words).
                8 => format!("123456789012345678901234567890.{:04}", lcg(&mut s) % 10000),
                _ => {
                    let sign = if lcg(&mut s) % 2 == 0 { "" } else { "-" };
                    let int = lcg(&mut s) % 1_000_000_000;
                    let frac = lcg(&mut s) % 100_000;
                    format!("{sign}{int}.{frac}")
                }
            })
        })
        .collect()
}

fn numeric_oracle(vals: &[Option<String>], nulls_first: bool) -> Vec<Option<Vec<u8>>> {
    let imgs: Vec<Option<::adt_numeric::NumericImage>> = vals
        .iter()
        .map(|v| {
            v.as_ref().map(|s| match s.as_str() {
                "NaN" => ::adt_numeric::NumericImage::nan(),
                "Infinity" => ::adt_numeric::NumericImage::pinf(),
                "-Infinity" => ::adt_numeric::NumericImage::ninf(),
                _ => ::adt_numeric::numeric_in(s, -1, None).unwrap().unwrap(),
            })
        })
        .collect();
    let mut idx: Vec<usize> = (0..imgs.len()).collect();
    idx.sort_by(|&i, &j| {
        use std::cmp::Ordering;
        match (&imgs[i], &imgs[j]) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => if nulls_first { Ordering::Less } else { Ordering::Greater },
            (Some(_), None) => if nulls_first { Ordering::Greater } else { Ordering::Less },
            (Some(x), Some(y)) => ::adt_numeric::cmp_numerics(x.num(), y.num()).cmp(&0),
        }
    });
    idx.iter()
        .map(|&i| imgs[i].as_ref().map(|img| img.as_bytes().to_vec()))
        .collect()
}

fn drain_numeric_datums(ts: &mut Tuplesort) -> Vec<Option<Vec<u8>>> {
    let mut got = Vec::new();
    while let Some(nd) = ts.getdatum(true).unwrap() {
        if nd.isnull {
            got.push(None);
            continue;
        }
        let p = nd.value.as_usize() as *const u8;
        // SAFETY: sort-owned datumCopy image (the ORIGINAL, never the word).
        got.push(Some(unsafe {
            std::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p)).to_vec()
        }));
    }
    got
}

#[test]
fn abbrev_numeric_datum_sort_matches_cmp_numerics_order() {
    let vals = numeric_corpus(800, 0x5eed);
    let mut ts = Tuplesort::begin_common(
        1024,
        TUPLESORT_NONE,
        &[numeric_key(false, false)],
        false,
        Some(Box::new(AbbrevState::new(numeric_abbrev_arm()))),
        SortVariant::Datum { byref_typlen: -1 },
    );
    let blobs: Vec<Option<Box<[u64]>>> =
        vals.iter().map(|v| v.as_ref().map(|s| numeric_blob(s))).collect();
    for b in &blobs {
        match b {
            Some(blob) => ts.putdatum(Datum::from_usize(blob.as_ptr() as usize), false).unwrap(),
            None => ts.putdatum(Datum::null(), true).unwrap(),
        }
    }
    ts.performsort().unwrap();
    assert_eq!(drain_numeric_datums(&mut ts), numeric_oracle(&vals, false));
    ts.end();
}

#[test]
fn abbrev_numeric_datum_sort_reverse_nulls_first() {
    let vals = numeric_corpus(300, 0xd06);
    let mut ts = Tuplesort::begin_common(
        1024,
        TUPLESORT_NONE,
        &[numeric_key(true, true)],
        false,
        Some(Box::new(AbbrevState::new(numeric_abbrev_arm()))),
        SortVariant::Datum { byref_typlen: -1 },
    );
    let blobs: Vec<Option<Box<[u64]>>> =
        vals.iter().map(|v| v.as_ref().map(|s| numeric_blob(s))).collect();
    for b in &blobs {
        match b {
            Some(blob) => ts.putdatum(Datum::from_usize(blob.as_ptr() as usize), false).unwrap(),
            None => ts.putdatum(Datum::null(), true).unwrap(),
        }
    }
    ts.performsort().unwrap();
    let mut oracle = numeric_oracle(&vals, false);
    oracle.reverse();
    assert_eq!(drain_numeric_datums(&mut ts), oracle);
    ts.end();
}

#[test]
fn numeric_abbrev_abort_low_cardinality_still_sorts() {
    // One distinct value through the 16384 abbrevNext checkpoint (the first
    // one past C's 10000-row floor): numeric_abbrev_abort fires; REMOVEABBREV
    // restores originals and the full comparator finishes the sort.
    let mut vals: Vec<Option<String>> = (0..17000).map(|_| Some("42.5".to_string())).collect();
    vals.extend((0..200).map(|i| Some(format!("{}.25", i % 97))));
    let mut ts = Tuplesort::begin_common(
        4096,
        TUPLESORT_NONE,
        &[numeric_key(false, false)],
        false,
        Some(Box::new(AbbrevState::new(numeric_abbrev_arm()))),
        SortVariant::Datum { byref_typlen: -1 },
    );
    let blobs: Vec<Option<Box<[u64]>>> =
        vals.iter().map(|v| v.as_ref().map(|s| numeric_blob(s))).collect();
    for b in &blobs {
        ts.putdatum(Datum::from_usize(b.as_ref().unwrap().as_ptr() as usize), false).unwrap();
    }
    ts.0.with(|st| assert!(st.abbrev.is_none(), "abort should have fired"));
    ts.0.with(|st| {
        assert!(matches!(st.sort_keys[0].comparator, SortComparator::Numeric));
    });
    ts.performsort().unwrap();
    assert_eq!(drain_numeric_datums(&mut ts), numeric_oracle(&vals, false));
    ts.end();
}

#[test]
fn numeric_bounded_sort_disarms_abbrev() {
    let vals = numeric_corpus(400, 0xb0b);
    let mut ts = Tuplesort::begin_common(
        1024,
        TUPLESORT_ALLOWBOUNDED,
        &[numeric_key(false, false)],
        false,
        Some(Box::new(AbbrevState::new(numeric_abbrev_arm()))),
        SortVariant::Datum { byref_typlen: -1 },
    );
    ts.set_bound(20);
    ts.0.with(|st| {
        assert!(st.abbrev.is_none());
        assert!(matches!(st.sort_keys[0].comparator, SortComparator::Numeric));
    });
    let blobs: Vec<Option<Box<[u64]>>> =
        vals.iter().map(|v| v.as_ref().map(|s| numeric_blob(s))).collect();
    for b in &blobs {
        match b {
            Some(blob) => ts.putdatum(Datum::from_usize(blob.as_ptr() as usize), false).unwrap(),
            None => ts.putdatum(Datum::null(), true).unwrap(),
        }
    }
    ts.performsort().unwrap();
    assert!(ts.used_bound());
    let mut got = Vec::new();
    for _ in 0..20 {
        let nd = ts.getdatum(true).unwrap().expect("bound rows present");
        got.push(if nd.isnull {
            None
        } else {
            let p = nd.value.as_usize() as *const u8;
            // SAFETY: sort-owned datumCopy image.
            Some(unsafe {
                std::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p)).to_vec()
            })
        });
    }
    assert_eq!(got[..], numeric_oracle(&vals, false)[..20]);
    ts.end();
}

// ---- radix over abbreviated keys ----

fn radix_counters_reset() {
    crate::testhooks::RADIX_ATTEMPTS.with(|c| c.set(0));
    crate::testhooks::RADIX_COMPLETED.with(|c| c.set(0));
}

fn radix_counters() -> (u32, u32) {
    (
        crate::testhooks::RADIX_ATTEMPTS.with(|c| c.get()),
        crate::testhooks::RADIX_COMPLETED.with(|c| c.get()),
    )
}

fn text_datum_sort_run(
    vals: &[Option<Vec<u8>>],
    nulls_first: bool,
    reverse: bool,
    disable_radix: bool,
) -> Vec<Option<Vec<u8>>> {
    crate::testhooks::RADIX_DISABLE.with(|c| c.set(disable_radix));
    let mut ts = Tuplesort::begin_common(
        8192,
        TUPLESORT_NONE,
        &[text_key(nulls_first, reverse)],
        false,
        Some(Box::new(AbbrevState::new(text_abbrev_arm()))),
        SortVariant::Datum { byref_typlen: -1 },
    );
    let blobs: Vec<Option<Box<[u64]>>> =
        vals.iter().map(|v| v.as_ref().map(|p| text_blob(p))).collect();
    for b in &blobs {
        match b {
            Some(blob) => ts.putdatum(Datum::from_usize(blob.as_ptr() as usize), false).unwrap(),
            None => ts.putdatum(Datum::null(), true).unwrap(),
        }
    }
    ts.performsort().unwrap();
    let got = drain_text_datums(&mut ts);
    ts.end();
    crate::testhooks::RADIX_DISABLE.with(|c| c.set(false));
    got
}

// Distinct full strings; group sizes 1-4 via the shared word prefix.
fn unique_texts(n: usize, seed: u64, group: usize) -> Vec<Option<Vec<u8>>> {
    let mut s = seed;
    (0..n)
        .map(|i| {
            let _ = lcg(&mut s);
            Some(format!("{:08x}{:06}", (i / group) as u32, i).into_bytes())
        })
        .collect()
}

#[test]
fn radix_text_datum_unique_matches_oracle_and_pgqsort() {
    let vals = unique_texts(5000, 0xace, 1);
    radix_counters_reset();
    let got = text_datum_sort_run(&vals, false, false, false);
    let (attempts, completed) = radix_counters();
    assert_eq!((attempts, completed), (1, 1), "radix must engage and complete");
    assert_eq!(got, text_oracle(vals.clone(), false));
    let got_qsort = text_datum_sort_run(&vals, false, false, true);
    assert_eq!(got, got_qsort);
}

#[test]
fn radix_word_groups_unique_tails() {
    // Groups of 4 equal abbrev words, distinct tails: group sort decides.
    let vals = unique_texts(4096, 7, 4);
    radix_counters_reset();
    let got = text_datum_sort_run(&vals, false, false, false);
    assert_eq!(radix_counters(), (1, 1));
    assert_eq!(got, text_oracle(vals, false));
}

#[test]
fn radix_ties_and_nulls_fall_back_bit_identical() {
    // Duplicate strings and >1 NULL: certificate must trip; output must be
    // pg_qsort's permutation bit-for-bit (payload col proves it).
    let vals = random_texts(3000, 0x5add, b"");
    radix_counters_reset();
    let got = text_datum_sort_run(&vals, false, false, false);
    let (attempts, completed) = radix_counters();
    assert_eq!(attempts, 1);
    assert_eq!(completed, 0, "duplicates must trip the certificate");
    let got_qsort = text_datum_sort_run(&vals, false, false, true);
    assert_eq!(got, got_qsort);
}

#[test]
fn radix_reverse_and_nulls_first() {
    let mut vals = unique_texts(3000, 3, 2);
    vals[137] = None; // single NULL: null group of 1, no fallback
    radix_counters_reset();
    let got = text_datum_sort_run(&vals, true, true, false);
    assert_eq!(radix_counters(), (1, 1));
    let got_qsort = text_datum_sort_run(&vals, true, true, true);
    assert_eq!(got, got_qsort);
    let mut oracle = text_oracle(vals, false);
    oracle.reverse(); // DESC NULLS FIRST == reverse of ASC NULLS LAST
    assert_eq!(got, oracle);
}

#[test]
fn radix_presorted_strictly_increasing_short_circuits() {
    let vals: Vec<Option<Vec<u8>>> =
        (0..2000).map(|i| Some(format!("{i:08}").into_bytes())).collect();
    radix_counters_reset();
    let got = text_datum_sort_run(&vals, false, false, false);
    assert_eq!(radix_counters(), (1, 1));
    assert_eq!(got, text_oracle(vals, false));
}

#[test]
fn radix_heap_two_key_matches_pgqsort() {
    let mcx = leaked_mcx();
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    let text_att = FormData_pg_attribute {
        attnum: 1,
        atttypid: 25,
        attlen: -1,
        attbyval: false,
        attalign: TYPALIGN_INT,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    compact.push(CompactAttribute::populate_from(&text_att));
    attrs.push(text_att);
    let int_att = FormData_pg_attribute {
        attnum: 2,
        atttypid: 23,
        attlen: 4,
        attbyval: true,
        attalign: TYPALIGN_INT,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    compact.push(CompactAttribute::populate_from(&int_att));
    attrs.push(int_att);
    let desc = Rc::new(TupleDescData {
        natts: 2,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    });
    let keys = [text_key(false, false), int32_key(2, false, false)];

    let mut s = 41u64;
    let rows: Vec<(Option<Vec<u8>>, i32)> = (0..3000)
        .map(|i| {
            let t = if lcg(&mut s) % 19 == 0 {
                None
            } else {
                // dup texts; (text, i2) pairs include full duplicates
                Some(format!("w{:07}", lcg(&mut s) % 700).into_bytes())
            };
            (t, (i % 5) as i32)
        })
        .collect();

    let run = |disable: bool| -> Vec<(Option<Vec<u8>>, i32)> {
        crate::testhooks::RADIX_DISABLE.with(|c| c.set(disable));
        let blobs: Vec<(Option<Box<[u64]>>, i32)> = rows
            .iter()
            .map(|(t, i)| (t.as_ref().map(|p| text_blob(p)), *i))
            .collect();
        let mut ts = Tuplesort::begin_common(
            8192,
            TUPLESORT_NONE,
            &keys,
            false,
            Some(Box::new(AbbrevState::new(text_abbrev_arm()))),
            SortVariant::Heap { tup_desc: desc.clone() },
        );
        let mut in_slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
        for (t, i) in &blobs {
            exectuples::exec_clear_tuple(&mut in_slot, mcx);
            let base = in_slot.base_mut();
            match t {
                Some(blob) => {
                    base.tts_values[0] = Datum::from_usize(blob.as_ptr() as usize);
                    base.tts_isnull[0] = false;
                }
                None => {
                    base.tts_values[0] = Datum::null();
                    base.tts_isnull[0] = true;
                }
            }
            base.tts_values[1] = Datum::from_i32(*i);
            base.tts_isnull[1] = false;
            exectuples::exec_store_virtual_tuple(&mut in_slot);
            ts.puttupleslot(&mut in_slot, mcx).unwrap();
        }
        ts.performsort().unwrap();
        let mut got = Vec::new();
        let mut out_slot = exectuples::make_tuple_table_slot(
            mcx,
            TupleSlotKind::MinimalTuple,
            Some(desc.clone()),
        );
        while ts.gettupleslot(true, false, &mut out_slot, mcx).unwrap() {
            let mut n1 = false;
            let mut n2 = false;
            let d1 = exectuples::slot_getattr(&mut out_slot, 1, &mut n1);
            let d2 = exectuples::slot_getattr(&mut out_slot, 2, &mut n2);
            let t = if n1 {
                None
            } else {
                let p = d1.as_usize() as *const u8;
                // SAFETY: live minimal-tuple varlena attr.
                Some(unsafe {
                    use ::types_tuple::varatt::{varatt_is_1b, varsize_1b, varsize_4b};
                    if varatt_is_1b(p) {
                        std::slice::from_raw_parts(p.add(1), varsize_1b(p) - 1).to_vec()
                    } else {
                        std::slice::from_raw_parts(p.add(4), varsize_4b(p) - 4).to_vec()
                    }
                })
            };
            got.push((t, d2.as_i32()));
        }
        ts.end();
        crate::testhooks::RADIX_DISABLE.with(|c| c.set(false));
        got
    };

    radix_counters_reset();
    let got_radix = run(false);
    let (attempts, _) = radix_counters();
    assert_eq!(attempts, 1);
    let got_qsort = run(true);
    assert_eq!(got_radix, got_qsort);
}

#[test]
fn radix_direct_small_covers_unsafe_paths() {
    // Direct calls below RADIX_MIN: Miri-sized coverage of the scatter /
    // group / copy-back unsafe blocks, success and fallback legs.
    let uniq = unique_texts(80, 9, 3);
    let mut ts = begin_text_datum_abbrev(TUPLESORT_NONE);
    let blobs: Vec<Option<Box<[u64]>>> =
        uniq.iter().map(|v| v.as_ref().map(|p| text_blob(p))).collect();
    for b in &blobs {
        ts.putdatum(Datum::from_usize(b.as_ref().unwrap().as_ptr() as usize), false).unwrap();
    }
    ts.0.with_mut(|st| {
        let mut tuples = core::mem::replace(&mut st.memtuples, PgVec::new_in(st.mcx));
        assert!(st.radix_sort_abbrev(&mut tuples).unwrap());
        st.memtuples = tuples;
    });
    ts.performsort().unwrap(); // presorted now; qsort fast path finishes
    assert_eq!(drain_text_datums(&mut ts), text_oracle(uniq, false));
    ts.end();

    let dups = random_texts(90, 0xdead, b"");
    let mut ts = begin_text_datum_abbrev(TUPLESORT_NONE);
    let blobs: Vec<Option<Box<[u64]>>> =
        dups.iter().map(|v| v.as_ref().map(|p| text_blob(p))).collect();
    for b in &blobs {
        match b {
            Some(blob) => ts.putdatum(Datum::from_usize(blob.as_ptr() as usize), false).unwrap(),
            None => ts.putdatum(Datum::null(), true).unwrap(),
        }
    }
    let before: Vec<(u64, bool)> = ts.0.with(|st| {
        st.memtuples.iter().map(|t| (t.datum1.as_u64(), t.isnull1)).collect()
    });
    ts.0.with_mut(|st| {
        let mut tuples = core::mem::replace(&mut st.memtuples, PgVec::new_in(st.mcx));
        assert!(!st.radix_sort_abbrev(&mut tuples).unwrap(), "dups must fall back");
        st.memtuples = tuples;
    });
    let after: Vec<(u64, bool)> = ts.0.with(|st| {
        st.memtuples.iter().map(|t| (t.datum1.as_u64(), t.isnull1)).collect()
    });
    assert_eq!(before, after, "fallback must leave the array untouched");
    ts.performsort().unwrap();
    assert_eq!(drain_text_datums(&mut ts), text_oracle(dups, false));
    ts.end();
}

mod spill {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Once;

    use super::*;
    use ::types_slot::TupleSlotKind;

    static SETUP: Once = Once::new();
    static WAL_SYNC_METHOD: AtomicI32 = AtomicI32::new(0);
    static CWD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn enter_datadir(tag: &str) -> (std::sync::MutexGuard<'static, ()>, String) {
        let guard = CWD.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = format!(
            "{}/pgrust-tsortspill-{}-{}",
            std::env::temp_dir().display(),
            std::process::id(),
            tag
        );
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(format!("{dir}/base/pgsql_tmp")).unwrap();
        std::env::set_current_dir(&dir).unwrap();
        (guard, dir)
    }

    fn setup() {
        SETUP.call_once(|| {
            guc_tables::init_seams();
            elog::init_seams();
            fd::init_seams();
            xact_seams::get_current_sub_transaction_id::set(|| 1);
            aio_seams::pgaio_closing_fd::set(|_| {});
            aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
            waitevent_seams::pgstat_report_wait_start::set(|_| {});
            waitevent_seams::pgstat_report_wait_end::set(|| {});
            pgstat_seams::pgstat_report_tempfile::set(|_| {});
            ipc_seams::before_shmem_exit::set(|_, _| Ok(()));
            ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
            resowner::init_seams();
            guc_tables::vars::wal_sync_method.install(guc_tables::GucVarAccessors {
                get: || WAL_SYNC_METHOD.load(Ordering::Relaxed),
                set: |v| WAL_SYNC_METHOD.store(v, Ordering::Relaxed),
            });
        });
        fd::InitFileAccess();
        let _ = fd::InitTemporaryFileAccess();
        if resowner_seams::current_resource_owner::call().is_null() {
            let owner =
                resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "spill-test")
                    .unwrap();
            resowner_seams::set_current_resource_owner::call(owner);
        }
    }

    fn temp_files(dir: &str) -> usize {
        std::fs::read_dir(format!("{dir}/base/pgsql_tmp"))
            .map(|d| d.count())
            .unwrap_or(0)
    }

    fn spill_datums(n: u64, sortopt: i32) -> (Tuplesort, Vec<i32>) {
        let mut ts = Tuplesort::begin_datum_with_key(int32_key(1, false, false), 64, sortopt);
        let mut seed = 42u64;
        let mut oracle: Vec<i32> = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let v = lcg(&mut seed) as i32;
            oracle.push(v);
            ts.putdatum(Datum::from_i32(v), false).unwrap();
        }
        oracle.sort_unstable();
        (ts, oracle)
    }

    #[test]
    fn datum_spill_final_merge() {
        setup();
        let (_cwd, dir) = enter_datadir("finalmerge");
        let (mut ts, oracle) = spill_datums(30_000, TUPLESORT_NONE);
        ts.performsort().unwrap();
        let mut got = Vec::with_capacity(oracle.len());
        while let Some(nd) = ts.getdatum(true).unwrap() {
            assert!(!nd.isnull);
            got.push(nd.value.as_i32());
        }
        assert_eq!(got, oracle);
        let stats = ts.get_stats();
        assert!(matches!(
            stats.sortMethod,
            ::types_core::instrument::TuplesortMethod::ExternalMerge
        ));
        assert!(matches!(
            stats.spaceType,
            ::types_core::instrument::TuplesortSpaceType::Disk
        ));
        assert!(temp_files(&dir) > 0, "temp file expected during merge");
        ts.end();
        assert_eq!(temp_files(&dir), 0, "temp files must be removed at end");
    }

    #[test]
    fn datum_spill_randomaccess_backward_rescan_markpos() {
        setup();
        let (_cwd, dir) = enter_datadir("ontape");
        let (mut ts, oracle) = spill_datums(20_000, TUPLESORT_RANDOMACCESS);
        ts.performsort().unwrap();

        // Forward walk.
        let mut got = Vec::with_capacity(oracle.len());
        while let Some(nd) = ts.getdatum(true).unwrap() {
            got.push(nd.value.as_i32());
        }
        assert_eq!(got, oracle);
        let stats = ts.get_stats();
        assert!(matches!(
            stats.sortMethod,
            ::types_core::instrument::TuplesortMethod::ExternalSort
        ));

        // Backward walk from EOF returns the whole thing reversed.
        let mut back = Vec::with_capacity(oracle.len());
        while let Some(nd) = ts.getdatum(false).unwrap() {
            back.push(nd.value.as_i32());
        }
        back.reverse();
        assert_eq!(back, oracle);

        // Rescan replays from the start.
        ts.rescan().unwrap();
        let first = ts.getdatum(true).unwrap().unwrap();
        assert_eq!(first.value.as_i32(), oracle[0]);

        // markpos/restorepos replay the same tuple.
        ts.markpos().unwrap();
        let second = ts.getdatum(true).unwrap().unwrap();
        assert_eq!(second.value.as_i32(), oracle[1]);
        ts.restorepos().unwrap();
        let second_again = ts.getdatum(true).unwrap().unwrap();
        assert_eq!(second_again.value.as_i32(), oracle[1]);

        // skiptuples over the tape arm.
        assert!(ts.skiptuples(100, true).unwrap());
        let after_skip = ts.getdatum(true).unwrap().unwrap();
        assert_eq!(after_skip.value.as_i32(), oracle[102]);

        ts.end();
        assert_eq!(temp_files(&dir), 0);
    }

    #[test]
    fn heap_spill_two_keys_matches_oracle() {
        setup();
        let (_cwd, dir) = enter_datadir("heapspill");
        let mcx = leaked_mcx();
        let desc = int4_desc(mcx, 2);
        let mut in_slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
        let mut out_slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));

        let keys = [int32_key(1, false, false), int32_key(2, false, false)];
        let mut ts = Tuplesort::begin_heap_with_keys(desc.clone(), &keys, 64, TUPLESORT_NONE);

        let mut seed = 7u64;
        let mut rows: Vec<(Option<i32>, Option<i32>)> = (0..40_000)
            .map(|_| {
                let a = lcg(&mut seed) % 1000;
                let b = lcg(&mut seed);
                (Some(a as i32), if b % 17 == 0 { None } else { Some((b % 100) as i32) })
            })
            .collect();
        for (a, b) in &rows {
            store_row(&mut in_slot, mcx, &[*a, *b]);
            ts.puttupleslot(&mut in_slot, mcx).unwrap();
        }
        ts.performsort().unwrap();

        rows.sort_by(|x, y| {
            let k1 = x.0.cmp(&y.0);
            k1.then_with(|| match (x.1, y.1) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, _) => std::cmp::Ordering::Greater,
                (_, None) => std::cmp::Ordering::Less,
                (Some(a), Some(b)) => a.cmp(&b),
            })
        });

        let mut got = Vec::new();
        while ts.gettupleslot(true, false, &mut out_slot, mcx).unwrap() {
            let mut n1 = false;
            let mut n2 = false;
            let v1 = exectuples::slot_getattr(&mut out_slot, 1, &mut n1);
            let v2 = exectuples::slot_getattr(&mut out_slot, 2, &mut n2);
            got.push((
                if n1 { None } else { Some(v1.as_i32()) },
                if n2 { None } else { Some(v2.as_i32()) },
            ));
        }
        assert_eq!(got.len(), rows.len());
        assert_eq!(got, rows);
        exectuples::exec_clear_tuple(&mut out_slot, mcx);
        ts.end();
        assert_eq!(temp_files(&dir), 0);
    }

    // The bounded-capable (aset caller-tuples) arm across a spill: dumptuples
    // writes the whole memory load to tape then releases the tuple context
    // WHOLESALE (C never pfrees the written tuples one by one — the per-tuple
    // writetup frees were removed upstream in favor of this reset).
    #[test]
    fn bounded_capable_spill_dumps_and_resets_caller_tuples() {
        setup();
        let (_cwd, dir) = enter_datadir("boundedspill");
        let mcx = leaked_mcx();
        let desc = int4_desc(mcx, 2);
        let mut in_slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
        let mut out_slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));

        let keys = [int32_key(1, false, false), int32_key(2, false, false)];
        // Bounded-capable, but the bound transition is never reached before
        // memory runs out: the sort spills as a plain external sort.
        let mut ts = Tuplesort::begin_heap_with_keys(desc.clone(), &keys, 64, TUPLESORT_ALLOWBOUNDED);
        ts.set_bound(20_000);

        let mut seed = 7u64;
        let mut rows: Vec<(i32, i32)> = (0..40_000)
            .map(|_| {
                let a = lcg(&mut seed) % 1000;
                let b = lcg(&mut seed) % 100;
                (a as i32, b as i32)
            })
            .collect();
        for (a, b) in &rows {
            store_row(&mut in_slot, mcx, &[Some(*a), Some(*b)]);
            ts.puttupleslot(&mut in_slot, mcx).unwrap();
        }
        ts.performsort().unwrap();

        rows.sort_unstable();
        let mut got = Vec::new();
        while got.len() < 20_000 && ts.gettupleslot(true, false, &mut out_slot, mcx).unwrap() {
            let mut n1 = false;
            let mut n2 = false;
            let v1 = exectuples::slot_getattr(&mut out_slot, 1, &mut n1);
            let v2 = exectuples::slot_getattr(&mut out_slot, 2, &mut n2);
            got.push((v1.as_i32(), v2.as_i32()));
        }
        assert_eq!(got[..], rows[..20_000]);
        exectuples::exec_clear_tuple(&mut out_slot, mcx);
        ts.end();
        assert_eq!(temp_files(&dir), 0);
    }

    #[test]
    fn spill_then_reset_reuses_state() {
        setup();
        let (_cwd, dir) = enter_datadir("reset");
        let (mut ts, oracle) = spill_datums(15_000, TUPLESORT_NONE);
        ts.performsort().unwrap();
        let first = ts.getdatum(true).unwrap().unwrap();
        assert_eq!(first.value.as_i32(), oracle[0]);

        ts.reset();
        assert_eq!(temp_files(&dir), 0, "reset must drop the tape files");

        // Second, in-memory batch works after a spilled one.
        for v in [5, 1, 3] {
            ts.putdatum(Datum::from_i32(v), false).unwrap();
        }
        ts.performsort().unwrap();
        let vals: Vec<i32> = std::iter::from_fn(|| {
            ts.getdatum(true).unwrap().map(|nd| nd.value.as_i32())
        })
        .collect();
        assert_eq!(vals, vec![1, 3, 5]);
        ts.end();
    }
}

mod detoast_payload {
    use super::*;
    use crate::ssup::with_varlena_payload;
    use core::mem::MaybeUninit;

    fn compressed_image(input: &[u8]) -> Vec<u8> {
        let mut dest = vec![MaybeUninit::<u8>::uninit(); pglz::pglz_max_output(input.len())];
        let n =
            pglz::pglz_compress_into(input, &mut dest, &pglz::PGLZ_STRATEGY_ALWAYS).unwrap();
        let total = 8 + n;
        let mut image = (((total as u32) << 2) | 0x02).to_ne_bytes().to_vec();
        image.extend_from_slice(&(input.len() as u32).to_ne_bytes());
        image.extend(dest[..n].iter().map(|b| unsafe { b.assume_init() }));
        image
    }

    #[test]
    fn payload_borrows_plain_and_short_detoasts_compressed() {
        detoast::init_seams();
        let phrase: Vec<u8> = (0..2000).map(|i| b"sortable sort key "[i % 18]).collect();

        let mut plain = ((phrase.len() as u32 + 4) << 2).to_ne_bytes().to_vec();
        plain.extend_from_slice(&phrase);
        let d = Datum::from_usize(plain.as_ptr() as usize);
        unsafe { with_varlena_payload(d, |b| assert_eq!(b, &phrase[..])) };

        let mut short = vec![(6u8 << 1) | 0x01];
        short.extend_from_slice(b"tiny!");
        let d = Datum::from_usize(short.as_ptr() as usize);
        unsafe { with_varlena_payload(d, |b| assert_eq!(b, b"tiny!")) };

        let compressed = compressed_image(&phrase);
        assert!(compressed.len() < phrase.len());
        let d = Datum::from_usize(compressed.as_ptr() as usize);
        unsafe { with_varlena_payload(d, |b| assert_eq!(b, &phrase[..])) };

        // Comparator semantics across mixed forms: compressed vs plain
        // compares decompressed bytes, C's DatumGetVarStringPP cadence.
        let bigger: Vec<u8> = (0..2000).map(|i| b"zortable sort key "[i % 18]).collect();
        let mut bigger_plain = ((bigger.len() as u32 + 4) << 2).to_ne_bytes().to_vec();
        bigger_plain.extend_from_slice(&bigger);
        let (dc, dp) = (
            Datum::from_usize(compressed.as_ptr() as usize),
            Datum::from_usize(bigger_plain.as_ptr() as usize),
        );
        let r = unsafe {
            with_varlena_payload(dc, |a| {
                with_varlena_payload(dp, |b| varlena::varstrfastcmp_c(a, b))
            })
        };
        assert!(r < 0);
    }
}

mod gist_point_zorder {
    use super::*;
    use crate::ssup::{apply_cmp, SortComparator};

    fn box_image(x: f64, y: f64) -> [u8; 32] {
        let p = ::types_core::geo::Point { x, y };
        ::types_core::geo::BOX { high: p, low: p }.to_datum_bytes()
    }

    #[test]
    fn cmp_over_box_datums() {
        let cases: [((f64, f64), (f64, f64), i32); 4] = [
            ((0.0, 0.0), (0.0, 0.0), 0),
            ((1.0, 1.0), (0.0, 0.0), 1),
            ((-1.0, 0.0), (0.0, 0.0), -1),
            ((2.0, 3.0), (3.0, 2.0), 1),
        ];
        for ((x1, y1), (x2, y2), expected) in cases {
            let (a, b) = (box_image(x1, y1), box_image(x2, y2));
            let r = apply_cmp(
                SortComparator::GistPointZorder,
                Datum::from_usize(a.as_ptr() as usize),
                Datum::from_usize(b.as_ptr() as usize),
            );
            assert_eq!(r, expected, "({x1},{y1}) vs ({x2},{y2})");
        }
    }
}

pub(super) mod pgrcolumnar_ingest {
    use super::*;
    use ::types_tuple::TYPALIGN_INT;

    // int8 + text descriptor (the pgrcolumnar ingest shape: by-val ints, inline
    // 4B-U varlenas).
    pub(super) fn i64_text_desc(mcx: Mcx<'static>) -> Rc<TupleDescData<'static>> {
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        for (i, (typid, len, byval, align)) in
            [(20u32, 8i16, true, ::types_tuple::TYPALIGN_DOUBLE), (25, -1, false, TYPALIGN_INT)]
                .iter()
                .enumerate()
        {
            let att = FormData_pg_attribute {
                attnum: (i + 1) as i16,
                atttypid: *typid,
                attlen: *len,
                attbyval: *byval,
                attalign: *align,
                attstorage: ::types_tuple::TYPSTORAGE_PLAIN,
                ..Default::default()
            };
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

    pub(super) fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_le_bytes());
        v.extend_from_slice(s);
        keep.push(v);
        Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
    }

    // putvalues/getvalues (the pgrcolumnar_ingest_sort seam machinery): rows come
    // back key-sorted (text C-order primary, i64 tiebreak) and deform exactly.
    #[test]
    fn putvalues_getvalues_sorts_rows() {
        let mcx = leaked_mcx();
        let desc = i64_text_desc(mcx);
        let keys = [
            SortSupport {
                ssup_collation: ::types_core::catalog::C_COLLATION_OID,
                ssup_reverse: false,
                ssup_nulls_first: false,
                ssup_attno: 2,
                comparator: SortComparator::TextC,
            },
            SortSupport {
                ssup_collation: 0,
                ssup_reverse: false,
                ssup_nulls_first: false,
                ssup_attno: 1,
                comparator: SortComparator::SignedI64,
            },
        ];
        let mut ts = Tuplesort::begin_heap_with_keys(desc, &keys, 1024, TUPLESORT_NONE);
        let mut seed = 42u64;
        let mut rows: Vec<(i64, Vec<u8>)> = (0..2000)
            .map(|_| {
                let k = lcg(&mut seed);
                ((k % 1000) as i64 - 500, format!("k{:03}", k % 250).into_bytes())
            })
            .collect();
        let mut keep = Vec::new();
        for (i, t) in rows.iter() {
            let vals = [Datum::from_i64(*i), text_datum(t, &mut keep)];
            ts.putvalues(&vals, &[false, false]).unwrap();
        }
        ts.performsort().unwrap();
        let mut got: Vec<(i64, Vec<u8>)> = Vec::new();
        let mut values = [Datum::null(); 2];
        let mut isnull = [false; 2];
        while ts.getvalues(true, &mut values, &mut isnull).unwrap() {
            assert!(!isnull[0] && !isnull[1]);
            let p = values[1].as_usize() as *const u8;
            // 4B-U inline image copied out before the next call.
            let len = unsafe { ((p as *const u32).read_unaligned() >> 2) as usize };
            let bytes = unsafe { std::slice::from_raw_parts(p.add(4), len - 4) }.to_vec();
            got.push((values[0].as_i64(), bytes));
        }
        rows.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        assert_eq!(got, rows);
    }
}

mod pgrcolumnar_ingest_large {
    use super::*;
    use super::pgrcolumnar_ingest::*;

    #[test]
    fn putvalues_getvalues_sorts_67k_rows() {
        let mcx = leaked_mcx();
        let desc = i64_text_desc(mcx);
        let keys = [
            SortSupport {
                ssup_collation: ::types_core::catalog::C_COLLATION_OID,
                ssup_reverse: false,
                ssup_nulls_first: false,
                ssup_attno: 2,
                comparator: SortComparator::TextC,
            },
            SortSupport {
                ssup_collation: 0,
                ssup_reverse: false,
                ssup_nulls_first: false,
                ssup_attno: 1,
                comparator: SortComparator::SignedI64,
            },
        ];
        let mut ts = Tuplesort::begin_heap_with_keys(desc, &keys, 65536, TUPLESORT_NONE);
        let mut seed = 7u64;
        let mut rows: Vec<(i64, Vec<u8>)> = (0..66770)
            .map(|_| {
                let k = lcg(&mut seed);
                ((k % 1000) as i64, format!("k{:04}", k % 300).into_bytes())
            })
            .collect();
        let mut keep = Vec::new();
        for (i, t) in rows.iter() {
            let vals = [Datum::from_i64(*i), text_datum(t, &mut keep)];
            ts.putvalues(&vals, &[false, false]).unwrap();
        }
        ts.performsort().unwrap();
        let mut got: Vec<(i64, Vec<u8>)> = Vec::new();
        let mut values = [Datum::null(); 2];
        let mut isnull = [false; 2];
        while ts.getvalues(true, &mut values, &mut isnull).unwrap() {
            let p = values[1].as_usize() as *const u8;
            let len = unsafe { ((p as *const u32).read_unaligned() >> 2) as usize };
            let bytes = unsafe { std::slice::from_raw_parts(p.add(4), len - 4) }.to_vec();
            got.push((values[0].as_i64(), bytes));
        }
        rows.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        assert_eq!(got.len(), rows.len());
        for (i, (g, r)) in got.iter().zip(rows.iter()).enumerate() {
            assert_eq!(g, r, "row {i}");
        }
    }
}

// Bounded-sort memory discipline (C's TupleSortUseBumpTupleCxt): the
// caller-tuples context is an aset iff TUPLESORT_ALLOWBOUNDED, and eviction
// really frees, so the context footprint tracks the BOUND, not the input.
// Regression fence for the containerized top-N OOM class (a bump tuple
// context silently held every discarded input tuple to sort end).
mod bounded_memory_discipline {
    use super::pgrcolumnar_ingest::{i64_text_desc, text_datum};
    use super::*;

    // Far above any credible per-bound footprint, far below the ~14MB the
    // pre-fix bump arena held for this input.
    const FOOT_CAP: usize = 512 * 1024;

    #[test]
    fn bounded_heap_sort_uses_aset_and_frees_evictions() {
        let mcx = leaked_mcx();
        let desc = i64_text_desc(mcx);
        let keys = [SortSupport {
            ssup_collation: 0,
            ssup_reverse: false,
            ssup_nulls_first: false,
            ssup_attno: 1,
            comparator: SortComparator::SignedI64,
        }];
        let mut ts = Tuplesort::begin_heap_with_keys(desc, &keys, 4096, TUPLESORT_ALLOWBOUNDED);
        ts.set_bound(10);
        assert_eq!(ts.tuplecontext_stats().kind, "AllocSet", "bounded arm must be pfree-capable");
        let mut seed = 7u64;
        let mut keep = Vec::new();
        let mut mins: Vec<i64> = Vec::new();
        for _ in 0..200_000 {
            let k = (lcg(&mut seed) % 1_000_000_000) as i64;
            mins.push(k);
            let payload = format!("pad-{k:032}").into_bytes();
            let vals = [Datum::from_i64(k), text_datum(&payload, &mut keep)];
            ts.putvalues(&vals, &[false, false]).unwrap();
            keep.clear();
        }
        ts.performsort().unwrap();
        let stats = ts.tuplecontext_stats();
        assert!(
            stats.arena_footprint < FOOT_CAP,
            "caller-tuples footprint {} must track the bound, not the 200k-row input",
            stats.arena_footprint
        );
        mins.sort_unstable();
        let mut got = Vec::new();
        let mut values = [Datum::null(); 2];
        let mut isnull = [false; 2];
        for _ in 0..10 {
            assert!(ts.getvalues(true, &mut values, &mut isnull).unwrap());
            got.push(values[0].as_i64());
        }
        assert_eq!(got, mins[..10], "top-N output survives the physical frees");
        ts.end();
    }

    #[test]
    fn bounded_text_datum_sort_frees_evictions() {
        let vals = random_texts(50_000, 0x5eed, b"");
        let mut ts = begin_text_datum_abbrev(TUPLESORT_ALLOWBOUNDED);
        ts.set_bound(25);
        assert_eq!(ts.tuplecontext_stats().kind, "AllocSet");
        let blobs: Vec<Option<Box<[u64]>>> =
            vals.iter().map(|v| v.as_ref().map(|p| text_blob(p))).collect();
        for b in &blobs {
            match b {
                Some(blob) => {
                    ts.putdatum(Datum::from_usize(blob.as_ptr() as usize), false).unwrap()
                }
                None => ts.putdatum(Datum::null(), true).unwrap(),
            }
        }
        ts.performsort().unwrap();
        assert!(ts.used_bound());
        let stats = ts.tuplecontext_stats();
        assert!(
            stats.arena_footprint < FOOT_CAP,
            "datumCopy footprint {} must track the bound, not the 50k-datum input",
            stats.arena_footprint
        );
        let mut got = Vec::new();
        for _ in 0..25 {
            let nd = ts.getdatum(true).unwrap().expect("bound rows present");
            got.push(if nd.isnull {
                None
            } else {
                let p = nd.value.as_usize() as *const u8;
                // SAFETY: sort-owned datumCopy image.
                Some(unsafe {
                    use ::types_tuple::varatt::{varatt_is_1b, varsize_1b, varsize_4b};
                    if varatt_is_1b(p) {
                        std::slice::from_raw_parts(p.add(1), varsize_1b(p) - 1).to_vec()
                    } else {
                        std::slice::from_raw_parts(p.add(4), varsize_4b(p) - 4).to_vec()
                    }
                })
            });
        }
        assert_eq!(got[..], text_oracle(vals, false)[..25]);
        ts.end();
    }

    #[test]
    fn unbounded_sort_keeps_the_bump_arm() {
        let mut ts =
            Tuplesort::begin_datum_with_key(int32_key(1, false, false), 1024, TUPLESORT_NONE);
        assert_eq!(
            ts.tuplecontext_stats().kind,
            "Bump",
            "no-bound sorts must keep the bump win (C parity)"
        );
        ts.end();
    }

    fn put_rows(ts: &mut Tuplesort, n: u64, seed: &mut u64) {
        let mut keep = Vec::new();
        for _ in 0..n {
            let k = (lcg(seed) % 1_000_000) as i64;
            let payload = format!("pad-{k:016}").into_bytes();
            let vals = [Datum::from_i64(k), text_datum(&payload, &mut keep)];
            ts.putvalues(&vals, &[false, false]).unwrap();
            keep.clear();
        }
    }

    fn drain(ts: &mut Tuplesort, max: usize) -> Vec<i64> {
        let mut got = Vec::new();
        let mut values = [Datum::null(); 2];
        let mut isnull = [false; 2];
        while got.len() < max && ts.getvalues(true, &mut values, &mut isnull).unwrap() {
            got.push(values[0].as_i64());
        }
        got
    }

    // tuplesort_reset's C contract: working memory releases WHOLESALE
    // (tuplesort_free resets the sort context — the caller-tuples child dies
    // with the RETAINED tuples still inside; readout never pfrees them — and
    // tuplesort_begin_batch recreates it). The aset arm must honor that
    // lifecycle: a reset with retained tuples still charged is the normal
    // batch boundary, not a leak.
    #[test]
    fn reset_after_bounded_readout_releases_retained_tuples() {
        let mcx = leaked_mcx();
        let desc = i64_text_desc(mcx);
        let keys = [SortSupport {
            ssup_collation: 0,
            ssup_reverse: false,
            ssup_nulls_first: false,
            ssup_attno: 1,
            comparator: SortComparator::SignedI64,
        }];
        let mut ts = Tuplesort::begin_heap_with_keys(desc, &keys, 4096, TUPLESORT_ALLOWBOUNDED);
        ts.set_bound(10);
        let mut seed = 11u64;
        put_rows(&mut ts, 100, &mut seed);
        ts.performsort().unwrap();
        let first = drain(&mut ts, 10);
        assert_eq!(first.len(), 10);

        // The bound survivors are still charged in the aset caller-tuples
        // context here (only EVICTIONS free per-tuple).
        ts.reset();

        // The recycled state sorts a second batch.
        put_rows(&mut ts, 50, &mut seed);
        ts.performsort().unwrap();
        let second = drain(&mut ts, usize::MAX);
        assert_eq!(second.len(), 50);
        assert!(second.windows(2).all(|w| w[0] <= w[1]));
        ts.end();
    }

    // The bounded-CAPABLE arm without set_bound (a bounded caller whose
    // current batch exceeds the small-group threshold puts with no bound):
    // nothing is ever evicted, so EVERY batch tuple is retained at reset.
    #[test]
    fn reset_without_bound_set_releases_whole_batch() {
        let mcx = leaked_mcx();
        let desc = i64_text_desc(mcx);
        let keys = [SortSupport {
            ssup_collation: 0,
            ssup_reverse: false,
            ssup_nulls_first: false,
            ssup_attno: 1,
            comparator: SortComparator::SignedI64,
        }];
        let mut ts = Tuplesort::begin_heap_with_keys(desc, &keys, 4096, TUPLESORT_ALLOWBOUNDED);
        let mut seed = 23u64;
        put_rows(&mut ts, 64, &mut seed);
        ts.performsort().unwrap();
        assert_eq!(drain(&mut ts, usize::MAX).len(), 64);

        ts.reset();

        put_rows(&mut ts, 8, &mut seed);
        ts.performsort().unwrap();
        assert_eq!(drain(&mut ts, usize::MAX).len(), 8);
        ts.end();
    }
}
