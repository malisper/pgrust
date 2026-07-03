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
        desc.clone(), &[int32_key(1, false, false)], 1, false, false, "t_a_idx", 1024,
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
        desc.clone(), &keys, 2, false, false, "t_ab_idx", 1024, TUPLESORT_NONE,
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

#[test]
fn index_sort_unique_violation_is_23505() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplesort::begin_index_with_keys(
        desc, &[int32_key(1, false, false)], 1, true, false, "t_a_key", 1024, TUPLESORT_NONE,
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
        desc.clone(), &[int32_key(1, false, false)], 1, true, false, "t_a_key", 1024,
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
        desc.clone(), &[key], 1, false, false, "t_txt_idx", 1024, TUPLESORT_NONE,
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
