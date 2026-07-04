use super::*;
use mcx::MemoryContext;
use std::collections::{BTreeMap, BTreeSet};
use types_storage::bufpage::MaxHeapTuplesPerPage;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn random_offsets(rng: &mut Rng, style: u64) -> Vec<OffsetNumber> {
    let n = match style % 4 {
        0 => 1 + (rng.next() % 3) as usize,
        1 => 1 + (rng.next() % 10) as usize,
        2 => 1 + (rng.next() % 80) as usize,
        _ => 1 + (rng.next() % MaxHeapTuplesPerPage as u64) as usize,
    };
    let mut set = BTreeSet::new();
    for _ in 0..n {
        set.insert(1 + (rng.next() % MAX_OFFSET_IN_BITMAP as u64) as OffsetNumber);
    }
    set.into_iter().collect()
}

fn check_oracle(
    ts: &TidStore,
    oracle: &BTreeMap<BlockNumber, Vec<OffsetNumber>>,
    probe_blocks: &[BlockNumber],
) {
    assert_eq!(ts.num_blocks(), oracle.len() as i64);

    let mut rng = Rng(0x5ca1e);
    for &blk in probe_blocks {
        let offs: &[OffsetNumber] = oracle.get(&blk).map(|v| v.as_slice()).unwrap_or(&[]);
        for &off in offs {
            assert!(ts.is_member(&ItemPointerData::new(blk, off)), "({blk},{off})");
        }
        for _ in 0..20 {
            let off = 1 + (rng.next() % MAX_OFFSET_IN_BITMAP as u64) as OffsetNumber;
            assert_eq!(
                ts.is_member(&ItemPointerData::new(blk, off)),
                offs.contains(&off),
                "({blk},{off})"
            );
        }
    }

    let mut iter = ts.begin_iterate();
    let mut seen: Vec<(BlockNumber, Vec<OffsetNumber>)> = Vec::new();
    let mut buf = [0 as OffsetNumber; MaxOffsetNumber as usize];
    while let Some(res) = iter.next() {
        let n = res.block_offsets(&mut buf);
        assert!(n <= buf.len());
        seen.push((res.blkno, buf[..n].to_vec()));
    }
    let expect: Vec<(BlockNumber, Vec<OffsetNumber>)> =
        oracle.iter().map(|(k, v)| (*k, v.clone())).collect();
    assert_eq!(seen, expect);
}

fn run_workload(mut blockgen: impl FnMut(&mut Rng) -> BlockNumber, seed: u64, n_blocks: usize) {
    let n_blocks = if cfg!(miri) { n_blocks / 20 } else { n_blocks };
    let ctx = MemoryContext::new("tidstore test");
    let mut ts = TidStore::create_local(ctx.mcx(), 64 * 1024 * 1024, true).unwrap();
    let mut oracle: BTreeMap<BlockNumber, Vec<OffsetNumber>> = BTreeMap::new();
    let mut rng = Rng(seed);
    let mut touched = Vec::new();

    for i in 0..n_blocks {
        let blk = blockgen(&mut rng);
        // Vacuum sets each block at most once per pass; replacement is still
        // valid TidStore API (bump dealloc is a no-op, unlike C's ERROR).
        let offs = random_offsets(&mut rng, i as u64);
        ts.set_block_offsets(blk, &offs).unwrap();
        oracle.insert(blk, offs);
        touched.push(blk);
    }
    check_oracle(&ts, &oracle, &touched);
    assert!(ts.memory_usage() > 0);
}

#[test]
fn dense_blocks() {
    run_workload(|rng| (rng.next() % 512) as BlockNumber, 101, 2000);
}

#[test]
fn sparse_blocks() {
    run_workload(|rng| rng.next() as BlockNumber, 202, 1500);
}

#[test]
fn sequential_blocks_vacuum_shaped() {
    let ctx = MemoryContext::new("tidstore test");
    let mut ts = TidStore::create_local(ctx.mcx(), 64 * 1024 * 1024, true).unwrap();
    let mut oracle = BTreeMap::new();
    let mut rng = Rng(303);
    let nb: u32 = if cfg!(miri) { 200 } else { 4000 };
    for blk in 0..nb {
        let offs = random_offsets(&mut rng, blk as u64);
        ts.set_block_offsets(blk, &offs).unwrap();
        oracle.insert(blk, offs);
    }
    let probes: Vec<BlockNumber> = (0..nb).collect();
    check_oracle(&ts, &oracle, &probes);
}

// test_tidstore.sql: boundary blocks {0, 1, maxblkno/2, maxblkno-1, maxblkno}
// x offsets {1, 2, maxoffset/2, maxoffset-1, maxoffset} with the offset limit
// at MAX_OFFSET_IN_BITMAP (== MaxOffsetNumber here).
#[test]
fn boundary_blocks_and_offsets() {
    const MAXBLKNO: BlockNumber = 0xFFFF_FFFF;
    let maxoff = MAX_OFFSET_IN_BITMAP;
    let ctx = MemoryContext::new("tidstore test");
    let mut ts = TidStore::create_local(ctx.mcx(), 2 * 1024 * 1024, false).unwrap();
    let blocks = [0, 1, MAXBLKNO / 2, MAXBLKNO - 1, MAXBLKNO];
    let offsets = [1, 2, maxoff / 2, maxoff - 1, maxoff];
    let mut oracle = BTreeMap::new();
    for &blk in &blocks {
        ts.set_block_offsets(blk, &offsets).unwrap();
        oracle.insert(blk, offsets.to_vec());
    }
    // "full" per test_is_full: usage grew past the empty-store baseline.
    let empty = TidStore::create_local(ctx.mcx(), 2 * 1024 * 1024, false).unwrap();
    assert!(ts.memory_usage() > empty.memory_usage());
    check_oracle(&ts, &oracle, &blocks);
}

// test_tidstore.sql: replacements crossing RT_CHILDPTR_IS_VALUE (embedded <->
// leaf) in both directions; non-insert-only store as the C test.
#[test]
fn replacement_crosses_embedded_boundary() {
    let ctx = MemoryContext::new("tidstore test");
    let mut ts = TidStore::create_local(ctx.mcx(), 2 * 1024 * 1024, false).unwrap();
    let steps: [&[OffsetNumber]; 9] = [
        &[1],
        &[1, 2],
        &[1, 2, 3],
        &[1, 2, 3, 4],
        &[1, 2, 3, 4, 100],
        &[1, 2, 3, 4],
        &[1, 2, 3],
        &[1, 2],
        &[1],
    ];
    for offs in steps {
        ts.set_block_offsets(1, offs).unwrap();
        let mut oracle = BTreeMap::new();
        oracle.insert(1, offs.to_vec());
        check_oracle(&ts, &oracle, &[1]);
    }
}

#[test]
fn header_only_and_bitmap_entries() {
    let ctx = MemoryContext::new("tidstore test");
    let mut ts = TidStore::create_local(ctx.mcx(), 1024 * 1024, true).unwrap();
    ts.set_block_offsets(1, &[7]).unwrap();
    ts.set_block_offsets(2, &[1, 2, 3]).unwrap();
    ts.set_block_offsets(3, &[1, 2, 3, 4]).unwrap();
    ts.set_block_offsets(4, &[MAX_OFFSET_IN_BITMAP]).unwrap();
    ts.set_block_offsets(5, &[1, MAX_OFFSET_IN_BITMAP]).unwrap();

    assert!(ts.is_member(&ItemPointerData::new(1, 7)));
    assert!(!ts.is_member(&ItemPointerData::new(1, 8)));
    assert!(ts.is_member(&ItemPointerData::new(3, 4)));
    assert!(!ts.is_member(&ItemPointerData::new(3, 5)));
    assert!(ts.is_member(&ItemPointerData::new(4, MAX_OFFSET_IN_BITMAP)));
    assert!(!ts.is_member(&ItemPointerData::new(4, 1)));
    assert!(ts.is_member(&ItemPointerData::new(5, MAX_OFFSET_IN_BITMAP)));
    assert!(!ts.is_member(&ItemPointerData::new(6, 1)));
    assert!(!ts.is_member(&ItemPointerData::new(0xffff_0000, 1)));
}

#[test]
fn offset_out_of_range_errors() {
    let ctx = MemoryContext::new("tidstore test");
    let mut ts = TidStore::create_local(ctx.mcx(), 1024 * 1024, true).unwrap();
    let err = ts.set_block_offsets(1, &[MAX_OFFSET_IN_BITMAP + 1]).unwrap_err();
    assert!(err.message().contains("tuple offset out of range"));
    let big: Vec<OffsetNumber> = (1..=4).chain([MAX_OFFSET_IN_BITMAP + 1]).collect();
    assert!(ts.set_block_offsets(2, &big).is_err());
}

#[test]
fn memory_usage_scales() {
    let ctx = MemoryContext::new("tidstore test");
    let mut ts = TidStore::create_local(ctx.mcx(), 256 * 1024 * 1024, true).unwrap();
    let mut rng = Rng(404);
    let start = ts.memory_usage();
    let nb2: u32 = if cfg!(miri) { 500 } else { 20_000 };
    for blk in 0..nb2 {
        let offs = random_offsets(&mut rng, 2);
        ts.set_block_offsets(blk, &offs).unwrap();
    }
    assert!(ts.memory_usage() > start);
}

#[test]
fn block_offsets_reports_needed_size() {
    let ctx = MemoryContext::new("tidstore test");
    let mut ts = TidStore::create_local(ctx.mcx(), 1024 * 1024, true).unwrap();
    ts.set_block_offsets(9, &[1, 5, 9, 200, 900]).unwrap();
    let mut iter = ts.begin_iterate();
    let res = iter.next().unwrap();
    let mut small = [0 as OffsetNumber; 2];
    assert_eq!(res.block_offsets(&mut small), 5);
    assert_eq!(small, [1, 5]);
    let mut exact = [0 as OffsetNumber; 5];
    assert_eq!(res.block_offsets(&mut exact), 5);
    assert_eq!(exact, [1, 5, 9, 200, 900]);
    assert!(iter.next().is_none());
}

#[test]
fn model_random_ops() {
    for seed in [7u64, 42, 0xFEED] {
        let ctx = MemoryContext::new("tidstore model");
        let mut ts = TidStore::create_local(ctx.mcx(), 64 * 1024 * 1024, false).unwrap();
        let mut model: BTreeMap<BlockNumber, BTreeSet<OffsetNumber>> = BTreeMap::new();
        let mut rng = Rng(seed);
        let steps = if cfg!(miri) { 300 } else { 6000 };

        for i in 0..steps {
            let r = rng.next();
            let blk = match r % 3 {
                0 => (rng.next() % 128) as BlockNumber,
                1 => (rng.next() % 100_000) as BlockNumber,
                _ => rng.next() as BlockNumber,
            };
            if r % 10 < 7 {
                let style = rng.next();
                let offs = random_offsets(&mut rng, style);
                ts.set_block_offsets(blk, &offs).unwrap();
                model.insert(blk, offs.iter().copied().collect());
            } else {
                let off = 1 + (rng.next() % MAX_OFFSET_IN_BITMAP as u64) as OffsetNumber;
                assert_eq!(
                    ts.is_member(&ItemPointerData::new(blk, off)),
                    model.get(&blk).is_some_and(|s| s.contains(&off)),
                    "step {i} ({blk},{off})"
                );
            }
        }

        assert_eq!(ts.num_blocks(), model.len() as i64);
        let mut iter = ts.begin_iterate();
        let mut buf = [0 as OffsetNumber; MaxOffsetNumber as usize];
        for (&mblk, moffs) in model.iter() {
            let res = iter.next().expect("iterator ended early");
            assert_eq!(res.blkno, mblk);
            let n = res.block_offsets(&mut buf);
            let got: BTreeSet<OffsetNumber> = buf[..n].iter().copied().collect();
            assert_eq!(&got, moffs, "block {mblk}");
        }
        assert!(iter.next().is_none());
    }
}

#[test]
fn shared_store_threads() {
    let shared = SharedTidStore::create_shared(2 * 1024 * 1024, 0).unwrap();
    let nthreads = 4u32;
    let per_thread = 500u32;

    std::thread::scope(|s| {
        for t in 0..nthreads {
            let shared = &shared;
            s.spawn(move || {
                for i in 0..per_thread {
                    let blk = t * per_thread + i;
                    let offs = [1, (2 + (blk % 200)) as OffsetNumber, 400];
                    let mut guard = shared.lock_exclusive();
                    guard.set_block_offsets(blk, &offs).unwrap();
                }
            });
        }
    });

    let guard = shared.lock_share();
    let mut iter = guard.begin_iterate();
    let mut buf = [0 as OffsetNumber; 8];
    for blk in 0..nthreads * per_thread {
        let res = iter.next().unwrap();
        assert_eq!(res.blkno, blk);
        let n = res.block_offsets(&mut buf);
        assert_eq!(&buf[..n], &[1, (2 + (blk % 200)) as OffsetNumber, 400]);
        assert!(guard.is_member(&ItemPointerData::new(blk, 400)));
        assert!(!guard.is_member(&ItemPointerData::new(blk, 401)));
    }
    assert!(iter.next().is_none());
    drop(guard);
    assert!(shared.memory_usage() > 0);
}
