// Paired with ../../cref/main.c — identical lane shapes, LCG, offsets, barrier.
use mcx::MemoryContext;
use std::time::Instant;
use tidstore::TidStore;
use types_core::{BlockNumber, OffsetNumber};
use types_tuple::ItemPointerData;

const NBLOCKS: u64 = 65536;
const NOFFS_BITMAP: usize = 20;
const INLINE_OFFS: [OffsetNumber; 2] = [4, 200];

fn bitmap_offs() -> [OffsetNumber; NOFFS_BITMAP] {
    core::array::from_fn(|j| (3 + 14 * j) as OffsetNumber)
}

#[inline(always)]
fn bar(mut v: u64) -> u64 {
    unsafe { std::arch::asm!("/* {0} */", inout(reg) v, options(nomem, nostack, preserves_flags)) };
    v
}

#[inline(always)]
fn lcg(s: &mut u64) -> u64 {
    *s = s
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *s
}

fn bench_set(iters: u64, offs: &[OffsetNumber]) -> f64 {
    let ctx = MemoryContext::new("bench");
    let mut ts: Option<TidStore> = None;
    let mut acc = 0u64;
    let t0 = Instant::now();
    for i in 0..iters {
        let b = i & (NBLOCKS - 1);
        if b == 0 {
            // destroy-then-create ordering as the C side.
            ts = None;
            ts = Some(TidStore::create_local(ctx.mcx(), 256 * 1024 * 1024, true).unwrap());
        }
        ts.as_mut().unwrap().set_block_offsets(b as BlockNumber, offs).unwrap();
        acc += b;
        acc = bar(acc);
    }
    let ns = t0.elapsed().as_nanos() as f64 / iters as f64;
    drop(ts);
    ns
}

fn build_store(ctx: &MemoryContext) -> TidStore {
    let offs = bitmap_offs();
    let mut ts = TidStore::create_local(ctx.mcx(), 256 * 1024 * 1024, true).unwrap();
    for b in 0..NBLOCKS {
        ts.set_block_offsets(b as BlockNumber, &offs).unwrap();
    }
    ts
}

fn bench_member(iters: u64, hit: bool) -> f64 {
    let ctx = MemoryContext::new("bench");
    let ts = build_store(&ctx);
    let mut s = 0x243F_6A88_85A3_08D3u64;
    let mut acc = 0u64;
    let t0 = Instant::now();
    for _ in 0..iters {
        let r = lcg(&mut s);
        let blk = ((r >> 33) & (NBLOCKS - 1)) as BlockNumber;
        let off = (3 + 14 * ((r >> 13) % NOFFS_BITMAP as u64) + if hit { 0 } else { 1 })
            as OffsetNumber;
        acc ^= ts.is_member(&ItemPointerData::new(blk, off)) as u64;
        acc = bar(acc);
    }
    t0.elapsed().as_nanos() as f64 / iters as f64
}

fn bench_iterate(iters: u64) -> f64 {
    let ctx = MemoryContext::new("bench");
    let ts = build_store(&ctx);
    let rounds = (iters / NBLOCKS).max(1);
    let mut acc = 0u64;
    let mut buf = [0 as OffsetNumber; 512];
    let t0 = Instant::now();
    for _ in 0..rounds {
        let mut it = ts.begin_iterate();
        while let Some(res) = it.next() {
            let n = res.block_offsets(&mut buf);
            acc ^= res.blkno as u64 + n as u64;
            acc = bar(acc);
        }
    }
    t0.elapsed().as_nanos() as f64 / (rounds * NBLOCKS) as f64
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let name = args.get(1).map(String::as_str).unwrap_or("tidstore_set_dense");
    let iters: u64 = args.get(2).map_or(3_000_000, |s| s.parse().unwrap());
    let reps: u32 = args.get(3).map_or(5, |s| s.parse().unwrap());

    let f: Box<dyn Fn(u64) -> f64> = match name {
        "tidstore_set_dense" => Box::new(|n| bench_set(n, &bitmap_offs())),
        "tidstore_set_inline" => Box::new(|n| bench_set(n, &INLINE_OFFS)),
        "tidstore_member_hit" => Box::new(|n| bench_member(n, true)),
        "tidstore_member_miss" => Box::new(|n| bench_member(n, false)),
        "tidstore_iterate" => Box::new(bench_iterate),
        other => {
            eprintln!("unknown bench {other}");
            std::process::exit(1);
        }
    };
    if reps > 1 {
        f(iters / 10);
    }
    let mut best = f64::INFINITY;
    for _ in 0..reps {
        best = best.min(f(iters));
    }
    println!("{name}\t{best:.4}");
}
