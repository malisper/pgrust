// Paired with ../cref.c — identical lane shapes, LCG, key set, barrier.
use dshash::{DshashHash, DshashParams, DshashTable};
use std::time::Instant;

#[inline(always)]
fn bar(mut v: u64) -> u64 {
    unsafe { std::arch::asm!("/* {0} */", inout(reg) v, options(nomem, nostack, preserves_flags)) };
    v
}

struct BenchParams;

struct BenchEntry {
    key: u64,
    value: u64,
}

impl DshashParams for BenchParams {
    type Key = u64;
    type Entry = BenchEntry;

    #[inline]
    fn hash(&self, key: &u64) -> DshashHash {
        (key.wrapping_mul(0x9E3779B97F4A7C15) >> 32) as DshashHash
    }

    #[inline]
    fn keys_equal(&self, a: &u64, b: &u64) -> bool {
        a == b
    }

    #[inline]
    fn entry_key<'e>(&self, entry: &'e BenchEntry) -> &'e u64 {
        &entry.key
    }

    #[inline]
    fn new_entry(&self, key: &u64) -> BenchEntry {
        BenchEntry { key: *key, value: 0 }
    }
}

const NKEYS: u64 = 4096;

fn bench_table() -> DshashTable<BenchParams> {
    let t = DshashTable::create(BenchParams, 1);
    for k in 0..NKEYS {
        let (mut e, _) = t.find_or_insert(&k).unwrap();
        e.value = k;
    }
    t
}

#[inline(always)]
fn lcg(s: &mut u64) -> u64 {
    *s = s
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *s
}

fn find_hit(iters: u64, exclusive: bool) -> f64 {
    let t = bench_table();
    let mut s = 0x243F_6A88_85A3_08D3u64;
    let mut acc = 0u64;
    let t0 = Instant::now();
    for _ in 0..iters {
        let key = (lcg(&mut s) >> 33) % NKEYS;
        if exclusive {
            let e = t.find_exclusive(&key).unwrap().unwrap();
            acc ^= e.value;
        } else {
            let e = t.find_shared(&key).unwrap().unwrap();
            acc ^= e.value;
        }
        acc = bar(acc);
    }
    t0.elapsed().as_nanos() as f64 / iters as f64
}

fn find_miss(iters: u64) -> f64 {
    let t = bench_table();
    let mut s = 0x243F_6A88_85A3_08D3u64;
    let mut acc = 0u64;
    let t0 = Instant::now();
    for _ in 0..iters {
        let key = NKEYS + (lcg(&mut s) >> 33) % NKEYS;
        acc ^= t.find_shared(&key).unwrap().is_some() as u64;
        acc = bar(acc);
    }
    t0.elapsed().as_nanos() as f64 / iters as f64
}

fn fii_hit(iters: u64) -> f64 {
    let t = bench_table();
    let mut s = 0x243F_6A88_85A3_08D3u64;
    let mut acc = 0u64;
    let t0 = Instant::now();
    for _ in 0..iters {
        let key = (lcg(&mut s) >> 33) % NKEYS;
        let (e, found) = t.find_or_insert(&key).unwrap();
        acc ^= e.value + found as u64;
        drop(e);
        acc = bar(acc);
    }
    t0.elapsed().as_nanos() as f64 / iters as f64
}

fn insert_delete(iters: u64) -> f64 {
    let t = bench_table();
    let mut acc = 0u64;
    let t0 = Instant::now();
    for i in 0..iters {
        let key = NKEYS + (i & 1023);
        let (mut e, found) = t.find_or_insert(&key).unwrap();
        e.value = key;
        acc ^= found as u64;
        drop(e);
        acc ^= t.delete_key(&key).unwrap() as u64;
        acc = bar(acc);
    }
    t0.elapsed().as_nanos() as f64 / iters as f64
}

// Informational contended lane, Rust-only (the C ref's wait path aborts).
// Reports total ns/op across 4 threads hammering 64 keys.
fn mt_fii_4t(iters: u64) -> f64 {
    install_wait_seams();
    let t = std::sync::Arc::new(bench_table());
    let per = iters / 4;
    let t0 = Instant::now();
    let handles: Vec<_> = (0..4)
        .map(|ti| {
            let t = t.clone();
            std::thread::spawn(move || {
                init_small::globals::SetMyProcNumber(ti + 1);
                let mut s = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(ti as u64 + 1);
                let mut acc = 0u64;
                for _ in 0..per {
                    let key = (lcg(&mut s) >> 33) % 64;
                    let (mut e, _) = t.find_or_insert(&key).unwrap();
                    e.value = e.value.wrapping_add(1);
                    drop(e);
                    acc = bar(acc);
                }
                acc
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    t0.elapsed().as_nanos() as f64 / (per * 4) as f64
}

static SEMA: [std::sync::atomic::AtomicI32; 8] =
    [const { std::sync::atomic::AtomicI32::new(0) }; 8];
static WAITING: [std::sync::atomic::AtomicU8; 8] =
    [const { std::sync::atomic::AtomicU8::new(0) }; 8];
static WAIT_MODE: [std::sync::atomic::AtomicU8; 8] =
    [const { std::sync::atomic::AtomicU8::new(0) }; 8];
static WAIT_LINK: [std::sync::atomic::AtomicU64; 8] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 8];

fn install_wait_seams() {
    use std::sync::atomic::Ordering::*;
    static ONCE: std::sync::Once = std::sync::Once::new();
    let mut fresh = false;
    ONCE.call_once(|| fresh = true);
    if !fresh {
        return;
    }
    lmgr_proc_seams::proc_lw_waiting::set(|p| WAITING[p as usize].load(Acquire));
    lmgr_proc_seams::set_proc_lw_waiting::set(|p, s| WAITING[p as usize].store(s, Release));
    lmgr_proc_seams::proc_lw_wait_mode::set(|p| WAIT_MODE[p as usize].load(Acquire));
    lmgr_proc_seams::set_proc_lw_wait_mode::set(|p, m| WAIT_MODE[p as usize].store(m, Release));
    lmgr_proc_seams::proc_lw_wait_link::set(|p| {
        let v = WAIT_LINK[p as usize].load(Acquire);
        lmgr_proc_seams::proclist_node {
            next: (v >> 32) as u32 as i32,
            prev: v as u32 as i32,
        }
    });
    lmgr_proc_seams::set_proc_lw_wait_link::set(|p, n| {
        WAIT_LINK[p as usize].store(((n.next as u32 as u64) << 32) | n.prev as u32 as u64, Release)
    });
    lmgr_proc_seams::pg_semaphore_lock::set(|p| {
        let sema = &SEMA[p as usize];
        loop {
            let c = sema.load(Acquire);
            if c > 0 && sema.compare_exchange(c, c - 1, AcqRel, Relaxed).is_ok() {
                return;
            }
            std::thread::yield_now();
        }
    });
    lmgr_proc_seams::pg_semaphore_unlock::set(|p| {
        SEMA[p as usize].fetch_add(1, AcqRel);
    });
    s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
    s_lock_seams::finish_spin_delay::set(|_| {});
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let name = args.get(1).map(String::as_str).unwrap_or("dshash_find_shared_hit");
    let iters: u64 = args.get(2).map_or(10_000_000, |s| s.parse().unwrap());
    let reps: u32 = args.get(3).map_or(5, |s| s.parse().unwrap());

    let f: fn(u64) -> f64 = match name {
        "dshash_find_shared_hit" => |n| find_hit(n, false),
        "dshash_find_excl_hit" => |n| find_hit(n, true),
        "dshash_find_miss" => find_miss,
        "dshash_fii_hit" => fii_hit,
        "dshash_insert_delete" => insert_delete,
        "dshash_mt_fii_4t" => mt_fii_4t,
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
