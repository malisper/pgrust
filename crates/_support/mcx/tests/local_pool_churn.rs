// Per-thread context pools, exercised as dependents see them: an integration
// build compiles the lib WITHOUT cfg(test), so the acct/child-vec/keeper
// pooling paths are live here (the in-crate unit tests bypass them).
//
// Run: cargo test -p mcx --features std [--features aset-stats]
#![cfg(feature = "std")]

use mcx::{box_new_in, MemoryContext, PgVec};

fn churn_once(round: usize) {
    // A context tree shaped like a backend's: a root, per-"query" children,
    // per-"tuple" bump children, with allocations checked for value fidelity.
    let root = MemoryContext::new("churn-root");
    for q in 0..4usize {
        let query = root.new_child("churn-query");
        let mut v: PgVec<u64> = PgVec::new_in(query.mcx());
        for i in 0..64u64 {
            v.push(i.wrapping_mul(round as u64 + 1));
        }
        assert_eq!(v[63], 63u64.wrapping_mul(round as u64 + 1));

        let mut tup = query.new_child_bump("churn-tuple");
        let b = box_new_in(tup.mcx(), [q as u8; 128]);
        assert_eq!(b[127], q as u8);
        drop(b);
        tup.reset();
        drop(tup);

        // Grand-children dropped BEFORE their parent (children-vec churn),
        // and one dropped after v to vary destruction order.
        let gc1 = query.new_child("churn-gc1");
        let gc2 = query.new_child_generation("churn-gc2");
        drop(gc1);
        drop(v);
        drop(gc2);
        drop(query);
    }
    drop(root);
}

/// The cross-thread churn hammer: every thread creates and destroys its own
/// context trees concurrently. Contexts are `!Send`, so create/destroy always
/// pair on one thread — the hammer validates the pools under exactly the
/// concurrency the server produces (many backend threads churning at once).
#[test]
fn multi_thread_context_churn() {
    let threads: Vec<_> = (0..8)
        .map(|t| {
            std::thread::spawn(move || {
                for round in 0..300usize {
                    churn_once(round ^ t);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    // The pools survive the storm: a fresh context on this thread still works.
    let ctx = MemoryContext::new("churn-after");
    let b = box_new_in(ctx.mcx(), 0xa5a5_5a5au32);
    assert_eq!(*b, 0xa5a5_5a5au32);
}

/// Same-thread reuse actually happens: with aset-stats compiled in, a
/// create→drop→create cycle of aset contexts must hit the keeper list.
#[cfg(feature = "aset-stats")]
#[test]
fn keeper_reuse_on_same_thread() {
    // Prime: park at least one keeper on THIS thread's list. The context must
    // allocate once so a keeper block exists to park.
    {
        let ctx = MemoryContext::new("reuse-prime");
        let _b = box_new_in(ctx.mcx(), [0u8; 256]);
    }
    let (_, reuses_before) = mcx::alloc_stats();
    {
        let ctx = MemoryContext::new("reuse-take");
        let _b = box_new_in(ctx.mcx(), [0u8; 256]);
    }
    let (_, reuses_after) = mcx::alloc_stats();
    // Counters are global (other tests may add); same-thread reuse guarantees
    // at least one increment between the reads.
    assert!(
        reuses_after > reuses_before,
        "no keeper reuse observed ({reuses_before} -> {reuses_after})"
    );
}

/// Targeted contention microbench (not a gate): N threads x context
/// create/destroy churn, ns/op per thread count. The global-pool posture is
/// the same binary with PGRUST_MCX_POOL_STRIPE=0.
///
/// Run: cargo test -p mcx --features std --release -- --ignored churn_bench --nocapture
#[test]
#[ignore]
fn churn_bench() {
    const ITERS: usize = 20_000;
    for nthreads in [1usize, 4, 16, 64] {
        let start = std::time::Instant::now();
        let threads: Vec<_> = (0..nthreads)
            .map(|t| {
                std::thread::spawn(move || {
                    for round in 0..ITERS {
                        std::hint::black_box(churn_once(round ^ t));
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let elapsed = start.elapsed();
        // churn_once = 1 root + 4x(query + tuple + 2 grandchildren) contexts.
        let ctxs = nthreads * ITERS * 17;
        println!(
            "churn_bench threads={:2} total={:?} ns/context-cycle={:.1}",
            nthreads,
            elapsed,
            elapsed.as_nanos() as f64 / ctxs as f64
        );
    }
}
