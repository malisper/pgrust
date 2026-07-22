// Kill switch: PGRUST_MCX_POOL_STRIPE=0 restores the global spin-locked
// pools. Its own binary: the knob latches on first use (OnceLock), so the env
// var must be set before ANY context operation in the process — hence exactly
// one #[test] here and none elsewhere in this file.
//
// Run: cargo test -p mcx --features std --test local_pool_kill_knob
#![cfg(feature = "std")]

use mcx::{box_new_in, MemoryContext, PgVec};

#[test]
fn global_pools_survive_multi_thread_churn() {
    // Edition-2021 std::env::set_var (safe fn); no other test in this binary
    // has touched a context yet, so the OnceLock latches the kill value.
    std::env::set_var("PGRUST_MCX_POOL_STRIPE", "0");

    let threads: Vec<_> = (0..8)
        .map(|t| {
            std::thread::spawn(move || {
                for round in 0..200usize {
                    let root = MemoryContext::new("knob-root");
                    let child = root.new_child("knob-child");
                    let mut v: PgVec<u32> = PgVec::new_in(child.mcx());
                    for i in 0..32u32 {
                        v.push(i + t);
                    }
                    assert_eq!(v[31], 31 + t);
                    let bump = child.new_child_bump("knob-bump");
                    let b = box_new_in(bump.mcx(), round as u64);
                    assert_eq!(*b, round as u64);
                    drop(b);
                    drop(bump);
                    drop(v);
                    drop(child);
                    drop(root);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
}
