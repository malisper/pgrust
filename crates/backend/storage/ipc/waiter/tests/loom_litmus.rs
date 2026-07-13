//! Documentation of the loom tool boundary the Dekker model works around
//! (tests/loom.rs): loom 0.7 does NOT model the single total order of
//! SeqCst operations/fences — the classic store-buffer litmus observes the
//! miss-miss outcome that the C++ memory model forbids for SC ops. This is
//! why the latch-over-Waiter model expresses its Dekker recheck edges with
//! RMW reads (which loom does bind to the latest value in modification
//! order). If this test ever FAILS (loom stops producing the weak outcome),
//! loom has gained real SC support and the Dekker model can drop the RMW
//! substitution.
#![cfg(loom)]

use loom::sync::atomic::{AtomicI32, Ordering};
use loom::sync::Arc;
use loom::thread;

#[test]
fn loom_admits_sc_store_buffer_weak_outcome() {
    let weak_seen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let weak_in_model = std::sync::Arc::clone(&weak_seen);
    loom::model(move || {
        let a = Arc::new(AtomicI32::new(0));
        let b = Arc::new(AtomicI32::new(0));
        let (a1, b1) = (Arc::clone(&a), Arc::clone(&b));
        let t = thread::spawn(move || {
            a1.store(1, Ordering::SeqCst);
            b1.load(Ordering::SeqCst)
        });
        b.store(1, Ordering::SeqCst);
        let ra = a.load(Ordering::SeqCst);
        let rb = t.join().unwrap();
        if ra == 0 && rb == 0 {
            weak_in_model.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    });
    assert!(
        weak_seen.load(std::sync::atomic::Ordering::SeqCst),
        "loom now forbids the SC store-buffer weak outcome: real SC support \
         landed; simplify tests/loom.rs's Dekker model to plain SC loads"
    );
}
