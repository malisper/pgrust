// The P5 W0 toolchain-validation smoke (docs/design/dst-and-wasm.md §5
// blocker table, top row): proves catch_unwind actually CATCHES on
// wasm32-wasip1 under a Wasm exception-handling runtime — the property the
// whole elog error architecture (ERROR/FATAL longjmp-equivalents,
// proc_exit-as-panic) stands on. Exit 0 = unwind caught + resumed; any
// abort/trap means panic=unwind is not real on this toolchain+runtime pair.

use std::panic;

fn boom(depth: u32) {
    if depth == 0 {
        panic!("wasm unwind smoke: intentional panic");
    }
    boom(depth - 1);
}

fn main() {
    // A Drop that must run during the unwind (destructors are the other half
    // of the panic=unwind contract).
    struct DropWitness<'a>(&'a std::cell::Cell<bool>);
    impl Drop for DropWitness<'_> {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    let dropped = std::cell::Cell::new(false);
    let caught = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        let _w = DropWitness(&dropped);
        boom(4);
    }));

    match caught {
        Err(payload) => {
            let msg = payload
                .downcast_ref::<&'static str>()
                .copied()
                .unwrap_or("<non-str payload>");
            assert!(dropped.get(), "Drop did not run during unwind");
            println!("CAUGHT: {msg}");
            println!("VERDICT: unwind-smoke PASS");
        }
        Ok(()) => {
            println!("VERDICT: unwind-smoke FAIL (closure returned normally)");
            std::process::exit(1);
        }
    }
}
