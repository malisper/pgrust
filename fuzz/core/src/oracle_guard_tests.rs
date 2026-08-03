//! MUST-FAIL CONTROLS for the oracle-serialization runtime holder check
//! (csrc/pg_oracle_guard.c + the `pgf_oracle_guard_violation` hook in
//! lib.rs). Charter: harness-detection-power law — an enforcement plane
//! that has never been seen to fire is presumed dead. This test drives the
//! check in BOTH modes through `pg_oracle_guard_probe`, a stateless C
//! entry instrumented exactly like the real oracle entries
//! (PG_ORACLE_GUARD_CHECK(__func__) first line).
//!
//! ONE test on purpose: the violation trap is process-global, so the
//! panic-path phase (trap disarmed) and the trap-mode phases must never
//! interleave with each other across libtest threads.

// "C-unwind": the violation hook panics; the unwind must be DEFINED through
// this C frame for the panic-path phase below (extern "C" would abort at
// the boundary instead of propagating).
extern "C-unwind" {
    fn pg_oracle_guard_probe();
}

#[test]
fn oracle_guard_holder_check_fires() {
    // Phase 1 — panic path (trap disarmed): the hook must PANIC and the
    // panic must name the C entry. Requires unwinding through the C probe
    // frame ("C-unwind" above; the cc-built objects carry unwind tables on
    // every campaign platform). If a platform ever ships without them, the
    // runtime aborts at the boundary instead — louder, not silent, and
    // this test is the place that would surface it.
    let err = std::panic::catch_unwind(|| unsafe { pg_oracle_guard_probe() })
        .expect_err("unguarded oracle entry must panic");
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(
        msg.contains("ORACLE GUARD VIOLATION") && msg.contains("pg_oracle_guard_probe"),
        "panic must name the violation and the C entry, got: {msg:?}"
    );

    // Phase 2 — trap mode, three directions.
    crate::oracle_guard_trap_arm();

    // 2a (must-fail): no oracle_serial() on this thread — must record.
    unsafe { pg_oracle_guard_probe() };

    // 2b (control): the same call under the guard is clean.
    {
        let _g = crate::c_oracle_serial();
        unsafe { pg_oracle_guard_probe() };
    }

    // 2c (cross-thread hold): guard held HERE, entry over THERE — the
    // nodesfam_diff_tests.rs pattern this check exists to catch. Mutual
    // exclusion technically holds (we block in join), but the
    // holder-thread invariant does not — the check must fire.
    {
        let _g = crate::c_oracle_serial();
        std::thread::spawn(|| unsafe { pg_oracle_guard_probe() })
            .join()
            .expect("probe thread");
    }

    let fired = crate::oracle_guard_trap_disarm();
    assert_eq!(
        fired,
        vec![
            "pg_oracle_guard_probe".to_string(),
            "pg_oracle_guard_probe".to_string()
        ],
        "holder check must fire on the unguarded and cross-thread entries \
         (and ONLY those): {fired:?}"
    );
}
