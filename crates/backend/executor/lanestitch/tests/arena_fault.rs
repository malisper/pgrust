// WAVE-9 WS-AG rung 0: the emit-arena fault leg (leg-3a gap closure).
//
// Lives in its OWN integration-test binary: the fault lever is process-
// global, and every other lanestitch test binary compiles bodies
// concurrently — a shared-process lever would make their refusal asserts
// flaky. One binary = one process = no cross-test interference (and no
// thread_local lever, keeping the TLS census at zero — standing law 8).
//
// The leg proves: with the master kill switch AND the family knob both
// still armed, an arena-stage refusal lands the compile on None (the
// caller's fail-open floor — for the DML chain, the DmlInsertOp portable
// host), and clearing the fault restores compilability at THIS layer (the
// E4 refused-compile caching is the CALLER's cache posture, decided and
// recorded in notes/se-wave9-ag.md — lanestitch itself never latches).

use lanestitch::{Program, Step, StitchedRowChain};

#[test]
fn arena_fault_refuses_compile_without_the_master_kill_exit() {
    let mut prog = Program::new();
    prog.steps.push(Step::ProtocolCall { call: 9905 });
    prog.steps.push(Step::NextRow);
    prog.steps.push(Step::ProtocolCall { call: 9906 });

    if !lanestitch::available() {
        // Off-arch / master-killed environment: nothing to prove here (the
        // master-kill exit is the wave-7-covered refusal, not this leg's).
        assert!(StitchedRowChain::compile_for_parity(&prog).is_none());
        return;
    }

    // Baseline: the shape compiles on this hardware.
    assert!(
        StitchedRowChain::compile_for_parity(&prog).is_some(),
        "baseline compile must succeed before the fault leg"
    );

    // Fault the arena stage. Both knobs still read armed — the refusal is
    // the INSTALL step's, exactly an exhausted-arena landing.
    lanestitch::_rowchain_arena_fault_set_for_tests(true);
    assert!(lanestitch::available(), "master availability must stay armed under the fault");
    assert!(
        StitchedRowChain::compile_for_parity(&prog).is_none(),
        "arena fault must land the compile on None (fail-open)"
    );

    // Clearing the fault restores compilability: lanestitch itself never
    // latches a refusal (caller caches are the E4 decision's domain).
    lanestitch::_rowchain_arena_fault_set_for_tests(false);
    assert!(
        StitchedRowChain::compile_for_parity(&prog).is_some(),
        "clearing the fault must restore compilability at the lanestitch layer"
    );
}
