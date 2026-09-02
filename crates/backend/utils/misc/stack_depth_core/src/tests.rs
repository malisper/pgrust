use super::*;

// The test's own read of the machine stack pointer, independent of the
// guard's probe: ASan's fake stack (detect_stack_use_after_return) relocates
// address-taken locals to heap pools but never the stack pointer itself.
#[inline(never)]
fn machine_sp_here() -> usize {
    let sp: usize;
    #[cfg(target_arch = "aarch64")]
    // SAFETY: register-only read of sp; touches no memory, keeps flags.
    unsafe {
        core::arch::asm!("mov {}, sp", out(reg) sp, options(nomem, nostack, preserves_flags));
    }
    #[cfg(target_arch = "x86_64")]
    // SAFETY: register-only read of rsp; touches no memory, keeps flags.
    unsafe {
        core::arch::asm!("mov {}, rsp", out(reg) sp, options(nomem, nostack, preserves_flags));
    }
    sp
}

// upstream c0bf1d89df29 (18.6): both halves of the depth probe must measure
// the machine stack, or under ASan's fake stack |base - probe| is garbage
// (100% of guard evaluations raised 54001 on the CI cluster). Run with
// ASAN_OPTIONS=detect_stack_use_after_return=1 to see the pre-fix failure.
#[test]
fn base_probe_reads_the_machine_stack() {
    let sp = machine_sp_here();
    let probe = current_stack_addr();
    assert!(
        sp.abs_diff(probe) < 16 * 1024,
        "base probe {probe:#x} is not on the machine stack (sp {sp:#x})"
    );
}

#[test]
fn guard_probe_reads_the_machine_stack() {
    let budget: isize = 1 << 20;
    let margin: usize = 64 * 1024;
    let sp = machine_sp_here();
    set_enforced_stack_budget_for_tests(budget);
    let within = budget as usize - margin;
    restore_stack_base(sp + within);
    assert!(
        !stack_is_too_deep(),
        "probe left the machine stack (base above sp {sp:#x})"
    );
    restore_stack_base(sp - within);
    assert!(
        !stack_is_too_deep(),
        "probe left the machine stack (base below sp {sp:#x})"
    );
    restore_stack_base(sp + budget as usize + margin);
    assert!(stack_is_too_deep());
    restore_stack_base(0);
}
