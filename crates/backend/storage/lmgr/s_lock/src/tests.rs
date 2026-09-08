use s_lock_seams::SpinDelayStatus;

#[test]
fn spins_per_delay_adapts_like_c() {
    super::set_spins_per_delay(100);
    let no_delay = SpinDelayStatus::new("f", 1, "t");
    super::finish_spin_delay(&no_delay);
    assert_eq!(super::update_spins_per_delay(100), (100 * 15 + 200) / 16);

    let delayed = SpinDelayStatus { cur_delay: 1000, ..SpinDelayStatus::new("f", 1, "t") };
    super::set_spins_per_delay(100);
    super::finish_spin_delay(&delayed);
    assert_eq!(super::update_spins_per_delay(100), (100 * 15 + 99) / 16);
}

#[test]
fn perform_spin_delay_counts_spins() {
    super::set_spins_per_delay(1000);
    let mut st = SpinDelayStatus::new("f", 1, "t");
    super::perform_spin_delay(&mut st);
    assert_eq!(st.spins, 1);
    assert_eq!(st.delays, 0);
}

// audit-18.6 w2-041 (fp-lmgr-b1): past NUM_DELAYS, perform_spin_delay is
// s_lock_stuck (s_lock.c:78-93) = elog(PANIC, "stuck spinlock detected at
// %s, %s:%d", func, file, line): the message goes through the error
// subsystem (log destinations, PANIC level) and the PANIC arm of errfinish
// ends the backend thread with C abort()'s rendering, PanicExitThread --
// never a bare Rust panic with a string payload. The check fires before the
// delay sleeps (s_lock.c:143-146), so a status parked at the threshold
// trips it on the first call.
static STUCK_REPORTS: std::sync::Mutex<Vec<(types_error::ErrorLevel, String, Option<String>)>> =
    std::sync::Mutex::new(Vec::new());

#[test]
fn stuck_spinlock_is_elog_panic_not_a_bare_panic() {
    elog_seams::ereport_msg::set(|level, msg, detail| {
        STUCK_REPORTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((level, msg, detail));
        // errfinish's PANIC arm: the unwind that ends the backend thread.
        std::panic::panic_any(types_error::PanicExitThread)
    });
    super::set_spins_per_delay(100);
    let mut st = SpinDelayStatus::new("s_lock.c", 89, "s_lock_stuck");
    st.spins = 99;
    st.delays = super::NUM_DELAYS;

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        super::perform_spin_delay(&mut st)
    }));
    let payload = outcome.err().expect("a stuck spinlock must not return");
    assert!(
        payload.is::<types_error::PanicExitThread>(),
        "stuck spinlock unwinds with PanicExitThread (elog PANIC), not a bare Rust panic: {:?}",
        payload.downcast_ref::<String>()
    );
    let reports = STUCK_REPORTS.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(
        reports.as_slice(),
        [(
            types_error::PANIC,
            "stuck spinlock detected at s_lock_stuck, s_lock.c:89".to_string(),
            None
        )]
    );
}
