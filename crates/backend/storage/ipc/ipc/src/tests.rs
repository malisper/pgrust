use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::cell::RefCell;

thread_local! {
    static LOG: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

fn install() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        init_seams();
        init_small::init_seams();
        pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
    });
}

fn log(entry: &'static str) {
    LOG.with(|l| l.borrow_mut().push(entry));
}

fn take_log() -> Vec<&'static str> {
    LOG.with(|l| std::mem::take(&mut *l.borrow_mut()))
}

fn exit_code_of(f: impl FnOnce()) -> i32 {
    let payload = catch_unwind(AssertUnwindSafe(f)).unwrap_err();
    payload
        .downcast_ref::<ProcExitThread>()
        .expect("unwind payload is ProcExitThread")
        .code
}

#[test]
fn proc_exit_runs_stages_in_c_order_lifo() {
    install();
    init_small::globals::SetMyProcPid(4242);
    let _ = take_log();

    on_proc_exit(|_, arg| log(if arg == 1 { "proc1" } else { "proc2" }), 1);
    on_proc_exit(|_, arg| log(if arg == 1 { "proc1" } else { "proc2" }), 2);
    on_shmem_exit(|_, _| log("shmem1"), 0);
    on_shmem_exit(|_, _| log("shmem2"), 0);
    before_shmem_exit(
        |code, _| {
            log(if code == 7 { "before1(code7)" } else { "before1" });
            Ok(())
        },
        Datum::from_i32(0),
    )
    .unwrap();
    before_shmem_exit(
        |_, _| {
            log("before2");
            Ok(())
        },
        Datum::from_i32(0),
    )
    .unwrap();

    let code = exit_code_of(|| proc_exit(7, 4242));
    assert_eq!(code, 7);
    assert_eq!(
        take_log(),
        vec!["before2", "before1(code7)", "shmem2", "shmem1", "proc2", "proc1"]
    );
    assert!(proc_exit_inprogress());
    assert!(!shmem_exit_inprogress());
    assert!(elog::config::proc_exit_inprogress());
    assert_eq!(init_small::globals::InterruptHoldoffCount(), 1);
    assert_eq!(init_small::globals::CritSectionCount(), 0);
    assert!(!init_small::globals::InterruptPending());
}

#[test]
fn proc_exit_on_wrong_thread_panics_like_c_child_process_check() {
    install();
    init_small::globals::SetMyProcPid(1111);
    let payload = catch_unwind(AssertUnwindSafe(|| proc_exit(0, 2222))).unwrap_err();
    let msg = payload.downcast_ref::<&str>().copied().unwrap_or_default();
    assert_eq!(msg, "proc_exit() called in child process");
}

#[test]
fn failing_before_callback_reenters_and_finishes_with_code_1() {
    install();
    init_small::globals::SetMyProcPid(5151);
    let _ = take_log();

    on_shmem_exit(|code, _| log(if code == 1 { "shmem(code1)" } else { "shmem" }), 0);
    before_shmem_exit(
        |_, _| {
            log("before-ok");
            Ok(())
        },
        Datum::from_i32(0),
    )
    .unwrap();
    before_shmem_exit(
        |_, _| {
            log("before-fail");
            Err(Box::new(PgError::error("exit callback exploded")))
        },
        Datum::from_i32(0),
    )
    .unwrap();

    let code = exit_code_of(|| proc_exit(0, 5151));
    assert_eq!(code, 1);
    assert_eq!(take_log(), vec!["before-fail", "before-ok", "shmem(code1)"]);
}

#[test]
fn cancel_before_shmem_exit_is_strict_lifo() {
    install();
    fn cb_a(_: i32, _: Datum) -> PgResult<()> {
        Ok(())
    }
    fn cb_b(_: i32, _: Datum) -> PgResult<()> {
        Ok(())
    }

    before_shmem_exit(cb_a, Datum::from_i32(1)).unwrap();
    before_shmem_exit(cb_b, Datum::from_i32(2)).unwrap();

    let err = cancel_before_shmem_exit(cb_a, Datum::from_i32(1)).unwrap_err();
    assert!(err.message.contains("is not the latest entry"), "{}", err.message);

    cancel_before_shmem_exit(cb_b, Datum::from_i32(2)).unwrap();
    cancel_before_shmem_exit(cb_a, Datum::from_i32(1)).unwrap();
    check_on_shmem_exit_lists_are_empty().unwrap();
}

#[test]
fn check_lists_empty_reports_c_messages() {
    install();
    on_shmem_exit(|_, _| {}, 0);
    let err = check_on_shmem_exit_lists_are_empty().unwrap_err();
    assert_eq!(err.message, "on_shmem_exit has been called prematurely");
    on_exit_reset();

    before_shmem_exit(|_, _| Ok(()), Datum::from_i32(0)).unwrap();
    let err = check_on_shmem_exit_lists_are_empty().unwrap_err();
    assert_eq!(err.message, "before_shmem_exit has been called prematurely");
    on_exit_reset();
    check_on_shmem_exit_lists_are_empty().unwrap();
}

#[test]
fn shmem_exit_alone_clears_inprogress_and_keeps_proc_lists() {
    install();
    let _ = take_log();
    on_proc_exit(|_, _| log("proc"), 0);
    on_shmem_exit(|_, _| log("shmem"), 0);
    before_shmem_exit(
        |_, _| {
            log(if shmem_exit_inprogress() { "before(inprogress)" } else { "before" });
            Ok(())
        },
        Datum::from_i32(0),
    )
    .unwrap();

    shmem_exit(0).unwrap();
    assert_eq!(take_log(), vec!["before(inprogress)", "shmem"]);
    assert!(!shmem_exit_inprogress());
    assert!(ipc_portal_seams::shmem_exit_inprogress::is_installed());
    assert!(!ipc_portal_seams::shmem_exit_inprogress::call());

    check_on_shmem_exit_lists_are_empty().unwrap();
    on_exit_reset();
}

#[test]
fn registration_overflow_is_fatal_and_unwinds() {
    install();
    init_small::globals::SetMyProcPid(6161);
    let result = catch_unwind(AssertUnwindSafe(|| {
        for _ in 0..=MAX_ON_EXITS {
            on_shmem_exit(|_, _| {}, 0);
        }
    }));
    assert!(result.is_err(), "21st registration must not return");
    assert!(proc_exit_inprogress());
}

#[test]
fn proc_exit_under_postmaster_defers_drain_to_thread_top() {
    install();
    init_small::globals::SetMyProcPid(8181);
    init_small::globals::SetIsUnderPostmaster(true);
    let _ = take_log();

    on_proc_exit(|_, _| log("proc"), 0);
    on_shmem_exit(|_, _| log("shmem"), 0);
    before_shmem_exit(
        |code, _| {
            log(if code == 5 { "before(code5)" } else { "before" });
            Ok(())
        },
        Datum::from_i32(0),
    )
    .unwrap();

    let code = exit_code_of(|| proc_exit(5, 8181));
    assert_eq!(code, 5);
    // Callbacks did NOT run at the raise point: the stack's Drop glue must
    // see live session state first (the cancelled-parallel-query ordering).
    assert_eq!(take_log(), Vec::<&str>::new());
    check_on_shmem_exit_lists_are_empty().unwrap_err();
    assert!(proc_exit_inprogress());

    assert_eq!(run_deferred_exit_callbacks(5), 5);
    assert_eq!(take_log(), vec!["before(code5)", "shmem", "proc"]);
    check_on_shmem_exit_lists_are_empty().unwrap();
    // Second call is a no-op: the deferred flag was consumed.
    assert_eq!(run_deferred_exit_callbacks(5), 5);
    init_small::globals::SetIsUnderPostmaster(false);
}

#[test]
fn deferred_drain_reentrant_proc_exit_continues_with_last_code() {
    install();
    init_small::globals::SetMyProcPid(9191);
    init_small::globals::SetIsUnderPostmaster(true);
    let _ = take_log();

    on_shmem_exit(|code, _| log(if code == 9 { "shmem(code9)" } else { "shmem" }), 0);
    before_shmem_exit(
        |_, _| {
            log("b-first");
            Ok(())
        },
        Datum::from_i32(0),
    )
    .unwrap();
    before_shmem_exit(
        |_, _| {
            log("reenter");
            // C's recursion arm: a proc_exit from inside a callback finishes
            // the remaining callbacks and the new code wins.
            proc_exit(9, init_small::globals::MyProcPid());
        },
        Datum::from_i32(0),
    )
    .unwrap();

    let code = exit_code_of(|| proc_exit(0, 9191));
    assert_eq!(code, 0);
    assert_eq!(take_log(), Vec::<&str>::new());

    assert_eq!(run_deferred_exit_callbacks(0), 9);
    assert_eq!(take_log(), vec!["reenter", "b-first", "shmem(code9)"]);
    check_on_shmem_exit_lists_are_empty().unwrap();
    init_small::globals::SetIsUnderPostmaster(false);
}

#[test]
fn exit_thread_raw_never_defers_a_drain() {
    install();
    init_small::globals::SetMyProcPid(9292);
    init_small::globals::SetIsUnderPostmaster(true);
    let _ = take_log();

    on_shmem_exit(|_, _| log("shmem"), 0);

    let payload = catch_unwind(AssertUnwindSafe(|| exit_thread_raw(2))).unwrap_err();
    assert_eq!(payload.downcast_ref::<ProcExitThread>().unwrap().code, 2);
    // quickdie contract: no callbacks, and nothing pending at the thread top.
    assert_eq!(run_deferred_exit_callbacks(2), 2);
    assert_eq!(take_log(), Vec::<&str>::new());
    check_on_shmem_exit_lists_are_empty().unwrap_err();
    on_exit_reset();
    init_small::globals::SetIsUnderPostmaster(false);
}

#[test]
fn seams_delegate_to_this_crate() {
    install();
    init_small::globals::SetMyProcPid(7171);
    let _ = take_log();
    ipc_seams::on_shmem_exit::call(|_, _| log("via-seam"), 0);
    ipc_seams::check_on_shmem_exit_lists_are_empty::call().unwrap_err();
    let code = exit_code_of(|| ipc_seams::proc_exit::call(3, 7171));
    assert_eq!(code, 3);
    assert_eq!(take_log(), vec!["via-seam"]);
}
