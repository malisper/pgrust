//! Runtime injection-point registry: the threaded-server port of C's
//! injection-point mechanism (utils/misc/injection_point.c plus the
//! error/notice/wait callbacks of src/test/modules/injection_points).
//!
//! What this is for: PostgreSQL's recovery TAP tests pause or fail the server
//! at named code spots ("injection points") to force rare timing windows.
//! A test attaches an action to a name over SQL (see the `injection_points`
//! contrib crate); ported code calls `injection_point("name")` at the same
//! spots C does. With nothing attached the call is one relaxed atomic load
//! and a not-taken branch — the registry ships inert, exactly like the seams
//! registry.
//!
//! DIVERGENCE from C: C compiles the call sites only under
//! --enable-injection-points and loads callbacks from a shared library. One
//! address space here, so the registry is a process-global and the three C
//! module callbacks (error/notice/wait) are built in. C's per-PID conditions
//! (injection_points_set_local) are not ported: every backend is a thread of
//! the one server process, and no test in our suite uses set_local.

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use pgsync::Mutex;

use condition_variable::{
    ConditionVariable, ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariablePrepareToSleep, ConditionVariableSleep,
};
use elog::ereport;
use types_error::{ErrorLocation, PgError, PgResult, NOTICE};

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Action {
    Error,
    Notice,
    Wait,
}

// Fast-path gate: number of currently attached points. Zero means every
// injection_point()/is_attached() call returns immediately.
static N_ATTACHED: AtomicUsize = AtomicUsize::new(0);

// name -> action. Cold: touched only by attach/detach and by call sites
// after the N_ATTACHED gate says something is attached.
pgsync::process_global! {
    static REGISTRY: Mutex<Vec<(String, Action)>> = Mutex::new(Vec::new());
}

// INJ_NAME_MAXLEN (injection_point.c:56).
const INJ_NAME_MAXLEN: usize = 64;

// MAX_INJECTION_POINTS (injection_point.c:74): C's shmem entry array is
// fixed-size, so attaching past it is an error (injection_point.c:324).
const MAX_INJECTION_POINTS: usize = 128;

// Wait machinery, mirroring the C module's InjectionPointSharedState:
// fixed wait slots (name + wakeup counter) plus one condition variable.
const INJ_MAX_WAIT: usize = 8;

#[derive(Clone, Default)]
struct WaitSlot {
    name: String, // empty = free
    wait_count: u32,
}

pgsync::process_global! {
    static WAIT_SLOTS: Mutex<[WaitSlot; INJ_MAX_WAIT]> =
        Mutex::new([const { WaitSlot { name: String::new(), wait_count: 0 } }; INJ_MAX_WAIT]);
}

static WAIT_POINT: ConditionVariable = ConditionVariable::new();

/// InjectionPointAttach + the module's action-name mapping
/// (injection_points.c:351). `action` is one of "error", "notice", "wait".
pub fn attach(name: &str, action: &str) -> PgResult<()> {
    // injection_point.c:285: names live in fixed INJ_NAME_MAXLEN (64) byte
    // shmem slots, so 64+ bytes is an error there and here.
    if name.len() >= INJ_NAME_MAXLEN {
        return Err(Box::new(PgError::error(format!(
            "injection point name {name} too long (maximum of {} characters)",
            INJ_NAME_MAXLEN - 1
        ))));
    }
    let action = match action {
        "error" => Action::Error,
        "notice" => Action::Notice,
        "wait" => Action::Wait,
        _ => {
            return Err(Box::new(PgError::error(format!(
                "incorrect action \"{action}\" for injection point creation"
            ))))
        }
    };
    let mut reg = REGISTRY.lock().unwrap();
    if reg.iter().any(|(n, _)| n == name) {
        return Err(Box::new(PgError::error(format!(
            "injection point \"{name}\" already defined"
        ))));
    }
    if reg.len() >= MAX_INJECTION_POINTS {
        return Err(Box::new(PgError::error("too many injection points")));
    }
    reg.push((name.to_string(), action));
    N_ATTACHED.store(reg.len(), Relaxed);
    Ok(())
}

/// InjectionPointDetach (returns false when the point was not attached; the
/// SQL wrapper turns that into an error, as C's module does).
pub fn detach(name: &str) -> bool {
    let mut reg = REGISTRY.lock().unwrap();
    let before = reg.len();
    reg.retain(|(n, _)| n != name);
    let found = reg.len() != before;
    N_ATTACHED.store(reg.len(), Relaxed);
    found
}

/// injection_points_wakeup (injection_points.c:462): bump the waiter's
/// counter and broadcast. Errors when no waiter of that name exists.
pub fn wakeup(name: &str) -> PgResult<()> {
    {
        let mut slots = WAIT_SLOTS.lock().unwrap();
        let Some(slot) = slots.iter_mut().find(|s| s.name == name) else {
            return Err(Box::new(PgError::error(format!(
                "could not find injection point {name} to wake up"
            ))));
        };
        slot.wait_count = slot.wait_count.wrapping_add(1);
    }
    ConditionVariableBroadcast(&WAIT_POINT);
    Ok(())
}

/// IS_INJECTION_POINT_ATTACHED.
#[inline]
pub fn is_attached(name: &str) -> bool {
    if N_ATTACHED.load(Relaxed) == 0 {
        return false;
    }
    is_attached_slow(name)
}

#[cold]
#[inline(never)]
fn is_attached_slow(name: &str) -> bool {
    REGISTRY.lock().unwrap().iter().any(|(n, _)| n == name)
}

/// INJECTION_POINT(name): run the attached action, if any.
#[inline]
pub fn injection_point(name: &str) -> PgResult<()> {
    if N_ATTACHED.load(Relaxed) == 0 {
        return Ok(());
    }
    injection_point_slow(name)
}

#[cold]
#[inline(never)]
fn injection_point_slow(name: &str) -> PgResult<()> {
    let action = {
        let reg = REGISTRY.lock().unwrap();
        reg.iter().find(|(n, _)| n == name).map(|(_, a)| *a)
    };
    match action {
        None => Ok(()),
        Some(Action::Error) => Err(Box::new(PgError::error(format!(
            "error triggered for injection point {name}"
        )))),
        Some(Action::Notice) => {
            ereport(NOTICE)
                .errmsg(format!("notice triggered for injection point {name}"))
                .finish(loc("injection_notice"))?;
            Ok(())
        }
        Some(Action::Wait) => wait(name),
    }
}

// injection_wait (injection_points.c:282): park on the condition variable
// until injection_points_wakeup() bumps our counter. The custom wait event
// carries the point's name, so pg_stat_activity shows
// wait_event_type='InjectionPoint', wait_event='<name>' while parked —
// PostgreSQL::Test::Cluster::wait_for_event() polls exactly that.
fn wait(name: &str) -> PgResult<()> {
    let wait_event = waitevent::custom::WaitEventInjectionPointNew(name)?;

    let (index, old_wait_count) = {
        let mut slots = WAIT_SLOTS.lock().unwrap();
        let Some(index) = slots.iter().position(|s| s.name.is_empty()) else {
            return Err(Box::new(PgError::error(format!(
                "could not find free slot for wait of injection point {name} "
            ))));
        };
        slots[index].name = name.to_string();
        (index, slots[index].wait_count)
    };

    let result = (|| -> PgResult<()> {
        ConditionVariablePrepareToSleep(&WAIT_POINT);
        loop {
            let new_wait_count = WAIT_SLOTS.lock().unwrap()[index].wait_count;
            if new_wait_count != old_wait_count {
                break;
            }
            ConditionVariableSleep(&WAIT_POINT, wait_event)?;
        }
        Ok(())
    })();

    // Unlike C (whose ERROR longjmp leaks the shmem slot; fine for its
    // short-lived test processes), always release the slot and the CV:
    // a leaked slot in a long-lived threaded server would eat one of the
    // eight wait slots forever.
    ConditionVariableCancelSleep();
    WAIT_SLOTS.lock().unwrap()[index].name.clear();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // The registry is process-global: tests that attach hold this so the
    // capacity test's full registry never starves a sibling's attach.
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn test_guard() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner())
    }

    // injection_point.c:74,324: MAX_INJECTION_POINTS (128) shmem slots; the
    // 129th distinct attach is elog(ERROR, "too many injection points").
    // Audit a186-candidate-fp-misc-injection_point-e61f4f024566df40b33a-1.
    #[test]
    fn too_many_injection_points_matches_c() {
        let _g = test_guard();
        let mut mine = Vec::new();
        loop {
            let n = REGISTRY.lock().unwrap().len();
            if n >= 128 {
                break;
            }
            let name = format!("crash-skips-cap-{}", mine.len());
            attach(&name, "notice").unwrap();
            mine.push(name);
        }
        let err = attach("crash-skips-cap-overflow", "notice").unwrap_err();
        assert_eq!(err.message(), "too many injection points");
        assert!(!is_attached("crash-skips-cap-overflow"));
        // Freeing one slot lets the next attach succeed, as C's generation
        // scan reuses a detached entry.
        assert!(detach(&mine.pop().unwrap()));
        attach("crash-skips-cap-overflow", "notice").unwrap();
        assert!(detach("crash-skips-cap-overflow"));
        for name in mine {
            assert!(detach(&name));
        }
    }

    #[test]
    fn attach_detach_lifecycle() {
        let _g = test_guard();
        assert!(!is_attached("crash-skips-test-point"));
        injection_point("crash-skips-test-point").unwrap();

        attach("crash-skips-test-point", "error").unwrap();
        assert!(is_attached("crash-skips-test-point"));
        // Duplicate attach errors, like C's InjectionPointAttach.
        assert!(attach("crash-skips-test-point", "notice").is_err());

        let err = injection_point("crash-skips-test-point").unwrap_err();
        assert!(
            err.to_string()
                .contains("error triggered for injection point crash-skips-test-point"),
            "unexpected message: {err}"
        );

        assert!(detach("crash-skips-test-point"));
        assert!(!detach("crash-skips-test-point"));
        assert!(!is_attached("crash-skips-test-point"));
        injection_point("crash-skips-test-point").unwrap();
    }

    #[test]
    fn bad_action_rejected() {
        assert!(attach("crash-skips-bad-action", "explode").is_err());
        assert!(!is_attached("crash-skips-bad-action"));
    }

    // injection_point.c:285-287. Audit
    // a186-candidate-fp-misc-injection_point-8a80633bd2916a715c32-1.
    #[test]
    fn name_length_limit_matches_c() {
        let _g = test_guard();
        let long = "x".repeat(64);
        let err = attach(&long, "notice").unwrap_err();
        assert_eq!(
            err.message(),
            format!("injection point name {long} too long (maximum of 63 characters)")
        );
        assert!(!is_attached(&long));
        let max = "y".repeat(63);
        attach(&max, "notice").unwrap();
        assert!(is_attached(&max));
        assert!(detach(&max));
    }

    #[test]
    fn wakeup_without_waiter_errors() {
        assert!(wakeup("crash-skips-nobody-waiting").is_err());
    }
}
