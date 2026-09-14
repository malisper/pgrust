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
//! Two tiers, as in C: the process-wide registry (C's shared-memory
//! `ActiveInjectionPoints` array, injection_point.c:79-84) and a
//! backend-local cache of loaded entries (C's `InjectionPointCache` HTAB,
//! injection_point.c:90-104 — one per backend, here one per backend thread).
//! `injection_point()` / `is_attached()` / `load()` go through the cache
//! refresh (InjectionPointCacheRefresh, injection_point.c:427-520): a cached
//! entry stays valid while its registry generation is unchanged, a stale one
//! is dropped and reloaded, and a refresh that finds nothing attached at all
//! destroys the cache. `injection_point_cached()` (InjectionPointCached,
//! injection_point.c:562-573) reads ONLY the cache — that is its point: it
//! runs where the registry lock or an allocation is forbidden, and it keeps
//! firing a detached point until the next refresh, exactly as C does.
//!
//! DIVERGENCE from C: C compiles the call sites only under
//! --enable-injection-points and loads callbacks from a shared library. One
//! address space here, so the registry is a process-global and the three C
//! module callbacks (error/notice/wait) are built in. C's per-PID conditions
//! (injection_points_set_local) are not ported: every backend is a thread of
//! the one server process, and no test in our suite uses set_local.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
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

// One registry entry: C's InjectionPointEntry (injection_point.c:59-77)
// without the library/function/private_data bytes (callbacks are built in).
// `generation` is the entry's identity for the backend-local cache: C bumps
// a per-slot counter on attach and detach (injection_point.c:346, :379), so a
// cached entry is valid iff the slot's generation still matches (:460-472).
// A process-wide counter gives every attach a fresh value, which is the same
// contract (detach + re-attach, same slot or not, never matches a stale
// cache entry).
struct Entry {
    name: String,
    action: Action,
    generation: u64,
}

// Fast-path gate. Zero means every injection_point()/is_attached() call
// returns immediately. Low half: number of attached points (C's max_inuse
// gate, injection_point.c:445). High half: number of backend threads whose
// local cache is non-empty — C destroys such a cache the next time a refresh
// finds max_inuse == 0 (:447-452), so a thread holding cached entries must
// still take the slow path after the last detach, to do that destruction.
// u64 on every target: the high half needs bit 32 (wasm32 has a 4-byte usize).
static GATE: AtomicU64 = AtomicU64::new(0);
const CACHE_HOLDER: u64 = 1 << 32;
const ATTACHED_MASK: u64 = CACHE_HOLDER - 1;

// Source of entry generations (see Entry::generation).
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

// name -> action. Cold: touched only by attach/detach and by call sites
// after the GATE says something is attached or cached.
pgsync::process_global! {
    static REGISTRY: Mutex<Vec<Entry>> = Mutex::new(Vec::new());
}

// InjectionPointCacheEntry (injection_point.c:90-104): the backend-local
// copy of a registry entry plus the generation it was loaded under.
struct CacheEntry {
    name: String,
    action: Action,
    generation: u64,
}

// `static HTAB *InjectionPointCache` (injection_point.c:106): one per backend
// in C, one per backend thread here. Lives for the thread's life (C: in
// TopMemoryContext), consulted only on the slow path.
thread_local! {
    static CACHE: RefCell<Vec<CacheEntry>> = const { RefCell::new(Vec::new()) };
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
    if reg.iter().any(|e| e.name == name) {
        return Err(Box::new(PgError::error(format!(
            "injection point \"{name}\" already defined"
        ))));
    }
    if reg.len() >= MAX_INJECTION_POINTS {
        return Err(Box::new(PgError::error("too many injection points")));
    }
    reg.push(Entry {
        name: name.to_string(),
        action,
        generation: NEXT_GENERATION.fetch_add(1, Relaxed),
    });
    GATE.fetch_add(1, Relaxed);
    Ok(())
}

/// InjectionPointDetach (returns false when the point was not attached; the
/// SQL wrapper turns that into an error, as C's module does).
pub fn detach(name: &str) -> bool {
    let mut reg = REGISTRY.lock().unwrap();
    let before = reg.len();
    reg.retain(|e| e.name != name);
    let removed = before - reg.len();
    GATE.fetch_sub(removed as u64, Relaxed);
    removed != 0
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

/// IS_INJECTION_POINT_ATTACHED (IsInjectionPointAttached,
/// injection_point.c:580-588: a cache refresh that finds the point).
#[inline]
pub fn is_attached(name: &str) -> bool {
    if GATE.load(Relaxed) == 0 {
        return false;
    }
    is_attached_slow(name)
}

#[cold]
#[inline(never)]
fn is_attached_slow(name: &str) -> bool {
    cache_refresh(name).is_some()
}

/// INJECTION_POINT_LOAD(name) (InjectionPointLoad, injection_point.c:532-539):
/// pull the point into the backend-local cache ahead of a code path where
/// the refresh itself must not run (C: no allocation in critical sections),
/// so that `injection_point_cached` can fire it there. Nothing attached under
/// that name is not an error.
pub fn load(name: &str) {
    if GATE.load(Relaxed) == 0 {
        return;
    }
    cache_refresh(name);
}

/// INJECTION_POINT(name): run the attached action, if any (InjectionPointRun,
/// injection_point.c:545-556, through the cache refresh).
#[inline]
pub fn injection_point(name: &str) -> PgResult<()> {
    if GATE.load(Relaxed) == 0 {
        return Ok(());
    }
    injection_point_slow(name)
}

#[cold]
#[inline(never)]
fn injection_point_slow(name: &str) -> PgResult<()> {
    match cache_refresh(name) {
        None => Ok(()),
        Some(action) => run_action(name, action, None),
    }
}

/// INJECTION_POINT_CACHED(name, arg) (InjectionPointCached,
/// injection_point.c:562-573): run the action from the backend-local cache
/// ONLY — no registry consult, so a point that was never loaded (or whose
/// cache was destroyed by a refresh) does nothing, and a detached one keeps
/// firing until the next refresh. `arg` is the module callbacks' optional
/// argument (injection_points.c:233/251: appended as " (arg)").
pub fn injection_point_cached(name: &str, arg: Option<&str>) -> PgResult<()> {
    // injection_point_cache_get (injection_point.c:207-225).
    let action = CACHE.with(|c| c.borrow().iter().find(|e| e.name == name).map(|e| e.action));
    match action {
        None => Ok(()),
        Some(action) => run_action(name, action, arg),
    }
}

// InjectionPointCacheRefresh (injection_point.c:427-520): validate or
// (re)load the backend-local cache entry for `name`, returning its action.
#[cold]
#[inline(never)]
fn cache_refresh(name: &str) -> Option<Action> {
    // :445-454: nothing attached at all -> destroy the whole cache.
    if GATE.load(Relaxed) & ATTACHED_MASK == 0 {
        cache_clear();
        return None;
    }

    // :460-472: a cached entry is still good iff its generation matches the
    // live registry entry; otherwise drop it and fall through to the search.
    let cached = CACHE.with(|c| {
        c.borrow()
            .iter()
            .find(|e| e.name == name)
            .map(|e| (e.action, e.generation))
    });
    let live = {
        let reg = REGISTRY.lock().unwrap();
        reg.iter()
            .find(|e| e.name == name)
            .map(|e| (e.action, e.generation))
    };
    if let Some((action, generation)) = cached {
        if live.is_some_and(|(_, g)| g == generation) {
            return Some(action);
        }
        cache_remove(name);
    }

    // :482-519: search the registry and load a hit into the cache.
    let (action, generation) = live?;
    cache_add(name, action, generation);
    Some(action)
}

// injection_point_cache_add (injection_point.c:113-153).
fn cache_add(name: &str, action: Action, generation: u64) {
    CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        debug_assert!(cache.iter().all(|e| e.name != name));
        if cache.is_empty() {
            GATE.fetch_add(CACHE_HOLDER, Relaxed);
        }
        cache.push(CacheEntry { name: name.to_string(), action, generation });
    });
}

// injection_point_cache_remove (injection_point.c:161-168).
fn cache_remove(name: &str) {
    CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        let before = cache.len();
        cache.retain(|e| e.name != name);
        debug_assert_eq!(cache.len() + 1, before);
        if cache.is_empty() && before != 0 {
            GATE.fetch_sub(CACHE_HOLDER, Relaxed);
        }
    });
}

// hash_destroy(InjectionPointCache) (injection_point.c:449-451).
fn cache_clear() {
    CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        if !cache.is_empty() {
            cache.clear();
            GATE.fetch_sub(CACHE_HOLDER, Relaxed);
        }
    });
}

// The three built-in module callbacks (injection_points.c:222-256,
// injection_wait :282): `arg` is the callbacks' optional text argument.
fn run_action(name: &str, action: Action, arg: Option<&str>) -> PgResult<()> {
    match action {
        Action::Error => Err(Box::new(PgError::error(match arg {
            Some(arg) => format!("error triggered for injection point {name} ({arg})"),
            None => format!("error triggered for injection point {name}"),
        }))),
        Action::Notice => {
            ereport(NOTICE)
                .errmsg(match arg {
                    Some(arg) => format!("notice triggered for injection point {name} ({arg})"),
                    None => format!("notice triggered for injection point {name}"),
                })
                .finish(loc("injection_notice"))?;
            Ok(())
        }
        Action::Wait => wait(name),
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

    // injection_point.c:532-539 InjectionPointLoad and :562-573
    // InjectionPointCached over the backend-local cache (:427-520 refresh).
    // Audit a186-candidate-fp-misc-injection_point-001d0c6d8af24b2ac224-1
    // and -6d7506a0eac1ac49d35a-1. The error action is the observable one
    // here (a NOTICE needs a session to land in).
    #[test]
    fn load_and_cached_match_c() {
        let _g = test_guard();
        let name = "crash-skips-load-cached";
        // Nothing in cache and nothing attached: both are no-ops.
        injection_point_cached(name, None).unwrap();
        load(name);
        injection_point_cached(name, None).unwrap();

        attach(name, "error").unwrap();
        // Attach alone does not populate this backend's cache (:562: cache
        // only), the load does.
        injection_point_cached(name, None).unwrap();
        load(name);
        let err = injection_point_cached(name, None).unwrap_err();
        assert_eq!(err.message(), format!("error triggered for injection point {name}"));
        // injection_points.c:233: the optional argument is appended.
        let err = injection_point_cached(name, Some("foobar")).unwrap_err();
        assert_eq!(err.message(), format!("error triggered for injection point {name} (foobar)"));
        // InjectionPointRun serves from the same cache (:545-556).
        let err = injection_point(name).unwrap_err();
        assert_eq!(err.message(), format!("error triggered for injection point {name}"));

        // Detached: the cache-only path keeps firing (:562 never consults
        // the registry) ...
        assert!(detach(name));
        assert!(injection_point_cached(name, None).is_err());
        // ... until a refresh runs with nothing attached at all, which
        // destroys the cache (:445-452).
        injection_point(name).unwrap();
        injection_point_cached(name, None).unwrap();

        // Detach + re-attach changes the generation: the stale entry is
        // still what the cache-only path runs, and the next refresh drops
        // it and reloads the new action (:460-472).
        attach(name, "notice").unwrap();
        attach("crash-skips-load-cached-keepalive", "notice").unwrap();
        load(name);
        assert!(detach(name));
        attach(name, "error").unwrap();
        // Stale notice entry: cache-only sees no error.
        // (The NOTICE itself cannot be emitted without a session; the
        // generation check is what this arm pins.)
        let stale = CACHE.with(|c| {
            c.borrow().iter().find(|e| e.name == name).map(|e| e.action)
        });
        assert_eq!(stale, Some(Action::Notice));
        load(name);
        let err = injection_point_cached(name, None).unwrap_err();
        assert_eq!(err.message(), format!("error triggered for injection point {name}"));
        assert!(detach(name));
        assert!(detach("crash-skips-load-cached-keepalive"));
        // Leave this thread's cache empty for the siblings.
        injection_point(name).unwrap();
        assert!(CACHE.with(|c| c.borrow().is_empty()));
    }

    // C's InjectionPointRun also loads into the cache: a point run once is
    // then available to the cache-only path (the regress "runs from cache"
    // arm of expected/injection_points.out).
    #[test]
    fn run_populates_cache() {
        let _g = test_guard();
        let name = "crash-skips-run-populates";
        attach(name, "error").unwrap();
        injection_point_cached(name, None).unwrap();
        assert!(injection_point(name).is_err());
        assert!(injection_point_cached(name, None).is_err());
        assert!(is_attached(name));
        assert!(detach(name));
        assert!(!is_attached(name));
        assert!(CACHE.with(|c| c.borrow().is_empty()));
    }

    #[test]
    fn wakeup_without_waiter_errors() {
        assert!(wakeup("crash-skips-nobody-waiting").is_err());
    }
}
