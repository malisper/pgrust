use std::cell::Cell;
use std::sync::RwLock;

use elog::ereport;
use guc_tables::{vars, GucVarAccessors};
use types_error::{PgResult, DEBUG1, ERRCODE_SYNTAX_ERROR, LOG};

use crate::process::loc;

const PLUGIN_PREFIX: &str = "$libdir/plugins/";

static SHARED_PRELOAD_LIBRARIES: RwLock<Option<String>> = RwLock::new(None);
static PRELOAD_CONTRIB: RwLock<Option<String>> = RwLock::new(None);

guc_tables::session_guc_string!(
    SESSION_PRELOAD_LIBRARIES,
    session_preload_libraries_string_get,
    session_preload_libraries_string_set,
    Some("")
);
guc_tables::session_guc_string!(
    LOCAL_PRELOAD_LIBRARIES,
    local_preload_libraries_string_get,
    local_preload_libraries_string_set,
    Some("")
);

thread_local! {
    static IN_PROGRESS: Cell<bool> = const { Cell::new(false) };
    static DONE: Cell<bool> = const { Cell::new(false) };
    static SHMEM_REQUESTS_IN_PROGRESS: Cell<bool> = const { Cell::new(false) };
}

fn string_get(cell: &'static RwLock<Option<String>>) -> Option<String> {
    match &*cell.read().unwrap() {
        Some(s) => Some(s.clone()),
        None => Some(String::new()),
    }
}

// load_libraries (miscinit.c): names resolve through the dfmgr
// builtin-library registry (no dlopen); an unknown name errors like C's
// "could not access file".
fn load_libraries(libraries: Option<&str>, gucname: &str, restricted: bool) -> PgResult<()> {
    let Some(list) = libraries else { return Ok(()) };
    if list.is_empty() {
        return Ok(()); // nothing to do
    }
    // SplitDirectoriesString(rawstring, ',', &elemlist): quoted names may
    // embed commas ('""' collapses to '"'), unquoted names trim C-locale
    // whitespace only, and empty items are a syntax error that C reports at
    // LOG and then skips the whole list (postgres:18.3:
    // shared_preload_libraries='foo,,bar' -> LOG: invalid list syntax in
    // parameter "shared_preload_libraries"; server starts with none loaded;
    // '"a,b"' -> one item, FATAL: could not access file "a,b").
    let Ok(elemlist) = pg_string::split_directories_string(list, b',') else {
        return ereport(LOG)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("invalid list syntax in parameter \"{gucname}\""))
            .finish(loc(1869, "load_libraries"));
    };
    for name in &elemlist {
        // C's SplitDirectoriesString canonicalize_path()s each name.
        let mut name = pg_path::canonicalize_path(name);
        if restricted && pg_path::first_dir_separator(&name).is_none() {
            name = format!("{PLUGIN_PREFIX}{name}");
        }
        dfmgr::load_file(&name, restricted)?;
        ereport(DEBUG1)
            .errmsg_internal(format!("loaded library \"{name}\""))
            .finish(loc(1890, "load_libraries"))?;
    }
    Ok(())
}

pub fn process_shared_preload_libraries() -> PgResult<()> {
    IN_PROGRESS.set(true);
    let r = load_libraries(
        string_get(&SHARED_PRELOAD_LIBRARIES).as_deref(),
        "shared_preload_libraries",
        false,
    );
    IN_PROGRESS.set(false);
    r?;
    DONE.set(true);
    Ok(())
}

// Compiled-in-contrib boot dispatch (hook-surface.md section 6 open Q2):
// `preload_contrib` names key the dfmgr builtin-library registry directly —
// no probin/dlopen indirection exists, so this is a plain name lookup +
// pg_init, run once, single-threaded, before any backend thread spawns
// (the same window `process_shared_preload_libraries` runs in).
pub fn process_preload_contrib() -> PgResult<()> {
    let Some(list) = string_get(&PRELOAD_CONTRIB) else { return Ok(()) };
    // Same boot window as shared_preload_libraries: a pg_init loaded here may
    // install hooks/shmem exactly as if preloaded.
    IN_PROGRESS.set(true);
    let r = load_libraries(Some(&list), "preload_contrib", false);
    IN_PROGRESS.set(false);
    r
}

pub fn process_shared_preload_libraries_done() -> bool {
    DONE.get()
}

/// `process_shared_preload_libraries_in_progress` (miscadmin.h).
pub fn process_shared_preload_libraries_in_progress() -> bool {
    IN_PROGRESS.get()
}

/// The C global is a plain writable `bool`; this is its store, for callers
/// (and their tests) that stand in for process_shared_preload_libraries.
pub fn set_process_shared_preload_libraries_in_progress(value: bool) {
    IN_PROGRESS.set(value);
}

pub fn process_session_preload_libraries() -> PgResult<()> {
    load_libraries(
        session_preload_libraries_string_get().as_deref(),
        "session_preload_libraries",
        false,
    )?;
    load_libraries(
        local_preload_libraries_string_get().as_deref(),
        "local_preload_libraries",
        true,
    )?;
    Ok(())
}

/// `shmem_request_hook_type` (miscadmin.h:533).
pub type ShmemRequestHook = fn() -> PgResult<()>;

// shmem_request_hook (miscinit.c:1841). C keeps one pointer and every
// library's _PG_init chains onto it (prev = shmem_request_hook;
// shmem_request_hook = mine; mine calls prev first, pg_stat_statements.c:
// 470-471/497-498), so the hooks run in registration order; the chain is
// kept as that list here (the centralization exec_hooks applies to the
// executor taps). Written only from a _PG_init under
// shared_preload_libraries — the single-threaded boot window.
static SHMEM_REQUEST_HOOKS: RwLock<Vec<ShmemRequestHook>> = RwLock::new(Vec::new());

/// `shmem_request_hook = hook` in a library's `_PG_init`.
pub fn register_shmem_request_hook(hook: ShmemRequestHook) {
    SHMEM_REQUEST_HOOKS.write().unwrap().push(hook);
}

/// `process_shmem_requests_in_progress` (miscadmin.h:532): raised only while
/// process_shmem_requests runs the hooks — RequestAddinShmemSpace
/// (ipci.c:76) and RequestNamedLWLockTranche (lwlock.c:686) refuse outside it.
pub fn process_shmem_requests_in_progress() -> bool {
    SHMEM_REQUESTS_IN_PROGRESS.get()
}

/// process_shmem_requests (miscinit.c:1931-1937).
pub fn process_shmem_requests() -> PgResult<()> {
    SHMEM_REQUESTS_IN_PROGRESS.set(true);
    let r = SHMEM_REQUEST_HOOKS
        .read()
        .unwrap()
        .iter()
        .try_for_each(|hook| hook());
    SHMEM_REQUESTS_IN_PROGRESS.set(false);
    r
}

pub(crate) fn install_preload_guc_vars() {
    vars::shared_preload_libraries_string.install(GucVarAccessors {
        get: || string_get(&SHARED_PRELOAD_LIBRARIES),
        set: |v| *SHARED_PRELOAD_LIBRARIES.write().unwrap() = v,
    });
    vars::session_preload_libraries_string.install(GucVarAccessors {
        get: session_preload_libraries_string_get,
        set: session_preload_libraries_string_set,
    });
    vars::local_preload_libraries_string.install(GucVarAccessors {
        get: local_preload_libraries_string_get,
        set: local_preload_libraries_string_set,
    });
    vars::preload_contrib_string.install(GucVarAccessors {
        get: || string_get(&PRELOAD_CONTRIB),
        set: |v| *PRELOAD_CONTRIB.write().unwrap() = v,
    });
    vars::Dynamic_library_path.install(GucVarAccessors {
        get: dfmgr::dynamic_library_path_get,
        set: dfmgr::dynamic_library_path_set,
    });
}
