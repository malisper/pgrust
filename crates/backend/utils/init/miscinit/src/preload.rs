use std::cell::Cell;
use std::sync::RwLock;

use guc_tables::{vars, GucVarAccessors};
use types_error::PgResult;

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
fn load_libraries(libraries: Option<&str>, _gucname: &str) -> PgResult<()> {
    let Some(list) = libraries else { return Ok(()) };
    for item in list.split(',') {
        let name = item.trim().trim_matches('"');
        if !name.is_empty() {
            dfmgr::load_file(name)?;
        }
    }
    Ok(())
}

pub fn process_shared_preload_libraries() -> PgResult<()> {
    IN_PROGRESS.set(true);
    let r = load_libraries(
        string_get(&SHARED_PRELOAD_LIBRARIES).as_deref(),
        "shared_preload_libraries",
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
    let r = (|| {
        for item in list.split(',') {
            let name = item.trim().trim_matches('"');
            if !name.is_empty() {
                dfmgr::load_file(name)?;
            }
        }
        Ok(())
    })();
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

pub fn process_session_preload_libraries() -> PgResult<()> {
    load_libraries(
        session_preload_libraries_string_get().as_deref(),
        "session_preload_libraries",
    )?;
    load_libraries(
        local_preload_libraries_string_get().as_deref(),
        "local_preload_libraries",
    )?;
    Ok(())
}

// shmem_request_hook can only be set from a preloaded library; with the
// empty-list fast path live there is never a hook to run.
pub fn process_shmem_requests() -> PgResult<()> {
    SHMEM_REQUESTS_IN_PROGRESS.set(true);
    SHMEM_REQUESTS_IN_PROGRESS.set(false);
    Ok(())
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
}
