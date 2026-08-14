use std::cell::Cell;
use std::sync::RwLock;

use elog::ereport;
use guc_tables::{vars, GucVarAccessors};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_SYNTAX_ERROR, ERROR, LOG,
};

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

// Restricted local_preload_libraries: only $libdir/plugins/<basename>.
fn check_restricted_library_name(name: &str) -> PgResult<()> {
    if !name.starts_with(PLUGIN_PREFIX)
        || pg_path::first_dir_separator(&name[PLUGIN_PREFIX.len()..]).is_some()
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("access to library \"{name}\" is not allowed"))
            .finish(ErrorLocation::new(
                "src/backend/utils/fmgr/dfmgr.c",
                525,
                "check_restricted_library_name",
            ));
    }
    Ok(())
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
        if restricted {
            check_restricted_library_name(&name)?;
        }
        dfmgr::load_file(&name)?;
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
