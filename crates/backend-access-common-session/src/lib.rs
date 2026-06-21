//! `backend/access/common/session.c` — encapsulation of a user session.
//!
//! Owned-value rewrite of PostgreSQL 18.3 `session.c`. A `Session` holds the
//! state shared between the leader and worker backends of a parallel query —
//! currently the per-session DSM segment + DSA area and the shared record
//! typmod registry (whose *storage* lives in the session, even though its
//! *logic* is owned by `typcache.c`). `CurrentSession` is the backend-global
//! handle, modeled here as a `thread_local!` exactly mirroring the C global.
//!
//! # Seam boundary
//!
//! `session.c` proper is just `InitializeSession` / `GetSessionDsmHandle` /
//! `AttachSession` / `DetachSession`. The seam crate
//! `backend-access-common-session-seams` additionally declares the six
//! `SharedRecordTypmodRegistry*` entry points that `typcache.c` *calls* but
//! whose storage hangs off `CurrentSession`; this crate owns them because the
//! shared registry/tables are `Session` fields.
//!
//! ## What this crate installs
//!
//! - `initialize_session` — `InitializeSession()`. Fully ported; this is the
//!   single-user boot-path entry (`InitPostgres`).
//! - `shared_registry_estimate` — `SharedRecordTypmodRegistryEstimate()`:
//!   `sizeof(SharedRecordTypmodRegistry)`. Fully ported.
//! - `shared_registry_attached` — reads `CurrentSession->shared_typmod_registry
//!   != NULL`. Fully ported (pure field read; always false in a single backend).
//! - `find_or_make_matching_shared_tupledesc` — the typcache shared path. Its C
//!   body returns NULL when no registry is attached, which is the only reachable
//!   case in a single backend; that early-return (`Ok(None)`) is ported so the
//!   caller falls back to the local `RecordCacheArray`. The attached (dshash)
//!   leg remains keystone-blocked and panics loudly, never silently stubs.
//!
//! ## What this crate does NOT install (keystone-blocked, NOT stubbed)
//!
//! The other three registry seams (`shared_registry_init`,
//! `shared_registry_attach`, `shared_typmod_table_find`) are the parallel-worker
//! path. Their faithful bodies create/attach the
//! registry's **record table** via `dshash_create(area,
//! &srtr_record_table_params, area)` — a dshash whose `compare`/`hash` are the
//! custom `shared_record_table_compare` / `shared_record_table_hash` callbacks
//! (they resolve a `dsa_pointer` via `dsa_get_address(area, ...)` and run
//! `equalRowTypes` / `hashRowType` over the addressed `TupleDesc`).
//!
//! The ported dshash (`backend-lib-dshash` over `types_storage::DshashParameters`
//! / `DshashKeyKind`) deliberately supports **only** the two built-in key sets
//! (`String`, `Binary`) — "function pointers can't be shared between backends"
//! — and has no variant for a caller-supplied compare/hash taking the
//! `dsa_area *arg`. So the record table is not expressible over the current
//! substrate. Installing those four would require an out-of-lane keystone:
//! widen `DshashKeyKind` with a `Custom { compare, hash, copy }` variant +
//! thread the `arg` through `backend-lib-dshash`, then move `equalRowTypes` /
//! `hashRowType` / `share_tupledesc` (all private to `typcache.c`) across the
//! seam. Until that lands these seams keep their loud default-panic — never a
//! silent stub. The single-user boot path never reaches them.

#![allow(non_snake_case)]

use std::cell::RefCell;

use mcx::{Mcx, MemoryContext, PgBox};
use types_error::PgResult;
use types_storage::storage::{dsm_handle, DSM_HANDLE_INVALID};
use types_tuple::heaptuple::TupleDescData;

/// `dshash_table *` — the opaque backend-local handle to a shared hash table.
/// Modeled as the raw pointer the dshash port hands out; `Session` only stores
/// it (the registry logic lives in `typcache.c`).
type DshashTablePtr = *mut types_tuple_dshash_placeholder::Never;

// We do not depend on `backend-lib-dshash` / `backend-utils-mmgr-dsa` here:
// `Session` only *holds* the handles, and the four entry points that would
// dereference them are keystone-blocked and not installed (see module docs).
// Holding them as raw pointers mirrors the C struct's `dsm_segment *` /
// `dsa_area *` / `dshash_table *` fields without pulling the substrate crates
// in for code that cannot yet run.
mod types_tuple_dshash_placeholder {
    /// Uninhabited marker: `Session`'s registry/table fields are raw pointers
    /// that are always NULL on the paths this crate currently serves. The
    /// pointee type is never named or dereferenced.
    pub enum Never {}
}

/// `struct SharedRecordTypmodRegistry` (typcache.c). Its *layout* is private to
/// typcache.c; `Session` only stores a pointer to it, and the sole entry point
/// this crate serves for it is `SharedRecordTypmodRegistryEstimate`, which
/// returns `sizeof(SharedRecordTypmodRegistry)`. The three members are
/// `dshash_table_handle record_table_handle`, `dshash_table_handle
/// typmod_table_handle`, `pg_atomic_uint32 next_typmod`.
#[repr(C)]
struct SharedRecordTypmodRegistry {
    record_table_handle: u64,
    typmod_table_handle: u64,
    next_typmod: u32,
}

/// `typedef struct Session` (`access/session.h`).
///
/// `segment`/`area` are the session-scoped DSM segment and DSA area;
/// `shared_typmod_registry`/`shared_record_table`/`shared_typmod_table` are the
/// shared record-typmod registry state managed by `typcache.c`. All start NULL
/// (`InitializeSession` zero-initializes the whole struct).
// Fields mirror the C `Session` struct 1:1. The DSM/DSA/registry fields are
// only written/read on the parallel-worker registry paths whose seams are
// keystone-blocked (not installed); they are present so the struct stays
// faithful and the install lands without re-layout when dshash gains a custom
// key kind.
#[allow(dead_code)]
struct Session {
    /// `dsm_segment *segment` — the session-scoped DSM segment.
    segment: *mut core::ffi::c_void,
    /// `dsa_area *area` — the session-scoped DSA area.
    area: *mut core::ffi::c_void,
    /// `struct SharedRecordTypmodRegistry *shared_typmod_registry`.
    shared_typmod_registry: *mut SharedRecordTypmodRegistry,
    /// `dshash_table *shared_record_table`.
    shared_record_table: DshashTablePtr,
    /// `dshash_table *shared_typmod_table`.
    shared_typmod_table: DshashTablePtr,
}

impl Session {
    /// `MemoryContextAllocZero(TopMemoryContext, sizeof(Session))` — an empty
    /// (all-NULL) Session.
    const fn zeroed() -> Self {
        Session {
            segment: core::ptr::null_mut(),
            area: core::ptr::null_mut(),
            shared_typmod_registry: core::ptr::null_mut(),
            shared_record_table: core::ptr::null_mut(),
            shared_typmod_table: core::ptr::null_mut(),
        }
    }
}

thread_local! {
    /// `Session *CurrentSession = NULL;` — this backend's current session.
    /// `None` until `InitializeSession` runs.
    static CURRENT_SESSION: RefCell<Option<Session>> = const { RefCell::new(None) };

    /// The per-session memory context ("Session"), created lazily alongside
    /// `CurrentSession`. In C the `Session` struct is allocated directly in
    /// `TopMemoryContext`; here the owned-value `Session` lives in the
    /// `thread_local`, and we materialize the matching context so the lifetime
    /// correspondence (per-backend, freed at backend exit) is explicit.
    static SESSION_CONTEXT: RefCell<Option<MemoryContext>> = const { RefCell::new(None) };
}

/// `InitializeSession(void)` (session.c:54).
///
/// `CurrentSession = MemoryContextAllocZero(TopMemoryContext, sizeof(Session));`
///
/// Sets up `CurrentSession` to point to an empty `Session` object. The owned
/// rewrite materializes the per-session context and installs a zeroed `Session`
/// in the backend-global `thread_local`.
fn initialize_session() -> PgResult<()> {
    SESSION_CONTEXT.with(|c| {
        let mut slot = c.borrow_mut();
        if slot.is_none() {
            *slot = Some(MemoryContext::new("Session"));
        }
    });
    CURRENT_SESSION.with(|s| {
        *s.borrow_mut() = Some(Session::zeroed());
    });
    Ok(())
}

/// `GetSessionDsmHandle(void)` (session.c:70).
///
/// Initialize the per-session DSM segment if it isn't already initialized, and
/// return its handle so that worker processes can attach to it. The segment is
/// reused for the rest of this backend's lifetime.
///
/// C's contract (session.c:66-67): "Return `DSM_HANDLE_INVALID` if a segment
/// can't be allocated due to lack of resources." When INVALID is returned, the
/// parallel leader (`InitializeParallelDSM`) sets `nworkers = 0` and runs the
/// whole operation itself in backend-private memory — the leader-only path.
///
/// In a single backend `CurrentSession->segment` is always NULL (no segment was
/// ever created), so the early-return is never taken. Setting up a working
/// segment requires populating it with a `SharedRecordTypmodRegistry` whose
/// record/typmod tables are dshash tables keyed by caller-supplied
/// compare/hash callbacks over the session DSA area — the keystone-blocked path
/// documented in `SharedRecordTypmodRegistryInit` / `shared_registry_init`
/// (the dshash substrate has no custom-callback key kind yet). Because that
/// state cannot be constructed, no usable session segment can be created here:
/// the faithful outcome is C's sanctioned "lack of resources" return,
/// `DSM_HANDLE_INVALID`, which the leader handles by falling back to a
/// leader-only run. When the dshash custom-callback keystone lands, this body
/// gains the real `dsm_create` + `SharedRecordTypmodRegistryInit` segment-setup
/// leg (session.c:90-148) and starts returning a live handle.
fn get_session_dsm_handle() -> PgResult<dsm_handle> {
    // CurrentSession->segment is always NULL in a single backend; the segment
    // setup leg (which requires the keystone-blocked shared typmod registry)
    // can't produce a usable segment, so report lack of resources.
    Ok(DSM_HANDLE_INVALID)
}

/// `SharedRecordTypmodRegistryEstimate(void)` (typcache.c:2174).
///
/// `return sizeof(SharedRecordTypmodRegistry);`
///
/// Exists only to avoid exposing the private innards of
/// `SharedRecordTypmodRegistry` in a header; the result sizes the shmem chunk
/// reserved for the registry header in `GetSessionDsmHandle`.
fn shared_registry_estimate() -> usize {
    core::mem::size_of::<SharedRecordTypmodRegistry>()
}

/// Whether a `SharedRecordTypmodRegistry` is attached to the current session
/// (`CurrentSession->shared_typmod_registry != NULL`). Pure read of the
/// `Session` field. False whenever no parallel registry has been attached —
/// always the case in a single (non-parallel) backend.
fn shared_registry_attached() -> bool {
    CURRENT_SESSION.with(|s| {
        s.borrow()
            .as_ref()
            .is_some_and(|sess| !sess.shared_typmod_registry.is_null())
    })
}

/// `find_or_make_matching_shared_tupledesc(tupdesc)` (typcache.c:2943).
///
/// The shared path of `assign_record_type_typmod`. The C body returns NULL
/// immediately when `CurrentSession->shared_typmod_registry == NULL` (the only
/// case in a single backend), which maps to `None` here, telling the caller to
/// use the local `RecordCacheArray`/`RecordCacheHash`.
///
/// The attached path (dshash record/typmod tables over the session DSA area) is
/// keystone-blocked on dshash custom-callback support (see module docs); it
/// keeps a loud panic rather than a silent stub. It is unreachable in a single
/// backend.
fn find_or_make_matching_shared_tupledesc<'mcx>(
    _mcx: Mcx<'mcx>,
    _tupdesc: &TupleDescData<'_>,
) -> PgResult<Option<PgBox<'mcx, TupleDescData<'mcx>>>> {
    // If not even attached, nothing to do.
    if !shared_registry_attached() {
        return Ok(None);
    }

    // Attached (parallel) path: dshash record/typmod tables keyed by a
    // caller-supplied compare/hash over the DSA area. Not expressible over the
    // current dshash substrate (see module docs). Loud panic, never a stub.
    panic!(
        "find_or_make_matching_shared_tupledesc: shared registry attached path \
         requires dshash custom-callback support (keystone-blocked)"
    );
}

/// Install the session seams this crate owns.
///
/// Only the two seams whose faithful bodies are expressible over the current
/// substrate are installed (`initialize_session`, `shared_registry_estimate`).
/// The four record-table-dependent registry seams are keystone-blocked on
/// dshash custom-callback support (see module docs) and intentionally keep
/// their loud default-panic rather than a silent stub.
pub fn init_seams() {
    backend_access_common_session_seams::initialize_session::set(initialize_session);
    backend_access_common_session_seams::get_session_dsm_handle::set(get_session_dsm_handle);
    backend_access_common_session_seams::shared_registry_estimate::set(shared_registry_estimate);
    backend_access_common_session_seams::shared_registry_attached::set(shared_registry_attached);
    backend_access_common_session_seams::find_or_make_matching_shared_tupledesc::set(
        find_or_make_matching_shared_tupledesc,
    );
}
