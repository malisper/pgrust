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
//!
//! ## What this crate does NOT install (keystone-blocked, NOT stubbed)
//!
//! The other four registry seams (`shared_registry_init`,
//! `shared_registry_attach`, `shared_typmod_table_find`,
//! `find_or_make_matching_shared_tupledesc`) and the `shared_registry_attached`
//! read are the parallel-worker path. Their faithful bodies create/attach the
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

use mcx::MemoryContext;
use types_error::PgResult;

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

/// Install the session seams this crate owns.
///
/// Only the two seams whose faithful bodies are expressible over the current
/// substrate are installed (`initialize_session`, `shared_registry_estimate`).
/// The four record-table-dependent registry seams are keystone-blocked on
/// dshash custom-callback support (see module docs) and intentionally keep
/// their loud default-panic rather than a silent stub.
pub fn init_seams() {
    backend_access_common_session_seams::initialize_session::set(initialize_session);
    backend_access_common_session_seams::shared_registry_estimate::set(shared_registry_estimate);
}
