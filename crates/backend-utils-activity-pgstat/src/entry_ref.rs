//! `PgStat_EntryRef`, the backend-local reference to a shared stats entry, plus
//! the process-local control state (`pgStatLocal`), the pending-flush list
//! (`pgStatPending`), and the entry-ref lookup hash (`pgStatEntryRefHash`).
//!
//! These are the owner-private model that `pgstat.c` / `pgstat_shmem.c` keep
//! file-static; `utils/pgstat_internal.h` documents them as visible only to the
//! files implementing the statistics subsystem, so they live in this owner
//! crate and never cross a seam (which is why `types-pgstat` deliberately omits
//! them).

use core::any::Any;

use alloc::boxed::Box;
use std::collections::HashMap;

use types_pgstat::pgstat_internal::{
    PgStat_HashKey, PgStat_ShmemControl, PgStat_Snapshot, PgStatShared_Common,
    PgStatShared_HashEntry,
};
use types_storage::ilist::{dlist_head, dlist_node};
use types_storage::{DsaArea, DshashTable};

/// `PgStat_EntryRef` (`utils/pgstat_internal.h`) — a backend-local reference to
/// a shared statistics entry, caching the resolved shared pointers and holding
/// this backend's not-yet-flushed pending data for the entry.
///
/// In C `shared_entry` and `shared_stats` are raw pointers into **shared
/// memory**: `shared_entry` points at the `PgStatShared_HashEntry` living in the
/// dshash-backed shared stats hashtable, and `shared_stats` is that entry's
/// `body` (a DSA pointer) already resolved with `dsa_get_address()` to a
/// backend-local address, cached to avoid repeated resolution. The same body is
/// shared concurrently by every backend, so the `PgStat_EntryRef` is purely a
/// per-backend *cache of pointers into the shared segment* — it does not own the
/// pointee, whose lifetime is the shared segment, not the EntryRef. Modeling
/// these as owned `Box`es would be contract-divergent (it would imply private
/// heap allocations that no other backend can see); the faithful model — and the
/// established repo idiom for dshash/DSA entries (`dshash.c` port #77 carries
/// resolved entries as `*mut`) — is a raw `*mut`. `null` until the reference is
/// bound to a live shared entry (C leaves these zero in a fresh ref).
///
/// The `pending` member is C's `void *`: each variable-numbered kind defines its
/// own pending block layout and (de)references it through its
/// `PgStat_KindInfo->pending_size`. That type erasure is modeled with
/// `Box<dyn Any>`, so per-kind crates downcast to their own pending struct in
/// their `flush_pending_cb` / `delete_pending_cb`. Unlike the two shared
/// pointers, `pending` *is* backend-private (`MemoryContextAlloc` in
/// `pgstat_prep_pending_entry`), so it remains an owned box.
pub struct PgStat_EntryRef {
    /// `PgStatShared_HashEntry *shared_entry` — pointer to the entry in the
    /// shared stats hashtable (shared memory). `null` until the reference is
    /// bound to a live shared entry. Not owned: the pointee lives in the shared
    /// dshash segment.
    pub shared_entry: *mut PgStatShared_HashEntry,

    /// `PgStatShared_Common *shared_stats` — the stats body
    /// (`shared_entry->body`) resolved to a backend-local pointer via
    /// `dsa_get_address()`, to avoid repeated resolution. Not owned: the pointee
    /// lives in the DSA-backed shared segment. `null` until bound.
    pub shared_stats: *mut PgStatShared_Common,

    /// `uint32 generation` — copy of `shared_entry->generation` taken when the
    /// shared entry was retrieved (number of times reused), to detect a
    /// concurrent reinit.
    pub generation: u32,

    /// `void *pending` — pending statistics awaiting flush to shared memory.
    /// `None` for kinds with `pending_size == 0`. Each kind owns the concrete
    /// type behind the erasure.
    pub pending: Option<Box<dyn Any>>,

    /// `dlist_node pending_node` — membership link in the `pgStatPending` list.
    pub pending_node: dlist_node,
}

impl PgStat_EntryRef {
    /// A fresh, unbound entry reference (all-zero in C: both shared pointers
    /// `NULL`).
    pub fn new() -> Self {
        PgStat_EntryRef {
            shared_entry: core::ptr::null_mut(),
            shared_stats: core::ptr::null_mut(),
            generation: 0,
            pending: None,
            pending_node: dlist_node::new(),
        }
    }
}

impl Default for PgStat_EntryRef {
    fn default() -> Self {
        Self::new()
    }
}

/// `PgStat_EntryRefHashEntry` (`pgstat_shmem.c`) — a hash-table entry mapping a
/// [`PgStat_HashKey`] to its backend-local [`PgStat_EntryRef`]. The C struct's
/// `char status` is `simplehash` bookkeeping; the idiomatic `HashMap` model in
/// [`PgStat_LocalState::entry_ref_hash`] manages occupancy itself, so only the
/// key and the entry reference are carried.
pub struct PgStat_EntryRefHashEntry {
    /// `PgStat_HashKey key` — hash key (also the map key).
    pub key: PgStat_HashKey,
    /// `PgStat_EntryRef *entry_ref` — the referenced backend-local entry.
    pub entry_ref: Box<PgStat_EntryRef>,
}

/// `PGSTAT_ENTRY_REF_HASH_SIZE` (`pgstat_shmem.c`) — initial size hint for the
/// entry-ref hash table.
pub const PGSTAT_ENTRY_REF_HASH_SIZE: usize = 128;

/// `PgStat_LocalState pgStatLocal` (`pgstat_internal.h` / `pgstat.c`) — the
/// process-local control structure for the cumulative statistics system. It
/// references the shared-memory control block, the DSA area, and the shared
/// hash, and holds this backend's current statistics snapshot.
///
/// In C `dsa` and `shared_hash` are `dsa_area *` / `dshash_table *` — opaque
/// backend-local handles into the shared DSA segment, created in
/// `StatsShmemInit()` and attached per-backend in `pgstat_attach_shmem()`. The
/// merged `backend-utils-mmgr-dsa` / `backend-lib-dshash` ports expose those
/// areas as the raw `*mut DsaArea` / `*mut DshashTable` handles the C code
/// holds (and that `dsa_get_address` / `dshash_find_or_insert` take by `*mut`),
/// so these fields carry them directly — the faithful contract, not an opaque
/// flag. `null` until the attach path binds them: C leaves them `NULL` in a
/// process that has not yet attached, and `pgstat_attach_shmem` populates them
/// with `dsa_attach_in_place` / `dshash_attach` against the area published in
/// shared memory. Not owned: the pointees live in the shared DSA segment, whose
/// lifetime is the segment, not this backend-local control block.
pub struct PgStat_LocalState {
    /// `PgStat_ShmemControl *shmem` — the shared control block.
    pub shmem: Option<Box<PgStat_ShmemControl>>,
    /// `dsa_area *dsa` — the DSA area the shared hash and entry bodies live in,
    /// as the backend-local `*mut DsaArea` handle the `dsa.c` substrate hands
    /// back from `dsa_attach_in_place`. `null` until this backend has attached.
    /// Not owned: the area lives in the shared segment.
    pub dsa: *mut DsaArea,
    /// `dshash_table *shared_hash` — the shared stats hash, as the
    /// backend-local `*mut DshashTable` handle from `dshash_attach`. `null`
    /// until attached. Not owned: the table lives in the shared DSA segment.
    pub shared_hash: *mut DshashTable,
    /// `PgStat_Snapshot snapshot` — the current materialized statistics
    /// snapshot.
    pub snapshot: PgStat_Snapshot,
}

impl PgStat_LocalState {
    pub fn new() -> Self {
        PgStat_LocalState {
            shmem: None,
            dsa: core::ptr::null_mut(),
            shared_hash: core::ptr::null_mut(),
            snapshot: PgStat_Snapshot::default(),
        }
    }
}

impl Default for PgStat_LocalState {
    fn default() -> Self {
        Self::new()
    }
}

/// The backend-local pending/flush bookkeeping that `pgstat_shmem.c` keeps
/// file-static: the `pgStatPending` list head and the `pgStatEntryRefHash`
/// lookup table. Grouped here so the follow-on `pgstat.c` / `pgstat_shmem.c`
/// port owns a single backend-local state object.
pub struct PgStat_PendingState {
    /// `dlist_head pgStatPending` (`pgstat_shmem.c`) — entries with pending
    /// data awaiting flush to shared memory.
    pub pending: dlist_head,
    /// `pgstat_entry_ref_hash_hash *pgStatEntryRefHash` (`pgstat_shmem.c`) —
    /// maps a [`PgStat_HashKey`] to its [`PgStat_EntryRefHashEntry`].
    pub entry_ref_hash: HashMap<PgStat_HashKey, PgStat_EntryRefHashEntry>,
}

impl PgStat_PendingState {
    pub fn new() -> Self {
        PgStat_PendingState {
            pending: dlist_head::new(),
            entry_ref_hash: HashMap::with_capacity(PGSTAT_ENTRY_REF_HASH_SIZE),
        }
    }
}

impl Default for PgStat_PendingState {
    fn default() -> Self {
        Self::new()
    }
}
