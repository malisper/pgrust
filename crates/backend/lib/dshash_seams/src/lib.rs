//! Seam declarations for the `dshash.c` substrate (`lib/dshash.c`, catalog
//! unit `backend-lib-no-ilist`/`backend-lib-all`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. A live `dshash_table` is a backend-local handle
//! into the dshash substrate's own structures, so it crosses the seam as the
//! raw `*mut DshashTable` pointer the C code holds — never dereferenced by
//! consumers. A found-or-inserted entry lives in the table's DSA-shared memory
//! and is named by the raw pointer the C `void *` is; the consumer (which
//! defines the entry layout) reads and writes the entry through it while the
//! returned [`DshashEntryGuard`] holds the partition lock.

use types_storage::{dshash_table_handle, DsaArea, DshashParameters, DshashTable};
use ::types_error::PgResult;

seam_core::seam!(
    /// `dshash_create(dsa_area *area, const dshash_parameters *params,
    /// void *arg)` — create a new hash table in the given DSA area (`arg` is
    /// always NULL for the registry). Returns the backend-local table handle.
    /// `Err` carries the `ereport(ERROR)` for an allocation failure.
    pub fn dshash_create(area: *mut DsaArea, params: DshashParameters) -> PgResult<*mut DshashTable>
);

seam_core::seam!(
    /// `dshash_attach(dsa_area *area, const dshash_parameters *params,
    /// dshash_table_handle handle, void *arg)` — attach to an existing hash
    /// table (`arg` always NULL for the registry). Returns the backend-local
    /// table handle. `Err` carries the `ereport(ERROR)` for an allocation
    /// failure.
    pub fn dshash_attach(
        area: *mut DsaArea,
        params: DshashParameters,
        handle: dshash_table_handle,
    ) -> PgResult<*mut DshashTable>
);

seam_core::seam!(
    /// `dshash_get_hash_table_handle(dshash_table *hash_table)` — the table's
    /// handle, for publishing to other backends that will `dshash_attach`.
    pub fn dshash_get_hash_table_handle(hash_table: *mut DshashTable) -> dshash_table_handle
);

seam_core::seam!(
    /// `dshash_find_or_insert(dshash_table *hash_table, const void *key,
    /// bool *found)` — find the entry for `key`, inserting a zeroed one if
    /// absent. `key` is the raw `const void *key` bytes (the first
    /// `params.key_size` bytes of the entry): a string consumer passes its
    /// fixed-width NUL-padded name, a binary consumer passes the key's byte
    /// image (e.g. `subid.to_ne_bytes()`). Holds the entry's partition lock;
    /// the returned [`DshashEntryGuard`] carries the raw entry pointer (into the
    /// table's DSA-shared memory) and releases the lock on drop. `Err` carries
    /// the `ereport(ERROR)` for an allocation failure while inserting.
    pub fn dshash_find_or_insert(
        hash_table: *mut DshashTable,
        key: &[u8],
    ) -> PgResult<DshashEntryGuard>
);

seam_core::seam!(
    /// `dshash_find(dshash_table *hash_table, const void *key, bool exclusive)`
    /// — find the entry for `key` without inserting. `key` is the raw
    /// `const void *key` bytes (see [`dshash_find_or_insert`]). Returns
    /// `Some(guard)` holding the entry's partition lock when present, `None`
    /// when the C `dshash_find` returns NULL (no entry). `exclusive` selects the
    /// lock mode (`false` = shared, as the launcher's read path uses).
    pub fn dshash_find(
        hash_table: *mut DshashTable,
        key: &[u8],
        exclusive: bool,
    ) -> PgResult<Option<DshashEntryGuard>>
);

seam_core::seam!(
    /// `dshash_delete_key(dshash_table *hash_table, const void *key)` — remove
    /// the entry for `key` if present, returning whether one was deleted. `key`
    /// is the raw `const void *key` bytes (see [`dshash_find_or_insert`]). No
    /// lock is left held on return.
    pub fn dshash_delete_key(hash_table: *mut DshashTable, key: &[u8]) -> PgResult<bool>
);

seam_core::seam!(
    /// `dshash_release_lock(dshash_table *hash_table, void *entry)` — release
    /// the partition lock held for `entry`. Reached only through
    /// [`DshashEntryGuard`] (`release()` or `Drop`); consumers never call it
    /// directly.
    pub fn dshash_release_lock(hash_table: *mut DshashTable, entry: *mut u8)
);

/// The held-entry token returned by [`dshash_find_or_insert`]: holds the
/// partition lock for the entry. `Drop` releases the lock (the abort path,
/// matching the C `dshash_release_lock` that the registry's `ereport` paths
/// would otherwise leak); [`Self::release`] is the explicit release at the
/// point where C calls `dshash_release_lock`.
#[derive(Debug)]
pub struct DshashEntryGuard {
    hash_table: *mut DshashTable,
    entry: *mut u8,
    /// The C `*found`: true when the entry already existed.
    pub found: bool,
    released: bool,
}

impl DshashEntryGuard {
    /// Wrap a just-located entry and its held partition lock. Called by the
    /// owner's installed implementation (and test fixtures); consumers only
    /// ever receive one.
    pub fn new(hash_table: *mut DshashTable, entry: *mut u8, found: bool) -> Self {
        DshashEntryGuard {
            hash_table,
            entry,
            found,
            released: false,
        }
    }

    /// The raw pointer to the entry in the table's DSA-shared memory. The
    /// consumer defines the entry layout and reads/writes its fields through
    /// this pointer while the lock is held.
    pub fn entry_ptr(&self) -> *mut u8 {
        self.entry
    }

    /// `dshash_release_lock(hash_table, entry)` at the C call site, consuming
    /// the guard.
    pub fn release(mut self) {
        self.released = true;
        dshash_release_lock::call(self.hash_table, self.entry);
    }
}

impl Drop for DshashEntryGuard {
    fn drop(&mut self) {
        if !self.released {
            dshash_release_lock::call(self.hash_table, self.entry);
        }
    }
}
