//! Seam declarations for the `backend-utils-hash-dynahash` unit
//! (`utils/hash/dynahash.c`).
//!
//! The owning unit (`backend-utils-hash-dynahash`) installs these from its
//! `init_seams()`. Entry pointers are raw (`*mut u8` for the C `void *` user
//! entry, `*mut HTAB` for the per-backend table handle) because shared hash
//! tables live in genuinely shared memory; the `HTAB`/`HASHHDR` bodies are
//! defined in `types_hash::hsearch` (owned by that crate).

#![allow(non_snake_case)]

use types_error::PgResult;
use types_hash::hsearch::{HASHACTION, HASHCTL, HASH_SEQ_STATUS, HTAB};

seam_core::seam!(
    /// `hash_create(tabname, nelem, info, flags)` (dynahash.c) — create (or,
    /// with `HASH_ATTACH`, attach to) a hash table. For shared tables the
    /// caller passes the in-shmem `HASHHDR` via `info.hctl`. `Err` carries
    /// the C `elog(ERROR)`s (bad flags, out of memory).
    pub fn hash_create(name: &str, nelem: i64, info: &HASHCTL, flags: i32) -> PgResult<*mut HTAB>
);

seam_core::seam!(
    /// `hash_search(hashp, keyPtr, action, foundPtr)` (dynahash.c). `key_ptr`
    /// mirrors the C `const void *keyPtr` (for `HASH_STRINGS` tables it
    /// points at a NUL-terminated name). Returns the entry pointer (null for
    /// not-found / `HASH_ENTER_NULL` out-of-memory) and the C `*foundPtr`.
    /// `Err` carries the `HASH_ENTER` out-of-memory `ereport(ERROR)`.
    pub fn hash_search(
        hashp: *mut HTAB,
        key_ptr: *const u8,
        action: HASHACTION,
    ) -> PgResult<(*mut u8, bool)>
);

seam_core::seam!(
    /// `hash_select_dirsize(num_entries)` (dynahash.c) — directory size for a
    /// shared hash table of the given max size. Infallible.
    pub fn hash_select_dirsize(num_entries: i64) -> i64
);

seam_core::seam!(
    /// `hash_destroy(hashp)` (dynahash.c) — free a local hash table by deleting
    /// its private memory context. Infallible.
    pub fn hash_destroy(hashp: *mut HTAB) -> PgResult<()>
);

seam_core::seam!(
    /// `hash_get_shared_size(info, flags)` (dynahash.c) — bytes of shared
    /// memory the table's fixed structures require. Infallible.
    pub fn hash_get_shared_size(info: &HASHCTL, flags: i32) -> usize
);

seam_core::seam!(
    /// `hash_estimate_size(long num_entries, Size entrysize)` (dynahash.c) —
    /// estimate the shared-memory bytes a hash table with `num_entries` of
    /// `entrysize` bytes each will consume (header + directory + segments +
    /// elements). Summed by ipci.c `CalculateShmemSize` for the shmem index.
    /// Infallible (pure arithmetic). Owner unported; scaffolded slot.
    pub fn hash_estimate_size(num_entries: i64, entrysize: usize) -> usize
);

seam_core::seam!(
    /// `hash_get_num_entries(hashp)` (dynahash.c) — number of entries currently
    /// in the table. Infallible.
    pub fn hash_get_num_entries(hashp: *mut HTAB) -> i64
);

seam_core::seam!(
    /// `hash_seq_init(status, hashp)` (dynahash.c) — start a sequential scan.
    pub fn hash_seq_init(status: &mut HASH_SEQ_STATUS, hashp: *mut HTAB)
);

seam_core::seam!(
    /// `hash_seq_search(status)` (dynahash.c) — next entry, or null at scan
    /// end (which also terminates the scan). `Err` carries the
    /// `elog(ERROR, "no hash_seq_search scan to end")` from the internal
    /// `hash_seq_term`.
    pub fn hash_seq_search(status: &mut HASH_SEQ_STATUS) -> PgResult<*mut u8>
);

seam_core::seam!(
    /// `AtEOXact_HashTables(isCommit)` (dynahash.c) — at transaction end, free
    /// any hash tables created in the (sub)transaction memory context. Called
    /// from auxiliary-process error recovery with `isCommit = false`. Pure
    /// bookkeeping; infallible.
    pub fn at_eoxact_hash_tables(is_commit: bool)
);
