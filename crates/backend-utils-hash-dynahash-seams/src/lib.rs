//! Seam declarations for the `backend-utils-hash-dynahash` unit
//! (`utils/hash/dynahash.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `AtEOXact_HashTables(isCommit)` (dynahash.c) — at transaction end, free
    /// any hash tables created in the (sub)transaction memory context. Called
    /// from auxiliary-process error recovery with `isCommit = false`. Pure
    /// bookkeeping; infallible.
    pub fn at_eoxact_hash_tables(is_commit: bool)
);
