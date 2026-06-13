//! Seam declarations for the `backend-utils-hash-dynahash` unit
//! (`utils/hash/dynahash.c`).
//!
//! Only the transaction-end hash-table cleanup the archiver's error-recovery
//! path needs. The owning unit installs this from its `init_seams()` when it
//! lands; until then a call panics loudly.

seam_core::seam!(
    /// `AtEOXact_HashTables(isCommit)` (`utils/hash/dynahash.c`) — clean up
    /// dynahash tables at transaction end (WARNs about leaks at commit).
    /// Infallible.
    pub fn at_eoxact_hash_tables(is_commit: bool)
);
