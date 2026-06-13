//! Inward seam declarations for `backend-utils-adt-ri-triggers`
//! (`utils/adt/ri_triggers.c`): the syscache invalidation callback the
//! catalog-cache owner fires on `pg_constraint` changes.
//!
//! The owning unit (`backend-utils-adt-ri-triggers`) installs this from its
//! `init_seams()`; the syscache owner (which registers the callback) invokes it
//! through this seam to avoid a dependency cycle. Until RI is installed, a call
//! panics loudly.

seam_core::seam!(
    /// `InvalidateConstraintCacheCallBack(arg, cacheid, hashvalue)`
    /// (ri_triggers.c): invalidate any `ri_constraint_cache` entry whose
    /// syscache hash value matches `hashvalue` (or all entries if
    /// `hashvalue == 0`). Infallible (mutates only process-local state).
    pub fn invalidate_constraint_cache_callback(hashvalue: u32)
);
