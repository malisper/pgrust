//! Install this crate's `backend-utils-hash-dynahash-seams` providers.

use backend_utils_hash_dynahash_seams as seams;

/// Wire every dynahash seam to the real implementation. Called once during
/// single-threaded startup via `seams-init::init_all`.
pub fn init_seams() {
    seams::hash_create::set(crate::hash_create);
    seams::hash_search::set(crate::hash_search);
    seams::hash_select_dirsize::set(crate::hash_select_dirsize);
    seams::hash_get_shared_size::set(crate::hash_get_shared_size);
    seams::hash_estimate_size::set(crate::hash_estimate_size);
    seams::hash_get_num_entries::set(crate::hash_get_num_entries);
    seams::hash_seq_init::set(crate::hash_seq_init);
    seams::hash_seq_search::set(crate::hash_seq_search);
    seams::at_eoxact_hash_tables::set(crate::AtEOXact_HashTables);
    // `hash_destroy` is infallible (`hash_destroy` returns `()` in C and here);
    // the seam shape is `PgResult<()>` to mirror the failure surface of the
    // hash family, so adapt with an `Ok(())`-returning wrapper.
    seams::hash_destroy::set(|hashp| {
        crate::hash_destroy(hashp);
        Ok(())
    });
}
