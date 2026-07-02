seam_core::seam!(
    // tuplestore_begin_heap(randomAccess, true /* interXact */, work_mem)
    // allocated under portal->holdContext (tuplestore.c); the owner reads
    // work_mem and keeps the store in its own registry behind the handle.
    pub fn tuplestore_begin_heap_hold(
        random_access: bool,
    ) -> types_error::PgResult<types_portal::TuplestoreHandle>
);

seam_core::seam!(
    // tuplestore_end(state) — frees the store and its temp files.
    pub fn tuplestore_end(store: types_portal::TuplestoreHandle)
);
