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

seam_core::seam!(
    // tuplestore_gettupleslot(state, forward, copy, slot) (tuplestore.c).
    pub fn tuplestore_gettupleslot<'a, 'mcx>(
        store: types_portal::TuplestoreHandle,
        forward: bool,
        copy: bool,
        slot: &'a mut types_slot::SlotData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    // tuplestore_rescan(state) (tuplestore.c) — active read pointer to start.
    pub fn tuplestore_rescan(store: types_portal::TuplestoreHandle)
);
