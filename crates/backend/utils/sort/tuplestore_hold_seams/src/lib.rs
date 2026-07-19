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
    pub fn tuplestore_rescan(store: types_portal::TuplestoreHandle) -> types_error::PgResult<()>
);

seam_core::seam!(
    // tuplestore_skiptuples(state, ntuples, forward) (tuplestore.c).
    pub fn tuplestore_skiptuples(
        store: types_portal::TuplestoreHandle,
        ntuples: i64,
        forward: bool,
    ) -> types_error::PgResult<bool>
);

// --- WS-CA wave-10 (cursors inc-2, contract §1/§4: "new seams ... whatever
// the portal layer lacks") -------------------------------------------------

seam_core::seam!(
    // tuplestore_begin_heap(randomAccess, interXact, work_mem) with the
    // inter_xact axis exposed: the §1.1 SCROLL-without-HOLD store is
    // inter_xact=false (dies at PortalDrop), unlike the holdStore shape.
    pub fn tuplestore_begin_heap_cursor(
        random_access: bool,
        inter_xact: bool,
    ) -> types_error::PgResult<types_portal::TuplestoreHandle>
);

seam_core::seam!(
    // tuplestore_tuple_count(state) (tuplestore.c) — fill_to's high-water read.
    pub fn tuplestore_tuple_count(store: types_portal::TuplestoreHandle) -> i64
);

seam_core::seam!(
    // §4.2 hidden row-identity sidecar append: one (tableoid, packed
    // block<<16|offset ctid) row per visible store row of a
    // CURRENT-OF-eligible plan. Stored as 2x int8 (layout is internal; the
    // fetch surface never sees these rows).
    pub fn tuplestore_tidstore_put(
        store: types_portal::TuplestoreHandle,
        tableoid: u32,
        tid_packed: u64,
    ) -> types_error::PgResult<()>
);
// --- end WS-CA wave-10 ------------------------------------------------------
