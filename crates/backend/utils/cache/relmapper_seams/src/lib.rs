seam_core::seam!(
    // Reloads the map file when loaded; the read can FATAL, carried as Err.
    pub fn relation_map_invalidate(shared: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn relation_map_invalidate_all() -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn relation_map_initialize()
);

seam_core::seam!(
    pub fn relation_map_initialize_phase2() -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn relation_map_initialize_phase3() -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn relation_map_update_map(
        relation_id: types_core::Oid,
        file_number: types_core::Oid,
        shared: bool,
        immediate: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    // Pure in-memory lookup, no ereport.
    pub fn relation_map_oid_to_filenumber(
        relation_id: types_core::Oid,
        shared: bool,
    ) -> types_core::RelFileNumber
);
