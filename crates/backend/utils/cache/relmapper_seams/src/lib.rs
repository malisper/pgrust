seam_core::seam!(
    // RelationMapInvalidate(shared) (relmapper.c): flag-setting only, no ereport.
    pub fn relation_map_invalidate(shared: bool)
);

seam_core::seam!(
    pub fn at_eoxact_relation_map(is_commit: bool, is_parallel_worker: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn at_cci_relation_map() -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn at_prepare_relation_map() -> types_error::PgResult<()>
);
