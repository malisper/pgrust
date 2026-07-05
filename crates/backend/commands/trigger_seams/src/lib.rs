use types_error::PgResult;

seam_core::seam!(
    pub fn after_trigger_begin_xact() -> PgResult<()>
);

seam_core::seam!(
    pub fn after_trigger_begin_sub_xact() -> PgResult<()>
);

seam_core::seam!(
    pub fn after_trigger_fire_deferred() -> PgResult<()>
);

seam_core::seam!(
    pub fn after_trigger_end_xact(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn after_trigger_end_sub_xact(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn after_trigger_pending_on_rel(relid: types_core::Oid) -> bool
);

seam_core::seam!(
    pub fn my_trigger_depth() -> i32
);
