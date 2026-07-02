use types_error::PgResult;

seam_core::seam!(
    pub fn init_dead_lock_checking() -> PgResult<()>
);
