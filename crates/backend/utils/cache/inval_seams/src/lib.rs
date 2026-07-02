use types_error::PgResult;

seam_core::seam!(
    pub fn accept_invalidation_messages() -> PgResult<()>
);
