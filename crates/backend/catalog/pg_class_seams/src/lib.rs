use types_error::PgResult;

seam_core::seam!(
    pub fn errdetail_relkind_not_supported(relkind: u8) -> PgResult<String>
);
