use types_error::PgError;

seam_core::seam!(
    pub fn write_csvlog(edata: &PgError)
);

seam_core::seam!(
    pub fn write_jsonlog(edata: &PgError)
);
