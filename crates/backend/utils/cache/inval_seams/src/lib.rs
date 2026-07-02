use types_error::PgResult;

// Err carries an error raised by an invalidation callback or catchup.
seam_core::seam!(
    pub fn accept_invalidation_messages() -> PgResult<()>
);
