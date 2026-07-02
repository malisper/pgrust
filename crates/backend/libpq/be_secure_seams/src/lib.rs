use types_error::PgResult;

seam_core::seam!(
    pub fn secure_read(buf: &mut [u8]) -> PgResult<Result<usize, i32>>
);

seam_core::seam!(
    pub fn secure_write(buf: &[u8]) -> PgResult<Result<usize, i32>>
);

seam_core::seam!(
    pub fn set_port_noblock(noblock: bool) -> bool
);
