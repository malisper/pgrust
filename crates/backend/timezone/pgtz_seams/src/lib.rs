seam_core::seam!(
    // pg_open_tzfile + tzload's single read into `buf`: Ok(None) is C's -1,
    // Ok(nread) caps at buf.len(); canonname NUL-terminated when present.
    // PgResult: C can ereport (AllocateDir fd pressure).
    pub fn pg_open_tzfile(
        name: &[u8],
        canonname: Option<&mut [u8; 256]>,
        buf: &mut [u8],
    ) -> types_error::PgResult<Option<usize>>
);
