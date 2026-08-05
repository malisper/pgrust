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

seam_core::seam!(
    pub fn secure_close()
);

seam_core::seam!(
    // be_tls_get_certificate_hash (be-secure-openssl.c): the server
    // certificate hash for tls-server-end-point channel binding; reachable
    // only with ssl_in_use (C's port arg is MyProcPort's TLS state).
    pub fn be_tls_get_certificate_hash() -> PgResult<Vec<u8>>
);
