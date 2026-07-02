seam_core::seam!(
    // ClientAuthentication(port) (libpq/auth.c); C's port arg is MyProcPort
    // (init_small), reachable to the owner directly. Err is the FATAL
    // authentication-failure path.
    pub fn client_authentication() -> types_error::PgResult<()>
);
seam_core::seam!(
    // load_hba() (libpq/hba.c): false = parse failure; caller FATALs.
    pub fn load_hba() -> bool
);
seam_core::seam!(
    // load_ident() (libpq/hba.c): false is non-fatal (C logs and continues).
    pub fn load_ident() -> bool
);
