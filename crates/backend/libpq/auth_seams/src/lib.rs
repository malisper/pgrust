seam_core::seam!(
    // ClientAuthentication(port) (libpq/auth.c); C's port arg is MyProcPort
    // (init_small), reachable to the owner directly. Err is the FATAL
    // authentication-failure path.
    pub fn client_authentication() -> types_error::PgResult<()>
);
