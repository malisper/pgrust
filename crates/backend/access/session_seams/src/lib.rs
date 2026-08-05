seam_core::seam!(
    // InitializeSession (access/session.c): allocate the Session state (no DSM
    // yet; that's GetSessionDsmHandle on first parallel query).
    pub fn initialize_session() -> types_error::PgResult<()>
);
