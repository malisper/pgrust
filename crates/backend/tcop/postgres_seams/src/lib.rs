seam_core::seam!(
    // PostgresMain (tcop/postgres.c); never returns.
    pub fn postgres_main(dbname: &str, username: &str) -> !
);
