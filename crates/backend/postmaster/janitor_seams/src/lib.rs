seam_core::seam!(
    // JanitorRegister (janitor crate, pgrust-only): static registration of
    // the ephemeral-database janitor bgworker when pgrust.ephemeral_db_prefix
    // is non-empty at postmaster start (docs/design/test-views.md D1).
    // Postmaster calls through the seam so the janitor crate (which depends
    // on dbcommands/xact for its work) never enters postmaster's dep graph —
    // same shape as launcher_seams::apply_launcher_register.
    pub fn janitor_register()
);
