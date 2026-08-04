seam_core::seam!(
    // JanitorRegister (janitor crate, pgrust-only): static registration of
    // the ephemeral-database janitor bgworker when pgrust.ephemeral_db_prefix
    // is non-empty at postmaster start (docs/design/test-views.md D1).
    // Postmaster calls through the seam so the janitor crate (which depends
    // on dbcommands/xact for its work) never enters postmaster's dep graph —
    // same shape as launcher_seams::apply_launcher_register.
    pub fn janitor_register()
);

seam_core::seam!(
    // EphemeralDbMintOnConnect (janitor crate, pgrust-only,
    // docs/design/test-views.md D2): called by InitPostgres on a database-
    // lookup MISS for an authenticated client backend, on the backend's own
    // thread. Same dep rationale as janitor_register: postinit can never
    // depend on the janitor crate (janitor -> bgworker/dbcommands -> ... ->
    // postinit), so the hook goes through this seam; janitor::init_seams
    // installs the impl.
    //
    // Contract (the impl is janitor::mint::mint_on_connect):
    //   Ok(true)  — an idempotent Ensure was posted, the janitor completed
    //               the mint (or the database already existed): drain
    //               invalidations and retry the lookup ONCE.
    //   Ok(false) — minting is not armed for this name/role (feature off,
    //               grammar mismatch, unlisted role, bare token with no
    //               default template): fall through to the stock
    //               does-not-exist FATAL, byte-identical.
    //   Err(_)    — a clean FATAL of its own: paused/absent janitor, wait
    //               timeout, per-role cap, or the janitor's saved CREATE
    //               DATABASE error fanned out to every waiter.
    pub fn ephemeral_db_mint_on_connect(dbname: &str) -> types_error::PgResult<bool>
);
