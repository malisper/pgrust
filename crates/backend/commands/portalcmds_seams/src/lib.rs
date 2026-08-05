seam_core::seam!(
    // PortalCleanup(portal) (portalcmds.c) — the portal->cleanup hook body;
    // shuts down the executor, may run user code.
    pub fn portal_cleanup(
        portal: &types_portal::Portal<'static>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    // PersistHoldablePortal(portal) (portalcmds.c) — materialize the cursor
    // result into portal->holdStore; runs the executor.
    pub fn persist_holdable_portal(
        portal: &types_portal::Portal<'static>,
    ) -> types_error::PgResult<()>
);
