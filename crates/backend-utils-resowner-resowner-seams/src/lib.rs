//! Seam declarations for the resource-owner currency portalcmds needs
//! (`utils/resowner/resowner.c`).
//!
//! `CurrentResourceOwner` is *not* exposed as a save/restore global pair — that
//! is the ambient-state anti-pattern the lifecycle model forbids
//! (docs/query-lifecycle-raii.md; resowner dissolves into RAII/scoped
//! capability). The C `saveResourceOwner = CurrentResourceOwner;
//! CurrentResourceOwner = portal->resowner; ...; CurrentResourceOwner =
//! saveResourceOwner;` save/run/restore idiom is modeled as a single scoped
//! callback: run `f` with the given owner current, restoring the prior owner
//! on the way out (including on the error path).

use types_error::PgResult;
use types_portal::ResourceOwner;

seam_core::seam!(
    /// Run `f` with `owner` installed as the current resource owner, restoring
    /// the previous current owner afterwards (and on error). When `owner`
    /// is the C NULL (`ResourceOwner::is_null`), the current owner is left
    /// unchanged for the duration (mirrors `if (portal->resowner)
    /// CurrentResourceOwner = portal->resowner;`). `f`'s error propagates.
    pub fn with_current_resource_owner(
        owner: ResourceOwner,
        f: &mut dyn FnMut() -> PgResult<()>,
    ) -> PgResult<()>
);
