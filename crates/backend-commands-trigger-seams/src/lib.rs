//! Seam declarations for the `backend-commands-trigger` unit
//! (`commands/trigger.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `AfterTriggerBeginXact()` — initialize the deferred-trigger manager.
    pub fn after_trigger_begin_xact() -> PgResult<()>
);

seam_core::seam!(
    /// `AfterTriggerFireDeferred()` — fire all pending deferred triggers
    /// (user code; can `ereport(ERROR)`).
    pub fn after_trigger_fire_deferred() -> PgResult<()>
);

seam_core::seam!(
    /// `AfterTriggerEndXact(isCommit)` — shut down the deferred-trigger
    /// manager.
    pub fn after_trigger_end_xact(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `AfterTriggerBeginSubXact()`.
    pub fn after_trigger_begin_sub_xact() -> PgResult<()>
);

seam_core::seam!(
    /// `AfterTriggerEndSubXact(isCommit)`.
    pub fn after_trigger_end_sub_xact(is_commit: bool) -> PgResult<()>
);
