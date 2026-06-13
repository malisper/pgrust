//! Seam declarations for the `backend-commands-event-trigger` unit
//! (`commands/event_trigger.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `AlterEventTriggerOwner_oid(trigOid, newOwnerId)` (event_trigger.c):
    /// change an event trigger's owner during REASSIGN OWNED. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_event_trigger_owner_oid(trig_oid: Oid, new_owner_id: Oid) -> PgResult<()>
);
