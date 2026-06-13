//! Seam declarations for the `backend-commands-subscriptioncmds` unit
//! (`commands/subscriptioncmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `AlterSubscriptionOwner_oid(subid, newOwnerId)` (subscriptioncmds.c):
    /// change a subscription's owner during REASSIGN OWNED. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_subscription_owner_oid(subid: Oid, new_owner_id: Oid) -> PgResult<()>
);
