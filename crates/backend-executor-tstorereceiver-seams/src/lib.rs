//! Seam declarations for the `backend-executor-tstoreReceiver` unit
//! (`executor/tstoreReceiver.c`) plus the `CreateDestReceiver`/`rDestroy`
//! dispatch in `tcop/dest.c` that portalcmds drives for a holdable cursor.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use types_portal::{DestReceiver, Portal};

seam_core::seam!(
    /// `CreateDestReceiver(DestTuplestore)` (dest.c) — allocate a tuplestore
    /// destination receiver. Specialized to the `DestTuplestore` case
    /// portalcmds uses. Can `ereport(ERROR)`.
    pub fn create_dest_receiver_tuplestore() -> PgResult<DestReceiver>
);

seam_core::seam!(
    /// `SetTuplestoreDestReceiverParams(self, tStore, tContext, detoast, ...)`
    /// (tstoreReceiver.c), specialized to portalcmds' call: `tStore` is
    /// `portal->holdStore` and `tContext` is `portal->holdContext` (read off
    /// the portal here), the slot/format args are NULL. `detoast` is the
    /// "detoast all data passed through" flag.
    pub fn set_tuplestore_dest_receiver_params(
        receiver: DestReceiver,
        portal: &Portal,
        detoast: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `dest->rDestroy(dest)` (dest.c dispatch) — destroy a destination
    /// receiver (consumes it).
    pub fn dest_destroy(receiver: DestReceiver) -> PgResult<()>
);
