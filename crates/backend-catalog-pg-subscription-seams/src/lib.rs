//! Seam declaration for reading the `pg_subscription` catalog the launcher
//! drives. The owner (the catalog/heapam transaction machinery) installs this
//! from its `init_seams()` when it lands; until then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_replication_launcher::Subscription;

seam_core::seam!(
    /// `get_subscription_list()` (launcher.c): inside a fresh transaction,
    /// `table_open(SubscriptionRelationId, AccessShareLock)`,
    /// `table_beginscan_catalog`, then for each `pg_subscription` tuple project
    /// the launcher-relevant fields (`oid`, `subdbid`, `subowner`,
    /// `subenabled`, `pstrdup(NameStr(subname))`) into a [`Subscription`];
    /// `table_endscan` / `table_close` / `CommitTransactionCommand`. The whole
    /// scan is the catalog/heapam/xact subsystem's; the launcher only consumes
    /// the resulting list, allocated in the context it passes (`mcx`, the
    /// per-cycle sublist context). Can `ereport(ERROR)`, carried on `Err`.
    pub fn get_subscription_list<'mcx>(
        mcx: Mcx<'mcx>,
    ) -> PgResult<PgVec<'mcx, Subscription>>
);
