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

seam_core::seam!(
    /// `CheckSubscriptionRelkind(relkind, nspname, relname)`
    /// (pg_subscription.c): raise `ERRCODE_WRONG_OBJECT_TYPE` ("cannot use
    /// relation \"%s.%s\" as logical replication target") unless `relkind` is a
    /// regular table or a partitioned table. `Ok(())` when the relkind is
    /// supported; the error is carried on `Err`.
    pub fn check_subscription_relkind(
        relkind: u8,
        nspname: &str,
        relname: &str,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `GetSubscriptionRelState(subid, relid, sublsn)`
    /// (pg_subscription.c): look up the `pg_subscription_rel` row for
    /// `(subid, relid)` and return its `srsubstate` (a `SUBREL_STATE_*` char),
    /// also writing `srsublsn` out through the returned tuple's second element.
    /// Returns `(srsubstate, srsublsn)`; on a missing row the C returns
    /// `SUBREL_STATE_UNKNOWN` with `sublsn = InvalidXLogRecPtr`. `Err` carries
    /// the catalog-scan error surface.
    pub fn get_subscription_rel_state(
        subid: types_core::primitive::Oid,
        relid: types_core::primitive::Oid,
    ) -> PgResult<(u8, types_core::primitive::XLogRecPtr)>
);
