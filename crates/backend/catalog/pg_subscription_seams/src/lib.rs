//! Seam declaration for reading the `pg_subscription` catalog the launcher
//! drives. The owner (the catalog/heapam transaction machinery) installs this
//! from its `init_seams()` when it lands; until then a call panics loudly.

use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use ::replication_launcher::Subscription;

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
        subid: ::types_core::primitive::Oid,
        relid: ::types_core::primitive::Oid,
    ) -> PgResult<(u8, ::types_core::primitive::XLogRecPtr)>
);

use ::types_catalog::pg_subscription as cat_sub;
use ::types_core::primitive::{Oid, XLogRecPtr};

seam_core::seam!(
    /// `GetSubscription(subid, missing_ok)` (pg_subscription.c):
    /// `SearchSysCache1(SUBSCRIPTIONOID)` then decode the full
    /// [`Subscription`] (all fixed-width columns, the `text`/`name`/`text[]`
    /// varlena columns via `SysCacheGetAttr*`/`TextDatumGetCString`/
    /// `textarray_to_stringlist`, and `superuser_arg(subowner)`).
    /// `None` when `missing_ok` and the row is absent; otherwise a missing
    /// row is the C `elog(ERROR, "cache lookup failed for subscription %u")`
    /// carried on `Err`. Allocated in `mcx`.
    pub fn get_subscription<'mcx>(
        mcx: Mcx<'mcx>,
        subid: Oid,
        missing_ok: bool,
    ) -> PgResult<Option<cat_sub::Subscription<'mcx>>>
);

seam_core::seam!(
    /// `CountDBSubscriptions(dbid)` (pg_subscription.c): keyed
    /// `systable_beginscan(pg_subscription, subdbid = dbid)` count loop. Used
    /// by `dropdb()` to check the database can be dropped.
    pub fn count_db_subscriptions(dbid: Oid) -> PgResult<i32>
);

seam_core::seam!(
    /// `DisableSubscription(subid)` (pg_subscription.c): set `subenabled =
    /// false` via `heap_modify_tuple` + `CatalogTupleUpdate` (under
    /// `LockSharedObject(SubscriptionRelationId, subid, AccessShareLock)`).
    pub fn disable_subscription(subid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `AddSubscriptionRelState(subid, relid, state, sublsn, retain_lock)`
    /// (pg_subscription.c): form + `CatalogTupleInsert` a new
    /// `pg_subscription_rel` row; `sublsn == InvalidXLogRecPtr` stores SQL
    /// NULL. Errors if the (relid, subid) mapping already exists.
    pub fn add_subscription_rel_state(
        subid: Oid,
        relid: Oid,
        state: u8,
        sublsn: XLogRecPtr,
        retain_lock: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `UpdateSubscriptionRelState(subid, relid, state, sublsn,
    /// already_locked)` (pg_subscription.c): replace `srsubstate` + `srsublsn`
    /// via `heap_modify_tuple` + `CatalogTupleUpdate`. Errors if the mapping
    /// does not exist.
    pub fn update_subscription_rel_state(
        subid: Oid,
        relid: Oid,
        state: u8,
        sublsn: XLogRecPtr,
        already_locked: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `RemoveSubscriptionRel(subid, relid)` (pg_subscription.c): catalog scan
    /// (by subid and/or relid, whichever is valid) + `CatalogTupleDelete` of
    /// every matching row, with the "sync in progress" guard that
    /// `ereport(ERROR)`s when dropping a relation-only mapping whose state is
    /// not `SUBREL_STATE_READY`.
    pub fn remove_subscription_rel(subid: Oid, relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `HasSubscriptionRelations(subid)` (pg_subscription.c): keyed
    /// `systable_beginscan(pg_subscription_rel, srsubid = subid)` single-tuple
    /// existence test.
    pub fn has_subscription_relations(subid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `GetSubscriptionRelations(subid, not_ready)` (pg_subscription.c): keyed
    /// scan returning each relation's [`SubscriptionRelState`] (with `srsublsn`
    /// null-resolved to `InvalidXLogRecPtr`); when `not_ready` only rows whose
    /// `srsubstate != SUBREL_STATE_READY`. Allocated in `mcx`.
    pub fn get_subscription_relations<'mcx>(
        mcx: Mcx<'mcx>,
        subid: Oid,
        not_ready: bool,
    ) -> PgResult<PgVec<'mcx, cat_sub::SubscriptionRelState>>
);
