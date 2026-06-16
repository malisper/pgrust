//! Seam declarations for the genuinely-unported binary-upgrade catalog-state
//! owners, as consumed by `pg_upgrade_support.c`'s `binary_upgrade_*`
//! functions.
//!
//! Each `binary_upgrade_set_next_*` setter writes one of the
//! `catalog/binary_upgrade.h` backend globals
//! (`binary_upgrade_next_pg_type_oid`, `..._heap_pg_class_relfilenumber`, …)
//! that `heap.c` / `pg_type.c` / DDL consults during a `pg_upgrade` restore.
//! Those globals — and the `IsBinaryUpgrade` mode flag, `InsertExtensionTuple`
//! (extension.c), `SetAttrMissing` (heap.c), `ReplicationSlotAcquire` /
//! `LogicalReplicationSlotHasPendingWal` (logical.c), `AddSubscriptionRelState`
//! (pg_subscription_rel.c), `replorigin_advance` (origin.c) — are not ported.
//! The whole setter/operation crosses one seam each (after the function's own
//! `CHECK_IS_BINARY_UPGRADE` / arg-NULL gates, which `pg_upgrade_support.c`
//! keeps). The owning units install these from `init_seams()` when they land;
//! until then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `IsBinaryUpgrade` (`catalog/binary_upgrade.h` / `globals.c`) — the mode
    /// flag every `binary_upgrade_*` function checks via
    /// `CHECK_IS_BINARY_UPGRADE`.
    pub fn is_binary_upgrade() -> bool
);

/// Which `binary_upgrade_next_*` global a [`set_next_oid`] call targets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NextOidSlot {
    /// `binary_upgrade_next_pg_tablespace_oid`.
    PgTablespace,
    /// `binary_upgrade_next_pg_type_oid`.
    PgType,
    /// `binary_upgrade_next_array_pg_type_oid`.
    ArrayPgType,
    /// `binary_upgrade_next_mrng_pg_type_oid`.
    MultirangePgType,
    /// `binary_upgrade_next_mrng_array_pg_type_oid`.
    MultirangeArrayPgType,
    /// `binary_upgrade_next_heap_pg_class_oid`.
    HeapPgClass,
    /// `binary_upgrade_next_heap_pg_class_relfilenumber`.
    HeapRelfilenode,
    /// `binary_upgrade_next_index_pg_class_oid`.
    IndexPgClass,
    /// `binary_upgrade_next_index_pg_class_relfilenumber`.
    IndexRelfilenode,
    /// `binary_upgrade_next_toast_pg_class_oid`.
    ToastPgClass,
    /// `binary_upgrade_next_toast_pg_class_relfilenumber`.
    ToastRelfilenode,
    /// `binary_upgrade_next_pg_enum_oid`.
    PgEnum,
    /// `binary_upgrade_next_pg_authid_oid`.
    PgAuthid,
}

seam_core::seam!(
    /// Assign one of the `binary_upgrade_next_*` OID/relfilenumber globals
    /// (the shared body of the `binary_upgrade_set_next_*` setters). Infallible
    /// (a plain global store); the function's `CHECK_IS_BINARY_UPGRADE` gate is
    /// applied by the caller.
    pub fn set_next_oid(slot: NextOidSlot, value: Oid)
);

seam_core::seam!(
    /// Read-and-reset one of the `binary_upgrade_next_*_pg_class_relfilenumber`
    /// globals (`catalog/binary_upgrade.h`) the way
    /// `RelationSetNewRelfilenumber` (relcache.c) consumes it during a
    /// `pg_upgrade` restore: it returns the global's current value and clears it
    /// to `InvalidOid`. `is_index` selects the index vs. heap global. Returns
    /// `InvalidOid` when the global was not set (the relcache caller raises the
    /// "relfilenumber value not set" error). A plain global read/store —
    /// infallible.
    pub fn consume_next_relfilenumber(is_index: bool) -> Oid
);

seam_core::seam!(
    /// Read-and-reset `binary_upgrade_next_heap_pg_class_oid` /
    /// `binary_upgrade_next_toast_pg_class_oid` (`catalog/binary_upgrade.h`) the
    /// way `heap_create_with_catalog` (catalog/heap.c) consumes it during a
    /// `pg_upgrade` restore: returns the global's value and clears it to
    /// `InvalidOid`. `is_toast` selects the toast vs. heap global. Returns
    /// `InvalidOid` when the global was not set (the caller raises the "OID
    /// value not set" error, except for the toast case where an unset value is
    /// expected — there may be no TOAST table). A plain global read/store —
    /// infallible.
    pub fn consume_next_pg_class_oid(is_toast: bool) -> Oid
);

seam_core::seam!(
    /// Read-and-reset `binary_upgrade_next_heap_pg_class_relfilenumber` /
    /// `binary_upgrade_next_toast_pg_class_relfilenumber`
    /// (`catalog/binary_upgrade.h`) the way `heap_create_with_catalog`
    /// (catalog/heap.c) consumes it during a `pg_upgrade` restore: returns the
    /// global's value and clears it to `InvalidOid`. `is_toast` selects the
    /// toast vs. heap global. Returns `InvalidRelFileNumber` when unset (the
    /// caller raises "relfilenumber value not set"). A plain global read/store —
    /// infallible.
    pub fn consume_next_pg_class_relfilenumber(is_toast: bool) -> Oid
);

seam_core::seam!(
    /// Read-and-reset `binary_upgrade_next_pg_type_oid`
    /// (`catalog/binary_upgrade.h`) the way `TypeShellMake` / `TypeCreate`
    /// (pg_type.c) consume it during a `pg_upgrade` restore: it returns the
    /// global's current value and clears it to `InvalidOid`. Returns
    /// `InvalidOid` when the global was not set (the caller raises "pg_type OID
    /// value not set when in binary upgrade mode"). A plain global read/store —
    /// infallible.
    pub fn consume_next_pg_type_oid() -> Oid
);

seam_core::seam!(
    /// Read-and-reset `binary_upgrade_next_pg_enum_oid`
    /// (`catalog/binary_upgrade.h`) the way `AddEnumLabel` (pg_enum.c) consumes
    /// it during a `pg_upgrade` restore: it returns the global's current value
    /// and clears it to `InvalidOid`. Returns `InvalidOid` when the global was
    /// not set (the caller raises "pg_enum OID value not set when in binary
    /// upgrade mode"). A plain global read/store — infallible.
    pub fn consume_next_pg_enum_oid() -> Oid
);

seam_core::seam!(
    /// `binary_upgrade_record_init_privs = record_init_privs` — the
    /// init-privs recording flag (`catalog/binary_upgrade.h`).
    pub fn set_record_init_privs(record_init_privs: bool)
);

seam_core::seam!(
    /// `SetAttrMissing(table_id, attname, value)` (heap.c) — set an
    /// attribute's `attmissingval` during upgrade. `Err` carries its
    /// `ereport(ERROR)`s.
    pub fn set_missing_value(table_id: Oid, attname: &str, value: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `binary_upgrade_create_empty_extension(...)` body (extension.c
    /// `InsertExtensionTuple`): create the pg_extension catalog row for a
    /// being-restored extension, given its name, schema name, relocatable
    /// flag, version, optional config/condition `text[]` varlena bytes (`None`
    /// = SQL NULL), and the names of its required extensions. `Err` carries
    /// the namespace/extension lookup and insert `ereport(ERROR)`s.
    pub fn create_empty_extension(
        ext_name: &str,
        schema_name: &str,
        relocatable: bool,
        ext_version: &str,
        ext_config: Option<&[u8]>,
        ext_condition: Option<&[u8]>,
        required_extension_names: &[&str],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `binary_upgrade_logical_slot_has_caught_up(slot_name)` body (logical.c):
    /// acquire the named logical slot, check whether any decodable WAL remains
    /// after its `confirmed_flush_lsn`, release it, and return `true` when the
    /// slot has consumed all changes (no pending WAL). `Err` carries the slot
    /// acquire / WAL scan `ereport(ERROR)`s.
    pub fn logical_slot_has_caught_up(slot_name: &str) -> PgResult<bool>
);

seam_core::seam!(
    /// `binary_upgrade_add_sub_rel_state(subname, relid, relstate, sublsn)`
    /// body (pg_subscription_rel.c `AddSubscriptionRelState`): add the relation
    /// with the given state to `pg_subscription_rel` for the named
    /// subscription. `sublsn` is `None` for a NULL LSN. `Err` carries the
    /// catalog `ereport(ERROR)`s.
    pub fn add_sub_rel_state(
        subname: &str,
        relid: Oid,
        relstate: i8,
        sublsn: Option<u64>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `binary_upgrade_replorigin_advance(subname, remote_commit)` body
    /// (origin.c `replorigin_advance`): set the remote_lsn for the named
    /// subscription's replication origin. `remote_commit` is `None` for a NULL
    /// LSN. `Err` carries the lookup / advance `ereport(ERROR)`s.
    pub fn replorigin_advance(subname: &str, remote_commit: Option<u64>) -> PgResult<()>
);
