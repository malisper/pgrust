//! `pg_db_role_setting` catalog vocabulary, mirroring `catalog/pg_db_role_setting.h`
//! (the `CATALOG(pg_db_role_setting,2964,DbRoleSettingRelationId)` definition).
//!
//! The relation / index OIDs themselves live in [`crate::catalog`]; this module
//! carries the attribute numbers the scan-key construction and `heap_getattr`
//! decode rely on.

use ::types_core::primitive::AttrNumber;

/* `Anum_pg_db_role_setting_*` (`pg_db_role_setting_d.h`) — attribute numbers in
 * the CATALOG field order of `catalog/pg_db_role_setting.h`. */

/// `setdatabase` — database OID, or 0 for a role-specific setting.
pub const Anum_pg_db_role_setting_setdatabase: AttrNumber = 1;
/// `setrole` — role OID, or 0 for a database-specific setting.
pub const Anum_pg_db_role_setting_setrole: AttrNumber = 2;
/// `setconfig text[]` — the GUC settings to apply at login.
pub const Anum_pg_db_role_setting_setconfig: AttrNumber = 3;

/// `Natts_pg_db_role_setting` (`pg_db_role_setting_d.h`).
pub const Natts_pg_db_role_setting: usize = 3;
