//! `pg_extension` relation / index OIDs and column numbers
//! (`catalog/pg_extension.h`), trimmed to the rows the extension command port
//! consumes.

use types_core::primitive::Oid;

/// `ExtensionRelationId` — pg_extension's relation OID (`pg_extension.h` CATALOG).
pub const ExtensionRelationId: Oid = 3079;
/// `ExtensionOidIndexId` — `pg_extension_oid_index` OID.
pub const ExtensionOidIndexId: Oid = 3080;
/// `ExtensionNameIndexId` — `pg_extension_name_index` OID.
pub const ExtensionNameIndexId: Oid = 3081;

/// `Anum_pg_extension_oid` = 1.
pub const Anum_pg_extension_oid: i32 = 1;
/// `Anum_pg_extension_extname` = 2 (NAME).
pub const Anum_pg_extension_extname: i32 = 2;
/// `Anum_pg_extension_extowner` = 3.
pub const Anum_pg_extension_extowner: i32 = 3;
/// `Anum_pg_extension_extnamespace` = 4.
pub const Anum_pg_extension_extnamespace: i32 = 4;
/// `Anum_pg_extension_extrelocatable` = 5.
pub const Anum_pg_extension_extrelocatable: i32 = 5;
/// `Anum_pg_extension_extversion` = 6 (text).
pub const Anum_pg_extension_extversion: i32 = 6;
/// `Anum_pg_extension_extconfig` = 7 (oid[]).
pub const Anum_pg_extension_extconfig: i32 = 7;
/// `Anum_pg_extension_extcondition` = 8 (text[]).
pub const Anum_pg_extension_extcondition: i32 = 8;
/// `Natts_pg_extension` = 8.
pub const Natts_pg_extension: i32 = 8;
