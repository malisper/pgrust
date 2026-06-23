//! `pg_collation` catalog vocabulary (`catalog/pg_collation.h` /
//! `pg_collation_d.h`) — relation / index OIDs, column numbers, attribute count,
//! and the `COLLPROVIDER_*` provider codes.
//!
//! The owning catalog crate (`backend-catalog-pg-collation`) forms a
//! `pg_collation` tuple from these column numbers against the relation
//! descriptor (`CollationCreate`), so consumers never touch the on-disk datum
//! layout. Field order is verified field-for-field against
//! `FormData_pg_collation` in `pg_collation.h`.

use types_core::primitive::Oid;

// ---------------------------------------------------------------------------
// Relation / index OIDs (catalog/pg_collation_d.h).
// ---------------------------------------------------------------------------

/// `CollationRelationId` — `pg_collation`'s relation OID
/// (`CATALOG(pg_collation,3456,CollationRelationId)`).
pub const CollationRelationId: Oid = 3456;

/// `CollationNameEncNspIndexId` — `pg_collation_name_enc_nsp_index` OID
/// (unique on `(collname, collencoding, collnamespace)`).
pub const CollationNameEncNspIndexId: Oid = 3164;

/// `CollationOidIndexId` — `pg_collation_oid_index` OID (unique on `oid`).
pub const CollationOidIndexId: Oid = 3085;

// ---------------------------------------------------------------------------
// Column numbers (catalog/pg_collation_d.h `Anum_pg_collation_*`).
//
// FormData_pg_collation field order (pg_collation.h):
//   oid, collname, collnamespace, collowner, collprovider,
//   collisdeterministic, collencoding,
//   [CATALOG_VARLEN] collcollate, collctype, colllocale, collicurules,
//   collversion
// ---------------------------------------------------------------------------

/// `Anum_pg_collation_oid` = 1.
pub const Anum_pg_collation_oid: i32 = 1;
/// `Anum_pg_collation_collname` = 2.
pub const Anum_pg_collation_collname: i32 = 2;
/// `Anum_pg_collation_collnamespace` = 3.
pub const Anum_pg_collation_collnamespace: i32 = 3;
/// `Anum_pg_collation_collowner` = 4.
pub const Anum_pg_collation_collowner: i32 = 4;
/// `Anum_pg_collation_collprovider` = 5.
pub const Anum_pg_collation_collprovider: i32 = 5;
/// `Anum_pg_collation_collisdeterministic` = 6.
pub const Anum_pg_collation_collisdeterministic: i32 = 6;
/// `Anum_pg_collation_collencoding` = 7.
pub const Anum_pg_collation_collencoding: i32 = 7;
/// `Anum_pg_collation_collcollate` = 8 (text, `_null_` default).
pub const Anum_pg_collation_collcollate: i32 = 8;
/// `Anum_pg_collation_collctype` = 9 (text, `_null_` default).
pub const Anum_pg_collation_collctype: i32 = 9;
/// `Anum_pg_collation_colllocale` = 10 (text, `_null_` default).
pub const Anum_pg_collation_colllocale: i32 = 10;
/// `Anum_pg_collation_collicurules` = 11 (text, `_null_` default).
pub const Anum_pg_collation_collicurules: i32 = 11;
/// `Anum_pg_collation_collversion` = 12 (text, `_null_` default).
pub const Anum_pg_collation_collversion: i32 = 12;

/// `Natts_pg_collation` = 12.
pub const Natts_pg_collation: usize = 12;

// ---------------------------------------------------------------------------
// Provider codes (`COLLPROVIDER_*`, pg_collation.h `EXPOSE_TO_CLIENT_CODE`).
// ---------------------------------------------------------------------------

/// `COLLPROVIDER_DEFAULT` — the `char` value `'d'`.
pub const COLLPROVIDER_DEFAULT: i8 = b'd' as i8;
/// `COLLPROVIDER_BUILTIN` — the `char` value `'b'`.
pub const COLLPROVIDER_BUILTIN: i8 = b'b' as i8;
/// `COLLPROVIDER_ICU` — the `char` value `'i'`.
pub const COLLPROVIDER_ICU: i8 = b'i' as i8;
/// `COLLPROVIDER_LIBC` — the `char` value `'c'`.
pub const COLLPROVIDER_LIBC: i8 = b'c' as i8;
