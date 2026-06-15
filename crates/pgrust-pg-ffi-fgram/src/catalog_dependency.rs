//! ABI types for the catalog dependency / object-address substrate.
//!
//! These mirror, with identical `repr(C)` layout and field order, the C
//! definitions in:
//!   * `catalog/objectaddress.h`  — `ObjectAddress`, `InvalidObjectAddress`
//!   * `catalog/dependency.h`     — `DependencyType`, `SharedDependencyType`,
//!                                  the `ObjectAddresses` typedef, the
//!                                  `PERFORM_DELETION_*` flags
//!   * `catalog/dependency.c`     — the file-private `ObjectAddressExtra` struct
//!                                  and the `DEPFLAG_*` flag bits
//!   * `catalog/pg_depend.h`      — `FormData_pg_depend` / `Form_pg_depend`
//!   * `catalog/pg_shdepend.h`    — `FormData_pg_shdepend` / `Form_pg_shdepend`
//!   * `catalog/pg_inherits.h`    — `FormData_pg_inherits` / `Form_pg_inherits`
//!   * `nodes/parsenodes.h`       — `DropBehavior`
//!
//! NB: PostgreSQL 18 removed the `ObjectClass` / `OCLASS_*` enum (the class-id
//! dispatch is now a table-driven lookup inside objectaddress.c), so no
//! `ObjectClass` type is defined here; it does not exist in 18.3.

use core::ffi::c_char;

use crate::types::Oid;

/* ---------------------------------------------------------------------------
 * ObjectAddress / ObjectAddresses / ObjectAddressExtra (objectaddress.h,
 * dependency.h, dependency.c)
 * ------------------------------------------------------------------------- */

/// `typedef struct ObjectAddress` — a database object of any type
/// (`catalog/objectaddress.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectAddress {
    /// Class Id from pg_class.
    pub classId: Oid,
    /// OID of the object.
    pub objectId: Oid,
    /// Subitem within object (eg column), or 0.
    pub objectSubId: i32,
}

/// `const ObjectAddress InvalidObjectAddress` — `{InvalidOid, InvalidOid, 0}`.
pub const InvalidObjectAddress: ObjectAddress = ObjectAddress {
    classId: crate::InvalidOid,
    objectId: crate::InvalidOid,
    objectSubId: 0,
};

/// `ObjectAddressExtra` — per-target deletion state (private to dependency.c).
///
/// `flags` is a bitmask of the `DEPFLAG_*` bits; `dependee` is the object
/// whose deletion forced this one.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectAddressExtra {
    /// Bitmask, see the `DEPFLAG_*` definitions.
    pub flags: i32,
    /// Object whose deletion forced this one.
    pub dependee: ObjectAddress,
}

/// `struct ObjectAddresses` — expansible list of `ObjectAddress`es (the
/// `typedef ObjectAddresses` is exposed by dependency.h; the struct body is
/// private to dependency.c).
///
/// `extras` is `NULL` when the array carries no per-target deletion state.
#[repr(C)]
#[derive(Debug)]
pub struct ObjectAddresses {
    /// palloc'd array of references.
    pub refs: *mut ObjectAddress,
    /// palloc'd array, or NULL if not used.
    pub extras: *mut ObjectAddressExtra,
    /// Current number of references.
    pub numrefs: i32,
    /// Current size of palloc'd array(s).
    pub maxrefs: i32,
}

/* ObjectAddressExtra flag bits (dependency.c) */
/// An original deletion target.
pub const DEPFLAG_ORIGINAL: i32 = 0x0001;
/// Reached via normal dependency.
pub const DEPFLAG_NORMAL: i32 = 0x0002;
/// Reached via auto dependency.
pub const DEPFLAG_AUTO: i32 = 0x0004;
/// Reached via internal dependency.
pub const DEPFLAG_INTERNAL: i32 = 0x0008;
/// Reached via partition dependency.
pub const DEPFLAG_PARTITION: i32 = 0x0010;
/// Reached via extension dependency.
pub const DEPFLAG_EXTENSION: i32 = 0x0020;
/// Reverse internal/extension link.
pub const DEPFLAG_REVERSE: i32 = 0x0040;
/// Has a partition dependency.
pub const DEPFLAG_IS_PART: i32 = 0x0080;
/// Subobject of another deletable object.
pub const DEPFLAG_SUBOBJECT: i32 = 0x0100;

/* ---------------------------------------------------------------------------
 * DependencyType / SharedDependencyType (dependency.h)
 *
 * Stored in a "char" field in pg_depend / pg_shdepend, so the enum members
 * take ASCII-code values.  Represented here as a transparent `c_char` newtype
 * to preserve the exact on-disk byte while keeping a distinct type.
 * ------------------------------------------------------------------------- */

/// `typedef enum DependencyType` (dependency.h).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependencyType(pub c_char);

impl DependencyType {
    pub const fn as_char(self) -> c_char {
        self.0
    }
}

pub const DEPENDENCY_NORMAL: DependencyType = DependencyType(b'n' as c_char);
pub const DEPENDENCY_AUTO: DependencyType = DependencyType(b'a' as c_char);
pub const DEPENDENCY_INTERNAL: DependencyType = DependencyType(b'i' as c_char);
pub const DEPENDENCY_PARTITION_PRI: DependencyType = DependencyType(b'P' as c_char);
pub const DEPENDENCY_PARTITION_SEC: DependencyType = DependencyType(b'S' as c_char);
pub const DEPENDENCY_EXTENSION: DependencyType = DependencyType(b'e' as c_char);
pub const DEPENDENCY_AUTO_EXTENSION: DependencyType = DependencyType(b'x' as c_char);

/// `typedef enum SharedDependencyType` (dependency.h).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SharedDependencyType(pub c_char);

impl SharedDependencyType {
    pub const fn as_char(self) -> c_char {
        self.0
    }
}

pub const SHARED_DEPENDENCY_OWNER: SharedDependencyType = SharedDependencyType(b'o' as c_char);
pub const SHARED_DEPENDENCY_ACL: SharedDependencyType = SharedDependencyType(b'a' as c_char);
pub const SHARED_DEPENDENCY_INITACL: SharedDependencyType = SharedDependencyType(b'i' as c_char);
pub const SHARED_DEPENDENCY_POLICY: SharedDependencyType = SharedDependencyType(b'r' as c_char);
pub const SHARED_DEPENDENCY_TABLESPACE: SharedDependencyType = SharedDependencyType(b't' as c_char);
/// Used as a parameter in internal routines; not valid in the catalog.
pub const SHARED_DEPENDENCY_INVALID: SharedDependencyType = SharedDependencyType(0);

/* ---------------------------------------------------------------------------
 * performDeletion / performMultipleDeletions flag bits (dependency.h)
 * ------------------------------------------------------------------------- */

/// Internal action.
pub const PERFORM_DELETION_INTERNAL: i32 = 0x0001;
/// Concurrent drop.
pub const PERFORM_DELETION_CONCURRENTLY: i32 = 0x0002;
/// Suppress notices.
pub const PERFORM_DELETION_QUIETLY: i32 = 0x0004;
/// Keep original obj.
pub const PERFORM_DELETION_SKIP_ORIGINAL: i32 = 0x0008;
/// Keep extensions.
pub const PERFORM_DELETION_SKIP_EXTENSIONS: i32 = 0x0010;
/// Normal drop with concurrent lock mode.
pub const PERFORM_DELETION_CONCURRENT_LOCK: i32 = 0x0020;

/* ---------------------------------------------------------------------------
 * DropBehavior (nodes/parsenodes.h)
 * ------------------------------------------------------------------------- */

/// `typedef enum DropBehavior` (parsenodes.h).  C-style `enum` with default
/// sequential discriminants, represented as a transparent `u32` newtype.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DropBehavior(pub u32);

/// Drop fails if any dependent objects.
pub const DROP_RESTRICT: DropBehavior = DropBehavior(0);
/// Remove dependent objects too.
pub const DROP_CASCADE: DropBehavior = DropBehavior(1);

/* ---------------------------------------------------------------------------
 * FormData_pg_depend (pg_depend.h, CATALOG pg_depend 2608)
 * ------------------------------------------------------------------------- */

/// `FormData_pg_depend` — one row of the pg_depend catalog.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_depend {
    /// OID of table containing the dependent (referencing) object.
    pub classid: Oid,
    /// OID of the dependent object itself.
    pub objid: Oid,
    /// Column number of the dependent object, or 0 if not used.
    pub objsubid: i32,
    /// OID of table containing the referenced object.
    pub refclassid: Oid,
    /// OID of the referenced object itself.
    pub refobjid: Oid,
    /// Column number of the referenced object, or 0 if not used.
    pub refobjsubid: i32,
    /// See `DependencyType` codes.
    pub deptype: c_char,
}

pub type Form_pg_depend = *mut FormData_pg_depend;

/* ---------------------------------------------------------------------------
 * FormData_pg_shdepend (pg_shdepend.h, CATALOG pg_shdepend 1214 SHARED)
 * ------------------------------------------------------------------------- */

/// `FormData_pg_shdepend` — one row of the pg_shdepend catalog.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_shdepend {
    /// OID of database containing the dependent object (0 for a shared object).
    pub dbid: Oid,
    /// OID of table containing the dependent (referencing) object.
    pub classid: Oid,
    /// OID of the dependent object itself.
    pub objid: Oid,
    /// Column number of the dependent object, or 0 if not used.
    pub objsubid: i32,
    /// OID of table containing the referenced (always shared) object.
    pub refclassid: Oid,
    /// OID of the referenced object itself.
    pub refobjid: Oid,
    /// See `SharedDependencyType` codes.
    pub deptype: c_char,
}

pub type Form_pg_shdepend = *mut FormData_pg_shdepend;

/* ---------------------------------------------------------------------------
 * FormData_pg_inherits (pg_inherits.h, CATALOG pg_inherits 2611)
 * ------------------------------------------------------------------------- */

/// `FormData_pg_inherits` — one row of the pg_inherits catalog.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_inherits {
    /// OID of the child relation.
    pub inhrelid: Oid,
    /// OID of the parent relation.
    pub inhparent: Oid,
    /// Inheritance sequence number.
    pub inhseqno: i32,
    /// Whether a partition detach is pending.
    pub inhdetachpending: bool,
}

pub type Form_pg_inherits = *mut FormData_pg_inherits;

/* ---------------------------------------------------------------------------
 * Catalog OIDs for the dependency/inherits catalogs (catalog/pg_*_d.h).
 * These are the bootstrap relation/index OIDs the substrate scans against.
 * ------------------------------------------------------------------------- */

/// `DependRelationId` — pg_depend.
pub const DEPEND_RELATION_ID: Oid = 2608;
/// `DependDependerIndexId` — pg_depend_depender_index.
pub const DEPEND_DEPENDER_INDEX_ID: Oid = 2673;
/// `DependReferenceIndexId` — pg_depend_reference_index.
pub const DEPEND_REFERENCE_INDEX_ID: Oid = 2674;

/// `InheritsRelationId` — pg_inherits.
pub const INHERITS_RELATION_ID: Oid = 2611;
/// `InheritsRelidSeqnoIndexId` — pg_inherits_relid_seqno_index.
pub const INHERITS_RELID_SEQNO_INDEX_ID: Oid = 2680;
/// `InheritsParentIndexId` — pg_inherits_parent_index.
pub const INHERITS_PARENT_INDEX_ID: Oid = 2187;

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn object_address_layout() {
        assert_eq!(size_of::<ObjectAddress>(), 12);
        assert_eq!(align_of::<ObjectAddress>(), 4);
        assert_eq!(offset_of!(ObjectAddress, classId), 0);
        assert_eq!(offset_of!(ObjectAddress, objectId), 4);
        assert_eq!(offset_of!(ObjectAddress, objectSubId), 8);
    }

    #[test]
    fn invalid_object_address_is_zeroed() {
        assert_eq!(InvalidObjectAddress.classId, crate::InvalidOid);
        assert_eq!(InvalidObjectAddress.objectId, crate::InvalidOid);
        assert_eq!(InvalidObjectAddress.objectSubId, 0);
    }

    #[test]
    fn object_address_extra_layout() {
        // flags (int, 4) then ObjectAddress (12), 4-byte aligned => 16 bytes.
        assert_eq!(offset_of!(ObjectAddressExtra, flags), 0);
        assert_eq!(offset_of!(ObjectAddressExtra, dependee), 4);
        assert_eq!(size_of::<ObjectAddressExtra>(), 16);
    }

    #[test]
    fn pg_depend_layout() {
        assert_eq!(offset_of!(FormData_pg_depend, classid), 0);
        assert_eq!(offset_of!(FormData_pg_depend, objid), 4);
        assert_eq!(offset_of!(FormData_pg_depend, objsubid), 8);
        assert_eq!(offset_of!(FormData_pg_depend, refclassid), 12);
        assert_eq!(offset_of!(FormData_pg_depend, refobjid), 16);
        assert_eq!(offset_of!(FormData_pg_depend, refobjsubid), 20);
        assert_eq!(offset_of!(FormData_pg_depend, deptype), 24);
    }

    #[test]
    fn pg_shdepend_layout() {
        assert_eq!(offset_of!(FormData_pg_shdepend, dbid), 0);
        assert_eq!(offset_of!(FormData_pg_shdepend, classid), 4);
        assert_eq!(offset_of!(FormData_pg_shdepend, objid), 8);
        assert_eq!(offset_of!(FormData_pg_shdepend, objsubid), 12);
        assert_eq!(offset_of!(FormData_pg_shdepend, refclassid), 16);
        assert_eq!(offset_of!(FormData_pg_shdepend, refobjid), 20);
        assert_eq!(offset_of!(FormData_pg_shdepend, deptype), 24);
    }

    #[test]
    fn pg_inherits_layout() {
        assert_eq!(offset_of!(FormData_pg_inherits, inhrelid), 0);
        assert_eq!(offset_of!(FormData_pg_inherits, inhparent), 4);
        assert_eq!(offset_of!(FormData_pg_inherits, inhseqno), 8);
        assert_eq!(offset_of!(FormData_pg_inherits, inhdetachpending), 12);
    }

    #[test]
    fn dependency_type_codes() {
        assert_eq!(DEPENDENCY_NORMAL.as_char(), b'n' as c_char);
        assert_eq!(DEPENDENCY_AUTO_EXTENSION.as_char(), b'x' as c_char);
        assert_eq!(SHARED_DEPENDENCY_INVALID.as_char(), 0);
    }
}
