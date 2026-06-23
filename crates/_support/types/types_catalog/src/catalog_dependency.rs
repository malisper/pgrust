//! Object-address and dependency vocabulary, mirroring with identical field
//! order the C definitions in `catalog/objectaddress.h` (`ObjectAddress`),
//! `catalog/dependency.h` (`DependencyType`), and `catalog/pg_depend.h`
//! (`FormData_pg_depend`), trimmed to the items the current ports consume.

use types_core::primitive::AttrNumber;
use types_core::primitive::InvalidOid;
use types_core::primitive::Oid;

/// `DependRelationId` — `pg_depend` (`pg_depend_d.h`,
/// `CATALOG(pg_depend,2608,DependRelationId)`).
pub const DEPEND_RELATION_ID: Oid = 2608;

/// `DependDependerIndexId` — `pg_depend_depender_index`, btree on
/// (classid, objid, objsubid) (`catalog/pg_depend.h` `DECLARE_INDEX`).
pub const DependDependerIndexId: Oid = 2673;
/// `DependReferenceIndexId` — `pg_depend_reference_index`, btree on
/// (refclassid, refobjid, refobjsubid) (`catalog/pg_depend.h`
/// `DECLARE_INDEX`).
pub const DependReferenceIndexId: Oid = 2674;

/* `Anum_pg_depend_*` (`pg_depend_d.h`) — attribute numbers in the CATALOG
 * field order of `catalog/pg_depend.h`. */
pub const Anum_pg_depend_classid: AttrNumber = 1;
pub const Anum_pg_depend_objid: AttrNumber = 2;
pub const Anum_pg_depend_objsubid: AttrNumber = 3;
pub const Anum_pg_depend_refclassid: AttrNumber = 4;
pub const Anum_pg_depend_refobjid: AttrNumber = 5;
pub const Anum_pg_depend_refobjsubid: AttrNumber = 6;
pub const Anum_pg_depend_deptype: AttrNumber = 7;
/// `Natts_pg_depend` (`pg_depend_d.h`).
pub const Natts_pg_depend: usize = 7;

/// `typedef struct ObjectAddress` — a database object of any type
/// (`catalog/objectaddress.h`).
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
    classId: InvalidOid,
    objectId: InvalidOid,
    objectSubId: 0,
};

/// `typedef enum DependencyType` (`catalog/dependency.h`). Stored in a `char`
/// column of pg_depend, so the members take ASCII-code values; represented as
/// a transparent `i8` newtype to preserve the exact on-disk byte while
/// keeping a distinct type.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependencyType(pub i8);

impl DependencyType {
    pub const fn as_char(self) -> i8 {
        self.0
    }
}

pub const DEPENDENCY_NORMAL: DependencyType = DependencyType(b'n' as i8);
pub const DEPENDENCY_AUTO: DependencyType = DependencyType(b'a' as i8);
pub const DEPENDENCY_INTERNAL: DependencyType = DependencyType(b'i' as i8);
pub const DEPENDENCY_PARTITION_PRI: DependencyType = DependencyType(b'P' as i8);
pub const DEPENDENCY_PARTITION_SEC: DependencyType = DependencyType(b'S' as i8);
pub const DEPENDENCY_EXTENSION: DependencyType = DependencyType(b'e' as i8);
pub const DEPENDENCY_AUTO_EXTENSION: DependencyType = DependencyType(b'x' as i8);

/// `FormData_pg_depend` — one row of the pg_depend catalog
/// (`catalog/pg_depend.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    pub deptype: i8,
}

/* ===========================================================================
 * Deletion-engine accumulator types and flag bits (catalog/dependency.c).
 * Owned by backend-catalog-dependency; defined here so the owner crate carries
 * real value types (no opaque handle in the owner).
 * ========================================================================= */

/// `ObjectAddressExtra` (dependency.c) — per-target deletion state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectAddressExtra {
    /// Bitmask, see the `DEPFLAG_*` bits.
    pub flags: i32,
    /// Object whose deletion forced this one.
    pub dependee: ObjectAddress,
}

/// `struct ObjectAddresses` (dependency.c) — expansible list of
/// `ObjectAddress`, optionally with parallel `extras`.
///
/// Owned-tree shape: the `refs`/`extras` `Vec`s grow on demand; `numrefs`
/// tracks the logical length and a non-empty `extras` is the C
/// `addrs->extras != NULL` flag. `maxrefs` mirrors the C initial capacity for
/// documentation parity only.
#[derive(Clone, Debug, Default)]
pub struct ObjectAddresses {
    /// The collected addresses.
    pub refs: Vec<ObjectAddress>,
    /// Parallel per-target extra state, or empty if not used.
    pub extras: Vec<ObjectAddressExtra>,
    /// Current number of references (== `refs.len()`).
    pub numrefs: i32,
    /// Current size of the C palloc'd array(s); documentation only.
    pub maxrefs: i32,
}

/* `ObjectAddressExtra` flag bits (dependency.c). */
/// `DEPFLAG_ORIGINAL` — an original deletion target.
pub const DEPFLAG_ORIGINAL: i32 = 0x0001;
/// `DEPFLAG_NORMAL` — reached via normal dependency.
pub const DEPFLAG_NORMAL: i32 = 0x0002;
/// `DEPFLAG_AUTO` — reached via auto dependency.
pub const DEPFLAG_AUTO: i32 = 0x0004;
/// `DEPFLAG_INTERNAL` — reached via internal dependency.
pub const DEPFLAG_INTERNAL: i32 = 0x0008;
/// `DEPFLAG_PARTITION` — reached via partition dependency.
pub const DEPFLAG_PARTITION: i32 = 0x0010;
/// `DEPFLAG_EXTENSION` — reached via extension dependency.
pub const DEPFLAG_EXTENSION: i32 = 0x0020;
/// `DEPFLAG_REVERSE` — reverse internal/extension link.
pub const DEPFLAG_REVERSE: i32 = 0x0040;
/// `DEPFLAG_IS_PART` — has a partition dependency.
pub const DEPFLAG_IS_PART: i32 = 0x0080;
/// `DEPFLAG_SUBOBJECT` — subobject of another deletable object.
pub const DEPFLAG_SUBOBJECT: i32 = 0x0100;
