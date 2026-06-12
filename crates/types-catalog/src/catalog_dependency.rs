//! Object-address and dependency vocabulary, mirroring with identical field
//! order the C definitions in `catalog/objectaddress.h` (`ObjectAddress`),
//! `catalog/dependency.h` (`DependencyType`), and `catalog/pg_depend.h`
//! (`FormData_pg_depend`), trimmed to the items the current ports consume.

use types_core::primitive::Oid;
use types_core::primitive::InvalidOid;

/// `DependRelationId` — `pg_depend` (`pg_depend_d.h`,
/// `CATALOG(pg_depend,2608,DependRelationId)`).
pub const DEPEND_RELATION_ID: Oid = 2608;

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
