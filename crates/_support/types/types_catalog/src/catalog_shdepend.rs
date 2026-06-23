//! Shared-dependency vocabulary, mirroring the C definitions in
//! `catalog/pg_shdepend.h` (`FormData_pg_shdepend`, the catalog/index OIDs,
//! the `Anum_*` column numbers) and the `SharedDependencyType` enum from
//! `catalog/dependency.h`, trimmed to what the pg_shdepend port consumes.

use types_core::primitive::AttrNumber;
use types_core::primitive::Oid;

/// `SharedDependRelationId` — `pg_shdepend`
/// (`CATALOG(pg_shdepend,1214,SharedDependRelationId)`).
pub const SHARED_DEPEND_RELATION_ID: Oid = 1214;
/// `SharedDependDependerIndexId` — `pg_shdepend_depender_index`, btree on
/// (dbid, classid, objid, objsubid) (`pg_shdepend.h` `DECLARE_INDEX`, OID
/// 1232).
pub const SharedDependDependerIndexId: Oid = 1232;
/// `SharedDependReferenceIndexId` — `pg_shdepend_reference_index`, btree on
/// (refclassid, refobjid) (`pg_shdepend.h` `DECLARE_INDEX`, OID 1233).
pub const SharedDependReferenceIndexId: Oid = 1233;

/* `Anum_pg_shdepend_*` (`pg_shdepend_d.h`) — attribute numbers in the CATALOG
 * field order of `catalog/pg_shdepend.h`. */
pub const Anum_pg_shdepend_dbid: AttrNumber = 1;
pub const Anum_pg_shdepend_classid: AttrNumber = 2;
pub const Anum_pg_shdepend_objid: AttrNumber = 3;
pub const Anum_pg_shdepend_objsubid: AttrNumber = 4;
pub const Anum_pg_shdepend_refclassid: AttrNumber = 5;
pub const Anum_pg_shdepend_refobjid: AttrNumber = 6;
pub const Anum_pg_shdepend_deptype: AttrNumber = 7;
/// `Natts_pg_shdepend` (`pg_shdepend_d.h`).
pub const Natts_pg_shdepend: usize = 7;

/// `typedef enum SharedDependencyType` (`catalog/dependency.h`). Stored in a
/// `char` column of pg_shdepend, so the members take ASCII-code values (and
/// `INVALID` is 0); represented as a transparent `i8` newtype to preserve the
/// exact on-disk byte while keeping a distinct type.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SharedDependencyType(pub i8);

impl SharedDependencyType {
    pub const fn as_char(self) -> i8 {
        self.0
    }
}

/// referenced object is the role owning the dependent object.
pub const SHARED_DEPENDENCY_OWNER: SharedDependencyType = SharedDependencyType(b'o' as i8);
/// referenced object is a role mentioned in the dependent's ACL.
pub const SHARED_DEPENDENCY_ACL: SharedDependencyType = SharedDependencyType(b'a' as i8);
/// referenced object is a role mentioned in a pg_init_privs ACL.
pub const SHARED_DEPENDENCY_INITACL: SharedDependencyType = SharedDependencyType(b'i' as i8);
/// referenced object is a role mentioned in a policy.
pub const SHARED_DEPENDENCY_POLICY: SharedDependencyType = SharedDependencyType(b'r' as i8);
/// referenced object is the dependent's default tablespace.
pub const SHARED_DEPENDENCY_TABLESPACE: SharedDependencyType = SharedDependencyType(b't' as i8);
/// internal-use sentinel (not stored).
pub const SHARED_DEPENDENCY_INVALID: SharedDependencyType = SharedDependencyType(0);

/// `FormData_pg_shdepend` — one row of the pg_shdepend catalog
/// (`catalog/pg_shdepend.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FormData_pg_shdepend {
    /// OID of database containing object (0 == shared object).
    pub dbid: Oid,
    /// OID of table containing the dependent object.
    pub classid: Oid,
    /// OID of the dependent object itself.
    pub objid: Oid,
    /// column number, or 0 if not used.
    pub objsubid: i32,
    /// OID of table containing the referenced object.
    pub refclassid: Oid,
    /// OID of the referenced object itself.
    pub refobjid: Oid,
    /// See `SharedDependencyType` codes.
    pub deptype: i8,
}
