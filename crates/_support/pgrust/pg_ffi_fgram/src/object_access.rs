use crate::types::Oid;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectAccessType(pub u32);

pub const OAT_POST_CREATE: ObjectAccessType = ObjectAccessType(0);
pub const OAT_DROP: ObjectAccessType = ObjectAccessType(1);
pub const OAT_POST_ALTER: ObjectAccessType = ObjectAccessType(2);
pub const OAT_NAMESPACE_SEARCH: ObjectAccessType = ObjectAccessType(3);
pub const OAT_FUNCTION_EXECUTE: ObjectAccessType = ObjectAccessType(4);
pub const OAT_TRUNCATE: ObjectAccessType = ObjectAccessType(5);

/// `ObjectAccessPostCreate` (`catalog/objectaccess.h:61-69`) — argument struct
/// for an `OAT_POST_CREATE` hook invocation.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectAccessPostCreate {
    /// Whether the create was made for the system's internal use (no
    /// user-issued statement).
    pub is_internal: bool,
}

/// `ObjectAccessDrop` (`catalog/objectaccess.h:71-81`) — argument struct for an
/// `OAT_DROP` hook invocation.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectAccessDrop {
    /// One or more of the `PERFORM_DELETION_*` flags.
    pub dropflags: i32,
}

/// `ObjectAccessPostAlter` (`catalog/objectaccess.h:83-103`) — argument struct
/// for an `OAT_POST_ALTER` hook invocation.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectAccessPostAlter {
    /// Secondary OID associated with the alter (e.g. the owner role for an
    /// ALTER ... OWNER TO).
    pub auxiliary_id: Oid,
    /// Whether the alter was made for the system's internal use.
    pub is_internal: bool,
}

/// `ObjectAccessNamespaceSearch` (`catalog/objectaccess.h:105-124`) — argument
/// struct for an `OAT_NAMESPACE_SEARCH` hook invocation.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectAccessNamespaceSearch {
    /// In: whether the caller will raise an error on a permission denial.
    pub ereport_on_violation: bool,
    /// Out: whether the access was allowed (or hook found no objection).
    pub result: bool,
}
