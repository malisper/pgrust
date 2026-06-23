//! `ObjectAccessType`, the per-event `ObjectAccess*` argument structs, and the
//! `OAT_*` constants (`catalog/objectaccess.h`).

use types_core::primitive::Oid;

/// `ObjectAccessType` (`catalog/objectaccess.h:48-56`) — the kind of object
/// access being reported to the hook.
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
#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectAccessPostCreate {
    /// Whether the create was made for the system's internal use (no
    /// user-issued statement).
    pub is_internal: bool,
}

/// `ObjectAccessDrop` (`catalog/objectaccess.h:71-81`) — argument struct for an
/// `OAT_DROP` hook invocation.
#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectAccessDrop {
    /// One or more of the `PERFORM_DELETION_*` flags.
    pub dropflags: i32,
}

/// `ObjectAccessPostAlter` (`catalog/objectaccess.h:83-103`) — argument struct
/// for an `OAT_POST_ALTER` hook invocation.
#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectAccessPostAlter {
    /// Secondary OID associated with the alter (pg_inherits,
    /// pg_db_role_setting or pg_user_mapping; `InvalidOid` elsewhere).
    pub auxiliary_id: Oid,
    /// Whether the alter was made for the system's internal use.
    pub is_internal: bool,
}

/// `ObjectAccessNamespaceSearch` (`catalog/objectaccess.h:105-124`) — argument
/// struct for an `OAT_NAMESPACE_SEARCH` hook invocation.
#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectAccessNamespaceSearch {
    /// In: whether the hook should report an error on a permission denial.
    pub ereport_on_violation: bool,
    /// Out: whether the access was allowed. Core initializes this to `true`;
    /// a hook denies access by resetting it to `false`.
    pub result: bool,
}

/// Typed stand-in for the C hook's `void *arg`.
///
/// In C the hook receives a `void *` pointing at one of the `ObjectAccess*`
/// stack structs (or `NULL` for `OAT_TRUNCATE` / `OAT_FUNCTION_EXECUTE`). This
/// enum carries the same information by `&mut` borrow, so a hook can both read
/// and mutate the argument (e.g. clear [`ObjectAccessNamespaceSearch::result`]
/// to deny access) exactly as the C does through the pointer.
pub enum ObjectAccessArg<'a> {
    /// `OAT_POST_CREATE` argument (the C `&pc_arg`).
    PostCreate(&'a mut ObjectAccessPostCreate),
    /// `OAT_DROP` argument (the C `&drop_arg`).
    Drop(&'a mut ObjectAccessDrop),
    /// `OAT_POST_ALTER` argument (the C `&pa_arg`).
    PostAlter(&'a mut ObjectAccessPostAlter),
    /// `OAT_NAMESPACE_SEARCH` argument (the C `&ns_arg`); `result` is an
    /// out-parameter the hook may clear to deny access.
    NamespaceSearch(&'a mut ObjectAccessNamespaceSearch),
    /// No argument struct — the C passes `NULL` (`OAT_TRUNCATE`,
    /// `OAT_FUNCTION_EXECUTE`).
    None,
}
