//! Multixact-related shared value types (`access/multixact.h`,
//! `access/heapam.c`).

/// The result of `DoesMultiXactIdConflict` (heapam.c) — C's `bool` return plus
/// the `*current_is_member` out param.
#[derive(Clone, Copy, Debug)]
pub struct MultiXactConflict {
    /// C's `bool` return: does the multixact conflict with the wanted mode?
    pub conflict: bool,
    /// C's `*current_is_member`: is the current backend already a member?
    pub current_is_member: bool,
}
