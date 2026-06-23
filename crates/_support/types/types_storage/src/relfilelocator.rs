//! `RelFileLocator` (`storage/relfilelocator.h`) — re-exported from
//! [`crate::storage`], where the canonical definition lives.

use ::types_core::ProcNumber;

pub use crate::storage::{RelFileLocator, RelFileLocatorEquals};

/// `RelFileLocatorBackend` (`storage/relfilelocator.h`) — a `RelFileLocator`
/// paired with the owning backend's `ProcNumber`, identifying whether the
/// relation's storage is shared or backend-local (a temp relation). For a
/// non-temp relation `backend` is `INVALID_PROC_NUMBER`.
///
/// Field-for-field with the C struct:
/// ```c
/// typedef struct RelFileLocatorBackend
/// {
///     RelFileLocator locator;
///     ProcNumber  backend;
/// } RelFileLocatorBackend;
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct RelFileLocatorBackend {
    /// `locator` — the physical relation identity.
    pub locator: RelFileLocator,
    /// `backend` — owning backend's `ProcNumber`, or `INVALID_PROC_NUMBER`.
    pub backend: ProcNumber,
}
