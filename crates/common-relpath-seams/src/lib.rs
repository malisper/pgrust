//! Seam declarations for the `common-relpath` unit (`src/common/relpath.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

extern crate alloc;

use alloc::string::String;

seam_core::seam!(
    /// `relpathbackend(rlocator, backend, forknum)` (relpath.h macro over
    /// `GetRelationPath`) — the on-disk path string of a relation fork.
    pub fn relpathbackend(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        forknum: types_core::primitive::ForkNumber,
    ) -> String
);
