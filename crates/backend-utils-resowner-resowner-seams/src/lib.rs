//! Seam declarations for the `backend-utils-resowner-resowner` unit
//! (`utils/resowner/resowner.c`) `CurrentResourceOwner` global, as consumed by
//! logical decoding's slot-advance helper (it saves/restores the executor's
//! resource owner across decoding).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_logical::ResourceOwnerHandle;

seam_core::seam!(
    /// Read `CurrentResourceOwner`.
    pub fn CurrentResourceOwner() -> ResourceOwnerHandle
);
seam_core::seam!(
    /// `CurrentResourceOwner = value`.
    pub fn set_CurrentResourceOwner(value: ResourceOwnerHandle)
);
