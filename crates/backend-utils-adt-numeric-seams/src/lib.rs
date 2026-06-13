//! Seam declarations for the `backend-utils-adt-numeric` unit
//! (`utils/adt/numeric.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `numeric_maximum_size(typmod)` (numeric.c): the maximum on-disk size of
    /// a `numeric` value with the given typmod, or -1 if indeterminate. Pure
    /// arithmetic on the typmod-encoded precision/scale; no allocation, no
    /// error path.
    pub fn numeric_maximum_size(typmod: i32) -> i32
);
