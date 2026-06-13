//! Seam declarations for the `common-hashfn` unit (`common/hashfn.c`): the
//! integer hash primitive callers use as a building block.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Pure computation; infallible.

seam_core::seam!(
    /// `hash_bytes_uint32(k)` (`common/hashfn.c`): hash a 32-bit value to a
    /// 32-bit value (the murmur-style mixer behind `hash_uint32`).
    pub fn hash_bytes_uint32(k: u32) -> u32
);
