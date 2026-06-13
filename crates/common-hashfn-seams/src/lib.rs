//! Seam declarations for the `common-hashfn` unit (`common/hashfn.c`): the
//! hash primitives callers use as building blocks (the integer mixer behind
//! `hash_uint32`, plus the byte-tag / string key helpers dshash's built-in
//! `dshash_memhash`/`dshash_strhash` key-helper sets forward to).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Pure computation; infallible. The keys are
//! read-only byte images, so they cross the seam as `&[u8]` (the C
//! `const void *key` over the first `keysize` bytes).

use types_core::{uint32, Size};

seam_core::seam!(
    /// `hash_bytes_uint32(k)` (`common/hashfn.c`): hash a 32-bit value to a
    /// 32-bit value (the murmur-style mixer behind `hash_uint32`).
    pub fn hash_bytes_uint32(k: u32) -> u32
);

seam_core::seam!(
    /// `hash_bytes_uint32_extended(k, seed)` (`common/hashfn.c`): hash a 32-bit
    /// value to a 64-bit value with a seed (the mixer behind
    /// `hash_uint32_extended`).
    pub fn hash_bytes_uint32_extended(k: u32, seed: u64) -> u64
);

seam_core::seam!(
    /// `uint32 tag_hash(const void *key, Size keysize)` (`common/hashfn.h`) —
    /// hash any fixed-size byte tag.
    pub fn tag_hash(key: &[u8], keysize: Size) -> uint32
);

seam_core::seam!(
    /// `uint32 string_hash(const void *key, Size keysize)` (`common/hashfn.h`) —
    /// hash a NUL-terminated string occupying up to `keysize` bytes.
    pub fn string_hash(key: &[u8], keysize: Size) -> uint32
);
