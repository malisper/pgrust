//! Seam declarations for the `backend-utils-cache-typcache` unit
//! (`utils/cache/typcache.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `AtEOXact_TypeCache()`.
    pub fn at_eoxact_type_cache()
);

seam_core::seam!(
    /// `AtEOSubXact_TypeCache()`.
    pub fn at_eosubxact_type_cache()
);
