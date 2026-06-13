//! Seam declarations for the `backend-catalog-pg-enum` unit
//! (`catalog/pg_enum.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `AtEOXact_Enum()` — discard the uncommitted-enum-value bookkeeping.
    pub fn at_eoxact_enum()
);
