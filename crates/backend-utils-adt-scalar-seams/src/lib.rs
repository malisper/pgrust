//! Seam declarations for the `backend-utils-adt-scalar` unit
//! (`utils/adt/bool.c` et al.). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `parse_bool(value, *result)` (`utils/adt/bool.c`) — parse a boolean GUC
    /// string ("true"/"false"/"on"/"off"/"yes"/"no"/"1"/"0", case-insensitive,
    /// unambiguous prefixes accepted). `Some(b)` on success (C returns `true`
    /// with `*result` set), `None` when the value is not a valid boolean (C
    /// returns `false`). Infallible at the ereport level.
    pub fn parse_bool(value: &str) -> Option<bool>
);
