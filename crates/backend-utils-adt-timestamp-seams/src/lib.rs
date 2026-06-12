//! Seam declarations for the `backend-utils-adt-timestamp` unit
//! (`src/backend/utils/adt/timestamp.c`). The owning unit installs these from
//! its `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `GetCurrentTimestamp()` (`timestamp.c`): current time as a
    /// `TimestampTz` (microseconds since the Postgres epoch).
    pub fn get_current_timestamp() -> types_core::TimestampTz
);
