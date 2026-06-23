//! Seam declarations for GUC variables defined in `utils/misc/guc_tables.c`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `cluster_name` (guc_tables.c) — the `cluster_name` GUC string (boot
    /// value `""`); the seam returns an owned copy.
    pub fn cluster_name() -> String
);

seam_core::seam!(
    /// `restrict_nonsystem_relation_kind` (`tcop/tcopprot.h`) — the parsed
    /// bitmask of the `restrict_nonsystem_relation_kind` GUC (the int the
    /// `assign_restrict_nonsystem_relation_kind` hook stores from the string
    /// list; boot value `0`). Read by `GetFdwRoutine` (`foreign/foreign.c`) for
    /// its `& RESTRICT_RELKIND_FOREIGN_TABLE` test. Per-process global, so a
    /// plain `i32` read with no allocation.
    pub fn restrict_nonsystem_relation_kind() -> i32
);
