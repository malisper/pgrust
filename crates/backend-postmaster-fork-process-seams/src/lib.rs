//! Seam declarations for the `backend-postmaster-fork-process` unit
//! (`src/backend/postmaster/fork_process.c`). The owning unit installs these
//! from its `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `fork_process()` (`fork_process.c`): wrapper for `fork()` that does
    /// the postmaster's bookkeeping around process creation. Returns the
    /// child pid in the parent, `0` in the child, `-1` on failure.
    pub fn fork_process() -> types_core::pid_t
);
