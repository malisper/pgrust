//! Inward seams for `src/common/wait_error.c` — interpret a `wait(2)`/`system()`
//! exit status. The owning unit installs these from its `init_seams()` when it
//! lands; until then a call panics loudly.

seam_core::seam!(
    /// `wait_result_is_signal(exit_status, signum)` (common/wait_error.c) — true
    /// if the child terminated due to the given signal.
    pub fn wait_result_is_signal(exit_status: i32, signum: i32) -> bool
);

seam_core::seam!(
    /// `wait_result_is_any_signal(exit_status, include_command_not_found)`
    /// (common/wait_error.c) — true if the child terminated due to any signal;
    /// when `include_command_not_found` is set, a shell exit code of 127
    /// ("command not found") also counts.
    pub fn wait_result_is_any_signal(exit_status: i32, include_command_not_found: bool) -> bool
);

seam_core::seam!(
    /// `wait_result_to_str(exit_status)` (common/wait_error.c) — render a child
    /// process's exit status as a human-readable string (the `palloc`'d-result
    /// analog returned as an owned `String`).
    pub fn wait_result_to_str(exit_status: i32) -> String
);
