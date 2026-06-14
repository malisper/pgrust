//! Seam declaration for `pg_check_dir` (`src/common/pgcheckdir.c`).
//!
//! The owning unit installs this from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pg_check_dir(const char *dir)` (`common/pgcheckdir.c`): inspect a
    /// directory and report its state. Returns:
    ///   * `0` — directory does not exist;
    ///   * `1` — exists and is empty;
    ///   * `2` — exists and contains only dot files;
    ///   * `3` — exists and contains a mount point (`lost+found`);
    ///   * `4` — exists and contains real data files;
    ///   * `-1` — access problem (with `errno` set on the live thread).
    pub fn pg_check_dir(dir: &str) -> i32
);
