//! Seam declarations for path/process helpers `miscinit.c` reaches:
//! `make_absolute_path`/`first_dir_separator` (`src/port/path.c`),
//! `getppid` (libc), the `kill(pid,0)` liveness probe, `PostPortNumber`
//! (`postmaster.c`), and `utime` (`miscinit.c`'s socket-lock touch).
//!
//! The owners are not all ported; calls panic until they land.

seam_core::seam!(
    /// `make_absolute_path(path)` (`src/port/path.c`) — if the path is relative,
    /// prepend the current working directory. Returns the owned absolute path.
    /// C `ereport(ERROR)`s on `getcwd`/`malloc` failure.
    pub fn make_absolute_path(path: &str) -> types_error::PgResult<String>
);

seam_core::seam!(
    /// `first_dir_separator(filename)` (`src/port/path.c`) — byte offset of the
    /// first directory separator, or `None` if the name has no separator (used
    /// by `load_libraries` to decide whether to prefix `$libdir/plugins/`).
    pub fn first_dir_separator(filename: &str) -> Option<usize>
);

seam_core::seam!(
    /// `getppid()` (libc) — the parent process id (the stale-lock ancestor-PID
    /// heuristic in `CreateLockFile`).
    pub fn getppid() -> i32
);

seam_core::seam!(
    /// `kill(pid, 0) == 0 || (errno != ESRCH && errno != EPERM)` — whether the
    /// PID in a stale lock file appears to belong to a live process.
    pub fn pid_appears_live(pid: i32) -> bool
);

seam_core::seam!(
    /// `PostPortNumber` (`postmaster.c`) — the configured listen port written
    /// to the lock-file's port line (default 5432).
    pub fn post_port_number() -> i32
);

seam_core::seam!(
    /// `utime(path, NULL)` — bump the socket lock file's access/mod times so a
    /// /tmp-cleaner does not remove it. Any error is ignored (C `(void)`).
    pub fn touch_file_times(path: &str)
);
