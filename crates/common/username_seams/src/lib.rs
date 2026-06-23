//! Seam declaration for `src/common/username.c`. The owning unit installs
//! this from its `init_seams()` when it lands; until then a call panics
//! loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `get_user_name_or_exit(progname)` (`common/username.c`): return the
    /// effective OS user name (via `getpwuid`), or print an error referencing
    /// `progname` to stderr and `exit(1)` if it cannot be determined. The
    /// frontend leg `exit(1)`s; modelled here as `Err` so the caller can map
    /// it to a fatal exit. On success returns the owned user name.
    pub fn get_user_name_or_exit(progname: &str) -> types_error::PgResult<String>
);
