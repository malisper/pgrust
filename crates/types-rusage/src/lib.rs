//! `PGRUsage` (`utils/pg_rusage.h`) — the resource-usage snapshot struct that
//! `pg_rusage_init`/`pg_rusage_show` populate and render. The C type is
//!
//! ```c
//! typedef struct PGRUsage { struct timeval tv; struct rusage ru; } PGRUsage;
//! ```
//!
//! a fully spelled-out stack struct that callers (cluster, vacuum, analyze,
//! index builds, tuplesort) declare as a local and pass by address. We mirror
//! it as a real owned struct so consumers can stack-allocate it exactly as the
//! C does, rather than threading an opaque handle. `pg_rusage_show` only ever
//! reads the wall-clock time and the two CPU `struct timeval`s out of the
//! `rusage`, so the struct carries exactly those three `timeval`s.

#![allow(non_snake_case)]

/// One `struct timeval`: whole seconds plus the microseconds-within-the-second
/// remainder. Stored as `i64` so a platform 64-bit `time_t`/`suseconds_t` is
/// preserved exactly; the deltas are narrowed to `int` when formatted, as in C.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Timeval {
    /// `tv_sec` — whole seconds.
    pub tv_sec: i64,
    /// `tv_usec` — microseconds within the second, in `[0, 999999]`.
    pub tv_usec: i64,
}

impl Timeval {
    /// Construct a `Timeval` from its `(tv_sec, tv_usec)` parts.
    pub const fn new(tv_sec: i64, tv_usec: i64) -> Self {
        Self { tv_sec, tv_usec }
    }
}

/// `PGRUsage` — the state struct `pg_rusage_init` fills and `pg_rusage_show`
/// renders. The C struct holds an entire `struct timeval tv` and `struct rusage
/// ru`, but only the wall-clock time and the user/system CPU `struct timeval`s
/// are ever read, so we carry exactly those three.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgRUsage {
    /// `tv` — wall-clock time, from `gettimeofday()`.
    pub tv: Timeval,
    /// `ru.ru_utime` — user-CPU time, from `getrusage(RUSAGE_SELF)`.
    pub ru_utime: Timeval,
    /// `ru.ru_stime` — system-CPU time, from `getrusage(RUSAGE_SELF)`.
    pub ru_stime: Timeval,
}
