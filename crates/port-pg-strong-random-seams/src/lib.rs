//! Seam declarations for `port/pg_strong_random.c` and the real-time clock
//! read used by UUID v7 generation.
//!
//! These are genuinely-external OS/CSPRNG facilities (`pg_strong_random`,
//! `clock_gettime(CLOCK_REALTIME)` / `gettimeofday`). Their owner
//! (`port-pg-strong-random`) is not ported yet, so a call panics loudly until
//! it lands.

seam_core::seam!(
    /// `pg_strong_random(buf, len)` (port/pg_strong_random.c): fill `buf` with
    /// `buf.len()` cryptographically strong random bytes. Returns `false` if the
    /// platform CSPRNG could not produce them.
    pub fn pg_strong_random(buf: &mut [u8]) -> bool
);

seam_core::seam!(
    /// The current real timestamp in nanoseconds since the UNIX epoch
    /// (`clock_gettime(CLOCK_REALTIME)` combined as `tv_sec * 1e9 + tv_nsec`, or
    /// `gettimeofday` as `tv_sec * 1e9 + tv_usec * 1e3` on platforms without
    /// nanosecond precision). uuid.c reads this in `get_real_time_ns_ascending`.
    pub fn clock_realtime_ns() -> i64
);
