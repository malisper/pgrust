//! Seam declarations for the `backend-libpq-be-secure` unit
//! (`libpq/be-secure.c`): the TLS/GSS/raw socket transport over a `Port`.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `ssize_t secure_read(Port *port, void *ptr, size_t len)` — read up to
    /// `buf.len()` bytes from the connection. Returns the C convention:
    /// `Ok(n > 0)` bytes read, `Ok(0)` on EOF, `Ok(-1)` on error with the
    /// process `errno` set (the caller distinguishes `EINTR`/`EAGAIN`/
    /// `EWOULDBLOCK` from real trouble). `Err` carries the
    /// `ereport(ERROR/FATAL)`s reachable through the blocking-mode wait loop's
    /// interrupt processing (`ProcessClientReadInterrupt`).
    pub fn secure_read(port: &mut types_net::Port, buf: &mut [u8]) -> types_error::PgResult<isize>
);

seam_core::seam!(
    /// `ssize_t secure_write(Port *port, void *ptr, size_t len)` — write up
    /// to `buf.len()` bytes. Same result convention as [`secure_read`]:
    /// `Ok(n > 0)` written, `Ok(<= 0)` on would-block/error with `errno` set,
    /// `Err` for interrupt-processing reports
    /// (`ProcessClientWriteInterrupt`).
    pub fn secure_write(port: &mut types_net::Port, buf: &[u8]) -> types_error::PgResult<isize>
);

seam_core::seam!(
    /// `void secure_close(Port *port)` — close the SSL/GSS layer if active.
    /// Infallible in C.
    pub fn secure_close(port: &mut types_net::Port)
);
