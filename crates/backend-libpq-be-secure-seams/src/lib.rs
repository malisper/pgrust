//! Seam declarations for the `backend-libpq-be-secure` unit
//! (`libpq/be-secure.c`): the TLS/GSS/raw socket transport over a `Port`.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.
//!
//! The C functions return `ssize_t` (`> 0` bytes, `0` EOF, `-1` error with
//! the process `errno` set). The seams carry the EOF/errno outcome
//! explicitly as [`types_net::SockResult`] instead of routing it through
//! ambient process state: the owner marshals `0` to `Err(SockError::Eof)`
//! and `-1` to `Err(SockError::Errno(errno))`.

seam_core::seam!(
    /// `ssize_t secure_read(Port *port, void *ptr, size_t len)` — read up to
    /// `buf.len()` bytes from the connection. `Ok(Ok(n))` bytes read,
    /// `Ok(Err(Eof))` on EOF, `Ok(Err(Errno(e)))` on error (the caller
    /// distinguishes `EINTR`/`EAGAIN`/`EWOULDBLOCK` from real trouble).
    /// `Err` carries the `ereport(ERROR/FATAL)`s reachable through the
    /// blocking-mode wait loop's interrupt processing
    /// (`ProcessClientReadInterrupt`).
    pub fn secure_read(
        port: &mut types_net::Port,
        buf: &mut [u8],
    ) -> types_error::PgResult<types_net::SockResult>
);

seam_core::seam!(
    /// `ssize_t secure_write(Port *port, const void *ptr, size_t len)` —
    /// write up to `buf.len()` bytes. Same result convention as
    /// [`secure_read`] (`Eof` marshals the never-in-practice `0` return);
    /// `Err` for interrupt-processing reports
    /// (`ProcessClientWriteInterrupt`).
    pub fn secure_write(
        port: &mut types_net::Port,
        buf: &[u8],
    ) -> types_error::PgResult<types_net::SockResult>
);

seam_core::seam!(
    /// `void secure_close(Port *port)` — close the SSL/GSS layer if active.
    /// Infallible in C.
    pub fn secure_close(port: &mut types_net::Port)
);
