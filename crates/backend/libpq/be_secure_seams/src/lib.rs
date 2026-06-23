//! Seam declarations for the `backend-libpq-be-secure` unit
//! (`libpq/be-secure.c`): the TLS/GSS/raw socket transport over a `Port`.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.
//!
//! The C functions return `ssize_t` (`> 0` bytes, `0` EOF, `-1` error with
//! the process `errno` set). The seams carry the EOF/errno outcome
//! explicitly as [`net::SockResult`] instead of routing it through
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
        port: &mut net::Port,
        buf: &mut [u8],
    ) -> types_error::PgResult<net::SockResult>
);

seam_core::seam!(
    /// `ssize_t secure_write(Port *port, const void *ptr, size_t len)` —
    /// write up to `buf.len()` bytes. Same result convention as
    /// [`secure_read`] (`Eof` marshals the never-in-practice `0` return);
    /// `Err` for interrupt-processing reports
    /// (`ProcessClientWriteInterrupt`).
    pub fn secure_write(
        port: &mut net::Port,
        buf: &[u8],
    ) -> types_error::PgResult<net::SockResult>
);

seam_core::seam!(
    /// `void secure_close(Port *port)` — close the SSL/GSS layer if active.
    /// Infallible in C.
    pub fn secure_close(port: &mut net::Port)
);

seam_core::seam!(
    /// `ssize_t secure_raw_read(Port *port, void *ptr, size_t len)` — read the
    /// raw (still-encrypted) bytes off the socket, consuming the Port's
    /// startup-packet `raw_buf` first. The OpenSSL `port_bio_read` BIO callback
    /// routes the TLS transport's reads through here. Returns the `ssize_t`
    /// directly (`> 0` bytes, `0` EOF, `-1` error with the process `errno`
    /// set), matching the C: the BIO consumer reads `errno` itself.
    pub fn secure_raw_read(port: &mut net::Port, buf: &mut [u8]) -> isize
);

seam_core::seam!(
    /// `ssize_t secure_raw_write(Port *port, const void *ptr, size_t len)` —
    /// write raw (already-encrypted) bytes to the socket. The OpenSSL
    /// `port_bio_write` BIO callback routes the TLS transport's writes through
    /// here. Returns the `ssize_t` directly (same convention as
    /// [`secure_raw_read`]).
    pub fn secure_raw_write(port: &mut net::Port, buf: &[u8]) -> isize
);

// --- backend-utils-init-postinit consumer (be-secure.c TLS accessors) ---

seam_core::seam!(
    /// `const char *be_tls_get_version(Port *port)` (`libpq/be-secure.c`) — the
    /// negotiated TLS protocol version string for the connection, used by
    /// `PerformAuthentication`'s `" SSL enabled (protocol=%s, ...)"` log line.
    /// Reads SSL state postinit does not own.
    pub fn be_tls_get_version<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        port: &mut net::Port,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `const char *be_tls_get_cipher(Port *port)` (`libpq/be-secure.c`) — the
    /// negotiated TLS cipher name string for the connection.
    pub fn be_tls_get_cipher<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        port: &mut net::Port,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `int be_tls_get_cipher_bits(Port *port)` (`libpq/be-secure.c`) — the
    /// number of effective bits in the negotiated TLS cipher.
    pub fn be_tls_get_cipher_bits(port: &mut net::Port) -> i32
);

seam_core::seam!(
    /// `void be_tls_get_peer_subject_name(Port *port, char *ptr, size_t len)`
    /// (`libpq/be-secure.c`) — the peer certificate's subject distinguished name
    /// (empty if none), recorded by `backend_status.c`'s `pgstat_bestart_security`.
    pub fn be_tls_get_peer_subject_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        port: &mut net::Port,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `void be_tls_get_peer_serial(Port *port, char *ptr, size_t len)`
    /// (`libpq/be-secure.c`) — the peer certificate's serial number string.
    pub fn be_tls_get_peer_serial<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        port: &mut net::Port,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `void be_tls_get_peer_issuer_name(Port *port, char *ptr, size_t len)`
    /// (`libpq/be-secure.c`) — the peer certificate's issuer distinguished name.
    pub fn be_tls_get_peer_issuer_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        port: &mut net::Port,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);

// ---------------------------------------------------------------------------
//  Negotiation guards + handshake openers (backend_startup.c crossings).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// Whether this build supports SSL (`#ifdef USE_SSL`). A compile-in guard
    /// backend_startup.c branches on; infallible.
    pub fn ssl_supported() -> bool
);

seam_core::seam!(
    /// Whether this build supports GSSAPI encryption (`#ifdef ENABLE_GSS`).
    /// A compile-in guard; infallible.
    pub fn gss_supported() -> bool
);

seam_core::seam!(
    /// `!LoadedSSL || port->laddr.addr.ss_family == AF_UNIX` — SSL is not
    /// offered for this connection (SSL disabled at runtime, or a Unix-domain
    /// socket). Infallible.
    pub fn ssl_negotiation_disabled(port: &mut net::Port) -> bool
);

seam_core::seam!(
    /// `port->laddr.addr.ss_family == AF_UNIX` — GSSAPI encryption is not
    /// offered over a Unix-domain socket. Infallible.
    pub fn gss_negotiation_disabled(port: &mut net::Port) -> bool
);

seam_core::seam!(
    /// `secure_open_server(Port *port)` (`libpq/be-secure.c`) — perform the
    /// server-side TLS handshake. Returns `0` on success or `-1` on failure
    /// (an appropriate TLS alert was already sent). Infallible at the ereport
    /// level.
    pub fn secure_open_server(port: &mut net::Port) -> i32
);

seam_core::seam!(
    /// `secure_open_gssapi(Port *port)` (`libpq/be-secure-gssapi.c`) — perform
    /// the server-side GSSAPI encryption handshake. Returns `0` on success or
    /// `-1` on failure. Infallible at the ereport level.
    pub fn secure_open_gssapi(port: &mut net::Port) -> i32
);
