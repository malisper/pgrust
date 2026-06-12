//! Seam declarations for the `common-ip` unit (`src/common/ip.c`): the
//! getaddrinfo/getnameinfo wrappers with Unix-socket-path support.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.
//!
//! `pg_freeaddrinfo_all` has no seam: the result list is an owned
//! `Vec<PgAddrInfo>`, so dropping it is the free.

seam_core::seam!(
    /// `pg_getaddrinfo_all(hostname, servname, hintp, result)` — resolve a
    /// host/service pair (or a Unix socket path) into a list of addresses
    /// appended to `result`. Returns `0` on success or a `getaddrinfo`
    /// `EAI_*` error code (render with `gai_strerror`). Never ereports.
    pub fn pg_getaddrinfo_all(
        hostname: Option<&str>,
        servname: Option<&str>,
        hint: &types_net::AddrInfoHint,
        result: &mut Vec<types_net::PgAddrInfo>,
    ) -> i32
);

seam_core::seam!(
    /// `pg_getnameinfo_all(addr, salen, node, nodelen, service, servicelen,
    /// flags)` — render an address as text. The `node`/`service` out-buffers
    /// are marshaled as optional `String`s (filled only when `Some`).
    /// Returns `0` on success or an `EAI_*` code; on failure the buffers get
    /// the `???` placeholder, as in C. Never ereports.
    pub fn pg_getnameinfo_all(
        addr: &types_net::SockAddr,
        node: Option<&mut String>,
        service: Option<&mut String>,
        flags: i32,
    ) -> i32
);
