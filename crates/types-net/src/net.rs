//! Per-connection state and process-latch types: `SockAddr` / `ClientSocket`
//! (`libpq/pqcomm.h`), `Port` and its `HbaLine` authentication line
//! (`libpq/libpq-be.h`, `libpq/hba.h`), and `Latch` (`storage/latch.h`).

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::sync::atomic::AtomicI32;

use types_core::{pgsocket, sig_atomic_t, ProtocolVersion, UserAuth};

/// `SockAddr` (`libpq/pqcomm.h`). `addr` mirrors the platform
/// `struct sockaddr_storage`, a fixed-size socket-address buffer
/// (`_SS_MAXSIZE` == 128 bytes); `salen` mirrors the platform `socklen_t`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SockAddr {
    pub addr: [u8; 128],
    pub salen: u32,
}

/// `ClientSocket` (`libpq/libpq-be.h`): an accepted socket plus the client's
/// remote address.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ClientSocket {
    pub sock: pgsocket,
    pub raddr: SockAddr,
}

/// `SCRAM_MAX_KEY_LEN` (`common/scram-common.h`) ==
/// `SCRAM_SHA_256_KEY_LEN` == `PG_SHA256_DIGEST_LENGTH` == 32.
pub const SCRAM_MAX_KEY_LEN: usize = 32;

/// Connection type of a `pg_hba.conf` entry (`libpq/hba.h` `enum ConnType`).
pub type ConnType = u32;
pub const ctLocal: ConnType = 0;
pub const ctHost: ConnType = 1;
pub const ctHostSSL: ConnType = 2;
pub const ctHostNoSSL: ConnType = 3;
pub const ctHostGSS: ConnType = 4;
pub const ctHostNoGSS: ConnType = 5;

/// IP-address comparison method (`libpq/hba.h` `enum IPCompareMethod`).
pub type IPCompareMethod = u32;
pub const ipCmpMask: IPCompareMethod = 0;
pub const ipCmpSameHost: IPCompareMethod = 1;
pub const ipCmpSameNet: IPCompareMethod = 2;
pub const ipCmpAll: IPCompareMethod = 3;

/// Client-certificate verification mode (`libpq/hba.h` `enum ClientCertMode`).
pub type ClientCertMode = u32;
pub const clientCertOff: ClientCertMode = 0;
pub const clientCertCA: ClientCertMode = 1;
pub const clientCertFull: ClientCertMode = 2;

/// Which certificate name to match against (`libpq/hba.h` `enum ClientCertName`).
pub type ClientCertName = u32;
pub const clientCertCN: ClientCertName = 0;
pub const clientCertDN: ClientCertName = 1;

/// A single string token lexed from an authentication configuration file
/// (`libpq/hba.h` `struct AuthToken`). The C struct also carries a
/// `regex_t *regex` (the compiled RE); no port consumes it yet, so the field
/// is trimmed until the regex-owning unit lands and defines the compiled-RE
/// type (docs/types.md rule 3).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthToken {
    /// Token text (`char *string`).
    pub string: Option<String>,
    /// Whether the token was quoted.
    pub quoted: bool,
}

/// Authentication line parsed from `pg_hba.conf` (`libpq/hba.h`
/// `struct HbaLine`). `addr` / `mask` mirror the platform
/// `struct sockaddr_storage`; the `radius*` lists hold the comma-split
/// option strings produced by `SplitGUCList`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HbaLine {
    pub sourcefile: Option<String>,
    pub linenumber: i32,
    pub rawline: Option<String>,
    pub conntype: ConnType,
    /// `List` of `AuthToken` matched database names.
    pub databases: Vec<AuthToken>,
    /// `List` of `AuthToken` matched role names.
    pub roles: Vec<AuthToken>,
    /// `struct sockaddr_storage addr`.
    pub addr: [u8; 128],
    /// Zero if we don't have a valid addr.
    pub addrlen: i32,
    /// `struct sockaddr_storage mask`.
    pub mask: [u8; 128],
    /// Zero if we don't have a valid mask.
    pub masklen: i32,
    pub ip_cmp_method: IPCompareMethod,
    pub hostname: Option<String>,
    pub auth_method: UserAuth,
    pub usermap: Option<String>,
    pub pamservice: Option<String>,
    pub pam_use_hostname: bool,
    pub ldaptls: bool,
    pub ldapscheme: Option<String>,
    pub ldapserver: Option<String>,
    pub ldapport: i32,
    pub ldapbinddn: Option<String>,
    pub ldapbindpasswd: Option<String>,
    pub ldapsearchattribute: Option<String>,
    pub ldapsearchfilter: Option<String>,
    pub ldapbasedn: Option<String>,
    pub ldapscope: i32,
    pub ldapprefix: Option<String>,
    pub ldapsuffix: Option<String>,
    pub clientcert: ClientCertMode,
    pub clientcertname: ClientCertName,
    pub krb_realm: Option<String>,
    pub include_realm: bool,
    pub compat_realm: bool,
    pub upn_username: bool,
    /// `List` of comma-split RADIUS server names.
    pub radiusservers: Vec<String>,
    pub radiusservers_s: Option<String>,
    /// `List` of comma-split RADIUS secrets.
    pub radiussecrets: Vec<String>,
    pub radiussecrets_s: Option<String>,
    /// `List` of comma-split RADIUS identifiers.
    pub radiusidentifiers: Vec<String>,
    pub radiusidentifiers_s: Option<String>,
    /// `List` of comma-split RADIUS ports.
    pub radiusports: Vec<String>,
    pub radiusports_s: Option<String>,
    pub oauth_issuer: Option<String>,
    pub oauth_scope: Option<String>,
    pub oauth_validator: Option<String>,
    pub oauth_skip_usermap: bool,
}

/// `struct Port` (`libpq/libpq-be.h`): per-connection state passed from the
/// postmaster into backend execution.
///
/// Trimmed relative to the C struct: the non-GSS build's `void *gss` (always
/// NULL, dead storage) is omitted, and the `USE_OPENSSL`-only `SSL *ssl` /
/// `X509 *peer` library handles are deferred until the TLS-owning unit
/// decides their representation — the no-TLS state is fully expressed by
/// `ssl_in_use` / `peer_cn` / `peer_dn` / `peer_cert_valid`.
///
/// `Default` is not derived: the `local_host` / SCRAM key / `SockAddr` byte
/// arrays exceed the length for which stdlib derives `Default`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Port {
    /// File descriptor.
    pub sock: pgsocket,
    /// Is the socket in non-blocking mode?
    pub noblock: bool,
    /// FE/BE protocol version.
    pub proto: ProtocolVersion,
    /// Local addr (postmaster).
    pub laddr: SockAddr,
    /// Remote addr (client).
    pub raddr: SockAddr,
    /// Name (or ip addr) of remote host.
    pub remote_host: Option<String>,
    /// Name (not ip addr) of remote host, if available.
    pub remote_hostname: Option<String>,
    pub remote_hostname_resolv: i32,
    pub remote_hostname_errcode: i32,
    /// Text rep of remote port.
    pub remote_port: Option<String>,

    /// Ip addr of local socket for client conn (`char local_host[64]`).
    pub local_host: [u8; 64],

    pub database_name: Option<String>,
    pub user_name: Option<String>,
    pub cmdline_options: Option<String>,
    /// `List *` of alternating GUC option names and values.
    pub guc_options: Vec<String>,

    /// Startup-packet application name (for the "connection authorized" log).
    pub application_name: Option<String>,

    /// Authentication line held during the authentication cycle.
    pub hba: Option<Box<HbaLine>>,

    /// TCP keepalive and user-timeout settings.
    pub default_keepalives_idle: i32,
    pub default_keepalives_interval: i32,
    pub default_keepalives_count: i32,
    pub default_tcp_user_timeout: i32,
    pub keepalives_idle: i32,
    pub keepalives_interval: i32,
    pub keepalives_count: i32,
    pub tcp_user_timeout: i32,

    /// SCRAM client key.
    pub scram_ClientKey: [u8; SCRAM_MAX_KEY_LEN],
    /// SCRAM server key.
    pub scram_ServerKey: [u8; SCRAM_MAX_KEY_LEN],
    /// True if the two SCRAM keys above are valid.
    pub has_scram_keys: bool,

    /// SSL state.
    pub ssl_in_use: bool,
    pub peer_cn: Option<String>,
    pub peer_dn: Option<String>,
    pub peer_cert_valid: bool,
    pub alpn_used: bool,
    pub last_read_was_eof: bool,

    /// Data previously read and "unread" for the SSL handshake. In C a
    /// `char *` of arbitrary bytes; an owned byte buffer preserves exact
    /// bytes (`Some`/`None` mirrors the C non-NULL/NULL test).
    pub raw_buf: Option<Vec<u8>>,
    /// `ssize_t raw_buf_consumed`.
    pub raw_buf_consumed: i64,
    /// `ssize_t raw_buf_remaining`.
    pub raw_buf_remaining: i64,
}

impl Port {
    /// Build a `Port` over an accepted socket fd: the all-zero/`None` field
    /// template the postmaster hands a freshly accepted backend, with only
    /// the socket descriptor filled in.
    pub fn for_socket(sock: pgsocket) -> Port {
        Port {
            sock,
            noblock: false,
            proto: 0,
            laddr: SockAddr {
                addr: [0; 128],
                salen: 0,
            },
            raddr: SockAddr {
                addr: [0; 128],
                salen: 0,
            },
            remote_host: None,
            remote_hostname: None,
            remote_hostname_resolv: 0,
            remote_hostname_errcode: 0,
            remote_port: None,
            local_host: [0; 64],
            database_name: None,
            user_name: None,
            cmdline_options: None,
            guc_options: Vec::new(),
            application_name: None,
            hba: None,
            default_keepalives_idle: 0,
            default_keepalives_interval: 0,
            default_keepalives_count: 0,
            default_tcp_user_timeout: 0,
            keepalives_idle: 0,
            keepalives_interval: 0,
            keepalives_count: 0,
            tcp_user_timeout: 0,
            scram_ClientKey: [0; SCRAM_MAX_KEY_LEN],
            scram_ServerKey: [0; SCRAM_MAX_KEY_LEN],
            has_scram_keys: false,
            ssl_in_use: false,
            peer_cn: None,
            peer_dn: None,
            peer_cert_valid: false,
            alpn_used: false,
            last_read_was_eof: false,
            raw_buf: None,
            raw_buf_consumed: 0,
            raw_buf_remaining: 0,
        }
    }
}

/// `struct Latch` (`storage/latch.h`).
///
/// In C a latch is always reached through a pointer (`MyLatch`,
/// `&proc->procLatch`) and is mutated concurrently: `SetLatch` runs from
/// signal handlers and, for shared latches living in PGPROC shared memory,
/// from other backends. The `volatile sig_atomic_t` wait/set fields are
/// therefore atomics here, and a latch is shared by handle
/// (e.g. `Arc<Latch>`), never copied by value. `is_shared` / `owner_pid` are
/// written only by `InitLatch`/`InitSharedLatch`/`OwnLatch` before the latch
/// is visible to other parties.
#[derive(Debug)]
pub struct Latch {
    /// `sig_atomic_t is_set;`
    pub is_set: AtomicI32,
    /// `sig_atomic_t maybe_sleeping;`
    pub maybe_sleeping: AtomicI32,
    /// `bool is_shared;`
    pub is_shared: bool,
    /// `int owner_pid;`
    pub owner_pid: i32,
}

impl Latch {
    /// A cleared latch (`is_set`/`maybe_sleeping` zero), as `InitLatch`
    /// leaves the flag fields.
    pub fn new(is_shared: bool, owner_pid: i32) -> Latch {
        Latch {
            is_set: AtomicI32::new(0),
            maybe_sleeping: AtomicI32::new(0),
            is_shared,
            owner_pid,
        }
    }
}

/// Assert the C field widths: `sig_atomic_t` is `int` on every supported
/// target and `AtomicI32` has the same in-memory representation.
const _: () = assert!(core::mem::size_of::<AtomicI32>() == core::mem::size_of::<sig_atomic_t>());
