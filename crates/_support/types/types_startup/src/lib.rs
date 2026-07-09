//! Startup vocabulary: `tcop/backend_startup.h`, the `libpq/libpq-be.h`
//! pre-auth subset (ClientSocket, Port) plus Port's hba surface and the
//! `libpq/hba.h` record types. SSL/GSS state is not this build.

#![allow(non_upper_case_globals)]

use ip::SockAddr;
use types_core::init::UserAuth;
use types_core::{pid_t, ProtocolVersion, TimestampTz};

pub type ConnType = u32;
pub const ctLocal: ConnType = 0;
pub const ctHost: ConnType = 1;
pub const ctHostSSL: ConnType = 2;
pub const ctHostNoSSL: ConnType = 3;
pub const ctHostGSS: ConnType = 4;
pub const ctHostNoGSS: ConnType = 5;

pub type IPCompareMethod = u32;
pub const ipCmpMask: IPCompareMethod = 0;
pub const ipCmpSameHost: IPCompareMethod = 1;
pub const ipCmpSameNet: IPCompareMethod = 2;
pub const ipCmpAll: IPCompareMethod = 3;

pub type ClientCertMode = u32;
pub const clientCertOff: ClientCertMode = 0;
pub const clientCertCA: ClientCertMode = 1;
pub const clientCertFull: ClientCertMode = 2;

pub type ClientCertName = u32;
pub const clientCertCN: ClientCertName = 0;
pub const clientCertDN: ClientCertName = 1;

// AuthToken (libpq/hba.h). C stores the compiled regex in the token; here
// parsed auth lines are shared across backend threads (RwLock statics in hba)
// and the engine carrier is Rc-based, so `regex` is the compiled-OK marker
// set by regcomp_auth_token (which validates by compiling); regexec_auth_token
// recompiles on the executing thread. token_has_regexp(t) == this flag.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthToken {
    pub string: String,
    pub quoted: bool,
    pub regex: bool,
}

// HbaLine (libpq/hba.h) scoped to the supported build: no ldap/radius/pam/
// krb/oauth option fields — their methods are rejected or deferred loud at
// parse time, so the fields are unrepresentable rather than silently unused.
#[derive(Clone, Debug)]
pub struct HbaLine {
    pub sourcefile: String,
    pub linenumber: i32,
    pub rawline: String,
    pub conntype: ConnType,
    pub databases: Vec<AuthToken>,
    pub roles: Vec<AuthToken>,
    pub addr: SockAddr,
    pub mask: SockAddr,
    pub ip_cmp_method: IPCompareMethod,
    pub hostname: Option<String>,
    pub auth_method: UserAuth,
    pub usermap: Option<String>,
    pub clientcert: ClientCertMode,
    pub clientcertname: ClientCertName,
}

impl HbaLine {
    pub fn new_zeroed() -> Self {
        Self {
            sourcefile: String::new(),
            linenumber: 0,
            rawline: String::new(),
            conntype: ctLocal,
            databases: Vec::new(),
            roles: Vec::new(),
            addr: SockAddr::zeroed(),
            mask: SockAddr::zeroed(),
            ip_cmp_method: ipCmpMask,
            hostname: None,
            auth_method: types_core::init::uaReject,
            usermap: None,
            clientcert: clientCertOff,
            clientcertname: clientCertCN,
        }
    }
}

pub const TIMESTAMP_MINUS_INFINITY: TimestampTz = i64::MIN;

// scram-common.h SCRAM_MAX_KEY_LEN (= SHA-256 digest length).
pub const SCRAM_MAX_KEY_LEN: usize = 32;

#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CacState {
    Ok = 0,
    Startup,
    Shutdown,
    Recovery,
    NotHotStandby,
    TooMany,
}

#[derive(Copy, Clone, Debug)]
pub struct BackendStartupData {
    pub can_accept_connections: CacState,
    pub socket_created: TimestampTz,
    pub fork_started: TimestampTz,
}

#[derive(Copy, Clone, Debug)]
pub struct BgWorkerStartupData {
    pub slot: i32,
    pub generation: u64,
}

#[derive(Copy, Clone, Debug)]
pub enum StartupData {
    None,
    Backend(BackendStartupData),
    BgWorker(BgWorkerStartupData),
}

#[derive(Copy, Clone, Debug)]
pub struct ClientSocket {
    pub sock: i32,
    pub raddr: SockAddr,
}

#[derive(Copy, Clone, Debug)]
pub struct ConnectionTiming {
    pub socket_create: TimestampTz,
    pub ready_for_use: TimestampTz,
    pub fork_start: TimestampTz,
    pub fork_end: TimestampTz,
    pub auth_start: TimestampTz,
    pub auth_end: TimestampTz,
}

impl ConnectionTiming {
    pub const INIT: Self = Self {
        socket_create: 0,
        ready_for_use: TIMESTAMP_MINUS_INFINITY,
        fork_start: 0,
        fork_end: 0,
        auth_start: 0,
        auth_end: 0,
    };
}

#[derive(Debug)]
pub struct Port {
    pub sock: i32,
    pub noblock: bool,
    pub proto: ProtocolVersion,
    pub laddr: SockAddr,
    pub raddr: SockAddr,
    pub remote_host: String,
    pub remote_hostname: Option<String>,
    pub remote_port: String,
    pub database_name: Option<String>,
    pub user_name: Option<String>,
    pub cmdline_options: Option<String>,
    pub guc_options: Vec<String>,
    pub application_name: Option<String>,
    pub default_keepalives_idle: i32,
    pub default_keepalives_interval: i32,
    pub default_keepalives_count: i32,
    pub default_tcp_user_timeout: i32,
    pub keepalives_idle: i32,
    pub keepalives_interval: i32,
    pub keepalives_count: i32,
    pub tcp_user_timeout: i32,
    pub ssl_in_use: bool,
    pub alpn_used: bool,
    // C: char *peer_cn / *peer_dn (TopMemoryContext copies); the SSL* itself
    // is the openssl backend's thread-local, not a Port field.
    pub peer_cn: Option<String>,
    pub peer_dn: Option<String>,
    pub peer_cert_valid: bool,
    // SCRAM_MAX_KEY_LEN keys, saved for SCRAM pass-through (postgres_fdw).
    pub scram_client_key: [u8; SCRAM_MAX_KEY_LEN],
    pub scram_server_key: [u8; SCRAM_MAX_KEY_LEN],
    pub has_scram_keys: bool,
    // C: `const HbaLine *hba` borrows the parsed_hba_lines list entry; an
    // owned clone of the matched line here (once per connection, cold).
    pub hba: Option<HbaLine>,
    // +1 forward lookup matches, 0 not checked, -1 mismatch, -2 lookup failed.
    pub remote_hostname_resolv: i32,
    pub remote_hostname_errcode: i32,
}

impl Port {
    pub fn new(client_sock: &ClientSocket) -> Self {
        Self {
            sock: client_sock.sock,
            noblock: false,
            proto: 0,
            laddr: SockAddr::zeroed(),
            raddr: client_sock.raddr,
            remote_host: String::new(),
            remote_hostname: None,
            remote_port: String::new(),
            database_name: None,
            user_name: None,
            cmdline_options: None,
            guc_options: Vec::new(),
            application_name: None,
            default_keepalives_idle: 0,
            default_keepalives_interval: 0,
            default_keepalives_count: 0,
            default_tcp_user_timeout: 0,
            keepalives_idle: 0,
            keepalives_interval: 0,
            keepalives_count: 0,
            tcp_user_timeout: 0,
            ssl_in_use: false,
            alpn_used: false,
            peer_cn: None,
            peer_dn: None,
            peer_cert_valid: false,
            scram_client_key: [0; SCRAM_MAX_KEY_LEN],
            scram_server_key: [0; SCRAM_MAX_KEY_LEN],
            has_scram_keys: false,
            hba: None,
            remote_hostname_resolv: 0,
            remote_hostname_errcode: 0,
        }
    }
}

// CancelRequestPacket layout offsets.
pub const CANCEL_REQUEST_OFFSET_BACKEND_PID: usize = 4;
pub const CANCEL_REQUEST_OFFSET_AUTH_CODE: usize = 8;

pub type ChildPid = pid_t;
