//! Startup vocabulary: `tcop/backend_startup.h` plus the `libpq/libpq-be.h`
//! pre-auth subset (ClientSocket, Port). Port's auth surface (hba, SSL/GSS
//! state, auth_method) lands with the auth units.

use ip::SockAddr;
use types_core::{pid_t, ProtocolVersion, TimestampTz};

pub const TIMESTAMP_MINUS_INFINITY: TimestampTz = i64::MIN;

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
pub enum StartupData {
    None,
    Backend(BackendStartupData),
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
    pub ssl_in_use: bool,
    pub alpn_used: bool,
}

impl Port {
    pub fn new(client_sock: &ClientSocket) -> Self {
        Self {
            sock: client_sock.sock,
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
            ssl_in_use: false,
            alpn_used: false,
        }
    }
}

// CancelRequestPacket layout offsets.
pub const CANCEL_REQUEST_OFFSET_BACKEND_PID: usize = 4;
pub const CANCEL_REQUEST_OFFSET_AUTH_CODE: usize = 8;

pub type ChildPid = pid_t;
