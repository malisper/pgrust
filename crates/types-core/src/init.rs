//! `enum BackendType` (`miscadmin.h`) — process-type discriminants. The
//! discriminants match the C enum order exactly (parity matters: they appear
//! in protocol/launch plumbing and stats indexing).

/// `enum BackendType` (`miscadmin.h:336-374`).
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BackendType {
    /// `B_INVALID`.
    Invalid = 0,

    /* Backends and other backend-like processes */
    /// `B_BACKEND`.
    Backend,
    /// `B_DEAD_END_BACKEND`.
    DeadEndBackend,
    /// `B_AUTOVAC_LAUNCHER`.
    AutovacLauncher,
    /// `B_AUTOVAC_WORKER`.
    AutovacWorker,
    /// `B_BG_WORKER`.
    BgWorker,
    /// `B_WAL_SENDER`.
    WalSender,
    /// `B_SLOTSYNC_WORKER`.
    SlotsyncWorker,
    /// `B_STANDALONE_BACKEND`.
    StandaloneBackend,

    /* Auxiliary processes (PGPROC entries, no database attachment) */
    /// `B_ARCHIVER`.
    Archiver,
    /// `B_BG_WRITER`.
    BgWriter,
    /// `B_CHECKPOINTER`.
    Checkpointer,
    /// `B_IO_WORKER`.
    IoWorker,
    /// `B_STARTUP`.
    Startup,
    /// `B_WAL_RECEIVER`.
    WalReceiver,
    /// `B_WAL_SUMMARIZER`.
    WalSummarizer,
    /// `B_WAL_WRITER`.
    WalWriter,

    /// `B_LOGGER` — not connected to shared memory; no PGPROC entry.
    Logger,
}

/// `BACKEND_NUM_TYPES` (`miscadmin.h`): `B_LOGGER + 1`.
pub const BACKEND_NUM_TYPES: usize = BackendType::Logger as usize + 1;

use crate::primitive::{InvalidOid, Oid};

/// `SECURITY_LOCAL_USERID_CHANGE` (`miscadmin.h`).
pub const SECURITY_LOCAL_USERID_CHANGE: i32 = 0x1;
/// `SECURITY_RESTRICTED_OPERATION` (`miscadmin.h`).
pub const SECURITY_RESTRICTED_OPERATION: i32 = 0x2;
/// `SECURITY_NOFORCE_RLS` (`miscadmin.h`).
pub const SECURITY_NOFORCE_RLS: i32 = 0x4;

/// The `-1` sentinel `SwitchToUntrustedUser` stores in `save_nestlevel` when
/// no GUC nest level was created.
pub const USER_CONTEXT_NO_NEST_LEVEL: i32 = -1;

/// `UserContext` (`utils/usercontext.h`): saved user identity for
/// `SwitchToUntrustedUser` / `RestoreUserContext`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct UserContext {
    pub save_userid: Oid,
    pub save_sec_context: i32,
    pub save_nestlevel: i32,
}

impl UserContext {
    pub const fn new(save_userid: Oid, save_sec_context: i32, save_nestlevel: i32) -> Self {
        Self {
            save_userid,
            save_sec_context,
            save_nestlevel,
        }
    }

    /// The C declaration `UserContext ucxt;` leaves the struct uninitialized
    /// until `SwitchToUntrustedUser` fills it; this is the safe stand-in.
    pub const fn uninitialized() -> Self {
        Self {
            save_userid: InvalidOid,
            save_sec_context: 0,
            save_nestlevel: USER_CONTEXT_NO_NEST_LEVEL,
        }
    }
}

impl Default for UserContext {
    fn default() -> Self {
        Self::uninitialized()
    }
}

/// `enum UserAuth` (`libpq/hba.h`).
pub type UserAuth = u32;
pub const uaReject: UserAuth = 0;
pub const uaImplicitReject: UserAuth = 1;
pub const uaTrust: UserAuth = 2;
pub const uaIdent: UserAuth = 3;
pub const uaPassword: UserAuth = 4;
pub const uaMD5: UserAuth = 5;
pub const uaSCRAM: UserAuth = 6;
pub const uaGSS: UserAuth = 7;
pub const uaSSPI: UserAuth = 8;
pub const uaPAM: UserAuth = 9;
pub const uaBSD: UserAuth = 10;
pub const uaLDAP: UserAuth = 11;
pub const uaCert: UserAuth = 12;
pub const uaRADIUS: UserAuth = 13;
pub const uaPeer: UserAuth = 14;
pub const uaOAuth: UserAuth = 15;
