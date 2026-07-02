// Discriminants match the C enum order: used in protocol and stats indexing.
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BackendType {
    Invalid = 0,
    Backend,
    DeadEndBackend,
    AutovacLauncher,
    AutovacWorker,
    BgWorker,
    WalSender,
    SlotsyncWorker,
    StandaloneBackend,
    Archiver,
    BgWriter,
    Checkpointer,
    IoWorker,
    Startup,
    WalReceiver,
    WalSummarizer,
    WalWriter,
    Logger,
}

impl BackendType {
    pub const ALL: [BackendType; BACKEND_NUM_TYPES] = [
        BackendType::Invalid,
        BackendType::Backend,
        BackendType::DeadEndBackend,
        BackendType::AutovacLauncher,
        BackendType::AutovacWorker,
        BackendType::BgWorker,
        BackendType::WalSender,
        BackendType::SlotsyncWorker,
        BackendType::StandaloneBackend,
        BackendType::Archiver,
        BackendType::BgWriter,
        BackendType::Checkpointer,
        BackendType::IoWorker,
        BackendType::Startup,
        BackendType::WalReceiver,
        BackendType::WalSummarizer,
        BackendType::WalWriter,
        BackendType::Logger,
    ];
}

pub const BACKEND_NUM_TYPES: usize = BackendType::Logger as usize + 1;

#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ProcessingMode {
    BootstrapProcessing = 0,
    InitProcessing,
    NormalProcessing,
}

use crate::primitive::{InvalidOid, Oid};

pub const SECURITY_LOCAL_USERID_CHANGE: i32 = 0x1;
pub const SECURITY_RESTRICTED_OPERATION: i32 = 0x2;
pub const SECURITY_NOFORCE_RLS: i32 = 0x4;

// `save_nestlevel` sentinel meaning no GUC nest level was created.
pub const USER_CONTEXT_NO_NEST_LEVEL: i32 = -1;

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
