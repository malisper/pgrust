use core::ffi::c_int;

use crate::{InvalidOid, Oid};

pub const SECURITY_LOCAL_USERID_CHANGE: c_int = 0x1;
pub const SECURITY_RESTRICTED_OPERATION: c_int = 0x2;
pub const SECURITY_NOFORCE_RLS: c_int = 0x4;

pub const USER_CONTEXT_NO_NEST_LEVEL: c_int = -1;

pub type BackendType = u32;
pub const B_INVALID: BackendType = 0;
pub const B_BACKEND: BackendType = 1;
pub const B_DEAD_END_BACKEND: BackendType = 2;
pub const B_AUTOVAC_LAUNCHER: BackendType = 3;
pub const B_AUTOVAC_WORKER: BackendType = 4;
pub const B_BG_WORKER: BackendType = 5;
pub const B_WAL_SENDER: BackendType = 6;
pub const B_SLOTSYNC_WORKER: BackendType = 7;
pub const B_STANDALONE_BACKEND: BackendType = 8;
pub const B_ARCHIVER: BackendType = 9;
pub const B_BG_WRITER: BackendType = 10;
pub const B_CHECKPOINTER: BackendType = 11;
pub const B_IO_WORKER: BackendType = 12;
pub const B_STARTUP: BackendType = 13;
pub const B_WAL_RECEIVER: BackendType = 14;
pub const B_WAL_SUMMARIZER: BackendType = 15;
pub const B_WAL_WRITER: BackendType = 16;
pub const B_LOGGER: BackendType = 17;

/// `BACKEND_NUM_TYPES` (`src/include/miscadmin.h`): `(B_LOGGER + 1)`, the number
/// of distinct [`BackendType`] values and the size of the per-type pools the
/// postmaster maintains in `pmchild.c`.
pub const BACKEND_NUM_TYPES: usize = (B_LOGGER + 1) as usize;

/// Opaque `struct RegisteredBgWorker` (`src/include/postmaster/bgworker_internals.h`).
///
/// Owned by the background-worker subsystem; `pmchild.c` only ever stores a
/// pointer to it inside [`PMChild`] (always `NULL` there), so the full layout is
/// not needed here. Kept as a distinct zero-sized type so the `*mut` field in
/// [`PMChild`] keeps a faithful, non-`c_void` pointee type.
#[repr(C)]
pub struct RegisteredBgWorker {
    _private: [u8; 0],
}

/// `PMChild` (`src/include/postmaster/postmaster.h`).
///
/// A struct representing an active postmaster child process, used to keep track
/// of how many children exist and to dispatch signals. Allocated from a fixed
/// per-type pool (or, for dead-end children, individually). `repr(C)` so the
/// embedded [`dlist_node`](crate::dlist_node) link keeps byte-for-byte layout
/// with the C definition.
#[repr(C)]
pub struct PMChild {
    /// process id of backend
    pub pid: crate::pid_t,
    /// PMChildSlot for this backend, if any
    pub child_slot: c_int,
    /// child process flavor
    pub bkend_type: BackendType,
    /// bgworker info, if this is a bgworker
    pub rw: *mut RegisteredBgWorker,
    /// gets bgworker start/stop notifications
    pub bgworker_notify: bool,
    /// list link in ActiveChildList
    pub elem: crate::dlist_node,
}

pub type ProcessingMode = u32;
pub const BootstrapProcessing: ProcessingMode = 0;
pub const InitProcessing: ProcessingMode = 1;
pub const NormalProcessing: ProcessingMode = 2;

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

#[repr(C)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ClientConnectionInfo {
    pub authn_id: *const core::ffi::c_char,
    pub auth_method: UserAuth,
}

impl ClientConnectionInfo {
    pub const fn empty() -> Self {
        Self {
            authn_id: core::ptr::null(),
            auth_method: uaReject,
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SerializedClientConnectionInfo {
    pub authn_id_len: i32,
    pub auth_method: UserAuth,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct UserContext {
    save_userid: Oid,
    save_sec_context: c_int,
    save_nestlevel: c_int,
}

impl UserContext {
    pub const fn new(save_userid: Oid, save_sec_context: c_int, save_nestlevel: c_int) -> Self {
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

    pub const fn save_userid(&self) -> Oid {
        self.save_userid
    }

    pub const fn save_sec_context(&self) -> c_int {
        self.save_sec_context
    }

    pub const fn save_nestlevel(&self) -> c_int {
        self.save_nestlevel
    }

    pub fn set_saved_context(&mut self, userid: Oid, sec_context: c_int) {
        self.save_userid = userid;
        self.save_sec_context = sec_context;
    }

    pub fn set_save_nestlevel(&mut self, nestlevel: c_int) {
        self.save_nestlevel = nestlevel;
    }
}

impl Default for UserContext {
    fn default() -> Self {
        Self::uninitialized()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn user_context_layout_matches_postgres() {
        assert_eq!(size_of::<UserContext>(), 12);
        assert_eq!(align_of::<UserContext>(), align_of::<c_int>());
        assert_eq!(offset_of!(UserContext, save_userid), 0);
        assert_eq!(offset_of!(UserContext, save_sec_context), 4);
        assert_eq!(offset_of!(UserContext, save_nestlevel), 8);
    }

    #[test]
    fn accessors_update_c_abi_storage() {
        let mut context = UserContext::uninitialized();

        context.set_saved_context(10, SECURITY_LOCAL_USERID_CHANGE);
        context.set_save_nestlevel(3);

        assert_eq!(context.save_userid(), 10);
        assert_eq!(context.save_sec_context(), SECURITY_LOCAL_USERID_CHANGE);
        assert_eq!(context.save_nestlevel(), 3);
    }

    #[test]
    fn client_connection_info_layout_matches_postgres() {
        assert_eq!(size_of::<ClientConnectionInfo>(), 16);
        assert_eq!(align_of::<ClientConnectionInfo>(), 8);
        assert_eq!(offset_of!(ClientConnectionInfo, authn_id), 0);
        assert_eq!(offset_of!(ClientConnectionInfo, auth_method), 8);

        assert_eq!(size_of::<SerializedClientConnectionInfo>(), 8);
        assert_eq!(align_of::<SerializedClientConnectionInfo>(), 4);
        assert_eq!(offset_of!(SerializedClientConnectionInfo, authn_id_len), 0);
        assert_eq!(offset_of!(SerializedClientConnectionInfo, auth_method), 4);
    }
}
