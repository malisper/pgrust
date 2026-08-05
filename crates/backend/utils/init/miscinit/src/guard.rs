//! C's GetUserIdAndSecContext/SetUserIdAndSecContext save-restore pairs become
//! this guard: `restore()` is the ordered teardown on the success path; `Drop`
//! is the abort/unwind path (docs/no-drop.md).

use types_core::{Oid, SECURITY_RESTRICTED_OPERATION};

use crate::userid::{GetUserIdAndSecContext, SetUserIdAndSecContext};

#[must_use]
pub struct SecContextGuard {
    save_userid: Oid,
    save_sec_context: i32,
}

impl SecContextGuard {
    pub fn set(userid: Oid, sec_context: i32) -> Self {
        let (save_userid, save_sec_context) = GetUserIdAndSecContext();
        SetUserIdAndSecContext(userid, sec_context);
        Self {
            save_userid,
            save_sec_context,
        }
    }

    /// Enter a SECURITY_RESTRICTED_OPERATION scope as `userid`.
    pub fn security_restricted(userid: Oid) -> Self {
        let (save_userid, save_sec_context) = GetUserIdAndSecContext();
        SetUserIdAndSecContext(userid, save_sec_context | SECURITY_RESTRICTED_OPERATION);
        Self {
            save_userid,
            save_sec_context,
        }
    }

    pub fn saved(&self) -> (Oid, i32) {
        (self.save_userid, self.save_sec_context)
    }

    pub fn restore(self) {
        SetUserIdAndSecContext(self.save_userid, self.save_sec_context);
        core::mem::forget(self);
    }
}

impl Drop for SecContextGuard {
    fn drop(&mut self) {
        SetUserIdAndSecContext(self.save_userid, self.save_sec_context);
    }
}
