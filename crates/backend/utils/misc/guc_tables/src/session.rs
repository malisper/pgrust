//! Per-session GUC backing storage. C gives every backend a private copy of
//! each GUC variable via fork; our backends are threads, so a bare static
//! atomic backing is process-shared — one session's SET leaks to every
//! session and a child thread's boot-value bring-up writes clobber others'
//! SETs mid-statement (notes/io-method-child-guc-race.md residual).
//!
//! Layout per variable: a global cell holding the postmaster-scope value
//! (boot/file/argv; written only by non-session threads) plus a per-thread
//! override slot, guarded by a per-variable override count on the same cache
//! line as the value. Read fast path when no session anywhere has SET the
//! variable: one extra same-line load + one predicted-not-taken branch over
//! the old bare atomic load (the select1 rail budget; std thread_local on the
//! read path costs ~40 instr/q and is confined to the override slow path).
//!
//! Write routing: threads spawned by postmaster_child_launch are marked
//! session threads; their writes land in the thread's override slot. An
//! override equal to the global value is elided so child bring-up snapshot
//! restores (postmaster values re-applied per thread) keep the fast path
//! override-free. Residual: after a config reload changes the global, a
//! session whose SET happened to equal the old global follows the new value
//! until its own ProcessConfigFile pass re-applies precedence — same window
//! C closes only at each backend's reload, minus the elided stack entry.

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;
use std::thread::LocalKey;

thread_local! {
    static IS_SESSION_THREAD: Cell<bool> = const { Cell::new(false) };
}

// Called once in each postmaster_child_launch thread preamble, before the
// child's GUC bring-up performs its first write.
pub fn mark_session_thread() {
    IS_SESSION_THREAD.with(|c| c.set(true));
}

pub fn is_session_thread() -> bool {
    IS_SESSION_THREAD.try_with(|c| c.get()).unwrap_or(false)
}

pub struct OverrideSlot<T: Copy + 'static> {
    val: Cell<Option<T>>,
    ovr: &'static AtomicU32,
}

impl<T: Copy> OverrideSlot<T> {
    pub fn new(ovr: &'static AtomicU32) -> Self {
        Self { val: Cell::new(None), ovr }
    }
}

impl<T: Copy> Drop for OverrideSlot<T> {
    fn drop(&mut self) {
        if self.val.get().is_some() {
            self.ovr.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

macro_rules! session_scalar {
    ($name:ident, $val:ty, $atomic:ty, $enc:expr, $dec:expr) => {
        pub struct $name {
            // Field order keeps value + override count adjacent (one line).
            global: $atomic,
            ovr: AtomicU32,
        }

        impl $name {
            pub const fn new(boot: $val) -> Self {
                Self { global: <$atomic>::new($enc(boot)), ovr: AtomicU32::new(0) }
            }

            pub fn override_slot(&'static self) -> OverrideSlot<$val> {
                OverrideSlot::new(&self.ovr)
            }

            #[inline]
            pub fn get(&'static self, tls: &'static LocalKey<OverrideSlot<$val>>) -> $val {
                if self.ovr.load(Ordering::Relaxed) != 0 {
                    // try_with: reads during TLS teardown fall back to global.
                    if let Some(v) = tls.try_with(|s| s.val.get()).ok().flatten() {
                        return v;
                    }
                }
                $dec(self.global.load(Ordering::Relaxed))
            }

            pub fn set(&'static self, tls: &'static LocalKey<OverrideSlot<$val>>, v: $val) {
                if !is_session_thread() {
                    self.global.store($enc(v), Ordering::Relaxed);
                    return;
                }
                let _ = tls.try_with(|s| {
                    let had = s.val.get().is_some();
                    if $enc(v) == self.global.load(Ordering::Relaxed) {
                        if had {
                            s.val.set(None);
                            self.ovr.fetch_sub(1, Ordering::Relaxed);
                        }
                    } else {
                        s.val.set(Some(v));
                        if !had {
                            self.ovr.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        }
    };
}

const fn id_bool(v: bool) -> bool {
    v
}
const fn id_i32(v: i32) -> i32 {
    v
}
const fn id_i64(v: i64) -> i64 {
    v
}
const fn id_u32(v: u32) -> u32 {
    v
}
const fn id_u64(v: u64) -> u64 {
    v
}
const fn id_usize(v: usize) -> usize {
    v
}

session_scalar!(SessionBool, bool, AtomicBool, id_bool, id_bool);
session_scalar!(SessionI32, i32, AtomicI32, id_i32, id_i32);
session_scalar!(SessionI64, i64, std::sync::atomic::AtomicI64, id_i64, id_i64);
session_scalar!(SessionU32, u32, AtomicU32, id_u32, id_u32);
session_scalar!(SessionU64, u64, AtomicU64, id_u64, id_u64);
session_scalar!(SessionUsize, usize, std::sync::atomic::AtomicUsize, id_usize, id_usize);
session_scalar!(SessionF64, f64, AtomicU64, f64::to_bits, f64::from_bits);

pub struct StrOverrideSlot {
    // Outer None: no override. Inner Option matches the global's value shape.
    val: RefCell<Option<Option<String>>>,
    ovr: &'static AtomicU32,
}

impl StrOverrideSlot {
    pub fn new(ovr: &'static AtomicU32) -> Self {
        Self { val: RefCell::new(None), ovr }
    }
}

impl Drop for StrOverrideSlot {
    fn drop(&mut self) {
        if self.val.borrow().is_some() {
            self.ovr.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub struct SessionStr {
    // None means the caller-supplied boot default (macro-level contract kept
    // from the old string_var! backing).
    global: RwLock<Option<String>>,
    ovr: AtomicU32,
}

impl SessionStr {
    pub const fn new() -> Self {
        Self { global: RwLock::new(None), ovr: AtomicU32::new(0) }
    }

    pub fn override_slot(&'static self) -> StrOverrideSlot {
        StrOverrideSlot::new(&self.ovr)
    }

    pub fn get(
        &'static self,
        tls: &'static LocalKey<StrOverrideSlot>,
        boot: Option<&'static str>,
    ) -> Option<String> {
        if self.ovr.load(Ordering::Relaxed) != 0 {
            let ovr = tls.try_with(|s| s.val.borrow().clone()).ok().flatten();
            if let Some(v) = ovr {
                return v;
            }
        }
        match &*self.global.read().unwrap() {
            Some(s) => Some(s.clone()),
            None => boot.map(str::to_owned),
        }
    }

    pub fn set(&'static self, tls: &'static LocalKey<StrOverrideSlot>, v: Option<String>) {
        if !is_session_thread() {
            *self.global.write().unwrap() = v;
            return;
        }
        let _ = tls.try_with(|s| {
            let had = s.val.borrow().is_some();
            if *self.global.read().unwrap() == v {
                if had {
                    *s.val.borrow_mut() = None;
                    self.ovr.fetch_sub(1, Ordering::Relaxed);
                }
            } else {
                *s.val.borrow_mut() = Some(v);
                if !had {
                    self.ovr.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }
}

impl Default for SessionStr {
    fn default() -> Self {
        Self::new()
    }
}

// Backing declarations for session-settable GUCs (context below PGC_SIGHUP):
// same call shape as the old bare-atomic macros; the per-thread override slot
// lives in a generated module named by the cell ident.
#[macro_export]
macro_rules! session_guc_bool {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        #[allow(non_snake_case)]
        mod $cell {
            pub(super) static CELL: $crate::session::SessionBool =
                $crate::session::SessionBool::new($boot);
            ::std::thread_local! {
                pub(super) static TLS: $crate::session::OverrideSlot<bool> = CELL.override_slot();
            }
        }
        #[inline]
        pub fn $get() -> bool {
            $cell::CELL.get(&$cell::TLS)
        }
        pub fn $set(v: bool) {
            $cell::CELL.set(&$cell::TLS, v)
        }
    };
}

#[macro_export]
macro_rules! session_guc_int {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        #[allow(non_snake_case)]
        mod $cell {
            pub(super) static CELL: $crate::session::SessionI32 =
                $crate::session::SessionI32::new($boot);
            ::std::thread_local! {
                pub(super) static TLS: $crate::session::OverrideSlot<i32> = CELL.override_slot();
            }
        }
        #[inline]
        pub fn $get() -> i32 {
            $cell::CELL.get(&$cell::TLS)
        }
        pub fn $set(v: i32) {
            $cell::CELL.set(&$cell::TLS, v)
        }
    };
}

#[macro_export]
macro_rules! session_guc_real {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        #[allow(non_snake_case)]
        mod $cell {
            pub(super) static CELL: $crate::session::SessionF64 =
                $crate::session::SessionF64::new($boot as f64);
            ::std::thread_local! {
                pub(super) static TLS: $crate::session::OverrideSlot<f64> = CELL.override_slot();
            }
        }
        #[inline]
        pub fn $get() -> f64 {
            $cell::CELL.get(&$cell::TLS)
        }
        pub fn $set(v: f64) {
            $cell::CELL.set(&$cell::TLS, v)
        }
    };
}

#[macro_export]
macro_rules! session_guc_string {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        #[allow(non_snake_case)]
        mod $cell {
            pub(super) static CELL: $crate::session::SessionStr =
                $crate::session::SessionStr::new();
            ::std::thread_local! {
                pub(super) static TLS: $crate::session::StrOverrideSlot = CELL.override_slot();
            }
        }
        pub fn $get() -> Option<String> {
            $cell::CELL.get(&$cell::TLS, $boot)
        }
        pub fn $set(v: Option<String>) {
            $cell::CELL.set(&$cell::TLS, v)
        }
    };
}
