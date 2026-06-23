//! Process-local FD-cookie-table state for be-fsstubs.c (idiomatic owned style).
//!
//! Mirrors be-fsstubs.c's file-scope statics — per-backend process-local state
//! (LO FDs are only valid within a transaction):
//!
//!   * `cookies` — the array of open large objects (an FD is an index into it;
//!     empty slots are free). In C this is an array of `LargeObjectDesc *`
//!     `palloc`'d in `fscxt`; in the idiomatic port the cookie slots *own* the
//!     boxed [`LargeObjectDesc`] (`Option<Box<LargeObjectDesc>>`) — exactly the
//!     box `inv_open` hands back — so the separate `fscxt` `MemoryContext` whose
//!     only job was to own those heap allocations disappears; Rust ownership
//!     replaces it.
//!   * `cookies_size` — the array's allocated length (kept explicit as the C
//!     `int cookies_size` to match its `<= 0` first-time test).
//!   * `lo_cleanup_needed` — whether any LO op happened this xact.
//!
//! The C `fscxt != NULL` predicate (used by `AtEOSubXact_LargeObject` and
//! `be_lo_unlink` to mean "any LO activity happened this xact, so the cookie
//! table exists") is `cookies_size > 0` here — the cookie table being allocated
//! is exactly the C "fscxt created" condition (both are first set in `newLOfd`).
//!
//! A PostgreSQL backend is single-threaded, so a `thread_local!` `RefCell`
//! reproduces the C statics' single-owner semantics.

use std::cell::RefCell;

use types_core::xact::SubTransactionId;
use types_core::Oid;
use types_storage::large_object::LargeObjectDesc;

/// The be-fsstubs file-scope statics, bundled.
pub struct LoState {
    /// `static LargeObjectDesc **cookies` — indexed by FD; `None` = free slot.
    /// The slot OWNS the descriptor box (the C `palloc`'d-in-`fscxt` pointer).
    cookies: Vec<Option<Box<LargeObjectDesc>>>,
    /// `static int cookies_size` — kept explicit to match the C integer and its
    /// `<= 0` first-time test.
    cookies_size: i32,
    /// `static bool lo_cleanup_needed`.
    lo_cleanup_needed: bool,
}

impl LoState {
    const fn new() -> Self {
        LoState {
            cookies: Vec::new(),
            cookies_size: 0,
            lo_cleanup_needed: false,
        }
    }

    /// `cookies_size`.
    pub fn cookies_size(&self) -> i32 {
        self.cookies_size
    }

    /// `cookies[fd] != NULL` — is slot `fd` occupied? (callers always guard the
    /// index first, exactly as the C does; out-of-range reads `false`).
    pub fn cookie_is_some(&self, fd: i32) -> bool {
        fd >= 0 && fd < self.cookies_size && self.cookies[fd as usize].is_some()
    }

    /// `cookies[fd]->id == lobjId` — the LO id stored in slot `fd`, if occupied.
    pub fn cookie_id(&self, fd: i32) -> Option<Oid> {
        if fd < 0 || fd >= self.cookies_size {
            return None;
        }
        self.cookies[fd as usize].as_ref().map(|d| d.id)
    }

    /// `cookies[fd]->subid` — the owning subxact id stored in slot `fd`, if
    /// occupied.
    pub fn cookie_subid(&self, fd: i32) -> Option<SubTransactionId> {
        if fd < 0 || fd >= self.cookies_size {
            return None;
        }
        self.cookies[fd as usize].as_ref().map(|d| d.subid)
    }

    /// `cookies[fd]->subid = parentSubid` — reassign slot `fd`'s owning subxact.
    pub fn set_cookie_subid(&mut self, fd: i32, subid: SubTransactionId) {
        if fd >= 0 && fd < self.cookies_size {
            if let Some(d) = self.cookies[fd as usize].as_mut() {
                d.subid = subid;
            }
        }
    }

    /// Borrow `cookies[fd]` mutably (callers guard the index + occupancy first).
    pub fn cookie_mut(&mut self, fd: i32) -> Option<&mut LargeObjectDesc> {
        if fd < 0 || fd >= self.cookies_size {
            return None;
        }
        self.cookies[fd as usize].as_deref_mut()
    }

    /// `cookies[fd] = lobj` — store an owned descriptor into slot `fd`.
    pub fn set_cookie(&mut self, fd: i32, lobj: Box<LargeObjectDesc>) {
        if fd >= 0 && fd < self.cookies_size {
            self.cookies[fd as usize] = Some(lobj);
        }
    }

    /// `lobj = cookies[fd]; cookies[fd] = NULL; return lobj` — take the owned
    /// descriptor out of slot `fd`, leaving it free.
    pub fn take_cookie(&mut self, fd: i32) -> Option<Box<LargeObjectDesc>> {
        if fd < 0 || fd >= self.cookies_size {
            return None;
        }
        self.cookies[fd as usize].take()
    }

    /// Grow the cookie array to `newsize` entries, new slots empty
    /// (`MemoryContextAllocZero` first time / `repalloc0_array` thereafter).
    pub fn grow_cookies(&mut self, newsize: i32) {
        self.cookies.resize_with(newsize as usize, || None);
        self.cookies_size = newsize;
    }

    /// `cookies = NULL; cookies_size = 0`.
    pub fn clear_cookies(&mut self) {
        self.cookies.clear();
        self.cookies_size = 0;
    }

    /// `lo_cleanup_needed`.
    pub fn lo_cleanup_needed(&self) -> bool {
        self.lo_cleanup_needed
    }

    /// Set `lo_cleanup_needed`.
    pub fn set_lo_cleanup_needed(&mut self, v: bool) {
        self.lo_cleanup_needed = v;
    }

    /// `fscxt != NULL` — modelled as "the cookie table has been allocated"
    /// (`cookies_size > 0`), which is exactly the C condition (both `fscxt` and
    /// the cookie array are first set in `newLOfd`).
    pub fn has_fscxt(&self) -> bool {
        self.cookies_size > 0
    }
}

thread_local! {
    static LO_STATE: RefCell<LoState> = const { RefCell::new(LoState::new()) };
}

/// Run `f` with mutable access to the process-local LO state.
pub fn with_state<R>(f: impl FnOnce(&mut LoState) -> R) -> R {
    LO_STATE.with(|s| f(&mut s.borrow_mut()))
}
