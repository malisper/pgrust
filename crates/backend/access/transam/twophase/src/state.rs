use std::cell::Cell;
use std::sync::OnceLock;

use types_core::{
    InvalidTransactionId, Oid, ProcNumber, TimestampTz, TransactionId, XLogRecPtr,
    INVALID_PROC_NUMBER,
};
use types_storage::storage::SyncCell;

pub const GIDSIZE: usize = 200;

pub const TWO_PHASE_STATE_LOCK: usize = 18;

pub(crate) fn TwoPhaseStateLock() -> &'static lwlock::LWLock {
    lwlock::main_lock(TWO_PHASE_STATE_LOCK)
}

pub(crate) fn lock_twophase_state(mode: lwlock::LWLockMode) {
    lwlock::LWLockAcquire(TwoPhaseStateLock(), mode, init_small::globals::MyProcNumber())
        .expect("TwoPhaseStateLock acquire");
}

pub(crate) fn unlock_twophase_state() {
    lwlock::LWLockRelease(TwoPhaseStateLock()).expect("TwoPhaseStateLock release");
}

#[derive(Clone, Copy)]
pub(crate) struct GidBuf {
    pub len: u16,
    pub buf: [u8; GIDSIZE],
}

impl GidBuf {
    const fn empty() -> Self {
        GidBuf { len: 0, buf: [0; GIDSIZE] }
    }

    pub fn set(&mut self, gid: &str) {
        debug_assert!(gid.len() < GIDSIZE);
        self.buf[..gid.len()].copy_from_slice(gid.as_bytes());
        self.len = gid.len() as u16;
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: buf[..len] was written from a &str by set().
        unsafe { core::str::from_utf8_unchecked(&self.buf[..self.len as usize]) }
    }
}

// GlobalTransactionData; every field is serialized by TwoPhaseStateLock with
// C's documented exceptions (FinishPreparedTransaction clears `valid`
// unlocked; EndPrepare writes the LSNs while the entry is still locked by
// this backend and not yet valid).
pub(crate) struct GXact {
    pub next: SyncCell<i32>,
    pub pgprocno: SyncCell<ProcNumber>,
    pub prepared_at: SyncCell<TimestampTz>,
    pub prepare_start_lsn: SyncCell<XLogRecPtr>,
    pub prepare_end_lsn: SyncCell<XLogRecPtr>,
    pub xid: SyncCell<TransactionId>,
    pub owner: SyncCell<Oid>,
    pub locking_backend: SyncCell<ProcNumber>,
    pub valid: SyncCell<bool>,
    pub ondisk: SyncCell<bool>,
    pub inredo: SyncCell<bool>,
    pub gid: SyncCell<GidBuf>,
}

pub(crate) const NO_GXACT: i32 = -1;

impl GXact {
    fn blank() -> Self {
        GXact {
            next: SyncCell::new(NO_GXACT),
            pgprocno: SyncCell::new(INVALID_PROC_NUMBER),
            prepared_at: SyncCell::new(0),
            prepare_start_lsn: SyncCell::new(0),
            prepare_end_lsn: SyncCell::new(0),
            xid: SyncCell::new(InvalidTransactionId),
            owner: SyncCell::new(0),
            locking_backend: SyncCell::new(INVALID_PROC_NUMBER),
            valid: SyncCell::new(false),
            ondisk: SyncCell::new(false),
            inredo: SyncCell::new(false),
            gid: SyncCell::new(GidBuf::empty()),
        }
    }
}

// TwoPhaseStateData: freelist + active list over the fixed gxact slab.
pub(crate) struct TwoPhaseShared {
    pub free_gxacts: SyncCell<i32>,
    pub num_prep_xacts: SyncCell<i32>,
    pub prep_xacts: Box<[SyncCell<i32>]>,
    pub gxacts: Box<[GXact]>,
}

// SAFETY: SyncCell fields are serialized by TwoPhaseStateLock as documented.
unsafe impl Sync for TwoPhaseShared {}

static TWO_PHASE_STATE: OnceLock<&'static TwoPhaseShared> = OnceLock::new();

pub(crate) fn TwoPhaseState() -> &'static TwoPhaseShared {
    TWO_PHASE_STATE
        .get()
        .unwrap_or_else(|| panic!("TwoPhaseState accessed before TwoPhaseShmemInit"))
}

impl TwoPhaseShared {
    pub fn gxact(&self, idx: i32) -> &GXact {
        &self.gxacts[idx as usize]
    }

    pub fn prep_xact(&self, i: i32) -> i32 {
        self.prep_xacts[i as usize].get()
    }

    /// C `TwoPhaseState->freeGXacts` pop; `None` mirrors the NULL head.
    pub fn pop_free(&self) -> Option<i32> {
        let head = self.free_gxacts.get();
        if head == NO_GXACT {
            return None;
        }
        self.free_gxacts.set(self.gxact(head).next.get());
        Some(head)
    }

    pub fn push_active(&self, idx: i32) {
        let n = self.num_prep_xacts.get();
        debug_assert!((n as usize) < self.prep_xacts.len());
        self.prep_xacts[n as usize].set(idx);
        self.num_prep_xacts.set(n + 1);
    }
}

pub fn TwoPhaseShmemSize() -> usize {
    let max = twophase_config::max_prepared_xacts() as usize;
    core::mem::size_of::<TwoPhaseShared>()
        + max * (core::mem::size_of::<GXact>() + core::mem::size_of::<i32>())
}

pub fn TwoPhaseShmemInit() {
    if TWO_PHASE_STATE.get().is_some() {
        return;
    }
    let max = twophase_config::max_prepared_xacts().max(0) as usize;
    let mut gxacts = Vec::with_capacity(max);
    let mut prep = Vec::with_capacity(max);
    let base = lmgr_proc::PreparedXactProcsBase();
    let mut free_head = NO_GXACT;
    for i in 0..max {
        let g = GXact::blank();
        g.next.set(free_head);
        free_head = i as i32;
        g.pgprocno.set(base + i as ProcNumber);
        gxacts.push(g);
        prep.push(SyncCell::new(NO_GXACT));
    }
    let shared = Box::leak(Box::new(TwoPhaseShared {
        free_gxacts: SyncCell::new(free_head),
        num_prep_xacts: SyncCell::new(0),
        prep_xacts: prep.into_boxed_slice(),
        gxacts: gxacts.into_boxed_slice(),
    }));
    let _ = TWO_PHASE_STATE.set(shared);
}

/// Crash-cycle reset to the boot image (ipci ResetShmemAfterCrash walk;
/// postmaster thread only, children dead).
pub fn TwoPhaseStateResetAfterCrash() {
    let Some(st) = TWO_PHASE_STATE.get() else {
        return;
    };
    let mut free_head = NO_GXACT;
    for (i, g) in st.gxacts.iter().enumerate() {
        g.next.set(free_head);
        free_head = i as i32;
        g.prepared_at.set(0);
        g.prepare_start_lsn.set(0);
        g.prepare_end_lsn.set(0);
        g.xid.set(InvalidTransactionId);
        g.owner.set(0);
        g.locking_backend.set(INVALID_PROC_NUMBER);
        g.valid.set(false);
        g.ondisk.set(false);
        g.inredo.set(false);
        g.gid.set(GidBuf::empty());
    }
    st.free_gxacts.set(free_head);
    st.num_prep_xacts.set(0);
}

thread_local! {
    // MyLockedGxact (twophase.c): gxacts index, NO_GXACT = none.
    pub(crate) static MY_LOCKED_GXACT: Cell<i32> = const { Cell::new(NO_GXACT) };
    pub(crate) static TWOPHASE_EXIT_REGISTERED: Cell<bool> = const { Cell::new(false) };
    // TwoPhaseGetGXact's cached_xid/cached_gxact statics.
    pub(crate) static CACHED_GXACT: Cell<(TransactionId, i32)> =
        const { Cell::new((InvalidTransactionId, NO_GXACT)) };
}
