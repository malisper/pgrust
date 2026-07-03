use std::cell::{Cell, UnsafeCell};
use std::mem::ManuallyDrop;

use mcx::{Mcx, MemoryContext, PgFxHashMap, PgVec};
use types_error::PgResult;
use types_resowner::ResourceOwner;
use types_storage::lock::{
    LOCALLOCKTAG, LOCK, LOCKMODE, LOCKTAG, LOCKTAG_RELATION_EXTEND, MaxLockMode, PROCLOCK,
};

use crate::fastpath::{decrement_strong_lock_count, FastPathStrongLockHashPartition};

#[derive(Clone, Copy, Debug)]
pub(crate) struct LOCALLOCKOWNER {
    pub owner: ResourceOwner,
    pub nLocks: i64,
}

pub(crate) struct LOCALLOCK {
    pub hashcode: u32,
    pub lock: *mut LOCK,
    pub proclock: *mut PROCLOCK,
    pub nLocks: i64,
    pub lockOwners: PgVec<'static, LOCALLOCKOWNER>,
    pub holdsStrongLockCount: bool,
    pub lockCleared: bool,
}

pub(crate) struct LocalState {
    pub mcx: Mcx<'static>,
    pub table: PgFxHashMap<'static, LOCALLOCKTAG, LOCALLOCK>,
    pub scratch: PgVec<'static, LOCALLOCKTAG>,
}

thread_local! {
    // UnsafeCell, not RefCell (rule 10): every with_local closure is a leaf
    // (no call back into this module or seams); LOCAL_BUSY enforces in debug.
    static LOCAL: UnsafeCell<Option<ManuallyDrop<LocalState>>> = const { UnsafeCell::new(None) };
    static LOCAL_BUSY: Cell<bool> = const { Cell::new(false) };
    static STRONG_LOCK_IN_PROGRESS: Cell<Option<LOCALLOCKTAG>> = const { Cell::new(None) };
    static AWAITED_LOCK: Cell<Option<(LOCALLOCKTAG, u32)>> = const { Cell::new(None) };
    static AWAITED_OWNER: Cell<ResourceOwner> = const { Cell::new(ResourceOwner::NULL) };
    // IsRelationExtensionLockHeld, assert-only as in C.
    static RELATION_EXTENSION_LOCK_HELD: Cell<bool> = const { Cell::new(false) };
}

pub fn InitLockManagerAccess() {
    let cx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("LOCALLOCK hash")));
    let mcx = cx.mcx();
    // SAFETY: no with_local borrow is live (single-threaded backend entry).
    LOCAL.with(|slot| unsafe {
        let slot = &mut *slot.get();
        assert!(slot.is_none(), "InitLockManagerAccess called twice");
        *slot = Some(ManuallyDrop::new(LocalState {
            mcx,
            table: PgFxHashMap::with_hasher_in(Default::default(), mcx),
            scratch: PgVec::new_in(mcx),
        }));
    });
}

pub(crate) fn with_local<R>(f: impl FnOnce(&mut LocalState) -> R) -> R {
    debug_assert!(!LOCAL_BUSY.replace(true), "with_local re-entered");
    // SAFETY: closures are leaves (no re-entry into this module or seams);
    // guarded in debug builds by LOCAL_BUSY.
    let r = LOCAL.with(|slot| unsafe {
        let state = (*slot.get())
            .as_mut()
            .unwrap_or_else(|| panic!("lock manager backend state not initialized"));
        f(state)
    });
    if cfg!(debug_assertions) {
        LOCAL_BUSY.set(false);
    }
    r
}

pub(crate) fn awaited_lock() -> Option<(LOCALLOCKTAG, u32)> {
    AWAITED_LOCK.get()
}

pub(crate) fn set_awaited_lock(tag: LOCALLOCKTAG, hashcode: u32, owner: ResourceOwner) {
    AWAITED_LOCK.set(Some((tag, hashcode)));
    AWAITED_OWNER.set(owner);
}

pub fn GetAwaitedLockHashcode() -> Option<u32> {
    AWAITED_LOCK.get().map(|(_, hashcode)| hashcode)
}

pub fn ResetAwaitedLock() {
    AWAITED_LOCK.set(None);
}

pub fn GrantAwaitedLock() {
    let (tag, _) = AWAITED_LOCK.get().expect("no awaited lock");
    GrantLockLocal(&tag, AWAITED_OWNER.get());
}

pub(crate) fn CheckAndSetLockHeld(tag: &LOCALLOCKTAG, acquired: bool) {
    if cfg!(debug_assertions) && tag.lock.locktag_type == LOCKTAG_RELATION_EXTEND {
        RELATION_EXTENSION_LOCK_HELD.set(acquired);
    }
}

pub(crate) fn assert_no_relation_extension_lock_held() {
    debug_assert!(!RELATION_EXTENSION_LOCK_HELD.get());
}

fn new_locallock(tag: &LOCALLOCKTAG, mcx: Mcx<'static>) -> LOCALLOCK {
    let mut owners = PgVec::new_in(mcx);
    owners.reserve(8);
    LOCALLOCK {
        hashcode: crate::shared::LockTagHashCode(&tag.lock),
        lock: std::ptr::null_mut(),
        proclock: std::ptr::null_mut(),
        nLocks: 0,
        lockOwners: owners,
        holdsStrongLockCount: false,
        lockCleared: false,
    }
}

#[inline]
fn grant_owner_slot(owners: &mut PgVec<'static, LOCALLOCKOWNER>, owner: ResourceOwner) {
    for slot in owners.iter_mut() {
        if slot.owner == owner {
            slot.nLocks += 1;
            return;
        }
    }
    owners.push(LOCALLOCKOWNER { owner, nLocks: 1 });
}

/// Entry pointer carried across the straight-line fast-path grant (C's
/// dynahash `locallock` pointer). Valid only while the table is untouched:
/// hashbrown entries move only on insert-driven rehash.
pub(crate) struct LocalLockPtr(std::ptr::NonNull<LOCALLOCK>);

pub(crate) enum LocalGrant {
    Held { cleared: bool },
    NotHeld { hashcode: u32, ll: LocalLockPtr },
}

/// One probe: find-or-create the LOCALLOCK and, when already held, grant it
/// locally in the same table access (C works on the hash_search pointer).
pub(crate) fn prepare_or_grant_locallock(tag: &LOCALLOCKTAG, owner: ResourceOwner) -> LocalGrant {
    let out = with_local(|state| {
        let mcx = state.mcx;
        let entry = state.table.entry(*tag).or_insert_with(|| new_locallock(tag, mcx));
        if entry.nLocks > 0 {
            entry.nLocks += 1;
            grant_owner_slot(&mut entry.lockOwners, owner);
            LocalGrant::Held {
                cleared: entry.lockCleared,
            }
        } else {
            entry.lockOwners.reserve(1);
            LocalGrant::NotHeld {
                hashcode: entry.hashcode,
                ll: LocalLockPtr(std::ptr::NonNull::from(entry)),
            }
        }
    });
    if let LocalGrant::Held { .. } = out {
        if !owner.is_null() {
            resowner::ResourceOwnerRememberLock(owner, *tag);
        }
        CheckAndSetLockHeld(tag, true);
    }
    out
}

/// Stale shared pointers MUST be cleared before the lock counts as fast-path
/// acquired. SAFETY contract: `ll` from prepare_or_grant_locallock for `tag`,
/// no LOCALLOCK-table access since.
pub(crate) unsafe fn grant_locallock_after_fastpath(
    tag: &LOCALLOCKTAG,
    owner: ResourceOwner,
    ll: LocalLockPtr,
) {
    {
        // SAFETY: caller contract; no other LocalState borrow is live.
        let ll = unsafe { &mut *ll.0.as_ptr() };
        ll.lock = std::ptr::null_mut();
        ll.proclock = std::ptr::null_mut();
        ll.nLocks += 1;
        grant_owner_slot(&mut ll.lockOwners, owner);
    }
    debug_assert!(with_local(|state| {
        state.table.get(tag).is_some_and(|e| e.nLocks > 0)
    }));
    if !owner.is_null() {
        resowner::ResourceOwnerRememberLock(owner, *tag);
    }
    CheckAndSetLockHeld(tag, true);
}

pub(crate) fn GrantLockLocal(tag: &LOCALLOCKTAG, owner: ResourceOwner) {
    with_local(|state| {
        let ll = state.table.get_mut(tag).expect("missing LOCALLOCK");
        ll.nLocks += 1;
        grant_owner_slot(&mut ll.lockOwners, owner);
    });
    if !owner.is_null() {
        resowner::ResourceOwnerRememberLock(owner, *tag);
    }
    CheckAndSetLockHeld(tag, true);
}

pub(crate) fn RemoveLocalLock(tag: &LOCALLOCKTAG) {
    // One probe: entry taken out before the (infallible) forget calls;
    // dropping the owner array is C's pfree(lockOwners).
    let ll = with_local(|state| state.table.remove(tag).expect("missing LOCALLOCK"));
    for o in ll.lockOwners.iter().rev() {
        if !o.owner.is_null() {
            resowner::ResourceOwnerForgetLock(o.owner, *tag).expect("ResourceOwnerForgetLock");
        }
    }
    if ll.holdsStrongLockCount {
        decrement_strong_lock_count(ll.hashcode);
    }
    drop(ll);
    CheckAndSetLockHeld(tag, false);
}

pub(crate) fn BeginStrongLockAcquire(tag: &LOCALLOCKTAG, fasthashcode: u32) {
    debug_assert!(STRONG_LOCK_IN_PROGRESS.get().is_none());
    with_local(|state| {
        let ll = state.table.get_mut(tag).expect("missing LOCALLOCK");
        debug_assert!(!ll.holdsStrongLockCount);
        crate::fastpath::increment_strong_lock_count_partition(fasthashcode);
        ll.holdsStrongLockCount = true;
    });
    STRONG_LOCK_IN_PROGRESS.set(Some(*tag));
}

pub(crate) fn FinishStrongLockAcquire() {
    STRONG_LOCK_IN_PROGRESS.set(None);
}

pub fn AbortStrongLockAcquire() {
    let Some(tag) = STRONG_LOCK_IN_PROGRESS.get() else {
        return;
    };
    with_local(|state| {
        let ll = state.table.get_mut(&tag).expect("missing LOCALLOCK");
        debug_assert!(ll.holdsStrongLockCount);
        let fasthashcode = FastPathStrongLockHashPartition(ll.hashcode);
        crate::fastpath::decrement_strong_lock_count_partition(fasthashcode);
        ll.holdsStrongLockCount = false;
    });
    STRONG_LOCK_IN_PROGRESS.set(None);
}

pub fn MarkLockClear(locktag: &LOCKTAG, lockmode: LOCKMODE) {
    let tag = LOCALLOCKTAG {
        lock: *locktag,
        mode: lockmode,
    };
    with_local(|state| {
        let ll = state.table.get_mut(&tag).expect("missing LOCALLOCK");
        debug_assert!(ll.nLocks > 0);
        ll.lockCleared = true;
    });
}

pub fn LockHeldByMe(locktag: &LOCKTAG, lockmode: LOCKMODE, orstronger: bool) -> bool {
    let held = with_local(|state| {
        let tag = LOCALLOCKTAG {
            lock: *locktag,
            mode: lockmode,
        };
        state.table.get(&tag).is_some_and(|ll| ll.nLocks > 0)
    });
    if held {
        return true;
    }
    if orstronger {
        for slockmode in lockmode + 1..=MaxLockMode {
            if LockHeldByMe(locktag, slockmode, false) {
                return true;
            }
        }
    }
    false
}

/// Warning-path helper: LockRelease/LockHasWaiters "you don't own a lock".
pub(crate) fn warn_not_owned(mode_name: &str) -> PgResult<()> {
    elog_seams::ereport_msg::call(
        types_error::WARNING,
        format!("you don't own a lock of type {mode_name}"),
        None,
    )
}
