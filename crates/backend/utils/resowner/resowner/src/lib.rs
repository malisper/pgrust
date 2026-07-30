#![allow(non_snake_case)]

use core::cell::{Cell, UnsafeCell};
use core::mem::ManuallyDrop;

use ::datum::Datum;
use ::hashfn::{hash_combine64, murmurhash64};
use ::types_error::{PgError, PgResult, WARNING};
use ::types_resowner::{
    ResourceOwner, ResourceOwnerDesc, ResourceReleaseCallback, ResourceReleasePhase,
    RESOURCE_RELEASE_AFTER_LOCKS, RESOURCE_RELEASE_BEFORE_LOCKS, RESOURCE_RELEASE_LOCKS,
};
use ::types_storage::lock::LOCALLOCKTAG;

mod seams;
#[cfg(test)]
mod tests;

pub const RESOWNER_ARRAY_SIZE: usize = 32;
pub const RESOWNER_HASH_INIT_SIZE: u32 = 64;
pub const MAX_RESOWNER_LOCKS: u8 = 15;

const fn resowner_hash_max_items(capacity: u32) -> u32 {
    let a = capacity - RESOWNER_ARRAY_SIZE as u32;
    let b = capacity / 4 * 3;
    if a < b {
        a
    } else {
        b
    }
}

const _: () = assert!(
    resowner_hash_max_items(RESOWNER_HASH_INIT_SIZE) >= RESOWNER_ARRAY_SIZE as u32,
    "initial hash size too small compared to array size"
);

// kind == None is C's `kind == NULL` free-hash-slot marker.
#[derive(Clone, Copy)]
struct ResourceElem {
    item: Datum,
    kind: Option<&'static ResourceOwnerDesc>,
}

const _: () = assert!(core::mem::size_of::<ResourceElem>() == 16);

impl ResourceElem {
    const EMPTY: ResourceElem = ResourceElem {
        item: Datum::null(),
        kind: None,
    };
}

struct ResourceOwnerData {
    parent: ResourceOwner,
    firstchild: ResourceOwner,
    nextchild: ResourceOwner,
    // C stores the caller's `const char *name` by pointer, never copying;
    // &'static str mirrors that (names are string literals).
    name: &'static str,

    releasing: bool,
    sorted: bool,

    // nlocks == MAX_RESOWNER_LOCKS + 1 is the overflowed-cache sentinel.
    nlocks: u8,
    narr: u8,
    nhash: u32,

    arr: [ResourceElem; RESOWNER_ARRAY_SIZE],

    // Owner structures live on the plain heap like C's TopMemoryContext
    // allocations (docs/no-drop.md: owners are outside the arenas), so the
    // open-addressing table is a heap Vec with the exact malloc/free shape of
    // C's MemoryContextAllocZero/pfree pair.
    hash: Vec<ResourceElem>,
    capacity: u32,
    grow_at: u32,

    locks: [LOCALLOCKTAG; MAX_RESOWNER_LOCKS as usize],

    aio_handles: Vec<usize>,
}

impl ResourceOwnerData {
    fn new(parent: ResourceOwner, name: &'static str) -> Self {
        Self {
            parent,
            firstchild: ResourceOwner::NULL,
            nextchild: ResourceOwner::NULL,
            name,
            releasing: false,
            sorted: false,
            nlocks: 0,
            narr: 0,
            nhash: 0,
            arr: [ResourceElem::EMPTY; RESOWNER_ARRAY_SIZE],
            hash: Vec::new(),
            capacity: 0,
            grow_at: 0,
            locks: [LOCALLOCKTAG::default(); MAX_RESOWNER_LOCKS as usize],
            aio_handles: Vec::new(),
        }
    }
}

// Power-of-two size: handle->data resolution is on the Remember/Forget hot
// path and a shift beats the 928-byte multiply there.
#[repr(align(1024))]
struct Slot {
    generation: u32,
    live: bool,
    data: ResourceOwnerData,
}

const _: () = assert!(core::mem::size_of::<Slot>() == 1024);

struct Arena {
    slots: Vec<Slot>,
    free: Vec<u32>,
    callbacks: Vec<(ResourceReleaseCallback, Datum)>,
}

impl Arena {
    const fn new() -> Arena {
        Arena {
            slots: Vec::new(),
            free: Vec::new(),
            callbacks: Vec::new(),
        }
    }

    fn alloc(&mut self, parent: ResourceOwner, name: &'static str) -> PgResult<ResourceOwner> {
        if let Some(idx) = self.free.pop() {
            let slot = &mut self.slots[idx as usize];
            debug_assert!(!slot.live);
            slot.live = true;
            // Scalar fields only: arr past narr and locks past nlocks are
            // never read, so the stale bytes C's create-time calloc would
            // zero stay untouched.
            let d = &mut slot.data;
            debug_assert!(d.hash.is_empty() && d.aio_handles.is_empty());
            d.parent = parent;
            d.firstchild = ResourceOwner::NULL;
            d.nextchild = ResourceOwner::NULL;
            d.name = name;
            d.releasing = false;
            d.sorted = false;
            d.nlocks = 0;
            d.narr = 0;
            d.nhash = 0;
            d.capacity = 0;
            d.grow_at = 0;
            Ok(ResourceOwner::from_parts(idx, slot.generation))
        } else {
            self.slots.try_reserve(1).map_err(|_| oom())?;
            let idx = self.slots.len() as u32;
            self.slots.push(Slot {
                generation: 0,
                live: true,
                data: ResourceOwnerData::new(parent, name),
            });
            Ok(ResourceOwner::from_parts(idx, 0))
        }
    }

    #[inline]
    fn data(&self, owner: ResourceOwner) -> &ResourceOwnerData {
        let Some(slot) = self.slots.get(owner.slot() as usize) else {
            bad_owner(owner);
        };
        debug_assert!(slot.live, "ResourceOwner already freed");
        debug_assert_eq!(slot.generation, owner.generation(), "stale ResourceOwner");
        &slot.data
    }

    #[inline]
    fn data_mut(&mut self, owner: ResourceOwner) -> &mut ResourceOwnerData {
        let Some(slot) = self.slots.get_mut(owner.slot() as usize) else {
            bad_owner(owner);
        };
        debug_assert!(slot.live, "ResourceOwner already freed");
        debug_assert_eq!(slot.generation, owner.generation(), "stale ResourceOwner");
        &mut slot.data
    }

    // C pfrees the hash and the owner struct here. Only the heap-backed
    // fields are freed; the stale inline bytes are overwritten by the next
    // alloc (its full-struct write is C's create-time calloc memset).
    fn freed(&mut self, owner: ResourceOwner) {
        let slot = &mut self.slots[owner.slot() as usize];
        debug_assert!(slot.live);
        debug_assert_eq!(slot.generation, owner.generation());
        slot.live = false;
        slot.generation = slot.generation.wrapping_add(1);
        drop(core::mem::take(&mut slot.data.hash));
        drop(core::mem::take(&mut slot.data.aio_handles));
        self.free.push(owner.slot());
    }
}

thread_local! {
    // ManuallyDrop keeps the payload !needs_drop so `with` compiles to a plain
    // TLS access with no lazy-init/dtor state machine (fabled-lessons §8); the
    // arena leaks at thread exit exactly as C's TopMemoryContext does.
    static ARENA: UnsafeCell<ManuallyDrop<Arena>> =
        const { UnsafeCell::new(ManuallyDrop::new(Arena::new())) };
    #[cfg(debug_assertions)]
    static ARENA_ENTERED: Cell<bool> = const { Cell::new(false) };

    static CURRENT_OWNER: Cell<ResourceOwner> = const { Cell::new(ResourceOwner::NULL) };
    static CUR_TRANSACTION_OWNER: Cell<ResourceOwner> = const { Cell::new(ResourceOwner::NULL) };
    static TOP_TRANSACTION_OWNER: Cell<ResourceOwner> = const { Cell::new(ResourceOwner::NULL) };
    static AUX_PROCESS_OWNER: Cell<ResourceOwner> = const { Cell::new(ResourceOwner::NULL) };
}

// INVARIANT (single entry): no closure passed here re-enters with_arena.
// ReleaseResource / DebugPrint / release callbacks may create, delete, or
// mutate owners, so every release path snapshots what it needs under one
// entry and invokes callbacks with no arena reference held.
#[inline(always)]
/// Session-memory teardown (FPBUDGET-1): drop the whole owner arena at clean
/// task end (C's owners die with the backend process). The TLS slot is left
/// holding an empty arena; nothing creates or releases owners afterwards.
pub fn session_mem_teardown() {
    ARENA.with(|cell| {
        // SAFETY: task-end teardown — no arena entry is live (single thread).
        let arena = unsafe { &mut **cell.get() };
        *arena = Arena {
            slots: Vec::new(),
            free: Vec::new(),
            callbacks: Vec::new(),
        };
    });
}

fn with_arena<R>(f: impl FnOnce(&mut Arena) -> R) -> R {
    // Guard module Drop: ENTERED must clear on panic unwind or every later
    // call — including abort cleanup — re-panics and the backend spins (the
    // snapmgr with_state wedge class, d1a86f62f).
    #[cfg(debug_assertions)]
    struct EnteredReset;
    #[cfg(debug_assertions)]
    impl Drop for EnteredReset {
        fn drop(&mut self) {
            ARENA_ENTERED.with(|e| e.set(false));
        }
    }
    ARENA.with(|cell| {
        #[cfg(debug_assertions)]
        let _entered = {
            ARENA_ENTERED.with(|e| assert!(!e.replace(true), "resowner arena re-entered"));
            EnteredReset
        };
        // SAFETY: one backend = one thread (TLS), and the single-entry
        // invariant above excludes aliasing &mut.
        f(unsafe { &mut **cell.get() })
    })
}

#[cold]
#[inline(never)]
fn bad_owner(owner: ResourceOwner) -> ! {
    if owner.is_null() {
        // C dereferences CurrentResourceOwner unconditionally; NULL here means
        // a resource op ran outside any owner (e.g. a guard dropped after
        // owner teardown) — a call-site bug, kept loud but catchable.
        panic!("resowner: operation on NULL ResourceOwner (CurrentResourceOwner not set)");
    }
    panic!("resowner: stale ResourceOwner {owner:?} (slot out of range)");
}

#[track_caller]
#[cold]
#[inline(never)]
fn oom() -> Box<PgError> {
    Box::new(PgError::error("out of memory"))
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_enlarge_after_release() -> Box<PgError> {
    Box::new(PgError::error(
        "ResourceOwnerEnlarge called after release started",
    ))
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_array_full() -> Box<PgError> {
    Box::new(PgError::error(
        "ResourceOwnerRemember called but array was full",
    ))
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_forget_after_release(kind: &'static ResourceOwnerDesc) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "ResourceOwnerForget called for {} after release started",
        kind.name
    )))
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_not_owned(kind: &'static ResourceOwnerDesc, value: Datum, owner: &str) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "{} 0x{:x} is not owned by resource owner {}",
        kind.name,
        value.as_usize(),
        owner
    )))
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported callee reached from resowner.c: {what}")
}

fn hash_resource_elem(value: Datum, kind: &'static ResourceOwnerDesc) -> u32 {
    let kind_id = core::ptr::from_ref(kind) as usize as u64;
    hash_combine64(murmurhash64(value.as_usize() as u64), kind_id) as u32
}

#[inline]
fn kind_eq(slot: Option<&'static ResourceOwnerDesc>, kind: &'static ResourceOwnerDesc) -> bool {
    match slot {
        Some(k) => core::ptr::eq(k, kind),
        None => false,
    }
}

fn add_to_hash(d: &mut ResourceOwnerData, value: Datum, kind: &'static ResourceOwnerDesc) {
    let mask = d.capacity - 1;
    let mut idx = hash_resource_elem(value, kind) & mask;
    while d.hash[idx as usize].kind.is_some() {
        idx = (idx + 1) & mask;
    }
    d.hash[idx as usize] = ResourceElem {
        item: value,
        kind: Some(kind),
    };
    d.nhash += 1;
}

// Reverse order: highest phase/priority sorts first, releases run from the end.
fn resource_priority_cmp(a: &ResourceElem, b: &ResourceElem) -> core::cmp::Ordering {
    let ka = a.kind.expect("sorting a free slot");
    let kb = b.kind.expect("sorting a free slot");
    let (pa, pb) = (ka.release_phase as u32, kb.release_phase as u32);
    if pa == pb {
        kb.release_priority.cmp(&ka.release_priority)
    } else {
        pb.cmp(&pa)
    }
}

// C uses qsort (unstable); sort_unstable_by matches without the merge-sort
// allocation of `sort_by`. The sorted pre-check mirrors C's Bentley-McIlroy
// qsort, which is ~O(n) on the equal/sorted runs release typically sees;
// ipnsort's small-sort network pays full work there.
fn sort_elems(items: &mut [ResourceElem]) {
    if !items.is_sorted_by(|a, b| resource_priority_cmp(a, b) != core::cmp::Ordering::Greater) {
        items.sort_unstable_by(resource_priority_cmp);
    }
}

fn resource_owner_sort(d: &mut ResourceOwnerData) {
    if d.nhash == 0 {
        sort_elems(&mut d.arr[..d.narr as usize]);
    } else {
        let mut dst = 0usize;
        for idx in 0..d.capacity as usize {
            if d.hash[idx].kind.is_some() {
                if dst != idx {
                    d.hash[dst] = d.hash[idx];
                }
                dst += 1;
            }
        }
        debug_assert!(dst + d.narr as usize <= d.capacity as usize);
        for idx in 0..d.narr as usize {
            d.hash[dst] = d.arr[idx];
            dst += 1;
        }
        debug_assert_eq!(dst, d.nhash as usize + d.narr as usize);
        d.narr = 0;
        d.nhash = dst as u32;
        sort_elems(&mut d.hash[..dst]);
    }
}

#[cold]
#[inline(never)]
fn leak_warning(kind: &'static ResourceOwnerDesc, value: Datum) {
    let res_str = match kind.DebugPrint {
        Some(print) => {
            let cx = ::mcx::MemoryContext::new("ResourceOwnerLeakWarning");
            let s = match print(cx.mcx(), value) {
                Ok(s) => s.as_str().to_string(),
                Err(_) => format!("{} 0x{:x}", kind.name, value.as_usize()),
            };
            s
        }
        None => format!("{} 0x{:x}", kind.name, value.as_usize()),
    };
    let _ = ::elog::elog(WARNING, format!("resource was not closed: {res_str}"));
}

// Collected in chunks: callbacks cannot Remember/Forget on a releasing owner,
// so batching the tail scan is safe; C likewise keeps its cursor in a local
// and writes narr/nhash back after its loop, not per item.
fn resource_owner_release_all(
    owner: ResourceOwner,
    phase: ResourceReleasePhase,
    print_leak_warnings: bool,
) {
    const CHUNK: usize = RESOWNER_ARRAY_SIZE;
    loop {
        // MaybeUninit: only chunk[..n] is written and read; zero-filling 512
        // bytes per pass would dominate the empty-phase sweeps C never pays.
        let mut chunk = [const { core::mem::MaybeUninit::<ResourceElem>::uninit() }; CHUNK];
        let n = with_arena(|a| {
            let d = a.data_mut(owner);
            debug_assert!(d.releasing);
            debug_assert!(d.sorted);
            let in_hash = d.nhash != 0;
            let mut nitems = if in_hash { d.nhash as usize } else { d.narr as usize };
            let mut n = 0;
            while nitems > 0 && n < CHUNK {
                let elem = if in_hash {
                    d.hash[nitems - 1]
                } else {
                    d.arr[nitems - 1]
                };
                let kind = elem.kind.expect("releasing a free slot");
                if (kind.release_phase as u32) > (phase as u32) {
                    break;
                }
                debug_assert_eq!(kind.release_phase, phase);
                chunk[n].write(elem);
                n += 1;
                nitems -= 1;
            }
            if in_hash {
                d.nhash = nitems as u32;
            } else {
                d.narr = nitems as u8;
            }
            n
        });

        for slot in &chunk[..n] {
            // SAFETY: chunk[..n] was written above before the count writeback.
            let elem = unsafe { slot.assume_init() };
            let kind = elem.kind.expect("releasing a free slot");
            if print_leak_warnings {
                leak_warning(kind, elem.item);
            }
            (kind.ReleaseResource)(elem.item);
        }
        if n < CHUNK {
            break;
        }
    }
}

pub fn ResourceOwnerCreate(parent: ResourceOwner, name: &'static str) -> PgResult<ResourceOwner> {
    with_arena(|a| {
        let owner = a.alloc(parent, name)?;
        if !parent.is_null() {
            let old_first = a.data(parent).firstchild;
            a.data_mut(owner).nextchild = old_first;
            a.data_mut(parent).firstchild = owner;
        }
        Ok(owner)
    })
}

pub fn ResourceOwnerEnlarge(owner: ResourceOwner) -> PgResult<()> {
    with_arena(|a| {
        let d = a.data_mut(owner);
        if d.releasing {
            return Err(err_enlarge_after_release());
        }
        if (d.narr as usize) < RESOWNER_ARRAY_SIZE {
            return Ok(());
        }
        enlarge_slow(d)
    })
}

#[inline(never)]
fn enlarge_slow(d: &mut ResourceOwnerData) -> PgResult<()> {
    if d.narr as u32 + d.nhash >= d.grow_at {
        let newcap = if d.capacity > 0 {
            d.capacity * 2
        } else {
            RESOWNER_HASH_INIT_SIZE
        };
        let mut newhash: Vec<ResourceElem> = Vec::new();
        newhash
            .try_reserve_exact(newcap as usize)
            .map_err(|_| oom())?;
        newhash.resize(newcap as usize, ResourceElem::EMPTY);

        // We assume we can't fail below this point.
        let oldhash = core::mem::replace(&mut d.hash, newhash);
        d.capacity = newcap;
        d.grow_at = resowner_hash_max_items(newcap);
        d.nhash = 0;
        for elem in oldhash {
            if let Some(kind) = elem.kind {
                add_to_hash(d, elem.item, kind);
            }
        }
    }

    let narr = d.narr as usize;
    for i in 0..narr {
        let elem = d.arr[i];
        add_to_hash(d, elem.item, elem.kind.expect("array element without kind"));
    }
    d.narr = 0;
    debug_assert!(d.nhash <= d.grow_at);
    Ok(())
}

// Caller must have previously done ResourceOwnerEnlarge.
pub fn ResourceOwnerRemember(
    owner: ResourceOwner,
    value: Datum,
    kind: &'static ResourceOwnerDesc,
) -> PgResult<()> {
    with_arena(|a| {
        let d = a.data_mut(owner);
        debug_assert!(!d.releasing);
        debug_assert!(!d.sorted);
        let idx = d.narr as usize;
        if idx >= RESOWNER_ARRAY_SIZE {
            return Err(err_array_full());
        }
        d.arr[idx] = ResourceElem {
            item: value,
            kind: Some(kind),
        };
        d.narr += 1;
        Ok(())
    })
}

pub fn ResourceOwnerForget(
    owner: ResourceOwner,
    value: Datum,
    kind: &'static ResourceOwnerDesc,
) -> PgResult<()> {
    with_arena(|a| {
        let d = a.data_mut(owner);
        if d.releasing {
            return Err(err_forget_after_release(kind));
        }
        debug_assert!(!d.sorted);

        let narr = d.narr as usize;
        for i in (0..narr).rev() {
            if d.arr[i].item == value && kind_eq(d.arr[i].kind, kind) {
                d.arr[i] = d.arr[narr - 1];
                d.narr -= 1;
                return Ok(());
            }
        }

        if d.nhash > 0 {
            let mask = d.capacity - 1;
            let mut idx = hash_resource_elem(value, kind) & mask;
            for _ in 0..d.capacity {
                let e = &mut d.hash[idx as usize];
                if e.item == value && kind_eq(e.kind, kind) {
                    *e = ResourceElem::EMPTY;
                    d.nhash -= 1;
                    return Ok(());
                }
                idx = (idx + 1) & mask;
            }
        }

        Err(err_not_owned(kind, value, d.name))
    })
}

pub fn ResourceOwnerRelease(
    owner: ResourceOwner,
    phase: ResourceReleasePhase,
    is_commit: bool,
    is_top_level: bool,
) -> PgResult<()> {
    resource_owner_release_internal(owner, phase, is_commit, is_top_level)
}

// C: an overflowed cache passes NULL/0 = lock.c walks the whole LOCALLOCK
// table. Tags are copied out first: the lock manager re-enters resowner
// (ForgetLock on us / RememberLock on the parent) mid-call.
fn reassign_or_release_owner_locks(owner: ResourceOwner, is_commit: bool) -> PgResult<()> {
    let cached = with_arena(|a| {
        let d = a.data(owner);
        (d.nlocks <= MAX_RESOWNER_LOCKS).then(|| (d.locks, d.nlocks as usize))
    });
    let locallocks = cached.as_ref().map(|(locks, n)| &locks[..*n]);
    if is_commit {
        lock_seams::lock_reassign_current_owner::call(locallocks)
    } else {
        lock_seams::lock_release_current_owner::call(locallocks)
    }
}

struct PhasePrep {
    has_items: bool,
    nlocks: u8,
    parent_is_null: bool,
    aio_nonempty: bool,
    has_callbacks: bool,
}

fn prep_phase(a: &mut Arena, owner: ResourceOwner, phase: ResourceReleasePhase) -> PhasePrep {
    let has_callbacks = !a.callbacks.is_empty();
    let d = a.data_mut(owner);
    if !d.releasing {
        debug_assert_eq!(phase, RESOURCE_RELEASE_BEFORE_LOCKS);
        debug_assert!(!d.sorted);
        d.releasing = true;
    }
    if !d.sorted {
        resource_owner_sort(d);
        d.sorted = true;
    }
    PhasePrep {
        has_items: (if d.nhash == 0 { d.narr as u32 } else { d.nhash }) != 0,
        nlocks: d.nlocks,
        parent_is_null: d.parent.is_null(),
        aio_nonempty: !d.aio_handles.is_empty(),
        has_callbacks,
    }
}

fn resource_owner_release_internal(
    owner: ResourceOwner,
    phase: ResourceReleasePhase,
    is_commit: bool,
    is_top_level: bool,
) -> PgResult<()> {
    // Empty-owner fast path — the per-statement TopTransaction/portal owner
    // that never remembered anything: no children, no items, no aio, no
    // registered callbacks. One arena resolve per phase (C's empty walk is a
    // few direct-pointer loads); only the LOCKS-phase side effects remain.
    let fast = with_arena(|a| {
        if !a.callbacks.is_empty() {
            return None;
        }
        let d = a.data_mut(owner);
        if d.firstchild.is_null()
            && d.narr == 0
            && d.nhash == 0
            && d.aio_handles.is_empty()
        {
            // The phase bookkeeping the slow path would have done (sorting
            // zero items is a flag store).
            d.releasing = true;
            d.sorted = true;
            Some((d.nlocks, d.parent.is_null()))
        } else {
            None
        }
    });
    if let Some((nlocks, parent_is_null)) = fast {
        if phase == RESOURCE_RELEASE_LOCKS {
            if is_top_level {
                if owner == TopTransactionResourceOwner() {
                    let save = CURRENT_OWNER.with(|c| c.replace(owner));
                    let result = ::lmgr_proc::ProcReleaseLocks(is_commit).and_then(|()| {
                        predicate_seams::release_predicate_locks::call(is_commit, false)
                    });
                    CURRENT_OWNER.with(|c| c.set(save));
                    result?;
                }
            } else {
                debug_assert!(!parent_is_null);
                if nlocks != 0 {
                    let save = CURRENT_OWNER.with(|c| c.replace(owner));
                    let result = reassign_or_release_owner_locks(owner, is_commit);
                    CURRENT_OWNER.with(|c| c.set(save));
                    result?;
                }
            }
        }
        return Ok(());
    }

    let mut child = with_arena(|a| a.data(owner).firstchild);
    while !child.is_null() {
        let next = with_arena(|a| a.data(child).nextchild);
        resource_owner_release_internal(child, phase, is_commit, is_top_level)?;
        child = next;
    }

    let prep = with_arena(|a| prep_phase(a, owner, phase));

    let save = CURRENT_OWNER.with(|c| c.replace(owner));

    let result = (|| -> PgResult<()> {
        match phase {
            RESOURCE_RELEASE_BEFORE_LOCKS => {
                if prep.has_items {
                    resource_owner_release_all(owner, phase, is_commit);
                }
                if prep.aio_nonempty {
                    // C walks owner->aio_handles head-first; each release
                    // forgets itself (ResourceOwnerForgetAioHandle) or
                    // submits+completes, so re-probe the head each round.
                    loop {
                        let node = with_arena(|a| a.data(owner).aio_handles.first().copied());
                        match node {
                            None => break,
                            Some(node) => {
                                aio_seams::pgaio_io_release_resowner::call(node, !is_commit);
                            }
                        }
                    }
                }
            }
            RESOURCE_RELEASE_LOCKS => {
                if is_top_level {
                    if owner == TopTransactionResourceOwner() {
                        ::lmgr_proc::ProcReleaseLocks(is_commit)?;
                        predicate_seams::release_predicate_locks::call(is_commit, false)?;
                    }
                } else {
                    debug_assert!(!prep.parent_is_null);
                    if prep.nlocks != 0 {
                        reassign_or_release_owner_locks(owner, is_commit)?;
                    }
                }
            }
            RESOURCE_RELEASE_AFTER_LOCKS => {
                if prep.has_items {
                    resource_owner_release_all(owner, phase, is_commit);
                }
            }
        }
        Ok(())
    })();

    if prep.has_callbacks {
        // C iterates head-first over a prepend list = most recently registered
        // first; callbacks may unregister themselves, so snapshot.
        let callbacks = with_arena(|a| a.callbacks.clone());
        for (callback, arg) in callbacks.into_iter().rev() {
            callback(phase, is_commit, is_top_level, arg);
        }
    }

    CURRENT_OWNER.with(|c| c.set(save));
    result
}

pub fn ResourceOwnerReleaseAllOfKind(
    owner: ResourceOwner,
    kind: &'static ResourceOwnerDesc,
) -> PgResult<()> {
    with_arena(|a| {
        let d = a.data_mut(owner);
        if d.releasing {
            return Err(err_forget_after_release(kind));
        }
        debug_assert!(!d.sorted);
        // Block Remember while scanning (an enlarge would lose our position).
        d.releasing = true;
        Ok(())
    })?;

    let mut i = 0usize;
    loop {
        let elem = with_arena(|a| {
            let d = a.data_mut(owner);
            if i >= d.narr as usize {
                return None;
            }
            if kind_eq(d.arr[i].kind, kind) {
                let value = d.arr[i].item;
                d.arr[i] = d.arr[d.narr as usize - 1];
                d.narr -= 1;
                Some(Some(value))
            } else {
                Some(None)
            }
        });
        match elem {
            None => break,
            Some(Some(value)) => (kind.ReleaseResource)(value),
            Some(None) => i += 1,
        }
    }

    let capacity = with_arena(|a| a.data(owner).capacity);
    for idx in 0..capacity as usize {
        let value = with_arena(|a| {
            let d = a.data_mut(owner);
            if kind_eq(d.hash[idx].kind, kind) {
                let value = d.hash[idx].item;
                d.hash[idx] = ResourceElem::EMPTY;
                d.nhash -= 1;
                Some(value)
            } else {
                None
            }
        });
        if let Some(value) = value {
            (kind.ReleaseResource)(value);
        }
    }

    with_arena(|a| a.data_mut(owner).releasing = false);
    Ok(())
}

// Reset-and-reuse arm of Delete + Create for the per-statement session owner
// (per-statement-path.md §3.3 pooling). Equivalent to ResourceOwnerDelete
// followed by ResourceOwnerCreate(NULL, same name) — the handle stays valid
// because the slot generation does not move. Only a fully drained, parentless,
// childless owner with no heap hash qualifies; anything else must take the
// real Delete so C's pfree shape is preserved.
pub fn ResourceOwnerRecycle(owner: ResourceOwner) -> bool {
    debug_assert_ne!(owner, CurrentResourceOwner());
    with_arena(|a| {
        let d = a.data_mut(owner);
        if !d.parent.is_null()
            || !d.firstchild.is_null()
            || d.narr != 0
            || d.nhash != 0
            || d.capacity != 0
            || !d.aio_handles.is_empty()
        {
            return false;
        }
        // Delete's own precondition: nlocks is 0 or the overflow sentinel.
        debug_assert!(d.nlocks == 0 || d.nlocks == MAX_RESOWNER_LOCKS + 1);
        d.nextchild = ResourceOwner::NULL;
        d.releasing = false;
        d.sorted = false;
        d.nlocks = 0;
        true
    })
}

// Caller must have already released all resources in the object tree.
pub fn ResourceOwnerDelete(owner: ResourceOwner) {
    debug_assert_ne!(owner, CurrentResourceOwner());
    #[cfg(debug_assertions)]
    with_arena(|a| {
        let d = a.data(owner);
        debug_assert_eq!(d.narr, 0);
        debug_assert_eq!(d.nhash, 0);
        debug_assert!(d.nlocks == 0 || d.nlocks == MAX_RESOWNER_LOCKS + 1);
    });

    loop {
        let freed = with_arena(|a| {
            let child = a.data(owner).firstchild;
            if child.is_null() {
                reparent_in(a, owner, ResourceOwner::NULL);
                a.freed(owner);
                ResourceOwner::NULL
            } else {
                child
            }
        });
        if freed.is_null() {
            break;
        }
        ResourceOwnerDelete(freed);
    }
}

pub fn ResourceOwnerGetParent(owner: ResourceOwner) -> ResourceOwner {
    with_arena(|a| a.data(owner).parent)
}

pub fn ResourceOwnerNewParent(owner: ResourceOwner, newparent: ResourceOwner) {
    with_arena(|a| reparent_in(a, owner, newparent));
}

fn reparent_in(a: &mut Arena, owner: ResourceOwner, newparent: ResourceOwner) {
    let oldparent = a.data(owner).parent;

    if !oldparent.is_null() {
        if owner == a.data(oldparent).firstchild {
            let nextchild = a.data(owner).nextchild;
            a.data_mut(oldparent).firstchild = nextchild;
        } else {
            let mut child = a.data(oldparent).firstchild;
            while !child.is_null() {
                if owner == a.data(child).nextchild {
                    let nextchild = a.data(owner).nextchild;
                    a.data_mut(child).nextchild = nextchild;
                    break;
                }
                child = a.data(child).nextchild;
            }
        }
    }

    if !newparent.is_null() {
        debug_assert_ne!(owner, newparent);
        let old_first = a.data(newparent).firstchild;
        let d = a.data_mut(owner);
        d.parent = newparent;
        d.nextchild = old_first;
        a.data_mut(newparent).firstchild = owner;
    } else {
        let d = a.data_mut(owner);
        d.parent = ResourceOwner::NULL;
        d.nextchild = ResourceOwner::NULL;
    }
}

pub fn RegisterResourceReleaseCallback(
    callback: ResourceReleaseCallback,
    arg: Datum,
) -> PgResult<()> {
    with_arena(|a| {
        a.callbacks.try_reserve(1).map_err(|_| oom())?;
        a.callbacks.push((callback, arg));
        Ok(())
    })
}

pub fn UnregisterResourceReleaseCallback(callback: ResourceReleaseCallback, arg: Datum) {
    with_arena(|a| {
        // C's list is prepend-ordered and removes the first match from the
        // head, i.e. the most recently registered pair.
        if let Some(idx) = a
            .callbacks
            .iter()
            .rposition(|&(cb, cb_arg)| core::ptr::fn_addr_eq(cb, callback) && cb_arg == arg)
        {
            a.callbacks.remove(idx);
        }
    });
}

pub fn CreateAuxProcessResourceOwner() -> PgResult<()> {
    debug_assert!(AuxProcessResourceOwner().is_null());
    debug_assert!(CurrentResourceOwner().is_null());
    let owner = ResourceOwnerCreate(ResourceOwner::NULL, "AuxiliaryProcess")?;
    AUX_PROCESS_OWNER.with(|c| c.set(owner));
    CURRENT_OWNER.with(|c| c.set(owner));

    ipc_seams::on_shmem_exit::call(ReleaseAuxProcessResourcesCallback, 0);
    Ok(())
}

pub fn ReleaseAuxProcessResources(is_commit: bool) -> PgResult<()> {
    let owner = AuxProcessResourceOwner();
    debug_assert!(!owner.is_null());
    ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, is_commit, true)?;
    ResourceOwnerRelease(owner, RESOURCE_RELEASE_LOCKS, is_commit, true)?;
    ResourceOwnerRelease(owner, RESOURCE_RELEASE_AFTER_LOCKS, is_commit, true)?;
    with_arena(|a| {
        let d = a.data_mut(owner);
        d.releasing = false;
        d.sorted = false;
    });
    Ok(())
}

fn ReleaseAuxProcessResourcesCallback(code: i32, _arg: usize) {
    ReleaseAuxProcessResources(code == 0).expect("ReleaseAuxProcessResources failed at shmem exit");
}

// The locks cache is lossy: past MAX_RESOWNER_LOCKS we stop tracking and
// release falls back to the lock manager's own table.
pub fn ResourceOwnerRememberLock(owner: ResourceOwner, locallock: LOCALLOCKTAG) {
    with_arena(|a| {
        let d = a.data_mut(owner);
        if d.nlocks > MAX_RESOWNER_LOCKS {
            return;
        }
        if d.nlocks < MAX_RESOWNER_LOCKS {
            d.locks[d.nlocks as usize] = locallock;
        }
        d.nlocks += 1;
    });
}

pub fn ResourceOwnerForgetLock(owner: ResourceOwner, locallock: LOCALLOCKTAG) -> PgResult<()> {
    with_arena(|a| {
        let d = a.data_mut(owner);
        if d.nlocks > MAX_RESOWNER_LOCKS {
            return Ok(());
        }
        debug_assert!(d.nlocks > 0);
        let nlocks = d.nlocks as usize;
        for i in (0..nlocks).rev() {
            if locallock == d.locks[i] {
                d.locks[i] = d.locks[nlocks - 1];
                d.nlocks -= 1;
                return Ok(());
            }
        }
        Err(Box::new(PgError::error(format!(
            "lock reference is not owned by resource owner {}",
            d.name
        ))))
    })
}

// AIO handles register inside critical sections, so they bypass ResourceElem.
pub fn ResourceOwnerRememberAioHandle(owner: ResourceOwner, ioh_node: usize) -> PgResult<()> {
    with_arena(|a| {
        let d = a.data_mut(owner);
        d.aio_handles.try_reserve(1).map_err(|_| oom())?;
        d.aio_handles.push(ioh_node);
        Ok(())
    })
}

pub fn ResourceOwnerForgetAioHandle(owner: ResourceOwner, ioh_node: usize) {
    with_arena(|a| {
        let handles = &mut a.data_mut(owner).aio_handles;
        if let Some(idx) = handles.iter().position(|&n| n == ioh_node) {
            handles.remove(idx);
        }
    });
}

pub fn CurrentResourceOwner() -> ResourceOwner {
    CURRENT_OWNER.with(|c| c.get())
}

pub fn SetCurrentResourceOwner(owner: ResourceOwner) {
    CURRENT_OWNER.with(|c| c.set(owner));
}

pub fn CurTransactionResourceOwner() -> ResourceOwner {
    CUR_TRANSACTION_OWNER.with(|c| c.get())
}

pub fn SetCurTransactionResourceOwner(owner: ResourceOwner) {
    CUR_TRANSACTION_OWNER.with(|c| c.set(owner));
}

pub fn TopTransactionResourceOwner() -> ResourceOwner {
    TOP_TRANSACTION_OWNER.with(|c| c.get())
}

pub fn SetTopTransactionResourceOwner(owner: ResourceOwner) {
    TOP_TRANSACTION_OWNER.with(|c| c.set(owner));
}

pub fn AuxProcessResourceOwner() -> ResourceOwner {
    AUX_PROCESS_OWNER.with(|c| c.get())
}

/// M4 bgjobs (docs/design/m4-bgjobs.md §3.4): the aux resource owner is
/// per-DAEMON state, but the cell is per-thread. The job envelope bind
/// points a pool worker's cell at the job's owner for one cycle (buffer
/// pins and the error path's ReleaseAuxProcessResources route through
/// it), and the dispatcher's per-lifecycle reset clears its own stale
/// cells before re-running CreateAuxProcessResourceOwner.
pub fn SetAuxProcessResourceOwner(owner: ResourceOwner) {
    AUX_PROCESS_OWNER.with(|c| c.set(owner));
}

pub fn ResourceOwnerStateClean() -> bool {
    ResourceOwnerStateIssue().is_none()
}

pub fn ResourceOwnerStateIssue() -> Option<&'static str> {
    ResourceOwnerStateIssueAllowing(ResourceOwner::NULL)
}

pub fn ResourceOwnerStateIssueAllowing(parked: ResourceOwner) -> Option<&'static str> {
    if !CurrentResourceOwner().is_null() {
        return Some("current resource owner is still live");
    }
    if !CurTransactionResourceOwner().is_null() {
        return Some("current transaction resource owner is still live");
    }
    if !TopTransactionResourceOwner().is_null() {
        return Some("top transaction resource owner is still live");
    }
    if !AuxProcessResourceOwner().is_null() {
        return Some("auxiliary process resource owner is still live");
    }
    if with_arena(|a| {
        a.slots.iter().enumerate().find_map(|(index, slot)| {
            if !slot.live {
                return None;
            }
            let owner = ResourceOwner::from_parts(index as u32, slot.generation);
            if owner == parked
                && slot.data.parent.is_null()
                && slot.data.firstchild.is_null()
                && slot.data.nextchild.is_null()
                && slot.data.narr == 0
                && slot.data.nhash == 0
                && slot.data.capacity == 0
                && slot.data.aio_handles.is_empty()
                && slot.data.nlocks == 0
                && !slot.data.releasing
            {
                return None;
            }
            Some(slot.data.name)
        })
    })
    .is_some()
    {
        return Some("resource-owner state is not empty");
    }
    None
}

pub fn init_seams() {
    seams::install();
}
