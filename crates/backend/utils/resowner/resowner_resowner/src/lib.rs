//! Port of PostgreSQL's resource-owner machinery
//! (`src/backend/utils/resowner/resowner.c`).
//!
//! Query-lifespan resources are tracked by associating them with
//! `ResourceOwner` objects, which form a parent/child tree. Each owner stores
//! its remembered resources in a small fixed-size array (32 entries); when the
//! array fills, the entries are moved into an open-addressing hash table. When
//! it is time to release, the resources are sorted by release phase + priority
//! and the per-kind `ReleaseResource` callback is invoked in that order. Local
//! locks live in a separate lossy 15-entry cache, and AIO handles in their own
//! list (they are registered in critical sections, so cannot use the normal
//! `ResourceElem` mechanism).
//!
//! # Owned-tree representation
//!
//! The C implementation is a pointer graph: owners cross-link via
//! `parent`/`firstchild`/`nextchild`, four process-global variables
//! (`CurrentResourceOwner` etc.) point into the tree, and owners are freed
//! explicitly. This port keeps a process-local arena that owns every
//! `ResourceOwnerData`; a [`ResourceOwner`] (`types_resowner`) is an opaque
//! `Copy` handle (slot + generation), and every cross-link and global is a
//! handle. The arena replaces `TopMemoryContext` ownership: dropping an owner
//! returns its slot to the free list, exactly as `pfree` returned its storage.
//!
//! `kind` identity is preserved by holding a `&'static ResourceOwnerDesc`
//! reference, mirroring C's `const ResourceOwnerDesc *` pointer identity.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use ::utils_error::elog;
use ::hashfn::{hash_combine64, murmurhash64};
use ::datum::Datum;
use ::types_error::{PgError, PgResult, WARNING};
use ::types_resowner::{
    ResourceOwner, ResourceOwnerDesc, ResourceReleasePhase, RESOURCE_RELEASE_AFTER_LOCKS,
    RESOURCE_RELEASE_BEFORE_LOCKS, RESOURCE_RELEASE_LOCKS,
};
use ::types_storage::lock::LOCALLOCKTAG;

mod seam_bodies;

pub use ::types_resowner::{
    ResourceDebugPrintCallback, ResourceReleaseCallback, ResourceReleaseCallbackArg,
    ResourceReleaseResourceCallback, RELEASE_PRIO_BUFFER_IOS, RELEASE_PRIO_BUFFER_PINS,
    RELEASE_PRIO_CATCACHE_LIST_REFS, RELEASE_PRIO_CATCACHE_REFS, RELEASE_PRIO_CRYPTOHASH_CONTEXTS,
    RELEASE_PRIO_DSMS, RELEASE_PRIO_FILES, RELEASE_PRIO_FIRST, RELEASE_PRIO_HMAC_CONTEXTS,
    RELEASE_PRIO_JIT_CONTEXTS, RELEASE_PRIO_LAST, RELEASE_PRIO_PLANCACHE_REFS,
    RELEASE_PRIO_RELCACHE_REFS, RELEASE_PRIO_SNAPSHOT_REFS, RELEASE_PRIO_TUPDESC_REFS,
    RELEASE_PRIO_WAITEVENTSETS,
};

/// `RESOWNER_ARRAY_SIZE` — size of the fixed-size array to hold most-recently
/// remembered resources.
const RESOWNER_ARRAY_SIZE: u32 = 32;

/// `RESOWNER_HASH_INIT_SIZE` — initially allocated size of a `ResourceOwner`'s
/// hash table. Must be a power of two because we use `(capacity - 1)` as the
/// hash mask.
const RESOWNER_HASH_INIT_SIZE: u32 = 64;

/// `MAX_RESOWNER_LOCKS` — size of the per-resource-owner locks cache.
const MAX_RESOWNER_LOCKS: u32 = 15;

/// `RESOWNER_HASH_MAX_ITEMS(capacity)` — how many items may be stored in a hash
/// table of the given capacity before we must resize.
const fn resowner_hash_max_items(capacity: u32) -> u32 {
    let a = capacity - RESOWNER_ARRAY_SIZE;
    let b = capacity / 4 * 3;
    if a < b {
        a
    } else {
        b
    }
}

const _: () = assert!(
    resowner_hash_max_items(RESOWNER_HASH_INIT_SIZE) >= RESOWNER_ARRAY_SIZE,
    "initial hash size too small compared to array size"
);

/// `ResourceElem` — a reference associated with a resource owner: a `Datum`
/// payload plus the [`ResourceOwnerDesc`] describing its kind. `kind == None`
/// indicates a free hash-table slot (C's `kind == NULL`).
#[derive(Clone, Copy)]
struct ResourceElem {
    item: Datum,
    kind: Option<&'static ResourceOwnerDesc>,
}

impl ResourceElem {
    const EMPTY: ResourceElem = ResourceElem {
        item: Datum::null(),
        kind: None,
    };
}

/// `struct ResourceOwnerData` — the owner object.
struct ResourceOwnerData {
    parent: Option<ResourceOwner>,
    firstchild: Option<ResourceOwner>,
    nextchild: Option<ResourceOwner>,
    name: String,

    releasing: bool,
    sorted: bool,

    narr: u32,
    nhash: u32,

    arr: [ResourceElem; RESOWNER_ARRAY_SIZE as usize],

    hash: Vec<ResourceElem>,
    capacity: u32,
    grow_at: u32,

    /// `uint8 nlocks` — number of owned locks. The sentinel value
    /// `MAX_RESOWNER_LOCKS + 1` means "overflowed".
    nlocks: u32,
    /// `LOCALLOCK *locks[MAX_RESOWNER_LOCKS]` — the local locks cache. The C
    /// owner stores `LOCALLOCK *`; in the handle model lock.c identifies a
    /// LOCALLOCK by its stable `LOCALLOCKTAG` key, so that is what we cache.
    locks: [LOCALLOCKTAG; MAX_RESOWNER_LOCKS as usize],

    /// `dlist_head aio_handles` — registered AIO handle nodes (the AIO
    /// subsystem's own `dlist_node` identity, by handle).
    aio_handles: Vec<u64>,
}

impl ResourceOwnerData {
    fn new(parent: Option<ResourceOwner>, name: String) -> Self {
        Self {
            parent,
            firstchild: None,
            nextchild: None,
            name,
            releasing: false,
            sorted: false,
            narr: 0,
            nhash: 0,
            arr: [ResourceElem::EMPTY; RESOWNER_ARRAY_SIZE as usize],
            hash: Vec::new(),
            capacity: 0,
            grow_at: 0,
            nlocks: 0,
            locks: [LOCALLOCKTAG::default(); MAX_RESOWNER_LOCKS as usize],
            aio_handles: Vec::new(),
        }
    }
}

struct ArenaSlot {
    generation: u32,
    data: Option<Box<ResourceOwnerData>>,
}

/// The process-local arena that owns every `ResourceOwnerData`, plus the four
/// global owner variables and the add-on release-callback list. resowner is
/// process-local (its only shmem touch is an `on_shmem_exit` registration), so
/// this is `thread_local` rather than shmem-resident.
#[derive(Default)]
struct ResourceOwnerArena {
    slots: Vec<ArenaSlot>,
    free: Vec<usize>,

    current: Option<ResourceOwner>,
    cur_transaction: Option<ResourceOwner>,
    top_transaction: Option<ResourceOwner>,
    aux_process: Option<ResourceOwner>,

    release_callbacks: Vec<ReleaseCallbackItem>,
}

#[derive(Clone)]
struct ReleaseCallbackItem {
    callback: ResourceReleaseCallback,
    arg: Option<Box<ResourceReleaseCallbackArg>>,
}

thread_local! {
    static ARENA: RefCell<ResourceOwnerArena> = RefCell::new(ResourceOwnerArena::default());
}

impl ResourceOwnerArena {
    fn alloc(&mut self, data: ResourceOwnerData) -> PgResult<ResourceOwner> {
        if let Some(index) = self.free.pop() {
            let slot = &mut self.slots[index];
            slot.data = Some(Box::new(data));
            Ok(ResourceOwner::from_parts(index as u32, slot.generation))
        } else {
            self.slots
                .try_reserve(1)
                .map_err(|_| PgError::error("out of memory"))?;
            let index = self.slots.len();
            self.slots.push(ArenaSlot {
                generation: 0,
                data: Some(Box::new(data)),
            });
            Ok(ResourceOwner::from_parts(index as u32, 0))
        }
    }

    fn data(&self, owner: ResourceOwner) -> &ResourceOwnerData {
        let slot = &self.slots[owner.slot() as usize];
        debug_assert_eq!(slot.generation, owner.generation(), "stale ResourceOwner");
        slot.data.as_ref().expect("ResourceOwner already freed")
    }

    fn data_mut(&mut self, owner: ResourceOwner) -> &mut ResourceOwnerData {
        let slot = &mut self.slots[owner.slot() as usize];
        debug_assert_eq!(slot.generation, owner.generation(), "stale ResourceOwner");
        slot.data.as_mut().expect("ResourceOwner already freed")
    }

    fn freed(&mut self, owner: ResourceOwner) {
        let slot = &mut self.slots[owner.slot() as usize];
        slot.data = None;
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(owner.slot() as usize);
    }
}

fn with_arena<R>(f: impl FnOnce(&mut ResourceOwnerArena) -> R) -> R {
    ARENA.with(|arena| f(&mut arena.borrow_mut()))
}

/* ------------------------------------------------------------------------
 * INTERNAL ROUTINES
 * --------------------------------------------------------------------- */

/// Hash function for a value+kind combination (`hash_resource_elem`).
fn hash_resource_elem(value: Datum, kind: &'static ResourceOwnerDesc) -> u32 {
    let kind_id = core::ptr::from_ref(kind) as usize as u64;
    hash_combine64(murmurhash64(value.as_usize() as u64), kind_id) as u32
}

/// `ResourceOwnerAddToHash` — add `value`/`kind` to the owner's hash table.
fn resource_owner_add_to_hash(
    data: &mut ResourceOwnerData,
    value: Datum,
    kind: &'static ResourceOwnerDesc,
) {
    let mask = data.capacity - 1;
    let mut idx = hash_resource_elem(value, kind) & mask;
    loop {
        if data.hash[idx as usize].kind.is_none() {
            break;
        }
        idx = (idx + 1) & mask;
    }
    data.hash[idx as usize].item = value;
    data.hash[idx as usize].kind = Some(kind);
    data.nhash += 1;
}

/// `resource_priority_cmp` — comparison to sort by release phase + priority, in
/// reverse order (highest phase/priority released first).
fn resource_priority_cmp(a: &ResourceElem, b: &ResourceElem) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    let ka = a.kind.expect("sorting a free slot");
    let kb = b.kind.expect("sorting a free slot");
    if ka.release_phase == kb.release_phase {
        kb.release_priority.cmp(&ka.release_priority)
    } else if ka.release_phase > kb.release_phase {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

/// `ResourceOwnerSort` — sort resources in reverse release priority.
fn resource_owner_sort(data: &mut ResourceOwnerData) {
    if data.nhash == 0 {
        let nitems = data.narr as usize;
        data.arr[..nitems].sort_by(resource_priority_cmp);
    } else {
        let mut dst = 0usize;
        for idx in 0..data.capacity as usize {
            if data.hash[idx].kind.is_some() {
                if dst != idx {
                    data.hash[dst] = data.hash[idx];
                }
                dst += 1;
            }
        }
        debug_assert!(dst + data.narr as usize <= data.capacity as usize);
        for idx in 0..data.narr as usize {
            data.hash[dst] = data.arr[idx];
            dst += 1;
        }
        debug_assert_eq!(dst, (data.nhash + data.narr) as usize);
        data.narr = 0;
        data.nhash = dst as u32;

        let nitems = data.nhash as usize;
        data.hash[..nitems].sort_by(resource_priority_cmp);
    }
}

/// `ResourceOwnerReleaseAll` — call the `ReleaseResource` callback on entries
/// with the given `phase`. `ResourceOwnerSort` must already have been called.
fn resource_owner_release_all(
    owner: ResourceOwner,
    phase: ResourceReleasePhase,
    print_leak_warnings: bool,
) {
    loop {
        let tail = with_arena(|a| {
            let data = a.data(owner);
            debug_assert!(data.releasing);
            debug_assert!(data.sorted);
            let nitems = if data.nhash == 0 { data.narr } else { data.nhash };
            if nitems == 0 {
                return None;
            }
            if data.nhash != 0 {
                debug_assert_eq!(data.narr, 0);
            }
            let idx = (nitems - 1) as usize;
            let elem = if data.nhash == 0 {
                data.arr[idx]
            } else {
                data.hash[idx]
            };
            Some(elem)
        });

        let elem = match tail {
            Some(e) => e,
            None => break,
        };
        let kind = elem.kind.expect("released a free slot");
        if kind.release_phase > phase {
            break;
        }
        debug_assert_eq!(kind.release_phase, phase);

        if print_leak_warnings {
            let res_str = debug_print(kind, elem.item);
            let _ = elog(WARNING, format!("resource was not closed: {res_str}"));
        }

        // The release callback may touch the arena, so call it with no arena
        // borrow held.
        if let Some(release) = kind.ReleaseResource {
            release(elem.item);
        }

        with_arena(|a| {
            let data = a.data_mut(owner);
            if data.nhash == 0 {
                data.narr -= 1;
            } else {
                data.nhash -= 1;
            }
        });
    }
}

/// Format a resource for a leak warning (`kind->DebugPrint` or the generic
/// `"%s %p"` fallback).
fn debug_print(kind: &'static ResourceOwnerDesc, value: Datum) -> String {
    if let Some(debug) = kind.DebugPrint {
        if let Some(text) = debug(value) {
            return text;
        }
    }
    let name = kind.name.as_deref().unwrap_or("");
    format!("{name} 0x{:x}", value.as_usize())
}

/* ------------------------------------------------------------------------
 * EXPORTED ROUTINES
 * --------------------------------------------------------------------- */

/// `ResourceOwnerCreate` — create an empty `ResourceOwner`.
pub fn ResourceOwnerCreate(parent: Option<ResourceOwner>, name: &str) -> PgResult<ResourceOwner> {
    with_arena(|a| {
        let owner = a.alloc(ResourceOwnerData::new(parent, name.to_owned()))?;
        if let Some(parent) = parent {
            let old_first = a.data(parent).firstchild;
            a.data_mut(owner).nextchild = old_first;
            a.data_mut(parent).firstchild = Some(owner);
        }
        Ok(owner)
    })
}

/// `ResourceOwnerEnlarge` — make sure there is room for at least one more
/// resource in the array.
pub fn ResourceOwnerEnlarge(owner: ResourceOwner) -> PgResult<()> {
    with_arena(|a| {
        if a.data(owner).releasing {
            return Err(PgError::error(
                "ResourceOwnerEnlarge called after release started",
            ));
        }

        if a.data(owner).narr < RESOWNER_ARRAY_SIZE {
            return Ok(());
        }

        let (narr, nhash, grow_at) = {
            let d = a.data(owner);
            (d.narr, d.nhash, d.grow_at)
        };
        if narr + nhash >= grow_at {
            let oldcap = a.data(owner).capacity;
            let newcap = if oldcap > 0 {
                oldcap * 2
            } else {
                RESOWNER_HASH_INIT_SIZE
            };

            let mut newhash: Vec<ResourceElem> = Vec::new();
            newhash
                .try_reserve(newcap as usize)
                .map_err(|_| PgError::error("out of memory"))?;
            newhash.resize(newcap as usize, ResourceElem::EMPTY);

            // We assume we can't fail below this point.
            let oldhash = core::mem::replace(&mut a.data_mut(owner).hash, newhash);
            {
                let d = a.data_mut(owner);
                d.capacity = newcap;
                d.grow_at = resowner_hash_max_items(newcap);
                d.nhash = 0;
            }

            for elem in oldhash.into_iter() {
                if let Some(kind) = elem.kind {
                    resource_owner_add_to_hash(a.data_mut(owner), elem.item, kind);
                }
            }
        }

        let narr = a.data(owner).narr as usize;
        for i in 0..narr {
            let elem = a.data(owner).arr[i];
            let kind = elem
                .kind
                .ok_or_else(|| PgError::error("ResourceOwnerEnlarge: array element has no kind"))?;
            resource_owner_add_to_hash(a.data_mut(owner), elem.item, kind);
        }
        a.data_mut(owner).narr = 0;

        debug_assert!(a.data(owner).nhash <= a.data(owner).grow_at);
        Ok(())
    })
}

/// `ResourceOwnerRemember` — remember that an object is owned by a resource
/// owner. The caller must have previously done [`ResourceOwnerEnlarge`].
pub fn ResourceOwnerRemember(
    owner: ResourceOwner,
    value: Datum,
    kind: &'static ResourceOwnerDesc,
) -> PgResult<()> {
    debug_assert_ne!(kind.release_phase, 0);
    debug_assert_ne!(kind.release_priority, 0);

    with_arena(|a| {
        let data = a.data_mut(owner);
        debug_assert!(!data.releasing);
        debug_assert!(!data.sorted);

        if data.narr >= RESOWNER_ARRAY_SIZE {
            return Err(PgError::error(
                "ResourceOwnerRemember called but array was full",
            ));
        }

        let idx = data.narr as usize;
        data.arr[idx].item = value;
        data.arr[idx].kind = Some(kind);
        data.narr += 1;
        Ok(())
    })
}

/// `ResourceOwnerForget` — forget that an object is owned by a resource owner.
pub fn ResourceOwnerForget(
    owner: ResourceOwner,
    value: Datum,
    kind: &'static ResourceOwnerDesc,
) -> PgResult<()> {
    with_arena(|a| {
        if a.data(owner).releasing {
            let name = kind.name.as_deref().unwrap_or("");
            return Err(PgError::error(format!(
                "ResourceOwnerForget called for {name} after release started"
            )));
        }
        debug_assert!(!a.data(owner).sorted);

        {
            let data = a.data_mut(owner);
            let mut i = data.narr as i64 - 1;
            while i >= 0 {
                let idx = i as usize;
                if data.arr[idx].item == value && elem_kind_eq(data.arr[idx].kind, kind) {
                    data.arr[idx] = data.arr[(data.narr - 1) as usize];
                    data.narr -= 1;
                    return Ok(());
                }
                i -= 1;
            }
        }

        if a.data(owner).nhash > 0 {
            let data = a.data_mut(owner);
            let mask = data.capacity - 1;
            let mut idx = hash_resource_elem(value, kind) & mask;
            for _ in 0..data.capacity {
                let slot = &mut data.hash[idx as usize];
                if slot.item == value && elem_kind_eq(slot.kind, kind) {
                    slot.item = Datum::null();
                    slot.kind = None;
                    data.nhash -= 1;
                    return Ok(());
                }
                idx = (idx + 1) & mask;
            }
        }

        let name = kind.name.as_deref().unwrap_or("");
        let owner_name = a.data(owner).name.clone();
        Err(PgError::error(format!(
            "{name} 0x{:x} is not owned by resource owner {owner_name}",
            value.as_usize()
        )))
    })
}

/// Identity comparison of a slot's kind against a target kind, matching C's
/// pointer comparison.
fn elem_kind_eq(slot: Option<&'static ResourceOwnerDesc>, kind: &'static ResourceOwnerDesc) -> bool {
    match slot {
        Some(k) => core::ptr::eq(k, kind),
        None => false,
    }
}

/// `ResourceOwnerRelease` — release all resources owned by a resource owner and
/// its descendants, but don't delete the owner objects themselves.
pub fn ResourceOwnerRelease(
    owner: ResourceOwner,
    phase: ResourceReleasePhase,
    is_commit: bool,
    is_top_level: bool,
) -> PgResult<()> {
    resource_owner_release_internal(owner, phase, is_commit, is_top_level)
}

fn resource_owner_release_internal(
    owner: ResourceOwner,
    phase: ResourceReleasePhase,
    is_commit: bool,
    is_top_level: bool,
) -> PgResult<()> {
    // Recurse to handle descendants.
    let mut child = with_arena(|a| a.data(owner).firstchild);
    while let Some(c) = child {
        let next = with_arena(|a| a.data(c).nextchild);
        resource_owner_release_internal(c, phase, is_commit, is_top_level)?;
        child = next;
    }

    with_arena(|a| {
        let data = a.data_mut(owner);
        if !data.releasing {
            debug_assert_eq!(phase, RESOURCE_RELEASE_BEFORE_LOCKS);
            debug_assert!(!data.sorted);
            data.releasing = true;
        }
        if !data.sorted {
            resource_owner_sort(data);
            data.sorted = true;
        }
    });

    // Make CurrentResourceOwner point to me for the duration of the callbacks.
    let save = CurrentResourceOwner();
    SetCurrentResourceOwner(Some(owner));

    let result = (|| -> PgResult<()> {
        if phase == RESOURCE_RELEASE_BEFORE_LOCKS {
            resource_owner_release_all(owner, phase, is_commit);

            // Release AIO handles. The callee removes the node from the list as
            // part of releasing it, so loop while the list is non-empty.
            loop {
                let node = with_arena(|a| a.data(owner).aio_handles.first().copied());
                match node {
                    Some(node) => aio_seams::pgaio_io_release_resowner::call(
                        node, !is_commit,
                    ),
                    None => break,
                }
            }
        } else if phase == RESOURCE_RELEASE_LOCKS {
            if is_top_level {
                if Some(owner) == TopTransactionResourceOwner() {
                    lmgr_proc::proc_waitqueue::ProcReleaseLocks(is_commit)?;
                    predicate::ReleasePredicateLocks(is_commit, false)?;
                }
            } else {
                debug_assert!(with_arena(|a| a.data(owner).parent.is_some()));

                let locks = with_arena(|a| {
                    let data = a.data(owner);
                    if data.nlocks > MAX_RESOWNER_LOCKS {
                        None
                    } else {
                        Some(data.locks[..data.nlocks as usize].to_vec())
                    }
                });

                if is_commit {
                    lock::LockReassignCurrentOwner(locks.as_deref())?;
                } else {
                    lock::LockReleaseCurrentOwner(locks.as_deref())?;
                }
            }
        } else if phase == RESOURCE_RELEASE_AFTER_LOCKS {
            resource_owner_release_all(owner, phase, is_commit);
        } else {
            return Err(PgError::error("invalid resource release phase"));
        }
        Ok(())
    })();

    call_release_callbacks(phase, is_commit, is_top_level);

    SetCurrentResourceOwner(save);
    result
}

fn call_release_callbacks(phase: ResourceReleasePhase, is_commit: bool, is_top_level: bool) {
    let callbacks = with_arena(|a| a.release_callbacks.clone());
    for item in callbacks {
        (item.callback)(phase, is_commit, is_top_level, item.arg);
    }
}

/// `ResourceOwnerReleaseAllOfKind` — release all resources of a certain type
/// held by this owner.
pub fn ResourceOwnerReleaseAllOfKind(
    owner: ResourceOwner,
    kind: &'static ResourceOwnerDesc,
) -> PgResult<()> {
    if with_arena(|a| a.data(owner).releasing) {
        let name = kind.name.as_deref().unwrap_or("");
        return Err(PgError::error(format!(
            "ResourceOwnerForget called for {name} after release started"
        )));
    }
    debug_assert!(with_arena(|a| !a.data(owner).sorted));

    with_arena(|a| a.data_mut(owner).releasing = true);

    // Array first.
    let mut i = 0i64;
    loop {
        let elem = with_arena(|a| {
            let data = a.data(owner);
            if i >= data.narr as i64 {
                None
            } else {
                Some(data.arr[i as usize])
            }
        });
        let elem = match elem {
            Some(e) => e,
            None => break,
        };
        if elem_kind_eq(elem.kind, kind) {
            let value = elem.item;
            with_arena(|a| {
                let data = a.data_mut(owner);
                data.arr[i as usize] = data.arr[(data.narr - 1) as usize];
                data.narr -= 1;
            });
            i -= 1;
            if let Some(release) = kind.ReleaseResource {
                release(value);
            }
        }
        i += 1;
    }

    // Then the hash.
    let capacity = with_arena(|a| a.data(owner).capacity);
    for idx in 0..capacity as usize {
        let elem = with_arena(|a| a.data(owner).hash[idx]);
        if elem_kind_eq(elem.kind, kind) {
            let value = elem.item;
            with_arena(|a| {
                let data = a.data_mut(owner);
                data.hash[idx].item = Datum::null();
                data.hash[idx].kind = None;
                data.nhash -= 1;
            });
            if let Some(release) = kind.ReleaseResource {
                release(value);
            }
        }
    }

    with_arena(|a| a.data_mut(owner).releasing = false);
    Ok(())
}

/// `ResourceOwnerDelete` — delete an owner object and its descendants.
pub fn ResourceOwnerDelete(owner: ResourceOwner) -> PgResult<()> {
    debug_assert_ne!(Some(owner), CurrentResourceOwner());

    with_arena(|a| {
        let data = a.data(owner);
        debug_assert_eq!(data.narr, 0);
        debug_assert_eq!(data.nhash, 0);
        debug_assert!(data.nlocks == 0 || data.nlocks == MAX_RESOWNER_LOCKS + 1);
    });

    while let Some(child) = with_arena(|a| a.data(owner).firstchild) {
        ResourceOwnerDelete(child)?;
    }

    ResourceOwnerNewParent(owner, None)?;

    with_arena(|a| a.freed(owner));
    Ok(())
}

/// `ResourceOwnerGetParent` — fetch the parent of a resource owner (`None` if
/// top-level).
pub fn ResourceOwnerGetParent(owner: ResourceOwner) -> Option<ResourceOwner> {
    with_arena(|a| a.data(owner).parent)
}

/// `ResourceOwnerNewParent` — reassign a resource owner to have a new parent.
pub fn ResourceOwnerNewParent(
    owner: ResourceOwner,
    newparent: Option<ResourceOwner>,
) -> PgResult<()> {
    with_arena(|a| {
        let oldparent = a.data(owner).parent;

        if let Some(oldparent) = oldparent {
            if Some(owner) == a.data(oldparent).firstchild {
                let nextchild = a.data(owner).nextchild;
                a.data_mut(oldparent).firstchild = nextchild;
            } else {
                let mut child = a.data(oldparent).firstchild;
                while let Some(c) = child {
                    if Some(owner) == a.data(c).nextchild {
                        let nextchild = a.data(owner).nextchild;
                        a.data_mut(c).nextchild = nextchild;
                        break;
                    }
                    child = a.data(c).nextchild;
                }
            }
        }

        if let Some(newparent) = newparent {
            debug_assert_ne!(owner, newparent);
            let old_first = a.data(newparent).firstchild;
            let d = a.data_mut(owner);
            d.parent = Some(newparent);
            d.nextchild = old_first;
            a.data_mut(newparent).firstchild = Some(owner);
        } else {
            let d = a.data_mut(owner);
            d.parent = None;
            d.nextchild = None;
        }
    });
    Ok(())
}

/// `RegisterResourceReleaseCallback` — register a callback for resource cleanup.
pub fn RegisterResourceReleaseCallback(
    callback: ResourceReleaseCallback,
    arg: Option<Box<ResourceReleaseCallbackArg>>,
) -> PgResult<()> {
    with_arena(|a| {
        a.release_callbacks
            .try_reserve(1)
            .map_err(|_| PgError::error("out of memory"))?;
        a.release_callbacks.push(ReleaseCallbackItem { callback, arg });
        Ok(())
    })
}

/// `UnregisterResourceReleaseCallback` — deregister a previously registered
/// callback.
pub fn UnregisterResourceReleaseCallback(
    callback: ResourceReleaseCallback,
    arg: Option<Box<ResourceReleaseCallbackArg>>,
) {
    with_arena(|a| {
        if let Some(index) = a
            .release_callbacks
            .iter()
            .position(|item| core::ptr::fn_addr_eq(item.callback, callback) && item.arg == arg)
        {
            a.release_callbacks.remove(index);
        }
    });
}

/// `CreateAuxProcessResourceOwner` — establish an `AuxProcessResourceOwner` for
/// the current process.
pub fn CreateAuxProcessResourceOwner() -> PgResult<()> {
    debug_assert!(AuxProcessResourceOwner().is_none());
    debug_assert!(CurrentResourceOwner().is_none());
    let owner = ResourceOwnerCreate(None, "AuxiliaryProcess")?;
    SetAuxProcessResourceOwner(Some(owner));
    SetCurrentResourceOwner(Some(owner));

    // Register a shmem-exit callback for cleanup of the aux-process resource
    // owner (this needs to run after, e.g., ShutdownXLOG). C passes
    // `ReleaseAuxProcessResourcesCallback` with arg 0.
    dsm_core_seams::on_shmem_exit::call(
        release_aux_process_resources_shmem_callback,
        types_tuple::Datum::ByVal(0),
    )?;
    Ok(())
}

/// shmem-exit adapter for `ReleaseAuxProcessResourcesCallback`. The C callback
/// signature is `(int code, Datum arg)`; the arg is unused (C passes 0).
fn release_aux_process_resources_shmem_callback(
    code: i32,
    _arg: types_tuple::Datum<'static>,
) -> PgResult<()> {
    ReleaseAuxProcessResourcesCallback(code)
}

/// `ReleaseAuxProcessResources` — release all resources tracked in
/// `AuxProcessResourceOwner`. Warns about leaked resources when `isCommit` is
/// true.
pub fn ReleaseAuxProcessResources(is_commit: bool) -> PgResult<()> {
    let owner = AuxProcessResourceOwner()
        .ok_or_else(|| PgError::error("AuxProcessResourceOwner is not set"))?;
    ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, is_commit, true)?;
    ResourceOwnerRelease(owner, RESOURCE_RELEASE_LOCKS, is_commit, true)?;
    ResourceOwnerRelease(owner, RESOURCE_RELEASE_AFTER_LOCKS, is_commit, true)?;
    with_arena(|a| {
        let data = a.data_mut(owner);
        data.releasing = false;
        data.sorted = false;
    });
    Ok(())
}

/// `ReleaseAuxProcessResourcesCallback` — shmem-exit callback. Warns about
/// leaked resources if the process exit `code` is zero (i.e. normal exit).
pub fn ReleaseAuxProcessResourcesCallback(code: i32) -> PgResult<()> {
    let is_commit = code == 0;
    ReleaseAuxProcessResources(is_commit)
}

/// `ResourceOwnerRememberLock` — remember that a local lock is owned by a
/// resource owner.
pub fn ResourceOwnerRememberLock(owner: ResourceOwner, locallock: LOCALLOCKTAG) {
    with_arena(|a| {
        let data = a.data_mut(owner);
        if data.nlocks > MAX_RESOWNER_LOCKS {
            return; // already overflowed
        }

        if data.nlocks < MAX_RESOWNER_LOCKS {
            data.locks[data.nlocks as usize] = locallock;
        }
        data.nlocks += 1;
    });
}

/// `ResourceOwnerForgetLock` — forget that a local lock is owned by a resource
/// owner.
pub fn ResourceOwnerForgetLock(owner: ResourceOwner, locallock: LOCALLOCKTAG) -> PgResult<()> {
    with_arena(|a| {
        let data = a.data_mut(owner);
        if data.nlocks > MAX_RESOWNER_LOCKS {
            return Ok(()); // overflowed
        }

        debug_assert!(data.nlocks > 0);
        let mut i = data.nlocks as i64 - 1;
        while i >= 0 {
            if locallock == data.locks[i as usize] {
                data.locks[i as usize] = data.locks[(data.nlocks - 1) as usize];
                data.nlocks -= 1;
                return Ok(());
            }
            i -= 1;
        }
        let owner_name = data.name.clone();
        Err(PgError::error(format!(
            "lock reference is not owned by resource owner {owner_name}"
        )))
    })
}

/// `ResourceOwnerRememberAioHandle` — push an AIO handle node onto this owner's
/// AIO list.
pub fn ResourceOwnerRememberAioHandle(owner: ResourceOwner, ioh_node: u64) -> PgResult<()> {
    with_arena(|a| {
        let data = a.data_mut(owner);
        data.aio_handles
            .try_reserve(1)
            .map_err(|_| PgError::error("out of memory"))?;
        data.aio_handles.push(ioh_node);
        Ok(())
    })
}

/// `ResourceOwnerForgetAioHandle` — remove an AIO handle node from this owner's
/// AIO list (`dlist_delete_from`).
pub fn ResourceOwnerForgetAioHandle(owner: ResourceOwner, ioh_node: u64) {
    with_arena(|a| {
        let handles = &mut a.data_mut(owner).aio_handles;
        if let Some(index) = handles.iter().position(|node| *node == ioh_node) {
            handles.remove(index);
        }
    });
}

/* ------------------------------------------------------------------------
 * GLOBAL MEMORY (CurrentResourceOwner etc.)
 * --------------------------------------------------------------------- */

/// `ResourceOwner CurrentResourceOwner`.
pub fn CurrentResourceOwner() -> Option<ResourceOwner> {
    with_arena(|a| a.current)
}

/// Set `CurrentResourceOwner`.
pub fn SetCurrentResourceOwner(owner: Option<ResourceOwner>) {
    with_arena(|a| a.current = owner);
}

/// `ResourceOwner CurTransactionResourceOwner`.
pub fn CurTransactionResourceOwner() -> Option<ResourceOwner> {
    with_arena(|a| a.cur_transaction)
}

/// Set `CurTransactionResourceOwner`.
pub fn SetCurTransactionResourceOwner(owner: Option<ResourceOwner>) {
    with_arena(|a| a.cur_transaction = owner);
}

/// `ResourceOwner TopTransactionResourceOwner`.
pub fn TopTransactionResourceOwner() -> Option<ResourceOwner> {
    with_arena(|a| a.top_transaction)
}

/// Set `TopTransactionResourceOwner`.
pub fn SetTopTransactionResourceOwner(owner: Option<ResourceOwner>) {
    with_arena(|a| a.top_transaction = owner);
}

/// `ResourceOwner AuxProcessResourceOwner`.
pub fn AuxProcessResourceOwner() -> Option<ResourceOwner> {
    with_arena(|a| a.aux_process)
}

/// Set `AuxProcessResourceOwner`.
pub fn SetAuxProcessResourceOwner(owner: Option<ResourceOwner>) {
    with_arena(|a| a.aux_process = owner);
}

/// Collect (without removing) the `item` Datums of every entry of `kind` held
/// by `owner` (array + hash). Used by the plancache release-all seam to drive
/// `ReleaseCachedPlan` re-entry.
pub(crate) fn collect_kind_items(
    owner: ResourceOwner,
    kind: &'static ResourceOwnerDesc,
) -> Vec<Datum> {
    with_arena(|a| {
        let data = a.data(owner);
        let mut out = Vec::new();
        for i in 0..data.narr as usize {
            if elem_kind_eq(data.arr[i].kind, kind) {
                out.push(data.arr[i].item);
            }
        }
        for i in 0..data.capacity as usize {
            if elem_kind_eq(data.hash[i].kind, kind) {
                out.push(data.hash[i].item);
            }
        }
        out
    })
}

/// Install every seam this crate owns.
pub fn init_seams() {
    seam_bodies::install();
}
