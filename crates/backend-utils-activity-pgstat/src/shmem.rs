//! `pgstat_shmem.c` core — the DSA/dshash-backed shared statistics hash table
//! and the backend-local entry-reference machinery.
//!
//! The shared statistics system stores every variable-numbered stats object in
//! one dshash table living in a dedicated DSA area. Each shared hash entry is a
//! [`PgStatShared_HashEntry`] whose `body` is a `dsa_pointer` to a
//! kind-specific `PgStatShared_*` struct (all starting with
//! [`PgStatShared_Common`]). Backends keep per-backend
//! [`PgStat_EntryRef`]s caching the resolved shared pointers, in the
//! `pgStatEntryRefHash` lookup table; entries with pending data are linked into
//! `pgStatPending`.
//!
//! This is a faithful port of PG 18.3 `utils/activity/pgstat_shmem.c`: the
//! shared-memory lifecycle (`StatsShmemSize` / `StatsShmemInit`), the
//! attach/detach path, the reference get/create/release/drop machinery over the
//! real `dshash_*` / `dsa_*` substrate, and the entry refcount lifecycle.

#![allow(clippy::missing_safety_doc)]

use core::sync::atomic::Ordering;

use types_core::{Oid, Size};
use types_error::PgResult;
use types_pgstat::activity_pgstat::PgStat_Kind;
use types_pgstat::pgstat_internal::{
    PgStat_HashKey, PgStat_ShmemControl, PgStatShared_Common, PgStatShared_HashEntry,
};
use types_storage::storage::INVALID_DSA_POINTER;
use types_storage::{
    DshashKeyKind, DshashParameters, DsaArea, DshashTable, LW_EXCLUSIVE, LW_SHARED,
    LWTRANCHE_PGSTATS_DATA, LWTRANCHE_PGSTATS_DSA,
};

use crate::entry_ref::{PgStat_EntryRef, PgStat_EntryRefHashEntry};
use crate::local;
use crate::registry;

// dshash / dsa / lwlock / proc / invalidation substrate (real, merged).
use backend_lib_dshash as dshash;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_proc_seams as proc_seams;
use backend_utils_mmgr_dsa_seams as dsa;

/// `PGSTAT_ENTRY_REF_MAGIC` style validity stamp written into a freshly-init'd
/// shared stats body's common header (`pgstat_init_entry`).
const PGSTAT_SHARED_MAGIC: u32 = 0xdeadbeef;

// ---------------------------------------------------------------------------
// Shared-memory lifecycle: StatsShmemSize / StatsShmemInit.
// ---------------------------------------------------------------------------

/// `StatsShmemSize()` (`pgstat_shmem.c`) — bytes of *static* shared memory the
/// cumulative stats subsystem needs. This is only the [`PgStat_ShmemControl`]
/// block; all variable-numbered entries live in the DSA area the control block
/// points at (which grows on demand and is not part of the static shmem
/// budget), exactly as in C (`StatsShmemSize` returns
/// `sizeof(PgStat_ShmemControl)`).
pub fn stats_shmem_size() -> PgResult<Size> {
    Ok(core::mem::size_of::<PgStat_ShmemControl>())
}

/// `StatsShmemInit()` (`pgstat_shmem.c`) — create (or, in EXEC_BACKEND, attach)
/// the cumulative-stats shared-memory structures.
///
/// In C the postmaster path creates the DSA *in place* inside the
/// `ShmemInitStruct`'d control block (`dsa_create_in_place` against
/// `ctl->raw_dsa_area`), creates the dshash on it, and publishes the dshash
/// handle in `ctl->hash_handle`. The merged `dsa.c` substrate offers a
/// `dsa_create(tranche_id)` that hands back the `dsa_area *` handle the C code
/// holds — the faithful contract for the registry/dshash consumers — so this
/// port creates the area with `dsa_create`, pins it for the cluster's lifetime
/// (`dsa_pin` / `dsa_pin_mapping`), creates the dshash, and records both the
/// resolved handles in `pgStatLocal` and the publishable handle / dsa base in
/// the control block.
pub fn stats_shmem_init() -> PgResult<()> {
    // ShmemInitStruct(PgStat_ShmemControl) — owned by the control block in
    // pgStatLocal.shmem (process-global, created once for the cluster).
    let mut ctl = Box::new(PgStat_ShmemControl::default());

    // dsa_create(LWTRANCHE_PGSTATS_DSA) — a fresh DSA area for stats entries.
    let area: *mut DsaArea = dsa::dsa_create::call(LWTRANCHE_PGSTATS_DSA)?;

    // dsa_pin(dsa): the area lives for the cluster's lifetime.
    dsa::dsa_pin::call(area)?;
    // dsa_pin_mapping(dsa): keep this backend's mapping for its lifetime.
    dsa::dsa_pin_mapping::call(area)?;

    // dshash_create(dsa, &dsh_params, NULL) — the shared stats hash.
    let params = dsh_params();
    let dsh: *mut DshashTable = dshash::dshash_create(area, &params)?;

    // Publish the handles: raw_dsa_area base + hash handle in the control block.
    ctl.raw_dsa_area = dsa::dsa_get_handle::call(area) as u64;
    ctl.hash_handle = dshash::dshash_get_hash_table_handle(dsh);
    ctl.is_shutdown = false;

    // Bind pgStatLocal to the live shared state.
    local::with_local(|l| {
        l.shmem = Some(ctl);
        l.dsa = area;
        l.shared_hash = dsh;
    });

    Ok(())
}

/// The dshash parameters used for the shared stats hash table
/// (`dsh_params` in `pgstat_shmem.c`). Binary-keyed on [`PgStat_HashKey`],
/// entry = [`PgStatShared_HashEntry`], partition locks in the
/// `LWTRANCHE_PGSTATS_DATA` tranche.
fn dsh_params() -> DshashParameters {
    DshashParameters {
        key_size: core::mem::size_of::<PgStat_HashKey>(),
        entry_size: core::mem::size_of::<PgStatShared_HashEntry>(),
        key_kind: DshashKeyKind::Binary,
        tranche_id: LWTRANCHE_PGSTATS_DATA,
    }
}

// ---------------------------------------------------------------------------
// attach / detach.
// ---------------------------------------------------------------------------

/// `pgstat_attach_shmem()` (`pgstat_shmem.c`) — bind this backend to the shared
/// stats system: attach the DSA mapping and the shared hash. In this
/// single-postmaster model `StatsShmemInit` already populated `pgStatLocal`
/// with live handles for the creating process; a child that has not attached
/// has `pgStatLocal.dsa == NULL` and re-derives the handles from the published
/// control block (`dsa_attach` + `dshash_attach`).
pub fn pgstat_attach_shmem() -> PgResult<()> {
    // Assert(pgStatLocal.dsa == NULL): only attach once.
    let already = local::with_local(|l| !l.dsa.is_null());
    if already {
        return Ok(());
    }

    // Re-derive the handles from the published control block. (When this
    // backend is the one that ran StatsShmemInit, dsa is already non-NULL and
    // we returned above.)
    let (raw_dsa, hash_handle) = local::with_local(|l| {
        let ctl = l
            .shmem
            .as_ref()
            .expect("pgstat_attach_shmem: shared control block not initialized");
        (ctl.raw_dsa_area, ctl.hash_handle)
    });

    let area = dsa::dsa_attach::call(raw_dsa as u32)?;
    dsa::dsa_pin_mapping::call(area)?;

    let params = dsh_params();
    let dsh = dshash::dshash_attach(area, &params, hash_handle)?;

    local::with_local(|l| {
        l.dsa = area;
        l.shared_hash = dsh;
    });
    Ok(())
}

/// `pgstat_detach_shmem()` (`pgstat_shmem.c`) — release this backend's mapping
/// and shared-hash handle. The shared state itself persists in the DSA segment.
pub fn pgstat_detach_shmem() -> PgResult<()> {
    local::with_local(|l| {
        // dshash_detach(pgStatLocal.shared_hash).
        if !l.shared_hash.is_null() {
            dshash::dshash_detach(l.shared_hash);
            l.shared_hash = core::ptr::null_mut();
        }
        // dsa_detach(pgStatLocal.dsa): clear the backend-local handle. The DSA
        // area itself stays mapped for process lifetime (it was pinned in
        // StatsShmemInit/attach), so we only forget the per-backend handle.
        l.dsa = core::ptr::null_mut();
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// shared-entry pointer helpers.
// ---------------------------------------------------------------------------

/// View a [`PgStat_HashKey`] as the `key_size` bytes the dshash table keys on.
fn key_bytes(key: &PgStat_HashKey) -> &[u8] {
    // SAFETY: PgStat_HashKey is #[repr(C)] with no padding (u32,u32,u64); its
    // size_of bytes are a stable, comparable key, exactly the bytes dshash
    // memcmp/memhash over `key_size`.
    unsafe {
        core::slice::from_raw_parts(
            key as *const PgStat_HashKey as *const u8,
            core::mem::size_of::<PgStat_HashKey>(),
        )
    }
}

/// Reinterpret a dshash entry address as the `PgStatShared_HashEntry` stored
/// there (the dshash entry value starts with the key, then the body).
#[inline]
unsafe fn shared_entry(entry: *mut u8) -> *mut PgStatShared_HashEntry {
    entry as *mut PgStatShared_HashEntry
}

/// `pgstat_get_entry_data(kind, entry)` (`pgstat_shmem.c`) — resolve a shared
/// hash entry's `body` dsa_pointer to its backend-local `PgStatShared_Common`
/// address.
unsafe fn resolve_body(
    area: *mut DsaArea,
    shent: *mut PgStatShared_HashEntry,
) -> PgResult<*mut PgStatShared_Common> {
    let body = (*shent).body;
    if body == INVALID_DSA_POINTER {
        return Ok(core::ptr::null_mut());
    }
    let addr = dsa::dsa_get_address_ptr::call(area, body)?;
    Ok(addr as usize as *mut PgStatShared_Common)
}

// ---------------------------------------------------------------------------
// reinit / setup of a shared entry.
// ---------------------------------------------------------------------------

/// `pgstat_init_entry(kind, shhashent)` (`pgstat_shmem.c`) — allocate and zero
/// the entry's stats body in the DSA area, initialize its common header, and
/// publish the `body` dsa_pointer. Returns the resolved body pointer.
///
/// Runs while the dshash entry is held exclusively; the caller has just created
/// it (`found == false`).
unsafe fn pgstat_init_entry(
    area: *mut DsaArea,
    kind: PgStat_Kind,
    shhashent: *mut PgStatShared_HashEntry,
) -> PgResult<*mut PgStatShared_Common> {
    let kind_info = registry::pgstat_get_kind_info(kind)
        .expect("pgstat_init_entry: kind has no registered KindInfo");
    let shared_size = kind_info.info.shared_size as usize;

    // The refcount starts at 1 to account for the dshash entry's existence
    // (released when the entry is dropped).
    (*shhashent).refcount.store(1, Ordering::Relaxed);
    (*shhashent).generation.store(0, Ordering::Relaxed);
    (*shhashent).dropped = false;

    // dsa_allocate0(dsa, shared_size): zeroed stats body.
    let chunk = dsa::dsa_allocate_extended::call(
        area,
        shared_size,
        types_dsa::DSA_ALLOC_ZERO,
    )?;
    (*shhashent).body = chunk;

    let shheader = dsa::dsa_get_address_ptr::call(area, chunk)? as usize
        as *mut PgStatShared_Common;

    // The stats body's header magic + lock are initialized by the kind's
    // init_shmem path for fixed kinds; for variable kinds the common header is
    // set up here (magic + LWLock in the data tranche), mirroring
    // pgstat_init_entry's `LWLockInitialize(&shheader->lock, ...)`.
    (*shheader).magic = PGSTAT_SHARED_MAGIC;
    lwlock::lwlock_initialize::call(&mut (*shheader).lock, LWTRANCHE_PGSTATS_DATA);

    Ok(shheader)
}

/// `pgstat_reinit_entry(kind, shhashent)` (`pgstat_shmem.c`) — re-activate a
/// previously-dropped entry whose memory is still allocated, bumping its
/// generation so stale references detect the reuse.
unsafe fn pgstat_reinit_entry(shhashent: *mut PgStatShared_HashEntry) {
    // Refcount back to 1 (the dshash entry).
    (*shhashent).refcount.store(1, Ordering::Relaxed);
    (*shhashent).generation.fetch_add(1, Ordering::Relaxed);
    (*shhashent).dropped = false;
}

/// `pgstat_acquire_entry_ref(entry_ref, shhashent, shheader)` tail
/// (`pgstat_shmem.c`) — fill a new backend-local reference's cached shared
/// pointers and generation. The shared refcount bump is done separately by the
/// caller *while the dshash entry lock is held* (matching C's acquire-then-
/// release ordering), so this only caches backend-local pointer state.
unsafe fn pgstat_fill_entry_ref(
    entry_ref: &mut PgStat_EntryRef,
    shhashent: *mut PgStatShared_HashEntry,
    shheader: *mut PgStatShared_Common,
) {
    entry_ref.shared_entry = shhashent;
    entry_ref.shared_stats = shheader;
    entry_ref.generation = (*shhashent).generation.load(Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// pgstat_get_entry_ref / _locked / _exists.
// ---------------------------------------------------------------------------

/// `pgstat_get_entry_ref(kind, dboid, objid, create, created_entry)`
/// (`pgstat_shmem.c`) — return this backend's reference to the shared stats
/// entry for `(kind, dboid, objid)`, creating the shared entry if `create` and
/// it does not yet exist.
///
/// On a cache hit in the backend-local `pgStatEntryRefHash` (and a matching
/// generation) the cached reference is reused without touching shared memory;
/// otherwise the shared dshash is consulted under its partition lock. Returns
/// `None` when `!create` and no entry exists; sets `*created_entry` when a new
/// shared entry was created.
pub fn pgstat_get_entry_ref(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
    create: bool,
    created_entry: Option<&mut bool>,
) -> PgResult<Option<EntryRefPtr>> {
    let key = PgStat_HashKey { kind, dboid, objid };
    let mut created_entry = created_entry;
    if let Some(ce) = created_entry.as_deref_mut() {
        *ce = false;
    }

    // ---- backend-local cache lookup ----
    //
    // pgstat_get_entry_ref's fast path: a cached reference whose generation
    // still matches the live shared entry is reused. Generation mismatch (the
    // entry was reinitialized) or `dropped` forces a re-resolve.
    let cached = local::with_pending(|p| {
        p.entry_ref_hash
            .get(&key)
            .map(|e| e.entry_ref.as_ref() as *const PgStat_EntryRef as *mut PgStat_EntryRef)
    });
    if let Some(existing) = cached {
        // SAFETY: pointer into the owner-private boxed entry-ref in the hash.
        let er = unsafe { &mut *existing };
        if !er.shared_entry.is_null() {
            // SAFETY: shared_entry points into the shared dshash segment.
            let gen = unsafe { (*er.shared_entry).generation.load(Ordering::Relaxed) };
            let dropped = unsafe { (*er.shared_entry).dropped };
            if gen == er.generation && !dropped {
                return Ok(Some(EntryRefPtr(existing)));
            }
        }
        // Stale: fall through to re-resolve (C drops the stale entry-ref hash
        // slot in pgstat_release_entry_ref before re-fetching).
        pgstat_release_entry_ref_for_key(&key)?;
    }

    let (area, dsh) = local::with_local(|l| (l.dsa, l.shared_hash));
    if dsh.is_null() {
        pgstat_attach_shmem()?;
    }
    let (area, dsh) = if area.is_null() {
        local::with_local(|l| (l.dsa, l.shared_hash))
    } else {
        (area, dsh)
    };

    // ---- shared dshash lookup / insert ----
    let (shhashent, shheader, created) = if create {
        let mut found = false;
        let entry = dshash::dshash_find_or_insert(dsh, key_bytes(&key), &mut found)?;
        // SAFETY: dshash returned a live, exclusively-locked entry value.
        let shhashent = unsafe { shared_entry(entry) };
        // SAFETY: dshash returned a live, exclusively-locked entry value. The
        // refcount is bumped (pgstat_acquire_entry_ref's atomic) while the
        // dshash partition lock is still held, so no concurrent drop can free
        // the body before this backend's reference is counted — exactly C's
        // ordering (acquire-then-release).
        unsafe {
            let shheader = if !found {
                // Brand new dshash slot: stamp the key and init the body.
                (*shhashent).key = key;
                pgstat_init_entry(area, kind, shhashent)?
            } else if (*shhashent).dropped {
                // Re-activate a dropped-but-not-yet-freed (or freed) entry.
                pgstat_reinit_entry(shhashent);
                if (*shhashent).body == INVALID_DSA_POINTER {
                    pgstat_init_entry(area, kind, shhashent)?
                } else {
                    resolve_body(area, shhashent)?
                }
            } else {
                resolve_body(area, shhashent)?
            };
            // Count this backend's new reference while still locked.
            (*shhashent).refcount.fetch_add(1, Ordering::Relaxed);
            dshash::dshash_release_lock(dsh, entry)?;
            (shhashent, shheader, !found)
        }
    } else {
        // dshash_find(dsh, &key, false) — no-create lookup.
        match dshash::dshash_find(dsh, key_bytes(&key), false)? {
            None => return Ok(None),
            Some(entry) => {
                // SAFETY: dshash returned a live, shared-locked entry value.
                let shhashent = unsafe { shared_entry(entry) };
                let dropped = unsafe { (*shhashent).dropped };
                if dropped {
                    dshash::dshash_release_lock(dsh, entry)?;
                    return Ok(None);
                }
                let shheader = unsafe { resolve_body(area, shhashent)? };
                // Count this backend's new reference while still locked.
                unsafe { (*shhashent).refcount.fetch_add(1, Ordering::Relaxed) };
                dshash::dshash_release_lock(dsh, entry)?;
                (shhashent, shheader, false)
            }
        }
    };

    // ---- install backend-local entry-ref ----
    // The shared refcount was already bumped under the dshash lock above; here we
    // only cache the resolved shared pointers + generation into the new ref.
    let mut new_ref = Box::new(PgStat_EntryRef::new());
    // SAFETY: shhashent/shheader are live shared addresses.
    unsafe { pgstat_fill_entry_ref(&mut new_ref, shhashent, shheader) };

    let raw = local::with_pending(|p| {
        let slot = p.entry_ref_hash.entry(key).or_insert(PgStat_EntryRefHashEntry {
            key,
            entry_ref: Box::new(PgStat_EntryRef::new()),
        });
        slot.entry_ref = new_ref;
        slot.entry_ref.as_mut() as *mut PgStat_EntryRef
    });

    if created {
        if let Some(ce) = created_entry.as_deref_mut() {
            *ce = true;
        }
    }

    Ok(Some(EntryRefPtr(raw)))
}

/// `pgstat_read_statsfile` helper — create a brand-new shared stats entry for
/// `key`, mirroring C's `dshash_find_or_insert(...)` + `pgstat_init_entry(...)`
/// during the on-disk restore. Returns the freshly-init'd body header, or `None`
/// if an entry for the key already existed (C's "duplicate stats entry" error).
///
/// Unlike [`pgstat_get_entry_ref`], this intentionally does *not* populate the
/// backend-local `pgStatEntryRefHash`: C avoids that during restore ("putting all
/// stats into checkpointer's pgStatEntryRefHash would be wasted effort and
/// memory").
pub fn pgstat_restore_create_entry(
    key: PgStat_HashKey,
) -> PgResult<Option<*mut PgStatShared_Common>> {
    let dsh = local::with_local(|l| l.shared_hash);
    if dsh.is_null() {
        pgstat_attach_shmem()?;
    }
    let (area, dsh) = local::with_local(|l| (l.dsa, l.shared_hash));

    let mut found = false;
    let entry = dshash::dshash_find_or_insert(dsh, key_bytes(&key), &mut found)?;
    // SAFETY: dshash returned a live, exclusively-locked entry value.
    let shhashent = unsafe { shared_entry(entry) };
    if found {
        // Duplicate entry — C releases the lock and treats it as a corrupt file.
        dshash::dshash_release_lock(dsh, entry)?;
        return Ok(None);
    }
    // SAFETY: brand-new dshash slot: stamp the key and init the body.
    let shheader = unsafe {
        (*shhashent).key = key;
        pgstat_init_entry(area, key.kind, shhashent)?
    };
    dshash::dshash_release_lock(dsh, entry)?;
    Ok(Some(shheader))
}

/// `pgstat_write_statsfile` helper — visit every live (non-dropped) variable
/// stats entry, calling `f(key, body)` with each entry's key and resolved body
/// header. Mirrors C's `dshash_seq_init` / `dshash_seq_next` walk in
/// `pgstat_write_statsfile`. The lock is shared (no entries are removed).
pub fn pgstat_for_each_entry(
    mut f: impl FnMut(PgStat_HashKey, *mut PgStatShared_Common) -> PgResult<()>,
) -> PgResult<()> {
    let (area, dsh) = local::with_local(|l| (l.dsa, l.shared_hash));
    if dsh.is_null() {
        return Ok(());
    }
    let mut hstat = dshash::dshash_seq_init(dsh, false);
    loop {
        let entry = match dshash::dshash_seq_next(&mut hstat)? {
            Some(e) => e,
            None => break,
        };
        // SAFETY: dshash_seq_next returns a live, locked entry address.
        let shent = unsafe { shared_entry(entry) };
        if unsafe { (*shent).dropped } {
            continue;
        }
        let key = unsafe { (*shent).key };
        let body = unsafe { resolve_body(area, shent)? };
        if body.is_null() {
            continue;
        }
        f(key, body)?;
    }
    dshash::dshash_seq_term(&mut hstat)?;
    Ok(())
}

/// A returned, backend-local reference to a shared stats entry. The pointer is
/// stable for as long as the entry stays in `pgStatEntryRefHash`; callers hold
/// it only transiently while recording pending stats. Mirrors the C
/// `PgStat_EntryRef *` return.
#[derive(Clone, Copy)]
pub struct EntryRefPtr(pub *mut PgStat_EntryRef);

impl EntryRefPtr {
    /// Borrow the referenced entry mutably.
    ///
    /// # Safety
    /// The reference must still be live in `pgStatEntryRefHash` (not released).
    pub unsafe fn get(self) -> &'static mut PgStat_EntryRef {
        &mut *self.0
    }
}

/// `pgstat_get_entry_ref_locked(kind, dboid, objid, nowait)`
/// (`pgstat_shmem.c`) — like [`pgstat_get_entry_ref`] with `create = true`, but
/// also acquires the shared entry's content lock (exclusive). Returns the
/// reference; the caller releases the lock with [`pgstat_unlock_entry`].
pub fn pgstat_get_entry_ref_locked(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
    nowait: bool,
) -> PgResult<Option<EntryRefPtr>> {
    let r = pgstat_get_entry_ref(kind, dboid, objid, true, None)?;
    if let Some(er) = r {
        // SAFETY: just-resolved live reference.
        let entry = unsafe { er.get() };
        let mode = LW_EXCLUSIVE;
        if !entry.shared_stats.is_null() {
            // SAFETY: shared_stats points at a live PgStatShared_Common with a
            // valid LWLock in shared memory.
            let lock = unsafe { &(*entry.shared_stats).lock };
            if nowait {
                // Best-effort: try to acquire; on contention C returns NULL.
                let acquired =
                    lwlock::lwlock_acquire::call(lock, mode, proc_seams::my_proc_number::call());
                match acquired {
                    // Hold the lock across the return: skip the guard's
                    // Drop-release; pgstat_unlock_entry releases it later
                    // (mirrors C returning with the content lock held).
                    Ok(guard) => core::mem::forget(guard),
                    Err(_) => return Ok(None),
                }
            } else {
                let guard =
                    lwlock::lwlock_acquire::call(lock, mode, proc_seams::my_proc_number::call())?;
                core::mem::forget(guard);
            }
        }
    }
    Ok(r)
}

/// `pgstat_lock_entry(entry_ref, nowait)` (`pgstat_shmem.c`) — acquire a
/// reference's shared content lock (exclusive). Returns `false` on `nowait`
/// contention.
pub fn pgstat_lock_entry(entry_ref: &PgStat_EntryRef, nowait: bool) -> PgResult<bool> {
    pgstat_lock_entry_mode(entry_ref, LW_EXCLUSIVE, nowait)
}

/// `pgstat_lock_entry_shared(entry_ref, nowait)` (`pgstat_shmem.c`).
pub fn pgstat_lock_entry_shared(entry_ref: &PgStat_EntryRef, nowait: bool) -> PgResult<bool> {
    pgstat_lock_entry_mode(entry_ref, LW_SHARED, nowait)
}

fn pgstat_lock_entry_mode(
    entry_ref: &PgStat_EntryRef,
    mode: types_storage::LWLockMode,
    nowait: bool,
) -> PgResult<bool> {
    if entry_ref.shared_stats.is_null() {
        return Ok(true);
    }
    // SAFETY: shared_stats points at a live shared header.
    let lock = unsafe { &(*entry_ref.shared_stats).lock };
    let r = lwlock::lwlock_acquire::call(lock, mode, proc_seams::my_proc_number::call());
    match r {
        Ok(guard) => {
            // Hold across return; pgstat_unlock_entry releases it.
            core::mem::forget(guard);
            Ok(true)
        }
        Err(e) => {
            if nowait {
                Ok(false)
            } else {
                Err(e)
            }
        }
    }
}

/// `pgstat_unlock_entry(entry_ref)` (`pgstat_shmem.c`) — release a reference's
/// shared content lock.
pub fn pgstat_unlock_entry(entry_ref: &PgStat_EntryRef) -> PgResult<()> {
    if entry_ref.shared_stats.is_null() {
        return Ok(());
    }
    // SAFETY: shared_stats points at a live shared header.
    let lock = unsafe { &(*entry_ref.shared_stats).lock };
    lwlock::lwlock_release::call(lock)
}

/// `pgstat_get_entry_ref(kind, dboid, objid, false, NULL) != NULL`
/// (`pgstat_shmem.c`) — existence verdict for the
/// `pgstat_get_entry_ref_exists` seam.
pub fn pgstat_get_entry_ref_exists(kind: PgStat_Kind, dboid: Oid, objid: u64) -> PgResult<bool> {
    Ok(pgstat_get_entry_ref(kind, dboid, objid, false, None)?.is_some())
}

// ---------------------------------------------------------------------------
// release / drop.
// ---------------------------------------------------------------------------

/// `pgstat_release_entry_ref(key, entry_ref, discard_pending)`
/// (`pgstat_shmem.c`) — drop this backend's reference: decrement the shared
/// refcount, free the shared entry's body if we were the last reference to a
/// dropped entry, and remove the backend-local entry-ref hash slot.
fn pgstat_release_entry_ref_for_key(key: &PgStat_HashKey) -> PgResult<()> {
    let shared_entry: *mut PgStatShared_HashEntry = local::with_pending(|p| {
        p.entry_ref_hash
            .remove(key)
            .map(|e| e.entry_ref.shared_entry)
            .unwrap_or(core::ptr::null_mut())
    });

    if shared_entry.is_null() {
        return Ok(());
    }

    let (area, dsh) = local::with_local(|l| (l.dsa, l.shared_hash));

    // SAFETY: shared_entry points into the live shared dshash segment.
    let last = unsafe { (*shared_entry).refcount.fetch_sub(1, Ordering::AcqRel) == 1 };
    if last {
        // We were the last reference. If the entry is dropped, free its body
        // and delete the dshash slot.
        unsafe {
            if (*shared_entry).dropped {
                let body = (*shared_entry).body;
                let key_for_delete = (*shared_entry).key;
                if body != INVALID_DSA_POINTER {
                    dsa::dsa_free_ptr::call(area, body)?;
                    (*shared_entry).body = INVALID_DSA_POINTER;
                }
                // dshash_delete_key(dsh, &key): remove the now-orphaned slot.
                let _ = dshash::dshash_delete_key(dsh, key_bytes(&key_for_delete))?;
            }
        }
    }
    Ok(())
}

/// `pgstat_drop_entry(kind, dboid, objid)` (`pgstat_shmem.c`) — mark a shared
/// stats entry as dropped so all backends release their references, freeing it
/// once the last reference is gone. Returns `true` if the entry was freed
/// immediately (no other live references), `false` if a GC is needed.
pub fn pgstat_drop_entry(kind: PgStat_Kind, dboid: Oid, objid: u64) -> PgResult<bool> {
    // For database drops, C also drops the per-database contents; that broader
    // sweep (pgstat_drop_database_and_contents) lands with the database-kind
    // crate. The single-entry drop is the faithful core here.
    let _ = kind;
    let key = PgStat_HashKey { kind, dboid, objid };

    let (area, dsh) = local::with_local(|l| (l.dsa, l.shared_hash));
    if dsh.is_null() {
        pgstat_attach_shmem()?;
    }
    let (area, dsh) = if dsh.is_null() {
        local::with_local(|l| (l.dsa, l.shared_hash))
    } else {
        (area, dsh)
    };

    // Drop our own backend-local reference, if any.
    pgstat_release_entry_ref_for_key(&key)?;

    match dshash::dshash_find(dsh, key_bytes(&key), true)? {
        None => Ok(true),
        Some(entry) => {
            // SAFETY: live, exclusively-locked dshash entry.
            let shhashent = unsafe { shared_entry(entry) };
            let freed = unsafe {
                (*shhashent).dropped = true;
                let refcount = (*shhashent).refcount.load(Ordering::Relaxed);
                if refcount <= 1 {
                    // No other live backend reference (only the dshash entry's
                    // own reference): free immediately.
                    let body = (*shhashent).body;
                    if body != INVALID_DSA_POINTER {
                        dsa::dsa_free_ptr::call(area, body)?;
                        (*shhashent).body = INVALID_DSA_POINTER;
                    }
                    dshash::dshash_delete_entry(dsh, entry)?;
                    true
                } else {
                    dshash::dshash_release_lock(dsh, entry)?;
                    // Other backends still reference it; ask them to GC.
                    pgstat_request_entry_refs_gc()?;
                    false
                }
            };
            Ok(freed)
        }
    }
}

/// `pgstat_drop_entry_internal(shent, hstat)` (`pgstat_shmem.c`) — mark the
/// seq-scan-current shared entry dropped and, if we held the last reference,
/// free its body and delete it from the table. The caller has already released
/// its backend-local reference. Returns `true` if the entry was freed.
///
/// # Safety
/// `shent` must be the live, exclusively-locked entry the scan currently sits
/// on (`hstat.curitem`).
unsafe fn pgstat_drop_entry_internal(
    area: *mut DsaArea,
    hstat: &mut dshash::DshashSeqStatus,
    shent: *mut PgStatShared_HashEntry,
) -> PgResult<bool> {
    debug_assert!((*shent).body != INVALID_DSA_POINTER);

    // Signal that the entry is dropped - this eventually causes other backends
    // to release their references.
    debug_assert!(!(*shent).dropped, "trying to drop stats entry already dropped");
    (*shent).dropped = true;

    // Release the refcount marking the entry as not dropped.
    if (*shent).refcount.fetch_sub(1, Ordering::AcqRel) == 1 {
        // pgstat_free_entry: fetch the dsa pointer, delete the current scan
        // entry, then free the body.
        let pdsa = (*shent).body;
        dshash::dshash_delete_current(hstat)?;
        dsa::dsa_free_ptr::call(area, pdsa)?;
        Ok(true)
    } else {
        // Other backends still hold references; the seq scan keeps its lock.
        Ok(false)
    }
}

/// `pgstat_drop_matching_entries(do_drop, match_data)` (`pgstat_shmem.c`) with
/// `do_drop == NULL` — drop *every* variable-numbered shared stats entry. Used
/// by [`pgstat_drop_all_entries`].
///
/// Walks the shared dshash under an exclusive seq scan, releasing each backend's
/// local reference and then dropping the entry. Entries still referenced by
/// other backends request a GC.
pub fn pgstat_drop_all_entries() -> PgResult<()> {
    let (area, dsh) = local::with_local(|l| (l.dsa, l.shared_hash));
    if dsh.is_null() {
        // Shared stats not attached: nothing to drop.
        return Ok(());
    }

    let mut not_freed_count: u64 = 0;

    // Entries are removed, so take an exclusive lock.
    let mut hstat = dshash::dshash_seq_init(dsh, true);
    loop {
        let entry = match dshash::dshash_seq_next(&mut hstat)? {
            Some(e) => e,
            None => break,
        };
        // SAFETY: dshash_seq_next returns a live, locked entry address.
        let shent = unsafe { shared_entry(entry) };

        if unsafe { (*shent).dropped } {
            continue;
        }

        // do_drop == NULL: drop every entry.

        // Delete the backend-local reference, if any.
        let key = unsafe { (*shent).key };
        pgstat_release_entry_ref_for_key(&key)?;

        if !unsafe { pgstat_drop_entry_internal(area, &mut hstat, shent)? } {
            not_freed_count += 1;
        }
    }
    dshash::dshash_seq_term(&mut hstat)?;

    if not_freed_count > 0 {
        pgstat_request_entry_refs_gc()?;
    }
    Ok(())
}

/// `pgstat_request_entry_refs_gc()` (`pgstat_shmem.c`) — bump the shared
/// gc-request counter so every backend garbage-collects its stale entry refs.
pub fn pgstat_request_entry_refs_gc() -> PgResult<()> {
    local::with_local(|l| {
        if let Some(ctl) = l.shmem.as_ref() {
            ctl.gc_request_count.fetch_add(1, Ordering::Relaxed);
        }
    });
    Ok(())
}
