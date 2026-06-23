#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! The dynamic shared memory **registry** (`storage/ipc/dsm_registry.c`).
//!
//! The registry lets libraries use shared memory without requesting it at
//! startup via a `shmem_request_hook`: it stores DSM segment handles keyed by a
//! library-specified string. A caller invokes [`GetNamedDSMSegment`]; if a
//! segment with that name does not yet exist it is created and initialized via
//! the caller's `init_callback`, otherwise the call merely ensures the segment
//! is attached to the current backend. The flow guarantees that exactly one
//! backend initializes a segment and that every other backend just attaches it.
//!
//! # In-crate logic vs. externals
//!
//! The in-crate logic is the registry **control block** ([`DSMRegistryCtxStruct`]
//! — the registry DSA handle + the dshash-table handle), the sizing
//! ([`DSMRegistryShmemSize`]), the shmem init ([`DSMRegistryShmemInit`]), the
//! lazy create-or-attach of the backing DSA + dshash ([`init_dsm_registry`]),
//! and the [`GetNamedDSMSegment`] orchestration. The control block is genuinely
//! shmem-resident (placed by `ShmemInitStruct`), so its fields are read and
//! written through a raw pointer, exactly as in C.
//!
//! External pieces reached through seams: shmem placement (`shmem-seams`), the
//! `DSMRegistryLock` (`lwlock-seams`, built-in offset
//! `::types_storage::DSM_REGISTRY_LOCK`), the DSA substrate (`dsa-seams`), and the
//! dshash substrate (`dshash-seams`). The DSM **segment** lifecycle is the
//! ported `backend-storage-ipc-dsm-core` crate, called directly.

use core::cell::Cell;

use ::utils_error::ereport;
use ::mcx::Mcx;
use types_error::{PgResult, ERROR};
use ::types_core::Size;
use types_storage::{
    dsa_handle, dshash_table_handle, DshashKeyKind, DshashParameters, DSM_HANDLE_INVALID,
    DSM_REGISTRY_LOCK, INVALID_DSA_POINTER, LW_EXCLUSIVE, LWTRANCHE_DSM_REGISTRY_DSA,
    LWTRANCHE_DSM_REGISTRY_HASH,
};

use ::dsm_core::dsm::{
    dsm_attach, dsm_create, dsm_find_mapping, dsm_pin_mapping, dsm_pin_segment,
    dsm_segment_address, dsm_segment_handle, DsmSegment,
};

use dshash_seams as dshash;
use ipc_shmem_seams as shmem;
use lwlock_seams as lwlock;
use dsa_seams as dsa;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Constants (dsm_registry.c / dsa.h / dshash.h).
// ---------------------------------------------------------------------------

/// `MAXIMUM_ALIGNOF` on the LP64 targets.
const MAXIMUM_ALIGNOF: usize = 8;

/// `offsetof(DSMRegistryEntry, handle)` — the dshash key size and the
/// name-length limit. `name[64]` starts at offset `0` and `handle` follows it
/// with no preceding padding, so this is exactly `64`.
const DSM_REGISTRY_ENTRY_NAME_LEN: usize = 64;

/// `DSA_HANDLE_INVALID` (`(dsa_handle) DSM_HANDLE_INVALID`).
pub const DSA_HANDLE_INVALID: dsa_handle = DSM_HANDLE_INVALID;

/// `DSHASH_HANDLE_INVALID` (`(dshash_table_handle) InvalidDsaPointer`).
pub const DSHASH_HANDLE_INVALID: dshash_table_handle = INVALID_DSA_POINTER;

// ---------------------------------------------------------------------------
// Shmem-resident control block + dshash entry (dsm_registry.c).
// ---------------------------------------------------------------------------

/// `typedef struct DSMRegistryCtxStruct` — the registry's shmem control block:
/// the registry DSA handle and the registry dshash-table handle. `repr(C)` so
/// [`DSMRegistryShmemSize`] matches the C `sizeof`, and because it lives in
/// genuine shared memory.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DSMRegistryCtxStruct {
    /// `dsa_handle dsah`.
    pub dsah: dsa_handle,
    /// `dshash_table_handle dshh`.
    pub dshh: dshash_table_handle,
}

/// `typedef struct DSMRegistryEntry` — one named registry entry, stored in the
/// registry dshash (in DSA-shared memory). `name` is the dshash key. `repr(C)`
/// because the dshash substrate stores it by its C layout.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DSMRegistryEntry {
    /// `char name[64]` — the dshash key.
    pub name: [core::ffi::c_char; DSM_REGISTRY_ENTRY_NAME_LEN],
    /// `dsm_handle handle`.
    pub handle: ::types_storage::dsm_handle,
    /// `size_t size`.
    pub size: Size,
}

/// `static const dshash_parameters dsh_params` — the registry's dshash key/entry
/// sizes, string-key helper set, and lock tranche. The C `offsetof` /
/// `sizeof` / `LWTRANCHE_DSM_REGISTRY_HASH` are owned here; the
/// `dshash_strcmp`/`dshash_strhash`/`dshash_strcpy` function pointers are the
/// dshash substrate's standard string helpers, named via [`DshashKeyKind`].
fn dsh_params() -> DshashParameters {
    DshashParameters {
        key_size: DSM_REGISTRY_ENTRY_NAME_LEN,
        entry_size: core::mem::size_of::<DSMRegistryEntry>(),
        key_kind: DshashKeyKind::String,
        tranche_id: LWTRANCHE_DSM_REGISTRY_HASH,
    }
}

// ---------------------------------------------------------------------------
// Backend-local file statics (dsm_registry.c).
//
// These mirror C's `DSMRegistryCtx` (a pointer into shmem placed by
// ShmemInitStruct), `dsm_registry_dsa` and `dsm_registry_table` (backend-local
// substrate handles). A backend is single-threaded, so these are thread-locals,
// not shared statics.
// ---------------------------------------------------------------------------

thread_local! {
    /// `static DSMRegistryCtxStruct *DSMRegistryCtx` — pointer into shared
    /// memory (placed by `ShmemInitStruct`).
    static DSM_REGISTRY_CTX: Cell<*mut DSMRegistryCtxStruct> = const { Cell::new(core::ptr::null_mut()) };
    /// `static dsa_area *dsm_registry_dsa`.
    static DSM_REGISTRY_DSA: Cell<*mut ::types_storage::DsaArea> = const { Cell::new(core::ptr::null_mut()) };
    /// `static dshash_table *dsm_registry_table`.
    static DSM_REGISTRY_TABLE: Cell<*mut ::types_storage::DshashTable> = const { Cell::new(core::ptr::null_mut()) };
}

fn registry_ctx() -> *mut DSMRegistryCtxStruct {
    DSM_REGISTRY_CTX.with(Cell::get)
}

// ---------------------------------------------------------------------------
// Sizing / shmem init.
// ---------------------------------------------------------------------------

/// `Size DSMRegistryShmemSize(void)` — bytes to reserve in main shmem for the
/// registry control block.
pub fn DSMRegistryShmemSize() -> Size {
    max_align(core::mem::size_of::<DSMRegistryCtxStruct>())
}

/// `void DSMRegistryShmemInit(void)` — place (or find) the registry control
/// block in shmem and, when freshly created, reset its handle fields to the
/// invalid sentinels.
pub fn DSMRegistryShmemInit() -> PgResult<()> {
    // DSMRegistryCtx = (DSMRegistryCtxStruct *)
    //     ShmemInitStruct("DSM Registry Data", DSMRegistryShmemSize(), &found);
    let (addr, found) = shmem::shmem_init_struct::call("DSM Registry Data", DSMRegistryShmemSize())?;
    let ctx = addr as *mut DSMRegistryCtxStruct;
    DSM_REGISTRY_CTX.with(|c| c.set(ctx));

    if !found {
        // SAFETY: ShmemInitStruct returned a block of DSMRegistryShmemSize()
        // bytes; on the freshly-created path we own it exclusively.
        unsafe {
            (*ctx).dsah = DSA_HANDLE_INVALID;
            (*ctx).dshh = DSHASH_HANDLE_INVALID;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install this crate's inward seams (`ipci.c` reaches the registry's sizing
/// and shmem-init through them). Wired into `seams-init::init_all`.
pub fn init_seams() {
    // `DSMRegistryShmemSize()` never fails (pure `MAXALIGN(sizeof(..))`); the
    // seam's `PgResult<Size>` is just an `Ok` wrapper, so install a thin shim.
    dsm_registry_seams::dsm_registry_shmem_size::set(|| {
        Ok(DSMRegistryShmemSize())
    });
    // `DSMRegistryShmemInit` already returns `PgResult<()>` matching the seam.
    dsm_registry_seams::dsm_registry_shmem_init::set(DSMRegistryShmemInit);
}

// ---------------------------------------------------------------------------
// init_dsm_registry (static).
// ---------------------------------------------------------------------------

/// `static void init_dsm_registry(void)` — initialize or attach to the dynamic
/// shared hash table that stores the registry entries, if not already done.
/// Must be called before accessing the table.
fn init_dsm_registry() -> PgResult<()> {
    // Quick exit if we already did this.
    if !DSM_REGISTRY_TABLE.with(Cell::get).is_null() {
        return Ok(());
    }

    // Otherwise, use a lock to ensure only one process creates the table.
    let lock = lwlock::lwlock_acquire_main::call(DSM_REGISTRY_LOCK, LW_EXCLUSIVE)?;

    let ctx = registry_ctx();
    // SAFETY: ctx points at the shmem control block placed by
    // DSMRegistryShmemInit; this lock serializes access to it.
    let dshh = unsafe { (*ctx).dshh };

    let result = (|| -> PgResult<()> {
        if dshh == DSHASH_HANDLE_INVALID {
            // Initialize dynamic shared hash table for registry.
            let area = dsa::dsa_create::call(LWTRANCHE_DSM_REGISTRY_DSA)?;
            DSM_REGISTRY_DSA.with(|c| c.set(area));
            let table = dshash::dshash_create::call(area, dsh_params())?;
            DSM_REGISTRY_TABLE.with(|c| c.set(table));

            dsa::dsa_pin::call(area)?;
            dsa::dsa_pin_mapping::call(area)?;

            // Store handles in shared memory for other backends to use.
            // SAFETY: as above; the lock is held.
            unsafe {
                (*ctx).dsah = dsa::dsa_get_handle::call(area);
                (*ctx).dshh = dshash::dshash_get_hash_table_handle::call(table);
            }
        } else {
            // Attach to existing dynamic shared hash table.
            // SAFETY: as above; the lock is held.
            let dsah = unsafe { (*ctx).dsah };
            let area = dsa::dsa_attach::call(dsah)?;
            DSM_REGISTRY_DSA.with(|c| c.set(area));
            dsa::dsa_pin_mapping::call(area)?;
            let dshh = unsafe { (*ctx).dshh };
            let table = dshash::dshash_attach::call(area, dsh_params(), dshh)?;
            DSM_REGISTRY_TABLE.with(|c| c.set(table));
        }
        Ok(())
    })();

    // LWLockRelease(DSMRegistryLock) — surface the release error like C, but
    // only after the body (a body error drops `lock`, releasing silently).
    result?;
    lock.release()
}

// ---------------------------------------------------------------------------
// GetNamedDSMSegment.
// ---------------------------------------------------------------------------

/// `void *GetNamedDSMSegment(const char *name, size_t size,
///                           void (*init_callback)(void *ptr), bool *found)` —
/// initialize or attach a named DSM segment, returning the mapped address of
/// the segment. `init_callback` is invoked to initialize the segment when it is
/// first created; it receives the segment's mapped address.
///
/// `found` is set exactly as in C: `true` iff this call merely attached to an
/// already-initialized segment.
///
/// `top_mcx` is the `TopMemoryContext`-equivalent handle: the C
/// `MemoryContextSwitchTo(TopMemoryContext)` here makes the descriptors that
/// `dsm_create`/`dsm_attach` allocate outlive the (short-lived) caller context,
/// so the registry threads that context into those calls.
pub fn GetNamedDSMSegment(
    name: &str,
    size: Size,
    init_callback: Option<&dyn Fn(*mut u8)>,
    found: &mut bool,
    top_mcx: Mcx<'static>,
) -> PgResult<*mut u8> {
    // Assert(found) — the &mut reference guarantees it.

    if name.is_empty() {
        return Err(ereport(ERROR)
            .errmsg("DSM segment name cannot be empty")
            .into_error());
    }

    // strlen(name) >= offsetof(DSMRegistryEntry, handle)
    if name.len() >= DSM_REGISTRY_ENTRY_NAME_LEN {
        return Err(ereport(ERROR)
            .errmsg("DSM segment name too long")
            .into_error());
    }

    if size == 0 {
        return Err(ereport(ERROR)
            .errmsg("DSM segment size must be nonzero")
            .into_error());
    }

    // Be sure any local memory allocated by DSM/DSA routines is persistent.
    //
    // The C `MemoryContextSwitchTo(TopMemoryContext)` keeps the descriptors
    // allocated by `dsm_create`/`dsm_attach` alive past the current (short-lived)
    // context. In this port those descriptors live in the dsm-core crate's
    // backend-local list (lifetime == the backend), so they are already
    // persistent — structurally a no-op here.

    // Connect to the registry.
    init_dsm_registry()?;

    // entry = dshash_find_or_insert(dsm_registry_table, name, found);
    let table = DSM_REGISTRY_TABLE.with(Cell::get);
    // The key is the `const void *key` byte image dshash hashes/compares; for
    // the registry's `DshashKeyKind::String` helper set that is the name's
    // bytes (NUL-padded into the `key_size`-wide field by `dshash_strcpy`).
    let entry_guard = dshash::dshash_find_or_insert::call(table, name.as_bytes())?;
    *found = entry_guard.found;
    let entry = entry_guard.entry_ptr() as *mut DSMRegistryEntry;

    if !*found {
        // SAFETY: entry points at a freshly-inserted entry in DSA-shared
        // memory, sized sizeof(DSMRegistryEntry); the partition lock is held.
        unsafe {
            (*entry).handle = DSM_HANDLE_INVALID;
            (*entry).size = size;
        }
    } else {
        // SAFETY: as above.
        let existing = unsafe { (*entry).size };
        if existing != size {
            // The C `ereport` longjmps past the dangling `dshash_release_lock`;
            // dropping `entry_guard` here releases the partition lock cleanly.
            return Err(ereport(ERROR)
                .errmsg("requested DSM segment size does not match size of existing segment")
                .into_error());
        }
    }

    let ret;

    // SAFETY: lock held; entry valid.
    let existing_handle = unsafe { (*entry).handle };
    if existing_handle == DSM_HANDLE_INVALID {
        *found = false;

        // Initialize the segment.
        //
        // `dsm_create(size, 0)` with flags == 0 never returns None (None is only
        // possible under DSM_CREATE_NULL_IF_MAXSEGMENTS), and on the
        // too-many-segments path it raises ERROR. A `?` here drops `entry_guard`,
        // releasing the partition lock, just as the C longjmp would (the C leaks
        // it; we do not).
        let seg: DsmSegment = dsm_create(size, 0, top_mcx)?
            .expect("dsm_create(size, 0) returned None without DSM_CREATE_NULL_IF_MAXSEGMENTS");
        let seg_id = seg.id();

        let addr = dsm_segment_address(seg_id);
        if let Some(cb) = init_callback {
            cb(addr);
        }

        dsm_pin_segment(seg_id)?;
        // dsm_pin_mapping(seg) — keep the mapping for the session (the C
        // resowner = NULL); consumes the guard.
        dsm_pin_mapping(seg);

        // SAFETY: lock held; entry valid.
        unsafe {
            (*entry).handle = dsm_segment_handle(seg_id);
        }

        ret = dsm_segment_address(seg_id);
    } else {
        // If the existing segment is not already attached, attach it now.
        let handle = existing_handle;
        let seg_id = match dsm_find_mapping(handle) {
            Some(seg_id) => seg_id,
            None => match dsm_attach(handle, top_mcx)? {
                Some(seg) => {
                    // dsm_pin_mapping(seg) — keep for session; consumes guard.
                    dsm_pin_mapping(seg)
                }
                None => {
                    // elog(ERROR, "could not map dynamic shared memory segment").
                    // Dropping `entry_guard` releases the partition lock.
                    return Err(ereport(ERROR)
                        .errmsg_internal("could not map dynamic shared memory segment")
                        .into_error());
                }
            },
        };

        ret = dsm_segment_address(seg_id);
    }

    // dshash_release_lock(dsm_registry_table, entry);
    entry_guard.release();

    // MemoryContextSwitchTo(oldcontext) — no-op, see above.

    Ok(ret)
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// `MAXALIGN(value)`.
fn max_align(value: Size) -> Size {
    (value + MAXIMUM_ALIGNOF - 1) & !(MAXIMUM_ALIGNOF - 1)
}
