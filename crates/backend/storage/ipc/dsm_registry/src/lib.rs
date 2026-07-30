//! dsm_registry.c. C keeps the registry in a dshash inside its own dsa; dsa/
//! dshash are skipped:thread-model-native, so the table is a process-global
//! vec and DSMRegistryLock covers entry access end-to-end (C's dshash
//! partition lock gave the same one-initializer guarantee).

#![allow(non_snake_case)]

use std::cell::UnsafeCell;
use std::sync::OnceLock;

use elog::{elog, ereport};
use init_small::globals;
use lwlock::{main_lock, LWLock, LWLockAcquire, LWLockRelease, LW_EXCLUSIVE};
use types_error::{ErrorLocation, PgResult, ERROR};
use types_storage::{dsm_handle, DSM_HANDLE_INVALID, DSM_REGISTRY_LOCK};

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

const DSM_REGISTRY_NAME_LEN: usize = 64;

struct DsmRegistryEntry {
    name: [u8; DSM_REGISTRY_NAME_LEN],
    handle: dsm_handle,
    size: usize,
}

struct Registry(UnsafeCell<Vec<DsmRegistryEntry>>);

// SAFETY: the vec is created once in DSMRegistryShmemInit and mutated only
// under DSMRegistryLock LW_EXCLUSIVE (crash reset runs with children dead).
unsafe impl Sync for Registry {}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

pub fn DSMRegistryShmemSize() -> usize {
    // C: MAXALIGN(sizeof(DSMRegistryCtxStruct)) = {dsa_handle, dsa_pointer}.
    16
}

pub fn DSMRegistryShmemInit() {
    REGISTRY
        .set(Registry(UnsafeCell::new(Vec::new())))
        .unwrap_or_else(|_| panic!("DSMRegistryShmemInit called twice"));
}

pub fn DSMRegistryShmemResetAfterCrash() {
    let reg = REGISTRY.get().expect("DSM registry accessed before DSMRegistryShmemInit");
    // SAFETY: crash-cycle single-writer window (see Sync argument above).
    unsafe { (*reg.0.get()).clear() };
}

struct RegistryLockGuard {
    lock: &'static LWLock,
    released: bool,
}

impl RegistryLockGuard {
    fn acquire() -> PgResult<Self> {
        let lock = main_lock(DSM_REGISTRY_LOCK);
        LWLockAcquire(lock, LW_EXCLUSIVE, globals::MyProcNumber())?;
        Ok(RegistryLockGuard { lock, released: false })
    }

    fn release(mut self) -> PgResult<()> {
        self.released = true;
        LWLockRelease(self.lock)
    }
}

impl Drop for RegistryLockGuard {
    // Abort path: C error recovery's LWLockReleaseAll.
    fn drop(&mut self) {
        if !self.released {
            let _ = LWLockRelease(self.lock);
        }
    }
}

pub fn GetNamedDSMSegment(
    name: &str,
    size: usize,
    init_callback: Option<fn(*mut u8)>,
) -> PgResult<(*mut u8, bool)> {
    if name.is_empty() {
        ereport(ERROR)
            .errmsg("DSM segment name cannot be empty")
            .finish(loc("GetNamedDSMSegment"))?;
    }
    if name.len() >= DSM_REGISTRY_NAME_LEN {
        ereport(ERROR)
            .errmsg("DSM segment name too long")
            .finish(loc("GetNamedDSMSegment"))?;
    }
    if size == 0 {
        ereport(ERROR)
            .errmsg("DSM segment size must be nonzero")
            .finish(loc("GetNamedDSMSegment"))?;
    }

    let mut key = [0u8; DSM_REGISTRY_NAME_LEN];
    key[..name.len()].copy_from_slice(name.as_bytes());

    let reg = REGISTRY.get().expect("DSM registry accessed before DSMRegistryShmemInit");
    let guard = RegistryLockGuard::acquire()?;
    // SAFETY: DSMRegistryLock held exclusive for the rest of this function.
    let entries = unsafe { &mut *reg.0.get() };

    let mut found = true;
    let idx = match entries.iter().position(|e| e.name == key) {
        Some(i) => i,
        None => {
            found = false;
            entries.push(DsmRegistryEntry { name: key, handle: DSM_HANDLE_INVALID, size });
            entries.len() - 1
        }
    };
    if found && entries[idx].size != size {
        ereport(ERROR)
            .errmsg("requested DSM segment size does not match size of existing segment")
            .finish(loc("GetNamedDSMSegment"))?;
    }

    let seg_id = if entries[idx].handle == DSM_HANDLE_INVALID {
        found = false;

        let seg = dsm_core::dsm::dsm_create(size, 0)?
            .expect("dsm_create without DSM_CREATE_NULL_IF_MAXSEGMENTS returned no segment");
        if let Some(cb) = init_callback {
            cb(dsm_core::dsm::dsm_segment_address(seg.id()));
        }
        dsm_core::dsm::dsm_pin_segment(seg.id())?;
        let seg_id = dsm_core::dsm::dsm_pin_mapping(seg);
        entries[idx].handle = dsm_core::dsm::dsm_segment_handle(seg_id);
        seg_id
    } else {
        match dsm_core::dsm::dsm_find_mapping(entries[idx].handle) {
            Some(seg_id) => seg_id,
            None => {
                let Some(seg) = dsm_core::dsm::dsm_attach(entries[idx].handle)? else {
                    elog(ERROR, "could not map dynamic shared memory segment")?;
                    unreachable!()
                };
                dsm_core::dsm::dsm_pin_mapping(seg)
            }
        }
    };

    let ret = dsm_core::dsm::dsm_segment_address(seg_id);
    guard.release()?;
    Ok((ret, found))
}

#[cfg(test)]
mod tests;
