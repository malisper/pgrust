//! Custom wait events (wait_event.c): a shared, name<->info registry backing
//! extension (`PG_WAIT_EXTENSION`) and injection-point (`PG_WAIT_INJECTIONPOINT`)
//! wait events. C's shmem dynahash pair (`WaitEventCustomHashByInfo`,
//! `WaitEventCustomHashByName`) becomes a process-lifetime `OnceLock` (one
//! address space here; no `HASH_ATTACH` needed), same dynahash flags as the
//! lock manager's shared table (`HASH_SHARED_MEM | HASH_FIXED_SIZE`).

use core::mem::size_of;
use std::sync::OnceLock;

use dynahash::{hash_create, hash_search, hash_seq_init, hash_seq_search};
use elog::{elog, ereport};
use init_small::globals as g;
use lwlock::{main_lock, LWLockAcquire, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use types_core::fmgr::NAMEDATALEN as C_NAMEDATALEN;
use types_error::{
    ErrorLocation, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};
use types_hash::hsearch::{
    HASHCTL, HASH_BLOBS, HASH_ELEM, HASH_ENTER, HASH_FIND, HASH_FIXED_SIZE, HASH_SEQ_STATUS,
    HASH_SHARED_MEM, HASH_STRINGS, HTAB,
};
use types_storage::storage::{Spinlock, SyncCell, WAIT_EVENT_CUSTOM_LOCK};

use crate::{PG_WAIT_EXTENSION, WAIT_EVENT_CLASS_MASK};

const NAMEDATALEN: usize = C_NAMEDATALEN as usize;
const WAIT_EVENT_CUSTOM_HASH_MAX_SIZE: i32 = 128;
const WAIT_EVENT_CUSTOM_INITIAL_ID: i32 = 1;

#[repr(C)]
struct EntryByInfo {
    wait_event_info: u32,
    wait_event_name: [u8; NAMEDATALEN],
}

#[repr(C)]
struct EntryByName {
    wait_event_name: [u8; NAMEDATALEN],
    wait_event_info: u32,
}

struct Counter {
    mutex: Spinlock,
    next_id: SyncCell<i32>,
}

struct CustomTables {
    by_info: *mut HTAB,
    by_name: *mut HTAB,
    counter: Counter,
}

// SAFETY: both tables are fixed-size (HASH_FIXED_SIZE) shared dynahash
// instances; entry mutation is confined to WaitEventCustomNew under
// WAIT_EVENT_CUSTOM_LOCK, matching C's shmem discipline.
unsafe impl Send for CustomTables {}
unsafe impl Sync for CustomTables {}

static CUSTOM: OnceLock<CustomTables> = OnceLock::new();

fn shared() -> &'static CustomTables {
    CUSTOM
        .get()
        .unwrap_or_else(|| panic!("wait event custom shmem not initialized"))
}

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

fn with_spin<R>(lock: &Spinlock, f: impl FnOnce() -> R) -> R {
    while lock.tas_spin() != 0 {
        core::hint::spin_loop();
    }
    let r = f();
    lock.unlock();
    r
}

fn key_buf(name: &str) -> [u8; NAMEDATALEN] {
    let mut key = [0u8; NAMEDATALEN];
    let n = name.len().min(NAMEDATALEN - 1);
    key[..n].copy_from_slice(&name.as_bytes()[..n]);
    key
}

pub fn WaitEventCustomShmemSize() -> usize {
    dynahash::hash_estimate_size(WAIT_EVENT_CUSTOM_HASH_MAX_SIZE as i64, size_of::<EntryByInfo>())
        + dynahash::hash_estimate_size(
            WAIT_EVENT_CUSTOM_HASH_MAX_SIZE as i64,
            size_of::<EntryByName>(),
        )
}

pub fn WaitEventCustomShmemInit() -> PgResult<()> {
    if CUSTOM.get().is_some() {
        return Ok(());
    }

    let mut by_info_ctl = HASHCTL::default();
    by_info_ctl.keysize = size_of::<u32>();
    by_info_ctl.entrysize = size_of::<EntryByInfo>();
    // HASH_FIXED_SIZE forbids growth: preallocate at max (C grows 16->128).
    let by_info = hash_create(
        "WaitEventCustom hash by wait event information",
        WAIT_EVENT_CUSTOM_HASH_MAX_SIZE as i64,
        &by_info_ctl,
        HASH_ELEM | HASH_BLOBS | HASH_SHARED_MEM | HASH_FIXED_SIZE,
    )?;

    let mut by_name_ctl = HASHCTL::default();
    by_name_ctl.keysize = NAMEDATALEN;
    by_name_ctl.entrysize = size_of::<EntryByName>();
    let by_name = hash_create(
        "WaitEventCustom hash by name",
        WAIT_EVENT_CUSTOM_HASH_MAX_SIZE as i64,
        &by_name_ctl,
        HASH_ELEM | HASH_STRINGS | HASH_SHARED_MEM | HASH_FIXED_SIZE,
    )?;

    let _ = CUSTOM.set(CustomTables {
        by_info,
        by_name,
        counter: Counter {
            mutex: Spinlock::new(),
            next_id: SyncCell::new(WAIT_EVENT_CUSTOM_INITIAL_ID),
        },
    });
    Ok(())
}

// Crash-cycle reset to the post-ShmemInit boot image.
pub fn WaitEventCustomShmemResetAfterCrash() {
    let Some(t) = CUSTOM.get() else { return };
    // SAFETY: crash choreography — children dead, postmaster thread only;
    // both tables are shared + fixed-size (asserted by the callee).
    unsafe {
        dynahash::hash_reset_after_crash(t.by_info);
        dynahash::hash_reset_after_crash(t.by_name);
    }
    t.counter.next_id.set(WAIT_EVENT_CUSTOM_INITIAL_ID);
    t.counter.mutex.unlock();
}

pub fn WaitEventExtensionNew(wait_event_name: &str) -> PgResult<u32> {
    WaitEventCustomNew(crate::PG_WAIT_EXTENSION, wait_event_name)
}

pub fn WaitEventInjectionPointNew(wait_event_name: &str) -> PgResult<u32> {
    WaitEventCustomNew(crate::PG_WAIT_INJECTIONPOINT, wait_event_name)
}

fn check_same_class(existing: u32, class_id: u32, name: &str) -> PgResult<u32> {
    let old_class = existing & WAIT_EVENT_CLASS_MASK;
    if old_class != class_id {
        ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "wait event \"{name}\" already exists in type \"{}\"",
                crate::pgstat_get_wait_event_type(old_class).unwrap_or("???")
            ))
            .finish(loc("WaitEventCustomNew"))?;
        unreachable!("ereport(ERROR) returns Err");
    }
    Ok(existing)
}

pub fn WaitEventCustomNew(class_id: u32, wait_event_name: &str) -> PgResult<u32> {
    if wait_event_name.len() >= NAMEDATALEN {
        elog(
            ERROR,
            format!(
                "cannot use custom wait event string longer than {} characters",
                NAMEDATALEN - 1
            ),
        )?;
        unreachable!("elog(ERROR) returns Err");
    }

    let tables = shared();
    let key = key_buf(wait_event_name);

    LWLockAcquire(main_lock(WAIT_EVENT_CUSTOM_LOCK), LW_SHARED, g::MyProcNumber())?;
    let found = hash_search(tables.by_name, key.as_ptr(), HASH_FIND, None)?;
    LWLockRelease(main_lock(WAIT_EVENT_CUSTOM_LOCK))?;
    if !found.is_null() {
        // SAFETY: HASH_FIND returns a live, never-removed EntryByName.
        let info = unsafe { (*(found as *const EntryByName)).wait_event_info };
        return check_same_class(info, class_id, wait_event_name);
    }

    LWLockAcquire(main_lock(WAIT_EVENT_CUSTOM_LOCK), LW_EXCLUSIVE, g::MyProcNumber())?;
    let found = hash_search(tables.by_name, key.as_ptr(), HASH_FIND, None)?;
    if !found.is_null() {
        // SAFETY: as above.
        let info = unsafe { (*(found as *const EntryByName)).wait_event_info };
        LWLockRelease(main_lock(WAIT_EVENT_CUSTOM_LOCK))?;
        return check_same_class(info, class_id, wait_event_name);
    }

    let next_id = with_spin(&tables.counter.mutex, || {
        let id = tables.counter.next_id.get();
        if id < WAIT_EVENT_CUSTOM_HASH_MAX_SIZE {
            tables.counter.next_id.set(id + 1);
        }
        id
    });
    if next_id >= WAIT_EVENT_CUSTOM_HASH_MAX_SIZE {
        LWLockRelease(main_lock(WAIT_EVENT_CUSTOM_LOCK))?;
        ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("too many custom wait events")
            .finish(loc("WaitEventCustomNew"))?;
        unreachable!("ereport(ERROR) returns Err");
    }

    let wait_event_info = class_id | (next_id as u32);
    let mut found_flag = false;

    let entry_by_info = hash_search(
        tables.by_info,
        &wait_event_info as *const u32 as *const u8,
        HASH_ENTER,
        Some(&mut found_flag),
    )?;
    debug_assert!(!found_flag);
    // SAFETY: freshly HASH_ENTER'd EntryByInfo; the key (wait_event_info) is
    // already written by dynahash's keycopy.
    unsafe { (*(entry_by_info as *mut EntryByInfo)).wait_event_name = key };

    let entry_by_name = hash_search(tables.by_name, key.as_ptr(), HASH_ENTER, Some(&mut found_flag))?;
    debug_assert!(!found_flag);
    // SAFETY: freshly HASH_ENTER'd EntryByName; the key (name) is already
    // written by dynahash's keycopy.
    unsafe { (*(entry_by_name as *mut EntryByName)).wait_event_info = wait_event_info };

    LWLockRelease(main_lock(WAIT_EVENT_CUSTOM_LOCK))?;
    Ok(wait_event_info)
}

pub fn GetWaitEventCustomIdentifier(wait_event_info: u32) -> &'static str {
    if wait_event_info == PG_WAIT_EXTENSION {
        return "Extension";
    }

    let tables = shared();
    LWLockAcquire(main_lock(WAIT_EVENT_CUSTOM_LOCK), LW_SHARED, g::MyProcNumber())
        .expect("GetWaitEventCustomIdentifier: lock");
    let entry = hash_search(
        tables.by_info,
        &wait_event_info as *const u32 as *const u8,
        HASH_FIND,
        None,
    )
    .expect("GetWaitEventCustomIdentifier: hash_search");
    LWLockRelease(main_lock(WAIT_EVENT_CUSTOM_LOCK)).expect("GetWaitEventCustomIdentifier: unlock");

    if entry.is_null() {
        panic!("could not find custom name for wait event information {wait_event_info}");
    }
    // SAFETY: the entry lives in a fixed-size table for the process lifetime
    // (never removed, never resized past its shmem-sized bound).
    let entry: &'static EntryByInfo = unsafe { &*(entry as *const EntryByInfo) };
    trim_name(&entry.wait_event_name)
}

fn trim_name(name: &'static [u8; NAMEDATALEN]) -> &'static str {
    let end = name.iter().position(|&b| b == 0).unwrap_or(NAMEDATALEN);
    core::str::from_utf8(&name[..end]).expect("custom wait event name is valid utf8")
}

// C returns a palloc'd char**; a Vec is the cold-path equivalent.
pub fn GetWaitEventCustomNames(class_id: u32) -> Vec<String> {
    let tables = shared();
    LWLockAcquire(main_lock(WAIT_EVENT_CUSTOM_LOCK), LW_SHARED, g::MyProcNumber())
        .expect("GetWaitEventCustomNames: lock");

    let mut status = HASH_SEQ_STATUS::new();
    hash_seq_init(&mut status, tables.by_name).expect("GetWaitEventCustomNames: hash_seq_init");
    let mut names = Vec::new();
    loop {
        let entry = hash_seq_search(&mut status).expect("GetWaitEventCustomNames: hash_seq_search");
        if entry.is_null() {
            break;
        }
        // SAFETY: hash_seq_search yields entries from a fixed-size table that
        // lives for the process lifetime (never removed, never relocated).
        let entry: &'static EntryByName = unsafe { &*(entry as *const EntryByName) };
        if entry.wait_event_info & WAIT_EVENT_CLASS_MASK != class_id {
            continue;
        }
        names.push(trim_name(&entry.wait_event_name).to_string());
    }

    LWLockRelease(main_lock(WAIT_EVENT_CUSTOM_LOCK)).expect("GetWaitEventCustomNames: unlock");
    names
}
