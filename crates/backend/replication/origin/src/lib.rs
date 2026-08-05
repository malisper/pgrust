// origin.c (replication/logical/origin.c): replication-origin catalog
// entries, the shared ReplicationState progress array, session origins, the
// replorigin_checkpoint file, and the REPLORIGIN rmgr redo.
//
// Thread-model renderings (see notes/recovery-standby-tail-state.md):
// - C's file-static `session_replication_state` pointer is a thread-local
//   Option<&'static ReplicationState>; acquired_by holds MyProcPid as in C.
// - The shmem ReplicationStateCtl array is a leaked boot allocation behind a
//   OnceLock (the slot crate's pattern); crash-cycle reset rewrites in place.
// - The exported globals replorigin_session_origin{,_lsn,_timestamp} are
//   thread-locals exposed through origin_seams for xact's commit records.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;
use std::rc::Rc;
use pgsync::OnceLock;

use condition_variable::{
    ConditionVariable, ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariableSleep,
};
use datum::Datum;
use elog::{elog, ereport, errno};
use lwlock::{LWLockAcquire, LWLockPadded, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use mcx::Mcx;
use types_core::{
    AttrNumber, DoNotReplicateId, InvalidOid, InvalidRepOriginId, InvalidXLogRecPtr, Oid,
    RepOriginId, TimestampTz, XLogRecPtr,
};
use types_core::primitive::RmgrIds::RM_REPLORIGIN_ID;
use types_error::{
    ErrorLocation, PgResult, DEBUG2, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED, ERRCODE_DATA_CORRUPTED,
    ERRCODE_OBJECT_IN_USE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_READ_ONLY_SQL_TRANSACTION, ERRCODE_UNDEFINED_OBJECT,
    ERROR, LOG, PANIC,
};
use types_storage::{SyncCell, LWTRANCHE_REPLICATION_ORIGIN_STATE, REPLICATION_ORIGIN_LOCK};

mod funcs;
pub use funcs::ORIGIN_BUILTINS;

const PG_REPLORIGIN_CHECKPOINT_FILENAME: &str = "pg_logical/replorigin_checkpoint";
const PG_REPLORIGIN_CHECKPOINT_TMPFILE: &str = "pg_logical/replorigin_checkpoint.tmp";

// Magic for on disk files (origin.c REPLICATION_STATE_MAGIC).
const REPLICATION_STATE_MAGIC: u32 = 0x1257DADE;

pub const MAX_RONAME_LEN: usize = 512;

// pg_replication_origin columns.
const Anum_pg_replication_origin_roident: i32 = 1;
const Anum_pg_replication_origin_roname: i32 = 2;
const Natts_pg_replication_origin: usize = 2;

// XLOG_REPLORIGIN_* (replication/origin.h).
pub const XLOG_REPLORIGIN_SET: u8 = 0x00;
pub const XLOG_REPLORIGIN_DROP: u8 = 0x10;

// WAIT_EVENT_REPLICATION_ORIGIN_DROP (wait_event_names.txt, IPC section; one
// before REPLICATION_SLOT_DROP=49 as in the slot crate).
const WAIT_EVENT_REPLICATION_ORIGIN_DROP: u32 = 0x0800_0000 + 48;

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

// ReplicationState (origin.c): replay progress of a single remote node.
pub struct ReplicationState {
    pub roident: SyncCell<RepOriginId>,
    // Location of the latest commit from the remote side.
    pub remote_lsn: SyncCell<XLogRecPtr>,
    // Local lsn of that commit, XLogFlush()ed at checkpoint.
    pub local_lsn: SyncCell<XLogRecPtr>,
    // PID of backend that's acquired the slot, or 0.
    pub acquired_by: SyncCell<i32>,
    // Signaled when acquired_by changes.
    pub origin_cv: ConditionVariable,
    // Protects remote_lsn and local_lsn.
    pub lock: lwlock::LWLock,
}

fn initial_state() -> ReplicationState {
    ReplicationState {
        roident: SyncCell::new(InvalidRepOriginId),
        remote_lsn: SyncCell::new(InvalidXLogRecPtr),
        local_lsn: SyncCell::new(InvalidXLogRecPtr),
        acquired_by: SyncCell::new(0),
        origin_cv: ConditionVariable::new(),
        lock: LWLockPadded::new_unlocked(LWTRANCHE_REPLICATION_ORIGIN_STATE).lock,
    }
}

struct StatesPtr {
    ptr: *mut ReplicationState,
    len: usize,
}
// SAFETY: leaked boot allocation; cross-thread access follows origin.c's
// locking model; in-place rewrites happen only with children dead.
unsafe impl Send for StatesPtr {}
unsafe impl Sync for StatesPtr {}

static REPLICATION_STATES: OnceLock<StatesPtr> = OnceLock::new();

fn replication_states() -> &'static [ReplicationState] {
    match REPLICATION_STATES.get() {
        // SAFETY: leaked boot allocation, valid for the process lifetime.
        Some(a) => unsafe { std::slice::from_raw_parts(a.ptr, a.len) },
        None if max_active_replication_origins() == 0 => &[],
        None => panic!("ReplicationOriginShmemInit has not run"),
    }
}

static MAX_ACTIVE_REPLICATION_ORIGINS: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(10);

pub fn max_active_replication_origins() -> i32 {
    MAX_ACTIVE_REPLICATION_ORIGINS.load(std::sync::atomic::Ordering::Relaxed)
}

fn origin_lock() -> &'static lwlock::LWLock {
    lwlock::main_lock(REPLICATION_ORIGIN_LOCK)
}

fn lw(lock: &'static lwlock::LWLock, mode: lwlock::LWLockMode) -> PgResult<()> {
    LWLockAcquire(lock, mode, init_small::globals::MyProcNumber())?;
    Ok(())
}

thread_local! {
    // C file-static session_replication_state.
    static SESSION_REPLICATION_STATE: Cell<Option<&'static ReplicationState>> =
        const { Cell::new(None) };
    // C globals replorigin_session_origin{,_lsn,_timestamp}.
    static SESSION_ORIGIN: Cell<RepOriginId> = const { Cell::new(InvalidRepOriginId) };
    static SESSION_ORIGIN_LSN: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    static SESSION_ORIGIN_TIMESTAMP: Cell<TimestampTz> = const { Cell::new(0) };
    static REGISTERED_CLEANUP: Cell<bool> = const { Cell::new(false) };
}

pub fn replorigin_session_origin() -> RepOriginId {
    SESSION_ORIGIN.get()
}
pub fn set_replorigin_session_origin(v: RepOriginId) {
    SESSION_ORIGIN.set(v);
}
pub fn replorigin_session_origin_lsn() -> XLogRecPtr {
    SESSION_ORIGIN_LSN.get()
}
pub fn set_replorigin_session_origin_lsn(v: XLogRecPtr) {
    SESSION_ORIGIN_LSN.set(v);
}
pub fn replorigin_session_origin_timestamp() -> TimestampTz {
    SESSION_ORIGIN_TIMESTAMP.get()
}
pub fn set_replorigin_session_origin_timestamp(v: TimestampTz) {
    SESSION_ORIGIN_TIMESTAMP.set(v);
}

// ---------------------------------------------------------------------------
// Prerequisites and reserved names
// ---------------------------------------------------------------------------

pub(crate) fn replorigin_check_prerequisites(
    check_origins: bool,
    recovery_ok: bool,
) -> PgResult<()> {
    if check_origins && max_active_replication_origins() == 0 {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(
                "cannot query or manipulate replication origin when \
                 \"max_active_replication_origins\" is 0",
            )
            .finish(loc("replorigin_check_prerequisites"))?;
        unreachable!();
    }
    if !recovery_ok && transam_xlog_seams::recovery_in_progress::call() {
        ereport(ERROR)
            .errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION)
            .errmsg("cannot manipulate replication origins during recovery")
            .finish(loc("replorigin_check_prerequisites"))?;
        unreachable!();
    }
    Ok(())
}

// IsReservedOriginName: "none" or "any" (pg_strcasecmp).
pub(crate) fn is_reserved_origin_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("none") || name.eq_ignore_ascii_case("any")
}

// ---------------------------------------------------------------------------
// Catalog: create / lookup / drop
// ---------------------------------------------------------------------------

fn text_datum_to_string(mcx: Mcx<'_>, d: Datum) -> PgResult<String> {
    let ptr = d.as_usize() as *const u8;
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(ptr, types_tuple::varatt::varsize_any(ptr)) };
    let img = detoast::detoast_attr(mcx, raw)?;
    let bytes = varlena::text_to_cstring(mcx, &img)?;
    Ok(String::from_utf8_lossy(&bytes[..bytes.len().saturating_sub(1)]).into_owned())
}

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    let img = varlena::cstring_to_text(mcx, s.as_bytes())?.into_image().leak();
    Ok(Datum::from_usize(img.as_ptr() as usize))
}

// replorigin_by_name (origin.c). Needs to be called in a transaction.
pub fn replorigin_by_name(roname: &str, missing_ok: bool) -> PgResult<RepOriginId> {
    use cache_syscache::cacheinfo::REPLORIGNAME;
    let mut roident = InvalidOid;
    if let Some(tup) = cache_syscache::SearchSysCache1(
        REPLORIGNAME,
        cache_syscache::SysCacheKey::Str(roname),
    )? {
        roident = cache_syscache::SysCacheGetAttr(
            REPLORIGNAME,
            &tup,
            Anum_pg_replication_origin_roident,
        )?
        .0
        .as_oid();
        cache_syscache::ReleaseSysCache(tup);
    } else if !missing_ok {
        ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("replication origin \"{roname}\" does not exist"))
            .finish(loc("replorigin_by_name"))?;
        unreachable!();
    }
    Ok(roident as RepOriginId)
}

// replorigin_create (origin.c). Needs to be called in a transaction.
pub fn replorigin_create(mcx: Mcx<'_>, roname: &str) -> PgResult<RepOriginId> {
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    use types_snapshot::{SnapshotData, SNAPSHOT_DIRTY};

    // Names are limited to 512 bytes so pg_replication_origin needs no TOAST
    // table.
    if roname.len() > MAX_RONAME_LEN {
        ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("replication origin name is too long")
            .errdetail(format!(
                "Replication origin names must be no longer than {MAX_RONAME_LEN} bytes."
            ))
            .finish(loc("replorigin_create"))?;
        unreachable!();
    }

    let roname_d = text_datum(mcx, roname)?;

    // The numeric origin id is 16 bits: scan for the first unused id under an
    // exclusive lock, reading with a dirty snapshot so in-progress rows count.
    let rel = table::table_open(mcx, catalog::ReplicationOriginRelationId, types_rel::ExclusiveLock)?;

    let mut created = InvalidRepOriginId;
    for roident in 1..(u16::MAX as u32) {
        postgres_seams::check_for_interrupts::call()?;

        let mut key = ScanKeyData::empty();
        key.sk_attno = Anum_pg_replication_origin_roident as AttrNumber;
        key.sk_strategy = BTEqualStrategyNumber;
        key.sk_collation = types_core::C_COLLATION_OID;
        key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
            .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
        key.sk_argument = Datum::from_oid(roident);

        let dirty = Rc::new(SnapshotData::sentinel(mcx, SNAPSHOT_DIRTY));
        let mut scan = genam::systable_beginscan(
            mcx,
            &rel,
            catalog::ReplicationOriginIdentIndex,
            true,
            Some(dirty),
            &[key],
        )?;
        let collides = genam::systable_getnext(mcx, &mut scan)?.is_some();
        genam::systable_endscan(mcx, scan)?;

        if !collides {
            let mut values = [Datum::null(); Natts_pg_replication_origin];
            let nulls = [false; Natts_pg_replication_origin];
            values[(Anum_pg_replication_origin_roident - 1) as usize] = Datum::from_oid(roident);
            values[(Anum_pg_replication_origin_roname - 1) as usize] = roname_d;
            let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
            catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;
            xact::CommandCounterIncrement()?;
            created = roident as RepOriginId;
            break;
        }
    }

    rel.close(types_rel::ExclusiveLock)?;

    if created == InvalidRepOriginId {
        ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("could not find free replication origin ID")
            .finish(loc("replorigin_create"))?;
        unreachable!();
    }
    Ok(created)
}

// replorigin_state_clear (origin.c): clear the in-memory slot for a dropped
// origin, waiting out (or erroring on) concurrent acquirers.
fn replorigin_state_clear(roident: RepOriginId, nowait: bool) -> PgResult<()> {
    'restart: loop {
        lw(origin_lock(), LW_EXCLUSIVE)?;

        for state in replication_states() {
            if state.roident.get() == roident {
                if state.acquired_by.get() != 0 {
                    let pid = state.acquired_by.get();
                    if nowait {
                        let r = ereport(ERROR)
                            .errcode(ERRCODE_OBJECT_IN_USE)
                            .errmsg(format!(
                                "could not drop replication origin with ID {roident}, \
                                 in use by PID {pid}"
                            ))
                            .finish(loc("replorigin_state_clear"));
                        LWLockRelease(origin_lock())?;
                        return r;
                    }
                    // Wait and retry (plain sleep, as in C; we could miss a
                    // prepare-to-sleep signal otherwise).
                    let cv: &'static ConditionVariable =
                        // SAFETY: states live for the process lifetime.
                        unsafe { &*(&state.origin_cv as *const ConditionVariable) };
                    LWLockRelease(origin_lock())?;
                    ConditionVariableSleep(cv, WAIT_EVENT_REPLICATION_ORIGIN_DROP)?;
                    continue 'restart;
                }

                // First make a WAL log entry, then clear the in-memory slot.
                let xlrec = serialize_replorigin_drop(roident);
                xloginsert::insert_record(
                    RM_REPLORIGIN_ID as u8,
                    XLOG_REPLORIGIN_DROP,
                    0,
                    &[&xlrec],
                    &[],
                )?;

                state.roident.set(InvalidRepOriginId);
                state.remote_lsn.set(InvalidXLogRecPtr);
                state.local_lsn.set(InvalidXLogRecPtr);
                break;
            }
        }
        LWLockRelease(origin_lock())?;
        ConditionVariableCancelSleep();
        return Ok(());
    }
}

// replorigin_drop_by_name (origin.c). Needs to be called in a transaction.
pub fn replorigin_drop_by_name(mcx: Mcx<'_>, name: &str, missing_ok: bool, nowait: bool) -> PgResult<()> {
    use cache_syscache::cacheinfo::REPLORIGIDENT;

    let rel = table::table_open(mcx, catalog::ReplicationOriginRelationId, types_rel::RowExclusiveLock)?;

    let roident = replorigin_by_name(name, missing_ok)?;

    // Lock the origin to prevent concurrent drops.
    lmgr::LockSharedObject(
        catalog::ReplicationOriginRelationId,
        roident as Oid,
        0,
        types_rel::AccessExclusiveLock,
    )?;

    let Some(tup) = cache_syscache::SearchSysCacheCopy(
        mcx,
        REPLORIGIDENT,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(roident as Oid)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?
    else {
        if !missing_ok {
            return elog(
                ERROR,
                format!("cache lookup failed for replication origin with ID {roident}"),
            );
        }
        lmgr::UnlockSharedObject(
            catalog::ReplicationOriginRelationId,
            roident as Oid,
            0,
            types_rel::AccessExclusiveLock,
        )?;
        return rel.close(types_rel::RowExclusiveLock);
    };

    replorigin_state_clear(roident, nowait)?;

    let tid = tup.as_tuple().t_self;
    catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    xact::CommandCounterIncrement()?;

    // We keep the lock on pg_replication_origin until commit.
    rel.close(types_rel::NoLock)
}

// replorigin_by_oid (origin.c): Ok(Some(name)) when known.
pub fn replorigin_by_oid(
    mcx: Mcx<'_>,
    roident: RepOriginId,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    use cache_syscache::cacheinfo::REPLORIGIDENT;
    debug_assert!(roident != InvalidRepOriginId);
    debug_assert!(roident != DoNotReplicateId);

    if let Some(tup) = cache_syscache::SearchSysCache1(
        REPLORIGIDENT,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(roident as Oid)),
    )? {
        let (d, _null) = cache_syscache::SysCacheGetAttr(
            REPLORIGIDENT,
            &tup,
            Anum_pg_replication_origin_roname,
        )?;
        let name = text_datum_to_string(mcx, d)?;
        cache_syscache::ReleaseSysCache(tup);
        Ok(Some(name))
    } else {
        if !missing_ok {
            ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("replication origin with ID {roident} does not exist"))
                .finish(loc("replorigin_by_oid"))?;
        unreachable!();
        }
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Shmem-array lifecycle
// ---------------------------------------------------------------------------

pub fn ReplicationOriginShmemInit() {
    let n = max_active_replication_origins();
    if n == 0 {
        return;
    }
    let states: Box<[ReplicationState]> = (0..n).map(|_| initial_state()).collect();
    let len = states.len();
    let ptr = Box::into_raw(states) as *mut ReplicationState;
    if REPLICATION_STATES.set(StatesPtr { ptr, len }).is_err() {
        panic!("ReplicationOriginShmemInit already ran");
    }
}

pub fn ReplicationOriginShmemResetAfterCrash() {
    let Some(a) = REPLICATION_STATES.get() else {
        return;
    };
    for i in 0..a.len {
        // SAFETY: crash-cycle reset on the postmaster thread with every
        // backend thread dead; no concurrent access exists.
        unsafe { std::ptr::write(a.ptr.add(i), initial_state()) };
    }
}

// ---------------------------------------------------------------------------
// Checkpoint / startup of the replorigin_checkpoint file
// ---------------------------------------------------------------------------

const DISK_STATE_SIZE: usize = 16; // ReplicationStateOnDisk: u16 + pad + u64 (MAXALIGNed)

fn serialize_disk_state(roident: RepOriginId, remote_lsn: XLogRecPtr) -> [u8; DISK_STATE_SIZE] {
    let mut b = [0u8; DISK_STATE_SIZE];
    b[0..2].copy_from_slice(&roident.to_ne_bytes());
    b[8..16].copy_from_slice(&remote_lsn.to_ne_bytes());
    b
}

// CheckPointReplicationOrigin (origin.c): MAGIC, ReplicationStateOnDisk...,
// CRC32C. Failures are PANIC as in C.
pub fn CheckPointReplicationOrigin() -> PgResult<()> {
    let tmppath = PG_REPLORIGIN_CHECKPOINT_TMPFILE;
    let path = PG_REPLORIGIN_CHECKPOINT_FILENAME;

    if max_active_replication_origins() == 0 {
        return Ok(());
    }

    // Make sure no old temp file is remaining. fd::pg_unlink = the fd-crate
    // front onto the Vfs choke (the s2.1 routing the t27-train allowlist row
    // chartered to t28): a raw libc::unlink acts on the REAL fs while the
    // file lives in SimVfs under pgrust_sim.
    if fd::pg_unlink(tmppath) < 0 && errno::current_errno() != libc::ENOENT {
        ereport(PANIC)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not remove file \"{tmppath}\": %m"))
            .finish(loc("CheckPointReplicationOrigin"))?;
        unreachable!();
    }

    // Only one checkpoint can happen at a time.
    let tmpfd = fd::OpenTransientFile(tmppath, libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY)?;
    if tmpfd < 0 {
        ereport(PANIC)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{tmppath}\": %m"))
            .finish(loc("CheckPointReplicationOrigin"))?;
        unreachable!();
    }

    // t28-compose finding (the relmapper/loadsort/alter_system dataplane
    // class): OpenTransientFile mints a Vfs fd — a raw libc::write EBADFs
    // under pgrust_sim. fd::pg_pwrite at the tracked offset == write(2) on
    // the just-created O_EXCL|O_WRONLY fd (relmapper precedent).
    let write_off = Cell::new(0i64);
    let write_all = |fd_: i32, bytes: &[u8]| -> PgResult<()> {
        if fd::pg_pwrite(fd_, bytes, write_off.get()) != bytes.len() as isize {
            let mut save_errno = errno::current_errno();
            if save_errno == 0 {
                save_errno = libc::ENOSPC;
            }
            ereport(PANIC)
                .with_saved_errno(save_errno)
                .errcode_for_file_access()
                .errmsg(format!("could not write to file \"{tmppath}\": %m"))
                .finish(loc("CheckPointReplicationOrigin"))?;
        unreachable!();
        }
        write_off.set(write_off.get() + bytes.len() as i64);
        Ok(())
    };

    let magic = REPLICATION_STATE_MAGIC.to_ne_bytes();
    write_all(tmpfd, &magic)?;
    let mut crc = crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &magic);

    // Prevent concurrent creations/drops.
    lw(origin_lock(), LW_SHARED)?;

    for state in replication_states() {
        if state.roident.get() == InvalidRepOriginId {
            continue;
        }

        lw(&state.lock, LW_SHARED)?;
        let disk = serialize_disk_state(state.roident.get(), state.remote_lsn.get());
        let local_lsn = state.local_lsn.get();
        LWLockRelease(&state.lock)?;

        // Make sure we only write out a commit that's persistent.
        transam_xlog::write::XLogFlush(local_lsn)?;

        write_all(tmpfd, &disk)?;
        crc = crc32c::pg_comp_crc32c(crc, &disk);
    }

    LWLockRelease(origin_lock())?;

    let crc = crc32c::fin_crc32c(crc);
    write_all(tmpfd, &crc.to_ne_bytes())?;

    if fd::CloseTransientFile(tmpfd) != 0 {
        ereport(PANIC)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{tmppath}\": %m"))
            .finish(loc("CheckPointReplicationOrigin"))?;
        unreachable!();
    }

    // durable_rename(tmppath, path, PANIC) — C's actual call (origin.c),
    // routed through the fd crate's vfs-backed fd.c:782 port (fsync temp,
    // rename, fsync file and containing directory). The hand-rolled
    // fsync+libc::rename+fsync it replaces drove the REAL fs while the file
    // lives in SimVfs under pgrust_sim (t28-compose finding).
    fd::durable_rename(tmppath, path, PANIC)?;
    Ok(())
}

// StartupReplicationOrigin (origin.c): recover progress from the checkpoint
// file written by CheckPointReplicationOrigin. Startup-only.
pub fn StartupReplicationOrigin() -> PgResult<()> {
    let path = PG_REPLORIGIN_CHECKPOINT_FILENAME;

    if max_active_replication_origins() == 0 {
        return Ok(());
    }

    let _ = elog(DEBUG2, "starting up replication origin progress state".to_string());

    let fd_ = match fd::OpenTransientFile(path, libc::O_RDONLY) {
        Ok(f) if f >= 0 => f,
        // Might have had max_active_replication_origins == 0 last run, or we
        // just brought up a standby.
        _ if errno::current_errno() == libc::ENOENT => return Ok(()),
        _ => {
            ereport(PANIC)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{path}\": %m"))
                .finish(loc("StartupReplicationOrigin"))?;
        unreachable!();
        }
    };

    // t28-compose finding (the relmapper/loadsort/alter_system dataplane
    // class): OpenTransientFile mints a Vfs fd — a raw libc::read EBADFs
    // under pgrust_sim. fd::pg_pread at the tracked offset == read(2) on the
    // just-opened O_RDONLY fd (relmapper precedent).
    let read_off = Cell::new(0i64);
    let read_exact = |fd_: i32, buf: &mut [u8]| -> PgResult<isize> {
        let n = fd::pg_pread(fd_, buf, read_off.get());
        if n < 0 {
            ereport(PANIC)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{path}\": %m"))
                .finish(loc("StartupReplicationOrigin"))?;
        unreachable!();
        }
        read_off.set(read_off.get() + n as i64);
        Ok(n)
    };

    // Verify magic, written even if nothing was active.
    let mut magic_buf = [0u8; 4];
    let n = read_exact(fd_, &mut magic_buf)?;
    if n != 4 {
        ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!("could not read file \"{path}\": read {n} of 4"))
            .finish(loc("StartupReplicationOrigin"))?;
        unreachable!();
    }
    let mut crc = crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &magic_buf);
    let magic = u32::from_ne_bytes(magic_buf);
    if magic != REPLICATION_STATE_MAGIC {
        ereport(PANIC)
            .errmsg(format!(
                "replication checkpoint has wrong magic {magic} instead of {REPLICATION_STATE_MAGIC}"
            ))
            .finish(loc("StartupReplicationOrigin"))?;
        unreachable!();
    }

    // We can skip locking here, no other access is possible.
    let states = replication_states();
    let mut last_state = 0usize;
    let file_crc: u32;
    loop {
        let mut disk = [0u8; DISK_STATE_SIZE];
        let n = read_exact(fd_, &mut disk)?;

        // No further data: the trailing 4 bytes are the CRC.
        if n == 4 {
            file_crc = u32::from_ne_bytes(disk[0..4].try_into().expect("4 bytes"));
            break;
        }
        if n != DISK_STATE_SIZE as isize {
            ereport(PANIC)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not read file \"{path}\": read {n} of {DISK_STATE_SIZE}"
                ))
                .finish(loc("StartupReplicationOrigin"))?;
        unreachable!();
        }

        crc = crc32c::pg_comp_crc32c(crc, &disk);

        if last_state == states.len() {
            ereport(PANIC)
                .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
                .errmsg(
                    "could not find free replication state, increase \
                     \"max_active_replication_origins\"",
                )
                .finish(loc("StartupReplicationOrigin"))?;
        unreachable!();
        }

        // Copy data to the shared array.
        let roident = u16::from_ne_bytes(disk[0..2].try_into().expect("2 bytes"));
        let remote_lsn = u64::from_ne_bytes(disk[8..16].try_into().expect("8 bytes"));
        states[last_state].roident.set(roident);
        states[last_state].remote_lsn.set(remote_lsn);
        last_state += 1;

        let _ = elog(
            LOG,
            format!(
                "recovered replication state of node {roident} to {:X}/{:X}",
                (remote_lsn >> 32) as u32,
                remote_lsn as u32
            ),
        );
    }

    let crc = crc32c::fin_crc32c(crc);
    if file_crc != crc {
        ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "replication slot checkpoint has wrong checksum {crc}, expected {file_crc}"
            ))
            .finish(loc("StartupReplicationOrigin"))?;
        unreachable!();
    }

    if fd::CloseTransientFile(fd_) != 0 {
        ereport(PANIC)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{path}\": %m"))
            .finish(loc("StartupReplicationOrigin"))?;
        unreachable!();
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// WAL: xl_replorigin_set / xl_replorigin_drop + redo
// ---------------------------------------------------------------------------

// xl_replorigin_set: remote_lsn u64 @0, node_id u16 @8, force bool @10 (16B).
fn serialize_replorigin_set(node: RepOriginId, remote_lsn: XLogRecPtr, force: bool) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0..8].copy_from_slice(&remote_lsn.to_ne_bytes());
    b[8..10].copy_from_slice(&node.to_ne_bytes());
    b[10] = force as u8;
    b
}

// xl_replorigin_drop: node_id u16 (2B).
fn serialize_replorigin_drop(node: RepOriginId) -> [u8; 2] {
    node.to_ne_bytes()
}

// replorigin_redo (origin.c).
pub fn replorigin_redo(record: &mut xlogreader_seams::XLogReaderState) -> PgResult<()> {
    const XLR_INFO_MASK: u8 = 0x0F;
    let end_rec_ptr = record.EndRecPtr;
    let decoded = record.record.as_ref().expect("replorigin_redo with no decoded record");
    let info = decoded.xl_info & !XLR_INFO_MASK;
    // SAFETY: the decoded record's main data lives for the redo call.
    let data = unsafe { decoded.main_data_bytes() };

    match info {
        XLOG_REPLORIGIN_SET => {
            let remote_lsn = u64::from_ne_bytes(data[0..8].try_into().expect("short record"));
            let node_id = u16::from_ne_bytes(data[8..10].try_into().expect("short record"));
            let force = data[10] != 0;
            replorigin_advance(node_id, remote_lsn, end_rec_ptr, force, false)?;
        }
        XLOG_REPLORIGIN_DROP => {
            let node_id = u16::from_ne_bytes(data[0..2].try_into().expect("short record"));
            for state in replication_states() {
                if state.roident.get() == node_id {
                    state.roident.set(InvalidRepOriginId);
                    state.remote_lsn.set(InvalidXLogRecPtr);
                    state.local_lsn.set(InvalidXLogRecPtr);
                    break;
                }
            }
        }
        other => {
            let _ = elog(PANIC, format!("replorigin_redo: unknown op code {other}"));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Progress tracking
// ---------------------------------------------------------------------------

// replorigin_advance (origin.c). Needs a RowExclusiveLock on
// pg_replication_origin unless running in recovery.
pub fn replorigin_advance(
    node: RepOriginId,
    remote_commit: XLogRecPtr,
    local_commit: XLogRecPtr,
    go_backward: bool,
    wal_log: bool,
) -> PgResult<()> {
    debug_assert!(node != InvalidRepOriginId);

    // We don't track DoNotReplicateId.
    if node == DoNotReplicateId {
        return Ok(());
    }

    // Lock exclusively, as we may have to create a new table entry.
    lw(origin_lock(), LW_EXCLUSIVE)?;

    let mut replication_state: Option<&'static ReplicationState> = None;
    let mut free_state: Option<&'static ReplicationState> = None;

    for curstate in replication_states() {
        if curstate.roident.get() == InvalidRepOriginId && free_state.is_none() {
            free_state = Some(curstate);
            continue;
        }
        if curstate.roident.get() != node {
            continue;
        }

        lw(&curstate.lock, LW_EXCLUSIVE)?;

        // Make sure it's not used by somebody else.
        if curstate.acquired_by.get() != 0 {
            let r = ereport(ERROR)
                .errcode(ERRCODE_OBJECT_IN_USE)
                .errmsg(format!(
                    "replication origin with ID {} is already active for PID {}",
                    curstate.roident.get(),
                    curstate.acquired_by.get()
                ))
                .finish(loc("replorigin_advance"));
            LWLockRelease(&curstate.lock)?;
            LWLockRelease(origin_lock())?;
            return r;
        }

        replication_state = Some(curstate);
        break;
    }

    if replication_state.is_none() && free_state.is_none() {
        let r = ereport(ERROR)
            .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
            .errmsg(format!(
                "could not find free replication state slot for replication origin with ID {node}"
            ))
            .errhint("Increase \"max_active_replication_origins\" and try again.")
            .finish(loc("replorigin_advance"));
        LWLockRelease(origin_lock())?;
        return r;
    }

    let state = match replication_state {
        Some(s) => s,
        None => {
            // Initialize new slot.
            let s = free_state.expect("free slot checked above");
            lw(&s.lock, LW_EXCLUSIVE)?;
            debug_assert!(s.remote_lsn.get() == InvalidXLogRecPtr);
            debug_assert!(s.local_lsn.get() == InvalidXLogRecPtr);
            s.roident.set(node);
            s
        }
    };

    debug_assert!(state.roident.get() != InvalidRepOriginId);

    // If somebody "forcefully" sets this slot, WAL-log it so it's durable and
    // the standby gets the message. During WAL replay no logging is needed.
    if wal_log {
        let xlrec = serialize_replorigin_set(node, remote_commit, go_backward);
        xloginsert::insert_record(RM_REPLORIGIN_ID as u8, XLOG_REPLORIGIN_SET, 0, &[&xlrec], &[])?;
    }

    // Checkpoint races can present older values; don't go backward unless
    // asked to.
    if go_backward || state.remote_lsn.get() < remote_commit {
        state.remote_lsn.set(remote_commit);
    }
    if local_commit != InvalidXLogRecPtr && (go_backward || state.local_lsn.get() < local_commit) {
        state.local_lsn.set(local_commit);
    }
    LWLockRelease(&state.lock)?;

    // Release *after* changing the LSNs; the slot isn't acquired and could
    // otherwise be dropped anytime.
    LWLockRelease(origin_lock())?;
    Ok(())
}

// replorigin_get_progress (origin.c).
pub fn replorigin_get_progress(node: RepOriginId, flush: bool) -> PgResult<XLogRecPtr> {
    let mut local_lsn = InvalidXLogRecPtr;
    let mut remote_lsn = InvalidXLogRecPtr;

    // Prevent slots from being concurrently dropped.
    lw(origin_lock(), LW_SHARED)?;
    for state in replication_states() {
        if state.roident.get() == node {
            lw(&state.lock, LW_SHARED)?;
            remote_lsn = state.remote_lsn.get();
            local_lsn = state.local_lsn.get();
            LWLockRelease(&state.lock)?;
            break;
        }
    }
    LWLockRelease(origin_lock())?;

    if flush && local_lsn != InvalidXLogRecPtr {
        transam_xlog::write::XLogFlush(local_lsn)?;
    }
    Ok(remote_lsn)
}

// ReplicationOriginExitCleanup (origin.c): tear down a configured session
// origin during process exit.
fn ReplicationOriginExitCleanup(_code: i32, _arg: usize) {
    let Some(state) = SESSION_REPLICATION_STATE.get() else {
        return;
    };

    if lw(origin_lock(), LW_EXCLUSIVE).is_err() {
        return;
    }

    let mut cv: Option<&'static ConditionVariable> = None;
    if state.acquired_by.get() == init_small::globals::MyProcPid() {
        cv = Some(
            // SAFETY: states live for the process lifetime.
            unsafe { &*(&state.origin_cv as *const ConditionVariable) },
        );
        state.acquired_by.set(0);
        SESSION_REPLICATION_STATE.set(None);
    }

    let _ = LWLockRelease(origin_lock());

    if let Some(cv) = cv {
        ConditionVariableBroadcast(cv);
    }
}

// replorigin_session_setup (origin.c): acquire (or share, for parallel apply
// when acquired_by != 0) the origin's shared state for this session.
pub fn replorigin_session_setup(node: RepOriginId, acquired_by: i32) -> PgResult<()> {
    if !REGISTERED_CLEANUP.get() {
        ipc::on_shmem_exit(ReplicationOriginExitCleanup, 0);
        REGISTERED_CLEANUP.set(true);
    }

    debug_assert!(max_active_replication_origins() > 0);

    if SESSION_REPLICATION_STATE.get().is_some() {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot setup replication origin when one is already setup")
            .finish(loc("replorigin_session_setup"))?;
        unreachable!();
    }

    // Lock exclusively, as we may have to create a new table entry.
    lw(origin_lock(), LW_EXCLUSIVE)?;

    let mut session_state: Option<&'static ReplicationState> = None;
    let mut free_slot: Option<&'static ReplicationState> = None;

    for curstate in replication_states() {
        if curstate.roident.get() == InvalidRepOriginId && free_slot.is_none() {
            free_slot = Some(curstate);
            continue;
        }
        if curstate.roident.get() != node {
            continue;
        }
        if curstate.acquired_by.get() != 0 && acquired_by == 0 {
            let r = ereport(ERROR)
                .errcode(ERRCODE_OBJECT_IN_USE)
                .errmsg(format!(
                    "replication origin with ID {} is already active for PID {}",
                    curstate.roident.get(),
                    curstate.acquired_by.get()
                ))
                .finish(loc("replorigin_session_setup"));
            LWLockRelease(origin_lock())?;
            return r;
        }
        session_state = Some(curstate);
        break;
    }

    let state = match (session_state, free_slot) {
        (Some(s), _) => s,
        (None, Some(free)) => {
            // Initialize new slot.
            debug_assert!(free.remote_lsn.get() == InvalidXLogRecPtr);
            debug_assert!(free.local_lsn.get() == InvalidXLogRecPtr);
            free.roident.set(node);
            free
        }
        (None, None) => {
            let r = ereport(ERROR)
                .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
                .errmsg(format!(
                    "could not find free replication state slot for replication origin with ID {node}"
                ))
                .errhint("Increase \"max_active_replication_origins\" and try again.")
                .finish(loc("replorigin_session_setup"));
            LWLockRelease(origin_lock())?;
            return r;
        }
    };

    debug_assert!(state.roident.get() != InvalidRepOriginId);

    if acquired_by == 0 {
        state.acquired_by.set(init_small::globals::MyProcPid());
    } else if state.acquired_by.get() != acquired_by {
        let r = elog(
            ERROR,
            format!(
                "could not find replication state slot for replication origin with OID {node} \
                 which was acquired by {acquired_by}"
            ),
        );
        LWLockRelease(origin_lock())?;
        return r;
    }
    SESSION_REPLICATION_STATE.set(Some(state));

    LWLockRelease(origin_lock())?;

    // SAFETY: states live for the process lifetime.
    let cv = unsafe { &*(&state.origin_cv as *const ConditionVariable) };
    ConditionVariableBroadcast(cv);
    Ok(())
}

// replorigin_session_reset (origin.c).
pub fn replorigin_session_reset() -> PgResult<()> {
    debug_assert!(max_active_replication_origins() != 0);

    let Some(state) = SESSION_REPLICATION_STATE.get() else {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("no replication origin is configured")
            .finish(loc("replorigin_session_reset"))?;
        unreachable!();
    };

    lw(origin_lock(), LW_EXCLUSIVE)?;
    state.acquired_by.set(0);
    SESSION_REPLICATION_STATE.set(None);
    LWLockRelease(origin_lock())?;

    // SAFETY: states live for the process lifetime.
    let cv = unsafe { &*(&state.origin_cv as *const ConditionVariable) };
    ConditionVariableBroadcast(cv);
    Ok(())
}

pub fn replorigin_session_is_setup() -> bool {
    SESSION_REPLICATION_STATE.get().is_some()
}

// replorigin_session_advance (origin.c): replorigin_advance on the session's
// configured origin, cheaper.
pub fn replorigin_session_advance(
    remote_commit: XLogRecPtr,
    local_commit: XLogRecPtr,
) -> PgResult<()> {
    let state = SESSION_REPLICATION_STATE
        .get()
        .expect("replorigin_session_advance without a session origin");
    debug_assert!(state.roident.get() != InvalidRepOriginId);

    lw(&state.lock, LW_EXCLUSIVE)?;
    if state.local_lsn.get() < local_commit {
        state.local_lsn.set(local_commit);
    }
    if state.remote_lsn.get() < remote_commit {
        state.remote_lsn.set(remote_commit);
    }
    LWLockRelease(&state.lock)?;
    Ok(())
}

// replorigin_session_get_progress (origin.c).
pub fn replorigin_session_get_progress(flush: bool) -> PgResult<XLogRecPtr> {
    let state = SESSION_REPLICATION_STATE
        .get()
        .expect("replorigin_session_get_progress without a session origin");

    lw(&state.lock, LW_SHARED)?;
    let remote_lsn = state.remote_lsn.get();
    let local_lsn = state.local_lsn.get();
    LWLockRelease(&state.lock)?;

    if flush && local_lsn != InvalidXLogRecPtr {
        transam_xlog::write::XLogFlush(local_lsn)?;
    }
    Ok(remote_lsn)
}

// Read-only view for pg_show_replication_origin_status.
pub(crate) fn show_status_rows() -> PgResult<Vec<(RepOriginId, XLogRecPtr, XLogRecPtr)>> {
    let mut rows = Vec::new();
    lw(origin_lock(), LW_SHARED)?;
    for state in replication_states() {
        if state.roident.get() == InvalidRepOriginId {
            continue;
        }
        lw(&state.lock, LW_SHARED)?;
        rows.push((state.roident.get(), state.remote_lsn.get(), state.local_lsn.get()));
        LWLockRelease(&state.lock)?;
    }
    LWLockRelease(origin_lock())?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

pub fn init_seams() {
    guc_tables::vars::max_active_replication_origins.install_if_absent(guc_tables::GucVarAccessors {
        get: max_active_replication_origins,
        set: |v| MAX_ACTIVE_REPLICATION_ORIGINS.store(v, std::sync::atomic::Ordering::Relaxed),
    });
    origin_seams::replorigin_session_origin::set(replorigin_session_origin);
    origin_seams::replorigin_session_origin_lsn::set(replorigin_session_origin_lsn);
    origin_seams::replorigin_session_origin_timestamp::set(replorigin_session_origin_timestamp);
    origin_seams::set_replorigin_session_origin_timestamp::set(
        set_replorigin_session_origin_timestamp,
    );
    origin_seams::replorigin_session_advance::set(replorigin_session_advance);
    origin_seams::replorigin_advance::set(replorigin_advance);
    origin_seams::startup_replication_origin::set(StartupReplicationOrigin);
    origin_seams::check_point_replication_origin::set(CheckPointReplicationOrigin);
    origin_seams::replorigin_redo::set(replorigin_redo);
}

#[cfg(test)]
mod tests;
