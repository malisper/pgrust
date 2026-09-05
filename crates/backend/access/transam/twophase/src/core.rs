use std::sync::atomic::Ordering::Relaxed;

use elog::ereport;
use lwlock::{LW_EXCLUSIVE, LW_SHARED};
use types_core::{
    InvalidTransactionId, Oid, ProcNumber, TimestampTz, TransactionId, TransactionIdIsValid,
    XLogRecPtr, INVALID_PROC_NUMBER,
};
use types_error::{
    PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_OUT_OF_MEMORY, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};
use types_storage::storage::{
    XidCacheStatus, NUM_LOCK_PARTITIONS, PGPROC_MAX_CACHED_SUBXIDS,
};

use crate::codec::{
    maxalign, BufferLayout, TwoPhaseFileHeader, TwoPhaseRecordOnDisk, MAX_ALLOC_SIZE,
    SIZEOF_TWOPHASE_RECORD_ON_DISK,
};
use crate::here;
use crate::state::{
    lock_twophase_state, unlock_twophase_state, GXact, TwoPhaseState, CACHED_GXACT, GIDSIZE,
    MY_LOCKED_GXACT, NO_GXACT, TWOPHASE_EXIT_REGISTERED,
};

pub const XLOG_XACT_PREPARE: u8 = 0x10;
pub const XLOG_XACT_OPMASK: u8 = 0x70;
const RM_XACT_ID: u8 = 1;

fn max_prepared_xacts() -> i32 {
    twophase_config::max_prepared_xacts()
}

fn register_exit_hook() -> PgResult<()> {
    if !TWOPHASE_EXIT_REGISTERED.get() {
        ipc::before_shmem_exit(at_proc_exit_twophase, datum::Datum::null())?;
        TWOPHASE_EXIT_REGISTERED.set(true);
    }
    Ok(())
}

fn at_proc_exit_twophase(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    AtAbort_Twophase();
    Ok(())
}

/// `AtAbort_Twophase`: release the gxact entry we were working on, removing it
/// entirely when it never became valid.
pub fn AtAbort_Twophase() {
    let slot = MY_LOCKED_GXACT.get();
    if slot == NO_GXACT {
        return;
    }
    let st = TwoPhaseState();
    lock_twophase_state(LW_EXCLUSIVE);
    if !unsafe { st.gxact(slot).valid.get() } {
        remove_gxact(slot);
    } else {
        unsafe { st.gxact(slot).locking_backend.set(INVALID_PROC_NUMBER) };
    }
    unlock_twophase_state();
    MY_LOCKED_GXACT.set(NO_GXACT);
}

/// `PostPrepare_Twophase`: unlock after state transfer is complete.
pub fn PostPrepare_Twophase() {
    let slot = MY_LOCKED_GXACT.get();
    let st = TwoPhaseState();
    lock_twophase_state(LW_EXCLUSIVE);
    unsafe { st.gxact(slot).locking_backend.set(INVALID_PROC_NUMBER) };
    unlock_twophase_state();
    MY_LOCKED_GXACT.set(NO_GXACT);
}

/// `MarkAsPreparing`: reserve the GID; returns the gxacts slot index.
pub fn MarkAsPreparing(
    xid: TransactionId,
    gid: &str,
    prepared_at: TimestampTz,
    owner: Oid,
    databaseid: Oid,
) -> PgResult<i32> {
    if gid.len() >= GIDSIZE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("transaction identifier \"{gid}\" is too long"))
            .finish(here("MarkAsPreparing"))
            .unwrap_err());
    }

    if max_prepared_xacts() == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("prepared transactions are disabled")
            .errhint("Set \"max_prepared_transactions\" to a nonzero value.")
            .finish(here("MarkAsPreparing"))
            .unwrap_err());
    }

    register_exit_hook()?;

    let st = TwoPhaseState();
    lock_twophase_state(LW_EXCLUSIVE);
    let result = (|| -> PgResult<i32> {
        for i in 0..unsafe { st.num_prep_xacts.get() } {
            let g = st.gxact(st.prep_xact(i));
            if unsafe { g.gid.get() }.as_str() == gid {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!(
                        "transaction identifier \"{gid}\" is already in use"
                    ))
                    .finish(here("MarkAsPreparing"))
                    .unwrap_err());
            }
        }

        let Some(idx) = st.pop_free() else {
            return Err(max_prepared_error("MarkAsPreparing"));
        };

        mark_as_preparing_guts(idx, xid, gid, prepared_at, owner, databaseid);
        unsafe { st.gxact(idx).ondisk.set(false) };
        st.push_active(idx);
        Ok(idx)
    })();
    unlock_twophase_state();
    result
}

pub(crate) fn max_prepared_error(func: &'static str) -> Box<types_error::PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg("maximum number of prepared transactions reached")
        .errhint(format!(
            "Increase \"max_prepared_transactions\" (currently {}).",
            max_prepared_xacts()
        ))
        .finish(here(func))
        .unwrap_err()
}

/// `MarkAsPreparingGuts`: fill the gxact and (re-)initialize its dummy PGPROC.
/// Caller holds TwoPhaseStateLock exclusive.
pub(crate) fn mark_as_preparing_guts(
    idx: i32,
    xid: TransactionId,
    gid: &str,
    prepared_at: TimestampTz,
    owner: Oid,
    databaseid: Oid,
) {
    let st = TwoPhaseState();
    let g: &GXact = st.gxact(idx);
    let proc = lmgr_proc::GetPGProcByNumber(unsafe { g.pgprocno.get() });

    unsafe { proc.links.set(types_storage::storage::proclist_node::detached()) };
    proc.waitStatus
        .store(types_storage::storage::PROC_WAIT_STATUS_OK, Relaxed);
    let my_procno = init_small::globals::MyProcNumber();
    let my_lxid = if my_procno >= 0 {
        lmgr_proc::GetPGProcByNumber(my_procno).vxid.lxid.load(Relaxed)
    } else {
        0
    };
    if my_lxid != 0 {
        // Clone the VXID, for TwoPhaseGetXidByVirtualXID to find.
        proc.vxid.lxid.store(my_lxid, Relaxed);
        proc.vxid.procNumber.store(my_procno, Relaxed);
    } else {
        // Recovery path: GetLockConflicts uses this to specify an XID wait.
        proc.vxid.lxid.store(xid, Relaxed);
        proc.vxid.procNumber.store(INVALID_PROC_NUMBER, Relaxed);
    }
    proc.xid.value.store(xid, Relaxed);
    debug_assert_eq!(proc.xmin.read(), InvalidTransactionId);
    proc.delayChkptFlags.store(0, Relaxed);
    proc.statusFlags.store(0, Relaxed);
    proc.pid.store(0, Relaxed);
    proc.databaseId.store(databaseid, Relaxed);
    proc.roleId.store(owner, Relaxed);
    proc.tempNamespaceId.store(0, Relaxed);
    proc.isRegularBackend.store(false, Relaxed);
    proc.lwWaiting.store(lwlock::LW_WS_NOT_WAITING, Relaxed);
    proc.lwWaitMode.store(0, Relaxed);
    unsafe { proc.waitLock.set(core::ptr::null_mut()) };
    unsafe { proc.waitProcLock.set(core::ptr::null_mut()) };
    proc.waitStart.write(0);
    for i in 0..NUM_LOCK_PARTITIONS as usize {
        debug_assert!(unsafe { proc.myProcLocks[i].get() }.head.next.is_none());
        unsafe { proc.myProcLocks[i].set(types_storage::ilist::dlist_head::new()) };
    }
    // Subxid data is filled later by GXactLoadSubxactData.
    unsafe { proc.subxidStatus.set(XidCacheStatus {
        count: 0,
        overflowed: false,
    }) };

    unsafe { g.prepared_at.set(prepared_at) };
    unsafe { g.xid.set(xid) };
    unsafe { g.owner.set(owner) };
    unsafe { g.locking_backend.set(my_procno) };
    unsafe { g.valid.set(false) };
    unsafe { g.inredo.set(false) };
    let mut gidbuf = unsafe { g.gid.get() };
    gidbuf.set(gid);
    unsafe { g.gid.set(gidbuf) };

    MY_LOCKED_GXACT.set(idx);
}

/// `GXactLoadSubxactData`: stuff subxact XIDs into the dummy PGPROC.
pub(crate) fn gxact_load_subxact_data(idx: i32, children: &[TransactionId]) {
    let st = TwoPhaseState();
    let proc = lmgr_proc::GetPGProcByNumber(unsafe { st.gxact(idx).pgprocno.get() });
    let mut n = children.len();
    let mut status = unsafe { proc.subxidStatus.get() };
    if n > PGPROC_MAX_CACHED_SUBXIDS {
        status.overflowed = true;
        n = PGPROC_MAX_CACHED_SUBXIDS;
    }
    if n > 0 {
        let mut cache = unsafe { proc.subxids.get() };
        cache.xids[..n].copy_from_slice(&children[..n]);
        unsafe { proc.subxids.set(cache) };
        status.count = n as u8;
    }
    unsafe { proc.subxidStatus.set(status) };
}

/// `MarkAsPrepared`: flip valid and enter the dummy proc into the ProcArray.
pub(crate) fn mark_as_prepared(idx: i32, lock_held: bool) -> PgResult<()> {
    let st = TwoPhaseState();
    if !lock_held {
        lock_twophase_state(LW_EXCLUSIVE);
    }
    debug_assert!(!unsafe { st.gxact(idx).valid.get() });
    unsafe { st.gxact(idx).valid.set(true) };
    if !lock_held {
        unlock_twophase_state();
    }
    procarray::ProcArrayAdd(unsafe { st.gxact(idx).pgprocno.get() })
}

/// `LockGXact`: locate by GID and mark busy for COMMIT/ROLLBACK PREPARED.
pub(crate) fn lock_gxact(gid: &str, user: Oid) -> PgResult<i32> {
    register_exit_hook()?;

    let st = TwoPhaseState();
    lock_twophase_state(LW_EXCLUSIVE);
    let result = (|| -> PgResult<i32> {
        for i in 0..unsafe { st.num_prep_xacts.get() } {
            let idx = st.prep_xact(i);
            let g = st.gxact(idx);
            if !unsafe { g.valid.get() } {
                continue;
            }
            if unsafe { g.gid.get() }.as_str() != gid {
                continue;
            }

            if unsafe { g.locking_backend.get() } != INVALID_PROC_NUMBER {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "prepared transaction with identifier \"{gid}\" is busy"
                    ))
                    .finish(here("LockGXact"))
                    .unwrap_err());
            }

            if user != unsafe { g.owner.get() } && !superuser_seams::superuser_arg::call(user)? {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg("permission denied to finish prepared transaction")
                    .errhint("Must be superuser or the user that prepared the transaction.")
                    .finish(here("LockGXact"))
                    .unwrap_err());
            }

            let proc = lmgr_proc::GetPGProcByNumber(unsafe { g.pgprocno.get() });
            if init_small::globals::MyDatabaseId() != proc.databaseId.load(Relaxed) {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("prepared transaction belongs to another database")
                    .errhint("Connect to the database where the transaction was prepared to finish it.")
                    .finish(here("LockGXact"))
                    .unwrap_err());
            }

            unsafe { g.locking_backend.set(init_small::globals::MyProcNumber()) };
            MY_LOCKED_GXACT.set(idx);
            return Ok(idx);
        }
        Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "prepared transaction with identifier \"{gid}\" does not exist"
            ))
            .finish(here("LockGXact"))
            .unwrap_err())
    })();
    unlock_twophase_state();
    result
}

/// `RemoveGXact`: drop from the active array, return the slot to the freelist.
/// Caller holds TwoPhaseStateLock exclusive.
pub(crate) fn remove_gxact(idx: i32) {
    let st = TwoPhaseState();
    let n = unsafe { st.num_prep_xacts.get() };
    for i in 0..n {
        if st.prep_xact(i) == idx {
            unsafe { st.num_prep_xacts.set(n - 1) };
            unsafe { st.prep_xacts[i as usize].set(st.prep_xact(n - 1)) };
            let g = st.gxact(idx);
            unsafe { g.next.set(st.free_gxacts.get()) };
            unsafe { st.free_gxacts.set(idx) };
            CACHED_GXACT.set((InvalidTransactionId, NO_GXACT));
            return;
        }
    }
    panic!("failed to find gxact {idx} in GlobalTransaction array");
}

/// `TwoPhaseGetGXact` (with C's cached_xid/cached_gxact single-entry cache).
pub(crate) fn two_phase_get_gxact(xid: TransactionId, lock_held: bool) -> PgResult<i32> {
    let (cached_xid, cached_idx) = CACHED_GXACT.get();
    if xid == cached_xid {
        return Ok(cached_idx);
    }

    let st = TwoPhaseState();
    if !lock_held {
        lock_twophase_state(LW_SHARED);
    }
    let mut result = NO_GXACT;
    for i in 0..unsafe { st.num_prep_xacts.get() } {
        let idx = st.prep_xact(i);
        if unsafe { st.gxact(idx).xid.get() } == xid {
            result = idx;
            break;
        }
    }
    if !lock_held {
        unlock_twophase_state();
    }
    if result == NO_GXACT {
        return Err(ereport(ERROR)
            .errmsg(format!("failed to find GlobalTransaction for xid {xid}"))
            .finish(here("TwoPhaseGetGXact"))
            .unwrap_err());
    }
    CACHED_GXACT.set((xid, result));
    Ok(result)
}

pub fn TwoPhaseGetDummyProcNumber(xid: TransactionId, lock_held: bool) -> PgResult<ProcNumber> {
    let idx = two_phase_get_gxact(xid, lock_held)?;
    Ok(unsafe { TwoPhaseState().gxact(idx).pgprocno.get() })
}

/// `TwoPhaseGetXidByVirtualXID`.
pub fn TwoPhaseGetXidByVirtualXID(
    vxid: (ProcNumber, u32),
    have_more: &mut bool,
) -> PgResult<TransactionId> {
    let st = TwoPhaseState();
    let mut result = InvalidTransactionId;
    lock_twophase_state(LW_SHARED);
    for i in 0..unsafe { st.num_prep_xacts.get() } {
        let g = st.gxact(st.prep_xact(i));
        if !unsafe { g.valid.get() } {
            continue;
        }
        let proc = lmgr_proc::GetPGProcByNumber(unsafe { g.pgprocno.get() });
        let proc_vxid = (
            proc.vxid.procNumber.load(Relaxed),
            proc.vxid.lxid.load(Relaxed),
        );
        if proc_vxid == vxid {
            debug_assert!(!unsafe { g.inredo.get() });
            if result != InvalidTransactionId {
                *have_more = true;
                break;
            }
            result = unsafe { g.xid.get() };
        }
    }
    unlock_twophase_state();
    Ok(result)
}

// ---- state-file assembly (C's file-scope `records` chain) ----

pub(crate) struct SaveState {
    // Cold path, one buffer per PREPARE, freed at EndPrepare — mirrors C's
    // per-prepare palloc chain (std Vec justified in the port notes).
    buf: Vec<u8>,
    pub total_len: u32,
}

impl SaveState {
    fn new() -> Self {
        SaveState {
            buf: Vec::new(),
            total_len: 0,
        }
    }

    /// `save_state_data`: append, padded to MAXALIGN.
    fn save(&mut self, data: &[u8]) {
        let padlen = maxalign(data.len());
        self.buf.reserve(padlen);
        self.buf.extend_from_slice(data);
        self.buf.resize(self.buf.len() + (padlen - data.len()), 0);
        self.total_len += padlen as u32;
    }
}

thread_local! {
    static PREPARE_BUILDER: std::cell::RefCell<Option<SaveState>> =
        const { std::cell::RefCell::new(None) };
}

/// `StartPrepare(gxact)` over the caller-gathered inputs
/// (`twophase_seams::StartPrepareArgs`).
pub(crate) fn start_prepare(args: &twophase_seams::StartPrepareArgs) -> PgResult<()> {
    let idx = MY_LOCKED_GXACT.get();
    assert!(idx != NO_GXACT, "StartPrepare without MarkAsPreparing");
    let st = TwoPhaseState();
    let g = st.gxact(idx);
    debug_assert_eq!(unsafe { g.xid.get() }, args.xid);

    let hdr = TwoPhaseFileHeader {
        magic: crate::codec::TWOPHASE_MAGIC,
        total_len: 0, // EndPrepare fills this in
        xid: args.xid,
        database: args.databaseid,
        prepared_at: args.prepared_at,
        owner: args.owner,
        nsubxacts: args.children.len() as i32,
        ncommitrels: args.ncommitrels,
        nabortrels: args.nabortrels,
        ncommitstats: args.ncommitstats,
        nabortstats: args.nabortstats,
        ninvalmsgs: args.ninvalmsgs,
        initfileinval: args.initfileinval,
        gidlen: (args.gid.len() + 1) as u16, // include '\0'
        origin_lsn: 0,
        origin_timestamp: 0,
    };

    let mut b = SaveState::new();
    b.save(&hdr.to_bytes());
    let mut gidbuf = Vec::with_capacity(args.gid.len() + 1);
    gidbuf.extend_from_slice(args.gid.as_bytes());
    gidbuf.push(0);
    b.save(&gidbuf);

    if !args.children.is_empty() {
        let mut sub = Vec::with_capacity(args.children.len() * 4);
        for c in &args.children {
            sub.extend_from_slice(&c.to_ne_bytes());
        }
        b.save(&sub);
        gxact_load_subxact_data(idx, &args.children);
    }
    if args.ncommitrels > 0 {
        b.save(&args.commitrels);
    }
    if args.nabortrels > 0 {
        b.save(&args.abortrels);
    }
    if args.ncommitstats > 0 {
        b.save(&args.commitstats);
    }
    if args.nabortstats > 0 {
        b.save(&args.abortstats);
    }
    if args.ninvalmsgs > 0 {
        b.save(&args.invalmsgs);
    }

    PREPARE_BUILDER.with(|c| *c.borrow_mut() = Some(b));
    Ok(())
}

/// `RegisterTwoPhaseRecord(rmid, info, data, len)`.
pub fn RegisterTwoPhaseRecord(rmid: u8, info: u16, data: &[u8]) -> PgResult<()> {
    PREPARE_BUILDER.with(|c| {
        let mut slot = c.borrow_mut();
        let b = slot
            .as_mut()
            .expect("RegisterTwoPhaseRecord without a StartPrepare builder");
        let record = TwoPhaseRecordOnDisk {
            rmid,
            info,
            len: data.len() as u32,
        };
        b.save(&record.to_bytes());
        if !data.is_empty() {
            b.save(data);
        }
    });
    Ok(())
}

fn replorigin_session() -> (types_core::RepOriginId, XLogRecPtr, TimestampTz) {
    // Uninstalled origin seams = C defaults (origin.c globals; xact precedent).
    if origin_seams::replorigin_session_origin::is_installed() {
        (
            origin_seams::replorigin_session_origin::call(),
            origin_seams::replorigin_session_origin_lsn::call(),
            origin_seams::replorigin_session_origin_timestamp::call(),
        )
    } else {
        (0, 0, 0)
    }
}

pub(crate) const DO_NOT_REPLICATE_ID: types_core::RepOriginId = 0xFFFF;

/// `EndPrepare(gxact)`: finish the state image, WAL-log it, MarkAsPrepared.
pub(crate) fn end_prepare() -> PgResult<()> {
    let idx = MY_LOCKED_GXACT.get();
    assert!(idx != NO_GXACT, "EndPrepare without MarkAsPreparing");
    let st = TwoPhaseState();
    let g = st.gxact(idx);

    RegisterTwoPhaseRecord(0, 0, &[])?; // TWOPHASE_RM_END_ID sentinel

    let mut builder = PREPARE_BUILDER
        .with(|c| c.borrow_mut().take())
        .expect("EndPrepare without a StartPrepare builder");

    let total_len = builder.total_len + 4; // sizeof(pg_crc32c)
    builder.buf[4..8].copy_from_slice(&total_len.to_ne_bytes());

    let (origin, origin_lsn, origin_ts) = replorigin_session();
    let replorigin = origin != 0 && origin != DO_NOT_REPLICATE_ID;
    if replorigin {
        builder.buf[56..64].copy_from_slice(&origin_lsn.to_ne_bytes());
        builder.buf[64..72].copy_from_slice(&origin_ts.to_ne_bytes());
    }

    if total_len > MAX_ALLOC_SIZE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("two-phase state file maximum length exceeded")
            .finish(here("EndPrepare"))
            .unwrap_err());
    }

    // The single contiguous image needs no XLogEnsureRecordSpace (C reserves
    // rdata slots for its chunk chain).
    let my_proc = lmgr_proc::GetPGProcByNumber(init_small::globals::MyProcNumber());
    init_small::globals::StartCriticalSection();
    debug_assert_eq!(
        my_proc.delayChkptFlags.load(Relaxed) & types_storage::storage::DELAY_CHKPT_START,
        0
    );
    my_proc
        .delayChkptFlags
        .fetch_or(types_storage::storage::DELAY_CHKPT_START, Relaxed);

    let prepare_end_lsn = xloginsert_seams::xlog_insert_with_flags::call(
        RM_XACT_ID,
        XLOG_XACT_PREPARE,
        transam_xlog::XLOG_INCLUDE_ORIGIN,
        &[&builder.buf],
    )?;
    unsafe { g.prepare_end_lsn.set(prepare_end_lsn) };

    if replorigin {
        origin_seams::replorigin_session_advance::call(origin_lsn, prepare_end_lsn)?;
    }

    transam_xlog::XLogFlush(prepare_end_lsn)?;

    // If we crash now, we have prepared: WAL replay will fix things.

    unsafe { g.prepare_start_lsn.set(transam_xlog::ProcLastRecPtr()) };

    mark_as_prepared(idx, false)?;

    my_proc
        .delayChkptFlags
        .fetch_and(!types_storage::storage::DELAY_CHKPT_START, Relaxed);

    MY_LOCKED_GXACT.set(idx);

    init_small::globals::EndCriticalSection();

    // C SyncRepWaitForLSN no-ops without sync standbys; syncrep unported.
    if syncrep_seams::sync_rep_wait_for_lsn::is_installed() {
        syncrep_seams::sync_rep_wait_for_lsn::call(prepare_end_lsn, false)?;
    }

    Ok(())
}

/// `XlogReadTwoPhaseData(lsn)`: re-read the prepare record body from WAL.
pub(crate) fn xlog_read_twophase_data(lsn: XLogRecPtr) -> PgResult<Vec<u8>> {
    let ctx = mcx::MemoryContext::new("XlogReadTwoPhaseData");
    let mut reader =
        xlogreader::XLogReaderState::allocate(ctx.mcx(), transam_xlog::wal_segment_size())?;
    let mut routine = xlogreader::LocalPageRead { wait_for_wal: true };

    let result = xlog_read_twophase_record(&mut reader, &mut routine, lsn);
    // XLogReaderFree (twophase.c:1437, and the error unwind's resource
    // release in C): close the segment the reader opened, on every path.
    reader.XLogReaderFree(&mut routine);
    result
}

fn xlog_read_twophase_record(
    reader: &mut xlogreader::XLogReaderState<'_>,
    routine: &mut xlogreader::LocalPageRead,
    lsn: XLogRecPtr,
) -> PgResult<Vec<u8>> {
    reader.XLogBeginRead(lsn);
    let record = reader.XLogReadRecord(routine)?;
    let (h, l) = ((lsn >> 32) as u32, lsn as u32);
    if record.is_none() {
        return match reader.errormsg() {
            Some(msg) => Err(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not read two-phase state from WAL at {h:X}/{l:X}: {msg}"
                ))
                .finish(here("XlogReadTwoPhaseData"))
                .unwrap_err()),
            None => Err(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not read two-phase state from WAL at {h:X}/{l:X}"
                ))
                .finish(here("XlogReadTwoPhaseData"))
                .unwrap_err()),
        };
    }

    if reader.XLogRecGetRmid() != RM_XACT_ID
        || (reader.XLogRecGetInfo() & XLOG_XACT_OPMASK) != XLOG_XACT_PREPARE
    {
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!(
                "expected two-phase state data is not present in WAL at {h:X}/{l:X}"
            ))
            .finish(here("XlogReadTwoPhaseData"))
            .unwrap_err());
    }

    Ok(reader.XLogRecGetData().to_vec())
}

/// `StandbyTransactionIdIsPrepared`.
pub fn StandbyTransactionIdIsPrepared(xid: TransactionId) -> PgResult<bool> {
    debug_assert!(TransactionIdIsValid(xid));
    if max_prepared_xacts() <= 0 {
        return Ok(false);
    }
    let Some(buf) = crate::files::read_twophase_file(xid, true)? else {
        return Ok(false);
    };
    let hdr = corrupt_guard(TwoPhaseFileHeader::from_bytes(&buf), "StandbyTransactionIdIsPrepared")?;
    Ok(hdr.xid == xid)
}

pub(crate) fn corrupt_guard(
    hdr: Option<TwoPhaseFileHeader>,
    func: &'static str,
) -> PgResult<TwoPhaseFileHeader> {
    hdr.ok_or_else(|| {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg("corrupted two-phase state buffer")
            .finish(here(func))
            .unwrap_err()
    })
}

/// Validate the state-file segment layout against the actual buffer length.
///
/// The counts and `gidlen` in the header are untrusted (disk file or a WAL
/// record from a possibly hostile primary). `BufferLayout::try_of` rejects
/// negative counts, arithmetic overflow, and any segment running past the
/// buffer; here we turn that rejection into a catchable data-corruption error
/// so no recovery/finish path can panic or over-read on crafted input.
pub(crate) fn buffer_layout(
    hdr: &TwoPhaseFileHeader,
    buf: &[u8],
    func: &'static str,
) -> PgResult<BufferLayout> {
    BufferLayout::try_of(hdr, buf.len()).ok_or_else(|| {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg("corrupted two-phase state buffer")
            .finish(here(func))
            .unwrap_err()
    })
}

/// `ProcessRecords`: dispatch each 2PC record to `callbacks[rmid]`.
pub(crate) fn process_records(
    buf: &[u8],
    mut off: usize,
    xid: TransactionId,
    callbacks: &[Option<twophase_rmgr::TwoPhaseCallback>; twophase_rmgr::NUM_TWOPHASE_RM],
) -> PgResult<()> {
    // Every field of the record stream is attacker-influenced (WAL PREPARE body
    // from a hostile primary, or a crafted pg_twophase file with a valid CRC).
    // C's ProcessRecords trusts the on-disk layout via an Assert, but here we
    // must validate in release builds too: a bad rmid, an oversized len, or a
    // truncated/unterminated stream must surface as a catchable
    // ERRCODE_DATA_CORRUPTED error rather than an out-of-bounds panic.
    let corrupt = |msg: &str| {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!("corrupted two-phase state record: {msg}"))
            .finish(here("process_records"))
            .unwrap_err()
    };
    loop {
        // The record offset itself (derived from an unvalidated layout) must be
        // within the buffer before we slice a header out of it.
        if off > buf.len() {
            return Err(corrupt("record offset past end of buffer"));
        }
        // A full record header must remain before we read it.
        let record = TwoPhaseRecordOnDisk::from_bytes(&buf[off..])
            .ok_or_else(|| corrupt("truncated two-phase record header"))?;
        // rmid must index within the callback table (NUM_TWOPHASE_RM entries);
        // any rmid >= NUM_TWOPHASE_RM would be an out-of-bounds array index.
        if record.rmid as usize >= callbacks.len() {
            return Err(corrupt("resource-manager id out of range"));
        }
        if record.rmid == twophase_rmgr::TWOPHASE_RM_END_ID {
            break;
        }
        off += maxalign(SIZEOF_TWOPHASE_RECORD_ON_DISK);
        let datalen = record.len as usize;
        // The declared payload length must fit within the remaining buffer, or
        // the callback slice would over-read past the end.
        if datalen > buf.len().saturating_sub(off) {
            return Err(corrupt("record length exceeds remaining buffer"));
        }
        if let Some(cb) = callbacks[record.rmid as usize] {
            cb(xid, record.info, &buf[off..off + datalen])?;
        }
        off += maxalign(datalen);
    }
    Ok(())
}

// ---- GID helpers for logical-replication consumers ----

/// `TwoPhaseTransactionGid`.
pub fn TwoPhaseTransactionGid(subid: Oid, xid: TransactionId) -> PgResult<String> {
    debug_assert!(subid != 0);
    if !TransactionIdIsValid(xid) {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid two-phase transaction ID")
            .finish(here("TwoPhaseTransactionGid"))
            .unwrap_err());
    }
    Ok(format!("pg_gid_{subid}_{xid}"))
}

/// `IsTwoPhaseTransactionGidForSubid` (twophase.c:2700-2727). The
/// reconstruction through `TwoPhaseTransactionGid` is unguarded in C: a
/// matching-subid GID whose xid part is 0 raises 08P01 "invalid two-phase
/// transaction ID" (twophase.c:2688) instead of answering false.
pub fn IsTwoPhaseTransactionGidForSubid(subid: Oid, gid: &str) -> PgResult<bool> {
    // sscanf(gid, "pg_gid_%u_%u", ...) == 2: %u accepts an optional sign
    // and leading whitespace and stops at the first non-digit; the
    // reconstruction below rejects anything but the canonical spelling.
    let Some(rest) = gid.strip_prefix("pg_gid_") else {
        return Ok(false);
    };
    let (subid_from_gid, rest) = scan_u32(rest);
    let Some(subid_from_gid) = subid_from_gid else {
        return Ok(false);
    };
    let Some(rest) = rest.strip_prefix('_') else {
        return Ok(false);
    };
    let (xid_from_gid, _) = scan_u32(rest);
    let Some(xid_from_gid) = xid_from_gid else {
        return Ok(false);
    };
    if subid != subid_from_gid {
        return Ok(false);
    }
    let tmp = TwoPhaseTransactionGid(subid, xid_from_gid)?;
    Ok(tmp == gid)
}

// One sscanf "%u" conversion: optional leading whitespace, optional sign,
// at least one digit (wrapping on overflow like strtoul's 32-bit truncation);
// returns the value and the unconsumed remainder.
fn scan_u32(s: &str) -> (Option<u32>, &str) {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    let mut neg = false;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        neg = b[i] == b'-';
        i += 1;
    }
    let start = i;
    let mut v: u64 = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        v = v.wrapping_mul(10).wrapping_add((b[i] - b'0') as u64);
        i += 1;
    }
    if i == start {
        return (None, s);
    }
    let v = v as u32;
    (Some(if neg { v.wrapping_neg() } else { v }), &s[i..])
}

/// `LookupGXactBySubid` (twophase.c:2735-2757). An ERROR raised by the GID
/// check inside the scan releases TwoPhaseStateLock through C's error
/// cleanup; here it is released explicitly before propagating.
pub fn LookupGXactBySubid(subid: Oid) -> PgResult<bool> {
    let st = TwoPhaseState();
    let mut found = false;
    lock_twophase_state(LW_SHARED);
    for i in 0..unsafe { st.num_prep_xacts.get() } {
        let g = st.gxact(st.prep_xact(i));
        if !unsafe { g.valid.get() } {
            continue;
        }
        match IsTwoPhaseTransactionGidForSubid(subid, unsafe { g.gid.get() }.as_str()) {
            Ok(true) => {
                found = true;
                break;
            }
            Ok(false) => {}
            Err(e) => {
                unlock_twophase_state();
                return Err(e);
            }
        }
    }
    unlock_twophase_state();
    Ok(found)
}
