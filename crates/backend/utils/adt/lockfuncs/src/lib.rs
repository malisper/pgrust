//! lockfuncs.c: pg_lock_status, pg_blocking_pids,
//! pg_safe_snapshot_blocking_pids, and the advisory-lock function family.

use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::{Oid, INT4OID};
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use ::types_storage::lock::{
    ExclusiveLock, LockInstanceData, NoLock, ShareLock, LOCKACQUIRE_NOT_AVAIL, LOCKBIT_ON,
    LOCKMODE, LOCKTAG, LOCKTAG_APPLY_TRANSACTION, LOCKTAG_DATABASE_FROZEN_IDS, LOCKTAG_LAST_TYPE,
    LOCKTAG_PAGE, LOCKTAG_RELATION, LOCKTAG_RELATION_EXTEND, LOCKTAG_SPECULATIVE_TOKEN,
    LOCKTAG_TRANSACTION, LOCKTAG_TUPLE, LOCKTAG_VIRTUALTRANSACTION, MAX_LOCKMODES,
    USER_LOCKMETHOD,
};
use predicate::internals::{
    GET_PREDICATELOCKTARGETTAG_DB, GET_PREDICATELOCKTARGETTAG_OFFSET,
    GET_PREDICATELOCKTARGETTAG_PAGE, GET_PREDICATELOCKTARGETTAG_RELATION,
    GET_PREDICATELOCKTARGETTAG_TYPE, PREDLOCKTAG_PAGE, PREDLOCKTAG_TUPLE,
};

// Must match enum LockTagType (lock.h) — pg_locks view names.
const LOCK_TAG_TYPE_NAMES: [&str; 12] = [
    "relation",
    "extend",
    "frozenid",
    "page",
    "tuple",
    "transactionid",
    "virtualxid",
    "spectoken",
    "object",
    "userlock",
    "advisory",
    "applytransaction",
];
const _: () = assert!(LOCK_TAG_TYPE_NAMES.len() == LOCKTAG_LAST_TYPE as usize + 1);

const PREDICATE_LOCK_TAG_TYPE_NAMES: [&str; 3] = ["relation", "page", "tuple"];

const NUM_LOCK_STATUS_COLUMNS: usize = 16;

fn set_locktag_int64(key: i64) -> LOCKTAG {
    LOCKTAG::advisory(
        init_small::globals::MyDatabaseId(),
        (key as u64 >> 32) as u32,
        key as u32,
        1,
    )
}

fn set_locktag_int32(key1: i32, key2: i32) -> LOCKTAG {
    LOCKTAG::advisory(init_small::globals::MyDatabaseId(), key1 as u32, key2 as u32, 2)
}

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?))
}

fn vxid_datum(mcx: Mcx<'_>, proc_number: i32, lxid: u32) -> PgResult<Datum> {
    text_datum(mcx, &format!("{proc_number}/{lxid}"))
}

fn int4_array_datum(mcx: Mcx<'_>, vals: &[i32]) -> PgResult<Datum> {
    // construct_md_array returns a zero-dimensional array for nelems == 0;
    // array_eq treats empty-1D and empty-0D as unequal, so this matters.
    let img = if vals.is_empty() {
        datum::array_build::construct_empty_array_image(mcx, INT4OID)?
    } else {
        let mut v: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, vals.len())?;
        v.extend(vals.iter().map(|&i| Datum::from_i32(i)));
        datum::array_build::construct_array_image(mcx, &v, INT4OID, 4, true, b'i')?
    };
    let img = img.leak();
    Ok(Datum::from_usize(img.as_ptr() as usize))
}

fn lock_status_row(
    srf: &mut funcapi::MaterializedSRF<'_>,
    mcx: Mcx<'_>,
    instance: &LockInstanceData,
    mode: LOCKMODE,
    granted: bool,
) -> PgResult<()> {
    let mut values = [Datum::from_usize(0); NUM_LOCK_STATUS_COLUMNS];
    let mut nulls = [false; NUM_LOCK_STATUS_COLUMNS];
    let tag = &instance.locktag;

    if tag.locktag_type <= LOCKTAG_LAST_TYPE {
        values[0] = text_datum(mcx, LOCK_TAG_TYPE_NAMES[tag.locktag_type as usize])?;
    } else {
        values[0] = text_datum(mcx, &format!("unknown {}", tag.locktag_type))?;
    }

    for n in &mut nulls[1..=9] {
        *n = true;
    }
    match tag.locktag_type {
        LOCKTAG_RELATION | LOCKTAG_RELATION_EXTEND => {
            values[1] = Datum::from_oid(tag.locktag_field1);
            values[2] = Datum::from_oid(tag.locktag_field2);
            nulls[1] = false;
            nulls[2] = false;
        }
        LOCKTAG_DATABASE_FROZEN_IDS => {
            values[1] = Datum::from_oid(tag.locktag_field1);
            nulls[1] = false;
        }
        LOCKTAG_PAGE => {
            values[1] = Datum::from_oid(tag.locktag_field1);
            values[2] = Datum::from_oid(tag.locktag_field2);
            values[3] = Datum::from_u32(tag.locktag_field3);
            nulls[1] = false;
            nulls[2] = false;
            nulls[3] = false;
        }
        LOCKTAG_TUPLE => {
            values[1] = Datum::from_oid(tag.locktag_field1);
            values[2] = Datum::from_oid(tag.locktag_field2);
            values[3] = Datum::from_u32(tag.locktag_field3);
            values[4] = Datum::from_u16(tag.locktag_field4);
            nulls[1] = false;
            nulls[2] = false;
            nulls[3] = false;
            nulls[4] = false;
        }
        LOCKTAG_TRANSACTION => {
            values[6] = Datum::from_transaction_id(tag.locktag_field1);
            nulls[6] = false;
        }
        LOCKTAG_VIRTUALTRANSACTION => {
            values[5] = vxid_datum(mcx, tag.locktag_field1 as i32, tag.locktag_field2)?;
            nulls[5] = false;
        }
        LOCKTAG_SPECULATIVE_TOKEN => {
            values[6] = Datum::from_transaction_id(tag.locktag_field1);
            values[8] = Datum::from_oid(tag.locktag_field2);
            nulls[6] = false;
            nulls[8] = false;
        }
        LOCKTAG_APPLY_TRANSACTION => {
            values[1] = Datum::from_oid(tag.locktag_field1);
            values[8] = Datum::from_oid(tag.locktag_field2);
            values[6] = Datum::from_oid(tag.locktag_field3);
            values[9] = Datum::from_i16(tag.locktag_field4 as i16);
            nulls[1] = false;
            nulls[6] = false;
            nulls[8] = false;
            nulls[9] = false;
        }
        // LOCKTAG_OBJECT, LOCKTAG_USERLOCK, LOCKTAG_ADVISORY; unknown types
        // take this arm too (C's default).
        _ => {
            values[1] = Datum::from_oid(tag.locktag_field1);
            values[7] = Datum::from_oid(tag.locktag_field2);
            values[8] = Datum::from_oid(tag.locktag_field3);
            values[9] = Datum::from_i16(tag.locktag_field4 as i16);
            nulls[1] = false;
            nulls[7] = false;
            nulls[8] = false;
            nulls[9] = false;
        }
    }

    values[10] = vxid_datum(mcx, instance.vxid.procNumber, instance.vxid.localTransactionId)?;
    if instance.pid != 0 {
        values[11] = Datum::from_i32(instance.pid);
    } else {
        nulls[11] = true;
    }
    values[12] = text_datum(mcx, lock::GetLockmodeName(tag.locktag_lockmethodid.into(), mode))?;
    values[13] = Datum::from_bool(granted);
    values[14] = Datum::from_bool(instance.fastpath);
    if !granted && instance.waitStart != 0 {
        values[15] = Datum::from_i64(instance.waitStart);
    } else {
        nulls[15] = true;
    }

    srf.putvalues(&values, &nulls)
}

pub fn fc_pg_lock_status(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_lock_status: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts as usize, NUM_LOCK_STATUS_COLUMNS);

    for instance in lock::GetLockStatusData()? {
        if instance.holdMask != 0 {
            for mode in 0..MAX_LOCKMODES as LOCKMODE {
                if instance.holdMask & LOCKBIT_ON(mode) != 0 {
                    lock_status_row(&mut srf, mcx, &instance, mode, true)?;
                }
            }
        }
        if instance.waitLockMode != NoLock {
            lock_status_row(&mut srf, mcx, &instance, instance.waitLockMode, false)?;
        }
    }

    for entry in predicate::GetPredicateLockStatusData()? {
        let mut values = [Datum::from_usize(0); NUM_LOCK_STATUS_COLUMNS];
        let mut nulls = [false; NUM_LOCK_STATUS_COLUMNS];
        let lock_type = GET_PREDICATELOCKTARGETTAG_TYPE(&entry.tag);

        values[0] = text_datum(mcx, PREDICATE_LOCK_TAG_TYPE_NAMES[lock_type as usize])?;
        values[1] = Datum::from_oid(GET_PREDICATELOCKTARGETTAG_DB(&entry.tag));
        values[2] = Datum::from_oid(GET_PREDICATELOCKTARGETTAG_RELATION(&entry.tag));
        if lock_type == PREDLOCKTAG_TUPLE {
            values[4] = Datum::from_u16(GET_PREDICATELOCKTARGETTAG_OFFSET(&entry.tag));
        } else {
            nulls[4] = true;
        }
        if lock_type == PREDLOCKTAG_TUPLE || lock_type == PREDLOCKTAG_PAGE {
            values[3] = Datum::from_u32(GET_PREDICATELOCKTARGETTAG_PAGE(&entry.tag));
        } else {
            nulls[3] = true;
        }
        for n in &mut nulls[5..=9] {
            *n = true;
        }
        values[10] = vxid_datum(mcx, entry.vxid.procNumber, entry.vxid.localTransactionId)?;
        if entry.pid != 0 {
            values[11] = Datum::from_i32(entry.pid);
        } else {
            nulls[11] = true;
        }
        // All predicate locks are held SIReadLocks with no fast path.
        values[12] = text_datum(mcx, "SIReadLock")?;
        values[13] = Datum::from_bool(true);
        values[14] = Datum::from_bool(false);
        nulls[15] = true;

        srf.putvalues(&values, &nulls)?;
    }

    Ok(srf.finish(fcinfo))
}

pub fn blocking_pids(blocked_pid: i32) -> PgResult<Vec<i32>> {
    let lock_data = lock::GetBlockerStatusData(blocked_pid)?;
    let mut pids = Vec::with_capacity(lock_data.locks.len());

    for bproc in &lock_data.procs {
        let instances = &lock_data.locks[bproc.first_lock..bproc.first_lock + bproc.num_locks];
        let preceding_waiters =
            &lock_data.waiter_pids[bproc.first_waiter..bproc.first_waiter + bproc.num_waiters];

        let blocked_instance = instances
            .iter()
            .find(|i| i.pid == bproc.pid)
            .expect("blocked proc's own lock instance missing");
        let table = lock::GetLockTagsMethodTable(&blocked_instance.locktag);
        let conflict_mask = table.conflictTab[blocked_instance.waitLockMode as usize];

        for instance in instances {
            if std::ptr::eq(instance, blocked_instance) {
                continue;
            }
            if instance.leaderPid == blocked_instance.leaderPid {
                continue;
            }
            if conflict_mask & instance.holdMask != 0 {
                // hard block: held lock conflicts with the awaited mode
            } else if instance.waitLockMode != NoLock
                && conflict_mask & LOCKBIT_ON(instance.waitLockMode) != 0
            {
                // soft block iff this waiter is ahead of blocked proc in queue
                if !preceding_waiters.contains(&instance.pid) {
                    continue;
                }
            } else {
                continue;
            }
            pids.push(instance.leaderPid);
        }
    }
    Ok(pids)
}

pub fn fc_pg_blocking_pids(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let blocked_pid = fcinfo.arg_i32(0);
    let pids = blocking_pids(blocked_pid)?;
    int4_array_datum(fcinfo.result_mcx(), &pids)
}

pub fn fc_pg_safe_snapshot_blocking_pids(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let blocked_pid = fcinfo.arg_i32(0);
    let blockers = predicate::GetSafeSnapshotBlockingPids(
        blocked_pid,
        init_small::globals::MaxBackends() as usize,
    )?;
    int4_array_datum(fcinfo.result_mcx(), &blockers)
}

macro_rules! advisory_lock_fn {
    ($name:ident, int8, $mode:expr, $session:expr) => {
        pub fn $name(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let tag = set_locktag_int64(fcinfo.arg_i64(0));
            lock::LockAcquire(&tag, $mode, $session, false)?;
            Ok(Datum::from_usize(0))
        }
    };
    ($name:ident, int4, $mode:expr, $session:expr) => {
        pub fn $name(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let tag = set_locktag_int32(fcinfo.arg_i32(0), fcinfo.arg_i32(1));
            lock::LockAcquire(&tag, $mode, $session, false)?;
            Ok(Datum::from_usize(0))
        }
    };
}

macro_rules! advisory_try_fn {
    ($name:ident, int8, $mode:expr, $session:expr) => {
        pub fn $name(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let tag = set_locktag_int64(fcinfo.arg_i64(0));
            let res = lock::LockAcquire(&tag, $mode, $session, true)?;
            Ok(Datum::from_bool(res != LOCKACQUIRE_NOT_AVAIL))
        }
    };
    ($name:ident, int4, $mode:expr, $session:expr) => {
        pub fn $name(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let tag = set_locktag_int32(fcinfo.arg_i32(0), fcinfo.arg_i32(1));
            let res = lock::LockAcquire(&tag, $mode, $session, true)?;
            Ok(Datum::from_bool(res != LOCKACQUIRE_NOT_AVAIL))
        }
    };
}

macro_rules! advisory_unlock_fn {
    ($name:ident, int8, $mode:expr) => {
        pub fn $name(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let tag = set_locktag_int64(fcinfo.arg_i64(0));
            Ok(Datum::from_bool(lock::LockRelease(&tag, $mode, true)?))
        }
    };
    ($name:ident, int4, $mode:expr) => {
        pub fn $name(_fl: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let tag = set_locktag_int32(fcinfo.arg_i32(0), fcinfo.arg_i32(1));
            Ok(Datum::from_bool(lock::LockRelease(&tag, $mode, true)?))
        }
    };
}

advisory_lock_fn!(fc_pg_advisory_lock_int8, int8, ExclusiveLock, true);
advisory_lock_fn!(fc_pg_advisory_xact_lock_int8, int8, ExclusiveLock, false);
advisory_lock_fn!(fc_pg_advisory_lock_shared_int8, int8, ShareLock, true);
advisory_lock_fn!(fc_pg_advisory_xact_lock_shared_int8, int8, ShareLock, false);
advisory_try_fn!(fc_pg_try_advisory_lock_int8, int8, ExclusiveLock, true);
advisory_try_fn!(fc_pg_try_advisory_xact_lock_int8, int8, ExclusiveLock, false);
advisory_try_fn!(fc_pg_try_advisory_lock_shared_int8, int8, ShareLock, true);
advisory_try_fn!(fc_pg_try_advisory_xact_lock_shared_int8, int8, ShareLock, false);
advisory_unlock_fn!(fc_pg_advisory_unlock_int8, int8, ExclusiveLock);
advisory_unlock_fn!(fc_pg_advisory_unlock_shared_int8, int8, ShareLock);
advisory_lock_fn!(fc_pg_advisory_lock_int4, int4, ExclusiveLock, true);
advisory_lock_fn!(fc_pg_advisory_xact_lock_int4, int4, ExclusiveLock, false);
advisory_lock_fn!(fc_pg_advisory_lock_shared_int4, int4, ShareLock, true);
advisory_lock_fn!(fc_pg_advisory_xact_lock_shared_int4, int4, ShareLock, false);
advisory_try_fn!(fc_pg_try_advisory_lock_int4, int4, ExclusiveLock, true);
advisory_try_fn!(fc_pg_try_advisory_xact_lock_int4, int4, ExclusiveLock, false);
advisory_try_fn!(fc_pg_try_advisory_lock_shared_int4, int4, ShareLock, true);
advisory_try_fn!(fc_pg_try_advisory_xact_lock_shared_int4, int4, ShareLock, false);
advisory_unlock_fn!(fc_pg_advisory_unlock_int4, int4, ExclusiveLock);
advisory_unlock_fn!(fc_pg_advisory_unlock_shared_int4, int4, ShareLock);

pub fn fc_pg_advisory_unlock_all(
    _fl: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    lock::LockReleaseSession(USER_LOCKMETHOD.into())?;
    Ok(Datum::from_usize(0))
}

const fn b(
    foid: Oid,
    name: &'static str,
    nargs: i16,
    retset: bool,
    func: PGFunction,
) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset, func }
}

pub const LOCKFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(1371, "pg_lock_status", 0, true, fc_pg_lock_status),
    b(2561, "pg_blocking_pids", 1, false, fc_pg_blocking_pids),
    b(3376, "pg_safe_snapshot_blocking_pids", 1, false, fc_pg_safe_snapshot_blocking_pids),
    b(2880, "pg_advisory_lock_int8", 1, false, fc_pg_advisory_lock_int8),
    b(3089, "pg_advisory_xact_lock_int8", 1, false, fc_pg_advisory_xact_lock_int8),
    b(2881, "pg_advisory_lock_shared_int8", 1, false, fc_pg_advisory_lock_shared_int8),
    b(3090, "pg_advisory_xact_lock_shared_int8", 1, false, fc_pg_advisory_xact_lock_shared_int8),
    b(2882, "pg_try_advisory_lock_int8", 1, false, fc_pg_try_advisory_lock_int8),
    b(3091, "pg_try_advisory_xact_lock_int8", 1, false, fc_pg_try_advisory_xact_lock_int8),
    b(2883, "pg_try_advisory_lock_shared_int8", 1, false, fc_pg_try_advisory_lock_shared_int8),
    b(
        3092,
        "pg_try_advisory_xact_lock_shared_int8",
        1,
        false,
        fc_pg_try_advisory_xact_lock_shared_int8,
    ),
    b(2884, "pg_advisory_unlock_int8", 1, false, fc_pg_advisory_unlock_int8),
    b(2885, "pg_advisory_unlock_shared_int8", 1, false, fc_pg_advisory_unlock_shared_int8),
    b(2886, "pg_advisory_lock_int4", 2, false, fc_pg_advisory_lock_int4),
    b(3093, "pg_advisory_xact_lock_int4", 2, false, fc_pg_advisory_xact_lock_int4),
    b(2887, "pg_advisory_lock_shared_int4", 2, false, fc_pg_advisory_lock_shared_int4),
    b(3094, "pg_advisory_xact_lock_shared_int4", 2, false, fc_pg_advisory_xact_lock_shared_int4),
    b(2888, "pg_try_advisory_lock_int4", 2, false, fc_pg_try_advisory_lock_int4),
    b(3095, "pg_try_advisory_xact_lock_int4", 2, false, fc_pg_try_advisory_xact_lock_int4),
    b(2889, "pg_try_advisory_lock_shared_int4", 2, false, fc_pg_try_advisory_lock_shared_int4),
    b(
        3096,
        "pg_try_advisory_xact_lock_shared_int4",
        2,
        false,
        fc_pg_try_advisory_xact_lock_shared_int4,
    ),
    b(2890, "pg_advisory_unlock_int4", 2, false, fc_pg_advisory_unlock_int4),
    b(2891, "pg_advisory_unlock_shared_int4", 2, false, fc_pg_advisory_unlock_shared_int4),
    b(2892, "pg_advisory_unlock_all", 0, false, fc_pg_advisory_unlock_all),
];

#[cfg(test)]
mod tests;
