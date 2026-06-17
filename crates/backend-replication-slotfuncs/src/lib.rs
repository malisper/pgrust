//! Port of PostgreSQL 18.3 `src/backend/replication/slotfuncs.c` — the
//! SQL-callable replication-slot support functions.
//!
//! Every function defined in `slotfuncs.c` is present, with bodies ported 1:1:
//! identical branch order, loop bounds, switch arms, struct-field order,
//! message text, and SQLSTATE.
//!
//! `slotfuncs.c` is orchestration over landed sibling subsystems and a few
//! not-yet-ported owners:
//!
//! * the replication-slot shared-memory state — `replication/slot.c`
//!   ([`backend_replication_slot`]): `MyReplicationSlot`, the slot array under
//!   `ReplicationSlotControlLock`, the lifecycle routines, and the spinlocked
//!   slot-array snapshots / `MyReplicationSlot` field access (the locking
//!   substrate is owned there);
//! * logical decoding — `replication/logical.c`
//!   ([`backend_replication_logical_logical`]);
//! * the physical-failover wakeup — `replication/walsender.c`
//!   ([`backend_replication_walsender::PhysicalWakeupLogicalWalSnd`]);
//! * the WAL position / GUC helpers — `access/transam/xlog*.c` (xlog seams);
//! * the SRF / tuplestore plumbing — `utils/fmgr/funcapi.c` (funcapi seams);
//! * slot synchronization — `replication/slotsync.c` + `libpqwalreceiver` +
//!   `dfmgr` `load_file` (their owners' seams; panic until they land).
//!
//! ## fmgr / `Datum` boundary
//!
//! Each `Datum f(PG_FUNCTION_ARGS)` entry point is exposed with its arguments
//! already unwrapped to native Rust types and its result returned as a typed
//! value (a `*Row` struct, or [`Datum`] for the SRF). The
//! `get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE`
//! assertion + `heap_form_tuple` + `HeapTupleGetDatum` have no value-boundary
//! analog (the fmgr dispatch layer builds the record from the typed result),
//! matching the repo's `xlogfuncs.c` / `walsummaryfuncs.c` convention; the
//! `pg_get_replication_slots` SRF emits rows through `InitMaterializedSRF` +
//! `materialized_srf_putvalues` exactly as the C calls `tuplestore_putvalues`.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

use core::cmp::{max, min};

use mcx::Mcx;
use types_core::{InvalidOid, OidIsValid, XLogRecPtr, XLogSegNo};
use backend_utils_error::ereport;
use types_error::{
    PgResult, ERRCODE_CONNECTION_FAILURE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::NameData;
use types_tuple::Datum;
use types_wal::WALAvailability;
use types_replication_slot::{ReplicationSlotInvalidationCause, ReplicationSlotPersistency};

use backend_replication_slot as slot;
use backend_replication_slot::{CopiedSlotValues, SlotSnapshot};

use backend_utils_fmgr_funcapi_seams as funcapi;
use backend_access_transam_xlog_seams as xlog;
use backend_access_transam_xlogrecovery_seams as xlogrecovery;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_init_small_seams as smallinit;
use backend_utils_misc_guc_seams as guc;
use backend_utils_fmgr_dfmgr_seams as dfmgr;
use backend_replication_logical_slotsync_seams as slotsync;
use backend_replication_libpqwalreceiver_seams as walreceiver;

/// `PG_GET_REPLICATION_SLOTS_COLS` — the SRF column count (a local `#define`).
const PG_GET_REPLICATION_SLOTS_COLS: usize = 20;

// ---------------------------------------------------------------------------
// Result-row value types (the typed composite returns).
// ---------------------------------------------------------------------------

/// A `(slot_name name, lsn pg_lsn)` two-column result row, shared by
/// `pg_create_physical_replication_slot`, `pg_create_logical_replication_slot`,
/// `pg_replication_slot_advance`, and the copy functions. `lsn = None` is the
/// C `nulls[1] = true`.
pub struct SlotNameLsnRow {
    /// `NameGetDatum(&MyReplicationSlot->data.name)` / `NameGetDatum(dst_name)`.
    pub slot_name: NameData,
    /// `LSNGetDatum(...)`, or NULL.
    pub lsn: Option<XLogRecPtr>,
}

// ---------------------------------------------------------------------------
// Header helpers mirrored from the headers slotfuncs.c includes.
// ---------------------------------------------------------------------------

/// `XLogRecPtrIsInvalid(r)` (xlogdefs.h).
#[inline]
fn xlog_rec_ptr_is_invalid(r: XLogRecPtr) -> bool {
    r == 0
}

/// `XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)` (xlog_internal.h).
#[inline]
fn xlbyte_to_seg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> XLogSegNo {
    xlrp / (wal_segsz_bytes as u64)
}

/// `XLogSegNoOffsetToRecPtr(segno, offset, wal_segsz_bytes, dest)`.
#[inline]
fn xlog_segno_offset_to_rec_ptr(segno: XLogSegNo, offset: u64, wal_segsz_bytes: i32) -> XLogRecPtr {
    segno * (wal_segsz_bytes as u64) + offset
}

/// `XLogMBVarToSegs(mbvar, wal_segsz_bytes)`.
#[inline]
fn xlog_mb_var_to_segs(mbvar: i32, wal_segsz_bytes: i32) -> u64 {
    (mbvar as u64) / ((wal_segsz_bytes as u64) / (1024 * 1024))
}

/// Format an `XLogRecPtr` the way `%X/%X` + `LSN_FORMAT_ARGS` does.
fn lsn_format(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// `int wal_level` global, read through the xlog GUC seam, as the bare `int`
/// the slot.c / logical.c entry points take.
fn wal_level_int() -> i32 {
    xlog::wal_level::call() as i32
}

/// `int wal_level` as `logical.c`'s `WalLevel(int)` newtype.
fn wal_level_logical() -> types_logical::WalLevel {
    types_logical::WalLevel(wal_level_int())
}

// ---------------------------------------------------------------------------
// create_physical_replication_slot
// ---------------------------------------------------------------------------

/*
 * Helper function for creating a new physical replication slot with
 * given arguments. Note that this function doesn't release the created
 * slot.
 *
 * If restart_lsn is a valid value, we use it without WAL reservation
 * routine. So the caller must guarantee that WAL is available.
 */
fn create_physical_replication_slot(
    name: &str,
    immediately_reserve: bool,
    temporary: bool,
    restart_lsn: XLogRecPtr,
) -> PgResult<()> {
    debug_assert!(!slot::my_replication_slot_is_set());

    /* acquire replication slot, this will check for conflicting names */
    slot::ReplicationSlotCreate(
        name,
        false,
        if temporary {
            ReplicationSlotPersistency::RS_TEMPORARY
        } else {
            ReplicationSlotPersistency::RS_PERSISTENT
        },
        false,
        false,
        false,
        InvalidOid,
    )?;

    if immediately_reserve {
        /* Reserve WAL as the user asked for it */
        if xlog_rec_ptr_is_invalid(restart_lsn) {
            slot::ReplicationSlotReserveWal()?;
        } else {
            slot::set_my_slot_restart_lsn(restart_lsn);
        }

        /* Write this slot to disk */
        slot::ReplicationSlotMarkDirty();
        slot::ReplicationSlotSave()?;
    }

    Ok(())
}

/*
 * SQL function for creating a new physical (streaming replication)
 * replication slot.
 */
/// `pg_create_physical_replication_slot(name, immediately_reserve, temporary)`.
pub fn pg_create_physical_replication_slot(
    mcx: Mcx<'_>,
    name: &str,
    immediately_reserve: bool,
    temporary: bool,
) -> PgResult<SlotNameLsnRow> {
    // get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE check:
    // a catalog-return-type assertion with no value-boundary analog (the fmgr
    // dispatch builds the 2-column record from the typed result).

    slot::CheckSlotPermissions(mcx, miscinit::get_user_id::call())?;

    slot::CheckSlotRequirements(wal_level_int())?;

    create_physical_replication_slot(name, immediately_reserve, temporary, 0)?;

    // values[0] = NameGetDatum(&MyReplicationSlot->data.name);
    let slot_name = slot::my_slot_name();
    let lsn = if immediately_reserve {
        // values[1] = LSNGetDatum(MyReplicationSlot->data.restart_lsn);
        Some(slot::my_slot_restart_lsn())
    } else {
        // nulls[1] = true;
        None
    };

    let result = SlotNameLsnRow { slot_name, lsn };

    slot::ReplicationSlotRelease()?;

    Ok(result)
}

// ---------------------------------------------------------------------------
// create_logical_replication_slot
// ---------------------------------------------------------------------------

/*
 * Helper function for creating a new logical replication slot with
 * given arguments. Note that this function doesn't release the created
 * slot.
 *
 * When find_startpoint is false, the slot's confirmed_flush is not set; it's
 * caller's responsibility to ensure it's set to something sensible.
 */
fn create_logical_replication_slot(
    name: &str,
    plugin: &str,
    temporary: bool,
    two_phase: bool,
    failover: bool,
    restart_lsn: XLogRecPtr,
    find_startpoint: bool,
) -> PgResult<()> {
    debug_assert!(!slot::my_replication_slot_is_set());

    /*
     * Acquire a logical decoding slot, this will check for conflicting names.
     * Initially create persistent slot as ephemeral - that allows us to
     * nicely handle errors during initialization because it'll get dropped if
     * this transaction fails. We'll make it persistent at the end. Temporary
     * slots can be created as temporary from beginning as they get dropped on
     * error as well.
     */
    slot::ReplicationSlotCreate(
        name,
        true,
        if temporary {
            ReplicationSlotPersistency::RS_TEMPORARY
        } else {
            ReplicationSlotPersistency::RS_EPHEMERAL
        },
        two_phase,
        failover,
        false,
        smallinit::my_database_id::call(),
    )?;

    /*
     * Create logical decoding context to find start point or, if we don't
     * need it, to 1) bump slot's restart_lsn and xmin 2) check plugin sanity.
     *
     * Note: when !find_startpoint this is still important, because it's at
     * this point that the output plugin is validated.
     *
     * XL_ROUTINE(.page_read = read_local_xlog_page, .segment_open =
     * wal_segment_open, .segment_close = wal_segment_close) is the default
     * local-read routine; logical.c only ever forwards the default handle.
     */
    let mut ctx = backend_replication_logical_logical::CreateInitDecodingContext(
        Some(plugin),
        Default::default(), // NIL output plugin options
        false,              // just catalogs is OK (need_full_snapshot = false)
        restart_lsn,
        Default::default(), // XL_ROUTINE local-read routine
        false,              // prepare_write = NULL
        false,              // do_write = NULL
        false,              // update_progress = NULL
        wal_level_logical(),
        xlog::wal_segment_size::call(),
        smallinit::my_database_id::call(),
    )?;

    /*
     * If caller needs us to determine the decoding start point, do so now.
     * This might take a while.
     */
    if find_startpoint {
        backend_replication_logical_logical::DecodingContextFindStartpoint(&mut ctx)?;
    }

    /* don't need the decoding context anymore */
    backend_replication_logical_logical::FreeDecodingContext(&mut ctx)?;

    Ok(())
}

/*
 * SQL function for creating a new logical replication slot.
 */
/// `pg_create_logical_replication_slot(name, plugin, temporary, two_phase,
/// failover)`.
pub fn pg_create_logical_replication_slot(
    mcx: Mcx<'_>,
    name: &str,
    plugin: &str,
    temporary: bool,
    two_phase: bool,
    failover: bool,
) -> PgResult<SlotNameLsnRow> {
    // get_call_result_type assertion elided (see above).

    slot::CheckSlotPermissions(mcx, miscinit::get_user_id::call())?;

    backend_replication_logical_logical::CheckLogicalDecodingRequirements(
        wal_level_logical(),
        smallinit::my_database_id::call(),
    )?;

    create_logical_replication_slot(name, plugin, temporary, two_phase, failover, 0, true)?;

    // values[0] = NameGetDatum(&MyReplicationSlot->data.name);
    // values[1] = LSNGetDatum(MyReplicationSlot->data.confirmed_flush);
    // memset(nulls, 0, sizeof(nulls));
    let result = SlotNameLsnRow {
        slot_name: slot::my_slot_name(),
        lsn: Some(slot::my_slot_confirmed_flush()),
    };

    /* ok, slot is now fully created, mark it as persistent if needed */
    if !temporary {
        slot::ReplicationSlotPersist()?;
    }
    slot::ReplicationSlotRelease()?;

    Ok(result)
}

// ---------------------------------------------------------------------------
// pg_drop_replication_slot
// ---------------------------------------------------------------------------

/*
 * SQL function for dropping a replication slot.
 */
/// `pg_drop_replication_slot(name)`.
pub fn pg_drop_replication_slot(mcx: Mcx<'_>, name: &str) -> PgResult<()> {
    slot::CheckSlotPermissions(mcx, miscinit::get_user_id::call())?;

    slot::CheckSlotRequirements(wal_level_int())?;

    slot::ReplicationSlotDrop(name, true)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// pg_get_replication_slots
// ---------------------------------------------------------------------------

/*
 * pg_get_replication_slots - SQL SRF showing all replication slots
 * that currently exist on the database cluster.
 */
/// `pg_get_replication_slots()` — the 20-column SRF. Rows are pushed through
/// `materialized_srf_putvalues`, matching the C `tuplestore_putvalues`; the
/// return value mirrors the C `(Datum) 0`.
pub fn pg_get_replication_slots<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let _ = mcx;
    let max_slot_wal_keep_size_mb = xlog::max_slot_wal_keep_size_mb::call();
    let wal_keep_size_mb = xlog::wal_keep_size_mb::call();
    let wal_segment_size = xlog::wal_segment_size::call();

    /*
     * We don't require any special permission to see this function's data
     * because nothing should be sensitive. The most critical being the slot
     * name, which shouldn't contain anything particularly sensitive.
     */

    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;

    let currlsn = xlog::get_xlog_write_rec_ptr::call();

    /*
     * LWLockAcquire(ReplicationSlotControlLock, LW_SHARED); the locked walk over
     * the slot array (each in-use slot copied under its per-slot spinlock) is
     * performed by the slot owner, which returns the snapshots in slot-array
     * order along with their array index, then releases the control lock.
     */
    let slots = slot::snapshot_all_slots()?;

    for slot_contents in slots.iter() {
        // `if (!slot->in_use) continue;` was applied by the snapshot walk.
        let slotno = slot_contents.slotno;

        // memset(values, 0, sizeof(values)); memset(nulls, 0, sizeof(nulls));
        let mut values: [Datum<'mcx>; PG_GET_REPLICATION_SLOTS_COLS] =
            core::array::from_fn(|_| Datum::null());
        let mut nulls = [false; PG_GET_REPLICATION_SLOTS_COLS];
        let mut i = 0usize;

        // values[i++] = NameGetDatum(&slot_contents.data.name);
        values[i] = name_datum(mcx, &slot_contents.data.name)?;
        i += 1;

        if slot_contents.data.database == InvalidOid {
            nulls[i] = true; // plugin
            i += 1;
        } else {
            values[i] = name_datum(mcx, &slot_contents.data.plugin)?;
            i += 1;
        }

        if slot_contents.data.database == InvalidOid {
            values[i] = funcapi::cstring_get_text_datum::call(mcx, "physical")?;
            i += 1;
        } else {
            values[i] = funcapi::cstring_get_text_datum::call(mcx, "logical")?;
            i += 1;
        }

        if slot_contents.data.database == InvalidOid {
            nulls[i] = true; // datoid
            i += 1;
        } else {
            values[i] = Datum::from_oid(slot_contents.data.database);
            i += 1;
        }

        values[i] = Datum::from_bool(
            slot_contents.data.persistency == ReplicationSlotPersistency::RS_TEMPORARY,
        );
        i += 1;
        values[i] = Datum::from_bool(slot_contents.active_pid != 0);
        i += 1;

        if slot_contents.active_pid != 0 {
            values[i] = Datum::from_i32(slot_contents.active_pid);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        if slot_contents.data.xmin != 0 {
            values[i] = Datum::from_transaction_id(slot_contents.data.xmin);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        if slot_contents.data.catalog_xmin != 0 {
            values[i] = Datum::from_transaction_id(slot_contents.data.catalog_xmin);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        if slot_contents.data.restart_lsn != 0 {
            values[i] = Datum::from_u64(slot_contents.data.restart_lsn);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        if slot_contents.data.confirmed_flush != 0 {
            values[i] = Datum::from_u64(slot_contents.data.confirmed_flush);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        /*
         * If the slot has not been invalidated, test availability from
         * restart_lsn.
         */
        let mut walstate =
            if slot_contents.data.invalidated != ReplicationSlotInvalidationCause::RS_INVAL_NONE {
                WALAvailability::Removed
            } else {
                xlog::get_wal_availability::call(slot_contents.data.restart_lsn)
            };

        // safe_wal_size below reads slot_contents.data.restart_lsn; the
        // WALAVAIL_REMOVED branch may refresh it from a second spinlocked read.
        let mut restart_lsn = slot_contents.data.restart_lsn;

        match walstate {
            WALAvailability::InvalidLsn => {
                nulls[i] = true; // wal_status
                i += 1;
            }
            WALAvailability::Reserved => {
                values[i] = funcapi::cstring_get_text_datum::call(mcx, "reserved")?;
                i += 1;
            }
            WALAvailability::Extended => {
                values[i] = funcapi::cstring_get_text_datum::call(mcx, "extended")?;
                i += 1;
            }
            WALAvailability::Unreserved => {
                values[i] = funcapi::cstring_get_text_datum::call(mcx, "unreserved")?;
                i += 1;
            }
            WALAvailability::Removed => {
                /*
                 * If we read the restart_lsn long enough ago, maybe that file
                 * has been removed by now.  However, the walsender could have
                 * moved forward enough that it jumped to another file after
                 * we looked.  If checkpointer signalled the process to
                 * termination, then it's definitely lost; but if a process is
                 * still alive, then "unreserved" seems more appropriate.
                 *
                 * If we do change it, save the state for safe_wal_size below.
                 */
                let mut done = false;
                if !xlog_rec_ptr_is_invalid(slot_contents.data.restart_lsn) {
                    let (pid, reread_restart_lsn) =
                        slot::reread_slot_active_pid_and_restart_lsn(slotno);
                    restart_lsn = reread_restart_lsn;
                    if pid != 0 {
                        values[i] = funcapi::cstring_get_text_datum::call(mcx, "unreserved")?;
                        i += 1;
                        walstate = WALAvailability::Unreserved;
                        done = true;
                    }
                }
                if !done {
                    values[i] = funcapi::cstring_get_text_datum::call(mcx, "lost")?;
                    i += 1;
                }
            }
        }

        /*
         * safe_wal_size is only computed for slots that have not been lost,
         * and only if there's a configured maximum size.
         */
        if walstate == WALAvailability::Removed || max_slot_wal_keep_size_mb < 0 {
            nulls[i] = true;
            i += 1;
        } else {
            let target_seg = xlbyte_to_seg(restart_lsn, wal_segment_size);

            /* determine how many segments can be kept by slots */
            let slot_keep_segs = xlog_mb_var_to_segs(max_slot_wal_keep_size_mb, wal_segment_size);
            /* ditto for wal_keep_size */
            let keep_segs = xlog_mb_var_to_segs(wal_keep_size_mb, wal_segment_size);

            /* if currpos reaches failLSN, we lose our segment */
            let fail_seg = target_seg + max(slot_keep_segs, keep_segs) + 1;
            let fail_lsn = xlog_segno_offset_to_rec_ptr(fail_seg, 0, wal_segment_size);

            values[i] = Datum::from_i64(fail_lsn.wrapping_sub(currlsn) as i64);
            i += 1;
        }

        values[i] = Datum::from_bool(slot_contents.data.two_phase);
        i += 1;

        if slot_contents.data.two_phase
            && !xlog_rec_ptr_is_invalid(slot_contents.data.two_phase_at)
        {
            values[i] = Datum::from_u64(slot_contents.data.two_phase_at);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        if slot_contents.inactive_since > 0 {
            // TimestampTzGetDatum(slot_contents.inactive_since) — a by-value int8.
            values[i] = Datum::from_i64(slot_contents.inactive_since);
            i += 1;
        } else {
            nulls[i] = true;
            i += 1;
        }

        let cause = slot_contents.data.invalidated;

        if slot::snapshot_is_physical(slot_contents) {
            nulls[i] = true; // conflicting
            i += 1;
        } else {
            /*
             * rows_removed and wal_level_insufficient are the only two
             * reasons for the logical slot's conflict with recovery.
             */
            if cause == ReplicationSlotInvalidationCause::RS_INVAL_HORIZON
                || cause == ReplicationSlotInvalidationCause::RS_INVAL_WAL_LEVEL
            {
                values[i] = Datum::from_bool(true);
                i += 1;
            } else {
                values[i] = Datum::from_bool(false);
                i += 1;
            }
        }

        if cause == ReplicationSlotInvalidationCause::RS_INVAL_NONE {
            nulls[i] = true; // invalidation_reason
            i += 1;
        } else {
            values[i] = funcapi::cstring_get_text_datum::call(
                mcx,
                &slot::GetSlotInvalidationCauseName(cause),
            )?;
            i += 1;
        }

        values[i] = Datum::from_bool(slot_contents.data.failover);
        i += 1;

        values[i] = Datum::from_bool(slot_contents.data.synced != 0);
        i += 1;

        // Assert(i == PG_GET_REPLICATION_SLOTS_COLS);
        debug_assert_eq!(i, PG_GET_REPLICATION_SLOTS_COLS);

        // tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
        let rsinfo = fcinfo
            .resultinfo
            .as_mut()
            .expect("InitMaterializedSRF set fcinfo->resultinfo");
        funcapi::materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;
    }

    // LWLockRelease(ReplicationSlotControlLock); — released inside the walk.

    // return (Datum) 0;
    Ok(Datum::null())
}

/// `namein(s)` / `NameGetDatum` — a 64-byte NUL-padded `NameData` by-reference
/// Datum image (the C `Name` is a pointer to a fixed-length `NameData`).
fn name_datum<'mcx>(mcx: Mcx<'mcx>, nd: &NameData) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, &nd.data)?))
}

// ---------------------------------------------------------------------------
// slot advance
// ---------------------------------------------------------------------------

/*
 * Helper function for advancing our physical replication slot forward.
 *
 * The LSN position to move to is compared simply to the slot's restart_lsn,
 * knowing that any position older than that would be removed by successive
 * checkpoints.
 */
fn pg_physical_replication_slot_advance(moveto: XLogRecPtr) -> PgResult<XLogRecPtr> {
    let startlsn = slot::my_slot_restart_lsn();
    let mut retlsn = startlsn;

    debug_assert!(moveto != 0);

    if startlsn < moveto {
        slot::set_my_slot_restart_lsn_locked(moveto);
        retlsn = moveto;

        /*
         * Dirty the slot so as it is written out at the next checkpoint. Note
         * that the LSN position advanced may still be lost in the event of a
         * crash, but this makes the data consistent after a clean shutdown.
         */
        slot::ReplicationSlotMarkDirty();

        /*
         * Wake up logical walsenders holding logical failover slots after
         * updating the restart_lsn of the physical slot.
         */
        backend_replication_walsender::PhysicalWakeupLogicalWalSnd();
    }

    Ok(retlsn)
}

/*
 * Advance our logical replication slot forward. See
 * LogicalSlotAdvanceAndCheckSnapState for details.
 */
fn pg_logical_replication_slot_advance(moveto: XLogRecPtr) -> PgResult<XLogRecPtr> {
    backend_replication_logical_logical::LogicalSlotAdvanceAndCheckSnapState(
        moveto,
        None,
        xlog::wal_segment_size::call(),
        smallinit::my_database_id::call(),
    )
}

/*
 * SQL function for moving the position in a replication slot.
 */
/// `pg_replication_slot_advance(slot_name, upto_lsn)`.
pub fn pg_replication_slot_advance(
    mcx: Mcx<'_>,
    slotname: &str,
    moveto: XLogRecPtr,
) -> PgResult<SlotNameLsnRow> {
    let mut moveto = moveto;

    debug_assert!(!slot::my_replication_slot_is_set());

    slot::CheckSlotPermissions(mcx, miscinit::get_user_id::call())?;

    if xlog_rec_ptr_is_invalid(moveto) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("invalid target WAL LSN")
            .into_error());
    }

    // get_call_result_type assertion elided (see above).

    /*
     * We can't move slot past what's been flushed/replayed so clamp the
     * target position accordingly.
     */
    if !xlog::recovery_in_progress::call() {
        moveto = min(moveto, xlog::get_flush_rec_ptr::call().0);
    } else {
        moveto = min(moveto, xlog::get_xlog_replay_rec_ptr::call());
    }

    /* Acquire the slot so we "own" it */
    slot::ReplicationSlotAcquire(slotname, true, true)?;

    /* A slot whose restart_lsn has never been reserved cannot be advanced */
    if xlog_rec_ptr_is_invalid(slot::my_slot_restart_lsn()) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "replication slot \"{slotname}\" cannot be advanced"
            ))
            .errdetail("This slot has never previously reserved WAL, or it has been invalidated.")
            .into_error());
    }

    /*
     * Check if the slot is not moving backwards.  Physical slots rely simply
     * on restart_lsn as a minimum point, while logical slots have confirmed
     * consumption up to confirmed_flush, meaning that in both cases data
     * older than that is not available anymore.
     */
    let minlsn = if OidIsValid(slot::my_slot_database()) {
        slot::my_slot_confirmed_flush()
    } else {
        slot::my_slot_restart_lsn()
    };

    if moveto < minlsn {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot advance replication slot to {}, minimum is {}",
                lsn_format(moveto),
                lsn_format(minlsn)
            ))
            .into_error());
    }

    /* Do the actual slot update, depending on the slot type */
    let endlsn = if OidIsValid(slot::my_slot_database()) {
        pg_logical_replication_slot_advance(moveto)?
    } else {
        pg_physical_replication_slot_advance(moveto)?
    };

    // values[0] = NameGetDatum(&MyReplicationSlot->data.name);
    let slot_name = slot::my_slot_name();

    /*
     * Recompute the minimum LSN and xmin across all slots to adjust with the
     * advancing potentially done.
     */
    slot::ReplicationSlotsComputeRequiredXmin(false)?;
    slot::ReplicationSlotsComputeRequiredLSN()?;

    slot::ReplicationSlotRelease()?;

    /* Return the reached position. */
    Ok(SlotNameLsnRow {
        slot_name,
        lsn: Some(endlsn),
    })
}

// ---------------------------------------------------------------------------
// copy_replication_slot + wrappers
// ---------------------------------------------------------------------------

/*
 * Helper function of copying a replication slot.
 *
 * The optional third (`temporary`) and fourth (`plugin`) arguments correspond
 * to the C `PG_NARGS()`-gated `PG_GETARG_*` reads; `None` here means the
 * argument was not supplied.
 */
fn copy_replication_slot(
    mcx: Mcx<'_>,
    src_name: &str,
    dst_name: &str,
    arg_temporary: Option<bool>,
    arg_plugin: Option<&str>,
    logical_slot: bool,
) -> PgResult<SlotNameLsnRow> {
    // get_call_result_type assertion elided (see above).

    slot::CheckSlotPermissions(mcx, miscinit::get_user_id::call())?;

    if logical_slot {
        backend_replication_logical_logical::CheckLogicalDecodingRequirements(
            wal_level_logical(),
            smallinit::my_database_id::call(),
        )?;
    } else {
        slot::CheckSlotRequirements(wal_level_int())?;
    }

    /*
     * We need to prevent the source slot's reserved WAL from being removed,
     * but we don't want to lock that slot for very long, and it can advance
     * in the meantime.  So obtain the source slot's data, and create a new
     * slot using its restart_lsn.  Afterwards we lock the source slot again
     * and verify that the data we copied (name, type) has not changed
     * incompatibly.  No inconvenient WAL removal can occur once the new slot
     * is created -- but since WAL removal could have occurred before we
     * managed to create the new slot, we advance the new slot's restart_lsn
     * to the source slot's updated restart_lsn the second time we lock it.
     *
     * The slot owner performs the LWLockAcquire(ReplicationSlotControlLock,
     * LW_SHARED) walk that finds the matching in-use slot, copies its contents
     * under the per-slot spinlock, then releases the control lock.
     */
    let first_slot_contents: SlotSnapshot = match slot::snapshot_slot_by_name(src_name)? {
        Some(c) => c,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("replication slot \"{src_name}\" does not exist"))
                .into_error());
        }
    };

    let src_islogical = slot::snapshot_is_logical(&first_slot_contents);
    let src_restart_lsn = first_slot_contents.data.restart_lsn;
    let mut temporary =
        first_slot_contents.data.persistency == ReplicationSlotPersistency::RS_TEMPORARY;
    // plugin = logical_slot ? NameStr(first_slot_contents.data.plugin) : NULL;
    let mut plugin: Option<String> = if logical_slot {
        Some(name_str(&first_slot_contents.data.plugin))
    } else {
        None
    };

    /* Check type of replication slot */
    if src_islogical != logical_slot {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(if src_islogical {
                format!(
                    "cannot copy physical replication slot \"{src_name}\" as a logical replication slot"
                )
            } else {
                format!(
                    "cannot copy logical replication slot \"{src_name}\" as a physical replication slot"
                )
            })
            .into_error());
    }

    /* Copying non-reserved slot doesn't make sense */
    if xlog_rec_ptr_is_invalid(src_restart_lsn) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot copy a replication slot that doesn't reserve WAL")
            .into_error());
    }

    /* Cannot copy an invalidated replication slot */
    if first_slot_contents.data.invalidated != ReplicationSlotInvalidationCause::RS_INVAL_NONE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot copy invalidated replication slot \"{src_name}\""
            ))
            .into_error());
    }

    /* Overwrite params from optional arguments */
    if let Some(t) = arg_temporary {
        // if (PG_NARGS() >= 3) temporary = PG_GETARG_BOOL(2);
        temporary = t;
    }
    if let Some(p) = arg_plugin {
        // if (PG_NARGS() >= 4) { Assert(logical_slot); plugin = NameStr(*(PG_GETARG_NAME(3))); }
        debug_assert!(logical_slot);
        plugin = Some(p.to_string());
    }

    /* Create new slot and acquire it */
    if logical_slot {
        /*
         * We must not try to read WAL, since we haven't reserved it yet --
         * hence pass find_startpoint false.  confirmed_flush will be set
         * below, by copying from the source slot.
         *
         * We don't copy the failover option to prevent potential issues with
         * slot synchronization.
         */
        create_logical_replication_slot(
            dst_name,
            plugin.as_deref().unwrap_or(""),
            temporary,
            false,
            false,
            src_restart_lsn,
            false,
        )?;
    } else {
        create_physical_replication_slot(dst_name, true, temporary, src_restart_lsn)?;
    }

    /*
     * Update the destination slot to current values of the source slot;
     * recheck that the source slot is still the one we saw previously.
     */
    {
        /* Copy data of source slot again */
        let second_slot_contents = slot::reread_slot_snapshot(first_slot_contents.slotno);

        let copy_effective_xmin = second_slot_contents.effective_xmin;
        let copy_effective_catalog_xmin = second_slot_contents.effective_catalog_xmin;

        let copy_xmin = second_slot_contents.data.xmin;
        let copy_catalog_xmin = second_slot_contents.data.catalog_xmin;
        let copy_restart_lsn = second_slot_contents.data.restart_lsn;
        let copy_confirmed_flush = second_slot_contents.data.confirmed_flush;

        /* for existence check */
        let copy_name = name_str(&second_slot_contents.data.name);
        let copy_islogical = slot::snapshot_is_logical(&second_slot_contents);

        /*
         * Check if the source slot still exists and is valid. We regard it as
         * invalid if the type of replication slot or name has been changed,
         * or the restart_lsn either is invalid or has gone backward.
         *
         * Since erroring out will release and drop the destination slot we
         * don't need to release it here.
         */
        if copy_restart_lsn < src_restart_lsn
            || src_islogical != copy_islogical
            || copy_name != src_name
        {
            return Err(ereport(ERROR)
                .errmsg(format!("could not copy replication slot \"{src_name}\""))
                .errdetail(
                    "The source replication slot was modified incompatibly during the copy operation.",
                )
                .into_error());
        }

        /* The source slot must have a consistent snapshot */
        if src_islogical && xlog_rec_ptr_is_invalid(copy_confirmed_flush) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot copy unfinished logical replication slot \"{src_name}\""
                ))
                .errhint("Retry when the source replication slot's confirmed_flush_lsn is valid.")
                .into_error());
        }

        /*
         * Copying an invalid slot doesn't make sense. Note that the source
         * slot can become invalid after we create the new slot and copy the
         * data of source slot.
         */
        if second_slot_contents.data.invalidated != ReplicationSlotInvalidationCause::RS_INVAL_NONE {
            return Err(ereport(ERROR)
                .errmsg(format!("cannot copy replication slot \"{src_name}\""))
                .errdetail("The source replication slot was invalidated during the copy operation.")
                .into_error());
        }

        /* Install copied values again */
        slot::install_my_slot_copied_values(&CopiedSlotValues {
            effective_xmin: copy_effective_xmin,
            effective_catalog_xmin: copy_effective_catalog_xmin,
            xmin: copy_xmin,
            catalog_xmin: copy_catalog_xmin,
            restart_lsn: copy_restart_lsn,
            confirmed_flush: copy_confirmed_flush,
        });

        slot::ReplicationSlotMarkDirty();
        slot::ReplicationSlotsComputeRequiredXmin(false)?;
        slot::ReplicationSlotsComputeRequiredLSN()?;
        slot::ReplicationSlotSave()?;

        // #ifdef USE_ASSERT_CHECKING
        // { XLByteToSeg(copy_restart_lsn, segno, wal_segment_size);
        //   Assert(XLogGetLastRemovedSegno() < segno); }
        #[cfg(debug_assertions)]
        {
            let wal_segment_size = xlog::wal_segment_size::call();
            let segno = xlbyte_to_seg(copy_restart_lsn, wal_segment_size);
            debug_assert!(xlog::xlog_get_last_removed_segno::call() < segno);
        }
    }

    /* target slot fully created, mark as persistent if needed */
    if logical_slot && !temporary {
        slot::ReplicationSlotPersist()?;
    }

    /* All done.  Set up the return values */
    // values[0] = NameGetDatum(dst_name);
    let confirmed_flush = slot::my_slot_confirmed_flush();
    let result = SlotNameLsnRow {
        slot_name: namedata_from_str(dst_name),
        lsn: if !xlog_rec_ptr_is_invalid(confirmed_flush) {
            Some(confirmed_flush)
        } else {
            None
        },
    };

    slot::ReplicationSlotRelease()?;

    Ok(result)
}

/* The wrappers below are all to appease opr_sanity */

/// `pg_copy_logical_replication_slot(src, dst)` — 2-arg form.
pub fn pg_copy_logical_replication_slot_a(
    mcx: Mcx<'_>,
    src_name: &str,
    dst_name: &str,
) -> PgResult<SlotNameLsnRow> {
    copy_replication_slot(mcx, src_name, dst_name, None, None, true)
}

/// `pg_copy_logical_replication_slot(src, dst, temporary)` — 3-arg form.
pub fn pg_copy_logical_replication_slot_b(
    mcx: Mcx<'_>,
    src_name: &str,
    dst_name: &str,
    temporary: bool,
) -> PgResult<SlotNameLsnRow> {
    copy_replication_slot(mcx, src_name, dst_name, Some(temporary), None, true)
}

/// `pg_copy_logical_replication_slot(src, dst, temporary, plugin)` — 4-arg form.
pub fn pg_copy_logical_replication_slot_c(
    mcx: Mcx<'_>,
    src_name: &str,
    dst_name: &str,
    temporary: bool,
    plugin: &str,
) -> PgResult<SlotNameLsnRow> {
    copy_replication_slot(mcx, src_name, dst_name, Some(temporary), Some(plugin), true)
}

/// `pg_copy_physical_replication_slot(src, dst)` — 2-arg form.
pub fn pg_copy_physical_replication_slot_a(
    mcx: Mcx<'_>,
    src_name: &str,
    dst_name: &str,
) -> PgResult<SlotNameLsnRow> {
    copy_replication_slot(mcx, src_name, dst_name, None, None, false)
}

/// `pg_copy_physical_replication_slot(src, dst, temporary)` — 3-arg form.
pub fn pg_copy_physical_replication_slot_b(
    mcx: Mcx<'_>,
    src_name: &str,
    dst_name: &str,
    temporary: bool,
) -> PgResult<SlotNameLsnRow> {
    copy_replication_slot(mcx, src_name, dst_name, Some(temporary), None, false)
}

// ---------------------------------------------------------------------------
// pg_sync_replication_slots
// ---------------------------------------------------------------------------

/*
 * Synchronize failover enabled replication slots to a standby server
 * from the primary server.
 */
/// `pg_sync_replication_slots()`.
pub fn pg_sync_replication_slots(mcx: Mcx<'_>) -> PgResult<()> {
    slot::CheckSlotPermissions(mcx, miscinit::get_user_id::call())?;

    if !xlog::recovery_in_progress::call() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("replication slots can only be synchronized to a standby server")
            .into_error());
    }

    // ValidateSlotSyncParams(ERROR);
    slotsync::validate_slot_sync_params::call(ERROR.0 as i32)?;

    /* Load the libpq-specific functions */
    dfmgr::load_file::call("libpqwalreceiver", false)?;

    let _ = slotsync::check_and_get_dbname_from_conninfo::call()?;

    // initStringInfo(&app_name);
    // if (cluster_name[0]) appendStringInfo(&app_name, "%s_slotsync", cluster_name);
    // else appendStringInfoString(&app_name, "slotsync");
    let cluster_name = guc::cluster_name::call();
    let app_name: String = if !cluster_name.is_empty() {
        format!("{cluster_name}_slotsync")
    } else {
        "slotsync".to_string()
    };

    /* Connect to the primary server. */
    let primary_conninfo = xlogrecovery::primary_conninfo::call(mcx)?;
    let wrconn = match walreceiver::walrcv_connect::call(
        primary_conninfo.as_str().to_string(),
        false,
        false,
        false,
        app_name.clone(),
    ) {
        Ok(handle) => handle,
        Err(err) => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg(format!(
                    "synchronization worker \"{app_name}\" could not connect to the primary server: {err}"
                ))
                .into_error());
        }
    };

    // pfree(app_name.data); -- nothing to free in this model

    slotsync::sync_replication_slots::call(wrconn)?;

    walreceiver::walrcv_disconnect::call(wrconn);

    Ok(())
}

// ---------------------------------------------------------------------------
// `Name` helpers
// ---------------------------------------------------------------------------

/// `NameStr(name)` — the bytes up to the first NUL, as a `String`.
fn name_str(nd: &NameData) -> String {
    String::from_utf8_lossy(nd.name_str()).into_owned()
}

/// Build a `NameData` (NUL-padded, truncated to `NAMEDATALEN-1`) from a name.
fn namedata_from_str(s: &str) -> NameData {
    let mut nd = NameData::default();
    nd.namestrcpy(s);
    nd
}

// ---------------------------------------------------------------------------
// fmgr builtin registration
// ---------------------------------------------------------------------------

mod fmgr_builtins;

/// Register the `slotfuncs.c` fmgr builtins (so `fmgr_isbuiltin` / by-OID
/// dispatch resolves them). This crate owns no inward seam crate, so this is the
/// only initialization it performs; `seams-init::init_all` calls it.
pub fn init_seams() {
    fmgr_builtins::register_slotfuncs_builtins();
}

#[cfg(test)]
mod tests;
