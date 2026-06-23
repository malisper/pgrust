//! `backend/commands/constraint.c` — PostgreSQL CONSTRAINT support code
//! (PostgreSQL 18.3).
//!
//! The single function this file defines, [`unique_key_recheck`], is the AFTER
//! ROW trigger that performs a *deferred* uniqueness or exclusion-constraint
//! check.  It is queued (as an after-trigger event) whenever a row is inserted
//! or updated through a deferrable unique / exclusion index, and fires at
//! end-of-statement, commit time, or on `SET CONSTRAINTS`.  It re-fetches the
//! row that was inserted/updated, skips the check if that row (and any live HOT
//! child) is now dead, otherwise rebuilds the index values and re-runs the
//! constraint check that was deferred at insert time:
//!
//!   * for a unique constraint: `index_insert(..., UNIQUE_CHECK_EXISTING)` — not
//!     a real insert, a re-verification that the already-present index entry is
//!     unique;
//!   * for an exclusion constraint: `check_exclusion_constraint(...)` (the
//!     ported `execIndexing.c` routine), now permitted to throw.
//!
//! # Ported substrate driven directly (acyclic)
//!
//! `unique_key_recheck` drives the *ported* table-AM (`table_slot_create`,
//! `table_index_fetch_{begin,tuple,end}`), index-AM (`index_open`,
//! `index_insert`, `index_insert_cleanup`, `index_close`), executor state
//! (`CreateExecutorState` / `FreeExecutorState` /
//! `GetPerTupleExprContext`), and `execIndexing.c`'s
//! `check_exclusion_constraint`, plus `catalog/index.c`'s `BuildIndexInfo` /
//! `FormIndexDatum` (through the `backend-catalog-index-seams` slots their owner
//! installs).
//!
//! # The genuine boundary: the live trigger carriers
//!
//! Unlike `ri_triggers.c` (which only reads scalars off the trigger relation),
//! `unique_key_recheck` needs the *live* `tg_relation` `Relation` and the
//! inserted/updated tuple's TID (from `tg_trigslot`/`tg_newslot`), plus the
//! constraint's index OID (`tg_trigger->tgconstrindid`).  Those carriers are
//! owned by the per-row AFTER-trigger firing substrate (`AfterTriggerExecute`
//! re-resolves the `Relation` and materializes the OLD/NEW slots), which is not
//! yet ported: the firing engine currently builds the `TriggerData` with
//! `tg_relation`/`tg_trigslot`/`tg_newslot` left NULL and loud-panics on the
//! per-row tuple fetch.  This crate therefore reaches them through the
//! `commands/trigger.c` owner's seam crate
//! ([`trigger_seams`]) — `called_as_trigger` / `tg_event` /
//! `tg_trigger` / `tg_trigslot` / `tg_newslot` / `slot_tid` /
//! `trigger_constrindid` / `tg_relation` — each of which panics until the
//! firing substrate lands (mirror-PG-and-panic).  No fmgr builtin registry is
//! added here, matching this tree's adt/trigger crates (`lsn-trigfuncs`,
//! `ri-triggers`), which expose the trigger core rather than a V1 dispatch row.
//!
//! # EState lifetime note
//!
//! C creates the throwaway `EState` only when the index has expressions or
//! exclusion ops, and `FormIndexDatum` accepts a NULL `estate` otherwise.  In
//! the owned model `FormIndexDatum` needs a live `&mut EStateData` (the slot it
//! reads is addressed by id in the EState's tuple-table pool), so the EState is
//! created up front and the per-tuple `ExprContext`/`ecxt_scantuple` is wired
//! only on the expression/exclusion path, exactly matching C's conditional.  The
//! standalone slot C frees with `ExecDropSingleTupleTableSlot` lives in the
//! EState's tuple-table pool and is reclaimed by `FreeExecutorState` (the same
//! compromise the ported `execIndexing` `check_exclusion_or_unique_constraint`
//! makes for its existing-tuple slot).
//!
//! C source: `constraint.c:38-206`.

#![allow(non_snake_case)]
// `PgError` is the large workspace-wide error type; boxing it would diverge from
// every sibling crate's `Result` shape.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;
use alloc::string::String;

use ::mcx::MemoryContext;

use ::types_core::Oid;
use ::datum::Datum;
use types_error::{PgResult, ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED, ERROR};
use ::nodes::execnodes::SlotId;
use ::snapshot::snapshot::{SnapshotData, SnapshotType};
use ::types_storage::lock::LOCKMODE;
use ::types_tableam::amapi::IndexUniqueCheck;
use ::types_tableam::index_info_carrier::IndexInfoCarrier;
use ::types_tuple::heaptuple::Datum as DatumV;
use ::types_tuple::heaptuple::ItemPointerData;
use ::types_ri_triggers::TriggerDataRef;

use ::utils_error::ereport;

use indexam as indexam;
use table_tableam as tableam;
use execIndexing as execIndexing;
use execUtils as execUtils;

use index_seams as index_seams;
use trigger_seams as trigger;
use execTuples_seams as execTuples_seams;

mod fmgr_builtins;

/// Register this crate's fmgr built-in (`unique_key_recheck`, OID 1250). This
/// crate installs no cross-crate seams; the name `init_seams` matches the
/// workspace convention so `seams-init` can aggregate it with one line.
pub fn init_seams() {
    fmgr_builtins::register_constraint_builtins();
}

// ---------------------------------------------------------------------------
// Trigger event bit predicates (commands/trigger.h). These mirror the C macros
// exactly; they operate on the `TriggerEvent` (`uint32`) from `tg_event`.
// ---------------------------------------------------------------------------

/// `TRIGGER_EVENT_INSERT`.
const TRIGGER_EVENT_INSERT: u32 = 0x0000_0000;
/// `TRIGGER_EVENT_UPDATE`.
const TRIGGER_EVENT_UPDATE: u32 = 0x0000_0002;
/// `TRIGGER_EVENT_OPMASK`.
const TRIGGER_EVENT_OPMASK: u32 = 0x0000_0003;
/// `TRIGGER_EVENT_ROW`.
const TRIGGER_EVENT_ROW: u32 = 0x0000_0004;
/// `TRIGGER_EVENT_BEFORE`.
const TRIGGER_EVENT_BEFORE: u32 = 0x0000_0008;
/// `TRIGGER_EVENT_AFTER`.
const TRIGGER_EVENT_AFTER: u32 = 0x0000_0000;
/// `TRIGGER_EVENT_TIMINGMASK`.
const TRIGGER_EVENT_TIMINGMASK: u32 = 0x0000_0018;

#[inline]
fn TRIGGER_FIRED_BY_INSERT(event: u32) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_INSERT
}
#[inline]
fn TRIGGER_FIRED_BY_UPDATE(event: u32) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_UPDATE
}
#[inline]
fn TRIGGER_FIRED_FOR_ROW(event: u32) -> bool {
    (event & TRIGGER_EVENT_ROW) != 0
}
#[inline]
fn TRIGGER_FIRED_AFTER(event: u32) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_AFTER
}

// `TRIGGER_FIRED_BEFORE` is part of the trigger-event macro family
// `constraint.c`'s header pulls in; it is unused by `unique_key_recheck` itself
// but kept for completeness of the ported macro set.
#[allow(dead_code)]
#[inline]
fn TRIGGER_FIRED_BEFORE(event: u32) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_BEFORE
}

/// `RowExclusiveLock` (`storage/lockdefs.h`) — lock mode 3.
const ROW_EXCLUSIVE_LOCK: LOCKMODE = 3;

/// `INDEX_MAX_KEYS` as a `usize`, for the `values`/`isnull` index arrays.
const INDEX_MAX_KEYS: usize = ::types_core::fmgr::INDEX_MAX_KEYS as usize;

/// `unique_key_recheck` (constraint.c:38) — trigger function to do a deferred
/// uniqueness check (this now also does deferred exclusion-constraint checks, so
/// the name is somewhat historical).
///
/// Invoked as an AFTER ROW trigger for both INSERT and UPDATE, for any rows
/// recorded as potentially violating a deferrable unique or exclusion
/// constraint.  May be an end-of-statement check, a commit-time check, or a
/// check triggered by a `SET CONSTRAINTS` command.
///
/// `trigdata` is the `(TriggerData *) fcinfo->context`; `called_as_trigger` is
/// the `CALLED_AS_TRIGGER(fcinfo)` test (both supplied by the trigger manager
/// through the trigger seams).  `parent` is the `CurrentMemoryContext` at the
/// trigger call (the parent of the throwaway `EState`'s per-query context).
///
/// On success returns `PointerGetDatum(NULL)` ([`Datum::null`]), exactly as C
/// returns from both the early dead-row skip and the normal completion path.
pub fn unique_key_recheck(
    parent: &MemoryContext,
    called_as_trigger: bool,
    trigdata: TriggerDataRef,
) -> PgResult<Datum> {
    const FUNCNAME: &str = "unique_key_recheck";

    // Make sure this is being called as an AFTER ROW trigger.  Note: translatable
    // error strings are shared with ri_triggers.c, so resist the temptation to
    // fold the function name into them.
    if !called_as_trigger {
        return Err(trigger_protocol_error(format!(
            "function \"{FUNCNAME}\" was not called by trigger manager"
        )));
    }

    let tg_event = trigger::tg_event::call(trigdata);

    if !TRIGGER_FIRED_AFTER(tg_event) || !TRIGGER_FIRED_FOR_ROW(tg_event) {
        return Err(trigger_protocol_error(format!(
            "function \"{FUNCNAME}\" must be fired AFTER ROW"
        )));
    }

    // Get the new data that was inserted/updated.  `checktid` is the TID of the
    // tuple whose deferred constraint we are checking.
    let checktid: ItemPointerData = if TRIGGER_FIRED_BY_INSERT(tg_event) {
        let slot = trigger::tg_trigslot::call(trigdata);
        trigger::slot_tid::call(slot)
    } else if TRIGGER_FIRED_BY_UPDATE(tg_event) {
        let slot = trigger::tg_newslot::call(trigdata);
        trigger::slot_tid::call(slot)
    } else {
        // The C emits the error here and (unreachably) sets checktid invalid to
        // keep the compiler quiet; the early return makes that dead store moot.
        return Err(trigger_protocol_error(format!(
            "function \"{FUNCNAME}\" must be fired for INSERT or UPDATE"
        )));
    };

    // `tgconstrindid` and the `TriggerData` handle are read before entering the
    // EState's context (they are plain scalars / a Copy handle).
    let constrindid: Oid = trigger::trigger_constrindid::call(trigger::tg_trigger::call(trigdata));

    // Create the throwaway EState now (see the module note on the EState
    // lifetime divergence): every relation/slot/index value below lives in the
    // EState's per-query memory context, so the table-AM and index-AM see a
    // single consistent `'mcx`.
    let mut estate = execUtils::CreateExecutorState(parent)?;

    let skipped = estate.with_mut(|estate| -> PgResult<Option<()>> {
        let mcx = estate.es_query_cxt;

        // The heap relation the trigger fired on (the C `trigdata->tg_relation`),
        // aliased into the EState's context.
        let heap = trigger::tg_relation::call(mcx, trigdata)?;

        // slot = table_slot_create(trigdata->tg_relation, NULL);  The standalone
        // slot lives in the EState's tuple-table pool (reclaimed by
        // FreeExecutorState; see the module note).
        let slot_data = tableam::table_slot_create(mcx, &heap)?;
        let slot: SlotId = estate.push_slot_data(slot_data)?;

        // If the row pointed at by checktid is now dead (ie, inserted and then
        // deleted within our transaction), we can skip the check.  However, we
        // have to be careful, because this trigger gets queued only in response
        // to index insertions; which means it does not get queued e.g. for HOT
        // updates.  The row we are called for might now be dead, but have a live
        // HOT child, in which case we still need to make the check ---
        // effectively, we're applying the check against the live child row,
        // although we can use the values from this row since by definition all
        // columns of interest to us are the same.
        //
        // This might look like just an optimization, because the index AM will
        // make this identical test before throwing an error.  But it's actually
        // needed for correctness, because the index AM will also throw an error
        // if it doesn't find the index entry for the row.  If the row's dead then
        // it's possible the index entry has also been marked dead, and even
        // removed.
        let mut tmptid: ItemPointerData = checktid;
        {
            let mut scan = tableam::table_index_fetch_begin(mcx, &heap)?;
            let mut call_again = false;
            // `mut` because table_index_fetch_tuple now takes `&mut` snapshot
            // (the dirty-snapshot output param). HeapTupleSatisfiesSelf does not
            // write xmin/xmax/speculativeToken, so this stays faithful.
            let mut snapshot_self = Some(SnapshotData::sentinel(SnapshotType::SNAPSHOT_SELF));

            // `&mut tmptid`: table_index_fetch_tuple mutates the tid in place to
            // the resolved live HOT-chain member's TID (heap's
            // ItemPointerSetOffsetNumber), exactly as C's
            // `table_index_fetch_tuple(..., &tmptid, ...)` does.
            let found = tableam::table_index_fetch_tuple(
                mcx,
                &mut scan,
                &mut tmptid,
                &mut snapshot_self,
                estate.slot_data_mut(slot),
                &mut call_again,
                None,
            )?;

            if !found {
                // All rows referenced by the index entry are dead, so skip the
                // check.  (ExecDropSingleTupleTableSlot is deferred to
                // FreeExecutorState; see the module note.)  Clear the slot first,
                // mirroring C's ExecDropSingleTupleTableSlot(slot): the standalone
                // slot lives in the EState tuple-table pool, and FreeExecutorState
                // does NOT release buffer pins, so the pin
                // table_index_fetch_tuple may have stored into the slot must be
                // released here or it leaks ("resource was not closed").
                tableam::table_index_fetch_end(scan)?;
                execTuples_seams::exec_clear_tuple::call(estate, slot)?;
                return Ok(None);
            }
            tableam::table_index_fetch_end(scan)?;
        }

        // Open the index, acquiring a RowExclusiveLock, just as if we were going
        // to update it.  (This protects against possible changes of the index
        // schema, not against concurrent updates.)
        let index_rel = indexam::index_open(mcx, constrindid, ROW_EXCLUSIVE_LOCK)?;
        let mut index_info = index_seams::build_index_info::call(mcx, &index_rel)?;

        // Typically the index won't have expressions, but if it does we need an
        // EState to evaluate them.  We need it for exclusion constraints too,
        // even if they are just on simple columns.
        let has_expressions = index_info.ii_Expressions.is_some();
        let has_exclusion_ops = index_info.ii_ExclusionOps.is_some();
        let need_estate = has_expressions || has_exclusion_ops;
        if need_estate {
            // econtext = GetPerTupleExprContext(estate);
            // econtext->ecxt_scantuple = slot;
            let econtext = execUtils::MakePerTupleExprContext(estate)?;
            estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);
        }

        // Form the index values and isnull flags for the index entry that we need
        // to check.
        //
        // Note: if the index uses functions that are not as immutable as they are
        // supposed to be, this could produce an index tuple different from the
        // original.  The index AM can catch such errors by verifying that it
        // finds a matching index entry with the tuple's TID.  For exclusion
        // constraints we check this in check_exclusion_constraint().
        let (values, isnull) = index_seams::form_index_datum::call(&index_info, slot, estate)?;

        // Now do the appropriate check.
        if !has_exclusion_ops {
            // Note: this is not a real insert; it is a check that the index entry
            // that has already been inserted is unique.  Passing the tuple's tid
            // (i.e. unmodified by table_index_fetch_tuple()) is correct even if
            // the row is now dead, because that is the TID the index will know
            // about.
            let num_index_attrs = index_info.ii_NumIndexAttrs as usize;
            {
                let mut carrier = IndexInfoCarrier::new(&mut index_info);
                indexam::index_insert(
                    mcx,
                    &index_rel,
                    &values[..num_index_attrs],
                    &isnull[..num_index_attrs],
                    &checktid,
                    &heap,
                    IndexUniqueCheck::UNIQUE_CHECK_EXISTING,
                    false,
                    &mut carrier,
                )?;
            }

            // Cleanup cache possibly initialized by index_insert.
            {
                let mut carrier = IndexInfoCarrier::new(&mut index_info);
                indexam::index_insert_cleanup(mcx, &index_rel, &mut carrier)?;
            }
        } else {
            // For exclusion constraints we just do the normal check, but now it's
            // okay to throw error.  In the HOT-update case, we must use the live
            // HOT child's TID here, else check_exclusion_constraint will think the
            // child is a conflict.
            //
            // On a conflict this returns Err. C lets the error escape and relies
            // on transaction abort (which does NOT print leak warnings) to release
            // the slot's buffer pin; in this port the pin is attributed to the
            // committing resource owner, so the slot must be cleared on the error
            // path too — otherwise the held pin surfaces as a spurious "resource
            // was not closed" warning ahead of the exclusion-violation error.
            let check = execIndexing::check_exclusion_constraint(
                mcx,
                estate,
                &heap,
                &index_rel,
                &index_info,
                Some(&tmptid),
                &values,
                &isnull,
                false,
            );
            if let Err(e) = check {
                execTuples_seams::exec_clear_tuple::call(estate, slot)?;
                return Err(e);
            }
        }

        // If that worked, then this index entry is unique or non-excluded, and we
        // are done.  index_close drops the RowExclusiveLock; the per-tuple
        // ExprContext / slot are torn down by FreeExecutorState below.
        indexam::index_close(index_rel, ROW_EXCLUSIVE_LOCK)?;

        // C's ExecDropSingleTupleTableSlot(slot): release the buffer pin
        // table_index_fetch_tuple stored into the standalone slot. The slot lives
        // in the EState tuple-table pool, and FreeExecutorState does not release
        // buffer pins, so clearing here prevents a per-row pin leak ("resource was
        // not closed").
        execTuples_seams::exec_clear_tuple::call(estate, slot)?;

        Ok(Some(()))
    })?;

    // FreeExecutorState releases the EState (shutting down any ExprContext and
    // reclaiming the tuple-table slot — the C ExecDropSingleTupleTableSlot +
    // FreeExecutorState).
    execUtils::FreeExecutorState(estate)?;

    let _ = skipped; // both the skip and the completion path return NULL.
    Ok(Datum::null())
}

/// Build a `TRIGGER_PROTOCOL_VIOLATED` (`ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED`,
/// SQLSTATE `39P01`) error with the given message — the SQLSTATE/text the three
/// `ereport(ERROR)` protocol checks in `constraint.c` raise.
fn trigger_protocol_error(message: String) -> ::types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED)
        .errmsg(message)
        .into_error()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Mutex, Once};

    // The trigger-event predicate helpers match the C macros bit-for-bit.
    #[test]
    fn event_predicates_match_c() {
        assert!(TRIGGER_FIRED_BY_INSERT(TRIGGER_EVENT_INSERT));
        assert!(!TRIGGER_FIRED_BY_INSERT(TRIGGER_EVENT_UPDATE));
        assert!(TRIGGER_FIRED_BY_UPDATE(TRIGGER_EVENT_UPDATE));
        assert!(!TRIGGER_FIRED_BY_UPDATE(TRIGGER_EVENT_INSERT));
        assert!(!TRIGGER_FIRED_BY_UPDATE(0x0001)); // DELETE op
        assert!(TRIGGER_FIRED_FOR_ROW(TRIGGER_EVENT_ROW));
        assert!(!TRIGGER_FIRED_FOR_ROW(0));
        assert!(TRIGGER_FIRED_AFTER(TRIGGER_EVENT_AFTER));
        assert!(!TRIGGER_FIRED_AFTER(TRIGGER_EVENT_BEFORE));
        assert!(TRIGGER_FIRED_BEFORE(TRIGGER_EVENT_BEFORE));
        assert!(!TRIGGER_FIRED_BEFORE(TRIGGER_EVENT_AFTER));
    }

    #[test]
    fn trigger_protocol_sqlstate_is_39p01() {
        assert_eq!(
            ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED,
            ::types_error::make_sqlstate(*b"39P01")
        );
    }

    // The three `ereport(ERROR)` protocol checks at the top of unique_key_recheck
    // short-circuit before any table-/index-AM call, so they are exercised
    // without driving the (live-relation) substrate.  `tg_event` is the only seam
    // these paths touch; it is installed once, process-wide, and the recorded
    // value is supplied per test under the lock.
    static TEST_LOCK: Mutex<()> = Mutex::new(());
    static EVENT: AtomicU32 = AtomicU32::new(0);
    static INSTALL: Once = Once::new();

    fn set_event(ev: u32) -> std::sync::MutexGuard<'static, ()> {
        // The seam closure runs re-entrantly inside `unique_key_recheck` while the
        // test still holds the lock, so the event value rides a separate atomic
        // (a std Mutex is not reentrant — re-locking it here would deadlock).
        let g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        INSTALL.call_once(|| {
            trigger::tg_event::set(|_td| EVENT.load(Ordering::SeqCst));
        });
        EVENT.store(ev, Ordering::SeqCst);
        g
    }

    fn ctx() -> MemoryContext {
        MemoryContext::new("constraint test")
    }

    #[test]
    fn not_called_by_trigger_manager_errors() {
        // The not-a-trigger path errors before reading tg_event, so no seam is
        // consulted.
        let parent = ctx();
        let err = unique_key_recheck(&parent, false, TriggerDataRef(1)).unwrap_err();
        assert_eq!(
            err.message(),
            "function \"unique_key_recheck\" was not called by trigger manager"
        );
        assert_eq!(err.sqlstate(), ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
    }

    #[test]
    fn must_be_after_row_when_before() {
        let _g = set_event(TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE);
        let parent = ctx();
        let err = unique_key_recheck(&parent, true, TriggerDataRef(2)).unwrap_err();
        assert_eq!(
            err.message(),
            "function \"unique_key_recheck\" must be fired AFTER ROW"
        );
        assert_eq!(err.sqlstate(), ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
    }

    #[test]
    fn must_be_after_row_when_statement() {
        // AFTER STATEMENT (no ROW bit) also fails the AFTER-ROW check.
        let _g = set_event(TRIGGER_EVENT_INSERT | TRIGGER_EVENT_AFTER);
        let parent = ctx();
        let err = unique_key_recheck(&parent, true, TriggerDataRef(2)).unwrap_err();
        assert_eq!(
            err.message(),
            "function \"unique_key_recheck\" must be fired AFTER ROW"
        );
    }

    #[test]
    fn must_be_insert_or_update() {
        // AFTER ROW DELETE (op = DELETE) is neither INSERT nor UPDATE.
        let _g = set_event(0x0001 | TRIGGER_EVENT_ROW | TRIGGER_EVENT_AFTER);
        let parent = ctx();
        let err = unique_key_recheck(&parent, true, TriggerDataRef(2)).unwrap_err();
        assert_eq!(
            err.message(),
            "function \"unique_key_recheck\" must be fired for INSERT or UPDATE"
        );
        assert_eq!(err.sqlstate(), ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
    }
}
