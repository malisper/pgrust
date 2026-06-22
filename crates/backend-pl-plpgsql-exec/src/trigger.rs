//! `plpgsql_exec_trigger` (pl_exec.c) and the trigger/record value legs it
//! needs: the `TG_*` promise fulfillment, the OLD/NEW record population
//! (`exec_move_row_from_datum` / `exec_move_row` NULL row), and the
//! before-trigger result-tuple handoff.
//!
//! These all bottom out in the expanded-record substrate
//! (`backend_utils_adt_misc2::expandedrecord`) reached through the call-scoped
//! [`crate::erh_table`] side-table, and in the trigger-manager data accessors
//! (`backend_commands_trigger_seams`) which resolve off the firing path's
//! per-call side-channel.

use backend_commands_trigger_seams as trig;
use backend_utils_adt_misc2::expandedrecord as er;
use mcx::{Mcx, MemoryContext};
use types_plpgsql::{
    int32, Datum, ExpandedRecordHeader as ErhHandle, PLpgSQL_datum, PLpgSQL_execstate,
    PLpgSQL_function, PLpgSQL_promise_type, PLpgSQL_rc, PLpgSQL_var,
};
use types_ri_triggers::{TriggerDataRef, TupleTableSlotRef};
use types_tuple::backend_access_common_heaptuple::{Datum as RichDatum, FormedTuple};

/// The current-trigger marker handle (`TriggerData(0)`), the only value the
/// trigger-data accessors key off (the rich payload rides the firing path's
/// per-call side-channel).
const TRIG_CURRENT: TriggerDataRef = TriggerDataRef(0);

/// `RECORDOID` (`catalog/pg_type_d.h`) — the anonymous-composite pseudo-type.
const RECORDOID: types_plpgsql::Oid = 2249;

// ---- TriggerEvent bit tests (commands/trigger.h) --------------------------
const TRIGGER_EVENT_INSERT: u32 = 0x0000;
const TRIGGER_EVENT_DELETE: u32 = 0x0001;
const TRIGGER_EVENT_UPDATE: u32 = 0x0002;
const TRIGGER_EVENT_TRUNCATE: u32 = 0x0003;
const TRIGGER_EVENT_OPMASK: u32 = 0x0003;
const TRIGGER_EVENT_ROW: u32 = 0x0004;
const TRIGGER_EVENT_BEFORE: u32 = 0x0008;
const TRIGGER_EVENT_AFTER: u32 = 0x0000;
const TRIGGER_EVENT_INSTEAD: u32 = 0x0010;
const TRIGGER_EVENT_TIMINGMASK: u32 = 0x0018;

#[inline]
fn fired_before(ev: u32) -> bool {
    ev & TRIGGER_EVENT_TIMINGMASK == TRIGGER_EVENT_BEFORE
}
#[inline]
fn fired_after(ev: u32) -> bool {
    ev & TRIGGER_EVENT_TIMINGMASK == TRIGGER_EVENT_AFTER
}
#[inline]
fn fired_instead(ev: u32) -> bool {
    ev & TRIGGER_EVENT_TIMINGMASK == TRIGGER_EVENT_INSTEAD
}

#[inline]
fn fired_for_row(ev: u32) -> bool {
    ev & TRIGGER_EVENT_ROW != 0
}
#[inline]
fn fired_by_insert(ev: u32) -> bool {
    ev & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_INSERT
}
#[inline]
fn fired_by_update(ev: u32) -> bool {
    ev & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_UPDATE
}
#[inline]
fn fired_by_delete(ev: u32) -> bool {
    ev & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_DELETE
}

/// Make a fresh call-scoped expanded-record header from a tuple descriptor,
/// register it in the [`crate::erh_table`] side-table, and return its handle.
/// (The C `make_expanded_record_from_tupdesc(tupdesc, estate->datum_context)`.)
fn build_erh_from_tupdesc(
    tupdesc: &types_tuple::heaptuple::TupleDescData<'_>,
) -> types_error::PgResult<ErhHandle> {
    let ctx = Box::new(MemoryContext::new("PL/pgSQL expanded record"));
    let header: ExpandedRecordHeader<'static> = {
        let mcx: Mcx<'static> = unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>(ctx.mcx()) };
        er::make_expanded_record_from_tupdesc(mcx, tupdesc)?
    };
    Ok(ErhHandle(crate::erh_table::register(ctx, header)))
}

type ExpandedRecordHeader<'mcx> = er::ExpandedRecordHeader<'mcx>;

// ===========================================================================
// plpgsql_fulfill_promise — TG_* (and FOUND etc) promise variables.
// ===========================================================================

/// `plpgsql_fulfill_promise(estate, var)` (pl_exec.c 1364) — compute and assign a
/// `DTYPE_PROMISE` variable's promised value on first read. The trigger
/// promises (`TG_NAME`/`TG_WHEN`/`TG_LEVEL`/`TG_OP`/`TG_RELID`/`TG_TABLE_NAME`/
/// `TG_TABLE_SCHEMA`/`TG_NARGS`/`TG_ARGV`) read off the firing trigger's data;
/// non-trigger promises (`SQLSTATE`/`SQLERRM` etc) are out of this leg.
pub fn plpgsql_fulfill_promise_impl(
    estate: &mut PLpgSQL_execstate,
    var: &mut PLpgSQL_var,
) -> types_error::PgResult<()> {
    use PLpgSQL_promise_type::*;
    let promise = var.promise;
    // Mark the promise as fulfilled (so we don't recompute) — C resets
    // var->promise = PLPGSQL_PROMISE_NONE before assigning.
    var.promise = PLPGSQL_PROMISE_NONE;

    // Build the promised value as a `(bare_word, byref_image, isnull)` triple,
    // then store it on the var. By-reference promises (`text` / `name` /
    // `text[]`) carry their verbatim header-ful/`NameData` image out-of-band in
    // `value_byref` (bare word `0`) so the by-ref fmgr lane reconstructs the
    // value; `oid`/`int4` are by-value words (no image).
    let (word, byref, isnull): (usize, Option<Vec<u8>>, bool) = match promise {
        PLPGSQL_PROMISE_NONE => return Ok(()),
        PLPGSQL_PROMISE_TG_NAME => (0, Some(name_image(&bytes_to_string(read_trigger_name()?))), false),
        PLPGSQL_PROMISE_TG_WHEN => {
            let ev = trig::tg_event::call(TRIG_CURRENT);
            let s = if fired_before(ev) {
                "BEFORE"
            } else if fired_after(ev) {
                "AFTER"
            } else if fired_instead(ev) {
                "INSTEAD OF"
            } else {
                unreachable!("unrecognized trigger execution time")
            };
            (0, Some(text_image(s)), false)
        }
        PLPGSQL_PROMISE_TG_LEVEL => {
            let ev = trig::tg_event::call(TRIG_CURRENT);
            (0, Some(text_image(if fired_for_row(ev) { "ROW" } else { "STATEMENT" })), false)
        }
        PLPGSQL_PROMISE_TG_OP => {
            let ev = trig::tg_event::call(TRIG_CURRENT);
            let s = if fired_by_insert(ev) {
                "INSERT"
            } else if fired_by_update(ev) {
                "UPDATE"
            } else if fired_by_delete(ev) {
                "DELETE"
            } else if ev & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_TRUNCATE {
                "TRUNCATE"
            } else {
                unreachable!("unrecognized trigger action")
            };
            (0, Some(text_image(s)), false)
        }
        PLPGSQL_PROMISE_TG_RELID => {
            let oid = trig::tg_relation_oid::call(TRIG_CURRENT);
            (oid as usize, None, false)
        }
        PLPGSQL_PROMISE_TG_TABLE_NAME => {
            (0, Some(name_image(&bytes_to_string(read_relation_name()?))), false)
        }
        PLPGSQL_PROMISE_TG_TABLE_SCHEMA => {
            let ns = trig::tg_relation_namespace::call(TRIG_CURRENT);
            let name = crate::exec_seams::get_namespace_name::call(ns)?;
            (0, Some(name_image(&name)), false)
        }
        PLPGSQL_PROMISE_TG_NARGS => {
            let n = trig::tg_nargs::call(TRIG_CURRENT);
            (n as u32 as usize, None, false)
        }
        PLPGSQL_PROMISE_TG_ARGV => {
            let n = trig::tg_nargs::call(TRIG_CURRENT);
            let mut args: Vec<Option<Vec<u8>>> = Vec::with_capacity(n.max(0) as usize);
            for i in 0..n {
                let v = crate::with_query_mcx(|mcx| {
                    trig::tg_argv::call(mcx, TRIG_CURRENT, i)
                        .map(|opt| opt.map(|b| b.as_slice().to_vec()))
                })?;
                args.push(v);
            }
            let image = crate::exec_seams::construct_text_array_datum::call(args)?;
            (0, Some(image), false)
        }
        PLPGSQL_PROMISE_TG_EVENT => {
            // assign_text_var(estate, var, estate->evtrigdata->event); the
            // evtrigdata == NULL guard is the None case.
            let event = backend_commands_event_trigger_seams::event_trigger_get_event::call()?
                .ok_or_else(|| {
                    types_error::PgError::error(
                        "event trigger promise is not in an event trigger function".to_string(),
                    )
                })?;
            (0, Some(text_image(&event)), false)
        }
        PLPGSQL_PROMISE_TG_TAG => {
            // assign_text_var(estate, var, GetCommandTagName(estate->evtrigdata->tag));
            let tag = backend_commands_event_trigger_seams::event_trigger_get_tag_name::call()?
                .ok_or_else(|| {
                    types_error::PgError::error(
                        "event trigger promise is not in an event trigger function".to_string(),
                    )
                })?;
            (0, Some(text_image(&tag)), false)
        }
    };

    crate::assign_simple_var(estate, var, Datum::from_usize(word), isnull, false);
    // assign_simple_var clears value_byref (a by-value store); set the by-ref
    // image for the by-reference promises so the snapshot/SPI bind carries it.
    if !isnull {
        var.value_byref = byref;
    }
    Ok(())
}

/// `CStringGetTextDatum(s)` image — a header-ful `text` varlena byte image,
/// stamped exactly like `Varlena::from_image` (`SET_VARSIZE_4B`).
fn text_image(s: &str) -> Vec<u8> {
    const VARHDRSZ: usize = 4;
    let bytes = s.as_bytes();
    let mut image = vec![0u8; VARHDRSZ + bytes.len()];
    image[..VARHDRSZ].copy_from_slice(&types_datum::varlena::set_varsize_4b(VARHDRSZ + bytes.len()));
    image[VARHDRSZ..].copy_from_slice(bytes);
    image
}

/// `namein(s)` image — the fixed 64-byte NUL-padded `NameData` (header-less).
fn name_image(s: &str) -> Vec<u8> {
    const NAMEDATALEN: usize = 64;
    let bytes = s.as_bytes();
    let n = bytes.len().min(NAMEDATALEN - 1);
    let mut image = vec![0u8; NAMEDATALEN];
    image[..n].copy_from_slice(&bytes[..n]);
    image
}

fn bytes_to_string(b: Vec<u8>) -> String {
    String::from_utf8_lossy(&b).into_owned()
}

/// `NameGetDatum(tg_trigger->tgname)` bytes (server-encoded), via the seam.
fn read_trigger_name() -> types_error::PgResult<Vec<u8>> {
    let trig_ref = trig::tg_trigger::call(TRIG_CURRENT);
    crate::with_query_mcx(|mcx| {
        trig::trigger_name::call(mcx, trig_ref).map(|b| b.as_slice().to_vec())
    })
}

/// `RelationGetRelationName(tg_relation)` bytes, via the seam.
fn read_relation_name() -> types_error::PgResult<Vec<u8>> {
    crate::with_query_mcx(|mcx| {
        trig::tg_relation_name::call(mcx, TRIG_CURRENT).map(|b| b.as_slice().to_vec())
    })
}

// ===========================================================================
// exec_move_row_from_datum / exec_move_row(NULL) — populate a REC target.
// ===========================================================================

/// `exec_move_row_from_datum(estate, rec, value)` (pl_exec.c 7616) — assign a
/// composite datum into a REC target by building an expanded record from it.
/// The bare-word path (no by-ref image) is only reachable for a by-value
/// composite, which does not occur; the real entry is the by-ref variant.
pub fn exec_move_row_from_datum_impl(
    estate: &mut PLpgSQL_execstate,
    target_dno: int32,
    value: Datum,
) -> types_error::PgResult<()> {
    exec_move_row_from_datum_byref_impl(estate, target_dno, value, None)
}

/// `exec_move_row_from_datum` with the composite value's verbatim
/// `HeapTupleHeader` varlena image (the by-ref companion; the bare `value` word
/// is `0` then). Builds an expanded record from the deserialized tuple and
/// installs it as the REC's live header.
pub fn exec_move_row_from_datum_byref_impl(
    estate: &mut PLpgSQL_execstate,
    target_dno: int32,
    _value: Datum,
    byref: Option<Vec<u8>>,
) -> types_error::PgResult<()> {
    let Some(image) = byref else {
        panic!(
            "exec_move_row_from_datum: composite source has no by-reference image \
             (a by-value composite is not representable here)"
        );
    };

    let ctx = Box::new(MemoryContext::new("PL/pgSQL expanded record"));
    let header: ExpandedRecordHeader<'static> = {
        let mcx: Mcx<'static> = unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>(ctx.mcx()) };
        let ft: FormedTuple<'static> = FormedTuple::from_datum_image(mcx, &image)?;
        er::make_expanded_record_from_datum(mcx, &ft)?
    };
    let handle = ErhHandle(crate::erh_table::register(ctx, header));
    set_rec_erh(estate, target_dno, Some(handle));
    Ok(())
}

/// `exec_move_row(estate, rec, NULL, NULL)` (pl_exec.c) — clear a REC target to
/// the unassigned (NULL) state: drop any live expanded header.
pub fn exec_move_row_null_impl(
    estate: &mut PLpgSQL_execstate,
    target_dno: int32,
) -> types_error::PgResult<()> {
    set_rec_erh(estate, target_dno, None);
    Ok(())
}

/// `exec_move_row(estate, rec, ...)` for a `SELECT ... INTO <record>` result
/// (pl_exec.c `exec_move_row` REC arm): build a transient record tupledesc from
/// the result columns, make an expanded record of it, set the column values, and
/// install it as the REC's live header. An empty `columns` (the no-rows case)
/// clears the record to the NULL state.
pub fn exec_move_row_into_record_impl(
    estate: &mut PLpgSQL_execstate,
    target_dno: int32,
    columns: &[crate::exec_seams::ExecsqlColumn],
) -> types_error::PgResult<()> {
    if columns.is_empty() {
        set_rec_erh(estate, target_dno, None);
        return Ok(());
    }

    // C's `make_expanded_record_for_rec` (pl_exec.c): a record variable declared
    // with a fixed composite type (`rec->rectypeid != RECORDOID`, e.g. `r tt`)
    // must produce an expanded record of *that* type — `er_typeid = rectypeid` —
    // not an anonymous RECORD. Only a genuinely RECORD-typed variable adopts the
    // ad-hoc tupdesc built from the result columns. Honoring this is what lets a
    // later `row(r.*)` / `r.*` star-expansion resolve the declared rowtype's
    // field set (a RECORDOID-stamped header has no resolvable named columns).
    let rectypeid = match &estate.datums[target_dno as usize] {
        PLpgSQL_datum::Rec(rec) => rec.rectypeid,
        _ => panic!("exec_move_row_into_record_impl: datum {target_dno} is not a REC"),
    };

    let ctx = Box::new(MemoryContext::new("PL/pgSQL expanded record"));
    let header: ExpandedRecordHeader<'static> = {
        let mcx: Mcx<'static> =
            unsafe { core::mem::transmute::<Mcx<'_>, Mcx<'static>>(ctx.mcx()) };

        let mut erh = if rectypeid != RECORDOID {
            // Declared composite type: build from the type's real rowtype
            // (make_expanded_record_from_typeid(rec->rectypeid, -1, ...)), which
            // stamps er_typeid = rectypeid. The result columns are physically
            // compatible with the declared rowtype (the query was planned to
            // match), so the field values map by position.
            er::make_expanded_record_from_typeid(mcx, rectypeid, -1)?
        } else {
            // RECORD variable: adopt an ad-hoc tupdesc built from the result
            // columns and blessed to assign a transient record typmod.
            let mut td =
                backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, columns.len() as i32)?;
            for (i, c) in columns.iter().enumerate() {
                backend_access_common_tupdesc::TupleDescInitEntry(
                    &mut td,
                    (i + 1) as i16,
                    Some(if c.name.is_empty() { "?column?" } else { &c.name }),
                    c.typeid,
                    c.typmod,
                    0,
                )?;
            }
            let boxed: mcx::PgBox<'static, types_tuple::heaptuple::TupleDescData<'static>> =
                mcx::PgBox::try_new_in(td, mcx).map_err(|_| mcx.oom(0))?;
            let blessed =
                backend_executor_execTuples::exectype_tupoutput::BlessTupleDesc(mcx, Some(boxed))?;
            let td_ref: &types_tuple::heaptuple::TupleDescData<'static> =
                blessed.as_ref().expect("blessed tupdesc");
            er::make_expanded_record_from_tupdesc(mcx, td_ref)?
        };

        // Set the field values (a by-reference column becomes a ByRef Datum from
        // its verbatim image; the bare word is a by-value scalar).
        let mut values: Vec<RichDatum<'static>> = Vec::with_capacity(columns.len());
        let mut nulls: Vec<bool> = Vec::with_capacity(columns.len());
        for c in columns {
            nulls.push(c.isnull);
            if c.isnull {
                values.push(RichDatum::null());
            } else if let Some(image) = &c.byref {
                let slice = mcx::slice_in(mcx, image)?;
                values.push(RichDatum::ByRef(slice));
            } else {
                values.push(RichDatum::from_usize(c.value));
            }
        }
        er::expanded_record_set_fields(mcx, &mut erh, &values, &nulls, true)?;
        erh
    };
    let handle = ErhHandle(crate::erh_table::register(ctx, header));
    set_rec_erh(estate, target_dno, Some(handle));
    Ok(())
}

/// Set (or clear) a REC datum's expanded-header handle.
fn set_rec_erh(estate: &mut PLpgSQL_execstate, dno: int32, handle: Option<ErhHandle>) {
    match &mut estate.datums[dno as usize] {
        PLpgSQL_datum::Rec(rec) => rec.erh = handle,
        _ => panic!("set_rec_erh: datum {dno} is not a REC"),
    }
}

// ===========================================================================
// plpgsql_exec_trigger — the DML-trigger executor entry (pl_exec.c 935).
// ===========================================================================

/// `plpgsql_exec_trigger(func, trigdata)` (pl_exec.c 935) — run a DML trigger
/// function: set up NEW/OLD expanded records from the firing tuples, run the
/// body, and return the result tuple a BEFORE/INSTEAD-OF row trigger asked to
/// apply (deposited on the firing path's return-tuple channel).
pub fn plpgsql_exec_trigger_impl(
    func: &PLpgSQL_function,
    _trigdata: types_plpgsql::TriggerData,
) -> types_error::PgResult<Datum> {
    // Save any outer call's live expanded-record table (a trigger that fires a
    // query that fires another trigger nests here); restored before return so the
    // outer call's records survive our `clear()`. New records this call registers
    // are 1-based from an empty table.
    let saved_erh = crate::erh_table::take_all();

    // Setup the execution state. (No simple_eval_estate/resowner — C passes NULL;
    // the SPI bracket the call handler opened owns the eval econtext.)
    let mut estate = crate::plpgsql_estate_setup(func, None, None, None);

    // estate.trigdata = trigdata (the current-trigger marker).
    estate.trigdata = Some(types_plpgsql::TriggerData(0));

    // Push this frame onto the live error_context_stack (see
    // plpgsql_exec_function); pops on scope exit.
    let _frame_guard = crate::ExecFrameGuard::push(&estate);

    // Make local execution copies of all the datums.
    estate.err_text = Some(crate::mem::sdup("during initialization of execution state"));
    crate::copy_plpgsql_datums(&mut estate, func);

    let tg_event = trig::tg_event::call(TRIG_CURRENT);

    // Put the OLD and NEW tuples into record variables. We set up expanded
    // records for BOTH even though only one may have a value (so record refs
    // succeed regardless of the current trigger type; an unsupplied field reads
    // NULL). tupdesc = RelationGetDescr(trigdata->tg_relation).
    let new_varno = func.new_varno;
    let old_varno = func.old_varno;

    // Run the fallible body in a closure so the call-scoped expanded-record
    // teardown below runs on BOTH the Ok and the Err path (C's PG_FINALLY: the
    // outer call's erh table must be restored even when the trigger body raises).
    let body = (|| -> types_error::PgResult<Datum> {
        let (new_handle, old_handle) = crate::with_query_mcx(|mcx| {
            let rel = trig::tg_relation::call(mcx, TRIG_CURRENT)?;
            let tupdesc = &rel.rd_att;
            let new_h = build_erh_from_tupdesc(tupdesc)?;
            let old_h = build_erh_from_tupdesc(tupdesc)?;
            Ok::<_, types_error::PgError>((new_h, old_h))
        })?;

        if new_varno >= 0 {
            set_rec_erh(&mut estate, new_varno, Some(new_handle));
        }
        if old_varno >= 0 {
            set_rec_erh(&mut estate, old_varno, Some(old_handle));
        }

        // Populate the appropriate record(s) from the firing tuples.
        if !fired_for_row(tg_event) {
            // Per-statement triggers don't use OLD/NEW.
        } else if fired_by_insert(tg_event) {
            set_record_from_slot(&mut estate, new_varno, TupleTableSlotRef(SLOT_TRIG))?;
        } else if fired_by_update(tg_event) {
            set_record_from_slot(&mut estate, new_varno, TupleTableSlotRef(SLOT_NEW))?;
            set_record_from_slot(&mut estate, old_varno, TupleTableSlotRef(SLOT_TRIG))?;

            // In a BEFORE trigger, stored generated columns are not computed
            // yet, so make them null in the NEW row. (Only needed in the UPDATE
            // branch; in the INSERT case they are already null, but in UPDATE the
            // field still contains the old value.) (pl_exec.c:1004)
            if fired_before(tg_event) {
                null_stored_generated_in_new(&mut estate, new_varno)?;
            }
        } else if fired_by_delete(tg_event) {
            set_record_from_slot(&mut estate, old_varno, TupleTableSlotRef(SLOT_TRIG))?;
        } else {
            panic!("unrecognized trigger action: not INSERT, DELETE, or UPDATE");
        }

        // (SPI_register_trigger_data for transition tables is the SPI-trigger leg;
        // a plain BEFORE ROW trigger uses no transition tables.)

        estate.err_text = Some(crate::mem::sdup("during function entry"));
        crate::exec_set_found(&mut estate, false);

        // Run the toplevel block.
        estate.err_text = None;
        let action = func
            .action
            .as_deref()
            .expect("compiled trigger function has an action block");
        let rc = crate::exec_toplevel_block(&mut estate, action)?;
        if rc != PLpgSQL_rc::PLPGSQL_RC_RETURN {
            estate.err_text = None;
            return Err(crate::seam::ereport_no_return_statement());
        }

        estate.err_text = Some(crate::mem::sdup("during function exit"));

        if estate.retisset {
            return Err(
                types_error::PgError::error("trigger procedure cannot return a set".to_string())
                    .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
            );
        }

        // Build the result tuple: NULL (do nothing) for a NULL return or a
        // per-statement trigger; otherwise the returned composite, deposited on
        // the firing path's BEFORE-trigger return-tuple channel. The deposit must
        // OUTLIVE this call — the firing path's `decode_before_trigger_result`
        // takes it back (and clones it into the query context) only after
        // `ExecCallTriggerFunc` (= this fmgr call) returns. So the bytes ride a
        // leaked, backend-lifetime context, mirroring C's BEFORE-trigger tuple
        // allocated in the firing query context (and matching how the SQLERRM-var
        // text image is built in this crate).
        let deposited: Option<FormedTuple<'static>> =
            if estate.retisnull || !fired_for_row(tg_event) {
                None
            } else {
                // estate.retval is the returned composite (rec_new/rec_old): its
                // HeapTupleHeader varlena image rides estate.retval_byref.
                let Some(image) = estate.retval_byref.clone() else {
                    return Err(
                        types_error::PgError::error(
                            "returned row structure does not match the structure of the triggering table"
                                .to_string(),
                        )
                        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
                    );
                };
                let ctx: &'static MemoryContext =
                    Box::leak(Box::new(MemoryContext::new("PL/pgSQL trigger result")));
                let ft = FormedTuple::from_datum_image(ctx.mcx(), &image)?;
                Some(ft)
            };

        // Deposit the result row on the BEFORE-trigger return-tuple channel; the
        // firing path takes it back (and clones it) after this call returns.
        trig::set_before_trigger_result_tuple::call(deposited);

        // Return the sentinel Datum (the real tuple rode the channel).
        Ok(Datum::from_usize(0))
    })();

    // PG_FINALLY: clean up the call-scoped expanded records, then restore the
    // outer call's table — on both the success and the error path.
    crate::exec_eval_cleanup(&mut estate);
    crate::erh_table::clear();
    crate::erh_table::restore_all(saved_erh);

    // Attach the PL/pgSQL error-context line built from the estate's err_*
    // state at the moment of failure (C's plpgsql_exec_error_callback; see
    // plpgsql_exec_function).
    body.map_err(|e| crate::attach_plpgsql_context(e, &estate, &func.fn_signature))
}

/// Populate a REC variable from the firing trigger's OLD/NEW slot tuple.
/// Null out every STORED generated column in the NEW record (rec_new), for a
/// BEFORE UPDATE row trigger. The generated values are recomputed by the
/// executor only after the trigger runs, but the projected UPDATE new tuple
/// still carries the column's old value, so the trigger would otherwise see a
/// stale value instead of NULL. (pl_exec.c:1012-1022,
/// `expanded_record_set_field_internal` per STORED generated attribute.)
fn null_stored_generated_in_new(
    estate: &mut PLpgSQL_execstate,
    new_dno: int32,
) -> types_error::PgResult<()> {
    if new_dno < 0 {
        return Ok(());
    }
    let handle = match &estate.datums[new_dno as usize] {
        PLpgSQL_datum::Rec(rec) => rec.erh.as_ref().map(|h| h.0).unwrap_or(0),
        _ => panic!("null_stored_generated_in_new: datum {new_dno} is not a REC"),
    };
    if handle == 0 {
        return Ok(());
    }

    // Collect the 1-based attnums of STORED generated columns off the firing
    // relation's descriptor (tupdesc = RelationGetDescr(trigdata->tg_relation)).
    // Early-out unless tupdesc->constr->has_generated_stored, mirroring C.
    let gen_attnums: Vec<i32> = crate::with_query_mcx(|mcx| {
        let rel = trig::tg_relation::call(mcx, TRIG_CURRENT)?;
        let tupdesc = &rel.rd_att;
        let has_gen = tupdesc
            .constr
            .as_ref()
            .map(|c| c.has_generated_stored)
            .unwrap_or(false);
        if !has_gen {
            return Ok::<Vec<i32>, types_error::PgError>(Vec::new());
        }
        let mut v = Vec::new();
        for i in 0..tupdesc.natts as usize {
            if tupdesc.attr(i).attgenerated
                == types_tuple::access::ATTRIBUTE_GENERATED_STORED
            {
                v.push((i + 1) as i32);
            }
        }
        Ok(v)
    })?;

    if gen_attnums.is_empty() {
        return Ok(());
    }

    crate::with_query_mcx(|_mcx| {
        let r = crate::erh_table::with_erh_mut(handle, |emcx, erh| {
            for &fnumber in &gen_attnums {
                er::expanded_record_set_field_internal(
                    emcx,
                    erh,
                    fnumber,
                    RichDatum::null(),
                    true,  /* isnull */
                    false, /* expand_external */
                    false, /* check_constraints */
                )?;
            }
            Ok::<(), types_error::PgError>(())
        });
        if let Some(res) = r {
            res?;
        }
        Ok::<(), types_error::PgError>(())
    })
}

fn set_record_from_slot(
    estate: &mut PLpgSQL_execstate,
    rec_dno: int32,
    slot: TupleTableSlotRef,
) -> types_error::PgResult<()> {
    if rec_dno < 0 {
        return Ok(());
    }
    let handle = match &estate.datums[rec_dno as usize] {
        PLpgSQL_datum::Rec(rec) => rec.erh.as_ref().map(|h| h.0).unwrap_or(0),
        _ => panic!("set_record_from_slot: datum {rec_dno} is not a REC"),
    };
    if handle == 0 {
        return Ok(());
    }
    crate::with_query_mcx(|mcx| {
        let formed = trig::tg_slot_formed_tuple::call(mcx, slot)?;
        if let Some(ft) = formed {
            let r = crate::erh_table::with_erh_mut(handle, |emcx, erh| {
                let into = ft_into(emcx, &ft)?;
                er::expanded_record_set_tuple(emcx, erh, Some(&into), true, false)
            });
            if let Some(res) = r {
                res?;
            }
        }
        Ok::<(), types_error::PgError>(())
    })
}

/// Re-anchor a `FormedTuple` into the expanded record's own context.
fn ft_into<'mcx>(
    mcx: Mcx<'mcx>,
    ft: &FormedTuple<'_>,
) -> types_error::PgResult<FormedTuple<'mcx>> {
    ft.clone_in(mcx)
}

// The slot markers the firing path publishes (mirrored from
// backend-commands-trigger's ri_accessors).
const SLOT_TRIG: u64 = 1;
const SLOT_NEW: u64 = 2;
