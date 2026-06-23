//! Production installer for the `DestRemoteSimple` result-set seams declared in
//! [`backup_copy_seams`] (`create_dest_remote_simple` /
//! `begin_tup_output_tupdesc` / `do_tup_output` / `end_tup_output`).
//!
//! These are the `executor.h` tuple-output trio + `CreateDestReceiver(
//! DestRemoteSimple)` used by `basebackup_copy.c`'s `SendXlogRecPtrResult` and
//! `SendTablespaceList`. The seam crate (`backend-backup-copy`, which is
//! `#![no_std] #![forbid(unsafe_code)]`) carries the rows as already-rendered,
//! typed values ([`ResultColumn`] / [`ResultValue`]); this thin bridge converts
//! those carriers to the canonical `TupleDesc` / `Datum` form and delegates to
//! the real, landed owners — `CreateDestReceiver` (tcop/dest.c) and
//! `begin/do/end_tup_output` (execTuples.c) — exactly as `walsender.c`'s
//! `IdentifySystem` does for its own `DestRemoteSimple` result set.
//!
//! It lives in its own crate (rather than `backend-backup-copy::init_seams`)
//! because the bridge needs the execTuples / dest / tupdesc / varlena machinery
//! *and* a `MemoryContext`-bound parking transmute, neither of which the
//! `forbid(unsafe_code)` sink crate can host. The seam-ownership boundary is the
//! same one `IdentifySystem` crosses — owner = "tcop/dest.c + execTuples.c".
//!
//! # In-flight state
//!
//! The seam API is stateful across three calls (`begin` returns a handle,
//! `do`/`end` consume it) but the carrier [`SeamTupOutputState`] holds only the
//! receiver handle. The real `TupOutputState<'mcx>` (and the `MemoryContext`
//! whose arena backs its output slot) must therefore be parked between calls.
//! We park the single in-flight pair in a thread-local: `basebackup_copy.c`
//! emits these result sets strictly sequentially (one full `begin`/`do`*/`end`
//! cycle for `SendTablespaceList`, then another for `SendXlogRecPtrResult`),
//! never nested and never interleaved, mirroring C's single
//! `TupOutputState *tstate` local.

use std::cell::RefCell;

use ::mcx::MemoryContext;
use ::types_core::catalog::OIDOID;
use ::types_tuple::heaptuple::Datum;
use ::types_tuple::heaptuple::{INT8OID, TEXTOID};

use backup_copy_seams::{
    ResultColumn, ResultColumnType, ResultValue, TupOutputState as SeamTupOutputState,
};

type RealTupOutputState<'mcx> = nodes::tuptable::TupOutputState<'mcx>;

/// The parked in-flight result set: the owning `MemoryContext` plus the real
/// `TupOutputState`, with its `'mcx` lifetime erased to `'static` for storage.
///
/// The context is **boxed** so its address is stable across the move into the
/// thread-local: `Mcx<'mcx>` is a `&'mcx MemoryContext`, and the real
/// `TupOutputState`'s slot allocations capture that very reference as their
/// allocator. Moving the `MemoryContext` struct itself would dangle those
/// captured references (use-after-free on drop); a `Box` keeps the struct
/// pinned on the heap while only the (movable) `Box` pointer travels into the
/// thread-local.
struct InFlight {
    /// The real `TupOutputState`, lifetime-erased to `'static` for parking and
    /// re-borrowed back to `ctx`'s `'mcx` on each access.
    state: Option<RealTupOutputState<'static>>,
    /// The (pinned) context whose arena backs `state`'s output slot. Alive for
    /// the whole `begin`..`end` window; dropped (reclaiming the arena) when this
    /// clears.
    ctx: Box<MemoryContext>,
}

thread_local! {
    /// The single in-flight `DestRemoteSimple` result set (see module docs:
    /// `basebackup_copy.c` is strictly sequential, never nested).
    static IN_FLIGHT: RefCell<Option<InFlight>> = const { RefCell::new(None) };
}

/// Map a seam [`ResultColumnType`] to its builtin type OID (the C
/// `TupleDescInitBuiltinEntry` type argument).
fn column_type_oid(typ: ResultColumnType) -> ::types_core::primitive::Oid {
    match typ {
        ResultColumnType::Text => TEXTOID,
        ResultColumnType::Int8 => INT8OID,
        ResultColumnType::Oid => OIDOID,
    }
}

/// `create_dest_remote_simple()` — `CreateDestReceiver(DestRemoteSimple)`.
/// Delegates to the real tcop/dest.c receiver factory, exactly as
/// `IdentifySystem`/`cmd_variable_show` do.
fn create_dest_remote_simple() -> nodes::parsestmt::DestReceiverHandle {
    dest_seams::create_dest_receiver::call(types_dest::CommandDest::RemoteSimple)
}

/// `begin_tup_output_tupdesc(dest, columns)` — build the real `TupleDesc` from
/// the seam column descriptors (`CreateTemplateTupleDesc` +
/// `TupleDescInitBuiltinEntry`), call the real `begin_tup_output_tupdesc`, park
/// the resulting state under an owned `MemoryContext`, and return the carrier.
fn begin_tup_output_tupdesc(
    dest: nodes::parsestmt::DestReceiverHandle,
    columns: Vec<ResultColumn>,
) -> SeamTupOutputState {
    // Own a MemoryContext for the duration of this result set (begin..end),
    // mirroring IdentifySystem's `::mcx::MemoryContext::new(...)`. Box it up front
    // so its address is stable: the slot allocations below capture `mcx` (a
    // `&MemoryContext`) as their allocator, so the struct must not move after
    // this point — only the `Box` pointer is parked in the thread-local.
    let ctx: Box<MemoryContext> = Box::new(MemoryContext::new("basebackup result"));
    let mcx = ctx.mcx();

    // tupdesc = CreateTemplateTupleDesc(natts);
    // TupleDescInitBuiltinEntry(tupdesc, i, name, <typeoid>, -1, 0);
    let mut tupdesc =
        tupdesc::CreateTemplateTupleDesc(mcx, columns.len() as i32)
            .expect("CreateTemplateTupleDesc(basebackup result)");
    for (i, col) in columns.iter().enumerate() {
        tupdesc::TupleDescInitBuiltinEntry(
            &mut tupdesc,
            (i + 1) as i16,
            &col.name,
            column_type_oid(col.typ),
            -1,
            0,
        )
        .expect("TupleDescInitBuiltinEntry(basebackup result column)");
    }
    let tupdesc = Some(::mcx::alloc_in(mcx, tupdesc).expect("alloc basebackup result tupdesc"));

    // tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);
    let state = execTuples_seams::begin_tup_output_tupdesc::call(
        mcx,
        dest,
        tupdesc,
        nodes::TupleSlotKind::Virtual,
    )
    .expect("begin_tup_output_tupdesc(basebackup result)");

    // Park the (state, ctx) pair. `state` borrows only `ctx`'s arena (its output
    // slot was allocated in `mcx == ctx.mcx()`), so erase its lifetime to
    // `'static` for storage; `ctx` is parked alongside and is dropped only by
    // `end_tup_output`, after the real `end_tup_output` consumes `state`.
    //
    // SAFETY: `state: RealTupOutputState<'mcx>` is reachable only through the
    // `InFlight` we store here, and only via the re-borrow helpers below which
    // pin it back to `in_flight.ctx`'s live `'mcx`. We never move/reset `ctx`
    // while `state` is parked, so the region the slot points into stays valid
    // for every `'mcx` re-borrow until `end_tup_output` drops both together.
    let state_static: RealTupOutputState<'static> = unsafe { core::mem::transmute(state) };
    IN_FLIGHT.with(|cell| {
        let mut slot = cell.borrow_mut();
        assert!(
            slot.is_none(),
            "basebackup DestRemoteSimple result sets must not nest"
        );
        *slot = Some(InFlight {
            state: Some(state_static),
            ctx,
        });
    });

    SeamTupOutputState { dest }
}

/// `do_tup_output(tstate, values)` — convert the seam values to `Datum`/`isnull`
/// in the parked context and emit one row through the real `do_tup_output`.
fn do_tup_output(_tstate: SeamTupOutputState, values: Vec<Option<ResultValue>>) {
    IN_FLIGHT.with(|cell| {
        let mut slot = cell.borrow_mut();
        let in_flight = slot
            .as_mut()
            .expect("do_tup_output without a begin_tup_output_tupdesc");
        let mcx = in_flight.ctx.mcx();

        // Build the Datum/isnull arrays (the C `Datum values[]` / `bool
        // nulls[]`). `None` == SQL NULL (isnull = true).
        let mut datum_values: Vec<Datum> = Vec::with_capacity(values.len());
        let mut isnull: Vec<bool> = Vec::with_capacity(values.len());
        for v in &values {
            match v {
                Some(ResultValue::Text(s)) => {
                    datum_values.push(
                        varlena_seams::cstring_to_text_v::call(mcx, s)
                            .expect("cstring_to_text(basebackup result text)"),
                    );
                    isnull.push(false);
                }
                Some(ResultValue::Int8(n)) => {
                    datum_values.push(Datum::from_i64(*n));
                    isnull.push(false);
                }
                Some(ResultValue::Oid(oid)) => {
                    datum_values.push(Datum::from_oid(*oid));
                    isnull.push(false);
                }
                None => {
                    datum_values.push(Datum::null());
                    isnull.push(true);
                }
            }
        }

        // Re-borrow the parked state back to the live context's `'mcx`.
        // SAFETY: `in_flight.ctx` is the very context the state was built in and
        // is still alive here, so the `'static` -> `'mcx` re-borrow is honest.
        let state_ref: &mut RealTupOutputState<'static> =
            in_flight.state.as_mut().expect("do_tup_output after end");
        let state_mcx: &mut RealTupOutputState = unsafe { core::mem::transmute(state_ref) };

        execTuples_seams::do_tup_output::call(
            mcx,
            state_mcx,
            &datum_values,
            &isnull,
        )
        .expect("do_tup_output(basebackup result)");
    });
}

/// `end_tup_output(tstate)` — finish the real `TupOutputState` and drop the
/// owned `MemoryContext`, clearing the in-flight slot.
fn end_tup_output(_tstate: SeamTupOutputState) {
    let mut in_flight = IN_FLIGHT.with(|cell| {
        cell.borrow_mut()
            .take()
            .expect("end_tup_output without a begin_tup_output_tupdesc")
    });

    let mcx = in_flight.ctx.mcx();

    // Take the state back out (lifetime-erased) and re-borrow to the live
    // context. SAFETY: same invariant as `do_tup_output`; `in_flight.ctx` is
    // alive for this whole call and is dropped only after the real
    // `end_tup_output` has consumed the state.
    let state_static = in_flight.state.take().expect("end_tup_output called twice");
    let state_mcx: RealTupOutputState = unsafe { core::mem::transmute(state_static) };

    execTuples_seams::end_tup_output::call(mcx, state_mcx)
        .expect("end_tup_output(basebackup result)");

    // `in_flight` (and thus `ctx`) drops here, reclaiming the result set's arena.
}

/// Install the four `DestRemoteSimple` result-set seams.
pub fn init_seams() {
    ::backup_copy_seams::create_dest_remote_simple::set(create_dest_remote_simple);
    ::backup_copy_seams::begin_tup_output_tupdesc::set(begin_tup_output_tupdesc);
    ::backup_copy_seams::do_tup_output::set(do_tup_output);
    ::backup_copy_seams::end_tup_output::set(end_tup_output);
}
