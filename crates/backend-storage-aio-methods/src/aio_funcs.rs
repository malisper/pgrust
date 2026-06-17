//! `storage/aio/aio_funcs.c` — the SQL interface for AIO: the `pg_get_aios()`
//! set-returning function that introspects every in-flight AIO handle.
//!
//! The lock-free copy/retry protocol of the C function is preserved exactly:
//! there is no lock that can stop an IO's state advancing concurrently, so for
//! each handle we (1) read its generation + state, (2) snapshot it into local
//! memory, (3) re-read generation/state — if the generation changed the IO was
//! recycled and is skipped, if only the state changed we retry the render.
//!
//! The `InitMaterializedSRF` / `ReturnSetInfo` / `Datum` set-returning-function
//! protocol and the `(Datum) 0` return belong to the project-wide fmgr/Datum
//! deferral; the algorithm is [`pg_get_aios_core`], which emits each rendered
//! [`AioRow`] through the `tuplestore_putvalues` seam (folding the inline
//! `Int32GetDatum`/`Int64GetDatum`/`BoolGetDatum`/`CStringGetTextDatum` Datum
//! assembly into the consumer). `GetPGProcByNumber(owner)->pid` crosses the
//! `proc_pid_by_number` seam into the unported proc.c PGPROC array.

extern crate alloc;

use alloc::string::ToString;
use core::sync::atomic::Ordering;

use types_error::PgResult;

use backend_storage_aio_funcs_seams::{self as seam, AioRow};

use crate::aio::{
    ioh, pgaio_io_get_id, pgaio_io_get_state_name, pgaio_result_status_string,
};
use crate::aio_io::pgaio_io_get_op_name;
use crate::aio_target::{pgaio_io_get_target_description, pgaio_io_get_target_name};
use crate::{
    pgaio_ctl, Iovec, PgAioHandleData, PgAioHandleState, PGAIO_HF_BUFFERED,
    PGAIO_HF_REFERENCES_LOCAL, PGAIO_HF_SYNCHRONOUS, PGAIO_OP_INVALID, PGAIO_OP_READV,
    PGAIO_OP_WRITEV, PG_IOV_MAX,
};

/// `static size_t iov_byte_length(const struct iovec *iov, int cnt)`
/// (aio_funcs.c) — byte length of an iovec.
fn iov_byte_length(iov: &[Iovec], cnt: i32) -> usize {
    let mut len = 0usize;
    for i in 0..cnt as usize {
        len += iov[i].iov_len;
    }
    len
}

/// `Datum pg_get_aios(PG_FUNCTION_ARGS)` (aio_funcs.c) — the per-handle render
/// loop, minus the fmgr `InitMaterializedSRF` set-up. Emits each rendered row
/// through the `tuplestore_putvalues` seam; returns `()` (C `return (Datum) 0`).
pub fn pg_get_aios_core() -> PgResult<()> {
    // InitMaterializedSRF(fcinfo, 0);  -- performed by the fmgr entry point.

    let ctl = pgaio_ctl();

    // for (uint64 i = 0; i < pgaio_ctl->io_handle_count; i++)
    for i in 0..ctl.io_handle_count as usize {
        // PgAioHandle *live_ioh = &pgaio_ctl->io_handles[i];
        let live = ioh(i);
        // int ioh_id = pgaio_io_get_id(live_ioh);
        let ioh_id = pgaio_io_get_id(i);

        // There is no lock that could prevent the state of the IO advancing
        // concurrently. So:
        //   1) Determine the state + generation of the IO.
        //   2) Copy the IO to local memory.
        //   3) Check if state or generation changed. If the state changed,
        //      retry; if the generation changed, don't display the IO.

        // 1) start_generation = live_ioh->generation;
        let start_generation = live.generation.load(Ordering::Relaxed);

        // Retry here so we accept changing states, but not changing generations.
        let row = 'retry: loop {
            // pg_read_barrier(); start_state = live_ioh->state;
            // (Acquire load subsumes the read barrier.)
            let start_state = live.state();

            // if (start_state == PGAIO_HS_IDLE) continue;
            if start_state == PgAioHandleState::Idle {
                break 'retry None;
            }

            // 2) memcpy(&ioh_copy, live_ioh, sizeof(PgAioHandle));
            let data_copy: PgAioHandleData = live.data().clone();
            let iovec_off = live.iovec_off as usize;
            // Safe to copy even if no iovec is used - we always reserve the
            // required space:
            //   memcpy(&iov_copy, &pgaio_ctl->iovecs[ioh_copy.iovec_off],
            //          PG_IOV_MAX * sizeof(struct iovec));
            let iov_copy: alloc::vec::Vec<Iovec> = {
                let iovecs = ctl.iovecs.lock().unwrap();
                iovecs[iovec_off..iovec_off + PG_IOV_MAX].to_vec()
            };

            // Copy owner info before 3) below; if the process exited it'd have
            // to wait for the IO to finish first, which we'd detect in 3).
            //   owner = ioh_copy.owner_procno;
            //   owner_proc = GetPGProcByNumber(owner);
            //   owner_pid = owner_proc->pid;
            let owner = live.owner_procno;
            let owner_pid = seam::proc_pid_by_number::call(owner)?;

            // 3) pg_read_barrier();
            // The IO completed and a new one started with the same ID. Don't
            // display it - it really started after this function was called.
            //   if (live_ioh->generation != start_generation) continue;
            if live.generation.load(Ordering::Acquire) != start_generation {
                break 'retry None;
            }

            // The IO's state changed while we were rendering it. Start from
            // scratch (no livelock risk: state changes go one direction).
            //   if (live_ioh->state != start_state) goto retry;
            if live.state() != start_state {
                continue 'retry;
            }

            // Now that we've copied the IO into local memory and confirmed it's
            // still in the same state, we are not allowed to access "live"
            // memory anymore. (C nulls out live_ioh / owner_proc here.)

            let mut r = AioRow::default();

            // column: owning pid
            //   if (owner_pid != 0) values[0] = Int32GetDatum(owner_pid);
            //   else nulls[0] = false;
            // The else branch sets nulls[0] = *false* (not true), so even when
            // owner_pid == 0 the column is emitted as Int32GetDatum(0) — never
            // NULL. Replicate that exactly: always Some(owner_pid).
            r.pid = Some(owner_pid);

            // column: IO's id
            r.io_id = ioh_id;
            // column: IO's generation
            r.io_generation = start_generation as i64;
            // column: IO's state
            r.state = pgaio_io_get_state_name(i).to_string();

            // If the IO is in PGAIO_HS_HANDED_OUT state, none of the following
            // fields are valid yet. Don't display any other columns.
            //   memset(nulls + 4, 1, ...); goto display;
            if start_state == PgAioHandleState::HandedOut {
                break 'retry Some(r);
            }

            // column: IO's operation
            r.operation = Some(pgaio_io_get_op_name(i).to_string());

            // columns: details about the IO's operation (offset, length)
            match data_copy.op {
                PGAIO_OP_INVALID => {
                    // nulls[5] = true; nulls[6] = true;
                }
                PGAIO_OP_READV | PGAIO_OP_WRITEV => {
                    r.off = Some(data_copy.op_data.offset as i64);
                    r.length = Some(iov_byte_length(
                        &iov_copy,
                        data_copy.op_data.iov_length as i32,
                    ) as i64);
                }
                _ => {}
            }

            // column: IO's target
            r.target = Some(pgaio_io_get_target_name(i).to_string());

            // column: length of IO's data array
            r.handle_data_len = Some(data_copy.handle_data_len as i16);

            // column: raw result (some form of syscall return value)
            if start_state == PgAioHandleState::CompletedIo
                || start_state == PgAioHandleState::CompletedShared
                || start_state == PgAioHandleState::CompletedLocal
            {
                r.raw_result = Some(live.result.load(Ordering::Relaxed));
            }

            // column: result in the higher-level representation (unknown if not
            // finished)
            r.result = Some(pgaio_result_status_string(data_copy.distilled_result.status).to_string());

            // column: target description
            r.target_desc = Some(pgaio_io_get_target_description(i)?);

            // columns: one for each flag
            r.f_sync = Some(data_copy.flags & PGAIO_HF_SYNCHRONOUS != 0);
            r.f_localmem = Some(data_copy.flags & PGAIO_HF_REFERENCES_LOCAL != 0);
            r.f_buffered = Some(data_copy.flags & PGAIO_HF_BUFFERED != 0);

            break 'retry Some(r);
        };

        // display: tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, ...)
        if let Some(row) = row {
            seam::tuplestore_putvalues::call(row)?;
        }
    }

    // return (Datum) 0;
    Ok(())
}

/// `pg_get_aios` — the SQL set-returning function (fmgr entry point).
///
/// The `ReturnSetInfo` / `InitMaterializedSRF` set-returning-function protocol
/// and the `(Datum) 0` return belong to the project-wide fmgr/Datum-layer
/// deferral; the algorithm is [`pg_get_aios_core`]. Loud panic until the fmgr
/// boundary is unified.
pub fn pg_get_aios() -> ! {
    panic!("fmgr/Datum-layer deferral: pg_get_aios (aio_funcs.c)")
}
