//! aio_funcs.c: pg_get_aios walks pgaio_ctl->io_handles — the shared handle
//! table aio_core keeps for every backend — and emits one row per non-IDLE
//! handle of any backend, copied under C's no-lock state/generation
//! re-check protocol (aio_core::pgaio_io_snapshot).

use datum::Datum;
use mcx::Mcx;
use types_error::PgResult;
use types_fmgr::varlena_result;
use types_storage::aio::{
    PGAIO_HF_BUFFERED, PGAIO_HF_REFERENCES_LOCAL, PGAIO_HF_SYNCHRONOUS, PGAIO_OP_INVALID,
    PGAIO_OP_READV, PGAIO_OP_WRITEV, PGAIO_TID_SMGR,
};

use aio_core::{
    PGAIO_HS_COMPLETED_IO, PGAIO_HS_COMPLETED_LOCAL, PGAIO_HS_COMPLETED_SHARED,
    PGAIO_HS_HANDED_OUT,
};

const PG_GET_AIOS_COLS: usize = 15;

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?))
}

/// smgr.c smgr_aio_describe_identity (the PGAIO_TID_SMGR describe_identity
/// callback pgaio_io_get_target_description dispatches to). C resolves a
/// temp relation's path with the VIEWER's MyProcNumber.
fn smgr_describe_identity(sd: &types_storage::aio::PgAioTargetSmgr) -> String {
    let backend = if sd.is_temp {
        init_small::globals::MyProcNumber()
    } else {
        types_core::INVALID_PROC_NUMBER
    };
    let path = relpath_seams::relpathbackend::call(sd.rlocator, backend, sd.forkNum);
    if sd.nblocks == 0 {
        format!("file \"{path}\"")
    } else if sd.nblocks == 1 {
        format!("block {} in file \"{path}\"", sd.blockNum)
    } else {
        format!(
            "blocks {}..{} in file \"{path}\"",
            sd.blockNum,
            sd.blockNum.wrapping_add(sd.nblocks).wrapping_sub(1)
        )
    }
}

pub fn fc_pg_get_aios(
    flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    let flinfo = flinfo.expect("pg_get_aios: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts as usize, PG_GET_AIOS_COLS);

    for i in 0..aio_core::pgaio_io_handle_count() {
        // 1)-3) of aio_funcs.c:71-136: IDLE and recycled handles are skipped,
        // a state change mid-copy retries.
        let Some(ioh) = aio_core::pgaio_io_snapshot(i as u32) else {
            continue;
        };

        let mut values = [Datum::null(); PG_GET_AIOS_COLS];
        let mut nulls = [false; PG_GET_AIOS_COLS];

        // Owner pid, read after the copy like C (a process that exited would
        // have waited for the IO first, which the generation check catches).
        let owner_pid = lmgr_proc::GetPGProcByNumber(ioh.owner_procno)
            .pid
            .load(std::sync::atomic::Ordering::Relaxed);

        // column: owning pid
        if owner_pid != 0 {
            values[0] = Datum::from_i32(owner_pid);
        } else {
            nulls[0] = true;
        }

        // column: IO's id
        values[1] = Datum::from_i32(ioh.id);

        // column: IO's generation
        values[2] = Datum::from_i64(ioh.generation as i64);

        // column: IO's state
        values[3] = text_datum(mcx, aio_core::pgaio_io_state_name(ioh.state))?;

        // If the IO is in PGAIO_HS_HANDED_OUT state, none of the following
        // fields are valid yet (or are in the process of being set).
        // Therefore we don't want to display any other columns.
        if ioh.state == PGAIO_HS_HANDED_OUT {
            for n in nulls.iter_mut().skip(4) {
                *n = true;
            }
            srf.putvalues(&values, &nulls)?;
            continue;
        }

        // column: IO's operation
        values[4] = text_datum(mcx, aio_core::pgaio_io_op_name(ioh.op))?;

        // columns: details about the IO's operation (offset, length)
        match ioh.op {
            PGAIO_OP_READV | PGAIO_OP_WRITEV => {
                values[5] = Datum::from_i64(ioh.op_offset as i64);
                values[6] = Datum::from_i64(ioh.iov_bytes);
            }
            _ => {
                debug_assert_eq!(ioh.op, PGAIO_OP_INVALID);
                nulls[5] = true;
                nulls[6] = true;
            }
        }

        // column: IO's target
        values[7] = text_datum(mcx, aio_core::pgaio_io_target_name(ioh.target))?;

        // column: length of IO's data array
        values[8] = Datum::from_i16(ioh.handle_data_len as i16);

        // column: raw result (i.e. some form of syscall return value)
        if matches!(
            ioh.state,
            PGAIO_HS_COMPLETED_IO | PGAIO_HS_COMPLETED_SHARED | PGAIO_HS_COMPLETED_LOCAL
        ) {
            values[9] = Datum::from_i32(ioh.result);
        } else {
            nulls[9] = true;
        }

        // column: result in the higher level representation (unknown if not
        // finished)
        values[10] =
            text_datum(mcx, aio_core::pgaio_result_status_string(ioh.distilled_status))?;

        // column: target description (aio_target.c pgaio_io_get_target_description)
        if ioh.target == PGAIO_TID_SMGR {
            values[11] = text_datum(mcx, &smgr_describe_identity(&ioh.target_data.smgr))?;
        } else {
            // C only has the smgr target with a describe_identity; a defined
            // IO always carries it (pgaio_io_stage asserts a target).
            nulls[11] = true;
        }

        // columns: one for each flag
        values[12] = Datum::from_bool(ioh.flags & PGAIO_HF_SYNCHRONOUS != 0);
        values[13] = Datum::from_bool(ioh.flags & PGAIO_HF_REFERENCES_LOCAL != 0);
        values[14] = Datum::from_bool(ioh.flags & PGAIO_HF_BUFFERED != 0);

        srf.putvalues(&values, &nulls)?;
    }

    Ok(srf.finish(fcinfo))
}

pub const AIO_FUNCS_BUILTINS: &[types_fmgr::FmgrBuiltin] = &[types_fmgr::FmgrBuiltin {
    foid: 6399,
    name: "pg_get_aios",
    nargs: 0,
    strict: true,
    retset: true,
    func: fc_pg_get_aios,
}];
