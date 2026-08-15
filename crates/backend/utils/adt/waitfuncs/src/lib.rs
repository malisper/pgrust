//! waitfuncs.c: pg_isolation_test_session_is_blocked.

use core::sync::atomic::Ordering::Relaxed;

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

pub fn fc_pg_isolation_test_session_is_blocked(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let blocked_pid = fcinfo.arg_i32(0);
    // SAFETY: catalog arg 1 is int4[]; strict fn.
    let interesting = unsafe { fcinfo.arg_varlena_packed(1)? };

    let Some(proc) = procarray::BackendPidGetProc(blocked_pid) else {
        return Ok(Datum::from_bool(false));
    };
    let wait_event_type =
        waitevent::pgstat_get_wait_event_type(proc.wait_event_info.load(Relaxed));
    if wait_event_type == Some("InjectionPoint") {
        return Ok(Datum::from_bool(true));
    }

    let interesting_pids = int4_array_values(interesting.data())?;
    let blocking_pids = lockfuncs::blocking_pids(blocked_pid)?;
    if blocking_pids.iter().any(|bp| interesting_pids.contains(bp)) {
        return Ok(Datum::from_bool(true));
    }

    if !predicate::GetSafeSnapshotBlockingPids(blocked_pid, 1)?.is_empty() {
        return Ok(Datum::from_bool(true));
    }

    Ok(Datum::from_bool(false))
}

// Payload past varlena header: ndim, dataoffset, elemtype, dims[], lbound[], data.
fn int4_array_values(payload: &[u8]) -> PgResult<Vec<i32>> {
    if payload.len() < 12 {
        return Ok(Vec::new());
    }
    let ndim = i32::from_ne_bytes(payload[0..4].try_into().unwrap());
    if ndim <= 0 {
        return Ok(Vec::new());
    }
    let nd = ndim as usize;
    let dims_end = 12 + 4 * nd;
    if payload.len() < dims_end {
        return Ok(Vec::new());
    }
    let mut nelems: usize = 1;
    for i in 0..nd {
        let dim = i32::from_ne_bytes(payload[12 + 4 * i..16 + 4 * i].try_into().unwrap());
        nelems = nelems.saturating_mul(dim.max(0) as usize);
    }
    let dataoffset = i32::from_ne_bytes(payload[4..8].try_into().unwrap());
    if dataoffset != 0 && bitmap_contains_nulls(payload, nd, nelems) {
        return Err(Box::new(
            PgError::error("array must not contain nulls").with_sqlstate(ERRCODE_INTERNAL_ERROR),
        ));
    }
    let data_off = if dataoffset != 0 {
        (dataoffset as usize).saturating_sub(4)
    } else {
        12 + 8 * nd
    };
    let need = data_off.saturating_add(nelems.saturating_mul(4));
    if payload.len() < need {
        return Ok(Vec::new());
    }
    let data = &payload[data_off..data_off + nelems * 4];
    Ok(data
        .chunks_exact(4)
        .map(|c| i32::from_ne_bytes(c.try_into().unwrap()))
        .collect())
}

fn bitmap_contains_nulls(payload: &[u8], ndim: usize, nelems: usize) -> bool {
    let off = 12 + 8 * ndim;
    let nbytes = nelems.div_ceil(8);
    if payload.len() < off + nbytes {
        return true;
    }
    let bitmap = &payload[off..off + nbytes];
    let mut left = nelems;
    for &byte in bitmap {
        if left >= 8 {
            if byte != 0xFF {
                return true;
            }
            left -= 8;
        } else {
            let mask = (1u8 << left) - 1;
            return byte & mask != mask;
        }
    }
    false
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const WAITFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(3378, "pg_isolation_test_session_is_blocked", 2, fc_pg_isolation_test_session_is_blocked),
];
