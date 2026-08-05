//! waitfuncs.c: pg_isolation_test_session_is_blocked.

use core::sync::atomic::Ordering::Relaxed;

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

pub fn fc_pg_isolation_test_session_is_blocked(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let blocked_pid = fcinfo.arg_i32(0);
    // SAFETY: catalog arg 1 is int4[]; strict fn.
    let interesting = unsafe { fcinfo.arg_varlena_packed(1)? };
    let interesting_pids = int4_array_values(interesting.data());

    let Some(proc) = procarray::BackendPidGetProc(blocked_pid) else {
        return Ok(Datum::from_bool(false));
    };
    let wait_event_type =
        waitevent::pgstat_get_wait_event_type(proc.wait_event_info.load(Relaxed));
    if wait_event_type == Some("InjectionPoint") {
        return Ok(Datum::from_bool(true));
    }

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
fn int4_array_values(payload: &[u8]) -> Vec<i32> {
    if payload.len() < 12 {
        return Vec::new();
    }
    let ndim = i32::from_ne_bytes(payload[0..4].try_into().unwrap());
    if ndim == 0 {
        return Vec::new();
    }
    assert_eq!(ndim, 1, "int4[] argument must be 1-D");
    assert_eq!(
        i32::from_ne_bytes(payload[4..8].try_into().unwrap()),
        0,
        "array must not contain nulls"
    );
    let nelems = i32::from_ne_bytes(payload[12..16].try_into().unwrap()) as usize;
    let data = &payload[20..20 + nelems * 4];
    data.chunks_exact(4)
        .map(|c| i32::from_ne_bytes(c.try_into().unwrap()))
        .collect()
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const WAITFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(3378, "pg_isolation_test_session_is_blocked", 2, fc_pg_isolation_test_session_is_blocked),
];
