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
    let _num_interesting = int4_array_nelems(interesting.data());

    let Some(proc) = procarray::BackendPidGetProc(blocked_pid) else {
        return Ok(Datum::from_bool(false));
    };
    let wait_event_type =
        waitevent::pgstat_get_wait_event_type(proc.wait_event_info.load(Relaxed));
    if wait_event_type == Some("InjectionPoint") {
        return Ok(Datum::from_bool(true));
    }

    panic!(
        "unported: pg_isolation_test_session_is_blocked heavyweight-lock arm \
         (lockfuncs.c pg_blocking_pids / lock.c GetBlockerStatusData)"
    );
}

// Array payload (past the varlena header): ndim, dataoffset, elemtype, dims[].
fn int4_array_nelems(payload: &[u8]) -> usize {
    if payload.len() < 12 {
        return 0;
    }
    let ndim = i32::from_ne_bytes(payload[0..4].try_into().unwrap());
    if ndim == 0 {
        return 0;
    }
    assert_eq!(ndim, 1, "int4[] argument must be 1-D");
    assert_eq!(
        i32::from_ne_bytes(payload[4..8].try_into().unwrap()),
        0,
        "array must not contain nulls"
    );
    i32::from_ne_bytes(payload[12..16].try_into().unwrap()) as usize
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const WAITFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(3378, "pg_isolation_test_session_is_blocked", 2, fc_pg_isolation_test_session_is_blocked),
];
