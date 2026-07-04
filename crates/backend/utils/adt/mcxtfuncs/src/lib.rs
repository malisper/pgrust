//! mcxtfuncs.c over the mcxt_stats root-context forest (no single
//! TopMemoryContext here — thread-native divergence).

use core::sync::atomic::Ordering::Relaxed;

use ::datum::Datum;
use ::mcx::{Mcx, TreeStats};
use ::types_error::{ErrorLocation, PgResult, WARNING};
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use ::types_storage::ProcSignalReason::PROCSIG_LOG_MEMORY_CONTEXT;
use elog::ereport;

const MEMORY_CONTEXT_IDENT_DISPLAY_SIZE: usize = 1024;
const COLS: usize = 10;
const INT4OID: types_core::Oid = 23;

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/utils/adt/mcxtfuncs.c", 0, funcname)
}

fn text_datum(mcx: Mcx<'_>, s: &[u8]) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, s)?))
}

fn int4_array_datum(mcx: Mcx<'_>, vals: &[i32]) -> PgResult<Datum> {
    let mut v: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, vals.len())?;
    v.extend(vals.iter().map(|&i| Datum::from_i32(i)));
    let img = datum::array_build::construct_array_image(mcx, &v, INT4OID, 4, true, b'i')?;
    let img = img.leak();
    Ok(Datum::from_usize(img.as_ptr() as usize))
}

fn put_context_row(
    srf: &mut funcapi::MaterializedSRF<'_>,
    mcx: Mcx<'_>,
    t: &TreeStats,
    path: &[i32],
) -> PgResult<()> {
    let mut values = [Datum::from_usize(0); COLS];
    let mut nulls = [false; COLS];

    let mut name: &str = t.name;
    let mut ident: Option<&str> = t.ident.as_deref();
    if let Some(id) = ident {
        if name == "dynahash" {
            name = id;
            ident = None;
        }
    }
    values[0] = text_datum(mcx, name.as_bytes())?;
    match ident {
        Some(id) => {
            let bytes = id.as_bytes();
            let mut idlen = bytes.len();
            if idlen >= MEMORY_CONTEXT_IDENT_DISPLAY_SIZE {
                idlen = mbutils::pg_mbcliplen(
                    bytes,
                    idlen as i32,
                    (MEMORY_CONTEXT_IDENT_DISPLAY_SIZE - 1) as i32,
                ) as usize;
            }
            values[1] = text_datum(mcx, &bytes[..idlen])?;
        }
        None => nulls[1] = true,
    }
    values[2] = text_datum(mcx, t.kind.as_bytes())?;
    values[3] = Datum::from_i32(path.len() as i32);
    values[4] = int4_array_datum(mcx, path)?;
    // C divergence: allocator-native accounting — block footprint where
    // tracked, charged bytes otherwise; free-chunk counts unavailable.
    let total = if t.arena_footprint > 0 { t.arena_footprint } else { t.used };
    let total = total.max(t.used);
    values[5] = Datum::from_i64(total as i64);
    values[6] = Datum::from_i64(t.nblocks.max(1) as i64);
    values[7] = Datum::from_i64((total - t.used) as i64);
    values[8] = Datum::from_i64(0);
    values[9] = Datum::from_i64(t.used as i64);

    srf.putvalues(&values, &nulls)
}

pub fn fc_pg_get_backend_memory_contexts(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_backend_memory_contexts: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts as usize, COLS);

    // breadth-first ids (C keeps them stable near the roots)
    let forest = mcxt_stats::backend_context_forest();
    let mut queue: std::collections::VecDeque<(&TreeStats, Vec<i32>)> = Default::default();
    let mut next_id = 1i32;
    for root in &forest {
        queue.push_back((root, Vec::new()));
    }
    while let Some((node, parent_path)) = queue.pop_front() {
        let mut path = parent_path;
        path.push(next_id);
        next_id += 1;
        put_context_row(&mut srf, mcx, node, &path)?;
        for child in &node.children {
            queue.push_back((child, path.clone()));
        }
    }

    Ok(srf.finish(fcinfo))
}

pub fn fc_pg_log_backend_memory_contexts(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let pid = fcinfo.arg_i32(0);

    let mut proc = procarray::BackendPidGetProc(pid);
    if proc.is_none() {
        proc = lmgr_proc::ProcGlobal()
            .allProcs
            .iter()
            .find(|p| pid != 0 && p.pid.load(Relaxed) == pid);
    }
    let Some(proc) = proc else {
        ereport(WARNING)
            .errmsg(format!("PID {pid} is not a PostgreSQL server process"))
            .finish(loc("pg_log_backend_memory_contexts"))?;
        return Ok(Datum::from_bool(false));
    };

    let proc_number = proc.vxid.procNumber.load(Relaxed);
    if procsignal::SendProcSignal(pid, PROCSIG_LOG_MEMORY_CONTEXT, proc_number) < 0 {
        ereport(WARNING)
            .errmsg(format!("could not send signal to process {pid}"))
            .finish(loc("pg_log_backend_memory_contexts"))?;
        return Ok(Datum::from_bool(false));
    }

    Ok(Datum::from_bool(true))
}

const fn b(foid: types_core::Oid, name: &'static str, retset: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: if retset { 0 } else { 1 }, strict: true, retset, func }
}

pub const MCXTFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(2282, "pg_get_backend_memory_contexts", true, fc_pg_get_backend_memory_contexts),
    b(4543, "pg_log_backend_memory_contexts", false, fc_pg_log_backend_memory_contexts),
];
