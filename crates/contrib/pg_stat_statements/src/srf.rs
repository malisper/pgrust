//! SQL surface: pg_stat_statements() across API versions 1.0–1.12, the
//! reset functions, and pg_stat_statements_info().

use datum::Datum;
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::store::entry_reset;
use crate::{
    not_loaded_error, resolved_flinfo, text_datum, Counters, PGSS, PGSS_EXEC, PGSS_NUMKIND,
};

const ROLE_PG_READ_ALL_STATS: types_core::Oid = 3375;

#[derive(Clone, Copy, PartialEq, PartialOrd)]
enum ApiVersion {
    V1_0,
    V1_1,
    V1_2,
    V1_3,
    V1_8,
    V1_9,
    V1_10,
    V1_11,
    V1_12,
}
use ApiVersion::*;

fn expected_cols(v: ApiVersion) -> i32 {
    match v {
        V1_0 => 14,
        V1_1 => 18,
        V1_2 => 19,
        V1_3 => 23,
        V1_8 => 32,
        V1_9 => 33,
        V1_10 => 43,
        V1_11 => 49,
        V1_12 => 52,
    }
}

pub(crate) fn fc_reset(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    entry_reset(0, 0, 0, false)?;
    Ok(Datum::from_usize(0))
}

pub(crate) fn fc_reset_1_7(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (userid, dbid, queryid) =
        (fcinfo.arg_oid(0), fcinfo.arg_oid(1), fcinfo.arg_i64(2));
    entry_reset(userid, dbid, queryid, false)?;
    Ok(Datum::from_usize(0))
}

pub(crate) fn fc_reset_1_11(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (userid, dbid, queryid, minmax_only) =
        (fcinfo.arg_oid(0), fcinfo.arg_oid(1), fcinfo.arg_i64(2), fcinfo.arg_bool(3));
    let ts = entry_reset(userid, dbid, queryid, minmax_only)?;
    Ok(Datum::from_i64(ts))
}

macro_rules! pgss_fc {
    ($name:ident, $ver:expr, $showtext_from_arg:expr) => {
        pub(crate) fn $name(
            flinfo: Option<&mut FmgrInfo>,
            fcinfo: &mut Fcinfo,
        ) -> PgResult<Datum> {
            let flinfo = resolved_flinfo(flinfo, stringify!($name));
            let showtext = if $showtext_from_arg { fcinfo.arg_bool(0) } else { true };
            pg_stat_statements_internal(flinfo, fcinfo, $ver, showtext)
        }
    };
}

pgss_fc!(fc_pgss_1_0, V1_0, false);
pgss_fc!(fc_pgss_1_2, V1_2, true);
pgss_fc!(fc_pgss_1_3, V1_3, true);
pgss_fc!(fc_pgss_1_8, V1_8, true);
pgss_fc!(fc_pgss_1_9, V1_9, true);
pgss_fc!(fc_pgss_1_10, V1_10, true);
pgss_fc!(fc_pgss_1_11, V1_11, true);
pgss_fc!(fc_pgss_1_12, V1_12, true);

fn numeric_datum(fcinfo: &Fcinfo, v: u64) -> PgResult<Datum> {
    let img = adt_numeric::numeric_in(&v.to_string(), -1, None)?
        .expect("u64 decimal string is valid numeric");
    let mcx = fcinfo.result_mcx();
    let mut buf: mcx::PgVec<'_, u8> = mcx::PgVec::new_in(mcx);
    mcx::vec_append_bytes(&mut buf, img.as_bytes())
        .map_err(|_| mcx.oom(img.as_bytes().len()))?;
    Ok(types_fmgr::varlena_result(datum::Varlena::from_image(buf)))
}

/// `pg_stat_statements_internal`.
fn pg_stat_statements_internal(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    mut api_version: ApiVersion,
    showtext: bool,
) -> PgResult<Datum> {
    let userid = miscinit::GetUserId();
    let is_allowed_role = adt_acl::has_privs_of_role(userid, ROLE_PG_READ_ALL_STATS)?;

    if PGSS.lock().unwrap().is_none() {
        return Err(not_loaded_error());
    }

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    // The 1.0/1.1 kluge: both map to the C symbol pg_stat_statements; the
    // declared OUT-column count tells them apart.
    let natts = srf.tupdesc.natts;
    if natts == expected_cols(V1_1) && api_version == V1_0 {
        api_version = V1_1;
    } else if natts != expected_cols(api_version) {
        return Err(Box::new(types_error::PgError::error(
            "incorrect number of output arguments",
        )));
    }

    let guard = PGSS.lock().unwrap();
    let shared = guard.as_ref().expect("checked above");

    for (key, entry) in shared.hash.iter() {
        let mut values: Vec<Datum> = Vec::with_capacity(natts as usize);
        let mut nulls: Vec<bool> = Vec::with_capacity(natts as usize);
        let mut push = |values: &mut Vec<Datum>, nulls: &mut Vec<bool>, d: Datum| {
            values.push(d);
            nulls.push(false);
        };
        let mut push_null = |values: &mut Vec<Datum>, nulls: &mut Vec<bool>| {
            values.push(Datum::null());
            nulls.push(true);
        };

        let tmp: Counters = entry.counters;
        // Skip unexecuted (pending sticky) entries.
        if tmp.is_sticky() {
            continue;
        }

        push(&mut values, &mut nulls, Datum::from_oid(key.userid));
        push(&mut values, &mut nulls, Datum::from_oid(key.dbid));
        if api_version >= V1_9 {
            push(&mut values, &mut nulls, Datum::from_bool(key.toplevel));
        }

        if is_allowed_role || key.userid == userid {
            if api_version >= V1_2 {
                push(&mut values, &mut nulls, Datum::from_i64(key.queryid));
            }
            if showtext {
                // Inline texts are always the database encoding: C's
                // pg_any_to_server is the identity conversion here.
                push(&mut values, &mut nulls, text_datum(fcinfo, &entry.query_text)?);
            } else {
                push_null(&mut values, &mut nulls);
            }
        } else {
            if api_version >= V1_2 {
                push_null(&mut values, &mut nulls);
            }
            if showtext {
                push(&mut values, &mut nulls, text_datum(fcinfo, "<insufficient privilege>")?);
            } else {
                push_null(&mut values, &mut nulls);
            }
        }

        // PGSS_PLAN block (>=1.8) then PGSS_EXEC block.
        for kind in 0..PGSS_NUMKIND {
            if kind == PGSS_EXEC || api_version >= V1_8 {
                push(&mut values, &mut nulls, Datum::from_i64(tmp.calls[kind]));
                push(&mut values, &mut nulls, Datum::from_f64(tmp.total_time[kind]));
            }
            if (kind == PGSS_EXEC && api_version >= V1_3) || api_version >= V1_8 {
                push(&mut values, &mut nulls, Datum::from_f64(tmp.min_time[kind]));
                push(&mut values, &mut nulls, Datum::from_f64(tmp.max_time[kind]));
                push(&mut values, &mut nulls, Datum::from_f64(tmp.mean_time[kind]));
                // Population stddev (no Bessel correction), as in C.
                let stddev = if tmp.calls[kind] > 1 {
                    (tmp.sum_var_time[kind] / tmp.calls[kind] as f64).sqrt()
                } else {
                    0.0
                };
                push(&mut values, &mut nulls, Datum::from_f64(stddev));
            }
        }
        push(&mut values, &mut nulls, Datum::from_i64(tmp.rows));
        push(&mut values, &mut nulls, Datum::from_i64(tmp.shared_blks_hit));
        push(&mut values, &mut nulls, Datum::from_i64(tmp.shared_blks_read));
        if api_version >= V1_1 {
            push(&mut values, &mut nulls, Datum::from_i64(tmp.shared_blks_dirtied));
        }
        push(&mut values, &mut nulls, Datum::from_i64(tmp.shared_blks_written));
        push(&mut values, &mut nulls, Datum::from_i64(tmp.local_blks_hit));
        push(&mut values, &mut nulls, Datum::from_i64(tmp.local_blks_read));
        if api_version >= V1_1 {
            push(&mut values, &mut nulls, Datum::from_i64(tmp.local_blks_dirtied));
        }
        push(&mut values, &mut nulls, Datum::from_i64(tmp.local_blks_written));
        push(&mut values, &mut nulls, Datum::from_i64(tmp.temp_blks_read));
        push(&mut values, &mut nulls, Datum::from_i64(tmp.temp_blks_written));
        if api_version >= V1_1 {
            push(&mut values, &mut nulls, Datum::from_f64(tmp.shared_blk_read_time));
            push(&mut values, &mut nulls, Datum::from_f64(tmp.shared_blk_write_time));
        }
        if api_version >= V1_11 {
            push(&mut values, &mut nulls, Datum::from_f64(tmp.local_blk_read_time));
            push(&mut values, &mut nulls, Datum::from_f64(tmp.local_blk_write_time));
        }
        if api_version >= V1_10 {
            push(&mut values, &mut nulls, Datum::from_f64(tmp.temp_blk_read_time));
            push(&mut values, &mut nulls, Datum::from_f64(tmp.temp_blk_write_time));
        }
        if api_version >= V1_8 {
            push(&mut values, &mut nulls, Datum::from_i64(tmp.wal_records));
            push(&mut values, &mut nulls, Datum::from_i64(tmp.wal_fpi));
            push(&mut values, &mut nulls, numeric_datum(fcinfo, tmp.wal_bytes)?);
        }
        if api_version >= V1_12 {
            push(&mut values, &mut nulls, Datum::from_i64(tmp.wal_buffers_full));
        }
        if api_version >= V1_10 {
            push(&mut values, &mut nulls, Datum::from_i64(tmp.jit_functions));
            push(&mut values, &mut nulls, Datum::from_f64(tmp.jit_generation_time));
            push(&mut values, &mut nulls, Datum::from_i64(tmp.jit_inlining_count));
            push(&mut values, &mut nulls, Datum::from_f64(tmp.jit_inlining_time));
            push(&mut values, &mut nulls, Datum::from_i64(tmp.jit_optimization_count));
            push(&mut values, &mut nulls, Datum::from_f64(tmp.jit_optimization_time));
            push(&mut values, &mut nulls, Datum::from_i64(tmp.jit_emission_count));
            push(&mut values, &mut nulls, Datum::from_f64(tmp.jit_emission_time));
        }
        if api_version >= V1_11 {
            push(&mut values, &mut nulls, Datum::from_i64(tmp.jit_deform_count));
            push(&mut values, &mut nulls, Datum::from_f64(tmp.jit_deform_time));
        }
        if api_version >= V1_12 {
            push(&mut values, &mut nulls, Datum::from_i64(tmp.parallel_workers_to_launch));
            push(&mut values, &mut nulls, Datum::from_i64(tmp.parallel_workers_launched));
        }
        if api_version >= V1_11 {
            push(&mut values, &mut nulls, Datum::from_i64(entry.stats_since));
            push(&mut values, &mut nulls, Datum::from_i64(entry.minmax_stats_since));
        }

        debug_assert_eq!(values.len() as i32, expected_cols(api_version));
        srf.putvalues(&values, &nulls)?;
    }
    drop(guard);

    Ok(srf.finish(fcinfo))
}

/// `pg_stat_statements_info`.
pub(crate) fn fc_pgss_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = resolved_flinfo(flinfo, "pg_stat_statements_info");
    let stats = {
        let guard = PGSS.lock().unwrap();
        let Some(shared) = guard.as_ref() else { return Err(not_loaded_error()) };
        shared.stats
    };

    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(types_error::PgError::error("return type must be a row type")));
    }
    let tupdesc = resolved.result_tuple_desc.expect("composite result carries a tupdesc");
    let values = [Datum::from_i64(stats.dealloc), Datum::from_i64(stats.stats_reset)];
    let nulls = [false, false];
    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup);
    Ok(d)
}
