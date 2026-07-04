//! Type I/O for pg_ndistinct/pg_dependencies/pg_mcv_list plus the
//! pg_stats_ext_mcvlist_items inspection SRF
//! (mvdistinct.c/dependencies.c/mcv.c).

use core::fmt::Write;
use datum::Datum;
use mcx::{Mcx, MemoryContext};
use types_core::{InvalidOid, Oid, BOOLOID, TEXTOID};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

#[cold]
#[inline(never)]
fn no_flinfo(name: &str) -> ! {
    panic!("{name}: result needs a resolved FmgrInfo's scratch")
}

// C pallocs each result per call; the resolved FmgrInfo owns retained scratch
// (ruleutils builtins precedent). The Datum aliases it until the next call
// through the same FmgrInfo.
struct OutBuf(Vec<u8>);

fn cstring_result(flinfo: Option<&mut FmgrInfo>, name: &'static str, s: &str) -> Datum {
    let Some(flinfo) = flinfo else { no_flinfo(name) };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutBuf(Vec::new()));
    }
    let buf = &mut flinfo.fn_extra_mut::<OutBuf>().unwrap().0;
    buf.clear();
    buf.reserve(s.len() + 1);
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
    Datum::from_usize(buf.as_ptr() as usize)
}

fn cannot_accept(typname: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("cannot accept a value of type {typname}"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

pub fn fc_pg_ndistinct_in(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(cannot_accept("pg_ndistinct"))
}

pub fn fc_pg_ndistinct_recv(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Err(cannot_accept("pg_ndistinct"))
}

pub fn fc_pg_ndistinct_out(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_ndistinct_out");
    // SAFETY: arg 0 is a live bytea datum.
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let nd = crate::mvdistinct::statext_ndistinct_deserialize(ctx.mcx(), v.data())?;
    let mut s = String::from("{");
    for (i, item) in nd.items.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        for (j, a) in item.attributes.iter().enumerate() {
            s.push_str(if j == 0 { "\"" } else { ", " });
            let _ = write!(s, "{a}");
        }
        let _ = write!(s, "\": {}", item.ndistinct as i32);
    }
    s.push('}');
    Ok(cstring_result(flinfo, "pg_ndistinct_out", &s))
}

pub fn fc_pg_dependencies_in(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Err(cannot_accept("pg_dependencies"))
}

pub fn fc_pg_dependencies_recv(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Err(cannot_accept("pg_dependencies"))
}

pub fn fc_pg_dependencies_out(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let ctx = MemoryContext::new("pg_dependencies_out");
    // SAFETY: arg 0 is a live bytea datum.
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let deps = crate::dependencies::statext_dependencies_deserialize(ctx.mcx(), v.data())?;
    let mut s = String::from("{");
    for (i, dep) in deps.deps.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push('"');
        let n = dep.attributes.len();
        for (j, a) in dep.attributes.iter().enumerate() {
            if j == n - 1 {
                s.push_str(" => ");
            } else if j > 0 {
                s.push_str(", ");
            }
            let _ = write!(s, "{a}");
        }
        let _ = write!(s, "\": {:.6}", dep.degree);
    }
    s.push('}');
    Ok(cstring_result(flinfo, "pg_dependencies_out", &s))
}

pub fn fc_pg_mcv_list_in(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(cannot_accept("pg_mcv_list"))
}

pub fn fc_pg_mcv_list_recv(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Err(cannot_accept("pg_mcv_list"))
}

// PG_GETARG_BYTEA_P: detoasted, short headers expanded to the 4-byte form.
unsafe fn bytea_body<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    // SAFETY: forwarded caller contract (strict fn, non-null varlena arg).
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    if v.is_short() {
        v.data_expanded(fcinfo.result_mcx())
    } else {
        Ok(v.data())
    }
}

fn output_datum_text(mcx: Mcx<'_>, typid: Oid, value: Datum) -> PgResult<Datum> {
    let (typoutput, _isvarlena) = lsyscache::typ::getTypeOutputInfo(typid)?;
    let mut finfo = fmgr_seams::fmgr_info::call(typoutput)?;
    let d = types_fmgr::function_call1_coll_in(&mut finfo, InvalidOid, mcx, value)?;
    // SAFETY: out functions return a NUL-terminated cstring datum; copied into
    // a text varlena before finfo (and its scratch) dies.
    let s = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    Ok(types_fmgr::varlena_result(varlena::cstring_to_text(mcx, s.to_bytes())?))
}

fn fc_pg_mcv_list_items(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_mcv_list_items: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: strict fn; arg 0 is a non-null pg_mcv_list varlena.
    let body = unsafe { bytea_body(fcinfo, 0)? };
    let mcvlist = crate::mcv::statext_mcv_deserialize(mcx, body)?;

    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    for (idx, item) in mcvlist.items.iter().enumerate() {
        let ndim = mcvlist.ndimensions;
        let mut val_elems = Vec::with_capacity(ndim);
        let mut val_nulls = Vec::with_capacity(ndim);
        let mut null_elems = Vec::with_capacity(ndim);
        for i in 0..ndim {
            null_elems.push(Datum::from_bool(item.isnull[i]));
            if item.isnull[i] {
                val_elems.push(Datum::null());
                val_nulls.push(true);
            } else {
                val_elems.push(output_datum_text(mcx, mcvlist.types[i], item.values[i])?);
                val_nulls.push(false);
            }
        }
        let values_arr = arrayfuncs::construct_md_array(
            mcx,
            &val_elems,
            Some(&val_nulls),
            1,
            &[ndim as i32],
            &[1],
            TEXTOID,
            -1,
            false,
            b'i',
        )?;
        let nulls_arr = arrayfuncs::construct_md_array(
            mcx,
            &null_elems,
            None,
            1,
            &[ndim as i32],
            &[1],
            BOOLOID,
            1,
            true,
            b'c',
        )?;
        let values = [
            Datum::from_i32(idx as i32),
            Datum::from_usize(values_arr.leak().as_ptr() as usize),
            Datum::from_usize(nulls_arr.leak().as_ptr() as usize),
            Datum::from_f64(item.frequency),
            Datum::from_f64(item.base_frequency),
        ];
        srf.putvalues(&values, &[false; 5])?;
    }
    Ok(srf.finish(fcinfo))
}

const fn b(foid: types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn bs(foid: types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

pub const STATISTICS_BUILTINS: &[FmgrBuiltin] = &[
    b(3355, "pg_ndistinct_in", 1, fc_pg_ndistinct_in),
    b(3356, "pg_ndistinct_out", 1, fc_pg_ndistinct_out),
    b(3357, "pg_ndistinct_recv", 1, fc_pg_ndistinct_recv),
    b(3358, "pg_ndistinct_send", 1, varlena::builtins::fc_byteasend),
    b(3404, "pg_dependencies_in", 1, fc_pg_dependencies_in),
    b(3405, "pg_dependencies_out", 1, fc_pg_dependencies_out),
    b(3406, "pg_dependencies_recv", 1, fc_pg_dependencies_recv),
    b(3407, "pg_dependencies_send", 1, varlena::builtins::fc_byteasend),
    bs(3427, "pg_stats_ext_mcvlist_items", 1, fc_pg_mcv_list_items),
    b(5018, "pg_mcv_list_in", 1, fc_pg_mcv_list_in),
    b(5019, "pg_mcv_list_out", 1, varlena::builtins::fc_byteaout),
    b(5020, "pg_mcv_list_recv", 1, fc_pg_mcv_list_recv),
    b(5021, "pg_mcv_list_send", 1, varlena::builtins::fc_byteasend),
];
