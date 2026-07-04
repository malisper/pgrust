//! fmgr wrappers for the extended-statistics type I/O + inspection SRF
//! (mvdistinct.c pg_ndistinct_out, dependencies.c pg_dependencies_out,
//! mcv.c pg_stats_ext_mcvlist_items).

use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, BOOLOID, TEXTOID};
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

fn cstring_out(fcinfo: &Fcinfo, s: &str) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mut v = mcx::vec_with_capacity_in(mcx, s.len() + 1)?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    mcx::vec_append_bytes(&mut v, &[0])?;
    Ok(types_fmgr::cstring_result(v))
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

fn fc_pg_ndistinct_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn; arg 0 is a non-null pg_ndistinct varlena.
    let body = unsafe { bytea_body(fcinfo, 0)? };
    let nd = crate::mvdistinct::statext_ndistinct_deserialize(fcinfo.result_mcx(), body)?;
    let mut out = String::from("{");
    for (i, item) in nd.items.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        for (j, &attnum) in item.attributes.iter().enumerate() {
            out.push_str(if j == 0 { "\"" } else { ", " });
            out.push_str(&attnum.to_string());
        }
        out.push_str("\": ");
        out.push_str(&(item.ndistinct as i32).to_string());
    }
    out.push('}');
    cstring_out(fcinfo, &out)
}

fn fc_pg_dependencies_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn; arg 0 is a non-null pg_dependencies varlena.
    let body = unsafe { bytea_body(fcinfo, 0)? };
    let deps = crate::dependencies::statext_dependencies_deserialize(fcinfo.result_mcx(), body)?;
    let mut out = String::from("{");
    for (i, dep) in deps.deps.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push('"');
        let n = dep.attributes.len();
        for (j, &attnum) in dep.attributes.iter().enumerate() {
            if j == n - 1 {
                out.push_str(" => ");
            } else if j > 0 {
                out.push_str(", ");
            }
            out.push_str(&attnum.to_string());
        }
        out.push_str(&format!("\": {:.6}", dep.degree));
    }
    out.push('}');
    cstring_out(fcinfo, &out)
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

fn fc_pg_mcv_list_items(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
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

#[cold]
fn cannot_accept(typname: &'static str) -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::error(format!("cannot accept a value of type {typname}"))
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

macro_rules! fc_reject {
    ($($fname:ident: $typname:literal;)*) => {$(
        fn $fname(_f: Option<&mut FmgrInfo>, _fc: &mut Fcinfo) -> PgResult<Datum> {
            Err(cannot_accept($typname))
        }
    )*};
}

fc_reject! {
    fc_pg_ndistinct_in: "pg_ndistinct";
    fc_pg_ndistinct_recv: "pg_ndistinct";
    fc_pg_dependencies_in: "pg_dependencies";
    fc_pg_dependencies_recv: "pg_dependencies";
    fc_pg_mcv_list_in: "pg_mcv_list";
    fc_pg_mcv_list_recv: "pg_mcv_list";
}

const fn b(foid: Oid, name: &'static str, nargs: i16, retset: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset, func }
}

pub static STATISTICS_BUILTINS: &[FmgrBuiltin] = &[
    b(3355, "pg_ndistinct_in", 1, false, fc_pg_ndistinct_in),
    b(3356, "pg_ndistinct_out", 1, false, fc_pg_ndistinct_out),
    b(3357, "pg_ndistinct_recv", 1, false, fc_pg_ndistinct_recv),
    b(3358, "pg_ndistinct_send", 1, false, varlena::builtins::fc_byteasend),
    b(3404, "pg_dependencies_in", 1, false, fc_pg_dependencies_in),
    b(3405, "pg_dependencies_out", 1, false, fc_pg_dependencies_out),
    b(3406, "pg_dependencies_recv", 1, false, fc_pg_dependencies_recv),
    b(3407, "pg_dependencies_send", 1, false, varlena::builtins::fc_byteasend),
    b(3427, "pg_stats_ext_mcvlist_items", 1, true, fc_pg_mcv_list_items),
    b(5018, "pg_mcv_list_in", 1, false, fc_pg_mcv_list_in),
    b(5019, "pg_mcv_list_out", 1, false, varlena::builtins::fc_byteaout),
    b(5020, "pg_mcv_list_recv", 1, false, fc_pg_mcv_list_recv),
    b(5021, "pg_mcv_list_send", 1, false, varlena::builtins::fc_byteasend),
];

pub fn register_builtins() {
    fmgr_core::register_late_builtins(STATISTICS_BUILTINS);
}
