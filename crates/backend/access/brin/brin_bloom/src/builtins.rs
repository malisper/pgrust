use ::datum::Datum;
use ::fmgr::{cstring_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};

#[cold]
#[inline(never)]
fn cannot_accept() -> PgError {
    PgError::error("cannot accept a value of type pg_brin_bloom_summary")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

fn fc_summary_in(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(cannot_accept().into())
}

fn fc_summary_recv(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(cannot_accept().into())
}

fn fc_summary_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn; arg 0 is a live pg_brin_bloom_summary varlena.
    let flat = unsafe { fcinfo.arg_varlena_packed(0)? };
    let b = flat.data();
    let nhashes = b[2];
    let nbits = u32::from_ne_bytes(b[4..8].try_into().unwrap());
    let nbits_set = u32::from_ne_bytes(b[8..12].try_into().unwrap());
    let mcx = fcinfo.result_mcx();
    let s = format!("{{mode: hashed  nhashes: {nhashes}  nbits: {nbits}  nbits_set: {nbits_set}}}");
    let mut v: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, s.len() + 1)?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    v.push(0); // cstring_result requires NUL termination
    Ok(cstring_result(v))
}

fn fc_summary_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // byteasend: the summary is serialized as a bytea.
    varlena::builtins::fc_byteasend(None, fcinfo)
}

fn fc_options(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: index_opclass_options passes &mut LocalRelopts as arg 0 (C
    // PG_GETARG_POINTER protocol).
    let relopts = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut reloptions::LocalRelopts) };
    relopts.init(crate::BLOOMOPTIONS_SIZE);
    relopts.add_real(
        "n_distinct_per_range",
        crate::BLOOM_DEFAULT_NDISTINCT_PER_RANGE,
        -1.0,
        i32::MAX as f64,
        crate::BLOOMOPTIONS_NDISTINCT_OFF,
    );
    relopts.add_real(
        "false_positive_rate",
        crate::BLOOM_DEFAULT_FALSE_POSITIVE_RATE,
        crate::BLOOM_MIN_FALSE_POSITIVE_RATE,
        crate::BLOOM_MAX_FALSE_POSITIVE_RATE,
        crate::BLOOMOPTIONS_FPR_OFF,
    );
    Ok(Datum::from_usize(0))
}

const fn b(foid: ::types_core::Oid, name: &'static str, nargs: i16, strict: bool, func: ::fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict, retset: false, func }
}

pub const BLOOM_BUILTINS: &[FmgrBuiltin] = &[
    b(4595, "brin_bloom_options", 1, false, fc_options),
    b(4596, "brin_bloom_summary_in", 1, true, fc_summary_in),
    b(4597, "brin_bloom_summary_out", 1, true, fc_summary_out),
    b(4598, "brin_bloom_summary_recv", 1, true, fc_summary_recv),
    b(4599, "brin_bloom_summary_send", 1, true, fc_summary_send),
];
