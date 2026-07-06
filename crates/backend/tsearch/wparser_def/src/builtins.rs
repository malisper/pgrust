


use ::datum::Datum;
use ::ts_locale::LexDescr;
use ::types_core::catalog::{INT4OID, RECORDOID, TEXTOID};
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::parser::{self, TParser};

// pg_ts_parser.dat: the 'default' parser row.
pub const DEFAULT_PARSER_OID: ::types_core::Oid = 3722;

// Internal-arg contract (mirrors the C fmgr shapes; callers are ts_parse /
// ts_cache resolving these by OID):
//   prsd_start(str *const u8, len i32) -> *mut TParser, Box-allocated; the
//     input buffer is borrowed and must outlive the parser.
//   prsd_nexttoken(*mut TParser, t *mut *const u8, len *mut i32) -> i32 type
//     (0 = done); *t points into the input buffer.
//   prsd_end(*mut TParser) frees it (Box::from_raw).
//   prsd_lextype(_) -> *mut Vec<ts_locale::LexDescr>; caller takes ownership.
pub fn fc_prsd_start(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let str_ptr = fcinfo.arg(0).as_usize() as *const u8;
    let len = fcinfo.arg(1).as_i32();
    let prs = parser::tparser_init(fcinfo.result_mcx(), str_ptr, len.max(0) as usize)?;
    Ok(Datum::from_usize(Box::into_raw(Box::new(prs)) as usize))
}

pub fn fc_prsd_nexttoken(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let prs_ptr = fcinfo.arg(0).as_usize() as *mut TParser;
    let t = fcinfo.arg(1).as_usize() as *mut *const u8;
    let len = fcinfo.arg(2).as_usize() as *mut i32;
    // SAFETY: internal-arg contract above; pointers come from prsd_start and
    // the caller's out-params.
    let prs = unsafe { &mut *prs_ptr };
    if !parser::tparser_get(prs)? {
        return Ok(Datum::from_i32(0));
    }
    // SAFETY: caller-supplied out-params are valid for writes.
    unsafe {
        *t = prs.token_ptr();
        *len = prs.lenbytetoken as i32;
    }
    Ok(Datum::from_i32(prs.type_))
}

pub fn fc_prsd_end(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let prs_ptr = fcinfo.arg(0).as_usize() as *mut TParser;
    // SAFETY: internal-arg contract; pointer originates from prsd_start.
    drop(unsafe { Box::from_raw(prs_ptr) });
    Ok(Datum::null())
}

pub fn lextype() -> Vec<LexDescr> {
    (1..=parser::LASTNUM)
        .map(|i| LexDescr {
            lexid: i,
            alias: parser::TOK_ALIAS[i as usize],
            descr: parser::LEX_DESCR[i as usize],
        })
        .collect()
}

pub fn fc_prsd_lextype(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_usize(Box::into_raw(Box::new(lextype())) as usize))
}

// Internal-arg contract (ts_headline entry in to_tsany):
//   arg0 *mut HeadlineParsedText, arg1 *const PgVec<DefListItem> (0 = NIL),
//   arg2 *const u8 tsquery varlena image.
pub fn fc_prsd_headline(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let prs_ptr = fcinfo.arg(0).as_usize() as *mut ::ts_parse::headline::HeadlineParsedText;
    let opts_ptr = fcinfo.arg(1).as_usize()
        as *const ::mcx::PgVec<'_, ::ts_cache::DefListItem<'_>>;
    let q_ptr = fcinfo.arg(2).as_usize() as *const u8;
    // SAFETY (all): internal-arg contract — live pointers from the
    // ts_headline frame; the tsquery image spans varsize_any bytes.
    let prs = unsafe { &mut *prs_ptr };
    let options: &[::ts_cache::DefListItem<'_>] =
        if opts_ptr.is_null() {
            &[]
        } else {
            // SAFETY: internal-arg contract — a live PgVec reference.
            let v = unsafe { &*opts_ptr };
            v.as_slice()
        };
    let payload = unsafe {
        let image =
            core::slice::from_raw_parts(q_ptr, ::types_tuple::varatt::varsize_any(q_ptr));
        &image[::types_tuple::varatt::VARHDRSZ..]
    };
    crate::headline::prsd_headline_impl(
        mcx,
        prs,
        options,
        ::adt_tsvector_core::query::TsQueryRef { payload },
    )?;
    Ok(Datum::from_usize(prs_ptr as usize))
}

enum SrfRows {
    Tuples(Vec<Vec<u8>>),
}

fn srf_drive(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    name: &'static str,
    collect: impl FnOnce(&Fcinfo) -> PgResult<SrfRows>,
) -> PgResult<Datum> {
    let flinfo = flinfo.unwrap_or_else(|| panic!("{name}: NULL flinfo"));
    if !flinfo.has_fn_extra() {
        let rows = collect(fcinfo)?;
        let fctx = ::funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = ::funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let SrfRows::Tuples(rows) = fctx
        .user_fctx
        .as_ref()
        .expect("SRF rows set at first call")
        .downcast_ref::<SrfRows>()
        .expect("user_fctx is SrfRows");
    match rows.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(::funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(::funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

fn require_default_parser(oid: ::types_core::Oid, name: &str) {
    if oid != DEFAULT_PARSER_OID {
        panic!("wparser_def: {name} for non-default parser {oid} unported (needs ts_cache parser lookup)");
    }
}

pub fn fc_ts_token_type_byid(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "ts_token_type_byid", |fcinfo| {
        require_default_parser(fcinfo.arg(0).as_oid(), "ts_token_type");
        token_type_rows(fcinfo)
    })
}

fn token_type_rows(fcinfo: &Fcinfo) -> PgResult<SrfRows> {
    let mcx = fcinfo.result_mcx();
    let mut desc = ::tupdesc::CreateTemplateTupleDesc(mcx, 3)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 1, Some("tokid"), INT4OID, -1, 0)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 2, Some("alias"), TEXTOID, -1, 0)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 3, Some("description"), TEXTOID, -1, 0)?;
    desc.tdtypeid = RECORDOID;
    desc.tdtypmod = -1;
    // BlessTupleDesc (via TupleDescGetAttInMetadata, wparser.c tt_setup):
    // FieldSelect over these tuples needs the registered typmod.
    ::typcache_seams::assign_record_type_typmod::call(&mut desc)?;
    let mut rows = Vec::with_capacity(parser::LASTNUM as usize);
    for d in lextype() {
        let alias = varlena_result(::varlena::cstring_to_text(mcx, d.alias.as_bytes())?);
        let descr = varlena_result(::varlena::cstring_to_text(mcx, d.descr.as_bytes())?);
        let tuple = ::heaptuple::heap_form_tuple(
            mcx,
            &desc,
            &[Datum::from_i32(d.lexid), alias, descr],
            &[false, false, false],
        )?;
        rows.push(tuple.image().to_vec());
    }
    Ok(SrfRows::Tuples(rows))
}

pub fn fc_ts_token_type_byname(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "ts_token_type_byname", |fcinfo| {
        require_default_parser(parser_oid_from_text_arg(fcinfo, 0)?, "ts_token_type");
        token_type_rows(fcinfo)
    })
}

// textToQualifiedNameList + get_ts_parser_oid (wparser.c byname entries).
fn parser_oid_from_text_arg(fcinfo: &Fcinfo, i: usize) -> PgResult<::types_core::Oid> {
    // SAFETY: strict fn; arg i is a text varlena.
    let v = unsafe { fcinfo.arg_varlena_packed(i) }?;
    let rawname = core::str::from_utf8(v.data())
        .map_err(|_| Box::new(::types_error::PgError::error("invalid UTF-8 in parser name")))?;
    let names = ::varlena::textToQualifiedNameList(fcinfo.result_mcx(), rawname)?;
    let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    namespace_seams::get_ts_parser_oid::call(&name_refs, false)
}

pub fn fc_ts_parse_byid(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "ts_parse_byid", |fcinfo| {
        require_default_parser(fcinfo.arg(0).as_oid(), "ts_parse");
        parse_rows(fcinfo)
    })
}

fn parse_rows(fcinfo: &Fcinfo) -> PgResult<SrfRows> {
    // SAFETY: strict fn; arg 1 is a text varlena.
    let txt = unsafe { fcinfo.arg_varlena_packed(1)? };
    let mcx = fcinfo.result_mcx();
    let data = txt.data();
    let mut prs = parser::tparser_init(mcx, data.as_ptr(), data.len())?;
    let mut desc = ::tupdesc::CreateTemplateTupleDesc(mcx, 2)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 1, Some("tokid"), INT4OID, -1, 0)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 2, Some("token"), TEXTOID, -1, 0)?;
    desc.tdtypeid = RECORDOID;
    desc.tdtypmod = -1;
    // BlessTupleDesc (wparser.c prs_setup), as tt_setup above.
    ::typcache_seams::assign_record_type_typmod::call(&mut desc)?;
    let mut rows = Vec::new();
    while parser::tparser_get(&mut prs)? {
        let token = varlena_result(::varlena::cstring_to_text(mcx, prs.token_bytes())?);
        let tuple = ::heaptuple::heap_form_tuple(
            mcx,
            &desc,
            &[Datum::from_i32(prs.type_), token],
            &[false, false],
        )?;
        rows.push(tuple.image().to_vec());
    }
    Ok(SrfRows::Tuples(rows))
}

pub fn fc_ts_parse_byname(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "ts_parse_byname", |fcinfo| {
        require_default_parser(parser_oid_from_text_arg(fcinfo, 0)?, "ts_parse");
        parse_rows(fcinfo)
    })
}

const fn b(foid: ::types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn srf(foid: ::types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

pub const WPARSER_BUILTINS: &[FmgrBuiltin] = &[
    srf(3713, "ts_token_type_byid", 1, fc_ts_token_type_byid),
    srf(3714, "ts_token_type_byname", 1, fc_ts_token_type_byname),
    srf(3715, "ts_parse_byid", 2, fc_ts_parse_byid),
    srf(3716, "ts_parse_byname", 2, fc_ts_parse_byname),
    b(3717, "prsd_start", 2, fc_prsd_start),
    b(3718, "prsd_nexttoken", 3, fc_prsd_nexttoken),
    b(3719, "prsd_end", 1, fc_prsd_end),
    b(3720, "prsd_headline", 3, fc_prsd_headline),
    b(3721, "prsd_lextype", 1, fc_prsd_lextype),
];
