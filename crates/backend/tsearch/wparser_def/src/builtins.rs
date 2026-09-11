


use ::datum::Datum;
use ::ts_locale::LexDescr;
use ::types_core::catalog::{INT4OID, RECORDOID, TEXTOID};
use ::types_error::{
    PgError, PgResult, SqlState, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_UNDEFINED_SCHEMA,
};
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::parser::{self, TParser};

use ::std::cell::Cell;
use ::std::rc::Rc;

// pg_ts_parser.dat: the 'default' parser row.
pub const DEFAULT_PARSER_OID: ::types_core::Oid = 3722;

// Owns the parser handed across the fmgr boundary plus its wide-char buffers
// (the TParser's Drop frees them). In C the TParser lives in
// CurrentMemoryContext, so an ERROR between prsd_start and prsd_end resets that
// context and reclaims it; here the Box would instead leak because error
// unwinding skips prsd_end. `armed` — shared with the reset callback that
// prsd_start registers on that same context — makes reclamation happen on
// EVERY exit path: whichever of prsd_end or the context reset runs first claims
// the flag (replace(false)) and frees the Box exactly once; the other becomes a
// no-op. The flag is an Rc, so it outlives the Box and is safe to consult from
// the reset callback even after prsd_end has freed the parser.
struct ParserHandle {
    armed: Rc<Cell<bool>>,
    parser: TParser,
}

// Internal-arg contract (mirrors the C fmgr shapes; callers are ts_parse /
// ts_cache resolving these by OID). The returned pointer is opaque to callers,
// who only hand it back to prsd_nexttoken/prsd_end:
//   prsd_start(str *const u8, len i32) -> *mut ParserHandle, Box-allocated and
//     tied to the result memory context (freed on context reset if prsd_end is
//     skipped by error unwinding); the input buffer is borrowed and must
//     outlive the parser.
//   prsd_nexttoken(*mut ParserHandle, t *mut *const u8, len *mut i32) -> i32
//     type (0 = done); *t points into the input buffer.
//   prsd_end(*mut ParserHandle) frees it eagerly (Box::from_raw).
//   prsd_lextype(_) -> *mut Vec<ts_locale::LexDescr>; caller takes ownership.
pub fn fc_prsd_start(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let str_ptr = fcinfo.arg(0).as_usize() as *const u8;
    let len = fcinfo.arg(1).as_i32();
    let mcx = fcinfo.result_mcx();
    let parser = parser::tparser_init(mcx, str_ptr, len.max(0) as usize)?;
    let armed = Rc::new(Cell::new(true));
    let raw = Box::into_raw(Box::new(ParserHandle {
        armed: Rc::clone(&armed),
        parser,
    }));
    // C's error cleanup resets CurrentMemoryContext, which reclaims the parser;
    // reproduce that here by reclaiming the Box on context reset unless prsd_end
    // already claimed it. prsd_end only runs before any reset on the normal
    // path, so the freed `raw` is never revisited.
    mcx.context().register_reset_callback(move || {
        if armed.replace(false) {
            // SAFETY: armed was true, so prsd_end did not run; `raw` is the live
            // Box allocated above and is reclaimed exactly once here.
            drop(unsafe { Box::from_raw(raw) });
        }
    });
    Ok(Datum::from_usize(raw as usize))
}

pub fn fc_prsd_nexttoken(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let handle_ptr = fcinfo.arg(0).as_usize() as *mut ParserHandle;
    let t = fcinfo.arg(1).as_usize() as *mut *const u8;
    let len = fcinfo.arg(2).as_usize() as *mut i32;
    // SAFETY: internal-arg contract above; pointers come from prsd_start and
    // the caller's out-params.
    let prs = unsafe { &mut (*handle_ptr).parser };
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
    let handle_ptr = fcinfo.arg(0).as_usize() as *mut ParserHandle;
    // SAFETY: internal-arg contract; pointer originates from prsd_start and, on
    // the normal path, prsd_end runs before any context reset, so the handle is
    // live. Claim the flag so the reset callback won't also free it.
    let armed = unsafe { (*handle_ptr).armed.replace(false) };
    if armed {
        // SAFETY: we claimed the parser, so this reclaims the Box exactly once.
        drop(unsafe { Box::from_raw(handle_ptr) });
    }
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

pub(crate) enum SrfRows {
    Tuples(Vec<Vec<u8>>),
}

// wparser.c prs_setup_firstcall/tt_setup materialise the whole set into a
// repalloc'd array: the same MaxAllocSize admission applies here, and every
// growth is fallible so exhaustion is C's error rather than an abort.
fn push_image(rows: &mut Vec<Vec<u8>>, img: &[u8], mcx: ::mcx::Mcx<'_>) -> PgResult<()> {
    ::mcx::check_alloc_size((rows.len() + 1) * 16)?;
    rows.try_reserve(1).map_err(|_| mcx.oom(16))?;
    let mut v = Vec::new();
    v.try_reserve_exact(img.len()).map_err(|_| mcx.oom(img.len()))?;
    v.extend_from_slice(img);
    rows.push(v);
    Ok(())
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

pub fn fc_ts_token_type_byid(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "ts_token_type_byid", |fcinfo| {
        token_type_rows(fcinfo, fcinfo.arg(0).as_oid())
    })
}

// tt_setup_firstcall (wparser.c): the parser's lextype method provides the
// descriptor list. The default parser short-circuits to the native
// implementation (the same function fmgr would resolve for OID 3721).
fn parser_lex_descrs(mcx: ::mcx::Mcx<'_>, prsid: ::types_core::Oid) -> PgResult<Vec<LexDescr>> {
    if prsid == DEFAULT_PARSER_OID {
        return Ok(lextype());
    }
    let prs = ::ts_cache::lookup_ts_parser_cache(prsid)?;
    if prs.lextype_oid == ::types_core::InvalidOid {
        return Err(Box::new(::types_error::PgError::error(format!(
            "method lextype isn't defined for text search parser {prsid}"
        ))));
    }
    // C: OidFunctionCall1(prs->lextypeOid, (Datum) 0).
    let mut flinfo = ::fmgr_seams::fmgr_info::call(prs.lextype_oid)?;
    let d = ::types_fmgr::function_call1_coll_in(
        &mut flinfo,
        ::types_core::InvalidOid,
        mcx,
        Datum::from_usize(0),
    )?;
    // SAFETY: internal-arg contract (fc_prsd_lextype above): lextype methods
    // return *mut Vec<LexDescr> and the caller takes ownership.
    Ok(*unsafe { Box::from_raw(d.as_usize() as *mut Vec<LexDescr>) })
}

pub(crate) fn token_type_rows(fcinfo: &Fcinfo, prsid: ::types_core::Oid) -> PgResult<SrfRows> {
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
    let descrs = parser_lex_descrs(mcx, prsid)?;
    let mut rows = Vec::with_capacity(descrs.len());
    for d in descrs {
        let alias = varlena_result(::varlena::cstring_to_text(mcx, d.alias.as_bytes())?);
        let descr = varlena_result(::varlena::cstring_to_text(mcx, d.descr.as_bytes())?);
        let tuple = ::heaptuple::heap_form_tuple(
            mcx,
            &desc,
            &[Datum::from_i32(d.lexid), alias, descr],
            &[false, false, false],
        )?;
        push_image(&mut rows, tuple.image(), mcx)?;
    }
    Ok(SrfRows::Tuples(rows))
}

pub fn fc_ts_token_type_byname(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "ts_token_type_byname", |fcinfo| {
        let prsid = parser_oid_from_text_arg(fcinfo, 0)?;
        token_type_rows(fcinfo, prsid)
    })
}

// textToQualifiedNameList + get_ts_parser_oid (wparser.c byname entries).
fn parser_oid_from_text_arg(fcinfo: &Fcinfo, i: usize) -> PgResult<::types_core::Oid> {
    // SAFETY: strict fn; arg i is a text varlena.
    let v = unsafe { fcinfo.arg_varlena_packed(i) }?;
    // text_to_cstring (varlena.c) copies the bytes without validating the
    // encoding: in a SQL_ASCII database the name may not be UTF-8.
    let names = ::varlena::text_to_qualified_name_list_bytes(fcinfo.result_mcx(), v.data())?;
    let name_refs: Result<Vec<&str>, _> =
        names.iter().map(|n| core::str::from_utf8(n)).collect();
    match name_refs {
        Ok(name_refs) => namespace_seams::get_ts_parser_oid::call(&name_refs, false),
        Err(_) => Err(parser_lookup_miss_raw(&names)?),
    }
}

// ereport whose message quotes raw (possibly non-UTF-8) name bytes.
fn raw_err(sqlstate: SqlState, prefix: &str, body: &[u8], suffix: &str) -> Box<PgError> {
    let mut m = Vec::with_capacity(prefix.len() + body.len() + suffix.len());
    m.extend_from_slice(prefix.as_bytes());
    m.extend_from_slice(body);
    m.extend_from_slice(suffix.as_bytes());
    Box::new(PgError::error_raw_message(m).with_sqlstate(sqlstate))
}

// get_ts_parser_oid (namespace.c) for a qualified name carrying non-UTF-8
// bytes. Every catalog name is valid UTF-8 (query text is UTF-8 or ASCII —
// the ratified server-encoding carve), so such a name can only miss; this
// walks C's arms in C's order so the SQLSTATE and the message bytes match:
// DeconstructQualifiedName (cross-database / too many dotted names),
// LookupExplicitNamespace (schema missing or unusable), then the
// `does not exist` miss with NameListToString's raw bytes.
pub(crate) fn parser_lookup_miss_raw(names: &[Vec<u8>]) -> PgResult<Box<PgError>> {
    let name_list_to_string = || names.join(&b'.');
    let schemaname = match names {
        [_parser_name] => None,
        [schemaname, _parser_name] => Some(schemaname),
        [catalogname, schemaname, _parser_name] => {
            let dbname = match core::str::from_utf8(catalogname) {
                Ok(c) => ::dbcommands_seams::get_database_name::call(
                    ::init_small::globals::MyDatabaseId(),
                )?
                .filter(|d| d == c),
                Err(_) => None,
            };
            if dbname.is_none() {
                return Ok(raw_err(
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                    "cross-database references are not implemented: ",
                    &name_list_to_string(),
                    "",
                ));
            }
            Some(schemaname)
        }
        _ => {
            return Ok(raw_err(
                ERRCODE_SYNTAX_ERROR,
                "improper qualified name (too many dotted names): ",
                &name_list_to_string(),
                "",
            ))
        }
    };
    if let Some(schemaname) = schemaname {
        match core::str::from_utf8(schemaname) {
            // A UTF-8 schema part may exist (or be unusable): C's lookup errors
            // come first; on success the parser part (non-UTF-8) misses below.
            Ok(schema) => {
                namespace_seams::lookup_explicit_namespace::call(schema, false)?;
            }
            Err(_) => {
                return Ok(raw_err(
                    ERRCODE_UNDEFINED_SCHEMA,
                    "schema \"",
                    schemaname,
                    "\" does not exist",
                ))
            }
        }
    }
    Ok(raw_err(
        ERRCODE_UNDEFINED_OBJECT,
        "text search parser \"",
        &name_list_to_string(),
        "\" does not exist",
    ))
}

pub fn fc_ts_parse_byid(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "ts_parse_byid", |fcinfo| {
        parse_rows(fcinfo, fcinfo.arg(0).as_oid())
    })
}

pub(crate) fn parse_rows(fcinfo: &Fcinfo, prsid: ::types_core::Oid) -> PgResult<SrfRows> {
    // SAFETY: strict fn; arg 1 is a text varlena.
    let txt = unsafe { fcinfo.arg_varlena_packed(1)? };
    let mcx = fcinfo.result_mcx();
    let data = txt.data();
    let mut desc = ::tupdesc::CreateTemplateTupleDesc(mcx, 2)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 1, Some("tokid"), INT4OID, -1, 0)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 2, Some("token"), TEXTOID, -1, 0)?;
    desc.tdtypeid = RECORDOID;
    desc.tdtypmod = -1;
    // BlessTupleDesc (wparser.c prs_setup), as tt_setup above.
    ::typcache_seams::assign_record_type_typmod::call(&mut desc)?;
    let mut rows = Vec::new();
    let mut push_row = |type_: i32, token_bytes: &[u8]| -> PgResult<()> {
        let token = varlena_result(::varlena::cstring_to_text(mcx, token_bytes)?);
        let tuple = ::heaptuple::heap_form_tuple(
            mcx,
            &desc,
            &[Datum::from_i32(type_), token],
            &[false, false],
        )?;
        push_image(&mut rows, tuple.image(), mcx)?;
        Ok(())
    };
    if prsid == DEFAULT_PARSER_OID {
        // Native short-circuit: the same functions fmgr would resolve.
        let mut prs = parser::tparser_init(mcx, data.as_ptr(), data.len())?;
        while parser::tparser_get(&mut prs)? {
            push_row(prs.type_, prs.token_bytes())?;
        }
    } else {
        // prs_setup_firstcall/prs_process_call (wparser.c): drive the
        // parser's methods through their cached FmgrInfos, C's
        // FunctionCall2/FunctionCall3/FunctionCall1 sequence.
        let entry = ::ts_cache::lookup_ts_parser_cache(prsid)?;
        let prsobj = ::types_fmgr::function_call2_coll_in(
            &mut entry.prsstart.borrow_mut(),
            ::types_core::InvalidOid,
            mcx,
            Datum::from_usize(data.as_ptr() as usize),
            Datum::from_i32(data.len() as i32),
        )?;
        loop {
            let mut lex: *const u8 = core::ptr::null();
            let mut llen: i32 = 0;
            let t = ::types_fmgr::function_call3_coll_in(
                &mut entry.prstoken.borrow_mut(),
                ::types_core::InvalidOid,
                mcx,
                prsobj,
                Datum::from_usize(&mut lex as *mut *const u8 as usize),
                Datum::from_usize(&mut llen as *mut i32 as usize),
            )?
            .as_i32();
            if t == 0 {
                break;
            }
            // SAFETY: internal-arg contract (fc_prsd_nexttoken above): *lex
            // spans llen bytes of the input buffer.
            let token_bytes =
                unsafe { core::slice::from_raw_parts(lex, llen.max(0) as usize) };
            push_row(t, token_bytes)?;
        }
        ::types_fmgr::function_call1_coll_in(
            &mut entry.prsend.borrow_mut(),
            ::types_core::InvalidOid,
            mcx,
            prsobj,
        )?;
    }
    Ok(SrfRows::Tuples(rows))
}

pub fn fc_ts_parse_byname(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "ts_parse_byname", |fcinfo| {
        let prsid = parser_oid_from_text_arg(fcinfo, 0)?;
        parse_rows(fcinfo, prsid)
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
