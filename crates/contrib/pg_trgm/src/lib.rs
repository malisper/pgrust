//! contrib/pg_trgm — trigram text-similarity functions, operators, and the
//! gin_trgm_ops / gist_trgm_ops index opclasses.
//!
//! Scalars + GiST support procs dispatch through the dfmgr builtin-library
//! registry (real fmgr frames, GISTENTRY protocol, tsgistidx precedent). The
//! GIN core dispatches its closed opclass set by enum: the TrgmOps arm calls
//! the gin_trgm_seams installed here (the gin_* symbols below exist only for
//! CREATE EXTENSION's C-symbol validation and error if invoked via fmgr).
//!
//! The RegExp / RegExpICase strategies run trgm_regexp.c's NFA engine
//! (src/regexp.rs): extract_query compiles the pattern and returns required
//! trigrams plus a packed graph; consistent evaluates the graph per entry.
//! Both GiST consistent/distance keep C's fn_extra cache of the extracted
//! query trigrams (and graph) across calls.
//!
//! Thresholds: C's DefineCustomRealVariable GUCs ride this repo's placeholder
//! custom-GUC store (SET pg_trgm.* works); reads parse the placeholder string
//! or fall back to the C defaults. DIVERGENCE: an out-of-range SET only
//! errors when the value is read, not at SET time.

pub mod gist;
pub mod regexp;
pub mod trgm;

use datum::Datum;
use mcx::MemoryContext;
use gin_vocab::TrgmPackedGraph;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{
    byref_result, varlena_result, FmgrInfo, FnExtra, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};
use types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};
use types_tuple::varatt;

use gist::{
    SIGLEN_DEFAULT, SIGLEN_MAX, EQUAL_STRATEGY, ILIKE_STRATEGY, LIKE_STRATEGY,
    REGEXP_ICASE_STRATEGY, REGEXP_STRATEGY, SIMILARITY_STRATEGY,
    STRICT_WORD_SIMILARITY_STRATEGY, WORD_SIMILARITY_STRATEGY,
};
use trgm::{
    calc_word_similarity, cnt_sml, generate_trgm, generate_wildcard_trgm, trgm2int, Trgm,
    TrgmEnv, WORD_SIMILARITY_CHECK_ONLY, WORD_SIMILARITY_STRICT,
};

const LIBRARY: &str = "pg_trgm";

const DEFAULT_COLLATION_OID: types_core::Oid = 100;

fn legacy_crc32(bytes: &[u8]) -> u32 {
    // trgm_op.c compact_trigram uses INIT/COMP/FIN_LEGACY_CRC32 — the
    // REFLECTED-table legacy variant (pg_crc.h), NOT the zlib/Ethernet one.
    // (Train-6 audit blocker B1.)
    crc32c::legacy_crc32_lexeme(bytes)
}

fn make_env() -> TrgmEnv<'static> {
    TrgmEnv {
        max_encoding_len: mbutils::pg_database_encoding_max_length(),
        mblen: &|s| mbutils::pg_mblen(s),
        isalnum: &|s| !s.is_empty() && ts_locale::t_isalnum(s),
        tolower: &|s| {
            let m = MemoryContext::new("pg_trgm tolower scratch");
            oracle_compat::casemap::str_tolower(m.mcx(), s, DEFAULT_COLLATION_OID)
                .map(|v| v.as_slice().to_vec())
                .expect("pg_trgm: str_tolower failed")
        },
    }
}

// ===========================================================================
// Threshold GUCs (pg_trgm.*_threshold).
// ===========================================================================

fn threshold(name: &str, default: f64) -> f64 {
    match guc::GetConfigOption(name, true, false) {
        Ok(Some(s)) => s.parse::<f64>().unwrap_or(default),
        _ => default,
    }
}

pub fn similarity_threshold() -> f64 {
    threshold("pg_trgm.similarity_threshold", 0.3)
}

pub fn word_similarity_threshold() -> f64 {
    threshold("pg_trgm.word_similarity_threshold", 0.6)
}

pub fn strict_word_similarity_threshold() -> f64 {
    threshold("pg_trgm.strict_word_similarity_threshold", 0.5)
}

fn index_strategy_get_limit(strategy: u16) -> PgResult<f64> {
    Ok(match strategy {
        SIMILARITY_STRATEGY => similarity_threshold(),
        WORD_SIMILARITY_STRATEGY => word_similarity_threshold(),
        STRICT_WORD_SIMILARITY_STRATEGY => strict_word_similarity_threshold(),
        other => {
            return Err(
                PgError::error(format!("unrecognized strategy number: {other}")).into(),
            )
        }
    })
}

// ===========================================================================
// Scalar functions (trgm_op.c).
// ===========================================================================

fn text_args<'a>(fcinfo: &'a Fcinfo) -> PgResult<(&'a [u8], &'a [u8])> {
    // SAFETY: catalog args are non-null text varlenas (strict fns).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok((a.data(), b.data()))
}

fn fc_show_trgm(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null text varlena (strict fn).
    let input = unsafe { fcinfo.arg_varlena_packed(0)? };
    let env = make_env();
    let trg = generate_trgm(input.data(), &env, &legacy_crc32);

    let multibyte = mbutils::pg_database_encoding_max_length() > 1;
    let mcx = fcinfo.result_mcx();
    let mut elems: Vec<Datum> = Vec::with_capacity(trg.len());
    for t in &trg {
        let bytes: Vec<u8> = if multibyte && !is_printable_trgm(t) {
            format!("0x{:06x}", trgm2int(t)).into_bytes()
        } else {
            t.to_vec()
        };
        let v = varlena::cstring_to_text(mcx, &bytes)?;
        elems.push(varlena_result(v));
    }
    let image = arrayfuncs::construct::construct_array(
        mcx,
        &elems,
        types_core::TEXTOID,
        -1,
        false,
        b'i',
    )?;
    let d = Datum::from_usize(image.as_ptr() as usize);
    core::mem::forget(image);
    Ok(d)
}

fn is_printable_char(c: u8) -> bool {
    c.is_ascii() && (c.is_ascii_alphanumeric() || c == b' ')
}

fn is_printable_trgm(t: &Trgm) -> bool {
    is_printable_char(t[0]) && is_printable_char(t[1]) && is_printable_char(t[2])
}

fn similarity_value(fcinfo: &Fcinfo) -> PgResult<f32> {
    let (a, b) = text_args(fcinfo)?;
    let env = make_env();
    let trg1 = generate_trgm(a, &env, &legacy_crc32);
    let trg2 = generate_trgm(b, &env, &legacy_crc32);
    Ok(cnt_sml(&trg1, &trg2, false))
}

fn word_sim_value(fcinfo: &Fcinfo, swapped: bool, flags: u8) -> PgResult<f32> {
    let (a, b) = text_args(fcinfo)?;
    let (a, b) = if swapped { (b, a) } else { (a, b) };
    let env = make_env();
    Ok(calc_word_similarity(
        a,
        b,
        flags,
        &env,
        &legacy_crc32,
        word_similarity_threshold(),
        strict_word_similarity_threshold(),
    ))
}

fn fc_similarity(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f32(similarity_value(fcinfo)?))
}

fn fc_similarity_dist(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f32(1.0 - similarity_value(fcinfo)?))
}

fn fc_similarity_op(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(
        similarity_value(fcinfo)? as f64 >= similarity_threshold(),
    ))
}

fn fc_word_similarity(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f32(word_sim_value(fcinfo, false, 0)?))
}

fn fc_strict_word_similarity(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f32(word_sim_value(fcinfo, false, WORD_SIMILARITY_STRICT)?))
}

macro_rules! fc_word_ops {
    ($($fname:ident: swapped=$sw:literal flags=$flags:expr, $ret:ident($conv:expr);)*) => {$(
        fn $fname(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let res = word_sim_value(fcinfo, $sw, $flags)?;
            Ok(Datum::$ret(($conv)(res)))
        }
    )*};
}

fc_word_ops! {
    fc_word_similarity_op: swapped=false flags=WORD_SIMILARITY_CHECK_ONLY,
        from_bool(|r: f32| r as f64 >= word_similarity_threshold());
    fc_word_similarity_commutator_op: swapped=true flags=WORD_SIMILARITY_CHECK_ONLY,
        from_bool(|r: f32| r as f64 >= word_similarity_threshold());
    fc_word_similarity_dist_op: swapped=false flags=0,
        from_f32(|r: f32| 1.0 - r);
    fc_word_similarity_dist_commutator_op: swapped=true flags=0,
        from_f32(|r: f32| 1.0 - r);
    fc_strict_word_similarity_op: swapped=false flags=WORD_SIMILARITY_CHECK_ONLY | WORD_SIMILARITY_STRICT,
        from_bool(|r: f32| r as f64 >= strict_word_similarity_threshold());
    fc_strict_word_similarity_commutator_op: swapped=true flags=WORD_SIMILARITY_CHECK_ONLY | WORD_SIMILARITY_STRICT,
        from_bool(|r: f32| r as f64 >= strict_word_similarity_threshold());
    fc_strict_word_similarity_dist_op: swapped=false flags=WORD_SIMILARITY_STRICT,
        from_f32(|r: f32| 1.0 - r);
    fc_strict_word_similarity_dist_commutator_op: swapped=true flags=WORD_SIMILARITY_STRICT,
        from_f32(|r: f32| 1.0 - r);
}

fn fc_set_limit(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let nlimit = a.value.as_f32();
    if !(0.0..=1.0).contains(&nlimit) {
        return Err(PgError::error("pg_trgm.similarity_threshold must be in range [0, 1]")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .into());
    }
    guc::SetConfigOption(
        "pg_trgm.similarity_threshold",
        Some(&format!("{nlimit}")),
        types_guc::GucContext::PGC_USERSET,
        types_guc::GucSource::PGC_S_SESSION,
    )?;
    Ok(Datum::from_f32(similarity_threshold() as f32))
}

fn fc_show_limit(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let _ = fcinfo;
    Ok(Datum::from_f32(similarity_threshold() as f32))
}

// ===========================================================================
// gin_trgm_ops cores (trgm_gin.c), installed on gin_trgm_seams.
// ===========================================================================

const GIN_SEARCH_MODE_ALL: i32 = 2;
const GIN_FALSE: i8 = 0;
const GIN_MAYBE: i8 = 2;

fn keys_of(trg: &[Trgm]) -> Vec<i32> {
    trg.iter().map(|t| trgm2int(t) as i32).collect()
}

fn trgm_extract_value(payload: &[u8]) -> PgResult<Vec<i32>> {
    let env = make_env();
    Ok(keys_of(&generate_trgm(payload, &env, &legacy_crc32)))
}

fn trgm_extract_query(
    payload: &[u8],
    strategy: u16,
    collation: types_core::Oid,
) -> PgResult<(Vec<i32>, i32, Option<TrgmPackedGraph>)> {
    let env = make_env();
    let (trg, graph) = match strategy {
        SIMILARITY_STRATEGY
        | WORD_SIMILARITY_STRATEGY
        | STRICT_WORD_SIMILARITY_STRATEGY
        | EQUAL_STRATEGY => (generate_trgm(payload, &env, &legacy_crc32), None),
        LIKE_STRATEGY | ILIKE_STRATEGY => {
            (generate_wildcard_trgm(payload, &env, &legacy_crc32), None)
        }
        REGEXP_STRATEGY | REGEXP_ICASE_STRATEGY => {
            match regexp::create_trgm_nfa(payload, collation, &env, &legacy_crc32)? {
                Some((trg, graph)) if !trg.is_empty() => (trg, Some(graph)),
                // Regex processing gave no usable trigrams: full index scan.
                _ => return Ok((Vec::new(), GIN_SEARCH_MODE_ALL, None)),
            }
        }
        other => {
            return Err(
                PgError::error(format!("unrecognized strategy number: {other}")).into(),
            )
        }
    };
    let keys = keys_of(&trg);
    let search_mode = if keys.is_empty() { GIN_SEARCH_MODE_ALL } else { 0 };
    Ok((keys, search_mode, graph))
}

fn trgm_consistent(
    check: &[i8],
    strategy: u16,
    nkeys: usize,
    graph: Option<&mut TrgmPackedGraph>,
) -> PgResult<(bool, bool)> {
    // All lanes served here are inexact.
    let res = match strategy {
        SIMILARITY_STRATEGY | WORD_SIMILARITY_STRATEGY | STRICT_WORD_SIMILARITY_STRATEGY => {
            let nlimit = index_strategy_get_limit(strategy)?;
            let ntrue = (0..nkeys).filter(|&i| check[i] != GIN_FALSE).count() as i32;
            // ntrue is a lower bound of len2, so ntrue / nkeys is the
            // similarity upper bound (see trgm_gin.c).
            nkeys != 0 && ((ntrue as f32) / (nkeys as f32)) as f64 >= nlimit
        }
        LIKE_STRATEGY | ILIKE_STRATEGY | EQUAL_STRATEGY => {
            (0..nkeys).all(|i| check[i] != GIN_FALSE)
        }
        REGEXP_STRATEGY | REGEXP_ICASE_STRATEGY => {
            if nkeys < 1 {
                // Regex processing gave no result: full index scan.
                true
            } else {
                let boolcheck: Vec<bool> = (0..nkeys).map(|i| check[i] != GIN_FALSE).collect();
                graph
                    .expect("pg_trgm: regexp scan key without a packed graph")
                    .matches(&boolcheck)
            }
        }
        other => {
            return Err(
                PgError::error(format!("unrecognized strategy number: {other}")).into(),
            )
        }
    };
    Ok((res, true))
}

fn trgm_triconsistent(
    check: &[i8],
    strategy: u16,
    nkeys: usize,
    graph: Option<&mut TrgmPackedGraph>,
) -> PgResult<i8> {
    Ok(match strategy {
        SIMILARITY_STRATEGY | WORD_SIMILARITY_STRATEGY | STRICT_WORD_SIMILARITY_STRATEGY => {
            let nlimit = index_strategy_get_limit(strategy)?;
            let ntrue = (0..nkeys).filter(|&i| check[i] != GIN_FALSE).count() as i32;
            if nkeys != 0 && ((ntrue as f32) / (nkeys as f32)) as f64 >= nlimit {
                GIN_MAYBE
            } else {
                GIN_FALSE
            }
        }
        LIKE_STRATEGY | ILIKE_STRATEGY | EQUAL_STRATEGY => {
            if (0..nkeys).any(|i| check[i] == GIN_FALSE) {
                GIN_FALSE
            } else {
                GIN_MAYBE
            }
        }
        REGEXP_STRATEGY | REGEXP_ICASE_STRATEGY => {
            if nkeys < 1 {
                GIN_MAYBE
            } else {
                // Promoting MAYBE to TRUE is conservative: the graph is a
                // monotone boolean function.
                let boolcheck: Vec<bool> = (0..nkeys).map(|i| check[i] != GIN_FALSE).collect();
                if graph
                    .expect("pg_trgm: regexp scan key without a packed graph")
                    .matches(&boolcheck)
                {
                    GIN_MAYBE
                } else {
                    GIN_FALSE
                }
            }
        }
        other => {
            return Err(
                PgError::error(format!("unrecognized strategy number: {other}")).into(),
            )
        }
    })
}

#[track_caller]
#[cold]
fn gin_symbol_via_fmgr(name: &str) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "pg_trgm: {name} reached through a raw fmgr call — the GIN core dispatches \
         gin_trgm_ops through its opclass enum (gin_trgm_seams), never via fmgr"
    )))
}

macro_rules! fc_gin_stub {
    ($($fname:ident: $sym:literal;)*) => {$(
        fn $fname(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            Err(gin_symbol_via_fmgr($sym))
        }
    )*};
}

fc_gin_stub! {
    fc_gin_extract_value_trgm: "gin_extract_value_trgm";
    fc_gin_extract_query_trgm: "gin_extract_query_trgm";
    fc_gin_trgm_consistent: "gin_trgm_consistent";
    fc_gin_trgm_triconsistent: "gin_trgm_triconsistent";
}

// ===========================================================================
// gist_trgm_ops fmgr wrappers (trgm_gist.c; tsgistidx protocol precedent).
// ===========================================================================

const TRGMGISTOPTIONS_SIZE: usize = 8;
const TRGMGISTOPTIONS_SIGLEN_OFF: usize = 4;

#[inline]
fn get_siglen(f: &Option<&mut FmgrInfo>) -> usize {
    match f.as_ref().and_then(|f| f.opclass_options()) {
        Some(img) => i32::from_ne_bytes(
            img[TRGMGISTOPTIONS_SIGLEN_OFF..TRGMGISTOPTIONS_SIGLEN_OFF + 4].try_into().unwrap(),
        ) as usize,
        None => SIGLEN_DEFAULT,
    }
}

// SAFETY helpers over the gist fmgr protocol (tsgistidx precedent).
unsafe fn entry_arg<'a>(fcinfo: &Fcinfo, i: usize) -> &'a GISTENTRY {
    unsafe { &*(fcinfo.arg(i).as_usize() as *const GISTENTRY) }
}

fn entry_result(fcinfo: &Fcinfo, e: &GISTENTRY) -> PgResult<Datum> {
    let bytes = unsafe {
        core::slice::from_raw_parts((e as *const GISTENTRY).cast::<u8>(), core::mem::size_of::<GISTENTRY>())
    };
    byref_result(fcinfo.result_mcx(), bytes)
}

fn detoasted_image<'m>(mcx: mcx::Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum readable through its header.
    unsafe {
        if varatt::varatt_is_4b_u(p) {
            Ok(core::slice::from_raw_parts(p, varatt::varsize_4b(p)))
        } else if varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p) {
            let src = core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            );
            let total = 4 + src.len();
            let mut buf: mcx::PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, total)?;
            mcx::vec_append_bytes(&mut buf, &varatt::set_varsize_4b_word(total as u32).to_ne_bytes())?;
            mcx::vec_append_bytes(&mut buf, src)?;
            let out = core::slice::from_raw_parts(buf.as_ptr(), buf.len());
            core::mem::forget(buf);
            Ok(out)
        } else {
            let raw = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            let flat = detoast::detoast_attr(mcx, raw)?;
            let out = core::slice::from_raw_parts(flat.as_ptr(), flat.len());
            core::mem::forget(flat);
            Ok(out)
        }
    }
}

fn key_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: gtrgm keys are plain 4B-header images built by this module.
    unsafe { core::slice::from_raw_parts(p, varatt::varsize_4b(p)) }
}

fn image_result(fcinfo: &Fcinfo, img: &[u8]) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), img)
}

fn fc_gtrgm_in(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(PgError::error("cannot accept a value of type gtrgm").into())
}

fn fc_gtrgm_out(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(PgError::error("cannot display a value of type gtrgm").into())
}

fn fc_gtrgm_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    if entry.leafkey {
        // SAFETY: the armed result mcx outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        let img = detoasted_image(mcx, entry.key)?;
        let env = make_env();
        let trg = generate_trgm(&img[4..], &env, &legacy_crc32);
        let res = gist::encode_arrkey(&trg);
        let key = image_result(fcinfo, &res)?;
        let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
        return entry_result(fcinfo, &retval);
    }
    let key = gist::decode_key(key_image(entry.key))?;
    if let gist::TrgmKey::Sign(sign) = &key {
        if sign.iter().all(|&b| b == 0xff) {
            let res = gist::encode_signkey(true, &[]);
            let key = image_result(fcinfo, &res)?;
            let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
            return entry_result(fcinfo, &retval);
        }
    }
    Ok(fcinfo.arg(0))
}

fn fc_gtrgm_decompress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let p = entry.key.as_usize() as *const u8;
    // SAFETY: non-null varlena key readable through its header.
    if unsafe { varatt::varatt_is_4b_u(p) } {
        return Ok(fcinfo.arg(0));
    }
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let img = detoasted_image(mcx, entry.key)?;
    let retval = GISTENTRY::init(
        Datum::from_usize(img.as_ptr() as usize),
        entry.offset,
        false,
        entry.page_is_leaf,
    );
    entry_result(fcinfo, &retval)
}

// C's gtrgm_consistent_cache (fn_extra): the extracted query trigrams —
// trigram extraction and especially the regexp NFA are too expensive to
// redo per index page. `trigrams` None = regex gave no usable extraction.
struct GtrgmCache {
    strategy: u16,
    query: Vec<u8>,
    trigrams: Option<Vec<Trgm>>,
    graph: Option<TrgmPackedGraph>,
}

fn gtrgm_cached<'f>(
    f: &'f mut Option<&mut FmgrInfo>,
    fcinfo: &Fcinfo,
    strategy: u16,
) -> PgResult<&'f mut GtrgmCache> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let qimg = detoasted_image(mcx, fcinfo.arg(1))?;
    let payload = &qimg[4..];
    let flinfo = f
        .as_mut()
        .expect("pg_trgm: gist support proc called without flinfo");

    let hit = flinfo.fn_extra.as_ref().is_some_and(|e| {
        let c = e.downcast_ref::<GtrgmCache>();
        c.strategy == strategy && c.query == payload
    });
    if !hit {
        let env = make_env();
        let (trigrams, graph) = match strategy {
            SIMILARITY_STRATEGY
            | WORD_SIMILARITY_STRATEGY
            | STRICT_WORD_SIMILARITY_STRATEGY
            | EQUAL_STRATEGY
            | gist::DISTANCE_STRATEGY
            | gist::WORD_DISTANCE_STRATEGY
            | gist::STRICT_WORD_DISTANCE_STRATEGY => {
                (Some(generate_trgm(payload, &env, &legacy_crc32)), None)
            }
            LIKE_STRATEGY | ILIKE_STRATEGY => {
                (Some(generate_wildcard_trgm(payload, &env, &legacy_crc32)), None)
            }
            REGEXP_STRATEGY | REGEXP_ICASE_STRATEGY => {
                match regexp::create_trgm_nfa(payload, fcinfo.fncollation, &env, &legacy_crc32)? {
                    Some((trg, graph)) if !trg.is_empty() => (Some(trg), Some(graph)),
                    _ => (None, None),
                }
            }
            other => {
                return Err(
                    PgError::error(format!("unrecognized strategy number: {other}")).into(),
                )
            }
        };
        flinfo.fn_extra = Some(FnExtra::new(GtrgmCache {
            strategy,
            query: payload.to_vec(),
            trigrams,
            graph,
        }));
    }
    Ok(flinfo
        .fn_extra
        .as_mut()
        .expect("just installed")
        .downcast_mut::<GtrgmCache>())
}

fn fc_gtrgm_consistent(mut f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u32() as u16;
    let key = gist::decode_key(key_image(entry.key))?;
    let cache = gtrgm_cached(&mut f, fcinfo, strategy)?;
    let (res, rc) = match strategy {
        REGEXP_STRATEGY | REGEXP_ICASE_STRATEGY => {
            let GtrgmCache { trigrams, graph, .. } = cache;
            let res = match graph {
                Some(graph) => gist::consistent_regexp(
                    entry.page_is_leaf,
                    &key,
                    trigrams.as_deref(),
                    graph,
                )?,
                // Trigram-free query: recheck everywhere.
                None => true,
            };
            (res, true)
        }
        _ => {
            let nlimit = match strategy {
                SIMILARITY_STRATEGY
                | WORD_SIMILARITY_STRATEGY
                | STRICT_WORD_SIMILARITY_STRATEGY => index_strategy_get_limit(strategy)?,
                _ => 0.0,
            };
            let qtrg = cache.trigrams.as_deref().expect("non-regexp cache has trigrams");
            gist::consistent(entry.page_is_leaf, &key, qtrg, strategy, nlimit)?
        }
    };
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param live in the caller frame.
    unsafe { *recheck = rc };
    Ok(Datum::from_bool(res))
}

fn fc_gtrgm_distance(mut f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u32() as u16;
    let key = gist::decode_key(key_image(entry.key))?;
    let cache = gtrgm_cached(&mut f, fcinfo, strategy)?;
    let qtrg = cache.trigrams.as_deref().expect("distance cache has trigrams");
    let (res, rc) = gist::distance(entry.page_is_leaf, &key, qtrg, strategy)?;
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param live in the caller frame.
    unsafe { *recheck = rc };
    Ok(Datum::from_f64(res))
}

fn fc_gtrgm_union(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let siglen = get_siglen(&f);
    let mut keys = Vec::with_capacity(entryvec.n as usize);
    for e in &entryvec.vector[..entryvec.n as usize] {
        keys.push(gist::decode_key(key_image(e.key))?);
    }
    let img = gist::union(&keys, siglen);
    let size_out = fcinfo.arg(1).as_usize() as *mut i32;
    // SAFETY: size out-param live in the caller frame.
    unsafe { *size_out = img.len() as i32 };
    image_result(fcinfo, &img)
}

fn fc_gtrgm_same(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = gist::decode_key(key_image(fcinfo.arg(0)))?;
    let b = gist::decode_key(key_image(fcinfo.arg(1)))?;
    let siglen = get_siglen(&f);
    let result = fcinfo.arg(2).as_usize() as *mut bool;
    // SAFETY: result out-param live in the caller frame.
    unsafe { *result = gist::same(&a, &b, siglen) };
    Ok(fcinfo.arg(2))
}

fn fc_gtrgm_penalty(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let origentry = unsafe { entry_arg(fcinfo, 0) };
    let newentry = unsafe { entry_arg(fcinfo, 1) };
    let siglen = get_siglen(&f);
    let origval = gist::decode_key(key_image(origentry.key))?;
    let newval = gist::decode_key(key_image(newentry.key))?;
    let penalty = fcinfo.arg(2).as_usize() as *mut f32;
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *penalty = gist::penalty(&origval, &newval, siglen) };
    Ok(fcinfo.arg(2))
}

fn fc_gtrgm_picksplit(f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };
    let siglen = get_siglen(&f);

    let maxoff = (entryvec.n - 1) as usize;
    let mut keys: Vec<Option<gist::TrgmKey>> = Vec::with_capacity(maxoff + 1);
    keys.push(None);
    for e in &entryvec.vector[1..=maxoff] {
        keys.push(Some(gist::decode_key(key_image(e.key))?));
    }

    let (spl_left, spl_right, ldatum, rdatum) = gist::picksplit(&keys, siglen)?;
    v.spl_left = spl_left;
    v.spl_right = spl_right;
    v.spl_ldatum = image_result(fcinfo, &ldatum)?;
    v.spl_rdatum = image_result(fcinfo, &rdatum)?;
    Ok(fcinfo.arg(1))
}

#[cold]
fn fc_gtrgm_options(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: index_opclass_options passes &mut LocalRelopts as arg 0.
    let relopts = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut reloptions::LocalRelopts) };
    relopts.init(TRGMGISTOPTIONS_SIZE);
    relopts.add_int(
        "siglen",
        SIGLEN_DEFAULT as i32,
        1,
        SIGLEN_MAX,
        TRGMGISTOPTIONS_SIGLEN_OFF,
    );
    Ok(Datum::from_usize(0))
}

// ===========================================================================
// Registration.
// ===========================================================================

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "set_limit" => fc_set_limit,
        "show_limit" => fc_show_limit,
        "show_trgm" => fc_show_trgm,
        "similarity" => fc_similarity,
        "word_similarity" => fc_word_similarity,
        "strict_word_similarity" => fc_strict_word_similarity,
        "similarity_dist" => fc_similarity_dist,
        "similarity_op" => fc_similarity_op,
        "word_similarity_op" => fc_word_similarity_op,
        "word_similarity_commutator_op" => fc_word_similarity_commutator_op,
        "word_similarity_dist_op" => fc_word_similarity_dist_op,
        "word_similarity_dist_commutator_op" => fc_word_similarity_dist_commutator_op,
        "strict_word_similarity_op" => fc_strict_word_similarity_op,
        "strict_word_similarity_commutator_op" => fc_strict_word_similarity_commutator_op,
        "strict_word_similarity_dist_op" => fc_strict_word_similarity_dist_op,
        "strict_word_similarity_dist_commutator_op" => fc_strict_word_similarity_dist_commutator_op,
        "gtrgm_in" => fc_gtrgm_in,
        "gtrgm_out" => fc_gtrgm_out,
        "gtrgm_consistent" => fc_gtrgm_consistent,
        "gtrgm_distance" => fc_gtrgm_distance,
        "gtrgm_compress" => fc_gtrgm_compress,
        "gtrgm_decompress" => fc_gtrgm_decompress,
        "gtrgm_penalty" => fc_gtrgm_penalty,
        "gtrgm_picksplit" => fc_gtrgm_picksplit,
        "gtrgm_union" => fc_gtrgm_union,
        "gtrgm_same" => fc_gtrgm_same,
        "gtrgm_options" => fc_gtrgm_options,
        "gin_extract_value_trgm" => fc_gin_extract_value_trgm,
        "gin_extract_query_trgm" => fc_gin_extract_query_trgm,
        "gin_trgm_consistent" => fc_gin_trgm_consistent,
        "gin_trgm_triconsistent" => fc_gin_trgm_triconsistent,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
    gin_trgm_seams::trgm_extract_value::set(trgm_extract_value);
    gin_trgm_seams::trgm_extract_query::set(trgm_extract_query);
    gin_trgm_seams::trgm_consistent::set(trgm_consistent);
    gin_trgm_seams::trgm_triconsistent::set(trgm_triconsistent);
}
