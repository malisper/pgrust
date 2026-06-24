#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `contrib/pg_trgm` — trigram-based text-similarity functions and operators,
//! ported as an in-process Rust builtin library.
//!
//! The scalar half of `trgm_op.c` is ported here 1:1 (the trigram extraction
//! algorithm lives in [`trgm`], unit-tested against the reference `show_trgm` /
//! `similarity` values). The fmgr-boundary marshaling (reading `text` args off
//! the by-ref lane, returning `float4`/`bool`/`text[]`) follows the same idioms
//! as `backend-utils-adt-varlena` / `pg_prewarm`.
//!
//! Registration mirrors `pg_prewarm`: the SQL emitted by the install script
//! (`CREATE FUNCTION similarity(text,text) ... LANGUAGE C AS
//! 'MODULE_PATHNAME','similarity'`) resolves through the dynamic-loader unit's
//! ported-library registry (the Rust backend exposes no C ABI), and the
//! `_PG_init` callback defines the three `pg_trgm.*_threshold` GUCs.
//!
//! The GIN/GiST index-access-method opclass support functions
//! (`gin_trgm_ops` / `gist_trgm_ops`, from `trgm_gin.c` / `trgm_gist.c` /
//! `trgm_regexp.c`) are NOT ported here — see the module docs and the port
//! report for the index-opclass gap. The scalar functions/operators make
//! `similarity()` + the `%`/`<%`/`<<%`/`<->` family work on sequential scans;
//! the opclass support functions are registered as loud-panic stubs so
//! `CREATE EXTENSION` (which validates every C symbol via `fmgr_c_validator`)
//! succeeds, but building a trigram index or scanning one panics.

mod trgm;

use ::datum::Datum;
use ::fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};
use ::fmgr::boundary::RefPayload;
use ::mcx::MemoryContext;
use ::types_error::{PgError, PgResult};
use ::types_tuple::heaptuple::DEFAULT_COLLATION_OID;

use std::cell::Cell;

use trgm::{
    calc_word_similarity, cnt_sml, generate_trgm, trgm2int, Trgm, TrgmEnv,
    WORD_SIMILARITY_CHECK_ONLY, WORD_SIMILARITY_STRICT,
};

/// The simple (suffix-free) module name — `$libdir/pg_trgm` reduces to this.
const LIBRARY: &str = "pg_trgm";

// ===========================================================================
// GUC variables (pg_trgm.{similarity,word_similarity,strict_word_similarity}
// _threshold). C declares them as `double` file-scope globals; here they are
// thread-local cells the custom-GUC accessors read/write (the owned GUC model).
// ===========================================================================

thread_local! {
    /// `double similarity_threshold = 0.3f;`
    static SIMILARITY_THRESHOLD: Cell<f64> = const { Cell::new(0.3) };
    /// `double word_similarity_threshold = 0.6f;`
    static WORD_SIMILARITY_THRESHOLD: Cell<f64> = const { Cell::new(0.6) };
    /// `double strict_word_similarity_threshold = 0.5f;`
    static STRICT_WORD_SIMILARITY_THRESHOLD: Cell<f64> = const { Cell::new(0.5) };
}

fn get_similarity_threshold() -> f64 {
    SIMILARITY_THRESHOLD.with(Cell::get)
}
fn set_similarity_threshold(v: f64) {
    SIMILARITY_THRESHOLD.with(|c| c.set(v));
}
fn get_word_similarity_threshold() -> f64 {
    WORD_SIMILARITY_THRESHOLD.with(Cell::get)
}
fn set_word_similarity_threshold(v: f64) {
    WORD_SIMILARITY_THRESHOLD.with(|c| c.set(v));
}
fn get_strict_word_similarity_threshold() -> f64 {
    STRICT_WORD_SIMILARITY_THRESHOLD.with(Cell::get)
}
fn set_strict_word_similarity_threshold(v: f64) {
    STRICT_WORD_SIMILARITY_THRESHOLD.with(|c| c.set(v));
}

// ===========================================================================
// Backend services threaded into the trigram core.
// ===========================================================================

/// The legacy CRC32 used by `compact_trigram` (`INIT_LEGACY_CRC32` /
/// `COMP_LEGACY_CRC32` / `FIN_LEGACY_CRC32` in pg_crc.h) — the traditional
/// (non-reflected, table-driven) CRC32.
fn legacy_crc32(bytes: &[u8]) -> u32 {
    ::crc32c::legacy::traditional_crc32(bytes)
}

/// Build the [`TrgmEnv`] from the live backend encoding/locale services.
fn make_env() -> TrgmEnv<'static> {
    let max_encoding_len = ::mbutils::pg_database_encoding_max_length();
    TrgmEnv {
        max_encoding_len,
        mblen: &mblen,
        isalnum: &isalnum,
        tolower: &tolower,
    }
}

/// `pg_mblen(ptr)`.
fn mblen(s: &[u8]) -> i32 {
    ::mbutils::pg_mblen(s)
}

/// `ISWORDCHR(c, len)` = `t_isalnum_with_len(c, len)`.
fn isalnum(s: &[u8]) -> bool {
    if s.is_empty() {
        return false;
    }
    ::ts_locale_seams::t_isalnum::call(s)
}

/// `str_tolower(buff, len, DEFAULT_COLLATION_OID)` — IGNORECASE case-fold.
fn tolower(s: &[u8]) -> Vec<u8> {
    let m = MemoryContext::new("pg_trgm tolower scratch");
    let lowered = ::formatting_seams::str_tolower::call(m.mcx(), s, DEFAULT_COLLATION_OID)
        .unwrap_or_else(|e| raise(e));
    lowered.as_slice().to_vec()
}

// ===========================================================================
// fmgr argument readers / error raising.
// ===========================================================================

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// `PGFunction` crosses (the `catch_unwind` in fmgr's `invoke_pgfunction`),
/// which downcasts the panic payload back to the structured [`PgError`].
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

/// `text_to_cstring(PG_GETARG_TEXT_PP(i))` — a `text` arg's `VARDATA_ANY`
/// payload bytes (the C `char *` + `VARSIZE_ANY_EXHDR`). pg_trgm functions are
/// STRICT, so the arg is never NULL.
fn arg_text_bytes(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Vec<u8> {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pg_trgm: text arg missing from by-ref lane");
    varlena_payload(image).to_vec()
}

/// `PG_GETARG_FLOAT4(i)`.
fn arg_float4(fcinfo: &FunctionCallInfoBaseData, i: usize) -> f32 {
    fcinfo
        .arg(i)
        .expect("pg_trgm: missing float4 arg")
        .value
        .as_f32()
}

/// `PG_GETARG_DATUM(i)` for a by-ref text arg, re-read as bytes — used by the
/// `*_dist`/`*_op` variants that the C drives via `DirectFunctionCall2`.
fn arg_datum_text_bytes(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Vec<u8> {
    arg_text_bytes(fcinfo, i)
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image.
fn varlena_payload(image: &[u8]) -> &[u8] {
    match image.first() {
        // VARATT_IS_1B && !VARATT_IS_1B_E: short 1-byte header (skip 1 byte).
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        // 4-byte uncompressed header (skip VARHDRSZ).
        Some(_) if image.len() >= ::datum::varlena::VARHDRSZ => {
            &image[::datum::varlena::VARHDRSZ..]
        }
        _ => &[],
    }
}

// ===========================================================================
// Scalar function bodies (trgm_op.c).
// ===========================================================================

/// `show_trgm(text) RETURNS text[]`.
fn fc_show_trgm(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let input = arg_text_bytes(fcinfo, 0);
    let env = make_env();
    let trg = generate_trgm(&input, &env, &legacy_crc32);

    let multibyte = ::mbutils::pg_database_encoding_max_length() > 1;

    // Build each element's text bytes. For multibyte encodings, a
    // non-ISPRINTABLETRGM trigram is rendered as "0x%06x".
    let items: Vec<Vec<u8>> = trg
        .iter()
        .map(|t| {
            if multibyte && !is_printable_trgm(t) {
                format!("0x{:06x}", trgm2int(t)).into_bytes()
            } else {
                t.to_vec()
            }
        })
        .collect();

    let m = MemoryContext::new("pg_trgm show_trgm scratch");
    let elems: Vec<Option<&[u8]>> = items.iter().map(|b| Some(b.as_slice())).collect();
    let image = ::arrayfuncs_seams::build_text_array_nullable::call(m.mcx(), &elems)
        .unwrap_or_else(|e| raise(e))
        .as_slice()
        .to_vec();

    fcinfo.set_ref_result(RefPayload::Varlena(image));
    fcinfo.isnull = false;
    Datum::from_usize(0)
}

/// `ISPRINTABLECHAR(a)` = isascii && (isalnum || ' ').
fn is_printable_char(c: u8) -> bool {
    c.is_ascii() && (c.is_ascii_alphanumeric() || c == b' ')
}

/// `ISPRINTABLETRGM(t)` — all three chars printable.
fn is_printable_trgm(t: &Trgm) -> bool {
    is_printable_char(t[0]) && is_printable_char(t[1]) && is_printable_char(t[2])
}

/// `similarity(text,text) RETURNS float4`.
fn fc_similarity(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = similarity_value(fcinfo, 0, 1);
    fcinfo.isnull = false;
    Datum::from_f32(res)
}

fn similarity_value(fcinfo: &FunctionCallInfoBaseData, a: usize, b: usize) -> f32 {
    let in1 = arg_text_bytes(fcinfo, a);
    let in2 = arg_text_bytes(fcinfo, b);
    let env = make_env();
    let trg1 = generate_trgm(&in1, &env, &legacy_crc32);
    let trg2 = generate_trgm(&in2, &env, &legacy_crc32);
    cnt_sml(&trg1, &trg2, false)
}

/// Compute `calc_word_similarity` over the args, with the given (str1,str2)
/// order and flags.
fn word_sim_value(
    fcinfo: &FunctionCallInfoBaseData,
    a: usize,
    b: usize,
    flags: u8,
) -> f32 {
    let in_a = arg_text_bytes(fcinfo, a);
    let in_b = arg_text_bytes(fcinfo, b);
    let env = make_env();
    calc_word_similarity(
        &in_a,
        &in_b,
        flags,
        &env,
        &legacy_crc32,
        get_word_similarity_threshold(),
        get_strict_word_similarity_threshold(),
    )
}

/// `word_similarity(text,text) RETURNS float4`.
fn fc_word_similarity(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = word_sim_value(fcinfo, 0, 1, 0);
    fcinfo.isnull = false;
    Datum::from_f32(res)
}

/// `strict_word_similarity(text,text) RETURNS float4`.
fn fc_strict_word_similarity(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = word_sim_value(fcinfo, 0, 1, WORD_SIMILARITY_STRICT);
    fcinfo.isnull = false;
    Datum::from_f32(res)
}

/// `similarity_dist(text,text) RETURNS float4` = `1 - similarity`.
fn fc_similarity_dist(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let _ = arg_datum_text_bytes(fcinfo, 0); // mirror DirectFunctionCall arg read
    let res = similarity_value(fcinfo, 0, 1);
    fcinfo.isnull = false;
    Datum::from_f32(1.0 - res)
}

/// `similarity_op(text,text) RETURNS bool` = `similarity >= similarity_threshold`.
fn fc_similarity_op(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = similarity_value(fcinfo, 0, 1);
    fcinfo.isnull = false;
    Datum::from_bool(res >= get_similarity_threshold() as f32)
}

/// `word_similarity_op(text,text)` = check-only word sim >= word threshold.
fn fc_word_similarity_op(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = word_sim_value(fcinfo, 0, 1, WORD_SIMILARITY_CHECK_ONLY);
    fcinfo.isnull = false;
    Datum::from_bool(res >= get_word_similarity_threshold() as f32)
}

/// `word_similarity_commutator_op(text,text)` — args swapped.
fn fc_word_similarity_commutator_op(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = word_sim_value(fcinfo, 1, 0, WORD_SIMILARITY_CHECK_ONLY);
    fcinfo.isnull = false;
    Datum::from_bool(res >= get_word_similarity_threshold() as f32)
}

/// `word_similarity_dist_op(text,text)` = `1 - word_similarity` (args in order).
fn fc_word_similarity_dist_op(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = word_sim_value(fcinfo, 0, 1, 0);
    fcinfo.isnull = false;
    Datum::from_f32(1.0 - res)
}

/// `word_similarity_dist_commutator_op(text,text)` = `1 - word_similarity`,
/// args swapped.
fn fc_word_similarity_dist_commutator_op(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = word_sim_value(fcinfo, 1, 0, 0);
    fcinfo.isnull = false;
    Datum::from_f32(1.0 - res)
}

/// `strict_word_similarity_op(text,text)` — check-only strict word sim.
fn fc_strict_word_similarity_op(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = word_sim_value(
        fcinfo,
        0,
        1,
        WORD_SIMILARITY_CHECK_ONLY | WORD_SIMILARITY_STRICT,
    );
    fcinfo.isnull = false;
    Datum::from_bool(res >= get_strict_word_similarity_threshold() as f32)
}

/// `strict_word_similarity_commutator_op(text,text)` — args swapped.
fn fc_strict_word_similarity_commutator_op(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = word_sim_value(
        fcinfo,
        1,
        0,
        WORD_SIMILARITY_CHECK_ONLY | WORD_SIMILARITY_STRICT,
    );
    fcinfo.isnull = false;
    Datum::from_bool(res >= get_strict_word_similarity_threshold() as f32)
}

/// `strict_word_similarity_dist_op(text,text)` = `1 - strict_word_similarity`.
fn fc_strict_word_similarity_dist_op(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let res = word_sim_value(fcinfo, 0, 1, WORD_SIMILARITY_STRICT);
    fcinfo.isnull = false;
    Datum::from_f32(1.0 - res)
}

/// `strict_word_similarity_dist_commutator_op(text,text)` — args swapped.
fn fc_strict_word_similarity_dist_commutator_op(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> Datum {
    let res = word_sim_value(fcinfo, 1, 0, WORD_SIMILARITY_STRICT);
    fcinfo.isnull = false;
    Datum::from_f32(1.0 - res)
}

/// `set_limit(float4) RETURNS float4` — deprecated; sets the
/// `pg_trgm.similarity_threshold` GUC and returns the new value. Ported as a
/// direct threshold set (the C round-trips through `SetConfigOption`).
fn fc_set_limit(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let nlimit = arg_float4(fcinfo, 0);
    if !(0.0..=1.0).contains(&nlimit) {
        raise(PgError::error(
            "pg_trgm.similarity_threshold must be in range [0, 1]",
        ));
    }
    set_similarity_threshold(nlimit as f64);
    fcinfo.isnull = false;
    Datum::from_f32(get_similarity_threshold() as f32)
}

/// `show_limit() RETURNS float4` — deprecated; returns the current threshold.
fn fc_show_limit(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.isnull = false;
    Datum::from_f32(get_similarity_threshold() as f32)
}

// ===========================================================================
// Index-opclass support stubs (trgm_gin.c / trgm_gist.c / trgm_regexp.c).
//
// pgrust's GIN/GiST custom-opclass extensibility is not wired for pg_trgm, so
// these symbols are registered as loud-panic stubs: `CREATE EXTENSION`'s C
// validator (fmgr_c_validator -> load_external_function) must find the symbol,
// but building or scanning a trigram index mirror-pg-and-panics.
// ===========================================================================

fn unported_index_symbol(name: &'static str) -> ! {
    raise(PgError::error(&format!(
        "pg_trgm: index-opclass support function \"{name}\" \
         (trgm_gin.c/trgm_gist.c/trgm_regexp.c) is unported — GIN/GiST \
         custom-opclass extensibility for pg_trgm is not yet ported"
    )));
}

macro_rules! index_stub {
    ($fn_name:ident, $sym:literal) => {
        fn $fn_name(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            unported_index_symbol($sym);
        }
    };
}

index_stub!(fc_gtrgm_in, "gtrgm_in");
index_stub!(fc_gtrgm_out, "gtrgm_out");
index_stub!(fc_gtrgm_consistent, "gtrgm_consistent");
index_stub!(fc_gtrgm_distance, "gtrgm_distance");
index_stub!(fc_gtrgm_compress, "gtrgm_compress");
index_stub!(fc_gtrgm_decompress, "gtrgm_decompress");
index_stub!(fc_gtrgm_penalty, "gtrgm_penalty");
index_stub!(fc_gtrgm_picksplit, "gtrgm_picksplit");
index_stub!(fc_gtrgm_union, "gtrgm_union");
index_stub!(fc_gtrgm_same, "gtrgm_same");
index_stub!(fc_gtrgm_options, "gtrgm_options");
index_stub!(fc_gin_extract_value_trgm, "gin_extract_value_trgm");
index_stub!(fc_gin_extract_query_trgm, "gin_extract_query_trgm");
index_stub!(fc_gin_trgm_consistent, "gin_trgm_consistent");
index_stub!(fc_gin_trgm_triconsistent, "gin_trgm_triconsistent");

// ===========================================================================
// _PG_init — define the three custom GUCs.
// ===========================================================================

/// `_PG_init(void)` (trgm_op.c) — define `pg_trgm.*_threshold` and reserve the
/// `pg_trgm` GUC prefix. Idempotent (the GUC registry tolerates re-definition;
/// the builtin-library loader may call this on every LOAD).
fn pg_init() -> PgResult<()> {
    use ::misc_guc::custom;
    use ::guc_tables::GucVarAccessors;
    use ::types_guc::PGC_USERSET;

    let _ = custom::define_custom_real_variable(
        "pg_trgm.similarity_threshold",
        Some("Sets the threshold used by the % operator."),
        Some("Valid range is 0.0 .. 1.0."),
        GucVarAccessors {
            get: get_similarity_threshold,
            set: set_similarity_threshold,
        },
        0.3,
        0.0,
        1.0,
        PGC_USERSET,
        0,
        None,
        None,
        None,
    );
    let _ = custom::define_custom_real_variable(
        "pg_trgm.word_similarity_threshold",
        Some("Sets the threshold used by the <% operator."),
        Some("Valid range is 0.0 .. 1.0."),
        GucVarAccessors {
            get: get_word_similarity_threshold,
            set: set_word_similarity_threshold,
        },
        0.6,
        0.0,
        1.0,
        PGC_USERSET,
        0,
        None,
        None,
        None,
    );
    let _ = custom::define_custom_real_variable(
        "pg_trgm.strict_word_similarity_threshold",
        Some("Sets the threshold used by the <<% operator."),
        Some("Valid range is 0.0 .. 1.0."),
        GucVarAccessors {
            get: get_strict_word_similarity_threshold,
            set: set_strict_word_similarity_threshold,
        },
        0.5,
        0.0,
        1.0,
        PGC_USERSET,
        0,
        None,
        None,
        None,
    );

    custom::mark_guc_prefix_reserved("pg_trgm");
    Ok(())
}

// ===========================================================================
// Builtin-library registration.
// ===========================================================================

/// Resolve a symbol of the `pg_trgm` module to its ported `PGFunction`.
fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        // Scalar functions / operators (ported).
        "set_limit" => Some(fc_set_limit),
        "show_limit" => Some(fc_show_limit),
        "show_trgm" => Some(fc_show_trgm),
        "similarity" => Some(fc_similarity),
        "word_similarity" => Some(fc_word_similarity),
        "strict_word_similarity" => Some(fc_strict_word_similarity),
        "similarity_dist" => Some(fc_similarity_dist),
        "similarity_op" => Some(fc_similarity_op),
        "word_similarity_op" => Some(fc_word_similarity_op),
        "word_similarity_commutator_op" => Some(fc_word_similarity_commutator_op),
        "word_similarity_dist_op" => Some(fc_word_similarity_dist_op),
        "word_similarity_dist_commutator_op" => Some(fc_word_similarity_dist_commutator_op),
        "strict_word_similarity_op" => Some(fc_strict_word_similarity_op),
        "strict_word_similarity_commutator_op" => Some(fc_strict_word_similarity_commutator_op),
        "strict_word_similarity_dist_op" => Some(fc_strict_word_similarity_dist_op),
        "strict_word_similarity_dist_commutator_op" => {
            Some(fc_strict_word_similarity_dist_commutator_op)
        }

        // GiST opclass support (unported — loud-panic stubs).
        "gtrgm_in" => Some(fc_gtrgm_in),
        "gtrgm_out" => Some(fc_gtrgm_out),
        "gtrgm_consistent" => Some(fc_gtrgm_consistent),
        "gtrgm_distance" => Some(fc_gtrgm_distance),
        "gtrgm_compress" => Some(fc_gtrgm_compress),
        "gtrgm_decompress" => Some(fc_gtrgm_decompress),
        "gtrgm_penalty" => Some(fc_gtrgm_penalty),
        "gtrgm_picksplit" => Some(fc_gtrgm_picksplit),
        "gtrgm_union" => Some(fc_gtrgm_union),
        "gtrgm_same" => Some(fc_gtrgm_same),
        "gtrgm_options" => Some(fc_gtrgm_options),

        // GIN opclass support (unported — loud-panic stubs).
        "gin_extract_value_trgm" => Some(fc_gin_extract_value_trgm),
        "gin_extract_query_trgm" => Some(fc_gin_extract_query_trgm),
        "gin_trgm_consistent" => Some(fc_gin_trgm_consistent),
        "gin_trgm_triconsistent" => Some(fc_gin_trgm_triconsistent),

        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        api_version: 1,
    })
}

/// Install this unit's inward seams: register the `pg_trgm` module with the
/// dynamic-loader unit's ported-library registry.
pub fn init_seams() {
    ::dfmgr_seams::register_builtin_library(::dfmgr_seams::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: Some(pg_init),
    });
}
