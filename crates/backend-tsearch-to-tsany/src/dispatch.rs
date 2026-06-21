//! The owned-model text-search dictionary `lexize` dispatch and the
//! config-cache projections `parsetext` needs.
//!
//! `parsetext` (`ts_parse.c`) reaches three seams that have no production owner
//! until this crate lands:
//!
//!  * `config_lenmap(cfg)` / `config_dict_ids(cfg, ttype)` — `cfg->lenmap` /
//!    `cfg->map[ttype]`, projected from `lookup_ts_config_cache`.
//!  * `dict_lexize(dictId, lemm, dstate)` — C's
//!    `FunctionCall4(&dict->lexize, dictData, lemm, lenlemm, &dstate)`.
//!
//! `dict_thesaurus.c` additionally reaches `subdict_lexize(dictId, input)` —
//! the single-shot (`dstate == NULL`) form of the same dispatch.
//!
//! ## The owned-model dispatch
//!
//! C's `lookup_ts_dictionary_cache` caches the *typed* dictionary object
//! (`dictData`, a `void *`) in the dictionary's private memory context and
//! reaches its `lexize` method through an `FmgrInfo`. In the owned model the
//! cache stores only an opaque token (`fmgr_seams::oid_function_call_1_deflist`
//! produces a `ScalarWord`, not a typed dict), so the typed object is not
//! reachable by OID. This dispatch therefore rebuilds the typed dictionary from
//! its `pg_ts_dict.dictinitoption` deflist on each call — running the ported
//! `*_init` then the ported `*_lexize` — keyed on the template's
//! `tmpllexize` method name (`get_func_name`), which is stable for both the
//! fixed-OID builtin templates and the snowball template (whose OID is assigned
//! at initdb). Output is identical to C; the only divergence is the per-call
//! re-init (C caches it). This mirrors the documented `dictCtx`/fmgr divergence
//! already noted in `ts_cache.c`'s port.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use backend_commands_define_seams::DefElemArg;
use backend_tsearch_parse_seams::{DictSubState, LexizeLexeme};
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_ts_cache as ts_cache;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_tsearch::{OwnedTSLexeme, TSLexeme, ThesaurusSubState};

/// `cfg->lenmap` (ts_parse.c): the length of configuration `cfg_id`'s
/// token-type -> dictionary map.
pub fn config_lenmap(cfg_id: u32) -> PgResult<i32> {
    let ctx = MemoryContext::new("config_lenmap");
    let mcx = ctx.mcx();
    let cfg = ts_cache::lookup_ts_config_cache(mcx, Oid::from(cfg_id))?;
    Ok(cfg.map.len() as i32)
}

/// `cfg->map[token_type]` (ts_parse.c): the dictionary OIDs mapped to
/// `token_type` for configuration `cfg_id`. An empty vec means no dictionaries.
pub fn config_dict_ids(cfg_id: u32, token_type: i32) -> PgResult<Vec<u32>> {
    let ctx = MemoryContext::new("config_dict_ids");
    let mcx = ctx.mcx();
    let cfg = ts_cache::lookup_ts_config_cache(mcx, Oid::from(cfg_id))?;
    if token_type < 0 || token_type as usize >= cfg.map.len() {
        return Ok(Vec::new());
    }
    let ids = cfg.map[token_type as usize]
        .dict_ids
        .iter()
        .map(|o| u32::from(*o))
        .collect();
    Ok(ids)
}

/// `FunctionCall4(&dict->lexize, dictData, lemm, lenlemm, &dstate)`
/// (ts_parse.c `LexizeExec`): run dictionary `dict_id`'s lexize over `lemm`,
/// threading the `DictSubState`.
pub fn dict_lexize(
    dict_id: u32,
    lemm: Vec<u8>,
    mut dstate: DictSubState,
) -> PgResult<(DictSubState, Option<Vec<LexizeLexeme>>)> {
    let ctx = MemoryContext::new("dict_lexize");
    let mcx = ctx.mcx();

    let res = lexize_by_oid(mcx, Oid::from(dict_id), &lemm, Some(&mut dstate))?;
    let out = res.map(|v| v.into_iter().map(to_lexize_lexeme).collect());
    Ok((dstate, out))
}

/// `FunctionCall4(&dict->lexize, dictData, in, len, PointerGetDatum(NULL))`
/// (dict.c / dict_thesaurus.c): the single-shot (`dstate == NULL`) lexize-by-OID
/// dispatch.
pub fn subdict_lexize(dict_id: Oid, input: Vec<u8>) -> PgResult<Option<Vec<OwnedTSLexeme>>> {
    let ctx = MemoryContext::new("subdict_lexize");
    let mcx = ctx.mcx();

    let res = lexize_by_oid(mcx, dict_id, &input, None)?;
    let out = res.map(|v| v.into_iter().map(to_owned_lexeme).collect());
    Ok(out)
}

/// The shared lexize-by-OID core: resolve the dictionary's template + options,
/// rebuild the typed dictionary, and run its ported `*_lexize` body. Returns
/// the owned lexeme list (`None` = C `NULL`, `Some(vec![])` = stopword reject).
fn lexize_by_oid(
    mcx: Mcx<'_>,
    dict_id: Oid,
    input: &[u8],
    dstate: Option<&mut DictSubState>,
) -> PgResult<Option<Vec<OwnedLexeme>>> {
    let info = ts_cache::lookup_ts_dict_template_info(mcx, dict_id)?;
    let options = deflist_pairs(&info.options);
    let len = input.len() as i32;

    // Key on the template's lexize method name (stable for builtin + snowball).
    let name = lsyscache::get_func_name::call(mcx, info.lexize_oid)?
        .map(|s| s.as_str().to_string())
        .ok_or_else(|| {
            PgError::error(alloc::format!(
                "cache lookup failed for function {}",
                u32::from(info.lexize_oid)
            ))
        })?;

    match name.as_str() {
        "dsimple_lexize" => {
            let d = backend_tsearch_dict::dict_simple::dsimple_init(mcx, &options)?;
            let r = backend_tsearch_dict::dict_simple::dsimple_lexize(mcx, &d, input, len)?;
            Ok(convert_pgvec(r))
        }
        "dsnowball_lexize" => {
            let d = backend_snowball_dict_snowball::dsnowball_init(mcx, &options)?;
            let r = backend_snowball_dict_snowball::dsnowball_lexize(mcx, &d, input, len)?;
            Ok(convert_pgvec(r))
        }
        "dsynonym_lexize" => {
            let d = backend_tsearch_dict::dict_synonym::dsynonym_init(mcx, &options)?;
            let r = backend_tsearch_dict::dict_synonym::dsynonym_lexize(mcx, &d, input, len)?;
            Ok(convert_pgvec(r))
        }
        "dispell_lexize" => {
            let d = backend_tsearch_ispell_regis::dispell_init(mcx, &options)?;
            let r = backend_tsearch_ispell_regis::dispell_lexize(mcx, &d, input, len)?;
            Ok(convert_pgvec(r))
        }
        "thesaurus_lexize" => {
            // The thesaurus carries multi-call arena state (`stored`) across
            // getnext re-issues. The owned-model per-call re-init resets that
            // arena, so the `private_state` cursor cannot persist; we run a
            // fresh single-shot lexize (the `stored == None` entry path), which
            // is correct for a phrase resolved within one call. Multi-call
            // thesaurus phrase continuation is the documented re-init divergence.
            let mut d = backend_tsearch_dict::dict_thesaurus::thesaurus_init(mcx, &options)?;
            let mut tstate = ThesaurusSubState {
                isend: dstate.as_ref().map(|s| s.isend).unwrap_or(false),
                getnext: false,
                stored: None,
            };
            let r = backend_tsearch_dict::dict_thesaurus::thesaurus_lexize(
                mcx, &mut d, input, len, &mut tstate,
            )?;
            if let Some(s) = dstate {
                s.getnext = tstate.getnext;
                // `stored` (an arena index into the freshly-built `d`) cannot be
                // exported across the re-init boundary; leave `private_state`
                // untouched so the caller treats this as a completed lexize.
            }
            Ok(convert_pgvec(r))
        }
        other => Err(PgError::error(alloc::format!(
            "text search lexize method \"{other}\" is not supported"
        ))),
    }
}

/// A template/dispatch-neutral owned lexeme (the fields shared by `TSLexeme`,
/// `LexizeLexeme`, and `OwnedTSLexeme`).
struct OwnedLexeme {
    nvariant: u16,
    flags: u16,
    lexeme: Vec<u8>,
}

/// Convert a ported lexize result (`Option<PgVec<TSLexeme>>`) into the
/// dispatch-neutral form. `None` stays `None`; an empty/populated vec is kept.
fn convert_pgvec(r: Option<PgVec<'_, TSLexeme<'_>>>) -> Option<Vec<OwnedLexeme>> {
    r.map(|v| {
        v.iter()
            .map(|lx| OwnedLexeme {
                nvariant: lx.nvariant,
                flags: lx.flags,
                lexeme: lx.lexeme.as_str().as_bytes().to_vec(),
            })
            .collect()
    })
}

fn to_lexize_lexeme(l: OwnedLexeme) -> LexizeLexeme {
    LexizeLexeme {
        nvariant: l.nvariant,
        flags: l.flags,
        lexeme: l.lexeme,
    }
}

fn to_owned_lexeme(l: OwnedLexeme) -> OwnedTSLexeme {
    OwnedTSLexeme {
        nvariant: l.nvariant,
        flags: l.flags,
        lexeme: String::from_utf8_lossy(&l.lexeme).into_owned(),
    }
}

/// Convert the `deserialize_deflist` rows into the `(defname, arg)` pairs the
/// `*_init` functions read, rebuilding each arg with its `buildDefItem`-inferred
/// node kind so `defGetBoolean`/`defGetInt32`/... see the same `nodeTag` C does
/// (e.g. `casesensitive = 1` reaches `defGetBoolean` as a `T_Integer`).
fn deflist_pairs(
    options: &PgVec<'_, types_cache::deflist::DefElemString<'_>>,
) -> Vec<(String, Option<DefElemArg>)> {
    use types_cache::deflist::DefElemValKind;
    options
        .iter()
        .map(|de| {
            let val = de.arg.as_str();
            let arg = match de.kind {
                DefElemValKind::Integer => DefElemArg::Integer(val.parse::<i64>().unwrap_or(0)),
                DefElemValKind::Float => DefElemArg::Float(val.to_string()),
                DefElemValKind::Boolean => DefElemArg::Boolean(val == "true"),
                DefElemValKind::String => DefElemArg::String(val.to_string()),
            };
            (de.defname.as_str().to_string(), Some(arg))
        })
        .collect()
}
