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
//! at initdb). Output is identical to C; for the stateless templates the only
//! divergence is the per-call re-init (C caches it), mirroring the documented
//! `dictCtx`/fmgr divergence already noted in `ts_cache.c`'s port. The
//! `thesaurus` template is the exception: it carries a `stored` arena-index
//! cursor across consecutive `getnext` calls, so its compiled `DictThesaurus`
//! *is* cached per-OID (see [`thesaurus_cache`]) — caching is required for
//! correctness there, not just speed.

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
            // The thesaurus carries multi-call arena state (`stored`, a
            // `LexemeInfo *` in C) across `getnext` re-issues. To preserve it we
            // cache the compiled `DictThesaurus` per dictionary OID in a backend-
            // lifetime context (mirroring C's `lookup_ts_dictionary_cache`, which
            // caches `dictData`), so `stored` arena indices stay valid between
            // calls. The cursor itself rides in `DictSubState.private_state`
            // (C's `void *private_state`), encoded as `index + 1` (0 == NULL).
            let stored_in = dstate
                .as_ref()
                .map(|s| decode_stored(s.private_state))
                .unwrap_or(None);
            let isend_in = dstate.as_ref().map(|s| s.isend).unwrap_or(false);

            let (out, getnext_out, stored_out) =
                thesaurus_cache::with_dict(dict_id, &options, |cmcx, d| {
                    let mut tstate = ThesaurusSubState {
                        isend: isend_in,
                        getnext: false,
                        stored: stored_in,
                    };
                    let r = backend_tsearch_dict::dict_thesaurus::thesaurus_lexize(
                        cmcx, d, input, len, &mut tstate,
                    )?;
                    // Convert the cache-context result to owned before leaving
                    // the borrow (the cursor `stored` is exported separately).
                    Ok((convert_pgvec(r), tstate.getnext, tstate.stored))
                })?;

            if let Some(s) = dstate {
                s.getnext = getnext_out;
                s.private_state = encode_stored(stored_out);
            }
            Ok(out)
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

/// Encode the thesaurus `stored` cursor (`Option<arena index>`) into the
/// `DictSubState.private_state` `u64` (C's `void *private_state`). `None` maps
/// to 0 (C `NULL`); `Some(i)` maps to `i + 1`.
fn encode_stored(stored: Option<usize>) -> u64 {
    match stored {
        None => 0,
        Some(i) => i as u64 + 1,
    }
}

/// Inverse of [`encode_stored`].
fn decode_stored(ps: u64) -> Option<usize> {
    if ps == 0 {
        None
    } else {
        Some((ps - 1) as usize)
    }
}

/// Backend-lifetime cache of compiled `DictThesaurus` objects, keyed by
/// dictionary OID — the owned-model stand-in for C's `lookup_ts_dictionary_cache`
/// caching `dictData` in the dictionary's private memory context. Caching is
/// required (not just an optimization) because the thesaurus phrase matcher
/// carries a `stored` arena-index cursor across consecutive `getnext` calls;
/// a per-call re-init would invalidate those indices and the multi-word phrase
/// substitution could never complete.
mod thesaurus_cache {
    use alloc::collections::BTreeMap;
    use alloc::string::String;
    use core::cell::RefCell;

    use backend_commands_define_seams::DefElemArg;
    use mcx::{Mcx, McxOwned, MemoryContext};
    use types_error::PgResult;
    use types_tsearch::DictThesaurus;

    struct Cache<'mcx> {
        mcx: Mcx<'mcx>,
        dicts: BTreeMap<u32, DictThesaurus<'mcx>>,
    }

    mcx::bind!(CacheTy => Cache<'mcx>);

    thread_local! {
        static STATE: RefCell<Option<McxOwned<CacheTy>>> = const { RefCell::new(None) };
    }

    /// Run `f` over the cached `DictThesaurus` for `dict_id`, compiling and
    /// inserting it from `options` on first use. The closure receives the
    /// cache's memory context (so any result it produces is allocated there and
    /// must be converted to an owned form before returning) and a mutable
    /// borrow of the dictionary (the phrase matcher links `nextvariant` chains
    /// in the arena during matching, exactly as C mutates its cached `dictData`).
    pub fn with_dict<R>(
        dict_id: u32,
        options: &[(String, Option<DefElemArg>)],
        f: impl for<'mcx> FnOnce(Mcx<'mcx>, &mut DictThesaurus<'mcx>) -> PgResult<R>,
    ) -> PgResult<R> {
        STATE.with(|s| {
            let mut slot = s.borrow_mut();
            if slot.is_none() {
                let owned = McxOwned::<CacheTy>::try_new(
                    MemoryContext::new("Tsearch thesaurus cache"),
                    |mcx| {
                        Ok(Cache {
                            mcx,
                            dicts: BTreeMap::new(),
                        })
                    },
                )?;
                *slot = Some(owned);
            }
            slot.as_mut().unwrap().with_mut(|cache| {
                if !cache.dicts.contains_key(&dict_id) {
                    let d = backend_tsearch_dict::dict_thesaurus::thesaurus_init(
                        cache.mcx, options,
                    )?;
                    cache.dicts.insert(dict_id, d);
                }
                let d = cache.dicts.get_mut(&dict_id).unwrap();
                f(cache.mcx, d)
            })
        })
    }
}
