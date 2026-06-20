//! Port of `src/backend/tsearch/to_tsany.c` (PostgreSQL 18.3) — the `to_ts*`
//! function definitions: the `to_tsvector` builders (`make_tsvector`,
//! `uniqueWORD`, `compareWORD`) and the `to_tsquery` / `plainto_tsquery` /
//! `phraseto_tsquery` / `websearch_to_tsquery` family (with the `pushval_morph`
//! morphology callback), plus `get_current_ts_config`.
//!
//! Memory model: a `tsvector` / `tsquery` value is its flat header-ful varlena
//! image (`&[u8]` in, `Vec<u8>` out), as the sibling tsvector/tsquery core
//! crates model it. Transient working buffers are charged to a scratch
//! `MemoryContext`.
//!
//! This crate is also the owner that **installs the dictionary `lexize`
//! dispatch** the `ts_parse.c` parser needs at runtime: the `config_lenmap` /
//! `config_dict_ids` / `dict_lexize` seams (parse-seams) and the
//! `subdict_lexize` seam (dict-seams). See [`dispatch`] for the owned-model
//! lexize-by-OID resolution. It sits above both `ts_cache` (OID -> template /
//! options) and the dictionary crates (the ported `*_lexize` bodies), so it is
//! the natural home for that dispatch.
//!
//! Not ported here (documented deferrals): the `json(b)_to_tsvector` family
//! (`add_to_tsvector`, the `*_to_tsvector_worker` helpers) — those drive the
//! `jsonfuncs` GIN-iteration seams and are an additive follow-on over the same
//! `parsetext` + `make_tsvector` machine landed here.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

pub mod dispatch;
pub mod fmgr_builtins;
pub mod make_tsvector;
pub mod to_tsquery;
pub mod to_tsvector;

/// Install this crate's seams: the `parsetext` config/lexize dispatch (into
/// `backend-tsearch-parse-seams` and `backend-tsearch-dict-seams`) and the
/// `to_ts*` fmgr builtins.
///
/// Ordering: must run after the dictionary, snowball, parse, tsvector-core, and
/// tsquery-core `init_seams` (it installs *into* their seam crates and *calls*
/// their bodies), and before any SQL invocation.
pub fn init_seams() {
    backend_tsearch_parse_seams::config_lenmap::set(dispatch::config_lenmap);
    backend_tsearch_parse_seams::config_dict_ids::set(dispatch::config_dict_ids);
    backend_tsearch_parse_seams::dict_lexize::set(dispatch::dict_lexize);
    backend_tsearch_dict_seams::subdict_lexize::set(dispatch::subdict_lexize);
    fmgr_builtins::register_to_tsany_builtins();
}
