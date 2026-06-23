//! Port of the tsearch dictionary templates (PostgreSQL 18.3):
//!
//! * `dict.c` — [`dict`]: the `ts_lexize(regdictionary, text)` SQL debug
//!   function;
//! * `dict_simple.c` — [`dict_simple`]: the `simple` dictionary (lowercase +
//!   optional stop-word filtering);
//! * `dict_synonym.c` — [`dict_synonym`]: the `synonym` dictionary (`in -> out`
//!   word rewriting, optional case-sensitivity / prefix `*`);
//! * `dict_thesaurus.c` — [`dict_thesaurus`]: the `thesaurus` dictionary
//!   (phrase-to-phrase substitution through a sub-dictionary).
//!
//! `dict_ispell.c` (the `ispell` dictionary template) is ported in the sibling
//! `backend-tsearch-ispell-regis` unit.
//!
//! # Owned model
//!
//! C builds intrusive `LexemeInfo` linked lists (`nextentry` / `nextvariant`
//! `palloc`'d pointers) and stores a raw `LexemeInfo *` in
//! `DictSubState.private_state`. This port replaces every pointer with an
//! **index into the carrying [`DictThesaurus`](tsearch::DictThesaurus)'s
//! `arena`** (`Option<usize>`), reproducing `findVariant` / `matchIdSubst` /
//! `checkMatch` 1:1; the cross-call state is carried in
//! [`ThesaurusSubState`](tsearch::ThesaurusSubState). No raw pointers, no
//! `palloc`/`pfree`, no `unsafe`.
//!
//! # Seams
//!
//! Genuinely-external helpers cross seams to already-ported owners (`defGet*`,
//! `str_tolower`, `readstoplist`/`searchstoplist`/`get_tsearch_config_filename`,
//! the `tsearch_readline` config reader, `pg_mblen`, `construct_array_builtin`).
//! The sub-dictionary `lexize` fmgr-dispatch and the thesaurus sub-dictionary
//! name resolution cross this unit's own outward seams
//! ([`dict_seams::subdict_lexize`] /
//! [`dict_seams::get_ts_dict_oid_from_name`]); their owners
//! (`tsearch/ts_cache.c` + fmgr) are not ported yet, so they panic loudly until
//! installed (project rule: no silent fallback).

// NB: not `#![no_std]` — the fmgr builtin registration layer (`fmgr_builtins`)
// needs `std` (the `register_builtins_native` table + `String`/`Vec` result
// framing). The dictionary-template value cores remain `alloc`-only.
#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;

use ::utils_error::ereport;
use ::types_error::{PgError, ERRCODE_CONFIG_FILE_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};

pub mod dict;
pub mod fmgr_builtins;
pub mod dict_simple;
pub mod dict_synonym;
pub mod dict_thesaurus;

/// `DEFAULT_COLLATION_OID` (`pg_collation_d.h`).
pub(crate) const DEFAULT_COLLATION_OID: types_core::Oid = 100;

/// `isspace((unsigned char) c)` for the C locale (the bytes the tsearch parsers
/// treat as whitespace).
#[inline]
pub(crate) fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// `ereport(ERROR, errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg(message))`.
pub(crate) fn invalid_param(message: impl Into<String>) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(message)
        .into_error()
}

/// `ereport(ERROR, errcode(ERRCODE_CONFIG_FILE_ERROR), errmsg(message))`.
pub(crate) fn config_file_error(message: impl Into<String>) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_CONFIG_FILE_ERROR)
        .errmsg(message)
        .into_error()
}

/// `ereport(ERROR, errcode(ERRCODE_CONFIG_FILE_ERROR), errmsg(msg), errhint(h))`.
pub(crate) fn config_file_error_hint(
    message: impl Into<String>,
    hint: impl Into<String>,
) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_CONFIG_FILE_ERROR)
        .errmsg(message)
        .errhint(hint)
        .into_error()
}

/// `elog(ERROR, message)` — an internal-error message.
pub(crate) fn elog_error(message: impl Into<String>) -> PgError {
    ereport(ERROR).errmsg_internal(message).into_error()
}

/// Install every seam this unit owns
/// (`backend-tsearch-dict-seams`): the `simple` / `synonym` / `thesaurus`
/// dictionary templates' `init`/`lexize` fmgr methods and the `ts_lexize` SQL
/// debug function.
pub fn init_seams() {
    dict_seams::dsimple_init::set(dict_simple::dsimple_init);
    dict_seams::dsimple_lexize::set(dict_simple::dsimple_lexize);
    dict_seams::dsynonym_init::set(dict_synonym::dsynonym_init);
    dict_seams::dsynonym_lexize::set(dict_synonym::dsynonym_lexize);
    dict_seams::thesaurus_init::set(dict_thesaurus::thesaurus_init);
    dict_seams::thesaurus_lexize::set(dict_thesaurus::thesaurus_lexize);
    dict_seams::ts_lexize::set(dict::ts_lexize);

    // dict.c: register the `ts_lexize(regdictionary, text)` fmgr builtin into
    // fmgr-core's by-OID dispatch table.
    fmgr_builtins::register_dict_builtins();
}
