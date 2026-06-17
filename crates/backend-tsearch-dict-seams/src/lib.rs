//! Seam declarations for the `backend-tsearch-dict` unit (`tsearch/dict.c`,
//! `dict_simple.c`, `dict_synonym.c`, `dict_thesaurus.c`).
//!
//! Two kinds of seam live here:
//!
//! * **Inward** — the dictionary-template fmgr methods this unit *owns*
//!   (`dsimple_init`/`dsimple_lexize`, `dsynonym_init`/`dsynonym_lexize`,
//!   `thesaurus_init`/`thesaurus_lexize`, and the `ts_lexize` SQL debug
//!   function). The owning unit installs these from its `init_seams()`;
//!   consumers reach them across the fmgr-dispatch boundary (the dictionary
//!   cache calls a template's registered C functions).
//!
//! * **Outward** — the genuinely-external fmgr-dispatch helpers `dict.c` and
//!   `dict_thesaurus.c` call: a `FunctionCall4(&dict->lexize, ...)` into a
//!   *sub-dictionary's* lexize method ([`subdict_lexize`]), and the thesaurus's
//!   `stringToQualifiedNameList(name) + get_ts_dict_oid(..., false)` name
//!   resolution ([`get_ts_dict_oid_from_name`]). These belong to the dictionary
//!   cache (`tsearch/ts_cache.c`) and fmgr dispatch, which have no idiomatic
//!   owner yet, so they cross a seam and panic loudly until installed.
//!
//! All other helpers (`defGetString`/`defGetBoolean`, `str_tolower`,
//! `readstoplist`/`searchstoplist`/`get_tsearch_config_filename`, the
//! `tsearch_readline` config reader, `pg_mblen`, and `construct_array_builtin`)
//! cross to their *already-seamed* owners directly from the unit crate.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use backend_commands_define_seams::DefElemArg;
use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_tsearch::{
    DictSimple, DictSyn, DictThesaurus, OwnedTSLexeme, TSLexeme, ThesaurusSubState,
};

// ---------------------------------------------------------------------------
// Inward: `simple` dictionary template (dict_simple.c)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `dsimple_init(dictoptions)` (dict_simple.c): parse the
    /// `stopwords`/`accept` options and build the `simple` dictionary. Each
    /// option is `(defname, def->arg)`. The built [`DictSimple`] is allocated in
    /// `mcx` (C palloc's it in the dictionary's long-lived cache context).
    /// Bad/duplicate options surface as `Err(ERRCODE_INVALID_PARAMETER_VALUE)`.
    pub fn dsimple_init<'mcx>(
        mcx: Mcx<'mcx>,
        dictoptions: &[(String, Option<DefElemArg>)],
    ) -> PgResult<DictSimple<'mcx>>
);

seam_core::seam!(
    /// `dsimple_lexize(d, in, len)` (dict_simple.c): lowercase, drop stop words,
    /// accept/reject. `None` mirrors C's `PG_RETURN_POINTER(NULL)` (unrecognized
    /// word with `accept=false`); `Some(vec![])` mirrors the empty
    /// `palloc0(2*TSLexeme)` (stop-word reject); `Some(vec)` is the accepted
    /// single lexeme. Allocated in `mcx`.
    pub fn dsimple_lexize<'mcx>(
        mcx: Mcx<'mcx>,
        d: &DictSimple<'_>,
        input: &[u8],
        len: i32,
    ) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>>
);

// ---------------------------------------------------------------------------
// Inward: `synonym` dictionary template (dict_synonym.c)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `dsynonym_init(dictoptions)` (dict_synonym.c): read the `synonyms` file,
    /// parse `casesensitive`, and build the sorted synonym table. Built
    /// [`DictSyn`] allocated in `mcx`.
    pub fn dsynonym_init<'mcx>(
        mcx: Mcx<'mcx>,
        dictoptions: &[(String, Option<DefElemArg>)],
    ) -> PgResult<DictSyn<'mcx>>
);

seam_core::seam!(
    /// `dsynonym_lexize(d, in, len)` (dict_synonym.c): bsearch the synonym table
    /// for the (optionally lowercased) input word. `None` for the
    /// `PG_RETURN_POINTER(NULL)` cases (empty input, empty table, no match);
    /// `Some(vec)` is the single substituted lexeme. Allocated in `mcx`.
    pub fn dsynonym_lexize<'mcx>(
        mcx: Mcx<'mcx>,
        d: &DictSyn<'_>,
        input: &[u8],
        len: i32,
    ) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>>
);

// ---------------------------------------------------------------------------
// Inward: `thesaurus` dictionary template (dict_thesaurus.c)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `thesaurus_init(dictoptions)` (dict_thesaurus.c): read the `.ths` rule
    /// file, resolve and compile through the sub-dictionary. Built
    /// [`DictThesaurus`] allocated in `mcx`.
    pub fn thesaurus_init<'mcx>(
        mcx: Mcx<'mcx>,
        dictoptions: &[(String, Option<DefElemArg>)],
    ) -> PgResult<DictThesaurus<'mcx>>
);

seam_core::seam!(
    /// `thesaurus_lexize(d, in, len, dstate)` (dict_thesaurus.c): match phrases
    /// and emit substitutions, threading the cross-call [`ThesaurusSubState`]
    /// (C's `DictSubState`). `None` for `PG_RETURN_POINTER(NULL)`. The dict may
    /// mutate its `LexemeInfo` arena's `nextvariant` links (C does), so it is
    /// taken `&mut`. Allocated lexemes in `mcx`.
    pub fn thesaurus_lexize<'mcx>(
        mcx: Mcx<'mcx>,
        d: &mut DictThesaurus<'mcx>,
        input: &[u8],
        len: i32,
        state: &mut ThesaurusSubState,
    ) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>>
);

// ---------------------------------------------------------------------------
// Inward: `ts_lexize(regdictionary, text)` SQL debug function (dict.c)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `ts_lexize(dictId, in)` (dict.c): the `text[]`-returning debug function.
    /// `dict_id` is `PG_GETARG_OID(0)`; `input` is `VARDATA_ANY` of arg 1.
    /// Returns `None` for `PG_RETURN_NULL()`, else the `construct_array_builtin`
    /// result (C's `ArrayType *` Datum), allocated in `mcx`.
    pub fn ts_lexize<'mcx>(
        mcx: Mcx<'mcx>,
        dict_id: Oid,
        input: &[u8],
    ) -> PgResult<Option<Datum>>
);

// ---------------------------------------------------------------------------
// Outward: fmgr-dispatch + name resolution (owners not yet ported)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `FunctionCall4(&dict->lexize, dictData, in, len, PointerGetDatum(NULL))`
    /// (dict.c / dict_thesaurus.c): dispatch a *cached* dictionary's `lexize`
    /// method by its OID, with `dstate == NULL` (single-shot lexize). Owner:
    /// `tsearch/ts_cache.c` + fmgr. Returns the owned [`OwnedTSLexeme`] array:
    /// `None` for C's `NULL`, `Some(vec![])` for the empty (stop-word) array,
    /// `Some(vec)` for a populated array.
    pub fn subdict_lexize(dict_id: Oid, input: Vec<u8>) -> PgResult<Option<Vec<OwnedTSLexeme>>>
);

seam_core::seam!(
    /// `stringToQualifiedNameList(name, NULL)` then `get_ts_dict_oid(namelist,
    /// false)` (dict_thesaurus.c): resolve a (possibly qualified) text-search
    /// dictionary name to its OID, raising `ERRCODE_UNDEFINED_OBJECT` on a miss.
    /// Owner: `catalog/namespace.c` + `utils/regproc.c`.
    pub fn get_ts_dict_oid_from_name(name: String) -> PgResult<Oid>
);
