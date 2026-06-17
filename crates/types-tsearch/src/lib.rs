//! Full-text-search type vocabulary.
//!
//! Two vocabularies share this crate:
//!
//! - The `tsvector_ops` index support functions (`tsginidx.c`,
//!   `tsgistidx.c`) and ranking functions (`tsrank.c`) — sources
//!   `src/include/tsearch/ts_type.h`, `src/include/tsearch/ts_utils.h`,
//!   `src/include/access/gin.h` (modules [`tsearch`], [`gin`], [`tsgistidx`]).
//! - The shared dictionary vocabulary (`tsearch/ts_public.h`): the dictionary
//!   lexize return type and stop-word list, trimmed to the fields the ported
//!   tsearch crates consume.
//!
//! The dictionary types carry no `repr(C)` layout — the C `char *` strings
//! become owned context-allocated [`PgString`]s and the C `NULL`-terminated
//! `TSLexeme[]` / `char **stop` arrays become [`PgVec`]s, so a `NULL` `lexeme`
//! entry (the C array terminator) is simply absent from the vector rather than
//! a sentinel element.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

extern crate alloc;

pub mod tsearch;
pub mod gin;
pub mod backend_access_gin_ginlogic;
pub mod tsgistidx;

use mcx::{PgString, PgVec};
use types_core::Oid;

/// `TSL_ADDPOS` (`ts_public.h`).
pub const TSL_ADDPOS: u16 = 0x01;
/// `TSL_PREFIX` (`ts_public.h`).
pub const TSL_PREFIX: u16 = 0x02;
/// `TSL_FILTER` (`ts_public.h`).
pub const TSL_FILTER: u16 = 0x04;

/// `TSLexeme` (`ts_public.h`): the return type of any dictionary lexize method.
///
/// The C struct's `char *lexeme` becomes an owned [`PgString`]; the C array's
/// `NULL`-lexeme terminator is represented by the end of the carrying
/// [`PgVec`], so every entry here is a real (non-`NULL`) lexeme.
#[derive(Debug)]
pub struct TSLexeme<'mcx> {
    /// `nvariant`: split-variant group of this lexeme (only changes between
    /// adjacent entries are significant).
    pub nvariant: u16,
    /// `flags`: `TSL_*` flag bits.
    pub flags: u16,
    /// `lexeme`: the C string (NUL dropped), owned in the lexize context.
    pub lexeme: PgString<'mcx>,
}

/// Opaque handle to an `IspellDict` build/normalize state owned by
/// `tsearch/dicts/spell.c` (not yet ported).
///
/// C embeds the full `IspellDict obj` inside `DictISpell`; that struct carries
/// large `repr`-laden build tables (`AffixData`, `Spell`, the `Conf` trie)
/// which belong to the spell unit. Until that unit lands, the build state
/// lives behind this token: `NIStartBuild` mints one, the import/sort/finish
/// steps and `NINormalizeWord` thread it, and the owner resolves it to its
/// real `IspellDict` storage. When `backend-tsearch-spell` lands it replaces
/// this token with the real owned `IspellDict` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpellHandle(pub u64);

/// `StopList` (`ts_public.h`): a sorted stop-word list.
///
/// C stores `int len` + `char **stop`; the owned model carries the words as a
/// [`PgVec`] of [`PgString`] (`stop.len()` is the C `len`). The words are kept
/// sorted (binary-search lookup), exactly as `readstoplist` leaves them.
#[derive(Debug)]
pub struct StopList<'mcx> {
    /// `stop`: the stop words, lowercased and sorted (the C `len` is its length).
    pub stop: PgVec<'mcx, PgString<'mcx>>,
}

/// `DictISpell` (`dict_ispell.c`): the ispell-dictionary state object the
/// `dispell_init` lexize template builds and `dispell_lexize` consumes.
///
/// C is `{ StopList stoplist; IspellDict obj; }` held behind the dictionary
/// cache's `void *dictData`. The `IspellDict obj` build state belongs to the
/// (unported) spell unit, so it lives behind the opaque [`SpellHandle`]; the
/// stop list is owned inline.
#[derive(Debug)]
pub struct DictISpell<'mcx> {
    /// C `stoplist`: the optional `StopWords` list (empty if none configured).
    pub stoplist: StopList<'mcx>,
    /// C `obj`: the built `IspellDict`, behind the spell unit's handle.
    pub obj: SpellHandle,
}

/// Opaque handle to a live Snowball stemmer environment (`struct SN_env *z`)
/// plus its `stem` method, owned by the snowball runtime
/// (`backend-snowball-runtime`, the libstemmer `api.c`/`utilities.c`
/// substrate).
///
/// C embeds a raw `struct SN_env *z` and a `int (*stem)(struct SN_env *)`
/// function pointer in `DictSnowball`. Those are raw addresses into the
/// runtime's hidden-header `symbol*` buffers (the runtime reads a
/// `[capacity, length]` header at negative offsets), and naming the runtime's
/// `*mut SN_env` here would invert the type/backend layering. So — exactly as
/// [`DictISpell`] holds its `IspellDict obj` behind [`SpellHandle`] — the live
/// environment + stem fn live behind this token; the dict unit
/// (`backend-snowball-dict-snowball`) resolves it to the real `*mut SN_env` +
/// stem fn it minted from the runtime's `STEMMER_MODULES`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnowballEnvHandle(pub u64);

/// `DictSnowball` (`dict_snowball.c`): the Snowball-dictionary state object the
/// `dsnowball_init` template builds and `dsnowball_lexize` consumes.
///
/// C is `{ struct SN_env *z; StopList stoplist; bool needrecode;
/// int (*stem)(struct SN_env *); MemoryContext dictCtx; }` held behind the
/// dictionary cache's `void *dictData`. The live `z`/`stem` belong to the
/// snowball runtime, so they live behind the opaque [`SnowballEnvHandle`]; the
/// stop list is owned inline.
#[derive(Debug)]
pub struct DictSnowball<'mcx> {
    /// C `z` + `stem`: the live stemmer environment and its stem method,
    /// behind the runtime's handle (`InvalidOid`-like `None` until a Language
    /// option is parsed).
    pub z: Option<SnowballEnvHandle>,
    /// C `stoplist`: the optional `StopWords` list (empty if none configured).
    pub stoplist: StopList<'mcx>,
    /// C `needrecode`: recode to/from UTF-8 around the stemmer call when the
    /// matched UTF-8 stemmer's encoding differs from the server encoding.
    pub needrecode: bool,
}

/// `#define DT_USEASIS 0x1000` (dict_thesaurus.c): a temporary reuse of
/// `TSLexeme.flags` during thesaurus compilation.
pub const DT_USEASIS: u16 = 0x1000;

/// `DictSimple` (`dict_simple.c`): the `simple` dictionary state object the
/// `dsimple_init` template builds and `dsimple_lexize` consumes.
///
/// C is `{ StopList stoplist; bool accept; }` held behind the dictionary
/// cache's `void *dictData`.
#[derive(Debug)]
pub struct DictSimple<'mcx> {
    /// C `stoplist`: the optional `StopWords` list (empty if none configured).
    pub stoplist: StopList<'mcx>,
    /// C `accept`: whether to accept (emit) recognized non-stop words
    /// (defaults to `true`).
    pub accept: bool,
}

/// `Syn` (`dict_synonym.c`): one synonym mapping `in -> out`.
#[derive(Debug)]
pub struct Syn<'mcx> {
    /// C `in`: the input word (lowercased unless `case_sensitive`).
    pub r#in: PgString<'mcx>,
    /// C `out`: the substituted word (lowercased unless `case_sensitive`).
    pub out: PgString<'mcx>,
    /// C `outlen`: byte length of `out` (kept to mirror C; `out.len()`).
    pub outlen: i32,
    /// C `flags`: `TSL_PREFIX` if the input ended with `*`, else 0.
    pub flags: u16,
}

/// `DictSyn` (`dict_synonym.c`): the `synonym` dictionary state object the
/// `dsynonym_init` template builds and `dsynonym_lexize` consumes.
///
/// C is `{ int len; Syn *syn; bool case_sensitive; }`; `syn.len()` is the
/// C `len`. The array is kept sorted by `in` (`compareSyn`).
#[derive(Debug)]
pub struct DictSyn<'mcx> {
    /// C `syn`/`len`: the sorted synonym mappings.
    pub syn: PgVec<'mcx, Syn<'mcx>>,
    /// C `case_sensitive`.
    pub case_sensitive: bool,
}

/// `LexemeInfo` (`dict_thesaurus.c`): an entry's number/position in a
/// substitution, plus the chain links.
///
/// C wires `nextentry` / `nextvariant` as raw `LexemeInfo *`; the owned model
/// replaces each pointer with an `Option<usize>` index into the carrying
/// [`DictThesaurus::arena`] (`None` = `NULL`).
#[derive(Clone, Copy, Debug, Default)]
pub struct LexemeInfo {
    /// C `idsubst`: the entry's number in `DictThesaurus.subst`.
    pub idsubst: u32,
    /// C `posinsubst`: position info within the entry.
    pub posinsubst: u16,
    /// C `tnvariant`: total number of lexemes in one variant.
    pub tnvariant: u16,
    /// C `nextentry` (`LexemeInfo *`): arena index of the next entry, or `None`.
    pub nextentry: Option<usize>,
    /// C `nextvariant` (`LexemeInfo *`): arena index of the next variant, or
    /// `None`.
    pub nextvariant: Option<usize>,
}

/// `TheLexeme` (`dict_thesaurus.c`): a lexeme string plus its [`LexemeInfo`]
/// chain (an arena index). C `char *lexeme` / `LexemeInfo *entries`.
#[derive(Debug)]
pub struct TheLexeme<'mcx> {
    /// C `lexeme`: the word, or `None` for the stop-word marker (`NULL`).
    pub lexeme: Option<PgString<'mcx>>,
    /// C `entries`: arena index of the `LexemeInfo` chain head.
    pub entries: Option<usize>,
}

/// `TheSubstitute` (`dict_thesaurus.c`): a prepared substitution result.
#[derive(Debug)]
pub struct TheSubstitute<'mcx> {
    /// C `lastlexeme`: the number of lexemes to substitute (minus one).
    pub lastlexeme: u16,
    /// C `reslen`: length of the substituted result.
    pub reslen: u16,
    /// C `res`: the prepared substituted result (the `NULL`-lexeme terminator
    /// slot is dropped).
    pub res: PgVec<'mcx, TSLexeme<'mcx>>,
}

/// `DictThesaurus` (`dict_thesaurus.c`): the `thesaurus` dictionary state object
/// the `thesaurus_init` template builds and `thesaurus_lexize` consumes.
///
/// C wires the `LexemeInfo` chains with raw pointers; the owned model holds
/// every node in [`arena`](Self::arena) and links them by `Option<usize>`
/// index, reproducing the `findVariant` / `matchIdSubst` / `checkMatch`
/// pointer-chasing 1:1.
#[derive(Debug)]
pub struct DictThesaurus<'mcx> {
    /// C `subdictOid`: the sub-dictionary used to normalize lexemes.
    pub subdict_oid: Oid,

    /// C `wrds`/`nwrds`/`ntwrds`: the lexeme array searched by exact match.
    pub wrds: PgVec<'mcx, TheLexeme<'mcx>>,

    /// C `subst`/`nsubst`: the per-expression substituted results.
    pub subst: PgVec<'mcx, TheSubstitute<'mcx>>,
    /// C `nsubst`: the number of substitution expressions (`subst.len()` after
    /// compilation, but tracked through the build for the C bound checks).
    pub nsubst: i32,

    /// Arena backing every `LexemeInfo` node referenced by `wrds[].entries`
    /// (the C `palloc`'d `LexemeInfo` nodes wired by `nextentry`/`nextvariant`).
    pub arena: PgVec<'mcx, LexemeInfo>,
}

/// Cross-call state for `thesaurus_lexize`, replacing C's
/// `DictSubState.private_state` (a raw `LexemeInfo *`) and its `isend`/`getnext`
/// flags. `stored` is the arena index of the variant-chain head carried between
/// consecutive `getnext` calls.
#[derive(Clone, Copy, Debug, Default)]
pub struct ThesaurusSubState {
    /// in: text end is reached (`DictSubState.isend`).
    pub isend: bool,
    /// out: the dictionary wants the next lexeme (`DictSubState.getnext`).
    pub getnext: bool,
    /// internal: the `LexemeInfo *` head carried between `getnext` calls.
    pub stored: Option<usize>,
}

/// Owned `TSLexeme` carrier crossing the sub-dictionary `lexize` fmgr-dispatch
/// seam (a `FunctionCall4(&dict->lexize, ...)`): C's `palloc`'d,
/// `NUL`-`lexeme`-terminated `TSLexeme *` array.
///
/// The result is `None` for C's `NULL` (word not recognized), `Some(vec![])`
/// for C's empty `palloc0(2*sizeof(TSLexeme))` (a non-null array whose first
/// `lexeme` is `NULL` — the stop-word case), and `Some(vec)` for a populated
/// array (the `NUL`-terminator slot dropped). Carried owned (not `'mcx`)
/// because the thesaurus stashes these into its `LexemeInfo` arena.
#[derive(Clone, Debug)]
pub struct OwnedTSLexeme {
    /// C `nvariant`.
    pub nvariant: u16,
    /// C `flags`.
    pub flags: u16,
    /// C `lexeme` (NUL dropped); every entry here is a real (non-`NULL`)
    /// lexeme.
    pub lexeme: alloc::string::String,
}
