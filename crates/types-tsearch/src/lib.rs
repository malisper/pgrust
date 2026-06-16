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
