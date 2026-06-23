//! Seam declarations for the `backend-tsearch-parse` unit
//! (`src/backend/tsearch/{wparser_def,ts_parse}.c`): the default word parser's
//! genuinely-external helpers.
//!
//! These are the helpers the two files reach which belong to subsystems not
//! (yet) ported as idiomatic crates and would otherwise create a dependency
//! cycle:
//!
//! * **`utils/mb/{pg_wchar,wchar,mbutils}.c`** — `pg_mblen_range` (leading-char
//!   byte length bounded by the buffer end), `pg_dsplen` (display width),
//!   `char2wchar` / `pg_mb2wchar_with_len` (the wide-char copies the parser
//!   indexes by char position), plus `database_encoding_max_length` /
//!   `get_database_encoding`.
//! * **`utils/adt/pg_locale.c`** — `database_ctype_is_c`, and the
//!   global-locale libc `is*` / `isw*` character-class predicates the parser's
//!   `p_iswhat` macro issues over a code point.
//! * **the text-search configuration / dictionary cache** — `config_lenmap` /
//!   `config_dict_ids` (the `cfg->lenmap` / `cfg->map[...]` lookups used by the
//!   lexize machine) and the fmgr `lexize` dispatch (`dict_lexize`).
//! * **the generic tsquery execution engine** (`utils/adt/tsvector_op.c`:
//!   `TS_execute` / `TS_execute_locations`) the headline selector invokes with
//!   the `checkcondition_HL` callback (`ts_execute_hl` /
//!   `ts_execute_locations_hl`).
//!
//! The owning unit installs every one of these from its `init_seams()`; until
//! then a call panics loudly. There is no silent fallback.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;
use ::types_error::PgResult;

// ---------------------------------------------------------------------------
// Driver-side dictionary-protocol / headline-tsquery types
//
// These are the owned-model types the parser threads across its `dict_lexize`,
// `ts_execute_hl`, and `ts_execute_locations_hl` seams. They are deliberately
// DIFFERENT in shape from the canonical `tsearch::{QueryItem, …}` on-disk
// representations (this is the driver's specialization to what the lexize /
// headline path actually carries across the seam boundary), so they live here
// under the owning unit's seam crate rather than overwriting the canonical
// items.
// ---------------------------------------------------------------------------

/// One normalized lexeme produced by a dictionary's `lexize`, mirroring the C
/// `TSLexeme`. `flags` carries `TSL_ADDPOS | TSL_PREFIX | TSL_FILTER`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LexizeLexeme {
    pub nvariant: u16,
    pub flags: u16,
    /// The lexeme text (the C `lexeme` C-string); an array of these is
    /// terminated in C by a NULL `lexeme`, which here is just the Vec length.
    pub lexeme: Vec<u8>,
}

/// The in/out `DictSubState` passed to a dictionary's `lexize`, as
/// `FunctionCall4(&dict->lexize, …, PointerGetDatum(&ld->dictState))` does in
/// `ts_parse.c`. A single instance is threaded across every `dict_lexize`
/// call of one lexize run.
///
/// `private_state` is the opaque C `void *private_state`, modelled as a
/// host-managed token (`0` == C `NULL`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DictSubState {
    /// in: this is the last lexeme of the parsed input (`isend`).
    pub isend: bool,
    /// out: dict wants next lexeme (`getnext`).
    pub getnext: bool,
    /// in/out: opaque per-dictionary parsing state (`void *private_state`);
    /// `0` is C `NULL`.
    pub private_state: u64,
}

/// `QueryOperand`: a tsquery operand (`ts_type.h`). Only the fields the
/// headline path reads are kept faithful; `length`/`distance` are the bitfield
/// `uint32 length:12, distance:20` split into two fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryOperand {
    /// `QI_VAL` for an operand.
    pub type_: i8,
    pub weight: u8,
    pub prefix: bool,
    pub valcrc: i32,
    /// `length:12` — length of the operand string in bytes.
    pub length: u32,
    /// `distance:20` — byte offset of the operand string within the query's
    /// operand area.
    pub distance: u32,
}

/// `QueryOperator` (`ts_type.h`): a tsquery operator node. The generic
/// `TS_execute` engine (run for the headline path through the
/// `ts_execute*_hl` seams) reads `oper` and `left` to recurse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryOperator {
    /// `QI_OPR` for an operator.
    pub type_: i8,
    /// `OP_NOT`/`OP_AND`/`OP_OR`/`OP_PHRASE`.
    pub oper: i8,
    /// distance between args for `OP_PHRASE`.
    pub distance: i16,
    /// offset to the left operand (the right operand is `item + 1`).
    pub left: u32,
}

/// `QueryItem`: one node in a tsquery — operator or operand
/// (`ts_type.h` union).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryItem {
    /// `QI_VAL` operand (`qoperand`).
    Operand(QueryOperand),
    /// `QI_OPR` operator (`qoperator`).
    Operator(QueryOperator),
}

/// `ExecPhraseData` (`ts_utils.h:159`): the per-operand/-phrase position list a
/// `ts_execute_locations_hl` result carries back to `hlCover`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExecPhraseData {
    /// `npos` — number of positions reported (`== pos.len()`).
    pub npos: i32,
    /// `pos` — ordered, non-duplicate lexeme positions.
    pub pos: Vec<i32>,
    /// `width` — width of the match in lexemes, less 1.
    pub width: i32,
}

// ---------------------------------------------------------------------------
// Multibyte-encoding subsystem (utils/mb/{pg_wchar,wchar,mbutils}.c)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `pg_database_encoding_max_length()` — max bytes per char in the database
    /// encoding. `> 1` selects the wide-char parse path.
    pub fn pg_database_encoding_max_length() -> i32
);

seam_core::seam!(
    /// `GetDatabaseEncoding()` — the database encoding id.
    pub fn get_database_encoding() -> i32
);

seam_core::seam!(
    /// `database_ctype_is_c()` — whether the database default collation's ctype
    /// is the C locale (selects the `pg_wchar` path over libc `char2wchar`).
    pub fn database_ctype_is_c() -> bool
);

seam_core::seam!(
    /// `pg_mblen_range(s, end)` — byte length of the first character of `s` in
    /// the database encoding, bounded by the buffer end. `s` is the remaining
    /// (non-empty) input slice. Raises (SQLSTATE `22021`) on a
    /// truncated/invalid multibyte sequence at end of buffer.
    pub fn pg_mblen_range(s: &[u8]) -> PgResult<i32>
);

seam_core::seam!(
    /// `pg_dsplen(s)` — display width of the first character of `s`. `0` for a
    /// zero-width mark, `-1` for control/error.
    pub fn pg_dsplen(s: &[u8]) -> i32
);

seam_core::seam!(
    /// `char2wchar(from, fromlen, …)` — convert the database-encoding string
    /// `from` to an array of `wchar_t` (libc-locale wide path), without the
    /// trailing NUL. May raise on a bad multibyte sequence for the locale.
    pub fn char2wchar(from: Vec<u8>) -> PgResult<Vec<u32>>
);

seam_core::seam!(
    /// `pg_mb2wchar_with_len(from, …)` — convert `from` to `pg_wchar` code
    /// points (the C-locale wide path).
    pub fn pg_mb2wchar_with_len(from: Vec<u8>) -> PgResult<Vec<u32>>
);

// ---------------------------------------------------------------------------
// Global-locale libc ctype (the wide / byte `p_iswhat` predicates)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `iswalnum(wc)` — wide alphanumeric test; C truth value.
    pub fn iswalnum(wc: u32) -> i32
);
seam_core::seam!(
    /// `iswalpha(wc)` — wide alphabetic test.
    pub fn iswalpha(wc: u32) -> i32
);
seam_core::seam!(
    /// `iswdigit(wc)` — wide decimal-digit test.
    pub fn iswdigit(wc: u32) -> i32
);
seam_core::seam!(
    /// `iswspace(wc)` — wide whitespace test.
    pub fn iswspace(wc: u32) -> i32
);
seam_core::seam!(
    /// `iswxdigit(wc)` — wide hex-digit test.
    pub fn iswxdigit(wc: u32) -> i32
);

seam_core::seam!(
    /// `isalnum(c)` — byte alphanumeric test; C truth value.
    pub fn isalnum(c: u32) -> i32
);
seam_core::seam!(
    /// `isalpha(c)` — byte alphabetic test.
    pub fn isalpha(c: u32) -> i32
);
seam_core::seam!(
    /// `isdigit(c)` — byte decimal-digit test.
    pub fn isdigit(c: u32) -> i32
);
seam_core::seam!(
    /// `isspace(c)` — byte whitespace test.
    pub fn isspace(c: u32) -> i32
);
seam_core::seam!(
    /// `isxdigit(c)` — byte hex-digit test.
    pub fn isxdigit(c: u32) -> i32
);

// ---------------------------------------------------------------------------
// Text-search configuration / dictionary cache
// (lookup_ts_config_cache: cfg->lenmap / cfg->map[token_type])
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `cfg->lenmap` — length of configuration `cfg_id`'s token-type ->
    /// dictionary map (`TSConfigCacheEntry.lenmap`).
    pub fn config_lenmap(cfg_id: u32) -> PgResult<i32>
);

seam_core::seam!(
    /// `cfg->map[token_type]` — the dictionary ids mapped to `token_type` for
    /// configuration `cfg_id`. An empty vec means `map->len == 0`.
    pub fn config_dict_ids(cfg_id: u32, token_type: i32) -> PgResult<Vec<u32>>
);

// ---------------------------------------------------------------------------
// Text-search dictionary fmgr lexize dispatch (FunctionCall4)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `FunctionCall4(&dict->lexize, dictData, lemm, lenlemm, &dictState)` —
    /// run dictionary `dict_id`'s `lexize` over `lemm`. The threaded
    /// `DictSubState` is passed by value and the (possibly mutated) copy
    /// returned alongside the result. Returns the normalized `TSLexeme` array,
    /// or `None` (C `NULL`: dict doesn't know the lexeme).
    pub fn dict_lexize(
        dict_id: u32,
        lemm: Vec<u8>,
        dstate: DictSubState,
    ) -> PgResult<(DictSubState, core::option::Option<Vec<LexizeLexeme>>)>
);

// ---------------------------------------------------------------------------
// Generic tsquery execution engine (utils/adt/tsvector_op.c)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `TS_execute(GETQUERY(query), &ch, flags, checkcondition_HL)` — does the
    /// query match the word range described by `match_table`? `items` is the
    /// query's `QueryItem` array; `match_table[i]` is the `(item, pos)` pair for
    /// `words[i]`; `flags` is the `TS_EXEC_*` bitmask.
    pub fn ts_execute_hl(
        items: Vec<QueryItem>,
        match_table: Vec<(core::option::Option<usize>, u16)>,
        flags: u32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `TS_execute_locations(GETQUERY(query), &ch, flags, checkcondition_HL)` —
    /// the per-AND'ed-term location lists used by `hlCover` to find covers.
    pub fn ts_execute_locations_hl(
        items: Vec<QueryItem>,
        match_table: Vec<(core::option::Option<usize>, u16)>,
        flags: u32,
    ) -> PgResult<Vec<ExecPhraseData>>
);
