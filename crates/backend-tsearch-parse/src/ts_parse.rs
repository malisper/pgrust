//! Idiomatic port of `src/backend/tsearch/ts_parse.c` (PostgreSQL 18.3).
//!
//! Drives a text-search configuration: it runs the configured parser over the
//! input, then feeds each token through the configuration's dictionaries (the
//! lexize state machine, which handles multi-token phrases via the
//! dictionaries' `getnext`/`isend` protocol).  Exposes [`parsetext`] (build a
//! [`ParsedText`]) and the headline framework ([`hlparsetext`],
//! [`generateHeadline`], `mark_fragment` in [`crate::wparser_def`]).
//!
//! # Idiomatic owned model (vs the faithful C-ABI port)
//!
//! The faithful port (`src/crates/backend-tsearch-parse`) reached the
//! configuration / dictionary cache and fmgr `lexize` dispatch through a
//! `BackendTsearchConfigRuntime` trait, and carried the lexize work lists as an
//! index arena over plain `Vec`s.  This idiomatic port:
//!
//!   * routes those genuinely-external lookups through the crate-local
//!     [`crate::seam`] module (`config_lenmap` / `config_dict_ids` /
//!     `dict_lexize`);
//!   * defines the dictionary-protocol structs ([`DictSubState`],
//!     [`LexizeLexeme`]) in-crate (the faithful port imported them from
//!     `pgrust-pg-traits`, which has no idiomatic analog);
//!   * owns the produced word lists and the lexize arena in context-charged
//!     [`PgVec`]s (NOT faithful `palloc`/`repalloc`); [`ParsedText::free`] /
//!     [`HeadlineParsedText::free`] and the per-call [`LexizeData`] cleanup
//!     release every charge so the contexts' `used()` return to `0`.
//!
//! Everything else — the `LexizeExec` machine, the `parsetext` / `hlparsetext`
//! drivers, and `generateHeadline` — is ported 1:1 from the C.

use alloc::format;
use alloc::vec;
use alloc::vec::Vec;

use backend_utils_error::ereport;
use types_error::{
    ErrorLocation, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, NOTICE,
};

use crate::seam;
use crate::wparser_def::{self, TParser};

/* Constants from tsearch/ts_type.h and ts_public.h. */
const MAXSTRLEN: usize = (1 << 11) - 1;
const MAXENTRYPOS: i32 = 1 << 14;
const TSL_ADDPOS: u16 = 0x01;
const TSL_PREFIX: u16 = 0x02;
const TSL_FILTER: u16 = 0x04;

#[inline]
fn limitpos(x: i32) -> u16 {
    (if x >= MAXENTRYPOS { MAXENTRYPOS - 1 } else { x }) as u16
}

/* ----------------------------------------------------------------------
 * Dictionary-protocol structs (the C `TSLexeme` / `DictSubState`, formerly
 * imported from `pgrust-pg-traits` in the faithful port).
 *
 * Hoisted to the central `types` crate under this crate's C-path module
 * (`types::backend_tsearch_parse`) so the `dict_lexize` seam could be folded
 * into the central `seams` crate (task #33).  Re-exported here verbatim so
 * `crate::ts_parse::{LexizeLexeme, DictSubState}` references still resolve.
 * They are namespaced rather than merged into `types::tsearch` because the
 * canonical `ts_public.h` shapes there differ (the on-disk `TSLexeme` /
 * `DictSubState` with an `Option<Box<DictPrivateState>>` private state).
 * -------------------------------------------------------------------- */

pub use backend_tsearch_parse_seams::{DictSubState, LexizeLexeme};

/* ---- ParsedText ---- */

/// `ParsedWord`: a normalized word produced by [`parsetext`].
///
/// In C `pos` is a union of an inline `uint16 pos` and a `uint16 *apos`
/// array; `parsetext` only ever uses the inline `pos` and leaves `alen == 0`,
/// so the port keeps the inline position.  The `word` bytes are charged to the
/// owning [`ParsedText`]'s context.
#[derive(Default)]
pub struct ParsedWord {
    pub flags: u16,
    pub len: u16,
    pub nvariant: u16,
    pub alen: u16,
    pub pos: u16,
    pub word: Vec<u8>,
}

/// `ParsedText`: the output of [`parsetext`].
///
/// The C struct `palloc`s a `ParsedWord *words` array seeded at `lenwords`
/// slots and `repalloc`s (doubling) it as it fills.  Here `words` is a
/// context-charged [`PgVec`]; `lenwords` still tracks the C field for parity.
pub struct ParsedText {
    pub words: Vec<ParsedWord>,
    pub lenwords: i32,
    pub curwords: i32,
    pub pos: i32,
}

impl Default for ParsedText {
    fn default() -> Self {
        ParsedText {
            words: Vec::new(),
            lenwords: 0,
            curwords: 0,
            pos: 0,
        }
    }
}

impl ParsedText {
    /// Create an empty result with the C `lenwords` seed (the initial `words`
    /// array capacity callers in `to_tsany.c` pass: 2/4/16/estimate).
    pub fn with_lenwords(lenwords: i32) -> Self {
        ParsedText {
            lenwords,
            ..Default::default()
        }
    }

    /// Release the word list's charge (the C `pfree(prs->words)` + each word's
    /// `pfree`), so the internal context's `used()` returns to `0`.
    pub fn free(&mut self) {
        self.words = Vec::new();
        self.curwords = 0;
    }

    /// Bytes currently held by the word list (test/leak-gate hook).
    #[doc(hidden)]
    pub fn charged_bytes(&self) -> usize {
        self.words.iter().map(|w| w.word.len()).sum()
    }
}

/* ---- Lexize subsystem ---- */

/// `ParsedLex`: a raw lexeme token in the lexize work list.  The C struct is
/// a node in a singly linked list (`next`); here nodes live in an arena and
/// are referenced by index, preserving the C identity semantics on which the
/// machine relies (`curSub`/`lastRes` point at specific nodes).
#[derive(Clone, Debug)]
struct ParsedLex {
    type_: i32,
    lemm: Vec<u8>,
    next: Option<usize>,
}

/// `ListParsedLex`: a singly linked list of [`ParsedLex`] with a tail pointer.
#[derive(Clone, Copy, Debug, Default)]
struct ListParsedLex {
    head: Option<usize>,
    tail: Option<usize>,
}

/// `LexizeData`: state for the lexize machine in [`lexize_exec`].
struct LexizeData {
    cfg_id: u32,
    cur_dict_id: u32, // InvalidOid == 0
    pos_dict: i32,
    /// The C `DictSubState dictState`: a single struct threaded across every
    /// `dict_lexize` call so `private_state` survives consecutive multiword
    /// `getnext` calls (and `isend`/`getnext` are set per call).
    dict_state: DictSubState,
    cur_sub: Option<usize>,
    towork: ListParsedLex,
    waste: ListParsedLex,
    last_res: Option<usize>,
    tmp_res: Option<Vec<LexizeLexeme>>,
    /// Arena holding all `ParsedLex` nodes for this run.
    arena: Vec<ParsedLex>,
}

const INVALID_OID: u32 = 0;

impl LexizeData {
    fn init(cfg_id: u32) -> Self {
        LexizeData {
            cfg_id,
            cur_dict_id: INVALID_OID,
            pos_dict: 0,
            dict_state: DictSubState::default(),
            cur_sub: None,
            towork: ListParsedLex::default(),
            waste: ListParsedLex::default(),
            last_res: None,
            tmp_res: None,
            arena: Vec::new(),
        }
    }

    /// `LPLAddTail`.
    fn lpl_add_tail(&mut self, list: &mut ListParsedLex, newpl: usize) {
        if let Some(tail) = list.tail {
            self.arena[tail].next = Some(newpl);
            list.tail = Some(newpl);
        } else {
            list.head = Some(newpl);
            list.tail = Some(newpl);
        }
        self.arena[newpl].next = None;
    }

    /// `LPLRemoveHead`.
    fn lpl_remove_head(&mut self, list: &mut ListParsedLex) -> Option<usize> {
        let res = list.head;
        if let Some(h) = list.head {
            list.head = self.arena[h].next;
        }
        if list.head.is_none() {
            list.tail = None;
        }
        res
    }

    /// `LexizeAddLemm`.
    fn lexize_add_lemm(&mut self, type_: i32, lemm: Vec<u8>) {
        let newpl = self.arena.len();
        self.arena.push(ParsedLex {
            type_,
            lemm,
            next: None,
        });
        let mut towork = self.towork;
        self.lpl_add_tail(&mut towork, newpl);
        self.towork = towork;
        self.cur_sub = self.towork.tail;
    }

    /// `RemoveHead`: move towork head to waste tail.
    fn remove_head(&mut self) {
        let mut towork = self.towork;
        let h = self.lpl_remove_head(&mut towork);
        self.towork = towork;
        if let Some(h) = h {
            let mut waste = self.waste;
            self.lpl_add_tail(&mut waste, h);
            self.waste = waste;
        }
        self.pos_dict = 0;
    }

    /// `setCorrLex`: return the waste list head when `want_corr`, then reset
    /// the waste list.  (Pruning the freed nodes is implicit in the arena.)
    fn set_corr_lex(&mut self, want_corr: bool) -> Option<usize> {
        let res = if want_corr { self.waste.head } else { None };
        self.waste.head = None;
        self.waste.tail = None;
        res
    }

    /// `moveToWaste`: drain towork into waste up to and including `stop`,
    /// setting `cur_sub` to `stop->next`.
    fn move_to_waste(&mut self, stop: usize) {
        let mut go = true;
        while self.towork.head.is_some() && go {
            if self.towork.head == Some(stop) {
                self.cur_sub = self.arena[stop].next;
                go = false;
            }
            self.remove_head();
        }
    }

    /// `setNewTmpRes`.
    fn set_new_tmp_res(&mut self, lex: usize, res: Vec<LexizeLexeme>) {
        self.tmp_res = Some(res);
        self.last_res = Some(lex);
    }
}

/// `LexizeExec`: run the lexize machine, returning the next normalized lexeme
/// array (or `None` when exhausted).  `corr` (when requested) receives the
/// arena index of the head of the waste list (the corresponding raw lexemes),
/// matching the C `correspondLexem` out-parameter.
fn lexize_exec(
    ld: &mut LexizeData,
    want_corr: bool,
    corr_out: &mut Option<usize>,
) -> PgResult<Option<Vec<LexizeLexeme>>> {
    if ld.cur_dict_id == INVALID_OID {
        // usual mode: dictionary wants only one word, but we should keep going
        // through the whole stack.
        while let Some(cur_val) = ld.towork.head {
            let cur_type = ld.arena[cur_val].type_;
            let mut cur_val_lemm = ld.arena[cur_val].lemm.clone();

            let lenmap = seam::config_lenmap::call(ld.cfg_id)?;
            let dict_ids = if cur_type >= 0 && cur_type < lenmap {
                seam::config_dict_ids::call(ld.cfg_id, cur_type)?
            } else {
                Vec::new()
            };

            if cur_type == 0 || cur_type >= lenmap || dict_ids.is_empty() {
                // skip this type of lexeme
                ld.remove_head();
                continue;
            }

            let mut i = ld.pos_dict as usize;
            let mut produced: Option<Vec<LexizeLexeme>> = None;
            while i < dict_ids.len() {
                let dict_id = dict_ids[i];

                // C: ld->dictState.isend = ld->dictState.getnext = false;
                //    ld->dictState.private_state = NULL;
                ld.dict_state.isend = false;
                ld.dict_state.getnext = false;
                ld.dict_state.private_state = 0;
                let (dstate, lexemes) =
                    seam::dict_lexize::call(dict_id, cur_val_lemm.clone(), ld.dict_state)?;
                ld.dict_state = dstate;

                if ld.dict_state.getnext {
                    // dictionary wants next word: store position, go to
                    // multiword mode.
                    ld.cur_dict_id = dict_id;
                    ld.pos_dict = i as i32 + 1;
                    ld.cur_sub = ld.arena[cur_val].next;
                    if let Some(res) = lexemes {
                        ld.set_new_tmp_res(cur_val, res);
                    }
                    return lexize_exec(ld, want_corr, corr_out);
                }

                let res = match lexemes {
                    None => {
                        // dictionary doesn't know this lexeme
                        i += 1;
                        continue;
                    }
                    Some(r) => r,
                };

                if !res.is_empty() && res[0].flags & TSL_FILTER != 0 {
                    cur_val_lemm = res[0].lexeme.clone();
                    i += 1;
                    continue;
                }

                produced = Some(res);
                break;
            }

            if let Some(res) = produced {
                ld.remove_head();
                *corr_out = ld.set_corr_lex(want_corr);
                return Ok(Some(res));
            }

            ld.remove_head();
        }
    } else {
        // curDictId is valid: dictionary asks about following words.
        let dict_id = ld.cur_dict_id;

        while let Some(cur_val) = ld.cur_sub {
            let cur_type = ld.arena[cur_val].type_;
            let lenmap = seam::config_lenmap::call(ld.cfg_id)?;

            if cur_type != 0 {
                let dict_ids = if cur_type < lenmap {
                    seam::config_dict_ids::call(ld.cfg_id, cur_type)?
                } else {
                    Vec::new()
                };

                if cur_type >= lenmap || dict_ids.is_empty() {
                    // skip this type of lexeme
                    ld.cur_sub = ld.arena[cur_val].next;
                    continue;
                }

                // current dictionary must recognize this lexeme type.
                let dict_exists = dict_ids.contains(&ld.cur_dict_id);
                if !dict_exists {
                    // dictionary can't work with current type: back to basic
                    // mode and redo all stored lexemes.
                    ld.cur_dict_id = INVALID_OID;
                    return lexize_exec(ld, want_corr, corr_out);
                }
            }

            // C: ld->dictState.isend = (curVal->type == 0);
            //    ld->dictState.getnext = false;
            // private_state is *not* reset here: it carries the multiword
            // dictionary's parsing state across consecutive getnext calls.
            ld.dict_state.isend = cur_type == 0;
            ld.dict_state.getnext = false;
            let lemm = ld.arena[cur_val].lemm.clone();
            let (dstate, lexemes) = seam::dict_lexize::call(dict_id, lemm, ld.dict_state)?;
            ld.dict_state = dstate;

            if ld.dict_state.getnext {
                // dictionary wants one more
                ld.cur_sub = ld.arena[cur_val].next;
                if let Some(res) = lexemes {
                    ld.set_new_tmp_res(cur_val, res);
                }
                continue;
            }

            if lexemes.is_some() || ld.tmp_res.is_some() {
                // dictionary normalizes lexemes: remove used lexemes, return
                // to basic mode and redo end of stack.
                let res = if let Some(res) = lexemes {
                    let stop = ld.cur_sub.ok_or_else(|| {
                        backend_utils_error::PgError::error("lexize_exec: cur_sub present")
                    })?;
                    ld.move_to_waste(stop);
                    res
                } else {
                    let res = ld.tmp_res.take().ok_or_else(|| {
                        backend_utils_error::PgError::error("lexize_exec: tmp_res present")
                    })?;
                    let stop = ld.last_res.ok_or_else(|| {
                        backend_utils_error::PgError::error("lexize_exec: last_res present")
                    })?;
                    ld.move_to_waste(stop);
                    res
                };

                // reset to initial state
                ld.cur_dict_id = INVALID_OID;
                ld.pos_dict = 0;
                ld.last_res = None;
                ld.tmp_res = None;
                *corr_out = ld.set_corr_lex(want_corr);
                return Ok(Some(res));
            }

            // dict doesn't want next lexeme and didn't recognize anything:
            // redo from towork.head.
            ld.cur_dict_id = INVALID_OID;
            return lexize_exec(ld, want_corr, corr_out);
        }
    }

    *corr_out = ld.set_corr_lex(want_corr);
    Ok(None)
}

/* ---- public driver ---- */

/// `parsetext`: parse `buf` under configuration `cfg_id`, lexize the tokens
/// through the configuration's dictionaries, and fill `prs`.
///
/// The default-parser encoding/locale helpers and the configuration /
/// dictionary cache + fmgr lexize dispatch are reached through [`crate::seam`].
pub fn parsetext(cfg_id: u32, prs: &mut ParsedText, buf: &[u8]) -> PgResult<()> {
    let mut ldata = LexizeData::init(cfg_id);

    // lookup_ts_config_cache / lookup_ts_parser_cache + prsstart: the default
    // parser is the in-crate wparser_def machine.
    let mut prsdata: TParser = wparser_def::prsd_start(buf.to_vec(), buf.len())?;

    // C do { ... } while (type > 0): faithful equivalent below, with the
    // TParser freed on every exit path (incl. the error returns).
    let result = parsetext_loop(&mut ldata, prs, &mut prsdata);

    // prsend: release the TParser's charged buffers.
    wparser_def::prsd_end(prsdata);

    result
}

fn parsetext_loop(
    ldata: &mut LexizeData,
    prs: &mut ParsedText,
    prsdata: &mut TParser,
) -> PgResult<()> {
    loop {
        // prstoken
        let (type_, token) = wparser_def::prsd_nexttoken(prsdata)?;
        let token = token.to_vec();
        let lenlemm = token.len();

        // The long-lexeme guard uses the IGNORE_LONGLEXEME path: emit a NOTICE
        // and `continue` to the loop-condition test, without lexizing.
        if type_ > 0 && lenlemm >= MAXSTRLEN {
            // ereport(NOTICE, errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            //   errmsg("word is too long to be indexed"),
            //   errdetail("Words longer than %d characters are ignored.",
            //             MAXSTRLEN));
            let _ = ereport(NOTICE)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg("word is too long to be indexed")
                .errdetail(format!(
                    "Words longer than {MAXSTRLEN} characters are ignored."
                ))
                .finish(ErrorLocation::new(
                    "src/backend/tsearch/ts_parse.c",
                    0,
                    "parsetext",
                ));
            // C `continue`: re-test the do-while condition (`type > 0`), which
            // is always true here, so loop again.
            continue;
        }

        ldata.lexize_add_lemm(type_, token);

        let mut corr = None;
        while let Some(norms) = lexize_exec(ldata, false, &mut corr)? {
            prs.pos += 1; // set pos

            for ptr in &norms {
                if prs.curwords == prs.lenwords {
                    // C: prs->lenwords *= 2; repalloc(prs->words, ...).
                    // The PgVec grows automatically; lenwords still tracks the C
                    // field.  C callers always seed lenwords > 0 (to_tsany.c
                    // uses 2/4/16/estimate), so `*= 2` matches C for every
                    // reachable state; the `0 -> 32` fallback only guards the
                    // Default(0) case C never produces, keeping the field from
                    // sticking at 0.
                    prs.lenwords = if prs.lenwords == 0 {
                        32
                    } else {
                        prs.lenwords * 2
                    };
                }
                if ptr.flags & TSL_ADDPOS != 0 {
                    prs.pos += 1;
                }
                let word = ptr.lexeme.clone();
                prs.words
                    .push(
                        ParsedWord {
                            flags: ptr.flags & TSL_PREFIX,
                            len: ptr.lexeme.len() as u16,
                            nvariant: ptr.nvariant,
                            alen: 0,
                            pos: limitpos(prs.pos),
                            word,
                        },
                    );
                prs.curwords += 1;
            }
        }

        // do-while condition.
        if type_ <= 0 {
            break;
        }
    }
    Ok(())
}

/* ======================================================================
 * Headline framework (ts_parse.c "Headline framework", lines 434-679)
 * ====================================================================== */

/* Constants from tsearch/ts_type.h. */
const QI_VAL: i8 = 1;
/// `VARHDRSZ`: a varlena's 4-byte length header.
const VARHDRSZ: usize = 4;

// `QueryOperand` / `QueryItem` / `ExecPhraseData` were hoisted to the central
// `types` crate under this crate's C-path module (`types::backend_tsearch_parse`)
// so the `ts_execute_hl` / `ts_execute_locations_hl` seams could be folded into
// the central `seams` crate (task #33).  Re-exported here verbatim so
// `crate::ts_parse::{QueryOperand, QueryItem, ExecPhraseData}` references still
// resolve.  They are namespaced rather than merged into `types::tsearch`
// because the canonical `ts_type.h` / `ts_utils.h` shapes there differ (the
// packed `len_dist` `QueryOperand`, the three-arm `QueryItem` union, and the
// `WordEntryPos`-positioned `ExecPhraseData`).
pub use backend_tsearch_parse_seams::{ExecPhraseData, QueryItem, QueryOperand, QueryOperator};

/// `TS_EXEC_EMPTY` (`ts_utils.h`): the default `TS_execute` flag the headline
/// path passes (no special empty/phrase handling).
pub const TS_EXEC_EMPTY: u32 = 0;

/// `TSQuery`: the in-memory tsquery the headline matcher walks
/// (`TSQueryData`).  `items` is `GETQUERY(query)` (the `QueryItem` array of
/// length `size`); `operands` is `GETOPERAND(query)` (the `'\0'`-terminated
/// operand strings, referenced by `QueryOperand.distance`/`length`).  Building
/// a `TSQuery` from text is external (the tsquery parser); the headline path
/// only reads it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TSQuery {
    pub size: i32,
    pub items: Vec<QueryItem>,
    pub operands: Vec<u8>,
}

/// `HeadlineWordEntry`: one token in the headline word list (`ts_public.h`).
/// The C struct packs `selected/in/replace/repeated/skip/unused/type/len` into
/// one `uint32` bitfield; here they are individual fields with the same widths.
/// `item` is the C `QueryOperand *item` (a matching query operand, or none);
/// the port models the pointer identity as the matching item's index in the
/// owning [`TSQuery::items`].
///
/// The `word` bytes are charged to the owning [`HeadlineParsedText`]'s context.
#[derive(Default)]
pub struct HeadlineWordEntry {
    /// `selected:1` — token is to be highlighted.
    pub selected: bool,
    /// `in:1` — token is part of headline (C field name `in`).
    pub in_: bool,
    /// `replace:1` — token is to be replaced with a space.
    pub replace: bool,
    /// `repeated:1` — duplicate entry to hold an item pointer.
    pub repeated: bool,
    /// `skip:1` — token is to be skipped (not output).
    pub skip: bool,
    /// `type:8` — parser's token category.
    pub type_: u8,
    /// `len:16` — length of token.
    pub len: u16,
    /// `pos` — `WordEntryPos` position of token.
    pub pos: u16,
    /// `word` — text of token (not null-terminated).
    pub word: Vec<u8>,
    /// `item` — index into the query's items of a matching `QI_VAL` operand,
    /// or `None`.  (C holds a `QueryOperand *`; the index preserves identity.)
    pub item: Option<usize>,
}

/// `HeadlineParsedText`: the text to be highlighted (`ts_public.h`).
///
/// The C struct `palloc`s `words` (seeded at `lenwords`), the selection
/// markup strings, and per-word `word` copies.  Here the word list lives in a
/// context-charged [`PgVec`]; the selection / fragment strings are plain owned
/// `Vec`s (set by the caller from GUC bytes, not charged to this context).
pub struct HeadlineParsedText {
    pub words: Vec<HeadlineWordEntry>,
    pub lenwords: i32,
    pub curwords: i32,
    pub vectorpos: i32,
    pub startsel: Vec<u8>,
    pub stopsel: Vec<u8>,
    pub fragdelim: Vec<u8>,
    pub startsellen: i16,
    pub stopsellen: i16,
    pub fragdelimlen: i16,
}

impl Default for HeadlineParsedText {
    fn default() -> Self {
        HeadlineParsedText {
            words: Vec::new(),
            lenwords: 0,
            curwords: 0,
            vectorpos: 0,
            startsel: Vec::new(),
            stopsel: Vec::new(),
            fragdelim: Vec::new(),
            startsellen: 0,
            stopsellen: 0,
            fragdelimlen: 0,
        }
    }
}

impl HeadlineParsedText {
    /// Create an empty headline result with the C `lenwords` seed.
    pub fn with_lenwords(lenwords: i32) -> Self {
        HeadlineParsedText {
            lenwords,
            ..Default::default()
        }
    }

    /// Release the word list's charge so the internal context's `used()`
    /// returns to `0`.
    pub fn free(&mut self) {
        self.words = Vec::new();
        self.curwords = 0;
    }

    /// Bytes currently held by the word list (test/leak-gate hook).
    #[doc(hidden)]
    pub fn charged_bytes(&self) -> usize {
        self.words.iter().map(|w| w.word.len()).sum()
    }
}

/// Test-only: push a fully-flagged word charged to the owning context, so the
/// headline-assembly and `mark_fragment` tests can build inputs without going
/// through the parse driver.  Mirrors C tests constructing `prs->words[]`
/// directly.
#[cfg(test)]
pub(crate) fn test_push_word(
    prs: &mut HeadlineParsedText,
    word: &[u8],
    in_: bool,
    selected: bool,
    replace: bool,
    skip: bool,
) {
    let w = word.to_vec();
    prs.words.push(HeadlineWordEntry {
        in_,
        selected,
        replace,
        skip,
        len: word.len() as u16,
        word: w,
        ..HeadlineWordEntry::default()
    });
}

/// Test-only: push a word carrying just a token type and optional matching
/// query item (used by the `mark_fragment` range test).
#[cfg(test)]
pub(crate) fn test_push_word_typed(prs: &mut HeadlineParsedText, type_: u8, item: Option<usize>) {
    prs.words.push(HeadlineWordEntry {
        type_,
        item,
        ..HeadlineWordEntry::default()
    });
}

/// Test-only: push a fully-described headline word (token type, length,
/// matching query-item index, repeated flag, lexeme position) and bump
/// `curwords` — the inputs `prsd_headline` reads.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn test_push_hl_word(
    prs: &mut HeadlineParsedText,
    word: &[u8],
    type_: u8,
    item: Option<usize>,
    repeated: bool,
    pos: u16,
) {
    let w = word.to_vec();
    prs.words.push(HeadlineWordEntry {
        type_,
        len: word.len() as u16,
        item,
        repeated,
        pos,
        word: w,
        ..HeadlineWordEntry::default()
    });
    prs.curwords += 1;
}

/// `tsCompareString` (`tsvector_op.c:1152`): compare lexeme string `a`
/// (`lena` bytes) against `b` (`lenb` bytes), with `prefix` semantics.
fn ts_compare_string(a: &[u8], lena: i32, b: &[u8], lenb: i32, prefix: bool) -> i32 {
    let cmp;

    if lena == 0 {
        if prefix {
            cmp = 0; // empty string is prefix of anything
        } else {
            cmp = if lenb > 0 { -1 } else { 0 };
        }
    } else if lenb == 0 {
        cmp = if lena > 0 { 1 } else { 0 };
    } else {
        let n = (lena as u32).min(lenb as u32) as usize;
        let mut c = memcmp(&a[..n], &b[..n]);

        if prefix {
            if c == 0 && lena > lenb {
                c = 1; // a is longer, so not a prefix of b
            }
        } else if c == 0 && lena != lenb {
            c = if lena < lenb { -1 } else { 1 };
        }
        cmp = c;
    }

    cmp
}

/// `memcmp(a, b, n)` for equal-length slices: byte-lexicographic comparison
/// returning the C sign convention.
fn memcmp(a: &[u8], b: &[u8]) -> i32 {
    for (x, y) in a.iter().zip(b.iter()) {
        if x != y {
            return (*x as i32) - (*y as i32);
        }
    }
    0
}

/// `hladdword` (`ts_parse.c:439`): add a word to `prs->words[]`.
fn hladdword(prs: &mut HeadlineParsedText, buf: &[u8], buflen: i32, type_: i32) -> PgResult<()> {
    if prs.curwords >= prs.lenwords {
        // C: prs->lenwords *= 2; repalloc(...).  The PgVec grows on push;
        // lenwords tracks the C field.  (C callers seed lenwords > 0; the
        // 0 -> 32 guard only covers the Default(0) case C never produces,
        // matching parsetext.)
        prs.lenwords = if prs.lenwords == 0 {
            32
        } else {
            prs.lenwords * 2
        };
    }
    // C: memset(&words[curwords], 0, ...); set type/len; palloc+memcpy word.
    let word = buf[..buflen as usize].to_vec();
    prs.words.push(HeadlineWordEntry {
        type_: type_ as u8,
        len: buflen as u16,
        word,
        ..HeadlineWordEntry::default()
    });
    prs.curwords += 1;
    Ok(())
}

/// `hlfinditem` (`ts_parse.c:463`): add pos and matching-query-item data to the
/// just-added word.  If the query contains more than one matching item, the
/// last-added word is replicated so each item can be pointed to; the duplicates
/// are marked `repeated`.
fn hlfinditem(
    prs: &mut HeadlineParsedText,
    query: &TSQuery,
    pos: i32,
    buf: &[u8],
    buflen: i32,
) -> PgResult<()> {
    while prs.curwords + query.size >= prs.lenwords {
        prs.lenwords = if prs.lenwords == 0 {
            32
        } else {
            prs.lenwords * 2
        };
    }

    // word = &prs->words[prs->curwords - 1]
    let word_idx = (prs.curwords - 1) as usize;
    prs.words[word_idx].pos = limitpos(pos);

    let mut i = 0;
    while i < query.size as usize {
        let matched = match &query.items[i] {
            QueryItem::Operand(op) if op.type_ == QI_VAL => {
                let start = op.distance as usize;
                let opstr = &query.operands[start..start + op.length as usize];
                ts_compare_string(opstr, op.length as i32, buf, buflen, op.prefix) == 0
            }
            _ => false,
        };

        if matched {
            if prs.words[word_idx].item.is_some() {
                // memcpy(&words[curwords], word, ...); set item; repeated = 1.
                let src = &prs.words[word_idx];
                let dup = HeadlineWordEntry {
                    selected: src.selected,
                    in_: src.in_,
                    replace: src.replace,
                    repeated: true,
                    skip: src.skip,
                    type_: src.type_,
                    len: src.len,
                    pos: src.pos,
                    word: src.word.clone(),
                    item: Some(i),
                };
                prs.words.push(dup);
                prs.curwords += 1;
            } else {
                prs.words[word_idx].item = Some(i);
            }
        }
        i += 1;
    }
    Ok(())
}

/// `addHLParsedLex` (`ts_parse.c:498`): route a run of raw lexemes (`lexs`, the
/// waste-list head from `LexizeExec`) and their normalized forms (`norms`)
/// into `prs`.  `lexs_head` is the arena index of the waste-list head returned
/// by [`lexize_exec`] via its `corr_out`; `None` is the C `NULL`.
fn add_hl_parsed_lex(
    ld: &LexizeData,
    prs: &mut HeadlineParsedText,
    query: &TSQuery,
    lexs_head: Option<usize>,
    norms: Option<&[LexizeLexeme]>,
) -> PgResult<()> {
    let mut lexs = lexs_head;
    while let Some(cur) = lexs {
        let lex = &ld.arena[cur];
        if lex.type_ > 0 {
            hladdword(prs, &lex.lemm, lex.lemm.len() as i32, lex.type_)?;
        }

        if let Some(norms) = norms {
            let mut savedpos = prs.vectorpos;
            for ptr in norms {
                if ptr.flags & TSL_ADDPOS != 0 {
                    savedpos += 1;
                }
                hlfinditem(prs, query, savedpos, &ptr.lexeme, ptr.lexeme.len() as i32)?;
            }
        }

        // tmplexs = lexs->next; pfree(lexs); lexs = tmplexs;
        lexs = ld.arena[cur].next;
    }

    if let Some(norms) = norms {
        for ptr in norms {
            if ptr.flags & TSL_ADDPOS != 0 {
                prs.vectorpos += 1;
            }
            // pfree(ptr->lexeme)
        }
        // pfree(norms)
    }
    Ok(())
}

/// `hlparsetext` (`ts_parse.c:539`): parse `buf` under configuration `cfg_id`,
/// lexize the tokens through the configuration's dictionaries, and fill the
/// headline word list `prs`, tagging words that match `query`.
///
/// Parallels [`parsetext`]; the only differences are that it threads a
/// [`TSQuery`] and routes every emitted/empty lexize result through
/// [`add_hl_parsed_lex`] (and bumps `prs.vectorpos` per produced lexeme array).
pub fn hlparsetext(
    cfg_id: u32,
    prs: &mut HeadlineParsedText,
    query: &TSQuery,
    buf: &[u8],
) -> PgResult<()> {
    let mut ldata = LexizeData::init(cfg_id);

    // lookup_ts_config_cache / lookup_ts_parser_cache + prsstart.
    let mut prsdata: TParser = wparser_def::prsd_start(buf.to_vec(), buf.len())?;

    let result = hlparsetext_loop(&mut ldata, prs, query, &mut prsdata);

    // prsend: release the TParser's charged buffers.
    wparser_def::prsd_end(prsdata);

    result
}

fn hlparsetext_loop(
    ldata: &mut LexizeData,
    prs: &mut HeadlineParsedText,
    query: &TSQuery,
    prsdata: &mut TParser,
) -> PgResult<()> {
    // C do { ... } while (type > 0).
    loop {
        // prstoken
        let (type_, token) = wparser_def::prsd_nexttoken(prsdata)?;
        let token = token.to_vec();
        let lenlemm = token.len();

        if type_ > 0 && lenlemm >= MAXSTRLEN {
            // IGNORE_LONGLEXEME: emit NOTICE and `continue` (re-test do-while
            // condition), without lexizing.
            let _ = ereport(NOTICE)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg("word is too long to be indexed")
                .errdetail(format!(
                    "Words longer than {MAXSTRLEN} characters are ignored."
                ))
                .finish(ErrorLocation::new(
                    "src/backend/tsearch/ts_parse.c",
                    0,
                    "hlparsetext",
                ));
            continue;
        }

        ldata.lexize_add_lemm(type_, token);

        // C inner do-while: LexizeExec returns norms and sets lexs (the waste
        // list head).  We always request the corresponding waste head, then
        // walk it in addHLParsedLex; norms==None mirrors the C NULL branch.
        loop {
            let mut corr = None;
            let norms = lexize_exec(ldata, true, &mut corr)?;
            match &norms {
                Some(n) => {
                    prs.vectorpos += 1;
                    add_hl_parsed_lex(ldata, prs, query, corr, Some(n))?;
                }
                None => {
                    add_hl_parsed_lex(ldata, prs, query, corr, None)?;
                }
            }
            if norms.is_none() {
                break;
            }
        }

        // do-while condition.
        if type_ <= 0 {
            break;
        }
    }
    Ok(())
}

/// `generateHeadline` (`ts_parse.c:606`): assemble the headline text object
/// from a `HeadlineParsedText`.  Returns the varlena payload bytes (a 4-byte
/// length header followed by the assembled text), with `SET_VARSIZE` written
/// into the header — exactly the `text *` the C function returns.
///
/// The per-word flags (`in`, `repeated`, `replace`, `skip`, `selected`) are
/// inputs, set upstream by the headline selector (`prsd_headline`/`mark_*`).
// The `else if (!wrd->repeated) { if (infrag) infrag = 0; ... }` branch is kept
// nested exactly as in C (ts_parse.c:667-672); the nested `if` is not collapsed
// so the fragment-close branch stays structurally 1:1 with the C source.
#[allow(clippy::collapsible_if)]
pub fn generateHeadline(prs: &HeadlineParsedText) -> Vec<u8> {
    // C `palloc(len=128)` for the varlena; `ptr` writes at out + VARHDRSZ.
    // The Vec models the varlena buffer; `out[0..VARHDRSZ]` is the header and
    // `out[VARHDRSZ..]` is the assembled payload.  The C `len *= 2` repalloc
    // loop is subsumed by Vec growth, so the growth loop is unnecessary; we
    // mirror C's control flow exactly otherwise.
    let mut out: Vec<u8> = vec![0u8; VARHDRSZ];
    let mut numfragments: i32 = 0;
    let mut infrag: i16 = 0;

    let mut i = 0usize;
    while (i as i32) < prs.curwords {
        let wrd = &prs.words[i];

        if wrd.in_ && !wrd.repeated {
            if infrag == 0 {
                // start of a new fragment
                infrag = 1;
                numfragments += 1;
                // add a fragment delimiter if this is after the first one
                if numfragments > 1 {
                    out.extend_from_slice(&prs.fragdelim[..prs.fragdelimlen as usize]);
                }
            }
            if wrd.replace {
                out.push(b' ');
            } else if !wrd.skip {
                if wrd.selected {
                    out.extend_from_slice(&prs.startsel[..prs.startsellen as usize]);
                }
                out.extend_from_slice(&wrd.word[..wrd.len as usize]);
                if wrd.selected {
                    out.extend_from_slice(&prs.stopsel[..prs.stopsellen as usize]);
                }
            }
        } else if !wrd.repeated {
            if infrag != 0 {
                infrag = 0;
            }
            // pfree(wrd->word): the word's bytes are dropped with `prs`.
        }

        i += 1;
    }

    // SET_VARSIZE(out, ptr - out): little-endian 4-byte header = len << 2.
    let total_len = out.len() as u32;
    out[..VARHDRSZ].copy_from_slice(&(total_len << 2).to_ne_bytes());
    out
}
