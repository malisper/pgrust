//! Port of `src/backend/tsearch/spell.c` (PostgreSQL 18.3) — the ISpell /
//! Hunspell dictionary build pipeline and word normalizer.
//!
//! Builds prefix trees (tries) of words ([`SpNode`]) and affixes
//! ([`AffixNode`]) from `.dict` / `.affix` files, then normalizes words via
//! affix stripping and (optionally) compound splitting. The build/normalize
//! API mirrors the C externs declared in `tsearch/dicts/spell.h`:
//! [`IspellDict::ni_start_build`], [`IspellDict::ni_import_dictionary`],
//! [`IspellDict::ni_import_affixes`], [`IspellDict::ni_sort_dictionary`],
//! [`IspellDict::ni_sort_affixes`], [`IspellDict::ni_finish_build`],
//! [`IspellDict::ni_normalize_word`].
//!
//! # Owned model (vs the C-ABI layout)
//!
//! The C struct keeps raw `char *` C-strings, `repalloc`-grown arrays, a
//! "compact palloc" bump arena, and flexible-array trie nodes walked through
//! raw pointers. This port owns its data:
//!
//!   * the word/affix prefix trees are **index arenas** ([`SpNode`] /
//!     [`AffixNode`] vectors); a `data[i].node` child link is an
//!     `Option<usize>` index rather than a raw `*mut Node`. The packed
//!     `uint32` bitfields (`val:8,isword:1,…`) become plain typed fields.
//!   * C-strings (`word`, affix `flag`/`find`/`repl`, the `AffixData`
//!     flagsets) are owned NUL-free byte vectors ([`PgVec<u8>`]); the
//!     `VoidString` "" sentinel is an empty slice.
//!   * the `compact_palloc0` bump arena disappears (a palloc-overhead
//!     optimization with no observable behaviour).
//!   * the compiled affix matcher is an enum [`AffixReg`]: a [`Regis`] subset,
//!     an engine-owned compiled-regex handle, or "simple" — replacing the C
//!     `union { regex_t *pregex; Regis regis; }` plus the `issimple`/`isregis`
//!     bitfields.
//!
//! # Memory model
//!
//! Every growable member is a [`PgVec`] charged to the dictionary's owning
//! [`MemoryContext`] (C: the dictionary cache context, with a temp `buildCxt`
//! for scratch). The dictionary is held as a movable [`McxOwned`] bundle keyed
//! by an opaque [`SpellHandle`] (this crate owns the
//! `backend-tsearch-spell-seams` build/normalize API the `ispell` template
//! calls). Dropping the bundle releases all of it.

#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use backend_utils_error::ereport;
use mcx::{Mcx, PgVec};
use types_error::{
    PgError, PgResult, ERRCODE_CONFIG_FILE_ERROR, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_REGULAR_EXPRESSION, ERROR,
};

pub use backend_tsearch_ispell_regis::{rs_compile, rs_execute, rs_is_regis, Regis, RegisNodeKind};

mod build;
mod normalize;
mod registry;

pub use registry::init_seams;

/* ---- Hunspell/ISpell flag bits (spell.h) ---- */
pub const FF_COMPOUNDONLY: i32 = 0x01;
pub const FF_COMPOUNDBEGIN: i32 = 0x02;
pub const FF_COMPOUNDMIDDLE: i32 = 0x04;
pub const FF_COMPOUNDLAST: i32 = 0x08;
pub const FF_COMPOUNDFLAG: i32 = FF_COMPOUNDBEGIN | FF_COMPOUNDMIDDLE | FF_COMPOUNDLAST;
pub const FF_COMPOUNDFLAGMASK: i32 = 0x0f;
pub const FF_COMPOUNDPERMITFLAG: i32 = 0x10;
pub const FF_COMPOUNDFORBIDFLAG: i32 = 0x20;
pub const FF_CROSSPRODUCT: i32 = 0x40;

/// Affix `type` field: `FF_SUFFIX` matches a word ending.
pub const FF_SUFFIX: i32 = 1;
/// Affix `type` field: `FF_PREFIX` matches a word beginning.
pub const FF_PREFIX: i32 = 0;

pub const FLAGNUM_MAXSIZE: i32 = 1 << 16;

const MAX_NORM: usize = 1024;
const MAXNORMLEN: usize = 256;

/// `DEFAULT_COLLATION_OID` — the collation spell.c folds with.
const DEFAULT_COLLATION_OID: types_core::Oid = 100;

/// `FlagMode`: encoding of affix flags in Hunspell dictionaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlagMode {
    /// One character (like ispell).
    Char,
    /// Two characters.
    Long,
    /// Number, `>= 0` and `< 65536`.
    Num,
}

// ---------------------------------------------------------------------------
// SPELL — a dictionary word entry during construction.
// ---------------------------------------------------------------------------

/// `SPELL` — an entry in the words list during construction.
///
/// The C struct unions `flag` (a `const char *`, used until `NISortDictionary`)
/// with `{ affix, len }` (set during the sort). Both members are kept here as
/// owned fields; `flag` is consumed/cleared when the sort fills `affix`/`len`.
pub struct Spell<'mcx> {
    /// The C flexible-array `word` (the lowercased word, NUL-free).
    pub word: PgVec<'mcx, u8>,
    /// The C union `flag`: the affix flag set, until the sort.
    pub flag: PgVec<'mcx, u8>,
    /// The C union `d.affix`: index into `AffixData`, after the sort.
    pub affix: i32,
    /// The C union `d.len`: `strlen(word)`, after the sort.
    pub len: i32,
}

// ---------------------------------------------------------------------------
// AFFIX — an affix-rule entry.
// ---------------------------------------------------------------------------

/// The compiled word-ending matcher for an affix rule (the C
/// `union { regex_t *pregex; Regis regis; }` plus the `issimple`/`isregis`
/// bitfields, as a single typed enum).
pub enum AffixReg<'mcx> {
    /// `issimple`: the mask is `.` or empty — every word matches.
    Simple,
    /// `isregis`: a [`Regis`] fast-subset matcher.
    Regis(Regis<'mcx>),
    /// neither: an engine-owned compiled regex (the C `regex_t *pregex`),
    /// carried as the [`types_regex::RegexCompiled`] value.
    Regex(types_regex::RegexCompiled),
}

impl Drop for AffixReg<'_> {
    fn drop(&mut self) {
        match self {
            AffixReg::Simple | AffixReg::Regis(_) => {}
            // C: the regex state lives in the dictionary context and is freed
            // when it is destroyed; the engine owns it, so release it through
            // the free seam (the clone drops one `Rc` ref; the carrier in
            // `self` is dropped immediately after, releasing the last one).
            AffixReg::Regex(h) => backend_regex_core_seams::pg_regfree::call(h.clone()),
        }
    }
}

/// `AFFIX` — an affix-rule entry.
pub struct Affix<'mcx> {
    /// The C `flag` (affix flag string, NUL-free).
    pub flag: PgVec<'mcx, u8>,
    /// The C `type` bitfield: [`FF_PREFIX`] or [`FF_SUFFIX`].
    pub type_: i32,
    /// The C `flagflags` bitfield (the `FF_*` compound option bits).
    pub flagflags: i32,
    /// The C `find` (text to find at the word edge, NUL-free).
    pub find: PgVec<'mcx, u8>,
    /// The C `repl` (replacement, NUL-free); `replen` is `repl.len()`.
    pub repl: PgVec<'mcx, u8>,
    /// The C `reg` union plus `issimple`/`isregis` bitfields.
    pub reg: AffixReg<'mcx>,
}

impl Affix<'_> {
    /// The C `replen` bitfield: `strlen(repl)`.
    #[inline]
    fn replen(&self) -> i32 {
        self.repl.len() as i32
    }
}

// ---------------------------------------------------------------------------
// SPNode — a dictionary trie node (prefix tree over words), arena-indexed.
// ---------------------------------------------------------------------------

/// `SPNodeData` — one slot in a word trie node. The C packed bitfields
/// (`val:8,isword:1,compoundflag:4,affix:19`) become plain fields, and the raw
/// `SPNode *node` child link becomes an `Option<usize>` index into the arena.
#[derive(Clone, Copy)]
pub struct SpNodeData {
    /// The C `val:8` — the character byte this slot matches.
    pub val: u8,
    /// The C `isword:1` — this slot ends a dictionary word.
    pub isword: bool,
    /// The C `compoundflag:4` — the compound options at this word.
    pub compoundflag: u32,
    /// The C `affix:19` — index into `AffixData` of this word's flag set.
    pub affix: u32,
    /// The C `node` — child subtree (one level deeper), arena index.
    pub node: Option<usize>,
}

impl SpNodeData {
    fn empty() -> Self {
        SpNodeData {
            val: 0,
            isword: false,
            compoundflag: 0,
            affix: 0,
            node: None,
        }
    }
}

/// `SPNode` — a dictionary trie node: the C flexible-array `data[length]`.
pub struct SpNode<'mcx> {
    /// The C `data[]` array (`length` == `data.len()`).
    pub data: PgVec<'mcx, SpNodeData>,
}

// ---------------------------------------------------------------------------
// AffixNode — an affix trie node (prefix tree over masks), arena-indexed.
// ---------------------------------------------------------------------------

/// `AffixNodeData` — one slot in an affix trie node. The C packed bitfields
/// (`val:8,naff:24`), the `AFFIX **aff` pointer array, and the raw
/// `AffixNode *node` link become a typed `val`, a `PgVec<usize>` of affix
/// indices, and an `Option<usize>` arena index.
pub struct AffixNodeData<'mcx> {
    /// The C `val:8` — the character byte this slot matches.
    pub val: u8,
    /// The C `aff` array (the C `naff` == `aff.len()`): indices into `Affix`.
    pub aff: PgVec<'mcx, usize>,
    /// The C `node` — child subtree (one level deeper), arena index.
    pub node: Option<usize>,
}

impl<'mcx> AffixNodeData<'mcx> {
    fn empty(mcx: Mcx<'mcx>) -> Self {
        AffixNodeData {
            val: 0,
            aff: PgVec::new_in(mcx),
            node: None,
        }
    }
    /// The C `naff` bitfield (`aff.len()`).
    #[inline]
    fn naff(&self) -> usize {
        self.aff.len()
    }
}

/// `AffixNode` — an affix trie node: the C `isvoid:1,length:31` + `data[]`.
pub struct AffixNode<'mcx> {
    /// The C `isvoid:1` — a synthetic node holding empty-replacement affixes.
    pub isvoid: bool,
    /// The C `data[]` array (`length` == `data.len()`).
    pub data: PgVec<'mcx, AffixNodeData<'mcx>>,
}

// ---------------------------------------------------------------------------
// CMPDAffix / CompoundAffixFlag
// ---------------------------------------------------------------------------

/// `CMPDAffix` — a compound-affix descriptor.
pub struct CmpdAffix<'mcx> {
    /// The C `affix` (the replacement text to split on, NUL-free).
    pub affix: PgVec<'mcx, u8>,
    /// The C `len`.
    pub len: i32,
    /// The C `issuffix`.
    pub issuffix: bool,
}

/// The C `CompoundAffixFlag.flag` union `{ const char *s; uint32 i; }`, tagged
/// by `flagMode`.
#[derive(Clone)]
pub enum FlagKey {
    /// `flagMode != Num`: the flag is a string.
    Str(Vec<u8>),
    /// `flagMode == Num`: the flag is a number.
    Num(u32),
}

/// `CompoundAffixFlag` — a Hunspell compound-affix option.
#[derive(Clone)]
pub struct CompoundAffixFlag {
    /// The C `flag` union, tagged by `flagMode`.
    pub flag: FlagKey,
    /// The C `flagMode`.
    pub flag_mode: FlagMode,
    /// The C `value` (an `FF_*` bit).
    pub value: u32,
}

// ---------------------------------------------------------------------------
// IspellDict — the dictionary control object (the C `IspellDict`).
// ---------------------------------------------------------------------------

/// `IspellDict` — the dictionary control object.
///
/// All growable members are [`PgVec`]s charged to the owning context the build
/// state is bundled with (via [`McxOwned`]); `mcx` is the handle for that
/// context, threaded so the `&mut self` build methods can allocate.
pub struct IspellDict<'mcx> {
    /// Allocation handle for the owning dictionary context.
    mcx: Mcx<'mcx>,

    /// The C `Affix` array.
    pub affixes: PgVec<'mcx, Affix<'mcx>>,

    /// The C `Suffix` trie root (arena index), with the arena `af_arena`.
    pub suffix: Option<usize>,
    /// The C `Prefix` trie root (arena index).
    pub prefix: Option<usize>,
    /// The affix-trie node arena (`Suffix`/`Prefix` index into it).
    pub af_arena: PgVec<'mcx, AffixNode<'mcx>>,

    /// The C `Dictionary` trie root (arena index), with the arena `sp_arena`.
    pub dictionary: Option<usize>,
    /// The word-trie node arena (`Dictionary` indexes into it).
    pub sp_arena: PgVec<'mcx, SpNode<'mcx>>,

    /// The C `AffixData`: array of affix flag-sets (each NUL-free).
    pub affix_data: PgVec<'mcx, PgVec<'mcx, u8>>,
    /// The C `useFlagAliases`.
    pub use_flag_aliases: bool,

    /// The C `CompoundAffix`.
    pub compound_affix: PgVec<'mcx, CmpdAffix<'mcx>>,

    /// The C `usecompound`.
    pub usecompound: bool,
    /// The C `flagMode`.
    pub flag_mode: FlagMode,

    /// The C `CompoundAffixFlags`.
    pub compound_affix_flags: PgVec<'mcx, CompoundAffixFlag>,

    /* construction-only field, the C `Spell` array (cleared by NIFinishBuild) */
    pub spell: PgVec<'mcx, Spell<'mcx>>,

    /// Whether `ni_start_build` has run (the C `buildCxt != NULL`).
    building: bool,
}

impl<'mcx> IspellDict<'mcx> {
    /// A fresh, empty dictionary (the C `palloc0(sizeof(IspellDict))`),
    /// allocating in `mcx`.
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        IspellDict {
            mcx,
            affixes: PgVec::new_in(mcx),
            suffix: None,
            prefix: None,
            af_arena: PgVec::new_in(mcx),
            dictionary: None,
            sp_arena: PgVec::new_in(mcx),
            affix_data: PgVec::new_in(mcx),
            use_flag_aliases: false,
            compound_affix: PgVec::new_in(mcx),
            usecompound: false,
            flag_mode: FlagMode::Char,
            compound_affix_flags: PgVec::new_in(mcx),
            spell: PgVec::new_in(mcx),
            building: false,
        }
    }

    /// `NIStartBuild`: set up the temporary construction memory context.
    ///
    /// (One context backs the whole dictionary lifetime, so this only flips the
    /// "building" flag, matching C's `buildCxt != NULL`.)
    pub fn ni_start_build(&mut self) -> PgResult<()> {
        self.building = true;
        Ok(())
    }

    /// `NIFinishBuild`: release the no-longer-needed construction scratch (the
    /// `Spell` word list and the compound-flag table), matching C's
    /// `MemoryContextDelete(buildCxt)` + zeroing of the dangling pointers.
    pub fn ni_finish_build(&mut self) -> PgResult<()> {
        let mcx = self.mcx;
        self.spell = PgVec::new_in(mcx);
        self.compound_affix_flags = PgVec::new_in(mcx);
        self.building = false;
        Ok(())
    }
}

/* ====================== error helpers ====================== */

/// `ereport(ERROR, errcode(ERRCODE_CONFIG_FILE_ERROR), errmsg(...))`.
fn config_file_error(msg: String) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_CONFIG_FILE_ERROR)
        .errmsg(msg)
        .into_error()
}

/// `elog(ERROR, ...)` (an `XX000` internal error).
fn elog_internal(msg: String) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg_internal(msg)
        .into_error()
}

/* ====================== locale/encoding seam wrappers ====================== */

/// `pg_mblen_cstr(c)` — byte length of the leading character of `s`.
#[inline]
fn pg_mblen(s: &[u8]) -> usize {
    if s.is_empty() {
        return 1;
    }
    backend_utils_mb_mbutils_seams::pg_mblen_range::call(s) as usize
}

/// `pg_mblen` over a non-empty slice, clamped to `s.len()` so the byte length
/// never indexes past the buffer.
#[inline]
fn pg_mblen_clamped(s: &[u8]) -> usize {
    pg_mblen(s).min(s.len()).max(1)
}

/// `t_isalpha_cstr(c)` — is the leading character of `s` alphabetic?
#[inline]
fn t_isalpha(s: &[u8]) -> bool {
    backend_tsearch_ts_locale_seams::t_isalpha::call(s)
}

/// `t_iseq(c, x)` — the leading byte of `s` equals the ASCII byte `x`.
#[inline]
fn t_iseq(s: &[u8], x: u8) -> bool {
    !s.is_empty() && s[0] == x
}

/// `str_tolower(src, len, DEFAULT_COLLATION_OID)` — locale-aware lowercasing,
/// returning a fresh owned buffer. C allocates in the caller's context; here
/// we fold into a plain `Vec` scratch (the result is copied into context-owned
/// storage by the callers via [`new_bytes`]).
#[inline]
fn str_tolower(mcx: Mcx<'_>, src: &[u8]) -> PgResult<Vec<u8>> {
    let folded = backend_utils_adt_formatting_seams::str_tolower::call(mcx, src, DEFAULT_COLLATION_OID)?;
    Ok(folded.as_slice().to_vec())
}

/// `check_stack_depth()` — `ereport(ERROR, ERRCODE_STATEMENT_TOO_COMPLEX)` on
/// stack overflow (the seam owner raises it).
#[inline]
fn check_stack_depth() -> PgResult<()> {
    backend_utils_misc_stack_depth_seams::check_stack_depth::call()
}

/* ====================== byte-string helpers ====================== */
//
// The owned model represents every C-string as a NUL-free `&[u8]`/`Vec<u8>`, so
// the C libc helpers (`strcmp`, `strncmp`, `findchar`, …) reduce to plain,
// bounds-checked slice work that reproduces C's behaviour byte-for-byte.

/// Compare two byte strings with C `strcmp` semantics (the shorter sorts first
/// when it is a prefix of the other — its implicit NUL is less than any byte).
fn bcmp(a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    let n = a.len().min(b.len());
    for i in 0..n {
        match a[i].cmp(&b[i]) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
    }
    a.len().cmp(&b.len())
}

/// `strncmp(a, b, n)` over NUL-free slices: compare at most `n` bytes.
fn bncmp(a: &[u8], b: &[u8], n: usize) -> core::cmp::Ordering {
    let alim = a.len().min(n);
    let blim = b.len().min(n);
    bcmp(&a[..alim], &b[..blim])
}

/// Backward byte compare (suffix-tree ops), like spell.c's `strbcmp`: compare
/// from the ends; the shorter string sorts first.
fn strbcmp(a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    let mut ia = a.iter().rev();
    let mut ib = b.iter().rev();
    loop {
        match (ia.next(), ib.next()) {
            (Some(x), Some(y)) => match x.cmp(y) {
                core::cmp::Ordering::Equal => {}
                ord => return ord,
            },
            (None, Some(_)) => return core::cmp::Ordering::Less,
            (Some(_), None) => return core::cmp::Ordering::Greater,
            (None, None) => return core::cmp::Ordering::Equal,
        }
    }
}

/// Backward byte compare over at most `count` bytes, like `strbncmp`.
fn strbncmp(a: &[u8], b: &[u8], count: usize) -> core::cmp::Ordering {
    let mut ia = a.iter().rev();
    let mut ib = b.iter().rev();
    let mut l = count;
    while l > 0 {
        match (ia.next(), ib.next()) {
            (Some(x), Some(y)) => match x.cmp(y) {
                core::cmp::Ordering::Equal => {}
                ord => return ord,
            },
            (None, Some(_)) => return core::cmp::Ordering::Less,
            (Some(_), None) => return core::cmp::Ordering::Greater,
            (None, None) => return core::cmp::Ordering::Equal,
        }
        l -= 1;
    }
    core::cmp::Ordering::Equal
}

/// `findchar(str, c)` (`ts_locale.c`): byte offset of the first whole character
/// equal to the ASCII byte `c`, walking by `pg_mblen`; `None` if absent.
fn findchar(s: &[u8], c: u8) -> Option<usize> {
    let mut off = 0usize;
    while off < s.len() {
        if t_iseq(&s[off..], c) {
            return Some(off);
        }
        off += pg_mblen(&s[off..]);
    }
    None
}

/// `findchar2(str, c1, c2)`: like [`findchar`] but matches either ASCII byte.
fn findchar2(s: &[u8], c1: u8, c2: u8) -> Option<usize> {
    let mut off = 0usize;
    while off < s.len() {
        if t_iseq(&s[off..], c1) || t_iseq(&s[off..], c2) {
            return Some(off);
        }
        off += pg_mblen(&s[off..]);
    }
    None
}

/// `strchr(s, c)`: byte offset of the first occurrence of the ASCII byte `c`.
fn bstrchr(s: &[u8], c: u8) -> Option<usize> {
    s.iter().position(|&b| b == c)
}

/// `strstr(haystack, needle)`: byte offset of the first occurrence of `needle`.
fn bstrstr(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > hay.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

#[inline]
fn isspace(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}
#[inline]
fn isprint(c: u8) -> bool {
    (0x20..=0x7e).contains(&c)
}
#[inline]
fn isdigit(c: u8) -> bool {
    c.is_ascii_digit()
}

/// Render a byte string for an error message (the C `%s`), losslessly.
fn bytes_lossy(s: &[u8]) -> String {
    String::from_utf8_lossy(s).into_owned()
}

/// Build a context-charged `PgVec<u8>` from `bytes` (the C `pstrdup`/`cpstrdup`).
fn new_bytes<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut v = PgVec::new_in(mcx);
    v.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    v.extend_from_slice(bytes);
    Ok(v)
}

/// `try_reserve` one `T` slot in `v`, mapping failure to the context OOM error.
#[inline]
fn reserve_one<'mcx, T>(mcx: Mcx<'mcx>, v: &mut PgVec<'mcx, T>) -> PgResult<()> {
    v.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<T>()))
}

/* ---- strtol ---- */

/// `strtol(s, &next, 10)` returning `(value, consumed_len, ok)`, mirroring the
/// C library `long strtol` (host `long` is 64-bit == [`i64`]).
///
/// `ok` is false only on no-digits or `errno == ERANGE` (i64 overflow); on
/// overflow the value clamps to `i64::MAX`/`MIN` exactly as libc does.
/// `consumed_len` is the byte offset of the C `next` (0 iff no digits).
fn strtol(s: &[u8]) -> (i64, usize, bool) {
    let mut p = 0usize;
    while p < s.len() && isspace(s[p]) {
        p += 1;
    }
    let mut neg = false;
    if p < s.len() && s[p] == b'+' {
        p += 1;
    } else if p < s.len() && s[p] == b'-' {
        neg = true;
        p += 1;
    }
    let digits_start = p;
    let mut val: i64 = 0; // accumulate in the negative space (LONG_MIN-safe)
    let mut overflow = false;
    while p < s.len() && isdigit(s[p]) {
        let d = i64::from(s[p] - b'0');
        match val.checked_mul(10).and_then(|v| v.checked_sub(d)) {
            Some(v) => val = v,
            None => {
                overflow = true;
                val = i64::MIN;
            }
        }
        p += 1;
    }
    if p == digits_start {
        return (0, 0, false);
    }
    let val = if neg {
        if overflow {
            i64::MIN
        } else {
            val
        }
    } else if overflow {
        i64::MAX
    } else {
        -val
    };
    (val, p, !overflow)
}

/// `atoi(s)`: leading-integer parse, 0 on failure (C semantics).
fn atoi(s: &[u8]) -> i32 {
    strtol(s).0 as i32
}

#[cfg(test)]
mod tests;
