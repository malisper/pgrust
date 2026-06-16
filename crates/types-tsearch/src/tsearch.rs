//! `tsvector` / `tsquery` on-disk vocabulary (`tsearch/ts_type.h`,
//! `tsearch/ts_utils.h`), trimmed to the items the index/rank ports consume.

use alloc::vec::Vec;
use types_core::{uint16, uint32};

/// `WordEntry` (ts_type.h) — one entry per lexeme in a `tsvector`. C is a
/// bitfield `uint32 haspos:1, len:11, pos:20`; stored as the raw word with
/// accessors reproducing the exact layout.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WordEntry {
    pub word: uint32,
}

#[allow(clippy::len_without_is_empty)]
impl WordEntry {
    /// `haspos:1` — whether position data follows the lexeme.
    #[inline]
    pub fn haspos(self) -> uint32 {
        self.word & 0x1
    }
    /// `len:11` — lexeme byte length.
    #[inline]
    pub fn len(self) -> uint32 {
        (self.word >> 1) & 0x7FF
    }
    /// `pos:20` — byte offset to the lexeme string.
    #[inline]
    pub fn pos(self) -> uint32 {
        (self.word >> 12) & 0xFFFFF
    }
    /// Set the `haspos:1` bit.
    #[inline]
    pub fn set_haspos(&mut self, v: uint32) {
        self.word = (self.word & !0x1) | (v & 0x1);
    }
    /// Set the `len:11` field.
    #[inline]
    pub fn set_len(&mut self, v: uint32) {
        self.word = (self.word & !(0x7FF << 1)) | ((v & 0x7FF) << 1);
    }
    /// Set the `pos:20` field.
    #[inline]
    pub fn set_pos(&mut self, v: uint32) {
        self.word = (self.word & !(0xFFFFF << 12)) | ((v & 0xFFFFF) << 12);
    }
}

/// `DATAHDRSIZE` (ts_type.h) — `offsetof(TSVectorData, entries)`: the varlena
/// length word (`int32 vl_len_`) plus the `int32 size` field.
pub const DATAHDRSIZE: usize = 8;

/// `LIMITPOS(x)` (ts_type.h) — clamp a position to `MAXENTRYPOS - 1`.
#[inline]
pub fn LIMITPOS(x: i32) -> i32 {
    if x >= MAXENTRYPOS as i32 {
        MAXENTRYPOS as i32 - 1
    } else {
        x
    }
}

/// `MAXSTRLEN` (ts_type.h) — `(1<<11) - 1`.
pub const MAXSTRLEN: u32 = (1 << 11) - 1;
/// `MAXSTRPOS` (ts_type.h) — `(1<<20) - 1`.
pub const MAXSTRPOS: u32 = (1 << 20) - 1;

/// `WordEntryPos` (ts_type.h) — a `uint16` bitfield `weight:2, pos:14`.
pub type WordEntryPos = uint16;

/// `WEP_GETWEIGHT(x)` — the 2-bit weight (`x >> 14`).
#[inline]
pub fn WEP_GETWEIGHT(x: WordEntryPos) -> uint16 {
    x >> 14
}
/// `WEP_GETPOS(x)` — the 14-bit position (`x & 0x3fff`).
#[inline]
pub fn WEP_GETPOS(x: WordEntryPos) -> uint16 {
    x & 0x3fff
}
/// `WEP_SETWEIGHT(x, v)` — set the 2-bit weight.
#[inline]
pub fn WEP_SETWEIGHT(x: &mut WordEntryPos, v: uint16) {
    *x = (v << 14) | (*x & 0x3fff);
}
/// `WEP_SETPOS(x, v)` — set the 14-bit position.
#[inline]
pub fn WEP_SETPOS(x: &mut WordEntryPos, v: uint16) {
    *x = (*x & 0xc000) | (v & 0x3fff);
}

/// `MAXENTRYPOS` (ts_type.h) — `1<<14`.
pub const MAXENTRYPOS: u16 = 1 << 14;
/// `MAXNUMPOS` (ts_type.h).
pub const MAXNUMPOS: i32 = 256;

/// `WordEntryPosVector1` (ts_type.h) — a position vector with exactly one entry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WordEntryPosVector1 {
    pub npos: uint16,
    pub pos: [WordEntryPos; 1],
}

/// `QueryItemType` (ts_type.h).
pub type QueryItemType = i8;

/// `QI_VAL` — a value (operand) node.
pub const QI_VAL: QueryItemType = 1;
/// `QI_OPR` — an operator node.
pub const QI_OPR: QueryItemType = 2;
/// `QI_VALSTOP` — intermediate parse-stack stopword.
pub const QI_VALSTOP: QueryItemType = 3;

/// `QueryOperand` (ts_type.h) — a value node. Trailing `length:12, distance:20`
/// C bitfield stored as `len_dist` with accessors.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryOperand {
    /// operand or kind of operator (`QI_VAL` here)
    pub type_: QueryItemType,
    /// bitmask of allowed weights (A: 1<<3 .. D: 1<<0; 0 = any)
    pub weight: u8,
    /// true if it's a prefix search
    pub prefix: bool,
    /// CRC32 of the operand text
    pub valcrc: i32,
    /// bits 0..=11 = `length`, bits 12..=31 = `distance`
    pub len_dist: uint32,
}

impl QueryOperand {
    /// `length:12` — operand byte length.
    #[inline]
    pub fn length(self) -> uint32 {
        self.len_dist & 0xFFF
    }
    /// `distance:20` — offset to the operand text.
    #[inline]
    pub fn distance(self) -> uint32 {
        (self.len_dist >> 12) & 0xFFFFF
    }
    /// Set `length:12`.
    #[inline]
    pub fn set_length(&mut self, v: uint32) {
        self.len_dist = (self.len_dist & !0xFFF) | (v & 0xFFF);
    }
    /// Set `distance:20`.
    #[inline]
    pub fn set_distance(&mut self, v: uint32) {
        self.len_dist = (self.len_dist & !(0xFFFFF << 12)) | ((v & 0xFFFFF) << 12);
    }
}

/// `HDRSIZETQ` (ts_type.h) — `VARHDRSZ + sizeof(int32)`, the `tsquery` header
/// size up to the start of the `QueryItem` array.
pub const HDRSIZETQ: usize = 4 + core::mem::size_of::<i32>();

/// `OP_NOT` (ts_type.h).
pub const OP_NOT: i8 = 1;
/// `OP_AND` (ts_type.h).
pub const OP_AND: i8 = 2;
/// `OP_OR` (ts_type.h).
pub const OP_OR: i8 = 3;
/// `OP_PHRASE` (ts_type.h).
pub const OP_PHRASE: i8 = 4;

/// `QueryOperator` (ts_type.h) — an operator node.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryOperator {
    /// `QI_OPR` here
    pub type_: QueryItemType,
    /// operator code: `OP_NOT`/`OP_AND`/`OP_OR`/`OP_PHRASE`
    pub oper: i8,
    /// distance between args for `OP_PHRASE`
    pub distance: i16,
    /// offset to the left operand (right operand is `item + 1`)
    pub left: uint32,
}

/// `QueryItem` (ts_type.h) — a C `union` of a bare type tag, a
/// [`QueryOperator`], and a [`QueryOperand`]; modeled as a Rust enum (selected
/// by the leading `type` byte all members share at offset 0).
#[derive(Clone, Debug)]
pub enum QueryItem {
    Type_(QueryItemType),
    Qoperator(QueryOperator),
    Qoperand(QueryOperand),
}

impl QueryItem {
    /// Read the shared leading `type` tag, regardless of the active variant.
    #[inline]
    pub fn item_type(&self) -> QueryItemType {
        match self {
            QueryItem::Type_(t) => *t,
            QueryItem::Qoperator(o) => o.type_,
            QueryItem::Qoperand(o) => o.type_,
        }
    }
}

impl Default for QueryItem {
    fn default() -> Self {
        QueryItem::Type_(0)
    }
}

/// `TSQuerySign` (ts_utils.h) — `typedef uint64 TSQuerySign`. A lossy bit
/// signature of a `tsquery`'s operand CRCs, used by the GiST opclass.
pub type TSQuerySign = u64;

/// `TSQS_SIGLEN` (ts_utils.h) — `sizeof(TSQuerySign) * BITS_PER_BYTE` = 64.
pub const TSQS_SIGLEN: u32 = 64;

/// `P_TSV_OPR_IS_DELIM` (ts_utils.h) — flag for `init_tsvector_parser`.
pub const P_TSV_OPR_IS_DELIM: i32 = 1 << 0;
/// `P_TSV_IS_TSQUERY` (ts_utils.h).
pub const P_TSV_IS_TSQUERY: i32 = 1 << 1;
/// `P_TSV_IS_WEB` (ts_utils.h).
pub const P_TSV_IS_WEB: i32 = 1 << 2;

/// `P_TSQ_PLAIN` (ts_utils.h) — flag for `parse_tsquery` (plain tokenizer).
pub const P_TSQ_PLAIN: i32 = 1 << 0;
/// `P_TSQ_WEB` (ts_utils.h) — flag for `parse_tsquery` (websearch tokenizer).
pub const P_TSQ_WEB: i32 = 1 << 1;

/// Opaque handle to a `TSVectorParseStateData` owned by
/// `utils/adt/tsvector_parser.c` (the unported `backend-utils-adt-tsvector-core`
/// unit). C declares `struct TSVectorParseStateData` opaque ("opaque struct in
/// tsvector_parser.c"); the `tsquery` parser only holds a `TSVectorParseState`
/// pointer and threads it through `init`/`reset`/`gettoken`/`close`. Until that
/// unit lands, the state lives behind this token, minted by the
/// `init_tsvector_parser` seam and resolved by the owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TsVectorParseStateHandle(pub u64);

/// `TSTernaryValue` (ts_utils.h) — ternary logic for `TS_execute`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TSTernaryValue {
    /// definitely no match
    TS_NO = 0,
    /// definitely does match
    TS_YES = 1,
    /// can't verify match for lack of pos data
    TS_MAYBE = 2,
}

/// `ExecPhraseData` (ts_utils.h) — position data passed to a `TSExecuteCallback`
/// for phrase matching. `pos` is the owned position list (C `WordEntryPos *`).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecPhraseData {
    /// number of positions reported
    pub npos: i32,
    /// `pos` points to palloc'd data?
    pub allocated: bool,
    /// positions are where the query is NOT matched
    pub negate: bool,
    /// ordered, non-duplicate lexeme positions
    pub pos: Vec<WordEntryPos>,
    /// width of match in lexemes, less 1
    pub width: i32,
}

/// `TS_EXEC_EMPTY` (ts_utils.h).
pub const TS_EXEC_EMPTY: uint32 = 0x00;
/// `TS_EXEC_SKIP_NOT` (ts_utils.h).
pub const TS_EXEC_SKIP_NOT: uint32 = 0x01;
/// `TS_EXEC_PHRASE_NO_POS` (ts_utils.h).
pub const TS_EXEC_PHRASE_NO_POS: uint32 = 0x02;

/// The query-operand check callback handed to the `TS_execute` family, mirroring
/// the C `TSExecuteCallback`
/// (`TSTernaryValue (*)(void *checkval, QueryOperand *val, ExecPhraseData *)`).
///
/// The first argument is the operand's index in the query's `QueryItem` array
/// (the C `(QueryItem *) val - GETQUERY(query)` identity); the second is the
/// operand; the third is the optional position-data output.
pub type CheckCondition<'a> =
    dyn FnMut(usize, &QueryOperand, Option<&mut ExecPhraseData>) -> TSTernaryValue + 'a;
