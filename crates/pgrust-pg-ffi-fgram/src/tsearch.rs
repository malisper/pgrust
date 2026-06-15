//! Full-text-search ABI structs from `src/include/tsearch/ts_type.h` and the
//! `TSLexeme` from `src/include/tsearch/ts_public.h`.
//!
//! These are the on-disk / `Datum` layouts of the `tsvector` and `tsquery`
//! types.  Both are varlena types: a `tsvector` is a `TSVectorData` (varlena
//! header + `int32 size` + a `FLEXIBLE_ARRAY_MEMBER` of [`WordEntry`] followed
//! by the lexeme/position storage), and a `tsquery` is a `TSQueryData`
//! (varlena header + `int32 size` + a `FLEXIBLE_ARRAY_MEMBER` of bytes holding
//! the [`QueryItem`] array and the operand C-strings).
//!
//! C uses bitfields for [`WordEntry`] (`haspos:1, len:11, pos:20` packed into a
//! single `uint32`) and for the value portion of [`QueryOperand`]
//! (`length:12, distance:20` packed into a single `uint32`).  Rust has no
//! native bitfields, so those packed words are stored as a plain `uint32` with
//! accessor/mutator methods that reproduce the exact bit layout the C macros
//! and struct definitions use.  The `#[repr(C)]` value structs reproduce the
//! exact field offsets, sizes, and alignment of their C counterparts; the
//! compile-time `static_assertions` and the unit tests pin the layout.

use core::ffi::{c_char, c_int};

use crate::{uint16, uint32};

/// `WordEntry` (ts_type.h) -- one entry per lexeme in a `tsvector`.
///
/// C definition is a bitfield:
/// ```c
/// typedef struct {
///     uint32 haspos:1, len:11, pos:20;
/// } WordEntry;
/// ```
/// `haspos` (1 bit) is whether position data follows the lexeme; `len`
/// (11 bits, MAX 2Kb) is the lexeme byte length; `pos` (20 bits, MAX 1Mb) is
/// the byte offset from the end of the `entries[]` array to the lexeme string.
/// The single `uint32` is stored directly to preserve the exact bit layout.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WordEntry {
    /// Packed bitfield word: bit 0 = `haspos`, bits 1..=11 = `len`,
    /// bits 12..=31 = `pos` (little-endian bitfield order, as emitted by the
    /// C ABI on the supported targets).
    pub word: uint32,
}

// `len()` here is the C `WordEntry.len:11` bitfield accessor (lexeme byte
// length), not a container length, so the `is_empty` companion clippy wants
// does not apply.
#[allow(clippy::len_without_is_empty)]
impl WordEntry {
    /// `haspos:1` -- whether position data follows the lexeme.
    #[inline]
    pub fn haspos(self) -> uint32 {
        self.word & 0x1
    }
    /// `len:11` -- lexeme byte length (MAX `MAXSTRLEN`).
    #[inline]
    pub fn len(self) -> uint32 {
        (self.word >> 1) & 0x7FF
    }
    /// `pos:20` -- byte offset to the lexeme string (MAX `MAXSTRPOS`).
    #[inline]
    pub fn pos(self) -> uint32 {
        (self.word >> 12) & 0xFFFFF
    }
    /// Set `haspos:1`.
    #[inline]
    pub fn set_haspos(&mut self, v: uint32) {
        self.word = (self.word & !0x1) | (v & 0x1);
    }
    /// Set `len:11`.
    #[inline]
    pub fn set_len(&mut self, v: uint32) {
        self.word = (self.word & !(0x7FF << 1)) | ((v & 0x7FF) << 1);
    }
    /// Set `pos:20`.
    #[inline]
    pub fn set_pos(&mut self, v: uint32) {
        self.word = (self.word & !(0xFFFFF << 12)) | ((v & 0xFFFFF) << 12);
    }
}

/// `MAXSTRLEN` (ts_type.h) -- `(1<<11) - 1`, max lexeme length.
pub const MAXSTRLEN: u32 = (1 << 11) - 1;
/// `MAXSTRPOS` (ts_type.h) -- `(1<<20) - 1`, max lexeme position offset.
pub const MAXSTRPOS: u32 = (1 << 20) - 1;

/// `WordEntryPos` (ts_type.h) -- a `uint16` bitfield `weight:2, pos:14`.
///
/// Accessed via the `WEP_*` helpers below rather than bitfield syntax.
pub type WordEntryPos = uint16;

/// `WEP_GETWEIGHT(x)` -- the 2-bit weight (`x >> 14`).
#[inline]
pub fn WEP_GETWEIGHT(x: WordEntryPos) -> uint16 {
    x >> 14
}
/// `WEP_GETPOS(x)` -- the 14-bit position (`x & 0x3fff`).
#[inline]
pub fn WEP_GETPOS(x: WordEntryPos) -> uint16 {
    x & 0x3fff
}
/// `WEP_SETWEIGHT(x, v)` -- set the 2-bit weight.
#[inline]
pub fn WEP_SETWEIGHT(x: &mut WordEntryPos, v: uint16) {
    *x = (v << 14) | (*x & 0x3fff);
}
/// `WEP_SETPOS(x, v)` -- set the 14-bit position.
#[inline]
pub fn WEP_SETPOS(x: &mut WordEntryPos, v: uint16) {
    *x = (*x & 0xc000) | (v & 0x3fff);
}

/// `MAXENTRYPOS` (ts_type.h) -- `1<<14`.
pub const MAXENTRYPOS: u16 = 1 << 14;
/// `MAXNUMPOS` (ts_type.h) -- max number of positions per lexeme.
pub const MAXNUMPOS: i32 = 256;

/// `LIMITPOS(x)` (ts_type.h) -- clamp a position to `MAXENTRYPOS - 1`.
#[inline]
pub fn LIMITPOS(x: i32) -> i32 {
    if x >= MAXENTRYPOS as i32 {
        MAXENTRYPOS as i32 - 1
    } else {
        x
    }
}

/// `WordEntryPosVector` (ts_type.h) -- header of a per-lexeme position vector;
/// `pos[]` is a `FLEXIBLE_ARRAY_MEMBER` stored out of line.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WordEntryPosVector {
    pub npos: uint16,
    /* WordEntryPos pos[FLEXIBLE_ARRAY_MEMBER] follows */
}

/// `WordEntryPosVector1` (ts_type.h) -- a position vector with exactly one
/// entry; used as a fixed-size local in C.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WordEntryPosVector1 {
    pub npos: uint16,
    pub pos: [WordEntryPos; 1],
}

/// `TSVectorData` (ts_type.h) -- the header of a complete `tsvector` datum.
///
/// ```c
/// typedef struct {
///     int32 vl_len_;   /* varlena header */
///     int32 size;
///     WordEntry entries[FLEXIBLE_ARRAY_MEMBER];
///     /* lexemes follow the entries[] array */
/// } TSVectorData;
/// typedef TSVectorData *TSVector;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TSVectorData {
    /// varlena header (do not touch directly!)
    pub vl_len_: i32,
    /// number of lexemes (WordEntry array entries)
    pub size: i32,
    /* WordEntry entries[FLEXIBLE_ARRAY_MEMBER] follows */
}

/// `DATAHDRSIZE` (ts_type.h) -- `offsetof(TSVectorData, entries)`.
pub const DATAHDRSIZE: usize = core::mem::size_of::<TSVectorData>();

/// `QueryItemType` (ts_type.h) -- operand or kind of operator.
pub type QueryItemType = i8;

/// `QI_VAL` -- a value (operand) node.
pub const QI_VAL: QueryItemType = 1;
/// `QI_OPR` -- an operator node.
pub const QI_OPR: QueryItemType = 2;
/// `QI_VALSTOP` -- only used in the intermediate parse stack (a stopword).
pub const QI_VALSTOP: QueryItemType = 3;

/// `QueryOperand` (ts_type.h) -- a value (operand) node in a `tsquery`.
///
/// The trailing `length:12, distance:20` C bitfield is stored as a single
/// `uint32` (`len_dist`) with accessors that reproduce the bit layout.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryOperand {
    /// operand or kind of operator (`QI_VAL` here)
    pub type_: QueryItemType,
    /// bitmask of allowed weights (A: 1<<3, B: 1<<2, C: 1<<1, D: 1<<0; 0 = any)
    pub weight: u8,
    /// true if it's a prefix search
    pub prefix: bool,
    /// CRC32 of the operand text (signed, per the C comment)
    pub valcrc: i32,
    /// Packed bitfield word: bits 0..=11 = `length` (operand byte length),
    /// bits 12..=31 = `distance` (offset to the operand text).
    pub len_dist: uint32,
}

impl QueryOperand {
    /// `length:12` -- operand byte length, correlating with `WordEntry`.
    #[inline]
    pub fn length(self) -> uint32 {
        self.len_dist & 0xFFF
    }
    /// `distance:20` -- offset to the operand text.
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

/// `OP_NOT` (ts_type.h) -- unary NOT operator code.
pub const OP_NOT: i8 = 1;
/// `OP_AND` (ts_type.h) -- AND operator code.
pub const OP_AND: i8 = 2;
/// `OP_OR` (ts_type.h) -- OR operator code.
pub const OP_OR: i8 = 3;
/// `OP_PHRASE` (ts_type.h) -- phrase (`<N>`) operator code (highest).
pub const OP_PHRASE: i8 = 4;
/// `OP_COUNT` (ts_type.h) -- number of operator codes.
pub const OP_COUNT: usize = 4;

/// `QueryOperator` (ts_type.h) -- an operator node in a `tsquery`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryOperator {
    /// `QI_OPR` here
    pub type_: QueryItemType,
    /// operator code: `OP_NOT`/`OP_AND`/`OP_OR`/`OP_PHRASE`
    pub oper: i8,
    /// distance between args for `OP_PHRASE`
    pub distance: i16,
    /// offset to the left operand (right operand is `item + 1`,
    /// left operand is `item + item->left`)
    pub left: uint32,
}

/// `QueryItem` (ts_type.h) -- a `union` of a bare type tag, a [`QueryOperator`],
/// and a [`QueryOperand`].  Represented as a `#[repr(C)]` union with the exact
/// size and alignment of the C union (`size = 12`, `align = 4`).  The active
/// variant is selected by the leading `type` byte, which all three variants
/// share at offset 0.
#[repr(C)]
#[derive(Clone, Copy)]
pub union QueryItem {
    pub type_: QueryItemType,
    pub qoperator: QueryOperator,
    pub qoperand: QueryOperand,
}

impl QueryItem {
    /// Read the shared leading `type` tag.
    #[inline]
    pub fn item_type(&self) -> QueryItemType {
        // SAFETY: all three union variants share `type` at offset 0 by the
        // C ABI, so reading it through any variant is always valid.
        unsafe { self.type_ }
    }
}

impl Default for QueryItem {
    fn default() -> Self {
        QueryItem { type_: 0 }
    }
}

impl core::fmt::Debug for QueryItem {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("QueryItem")
            .field("type_", &self.item_type())
            .finish_non_exhaustive()
    }
}

/// `TSQueryData` (ts_type.h) -- the header of a complete `tsquery` datum.
///
/// ```c
/// typedef struct {
///     int32 vl_len_;   /* varlena header */
///     int32 size;      /* number of QueryItems */
///     char  data[FLEXIBLE_ARRAY_MEMBER];
/// } TSQueryData;
/// typedef TSQueryData *TSQuery;
/// ```
/// Layout: `(len)(size)(array of QueryItem)(operands as '\0'-terminated
/// C-strings)`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TSQueryData {
    /// varlena header (do not touch directly!)
    pub vl_len_: i32,
    /// number of QueryItems
    pub size: i32,
    /* char data[FLEXIBLE_ARRAY_MEMBER] follows */
}

/// `HDRSIZETQ` (ts_type.h) -- `VARHDRSZ + sizeof(int32)`, the `tsquery` header
/// size up to the start of the `QueryItem` array.  `VARHDRSZ` (4) is reused
/// from [`crate::heaptuple`].
pub const HDRSIZETQ: usize = crate::heaptuple::VARHDRSZ + core::mem::size_of::<i32>();

/// `TSQUERYOID` (`catalog/pg_type_d.h`) -- the `tsquery` type OID (3615).
/// Defined canonically in [`crate::heaptuple`]; re-exported here (rather than
/// redefined) so that `tsearch::TSQUERYOID` still resolves while both crate-root
/// globs point to the SAME item — a second definition would make the root path
/// `pgrust_pg_ffi::TSQUERYOID` ambiguous (E0432 for downstream importers).
pub use crate::heaptuple::TSQUERYOID;

/// `TSQuerySign` (ts_utils.h) -- a 64-bit signature of a `tsquery`'s operand
/// CRCs, used by the `@>`/`<@` containment operators (`tsq_mcontains`).
pub type TSQuerySign = u64;

/// `TSQS_SIGLEN` (ts_utils.h) -- number of bits in a [`TSQuerySign`].
pub const TSQS_SIGLEN: usize = core::mem::size_of::<TSQuerySign>() * 8;

/// `TSTernaryValue` (ts_utils.h) -- `TS_execute` requires ternary logic to
/// handle NOT with phrase matches.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TSTernaryValue {
    /// definitely no match
    TS_NO = 0,
    /// definitely does match
    TS_YES = 1,
    /// can't verify match for lack of pos data
    TS_MAYBE = 2,
}

/// `ExecPhraseData` (ts_utils.h) -- passed to a `TSExecuteCallback` when lexeme
/// position data is needed for phrase matching.  All fields are initially
/// zeroed by the caller.  `pos` may point directly into a `tsvector`'s
/// `WordEntryPos` storage (`allocated == false`) or be palloc'd workspace.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ExecPhraseData {
    /// number of positions reported
    pub npos: i32,
    /// `pos` points to palloc'd data?
    pub allocated: bool,
    /// positions are where the query is NOT matched
    pub negate: bool,
    /// ordered, non-duplicate lexeme positions
    pub pos: *mut WordEntryPos,
    /// width of match in lexemes, less 1
    pub width: i32,
}

/// `TS_EXEC_EMPTY` (ts_utils.h) -- no flags.
pub const TS_EXEC_EMPTY: uint32 = 0x00;
/// `TS_EXEC_SKIP_NOT` (ts_utils.h) -- evaluate NOT sub-expressions as true
/// (deprecated).
pub const TS_EXEC_SKIP_NOT: uint32 = 0x01;
/// `TS_EXEC_PHRASE_NO_POS` (ts_utils.h) -- allow `OP_PHRASE` to execute lossily
/// when position data is absent.
pub const TS_EXEC_PHRASE_NO_POS: uint32 = 0x02;

/// `TSLexeme` (ts_public.h) -- a single lexeme produced by a text-search
/// dictionary.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TSLexeme {
    pub nvariant: uint16,
    pub flags: uint16,
    pub lexeme: *mut c_char,
}

/* Flag bits that can appear in TSLexeme.flags */
pub const TSL_ADDPOS: c_int = 0x01;
pub const TSL_PREFIX: c_int = 0x02;
pub const TSL_FILTER: c_int = 0x04;

/// `DictSubState` from `tsearch/ts_public.h`. Passed as the 4th lexize argument.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct DictSubState {
    /// in: marks for lexize_info that text end is reached
    pub isend: bool,
    /// out: dict wants next lexeme
    pub getnext: bool,
    /// internal dict state between calls with getnext == true
    pub private_state: *mut core::ffi::c_void,
}

/// `LexDescr` from `tsearch/ts_public.h`. Returned type for `prslextype`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LexDescr {
    pub lexid: c_int,
    pub alias: *mut c_char,
    pub descr: *mut c_char,
}

/// `StopList` from `tsearch/ts_public.h`. Stopword list management.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct StopList {
    pub len: c_int,
    pub stop: *mut *mut c_char,
}

/*
 * Regis (fast regex subset used by ISpell) from tsearch/dicts/regis.h.
 *
 * The C structs use bitfields packed into a uint32 word. We model them as
 * exact-layout repr(C) structs holding the packed word; accessor helpers live
 * in the implementing crate.
 */

pub const RSF_ONEOF: c_int = 1;
pub const RSF_NONEOF: c_int = 2;

/// `RegisNode` from `tsearch/dicts/regis.h`.
///
/// Bitfields (low to high): `type:2`, `len:16`, `unused:14` packed into
/// `type_len_unused`. `data` is a `FLEXIBLE_ARRAY_MEMBER` of `unsigned char`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RegisNode {
    pub type_len_unused: uint32,
    pub next: *mut RegisNode,
    pub data: [core::ffi::c_uchar; 0],
}

/// Size of the `RegisNode` header (offsetof(RegisNode, data)).
pub const RNHDRSZ: usize = core::mem::offset_of!(RegisNode, data);

impl RegisNode {
    /// `type` bitfield (bits 0..=1).
    #[inline]
    pub fn get_type(&self) -> uint32 {
        self.type_len_unused & 0x3
    }

    /// Set the `type` bitfield (bits 0..=1).
    #[inline]
    pub fn set_type(&mut self, value: uint32) {
        self.type_len_unused = (self.type_len_unused & !0x3) | (value & 0x3);
    }

    /// `len` bitfield (bits 2..=17).
    #[inline]
    pub fn get_len(&self) -> uint32 {
        (self.type_len_unused >> 2) & 0xFFFF
    }

    /// Set the `len` bitfield (bits 2..=17).
    #[inline]
    pub fn set_len(&mut self, value: uint32) {
        self.type_len_unused = (self.type_len_unused & !(0xFFFF << 2)) | ((value & 0xFFFF) << 2);
    }
}

/// `Regis` from `tsearch/dicts/regis.h`.
///
/// Bitfields (low to high): `issuffix:1`, `nchar:16`, `unused:15` packed into
/// `issuffix_nchar_unused`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Regis {
    pub node: *mut RegisNode,
    pub issuffix_nchar_unused: uint32,
}

impl Regis {
    /// `issuffix` bitfield (bit 0).
    #[inline]
    pub fn get_issuffix(&self) -> uint32 {
        self.issuffix_nchar_unused & 0x1
    }

    /// Set the `issuffix` bitfield (bit 0).
    #[inline]
    pub fn set_issuffix(&mut self, value: uint32) {
        self.issuffix_nchar_unused = (self.issuffix_nchar_unused & !0x1) | (value & 0x1);
    }

    /// `nchar` bitfield (bits 1..=16).
    #[inline]
    pub fn get_nchar(&self) -> uint32 {
        (self.issuffix_nchar_unused >> 1) & 0xFFFF
    }

    /// Set the `nchar` bitfield (bits 1..=16).
    #[inline]
    pub fn set_nchar(&mut self, value: uint32) {
        self.issuffix_nchar_unused =
            (self.issuffix_nchar_unused & !(0xFFFF << 1)) | ((value & 0xFFFF) << 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn wordentry_layout_matches_postgres_abi() {
        // C: sizeof(WordEntry) == 4, align 4 (single uint32 bitfield word).
        assert_eq!(size_of::<WordEntry>(), 4);
        assert_eq!(align_of::<WordEntry>(), 4);
        let mut w = WordEntry::default();
        w.set_haspos(1);
        w.set_len(MAXSTRLEN);
        w.set_pos(MAXSTRPOS);
        assert_eq!(w.haspos(), 1);
        assert_eq!(w.len(), MAXSTRLEN);
        assert_eq!(w.pos(), MAXSTRPOS);
    }

    #[test]
    fn wordentrypos_helpers_roundtrip() {
        let mut x: WordEntryPos = 0;
        WEP_SETWEIGHT(&mut x, 3);
        WEP_SETPOS(&mut x, 0x3fff);
        assert_eq!(WEP_GETWEIGHT(x), 3);
        assert_eq!(WEP_GETPOS(x), 0x3fff);
    }

    #[test]
    fn posvector_layout_matches_postgres_abi() {
        assert_eq!(size_of::<WordEntryPosVector>(), 2);
        assert_eq!(align_of::<WordEntryPosVector>(), 2);
        assert_eq!(offset_of!(WordEntryPosVector, npos), 0);
        assert_eq!(size_of::<WordEntryPosVector1>(), 4);
    }

    #[test]
    fn tsvectordata_layout_matches_postgres_abi() {
        assert_eq!(size_of::<TSVectorData>(), 8);
        assert_eq!(align_of::<TSVectorData>(), 4);
        assert_eq!(offset_of!(TSVectorData, vl_len_), 0);
        assert_eq!(offset_of!(TSVectorData, size), 4);
        assert_eq!(DATAHDRSIZE, 8);
    }

    #[test]
    fn queryoperand_layout_matches_postgres_abi() {
        assert_eq!(size_of::<QueryOperand>(), 12);
        assert_eq!(align_of::<QueryOperand>(), 4);
        assert_eq!(offset_of!(QueryOperand, type_), 0);
        assert_eq!(offset_of!(QueryOperand, weight), 1);
        assert_eq!(offset_of!(QueryOperand, prefix), 2);
        assert_eq!(offset_of!(QueryOperand, valcrc), 4);
        assert_eq!(offset_of!(QueryOperand, len_dist), 8);
        let mut q = QueryOperand::default();
        q.set_length(0xFFF);
        q.set_distance(0xFFFFF);
        assert_eq!(q.length(), 0xFFF);
        assert_eq!(q.distance(), 0xFFFFF);
    }

    #[test]
    fn queryoperator_layout_matches_postgres_abi() {
        assert_eq!(size_of::<QueryOperator>(), 8);
        assert_eq!(align_of::<QueryOperator>(), 4);
        assert_eq!(offset_of!(QueryOperator, type_), 0);
        assert_eq!(offset_of!(QueryOperator, oper), 1);
        assert_eq!(offset_of!(QueryOperator, distance), 2);
        assert_eq!(offset_of!(QueryOperator, left), 4);
    }

    #[test]
    fn queryitem_layout_matches_postgres_abi() {
        // C union: size 12 (largest variant QueryOperand), align 4.
        assert_eq!(size_of::<QueryItem>(), 12);
        assert_eq!(align_of::<QueryItem>(), 4);
    }

    #[test]
    fn tsquerydata_layout_matches_postgres_abi() {
        assert_eq!(size_of::<TSQueryData>(), 8);
        assert_eq!(align_of::<TSQueryData>(), 4);
        assert_eq!(offset_of!(TSQueryData, vl_len_), 0);
        assert_eq!(offset_of!(TSQueryData, size), 4);
        assert_eq!(HDRSIZETQ, 8);
    }

    #[test]
    fn tslexeme_layout_matches_postgres_abi() {
        assert_eq!(size_of::<TSLexeme>(), 16);
        assert_eq!(align_of::<TSLexeme>(), align_of::<*mut c_char>());
        assert_eq!(offset_of!(TSLexeme, nvariant), 0);
        assert_eq!(offset_of!(TSLexeme, flags), 2);
        assert_eq!(offset_of!(TSLexeme, lexeme), 8);
    }

    #[test]
    fn dictsubstate_layout_matches_postgres_abi() {
        assert_eq!(size_of::<DictSubState>(), 16);
        assert_eq!(align_of::<DictSubState>(), 8);
        assert_eq!(offset_of!(DictSubState, isend), 0);
        assert_eq!(offset_of!(DictSubState, getnext), 1);
        assert_eq!(offset_of!(DictSubState, private_state), 8);
    }

    #[test]
    fn lexdescr_layout_matches_postgres_abi() {
        assert_eq!(size_of::<LexDescr>(), 24);
        assert_eq!(align_of::<LexDescr>(), 8);
        assert_eq!(offset_of!(LexDescr, lexid), 0);
        assert_eq!(offset_of!(LexDescr, alias), 8);
        assert_eq!(offset_of!(LexDescr, descr), 16);
    }

    #[test]
    fn stoplist_layout_matches_postgres_abi() {
        assert_eq!(size_of::<StopList>(), 16);
        assert_eq!(align_of::<StopList>(), 8);
        assert_eq!(offset_of!(StopList, len), 0);
        assert_eq!(offset_of!(StopList, stop), 8);
    }

    #[test]
    fn regisnode_layout_matches_postgres_abi() {
        // uint32 bitfields (4) + 4 padding + pointer (8) = 16; flexible array at 16.
        assert_eq!(size_of::<RegisNode>(), 16);
        assert_eq!(align_of::<RegisNode>(), align_of::<*mut c_char>());
        assert_eq!(offset_of!(RegisNode, type_len_unused), 0);
        assert_eq!(offset_of!(RegisNode, next), 8);
        assert_eq!(offset_of!(RegisNode, data), 16);
        assert_eq!(RNHDRSZ, 16);
    }

    #[test]
    fn regis_layout_matches_postgres_abi() {
        assert_eq!(size_of::<Regis>(), 16);
        assert_eq!(align_of::<Regis>(), align_of::<*mut c_char>());
        assert_eq!(offset_of!(Regis, node), 0);
        assert_eq!(offset_of!(Regis, issuffix_nchar_unused), 8);
    }

    #[test]
    fn regisnode_bitfields_match_postgres_packing() {
        // type:2 (bits 0..=1), len:16 (bits 2..=17), unused:14 (bits 18..=31).
        let mut n = RegisNode {
            type_len_unused: 0,
            next: core::ptr::null_mut(),
            data: [],
        };
        n.set_type(3);
        n.set_len(0xFFFF);
        assert_eq!(n.get_type(), 3);
        assert_eq!(n.get_len(), 0xFFFF);
        // Raw word: type in low 2 bits, len in next 16 bits.
        assert_eq!(n.type_len_unused, 0x3 | (0xFFFF << 2));
        // Fields are independent.
        n.set_type(1);
        assert_eq!(n.get_len(), 0xFFFF);
        n.set_len(0);
        assert_eq!(n.get_type(), 1);
    }

    #[test]
    fn regis_bitfields_match_postgres_packing() {
        // issuffix:1 (bit 0), nchar:16 (bits 1..=16), unused:15 (bits 17..=31).
        let mut r = Regis {
            node: core::ptr::null_mut(),
            issuffix_nchar_unused: 0,
        };
        r.set_issuffix(1);
        r.set_nchar(0xFFFF);
        assert_eq!(r.get_issuffix(), 1);
        assert_eq!(r.get_nchar(), 0xFFFF);
        assert_eq!(r.issuffix_nchar_unused, 0x1 | (0xFFFF << 1));
        r.set_issuffix(0);
        assert_eq!(r.get_nchar(), 0xFFFF);
        r.set_nchar(0);
        assert_eq!(r.get_issuffix(), 0);
    }
}
