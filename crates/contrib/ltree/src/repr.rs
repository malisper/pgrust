//! On-disk varlena layouts for `ltree`, `lquery`, and `ltxtquery`, plus the
//! readers/writers that walk them. These mirror `contrib/ltree/ltree.h` byte
//! for byte (the formats are on-disk-compatible and used by GiST indexes, so
#![allow(dead_code)]

pub const MAXALIGN_BYTES: usize = 8;

#[inline]
pub fn maxalign(len: usize) -> usize {
    (len + (MAXALIGN_BYTES - 1)) & !(MAXALIGN_BYTES - 1)
}

#[inline]
pub fn intalign(len: usize) -> usize {
    (len + 3) & !3
}

pub const VARHDRSZ: usize = 4;

/// `LTREE_MAX_LEVELS` (== PG_UINT16_MAX).
pub const LTREE_MAX_LEVELS: i32 = u16::MAX as i32;
pub const LQUERY_MAX_LEVELS: i32 = u16::MAX as i32;
pub const LTREE_LABEL_MAX_CHARS: i32 = 1000;

// lquery_variant flags
pub const LVAR_ANYEND: u8 = 0x01; // '*' prefix match
pub const LVAR_INCASE: u8 = 0x02; // '@' case-insensitive
pub const LVAR_SUBLEXEME: u8 = 0x04; // '%' word-wise

// lquery_level flags (do not overlap LVAR_*)
pub const LQL_NOT: u16 = 0x10;
pub const LQL_COUNT: u16 = 0x20;

// lquery flag
pub const LQUERY_HASNOT: u16 = 0x01;

// ltxtquery ITEM types
pub const END: i32 = 0;
pub const ERR: i32 = 1;
pub const VAL: i32 = 2;
pub const OPR: i32 = 3;
pub const OPEN: i32 = 4;
pub const CLOSE: i32 = 5;
pub const VALTRUE: i32 = 6;
pub const VALFALSE: i32 = 7;


#[inline]
pub fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes([buf[off], buf[off + 1]])
}
#[inline]
pub fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}
#[inline]
pub fn read_i32(buf: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}
#[inline]
pub fn read_i16(buf: &[u8], off: usize) -> i16 {
    i16::from_ne_bytes([buf[off], buf[off + 1]])
}

#[inline]
pub fn write_u16(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_ne_bytes());
}
#[inline]
pub fn write_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_ne_bytes());
}
#[inline]
pub fn write_i32(buf: &mut [u8], off: usize, v: i32) {
    buf[off..off + 4].copy_from_slice(&v.to_ne_bytes());
}
#[inline]
pub fn write_i16(buf: &mut [u8], off: usize, v: i16) {
    buf[off..off + 2].copy_from_slice(&v.to_ne_bytes());
}

#[inline]
pub fn set_varsize(buf: &mut [u8], len: usize) {
    buf[0..VARHDRSZ].copy_from_slice(&::datum::varlena::set_varsize_4b(len));
}

#[inline]
pub fn varsize(buf: &[u8]) -> usize {
    let word = read_u32(buf, 0);
    #[cfg(target_endian = "big")]
    let len = word & 0x3FFF_FFFF;
    #[cfg(target_endian = "little")]
    let len = (word >> 2) & 0x3FFF_FFFF;
    len as usize
}


pub const LTREE_HDRSIZE: usize = 8;
/// `LEVEL_HDRSIZE = offsetof(ltree_level, name)` = 2 (uint16 len).
pub const LEVEL_HDRSIZE: usize = 2;

pub struct Ltree<'a> {
    pub buf: &'a [u8],
}

impl<'a> Ltree<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Ltree { buf }
    }
    pub fn numlevel(&self) -> usize {
        read_u16(self.buf, 4) as usize
    }
    pub fn levels(&self) -> LevelIter<'a> {
        LevelIter {
            buf: self.buf,
            off: LTREE_HDRSIZE,
            remaining: self.numlevel(),
        }
    }
}

pub struct LevelView<'a> {
    pub name: &'a [u8],
}

pub struct LevelIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

impl<'a> Iterator for LevelIter<'a> {
    type Item = LevelView<'a>;
    fn next(&mut self) -> Option<LevelView<'a>> {
        if self.remaining == 0 {
            return None;
        }
        let len = read_u16(self.buf, self.off) as usize;
        let name_off = self.off + LEVEL_HDRSIZE;
        let name = &self.buf[name_off..name_off + len];
        self.off += maxalign(len + LEVEL_HDRSIZE);
        self.remaining -= 1;
        Some(LevelView { name })
    }
}

pub fn build_ltree(levels: &[&[u8]]) -> Vec<u8> {
    let mut totallen = 0usize;
    for l in levels {
        totallen += maxalign(l.len() + LEVEL_HDRSIZE);
    }
    let total = LTREE_HDRSIZE + totallen;
    let mut buf = vec![0u8; total];
    set_varsize(&mut buf, total);
    write_u16(&mut buf, 4, levels.len() as u16);
    let mut off = LTREE_HDRSIZE;
    for l in levels {
        write_u16(&mut buf, off, l.len() as u16);
        buf[off + LEVEL_HDRSIZE..off + LEVEL_HDRSIZE + l.len()].copy_from_slice(l);
        off += maxalign(l.len() + LEVEL_HDRSIZE);
    }
    buf
}


pub const LQUERY_HDRSIZE: usize = 16;
pub const LQL_HDRSIZE: usize = 16;
pub const LVAR_HDRSIZE: usize = 8;

// lquery_level field offsets within an LQL header:
const LQL_OFF_TOTALLEN: usize = 0;
const LQL_OFF_FLAG: usize = 2;
const LQL_OFF_NUMVAR: usize = 4;
const LQL_OFF_LOW: usize = 6;
const LQL_OFF_HIGH: usize = 8;

// lquery_variant field offsets within an LVAR header:
const LVAR_OFF_VAL: usize = 0;
const LVAR_OFF_LEN: usize = 4;
const LVAR_OFF_FLAG: usize = 6;

pub struct VariantView<'a> {
    pub val: i32,
    pub flag: u8,
    pub name: &'a [u8],
}

pub struct LqlView<'a> {
    buf: &'a [u8],
    /// absolute byte offset of this level's LQL header in `buf`.
    off: usize,
}

impl<'a> LqlView<'a> {
    pub fn totallen(&self) -> usize {
        read_u16(self.buf, self.off + LQL_OFF_TOTALLEN) as usize
    }
    pub fn flag(&self) -> u16 {
        read_u16(self.buf, self.off + LQL_OFF_FLAG)
    }
    pub fn numvar(&self) -> usize {
        read_u16(self.buf, self.off + LQL_OFF_NUMVAR) as usize
    }
    pub fn low(&self) -> u16 {
        read_u16(self.buf, self.off + LQL_OFF_LOW)
    }
    pub fn high(&self) -> u16 {
        read_u16(self.buf, self.off + LQL_OFF_HIGH)
    }
    pub fn variants(&self) -> VariantIter<'a> {
        VariantIter {
            buf: self.buf,
            off: self.off + LQL_HDRSIZE,
            remaining: self.numvar(),
        }
    }
}

pub struct VariantIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

impl<'a> Iterator for VariantIter<'a> {
    type Item = VariantView<'a>;
    fn next(&mut self) -> Option<VariantView<'a>> {
        if self.remaining == 0 {
            return None;
        }
        let val = read_i32(self.buf, self.off + LVAR_OFF_VAL);
        let len = read_u16(self.buf, self.off + LVAR_OFF_LEN) as usize;
        let flag = self.buf[self.off + LVAR_OFF_FLAG];
        let name_off = self.off + LVAR_HDRSIZE;
        let name = &self.buf[name_off..name_off + len];
        // LVAR_NEXT: MAXALIGN(len) + LVAR_HDRSIZE
        self.off += maxalign(len) + LVAR_HDRSIZE;
        self.remaining -= 1;
        Some(VariantView { val, flag, name })
    }
}

pub struct Lquery<'a> {
    pub buf: &'a [u8],
}

impl<'a> Lquery<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Lquery { buf }
    }
    pub fn numlevel(&self) -> usize {
        read_u16(self.buf, 4) as usize
    }
    pub fn firstgood(&self) -> u16 {
        read_u16(self.buf, 6)
    }
    pub fn flag(&self) -> u16 {
        read_u16(self.buf, 8)
    }
    pub fn levels(&self) -> LqlIter<'a> {
        LqlIter {
            buf: self.buf,
            off: LQUERY_HDRSIZE,
            remaining: self.numlevel(),
        }
    }
}

pub struct LqlIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

impl<'a> Iterator for LqlIter<'a> {
    type Item = LqlView<'a>;
    fn next(&mut self) -> Option<LqlView<'a>> {
        if self.remaining == 0 {
            return None;
        }
        let view = LqlView {
            buf: self.buf,
            off: self.off,
        };
        // LQL_NEXT: MAXALIGN(totallen)
        self.off += maxalign(view.totallen());
        self.remaining -= 1;
        Some(view)
    }
}

//
// Storage: (varlena hdr)(int32 size)(array of ITEM)(operand bytes)
// HDRSIZEQT = MAXALIGN(VARHDRSZ + sizeof(int32)) = MAXALIGN(8) = 8.
// ITEM = { int16 type; int16 left; int32 val; uint8 flag; uint8 length;
//          uint16 distance } = 12 bytes (no internal padding: 2+2+4+1+1+2).

pub const HDRSIZEQT: usize = 8;
pub const ITEM_SIZE: usize = 12;

const ITEM_OFF_TYPE: usize = 0;
const ITEM_OFF_LEFT: usize = 2;
const ITEM_OFF_VAL: usize = 4;
const ITEM_OFF_FLAG: usize = 8;
const ITEM_OFF_LENGTH: usize = 9;
const ITEM_OFF_DISTANCE: usize = 10;

pub fn computesize(size: usize, lenofoperand: usize) -> usize {
    HDRSIZEQT + size * ITEM_SIZE + lenofoperand
}

#[derive(Clone, Copy, Debug)]
pub struct Item {
    pub typ: i16,
    pub left: i16,
    pub val: i32,
    pub flag: u8,
    pub length: u8,
    pub distance: u16,
}

pub struct Ltxtquery<'a> {
    pub buf: &'a [u8],
}

impl<'a> Ltxtquery<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Ltxtquery { buf }
    }
    pub fn size(&self) -> usize {
        read_i32(self.buf, 4) as usize
    }
    pub fn item(&self, i: usize) -> Item {
        let off = HDRSIZEQT + i * ITEM_SIZE;
        Item {
            typ: read_i16(self.buf, off + ITEM_OFF_TYPE),
            left: read_i16(self.buf, off + ITEM_OFF_LEFT),
            val: read_i32(self.buf, off + ITEM_OFF_VAL),
            flag: self.buf[off + ITEM_OFF_FLAG],
            length: self.buf[off + ITEM_OFF_LENGTH],
            distance: read_u16(self.buf, off + ITEM_OFF_DISTANCE),
        }
    }
    pub fn operand(&self) -> &'a [u8] {
        let off = HDRSIZEQT + self.size() * ITEM_SIZE;
        &self.buf[off..]
    }
}

pub fn write_item(buf: &mut [u8], i: usize, it: &Item) {
    let off = HDRSIZEQT + i * ITEM_SIZE;
    write_i16(buf, off + ITEM_OFF_TYPE, it.typ);
    write_i16(buf, off + ITEM_OFF_LEFT, it.left);
    write_i32(buf, off + ITEM_OFF_VAL, it.val);
    buf[off + ITEM_OFF_FLAG] = it.flag;
    buf[off + ITEM_OFF_LENGTH] = it.length;
    write_u16(buf, off + ITEM_OFF_DISTANCE, it.distance);
}
