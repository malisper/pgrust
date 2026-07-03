use ::mcx::{vec_append_bytes, vec_with_capacity_in, Mcx, PgVec};
use ::types_error::PgResult;

pub const MAXSTRLEN: usize = (1 << 11) - 1;
pub const MAXSTRPOS: usize = (1 << 20) - 1;
pub const MAXENTRYPOS: u32 = 1 << 14;
pub const MAXNUMPOS: usize = 256;

pub type WordEntryPos = u16;

#[inline]
pub fn wep_getweight(x: WordEntryPos) -> u16 {
    x >> 14
}

#[inline]
pub fn wep_getpos(x: WordEntryPos) -> u16 {
    x & 0x3fff
}

#[inline]
pub fn wep_setweight(x: &mut WordEntryPos, w: u16) {
    *x = (w << 14) | (*x & 0x3fff);
}

#[inline]
pub fn wep_setpos(x: &mut WordEntryPos, p: u16) {
    *x = (*x & 0xc000) | (p & 0x3fff);
}

#[inline]
pub fn limitpos(x: u32) -> u16 {
    if x >= MAXENTRYPOS {
        (MAXENTRYPOS - 1) as u16
    } else {
        x as u16
    }
}

#[inline]
pub fn shortalign(x: usize) -> usize {
    (x + 1) & !1
}

// ts_type.h WordEntry: u32 bitfield haspos:1, len:11, pos:20 (LE bit order).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct WordEntry(pub u32);

impl WordEntry {
    #[inline]
    pub fn new(haspos: bool, len: usize, pos: usize) -> WordEntry {
        WordEntry((haspos as u32) | ((len as u32) << 1) | ((pos as u32) << 12))
    }

    #[inline]
    pub fn haspos(self) -> bool {
        self.0 & 1 != 0
    }

    #[inline]
    pub fn len(self) -> usize {
        ((self.0 >> 1) & 0x7ff) as usize
    }

    #[inline]
    pub fn pos(self) -> usize {
        (self.0 >> 12) as usize
    }
}

// A tsvector payload (varlena header stripped): int32 size, WordEntry[size],
// then lexeme storage. Entry `pos` offsets are relative to the storage base.
#[derive(Clone, Copy)]
pub struct TsVec<'a> {
    pub payload: &'a [u8],
}

pub const DATAHDRSIZE: usize = 8;
const PAYLOAD_ENTRIES_OFF: usize = 4;

impl<'a> TsVec<'a> {
    #[inline]
    pub fn size(self) -> usize {
        i32::from_ne_bytes(self.payload[0..4].try_into().unwrap()).max(0) as usize
    }

    #[inline]
    pub fn entry(self, i: usize) -> WordEntry {
        let off = PAYLOAD_ENTRIES_OFF + i * 4;
        WordEntry(u32::from_ne_bytes(self.payload[off..off + 4].try_into().unwrap()))
    }

    #[inline]
    pub fn str_off(self) -> usize {
        PAYLOAD_ENTRIES_OFF + self.size() * 4
    }

    #[inline]
    pub fn strdata(self) -> &'a [u8] {
        &self.payload[self.str_off()..]
    }

    #[inline]
    pub fn lexeme(self, e: WordEntry) -> &'a [u8] {
        let base = self.str_off() + e.pos();
        &self.payload[base..base + e.len()]
    }

    pub fn positions(self, e: WordEntry) -> &'a [WordEntryPos] {
        if !e.haspos() {
            return &[];
        }
        let off = self.str_off() + shortalign(e.pos() + e.len());
        let npos = u16::from_ne_bytes(self.payload[off..off + 2].try_into().unwrap()) as usize;
        let start = off + 2;
        let bytes = &self.payload[start..start + npos * 2];
        debug_assert!(bytes.as_ptr() as usize % 2 == 0);
        // SAFETY: bounds sliced above; the payload base is >=4-aligned (palloc
        // or expanded copy) and `start` keeps SHORTALIGN parity, so the u16
        // view is aligned.
        unsafe { core::slice::from_raw_parts(bytes.as_ptr().cast::<u16>(), npos) }
    }

    // Raw (npos, pos[]) block, count word included (verbatim-copy paths).
    pub fn posblock(self, e: WordEntry) -> &'a [u8] {
        if !e.haspos() {
            return &[];
        }
        let off = self.str_off() + shortalign(e.pos() + e.len());
        let npos = u16::from_ne_bytes(self.payload[off..off + 2].try_into().unwrap()) as usize;
        &self.payload[off..off + 2 + npos * 2]
    }
}

// tsCompareString; prefix=true returns 0 iff b has prefix a.
pub fn ts_compare_string(a: &[u8], b: &[u8], prefix: bool) -> i32 {
    if a.is_empty() {
        if prefix {
            0
        } else if !b.is_empty() {
            -1
        } else {
            0
        }
    } else if b.is_empty() {
        1
    } else {
        let n = a.len().min(b.len());
        let mut cmp = match a[..n].cmp(&b[..n]) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        };
        if prefix {
            if cmp == 0 && a.len() > b.len() {
                cmp = 1;
            }
        } else if cmp == 0 && a.len() != b.len() {
            cmp = if a.len() < b.len() { -1 } else { 1 };
        }
        cmp
    }
}

// Image assembler: entries collected first, storage bytes appended as pushed;
// finish() stamps header + size + entry words + storage contiguously.
pub struct TsVecBuilder<'mcx> {
    entries: PgVec<'mcx, WordEntry>,
    data: PgVec<'mcx, u8>,
}

impl<'mcx> TsVecBuilder<'mcx> {
    pub fn with_capacity(mcx: Mcx<'mcx>, n: usize, datalen: usize) -> PgResult<Self> {
        Ok(TsVecBuilder {
            entries: vec_with_capacity_in(mcx, n)?,
            data: vec_with_capacity_in(mcx, datalen)?,
        })
    }

    #[inline]
    pub fn cur_off(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn nentries(&self) -> usize {
        self.entries.len()
    }

    pub fn push(&mut self, word: &[u8], positions: &[WordEntryPos]) -> PgResult<()> {
        let pos = self.data.len();
        self.entries.push(WordEntry::new(!positions.is_empty(), word.len(), pos));
        vec_append_bytes(&mut self.data, word)?;
        if !positions.is_empty() {
            if self.data.len() & 1 != 0 {
                self.data.push(0);
            }
            vec_append_bytes(&mut self.data, &(positions.len() as u16).to_ne_bytes())?;
            // SAFETY: &[u16] -> &[u8] readonly reinterpret for the bulk copy.
            let bytes = unsafe {
                core::slice::from_raw_parts(positions.as_ptr().cast::<u8>(), positions.len() * 2)
            };
            vec_append_bytes(&mut self.data, bytes)?;
        }
        Ok(())
    }

    // Verbatim lexeme + raw posblock copy (concat/delete paths).
    pub fn push_raw(&mut self, word: &[u8], posblock: &[u8]) -> PgResult<()> {
        let pos = self.data.len();
        self.entries.push(WordEntry::new(!posblock.is_empty(), word.len(), pos));
        vec_append_bytes(&mut self.data, word)?;
        if !posblock.is_empty() {
            if self.data.len() & 1 != 0 {
                self.data.push(0);
            }
            vec_append_bytes(&mut self.data, posblock)?;
        }
        Ok(())
    }

    pub fn strlen(&self) -> usize {
        self.data.len()
    }

    pub fn finish(self, mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
        let total = 4 + 4 + self.entries.len() * 4 + self.data.len();
        let mut img = vec_with_capacity_in(mcx, total)?;
        vec_append_bytes(&mut img, &[0u8; 4])?;
        vec_append_bytes(&mut img, &(self.entries.len() as i32).to_ne_bytes())?;
        // SAFETY: WordEntry is a transparentable u32; readonly byte view.
        let ebytes = unsafe {
            core::slice::from_raw_parts(self.entries.as_ptr().cast::<u8>(), self.entries.len() * 4)
        };
        vec_append_bytes(&mut img, ebytes)?;
        vec_append_bytes(&mut img, &self.data)?;
        Ok(img)
    }
}
