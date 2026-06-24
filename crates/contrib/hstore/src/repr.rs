//! `contrib/hstore/hstore.h` — the on-disk hstore varlena format, byte-for-byte.
//!
//! An hstore varlena image is:
//!
//! ```text
//! [ 4-byte varlena header ][ uint32 size_ ][ 2*count HEntry (u32 each) ][ string pool ]
//! ```
//!
//! `size_` packs `HS_FLAG_NEWVERSION` (bit 31) | the pair count (low 28 bits).
//! Each pair `i` owns key entry `2*i` and value entry `2*i+1`. An `HEntry`
//! packs `ISFIRST` (bit 31, set only on entry 0), `ISNULL` (bit 30, set on a
//! NULL value entry), and the cumulative END offset of the string within the
//! pool (low 30 bits). A string's start offset is the previous entry's end
//! offset (0 for the first entry); its length is the difference. Pairs are
//! stored sorted by (keylen, key bytes) and deduplicated.

use ::datum::varlena::{set_varsize_4b, VARHDRSZ};

pub const HENTRY_ISFIRST: u32 = 0x8000_0000;
pub const HENTRY_ISNULL: u32 = 0x4000_0000;
pub const HENTRY_POSMASK: u32 = 0x3FFF_FFFF;

pub const HS_FLAG_NEWVERSION: u32 = 0x8000_0000;
pub const HS_COUNT_MASK: u32 = 0x0FFF_FFFF;

/// `HSTORE_MAX_KEY_LEN` / `HSTORE_MAX_VALUE_LEN` (hstore.h).
pub const HSTORE_MAX_KEY_LEN: usize = 0x3FFF_FFFF;
pub const HSTORE_MAX_VALUE_LEN: usize = 0x3FFF_FFFF;

/// `HSHRDSIZE = sizeof(HStore)` — the 8-byte header (vl_len_ + size_).
pub const HSHRDSIZE: usize = 8;

/// `CALCDATASIZE(x, lenstr) = x*2*sizeof(HEntry) + HSHRDSIZE + lenstr`.
#[inline]
pub fn calc_data_size(count: usize, lenstr: usize) -> usize {
    count * 2 * 4 + HSHRDSIZE + lenstr
}

/// A decompressed key/value pair (C `Pairs`). `val` is `None` for a SQL NULL.
#[derive(Clone, Debug)]
pub struct Pair {
    pub key: Vec<u8>,
    pub val: Option<Vec<u8>>,
    /// C `needfree`: only matters for the dedup tiebreak in [`unique_pairs`].
    pub needfree: bool,
}

impl Pair {
    pub fn keylen(&self) -> usize {
        self.key.len()
    }
    pub fn vallen(&self) -> usize {
        self.val.as_ref().map_or(0, |v| v.len())
    }
    pub fn isnull(&self) -> bool {
        self.val.is_none()
    }
}

/// `comparePairs` (hstore_io.c) — order by keylen asc, then key bytes; for an
/// exact tie, the `needfree == true` entry sorts AFTER (`Greater`).
fn compare_pairs(a: &Pair, b: &Pair) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    if a.key.len() == b.key.len() {
        match a.key.cmp(&b.key) {
            Ordering::Equal => {
                if a.needfree == b.needfree {
                    Ordering::Equal
                } else if a.needfree {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            other => other,
        }
    } else if a.key.len() > b.key.len() {
        Ordering::Greater
    } else {
        Ordering::Less
    }
}

/// `hstoreUniquePairs(a, l, &buflen)` (hstore_io.c) — sort by (keylen,key),
/// drop later duplicates of equal keys (keeping the first in sort order), and
/// return the surviving pairs plus the total string-pool byte length
/// (`sum(keylen + (isnull?0:vallen))`).
pub fn unique_pairs(mut pairs: Vec<Pair>) -> (Vec<Pair>, usize) {
    if pairs.len() < 2 {
        let buflen = pairs
            .first()
            .map_or(0, |p| p.keylen() + if p.isnull() { 0 } else { p.vallen() });
        return (pairs, buflen);
    }

    pairs.sort_by(compare_pairs);

    let mut out: Vec<Pair> = Vec::with_capacity(pairs.len());
    let mut buflen = 0usize;
    for p in pairs.into_iter() {
        match out.last() {
            Some(last) if last.key.len() == p.key.len() && last.key == p.key => {
                // duplicate key: drop the later one (C frees if needfree).
            }
            _ => {
                buflen += p.keylen() + if p.isnull() { 0 } else { p.vallen() };
                out.push(p);
            }
        }
    }
    (out, buflen)
}

/// `hstorePairs(pairs, pcount, buflen)` (hstore_io.c) — serialize already
/// sorted+unique pairs into a new-format hstore varlena image (header-ful).
pub fn build_hstore(pairs: &[Pair]) -> Vec<u8> {
    let count = pairs.len();
    let buflen: usize = pairs
        .iter()
        .map(|p| p.keylen() + if p.isnull() { 0 } else { p.vallen() })
        .sum();
    let total = calc_data_size(count, buflen);

    let mut img = vec![0u8; total];
    img[..VARHDRSZ].copy_from_slice(&set_varsize_4b(total));

    if count == 0 {
        write_u32(&mut img, 4, HS_FLAG_NEWVERSION);
        return img;
    }

    write_u32(&mut img, 4, (count as u32) | HS_FLAG_NEWVERSION);

    let entries_off = HSHRDSIZE;
    let str_off = HSHRDSIZE + count * 2 * 4;
    let mut pos: u32 = 0; // running end offset within the string pool.

    for (i, p) in pairs.iter().enumerate() {
        let kstart = str_off + pos as usize;
        img[kstart..kstart + p.keylen()].copy_from_slice(&p.key);
        pos += p.keylen() as u32;
        write_u32(&mut img, entries_off + (2 * i) * 4, pos & HENTRY_POSMASK);

        if p.isnull() {
            write_u32(
                &mut img,
                entries_off + (2 * i + 1) * 4,
                (pos & HENTRY_POSMASK) | HENTRY_ISNULL,
            );
        } else {
            let v = p.val.as_ref().unwrap();
            let vstart = str_off + pos as usize;
            img[vstart..vstart + v.len()].copy_from_slice(v);
            pos += v.len() as u32;
            write_u32(&mut img, entries_off + (2 * i + 1) * 4, pos & HENTRY_POSMASK);
        }
    }

    // HS_FINALIZE: OR ISFIRST into entry 0.
    let e0 = read_u32(&img, entries_off);
    write_u32(&mut img, entries_off, e0 | HENTRY_ISFIRST);

    img
}

/// A read-only view over an hstore varlena body (VARDATA — varlena header
/// stripped). New-format only (the only format pgrust produces).
pub struct HstoreView<'a> {
    body: &'a [u8],
    count: usize,
}

impl<'a> HstoreView<'a> {
    pub fn from_vardata(body: &'a [u8]) -> Self {
        let size_ = read_u32(body, 0);
        let count = (size_ & HS_COUNT_MASK) as usize;
        HstoreView { body, count }
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    #[inline]
    fn entry(&self, idx: usize) -> u32 {
        read_u32(self.body, 4 + idx * 4)
    }

    #[inline]
    fn endpos(&self, idx: usize) -> u32 {
        self.entry(idx) & HENTRY_POSMASK
    }

    #[inline]
    fn isfirst(&self, idx: usize) -> bool {
        self.entry(idx) & HENTRY_ISFIRST != 0
    }

    #[inline]
    fn off(&self, idx: usize) -> u32 {
        if self.isfirst(idx) {
            0
        } else {
            self.endpos(idx - 1)
        }
    }

    #[inline]
    fn len_at(&self, idx: usize) -> u32 {
        if self.isfirst(idx) {
            self.endpos(idx)
        } else {
            self.endpos(idx) - self.endpos(idx - 1)
        }
    }

    #[inline]
    fn str_base(&self) -> usize {
        4 + self.count * 2 * 4
    }

    pub fn key(&self, i: usize) -> &'a [u8] {
        let off = self.off(2 * i) as usize;
        let len = self.len_at(2 * i) as usize;
        let base = self.str_base() + off;
        &self.body[base..base + len]
    }

    pub fn val(&self, i: usize) -> &'a [u8] {
        let off = self.off(2 * i + 1) as usize;
        let len = self.len_at(2 * i + 1) as usize;
        let base = self.str_base() + off;
        &self.body[base..base + len]
    }

    pub fn val_isnull(&self, i: usize) -> bool {
        self.entry(2 * i + 1) & HENTRY_ISNULL != 0
    }

    pub fn keylen(&self, i: usize) -> usize {
        self.len_at(2 * i) as usize
    }

    pub fn vallen(&self, i: usize) -> usize {
        self.len_at(2 * i + 1) as usize
    }

    /// `VARDATA(hs)` body (for hashing; length = `VARSIZE-VARHDRSZ`).
    pub fn vardata(&self) -> &'a [u8] {
        self.body
    }

    pub fn raw_endpos(&self, idx: usize) -> u32 {
        self.endpos(idx)
    }

    pub fn raw_isnull(&self, idx: usize) -> bool {
        self.entry(idx) & HENTRY_ISNULL != 0
    }

    /// `HSE_ENDPOS(ent[2*count-1])` — string-pool byte length (for hstore_cmp).
    pub fn pool_len(&self) -> usize {
        if self.count == 0 {
            0
        } else {
            self.endpos(2 * self.count - 1) as usize
        }
    }

    pub fn to_pairs(&self) -> Vec<Pair> {
        (0..self.count)
            .map(|i| Pair {
                key: self.key(i).to_vec(),
                val: if self.val_isnull(i) {
                    None
                } else {
                    Some(self.val(i).to_vec())
                },
                needfree: false,
            })
            .collect()
    }
}

/// `hstoreFindKey(hs, lowbound, key, keylen)` — binary search over the
/// sorted-by-(keylen,key) pairs.
pub fn find_key(hs: &HstoreView, mut lowbound: Option<&mut usize>, key: &[u8]) -> Option<usize> {
    let keylen = key.len();
    let mut stop_low = lowbound.as_deref().copied().unwrap_or(0);
    let mut stop_high = hs.count();

    while stop_low < stop_high {
        let mid = stop_low + (stop_high - stop_low) / 2;
        let mid_keylen = hs.keylen(mid);
        let difference = if mid_keylen == keylen {
            hs.key(mid).cmp(key)
        } else if mid_keylen > keylen {
            core::cmp::Ordering::Greater
        } else {
            core::cmp::Ordering::Less
        };
        match difference {
            core::cmp::Ordering::Equal => {
                if let Some(lb) = lowbound.as_deref_mut() {
                    *lb = mid + 1;
                }
                return Some(mid);
            }
            core::cmp::Ordering::Less => stop_low = mid + 1,
            core::cmp::Ordering::Greater => stop_high = mid,
        }
    }
    if let Some(lb) = lowbound.as_deref_mut() {
        *lb = stop_low;
    }
    None
}

#[inline]
fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

#[inline]
fn write_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_ne_bytes());
}
