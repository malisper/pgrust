//! hstore.h on-disk format: [varlena hdr | u32 size_ | 2*count HEntry | pool].
//! size_ packs HS_FLAG_NEWVERSION (bit 31) | count (low 28 bits). HEntry i
//! packs ISFIRST (bit 31, entry 0 only), ISNULL (bit 30), cumulative pool END
//! offset (low 30). Pairs are stored sorted by (keylen, key) and deduped.

use datum::varlena::{set_varsize_4b, VARHDRSZ};

pub const HENTRY_ISFIRST: u32 = 0x8000_0000;
pub const HENTRY_ISNULL: u32 = 0x4000_0000;
pub const HENTRY_POSMASK: u32 = 0x3FFF_FFFF;

pub const HS_FLAG_NEWVERSION: u32 = 0x8000_0000;
pub const HS_COUNT_MASK: u32 = 0x0FFF_FFFF;

pub const HSTORE_MAX_KEY_LEN: usize = 0x3FFF_FFFF;
pub const HSTORE_MAX_VALUE_LEN: usize = 0x3FFF_FFFF;

const HSHRDSIZE: usize = 8;

#[derive(Clone, Debug)]
pub struct Pair {
    pub key: Vec<u8>,
    pub val: Option<Vec<u8>>,
    // C needfree: only the dedup tiebreak in unique_pairs.
    pub needfree: bool,
}

impl Pair {
    pub fn isnull(&self) -> bool {
        self.val.is_none()
    }
}

// comparePairs (hstore_io.c): (keylen, key) asc; exact tie sorts the
// needfree entry after.
fn compare_pairs(a: &Pair, b: &Pair) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    match a.key.len().cmp(&b.key.len()) {
        Ordering::Equal => match a.key.cmp(&b.key) {
            Ordering::Equal => a.needfree.cmp(&b.needfree),
            other => other,
        },
        other => other,
    }
}

// hstoreUniquePairs: sort, keep the first of each equal-key run.
pub fn unique_pairs(mut pairs: Vec<Pair>) -> Vec<Pair> {
    if pairs.len() < 2 {
        return pairs;
    }
    pairs.sort_by(compare_pairs);
    let mut out: Vec<Pair> = Vec::with_capacity(pairs.len());
    for p in pairs {
        match out.last() {
            Some(last) if last.key == p.key => {}
            _ => out.push(p),
        }
    }
    out
}

// hstorePairs: serialize sorted+unique pairs into a header-ful image.
pub fn build_hstore(pairs: &[Pair]) -> Vec<u8> {
    let count = pairs.len();
    let buflen: usize = pairs
        .iter()
        .map(|p| p.key.len() + p.val.as_ref().map_or(0, |v| v.len()))
        .sum();
    let total = count * 2 * 4 + HSHRDSIZE + buflen;

    let mut img = vec![0u8; total];
    img[..VARHDRSZ].copy_from_slice(&set_varsize_4b(total));

    if count == 0 {
        write_u32(&mut img, 4, HS_FLAG_NEWVERSION);
        return img;
    }
    write_u32(&mut img, 4, (count as u32) | HS_FLAG_NEWVERSION);

    let entries_off = HSHRDSIZE;
    let str_off = HSHRDSIZE + count * 2 * 4;
    let mut pos: u32 = 0;
    for (i, p) in pairs.iter().enumerate() {
        let kstart = str_off + pos as usize;
        img[kstart..kstart + p.key.len()].copy_from_slice(&p.key);
        pos += p.key.len() as u32;
        write_u32(&mut img, entries_off + (2 * i) * 4, pos & HENTRY_POSMASK);
        match &p.val {
            None => write_u32(
                &mut img,
                entries_off + (2 * i + 1) * 4,
                (pos & HENTRY_POSMASK) | HENTRY_ISNULL,
            ),
            Some(v) => {
                let vstart = str_off + pos as usize;
                img[vstart..vstart + v.len()].copy_from_slice(v);
                pos += v.len() as u32;
                write_u32(&mut img, entries_off + (2 * i + 1) * 4, pos & HENTRY_POSMASK);
            }
        }
    }
    let e0 = read_u32(&img, entries_off);
    write_u32(&mut img, entries_off, e0 | HENTRY_ISFIRST);
    img
}

/// Read-only view over an hstore body (VARDATA; header stripped). New-format
/// only — every image this repo produces carries HS_FLAG_NEWVERSION.
pub struct HstoreView<'a> {
    body: &'a [u8],
    count: usize,
}

impl<'a> HstoreView<'a> {
    pub fn from_vardata(body: &'a [u8]) -> Self {
        let count = (read_u32(body, 0) & HS_COUNT_MASK) as usize;
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
    fn off(&self, idx: usize) -> u32 {
        if self.entry(idx) & HENTRY_ISFIRST != 0 {
            0
        } else {
            self.endpos(idx - 1)
        }
    }

    #[inline]
    fn str_at(&self, idx: usize) -> &'a [u8] {
        let base = 4 + self.count * 2 * 4;
        let start = base + self.off(idx) as usize;
        let end = base + self.endpos(idx) as usize;
        &self.body[start..end]
    }

    pub fn key(&self, i: usize) -> &'a [u8] {
        self.str_at(2 * i)
    }

    pub fn val(&self, i: usize) -> &'a [u8] {
        self.str_at(2 * i + 1)
    }

    pub fn val_isnull(&self, i: usize) -> bool {
        self.entry(2 * i + 1) & HENTRY_ISNULL != 0
    }

    pub fn keylen(&self, i: usize) -> usize {
        self.key(i).len()
    }

    pub fn vallen(&self, i: usize) -> usize {
        self.val(i).len()
    }

    pub fn vardata(&self) -> &'a [u8] {
        self.body
    }

    pub fn raw_endpos(&self, idx: usize) -> u32 {
        self.endpos(idx)
    }

    pub fn raw_isnull(&self, idx: usize) -> bool {
        self.entry(idx) & HENTRY_ISNULL != 0
    }

    // HSE_ENDPOS(ent[2*count-1]): pool byte length (hstore_cmp).
    pub fn pool_len(&self) -> usize {
        if self.count == 0 {
            0
        } else {
            self.endpos(2 * self.count - 1) as usize
        }
    }

    pub fn pool_bytes(&self) -> &'a [u8] {
        &self.body[4 + self.count * 2 * 4..]
    }

    pub fn pair(&self, i: usize) -> Pair {
        Pair {
            key: self.key(i).to_vec(),
            val: (!self.val_isnull(i)).then(|| self.val(i).to_vec()),
            needfree: false,
        }
    }

    pub fn to_pairs(&self) -> Vec<Pair> {
        (0..self.count).map(|i| self.pair(i)).collect()
    }
}

// hstoreFindKey: binary search over (keylen, key)-sorted pairs.
pub fn find_key(hs: &HstoreView, mut lowbound: Option<&mut usize>, key: &[u8]) -> Option<usize> {
    let mut lo = lowbound.as_deref().copied().unwrap_or(0);
    let mut hi = hs.count();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let mk = hs.key(mid);
        let diff = match mk.len().cmp(&key.len()) {
            core::cmp::Ordering::Equal => mk.cmp(key),
            other => other,
        };
        match diff {
            core::cmp::Ordering::Equal => {
                if let Some(lb) = lowbound.as_deref_mut() {
                    *lb = mid + 1;
                }
                return Some(mid);
            }
            core::cmp::Ordering::Less => lo = mid + 1,
            core::cmp::Ordering::Greater => hi = mid,
        }
    }
    if let Some(lb) = lowbound.as_deref_mut() {
        *lb = lo;
    }
    None
}

#[inline]
fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline]
fn write_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_ne_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(k: &str, v: Option<&str>) -> Pair {
        Pair { key: k.as_bytes().to_vec(), val: v.map(|s| s.as_bytes().to_vec()), needfree: false }
    }

    #[test]
    fn build_and_view_roundtrip() {
        let pairs = unique_pairs(vec![p("b", Some("2")), p("a", Some("1")), p("cc", None)]);
        let img = build_hstore(&pairs);
        let hs = HstoreView::from_vardata(&img[4..]);
        assert_eq!(hs.count(), 3);
        assert_eq!(hs.key(0), b"a");
        assert_eq!(hs.val(0), b"1");
        assert_eq!(hs.key(1), b"b");
        assert_eq!(hs.key(2), b"cc");
        assert!(hs.val_isnull(2));
    }

    #[test]
    fn dedup_keeps_first_original() {
        // needfree=false sorts before needfree=true on an exact tie.
        let pairs = unique_pairs(vec![
            Pair { key: b"k".to_vec(), val: Some(b"new".to_vec()), needfree: true },
            Pair { key: b"k".to_vec(), val: Some(b"old".to_vec()), needfree: false },
        ]);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].val.as_deref(), Some(b"old".as_slice()));
    }

    #[test]
    fn find_key_binary_search() {
        let pairs = unique_pairs(vec![p("a", None), p("bb", None), p("ccc", None)]);
        let img = build_hstore(&pairs);
        let hs = HstoreView::from_vardata(&img[4..]);
        assert_eq!(find_key(&hs, None, b"bb"), Some(1));
        assert_eq!(find_key(&hs, None, b"zz"), None);
        let mut lb = 0usize;
        assert_eq!(find_key(&hs, Some(&mut lb), b"a"), Some(0));
        assert_eq!(lb, 1);
    }

    #[test]
    fn empty_hstore_layout() {
        let img = build_hstore(&[]);
        assert_eq!(img.len(), 8);
        let hs = HstoreView::from_vardata(&img[4..]);
        assert_eq!(hs.count(), 0);
        assert_eq!(hs.pool_len(), 0);
    }
}
