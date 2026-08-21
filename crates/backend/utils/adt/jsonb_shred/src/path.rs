//! Simple paths (memo §2.2): chains of object keys from the document root.
//! The empty path denotes the root value itself — the residual home of
//! non-object root documents.

use adt_jsonb::container::length_compare_jsonb_string;
use core::cmp::Ordering;

/// An owned object-key chain. Segment bytes are the verbatim jsonb key bytes
/// (UTF-8 by jsonb_in, but treated as bytes throughout — key order is
/// length-then-memcmp, never collation).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct JsonPath {
    segs: Vec<Vec<u8>>,
}

impl JsonPath {
    pub fn root() -> JsonPath {
        JsonPath { segs: Vec::new() }
    }

    pub fn new(segs: Vec<Vec<u8>>) -> JsonPath {
        JsonPath { segs }
    }

    /// Convenience for hint construction from dotted names. Only usable for
    /// keys without dots; real key bytes go through [`JsonPath::new`].
    pub fn from_dotted(s: &str) -> JsonPath {
        JsonPath {
            segs: s.split('.').map(|p| p.as_bytes().to_vec()).collect(),
        }
    }

    pub fn from_borrowed(segs: &[&[u8]]) -> JsonPath {
        JsonPath {
            segs: segs.iter().map(|s| s.to_vec()).collect(),
        }
    }

    pub fn is_root(&self) -> bool {
        self.segs.is_empty()
    }

    pub fn depth(&self) -> usize {
        self.segs.len()
    }

    pub fn segments(&self) -> &[Vec<u8>] {
        &self.segs
    }

    /// True iff `self` is a strict prefix of `other` (the election
    /// prefix-freeness cap tests both directions).
    pub fn is_strict_prefix_of(&self, other: &JsonPath) -> bool {
        self.segs.len() < other.segs.len()
            && self
                .segs
                .iter()
                .zip(&other.segs)
                .all(|(a, b)| a == b)
    }

    /// Flat byte encoding for the manifest path dictionary: nseg u16 LE +
    /// per segment (len u32 LE + bytes). Length-prefixed per segment so keys
    /// containing any byte (including separators) stay unambiguous.
    ///
    /// Memo §2.6 delta (flagged in the C0 PR): the memo sketches (len u16,
    /// bytes) dictionary entries; jsonb keys can exceed 65,535 bytes
    /// (JENTRY_OFFLENMASK is the real bound), so segment lengths are u32.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        debug_assert!(self.segs.len() <= u16::MAX as usize);
        out.extend_from_slice(&(self.segs.len() as u16).to_le_bytes());
        for seg in &self.segs {
            out.extend_from_slice(&(seg.len() as u32).to_le_bytes());
            out.extend_from_slice(seg);
        }
    }

    /// Inverse of [`JsonPath::encode_into`] over one entry's bytes; refuses
    /// trailing garbage.
    pub fn decode(bytes: &[u8]) -> Option<JsonPath> {
        let mut cur = bytes;
        let nseg = u16::from_le_bytes(cur.get(..2)?.try_into().ok()?) as usize;
        cur = &cur[2..];
        let mut segs = Vec::with_capacity(nseg);
        for _ in 0..nseg {
            let len = u32::from_le_bytes(cur.get(..4)?.try_into().ok()?) as usize;
            cur = &cur[4..];
            segs.push(cur.get(..len)?.to_vec());
            cur = &cur[len..];
        }
        if !cur.is_empty() {
            return None;
        }
        Some(JsonPath { segs })
    }

    /// Residual bucket assignment (memo §2.5): FNV-1a64 over the segments,
    /// each length-prefixed. Stable across platforms — the hash is format
    /// (versioned by the manifest version byte).
    pub fn bucket(&self, nbuckets: u8) -> u8 {
        debug_assert!(nbuckets > 0);
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        let mut h = FNV_OFFSET;
        let mut eat = |bytes: &[u8]| {
            for &b in bytes {
                h ^= b as u64;
                h = h.wrapping_mul(FNV_PRIME);
            }
        };
        for seg in &self.segs {
            eat(&(seg.len() as u32).to_le_bytes());
            eat(seg);
        }
        (h % nbuckets as u64) as u8
    }
}

/// Canonical path order: segment-wise jsonb key order (length-then-bytes,
/// C's lengthCompareJsonbString), a strict prefix sorting first. This is the
/// dictionary sort order and the election tie-break (memo §2.2 item 2).
impl Ord for JsonPath {
    fn cmp(&self, other: &JsonPath) -> Ordering {
        for (a, b) in self.segs.iter().zip(&other.segs) {
            match length_compare_jsonb_string(a, b) {
                Ordering::Equal => continue,
                ne => return ne,
            }
        }
        self.segs.len().cmp(&other.segs.len())
    }
}

impl PartialOrd for JsonPath {
    fn partial_cmp(&self, other: &JsonPath) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Segment-wise compare of a borrowed path against an owned one (the walker
/// probes the dictionary with borrowed segments; no allocation per probe).
pub(crate) fn cmp_borrowed(a: &[&[u8]], b: &JsonPath) -> Ordering {
    for (x, y) in a.iter().zip(b.segments()) {
        match length_compare_jsonb_string(x, y) {
            Ordering::Equal => continue,
            ne => return ne,
        }
    }
    a.len().cmp(&b.segments().len())
}
