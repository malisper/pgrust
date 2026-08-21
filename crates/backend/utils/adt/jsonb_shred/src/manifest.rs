//! The shred manifest (memo §2.6): the one new decode surface a reader ever
//! learns. Self-contained versioned bytes — the future per-column footer
//! section stores them verbatim; parts stay self-describing (GL-BANKGEOM
//! discipline: verifiable without trusting external state).

use crate::path::{cmp_borrowed, JsonPath};
use core::cmp::Ordering;

pub const MANIFEST_VERSION: u8 = 1;

/// Typed lane classes (memo §2.3). One elected lane per path; occurrences a
/// lane cannot hold byte-exactly are exceptions in the residual — never a
/// second variant column (§2.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Lane {
    /// jsonb strings, verbatim bytes.
    Text = 0,
    /// Hint-gated uuid16: only canonical lowercase-hyphenated forms verify.
    Uuid16 = 1,
    /// A6b fixed-scale numeric: chunk-level shared scale, per-value
    /// `fixed_scale_fit`.
    NumericFs = 2,
    Bool = 3,
}

impl Lane {
    pub fn from_u8(b: u8) -> Option<Lane> {
        match b {
            0 => Some(Lane::Text),
            1 => Some(Lane::Uuid16),
            2 => Some(Lane::NumericFs),
            3 => Some(Lane::Bool),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ElectedPath {
    pub path_id: u32,
    pub lane: Lane,
    /// Position in the part's substream directory (assigned 0..P in elected
    /// order here; the C1 writer owns the real directory mapping).
    pub substream_idx: u16,
    pub hinted: bool,
    /// Presence count observed at election (planner-visible later, §3.6).
    pub presence: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ManifestError {
    Truncated,
    BadVersion(u8),
    BadLane(u8),
    BadPathId(u32),
    DuplicatePath,
    DuplicateElection,
    TrailingBytes,
    ZeroBuckets,
}

/// In-memory manifest. The dictionary is sorted canonically at election and
/// APPEND-ONLY afterwards: later chunks of the same part intern paths the
/// election never saw (path IDs already written into chunk data can never be
/// renumbered), so per-row residual ID order is enforced by an explicit
/// per-row sort at emit rather than assumed from dictionary order — a memo
/// §2.5 delta flagged in the C0 PR.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShredManifest {
    pub version: u8,
    pub nbuckets: u8,
    pub flags: u16,
    /// Walk scoping is manifest state (later chunks must walk exactly as the
    /// election did), not a session option — a §2.6 delta flagged in the PR.
    pub max_depth: u8,
    dict: Vec<JsonPath>,
    /// Dictionary index sorted canonically (rebuilt, never serialized).
    dict_order: Vec<u32>,
    pub elected: Vec<ElectedPath>,
    pub residual_substream_base: u16,
}

impl ShredManifest {
    pub(crate) fn new(nbuckets: u8, max_depth: u8) -> ShredManifest {
        assert!(nbuckets > 0, "residual needs at least one bucket");
        ShredManifest {
            version: MANIFEST_VERSION,
            nbuckets,
            flags: 0,
            max_depth,
            dict: Vec::new(),
            dict_order: Vec::new(),
            elected: Vec::new(),
            residual_substream_base: 0,
        }
    }

    pub fn dict(&self) -> &[JsonPath] {
        &self.dict
    }

    pub fn path(&self, id: u32) -> &JsonPath {
        &self.dict[id as usize]
    }

    pub fn lookup(&self, path: &JsonPath) -> Option<u32> {
        self.dict_order
            .binary_search_by(|&id| self.dict[id as usize].cmp(path))
            .ok()
            .map(|pos| self.dict_order[pos])
    }

    pub(crate) fn lookup_borrowed(&self, segs: &[&[u8]]) -> Option<u32> {
        self.dict_order
            .binary_search_by(|&id| cmp_borrowed_rev(&self.dict[id as usize], segs))
            .ok()
            .map(|pos| self.dict_order[pos])
    }

    /// Intern a path, returning its stable ID. New paths append (the ID is
    /// the dictionary index for the life of the part).
    pub(crate) fn intern(&mut self, path: JsonPath) -> u32 {
        match self
            .dict_order
            .binary_search_by(|&id| self.dict[id as usize].cmp(&path))
        {
            Ok(pos) => self.dict_order[pos],
            Err(pos) => {
                let id = self.dict.len() as u32;
                self.dict.push(path);
                self.dict_order.insert(pos, id);
                id
            }
        }
    }

    pub(crate) fn intern_borrowed(&mut self, segs: &[&[u8]]) -> u32 {
        match self.lookup_borrowed(segs) {
            Some(id) => id,
            None => self.intern(JsonPath::from_borrowed(segs)),
        }
    }

    /// Serialize (little-endian throughout, packed — no alignment).
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.push(self.version);
        out.push(self.nbuckets);
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.push(self.max_depth);
        out.extend_from_slice(&[0u8; 3]); // reserved
        out.extend_from_slice(&(self.dict.len() as u32).to_le_bytes());
        let mut flat = Vec::new();
        for p in &self.dict {
            flat.clear();
            p.encode_into(&mut flat);
            out.extend_from_slice(&(flat.len() as u32).to_le_bytes());
            out.extend_from_slice(&flat);
        }
        out.extend_from_slice(&(self.elected.len() as u32).to_le_bytes());
        for e in &self.elected {
            out.extend_from_slice(&e.path_id.to_le_bytes());
            out.push(e.lane as u8);
            out.extend_from_slice(&e.substream_idx.to_le_bytes());
            out.push(e.hinted as u8);
            out.push(0); // reserved
            out.extend_from_slice(&e.presence.to_le_bytes());
        }
        out.extend_from_slice(&self.residual_substream_base.to_le_bytes());
        out
    }

    /// Deserialize + validate. Refuses trailing bytes, out-of-range IDs,
    /// duplicate dictionary paths and duplicate elections — the manifest is
    /// footer data, drift-checked on read.
    pub fn deserialize(bytes: &[u8]) -> Result<ShredManifest, ManifestError> {
        let mut cur = Cursor { b: bytes };
        let version = cur.u8()?;
        if version != MANIFEST_VERSION {
            return Err(ManifestError::BadVersion(version));
        }
        let nbuckets = cur.u8()?;
        if nbuckets == 0 {
            return Err(ManifestError::ZeroBuckets);
        }
        let flags = cur.u16()?;
        let max_depth = cur.u8()?;
        cur.take(3)?; // reserved
        let ndict = cur.u32()? as usize;
        let mut dict = Vec::with_capacity(ndict.min(1 << 16));
        for _ in 0..ndict {
            let len = cur.u32()? as usize;
            let flat = cur.take(len)?;
            dict.push(JsonPath::decode(flat).ok_or(ManifestError::Truncated)?);
        }
        let mut dict_order: Vec<u32> = (0..dict.len() as u32).collect();
        dict_order.sort_by(|&a, &b| dict[a as usize].cmp(&dict[b as usize]));
        if dict_order
            .windows(2)
            .any(|w| dict[w[0] as usize] == dict[w[1] as usize])
        {
            return Err(ManifestError::DuplicatePath);
        }
        let nelected = cur.u32()? as usize;
        let mut elected = Vec::with_capacity(nelected.min(1 << 16));
        for _ in 0..nelected {
            let path_id = cur.u32()?;
            if path_id as usize >= dict.len() {
                return Err(ManifestError::BadPathId(path_id));
            }
            let lane_b = cur.u8()?;
            let lane = Lane::from_u8(lane_b).ok_or(ManifestError::BadLane(lane_b))?;
            let substream_idx = cur.u16()?;
            let hinted = cur.u8()? != 0;
            cur.u8()?; // reserved
            let presence = cur.u64()?;
            elected.push(ElectedPath {
                path_id,
                lane,
                substream_idx,
                hinted,
                presence,
            });
        }
        {
            let mut ids: Vec<u32> = elected.iter().map(|e| e.path_id).collect();
            ids.sort_unstable();
            if ids.windows(2).any(|w| w[0] == w[1]) {
                return Err(ManifestError::DuplicateElection);
            }
        }
        let residual_substream_base = cur.u16()?;
        if !cur.b.is_empty() {
            return Err(ManifestError::TrailingBytes);
        }
        Ok(ShredManifest {
            version,
            nbuckets,
            flags,
            max_depth,
            dict,
            dict_order,
            elected,
            residual_substream_base,
        })
    }
}

/// `cmp_borrowed` with the manifest's probe direction (owned vs borrowed).
fn cmp_borrowed_rev(a: &JsonPath, b: &[&[u8]]) -> Ordering {
    cmp_borrowed(b, a).reverse()
}

struct Cursor<'a> {
    b: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], ManifestError> {
        if self.b.len() < n {
            return Err(ManifestError::Truncated);
        }
        let (head, tail) = self.b.split_at(n);
        self.b = tail;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, ManifestError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ManifestError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, ManifestError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, ManifestError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}
