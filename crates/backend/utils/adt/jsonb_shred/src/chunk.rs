//! In-memory shredded-chunk shapes: typed lanes with validity + per-granule
//! exception masks (memo §2.3/§2.4), and the interned-path-ID bucketed
//! residual (§2.5). These are the value-level contents the C1 writer will
//! feed through the ordinary v9 substream encoders; nothing here knows about
//! storage.

use crate::manifest::ShredManifest;
use crate::path::JsonPath;

/// pgrcolumnar granule geometry (format.rs GRANULE): the default unit the
/// per-granule exception masks are computed over.
pub const GRANULE_ROWS: u32 = 8192;

/// Plain bitmap, LSB-first within u64 words.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Bitmap {
    nbits: u32,
    words: Vec<u64>,
}

impl Bitmap {
    pub fn new(nbits: u32) -> Bitmap {
        Bitmap {
            nbits,
            words: vec![0; nbits.div_ceil(64) as usize],
        }
    }

    pub fn len(&self) -> u32 {
        self.nbits
    }

    pub fn is_empty(&self) -> bool {
        self.nbits == 0
    }

    pub fn set(&mut self, i: u32) {
        debug_assert!(i < self.nbits);
        self.words[(i / 64) as usize] |= 1u64 << (i % 64);
    }

    pub fn get(&self, i: u32) -> bool {
        debug_assert!(i < self.nbits);
        self.words[(i / 64) as usize] >> (i % 64) & 1 != 0
    }

    /// Count of set bits strictly below `i` — the dense-value rank of row
    /// `i` in a validity bitmap.
    pub fn rank(&self, i: u32) -> u32 {
        debug_assert!(i <= self.nbits);
        let full = (i / 64) as usize;
        let mut n: u32 = self.words[..full].iter().map(|w| w.count_ones()).sum();
        if i % 64 != 0 {
            n += (self.words[full] & ((1u64 << (i % 64)) - 1)).count_ones();
        }
        n
    }

    pub fn count(&self) -> u32 {
        self.words.iter().map(|w| w.count_ones()).sum()
    }
}

/// Dense typed values, one entry per set validity bit, in row order
/// (memo §2.3's lane table). Text values are varlena text images (4-byte
/// header + bytes) so each slice is datum-shaped for the future fill path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaneValues {
    Text {
        /// offsets.len() == count + 1; value i = bytes[offsets[i]..offsets[i+1]].
        offsets: Vec<u32>,
        bytes: Vec<u8>,
    },
    Uuid16 {
        vals: Vec<[u8; 16]>,
    },
    NumericFs {
        /// Chunk-level shared scale (A6b): elected from the first numeric
        /// occurrence's dscale; values that don't `fixed_scale_fit` at it
        /// are exceptions.
        scale: i32,
        packed: Vec<i64>,
    },
    Bool {
        /// Dense bit i = i-th valid row's value.
        bits: Bitmap,
    },
}

impl LaneValues {
    pub fn count(&self) -> u32 {
        match self {
            LaneValues::Text { offsets, .. } => (offsets.len() - 1) as u32,
            LaneValues::Uuid16 { vals } => vals.len() as u32,
            LaneValues::NumericFs { packed, .. } => packed.len() as u32,
            LaneValues::Bool { bits } => bits.len(),
        }
    }

    /// Text value at dense rank (the varlena image slice).
    pub fn text_at(&self, rank: u32) -> &[u8] {
        let LaneValues::Text { offsets, bytes } = self else {
            panic!("not a text lane");
        };
        &bytes[offsets[rank as usize] as usize..offsets[rank as usize + 1] as usize]
    }
}

/// One elected path's shredded content (parallel to `manifest.elected`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypedLane {
    /// Bit r set = the typed lane holds row r's value. Absent-path rows and
    /// exception rows are both 0 — the granule exception mask is what
    /// distinguishes "validity is the whole truth" from "probe the residual"
    /// (memo §2.4).
    pub validity: Bitmap,
    /// Bit g set = granule g has at least one exception row for this path.
    pub exceptions: Bitmap,
    pub values: LaneValues,
}

/// One residual bucket: three parallel streams in the A9 sizes+elements
/// shape, positionally aligned with the chunk's rows (memo §2.5).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ResidualBucket {
    /// Entries per row; sizes.len() == nrows.
    pub sizes: Vec<u32>,
    /// Prefix sums of sizes (len nrows + 1) — derived, kept for the probe.
    pub row_starts: Vec<u32>,
    /// Flattened path IDs, ascending per row (Sinew's sorted-ID header: the
    /// per-row probe is a binary search).
    pub path_ids: Vec<u32>,
    /// Flattened verbatim jsonb value images: entry k =
    /// val_bytes[val_offsets[k]..val_offsets[k+1]], a root-level jsonb image
    /// of the value (scalars raw-scalar-wrapped, subtrees verbatim windows —
    /// `->` on a residual hit returns it without re-serialization).
    pub val_offsets: Vec<u32>,
    pub val_bytes: Vec<u8>,
}

impl ResidualBucket {
    pub(crate) fn new() -> ResidualBucket {
        ResidualBucket {
            sizes: Vec::new(),
            row_starts: vec![0],
            path_ids: Vec::new(),
            val_offsets: vec![0],
            val_bytes: Vec::new(),
        }
    }

    /// The row's (path_id, image) entries.
    pub fn row_entries(&self, row: u32) -> impl Iterator<Item = (u32, &[u8])> {
        let lo = self.row_starts[row as usize] as usize;
        let hi = self.row_starts[row as usize + 1] as usize;
        (lo..hi).map(move |k| {
            (
                self.path_ids[k],
                &self.val_bytes
                    [self.val_offsets[k] as usize..self.val_offsets[k + 1] as usize],
            )
        })
    }

    fn get(&self, row: u32, path_id: u32) -> Option<&[u8]> {
        let lo = self.row_starts[row as usize] as usize;
        let hi = self.row_starts[row as usize + 1] as usize;
        let k = lo + self.path_ids[lo..hi].binary_search(&path_id).ok()?;
        Some(&self.val_bytes[self.val_offsets[k] as usize..self.val_offsets[k + 1] as usize])
    }
}

/// One shredded chunk (row batch). Row identity is positional throughout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShredChunk {
    pub nrows: u32,
    /// Geometry the exception masks were computed over.
    pub granule_rows: u32,
    /// Parallel to `manifest.elected`.
    pub lanes: Vec<TypedLane>,
    /// `manifest.nbuckets` buckets.
    pub buckets: Vec<ResidualBucket>,
}

impl ShredChunk {
    pub fn n_granules(&self) -> u32 {
        self.nrows.div_ceil(self.granule_rows).max(1)
    }

    /// Partial residual access (the memo's §2.5 bucketed promise): probing
    /// one path touches exactly ONE bucket's streams — the bucket choice is
    /// a pure function of the path, and the row's entries are ID-sorted so
    /// the probe is a binary search. Returns the stored jsonb image.
    pub fn residual_get<'c>(
        &'c self,
        manifest: &ShredManifest,
        row: u32,
        path: &JsonPath,
    ) -> Option<&'c [u8]> {
        let path_id = manifest.lookup(path)?;
        let bucket = &self.buckets[path.bucket(manifest.nbuckets) as usize];
        bucket.get(row, path_id)
    }
}
