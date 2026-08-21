//! Ingest normalization + column accumulation.
//!
//! **The detoast-on-ingest law** (charter §7, spec §3): byte-exactness is
//! owed on the DETOASTED, decompressed image — the toast form is a
//! non-surface. Every varlena input is normalized here to its plain payload
//! bytes before anything else sees it, so toasted and inline presentations
//! of the same value converge to identical part bytes (the M3-D pinning
//! test). Arms:
//!
//! - 4B uncompressed → payload borrowed directly;
//! - 1B short header → payload borrowed past the 1-byte header;
//! - 4B inline-compressed → decompressed here: pglz via the pure `pglz`
//!   port; lz4 toast raises the tree's standing typed refusal (built
//!   without lz4 toast, C parity — the same posture as `detoast`/
//!   `heaptoast`);
//! - external/indirect (1B_E tag) → the [`ExternalDetoast`] capability
//!   (installed by M3-H over the real `detoast` seam machinery; COPY — the
//!   only M3 ingest under O-M3-1(a) — feeds fresh inline datums, so the
//!   default [`NoExternalDetoast`] refusal is never reachable from the M3
//!   surface).
//!
//! **[`ColBuffer`]** accumulates one column of one part: datum words for
//! byval classes, an 8-aligned image heap for byref classes (varlena images
//! stored VARLENA-SHAPED — the StrView §7b invariant holds from the
//! accumulation buffer onward), a validity bitset, and the exact chunk
//! stats (constancy, value bytes, oversize count) that feed the election
//! plus the O-10 logical column hash. Granule slices materialize
//! [`EncodeInput`]s for the seal loop at the part's ELECTED grain (SB-10);
//! every ladder grain divides by 64 exactly (1024/64 = 16 … 8192/64 = 128),
//! so per-granule validity is a word-aligned slice of the part bitset at
//! any grain.

use pgrc2_format::abi::EncodeInput;
use pgrc2_format::bank::LogicalColHash;
use pgrc2_format::class::{ColSchema, StorageClass};
use pgrc2_format::geom::{GranuleGrain, OVERSIZE_THRESHOLD};
use pgrc2_format::wire::varlena_header_4b_u;

use crate::{WriteError, WriteResult};

/// One ingest datum, in the safe borrowed shape (M3-H unpacks PG datum
/// arrays into this at the AM boundary; tests build it directly).
#[derive(Debug, Clone, Copy)]
pub enum RawDatum<'a> {
    Null,
    /// Byval classes: the datum word in spec §6.7 extension convention.
    Word(u64),
    /// Byref classes: Fixed = exactly `len` raw bytes; Varlena = a raw
    /// varlena image in ANY toast form (normalized here).
    Bytes(&'a [u8]),
}

/// Capability for external/indirect toast pointers (heap access — a passed
/// capability, never a global). `out` receives the fully detoasted,
/// decompressed PAYLOAD bytes (no varlena header).
pub trait ExternalDetoast {
    fn detoast_external(&mut self, image: &[u8], out: &mut Vec<u8>) -> WriteResult<()>;
}

/// The M3 default: typed refusal (unreachable from COPY ingest, the only
/// M3 DML surface per O-M3-1(a)).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoExternalDetoast;

impl ExternalDetoast for NoExternalDetoast {
    fn detoast_external(&mut self, _image: &[u8], _out: &mut Vec<u8>) -> WriteResult<()> {
        Err(WriteError::Refused {
            what: "external toast pointer without a detoast capability",
        })
    }
}

/// Which arm normalization took (surfaced for the convergence pin test).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarlenaForm {
    Plain4B,
    Short1B,
    CompressedPglz,
    External,
}

const VARATT_MASK_4B: u8 = 0x03;
const VARATT_4B_U: u8 = 0x00;
const VARATT_4B_C: u8 = 0x02;
/// tcinfo top-2-bit compression methods (C toast_compression.h).
const TOAST_PGLZ: u32 = 0;
const TOAST_LZ4: u32 = 1;

/// Normalize a raw varlena image to detoasted, decompressed payload bytes.
/// Returns the borrowed payload when no transformation is needed, else
/// fills `scratch` and returns a slice of it (caller keeps `scratch` alive).
pub fn normalize_varlena<'a>(
    image: &'a [u8],
    ext: &mut dyn ExternalDetoast,
    scratch: &'a mut Vec<u8>,
) -> WriteResult<(&'a [u8], VarlenaForm)> {
    let b0 = *image.first().ok_or(WriteError::Contract {
        detail: "empty varlena image",
    })?;
    if b0 == 0x01 {
        // 1B_E: external / indirect toast pointer.
        scratch.clear();
        ext.detoast_external(image, scratch)?;
        return Ok((scratch.as_slice(), VarlenaForm::External));
    }
    if b0 & 0x01 == 0x01 {
        // 1B short header: total size (incl. header) in bits 1..7.
        let total = ((b0 >> 1) & 0x7F) as usize;
        if total < 1 || total > image.len() {
            return Err(WriteError::Contract {
                detail: "short varlena size out of bounds",
            });
        }
        return Ok((&image[1..total], VarlenaForm::Short1B));
    }
    if image.len() < 4 {
        return Err(WriteError::Contract {
            detail: "varlena image shorter than its header",
        });
    }
    let header = u32::from_le_bytes(image[..4].try_into().expect("len 4"));
    let total = (header >> 2) as usize;
    if total < 4 || total > image.len() {
        return Err(WriteError::Contract {
            detail: "varlena size out of bounds",
        });
    }
    match b0 & VARATT_MASK_4B {
        VARATT_4B_U => Ok((&image[4..total], VarlenaForm::Plain4B)),
        VARATT_4B_C => {
            // 4B compressed: [header][va_tcinfo][compressed bytes].
            if total < 8 {
                return Err(WriteError::Contract {
                    detail: "compressed varlena shorter than tcinfo",
                });
            }
            let tcinfo = u32::from_le_bytes(image[4..8].try_into().expect("len 4"));
            let rawsize = (tcinfo & 0x3FFF_FFFF) as usize;
            let method = tcinfo >> 30;
            match method {
                TOAST_PGLZ => {
                    scratch.clear();
                    scratch.resize(rawsize, 0);
                    let n = pglz::pglz_decompress_slice(&image[8..total], scratch, true)
                        .ok_or(WriteError::Contract {
                            detail: "corrupt pglz toast payload",
                        })?;
                    if n != rawsize {
                        return Err(WriteError::Contract {
                            detail: "pglz toast rawsize mismatch",
                        });
                    }
                    Ok((scratch.as_slice(), VarlenaForm::CompressedPglz))
                }
                TOAST_LZ4 => Err(WriteError::Refused {
                    what: "compression method lz4 not supported",
                }),
                _ => Err(WriteError::Contract {
                    detail: "invalid toast compression method",
                }),
            }
        }
        _ => Err(WriteError::Contract {
            detail: "unrecognized varlena header form",
        }),
    }
}

/// Canonical value-byte width of a word class (spec §18.1).
fn word_canonical_width(class: StorageClass) -> usize {
    match class {
        StorageClass::ByvalWord { width, .. } => width as usize,
        StorageClass::F32 => 4,
        StorageClass::F64 => 8,
        StorageClass::Bool => 1,
        _ => unreachable!("word classes only"),
    }
}

// ---------------------------------------------------------------------------
// SEAL-SPEED-2 D2: the inherited-dictionary side channel
// ---------------------------------------------------------------------------

/// A source dictionary inherited from a foreign file's own structure (D2:
/// parquet dict pages). Entries are the source's payload bytes in SOURCE
/// order — code `i` names entry `i`; nothing here is sorted or deduplicated
/// across sources (that is the seal's merge). Implemented by the ingest
/// driver over its format reader (layering: this crate never sees parquet).
pub trait DictEntries: Send + Sync {
    fn entry_count(&self) -> u32;
    /// Entry payload bytes of `code` (caller contract: `code < entry_count`).
    fn entry(&self, code: u32) -> &[u8];
}

/// Per-row sidecar slot: `(source_idx << 32) | code` for a row decoded off
/// a source dictionary; [`DICT_ROW_PLAIN`] for a row with no code (PLAIN
/// fallback pages — the hybrid law hashes those — and null rows, which the
/// merge skips through validity anyway).
pub const DICT_ROW_PLAIN: u64 = u64::MAX;

/// The inherited-dictionary side channel of one column of one part: the
/// source dictionaries covering the part's rows plus each row's
/// `(source, code)`. VALID only when it covers every row (`rows.len() ==
/// ColBuffer::rows`) — a part spliced from any chunk without the channel
/// drops it whole (deterministic: channel presence is a pure function of
/// the input file bytes, never of the claim schedule). Sidecar bytes are
/// measurement-invisible: nothing here ever reaches part bytes except
/// through the seal's canonical-form dict build, which is proven
/// byte-identical to the rebuild path (the D2 twin gates).
pub struct DictSide {
    pub sources: Vec<std::sync::Arc<dyn DictEntries>>,
    /// Row-dense, one slot per part row.
    pub rows: Vec<u64>,
}

/// One column of one part, accumulated (whole-part buffering — the design
/// forced by global dictionaries + per-stream elections; see crate docs).
pub struct ColBuffer {
    pub schema: ColSchema,
    /// Byval classes: datum words. Byref classes: heap offsets of the image
    /// start (varlena: the 4-B header). Null rows: 0.
    words: Vec<u64>,
    /// Byref image heap, entries 8-aligned; varlena entries varlena-shaped.
    heap: Vec<u8>,
    /// Part-global validity bitset, LSB-first words.
    validity: Vec<u64>,
    has_null: bool,
    rows: u64,
    nonnull: u64,
    value_bytes: u64,
    oversize_values: u64,
    /// Canonical payload bytes of oversize values only (SB-10 grain pricing:
    /// oversize varlenas route to the overflow stream at encode, so the
    /// granule byte bound prices them at their 16-B inline OverflowRef stub,
    /// not their payload — this accumulator subtracts them out).
    oversize_bytes: u64,
    constant: bool,
    first_canon: Option<Vec<u8>>,
    hash: LogicalColHash,
    /// D2 side channel (None = no inherited structure; the rebuild path).
    dict_side: Option<DictSide>,
}

impl ColBuffer {
    pub fn new(schema: ColSchema) -> ColBuffer {
        ColBuffer {
            schema,
            words: Vec::new(),
            heap: Vec::new(),
            validity: Vec::new(),
            has_null: false,
            rows: 0,
            nonnull: 0,
            value_bytes: 0,
            oversize_values: 0,
            oversize_bytes: 0,
            constant: true,
            first_canon: None,
            hash: LogicalColHash::new(),
            dict_side: None,
        }
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn has_null(&self) -> bool {
        self.has_null
    }

    /// Approximate resident bytes (the part-cut byte budget input).
    pub fn approx_bytes(&self) -> u64 {
        (self.words.len() * 8 + self.heap.len() + self.validity.len() * 8) as u64
    }

    /// Heap bytes buffered so far — split out of [`ColBuffer::approx_bytes`]
    /// for the parallel cut cursor (#597). The heap is the ONE
    /// `approx_bytes` component whose part-accumulated length is not the
    /// sum of chunk-local lengths: [`ColBuffer::splice_chunk`] pads the
    /// accumulated heap to 8 at each interior chunk seam, exactly as serial
    /// ingest pads before the seam's first entry. The cursor therefore
    /// accumulates each column's heap through that same pad-to-8-then-add
    /// arithmetic (words and validity, being seam-invariant, sum flat).
    pub fn heap_len(&self) -> u64 {
        self.heap.len() as u64
    }

    /// The O-10 logical column identity accumulated so far.
    pub fn logical_hash(&self) -> &LogicalColHash {
        &self.hash
    }

    /// Attach the D2 side channel for rows appended so far. `rows` must be
    /// row-dense over the CURRENT row count (attach happens after a chunk's
    /// appends, before any splice into a part accumulator).
    pub fn attach_dict_side(
        &mut self,
        sources: Vec<std::sync::Arc<dyn DictEntries>>,
        rows: Vec<u64>,
    ) -> WriteResult<()> {
        if rows.len() as u64 != self.rows {
            return Err(WriteError::Contract {
                detail: "dict side channel not row-dense",
            });
        }
        self.dict_side = Some(DictSide { sources, rows });
        Ok(())
    }

    /// The D2 side channel, iff it covers EVERY row of the part (the
    /// coverage law — partial coverage is dropped at splice time).
    pub fn dict_side(&self) -> Option<&DictSide> {
        match &self.dict_side {
            Some(s) if s.rows.len() as u64 == self.rows => Some(s),
            _ => None,
        }
    }

    fn push_validity(&mut self, valid: bool) {
        let bit = self.rows as usize;
        let word = bit / 64;
        if word >= self.validity.len() {
            self.validity.push(0);
        }
        if valid {
            self.validity[word] |= 1 << (bit % 64);
        } else {
            self.has_null = true;
        }
    }

    fn note_canon(&mut self, canon: &[u8]) {
        self.hash.observe(canon);
        self.value_bytes += canon.len() as u64;
        match &self.first_canon {
            None => self.first_canon = Some(canon.to_vec()),
            Some(first) => {
                if self.constant && first.as_slice() != canon {
                    self.constant = false;
                }
            }
        }
    }

    pub fn append_null(&mut self) {
        self.push_validity(false);
        self.words.push(0);
        self.hash.observe_null();
        self.rows += 1;
    }

    /// Byval word (datum extension convention per spec §6.7). Bool datums
    /// normalize to 0/1.
    pub fn append_word(&mut self, w: u64) -> WriteResult<()> {
        let w = match self.schema.class {
            StorageClass::Bool => (w != 0) as u64,
            StorageClass::ByvalWord { .. } | StorageClass::F32 | StorageClass::F64 => w,
            _ => {
                return Err(WriteError::Contract {
                    detail: "Word datum on byref class",
                })
            }
        };
        self.push_validity(true);
        let width = word_canonical_width(self.schema.class);
        let canon = w.to_le_bytes();
        self.note_canon(&canon[..width]);
        self.words.push(w);
        self.nonnull += 1;
        self.rows += 1;
        Ok(())
    }

    /// Fixed(N) image, exactly N bytes.
    pub fn append_fixed(&mut self, image: &[u8]) -> WriteResult<()> {
        let StorageClass::Fixed { len } = self.schema.class else {
            return Err(WriteError::Contract {
                detail: "fixed append on non-fixed class",
            });
        };
        if image.len() != len as usize {
            return Err(WriteError::Contract {
                detail: "fixed image length mismatch",
            });
        }
        self.push_validity(true);
        pad8(&mut self.heap);
        let off = self.heap.len() as u64;
        self.heap.extend_from_slice(image);
        self.note_canon(image);
        self.words.push(off);
        self.nonnull += 1;
        self.rows += 1;
        Ok(())
    }

    /// Normalized varlena PAYLOAD bytes (header re-added here so the stored
    /// heap entry is varlena-shaped — StrView §7b).
    pub fn append_varlena_payload(&mut self, payload: &[u8]) -> WriteResult<()> {
        if self.schema.class != StorageClass::VarlenaVerbatim {
            return Err(WriteError::Contract {
                detail: "varlena append on non-varlena class",
            });
        }
        self.push_validity(true);
        pad8(&mut self.heap);
        let off = self.heap.len() as u64;
        self.heap
            .extend_from_slice(&varlena_header_4b_u(payload.len() as u32).to_le_bytes());
        self.heap.extend_from_slice(payload);
        self.note_canon(payload);
        if payload.len() as u32 >= OVERSIZE_THRESHOLD {
            self.oversize_values += 1;
            self.oversize_bytes += payload.len() as u64;
        }
        self.words.push(off);
        self.nonnull += 1;
        self.rows += 1;
        Ok(())
    }

    /// Exact whole-part election stats (analyze-then-elect input).
    pub fn stream_stats(&self) -> crate::elect::StreamStats {
        crate::elect::StreamStats {
            class: self.schema.class,
            rows: self.rows,
            nonnull: self.nonnull,
            constant: self.constant,
            value_bytes: self.value_bytes,
            oversize_values: self.oversize_values,
        }
    }

    /// The SB-10 grain-election byte operand for this stream: exact
    /// whole-part canonical value bytes with every OVERSIZE value priced at
    /// its 16-B inline OverflowRef stub (the existing overflow law — those
    /// payloads land in the overflow stream, not the granule).
    pub fn granule_pricing_bytes(&self) -> u64 {
        self.value_bytes - self.oversize_bytes + self.oversize_values * 16
    }

    /// The WHOLE part's row-dense datum currency in ONE materialization
    /// (SEAL-FUSION): byval classes borrow the staged words outright (the
    /// datum word array IS the row-dense currency — zero copy, the old
    /// per-granule copy walk disappears); byref classes materialize heap
    /// pointers in a single pass. Granule inputs at EVERY grain are slices
    /// of this one array (granule g at grain G covers rows
    /// `[g·G, g·G + rows_in_granule)`), which is byte-for-byte what
    /// [`ColBuffer::granule_ptrs`] materialized per granule — the elected-
    /// grain and default-grain slicings both cover the identical sequence,
    /// so the seal's double materialization (#1b) is structural history.
    pub fn part_ptrs(&self) -> std::borrow::Cow<'_, [u64]> {
        let byref = matches!(
            self.schema.class,
            StorageClass::Fixed { .. } | StorageClass::VarlenaVerbatim
        );
        if !byref {
            return std::borrow::Cow::Borrowed(&self.words);
        }
        let mut out: Vec<u64> = Vec::with_capacity(self.rows as usize);
        let base = self.heap.as_ptr() as u64;
        for r in 0..self.rows as usize {
            let w = self.words[r];
            if self.is_valid(r) {
                out.push(base + w);
            } else {
                out.push(w);
            }
        }
        std::borrow::Cow::Owned(out)
    }

    /// Materialize granule `g`'s datum pointers into `ptrs` and return the
    /// `EncodeInput` facts. Granule slicing honors the part's elected
    /// `grain` (SB-10). Byref pointers reference `self.heap`, which is
    /// stable for the borrow's lifetime (no appends during seal).
    pub fn granule_ptrs(&self, g: u32, grain: GranuleGrain, ptrs: &mut Vec<u64>) -> u32 {
        let start = g as u64 * grain.rows() as u64;
        let rows_g = pgrc2_format::geom::rows_in_granule_at(self.rows, grain, g);
        ptrs.clear();
        let byref = matches!(
            self.schema.class,
            StorageClass::Fixed { .. } | StorageClass::VarlenaVerbatim
        );
        for r in 0..rows_g as u64 {
            let w = self.words[(start + r) as usize];
            if byref && self.is_valid((start + r) as usize) {
                ptrs.push(self.heap.as_ptr() as u64 + w);
            } else {
                ptrs.push(w);
            }
        }
        rows_g
    }

    fn is_valid(&self, row: usize) -> bool {
        let word = row / 64;
        word < self.validity.len() && (self.validity[word] >> (row % 64)) & 1 == 1
    }

    /// Row validity (cluster-sort comparator input; IN-1).
    pub fn valid_at(&self, row: u64) -> bool {
        self.is_valid(row as usize)
    }

    /// Row `row`'s datum word (byval classes: the value in extension
    /// convention; byref classes: the heap offset). Comparator input.
    pub fn word_at(&self, row: u64) -> u64 {
        self.words[row as usize]
    }

    /// Row `row`'s Fixed(N) image bytes (None = null).
    pub fn fixed_at(&self, row: u64) -> WriteResult<Option<&[u8]>> {
        let StorageClass::Fixed { len } = self.schema.class else {
            return Err(WriteError::Contract {
                detail: "fixed_at on non-fixed class",
            });
        };
        if !self.is_valid(row as usize) {
            return Ok(None);
        }
        let off = self.words[row as usize] as usize;
        Ok(Some(&self.heap[off..off + len as usize]))
    }

    /// Rebuild this buffer with rows in `perm` order (the cluster-sort
    /// apply; IN-1). `perm` must be a permutation of `0..rows`; the rebuild
    /// routes every row through the SAME `append_*` calls ingest used, so
    /// the permuted buffer is bit-identical to having ingested the rows in
    /// that order (heap pad-8 placement included). The O-10 logical
    /// multiset hash is order-independent by construction, so the digest is
    /// unchanged — the sort is byte-visible on disk and invisible to the
    /// logical identity, exactly as a sort must be.
    pub fn permuted(&self, perm: &[u32]) -> WriteResult<ColBuffer> {
        if perm.len() as u64 != self.rows {
            return Err(WriteError::Contract {
                detail: "permutation length != row count",
            });
        }
        let mut out = ColBuffer::new(self.schema);
        for &r in perm {
            let r = r as u64;
            if r >= self.rows {
                return Err(WriteError::Contract {
                    detail: "permutation index out of range",
                });
            }
            if !self.is_valid(r as usize) {
                out.append_null();
                continue;
            }
            match self.schema.class {
                StorageClass::ByvalWord { .. }
                | StorageClass::F32
                | StorageClass::F64
                | StorageClass::Bool => out.append_word(self.words[r as usize])?,
                StorageClass::Fixed { .. } => {
                    let img = self.fixed_at(r)?.expect("valid row");
                    out.append_fixed(img)?
                }
                StorageClass::VarlenaVerbatim => {
                    let payload = self.varlena_payload(r)?.expect("valid row");
                    out.append_varlena_payload(payload)?
                }
            }
        }
        // D2 side channel follows the permutation (row-dense slots move
        // with their rows; the source table is order-independent).
        if let Some(side) = self.dict_side() {
            let rows: Vec<u64> = perm.iter().map(|&r| side.rows[r as usize]).collect();
            out.attach_dict_side(side.sources.clone(), rows)?;
        }
        Ok(out)
    }

    /// Upper bound on the decode-arena bytes granule `g` needs at verify
    /// (payload bytes + per-value header/alignment overhead), at the part's
    /// elected `grain`.
    pub fn granule_arena_bound(&self, g: u32, grain: GranuleGrain) -> u64 {
        let rows_g = pgrc2_format::geom::rows_in_granule_at(self.rows, grain, g) as u64;
        match self.schema.class {
            StorageClass::ByvalWord { .. }
            | StorageClass::F32
            | StorageClass::F64
            | StorageClass::Bool => 0,
            StorageClass::Fixed { len } => rows_g * (len as u64 + 8),
            StorageClass::VarlenaVerbatim => {
                let start = g as u64 * grain.rows() as u64;
                // Null slots still cost arena on pointer classes (decoders
                // emit the canonical zero-length placeholder entries) and
                // images align per value — mirror the codec crate's verify
                // envelope (`worst_granule_arena`), never undercount. The
                // old bound skipped null slots entirely and undercounted
                // alignment; the shared grow-only verify arena masked it
                // until a null-carrying single-column seal hit
                // `ArenaExhausted` (found RED by the CMP-A wrapped-part
                // null fixture).
                let mut sum = rows_g * 16;
                for r in start..start + rows_g {
                    if self.is_valid(r as usize) {
                        let off = self.words[r as usize] as usize;
                        let header =
                            u32::from_le_bytes(self.heap[off..off + 4].try_into().expect("len 4"));
                        // `header >> 2` = payload + the 4-byte header.
                        sum += ((header >> 2) as u64).div_ceil(8) * 8 + 8;
                    }
                }
                sum
            }
        }
    }

    /// Row `row`'s stored varlena PAYLOAD bytes (None = null). The shred
    /// lane derivation reads parent documents through this.
    pub fn varlena_payload(&self, row: u64) -> WriteResult<Option<&[u8]>> {
        if self.schema.class != StorageClass::VarlenaVerbatim {
            return Err(WriteError::Contract {
                detail: "varlena_payload on non-varlena class",
            });
        }
        if row >= self.rows {
            return Err(WriteError::Contract {
                detail: "varlena_payload row out of range",
            });
        }
        if !self.is_valid(row as usize) {
            return Ok(None);
        }
        let off = self.words[row as usize] as usize;
        let (_, payload) =
            pgrc2_format::wire::varlena_entry_at(&self.heap, off, "col heap entry")
                .map_err(WriteError::Format)?;
        Ok(Some(payload))
    }

    /// Granule `g`'s validity words at the part's elected `grain` — a
    /// word-aligned slice: every ladder grain is a multiple of 1024, so
    /// `grain / 64` is whole (128 at the default 8192 grain).
    pub fn granule_validity(&self, g: u32, grain: GranuleGrain) -> Option<&[u64]> {
        if !self.has_null {
            return None;
        }
        let words_per_granule = (grain.rows() / 64) as usize;
        let rows_g = pgrc2_format::geom::rows_in_granule_at(self.rows, grain, g) as usize;
        let start = g as usize * words_per_granule;
        let words = rows_g.div_ceil(64);
        Some(&self.validity[start..(start + words).min(self.validity.len())])
    }

    /// Splice a chunk-local buffer onto the end of `self` — THE
    /// seam-replayed-tracker mechanism (chunk M3-I; charter
    /// `pgrcolumnar-v2.md` §1 "ordered-commit parallel COPY with
    /// seam-replayed trackers").
    ///
    /// Parallel ingest workers accumulate `chunk`s independently (each built
    /// from row 0 by the same `append_*` calls); the part assembler replays
    /// them IN INPUT ORDER through this splice, which reproduces EXACTLY the
    /// state serial appends would have produced — field by field:
    ///
    /// - `heap`: 8-aligned base + verbatim chunk bytes ≡ serial `pad8`
    ///   placement (entries are padded BEFORE each append, never after, so a
    ///   chunk's relative layout transplants verbatim onto any 8-aligned
    ///   base);
    /// - `words`: byval datums verbatim; byref offsets shifted by the heap
    ///   base for VALID rows (null rows stay 0, as serial pushes them);
    /// - `validity`: whole-word concatenation — legal only because chunk
    ///   seams fall on 64-row boundaries (the caller's contract; enforced);
    /// - constancy: part stays constant iff both sides are constant and
    ///   their first canonical values agree (a value-free side is neutral);
    /// - `hash`: [`LogicalColHash::merge`] — wrapping-add lanes, so replay
    ///   in input order is bit-identical to serial observes;
    /// - counters (`rows`/`nonnull`/`value_bytes`/`oversize_values`/
    ///   `has_null`): sums/or.
    ///
    /// Contract: same schema; `self.rows % 64 == 0` (a partial chunk is only
    /// ever the LAST splice — the final chunk of the final part).
    pub fn splice_chunk(&mut self, chunk: &ColBuffer) -> WriteResult<()> {
        if chunk.schema != self.schema {
            return Err(WriteError::Contract {
                detail: "splice of a foreign-schema chunk",
            });
        }
        if self.rows % 64 != 0 {
            return Err(WriteError::Contract {
                detail: "splice seam off the 64-row validity-word boundary",
            });
        }
        // D2 side-channel merge (before counters move): both sides must
        // cover their rows or the part's channel drops whole. Chunk source
        // ordinals translate into the accumulator's source table (dedup by
        // Arc identity — chunks off one source row group share one Arc).
        // Deterministic: chunks splice in INPUT order, so the part source
        // table is first-use ordered — a pure function of the input.
        self.dict_side = match (self.rows, self.dict_side.take(), &chunk.dict_side) {
            (0, _, Some(cs)) if cs.rows.len() as u64 == chunk.rows => Some(DictSide {
                sources: cs.sources.clone(),
                rows: cs.rows.clone(),
            }),
            (_, Some(mut acc), Some(cs))
                if acc.rows.len() as u64 == self.rows
                    && cs.rows.len() as u64 == chunk.rows =>
            {
                let mut xlat: Vec<u32> = Vec::with_capacity(cs.sources.len());
                for s in &cs.sources {
                    let idx = match acc
                        .sources
                        .iter()
                        .position(|a| std::sync::Arc::ptr_eq(a, s))
                    {
                        Some(i) => i,
                        None => {
                            acc.sources.push(s.clone());
                            acc.sources.len() - 1
                        }
                    };
                    xlat.push(idx as u32);
                }
                acc.rows.reserve(cs.rows.len());
                for &slot in &cs.rows {
                    acc.rows.push(if slot == DICT_ROW_PLAIN {
                        DICT_ROW_PLAIN
                    } else {
                        let src = (slot >> 32) as u32;
                        let code = slot as u32;
                        (u64::from(xlat[src as usize]) << 32) | u64::from(code)
                    });
                }
                Some(acc)
            }
            _ => None,
        };
        let byref = matches!(
            self.schema.class,
            StorageClass::Fixed { .. } | StorageClass::VarlenaVerbatim
        );
        if byref {
            // Serial pads before each ENTRY, never after the last one: a
            // value-free chunk contributes no pad, so only pad when the
            // chunk actually carries heap bytes.
            if !chunk.heap.is_empty() {
                pad8(&mut self.heap);
            }
            let base = self.heap.len() as u64;
            self.heap.extend_from_slice(&chunk.heap);
            self.words.reserve(chunk.words.len());
            for r in 0..chunk.rows as usize {
                if chunk.is_valid(r) {
                    self.words.push(chunk.words[r] + base);
                } else {
                    self.words.push(chunk.words[r]);
                }
            }
        } else {
            self.words.extend_from_slice(&chunk.words);
        }
        self.validity.extend_from_slice(&chunk.validity);
        self.has_null |= chunk.has_null;
        self.rows += chunk.rows;
        self.nonnull += chunk.nonnull;
        self.value_bytes += chunk.value_bytes;
        self.oversize_values += chunk.oversize_values;
        self.oversize_bytes += chunk.oversize_bytes;
        match (&self.first_canon, &chunk.first_canon) {
            (_, None) => {
                // Value-free chunk (all null or empty): constancy-neutral.
            }
            (None, Some(f)) => {
                self.constant = self.constant && chunk.constant;
                self.first_canon = Some(f.clone());
            }
            (Some(a), Some(b)) => {
                self.constant = self.constant && chunk.constant && a == b;
            }
        }
        self.hash.merge(&chunk.hash);
        Ok(())
    }

    /// Build the `EncodeInput` for granule `g` over materialized pointers,
    /// at the part's elected `grain`.
    pub fn encode_input<'a>(
        &'a self,
        g: u32,
        grain: GranuleGrain,
        ptrs: &'a [u64],
        rows_g: u32,
    ) -> EncodeInput<'a> {
        EncodeInput {
            class: self.schema.class,
            rows: rows_g,
            datums: ptrs,
            validity: self.granule_validity(g, grain),
        }
    }
}

fn pad8(buf: &mut Vec<u8>) {
    let rem = buf.len() % 8;
    if rem != 0 {
        buf.resize(buf.len() + (8 - rem), 0);
    }
}
