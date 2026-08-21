//! JSONB_SHRED — the shredded-jsonb lane encodings (spec §4 id 11;
//! vocabulary authority: the vendored `adt_jsonb_shred`, M3-B provenance;
//! ruling record: the charter §7 jsonb row — one elected type per path,
//! exceptions NEVER discriminators, numeric lanes float-banned, NULL
//! trichotomy + error-identity laws, dual-store per O-4).
//!
//! id 11 is a STRUCTURAL election (spec §4 calls it exactly that): no §6.4
//! section header carries it. On disk an elected jsonb column is:
//!
//! - the **image lane**: the root `Values` stream (path_ord 0) under plain
//!   VERBATIM varlena — the dual-store side every M3 read serves from
//!   (reconstruction-read is NOT an M3 deliverable, O-4);
//! - **typed path lanes**: `Values`/`Validity` streams with `path_ord ≠ 0`
//!   (spec §6.1 — chunk substreams, row-aligned), one elected lane type per
//!   path, each under the ordinary encoding its lane class maps to below;
//! - the shred manifest + path table (spec §6.5) tying `path_ord` to paths
//!   (M3-D writes them; the vendored manifest bytes are self-describing).
//!
//! Lane class → stream encoding (this module's adapters):
//!
//! | lane      | storage class      | encoding                             |
//! |-----------|--------------------|--------------------------------------|
//! | Text      | VarlenaVerbatim    | VERBATIM varlena (dict election may  |
//! |           |                    | upgrade it like any text stream)     |
//! | Uuid16    | Fixed(16)          | VERBATIM fixed                       |
//! | NumericFs | ByvalWord8 signed  | BYTE_FOR over mantissas (scale rides |
//! |           |                    | the manifest)                        |
//! | Bool      | Bool               | BOOL_BITMAP                          |
//!
//! **NULL trichotomy** (the law this module's adapters must never blur):
//! SQL NULL = the ROOT stream's validity bit; jsonb `null` at a path counts
//! presence but never occupies a lane (it rides the residual — `->` must
//! distinguish jsonb null from absent); absent path = lane validity 0 with
//! no residual entry. Lane validity plus the per-granule exception masks
//! carry through the vendored `ShredChunk` unchanged — the adapters are
//! representation moves (dense ↔ row-aligned), never semantic ones, and
//! `tests/jsonb_lanes.rs` pins the trichotomy through a full
//! shred → lane-encode → decode → reconstruct loop.
//!
//! **Residual buckets + exception masks**: value-level shapes live in the
//! vendored `ResidualBucket` (A9 sizes+elements form). Their part-level
//! stream-role vocabulary is NOT in the frozen spec (§6.1 has no
//! exception/bucket role); that is an M3-A vocabulary gap this lane REPORTS
//! rather than inventing (chunk-table §4: consumers never side-edit the
//! freeze). At M3 the dual-store image lane makes reads whole regardless.

use adt_jsonb_shred::{Bitmap, LaneValues, ShredBudgets, TypedLane};
use pgrc2_format::abi::EncodeInput;
use pgrc2_format::class::StorageClass;
use pgrc2_format::relopt;
use pgrc2_format::{FormatError, FormatResult};

/// Map the O-9 reloption vocabulary (spec §17) onto the vendored election
/// budgets. `max_paths`/`auto floor` come from the reloptions; depth and
/// bucket count are the vendored defaults (election-time inputs recorded in
/// the manifest, so readers never re-derive them).
pub fn budgets_from_reloptions(shred_max_paths: i32) -> ShredBudgets {
    ShredBudgets {
        max_paths: shred_max_paths.max(0) as usize,
        ..ShredBudgets::default()
    }
}

const _: () = {
    // The reloption default must agree with the frozen vocabulary.
    assert!(relopt::RELOPT_SHRED_MAX_PATHS_DEFAULT == 64);
};

/// One lane in the row-aligned form the §19.6 encode faces consume: datums
/// (one slot per row; null slots zero) + validity mask words.
pub struct RowAlignedLane {
    pub class: StorageClass,
    pub datums: Vec<u64>,
    pub validity: Vec<u64>,
    /// NumericFs lanes: the chunk's shared scale (manifest-recorded).
    pub scale: Option<i32>,
}

impl RowAlignedLane {
    pub fn encode_input(&self, rows: u32) -> EncodeInput<'_> {
        EncodeInput {
            class: self.class,
            rows,
            datums: &self.datums,
            validity: Some(&self.validity),
        }
    }
}

fn bitmap_words(b: &Bitmap) -> Vec<u64> {
    let mut words = vec![0u64; (b.len() as usize).div_ceil(64)];
    for i in 0..b.len() {
        if b.get(i) {
            words[(i / 64) as usize] |= 1 << (i % 64);
        }
    }
    words
}

/// Dense vendored lane → row-aligned encode form. Pointer-class datums
/// alias the vendored lane's own buffers (`Text` entries are datum-shaped
/// varlena images by the vendored contract; `Uuid16` entries are raw
/// 16-byte images), so the returned lane must not outlive `lane`.
pub fn lane_to_row_aligned(lane: &TypedLane, rows: u32) -> FormatResult<RowAlignedLane> {
    if lane.validity.len() != rows {
        return Err(FormatError::EncodeContract {
            detail: "lane validity length",
        });
    }
    let validity = bitmap_words(&lane.validity);
    let mut datums = vec![0u64; rows as usize];
    let mut rank = 0u32;
    let (class, scale) = match &lane.values {
        LaneValues::Text { offsets, bytes } => {
            for r in 0..rows {
                if lane.validity.get(r) {
                    let start = offsets[rank as usize] as usize;
                    datums[r as usize] = bytes[start..].as_ptr() as u64;
                    rank += 1;
                }
            }
            (StorageClass::VarlenaVerbatim, None)
        }
        LaneValues::Uuid16 { vals } => {
            for r in 0..rows {
                if lane.validity.get(r) {
                    datums[r as usize] = vals[rank as usize].as_ptr() as u64;
                    rank += 1;
                }
            }
            (StorageClass::Fixed { len: 16 }, None)
        }
        LaneValues::NumericFs { scale, packed } => {
            for r in 0..rows {
                if lane.validity.get(r) {
                    datums[r as usize] = packed[rank as usize] as u64;
                    rank += 1;
                }
            }
            (
                StorageClass::ByvalWord {
                    width: 8,
                    signed: true,
                },
                Some(*scale),
            )
        }
        LaneValues::Bool { bits } => {
            for r in 0..rows {
                if lane.validity.get(r) {
                    datums[r as usize] = bits.get(rank) as u64;
                    rank += 1;
                }
            }
            (StorageClass::Bool, None)
        }
    };
    if rank != lane.values.count() {
        return Err(FormatError::EncodeContract {
            detail: "lane rank/count mismatch",
        });
    }
    Ok(RowAlignedLane {
        class,
        datums,
        validity,
        scale,
    })
}

/// Row-aligned decoded lane → the dense vendored form (the reconstruction
/// suites' inverse; also the shape a future lane-serving read would build).
/// `datum_bytes(datum)` resolves pointer-class datums (test harnesses pass
/// an arena-backed resolver); exceptions bitmap is carried through
/// unchanged.
pub fn lane_from_row_aligned(
    lane_kind: &LaneValues,
    rows: u32,
    validity_words: &[u64],
    datums: &[u64],
    exceptions: Bitmap,
    datum_bytes: impl Fn(u64) -> FormatResult<Vec<u8>>,
    scale: Option<i32>,
) -> FormatResult<TypedLane> {
    let mut validity = Bitmap::new(rows);
    let valid = |r: u32| {
        validity_words
            .get((r / 64) as usize)
            .is_some_and(|w| w >> (r % 64) & 1 == 1)
    };
    let values = match lane_kind {
        LaneValues::Text { .. } => {
            let mut offsets = vec![0u32];
            let mut bytes = Vec::new();
            for r in 0..rows {
                if valid(r) {
                    validity.set(r);
                    bytes.extend_from_slice(&datum_bytes(datums[r as usize])?);
                    offsets.push(bytes.len() as u32);
                }
            }
            LaneValues::Text { offsets, bytes }
        }
        LaneValues::Uuid16 { .. } => {
            let mut vals = Vec::new();
            for r in 0..rows {
                if valid(r) {
                    validity.set(r);
                    let b = datum_bytes(datums[r as usize])?;
                    let img: [u8; 16] =
                        b.as_slice().try_into().map_err(|_| FormatError::Corrupt {
                            at: "uuid16 lane image",
                        })?;
                    vals.push(img);
                }
            }
            LaneValues::Uuid16 { vals }
        }
        LaneValues::NumericFs { .. } => {
            let scale = scale.ok_or(FormatError::Corrupt {
                at: "numeric lane scale",
            })?;
            let mut packed = Vec::new();
            for r in 0..rows {
                if valid(r) {
                    validity.set(r);
                    packed.push(datums[r as usize] as i64);
                }
            }
            LaneValues::NumericFs { scale, packed }
        }
        LaneValues::Bool { .. } => {
            let mut count = 0u32;
            for r in 0..rows {
                if valid(r) {
                    count += 1;
                }
            }
            let mut bits = Bitmap::new(count);
            let mut rank = 0u32;
            for r in 0..rows {
                if valid(r) {
                    validity.set(r);
                    if datums[r as usize] != 0 {
                        bits.set(rank);
                    }
                    rank += 1;
                }
            }
            LaneValues::Bool { bits }
        }
    };
    Ok(TypedLane {
        validity,
        exceptions,
        values,
    })
}
