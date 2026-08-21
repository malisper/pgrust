//! The emit pass (memo §4): rows + manifest → typed lanes + residual. The
//! universal law (§2.3): a typed lane stores a value ONLY when it can
//! reproduce the original jsonb scalar byte-exactly; every conversion
//! verifies at encode, failures become exceptions — never errors, never
//! lossy stores.

use crate::chunk::{Bitmap, LaneValues, ResidualBucket, ShredChunk, TypedLane};
use crate::manifest::{Lane, ShredManifest};
use crate::path::JsonPath;
use crate::walk::{walk_row, PathValue};
use adt_jsonb::build::item_to_jsonb_image;
use adt_jsonb::container::JsonbItem;
use adt_numeric::{fixed_scale_fit, Num};
use adt_uuid::{uuid_in, uuid_out_into, PgUuid, UUID_OUT_LEN};
use mcx::Mcx;
use std::collections::HashMap;
use types_error::{PgResult, SoftErrorContext};

/// Electable NumericFs chunk-scale cap (C1 storage contract, C2 memo §7 ask
/// 3): the pgrcolumnar consumer stores the elected scale in the u8
/// `ChunkHeader.numeric_scale`, so a scale a u8 cannot carry must never be
/// elected. Values whose dscale exceeds the cap are ordinary exceptions
/// (byte-exact through the residual); a chunk whose FIRST numeric occurrence
/// exceeds it simply elects from the next electable occurrence instead.
pub const FS_ELECT_SCALE_MAX: i32 = u8::MAX as i32;

/// Per-lane in-progress builder.
struct LaneBuilder {
    lane: Lane,
    validity: Bitmap,
    exceptions: Bitmap,
    // Dense value storage; one representation live per lane class.
    text_offsets: Vec<u32>,
    text_bytes: Vec<u8>,
    uuids: Vec<[u8; 16]>,
    num_scale: Option<i32>,
    num_packed: Vec<i64>,
    bool_bits: Vec<bool>,
}

impl LaneBuilder {
    fn new(lane: Lane, nrows: u32, ngranules: u32) -> LaneBuilder {
        LaneBuilder {
            lane,
            validity: Bitmap::new(nrows),
            exceptions: Bitmap::new(ngranules),
            text_offsets: vec![0],
            text_bytes: Vec::new(),
            uuids: Vec::new(),
            num_scale: None,
            num_packed: Vec::new(),
            bool_bits: Vec::new(),
        }
    }

    /// Try to hold `item` in the typed lane; true = held (the caller sets
    /// validity), false = exception. Verification per memo §2.3's table.
    fn try_encode(&mut self, item: &JsonbItem<'_>) -> bool {
        match (self.lane, *item) {
            (Lane::Text, JsonbItem::String(s)) => {
                // Datum-shaped: a varlena text image per value.
                let total = 4 + s.len();
                self.text_bytes
                    .extend_from_slice(&((total as u32) << 2).to_ne_bytes());
                self.text_bytes.extend_from_slice(s);
                self.text_offsets.push(self.text_bytes.len() as u32);
                true
            }
            (Lane::Uuid16, JsonbItem::String(s)) => match verify_canonical_uuid(s) {
                Some(u) => {
                    self.uuids.push(u);
                    true
                }
                None => false,
            },
            (Lane::NumericFs, JsonbItem::Numeric(image)) => {
                let num = Num::from_payload(&image[4..]);
                // Chunk-scale election: the first ELECTABLE numeric
                // occurrence's dscale (the fixed_scale_elect discipline);
                // every value — including the first — fits at it or is an
                // exception. Electability cap (C1 storage contract, C2 memo
                // §7 ask 3): the elected scale must fit the storage
                // consumer's u8 `ChunkHeader.numeric_scale`, so scale
                // election SKIPS values with dscale > FS_ELECT_SCALE_MAX —
                // they are exceptions (residual round-trip, still
                // byte-exact) and must never become the chunk scale. jsonb
                // dscale is NUMERIC_DSCALE_MAX-bounded (14-bit), so
                // adversarial inputs reach this legitimately.
                if num.dscale() > FS_ELECT_SCALE_MAX && self.num_scale.is_none() {
                    return false;
                }
                let scale = *self.num_scale.get_or_insert_with(|| num.dscale());
                match fixed_scale_fit(num, scale) {
                    Some(v) => {
                        self.num_packed.push(v);
                        true
                    }
                    None => false,
                }
            }
            (Lane::Bool, JsonbItem::Bool(b)) => {
                self.bool_bits.push(b);
                true
            }
            // Wrong scalar class — and jsonb null always (the validity bit
            // cannot carry three states; `->` must still find the null).
            _ => false,
        }
    }

    fn finish(self) -> TypedLane {
        let values = match self.lane {
            Lane::Text => LaneValues::Text {
                offsets: self.text_offsets,
                bytes: self.text_bytes,
            },
            Lane::Uuid16 => LaneValues::Uuid16 { vals: self.uuids },
            Lane::NumericFs => LaneValues::NumericFs {
                scale: self.num_scale.unwrap_or(0),
                packed: self.num_packed,
            },
            Lane::Bool => {
                let mut bits = Bitmap::new(self.bool_bits.len() as u32);
                for (i, b) in self.bool_bits.iter().enumerate() {
                    if *b {
                        bits.set(i as u32);
                    }
                }
                LaneValues::Bool { bits }
            }
        };
        TypedLane {
            validity: self.validity,
            exceptions: self.exceptions,
            values,
        }
    }
}

/// The §2.3 uuid verify predicate: `uuid_out(uuid_in(s)) == s`, i.e. only
/// canonical lowercase-hyphenated forms enter the Fixed16 lane
/// (uppercase/braced/oddly-hyphenated forms are exceptions — reconstruction
/// must reprint the original bytes, and `uuid_out` has one output form).
fn verify_canonical_uuid(s: &[u8]) -> Option<PgUuid> {
    if s.len() != UUID_OUT_LEN {
        return None;
    }
    let mut soft = SoftErrorContext::new(false);
    let uuid = uuid_in(s, Some(&mut soft)).ok()?;
    if soft.error_occurred() {
        return None;
    }
    let mut buf = [0u8; UUID_OUT_LEN];
    uuid_out_into(&uuid, &mut buf);
    (&buf[..] == s).then_some(uuid)
}

/// One collected walk position (borrowed path segments + value).
enum RowPos<'a> {
    Scalar(Vec<&'a [u8]>, JsonbItem<'a>),
    Subtree(Vec<&'a [u8]>, &'a [u8]),
    Root,
}

/// Shred one chunk of rows (container payloads, varlena headers stripped)
/// against the manifest. Granule geometry is a parameter so tests can pin
/// mask boundaries; production callers pass [`crate::chunk::GRANULE_ROWS`].
///
/// The manifest is mutable for one reason only: residual paths the election
/// never saw (later chunks of the part) intern new dictionary IDs —
/// append-only; elections never change. `mcx` is scratch for raw-scalar
/// residual wrapping (arena bytes are copied out; the chunk owns its data).
pub fn shred_chunk<'mcx, 'r>(
    mcx: Mcx<'mcx>,
    manifest: &mut ShredManifest,
    rows: &[&'r [u8]],
    granule_rows: u32,
) -> PgResult<ShredChunk> {
    assert!(granule_rows > 0);
    let nrows = rows.len() as u32;
    let ngranules = nrows.div_ceil(granule_rows).max(1);

    let mut lanes: Vec<LaneBuilder> = manifest
        .elected
        .iter()
        .map(|e| LaneBuilder::new(e.lane, nrows, ngranules))
        .collect();
    // Elected lookup by path_id (dictionary IDs are stable).
    let elected_of_path: HashMap<u32, usize> = manifest
        .elected
        .iter()
        .enumerate()
        .map(|(i, e)| (e.path_id, i))
        .collect();

    let mut buckets: Vec<ResidualBucket> = (0..manifest.nbuckets)
        .map(|_| ResidualBucket::new())
        .collect();

    let mut positions: Vec<RowPos<'r>> = Vec::new();
    // Per-row residual staging: (bucket, path_id, image bytes).
    let mut staged: Vec<(u8, u32, Vec<u8>)> = Vec::new();

    for (r, payload) in rows.iter().enumerate() {
        let r = r as u32;
        let g = r / granule_rows;

        positions.clear();
        {
            // Recursion stops at elected object positions (§2.4: a
            // non-scalar value at an elected path is ONE whole exception,
            // never a scatter of descendants).
            let mut stop_at = |segs: &[&'r [u8]]| {
                manifest
                    .lookup_borrowed(segs)
                    .is_some_and(|id| elected_of_path.contains_key(&id))
            };
            let mut collect = |segs: &[&'r [u8]], v: PathValue<'r>| {
                positions.push(match v {
                    PathValue::Scalar(item) => RowPos::Scalar(segs.to_vec(), item),
                    PathValue::Subtree(c) => RowPos::Subtree(segs.to_vec(), c),
                    PathValue::Root => RowPos::Root,
                });
            };
            // SAFETY-free lifetime note: `positions` borrows `payload` only
            // within this row's iteration; it is cleared before reuse.
            walk_row(payload, manifest.max_depth, &mut stop_at, &mut collect);
        }

        staged.clear();
        for pos in positions.drain(..) {
            match pos {
                RowPos::Scalar(segs, item) => {
                    let id = manifest.intern_borrowed(&segs);
                    if let Some(&li) = elected_of_path.get(&id) {
                        if lanes[li].try_encode(&item) {
                            lanes[li].validity.set(r);
                            continue;
                        }
                        // Exception: present but not held by the lane.
                        lanes[li].exceptions.set(g);
                    }
                    let image = item_to_jsonb_image(mcx, item)?;
                    staged.push((
                        manifest.path(id).bucket(manifest.nbuckets),
                        id,
                        image[..].to_vec(),
                    ));
                }
                RowPos::Subtree(segs, c) => {
                    let id = manifest.intern_borrowed(&segs);
                    if let Some(&li) = elected_of_path.get(&id) {
                        lanes[li].exceptions.set(g);
                    }
                    let image = item_to_jsonb_image(mcx, JsonbItem::Binary(c))?;
                    staged.push((
                        manifest.path(id).bucket(manifest.nbuckets),
                        id,
                        image[..].to_vec(),
                    ));
                }
                RowPos::Root => {
                    // Whole-document image, verbatim (reconstruction hands
                    // it back untouched).
                    let root = JsonPath::root();
                    let id = manifest.intern(root.clone());
                    let mut img = Vec::with_capacity(4 + payload.len());
                    img.extend_from_slice(&(((4 + payload.len()) as u32) << 2).to_ne_bytes());
                    img.extend_from_slice(payload);
                    staged.push((root.bucket(manifest.nbuckets), id, img));
                }
            }
        }

        // Per-row, per-bucket, ID-ascending entries (explicit sort: late-
        // interned IDs are append-ordered, not canonical — manifest.rs note).
        staged.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        let mut cursor = 0usize;
        for (b, bucket) in buckets.iter_mut().enumerate() {
            let start = cursor;
            while cursor < staged.len() && staged[cursor].0 as usize == b {
                let (_, id, image) = &staged[cursor];
                bucket.path_ids.push(*id);
                bucket.val_bytes.extend_from_slice(image);
                bucket.val_offsets.push(bucket.val_bytes.len() as u32);
                cursor += 1;
            }
            bucket.sizes.push((cursor - start) as u32);
            bucket.row_starts.push(bucket.path_ids.len() as u32);
        }
        debug_assert_eq!(cursor, staged.len());
    }

    Ok(ShredChunk {
        nrows,
        granule_rows,
        lanes: lanes.into_iter().map(LaneBuilder::finish).collect(),
        buckets,
    })
}
