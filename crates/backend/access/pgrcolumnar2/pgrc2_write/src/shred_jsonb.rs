//! The adt_jsonb_shred-backed [`ShredLaneSource`] (lanev4 TY-3 writer
//! wiring; OD-4 RULED: two-arm election — JsonbShred where shreddable vs
//! the dict/FSST-eligible text plane).
//!
//! [`JsonbShredSource`] plugs the vendored shred machinery (path election +
//! document walking, `crates/backend/utils/adt/jsonb_shred`) into the seam
//! [`crate::shred::ShredLaneSource`] defines — exactly the slot the v3
//! `NoShred` default left dark. Per registered jsonb column, at seal:
//!
//! 1. Gather the part's NON-NULL document payloads (jsonb container bytes —
//!    the stored form past the varlena header; SQL NULL rows contribute
//!    nothing and re-enter as lane NULLs on expansion).
//! 2. `elect_manifest` under the O-9 reloption budgets
//!    (`pgrc2_shred_max_paths` → `ShredBudgets.max_paths`; comma-separated
//!    `pgrc2_shred_paths` hints ride as Text-lane standing offers). An
//!    EMPTY election = not shreddable (heterogeneous/scalar-root/thin
//!    presence): the column falls to the TEXT PLANE — no lanes, ordinary
//!    verbatim/dict election, no JsonbShred witness.
//! 3. `shred_chunk` → typed lanes; each EXCEPTION-FREE elected lane lands
//!    as a row-aligned [`ShredLane`] `ColBuffer` (dense lane values
//!    expanded over the parent's row space). The exception-freedom seal
//!    law (JSON-routing landing): a lane with ANY exception in the part —
//!    wrong-type occurrences AND jsonb nulls, which the vendored granule
//!    mask conflates — is NOT emitted, so an emitted lane's NULL geometry
//!    is exactly SQL-NULL ∪ absent-path (the `->>`-class extraction
//!    semantics the read supply serves; see `shred` below).
//!
//! The dual-store law (O-4) is untouched: the IMAGE lane (path_ord 0)
//! remains byte-exact and remains the read truth; lanes are additive
//! substreams. The seal marks the parent's election witness `JsonbShred`
//! iff lanes actually derived (`seal.rs` — the witness is the corpus's
//! assertion surface, never faked).
//!
//! ## Reported vocabulary gaps (carried, not invented — chunk-table §4)
//!
//! - Per-granule exception masks and the NumericFs lane SCALE have no
//!   frozen part-level section/slot (spec §6.1/§6.5 gap, same report as
//!   `pgrc2_codec::jsonbshred`): lanes whose values need them still land
//!   byte-faithfully (scale-carrying values verify at encode by the
//!   vendored fixed-scale law), but the scale itself rides only the
//!   in-memory manifest at M3. The image lane keeps reads whole.

use adt_jsonb_shred::{elect_manifest, shred_chunk, JsonPath, Lane, LaneValues, PathHint, TypedLane};
use mcx::MemoryContext;
use pgrc2_format::class::{ColSchema, StorageClass};
use pgrc2_format::geom;
use pgrc2_format::relopt::ShredOptions;
use pgrc2_format::shredlane::ShredLaneKind;

use crate::ingest::ColBuffer;
use crate::par::ShredProvider;
use crate::shred::{ShredLane, ShredLaneSource};
use crate::{WriteError, WriteResult};

/// The production [`ShredLaneSource`]: shreds the REGISTERED jsonb columns
/// (catalog knowledge — the writer cannot derive "jsonb" from `ColSchema`,
/// exactly the `DictPolicy` posture), leaves every other column alone.
#[derive(Debug, Default, Clone)]
pub struct JsonbShredSource {
    columns: Vec<u32>,
}

impl JsonbShredSource {
    pub fn new() -> JsonbShredSource {
        JsonbShredSource::default()
    }

    /// Register `attno` as a jsonb column eligible for the shred arm.
    pub fn with_column(mut self, attno: u32) -> JsonbShredSource {
        self.columns.push(attno);
        self
    }
}

/// The M3-I provider face (the [`crate::par`] seam: mint one shred-lane
/// source per part-assembly task). The source carries only the registered
/// column set, and [`ShredLaneSource::shred`] derives lanes purely from the
/// accumulated parent column at seal time, so a clone-mint per task is
/// EXACTLY the serial source: serial `cut_part` and parallel `seal_one`
/// hand the same per-part column bytes to the same derivation — parts stay
/// byte-identical by construction (the l2.dirsha law). Election order is
/// deterministic end-to-end (`elect_manifest` accumulates in a `BTreeMap`;
/// the shred-side `HashMap` is lookup-only), per the `ShredLaneSource`
/// lane-order contract.
impl ShredProvider for JsonbShredSource {
    fn make(&self) -> Box<dyn ShredLaneSource + Send> {
        Box::new(self.clone())
    }
}

impl ShredLaneSource for JsonbShredSource {
    fn shred(&mut self, parent: &ColBuffer, opts: &ShredOptions) -> WriteResult<Vec<ShredLane>> {
        if !self.columns.contains(&parent.schema.attno)
            || parent.schema.class != StorageClass::VarlenaVerbatim
        {
            return Ok(Vec::new());
        }
        // Non-null document payloads + the rank → parent-row map.
        let mut payloads: Vec<&[u8]> = Vec::new();
        let mut row_of_rank: Vec<u64> = Vec::new();
        for row in 0..parent.rows() {
            if let Some(p) = parent.varlena_payload(row)? {
                payloads.push(p);
                row_of_rank.push(row);
            }
        }
        if payloads.is_empty() {
            // All-null column (jb_allnull): text plane — verbatim MUST.
            return Ok(Vec::new());
        }
        let budgets = pgrc2_codec::jsonbshred::budgets_from_reloptions(opts.max_paths);
        let hints: Vec<PathHint> = opts
            .paths
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| PathHint {
                path: JsonPath::from_dotted(s),
                lane: Lane::Text,
            })
            .collect();
        let mut manifest = elect_manifest(&payloads, &hints, &budgets);
        if manifest.elected.is_empty() {
            // Not shreddable (OD-4 arm 2): the text plane carries it.
            return Ok(Vec::new());
        }
        // Shred. Exception masks are minted at the capacity granule grain;
        // they have no frozen persistence slot at M3 (module doc) and the
        // grain is elected later in the seal, so the capacity grain is the
        // deterministic choice.
        let ctx = MemoryContext::new("pgrc2 shred seal");
        let chunk = match shred_chunk(ctx.mcx(), &mut manifest, &payloads, geom::GRANULE_ROWS) {
            Ok(c) => c,
            // A shred failure must never fail an ingest (the image lane is
            // whole regardless — dual-store law): fall to the text plane.
            Err(_) => return Ok(Vec::new()),
        };
        let mut out = Vec::with_capacity(manifest.elected.len());
        for (li, e) in manifest.elected.iter().enumerate() {
            let lane = &chunk.lanes[li];
            // The EXCEPTION-FREEDOM seal law (JSON-routing landing; the
            // read supply's equivalence theorem): a lane is emitted ONLY
            // when this part holds ZERO exceptions for its path. Emitted
            // lane ⟹ every occurrence at the path is a held typed value ⟹
            // lane NULL = SQL-NULL ∪ absent-path EXACTLY — the text-
            // extraction (`->>`-class) semantics the executor's lane
            // routing serves, provable from the part alone (the exception
            // masks have no persistence slot — the reported §6.5 gap — so
            // freedom-by-construction is the only sound admission).
            // The vendored exception bitmap conflates jsonb-null with
            // wrong-type occurrences at granule grain, so BOTH forfeit the
            // lane: over-refusal (readers fall to the image lane), never
            // unsoundness. Refinement, if real corpora need it: a
            // null/type exception split in the vendored chunk vocabulary.
            if lane.exceptions.count() > 0 {
                continue;
            }
            let col = lane_to_colbuffer(parent, lane, &row_of_rank)?;
            let scale = match &lane.values {
                LaneValues::NumericFs { scale, .. } => Some(*scale),
                _ => None,
            };
            out.push(ShredLane {
                parent_attno: parent.schema.attno,
                path: dotted(manifest.path(e.path_id)),
                col,
                scale,
            });
        }
        Ok(out)
    }
}

/// Render a shred path for the part-level PathTable (spec §6.5 pins only
/// the framing; the string form is the canonical, INJECTIVE encoding both
/// sides speak — [`pgrc2_format::shredlane::encode_shred_path`]). The read
/// side binds lanes by plain string equality over these strings, so the
/// separator (and the escape byte) MUST be escaped inside each key
/// segment: a raw `join(".")` here lets a literal key `"a.b"` collide with
/// the nested chain `a`→`b` on disk (the encoding-confusion aliasing —
/// distinct jsonb structures forging one lane column). Escaping keeps the
/// map injective; dot-free keys are unchanged.
fn dotted(p: &JsonPath) -> String {
    let segs: Vec<std::borrow::Cow<'_, str>> = p
        .segments()
        .iter()
        .map(|s| String::from_utf8_lossy(s))
        .collect();
    pgrc2_format::shredlane::encode_shred_path(segs.iter().map(|s| s.as_ref()))
}

/// The per-lane column descriptor: a typed lane's semantics are its OWN
/// (an int lane shredded out of jsonb is an int — `meta_wire` doc). The
/// mapping itself lives in `pgrc2_format::shredlane` — the ONE authority
/// the read-side lane supply resolves against.
fn lane_schema(attno: u32, values: &LaneValues) -> ColSchema {
    let kind = match values {
        LaneValues::Text { .. } => ShredLaneKind::Text,
        LaneValues::Uuid16 { .. } => ShredLaneKind::Uuid16,
        LaneValues::NumericFs { .. } => ShredLaneKind::NumericFs,
        LaneValues::Bool { .. } => ShredLaneKind::Bool,
    };
    kind.col_schema(attno)
}

/// Expand one dense vendored lane over the parent's row space into a
/// row-aligned [`ColBuffer`]: SQL-NULL parent rows and lane-invalid
/// positions (absent path / jsonb null / exception) become lane NULLs;
/// valid positions append the lane value in its class currency.
fn lane_to_colbuffer(
    parent: &ColBuffer,
    lane: &TypedLane,
    row_of_rank: &[u64],
) -> WriteResult<ColBuffer> {
    if lane.validity.len() as usize != row_of_rank.len() {
        return Err(WriteError::Contract {
            detail: "shred lane validity not aligned with non-null rows",
        });
    }
    let mut col = ColBuffer::new(lane_schema(parent.schema.attno, &lane.values));
    let mut next = 0usize; // next index into row_of_rank
    let mut rank = 0u32; // dense value rank within the lane
    for row in 0..parent.rows() {
        let present = next < row_of_rank.len() && row_of_rank[next] == row;
        if !present {
            col.append_null();
            continue;
        }
        let r = next as u32;
        next += 1;
        if !lane.validity.get(r) {
            col.append_null();
            continue;
        }
        match &lane.values {
            LaneValues::Text { offsets, bytes } => {
                // Entries are datum-shaped varlena images (4-B header +
                // payload — the vendored lane contract).
                let start = offsets[rank as usize] as usize;
                let end = offsets[rank as usize + 1] as usize;
                col.append_varlena_payload(&bytes[start + 4..end])?;
            }
            LaneValues::Uuid16 { vals } => {
                col.append_fixed(&vals[rank as usize])?;
            }
            LaneValues::NumericFs { packed, .. } => {
                col.append_word(packed[rank as usize] as u64)?;
            }
            LaneValues::Bool { bits } => {
                col.append_word(bits.get(rank) as u64)?;
            }
        }
        rank += 1;
    }
    if rank != lane.values.count() {
        return Err(WriteError::Contract {
            detail: "shred lane rank/count mismatch at expansion",
        });
    }
    Ok(col)
}

#[cfg(test)]
mod tests {
    use super::dotted;
    use adt_jsonb_shred::JsonPath;

    /// The PathTable string a literal dotted key seals to can NEVER equal
    /// the string a nested chain seals to (the encoding-confusion fix): the
    /// read side binds lanes by string equality, so distinct jsonb
    /// structures must render to distinct strings. Dot-free keys stay
    /// verbatim so ordinary lanes keep resolving.
    #[test]
    fn dotted_key_never_aliases_nested_path() {
        let literal = dotted(&JsonPath::new(vec![b"a.b".to_vec()])); // key "a.b"
        let nested = dotted(&JsonPath::new(vec![b"a".to_vec(), b"b".to_vec()])); // a -> b
        assert_ne!(literal, nested, "dotted key must not alias the nested path");
        assert_eq!(nested, "a.b", "dot-free chain unchanged");
        assert_eq!(literal, "a\\.b", "separator escaped inside the key");

        // Ordinary keys are byte-for-byte unchanged.
        assert_eq!(dotted(&JsonPath::new(vec![b"name".to_vec()])), "name");
    }
}
