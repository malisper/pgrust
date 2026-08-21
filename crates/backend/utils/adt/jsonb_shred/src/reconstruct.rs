//! Reconstruction (memo §2.7): shredded pieces → the byte-identical original
//! jsonb varlena image. Deliberately built and burned in FIRST (PR #43
//! §D-C1): production reads only use it after the C5 elision flip, but the
//! module ships tested from day one — the flip flips a read-source, not
//! fresh code.
//!
//! Byte-exactness argument: jsonb_in normalization (last-wins dedup +
//! length-then-bytes key sort, uniqueifyJsonbObject) means the ORIGINAL
//! image's key order at every level IS canonical order; rebuilding the same
//! value tree through the same JEntry writer (`convert_to_jsonb`) is
//! deterministic, so tree identity gives image identity. Typed lanes
//! reproduce scalars byte-exactly by their §2.3 encode-time verification
//! (fixed_scale_unpack's canonical-digit contract, uuid_out over
//! verified-canonical forms, verbatim text/bool); residual values are stored
//! as verbatim subtree images and re-expanded.

use crate::chunk::{LaneValues, ShredChunk};
use crate::manifest::ShredManifest;
use crate::path::JsonPath;
use adt_jsonb::build::{convert_to_jsonb, JsonbBuildState, JsonbValue};
use adt_jsonb::container::{
    container_is_array, container_is_object, container_is_scalar, container_size, fill_item,
    get_jsonb_length, get_jsonb_offset, JsonbItem,
};
use adt_jsonb::io::extract_scalar;
use adt_numeric::fixed_scale_unpack;
use adt_uuid::{uuid_out_into, UUID_OUT_LEN};
use mcx::Mcx;
use types_error::PgResult;

/// One row's reconstruction inputs: (path, leaf) in manifest-path terms.
enum Leaf<'x> {
    /// String bytes (typed Text lane, header already stripped).
    Text(&'x [u8]),
    /// Canonical uuid render of a Fixed16 value (== original bytes by the
    /// encode-time verify).
    Uuid([u8; UUID_OUT_LEN]),
    /// Packed fixed-scale numeric (unpacked to the byte-identical image at
    /// emit).
    NumericFs { packed: i64, scale: i32 },
    Bool(bool),
    /// A residual value: a root-level jsonb image (raw-scalar-wrapped scalar
    /// or verbatim subtree window).
    ResidualImage(&'x [u8]),
}

/// Reconstruct every row of the chunk, in row order. `mcx` is build scratch;
/// returned images are plain owned bytes (full 4-byte-header varlena form,
/// exactly `jsonb_in`'s output — short-varlena re-wrapping at tuple
/// formation is a pure function of these bytes and stays the caller's
/// varlena concern).
pub fn reconstruct_rows<'mcx>(
    mcx: Mcx<'mcx>,
    manifest: &ShredManifest,
    chunk: &ShredChunk,
) -> PgResult<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(chunk.nrows as usize);
    // Sequential cursors: rank(r) is a running count in row order.
    let mut cursors = vec![0u32; chunk.lanes.len()];
    let mut entries: Vec<(&JsonPath, Leaf<'_>)> = Vec::new();
    for r in 0..chunk.nrows {
        entries.clear();
        for (li, lane) in chunk.lanes.iter().enumerate() {
            if lane.validity.get(r) {
                let rank = cursors[li];
                cursors[li] += 1;
                entries.push((
                    manifest.path(manifest.elected[li].path_id),
                    lane_leaf(&lane.values, rank),
                ));
            }
        }
        gather_residual(manifest, chunk, r, &mut entries);
        out.push(build_row(mcx, manifest, &mut entries)?);
    }
    Ok(out)
}

/// Random-access single-row reconstruction (lane ranks via bitmap popcount).
pub fn reconstruct_row<'mcx>(
    mcx: Mcx<'mcx>,
    manifest: &ShredManifest,
    chunk: &ShredChunk,
    row: u32,
) -> PgResult<Vec<u8>> {
    assert!(row < chunk.nrows);
    let mut entries: Vec<(&JsonPath, Leaf<'_>)> = Vec::new();
    for (li, lane) in chunk.lanes.iter().enumerate() {
        if lane.validity.get(row) {
            entries.push((
                manifest.path(manifest.elected[li].path_id),
                lane_leaf(&lane.values, lane.validity.rank(row)),
            ));
        }
    }
    gather_residual(manifest, chunk, row, &mut entries);
    build_row(mcx, manifest, &mut entries)
}

fn lane_leaf(values: &LaneValues, rank: u32) -> Leaf<'_> {
    match values {
        LaneValues::Text { .. } => {
            let image = values.text_at(rank);
            Leaf::Text(&image[4..])
        }
        LaneValues::Uuid16 { vals } => {
            let mut buf = [0u8; UUID_OUT_LEN];
            uuid_out_into(&vals[rank as usize], &mut buf);
            Leaf::Uuid(buf)
        }
        LaneValues::NumericFs { scale, packed } => Leaf::NumericFs {
            packed: packed[rank as usize],
            scale: *scale,
        },
        LaneValues::Bool { bits } => Leaf::Bool(bits.get(rank)),
    }
}

fn gather_residual<'x>(
    manifest: &'x ShredManifest,
    chunk: &'x ShredChunk,
    row: u32,
    entries: &mut Vec<(&'x JsonPath, Leaf<'x>)>,
) {
    for bucket in &chunk.buckets {
        for (path_id, image) in bucket.row_entries(row) {
            entries.push((manifest.path(path_id), Leaf::ResidualImage(image)));
        }
    }
}

fn build_row<'mcx>(
    mcx: Mcx<'mcx>,
    _manifest: &ShredManifest,
    entries: &mut Vec<(&JsonPath, Leaf<'_>)>,
) -> PgResult<Vec<u8>> {
    // Non-object root: the whole original document rides the residual at the
    // empty path, verbatim — hand it back.
    if let Some(pos) = entries.iter().position(|(p, _)| p.is_root()) {
        debug_assert_eq!(entries.len(), 1, "root residual is always alone");
        let (_, leaf) = &entries[pos];
        let Leaf::ResidualImage(image) = leaf else {
            unreachable!("root entries are residual by construction")
        };
        return Ok(image.to_vec());
    }

    // Canonical path order; per-row paths are prefix-free, so sibling
    // grouping below is a clean partition.
    entries.sort_by(|a, b| a.0.cmp(b.0));

    let mut state = JsonbBuildState::new(mcx)?;
    state.begin_object(false)?;
    build_level(mcx, &mut state, entries, 0)?;
    let root = state
        .end_object()?
        .expect("balanced begin/end yields the finished tree");
    Ok(convert_to_jsonb(mcx, &root)?[..].to_vec())
}

/// Emit one object level: `entries` all share their first `depth` segments
/// and are sorted canonically. Groups by segment `depth`; a single entry of
/// exactly depth+1 is that key's value, a deeper group is a nested object.
fn build_level<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut JsonbBuildState<'mcx>,
    entries: &[(&JsonPath, Leaf<'_>)],
    depth: usize,
) -> PgResult<()> {
    let mut i = 0;
    while i < entries.len() {
        let seg = &entries[i].0.segments()[depth];
        let mut j = i + 1;
        while j < entries.len() && &entries[j].0.segments()[depth] == seg {
            j += 1;
        }
        state.push_key(mcx::slice_in(mcx, seg)?.leak())?;
        if entries[i].0.depth() == depth + 1 {
            debug_assert_eq!(j, i + 1, "prefix-free per row: a leaf has no descendants");
            emit_leaf(mcx, state, &entries[i].1)?;
        } else {
            state.begin_object(false)?;
            build_level(mcx, state, &entries[i..j], depth + 1)?;
            state.end_object()?;
        }
        i = j;
    }
    Ok(())
}

fn emit_leaf<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut JsonbBuildState<'mcx>,
    leaf: &Leaf<'_>,
) -> PgResult<()> {
    match leaf {
        Leaf::Text(s) => {
            state.push_value(JsonbValue::String(mcx::slice_in(mcx, s)?.leak()));
        }
        Leaf::Uuid(buf) => {
            state.push_value(JsonbValue::String(mcx::slice_in(mcx, &buf[..])?.leak()));
        }
        Leaf::NumericFs { packed, scale } => {
            // Byte-identical to the original embedded image by the A6b
            // fixed-scale round-trip contract (the encode-side fit gate).
            let image = fixed_scale_unpack(*packed, *scale)?;
            state.push_value(JsonbValue::Numeric(
                mcx::slice_in(mcx, image.as_bytes())?.leak(),
            ));
        }
        Leaf::Bool(b) => state.push_value(JsonbValue::Bool(*b)),
        Leaf::ResidualImage(image) => {
            let payload = &image[4..];
            if container_is_scalar(payload) {
                // Raw-scalar wrap: unwrap back to the bare scalar.
                let item = extract_scalar(payload).expect("stored scalar wrap");
                state.push_value(item_to_value(mcx, item)?);
            } else {
                emit_container(mcx, state, payload)?;
            }
        }
    }
    Ok(())
}

/// Re-expand a stored subtree image into build pushes (arrays and nested
/// objects re-enter the writer as values, matching what jsonb_in built).
fn emit_container<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut JsonbBuildState<'mcx>,
    c: &[u8],
) -> PgResult<()> {
    if container_is_object(c) {
        state.begin_object(false)?;
        let n = container_size(c);
        let base_off = 4 + 8 * n;
        for i in 0..n {
            let start = (base_off + get_jsonb_offset(c, i)) as usize;
            let len = get_jsonb_length(c, i) as usize;
            state.push_key(mcx::slice_in(mcx, &c[start..start + len])?.leak())?;
            let vi = i + n;
            match fill_item(c, vi, base_off, get_jsonb_offset(c, vi)) {
                JsonbItem::Binary(child) => emit_container(mcx, state, child)?,
                scalar => state.push_value(item_to_value(mcx, scalar)?),
            }
        }
        state.end_object()?;
    } else {
        debug_assert!(container_is_array(c) && !container_is_scalar(c));
        state.begin_array(false)?;
        let n = container_size(c);
        let base_off = 4 + 4 * n;
        for i in 0..n {
            match fill_item(c, i, base_off, get_jsonb_offset(c, i)) {
                JsonbItem::Binary(child) => emit_container(mcx, state, child)?,
                scalar => state.push_elem(item_to_value(mcx, scalar)?)?,
            }
        }
        state.end_array()?;
    }
    Ok(())
}

fn item_to_value<'mcx>(mcx: Mcx<'mcx>, item: JsonbItem<'_>) -> PgResult<JsonbValue<'mcx>> {
    Ok(match item {
        JsonbItem::Null => JsonbValue::Null,
        JsonbItem::Bool(b) => JsonbValue::Bool(b),
        JsonbItem::String(s) => JsonbValue::String(mcx::slice_in(mcx, s)?.leak()),
        JsonbItem::Numeric(n) => JsonbValue::Numeric(mcx::slice_in(mcx, n)?.leak()),
        _ => unreachable!("fill_item yields only leaves and Binary"),
    })
}
