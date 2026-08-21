//! # readahead — the claim-space advisory mapping (M5b; ledger v2-12/v3-22)
//!
//! The cold-readahead EXECUTION leg's pgrc2 half: map (part, staged
//! attnos, upcoming part-granule ordinals) to the stream extents a stage
//! of those granules would fault, and hand them to
//! [`OpenPart::advise_extent_run`] (the S6-landed advisory face: run-
//! merged, residency-skipped, bounds-clamped, WILLNEED through the vfs
//! choke). The DRIVER — who decides which granules are "upcoming" — is
//! the executor's claim-hook ([`lx4_pipe::readahead`]); this module owns
//! only the format-aware mapping, so the survivor-projected claim space
//! (the ledger's "survivor spans ARE the cold-read set") is translated
//! to bytes in exactly one place.
//!
//! Contract (inherited from the advisory face, restated because callers
//! rely on it): ADVISORY ONLY — this function never faults, never
//! errors, never changes what a later read returns. Any error on the
//! mapping path (unreadable stream directory, extent-coverage refusal)
//! degrades to fewer hints, never to a scan-visible effect.
//!
//! Stream-role policy:
//! - **Granule-organized roles** (Values / Validity / Sizes /
//!   ChildValues): advised per upcoming granule — these ARE the cold
//!   staging bytes.
//! - **DictIndex / DictPayload** (stream-organized): advised ONCE per
//!   part when `include_stream_organized` is passed — dict-lane decode
//!   faults them at part entry, and on dict-heavy parts they are the
//!   front of the cold read.
//! - **Overflow**: NEVER advised. Overflow reads are OverflowRef-
//!   directed and sparse; a whole-stream WILLNEED over-fetches by
//!   construction (the same reason the part cache reads it lazily).

use std::collections::BTreeSet;

use pgrc2_read::OpenPart;
use pgrc2_format::part::{ExtentRecord, StreamRole};

/// Advise the extents a stage of `granules` (ascending part-granule
/// ordinals) over the columns in `attnos` would fault. Returns
/// `(hints_issued, bytes_hinted)` — the caller's witness input (the
/// no-silent-no-op law rides through from the advisory face).
pub fn advise_part_granules(
    part: &OpenPart,
    attnos: &[u32],
    granules: &[u32],
    include_stream_organized: bool,
) -> (u64, u64) {
    if granules.is_empty() && !include_stream_organized {
        return (0, 0);
    }
    let Ok(dir) = part.stream_directory() else {
        // Advisory path: an unreadable directory is the REAL read's typed
        // error to report; the hint quietly does nothing.
        return (0, 0);
    };
    // (file_off, len) set: dedupes the same extent reached through
    // several granules AND keeps the advise walk file-ascending, so the
    // face's run-merging sees contiguous extents adjacent.
    let mut ranges: BTreeSet<(u64, u64)> = BTreeSet::new();
    for ps in dir.streams() {
        if !attnos.contains(&ps.entry.attno) {
            continue;
        }
        if ps.granule_organized() {
            let mut last: Option<u32> = None;
            for &g in granules {
                let Ok((i, r)) = ps.extent_for_granule(g) else {
                    // Coverage refusals belong to the real read.
                    continue;
                };
                if last != Some(i) {
                    ranges.insert((r.file_off, r.len));
                    last = Some(i);
                }
            }
        } else if include_stream_organized && ps.role != StreamRole::Overflow {
            for r in &ps.extents {
                ranges.insert((r.file_off, r.len));
            }
        }
    }
    if ranges.is_empty() {
        return (0, 0);
    }
    let recs: Vec<ExtentRecord> = ranges
        .into_iter()
        .map(|(file_off, len)| ExtentRecord {
            file_off,
            len,
            values: 0,
            granule_start: 0,
            granule_count: 0,
            crc: 0,
            flags: 0,
        })
        .collect();
    part.advise_extent_run(&recs)
}
