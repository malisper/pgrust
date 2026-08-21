// PROVENANCE (O-1 vendoring, lane M3-B): vendored VERBATIM from
// origin/appbench-types @ 4000b2794c773909aa985d87e4d38ee842cffaab — donor tests carried;
// no semantic edits during the move (adaptations are separate commits).

//! jsonb shredding core — Phase C0 of the pgrcolumnar jsonb shredding design
//! (docs/design/pgrcolumnar-jsonb-shredding.md, PR #43): the PURE shredder +
//! reconstruction pair, purely functional over jsonb datum bytes. No storage,
//! no pgrcolumnar contact, no operator changes — the C1 writer and C2 reader
//! consume this crate later (the alp-crate sequencing precedent).
//!
//! Pipeline shape (memo §2):
//!   walk    — enumerate a canonical JEntry tree into (path, value) positions
//!             (memo §2.2 path vocabulary, §2.10 v1 array scoping);
//!   elect   — deterministic per-chunk path election under caps + hints
//!             (§2.2), producing the [`manifest::ShredManifest`] (§2.6);
//!   shred   — rows + manifest → typed lanes (§2.3) with validity, per-granule
//!             exception masks (§2.4), and the interned-path-ID bucketed
//!             residual (§2.5);
//!   reconstruct — shredded pieces → the byte-identical original jsonb image
//!             (§2.7), rebuilt bottom-up through the adt_jsonb build machinery.
//!
//! The reconstruction contract is the crate's reason to exist ahead of any
//! storage integration: `reconstruct(shred(manifest, x)) == x` byte-for-byte,
//! for every jsonb value jsonb_in can produce — jsonb_in's normalization
//! (last-wins key dedup + length-then-bytes key sort) is the input contract,
//! so the canonical tree the shredder walks is also the canonical order the
//! rebuild emits.

pub mod chunk;
pub mod elect;
pub mod manifest;
pub mod path;
pub mod reconstruct;
pub mod shred;
#[cfg(any(test, feature = "testgen"))]
pub mod testgen;
pub mod walk;

#[cfg(test)]
mod tests;

pub use chunk::{Bitmap, LaneValues, ResidualBucket, ShredChunk, TypedLane};
pub use elect::{elect_manifest, PathHint, ShredBudgets};
pub use manifest::{ElectedPath, Lane, ManifestError, ShredManifest};
pub use path::JsonPath;
pub use reconstruct::{reconstruct_row, reconstruct_rows};
pub use shred::{shred_chunk, FS_ELECT_SCALE_MAX};
