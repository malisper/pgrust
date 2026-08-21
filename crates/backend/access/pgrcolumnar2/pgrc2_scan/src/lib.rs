//! # pgrc2_scan — the M3-L3 reader/scan driver (claim-plane consumer #2)
//!
//! The v4-native rebuild of the v3 `lx_source` reader cone on the FROZEN
//! contracts (`lanev4-parallel-contract.md`, `lanev4-batch-abi.md`),
//! parallel-native from birth (binding law 8; a serial-only landing is
//! prohibited — dop=1 is the determinism reference, not a separate path):
//!
//! - [`spans`] — the granule-ordinal unit space over an opened part set
//!   (PC-2.1/PC-3.1; part edges are the hard boundaries);
//! - [`scan`] — the worker drive: span claims through `pgrc2_claim`, part
//!   pins released at end_claim BY CONSTRUCTION (PC-2.4, IN-4),
//!   release-on-advance part caching (the #802 lineage, kill switch
//!   `PGRUST_SCAN_PART_RELEASE=0`), ABI batches (dict-code lanes under the
//!   OD-9 max-code guard, StrView gather per AP-3, RowId 32/19/13);
//! - [`dictspace`] — the `DictSpace` seam over the frame-lazy
//!   `pgrc2_read::DictHandle` (SB-7: only touched frames fault);
//! - [`meta`] — verdict-plane consult (zone keys + PSMA windows + bloom)
//!   with the XC-5 census attributing exactly.
//!
//! The engine executor binding (table AM, Gather-free hosting, the
//! statement-grain pool) is M4's; this crate is the AM-capable parallel
//! feeder the M3 plan assigns to L3, and the shapes here are what M4's
//! compilation substrate consumes unchanged (AB-7.4).

pub mod dictspace;
pub mod dv;
pub mod meta;
pub mod prune;
pub mod readahead;
pub mod scan;
pub mod spans;

pub use dictspace::ScanDictSpace;
pub use dv::{PartDeletes, DV_GRANULE_WORDS};
pub use meta::{ColumnMeta, GranuleConsult, ScanConst, ScanPredicate};
pub use prune::{PruneOp, PrunePlane, PrunePred, PruneVerdicts};
pub use scan::{
    code_bound_guard, PinWitness, ScanCensus, ScanColumn, ScanOptions, ScanResult, ScanWorker,
    SharedCounters, TableScan,
};
pub use spans::{GranuleSpans, SpanClaimSource, SurvivorSpans};
// The error surface consumers match on (the verify-probe face returns
// `pgrc2_read` errors; re-exported so engine crates need no direct dep).
pub use pgrc2_read::{ReadError, ReadResult};

#[cfg(test)]
mod shred_tests;
#[cfg(test)]
mod tests;
