//! The pgrc2_write test suite (M3-D exit slice + the M3-I parallel
//! suite). Shared scaffolding lives in [`crate::testkit`] (the pgrc2_read
//! `testpart` precedent: a test-support module in the lib so the
//! `pgrc2_ingest_par` binding crate's integration tests drive the same kit).

mod bankplane_tests;
mod crash_matrix;
mod detoast_tests;
mod determinism;
mod grain_tests;
mod par_determinism;
mod par_seam;
mod dict_inherit;
mod dict_tests;
mod fold_fusion;
mod election;
mod full_election;
mod lifecycle_tests;
mod meta_wire_tests;
mod overflow_tests;
mod publish_order;
mod roundtrip_gate;
mod shred_jsonb_tests;
mod shred_tests;
mod size_pins;
mod sorted_ingest;
mod stats_sidecar;
mod structural_arrays;
mod two_witness;

pub use crate::testkit::*;

// The suite files were written against the pre-split scaffolding where these
// names lived in this module; re-export them so `use super::*` keeps working.
// (Some rows shadow testkit re-exports — allow the overlap.)
#[allow(unused_imports)]
pub use crate::elect::ReferenceCandidates;
#[allow(unused_imports)]
pub use crate::par::{SharedMemVfs, SharedMemVfsProvider};
#[allow(unused_imports)]
pub use crate::ingest::{NoExternalDetoast, RawDatum};
#[allow(unused_imports)]
pub use crate::publish::{TxnProbe, TxnVerdict};
#[allow(unused_imports)]
pub use crate::seal::ReferenceResolver;
#[allow(unused_imports)]
pub use crate::shred::NoShred;
#[allow(unused_imports)]
pub use crate::writer::{PartCutPolicy, SealEnv, SubxactEvidence, TableWriter, TxnStamp};
#[allow(unused_imports)]
pub use crate::wvfs::{MemVfs, WriteVfs};
#[allow(unused_imports)]
pub use pgrc2_format::class::{ColSchema, CollationClass, StorageClass, TypeSemantics};
#[allow(unused_imports)]
pub use pgrc2_format::manifest::Manifest;
#[allow(unused_imports)]
pub use pgrc2_format::part::{
    ExtentRecord, FooterFixed, PartTail, SectionEntry, SectionKind, StreamEntry, StreamRole,
};
#[allow(unused_imports)]
pub use pgrc2_format::relopt::ShredOptions;
#[allow(unused_imports)]
pub use pgrc2_format::wire::Cur;
#[allow(unused_imports)]
pub use std::collections::BTreeMap;

