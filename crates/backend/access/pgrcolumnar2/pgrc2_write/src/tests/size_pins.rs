//! `size_of` pins on the writer's cross-lane ABI faces (the issue-#69
//! template, adapted: this crate defines NO on-disk structs — every wire
//! byte goes through the frozen `pgrc2_format` encoders, pinned at M3-A;
//! the shred path table has its byte-golden in `shred_tests`). What IS
//! pinned here are the types that cross lane boundaries by value — M3-H
//! unpacks PG datums into [`RawDatum`] per COPY row and passes
//! [`TxnStamp`]/[`SubxactEvidence`]/[`PartCutPolicy`]; M3-I inherits the
//! whole ingest face. Growth is a deliberate act reviewed at the seam, not
//! an accident a field-add smuggles in.
//!
//! Pins are `const` asserts: they fire at `cargo check --tests` time.

use crate::elect::{ElectionWitness, ExtentShape};
use crate::ingest::RawDatum;
use crate::seal::PartSpec;
use crate::writer::{FreezeDecision, PartCutPolicy, SubxactEvidence, TxnStamp};
use core::mem::{align_of, size_of};

// The per-row hot face: one enum per column per COPY row (M3-H boundary).
// Fat-pointer variant + discriminant on 64-bit.
const _: () = assert!(size_of::<RawDatum<'_>>() == 24);
const _: () = assert!(align_of::<RawDatum<'_>>() == 8);

// Statement identity + freeze belt (passed capabilities, spec §13).
const _: () = assert!(size_of::<TxnStamp>() == 16);
const _: () = assert!(size_of::<SubxactEvidence>() == 3);
const _: () = assert!(size_of::<FreezeDecision>() == 1);

// Deterministic part-cut bounds. Two budgets plus the M3-I cut granule —
// 8 + 8 + 4 + 4 tail padding. The granule grew this from 16: it is the
// shared serial/parallel cut-decision granularity that makes a
// parallel-loaded table byte-identical to a serial-loaded one.
const _: () = assert!(size_of::<PartCutPolicy>() == 24);

// Part identity facts (spec §5.5 fingerprint currency).
const _: () = assert!(size_of::<PartSpec>() == 24);

// Election vocabulary recorded in every SealReport (M3-C's seam currency).
const _: () = assert!(size_of::<ElectionWitness>() == 32);
const _: () = assert!(size_of::<ExtentShape>() == 16);

// The parallel-session bounds (M3-I; M3-H constructs these at COPY time).
const _: () = assert!(size_of::<crate::par::ParIngestOpts>() == 24);

/// Runtime witness so the pins appear in the test ledger (the const
/// asserts above are the actual gate).
#[test]
fn abi_face_sizes_pinned() {
    assert_eq!(
        (
            size_of::<RawDatum<'_>>(),
            size_of::<TxnStamp>(),
            size_of::<SubxactEvidence>(),
            size_of::<FreezeDecision>(),
            size_of::<PartCutPolicy>(),
            size_of::<PartSpec>(),
            size_of::<ElectionWitness>(),
            size_of::<ExtentShape>(),
            size_of::<crate::par::ParIngestOpts>(),
        ),
        // PartCutPolicy is 24 (was 16): the M3-I cut granule — see the const
        // assert above for why it grew.
        (24, 16, 3, 1, 24, 24, 32, 16, 24)
    );
}
