//! ABI fidelity checks for the `nodeSamplescan.c` state/descriptor structs.
//!
//! Sizes/offsets are derived from PostgreSQL 18.3 on a 64-bit LP64 target:
//!   * `PlanState` = 200 bytes, `ScanState` = `PlanState` + 3 pointers = 224.
//!   * `SampleScanState` = `ScanState`(224) + 4 ptrs(32) + 3 bool(3) + pad(1)
//!     + uint32 seed(4) + int64 donetuples(8) + 2 bool(2) + tail pad(2) = 280.
//!   * `TsmRoutine` = NodeTag(4)+pad(4) + List*(8) + 2 bool(2)+pad(6) + 6 fn
//!     ptrs(48) = 72.
//!   * `TableSampleClause` = NodeTag(4)+pad(4) + Oid(4)+pad(4)? no: NodeTag(4),
//!     Oid(4), List*(8), Expr*(8) = 24.
//!   * `SampleScan` = `Scan`(112) + TableSampleClause*(8) = 120.

use core::mem::{align_of, offset_of, size_of};
use ::pg_ffi_fgram::{
    PlanStateData, SampleScan, SampleScanState, ScanStateData, TableSampleClause, TsmRoutine,
};

#[test]
fn samplescan_abi_layout_matches_postgres_on_64_bit() {
    assert_eq!(size_of::<PlanStateData>(), 200);
    assert_eq!(size_of::<ScanStateData>(), 224);

    assert_eq!(offset_of!(SampleScanState, ss), 0);
    assert_eq!(offset_of!(SampleScanState, args), 224);
    assert_eq!(offset_of!(SampleScanState, repeatable), 232);
    assert_eq!(offset_of!(SampleScanState, tsmroutine), 240);
    assert_eq!(offset_of!(SampleScanState, tsm_state), 248);
    assert_eq!(offset_of!(SampleScanState, use_bulkread), 256);
    assert_eq!(offset_of!(SampleScanState, use_pagemode), 257);
    assert_eq!(offset_of!(SampleScanState, begun), 258);
    assert_eq!(offset_of!(SampleScanState, seed), 260);
    assert_eq!(offset_of!(SampleScanState, donetuples), 264);
    assert_eq!(offset_of!(SampleScanState, haveblock), 272);
    assert_eq!(offset_of!(SampleScanState, done), 273);
    assert_eq!(size_of::<SampleScanState>(), 280);
    assert_eq!(align_of::<SampleScanState>(), 8);

    assert_eq!(size_of::<TsmRoutine>(), 72);
    assert_eq!(offset_of!(TsmRoutine, type_), 0);
    assert_eq!(offset_of!(TsmRoutine, parameterTypes), 8);
    assert_eq!(offset_of!(TsmRoutine, repeatable_across_queries), 16);
    assert_eq!(offset_of!(TsmRoutine, repeatable_across_scans), 17);
    assert_eq!(offset_of!(TsmRoutine, SampleScanGetSampleSize), 24);
    assert_eq!(offset_of!(TsmRoutine, InitSampleScan), 32);
    assert_eq!(offset_of!(TsmRoutine, BeginSampleScan), 40);
    assert_eq!(offset_of!(TsmRoutine, NextSampleBlock), 48);
    assert_eq!(offset_of!(TsmRoutine, NextSampleTuple), 56);
    assert_eq!(offset_of!(TsmRoutine, EndSampleScan), 64);

    assert_eq!(size_of::<TableSampleClause>(), 24);
    assert_eq!(offset_of!(TableSampleClause, type_), 0);
    assert_eq!(offset_of!(TableSampleClause, tsmhandler), 4);
    assert_eq!(offset_of!(TableSampleClause, args), 8);
    assert_eq!(offset_of!(TableSampleClause, repeatable), 16);

    assert_eq!(offset_of!(SampleScan, scan), 0);
    assert_eq!(size_of::<SampleScan>(), 120);
}
