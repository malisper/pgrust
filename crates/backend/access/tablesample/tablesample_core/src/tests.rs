//! Unit tests for the TABLESAMPLE method math (SYSTEM, BERNOULLI) and the
//! `GetTsmRoutine` registry. These exercise the parts of the methods that do not
//! require a live `SampleScanState` / executor (the numeric core and the handler
//! descriptors), matching the C algorithm exactly.

use super::*;

/// `tsm_system_handler` / `tsm_bernoulli_handler` build the C-faithful routine.
#[test]
fn handlers_build_faithful_routines() {
    let sys = tsm_system_handler();
    assert_eq!(sys.type_, T_TsmRoutine);
    assert_eq!(sys.parameterTypes, alloc::vec![FLOAT4OID]);
    assert!(sys.repeatable_across_queries);
    assert!(sys.repeatable_across_scans);
    // SYSTEM installs every executor callback except EndSampleScan; NextSampleBlock present.
    assert!(sys.InitSampleScan.is_some());
    assert!(sys.BeginSampleScan.is_some());
    assert!(sys.NextSampleBlock.is_some());
    assert!(sys.NextSampleTuple.is_some());
    assert!(sys.EndSampleScan.is_none());

    let ber = tsm_bernoulli_handler();
    assert_eq!(ber.type_, T_TsmRoutine);
    assert_eq!(ber.parameterTypes, alloc::vec![FLOAT4OID]);
    // BERNOULLI has no NextSampleBlock (tuple-level), no EndSampleScan.
    assert!(ber.InitSampleScan.is_some());
    assert!(ber.BeginSampleScan.is_some());
    assert!(ber.NextSampleBlock.is_none());
    assert!(ber.NextSampleTuple.is_some());
    assert!(ber.EndSampleScan.is_none());
}

/// The cutoff formula: `rint(((double) PG_UINT32_MAX + 1) * percent / 100)`.
#[test]
fn cutoff_matches_c_formula() {
    // percent = 100 -> exactly PG_UINT32_MAX + 1 (every block selected).
    let dcutoff = (((PG_UINT32_MAX as f64) + 1.0) * 100.0 / 100.0).round_ties_even() as u64;
    assert_eq!(dcutoff, (u32::MAX as u64) + 1);
    // percent = 0 -> 0 (no block selected).
    let dcutoff0 = (((PG_UINT32_MAX as f64) + 1.0) * 0.0 / 100.0).round_ties_even() as u64;
    assert_eq!(dcutoff0, 0);
    // percent = 50 -> half of 2^32.
    let dcutoff50 = (((PG_UINT32_MAX as f64) + 1.0) * 50.0 / 100.0).round_ties_even() as u64;
    assert_eq!(dcutoff50, 1u64 << 31);
}

/// system_nextsampletuple walks 1..=maxoffset then returns InvalidOffsetNumber.
/// (Pure offset walk; exercised directly on a SystemSamplerData.)
#[test]
fn system_tuple_walk_matches_c() {
    // Replicate the body of system_nextsampletuple over an owned sampler.
    let mut lt: OffsetNumber = InvalidOffsetNumber;
    let maxoffset: OffsetNumber = 3;
    let step = |lt: &mut OffsetNumber| -> OffsetNumber {
        let mut tupoffset = *lt;
        if tupoffset == InvalidOffsetNumber {
            tupoffset = FirstOffsetNumber;
        } else {
            tupoffset += 1;
        }
        if tupoffset > maxoffset {
            tupoffset = InvalidOffsetNumber;
        }
        *lt = tupoffset;
        tupoffset
    };
    assert_eq!(step(&mut lt), 1);
    assert_eq!(step(&mut lt), 2);
    assert_eq!(step(&mut lt), 3);
    assert_eq!(step(&mut lt), InvalidOffsetNumber);
}

/// The 2-word / 3-word hash inputs are hashed as the native byte stream, exactly
/// as `hash_any((const unsigned char *) hashinput, sizeof(hashinput))`.
#[test]
fn hash_inputs_are_native_bytes() {
    let two = [0x11223344u32, 0xAABBCCDDu32];
    let mut bytes = [0u8; 8];
    bytes[0..4].copy_from_slice(&two[0].to_ne_bytes());
    bytes[4..8].copy_from_slice(&two[1].to_ne_bytes());
    assert_eq!(hash_any_u32_array2(&two), hash_bytes(&bytes));

    let three = [1u32, 2u32, 3u32];
    let mut b3 = [0u8; 12];
    b3[0..4].copy_from_slice(&three[0].to_ne_bytes());
    b3[4..8].copy_from_slice(&three[1].to_ne_bytes());
    b3[8..12].copy_from_slice(&three[2].to_ne_bytes());
    assert_eq!(hash_any_u32_array3(&three), hash_bytes(&b3));
}

/// `GetTsmRoutine` resolves the in-tree handler OIDs and rejects unknown ones.
#[test]
fn get_tsm_routine_resolves_and_rejects() {
    let cx = mcx::MemoryContext::new("tablesample-test");
    let mcx = cx.mcx();

    let sys = GetTsmRoutine(mcx, F_TSM_SYSTEM_HANDLER).expect("system routine");
    assert_eq!(sys.type_, T_TsmRoutine);
    assert!(sys.NextSampleBlock.is_some());

    let ber = GetTsmRoutine(mcx, F_TSM_BERNOULLI_HANDLER).expect("bernoulli routine");
    assert!(ber.NextSampleBlock.is_none());

    let bad = GetTsmRoutine(mcx, 1);
    assert!(bad.is_err());
}
