//! Smoke tests for the nodeIncrementalSort port.

use super::*;

#[test]
fn min_i64_picks_smaller() {
    assert_eq!(min_i64(3, 7), 3);
    assert_eq!(min_i64(7, 3), 3);
    assert_eq!(min_i64(-1, 0), -1);
}

#[test]
fn group_size_constants_match_c() {
    assert_eq!(DEFAULT_MIN_GROUP_SIZE, 32);
    assert_eq!(DEFAULT_MAX_FULL_SORT_GROUP_SIZE, 64);
}

#[test]
fn instrument_sorted_group_accumulates_disk() {
    let mut gi = IncrementalSortGroupInfo::default();
    let instr = TuplesortInstrumentation {
        sortMethod: nodes::nodesort::TuplesortMethod::SORT_TYPE_EXTERNAL_SORT,
        spaceType: TuplesortSpaceType::SORT_SPACE_TYPE_DISK,
        spaceUsed: 100,
    };
    instrument_sorted_group(&mut gi, &instr);
    instrument_sorted_group(&mut gi, &instr);
    assert_eq!(gi.groupCount, 2);
    assert_eq!(gi.totalDiskSpaceUsed, 200);
    assert_eq!(gi.maxDiskSpaceUsed, 100);
    assert_eq!(gi.totalMemorySpaceUsed, 0);
    assert_ne!(gi.sortMethods, 0);
}

#[test]
fn instrument_sorted_group_accumulates_memory() {
    let mut gi = IncrementalSortGroupInfo::default();
    let instr = TuplesortInstrumentation {
        sortMethod: nodes::nodesort::TuplesortMethod::SORT_TYPE_QUICKSORT,
        spaceType: TuplesortSpaceType::SORT_SPACE_TYPE_MEMORY,
        spaceUsed: 50,
    };
    instrument_sorted_group(&mut gi, &instr);
    assert_eq!(gi.groupCount, 1);
    assert_eq!(gi.maxMemorySpaceUsed, 50);
    assert_eq!(gi.totalMemorySpaceUsed, 50);
    assert_eq!(gi.totalDiskSpaceUsed, 0);
}
