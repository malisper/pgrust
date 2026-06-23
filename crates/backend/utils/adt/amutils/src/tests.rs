//! Decision-tree parity tests for the `amutils` port.
//!
//! These install mock implementations for the catalog / index-AM seams that
//! model the real catalog rows + index-AM routine flags for the access methods
//! exercised by `src/test/regress/sql/amutils.sql`, then assert that
//! `indexam_property` and its four SQL wrappers reproduce the exact truth
//! tables in `src/test/regress/expected/amutils.out`.

extern crate std;

use super::*;
use alloc::string::{String, ToString};
use alloc::vec;
use ::amutils_seams::{
    AmPropertyRequest, IndexAmRoutineFlags, IndexFormInfo, IndexRelationInfo,
};
use ::mcx::MemoryContext;
use ::types_core::primitive::Oid;
use ::types_error::PgResult;

// --- Synthetic OIDs for the modeled indexes and AMs. -----------------------

const AM_BTREE: Oid = 403;
const AM_GIST: Oid = 783;

// onek_hundred: btree index, single key column, ASC NULLS LAST (indoption 0).
const IDX_ONEK_HUNDRED: Oid = 10_001;
// gcircleind: gist index, single key column.
const IDX_GCIRCLE: Oid = 10_002;
// foocover: btree, key col f1 (indoption 0), INCLUDE (f2, f3).
const IDX_FOOCOVER: Oid = 10_003;
// fooindex: btree, 4 cols: f1 desc, f2 asc, f3 nulls first, f4 nulls last.
const IDX_FOOINDEX: Oid = 10_004;

/// The btree `IndexAmRoutine` capability flags (from `nbtree.c`).
fn btree_routine() -> IndexAmRoutineFlags {
    IndexAmRoutineFlags {
        amcanorder: true,
        amcanorderbyop: false,
        amcanbackward: true,
        amcanunique: true,
        amcanmulticol: true,
        amsearcharray: true,
        amsearchnulls: true,
        amclusterable: true,
        amcaninclude: true,
        has_amproperty: false,
        has_amcanreturn: true,
        has_amgettuple: true,
        has_amgetbitmap: true,
        has_ambuildphasename: true,
    }
}

/// The gist `IndexAmRoutine` capability flags (from `gist.c`); gist supplies a
/// custom `amproperty` routine.
fn gist_routine() -> IndexAmRoutineFlags {
    IndexAmRoutineFlags {
        amcanorder: false,
        amcanorderbyop: true,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: true,
        amsearcharray: false,
        amsearchnulls: true,
        amclusterable: true,
        amcaninclude: true,
        has_amproperty: true,
        has_amcanreturn: true,
        has_amgettuple: true,
        has_amgetbitmap: true,
        has_ambuildphasename: true,
    }
}

fn mock_am_routine(amoid: Oid) -> PgResult<Option<IndexAmRoutineFlags>> {
    Ok(match amoid {
        AM_BTREE => Some(btree_routine()),
        AM_GIST => Some(gist_routine()),
        _ => None,
    })
}

fn mock_index_relation(index_oid: Oid) -> PgResult<Option<IndexRelationInfo>> {
    let (relam, relnatts) = match index_oid {
        IDX_ONEK_HUNDRED => (AM_BTREE, 1),
        IDX_GCIRCLE => (AM_GIST, 1),
        IDX_FOOCOVER => (AM_BTREE, 3),
        IDX_FOOINDEX => (AM_BTREE, 4),
        _ => return Ok(None),
    };
    Ok(Some(IndexRelationInfo {
        relkind: RELKIND_INDEX,
        relam,
        relnatts,
    }))
}

fn mock_index_form(index_oid: Oid) -> PgResult<Option<IndexFormInfo>> {
    let (indnatts, indnkeyatts, indoption): (i16, i16, alloc::vec::Vec<i16>) = match index_oid {
        IDX_ONEK_HUNDRED => (1, 1, vec![0]),
        IDX_GCIRCLE => (1, 1, vec![0]),
        IDX_FOOCOVER => (3, 1, vec![0, 0, 0]),
        // f1 desc, f2 asc, f3 nulls first, f4 nulls last. Note that DESC implies
        // NULLS FIRST by default, so col 1's stored indoption is
        // INDOPTION_DESC | INDOPTION_NULLS_FIRST (matching amutils.out).
        IDX_FOOINDEX => (
            4,
            4,
            vec![
                INDOPTION_DESC | INDOPTION_NULLS_FIRST,
                0,
                INDOPTION_NULLS_FIRST,
                0,
            ],
        ),
        _ => return Ok(None),
    };
    Ok(Some(IndexFormInfo {
        indexrelid: index_oid,
        indnatts,
        indnkeyatts,
        indoption,
    }))
}

fn mock_am_property(_mcx: ::mcx::Mcx<'_>, req: AmPropertyRequest) -> PgResult<Option<(bool, bool)>> {
    // Model gist's amproperty just for DISTANCE_ORDERABLE on a key column, which
    // is what the regress test relies on (gcircleind col 1 -> t).
    if req.amoid == AM_GIST
        && req.prop == IndexAmProperty::DistanceOrderable
        && req.attno == 1
    {
        return Ok(Some((true, false)));
    }
    // Otherwise fall through to the generic logic.
    Ok(None)
}

fn mock_index_can_return(_mcx: ::mcx::Mcx<'_>, index_oid: Oid, _attno: i32) -> PgResult<bool> {
    // btree can always return; gist circle cannot.
    Ok(index_oid == IDX_ONEK_HUNDRED || index_oid == IDX_FOOCOVER)
}

fn mock_am_buildphasename(_amoid: Oid, phasenum: i64) -> PgResult<Option<String>> {
    Ok(match phasenum {
        1 => Some("initializing".to_string()),
        _ => None,
    })
}

/// Install all mock catalog seams. The seam slots are process-global
/// install-once `OnceLock`s, so a `Once` guard makes this safe and idempotent
/// across the several (parallel) tests that call it.
fn install_test_catalog() {
    use std::sync::Once;
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        seam::am_routine::set(mock_am_routine);
        seam::index_relation::set(mock_index_relation);
        seam::index_form::set(mock_index_form);
        seam::am_property::set(mock_am_property);
        seam::index_can_return::set(mock_index_can_return);
        seam::am_buildphasename::set(mock_am_buildphasename);
    });
}

#[test]
fn lookup_prop_name_is_case_insensitive_and_unknown_safe() {
    assert_eq!(lookup_prop_name("asc"), IndexAmProperty::Asc);
    assert_eq!(lookup_prop_name("ASC"), IndexAmProperty::Asc);
    assert_eq!(lookup_prop_name("Can_Include"), IndexAmProperty::CanInclude);
    assert_eq!(
        lookup_prop_name("distance_orderable"),
        IndexAmProperty::DistanceOrderable
    );
    // Unknown names never error; AMs can define their own.
    assert_eq!(lookup_prop_name("bogus"), IndexAmProperty::Unknown);
    assert_eq!(lookup_prop_name(""), IndexAmProperty::Unknown);
}

#[test]
fn test_indoption_guard_and_bits() {
    // guard=false -> forced false (not null).
    assert_eq!(
        test_indoption(&[0], 1, false, INDOPTION_DESC, 0),
        Some(false)
    );
    // ASC test: (0 & DESC) == 0 -> true.
    assert_eq!(test_indoption(&[0], 1, true, INDOPTION_DESC, 0), Some(true));
    // DESC test on a DESC column -> true.
    assert_eq!(
        test_indoption(&[INDOPTION_DESC], 1, true, INDOPTION_DESC, INDOPTION_DESC),
        Some(true)
    );
    // NULLS FIRST test on a non-nulls-first column -> false.
    assert_eq!(
        test_indoption(&[0], 1, true, INDOPTION_NULLS_FIRST, INDOPTION_NULLS_FIRST),
        Some(false)
    );
}

/// Golden truth table for `pg_index_column_has_property('onek_hundred', 1, …)`
/// (btree), from `amutils.out`.
#[test]
fn btree_column_properties_match_golden() {
    install_test_catalog();
    let cx = MemoryContext::new("amutils_test");
    let col = |p: &str| pg_index_column_has_property(cx.mcx(), IDX_ONEK_HUNDRED, 1, p).unwrap();
    assert_eq!(col("asc"), Some(true));
    assert_eq!(col("desc"), Some(false));
    assert_eq!(col("nulls_first"), Some(false));
    assert_eq!(col("nulls_last"), Some(true));
    assert_eq!(col("orderable"), Some(true));
    assert_eq!(col("distance_orderable"), Some(false));
    assert_eq!(col("returnable"), Some(true));
    assert_eq!(col("search_array"), Some(true));
    assert_eq!(col("search_nulls"), Some(true));
    assert_eq!(col("bogus"), None);
}

/// Golden truth table for the gist `gcircleind` column (from `amutils.out`).
#[test]
fn gist_column_properties_match_golden() {
    install_test_catalog();
    let cx = MemoryContext::new("amutils_test");
    let col = |p: &str| pg_index_column_has_property(cx.mcx(), IDX_GCIRCLE, 1, p).unwrap();
    assert_eq!(col("asc"), Some(false));
    assert_eq!(col("desc"), Some(false));
    assert_eq!(col("nulls_first"), Some(false));
    assert_eq!(col("nulls_last"), Some(false));
    assert_eq!(col("orderable"), Some(false));
    // distance_orderable is answered by gist's amproperty routine.
    assert_eq!(col("distance_orderable"), Some(true));
    assert_eq!(col("returnable"), Some(false));
    assert_eq!(col("search_array"), Some(false));
    assert_eq!(col("search_nulls"), Some(true));
    assert_eq!(col("bogus"), None);
}

/// Golden truth table for index-level properties (from `amutils.out`).
#[test]
fn index_level_properties_match_golden() {
    install_test_catalog();
    let cx = MemoryContext::new("amutils_test");
    let btree = |p: &str| pg_index_has_property(cx.mcx(), IDX_ONEK_HUNDRED, p).unwrap();
    assert_eq!(btree("clusterable"), Some(true));
    assert_eq!(btree("index_scan"), Some(true));
    assert_eq!(btree("bitmap_scan"), Some(true));
    assert_eq!(btree("backward_scan"), Some(true));
    assert_eq!(btree("bogus"), None);
    // Column-level / AM-level names asked at index level -> NULL.
    assert_eq!(btree("asc"), None);
    assert_eq!(btree("can_order"), None);

    let gist = |p: &str| pg_index_has_property(cx.mcx(), IDX_GCIRCLE, p).unwrap();
    assert_eq!(gist("clusterable"), Some(true));
    assert_eq!(gist("index_scan"), Some(true));
    assert_eq!(gist("bitmap_scan"), Some(true));
    assert_eq!(gist("backward_scan"), Some(false));
}

/// Golden truth table for AM-level properties (from `amutils.out`).
#[test]
fn am_level_properties_match_golden() {
    install_test_catalog();
    let cx = MemoryContext::new("amutils_test");
    let btree = |p: &str| pg_indexam_has_property(cx.mcx(), AM_BTREE, p).unwrap();
    assert_eq!(btree("can_order"), Some(true));
    assert_eq!(btree("can_unique"), Some(true));
    assert_eq!(btree("can_multi_col"), Some(true));
    assert_eq!(btree("can_exclude"), Some(true));
    assert_eq!(btree("can_include"), Some(true));
    assert_eq!(btree("bogus"), None);
    // An index-level / column-level name asked at AM level -> NULL.
    assert_eq!(btree("clusterable"), None);
    assert_eq!(btree("asc"), None);

    let gist = |p: &str| pg_indexam_has_property(cx.mcx(), AM_GIST, p).unwrap();
    assert_eq!(gist("can_order"), Some(false));
    assert_eq!(gist("can_unique"), Some(false));
    assert_eq!(gist("can_multi_col"), Some(true));
    assert_eq!(gist("can_exclude"), Some(true));
    assert_eq!(gist("can_include"), Some(true));
}

/// Golden truth table for the multi-column `fooindex` (from `amutils.out`):
/// f1 desc, f2 asc, f3 nulls first, f4 nulls last.
#[test]
fn fooindex_per_column_match_golden() {
    install_test_catalog();
    let cx = MemoryContext::new("amutils_test");
    let c = |col: i32, p: &str| pg_index_column_has_property(cx.mcx(), IDX_FOOINDEX, col, p).unwrap();
    // col 1: orderable t, asc f, desc t, nulls_first t, nulls_last f
    assert_eq!(c(1, "orderable"), Some(true));
    assert_eq!(c(1, "asc"), Some(false));
    assert_eq!(c(1, "desc"), Some(true));
    assert_eq!(c(1, "nulls_first"), Some(true));
    assert_eq!(c(1, "nulls_last"), Some(false));
    assert_eq!(c(1, "bogus"), None);
    // col 2: asc t, desc f, nulls_first f, nulls_last t
    assert_eq!(c(2, "asc"), Some(true));
    assert_eq!(c(2, "desc"), Some(false));
    assert_eq!(c(2, "nulls_first"), Some(false));
    assert_eq!(c(2, "nulls_last"), Some(true));
    // col 3: nulls_first t, nulls_last f
    assert_eq!(c(3, "nulls_first"), Some(true));
    assert_eq!(c(3, "nulls_last"), Some(false));
    // col 4: nulls_first f, nulls_last t
    assert_eq!(c(4, "nulls_first"), Some(false));
    assert_eq!(c(4, "nulls_last"), Some(true));
}

/// Golden truth table for the covering index `foocover` (from `amutils.out`):
/// key col f1, INCLUDE (f2, f3). Nonkey columns return NULL for most props.
#[test]
fn foocover_include_columns_match_golden() {
    install_test_catalog();
    let cx = MemoryContext::new("amutils_test");
    let c = |col: i32, p: &str| pg_index_column_has_property(cx.mcx(), IDX_FOOCOVER, col, p).unwrap();
    // col 1 (key): orderable t, asc t, desc f, nulls_first f, nulls_last t,
    // distance_orderable f, returnable t.
    assert_eq!(c(1, "orderable"), Some(true));
    assert_eq!(c(1, "asc"), Some(true));
    assert_eq!(c(1, "desc"), Some(false));
    assert_eq!(c(1, "nulls_first"), Some(false));
    assert_eq!(c(1, "nulls_last"), Some(true));
    assert_eq!(c(1, "distance_orderable"), Some(false));
    assert_eq!(c(1, "returnable"), Some(true));
    assert_eq!(c(1, "bogus"), None);
    // col 2 (nonkey): orderable f, asc/desc/nulls_* NULL,
    // distance_orderable f, returnable t.
    assert_eq!(c(2, "orderable"), Some(false));
    assert_eq!(c(2, "asc"), None);
    assert_eq!(c(2, "desc"), None);
    assert_eq!(c(2, "nulls_first"), None);
    assert_eq!(c(2, "nulls_last"), None);
    assert_eq!(c(2, "distance_orderable"), Some(false));
    assert_eq!(c(2, "returnable"), Some(true));
    // col 3 (nonkey): same shape as col 2.
    assert_eq!(c(3, "orderable"), Some(false));
    assert_eq!(c(3, "asc"), None);
    assert_eq!(c(3, "returnable"), Some(true));
}

#[test]
fn attno_out_of_range_and_zero_are_null() {
    install_test_catalog();
    let cx = MemoryContext::new("amutils_test");
    // attno 0 is rejected immediately by pg_index_column_has_property.
    assert_eq!(
        pg_index_column_has_property(cx.mcx(), IDX_ONEK_HUNDRED, 0, "asc").unwrap(),
        None
    );
    // attno beyond natts -> NULL.
    assert_eq!(
        pg_index_column_has_property(cx.mcx(), IDX_ONEK_HUNDRED, 5, "asc").unwrap(),
        None
    );
    // negative attno -> NULL.
    assert_eq!(
        pg_index_column_has_property(cx.mcx(), IDX_ONEK_HUNDRED, -1, "asc").unwrap(),
        None
    );
}

#[test]
fn missing_index_and_am_return_null() {
    install_test_catalog();
    let cx = MemoryContext::new("amutils_test");
    // Unknown index OID -> NULL (pg_class lookup miss).
    assert_eq!(pg_index_has_property(cx.mcx(), 99_999, "clusterable").unwrap(), None);
    // Unknown AM OID -> NULL (GetIndexAmRoutineByAmId miss).
    assert_eq!(pg_indexam_has_property(cx.mcx(), 99_999, "can_order").unwrap(), None);
}

#[test]
fn progress_phasename_dispatches_through_seam() {
    install_test_catalog();
    assert_eq!(
        pg_indexam_progress_phasename(AM_BTREE, 1).unwrap(),
        Some("initializing".to_string())
    );
    // NULL name for an unmodeled phase.
    assert_eq!(pg_indexam_progress_phasename(AM_BTREE, 99).unwrap(), None);
    // Unknown AM -> NULL.
    assert_eq!(pg_indexam_progress_phasename(99_999, 1).unwrap(), None);
}

/// Lock in the C `int32 phasenum = PG_GETARG_INT32(1)` truncation: the int8 SQL
/// argument is reduced to its low 32 bits (sign-extended) before the callback
/// is invoked, exactly as `DatumGetInt32` does. C calls `ambuildphasename(1)`
/// for 0x1_0000_0001; a faithful port must do the same (NOT call it with
/// 4294967297, which the test catalog would map to NULL).
#[test]
fn progress_phasename_truncates_int8_arg_to_int32() {
    install_test_catalog();
    // 0x1_0000_0001 = 4294967297; low 32 bits = 1 -> "initializing".
    assert_eq!(
        pg_indexam_progress_phasename(AM_BTREE, 0x1_0000_0001).unwrap(),
        Some("initializing".to_string())
    );
    // i32::MAX + 1 wraps (as i32) to i32::MIN, which the catalog maps to NULL.
    assert_eq!(
        pg_indexam_progress_phasename(AM_BTREE, i32::MAX as i64 + 1).unwrap(),
        None
    );
    // 0x2_0000_0063 (= 8589934691) truncates to 0x63 = 99 -> NULL phase.
    assert_eq!(
        pg_indexam_progress_phasename(AM_BTREE, 0x2_0000_0063).unwrap(),
        None
    );
    // Sanity: the low 32 bits being 1 yields the name regardless of high bits.
    assert_eq!(
        pg_indexam_progress_phasename(AM_BTREE, 0xDEAD_BEEF_0000_0001u64 as i64).unwrap(),
        Some("initializing".to_string())
    );
}
