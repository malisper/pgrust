//! Unit tests for the in-crate (seam-free) logic of the vacuumlazy port.
//!
//! These cover the pure decision/formatting functions that do not require any
//! substrate seam to be installed: the `errcontext` message construction, the
//! save/restore of vacuum error state, the transaction-id wraparound compares,
//! and the truncation-decision arithmetic.

use ::mcx::{Mcx, MemoryContext, PgString, PgVec};
use ::rel::{FormData_pg_class, Relation, RelationData};
use ::types_storage::RelFileLocator;
use ::types_tuple::heaptuple::TupleDescData;

use crate::consts::*;
use crate::core::{LVRelState, LVSavedErrInfo, VacErrPhase};
use crate::errcb::{restore_vacuum_error_info, update_vacuum_error_info, vacuum_error_callback};

/// A minimal transient `Relation` for the seam-free unit tests. The error-context
/// tests never read any of its fields (they exercise only the reporting strings
/// stored directly on `LVRelState`), so the trimmed defaults are sufficient.
fn test_relation<'mcx>(mcx: Mcx<'mcx>) -> Relation<'mcx> {
    let td = TupleDescData {
        natts: 0,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: PgVec::new_in(mcx),
        attrs: PgVec::new_in(mcx),
    };
    let data = RelationData {
        rd_id: 0,
        rd_locator: RelFileLocator {
            spcOid: 0,
            dbOid: 0,
            relNumber: 0,
        },
        rd_backend: types_core::primitive::INVALID_PROC_NUMBER,
        rd_rel: FormData_pg_class {
            relname: PgString::from_str_in("t", mcx).unwrap(),
            relnamespace: 0,
            relowner: 0,
            relrowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            reltoastrelid: 0,
            reltablespace: 0,
            relfilenode: 0,
            relisshared: false,
            relhasindex: false,
            relhassubclass: false,
            relpersistence: b'p',
            relkind: b'r',
            reltype: 0,
            relam: 0,
            relispopulated: true,
            relreplident: b'd',
            relispartition: false,
            relfrozenxid: 0,
            relminmxid: 0,
        },
        rd_att: ::mcx::alloc_in(mcx, td).unwrap(),
        rd_options: None,
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_trigdesc: None,
        pgstat_enabled: false,
    };
    Relation::open(data, None)
}

fn base_state<'mcx>(mcx: Mcx<'mcx>) -> LVRelState<'mcx> {
    let mut vr = LVRelState::new_zeroed(mcx, test_relation(mcx));
    vr.relnamespace = "public".into();
    vr.relname = "t".into();
    vr
}

#[test]
fn errcb_scan_heap_messages() {
    let ctx = MemoryContext::new("vacuumlazy-test");
    let mut vr = base_state(ctx.mcx());
    vr.phase = VacErrPhase::ScanHeap;

    // invalid block -> "while scanning relation"
    vr.blkno = InvalidBlockNumber;
    vr.offnum = InvalidOffsetNumber;
    assert_eq!(
        vacuum_error_callback(&vr).unwrap(),
        "while scanning relation \"public.t\""
    );

    // valid block, invalid offset -> "while scanning block N"
    vr.blkno = 7;
    vr.offnum = InvalidOffsetNumber;
    assert_eq!(
        vacuum_error_callback(&vr).unwrap(),
        "while scanning block 7 of relation \"public.t\""
    );

    // valid block + offset -> "while scanning block N offset M"
    vr.blkno = 7;
    vr.offnum = 3;
    assert_eq!(
        vacuum_error_callback(&vr).unwrap(),
        "while scanning block 7 offset 3 of relation \"public.t\""
    );
}

#[test]
fn errcb_index_and_truncate_messages() {
    let ctx = MemoryContext::new("vacuumlazy-test");
    let mut vr = base_state(ctx.mcx());
    vr.indname = Some("t_pkey".into());

    vr.phase = VacErrPhase::VacuumIndex;
    assert_eq!(
        vacuum_error_callback(&vr).unwrap(),
        "while vacuuming index \"t_pkey\" of relation \"public.t\""
    );

    vr.phase = VacErrPhase::IndexCleanup;
    assert_eq!(
        vacuum_error_callback(&vr).unwrap(),
        "while cleaning up index \"t_pkey\" of relation \"public.t\""
    );

    // truncate with valid block
    vr.phase = VacErrPhase::Truncate;
    vr.blkno = 42;
    assert_eq!(
        vacuum_error_callback(&vr).unwrap(),
        "while truncating relation \"public.t\" to 42 blocks"
    );

    // truncate with invalid block -> None (C does nothing)
    vr.blkno = InvalidBlockNumber;
    assert!(vacuum_error_callback(&vr).is_none());

    // unknown phase -> None
    vr.phase = VacErrPhase::Unknown;
    assert!(vacuum_error_callback(&vr).is_none());
}

#[test]
fn update_and_restore_error_info() {
    let ctx = MemoryContext::new("vacuumlazy-test");
    let mut vr = base_state(ctx.mcx());
    vr.blkno = 1;
    vr.offnum = 2;
    vr.phase = VacErrPhase::ScanHeap;

    let mut saved = LVSavedErrInfo {
        blkno: 0,
        offnum: 0,
        phase: VacErrPhase::Unknown,
    };
    update_vacuum_error_info(&mut vr, Some(&mut saved), VacErrPhase::VacuumIndex, 99, 0);

    // saved holds the prior values
    assert_eq!(saved.blkno, 1);
    assert_eq!(saved.offnum, 2);
    assert_eq!(saved.phase, VacErrPhase::ScanHeap);
    // vr now holds the new values
    assert_eq!(vr.blkno, 99);
    assert_eq!(vr.offnum, 0);
    assert_eq!(vr.phase, VacErrPhase::VacuumIndex);

    restore_vacuum_error_info(&mut vr, &saved);
    assert_eq!(vr.blkno, 1);
    assert_eq!(vr.offnum, 2);
    assert_eq!(vr.phase, VacErrPhase::ScanHeap);
}

#[test]
fn txid_wraparound_compares() {
    // normal ordering
    assert!(transaction_id_precedes(10, 20));
    assert!(!transaction_id_precedes(20, 10));
    assert!(transaction_id_precedes_or_equals(20, 20));
    assert!(transaction_id_follows(20, 10));

    // wraparound: a huge xid "precedes" a small one modularly
    let big = u32::MAX - 5;
    assert!(transaction_id_precedes(big, 3));
    assert!(transaction_id_follows(3, big));

    // special (non-normal) xids fall back to plain integer compare
    assert!(transaction_id_precedes(0, 1));
    assert!(!transaction_id_is_normal(InvalidTransactionId));
    assert!(transaction_id_is_normal(types_core::FirstNormalTransactionId));
}

#[test]
fn mxid_wraparound_compares() {
    assert!(multi_xact_id_precedes(10, 20));
    assert!(multi_xact_id_precedes_or_equals(20, 20));
    assert!(!multi_xact_id_is_valid(InvalidMultiXactId));
    assert!(multi_xact_id_is_valid(5));
}

#[test]
fn offset_and_buffer_helpers() {
    assert_eq!(offset_number_next(1), 2);
    // wrapping at u16 max (C OffsetNumberNext is 1 + offnum on uint16)
    assert_eq!(offset_number_next(u16::MAX), 0);
    assert!(buffer_is_valid(5));
    assert!(!buffer_is_valid(InvalidBuffer));
    assert!(pg_cmp_u16(3, 1) > 0);
    assert!(pg_cmp_u16(1, 3) < 0);
    assert_eq!(pg_cmp_u16(2, 2), 0);
}

#[test]
fn should_attempt_truncation_arithmetic() {
    // This exercises the pure arithmetic of the decision without the failsafe
    // seam by reconstructing the same predicate the function uses.
    let rel_pages: u32 = 100_000;
    let nonempty_pages: u32 = 90_000;
    let possibly_freeable = rel_pages - nonempty_pages; // 10_000
    assert!(
        possibly_freeable > 0
            && (possibly_freeable >= crate::core::REL_TRUNCATE_MINIMUM
                || possibly_freeable >= rel_pages / crate::core::REL_TRUNCATE_FRACTION)
    );

    // Below both thresholds -> no truncation.
    let rel_pages2: u32 = 100;
    let nonempty2: u32 = 99;
    let pf2 = rel_pages2 - nonempty2; // 1
    assert!(
        !(pf2 > 0
            && (pf2 >= crate::core::REL_TRUNCATE_MINIMUM
                || pf2 >= rel_pages2 / crate::core::REL_TRUNCATE_FRACTION))
    );
}

#[test]
fn failsafe_every_pages_value() {
    // (4 GiB / 8 KiB) blocks.
    assert_eq!(
        crate::core::FAILSAFE_EVERY_PAGES,
        (4u64 * 1024 * 1024 * 1024 / 8192) as u32
    );
    assert_eq!(
        crate::core::VACUUM_FSM_EVERY_PAGES,
        (8u64 * 1024 * 1024 * 1024 / 8192) as u32
    );
}
