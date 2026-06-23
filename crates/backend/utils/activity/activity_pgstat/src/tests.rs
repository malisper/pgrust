//! F0 carrier tests: the EntryRef/pending/local-state model and the kind-info
//! callback registry assemble and the three fixed kinds register with the right
//! callback slots populated.

use crate::entry_ref::{
    PgStat_EntryRef, PgStat_EntryRefHashEntry, PgStat_LocalState, PgStat_PendingState,
};
use crate::kind_info::{KindInfoBuilder, PgStat_KindCallbacks, PgStat_KindInfoTable};
use crate::registry;

use ::types_pgstat::activity_pgstat::{
    PgStat_Kind, PGSTAT_KIND_ARCHIVER, PGSTAT_KIND_BGWRITER, PGSTAT_KIND_CHECKPOINTER,
    PGSTAT_KIND_RELATION,
};
use ::types_pgstat::pgstat_internal::{PgStat_HashKey, PgStat_KindInfo};

fn dummy_info(name: &'static str, fixed: bool) -> PgStat_KindInfo {
    PgStat_KindInfo {
        fixed_amount: fixed,
        accessed_across_databases: false,
        write_to_file: true,
        shared_size: 0,
        snapshot_ctl_off: 0,
        shared_ctl_off: 0,
        shared_data_off: 0,
        shared_data_len: 0,
        pending_size: 0,
        name,
    }
}

#[test]
fn entry_ref_default_is_unbound() {
    let r = PgStat_EntryRef::new();
    assert!(r.shared_entry.is_null());
    assert!(r.shared_stats.is_null());
    assert!(r.pending.is_none());
    assert_eq!(r.generation, 0);
}

#[test]
fn pending_block_is_type_erased() {
    // The `void *pending` carries an arbitrary per-kind pending struct.
    #[derive(Debug, PartialEq)]
    struct MyPending {
        n: u64,
    }
    let mut r = PgStat_EntryRef::new();
    r.pending = Some(Box::new(MyPending { n: 42 }));
    let got = r.pending.as_ref().unwrap().downcast_ref::<MyPending>().unwrap();
    assert_eq!(got, &MyPending { n: 42 });
}

#[test]
fn local_and_pending_state_construct() {
    let local = PgStat_LocalState::new();
    assert!(local.shmem.is_null());
    assert!(local.dsa.is_null());
    assert!(local.shared_hash.is_null());

    let pend = PgStat_PendingState::new();
    assert!(pend.entry_ref_hash.is_empty());
}

#[test]
fn entry_ref_hash_entry_round_trips() {
    let mut pend = PgStat_PendingState::new();
    let key = PgStat_HashKey {
        kind: PGSTAT_KIND_RELATION,
        dboid: 5,
        objid: 99,
    };
    pend.entry_ref_hash.insert(
        key,
        PgStat_EntryRefHashEntry {
            key,
            entry_ref: Box::new(PgStat_EntryRef::new()),
        },
    );
    assert_eq!(pend.entry_ref_hash.get(&key).unwrap().key.objid, 99);
}

#[test]
fn kind_table_register_get_iter() {
    let mut t = PgStat_KindInfoTable::new();
    assert!(t.get(PGSTAT_KIND_BGWRITER).is_none());

    let (k, full) = KindInfoBuilder::new(PGSTAT_KIND_BGWRITER, dummy_info("bgwriter", true))
        .init_shmem_cb(|_ctl| {})
        .build();
    t.register(k, full);

    let got = t.get(PGSTAT_KIND_BGWRITER).expect("registered");
    assert_eq!(got.info.name, "bgwriter");
    assert!(got.cb.init_shmem_cb.is_some());
    assert!(got.cb.flush_pending_cb.is_none());

    assert_eq!(t.iter().count(), 1);
}

#[test]
#[should_panic(expected = "registered twice")]
fn kind_table_rejects_double_register() {
    let mut t = PgStat_KindInfoTable::new();
    let (k, full) = KindInfoBuilder::new(PGSTAT_KIND_BGWRITER, dummy_info("bgwriter", true)).build();
    t.register(k, full);
    let (k2, full2) =
        KindInfoBuilder::new(PGSTAT_KIND_BGWRITER, dummy_info("bgwriter", true)).build();
    t.register(k2, full2);
}

#[test]
#[should_panic(expected = "not a builtin")]
fn kind_table_rejects_non_builtin() {
    let mut t = PgStat_KindInfoTable::new();
    let (_k, full) = KindInfoBuilder::new(PgStat_Kind(30), dummy_info("custom", true)).build();
    t.register(PgStat_Kind(30), full);
}

#[test]
fn callbacks_default_all_none() {
    let cb = PgStat_KindCallbacks::default();
    assert!(cb.init_backend_cb.is_none());
    assert!(cb.flush_pending_cb.is_none());
    assert!(cb.snapshot_cb.is_none());
    assert!(cb.reset_all_cb.is_none());
}

#[test]
fn init_seams_registers_three_fixed_kinds_proof_of_shape() {
    // Drive the real proof-of-shape registration, then read the building stage
    // back (without sealing, so the global OnceLock stays free for production).
    registry::reset_for_test();
    crate::register_fixed_kinds();

    // Re-run registration into a private table by inspecting the building stage.
    // We confirm via the public builder shape: each fixed kind has the three
    // fixed-kind callbacks installed and no variable-kind callbacks.
    for &kind in &[
        PGSTAT_KIND_BGWRITER,
        PGSTAT_KIND_ARCHIVER,
        PGSTAT_KIND_CHECKPOINTER,
    ] {
        let full = registry::take_building_kind_for_test(kind)
            .unwrap_or_else(|| panic!("{:?} not registered by init_seams", kind));
        assert!(full.info.fixed_amount);
        assert!(full.cb.init_shmem_cb.is_some(), "{:?} init_shmem_cb", kind);
        assert!(full.cb.reset_all_cb.is_some(), "{:?} reset_all_cb", kind);
        assert!(full.cb.snapshot_cb.is_some(), "{:?} snapshot_cb", kind);
        assert!(full.cb.flush_pending_cb.is_none());
        assert!(full.info.shared_size > 0, "{:?} shared_size", kind);
        // The on-disk stats-file codec slots: every write_to_file fixed kind
        // must serialize/deserialize its typed snapshot/shmem field, and the
        // serialized length equals shared_data_len.
        assert!(full.cb.read_fixed_cb.is_some(), "{:?} read_fixed_cb", kind);
        assert!(full.cb.write_fixed_cb.is_some(), "{:?} write_fixed_cb", kind);
        assert!(full.info.shared_data_len > 0, "{:?} shared_data_len", kind);
        let snap = ::types_pgstat::pgstat_internal::PgStat_Snapshot::default();
        let blob = full.cb.write_fixed_cb.as_ref().unwrap()(&snap);
        assert_eq!(
            blob.len(),
            full.info.shared_data_len as usize,
            "{:?}: write_fixed_cb length != shared_data_len",
            kind
        );
    }
    registry::reset_for_test();
}

/// The byte-image helpers round-trip a representative POD stats struct.
#[test]
fn serialize_pod_round_trips_stats_struct() {
    use crate::kind_info::{pgstat_deserialize_pod, pgstat_serialize_pod};
    use ::types_pgstat::activity_pgstat::PgStat_ArchiverStats;

    let mut a = PgStat_ArchiverStats::default();
    a.archived_count = 42;
    a.failed_count = 7;
    a.last_archived_wal[..4].copy_from_slice(b"0001");
    a.last_archived_timestamp = 0x0011_2233_4455_6677;

    let bytes = pgstat_serialize_pod(&a);
    assert_eq!(bytes.len(), core::mem::size_of::<PgStat_ArchiverStats>());
    let b: PgStat_ArchiverStats = pgstat_deserialize_pod(&bytes);
    assert_eq!(a, b);
}

/// The Rust `#[repr(C)]` stats structs have exactly the byte sizes a real
/// C-produced PostgreSQL 18.3 `pgstat.stat` uses for each entry's
/// `shared_data_len`. These constants were derived by decoding a real initdb
/// stats file (`pg_stat/pgstat.stat`); a mismatch means the on-disk byte image
/// would not line up with C, silently corrupting restored counters.
#[test]
fn ondisk_stats_struct_sizes_match_c_layout() {
    use ::types_pgstat::activity_pgstat as a;
    use ::types_pgstat::pgstat_internal::SLRU_NUM_ELEMENTS;

    assert_eq!(core::mem::size_of::<a::PgStat_ArchiverStats>(), 136, "archiver");
    assert_eq!(
        core::mem::size_of::<
            ::types_pgstat::backend_utils_activity_pgstat_bgwriter::PgStat_BgWriterStats,
        >(),
        32,
        "bgwriter"
    );
    assert_eq!(
        core::mem::size_of::<a::PgStat_CheckpointerStats>(),
        88,
        "checkpointer"
    );
    assert_eq!(core::mem::size_of::<a::PgStat_IO>(), 51848, "io");
    assert_eq!(
        core::mem::size_of::<[a::PgStat_SLRUStats; SLRU_NUM_ELEMENTS]>(),
        512,
        "slru"
    );
    assert_eq!(core::mem::size_of::<a::PgStat_WalStats>(), 40, "wal");
    assert_eq!(core::mem::size_of::<a::PgStat_StatDBEntry>(), 264, "database");
    assert_eq!(core::mem::size_of::<a::PgStat_StatTabEntry>(), 216, "relation");
    assert_eq!(core::mem::size_of::<a::PgStat_StatFuncEntry>(), 24, "function");
    assert_eq!(
        core::mem::size_of::<a::PgStat_StatReplSlotEntry>(),
        72,
        "replslot"
    );
    assert_eq!(core::mem::size_of::<a::PgStat_StatSubEntry>(), 80, "subscription");
    // PgStat_Backend (kind 6) has write_to_file = false and never appears in the
    // on-disk file, so its layout is not part of the statsfile contract.
}
