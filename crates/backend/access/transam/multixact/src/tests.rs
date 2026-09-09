use super::*;
use init_small::globals as g;
use std::sync::atomic::AtomicU32 as StdAtomicU32;
use std::sync::{Mutex, Once, OnceLock};
use types_storage::multixact::MultiXactStatus::*;

thread_local! {
    static INJECT_WAL_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

static XLOG_INSERTS: Mutex<Vec<(u8, u8, Vec<u8>)>> = Mutex::new(Vec::new());
static IN_PROGRESS_XIDS: Mutex<Vec<TransactionId>> = Mutex::new(Vec::new());
static CURRENT_XID: StdAtomicU32 = StdAtomicU32::new(0);
static IN_RECOVERY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
// Test-controlled xact-state seams for SetMultiXactIdLimit's warning branch.
static TEST_IN_XACT_OR_BLOCK: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static TEST_IN_XACT_STATE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static TEST_DBNAME_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn shmem_registry() -> &'static Mutex<std::collections::HashMap<String, usize>> {
    static R: OnceLock<Mutex<std::collections::HashMap<String, usize>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

// Datadir-shaped fixture, as C initdb + one committed multixact would leave
// it: multi 1 = members {100 sh, 101 keysh, 102 nokeyupd} at offsets 1..4.
fn write_fixture_segments(dir: &std::path::Path) {
    // Two offsets pages: page 0 holds the live multis; page 1 (multis
    // 2048..4095, untouched by TrimMultiXact's zeroing of the CURRENT page)
    // carries a corrupt pair for corrupt_negative_member_count_is_c_palloc_error.
    let mut offsets_page = vec![0u8; 2 * BLCKSZ];
    offsets_page[4..8].copy_from_slice(&1u32.to_ne_bytes()); // multi 1 -> offset 1
    offsets_page[8..12].copy_from_slice(&4u32.to_ne_bytes()); // multi 2 -> offset 4
    // multi 3000 -> offset 100, multi 3001 -> offset 50: length = 50 - 100 = -50.
    let e3000 = BLCKSZ + (3000 - 2048) * 4;
    offsets_page[e3000..e3000 + 4].copy_from_slice(&100u32.to_ne_bytes());
    offsets_page[e3000 + 4..e3000 + 8].copy_from_slice(&50u32.to_ne_bytes());
    std::fs::write(dir.join("pg_multixact/offsets/0000"), &offsets_page).unwrap();

    // Group 0 layout: flags word at 0, xids at 8/12/16 for offsets 1/2/3.
    let mut members_page = vec![0u8; BLCKSZ];
    let flags: u32 = (1 << 8) | (0 << 16) | (4 << 24);
    members_page[0..4].copy_from_slice(&flags.to_ne_bytes());
    members_page[8..12].copy_from_slice(&100u32.to_ne_bytes());
    members_page[12..16].copy_from_slice(&101u32.to_ne_bytes());
    members_page[16..20].copy_from_slice(&102u32.to_ne_bytes());
    std::fs::write(dir.join("pg_multixact/members/0000"), &members_page).unwrap();
}

fn install_test_wal_seam() {
    if !xloginsert_seams::xlog_insert::is_installed() {
        xloginsert_seams::xlog_insert::set(|rmid, info, fragments| {
            if INJECT_WAL_ERROR.get() {
                assert_eq!(fragments[1].len(), 16384);
                return Err(Box::new(PgError::new(ERROR, "injected WAL error")));
            }
            let mut data = Vec::new();
            for f in fragments {
                data.extend_from_slice(f);
            }
            XLOG_INSERTS.lock().unwrap().push((rmid, info, data));
            Ok(0x1000)
        });
    }
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        let tmp = std::env::temp_dir().join(format!("multixact_test_{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("pg_multixact/offsets")).unwrap();
        std::fs::create_dir_all(tmp.join("pg_multixact/members")).unwrap();
        write_fixture_segments(&tmp);
        std::env::set_current_dir(&tmp).unwrap();

        g::SetMaxConnections(8);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(17);
        g::SetMyProcPid(4242);
        g::SetMyProcNumber(0);
        g::set_multixact_offset_buffers(16);
        g::set_multixact_member_buffers(16);

        use std::sync::atomic::{AtomicI32, Ordering::Relaxed as R};
        static MAX_PREPARED: AtomicI32 = AtomicI32::new(2);
        static FREEZE_MAX_AGE: AtomicI32 = AtomicI32::new(400_000_000);
        guc_tables::vars::max_prepared_xacts.install(guc_tables::GucVarAccessors {
            get: || MAX_PREPARED.load(R),
            set: |v| MAX_PREPARED.store(v, R),
        });
        guc_tables::vars::autovacuum_multixact_freeze_max_age.install(
            guc_tables::GucVarAccessors {
                get: || FREEZE_MAX_AGE.load(R),
                set: |v| FREEZE_MAX_AGE.store(v, R),
            },
        );

        shmem_seams::shmem_init_struct::set(|name, size| {
            let mut reg = shmem_registry().lock().unwrap();
            if let Some(&addr) = reg.get(name) {
                return Ok((std::ptr::with_exposed_provenance_mut(addr), true));
            }
            let layout = std::alloc::Layout::from_size_align(size, 128).unwrap();
            let p = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!p.is_null());
            reg.insert(name.to_string(), p.expose_provenance());
            Ok((p, false))
        });
        shmem_seams::add_size::set(|a, b| Ok(a + b));
        shmem_seams::mul_size::set(|a, b| Ok(a * b));
        shmem_seams::shmem_alloc::set(|size| {
            Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
        });

        file_seams::open_transient_file::set(|name, flags| {
            let c = std::ffi::CString::new(name).unwrap();
            Ok(unsafe { libc::open(c.as_ptr(), flags, 0o600 as libc::c_uint) })
        });
        file_seams::close_transient_file::set(|fd| unsafe { libc::close(fd) });
        file_seams::pg_fsync::set(|fd| unsafe { libc::fsync(fd) });
        file_seams::fsync_fname::set(|_, _| Ok(()));
        file_seams::data_sync_elevel::set(|e| e);
        file_seams::with_allocated_dir::set(|dirname, cb| {
            let mut ret = false;
            for entry in std::fs::read_dir(dirname).unwrap() {
                ret = cb(entry.unwrap().file_name().to_str().unwrap())?;
                if ret {
                    break;
                }
            }
            Ok(ret)
        });
        sync_seams::register_sync_request::set(|_, _, _| Ok(true));

        pgstat_seams::pgstat_get_slru_index::set(|_| 0);
        pgstat_seams::pgstat_count_slru_page_zeroed::set(|_| {});
        pgstat_seams::pgstat_count_slru_page_hit::set(|_| {});
        pgstat_seams::pgstat_count_slru_page_read::set(|_| {});
        pgstat_seams::pgstat_count_slru_page_written::set(|_| {});
        pgstat_seams::pgstat_count_slru_page_exists::set(|_| {});
        pgstat_seams::pgstat_count_slru_flush::set(|_| {});
        pgstat_seams::pgstat_count_slru_truncate::set(|_| {});
        pgstat_seams::pgstat_count_checkpointer_slru_written::set(|| {});
        waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});

        xlogutils_seams::in_recovery::set(|| {
            IN_RECOVERY.load(std::sync::atomic::Ordering::Relaxed)
        });
        transam_xlog_seams::recovery_in_progress::set(|| false);
        transam_xlog_seams::xlog_flush::set(|_| Ok(()));
        transam_xlog_seams::count_ckpt_slru_written::set(|| {});
        install_test_wal_seam();
        varsup_seams::advance_next_full_transaction_id_past_xid::set(|_| Ok(()));

        xact_seams::transaction_id_is_current_transaction_id::set(|xid| {
            CURRENT_XID.load(std::sync::atomic::Ordering::Relaxed) == xid
        });
        xact_seams::is_transaction_or_transaction_block::set(|| {
            TEST_IN_XACT_OR_BLOCK.load(std::sync::atomic::Ordering::Relaxed)
        });
        xact_seams::is_transaction_state::set(|| {
            TEST_IN_XACT_STATE.load(std::sync::atomic::Ordering::Relaxed)
        });
        procarray_seams::transaction_id_is_in_progress::set(|xid| {
            Ok(IN_PROGRESS_XIDS.lock().unwrap().contains(&xid))
        });
        dbcommands_seams::get_database_name::set(|_| {
            if TEST_DBNAME_FAILS.load(std::sync::atomic::Ordering::Relaxed) {
                Err(Box::new(types_error::PgError::error("cache lookup failed")))
            } else {
                Ok(Some("testdb".to_string()))
            }
        });

        // Dummy proc numbers start at MaxBackends + NUM_AUXILIARY_PROCS; this
        // fake returns the second prepared-xact proc number.
        twophase_seams::two_phase_get_dummy_proc_number::set(|_, _| {
            Ok(g::MaxBackends() + NUM_AUXILIARY_PROCS + 1)
        });
        twophase_seams::register_two_phase_record::set(|_, _, _| Ok(()));

        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);

        lwlock::CreateLWLocks(false).unwrap();

        init_seams();
        MultiXactShmemInit().unwrap();

        // StartupXLOG boot order over the fixture "checkpoint": nextMulti 2,
        // nextOffset 4, oldestMulti 1.
        multixact_seams::multixact_set_next_mxact::call(2, 4);
        multixact_seams::set_multixact_id_limit::call(1, 1, true);
        multixact_seams::startup_multixact::call().unwrap();
        multixact_seams::trim_multixact::call().unwrap();
    });
    g::SetMaxBackends(17);
    g::SetMyProcNumber(0);
}

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn get_members(multi: MultiXactId) -> (i32, Vec<MultiXactMember>) {
    let mut out = Vec::new();
    let n = multixact_seams::get_multi_xact_id_members::call(multi, false, false, &mut |ms| {
        out.extend_from_slice(ms);
    })
    .unwrap();
    (n, out)
}

fn sorted_xids(members: &[MultiXactMember]) -> Vec<TransactionId> {
    let mut xids: Vec<_> = members.iter().map(|m| m.xid).collect();
    xids.sort_unstable();
    xids
}

#[test]
fn constants_match_c_headers() {
    assert_eq!(MULTIXACT_OFFSETS_PER_PAGE, 2048);
    assert_eq!(MULTIXACT_MEMBERGROUPS_PER_PAGE, 409);
    assert_eq!(MULTIXACT_MEMBERS_PER_PAGE, 1636);
    assert_eq!(MAX_MEMBERS_IN_LAST_MEMBERS_PAGE, 1036);
    assert_eq!(RM_MULTIXACT_ID, 6);
    assert_eq!(MULTI_XACT_GEN_LOCK, 13);
    assert_eq!(MULTI_XACT_TRUNCATION_LOCK, 41);

    assert_eq!(MultiXactIdToOffsetPage(2048), 1);
    assert_eq!(MultiXactIdToOffsetEntry(2049), 1);
    assert_eq!(MXOffsetToMemberPage(1636), 1);
    assert_eq!(MXOffsetToFlagsOffset(1), 0);
    assert_eq!(MXOffsetToFlagsBitShift(1), 8);
    assert_eq!(MXOffsetToMemberOffset(1), 8);
    assert_eq!(MXOffsetToFlagsOffset(4), 20);
    assert_eq!(MXOffsetToMemberOffset(4), 24);
    assert_eq!(MXOffsetToMemberOffset(1636), 4);
}

#[test]
fn startup_reads_fixture_datadir_segments() {
    let _l = test_lock();
    setup();

    AtEOXact_MultiXact();
    let before = CACHE_ID_HITS.with(|h| h.get());

    let (n, members) = get_members(1);
    assert_eq!(n, 3);
    assert_eq!(members[0].xid, 100);
    assert_eq!(members[0].status, MultiXactStatusForShare);
    assert_eq!(members[1].xid, 101);
    assert_eq!(members[1].status, MultiXactStatusForKeyShare);
    assert_eq!(members[2].xid, 102);
    assert_eq!(members[2].status, MultiXactStatusNoKeyUpdate);
    assert_eq!(CACHE_ID_HITS.with(|h| h.get()), before);

    let (n2, members2) = get_members(1);
    assert_eq!(n2, 3);
    assert_eq!(sorted_xids(&members2), vec![100, 101, 102]);
    assert_eq!(CACHE_ID_HITS.with(|h| h.get()), before + 1);
}

#[test]
fn create_three_members_and_read_back_exact() {
    let _l = test_lock();
    setup();

    multixact_seams::multi_xact_id_set_oldest_member::call().unwrap();
    let mut members = [
        MultiXactMember { xid: 503, status: MultiXactStatusNoKeyUpdate },
        MultiXactMember { xid: 501, status: MultiXactStatusForKeyShare },
        MultiXactMember { xid: 502, status: MultiXactStatusForShare },
    ];
    let multi = MultiXactIdCreateFromMembers(&mut members).unwrap();
    assert!(MultiXactIdIsValid(multi));
    assert_eq!(g::CritSectionCount(), 0);

    let (rmid, info, data) = XLOG_INSERTS.lock().unwrap().last().unwrap().clone();
    assert_eq!(rmid, RM_MULTIXACT_ID);
    assert_eq!(info, XLOG_MULTIXACT_CREATE_ID);
    assert_eq!(u32::from_ne_bytes(data[0..4].try_into().unwrap()), multi);
    assert_eq!(i32::from_ne_bytes(data[8..12].try_into().unwrap()), 3);
    assert_eq!(data.len(), SIZE_OF_MULTIXACT_CREATE + 3 * SIZE_OF_MULTIXACT_MEMBER);

    // Drop the cache so the read exercises the SLRU path.
    AtEOXact_MultiXact();

    let (n, got) = get_members(multi);
    assert_eq!(n, 3);
    assert_eq!(got[0].xid, 501);
    assert_eq!(got[0].status, MultiXactStatusForKeyShare);
    assert_eq!(got[1].xid, 502);
    assert_eq!(got[1].status, MultiXactStatusForShare);
    assert_eq!(got[2].xid, 503);
    assert_eq!(got[2].status, MultiXactStatusNoKeyUpdate);
}

#[test]
fn cache_hit_on_identical_member_set_recreate() {
    let _l = test_lock();
    setup();

    multixact_seams::multi_xact_id_set_oldest_member::call().unwrap();
    let mut members = [
        MultiXactMember { xid: 701, status: MultiXactStatusForKeyShare },
        MultiXactMember { xid: 702, status: MultiXactStatusForShare },
    ];
    let first = MultiXactIdCreateFromMembers(&mut members).unwrap();

    let next_before = ReadNextMultiXactId().unwrap();
    let hits_before = CACHE_SET_HITS.with(|h| h.get());

    // Same set, different order: dedup must come from the cache probe.
    let mut permuted = [
        MultiXactMember { xid: 702, status: MultiXactStatusForShare },
        MultiXactMember { xid: 701, status: MultiXactStatusForKeyShare },
    ];
    let second = MultiXactIdCreateFromMembers(&mut permuted).unwrap();

    assert_eq!(second, first);
    assert_eq!(CACHE_SET_HITS.with(|h| h.get()), hits_before + 1);
    assert_eq!(ReadNextMultiXactId().unwrap(), next_before);

    // A different set misses the cache and burns a new id.
    let mut other = [
        MultiXactMember { xid: 701, status: MultiXactStatusForKeyShare },
        MultiXactMember { xid: 703, status: MultiXactStatusForShare },
    ];
    let third = MultiXactIdCreateFromMembers(&mut other).unwrap();
    assert_ne!(third, first);
    assert_eq!(ReadNextMultiXactId().unwrap(), next_before + 1);
}

#[test]
fn is_running_against_fake_procarray() {
    let _l = test_lock();
    setup();

    multixact_seams::multi_xact_id_set_oldest_member::call().unwrap();
    let mut members = [
        MultiXactMember { xid: 601, status: MultiXactStatusForKeyShare },
        MultiXactMember { xid: 602, status: MultiXactStatusForShare },
    ];
    let multi = MultiXactIdCreateFromMembers(&mut members).unwrap();

    IN_PROGRESS_XIDS.lock().unwrap().clear();
    CURRENT_XID.store(0, std::sync::atomic::Ordering::Relaxed);
    assert!(!multixact_seams::multi_xact_id_is_running::call(multi, false).unwrap());

    IN_PROGRESS_XIDS.lock().unwrap().push(602);
    assert!(multixact_seams::multi_xact_id_is_running::call(multi, false).unwrap());

    IN_PROGRESS_XIDS.lock().unwrap().clear();
    CURRENT_XID.store(601, std::sync::atomic::Ordering::Relaxed);
    assert!(multixact_seams::multi_xact_id_is_running::call(multi, false).unwrap());
    CURRENT_XID.store(0, std::sync::atomic::Ordering::Relaxed);

    assert!(!multixact_seams::multi_xact_id_is_running::call(InvalidMultiXactId, false).unwrap());
    // from_pgupgrade multis resolve to "no members".
    let mut out = Vec::new();
    let n = multixact_seams::get_multi_xact_id_members::call(multi, true, false, &mut |ms| {
        out.extend_from_slice(ms);
    })
    .unwrap();
    assert_eq!(n, -1);
    assert!(out.is_empty());
}

#[test]
fn offsets_page_boundary_crossed() {
    let _l = test_lock();
    setup();

    let (_, next_offset, _, _) = MultiXactGetCheckptMulti(false).unwrap();
    MultiXactSetNextMXact(MULTIXACT_OFFSETS_PER_PAGE - 1, next_offset).unwrap();

    multixact_seams::multi_xact_id_set_oldest_member::call().unwrap();
    let mut members = [
        MultiXactMember { xid: 801, status: MultiXactStatusForKeyShare },
        MultiXactMember { xid: 802, status: MultiXactStatusForShare },
        MultiXactMember { xid: 803, status: MultiXactStatusForShare },
    ];
    let multi = MultiXactIdCreateFromMembers(&mut members).unwrap();
    assert_eq!(multi, MULTIXACT_OFFSETS_PER_PAGE - 1);
    assert_eq!(MultiXactIdToOffsetPage(multi), 0);
    assert_eq!(MultiXactIdToOffsetPage(multi + 1), 1);

    AtEOXact_MultiXact();
    let (n, got) = get_members(multi);
    assert_eq!(n, 3);
    assert_eq!(sorted_xids(&got), vec![801, 802, 803]);
}

#[test]
fn members_page_boundary_crossed() {
    let _l = test_lock();
    setup();

    multixact_seams::multi_xact_id_set_oldest_member::call().unwrap();
    let count = MULTIXACT_MEMBERS_PER_PAGE as usize + 64;
    let mut members: Vec<MultiXactMember> = (0..count)
        .map(|i| MultiXactMember {
            xid: 20_000 + i as u32,
            status: MultiXactStatusForKeyShare,
        })
        .collect();
    let multi = MultiXactIdCreateFromMembers(&mut members).unwrap();

    AtEOXact_MultiXact();
    let (n, got) = get_members(multi);
    assert_eq!(n as usize, count);
    let xids = sorted_xids(&got);
    assert_eq!(xids[0], 20_000);
    assert_eq!(xids[count - 1], 20_000 + count as u32 - 1);
    assert_eq!(xids.len(), count);
    assert!(got.iter().all(|m| m.status == MultiXactStatusForKeyShare));
}

#[test]
fn update_xid_and_eoxact_reset() {
    let _l = test_lock();
    setup();

    multixact_seams::multi_xact_id_set_oldest_member::call().unwrap();
    assert!(MultiXactIdIsValid(oldest_member(0)));

    let mut members = [
        MultiXactMember { xid: 901, status: MultiXactStatusForKeyShare },
        MultiXactMember { xid: 902, status: MultiXactStatusUpdate },
    ];
    let multi = MultiXactIdCreateFromMembers(&mut members).unwrap();
    assert_eq!(MultiXactIdGetUpdateXid(multi, false).unwrap(), 902);
    assert_eq!(MultiXactIdGetUpdateXid(multi, true).unwrap(), 0);

    // Two updating members is a hard error.
    let mut bad = [
        MultiXactMember { xid: 903, status: MultiXactStatusUpdate },
        MultiXactMember { xid: 904, status: MultiXactStatusNoKeyUpdate },
    ];
    assert!(MultiXactIdCreateFromMembers(&mut bad).is_err());
    assert_eq!(g::CritSectionCount(), 0);

    AtEOXact_MultiXact();
    assert_eq!(oldest_member(0), InvalidMultiXactId);
    assert_eq!(oldest_visible(0), InvalidMultiXactId);

    multixact_seams::at_prepare_multixact::call().unwrap();
    multixact_seams::post_prepare_multixact::call(77);
}

#[test]
fn checkpoint_flushes_segments_to_disk() {
    let _l = test_lock();
    setup();

    multixact_seams::multi_xact_id_set_oldest_member::call().unwrap();
    let mut members = [
        MultiXactMember { xid: 951, status: MultiXactStatusForKeyShare },
        MultiXactMember { xid: 952, status: MultiXactStatusForShare },
    ];
    MultiXactIdCreateFromMembers(&mut members).unwrap();

    multixact_seams::check_point_multixact::call().unwrap();

    let offsets = std::fs::metadata("pg_multixact/offsets/0000").unwrap();
    let mems = std::fs::metadata("pg_multixact/members/0000").unwrap();
    assert!(offsets.len() >= BLCKSZ as u64);
    assert!(mems.len() >= BLCKSZ as u64);
}

#[test]
fn panic_in_consume_does_not_wedge_member_scratch() {
    let _l = test_lock();
    setup();

    multixact_seams::multi_xact_id_set_oldest_member::call().unwrap();
    let mut members = [
        MultiXactMember { xid: 701, status: MultiXactStatusForKeyShare },
        MultiXactMember { xid: 702, status: MultiXactStatusForShare },
    ];
    let multi = MultiXactIdCreateFromMembers(&mut members).unwrap();

    // Wedge regression (with_state class): a panic unwinding out of the
    // consumer must return the scratch to its slot, or every later call
    // panics "GetMultiXactIdMembers re-entered from its consumer" forever —
    // in release builds too.
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        GetMultiXactIdMembers(multi, false, false, &mut |_| {
            panic!("injected loud inside consume")
        })
    }));
    assert!(unwound.is_err());

    let mut out = Vec::new();
    let n = GetMultiXactIdMembers(multi, false, false, &mut |ms| {
        out.extend_from_slice(ms);
    })
    .unwrap();
    assert_eq!(n, 2);
    assert_eq!(out.len(), 2);
}

// Upstream 0a50ef09: prepared-xact OldestMemberMXactId slots come after the
// MaxBackends backend slots, NOT at the raw dummy proc number (which starts at
// MaxBackends + NUM_AUXILIARY_PROCS and would overflow into — or past — the
// OldestVisibleMXactId half).
#[test]
fn prepared_xact_oldest_member_slot_indexing() {
    let _l = test_lock();
    setup();

    AtEOXact_MultiXact();
    multixact_seams::multi_xact_id_set_oldest_member::call().unwrap();
    let my_oldest = oldest_member(0);
    assert!(MultiXactIdIsValid(my_oldest));

    multixact_seams::at_prepare_multixact::call().unwrap();
    multixact_seams::post_prepare_multixact::call(88);

    let st = MultiXactState();
    let prepared_slot = g::MaxBackends() as usize + 1;
    assert_eq!(oldest_member(prepared_slot), my_oldest);
    assert_eq!(oldest_member(0), InvalidMultiXactId);
    for i in 0..(st.perBackendXactIds.len() - st.num_member_slots) {
        assert_eq!(oldest_visible(i), InvalidMultiXactId, "visible slot {i} corrupted");
    }

    let oldest = GetOldestMultiXactId().unwrap();
    assert!(MultiXactIdPrecedesOrEquals(oldest, my_oldest));

    multixact_twophase_postcommit(88, 0, &my_oldest.to_ne_bytes()).unwrap();
    assert_eq!(oldest_member(prepared_slot), InvalidMultiXactId);

    // Recovery path lands in the same slot.
    multixact_twophase_recover(88, 0, &my_oldest.to_ne_bytes()).unwrap();
    assert_eq!(oldest_member(prepared_slot), my_oldest);
    multixact_twophase_postabort(88, 0, &my_oldest.to_ne_bytes()).unwrap();
    assert_eq!(oldest_member(prepared_slot), InvalidMultiXactId);
}


// Upstream 0852643e: a CHECKPOINT record can seed latest_page_number to the
// next offsets page before the CREATE_ID that crosses onto it is replayed, so
// the old latest_page_number==pageno pre-init check skipped the page. The fix
// probes physical existence (or the last replayed ZERO_OFF_PAGE) instead.
#[test]
fn recovery_checkpoint_race_initializes_next_offsets_page() {
    let _l = test_lock();
    setup();

    struct Restore {
        next: MultiXactId,
        off: MultiXactOffset,
        latest: i64,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            IN_RECOVERY.store(false, std::sync::atomic::Ordering::Relaxed);
            PRE_INITIALIZED_OFFSETS_PAGE.set(-1);
            LAST_INITIALIZED_OFFSETS_PAGE.set(-1);
            multixact_seams::multixact_set_next_mxact::call(self.next, self.off);
            OffsetCtl().set_latest_page_number(self.latest);
        }
    }
    let st = MultiXactState();
    let octl = OffsetCtl();
    let _restore = Restore {
        next: st.nextMXact.load(Relaxed),
        off: st.nextOffset.load(Relaxed),
        latest: octl.latest_page_number(),
    };
    let save_off = st.nextOffset.load(Relaxed);

    // Older-minor WAL laid out pages 0..=5; page 5 got its ZERO_OFF_PAGE.
    {
        let mut bank = LwGuard::acquire(SimpleLruGetBankLock(octl, 5), LW_EXCLUSIVE).unwrap();
        let slotno = ZeroMultiXactOffsetPage(5, false, &mut bank).unwrap();
        SimpleLruWritePage(octl, slotno, &mut bank).unwrap();
        bank.release().unwrap();
    }

    // Checkpoint said nextMulti = first multi of page 6; StartupMultiXact
    // seeds latest_page_number to 6 before any CREATE_ID for it is replayed.
    let boundary = 6 * MULTIXACT_OFFSETS_PER_PAGE;
    multixact_seams::multixact_set_next_mxact::call(boundary, save_off);
    multixact_seams::startup_multixact::call().unwrap();
    assert_eq!(octl.latest_page_number(), 6);
    assert!(!SimpleLruDoesPhysicalPageExist(octl, 6).unwrap());

    IN_RECOVERY.store(true, std::sync::atomic::Ordering::Relaxed);
    PRE_INITIALIZED_OFFSETS_PAGE.set(-1);
    LAST_INITIALIZED_OFFSETS_PAGE.set(-1);

    // No-ZERO_OFF_PAGE-seen branch: CREATE_ID for the last multi of page 5
    // crosses onto missing page 6; the physical-existence probe must
    // initialize it despite latest_page_number already being 6.
    let members = [MultiXactMember { xid: 950, status: MultiXactStatusForShare }];
    RecordNewMultiXact(boundary - 1, save_off, &members).unwrap();
    assert!(SimpleLruDoesPhysicalPageExist(octl, 6).unwrap());
    assert_eq!(PRE_INITIALIZED_OFFSETS_PAGE.get(), 6);
    assert_eq!(LAST_INITIALIZED_OFFSETS_PAGE.get(), 6);

    // Last-initialized-page branch: the next boundary crossing (page 6 -> 7)
    // must initialize page 7 without a physical probe.
    PRE_INITIALIZED_OFFSETS_PAGE.set(-1);
    RecordNewMultiXact(7 * MULTIXACT_OFFSETS_PER_PAGE - 1, save_off + 1, &members).unwrap();
    assert!(SimpleLruDoesPhysicalPageExist(octl, 7).unwrap());
    assert_eq!(LAST_INITIALIZED_OFFSETS_PAGE.get(), 7);
}

// A crafted 2PC state file can present a TWOPHASE_RM_MULTIXACT_ID record whose
// payload is not sizeof(MultiXactId). C only Asserts this (compiled out in
// release), so a wrong length must surface as a typed ERRCODE_DATA_CORRUPTED
// error here, never a panic.
#[test]
fn multixact_recdata_len_rejects_wrong_length() {
    let expected = core::mem::size_of::<MultiXactId>();

    // Valid length passes.
    check_multixact_recdata_len(&vec![0u8; expected], "test").unwrap();

    for bad_len in [0usize, 1, 3, 5, 8, 64] {
        if bad_len == expected {
            continue;
        }
        let err = check_multixact_recdata_len(&vec![0u8; bad_len], "test")
            .err().expect("wrong-length payload must be rejected");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);
    }
}

// GetMultiXactIdMembers derives the member count from two offset entries read
// verbatim from the offsets SLRU (untrusted on-disk / WAL-replayed bytes). A
// corrupt offset pair can yield a non-positive count (a >2^31 modular delta
// reinterpreted as i32) or one far larger than any real multixact. Before this
// fix that count drove an infallible reserve() that aborted the whole process
// (thread-per-backend => full cluster crash) on requests above MaxAllocSize.
// The count must now be classified as corrupt so the caller raises a catchable
// error instead, matching C's palloc() MaxAllocSize behavior.
#[test]
fn corrupt_member_count_is_rejected() {
    const MAX_MEMBERS: i32 =
        (MAX_ALLOC_SIZE / core::mem::size_of::<MultiXactMember>()) as i32;

    // Plausible counts for a real multixact are accepted.
    for good in [1i32, 2, 100, MAX_MEMBERS] {
        assert!(!member_count_is_corrupt(good), "count {good} should be valid");
    }

    // Zero/negative (>2^31 modular delta) and above-ceiling counts are rejected.
    // 0x0800_0000 (~134M) * 8 bytes = 1GB, past MaxAllocSize.
    for bad in [0i32, -1, i32::MIN, 0x0800_0000, 0x7FFF_FFFF, MAX_MEMBERS + 1] {
        assert!(member_count_is_corrupt(bad), "count {bad} must be rejected");
    }
}

// upstream c8d68bfd52d7 (18.5): the MultiXactId wraparound hints no longer
// suggest dropping replication slots (slots do not hold back multixact
// cleanup); both the SetMultiXactIdLimit and the GetNewMultiXactId WARNING
// carry the 18.6 text.
static WARNING_HINTS: Mutex<Vec<Option<String>>> = Mutex::new(Vec::new());

fn capture_warning_hint(err: &types_error::PgError, _output_to_server: &mut bool) {
    if err.level == types_error::WARNING {
        WARNING_HINTS.lock().unwrap().push(err.hint.clone());
    }
}

#[test]
fn wraparound_warning_hints_carry_no_replication_slot_advice() {
    let _l = test_lock();
    setup();

    let st = MultiXactState();
    let saved_next = st.nextMXact.load(Relaxed);
    let saved_offset = st.nextOffset.load(Relaxed);
    // SetMultiXactIdLimit(1, ..) puts the warn limit 40M short of the wrap
    // limit; park nextMXact just past it, on an offsets-page boundary (the
    // page is zeroed on first use, not read) and off a 64K multiple (no
    // postmaster signal).
    let wrap_limit = FirstMultiXactId.wrapping_add(MaxMultiXactId >> 1);
    let warn_limit = wrap_limit.wrapping_sub(40_000_000);
    let per_page = MULTIXACT_OFFSETS_PER_PAGE;
    let mut next = warn_limit.wrapping_add(per_page) & !(per_page - 1);
    if next % 65536 == 0 {
        next += per_page;
    }
    assert!(MultiXactIdPrecedes(warn_limit, next));
    st.nextMXact.store(next, Relaxed);
    // The offsets page the new multi lands on (RecordNewMultiXact reads it).
    ExtendMultiXactOffset(next).unwrap();

    let prev_hook = elog::set_emit_log_hook(Some(capture_warning_hint));
    WARNING_HINTS.lock().unwrap().clear();
    SetMultiXactIdLimit(FirstMultiXactId, 1, false).unwrap();

    multixact_seams::multi_xact_id_set_oldest_member::call().unwrap();
    let mut members = [
        MultiXactMember { xid: 1901, status: MultiXactStatusForKeyShare },
        MultiXactMember { xid: 1902, status: MultiXactStatusForShare },
    ];
    let multi = MultiXactIdCreateFromMembers(&mut members).unwrap();
    assert_eq!(multi, next);
    elog::set_emit_log_hook(prev_hook);
    AtEOXact_MultiXact();

    st.nextMXact.store(saved_next, Relaxed);
    st.nextOffset.store(saved_offset, Relaxed);
    SetMultiXactIdLimit(FirstMultiXactId, 1, false).unwrap();

    let hints = core::mem::take(&mut *WARNING_HINTS.lock().unwrap());
    assert_eq!(
        hints,
        vec![
            Some(
                "To avoid MultiXactId assignment failures, execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions."
                    .to_string()
            ),
            Some(
                "Execute a database-wide VACUUM in that database.\nYou might also need to commit or roll back old prepared transactions."
                    .to_string()
            ),
        ]
    );
}

// SetMultiXactIdLimit's warning branch gates the get_database_name() syscache
// lookup on the transaction state. C uses IsTransactionState() (multixact.c:2643),
// true only in TRANS_INPROGRESS; in an aborted transaction block a catalog
// lookup is invalid, so C falls back to the database OID. Using the broader
// is_transaction_or_transaction_block() (true in TBLOCK_ABORT/TBLOCK_SUBABORT)
// drove a failing syscache lookup where C emits a plain WARNING. This asserts
// the aborted-block case now takes the OID path (no syscache call) and succeeds.
#[test]
fn set_multixact_id_limit_in_aborted_block_uses_oid_not_syscache() {
    let _l = test_lock();
    setup();
    use std::sync::atomic::Ordering::Relaxed as R;

    // Aborted transaction block: in-a-block is true, IsTransactionState is
    // false, and any catalog lookup would fail.
    TEST_IN_XACT_OR_BLOCK.store(true, R);
    TEST_IN_XACT_STATE.store(false, R);
    TEST_DBNAME_FAILS.store(true, R);

    let st = MultiXactState();
    let saved_next = st.nextMXact.load(R);
    let saved_offset = st.nextOffset.load(R);

    // Park nextMXact just past the warn limit so the warning branch fires
    // (same construction as wraparound_warning_hints_carry_no_replication_slot_advice).
    let wrap_limit = FirstMultiXactId.wrapping_add(MaxMultiXactId >> 1);
    let warn_limit = wrap_limit.wrapping_sub(40_000_000);
    let per_page = MULTIXACT_OFFSETS_PER_PAGE;
    let mut next = warn_limit.wrapping_add(per_page) & !(per_page - 1);
    if next % 65536 == 0 {
        next += per_page;
    }
    assert!(MultiXactIdPrecedes(warn_limit, next));
    st.nextMXact.store(next, R);
    ExtendMultiXactOffset(next).unwrap();

    let r = SetMultiXactIdLimit(FirstMultiXactId, 1, false);

    // Restore state before asserting.
    st.nextMXact.store(saved_next, R);
    st.nextOffset.store(saved_offset, R);
    TEST_IN_XACT_OR_BLOCK.store(false, R);
    TEST_DBNAME_FAILS.store(false, R);
    SetMultiXactIdLimit(FirstMultiXactId, 1, false).unwrap();

    r.expect(
        "SetMultiXactIdLimit in an aborted transaction block must warn with the \
         database OID (IsTransactionState()==false), not attempt a syscache lookup",
    );
}

// multixact.c:2150 MultiXactShmemInit registers the state through
// ShmemInitStruct("Shared MultiXact State", MultiXactSharedStateShmemSize(),
// &found), so pg_shmem_allocations lists it; the pre-fix port Box::leak'ed it
// on the heap with no ShmemIndex row. Audit
// a186-candidate-fp-transam-multixact-p1-1e852a4898341b545ff9-1.
#[test]
fn shmem_init_registers_shared_multixact_state_in_shmem_index() {
    let _l = test_lock();
    setup();
    // The test registry maps ShmemIndex name -> arena address.
    let reg = shmem_registry().lock().unwrap();
    assert!(
        reg.contains_key("Shared MultiXact State"),
        "Shared MultiXact State is missing from the ShmemIndex: {:?}",
        reg.keys().collect::<Vec<_>>()
    );
}

// multixact.c:1656 `palloc(length * sizeof(MultiXactMember))`: a corrupt
// offsets pair whose delta reinterprets to a negative int becomes a huge
// size_t (int -> size_t conversion wraps), and palloc raises the catchable
// elog(ERROR, "invalid memory alloc request size %zu") — SQLSTATE XX000.
// The pre-fix port classified the count first and raised its own XX001
// "MultiXact %u has invalid member count %d". Audit
// a186-verified-fp-transam-multixact-p1-9b45966126bc83c484bf-1.
#[test]
fn corrupt_negative_member_count_is_c_palloc_error() {
    let _l = test_lock();
    setup();
    AtEOXact_MultiXact();
    let st = MultiXactState();
    let saved_next = st.nextMXact.load(Relaxed);
    // multi 3000 must precede nextMXact to pass the wraparound guards.
    st.nextMXact.store(3002, Relaxed);
    let r = multixact_seams::get_multi_xact_id_members::call(3000, false, false, &mut |_| {});
    st.nextMXact.store(saved_next, Relaxed);
    AtEOXact_MultiXact();
    let err = r.expect_err("length -50 must be rejected");
    // (size_t) -50 * 8 == 2^64 - 400.
    assert_eq!(err.message(), "invalid memory alloc request size 18446744073709551216");
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.level, ERROR);
}

// multixact.c:3713: an unrecognized multixact WAL opcode is
// elog(PANIC, "multixact_redo: unknown op code %u", info) — a reported PANIC
// with C's message bytes, not a Rust panic unwinding the startup thread.
// Audit a186-candidate-fp-transam-multixact-p2-34355a19140ba027ceed-1.
#[test]
fn multixact_redo_unknown_op_code_is_c_panic_report() {
    let rec = xlogreader_seams::DecodedXLogRecord { xl_info: 0x70, ..Default::default() };
    let mut state = XLogReaderState { record: Some(rec), ..Default::default() };
    let err = multixact_redo(&mut state).unwrap_err();
    assert_eq!(err.level, types_error::PANIC);
    assert_eq!(err.message(), "multixact_redo: unknown op code 112");
}

thread_local! {
    static OWNER_CLEANUPS: RefCell<Vec<Box<dyn FnOnce()>>> = const { RefCell::new(Vec::new()) };
}

fn owner_cleanup_sink(_: mcx::SessionCleanupPhase, f: Box<dyn FnOnce()>) {
    OWNER_CLEANUPS.with(|s| s.borrow_mut().push(f));
}

fn drain_owner_cleanups() {
    loop {
        let f = OWNER_CLEANUPS.with(|s| s.borrow_mut().pop());
        match f {
            Some(f) => f(),
            None => break,
        }
    }
}

#[test]
fn owned_family_session_lifecycle() {
    let _l = test_lock();
    mcx::set_session_cleanup_sink(owner_cleanup_sink);
    MXACT_CACHE.with(|c| drop(c.borrow_mut().take()));
    clear_member_scratch();
    for _ in 0..3 {
        let mut members = [MultiXactMember {
            xid: 77,
            status: MultiXactStatusForShare,
        }; 2048];
        assert_eq!(mXactCacheGetBySet(&mut members), InvalidMultiXactId);
        MXACT_CACHE.with(|c| assert!(c.borrow().is_none()));
        mXactCachePut(42, &members).unwrap();
        let mut scratch = take_member_scratch().unwrap();
        scratch.owner.with_mut(|s| {
            assert_eq!(mXactCacheGetById(42, &mut s.buf), Some(2048));
            let capacity = s.buf.capacity();
            cache_clear();
            mXactCachePut(42, &members).unwrap();
            assert_eq!(mXactCacheGetById(42, &mut s.buf), Some(2048));
            assert_eq!(s.buf.capacity(), capacity);
            drain_owner_cleanups();
            assert_eq!(s.buf[2047].xid, 77);
        });
        let mut next = take_member_scratch().unwrap();
        let generation = next.generation;
        next.owner.with_mut(|s| {
            s.buf.push(MultiXactMember {
                xid: 88,
                status: MultiXactStatusForShare,
            })
        });
        put_member_scratch(next);
        put_member_scratch(scratch);
        let next = take_member_scratch().unwrap();
        assert_eq!(next.generation, generation);
        next.owner.with(|s| assert_eq!(s.buf[0].xid, 88));
        put_member_scratch(next);
        drain_owner_cleanups();
        MEMBER_SCRATCH.with(|s| assert!(s.borrow().is_none()));
        assert!(!MEMBER_SCRATCH_INIT.get());
        MXACT_CACHE.with(|c| assert!(c.borrow().is_none()));
    }
}

#[test]
fn owned_family_consumer_panic_and_reentry() {
    let _l = test_lock();
    mcx::set_session_cleanup_sink(owner_cleanup_sink);
    clear_member_scratch();
    let members = [MultiXactMember {
        xid: 77,
        status: MultiXactStatusForShare,
    }];
    mXactCachePut(42, &members).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        GetMultiXactIdMembers(42, false, false, &mut |_| {
            GetMultiXactIdMembers(42, false, false, &mut |_| {}).unwrap();
        })
        .unwrap();
    }));
    assert!(result.is_err());
    assert_eq!(
        GetMultiXactIdMembers(42, false, false, &mut |s| {
            drain_owner_cleanups();
            assert_eq!(s[0].xid, 77);
        })
        .unwrap(),
        1
    );
    assert!(!MEMBER_SCRATCH_INIT.get());
    let scratch = take_member_scratch().unwrap();
    put_member_scratch(scratch);
    drain_owner_cleanups();
}

#[test]
fn owned_family_wal_error_retains_buffer() {
    let _l = test_lock();
    install_test_wal_seam();
    mcx::set_session_cleanup_sink(owner_cleanup_sink);
    INJECT_WAL_ERROR.set(true);
    let members = [MultiXactMember {
        xid: 77,
        status: MultiXactStatusForShare,
    }; 2048];
    assert!(write_create_wal(&[], &members).is_err());
    let capacity = WAL_SCRATCH.with(|s| s.borrow().as_ref().unwrap().with(|s| s.buf.capacity()));
    assert!(write_create_wal(&[], &members).is_err());
    WAL_SCRATCH.with(|s| {
        s.borrow()
            .as_ref()
            .unwrap()
            .with(|s| assert_eq!(s.buf.capacity(), capacity))
    });
    drain_owner_cleanups();
    WAL_SCRATCH.with(|s| assert!(s.borrow().is_none()));
    INJECT_WAL_ERROR.set(false);
}
