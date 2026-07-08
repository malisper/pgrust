use super::*;
use init_small::globals as g;
use std::sync::atomic::AtomicU32 as StdAtomicU32;
use std::sync::{Mutex, Once, OnceLock};
use types_storage::multixact::MultiXactStatus::*;

static XLOG_INSERTS: Mutex<Vec<(u8, u8, Vec<u8>)>> = Mutex::new(Vec::new());
static IN_PROGRESS_XIDS: Mutex<Vec<TransactionId>> = Mutex::new(Vec::new());
static CURRENT_XID: StdAtomicU32 = StdAtomicU32::new(0);
static IN_RECOVERY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn shmem_registry() -> &'static Mutex<std::collections::HashMap<String, usize>> {
    static R: OnceLock<Mutex<std::collections::HashMap<String, usize>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

// Datadir-shaped fixture, as C initdb + one committed multixact would leave
// it: multi 1 = members {100 sh, 101 keysh, 102 nokeyupd} at offsets 1..4.
fn write_fixture_segments(dir: &std::path::Path) {
    let mut offsets_page = vec![0u8; BLCKSZ];
    offsets_page[4..8].copy_from_slice(&1u32.to_ne_bytes()); // multi 1 -> offset 1
    offsets_page[8..12].copy_from_slice(&4u32.to_ne_bytes()); // multi 2 -> offset 4
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
        xloginsert_seams::xlog_insert::set(|rmid, info, fragments| {
            let mut data = Vec::new();
            for f in fragments {
                data.extend_from_slice(f);
            }
            XLOG_INSERTS.lock().unwrap().push((rmid, info, data));
            Ok(0x1000)
        });
        varsup_seams::advance_next_full_transaction_id_past_xid::set(|_| Ok(()));

        xact_seams::transaction_id_is_current_transaction_id::set(|xid| {
            CURRENT_XID.load(std::sync::atomic::Ordering::Relaxed) == xid
        });
        xact_seams::is_transaction_or_transaction_block::set(|| false);
        procarray_seams::transaction_id_is_in_progress::set(|xid| {
            Ok(IN_PROGRESS_XIDS.lock().unwrap().contains(&xid))
        });
        dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".to_string())));

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
