// TruncateMultiXact vacuum-lane port (multixact.c): the post-VACUUM
// truncation path vac_truncate_clog drives once relminmxid advances.
// Separate test binary: truncation deletes SLRU segment files, so it gets
// its own datadir instead of sharing the unit tests' fixture.
#![allow(non_snake_case)]

use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64};
use std::sync::{Mutex, Once, OnceLock};

use init_small::globals as g;
use multixact::*;
use types_core::MultiXactId;
use types_storage::multixact::MultiXactMember;
use types_storage::multixact::MultiXactStatus::*;
use types_storage::storage::{DELAY_CHKPT_START, NUM_AUXILIARY_PROCS};

static XLOG_INSERTS: Mutex<Vec<(u8, u8, Vec<u8>)>> = Mutex::new(Vec::new());
static XLOG_FLUSHES: AtomicU64 = AtomicU64::new(0);
static DELAY_CHKPT_FLAGS: AtomicI32 = AtomicI32::new(0);
static DELAY_CHKPT_ADDS: AtomicI32 = AtomicI32::new(0);
static DELAY_CHKPT_CLEARS: AtomicI32 = AtomicI32::new(0);
// The WAL truncate record must be inserted while DELAY_CHKPT_START is held
// (multixact.c: the record must be replayed if the truncation survives).
static TRUNCATE_WAL_UNDER_DELAY: AtomicBool = AtomicBool::new(false);

fn shmem_registry() -> &'static Mutex<std::collections::HashMap<String, usize>> {
    static R: OnceLock<Mutex<std::collections::HashMap<String, usize>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        let tmp =
            std::env::temp_dir().join(format!("multixact_truncate_test_{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("pg_multixact/offsets")).unwrap();
        std::fs::create_dir_all(tmp.join("pg_multixact/members")).unwrap();
        std::env::set_current_dir(&tmp).unwrap();

        g::SetMaxConnections(8);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(17);
        g::SetMyProcPid(4242);
        g::SetMyProcNumber(0);
        g::set_multixact_offset_buffers(16);
        g::set_multixact_member_buffers(16);

        use std::sync::atomic::AtomicI32 as I32;
        static MAX_PREPARED: I32 = I32::new(2);
        static FREEZE_MAX_AGE: I32 = I32::new(400_000_000);
        guc_tables::vars::max_prepared_xacts.install(guc_tables::GucVarAccessors {
            get: || MAX_PREPARED.load(Relaxed),
            set: |v| MAX_PREPARED.store(v, Relaxed),
        });
        guc_tables::vars::autovacuum_multixact_freeze_max_age.install(
            guc_tables::GucVarAccessors {
                get: || FREEZE_MAX_AGE.load(Relaxed),
                set: |v| FREEZE_MAX_AGE.store(v, Relaxed),
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

        xlogutils_seams::in_recovery::set(|| false);
        transam_xlog_seams::recovery_in_progress::set(|| false);
        transam_xlog_seams::xlog_flush::set(|_| {
            XLOG_FLUSHES.fetch_add(1, Relaxed);
            Ok(())
        });
        transam_xlog_seams::count_ckpt_slru_written::set(|| {});
        xloginsert_seams::xlog_insert::set(|rmid, info, fragments| {
            let mut data = Vec::new();
            for f in fragments {
                data.extend_from_slice(f);
            }
            if rmid == RM_MULTIXACT_ID && info == XLOG_MULTIXACT_TRUNCATE_ID {
                TRUNCATE_WAL_UNDER_DELAY.store(
                    DELAY_CHKPT_FLAGS.load(Relaxed) & DELAY_CHKPT_START != 0,
                    Relaxed,
                );
            }
            XLOG_INSERTS.lock().unwrap().push((rmid, info, data));
            Ok(0x1000)
        });
        varsup_seams::advance_next_full_transaction_id_past_xid::set(|_| Ok(()));

        xact_seams::transaction_id_is_current_transaction_id::set(|_| false);
        xact_seams::is_transaction_or_transaction_block::set(|| false);
        procarray_seams::transaction_id_is_in_progress::set(|_| Ok(false));
        dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".to_string())));

        twophase_seams::two_phase_get_dummy_proc_number::set(|_, _| {
            Ok(g::MaxBackends() + NUM_AUXILIARY_PROCS + 1)
        });
        twophase_seams::register_two_phase_record::set(|_, _, _| Ok(()));

        lmgr_proc_seams::my_proc_add_delay_chkpt_flags::set(|flags| {
            DELAY_CHKPT_ADDS.fetch_add(1, Relaxed);
            DELAY_CHKPT_FLAGS.fetch_or(flags, Relaxed)
        });
        lmgr_proc_seams::my_proc_clear_delay_chkpt_flags::set(|flags| {
            DELAY_CHKPT_CLEARS.fetch_add(1, Relaxed);
            DELAY_CHKPT_FLAGS.fetch_and(!flags, Relaxed);
        });

        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);

        lwlock::CreateLWLocks(false).unwrap();

        multixact::init_seams();
        MultiXactShmemInit().unwrap();

        // initdb boot order: bootstrap page 0 of both SLRUs, then start from
        // the bootstrap "checkpoint" (nextMulti 1, nextOffset 0, oldest 1).
        BootStrapMultiXact().unwrap();
        MultiXactSetNextMXact(1, 0).unwrap();
        SetMultiXactIdLimit(1, 1, true).unwrap();
        StartupMultiXact().unwrap();
        TrimMultiXact().unwrap();
    });
}

fn segment_exists(kind: &str, name: &str) -> bool {
    std::path::Path::new(&format!("pg_multixact/{kind}/{name}")).exists()
}

// Enough multis to cross an offsets segment boundary (SLRU segment = 32
// pages x 2048 offsets/page = 65536 multis) and put newOldestOffset in
// members segment 2, so truncation deletes real files on both SLRUs.
const NMULTIS: u32 = 70_000;
const NEW_OLDEST: MultiXactId = 66_000;
const NEW_OLDEST_DB: u32 = 5;

// Multi m holds members {xid 1000+2(m-1) sh, xid 1001+2(m-1) keysh} at
// offset 2m-1 (the first create skips reserved member slot 0).
fn expected_offset(m: MultiXactId) -> u32 {
    2 * m - 1
}

#[test]
fn truncate_multixact_vacuum_lane() {
    setup();

    MultiXactIdSetOldestMember().unwrap();
    for i in 0..NMULTIS {
        let mut members = [
            MultiXactMember { xid: 1000 + 2 * i, status: MultiXactStatusForShare },
            MultiXactMember { xid: 1001 + 2 * i, status: MultiXactStatusForKeyShare },
        ];
        let multi = MultiXactIdCreateFromMembers(&mut members).unwrap();
        assert_eq!(multi, i + 1);
    }
    AtEOXact_MultiXact();
    CheckPointMultiXact().unwrap();

    // Baseline: both SLRUs span multiple segments on disk.
    for (kind, name) in
        [("offsets", "0000"), ("offsets", "0001"), ("members", "0000"), ("members", "0001"), ("members", "0002")]
    {
        assert!(segment_exists(kind, name), "expected pg_multixact/{kind}/{name} before truncation");
    }
    assert_eq!(ReadMultiXactIdRange().unwrap(), (1, NMULTIS + 1));

    // No-op guard: not past the current oldest — no WAL, no state change.
    let inserts_before = XLOG_INSERTS.lock().unwrap().len();
    TruncateMultiXact(1, NEW_OLDEST_DB).unwrap();
    assert_eq!(XLOG_INSERTS.lock().unwrap().len(), inserts_before);
    assert_eq!(ReadMultiXactIdRange().unwrap(), (1, NMULTIS + 1));

    // The issue #60 path: relminmxid advanced database-wide, vacuum truncates.
    TruncateMultiXact(NEW_OLDEST, NEW_OLDEST_DB).unwrap();

    // WAL: exactly one XLOG_MULTIXACT_TRUNCATE_ID, flushed, written while
    // DELAY_CHKPT_START was held, and the flag pair balanced afterwards.
    let inserts = XLOG_INSERTS.lock().unwrap();
    let truncates: Vec<_> = inserts
        .iter()
        .filter(|(rmid, info, _)| *rmid == RM_MULTIXACT_ID && *info == XLOG_MULTIXACT_TRUNCATE_ID)
        .collect();
    assert_eq!(truncates.len(), 1);
    let data = &truncates[0].2;
    assert_eq!(data.len(), 20);
    assert_eq!(u32::from_ne_bytes(data[0..4].try_into().unwrap()), NEW_OLDEST_DB);
    assert_eq!(u32::from_ne_bytes(data[4..8].try_into().unwrap()), 1); // startTruncOff
    assert_eq!(u32::from_ne_bytes(data[8..12].try_into().unwrap()), NEW_OLDEST); // endTruncOff
    assert_eq!(u32::from_ne_bytes(data[12..16].try_into().unwrap()), expected_offset(1)); // startTruncMemb
    assert_eq!(u32::from_ne_bytes(data[16..20].try_into().unwrap()), expected_offset(NEW_OLDEST)); // endTruncMemb
    drop(inserts);
    assert_eq!(XLOG_FLUSHES.load(Relaxed), 1);
    assert!(TRUNCATE_WAL_UNDER_DELAY.load(Relaxed));
    assert_eq!(DELAY_CHKPT_ADDS.load(Relaxed), 1);
    assert_eq!(DELAY_CHKPT_CLEARS.load(Relaxed), 1);
    assert_eq!(DELAY_CHKPT_FLAGS.load(Relaxed), 0);
    assert_eq!(g::CritSectionCount(), 0);

    // In-memory horizon advanced.
    assert_eq!(ReadMultiXactIdRange().unwrap(), (NEW_OLDEST, NMULTIS + 1));

    // Offsets: segment 0000 (multis < 65536) deleted, 0001 retained.
    // Members: endTruncMemb sits in segment 2, so 0000/0001 deleted.
    assert!(!segment_exists("offsets", "0000"));
    assert!(segment_exists("offsets", "0001"));
    assert!(!segment_exists("members", "0000"));
    assert!(!segment_exists("members", "0001"));
    assert!(segment_exists("members", "0002"));

    // Surviving multis stay readable through the SLRU path.
    let mut got: Vec<MultiXactMember> = Vec::new();
    let n = GetMultiXactIdMembers(NEW_OLDEST, false, false, &mut |ms| {
        got.extend_from_slice(ms);
    })
    .unwrap();
    assert_eq!(n, 2);
    let mut xids: Vec<u32> = got.iter().map(|m| m.xid).collect();
    xids.sort_unstable();
    assert_eq!(xids, vec![1000 + 2 * (NEW_OLDEST - 1), 1001 + 2 * (NEW_OLDEST - 1)]);

    // Truncated-away multis now fail the wraparound range check.
    let err = GetMultiXactIdMembers(5, false, false, &mut |_| {});
    assert!(err.is_err(), "reading a truncated multixact must error");

    // Idempotent: re-truncating to the same horizon is the C-exact early
    // return — no second WAL record, no flag churn.
    TruncateMultiXact(NEW_OLDEST, NEW_OLDEST_DB).unwrap();
    assert_eq!(XLOG_FLUSHES.load(Relaxed), 1);
    assert_eq!(DELAY_CHKPT_ADDS.load(Relaxed), 1);
}
