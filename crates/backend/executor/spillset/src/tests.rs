//! SpillSet unit suite (M3.5 inc-1, design §13). Harness mirrors
//! fd/src/tests.rs: seams + per-thread InitFileAccess + a scratch data dir
//! (temp files land under `base/pgsql_tmp` relative to cwd). The
//! cross-thread legs are the load-bearing ones: they prove the
//! write-on-thread-A / open-by-name-read-on-thread-B design against the
//! real thread-local VFD substrate.

use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Once};

use crate::{SpillFile, SpillSet};

static SETUP: Once = Once::new();
static WAL_SYNC_METHOD: AtomicI32 = AtomicI32::new(0);
// Serializes tests: they chdir into scratch data directories.
static CWD: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn setup_process() {
    SETUP.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();

        xact_seams::get_current_sub_transaction_id::set(|| 1);
        aio_seams::pgaio_closing_fd::set(|_| {});
        aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        pgstat_seams::pgstat_report_tempfile::set(|_| {});
        guc_tables::vars::wal_sync_method.install(guc_tables::GucVarAccessors {
            get: || WAL_SYNC_METHOD.load(Ordering::Relaxed),
            set: |v| WAL_SYNC_METHOD.store(v, Ordering::Relaxed),
        });
        resowner::init_seams();
        ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
        ipc_seams::before_shmem_exit::set(|_cb, _arg| Ok(()));
        postgres_seams::check_for_interrupts::set(|| Ok(()));
    });
}

/// Per-thread fd substrate: every thread that touches spill files needs its
/// own InitFileAccess + temp permission + a resource owner (the design's
/// §6.2 boundary, exercised for real here).
fn setup_thread() {
    fd::vfd::InitFileAccess();
    fd::vfd::InitTemporaryFileAccess().unwrap();
    let owner =
        resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "spillset-test")
            .unwrap();
    resowner_seams::set_current_resource_owner::call(owner);
}

fn scratch_datadir(tag: &str) -> (String, std::sync::MutexGuard<'static, ()>) {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let guard = CWD.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "pgrust_spillset_test_{}_{tag}_{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(dir.join("base/pgsql_tmp")).unwrap();
    std::env::set_current_dir(&dir).unwrap();
    (dir.to_str().unwrap().to_owned(), guard)
}

fn pattern(part: u32, epoch: u32, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (part as usize * 131 + epoch as usize * 17 + i) as u8)
        .collect()
}

#[test]
fn epoch_roundtrip_multi_part_multi_epoch() {
    setup_process();
    setup_thread();
    let (_dir, _cwd) = scratch_datadir("roundtrip");
    let ctx = mcx::MemoryContext::new("spillset-test");

    let set = SpillSet::create().unwrap();
    let mut f = SpillFile::new(Arc::clone(&set), SpillSet::file_name("agg", 1, 3), 8);

    // Epoch sizes cross the 8K BufFile buffer to exercise real writes.
    let sizes = [100usize, 9000, 40000];
    for (e, size) in sizes.iter().enumerate() {
        let mut w = f.begin_epoch(ctx.mcx()).unwrap();
        // Ascending parts; part 5 skipped in epoch 1 (absent partitions).
        for part in [0u32, 2, 5, 7] {
            if part == 5 && e == 1 {
                continue;
            }
            w.write_part(part, &pattern(part, e as u32, *size)).unwrap();
            // Same-part continuation extends the extent.
            w.write_part(part, &pattern(part, e as u32, 16)).unwrap();
        }
        w.finish().unwrap();
    }
    assert_eq!(f.epochs(), 3);
    let total: u64 = f.spilled_bytes();
    assert!(total > 0);

    for part in 0..8u32 {
        let expect: Vec<u8> = sizes
            .iter()
            .enumerate()
            .filter(|(e, _)| !(part == 5 && *e == 1))
            .flat_map(|(e, size)| {
                let mut v = pattern(part, e as u32, *size);
                v.extend(pattern(part, e as u32, 16));
                v
            })
            .collect();
        match part {
            0 | 2 | 5 | 7 => {
                assert_eq!(f.part_len(part), expect.len() as u64);
                let mut r = f.read_part(ctx.mcx(), part).unwrap().expect("has bytes");
                assert_eq!(r.total_len(), expect.len() as u64);
                let got = r.read_to_end().unwrap();
                r.close().unwrap();
                assert_eq!(got, expect, "part {part}");
            }
            _ => {
                assert_eq!(f.part_len(part), 0);
                assert!(f.read_part(ctx.mcx(), part).unwrap().is_none());
            }
        }
    }
}

#[test]
fn abandoned_epoch_is_not_committed_and_tail_is_overwritten() {
    setup_process();
    setup_thread();
    let (_dir, _cwd) = scratch_datadir("abandon");
    let ctx = mcx::MemoryContext::new("spillset-test");

    let set = SpillSet::create().unwrap();
    let mut f = SpillFile::new(Arc::clone(&set), SpillSet::file_name("agg", 1, 0), 4);

    let mut w = f.begin_epoch(ctx.mcx()).unwrap();
    w.write_part(0, &pattern(0, 0, 500)).unwrap();
    w.finish().unwrap();
    let committed = f.spilled_bytes();

    // Abandon one epoch mid-write (unwind shape: drop without finish).
    {
        let mut w = f.begin_epoch(ctx.mcx()).unwrap();
        w.write_part(0, &pattern(9, 9, 20_000)).unwrap();
        // dropped un-finished
    }
    assert_eq!(f.spilled_bytes(), committed, "abandoned epoch commits nothing");
    assert_eq!(f.epochs(), 1);

    // The next epoch lands at the committed offset, overwriting the tail.
    let mut w = f.begin_epoch(ctx.mcx()).unwrap();
    w.write_part(1, &pattern(1, 2, 700)).unwrap();
    w.finish().unwrap();

    let mut r = f.read_part(ctx.mcx(), 0).unwrap().unwrap();
    let got0 = r.read_to_end().unwrap();
    r.close().unwrap();
    assert_eq!(got0, pattern(0, 0, 500));
    let mut r = f.read_part(ctx.mcx(), 1).unwrap().unwrap();
    let got1 = r.read_to_end().unwrap();
    r.close().unwrap();
    assert_eq!(got1, pattern(1, 2, 700));
}

/// THE load-bearing leg: files written by two "worker" threads are read by
/// a third "combine" thread that never wrote — open-by-name against each
/// thread's own VFD cache (the §6.2 hazard exercised for real), with the
/// SpillFile structs moved across threads like sink Locals through SEAL.
#[test]
fn cross_thread_write_then_read_by_name() {
    setup_process();
    setup_thread();
    let (_dir, _cwd) = scratch_datadir("xthread");

    let set = SpillSet::create().unwrap();

    // Two writer threads, one file each (single-writer law).
    let files: Vec<SpillFile> = std::thread::scope(|s| {
        let mut handles = Vec::new();
        for worker in 0..2usize {
            let set = Arc::clone(&set);
            handles.push(s.spawn(move || {
                setup_thread(); // each thread arms its own fd substrate
                let ctx = mcx::MemoryContext::new("spillset-writer");
                let mut f =
                    SpillFile::new(set, SpillSet::file_name("dst", 7, worker), 4);
                for e in 0..2u32 {
                    let mut w = f.begin_epoch(ctx.mcx()).unwrap();
                    for part in 0..4u32 {
                        w.write_part(part, &pattern(part + worker as u32 * 100, e, 5000))
                            .unwrap();
                    }
                    w.finish().unwrap();
                }
                f // moves back like a Local through SEAL
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // A combine thread that never wrote reads every (file, part).
    std::thread::scope(|s| {
        let files = &files;
        s.spawn(move || {
            setup_thread();
            let ctx = mcx::MemoryContext::new("spillset-combine");
            for (worker, f) in files.iter().enumerate() {
                for part in 0..4u32 {
                    let mut expect = pattern(part + worker as u32 * 100, 0, 5000);
                    expect.extend(pattern(part + worker as u32 * 100, 1, 5000));
                    let mut r = f.read_part(ctx.mcx(), part).unwrap().unwrap();
                    let got = r.read_to_end().unwrap();
                    r.close().unwrap();
                    assert_eq!(got, expect, "worker {worker} part {part}");
                }
            }
        })
        .join()
        .unwrap();
    });
}

/// Teardown: dropping the engagement's SpillSet deletes the fileset
/// directories with every spill file in them (payload-drop semantics).
#[test]
fn spillset_drop_deletes_files() {
    setup_process();
    setup_thread();
    let (dir, _cwd) = scratch_datadir("teardown");
    let ctx = mcx::MemoryContext::new("spillset-test");

    let set = SpillSet::create().unwrap();
    let mut f = SpillFile::new(Arc::clone(&set), SpillSet::file_name("agg", 1, 0), 2);
    let mut w = f.begin_epoch(ctx.mcx()).unwrap();
    w.write_part(0, &pattern(0, 0, 4096)).unwrap();
    w.finish().unwrap();

    let tmpdir = format!("{dir}/base/pgsql_tmp");
    let count_entries = || std::fs::read_dir(&tmpdir).map(|d| d.count()).unwrap_or(0);
    assert!(count_entries() > 0, "fileset dir exists while the set lives");

    drop(f);
    drop(set); // last Arc: FileSet::drop → delete_all
    assert_eq!(count_entries(), 0, "payload drop removed every spill file");
}

/// M3.5 join batches (inc-4): per-extent claims — `part_extents` enumerates
/// exactly the committed extents and `read_extent` streams each one alone,
/// concatenating to the full partition image.
#[test]
fn extent_claims_roundtrip() {
    setup_process();
    setup_thread();
    let (_dir, _cwd) = scratch_datadir("extents");
    let ctx = mcx::MemoryContext::new("spillset-test");

    let set = SpillSet::create().unwrap();
    let mut f = SpillFile::new(Arc::clone(&set), SpillSet::file_name("hj-in", 0, 1), 4);
    // Three epochs; part 2 written in epochs 0 and 2 only.
    for e in 0..3u32 {
        let mut w = f.begin_epoch(ctx.mcx()).unwrap();
        w.write_part(1, &pattern(1, e, 3000)).unwrap();
        if e != 1 {
            w.write_part(2, &pattern(2, e, 12_000)).unwrap();
        }
        w.finish().unwrap();
    }
    let xs = f.part_extents(2);
    assert_eq!(xs.len(), 2, "one extent per contributing epoch");
    let mut got = Vec::new();
    for x in &xs {
        let mut r = f.read_extent(ctx.mcx(), *x).unwrap();
        assert_eq!(r.total_len(), x.len);
        got.extend(r.read_to_end().unwrap());
        r.close().unwrap();
    }
    let mut expect = pattern(2, 0, 12_000);
    expect.extend(pattern(2, 2, 12_000));
    assert_eq!(got, expect, "extent claims concatenate to the partition image");
    // And they agree with the whole-partition reader.
    let mut r = f.read_part(ctx.mcx(), 2).unwrap().unwrap();
    assert_eq!(r.read_to_end().unwrap(), expect);
    r.close().unwrap();
}

/// Empty file: a SpillFile that never crossed a budget creates nothing and
/// reads as empty.
#[test]
fn never_spilled_file_is_absent() {
    setup_process();
    setup_thread();
    let (_dir, _cwd) = scratch_datadir("empty");
    let ctx = mcx::MemoryContext::new("spillset-test");

    let set = SpillSet::create().unwrap();
    let f = SpillFile::new(Arc::clone(&set), SpillSet::file_name("agg", 1, 0), 4);
    assert_eq!(f.spilled_bytes(), 0);
    for part in 0..4 {
        assert!(f.read_part(ctx.mcx(), part).unwrap().is_none());
    }
}
