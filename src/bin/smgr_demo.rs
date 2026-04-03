//! smgr_demo — interactive smoke test for MdStorageManager
//!
//! Runs a series of operations against real files in a temp directory and
//! prints what it's doing at each step. Shows that the storage layer actually
//! works end-to-end, not just in unit tests.
//!
//! Run with:  cargo run --bin smgr_demo

use pgrust::smgr::{
    BlockNumber, ForkNumber, MdStorageManager, RelFileLocator, StorageManager, BLCKSZ,
};
use std::fs;
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn header(title: &str) {
    println!();
    println!("=== {} ===", title);
}

fn ok(msg: &str) {
    println!("  [ok] {}", msg);
}

fn info(msg: &str) {
    println!("       {}", msg);
}

/// Fill a page with a recognizable pattern so we can verify reads.
/// Each byte is (block * 13 + byte_index) % 251.
fn make_page(block: BlockNumber) -> Vec<u8> {
    (0..BLCKSZ)
        .map(|i| ((block as usize * 13 + i) % 251) as u8)
        .collect()
}

fn check_page(label: &str, buf: &[u8], expected_block: BlockNumber) {
    let expected = make_page(expected_block);
    assert_eq!(buf, expected.as_slice(), "{}: data mismatch for block {}", label, expected_block);
    ok(&format!("{}: data matches expected pattern", label));
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    // Use a fresh temp directory so each run starts clean.
    let base_dir = PathBuf::from(std::env::temp_dir()).join("pgrust_smgr_demo");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();
    info(&format!("base directory: {:?}", base_dir));

    let mut smgr = MdStorageManager::new(&base_dir);

    // A simple relation: db=1, rel=1000, default tablespace.
    let rel = RelFileLocator { spc_oid: 0, db_oid: 1, rel_number: 1000 };

    // -----------------------------------------------------------------------
    // 1. Create
    // -----------------------------------------------------------------------
    header("1. Create relation");
    smgr.open(rel).unwrap();
    info(&format!("exists before create: {}", smgr.exists(rel, ForkNumber::Main)));
    smgr.create(rel, ForkNumber::Main, false).unwrap();
    ok("created main fork");
    info(&format!("exists after create:  {}", smgr.exists(rel, ForkNumber::Main)));
    info(&format!("nblocks after create: {}", smgr.nblocks(rel, ForkNumber::Main).unwrap()));

    // -----------------------------------------------------------------------
    // 2. Extend — write 10 blocks
    // -----------------------------------------------------------------------
    header("2. Extend — write 10 blocks");
    for i in 0..10u32 {
        smgr.extend(rel, ForkNumber::Main, i, &make_page(i), true).unwrap();
    }
    let n = smgr.nblocks(rel, ForkNumber::Main).unwrap();
    ok(&format!("wrote 10 blocks; nblocks = {}", n));
    assert_eq!(n, 10);

    // -----------------------------------------------------------------------
    // 3. Read back every block and verify content
    // -----------------------------------------------------------------------
    header("3. Read back and verify all 10 blocks");
    let mut buf = vec![0u8; BLCKSZ];
    for i in 0..10u32 {
        smgr.read_block(rel, ForkNumber::Main, i, &mut buf).unwrap();
        check_page(&format!("block {}", i), &buf, i);
    }

    // -----------------------------------------------------------------------
    // 4. Overwrite a block
    // -----------------------------------------------------------------------
    header("4. Overwrite block 5");
    let new_data = make_page(99); // use block-99's pattern as the new content
    smgr.write_block(rel, ForkNumber::Main, 5, &new_data, true).unwrap();
    smgr.read_block(rel, ForkNumber::Main, 5, &mut buf).unwrap();
    check_page("block 5 after overwrite", &buf, 99);

    // -----------------------------------------------------------------------
    // 5. Multiple forks — create FSM and write a block
    // -----------------------------------------------------------------------
    header("5. Multiple forks — FSM fork");
    smgr.create(rel, ForkNumber::Fsm, false).unwrap();
    let fsm_data = make_page(42);
    smgr.extend(rel, ForkNumber::Fsm, 0, &fsm_data, true).unwrap();
    smgr.read_block(rel, ForkNumber::Fsm, 0, &mut buf).unwrap();
    check_page("FSM block 0", &buf, 42);
    ok(&format!("main nblocks={}, fsm nblocks={}",
        smgr.nblocks(rel, ForkNumber::Main).unwrap(),
        smgr.nblocks(rel, ForkNumber::Fsm).unwrap()));

    // -----------------------------------------------------------------------
    // 6. zero_extend — bulk-add 5 zero pages
    // -----------------------------------------------------------------------
    header("6. zero_extend — add 5 zero pages");
    let before = smgr.nblocks(rel, ForkNumber::Main).unwrap();
    smgr.zero_extend(rel, ForkNumber::Main, before, 5, true).unwrap();
    let after = smgr.nblocks(rel, ForkNumber::Main).unwrap();
    ok(&format!("nblocks: {} → {}", before, after));
    assert_eq!(after, before + 5);
    for i in before..after {
        smgr.read_block(rel, ForkNumber::Main, i, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0), "zero_extend block {} not zero", i);
    }
    ok("all 5 new blocks are zero");

    // -----------------------------------------------------------------------
    // 7. Close and re-open — verify data persists across handle close
    // -----------------------------------------------------------------------
    header("7. Close handles and re-read (persistence check)");
    smgr.close(rel, ForkNumber::Main).unwrap();
    ok("closed main fork handles");
    smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
    check_page("block 0 after close+reopen", &buf, 0);

    // -----------------------------------------------------------------------
    // 8. release_all — close everything, then re-read
    // -----------------------------------------------------------------------
    header("8. release_all — close all handles");
    smgr.release_all();
    ok("released all handles");
    smgr.read_block(rel, ForkNumber::Main, 3, &mut buf).unwrap();
    check_page("block 3 after release_all", &buf, 3);

    // -----------------------------------------------------------------------
    // 9. immedsync — fsync to durable storage
    // -----------------------------------------------------------------------
    header("9. immedsync");
    smgr.immedsync(rel, ForkNumber::Main).unwrap();
    ok("fsync'd all main fork segments");

    // -----------------------------------------------------------------------
    // 10. max_combine
    // -----------------------------------------------------------------------
    header("10. max_combine");
    use pgrust::smgr::{MAX_IO_COMBINE_LIMIT, RELSEG_SIZE};
    let mc_mid = smgr.max_combine(rel, ForkNumber::Main, 0);
    let mc_edge = smgr.max_combine(rel, ForkNumber::Main, RELSEG_SIZE - 1);
    info(&format!("max_combine at block 0:               {} (expect {})", mc_mid, MAX_IO_COMBINE_LIMIT));
    info(&format!("max_combine at block RELSEG_SIZE-1:   {} (expect 1)", mc_edge));
    assert_eq!(mc_mid, MAX_IO_COMBINE_LIMIT);
    assert_eq!(mc_edge, 1);
    ok("max_combine values correct");

    // -----------------------------------------------------------------------
    // 11. prefetch (no-op on macOS, syscall on Linux — both should succeed)
    // -----------------------------------------------------------------------
    header("11. prefetch");
    smgr.prefetch(rel, ForkNumber::Main, 0, 5).unwrap();
    ok("prefetch 5 blocks from block 0 — no error");

    // -----------------------------------------------------------------------
    // 12. fd — raw file descriptor
    // -----------------------------------------------------------------------
    #[cfg(unix)]
    {
        header("12. fd — raw file descriptor");
        let (fd, offset) = smgr.fd(rel, ForkNumber::Main, 0).unwrap();
        info(&format!("block 0: fd={}, byte_offset={}", fd, offset));
        assert!(fd >= 0);
        assert_eq!(offset, 0);
        let (fd2, offset2) = smgr.fd(rel, ForkNumber::Main, 7).unwrap();
        info(&format!("block 7: fd={}, byte_offset={}", fd2, offset2));
        assert_eq!(offset2, 7 * BLCKSZ as u64);
        ok("fd values correct");
    }

    // -----------------------------------------------------------------------
    // 13. Truncate
    // -----------------------------------------------------------------------
    header("13. Truncate to 3 blocks");
    let before_trunc = smgr.nblocks(rel, ForkNumber::Main).unwrap();
    smgr.truncate(rel, ForkNumber::Main, 3).unwrap();
    let after_trunc = smgr.nblocks(rel, ForkNumber::Main).unwrap();
    ok(&format!("nblocks: {} → {}", before_trunc, after_trunc));
    assert_eq!(after_trunc, 3);

    // Verify first 3 blocks are still intact.
    smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
    check_page("block 0 after truncate", &buf, 0);
    smgr.read_block(rel, ForkNumber::Main, 2, &mut buf).unwrap();
    check_page("block 2 after truncate", &buf, 2);

    // Block 3 must be gone.
    let err = smgr.read_block(rel, ForkNumber::Main, 3, &mut buf);
    assert!(err.is_err(), "block 3 should not exist after truncate");
    ok("block 3 correctly gone after truncate");

    // -----------------------------------------------------------------------
    // 14. Unlink
    // -----------------------------------------------------------------------
    header("14. Unlink — remove all forks");
    assert!(smgr.exists(rel, ForkNumber::Main));
    assert!(smgr.exists(rel, ForkNumber::Fsm));
    smgr.unlink(rel, None, false);
    info(&format!("main exists after unlink: {}", smgr.exists(rel, ForkNumber::Main)));
    info(&format!("fsm  exists after unlink: {}", smgr.exists(rel, ForkNumber::Fsm)));
    assert!(!smgr.exists(rel, ForkNumber::Main));
    assert!(!smgr.exists(rel, ForkNumber::Fsm));
    ok("all forks removed");

    // -----------------------------------------------------------------------
    // 15. Recovery mode — create is idempotent
    // -----------------------------------------------------------------------
    header("15. Recovery mode — idempotent create");
    let recovery_dir = base_dir.join("recovery");
    fs::create_dir_all(&recovery_dir).unwrap();
    let mut rec_smgr = MdStorageManager::new_in_recovery(&recovery_dir);
    let rec_rel = RelFileLocator { spc_oid: 0, db_oid: 1, rel_number: 2000 };
    rec_smgr.open(rec_rel).unwrap();
    rec_smgr.create(rec_rel, ForkNumber::Main, false).unwrap();
    rec_smgr.create(rec_rel, ForkNumber::Main, true).unwrap(); // is_redo=true: must not error
    ok("second create with is_redo=true succeeded");

    // -----------------------------------------------------------------------
    // Done
    // -----------------------------------------------------------------------
    println!();
    println!("All checks passed.");
    println!("Files are in {:?} — inspect them if you like.", base_dir);
    println!("Run `ls -la {:?}` to see segment files.", base_dir.join("1"));
}
