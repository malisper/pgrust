//! smgr_inspect — creates relation files and prints their raw on-disk layout.
//!
//! Unlike smgr_demo, this does NOT unlink at the end so you can inspect the
//! files yourself. It also prints hex dumps of page headers and verifies the
//! byte layout matches what we expect.
//!
//! Run with:  cargo run --bin smgr_inspect

use pgrust::storage::smgr::{BLCKSZ, ForkNumber, MdStorageManager, RelFileLocator, StorageManager};
use std::fs;
use std::io::Read;
use std::path::PathBuf;

fn main() {
    let base_dir = PathBuf::from(std::env::temp_dir()).join("pgrust_smgr_inspect");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let mut smgr = MdStorageManager::new(&base_dir);
    let rel = RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 1000,
    };

    // -----------------------------------------------------------------------
    // Write 3 blocks with known patterns
    // -----------------------------------------------------------------------
    smgr.open(rel).unwrap();
    smgr.create(rel, ForkNumber::Main, false).unwrap();

    // Block 0: all bytes = 0xAA
    smgr.extend(rel, ForkNumber::Main, 0, &[0xAAu8; BLCKSZ], true)
        .unwrap();
    // Block 1: all bytes = 0xBB
    smgr.extend(rel, ForkNumber::Main, 1, &[0xBBu8; BLCKSZ], true)
        .unwrap();
    // Block 2: all bytes = 0xCC
    smgr.extend(rel, ForkNumber::Main, 2, &[0xCCu8; BLCKSZ], true)
        .unwrap();

    // Also write the FSM fork with a distinct pattern.
    smgr.create(rel, ForkNumber::Fsm, false).unwrap();
    smgr.extend(rel, ForkNumber::Fsm, 0, &[0xFFu8; BLCKSZ], true)
        .unwrap();

    smgr.immedsync(rel, ForkNumber::Main).unwrap();
    smgr.immedsync(rel, ForkNumber::Fsm).unwrap();

    // -----------------------------------------------------------------------
    // Show what's on disk
    // -----------------------------------------------------------------------
    let db_dir = base_dir.join("1");
    println!("Files written to: {:?}", db_dir);
    println!();

    let mut entries: Vec<_> = fs::read_dir(&db_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    entries.sort();

    for path in &entries {
        let meta = fs::metadata(path).unwrap();
        let size_bytes = meta.len();
        let size_blocks = size_bytes / BLCKSZ as u64;
        println!("File: {:?}", path.file_name().unwrap());
        println!(
            "  size: {} bytes = {} block(s) of {} bytes",
            size_bytes, size_blocks, BLCKSZ
        );

        // Read and hex-dump the first 32 bytes of each block.
        let mut f = fs::File::open(path).unwrap();
        let mut contents = Vec::new();
        f.read_to_end(&mut contents).unwrap();

        for blk in 0..size_blocks {
            let start = blk as usize * BLCKSZ;
            let sample = &contents[start..start + 32];
            let hex: Vec<String> = sample.iter().map(|b| format!("{:02x}", b)).collect();
            // All bytes in a block should be the same value in our test.
            let first = sample[0];
            let all_same = sample.iter().all(|&b| b == first);
            println!(
                "  block {}: first 32 bytes = {} ... (all_same={})",
                blk,
                hex.join(" "),
                all_same
            );
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Cross-check: read back via smgr and compare to raw file bytes
    // -----------------------------------------------------------------------
    println!("Cross-check: smgr read vs raw file bytes");
    let raw_main_path = db_dir.join("1000");
    let raw = fs::read(&raw_main_path).unwrap();

    let expected_patterns: &[(u32, u8)] = &[(0, 0xAA), (1, 0xBB), (2, 0xCC)];
    for &(blk, expected_byte) in expected_patterns {
        // Read via smgr.
        let mut buf = vec![0u8; BLCKSZ];
        smgr.read_block(rel, ForkNumber::Main, blk, &mut buf)
            .unwrap();

        // Read from raw file bytes.
        let raw_slice = &raw[blk as usize * BLCKSZ..(blk as usize + 1) * BLCKSZ];

        // Both should match the expected pattern.
        let smgr_ok = buf.iter().all(|&b| b == expected_byte);
        let raw_ok = raw_slice.iter().all(|&b| b == expected_byte);
        let agree = buf.as_slice() == raw_slice;

        println!(
            "  block {}: smgr={} raw={} agree={}",
            blk,
            if smgr_ok { "ok" } else { "WRONG" },
            if raw_ok { "ok" } else { "WRONG" },
            if agree { "yes" } else { "NO" },
        );

        assert!(smgr_ok, "smgr read wrong for block {}", blk);
        assert!(raw_ok, "raw file wrong for block {}", blk);
        assert!(agree, "smgr and raw file disagree for block {}", blk);
    }

    // -----------------------------------------------------------------------
    // Verify file size matches nblocks
    // -----------------------------------------------------------------------
    println!();
    println!("File size consistency:");
    let main_meta = fs::metadata(db_dir.join("1000")).unwrap();
    let fsm_meta = fs::metadata(db_dir.join("1000_fsm")).unwrap();
    let main_nblocks = smgr.nblocks(rel, ForkNumber::Main).unwrap();
    let fsm_nblocks = smgr.nblocks(rel, ForkNumber::Fsm).unwrap();

    println!(
        "  main fork: file size {} bytes / {} = {} blocks, nblocks()={}  match={}",
        main_meta.len(),
        BLCKSZ,
        main_meta.len() / BLCKSZ as u64,
        main_nblocks,
        main_meta.len() / BLCKSZ as u64 == main_nblocks as u64
    );

    println!(
        "  fsm  fork: file size {} bytes / {} = {} blocks, nblocks()={}  match={}",
        fsm_meta.len(),
        BLCKSZ,
        fsm_meta.len() / BLCKSZ as u64,
        fsm_nblocks,
        fsm_meta.len() / BLCKSZ as u64 == fsm_nblocks as u64
    );

    assert_eq!(main_meta.len() / BLCKSZ as u64, main_nblocks as u64);
    assert_eq!(fsm_meta.len() / BLCKSZ as u64, fsm_nblocks as u64);

    println!();
    println!(
        "All checks passed. Files left in {:?} for manual inspection.",
        db_dir
    );
}
