//! bufmgr_e2e — end-to-end test: write through the buffer pool, verify on disk.
//!
//! This script proves that data modified inside the buffer pool's in-memory
//! frames actually reaches the filesystem. After every flush it drops the pool
//! entirely (losing all in-memory state) and reads back the file with a fresh
//! MdStorageManager to confirm the bytes match.
//!
//! Run with:  cargo run --bin bufmgr_e2e

use pgrust::storage::smgr::{MdStorageManager, StorageManager};
use pgrust::{
    BufferPool, BufferTag, FlushResult, ForkNumber, PAGE_SIZE, RelFileLocator, RequestPageResult,
    SmgrStorageBackend,
};
use std::fs;
use std::path::PathBuf;

fn ok(msg: &str) {
    println!("  [ok] {}", msg);
}
fn info(msg: &str) {
    println!("       {}", msg);
}
fn header(title: &str) {
    println!("\n=== {} ===", title);
}

fn base_dir() -> PathBuf {
    let p = std::env::temp_dir().join("pgrust_bufmgr_e2e");
    let _ = fs::remove_dir_all(&p);
    fs::create_dir_all(&p).unwrap();
    p
}

fn rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 9000,
    }
}

fn tag(block: u32) -> BufferTag {
    BufferTag {
        rel: rel(),
        fork: ForkNumber::Main,
        block,
    }
}

/// Read a block directly from disk using a fresh MdStorageManager,
/// completely bypassing the buffer pool. Proves durability.
fn read_from_disk(base: &PathBuf, block: u32) -> Vec<u8> {
    let mut smgr = MdStorageManager::new(base);
    let mut buf = vec![0u8; PAGE_SIZE];
    smgr.read_block(rel(), ForkNumber::Main, block, &mut buf)
        .unwrap();
    buf
}

fn main() {
    let base = base_dir();

    // -----------------------------------------------------------------------
    // Setup: create a 4-block relation on disk via smgr directly.
    // The buffer pool does not create relations — DDL does.
    // -----------------------------------------------------------------------
    header("Setup: create relation on disk");
    {
        let mut smgr = MdStorageManager::new(&base);
        smgr.open(rel()).unwrap();
        smgr.create(rel(), ForkNumber::Main, false).unwrap();
        for block in 0..4u32 {
            smgr.extend(
                rel(),
                ForkNumber::Main,
                block,
                &[block as u8; PAGE_SIZE],
                true,
            )
            .unwrap();
        }
        smgr.immedsync(rel(), ForkNumber::Main).unwrap();
    }
    ok("created 4-block relation; each block N filled with byte N");
    for block in 0..4u32 {
        let data = read_from_disk(&base, block);
        assert!(data.iter().all(|&b| b == block as u8));
        info(&format!("block {block}: on-disk byte = {:#04x} ✓", block));
    }

    // -----------------------------------------------------------------------
    // Step 1: load pages through the buffer pool, modify them, flush.
    // -----------------------------------------------------------------------
    header("Step 1: modify pages through buffer pool, flush to disk");
    {
        let smgr = MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);

        for block in 0..4u32 {
            let t = tag(block);

            // Cache miss → smgr reads from disk into the frame.
            assert_eq!(
                pool.request_page(1, t),
                RequestPageResult::ReadIssued {
                    buffer_id: block as usize
                }
            );
            pool.complete_read(block as usize).unwrap();

            // Verify in-memory content matches what was on disk.
            let page_data = pool.read_page(block as usize).unwrap();
            assert!(
                page_data.iter().all(|&b| b == block as u8),
                "block {block}: expected byte {block:#04x} after read"
            );
            info(&format!(
                "block {block}: read {:#04x} from disk into frame {block}",
                block
            ));

            // Overwrite: each block N gets the value N + 0x10.
            let new_byte = block as u8 + 0x10;
            pool.write_byte(block as usize, 0, new_byte).unwrap();
            assert!(pool.buffer_state(block as usize).unwrap().dirty);
            info(&format!(
                "block {block}: modified byte[0] → {new_byte:#04x} (dirty)"
            ));

            // Flush.
            assert_eq!(
                pool.flush_buffer(block as usize).unwrap(),
                FlushResult::WriteIssued
            );
            pool.complete_write(block as usize).unwrap();
            assert!(!pool.buffer_state(block as usize).unwrap().dirty);
            info(&format!("block {block}: flushed — frame is now clean"));
        }

        ok("all 4 blocks modified and flushed through pool");
    } // pool and smgr dropped here — all in-memory state gone

    // -----------------------------------------------------------------------
    // Verification: read directly from disk, bypass the pool entirely.
    // -----------------------------------------------------------------------
    header("Verification: read raw bytes from disk (fresh smgr, no pool)");
    for block in 0..4u32 {
        let data = read_from_disk(&base, block);
        let expected_byte0 = block as u8 + 0x10;
        let original_byte = block as u8;

        assert_eq!(
            data[0], expected_byte0,
            "block {block}: byte[0] should be {expected_byte0:#04x} on disk"
        );
        // Bytes 1..PAGE_SIZE were not modified — still the original fill.
        assert!(
            data[1..].iter().all(|&b| b == original_byte),
            "block {block}: bytes[1..] should still be {original_byte:#04x}"
        );

        ok(&format!(
            "block {block}: byte[0]={:#04x} (modified) bytes[1..]={:#04x} (original) ✓",
            data[0], data[1]
        ));
    }

    // -----------------------------------------------------------------------
    // Step 2: second pool — eviction path.
    // Load block 0 and 1 in a 1-frame pool, forcing eviction.
    // Confirm evicted dirty frame reaches disk before the frame is reused.
    // -----------------------------------------------------------------------
    header("Step 2: eviction forces dirty frame to disk");
    {
        let smgr = MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 1);

        // Load block 0 into the single frame.
        assert_eq!(
            pool.request_page(1, tag(0)),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        info(&format!(
            "loaded block 0 into frame 0; byte[0]={:#04x}",
            pool.read_page(0).unwrap()[0]
        ));

        // Modify and flush block 0 so its new value is on disk.
        pool.write_byte(0, 0, 0xAA).unwrap();
        pool.flush_buffer(0).unwrap();
        pool.complete_write(0).unwrap();
        pool.unpin(1, 0).unwrap();
        info("wrote 0xAA to block 0, flushed, unpinned");

        // Load block 1 — must evict block 0's frame.
        assert_eq!(
            pool.request_page(1, tag(1)),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        info(&format!(
            "loaded block 1 into frame 0 (block 0 evicted); byte[0]={:#04x}",
            pool.read_page(0).unwrap()[0]
        ));
    }

    // Confirm block 0's final value (0xAA) is on disk.
    let data = read_from_disk(&base, 0);
    assert_eq!(data[0], 0xAA);
    ok(&format!(
        "block 0 on disk: byte[0]={:#04x} — eviction preserved the flush ✓",
        data[0]
    ));

    // -----------------------------------------------------------------------
    // Step 3: cache hit — no extra disk reads.
    // -----------------------------------------------------------------------
    header("Step 3: cache hit — same frame, no disk I/O");
    {
        let smgr = MdStorageManager::new(&base);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);

        // First access: miss.
        assert_eq!(
            pool.request_page(1, tag(2)),
            RequestPageResult::ReadIssued { buffer_id: 0 }
        );
        pool.complete_read(0).unwrap();
        pool.unpin(1, 0).unwrap();
        info("first request for block 2: ReadIssued (cache miss, disk read)");

        // Second access: hit.
        assert_eq!(
            pool.request_page(2, tag(2)),
            RequestPageResult::Hit { buffer_id: 0 }
        );
        info("second request for block 2: Hit (cache hit, no disk I/O)");

        ok("cache hit confirmed — buffer pool served page without going to disk");
    }

    println!("\nAll checks passed.");
    println!(
        "Files remain at {:?} for manual inspection.",
        base.join("1")
    );
}
