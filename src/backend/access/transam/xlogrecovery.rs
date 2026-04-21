//! WAL recovery/replay — applies WAL records to bring data pages up to date
//! after a restart. Mirrors PostgreSQL's `PerformWalRecovery`.

use std::collections::HashSet;
use std::path::Path;

use crate::BLCKSZ;
use crate::backend::access::gist::wal::gist_redo;
use crate::backend::access::nbtree::nbtxlog::btree_redo;
use crate::backend::access::transam::xact::TransactionManager;
use crate::backend::storage::page::bufpage::page_add_item;
use crate::backend::storage::smgr::md::MdStorageManager;
use crate::backend::storage::smgr::{ForkNumber, StorageManager};

use super::{
    INVALID_LSN, Lsn, RM_BTREE_ID, RM_GIST_ID, RM_HEAP_ID, RM_XACT_ID, RM_XLOG_ID, WalError,
    WalReader, XLOG_CHECKPOINT_ONLINE, XLOG_CHECKPOINT_SHUTDOWN, XLOG_FPI, XLOG_HEAP_INSERT,
    XLOG_XACT_COMMIT,
};

/// Statistics from WAL recovery, printed at startup.
pub struct RecoveryStats {
    pub records_replayed: u64,
    pub fpis: u64,
    pub inserts: u64,
    pub commits: u64,
    pub aborted: u64,
}

/// Replay all WAL records from the log file, applying page changes and
/// updating the CLOG. Called during `Database::open` before the buffer
/// pool is created.
pub fn perform_wal_recovery(
    wal_dir: &Path,
    smgr: &mut MdStorageManager,
    txns: &mut TransactionManager,
) -> Result<RecoveryStats, WalError> {
    perform_wal_recovery_from(wal_dir, smgr, txns, INVALID_LSN)
}

pub fn perform_wal_recovery_from(
    wal_dir: &Path,
    smgr: &mut MdStorageManager,
    txns: &mut TransactionManager,
    start_lsn: Lsn,
) -> Result<RecoveryStats, WalError> {
    let mut reader = if start_lsn == INVALID_LSN {
        WalReader::open(wal_dir)?
    } else {
        WalReader::open_from_lsn(wal_dir, start_lsn)?
    };
    let mut stats = RecoveryStats {
        records_replayed: 0,
        fpis: 0,
        inserts: 0,
        commits: 0,
        aborted: 0,
    };

    let mut seen_xids: HashSet<u32> = HashSet::new();
    let mut committed_xids: HashSet<u32> = HashSet::new();

    while let Some(record) = reader.next_decoded_record()? {
        let record_lsn = record.end_lsn;
        stats.records_replayed += 1;
        if record.xid != 0 {
            seen_xids.insert(record.xid);
        }

        match (record.rmid, record.info) {
            (RM_HEAP_ID, XLOG_FPI) => {
                stats.fpis += record.blocks.len() as u64;
                let block = record
                    .blocks
                    .first()
                    .ok_or_else(|| WalError::Corrupt("heap FPI missing block ref".into()))?;
                let mut page = block
                    .image
                    .as_ref()
                    .ok_or_else(|| WalError::Corrupt("heap FPI missing page image".into()))?
                    .clone();
                ensure_block_exists(smgr, block.tag.rel, block.tag.fork, block.tag.block)?;
                page[0..8].copy_from_slice(&record_lsn.to_le_bytes());
                smgr.write_block(block.tag.rel, block.tag.fork, block.tag.block, &*page, true)
                    .map_err(smgr_to_wal)?;
            }
            (RM_BTREE_ID, _) => {
                stats.fpis += record
                    .blocks
                    .iter()
                    .filter(|block| block.image.is_some())
                    .count() as u64;
                btree_redo(smgr, record_lsn, &record)?;
            }
            (RM_GIST_ID, _) => {
                stats.fpis += record
                    .blocks
                    .iter()
                    .filter(|block| block.image.is_some())
                    .count() as u64;
                gist_redo(smgr, record_lsn, &record)?;
            }
            (RM_HEAP_ID, XLOG_HEAP_INSERT) => {
                let block = record
                    .block_ref(0)
                    .or_else(|| record.blocks.first())
                    .ok_or_else(|| WalError::Corrupt("heap insert missing block ref".into()))?;
                if block.data.len() < 4 {
                    return Err(WalError::Corrupt("heap insert block data too short".into()));
                }
                let tag = block.tag;
                let offset_number = u16::from_le_bytes(block.data[0..2].try_into().unwrap());
                let tuple_len = u16::from_le_bytes(block.data[2..4].try_into().unwrap()) as usize;
                if block.data.len() < 4 + tuple_len {
                    return Err(WalError::Corrupt("heap insert tuple data truncated".into()));
                }
                let tuple_data = block.data[4..4 + tuple_len].to_vec();
                stats.inserts += 1;

                ensure_block_exists(smgr, tag.rel, tag.fork, tag.block)?;

                let mut page = [0u8; BLCKSZ];
                smgr.read_block(tag.rel, tag.fork, tag.block, &mut page)
                    .map_err(smgr_to_wal)?;

                let page_lsn = u64::from_le_bytes(page[0..8].try_into().unwrap());
                if page_lsn >= record_lsn {
                    continue; // already applied
                }

                // Apply insert delta: append tuple to page.
                let page_ref: &mut [u8; BLCKSZ] = &mut page;
                let actual_offset = page_add_item(page_ref, &tuple_data)
                    .map_err(|e| WalError::Corrupt(format!(
                        "page_add_item failed: {e:?} (rel={}, block={}, offset={}, page_lsn={}, record_lsn={}, tuple_len={})",
                        tag.rel.rel_number, tag.block, offset_number, page_lsn, record_lsn, tuple_data.len()
                    )))?;
                assert_eq!(
                    actual_offset, offset_number,
                    "WAL replay offset mismatch: expected {offset_number}, got {actual_offset} \
                     (rel={}, block={}, page_lsn={page_lsn}, record_lsn={record_lsn})",
                    tag.rel.rel_number, tag.block,
                );
                page[0..8].copy_from_slice(&record_lsn.to_le_bytes());
                smgr.write_block(tag.rel, tag.fork, tag.block, &page, true)
                    .map_err(smgr_to_wal)?;
            }
            (RM_XACT_ID, XLOG_XACT_COMMIT) => {
                stats.commits += 1;
                committed_xids.insert(record.xid);
                txns.replay_commit(record.xid);
            }
            (RM_XLOG_ID, XLOG_CHECKPOINT_ONLINE | XLOG_CHECKPOINT_SHUTDOWN) => {}
            _ => {
                return Err(WalError::Corrupt(format!(
                    "unknown WAL record during recovery: rmid={} info={}",
                    record.rmid, record.info
                )));
            }
        }
    }

    // Any transaction with WAL records but no commit record was in-flight
    // at crash time — mark it aborted.
    for &xid in &seen_xids {
        if !committed_xids.contains(&xid) {
            txns.replay_abort(xid);
            stats.aborted += 1;
        }
    }

    // Persist the updated CLOG to disk.
    txns.flush_clog().map_err(|err| {
        WalError::Corrupt(format!("failed to flush clog after recovery: {err:?}"))
    })?;

    Ok(stats)
}

/// Ensure the relation file has at least `block + 1` blocks,
/// extending with zero pages if necessary.
fn ensure_block_exists(
    smgr: &mut MdStorageManager,
    rel: crate::backend::storage::smgr::RelFileLocator,
    fork: ForkNumber,
    block: u32,
) -> Result<(), WalError> {
    let nblocks = smgr.nblocks(rel, fork).map_err(smgr_to_wal)?;
    if block >= nblocks {
        // Extend one block at a time from nblocks to block (inclusive).
        let zero_page = [0u8; BLCKSZ];
        for b in nblocks..=block {
            smgr.extend(rel, fork, b, &zero_page, true)
                .map_err(smgr_to_wal)?;
        }
    }
    Ok(())
}

fn smgr_to_wal(e: crate::backend::storage::smgr::SmgrError) -> WalError {
    WalError::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("{e:?}"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::transam::CheckpointRecord;
    use crate::backend::access::transam::xact::{TransactionManager, TransactionStatus};
    use crate::backend::access::transam::xlog::{
        WalReader, WalRecord, WalWriter, wal_segment_path_for_lsn,
    };
    use crate::backend::storage::buffer::{BufferTag, PAGE_SIZE};
    use crate::backend::storage::page::bufpage::{
        page_add_item, page_get_item, page_get_max_offset_number,
    };
    use crate::backend::storage::smgr::md::MdStorageManager;
    use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, StorageManager};
    use crate::include::access::htup::heap_page_init;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_wal_replay_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn test_rel(n: u32) -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: n,
        }
    }

    fn test_tag(rel_num: u32, block: u32) -> BufferTag {
        BufferTag {
            rel: test_rel(rel_num),
            fork: ForkNumber::Main,
            block,
        }
    }

    fn make_page_with_tuples(tuples: &[&[u8]]) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        heap_page_init(&mut page);
        for tuple in tuples {
            page_add_item(&mut page, tuple).unwrap();
        }
        page
    }

    fn setup_relation(smgr: &mut MdStorageManager, rel: RelFileLocator) {
        let _ = smgr.open(rel);
        let _ = smgr.create(rel, ForkNumber::Main, false); // ignore AlreadyExists
    }

    // ---------------------------------------------------------------
    // WalReader tests
    // ---------------------------------------------------------------

    #[test]
    fn reader_empty_wal() {
        let dir = temp_dir("reader_empty");
        let wal_dir = dir.join("pg_wal");
        let _wal = WalWriter::new(&wal_dir).unwrap();
        // Flush to ensure file exists but is empty.
        _wal.flush().unwrap();
        drop(_wal);

        let mut reader = WalReader::open(&wal_dir).unwrap();
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn reader_roundtrip_fpi() {
        let dir = temp_dir("reader_fpi");
        let wal_dir = dir.join("pg_wal");
        let wal = WalWriter::new(&wal_dir).unwrap();

        let mut page = [0u8; PAGE_SIZE];
        page[100] = 0xAB;
        page[8000] = 0xCD;
        let tag = test_tag(42, 7);
        let lsn = wal.write_record(10, tag, &page).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut reader = WalReader::open(&wal_dir).unwrap();
        let (rec_lsn, record) = reader.next_record().unwrap().unwrap();
        assert_eq!(rec_lsn, lsn);
        match record {
            WalRecord::FullPageImage {
                xid,
                tag: t,
                page: p,
            } => {
                assert_eq!(xid, 10);
                assert_eq!(t, tag);
                assert_eq!(p[100], 0xAB);
                assert_eq!(p[8000], 0xCD);
            }
            _ => panic!("expected FPI"),
        }
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn reader_roundtrip_fpi_with_hole() {
        let dir = temp_dir("reader_fpi_hole");
        let wal_dir = dir.join("pg_wal");
        let wal = WalWriter::new(&wal_dir).unwrap();

        // Create a page with a real hole (initialized page has pd_lower < pd_upper).
        let mut page = [0u8; PAGE_SIZE];
        heap_page_init(&mut page);
        page_add_item(&mut page, &[0xAA; 20]).unwrap();
        let tag = test_tag(42, 0);
        wal.write_record(1, tag, &page).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut reader = WalReader::open(&wal_dir).unwrap();
        let (_, record) = reader.next_record().unwrap().unwrap();
        match record {
            WalRecord::FullPageImage { page: p, .. } => {
                // The decompressed page should match the original.
                // Compare the item data (skip pd_lsn since the writer doesn't stamp it).
                assert_eq!(page_get_item(&p, 1).unwrap(), &[0xAA; 20]);
                assert_eq!(page_get_max_offset_number(&p).unwrap(), 1);
            }
            _ => panic!("expected FPI"),
        }
    }

    #[test]
    fn reader_roundtrip_insert_delta() {
        let dir = temp_dir("reader_insert");
        let wal_dir = dir.join("pg_wal");
        let wal = WalWriter::new(&wal_dir).unwrap();

        let mut page = [0u8; PAGE_SIZE];
        heap_page_init(&mut page);
        let tag = test_tag(42, 0);

        // First write is always FPI.
        wal.write_insert(1, tag, &page, 1, &[0xBB; 30]).unwrap();
        // Second write should be a delta.
        page_add_item(&mut page, &[0xBB; 30]).unwrap();
        let lsn2 = wal.write_insert(1, tag, &page, 2, &[0xCC; 25]).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut reader = WalReader::open(&wal_dir).unwrap();
        // Record 1: FPI
        let (_, rec1) = reader.next_record().unwrap().unwrap();
        assert!(matches!(rec1, WalRecord::FullPageImage { .. }));
        // Record 2: Insert delta
        let (rec_lsn, rec2) = reader.next_record().unwrap().unwrap();
        assert_eq!(rec_lsn, lsn2);
        match rec2 {
            WalRecord::HeapInsert {
                xid,
                tag: t,
                offset_number,
                tuple_data,
            } => {
                assert_eq!(xid, 1);
                assert_eq!(t, tag);
                assert_eq!(offset_number, 2);
                assert_eq!(tuple_data, vec![0xCC; 25]);
            }
            _ => panic!("expected HeapInsert"),
        }
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn reader_roundtrip_commit() {
        let dir = temp_dir("reader_commit");
        let wal_dir = dir.join("pg_wal");
        let wal = WalWriter::new(&wal_dir).unwrap();

        let lsn = wal.write_commit(99).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut reader = WalReader::open(&wal_dir).unwrap();
        let (rec_lsn, record) = reader.next_record().unwrap().unwrap();
        assert_eq!(rec_lsn, lsn);
        match record {
            WalRecord::XactCommit { xid } => assert_eq!(xid, 99),
            _ => panic!("expected commit"),
        }
    }

    #[test]
    fn reader_multiple_record_types() {
        let dir = temp_dir("reader_multi");
        let wal_dir = dir.join("pg_wal");
        let wal = WalWriter::new(&wal_dir).unwrap();

        let page = make_page_with_tuples(&[&[0xAA; 20]]);
        let tag = test_tag(10, 0);
        wal.write_record(1, tag, &page).unwrap();
        wal.write_commit(1).unwrap();
        wal.write_record(2, test_tag(10, 1), &page).unwrap();
        wal.write_commit(2).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut reader = WalReader::open(&wal_dir).unwrap();
        let mut count = 0;
        while reader.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 4); // FPI, commit, FPI, commit
    }

    #[test]
    fn reader_truncated_header_returns_none() {
        let dir = temp_dir("reader_truncated_header");
        let wal_dir = dir.join("pg_wal");
        fs::create_dir_all(&wal_dir).unwrap();
        // Write less than 24 bytes (the header size).
        fs::write(wal_segment_path_for_lsn(&wal_dir, 0), &[0u8; 10]).unwrap();

        let mut reader = WalReader::open(&wal_dir).unwrap();
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn reader_truncated_record_returns_none() {
        let dir = temp_dir("reader_truncated_record");
        let wal_dir = dir.join("pg_wal");
        let wal = WalWriter::new(&wal_dir).unwrap();

        let page = [0u8; PAGE_SIZE];
        wal.write_record(1, test_tag(1, 0), &page).unwrap();
        wal.flush().unwrap();
        drop(wal);

        // Truncate the file mid-record by removing last 100 bytes.
        let path = wal_segment_path_for_lsn(&wal_dir, 0);
        let data = fs::read(&path).unwrap();
        fs::write(&path, &data[..data.len() - 100]).unwrap();

        // Write a second record's header but truncate the body.
        let mut reader = WalReader::open(&wal_dir).unwrap();
        // First record should still be readable (it was complete).
        // Actually no — we truncated the first (and only) record.
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn reader_corrupt_crc_returns_none() {
        let dir = temp_dir("reader_corrupt_crc");
        let wal_dir = dir.join("pg_wal");
        let wal = WalWriter::new(&wal_dir).unwrap();

        let page = [0u8; PAGE_SIZE];
        wal.write_record(1, test_tag(1, 0), &page).unwrap();
        wal.flush().unwrap();
        drop(wal);

        // Corrupt one byte in the record.
        let path = wal_segment_path_for_lsn(&wal_dir, 0);
        let mut data = fs::read(&path).unwrap();
        data[50] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let mut reader = WalReader::open(&wal_dir).unwrap();
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn reader_valid_record_then_corrupt_stops_at_corruption() {
        let dir = temp_dir("reader_partial_corrupt");
        let wal_dir = dir.join("pg_wal");
        let wal = WalWriter::new(&wal_dir).unwrap();

        let page = [0u8; PAGE_SIZE];
        wal.write_record(1, test_tag(1, 0), &page).unwrap();
        wal.write_record(2, test_tag(1, 1), &page).unwrap();
        wal.flush().unwrap();
        let file_len = fs::metadata(wal_segment_path_for_lsn(&wal_dir, 0))
            .unwrap()
            .len();
        drop(wal);

        // Corrupt the second record (byte in the middle of record 2).
        let path = wal_segment_path_for_lsn(&wal_dir, 0);
        let mut data = fs::read(&path).unwrap();
        let mid_rec2 = (file_len / 2) as usize + 100;
        data[mid_rec2] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let mut reader = WalReader::open(&wal_dir).unwrap();
        // First record should be fine.
        assert!(matches!(
            reader.next_record().unwrap().unwrap().1,
            WalRecord::FullPageImage { .. }
        ));
        // Second record has corrupt CRC — returns None.
        assert!(reader.next_record().unwrap().is_none());
    }

    // ---------------------------------------------------------------
    // Recovery tests
    // ---------------------------------------------------------------

    fn setup_recovery(label: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = temp_dir(label);
        let wal_dir = dir.join("pg_wal");
        fs::create_dir_all(&wal_dir).unwrap();
        (dir, wal_dir)
    }

    #[test]
    fn recovery_empty_wal_is_noop() {
        let (dir, wal_dir) = setup_recovery("recovery_empty");
        let wal = WalWriter::new(&wal_dir).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        let mut txns = TransactionManager::default();
        let stats = perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();
        assert_eq!(stats.records_replayed, 0);
    }

    #[test]
    fn recovery_fpi_restores_page() {
        let (dir, wal_dir) = setup_recovery("recovery_fpi");
        let rel = test_rel(100);
        let tag = test_tag(100, 0);

        // Write FPI + commit.
        let wal = WalWriter::new(&wal_dir).unwrap();
        let page = make_page_with_tuples(&[&[0xDE; 40]]);
        wal.write_record(1, tag, &page).unwrap();
        wal.write_commit(1).unwrap();
        wal.flush().unwrap();
        drop(wal);

        // Run recovery.
        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, rel);
        let mut txns = TransactionManager::default();
        let stats = perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        assert_eq!(stats.fpis, 1);
        assert_eq!(stats.commits, 1);

        // Verify the page was written to disk.
        let mut disk_page = [0u8; PAGE_SIZE];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut disk_page)
            .unwrap();
        assert_eq!(page_get_item(&disk_page, 1).unwrap(), &[0xDE; 40]);
    }

    #[test]
    fn recovery_fpi_extends_relation() {
        let (dir, wal_dir) = setup_recovery("recovery_fpi_extend");
        let rel = test_rel(101);
        let tag = test_tag(101, 5); // block 5, but file doesn't exist yet

        let wal = WalWriter::new(&wal_dir).unwrap();
        let page = make_page_with_tuples(&[&[0x11; 10]]);
        wal.write_record(1, tag, &page).unwrap();
        wal.write_commit(1).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, rel);
        let mut txns = TransactionManager::default();
        perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        // File should have been extended to at least 6 blocks.
        assert!(smgr.nblocks(rel, ForkNumber::Main).unwrap() >= 6);
        let mut disk_page = [0u8; PAGE_SIZE];
        smgr.read_block(rel, ForkNumber::Main, 5, &mut disk_page)
            .unwrap();
        assert_eq!(page_get_item(&disk_page, 1).unwrap(), &[0x11; 10]);
    }

    #[test]
    fn recovery_insert_delta_applies_tuple() {
        let (dir, wal_dir) = setup_recovery("recovery_insert_delta");
        let rel = test_rel(102);
        let tag = test_tag(102, 0);

        let wal = WalWriter::new(&wal_dir).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        heap_page_init(&mut page);

        // First insert → FPI (with the page containing tuple 1).
        let tuple1 = [0xAA; 30];
        page_add_item(&mut page, &tuple1).unwrap();
        wal.write_insert(1, tag, &page, 1, &tuple1).unwrap();

        // Second insert → delta.
        let tuple2 = [0xBB; 25];
        page_add_item(&mut page, &tuple2).unwrap();
        wal.write_insert(1, tag, &page, 2, &tuple2).unwrap();

        wal.write_commit(1).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, rel);
        let mut txns = TransactionManager::default();
        let stats = perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        assert_eq!(stats.fpis, 1);
        assert_eq!(stats.inserts, 1);

        let mut disk_page = [0u8; PAGE_SIZE];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut disk_page)
            .unwrap();
        assert_eq!(page_get_max_offset_number(&disk_page).unwrap(), 2);
        assert_eq!(page_get_item(&disk_page, 1).unwrap(), &[0xAA; 30]);
        assert_eq!(page_get_item(&disk_page, 2).unwrap(), &[0xBB; 25]);
    }

    #[test]
    fn recovery_commit_marks_clog() {
        let (dir, wal_dir) = setup_recovery("recovery_clog");
        let wal = WalWriter::new(&wal_dir).unwrap();
        let page = [0u8; PAGE_SIZE];
        wal.write_record(5, test_tag(200, 0), &page).unwrap();
        wal.write_commit(5).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, test_rel(200));
        let mut txns = TransactionManager::default();
        perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        assert_eq!(txns.status(5), Some(TransactionStatus::Committed));
    }

    #[test]
    fn recovery_uncommitted_transaction_aborted() {
        let (dir, wal_dir) = setup_recovery("recovery_uncommitted");
        let wal = WalWriter::new(&wal_dir).unwrap();
        let page = [0u8; PAGE_SIZE];

        // Write data for xid=10 but no commit.
        wal.write_record(10, test_tag(300, 0), &page).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, test_rel(300));
        let mut txns = TransactionManager::default();
        let stats = perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        assert_eq!(stats.aborted, 1);
        assert_eq!(txns.status(10), Some(TransactionStatus::Aborted));
    }

    #[test]
    fn recovery_mixed_committed_and_uncommitted() {
        let (dir, wal_dir) = setup_recovery("recovery_mixed");
        let wal = WalWriter::new(&wal_dir).unwrap();
        let page = [0u8; PAGE_SIZE];

        wal.write_record(1, test_tag(400, 0), &page).unwrap();
        wal.write_commit(1).unwrap(); // committed
        wal.write_record(2, test_tag(400, 1), &page).unwrap();
        // xid 2 has no commit → should be aborted
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, test_rel(400));
        let mut txns = TransactionManager::default();
        let stats = perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        assert_eq!(stats.commits, 1);
        assert_eq!(stats.aborted, 1);
        assert_eq!(txns.status(1), Some(TransactionStatus::Committed));
        assert_eq!(txns.status(2), Some(TransactionStatus::Aborted));
    }

    #[test]
    fn recovery_idempotent_fpi() {
        let (dir, wal_dir) = setup_recovery("recovery_idempotent");
        let rel = test_rel(500);
        let tag = test_tag(500, 0);

        let wal = WalWriter::new(&wal_dir).unwrap();
        let page = make_page_with_tuples(&[&[0xFF; 50]]);
        wal.write_record(1, tag, &page).unwrap();
        wal.write_commit(1).unwrap();
        wal.flush().unwrap();
        drop(wal);

        // Run recovery twice.
        for _ in 0..2 {
            let mut smgr = MdStorageManager::new_in_recovery(&dir);
            setup_relation(&mut smgr, rel);
            let mut txns = TransactionManager::default();
            perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

            let mut disk_page = [0u8; PAGE_SIZE];
            smgr.read_block(rel, ForkNumber::Main, 0, &mut disk_page)
                .unwrap();
            assert_eq!(page_get_item(&disk_page, 1).unwrap(), &[0xFF; 50]);
        }
    }

    #[test]
    fn recovery_idempotent_insert_delta() {
        let (dir, wal_dir) = setup_recovery("recovery_idempotent_delta");
        let rel = test_rel(501);
        let tag = test_tag(501, 0);

        let wal = WalWriter::new(&wal_dir).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        heap_page_init(&mut page);
        let tuple1 = [0xAA; 20];
        page_add_item(&mut page, &tuple1).unwrap();
        wal.write_insert(1, tag, &page, 1, &tuple1).unwrap();
        let tuple2 = [0xBB; 20];
        page_add_item(&mut page, &tuple2).unwrap();
        wal.write_insert(1, tag, &page, 2, &tuple2).unwrap();
        wal.write_commit(1).unwrap();
        wal.flush().unwrap();
        drop(wal);

        // Run recovery twice — second run should skip already-applied records.
        for _ in 0..2 {
            let mut smgr = MdStorageManager::new_in_recovery(&dir);
            setup_relation(&mut smgr, rel);
            let mut txns = TransactionManager::default();
            perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

            let mut disk_page = [0u8; PAGE_SIZE];
            smgr.read_block(rel, ForkNumber::Main, 0, &mut disk_page)
                .unwrap();
            assert_eq!(page_get_max_offset_number(&disk_page).unwrap(), 2);
        }
    }

    #[test]
    fn recovery_multiple_pages_same_relation() {
        let (dir, wal_dir) = setup_recovery("recovery_multi_page");
        let rel = test_rel(600);

        let wal = WalWriter::new(&wal_dir).unwrap();
        for block in 0..5 {
            let page = make_page_with_tuples(&[&[block as u8; 20]]);
            wal.write_record(1, test_tag(600, block), &page).unwrap();
        }
        wal.write_commit(1).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, rel);
        let mut txns = TransactionManager::default();
        perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        for block in 0..5u32 {
            let mut disk_page = [0u8; PAGE_SIZE];
            smgr.read_block(rel, ForkNumber::Main, block, &mut disk_page)
                .unwrap();
            assert_eq!(page_get_item(&disk_page, 1).unwrap(), &[block as u8; 20]);
        }
    }

    #[test]
    fn recovery_multiple_relations() {
        let (dir, wal_dir) = setup_recovery("recovery_multi_rel");

        let wal = WalWriter::new(&wal_dir).unwrap();
        let page1 = make_page_with_tuples(&[&[0x11; 10]]);
        let page2 = make_page_with_tuples(&[&[0x22; 10]]);
        wal.write_record(1, test_tag(700, 0), &page1).unwrap();
        wal.write_record(2, test_tag(701, 0), &page2).unwrap();
        wal.write_commit(1).unwrap();
        wal.write_commit(2).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, test_rel(700));
        setup_relation(&mut smgr, test_rel(701));
        let mut txns = TransactionManager::default();
        perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        let mut p1 = [0u8; PAGE_SIZE];
        let mut p2 = [0u8; PAGE_SIZE];
        smgr.read_block(test_rel(700), ForkNumber::Main, 0, &mut p1)
            .unwrap();
        smgr.read_block(test_rel(701), ForkNumber::Main, 0, &mut p2)
            .unwrap();
        assert_eq!(page_get_item(&p1, 1).unwrap(), &[0x11; 10]);
        assert_eq!(page_get_item(&p2, 1).unwrap(), &[0x22; 10]);
    }

    #[test]
    fn recovery_next_xid_advanced() {
        let (dir, wal_dir) = setup_recovery("recovery_next_xid");
        let wal = WalWriter::new(&wal_dir).unwrap();
        let page = [0u8; PAGE_SIZE];
        wal.write_record(100, test_tag(800, 0), &page).unwrap();
        wal.write_commit(100).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, test_rel(800));
        let mut txns = TransactionManager::default();
        perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        // next_xid should be at least 101.
        let new_xid = txns.begin();
        assert!(new_xid > 100);
    }

    #[test]
    fn recovery_fpi_overwrites_even_with_higher_lsn() {
        let (dir, wal_dir) = setup_recovery("recovery_fpi_overwrite");
        let rel = test_rel(900);
        let tag = test_tag(900, 0);

        // Write a page to disk with a high LSN (simulating a torn page
        // where the LSN field was written but the rest is corrupt).
        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, rel);
        let mut existing_page = make_page_with_tuples(&[&[0xEE; 30]]);
        existing_page[0..8].copy_from_slice(&999999u64.to_le_bytes());
        smgr.extend(rel, ForkNumber::Main, 0, &existing_page, true)
            .unwrap();
        drop(smgr);

        // Write a WAL FPI with a lower LSN.
        let wal = WalWriter::new(&wal_dir).unwrap();
        let new_page = make_page_with_tuples(&[&[0x00; 30]]);
        wal.write_record(1, tag, &new_page).unwrap();
        wal.write_commit(1).unwrap();
        wal.flush().unwrap();
        drop(wal);

        // FPI should overwrite the page even though page LSN was higher
        // (torn page protection).
        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, rel);
        let mut txns = TransactionManager::default();
        perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        let mut disk_page = [0u8; PAGE_SIZE];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut disk_page)
            .unwrap();
        // FPI data should have replaced the existing page.
        assert_eq!(page_get_item(&disk_page, 1).unwrap(), &[0x00; 30]);
    }

    #[test]
    fn recovery_truncated_wal_replays_valid_prefix() {
        let (dir, wal_dir) = setup_recovery("recovery_truncated");
        let rel = test_rel(1000);

        let wal = WalWriter::new(&wal_dir).unwrap();
        let page1 = make_page_with_tuples(&[&[0x11; 20]]);
        let page2 = make_page_with_tuples(&[&[0x22; 20]]);
        wal.write_record(1, test_tag(1000, 0), &page1).unwrap();
        let first_commit_end = wal.write_commit(1).unwrap();
        let second_fpi_end = wal.write_record(2, test_tag(1000, 1), &page2).unwrap();
        wal.write_commit(2).unwrap();
        wal.flush().unwrap();
        drop(wal);

        // Truncate the file to lose the second FPI + commit.
        let path = wal_segment_path_for_lsn(&wal_dir, 0);
        let data = fs::read(&path).unwrap();
        let truncate_at =
            first_commit_end as usize + ((second_fpi_end - first_commit_end) as usize / 2);
        fs::write(&path, &data[..truncate_at]).unwrap();

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, rel);
        let mut txns = TransactionManager::default();
        let stats = perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        // Should have replayed at least the first FPI and commit.
        assert!(stats.fpis >= 1);
        assert!(stats.commits >= 1);

        // First page should be recovered.
        let mut disk_page = [0u8; PAGE_SIZE];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut disk_page)
            .unwrap();
        assert_eq!(page_get_item(&disk_page, 1).unwrap(), &[0x11; 20]);
    }

    #[test]
    fn recovery_many_inserts_to_same_page() {
        let (dir, wal_dir) = setup_recovery("recovery_many_inserts");
        let rel = test_rel(1100);
        let tag = test_tag(1100, 0);

        let wal = WalWriter::new(&wal_dir).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        heap_page_init(&mut page);

        // Insert 50 tuples — first is FPI, rest are deltas.
        for i in 0..50u8 {
            let tuple = vec![i; 20];
            page_add_item(&mut page, &tuple).unwrap();
            wal.write_insert(1, tag, &page, (i as u16) + 1, &tuple)
                .unwrap();
        }
        wal.write_commit(1).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, rel);
        let mut txns = TransactionManager::default();
        let stats = perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        assert_eq!(stats.fpis, 1);
        assert_eq!(stats.inserts, 49);

        let mut disk_page = [0u8; PAGE_SIZE];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut disk_page)
            .unwrap();
        assert_eq!(page_get_max_offset_number(&disk_page).unwrap(), 50);
        for i in 0..50u8 {
            assert_eq!(
                page_get_item(&disk_page, (i as u16) + 1).unwrap(),
                &vec![i; 20]
            );
        }
    }

    #[test]
    fn recovery_multiple_transactions_interleaved() {
        let (dir, wal_dir) = setup_recovery("recovery_interleaved");
        let rel = test_rel(1200);

        let wal = WalWriter::new(&wal_dir).unwrap();
        let page_a = make_page_with_tuples(&[&[0xAA; 20]]);
        let page_b = make_page_with_tuples(&[&[0xBB; 20]]);

        // Interleave two transactions.
        wal.write_record(1, test_tag(1200, 0), &page_a).unwrap();
        wal.write_record(2, test_tag(1200, 1), &page_b).unwrap();
        wal.write_commit(2).unwrap();
        wal.write_commit(1).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, rel);
        let mut txns = TransactionManager::default();
        perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        assert_eq!(txns.status(1), Some(TransactionStatus::Committed));
        assert_eq!(txns.status(2), Some(TransactionStatus::Committed));
    }

    #[test]
    fn recovery_fpi_full_page_no_hole() {
        let (dir, wal_dir) = setup_recovery("recovery_fpi_no_hole");
        let rel = test_rel(1300);
        let tag = test_tag(1300, 0);

        // Create a page completely filled (no hole).
        let page = [0xFE; PAGE_SIZE];
        let wal = WalWriter::new(&wal_dir).unwrap();
        wal.write_record(1, tag, &page).unwrap();
        wal.write_commit(1).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, rel);
        let mut txns = TransactionManager::default();
        perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        let mut disk_page = [0u8; PAGE_SIZE];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut disk_page)
            .unwrap();
        // Skip LSN bytes (0-7 are overwritten with record LSN).
        assert_eq!(disk_page[8..100], [0xFE; 92]);
    }

    #[test]
    fn recovery_stats_correct() {
        let (dir, wal_dir) = setup_recovery("recovery_stats");
        let wal = WalWriter::new(&wal_dir).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        heap_page_init(&mut page);

        let tag = test_tag(1400, 0);
        let tuple = [0x01; 20];
        page_add_item(&mut page, &tuple).unwrap();
        wal.write_insert(1, tag, &page, 1, &tuple).unwrap(); // → FPI
        let tuple2 = [0x02; 20];
        page_add_item(&mut page, &tuple2).unwrap();
        wal.write_insert(1, tag, &page, 2, &tuple2).unwrap(); // → delta
        wal.write_commit(1).unwrap();

        wal.write_record(2, test_tag(1400, 1), &page).unwrap(); // FPI, no commit → aborted
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, test_rel(1400));
        let mut txns = TransactionManager::default();
        let stats = perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        assert_eq!(stats.fpis, 2);
        assert_eq!(stats.inserts, 1);
        assert_eq!(stats.commits, 1);
        assert_eq!(stats.aborted, 1);
        assert_eq!(stats.records_replayed, 4); // FPI + delta + commit + FPI
    }

    #[test]
    fn recovery_commit_only_no_data() {
        let (dir, wal_dir) = setup_recovery("recovery_commit_only");
        let wal = WalWriter::new(&wal_dir).unwrap();
        wal.write_commit(42).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        let mut txns = TransactionManager::default();
        let stats = perform_wal_recovery(&wal_dir, &mut smgr, &mut txns).unwrap();

        assert_eq!(stats.commits, 1);
        assert_eq!(stats.fpis, 0);
        assert_eq!(txns.status(42), Some(TransactionStatus::Committed));
    }

    #[test]
    fn recovery_from_redo_lsn_skips_precheckpoint_records() {
        let (dir, wal_dir) = setup_recovery("recovery_from_redo_lsn");
        let rel = test_rel(1500);
        let tag = test_tag(1500, 0);
        let tuple1 = [0x11; 20];
        let tuple2 = [0x22; 20];

        let page1 = make_page_with_tuples(&[&tuple1]);
        let mut page2 = page1;
        let second_offset = page_add_item(&mut page2, &tuple2).unwrap();
        assert_eq!(second_offset, 2);

        let wal = WalWriter::new(&wal_dir).unwrap();
        wal.write_record(1, tag, &page1).unwrap();
        wal.write_commit(1).unwrap();
        let redo_lsn = wal.insert_lsn();
        wal.write_checkpoint_record(CheckpointRecord { redo_lsn }, false)
            .unwrap();
        wal.write_insert(2, tag, &page2, second_offset, &tuple2)
            .unwrap();
        wal.write_commit(2).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut smgr = MdStorageManager::new_in_recovery(&dir);
        setup_relation(&mut smgr, rel);
        smgr.extend(rel, ForkNumber::Main, 0, &page1, true).unwrap();

        let mut txns = TransactionManager::default();
        txns.replay_commit(1);
        let stats = perform_wal_recovery_from(&wal_dir, &mut smgr, &mut txns, redo_lsn).unwrap();

        let mut recovered = [0u8; BLCKSZ];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut recovered)
            .unwrap();
        assert_eq!(stats.records_replayed, 3);
        assert_eq!(page_get_item(&recovered, 1).unwrap(), tuple1);
        assert_eq!(page_get_item(&recovered, 2).unwrap(), tuple2);
        assert_eq!(txns.status(1), Some(TransactionStatus::Committed));
        assert_eq!(txns.status(2), Some(TransactionStatus::Committed));
    }
}
