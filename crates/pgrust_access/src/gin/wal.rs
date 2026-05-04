use crate::transam::xlog::{RM_GIN_ID, WalError, XLOG_FPI};
use crate::transam::xlogreader::{DecodedBkpBlock, DecodedXLogRecord};
use pgrust_storage::BLCKSZ;
use pgrust_storage::smgr::md::MdStorageManager;
use pgrust_storage::smgr::{ForkNumber, StorageManager};

pub(crate) fn gin_redo(
    smgr: &mut MdStorageManager,
    record_lsn: u64,
    record: &DecodedXLogRecord,
) -> Result<(), WalError> {
    match (record.rmid, record.info) {
        (RM_GIN_ID, XLOG_FPI) => {
            for block in &record.blocks {
                apply_block_image(smgr, record_lsn, block)?;
            }
            Ok(())
        }
        (_, other) => Err(WalError::Corrupt(format!(
            "unknown gin WAL info code {other}"
        ))),
    }
}

fn apply_block_image(
    smgr: &mut MdStorageManager,
    record_lsn: u64,
    block: &DecodedBkpBlock,
) -> Result<(), WalError> {
    let mut page = block
        .image
        .as_ref()
        .ok_or_else(|| WalError::Corrupt("gin replay record missing page image".into()))?
        .as_ref()
        .to_owned();
    page[0..8].copy_from_slice(&record_lsn.to_le_bytes());
    ensure_block_exists(smgr, block.tag.rel, block.tag.fork, block.tag.block)?;
    smgr.write_block(block.tag.rel, block.tag.fork, block.tag.block, &page, true)
        .map_err(smgr_to_wal)
}

fn ensure_block_exists(
    smgr: &mut MdStorageManager,
    rel: pgrust_storage::smgr::RelFileLocator,
    fork: ForkNumber,
    block: u32,
) -> Result<(), WalError> {
    let nblocks = smgr.nblocks(rel, fork).map_err(smgr_to_wal)?;
    if block >= nblocks {
        let zero_page = [0u8; BLCKSZ];
        for b in nblocks..=block {
            smgr.extend(rel, fork, b, &zero_page, true)
                .map_err(smgr_to_wal)?;
        }
    }
    Ok(())
}

fn smgr_to_wal(e: pgrust_storage::smgr::SmgrError) -> WalError {
    WalError::Io(std::io::Error::other(format!("{e:?}")))
}
