use crate::transam::xlog::{
    REGBUF_FORCE_IMAGE, REGBUF_STANDARD, REGBUF_WILL_INIT, RM_HASH_ID, WalError, XLOG_FPI,
    XLOG_HASH_ADD_OVFL_PAGE, XLOG_HASH_DELETE, XLOG_HASH_INIT_META_PAGE, XLOG_HASH_INSERT,
    XLOG_HASH_SPLIT_ALLOCATE_PAGE, XLOG_HASH_SPLIT_PAGE, XLOG_HASH_VACUUM,
};
use crate::transam::xloginsert::{
    xlog_begin_insert, xlog_insert, xlog_register_buffer, xlog_register_buffer_image,
    xlog_register_data,
};
use crate::transam::xlogreader::{DecodedBkpBlock, DecodedXLogRecord};
use pgrust_storage::BLCKSZ;
use pgrust_storage::buffer::{BufferTag, PAGE_SIZE};
use pgrust_storage::smgr::md::MdStorageManager;
use pgrust_storage::smgr::{ForkNumber, StorageManager};

#[allow(dead_code)]
pub(crate) struct LoggedHashBlock<'a> {
    pub block_id: u8,
    pub tag: BufferTag,
    pub page: &'a [u8; PAGE_SIZE],
    pub will_init: bool,
}

#[allow(dead_code)]
pub(crate) fn log_hash_record(
    wal: &crate::transam::xlog::WalWriter,
    xid: u32,
    info: u8,
    blocks: &[LoggedHashBlock<'_>],
    main_data: &[u8],
) -> Result<u64, WalError> {
    xlog_begin_insert();
    for block in blocks {
        let mut flags = REGBUF_STANDARD | REGBUF_FORCE_IMAGE;
        if block.will_init {
            flags |= REGBUF_WILL_INIT;
        }
        xlog_register_buffer(block.block_id, block.tag, flags);
        xlog_register_buffer_image(block.block_id, block.page);
    }
    if !main_data.is_empty() {
        xlog_register_data(main_data);
    }
    xlog_insert(wal, xid, RM_HASH_ID, info)
}

pub(crate) fn hash_redo(
    smgr: &mut MdStorageManager,
    record_lsn: u64,
    record: &DecodedXLogRecord,
) -> Result<(), WalError> {
    match record.info {
        XLOG_FPI
        | XLOG_HASH_INIT_META_PAGE
        | XLOG_HASH_INSERT
        | XLOG_HASH_ADD_OVFL_PAGE
        | XLOG_HASH_SPLIT_ALLOCATE_PAGE
        | XLOG_HASH_SPLIT_PAGE
        | XLOG_HASH_DELETE
        | XLOG_HASH_VACUUM => {
            for block in &record.blocks {
                apply_block_image(smgr, record_lsn, block)?;
            }
            Ok(())
        }
        other => Err(WalError::Corrupt(format!(
            "unknown hash WAL info code {other}"
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
        .ok_or_else(|| WalError::Corrupt("hash replay record missing page image".into()))?
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
