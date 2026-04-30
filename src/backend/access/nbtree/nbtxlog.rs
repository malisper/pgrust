use crate::BLCKSZ;
use crate::backend::access::transam::xlog::{
    REGBUF_FORCE_IMAGE, REGBUF_STANDARD, REGBUF_WILL_INIT, RM_BTREE_ID, WalError,
    XLOG_BTREE_DELETE, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_INSERT_META, XLOG_BTREE_INSERT_UPPER,
    XLOG_BTREE_MARK_PAGE_HALFDEAD, XLOG_BTREE_NEWROOT, XLOG_BTREE_REUSE_PAGE, XLOG_BTREE_SPLIT_L,
    XLOG_BTREE_SPLIT_R, XLOG_BTREE_UNLINK_PAGE, XLOG_BTREE_UNLINK_PAGE_META, XLOG_BTREE_VACUUM,
    XLOG_FPI,
};
use crate::backend::access::transam::xloginsert::{
    xlog_begin_insert, xlog_insert, xlog_register_buf_data, xlog_register_buffer,
    xlog_register_buffer_image, xlog_register_data,
};
use crate::backend::access::transam::xlogreader::{DecodedBkpBlock, DecodedXLogRecord};
use crate::backend::storage::buffer::{BufferTag, PAGE_SIZE};
use crate::backend::storage::page::bufpage::{page_add_item_at, page_header};
use crate::backend::storage::smgr::md::MdStorageManager;
use crate::backend::storage::smgr::{ForkNumber, StorageManager};

pub struct LoggedBtreeBlock<'a> {
    pub block_id: u8,
    pub tag: BufferTag,
    pub page: &'a [u8; PAGE_SIZE],
    pub will_init: bool,
    pub force_image: bool,
    pub data: &'a [u8],
}

pub fn log_btree_record(
    wal: &crate::backend::access::transam::xlog::WalWriter,
    xid: u32,
    info: u8,
    blocks: &[LoggedBtreeBlock<'_>],
    main_data: &[u8],
) -> Result<u64, WalError> {
    xlog_begin_insert();
    for block in blocks {
        let mut flags = REGBUF_STANDARD;
        if block.force_image {
            flags |= REGBUF_FORCE_IMAGE;
        }
        if block.will_init {
            flags |= REGBUF_WILL_INIT;
        }
        xlog_register_buffer(block.block_id, block.tag, flags);
        if block.force_image {
            xlog_register_buffer_image(block.block_id, block.page);
        }
        if !block.data.is_empty() {
            xlog_register_buf_data(block.block_id, block.data);
        }
    }
    if !main_data.is_empty() {
        xlog_register_data(main_data);
    }
    xlog_insert(wal, xid, RM_BTREE_ID, info)
}

pub fn btree_redo(
    smgr: &mut MdStorageManager,
    record_lsn: u64,
    record: &DecodedXLogRecord,
) -> Result<(), WalError> {
    match record.info {
        XLOG_BTREE_INSERT_LEAF | XLOG_BTREE_INSERT_UPPER => {
            for block in &record.blocks {
                if block.image.is_some() {
                    apply_block_image(smgr, record_lsn, block)?;
                } else {
                    apply_insert_delta(smgr, record_lsn, block, &record.main_data)?;
                }
            }
            Ok(())
        }
        XLOG_FPI
        | XLOG_BTREE_INSERT_META
        | XLOG_BTREE_SPLIT_L
        | XLOG_BTREE_SPLIT_R
        | XLOG_BTREE_NEWROOT
        | XLOG_BTREE_VACUUM
        | XLOG_BTREE_DELETE
        | XLOG_BTREE_MARK_PAGE_HALFDEAD
        | XLOG_BTREE_UNLINK_PAGE
        | XLOG_BTREE_UNLINK_PAGE_META
        | XLOG_BTREE_REUSE_PAGE => {
            for block in &record.blocks {
                apply_block_image(smgr, record_lsn, block)?;
            }
            Ok(())
        }
        other => Err(WalError::Corrupt(format!(
            "unknown btree WAL info code {other}"
        ))),
    }
}

fn apply_insert_delta(
    smgr: &mut MdStorageManager,
    record_lsn: u64,
    block: &DecodedBkpBlock,
    main_data: &[u8],
) -> Result<(), WalError> {
    if main_data.len() != 2 {
        return Err(WalError::Corrupt(
            "btree insert WAL record has invalid main data".into(),
        ));
    }
    crate::include::access::itup::IndexTupleData::parse(&block.data)
        .map_err(|err| WalError::Corrupt(format!("btree insert tuple is corrupt: {err:?}")))?;
    let offnum = u16::from_le_bytes([main_data[0], main_data[1]]);
    ensure_block_exists(smgr, block.tag.rel, block.tag.fork, block.tag.block)?;
    let mut page = [0u8; BLCKSZ];
    smgr.read_block(block.tag.rel, block.tag.fork, block.tag.block, &mut page)
        .map_err(smgr_to_wal)?;
    if let Ok(header) = page_header(&page)
        && header.pd_lsn >= record_lsn
    {
        return Ok(());
    }
    page_add_item_at(&mut page, &block.data, offnum)
        .map_err(|err| WalError::Corrupt(format!("btree insert redo failed: {err:?}")))?;
    page[0..8].copy_from_slice(&record_lsn.to_le_bytes());
    smgr.write_block(block.tag.rel, block.tag.fork, block.tag.block, &page, true)
        .map_err(smgr_to_wal)
}

fn apply_block_image(
    smgr: &mut MdStorageManager,
    record_lsn: u64,
    block: &DecodedBkpBlock,
) -> Result<(), WalError> {
    let mut page = block
        .image
        .as_ref()
        .ok_or_else(|| WalError::Corrupt("btree replay record missing page image".into()))?
        .as_ref()
        .to_owned();
    page[0..8].copy_from_slice(&record_lsn.to_le_bytes());
    ensure_block_exists(smgr, block.tag.rel, block.tag.fork, block.tag.block)?;
    smgr.write_block(block.tag.rel, block.tag.fork, block.tag.block, &page, true)
        .map_err(smgr_to_wal)
}

fn ensure_block_exists(
    smgr: &mut MdStorageManager,
    rel: crate::backend::storage::smgr::RelFileLocator,
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

fn smgr_to_wal(e: crate::backend::storage::smgr::SmgrError) -> WalError {
    WalError::Io(std::io::Error::other(format!("{e:?}")))
}
