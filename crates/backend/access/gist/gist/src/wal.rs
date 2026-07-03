//! gistxlog.c WAL producers (redo lives in gist_xlog).

use ::bufmgr_seams::BufferPin;
use ::types_core::{BlockNumber, OffsetNumber, TransactionId, XLogRecPtr};
use ::types_core::xact::FullTransactionId;
use ::types_error::PgResult;
use ::types_gist::{
    GistNSN, GistxlogDelete, GistxlogPageSplit, GistxlogPageUpdate, SizeOfGistxlogPageReuse,
    XLOG_GIST_ASSIGN_LSN, XLOG_GIST_DELETE, XLOG_GIST_PAGE_REUSE, XLOG_GIST_PAGE_SPLIT,
    XLOG_GIST_PAGE_UPDATE,
};
use ::types_rel::Relation;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::insert::SplitPageLayout;

pub const RM_GIST_ID: u8 = ::types_core::primitive::RmgrIds::RM_GIST_ID as u8;
// XLOG_MARK_UNIMPORTANT (xlog.h record_flags).
const XLOG_MARK_UNIMPORTANT: u8 = 0x02;

/// gistXLogUpdate.
pub fn gistXLogUpdate(
    buffer: &BufferPin,
    todelete: &[OffsetNumber],
    itup: &[&[u8]],
    leftchildbuf: Option<&BufferPin>,
) -> PgResult<XLogRecPtr> {
    let xlrec = GistxlogPageUpdate {
        ntodelete: todelete.len() as u16,
        ntoinsert: itup.len() as u16,
    }
    .encode();

    // SAFETY: OffsetNumber (u16) as raw ne bytes.
    let todelete_bytes = unsafe {
        core::slice::from_raw_parts(todelete.as_ptr().cast::<u8>(), todelete.len() * 2)
    };
    let mut bufdata: Vec<&[u8]> = Vec::with_capacity(1 + itup.len());
    bufdata.push(todelete_bytes);
    for it in itup {
        bufdata.push(it);
    }

    let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(2);
    bufs.push(XLogRegBuf {
        block_id: 0,
        buffer: buffer.buffer(),
        flags: REGBUF_STANDARD,
        bufdata: &bufdata,
    });
    if let Some(lc) = leftchildbuf {
        bufs.push(XLogRegBuf {
            block_id: 1,
            buffer: lc.buffer(),
            flags: REGBUF_STANDARD,
            bufdata: &[],
        });
    }

    xloginsert_seams::xlog_insert_record::call(
        RM_GIST_ID,
        XLOG_GIST_PAGE_UPDATE,
        0,
        &[&xlrec],
        &bufs,
    )
}

/// gistXLogDelete.
pub fn gistXLogDelete(
    buffer: &BufferPin,
    todelete: &[OffsetNumber],
    snapshot_conflict_horizon: TransactionId,
    heaprel: &Relation<'_>,
) -> PgResult<XLogRecPtr> {
    let is_catalog = crate::relation_is_accessible_in_logical_decoding(heaprel);
    let xlrec = GistxlogDelete {
        snapshotConflictHorizon: snapshot_conflict_horizon,
        ntodelete: todelete.len() as u16,
        isCatalogRel: is_catalog,
    }
    .encode();

    // SAFETY: OffsetNumber (u16) as raw ne bytes.
    let todelete_bytes = unsafe {
        core::slice::from_raw_parts(todelete.as_ptr().cast::<u8>(), todelete.len() * 2)
    };

    xloginsert_seams::xlog_insert_record::call(
        RM_GIST_ID,
        XLOG_GIST_DELETE,
        0,
        &[&xlrec, todelete_bytes],
        &[XLogRegBuf {
            block_id: 0,
            buffer: buffer.buffer(),
            flags: REGBUF_STANDARD,
            bufdata: &[],
        }],
    )
}

/// gistXLogSplit; dist pages must be locked and dirty.
pub fn gistXLogSplit(
    page_is_leaf: bool,
    dist: &[SplitPageLayout<'_>],
    origrlink: BlockNumber,
    orignsn: GistNSN,
    leftchildbuf: Option<&BufferPin>,
    markfollowright: bool,
) -> PgResult<XLogRecPtr> {
    let npage = dist.len();
    let xlrec = GistxlogPageSplit {
        origrlink,
        orignsn,
        origleaf: page_is_leaf,
        npage: npage as u16,
        markfollowright,
    }
    .encode();

    let nums: Vec<[u8; 4]> = dist.iter().map(|d| d.block_num.to_ne_bytes()).collect();
    let mut bufdatas: Vec<[&[u8]; 2]> = Vec::with_capacity(npage);
    for (i, d) in dist.iter().enumerate() {
        bufdatas.push([&nums[i], &d.list]);
    }

    let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(npage + 1);
    if let Some(lc) = leftchildbuf {
        bufs.push(XLogRegBuf {
            block_id: 0,
            buffer: lc.buffer(),
            flags: REGBUF_STANDARD,
            bufdata: &[],
        });
    }
    for (i, d) in dist.iter().enumerate() {
        bufs.push(XLogRegBuf {
            block_id: (i + 1) as u8,
            buffer: d.buf,
            flags: REGBUF_WILL_INIT,
            bufdata: &bufdatas[i],
        });
    }

    xloginsert_seams::xlog_insert_record::call(
        RM_GIST_ID,
        XLOG_GIST_PAGE_SPLIT,
        0,
        &[&xlrec],
        &bufs,
    )
}

/// gistXLogPageDelete (producer for the LOUD gistvacuum lane; redo is live).
pub fn gistXLogPageDelete(
    buffer: &BufferPin,
    xid: FullTransactionId,
    parent_buffer: &BufferPin,
    downlink_offset: OffsetNumber,
) -> PgResult<XLogRecPtr> {
    let xlrec = ::types_gist::GistxlogPageDelete {
        deleteXid: xid,
        downlinkOffset: downlink_offset,
    }
    .encode();
    xloginsert_seams::xlog_insert_record::call(
        RM_GIST_ID,
        ::types_gist::XLOG_GIST_PAGE_DELETE,
        0,
        &[&xlrec],
        &[
            XLogRegBuf {
                block_id: 0,
                buffer: buffer.buffer(),
                flags: REGBUF_STANDARD,
                bufdata: &[],
            },
            XLogRegBuf {
                block_id: 1,
                buffer: parent_buffer.buffer(),
                flags: REGBUF_STANDARD,
                bufdata: &[],
            },
        ],
    )
}

/// gistXLogAssignLSN.
pub fn gistXLogAssignLSN() -> PgResult<XLogRecPtr> {
    let dummy: i32 = 0;
    xloginsert_seams::xlog_insert_with_flags::call(
        RM_GIST_ID,
        XLOG_GIST_ASSIGN_LSN,
        XLOG_MARK_UNIMPORTANT,
        &[&dummy.to_ne_bytes()],
    )
}

/// gistXLogPageReuse.
pub fn gistXLogPageReuse(
    rel: &Relation<'_>,
    heaprel: &Relation<'_>,
    blkno: BlockNumber,
    delete_xid: FullTransactionId,
) -> PgResult<XLogRecPtr> {
    let locator = rel.rd_locator.get();
    let mut b = [0u8; SizeOfGistxlogPageReuse];
    b[0..4].copy_from_slice(&locator.spcOid.to_ne_bytes());
    b[4..8].copy_from_slice(&locator.dbOid.to_ne_bytes());
    b[8..12].copy_from_slice(&locator.relNumber.to_ne_bytes());
    b[12..16].copy_from_slice(&blkno.to_ne_bytes());
    b[16..24].copy_from_slice(&delete_xid.to_u64().to_ne_bytes());
    b[24] = crate::relation_is_accessible_in_logical_decoding(heaprel) as u8;

    xloginsert_seams::xlog_insert::call(RM_GIST_ID, XLOG_GIST_PAGE_REUSE, &[&b])
}
