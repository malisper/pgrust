//! xlogstats.c: WAL statistics accounting over decoded records.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use xlogreader_seams::XLogReaderState;

pub const MAX_XLINFO_TYPES: usize = 16;
// RM_MAX_ID (rmgr.h): RmgrId is a u8 and custom ids run to UINT8_MAX.
pub const RM_MAX_ID: usize = u8::MAX as usize;
const RM_XACT_ID: u8 = types_core::RmgrIds::RM_XACT_ID as u8;

#[derive(Clone, Copy, Default)]
pub struct XLogRecStats {
    pub count: u64,
    pub rec_len: u64,
    pub fpi_len: u64,
}

// C's FRONTEND-only startptr/endptr ride along: the consumers are frontend-shaped.
pub struct XLogStats {
    pub count: u64,
    pub startptr: types_core::XLogRecPtr,
    pub endptr: types_core::XLogRecPtr,
    pub rmgr_stats: [XLogRecStats; RM_MAX_ID + 1],
    pub record_stats: [[XLogRecStats; MAX_XLINFO_TYPES]; RM_MAX_ID + 1],
}

impl XLogStats {
    pub const ZEROED: XLogStats = XLogStats {
        count: 0,
        startptr: 0,
        endptr: 0,
        rmgr_stats: [XLogRecStats { count: 0, rec_len: 0, fpi_len: 0 }; RM_MAX_ID + 1],
        record_stats: [[XLogRecStats { count: 0, rec_len: 0, fpi_len: 0 }; MAX_XLINFO_TYPES];
            RM_MAX_ID + 1],
    };
}

pub fn XLogRecGetLen(record: &XLogReaderState) -> (u32, u32) {
    let rec = record
        .record
        .as_ref()
        .expect("XLogRecGetLen on a reader with no decoded record");
    let mut fpi_len: u32 = 0;
    for block_id in 0..(rec.max_block_id as i32 + 1).max(0) as u8 {
        if !record.has_block_ref(block_id) {
            continue;
        }
        if record.has_block_image(block_id) {
            fpi_len += rec.blocks[block_id as usize].bimg_len as u32;
        }
    }
    (rec.xl_tot_len - fpi_len, fpi_len)
}

pub fn XLogRecStoreStats(stats: &mut XLogStats, record: &XLogReaderState) {
    let rec = record
        .record
        .as_ref()
        .expect("XLogRecStoreStats on a reader with no decoded record");
    stats.count += 1;

    let rmid = rec.xl_rmid;
    let (rec_len, fpi_len) = XLogRecGetLen(record);

    let rs = &mut stats.rmgr_stats[rmid as usize];
    rs.count += 1;
    rs.rec_len += rec_len as u64;
    rs.fpi_len += fpi_len as u64;

    let mut recid = rec.xl_info >> 4;
    // XACT records: the high bit of the rmgr nibble is an optional flag; only
    // the low three bits are the opcode identity.
    if rmid == RM_XACT_ID {
        recid &= 0x07;
    }

    let rs = &mut stats.record_stats[rmid as usize][recid as usize];
    rs.count += 1;
    rs.rec_len += rec_len as u64;
    rs.fpi_len += fpi_len as u64;
}

pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;
    use xlogreader_seams::{DecodedBkpBlock, DecodedXLogRecord};

    fn reader(rmid: u8, info: u8, tot_len: u32, images: &[(usize, u16)]) -> XLogReaderState {
        let mut rec = DecodedXLogRecord::default();
        rec.xl_rmid = rmid;
        rec.xl_info = info;
        rec.xl_tot_len = tot_len;
        for &(id, bimg_len) in images {
            let mut blk = DecodedBkpBlock::EMPTY;
            blk.in_use = true;
            blk.has_image = bimg_len > 0;
            blk.bimg_len = bimg_len;
            rec.blocks[id] = blk;
            rec.max_block_id = rec.max_block_id.max(id as i8);
        }
        XLogReaderState { record: Some(rec), ..Default::default() }
    }

    #[test]
    fn len_split_sums_block_images() {
        // C xlogstats.c:33-47: fpi = sum of bimg_len over imaged blocks, rec = total - fpi.
        let r = reader(20, 0, 9000, &[(0, 8192), (1, 0), (3, 500)]);
        assert_eq!(XLogRecGetLen(&r), (9000 - 8692, 8692));

        let r = reader(20, 0, 64, &[]);
        assert_eq!(XLogRecGetLen(&r), (64, 0));
    }

    #[test]
    fn store_stats_buckets_by_rmgr_and_recid() {
        let mut stats = Box::new(XLogStats::ZEROED);
        let r = reader(20, 0x30, 100, &[(0, 40)]);
        XLogRecStoreStats(&mut stats, &r);
        XLogRecStoreStats(&mut stats, &r);
        assert_eq!(stats.count, 2);
        assert_eq!(stats.rmgr_stats[20].count, 2);
        assert_eq!(stats.rmgr_stats[20].rec_len, 120);
        assert_eq!(stats.rmgr_stats[20].fpi_len, 80);
        assert_eq!(stats.record_stats[20][3].count, 2);
        assert_eq!(stats.record_stats[20][3].rec_len, 120);
        assert_eq!(stats.record_stats[20][3].fpi_len, 80);
        assert_eq!(stats.record_stats[20][0].count, 0);
    }

    #[test]
    fn xact_recid_masks_opcode_bits() {
        let mut stats = Box::new(XLogStats::ZEROED);
        let r = reader(RM_XACT_ID, 0x80, 40, &[]);
        XLogRecStoreStats(&mut stats, &r);
        assert_eq!(stats.record_stats[RM_XACT_ID as usize][0].count, 1);
        let r = reader(20, 0x80, 40, &[]);
        XLogRecStoreStats(&mut stats, &r);
        assert_eq!(stats.record_stats[20][8].count, 1);
    }
}
