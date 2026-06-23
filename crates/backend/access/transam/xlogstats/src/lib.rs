//! Port of PostgreSQL's `src/backend/access/transam/xlogstats.c`.
//!
//! Functions for WAL statistics: per-rmgr and per-record byte/count
//! accounting, splitting a record's length into its non-FPI and
//! full-page-image (FPI) parts.
//!
//! Upstream `XLogRecGetLen`/`XLogRecStoreStats` take an `XLogReaderState *`
//! and read its decoded record (`record->record`); here they take the
//! [`DecodedXLogRecord`] directly — exactly the data the C code reads.

use ::types_core::{uint32, uint64, uint8, RmgrId, XLogRecPtr};
use ::wal::{DecodedXLogRecord, MAX_XLINFO_TYPES, RM_MAX_ID, RM_XACT_ID};

/// Per-rmgr / per-record byte and count accounting
/// (C `struct XLogRecStats`, access/xlogstats.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XLogRecStats {
    count: uint64,
    rec_len: uint64,
    fpi_len: uint64,
}

impl XLogRecStats {
    /// Number of records folded into this bucket (`XLogRecStats.count`).
    pub const fn count(&self) -> uint64 {
        self.count
    }

    /// Accumulated non-FPI record length (`XLogRecStats.rec_len`).
    pub const fn rec_len(&self) -> uint64 {
        self.rec_len
    }

    /// Accumulated full-page-image length (`XLogRecStats.fpi_len`).
    pub const fn fpi_len(&self) -> uint64 {
        self.fpi_len
    }

    /// Fold one record's lengths into the bucket. Wrapping arithmetic matches
    /// C's unsigned overflow semantics on `uint64`.
    fn add_record(&mut self, rec_len: uint32, fpi_len: uint32) {
        self.count = self.count.wrapping_add(1);
        self.rec_len = self.rec_len.wrapping_add(rec_len as uint64);
        self.fpi_len = self.fpi_len.wrapping_add(fpi_len as uint64);
    }
}

/// Aggregate WAL statistics over a stream of records
/// (C `struct XLogStats`, access/xlogstats.h).
///
/// The `startptr` / `endptr` fields exist only in `FRONTEND` builds upstream;
/// they are kept here as owned fields so frontend consumers (e.g.
/// `pg_waldump`) can record the scanned range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XLogStats {
    count: uint64,
    startptr: XLogRecPtr,
    endptr: XLogRecPtr,
    rmgr_stats: [XLogRecStats; RM_MAX_ID + 1],
    record_stats: [[XLogRecStats; MAX_XLINFO_TYPES]; RM_MAX_ID + 1],
}

impl Default for XLogStats {
    fn default() -> Self {
        Self {
            count: 0,
            startptr: 0,
            endptr: 0,
            rmgr_stats: [XLogRecStats::default(); RM_MAX_ID + 1],
            record_stats: [[XLogRecStats::default(); MAX_XLINFO_TYPES]; RM_MAX_ID + 1],
        }
    }
}

impl XLogStats {
    /// Total number of records folded in (`XLogStats.count`).
    pub const fn count(&self) -> uint64 {
        self.count
    }

    /// Start of the scanned WAL range (frontend `XLogStats.startptr`).
    pub const fn startptr(&self) -> XLogRecPtr {
        self.startptr
    }

    /// Set the start of the scanned WAL range.
    pub fn set_startptr(&mut self, ptr: XLogRecPtr) {
        self.startptr = ptr;
    }

    /// End of the scanned WAL range (frontend `XLogStats.endptr`).
    pub const fn endptr(&self) -> XLogRecPtr {
        self.endptr
    }

    /// Set the end of the scanned WAL range.
    pub fn set_endptr(&mut self, ptr: XLogRecPtr) {
        self.endptr = ptr;
    }

    /// Per-rmgr stats for `rmid` (`XLogStats.rmgr_stats[rmid]`).
    pub fn rmgr_stats(&self, rmid: RmgrId) -> Option<&XLogRecStats> {
        self.rmgr_stats.get(rmid as usize)
    }

    /// Per-record stats for `(rmid, recid)`
    /// (`XLogStats.record_stats[rmid][recid]`).
    pub fn record_stats(&self, rmid: RmgrId, recid: uint8) -> Option<&XLogRecStats> {
        self.record_stats
            .get(rmid as usize)
            .and_then(|stats| stats.get(recid as usize))
    }
}

/// Calculate the size of a record, split into non-FPI (`rec_len`) and FPI
/// (`fpi_len`) parts. C `XLogRecGetLen`.
///
/// Upstream sums `bimg_len` for every in-use block that carries a block
/// image; [`DecodedBkpBlock::fpi_len`] encodes exactly that
/// `in_use && has_image` predicate, so the fold reproduces the C loop. The
/// non-FPI length is the total record length minus the FPI bytes.
///
/// Returns `(rec_len, fpi_len)`.
///
/// [`DecodedBkpBlock::fpi_len`]: ::wal::DecodedBkpBlock::fpi_len
pub fn xlog_rec_get_len(record: &DecodedXLogRecord<'_>) -> (uint32, uint32) {
    let fpi_len = record
        .blocks()
        .iter()
        .fold(0_u32, |total, block| total.wrapping_add(block.fpi_len()));
    let rec_len = record.header().total_len().wrapping_sub(fpi_len);
    (rec_len, fpi_len)
}

/// Store per-rmgr and per-record statistics for a given record.
/// C `XLogRecStoreStats`.
pub fn xlog_rec_store_stats(stats: &mut XLogStats, record: &DecodedXLogRecord<'_>) {
    stats.count = stats.count.wrapping_add(1);

    let rmid = record.header().rmid();
    let (rec_len, fpi_len) = xlog_rec_get_len(record);

    // Update per-rmgr statistics.
    stats.rmgr_stats[rmid as usize].add_record(rec_len, fpi_len);

    // Update per-record statistics, where the record is identified by a
    // combination of the RmgrId and the four bits of the xl_info field that
    // are the rmgr's domain (resulting in sixteen possible entries per
    // RmgrId).
    let mut recid = record.header().info() >> 4;

    // XACT records need to be handled differently. Those records use the
    // first bit of those four bits for an optional flag variable and the
    // following three bits for the opcode. We filter opcode out of xl_info
    // and use it as the identifier of the record.
    if rmid == RM_XACT_ID {
        recid &= 0x07;
    }

    stats.record_stats[rmid as usize][recid as usize].add_record(rec_len, fpi_len);
}

/// Wires this crate's seams. It declares none, so this is a no-op kept for
/// the uniform `seams-init` startup convention.
pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::{slice_in, Mcx, MemoryContext};
    use ::wal::{DecodedBkpBlock, XLogRecord};

    #[test]
    fn rec_stats_add_record_accumulates_count_and_lengths() {
        let mut s = XLogRecStats::default();
        assert_eq!((s.count(), s.rec_len(), s.fpi_len()), (0, 0, 0));

        s.add_record(58, 42);
        assert_eq!((s.count(), s.rec_len(), s.fpi_len()), (1, 58, 42));

        s.add_record(88, 12);
        assert_eq!((s.count(), s.rec_len(), s.fpi_len()), (2, 146, 54));
    }

    #[test]
    fn rec_stats_uses_wrapping_arithmetic_like_c_uint64() {
        let mut s = XLogRecStats {
            count: 0,
            rec_len: u64::MAX,
            fpi_len: u64::MAX - 1,
        };
        s.add_record(1, 2);
        // u64::MAX + 1 wraps to 0; (MAX - 1) + 2 wraps to 0.
        assert_eq!((s.count(), s.rec_len(), s.fpi_len()), (1, 0, 0));
    }

    #[test]
    fn stats_default_is_all_zero_and_indexable_to_table_bounds() {
        let stats = XLogStats::default();
        assert_eq!(stats.count(), 0);
        assert_eq!(stats.startptr(), 0);
        assert_eq!(stats.endptr(), 0);

        // Tables are sized RM_MAX_ID + 1 x MAX_XLINFO_TYPES; every cell is zero.
        let last = stats.rmgr_stats(RM_MAX_ID as RmgrId).unwrap();
        assert_eq!(last.count(), 0);
        let last_rec = stats
            .record_stats(RM_MAX_ID as RmgrId, (MAX_XLINFO_TYPES - 1) as uint8)
            .unwrap();
        assert_eq!(last_rec.count(), 0);
    }

    #[test]
    fn record_stats_out_of_range_recid_is_none() {
        let stats = XLogStats::default();
        // recid only has MAX_XLINFO_TYPES (16) valid slots per rmgr.
        assert!(stats.record_stats(0, MAX_XLINFO_TYPES as uint8).is_none());
        assert!(stats
            .record_stats(0, (MAX_XLINFO_TYPES - 1) as uint8)
            .is_some());
    }

    #[test]
    fn scanned_range_round_trips() {
        let mut stats = XLogStats::default();
        stats.set_startptr(0x1000);
        stats.set_endptr(0x2000);
        assert_eq!(stats.startptr(), 0x1000);
        assert_eq!(stats.endptr(), 0x2000);
    }

    fn record<'mcx>(
        mcx: Mcx<'mcx>,
        tot_len: u32,
        info: u8,
        rmid: RmgrId,
        blocks: &[DecodedBkpBlock<'mcx>],
    ) -> DecodedXLogRecord<'mcx> {
        DecodedXLogRecord::new(
            XLogRecord::new(tot_len, 0, 0, info, rmid, 0),
            &[],
            slice_in(mcx, blocks).unwrap(),
        )
    }

    /// A block reference with no block-data borrow, as the stats tests need.
    fn block(in_use: bool, has_image: bool, bimg_len: u16) -> DecodedBkpBlock<'static> {
        DecodedBkpBlock::new(in_use, has_image, false, bimg_len, None)
    }

    #[test]
    fn get_len_splits_fpi_and_non_fpi_parts() {
        let ctx = MemoryContext::new("test");
        let rec = record(
            ctx.mcx(),
            1000,
            0,
            0,
            &[
                block(true, true, 300),  // counted
                block(true, false, 50),  // no image: skipped
                block(false, true, 70),  // not in use: skipped
                block(true, true, 100),  // counted
            ],
        );
        assert_eq!(xlog_rec_get_len(&rec), (600, 400));
    }

    #[test]
    fn store_stats_updates_total_per_rmgr_and_per_record_buckets() {
        let ctx = MemoryContext::new("test");
        let mut stats = XLogStats::default();
        // Non-XACT rmgr keeps the full four xl_info bits.
        let rec = record(ctx.mcx(), 100, 0xf0, 10, &[block(true, true, 40)]);
        xlog_rec_store_stats(&mut stats, &rec);

        assert_eq!(stats.count(), 1);
        let rm = stats.rmgr_stats(10).unwrap();
        assert_eq!((rm.count(), rm.rec_len(), rm.fpi_len()), (1, 60, 40));
        let per = stats.record_stats(10, 0x0f).unwrap();
        assert_eq!((per.count(), per.rec_len(), per.fpi_len()), (1, 60, 40));
    }

    #[test]
    fn store_stats_masks_xact_opcode_to_low_three_bits() {
        let ctx = MemoryContext::new("test");
        let mut stats = XLogStats::default();
        // xl_info = 0xf0 -> recid = 0x0f; XACT masks &0x07 -> 7.
        let rec = record(ctx.mcx(), 100, 0xf0, RM_XACT_ID, &[]);
        xlog_rec_store_stats(&mut stats, &rec);

        assert_eq!(stats.record_stats(RM_XACT_ID, 0x0f).unwrap().count(), 0);
        let per = stats.record_stats(RM_XACT_ID, 0x07).unwrap();
        assert_eq!((per.count(), per.rec_len(), per.fpi_len()), (1, 100, 0));
    }

    #[test]
    fn record_block_array_accounting_is_exact() {
        let ctx = MemoryContext::new("test");
        let blocks = [
            block(true, true, 300),
            block(true, false, 50),
        ];
        let rec = record(ctx.mcx(), 1000, 0, 0, &blocks);
        // The block array is the record's only context allocation.
        assert_eq!(
            ctx.used(),
            rec.blocks().len() * core::mem::size_of::<DecodedBkpBlock<'_>>(),
            "context is charged exactly the decoded block array"
        );
        // Reading stats allocates nothing.
        let mut stats = XLogStats::default();
        xlog_rec_store_stats(&mut stats, &rec);
        assert_eq!(
            ctx.used(),
            rec.blocks().len() * core::mem::size_of::<DecodedBkpBlock<'_>>()
        );
    }

    #[test]
    fn all_bytes_return_on_drop() {
        let ctx = MemoryContext::new("test");
        {
            let recs = [
                record(ctx.mcx(), 1000, 0, 0, &[block(true, true, 300)]),
                record(ctx.mcx(), 100, 0xf0, RM_XACT_ID, &[]),
            ];
            assert!(ctx.used() > 0, "decoded records are charged to the context");
            let mut stats = XLogStats::default();
            for rec in &recs {
                xlog_rec_store_stats(&mut stats, rec);
            }
            assert_eq!(stats.count(), 2);
        }
        assert_eq!(ctx.used(), 0, "dropping the records returns every byte");
    }
}
