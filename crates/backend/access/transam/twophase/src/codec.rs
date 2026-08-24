use types_core::{Oid, TimestampTz, TransactionId, XLogRecPtr};

use crate::state::GIDSIZE;

pub const TWOPHASE_MAGIC: u32 = 0x57F94534;
pub const MAX_ALLOC_SIZE: u32 = 0x3fffffff;

pub const fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

// xl_xact_prepare (xact.h), aka TwoPhaseFileHeader: 72 bytes with origin_lsn
// 8-aligned at 56.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TwoPhaseFileHeader {
    pub magic: u32,
    pub total_len: u32,
    pub xid: TransactionId,
    pub database: Oid,
    pub prepared_at: TimestampTz,
    pub owner: Oid,
    pub nsubxacts: i32,
    pub ncommitrels: i32,
    pub nabortrels: i32,
    pub ncommitstats: i32,
    pub nabortstats: i32,
    pub ninvalmsgs: i32,
    pub initfileinval: bool,
    pub gidlen: u16,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

pub const SIZEOF_TWOPHASE_FILE_HEADER: usize = 72;

impl TwoPhaseFileHeader {
    pub fn to_bytes(&self) -> [u8; SIZEOF_TWOPHASE_FILE_HEADER] {
        let mut b = [0u8; SIZEOF_TWOPHASE_FILE_HEADER];
        b[0..4].copy_from_slice(&self.magic.to_ne_bytes());
        b[4..8].copy_from_slice(&self.total_len.to_ne_bytes());
        b[8..12].copy_from_slice(&self.xid.to_ne_bytes());
        b[12..16].copy_from_slice(&self.database.to_ne_bytes());
        b[16..24].copy_from_slice(&self.prepared_at.to_ne_bytes());
        b[24..28].copy_from_slice(&self.owner.to_ne_bytes());
        b[28..32].copy_from_slice(&self.nsubxacts.to_ne_bytes());
        b[32..36].copy_from_slice(&self.ncommitrels.to_ne_bytes());
        b[36..40].copy_from_slice(&self.nabortrels.to_ne_bytes());
        b[40..44].copy_from_slice(&self.ncommitstats.to_ne_bytes());
        b[44..48].copy_from_slice(&self.nabortstats.to_ne_bytes());
        b[48..52].copy_from_slice(&self.ninvalmsgs.to_ne_bytes());
        b[52] = self.initfileinval as u8;
        b[54..56].copy_from_slice(&self.gidlen.to_ne_bytes());
        b[56..64].copy_from_slice(&self.origin_lsn.to_ne_bytes());
        b[64..72].copy_from_slice(&self.origin_timestamp.to_ne_bytes());
        b
    }

    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < SIZEOF_TWOPHASE_FILE_HEADER {
            return None;
        }
        let u = |o: usize| u32::from_ne_bytes(buf[o..o + 4].try_into().unwrap());
        let i = |o: usize| i32::from_ne_bytes(buf[o..o + 4].try_into().unwrap());
        let i64_ = |o: usize| i64::from_ne_bytes(buf[o..o + 8].try_into().unwrap());
        let u64_ = |o: usize| u64::from_ne_bytes(buf[o..o + 8].try_into().unwrap());
        Some(TwoPhaseFileHeader {
            magic: u(0),
            total_len: u(4),
            xid: u(8),
            database: u(12),
            prepared_at: i64_(16),
            owner: u(24),
            nsubxacts: i(28),
            ncommitrels: i(32),
            nabortrels: i(36),
            ncommitstats: i(40),
            nabortstats: i(44),
            ninvalmsgs: i(48),
            initfileinval: buf[52] != 0,
            gidlen: u16::from_ne_bytes(buf[54..56].try_into().unwrap()),
            origin_lsn: u64_(56),
            origin_timestamp: i64_(64),
        })
    }
}

// TwoPhaseRecordOnDisk { uint32 len; uint8 rmid; uint16 info; } => 8 bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TwoPhaseRecordOnDisk {
    pub len: u32,
    pub rmid: u8,
    pub info: u16,
}

pub const SIZEOF_TWOPHASE_RECORD_ON_DISK: usize = 8;

impl TwoPhaseRecordOnDisk {
    pub fn to_bytes(&self) -> [u8; SIZEOF_TWOPHASE_RECORD_ON_DISK] {
        let mut b = [0u8; SIZEOF_TWOPHASE_RECORD_ON_DISK];
        b[0..4].copy_from_slice(&self.len.to_ne_bytes());
        b[4] = self.rmid;
        b[6..8].copy_from_slice(&self.info.to_ne_bytes());
        b
    }

    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < SIZEOF_TWOPHASE_RECORD_ON_DISK {
            return None;
        }
        Some(TwoPhaseRecordOnDisk {
            len: u32::from_ne_bytes(buf[0..4].try_into().unwrap()),
            rmid: buf[4],
            info: u16::from_ne_bytes(buf[6..8].try_into().unwrap()),
        })
    }
}

pub const SIZEOF_REL_FILE_LOCATOR: usize = 12;
pub const SIZEOF_XL_XACT_STATS_ITEM: usize = 16;
pub const SIZEOF_SHARED_INVAL_MSG: usize = 16;

/// Byte offsets of each state-file segment after the header.
#[derive(Clone, Copy, Debug)]
pub struct BufferLayout {
    pub gid: usize,
    pub children: usize,
    pub commitrels: usize,
    pub abortrels: usize,
    pub commitstats: usize,
    pub abortstats: usize,
    pub invalmsgs: usize,
    pub records: usize,
}

impl BufferLayout {
    /// Compute the segment offsets from a header, validating every
    /// attacker-influenced count against the actual buffer length.
    ///
    /// The header fields (`gidlen` and the six signed element counts) come
    /// straight off disk or out of a WAL record replayed from a possibly
    /// hostile primary. C computes the same offsets with raw pointer
    /// arithmetic and relies on the CRC as its only guard; per the pgrust
    /// threat model the CRC is attacker-computable and is not a boundary, and
    /// unchecked arithmetic here would either panic (debug) or wrap into
    /// arbitrary/overlapping offsets (release). So every count must be
    /// non-negative, `gidlen` must fit the shared-memory gid slot, and the
    /// fully accumulated layout must fit within `buf_len`, all with checked
    /// arithmetic. Any violation returns `None`, which callers surface as a
    /// catchable `ERRCODE_DATA_CORRUPTED` error instead of a panic.
    pub fn try_of(hdr: &TwoPhaseFileHeader, buf_len: usize) -> Option<BufferLayout> {
        // Advance `off` past a segment of `count` elements of `elt` bytes
        // each (MAXALIGN-padded), rejecting negative counts, multiply/add
        // overflow, and any segment that runs past the end of the buffer.
        fn advance(off: usize, count: i32, elt: usize, buf_len: usize) -> Option<usize> {
            if count < 0 {
                return None;
            }
            let bytes = (count as usize).checked_mul(elt)?;
            let next = off.checked_add(maxalign(bytes))?;
            if next > buf_len {
                return None;
            }
            Some(next)
        }

        let mut off = maxalign(SIZEOF_TWOPHASE_FILE_HEADER);
        if off > buf_len {
            return None;
        }
        let gid = off;
        // A valid gid is NUL-terminated and shorter than GIDSIZE, so its
        // stored length (including the NUL) never exceeds GIDSIZE; a larger
        // value is corruption and would also overflow the fixed gid slot.
        if hdr.gidlen as usize > GIDSIZE {
            return None;
        }
        off = off.checked_add(maxalign(hdr.gidlen as usize))?;
        if off > buf_len {
            return None;
        }
        let children = off;
        off = advance(off, hdr.nsubxacts, 4, buf_len)?;
        let commitrels = off;
        off = advance(off, hdr.ncommitrels, SIZEOF_REL_FILE_LOCATOR, buf_len)?;
        let abortrels = off;
        off = advance(off, hdr.nabortrels, SIZEOF_REL_FILE_LOCATOR, buf_len)?;
        let commitstats = off;
        off = advance(off, hdr.ncommitstats, SIZEOF_XL_XACT_STATS_ITEM, buf_len)?;
        let abortstats = off;
        off = advance(off, hdr.nabortstats, SIZEOF_XL_XACT_STATS_ITEM, buf_len)?;
        let invalmsgs = off;
        off = advance(off, hdr.ninvalmsgs, SIZEOF_SHARED_INVAL_MSG, buf_len)?;
        Some(BufferLayout {
            gid,
            children,
            commitrels,
            abortrels,
            commitstats,
            abortstats,
            invalmsgs,
            records: off,
        })
    }
}
