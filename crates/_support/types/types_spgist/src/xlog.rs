//! spgxlog.h record codecs; byte layouts are the C struct images (WAL parity
//! is gated byte-for-byte).
use ::types_core::{OffsetNumber, TransactionId};
use ::types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};

/// An SP-GiST WAL record's `main_data` length is attacker-declared (0 is legal
/// and passes all xlogreader validation — only fragment lengths and the
/// secret-less CRC are checked), so every fixed-offset decode below must confirm
/// the payload is long enough before indexing. Raising a catchable
/// `ERRCODE_DATA_CORRUPTED` error (rather than panicking on an out-of-bounds
/// index) keeps a malformed record from crash-looping the startup redo thread.
/// C reads the struct straight out of the decode buffer (spgxlog.c); a short
/// record reads garbage there but does not crash — this restores that property
/// while surfacing the corruption instead of silently trusting garbage.
#[cold]
#[inline(never)]
fn short_record_err(what: &str, need: usize, got: usize) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "SP-GiST redo: {what} record too short: need {need} bytes, got {got}"
        ))
        .with_sqlstate(ERRCODE_DATA_CORRUPTED),
    )
}

pub const XLOG_SPGIST_ADD_LEAF: u8 = 0x10;
pub const XLOG_SPGIST_MOVE_LEAFS: u8 = 0x20;
pub const XLOG_SPGIST_ADD_NODE: u8 = 0x30;
pub const XLOG_SPGIST_SPLIT_TUPLE: u8 = 0x40;
pub const XLOG_SPGIST_PICKSPLIT: u8 = 0x50;
pub const XLOG_SPGIST_VACUUM_LEAF: u8 = 0x60;
pub const XLOG_SPGIST_VACUUM_ROOT: u8 = 0x70;
pub const XLOG_SPGIST_VACUUM_REDIRECT: u8 = 0x80;

#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)] // C-parity name
pub struct spgxlogState {
    pub redirectXid: TransactionId,
    pub isBuild: bool,
}

impl spgxlogState {
    // 8 bytes: xid@0, isBuild@4, pad 5..8.
    fn encode_into(&self, b: &mut [u8]) {
        b[0..4].copy_from_slice(&self.redirectXid.to_ne_bytes());
        b[4] = self.isBuild as u8;
    }

    fn decode_from(b: &[u8]) -> Self {
        spgxlogState {
            redirectXid: TransactionId::from_ne_bytes([b[0], b[1], b[2], b[3]]),
            isBuild: b[4] != 0,
        }
    }
}

fn u16_at(b: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes([b[off], b[off + 1]])
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)] // C-parity name
pub struct spgxlogAddLeaf {
    pub newPage: bool,
    pub storesNulls: bool,
    pub offnumLeaf: OffsetNumber,
    pub offnumHeadLeaf: OffsetNumber,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
}

pub const SizeOfSpgxlogAddLeaf: usize = 10;

impl spgxlogAddLeaf {
    pub fn encode(&self) -> [u8; SizeOfSpgxlogAddLeaf] {
        let mut b = [0u8; SizeOfSpgxlogAddLeaf];
        b[0] = self.newPage as u8;
        b[1] = self.storesNulls as u8;
        b[2..4].copy_from_slice(&self.offnumLeaf.to_ne_bytes());
        b[4..6].copy_from_slice(&self.offnumHeadLeaf.to_ne_bytes());
        b[6..8].copy_from_slice(&self.offnumParent.to_ne_bytes());
        b[8..10].copy_from_slice(&self.nodeI.to_ne_bytes());
        b
    }

    pub fn decode(b: &[u8]) -> PgResult<Self> {
        if b.len() < SizeOfSpgxlogAddLeaf {
            return Err(short_record_err("spgxlogAddLeaf", SizeOfSpgxlogAddLeaf, b.len()));
        }
        Ok(spgxlogAddLeaf {
            newPage: b[0] != 0,
            storesNulls: b[1] != 0,
            offnumLeaf: u16_at(b, 2),
            offnumHeadLeaf: u16_at(b, 4),
            offnumParent: u16_at(b, 6),
            nodeI: u16_at(b, 8),
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)] // C-parity name
pub struct spgxlogMoveLeafs {
    pub nMoves: u16,
    pub newPage: bool,
    pub replaceDead: bool,
    pub storesNulls: bool,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
    pub stateSrc: spgxlogState,
}

pub const SizeOfSpgxlogMoveLeafs: usize = 20;

impl spgxlogMoveLeafs {
    pub fn encode(&self) -> [u8; SizeOfSpgxlogMoveLeafs] {
        let mut b = [0u8; SizeOfSpgxlogMoveLeafs];
        b[0..2].copy_from_slice(&self.nMoves.to_ne_bytes());
        b[2] = self.newPage as u8;
        b[3] = self.replaceDead as u8;
        b[4] = self.storesNulls as u8;
        b[6..8].copy_from_slice(&self.offnumParent.to_ne_bytes());
        b[8..10].copy_from_slice(&self.nodeI.to_ne_bytes());
        self.stateSrc.encode_into(&mut b[12..20]);
        b
    }

    pub fn decode(b: &[u8]) -> PgResult<Self> {
        if b.len() < SizeOfSpgxlogMoveLeafs {
            return Err(short_record_err("spgxlogMoveLeafs", SizeOfSpgxlogMoveLeafs, b.len()));
        }
        Ok(spgxlogMoveLeafs {
            nMoves: u16_at(b, 0),
            newPage: b[2] != 0,
            replaceDead: b[3] != 0,
            storesNulls: b[4] != 0,
            offnumParent: u16_at(b, 6),
            nodeI: u16_at(b, 8),
            stateSrc: spgxlogState::decode_from(&b[12..20]),
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)] // C-parity name
pub struct spgxlogAddNode {
    pub offnum: OffsetNumber,
    pub offnumNew: OffsetNumber,
    pub newPage: bool,
    pub parentBlk: i8,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
    pub stateSrc: spgxlogState,
}

pub const SizeOfSpgxlogAddNode: usize = 20;

impl spgxlogAddNode {
    pub fn encode(&self) -> [u8; SizeOfSpgxlogAddNode] {
        let mut b = [0u8; SizeOfSpgxlogAddNode];
        b[0..2].copy_from_slice(&self.offnum.to_ne_bytes());
        b[2..4].copy_from_slice(&self.offnumNew.to_ne_bytes());
        b[4] = self.newPage as u8;
        b[5] = self.parentBlk as u8;
        b[6..8].copy_from_slice(&self.offnumParent.to_ne_bytes());
        b[8..10].copy_from_slice(&self.nodeI.to_ne_bytes());
        self.stateSrc.encode_into(&mut b[12..20]);
        b
    }

    pub fn decode(b: &[u8]) -> PgResult<Self> {
        if b.len() < SizeOfSpgxlogAddNode {
            return Err(short_record_err("spgxlogAddNode", SizeOfSpgxlogAddNode, b.len()));
        }
        Ok(spgxlogAddNode {
            offnum: u16_at(b, 0),
            offnumNew: u16_at(b, 2),
            newPage: b[4] != 0,
            parentBlk: b[5] as i8,
            offnumParent: u16_at(b, 6),
            nodeI: u16_at(b, 8),
            stateSrc: spgxlogState::decode_from(&b[12..20]),
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)] // C-parity name
pub struct spgxlogSplitTuple {
    pub offnumPrefix: OffsetNumber,
    pub offnumPostfix: OffsetNumber,
    pub newPage: bool,
    pub postfixBlkSame: bool,
}

pub const SizeOfSpgxlogSplitTuple: usize = 6;

impl spgxlogSplitTuple {
    pub fn encode(&self) -> [u8; SizeOfSpgxlogSplitTuple] {
        let mut b = [0u8; SizeOfSpgxlogSplitTuple];
        b[0..2].copy_from_slice(&self.offnumPrefix.to_ne_bytes());
        b[2..4].copy_from_slice(&self.offnumPostfix.to_ne_bytes());
        b[4] = self.newPage as u8;
        b[5] = self.postfixBlkSame as u8;
        b
    }

    pub fn decode(b: &[u8]) -> PgResult<Self> {
        if b.len() < SizeOfSpgxlogSplitTuple {
            return Err(short_record_err("spgxlogSplitTuple", SizeOfSpgxlogSplitTuple, b.len()));
        }
        Ok(spgxlogSplitTuple {
            offnumPrefix: u16_at(b, 0),
            offnumPostfix: u16_at(b, 2),
            newPage: b[4] != 0,
            postfixBlkSame: b[5] != 0,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)] // C-parity name
pub struct spgxlogPickSplit {
    pub isRootSplit: bool,
    pub nDelete: u16,
    pub nInsert: u16,
    pub initSrc: bool,
    pub initDest: bool,
    pub offnumInner: OffsetNumber,
    pub initInner: bool,
    pub storesNulls: bool,
    pub innerIsParent: bool,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
    pub stateSrc: spgxlogState,
}

pub const SizeOfSpgxlogPickSplit: usize = 28;

impl spgxlogPickSplit {
    pub fn encode(&self) -> [u8; SizeOfSpgxlogPickSplit] {
        let mut b = [0u8; SizeOfSpgxlogPickSplit];
        b[0] = self.isRootSplit as u8;
        b[2..4].copy_from_slice(&self.nDelete.to_ne_bytes());
        b[4..6].copy_from_slice(&self.nInsert.to_ne_bytes());
        b[6] = self.initSrc as u8;
        b[7] = self.initDest as u8;
        b[8..10].copy_from_slice(&self.offnumInner.to_ne_bytes());
        b[10] = self.initInner as u8;
        b[11] = self.storesNulls as u8;
        b[12] = self.innerIsParent as u8;
        b[14..16].copy_from_slice(&self.offnumParent.to_ne_bytes());
        b[16..18].copy_from_slice(&self.nodeI.to_ne_bytes());
        self.stateSrc.encode_into(&mut b[20..28]);
        b
    }

    pub fn decode(b: &[u8]) -> PgResult<Self> {
        if b.len() < SizeOfSpgxlogPickSplit {
            return Err(short_record_err("spgxlogPickSplit", SizeOfSpgxlogPickSplit, b.len()));
        }
        Ok(spgxlogPickSplit {
            isRootSplit: b[0] != 0,
            nDelete: u16_at(b, 2),
            nInsert: u16_at(b, 4),
            initSrc: b[6] != 0,
            initDest: b[7] != 0,
            offnumInner: u16_at(b, 8),
            initInner: b[10] != 0,
            storesNulls: b[11] != 0,
            innerIsParent: b[12] != 0,
            offnumParent: u16_at(b, 14),
            nodeI: u16_at(b, 16),
            stateSrc: spgxlogState::decode_from(&b[20..28]),
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)] // C-parity name
pub struct spgxlogVacuumLeaf {
    pub nDead: u16,
    pub nPlaceholder: u16,
    pub nMove: u16,
    pub nChain: u16,
    pub stateSrc: spgxlogState,
}

pub const SizeOfSpgxlogVacuumLeaf: usize = 16;

impl spgxlogVacuumLeaf {
    pub fn encode(&self) -> [u8; SizeOfSpgxlogVacuumLeaf] {
        let mut b = [0u8; SizeOfSpgxlogVacuumLeaf];
        b[0..2].copy_from_slice(&self.nDead.to_ne_bytes());
        b[2..4].copy_from_slice(&self.nPlaceholder.to_ne_bytes());
        b[4..6].copy_from_slice(&self.nMove.to_ne_bytes());
        b[6..8].copy_from_slice(&self.nChain.to_ne_bytes());
        self.stateSrc.encode_into(&mut b[8..16]);
        b
    }

    pub fn decode(b: &[u8]) -> PgResult<Self> {
        if b.len() < SizeOfSpgxlogVacuumLeaf {
            return Err(short_record_err("spgxlogVacuumLeaf", SizeOfSpgxlogVacuumLeaf, b.len()));
        }
        Ok(spgxlogVacuumLeaf {
            nDead: u16_at(b, 0),
            nPlaceholder: u16_at(b, 2),
            nMove: u16_at(b, 4),
            nChain: u16_at(b, 6),
            stateSrc: spgxlogState::decode_from(&b[8..16]),
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)] // C-parity name
pub struct spgxlogVacuumRoot {
    pub nDelete: u16,
    pub stateSrc: spgxlogState,
}

pub const SizeOfSpgxlogVacuumRoot: usize = 12;

impl spgxlogVacuumRoot {
    pub fn encode(&self) -> [u8; SizeOfSpgxlogVacuumRoot] {
        let mut b = [0u8; SizeOfSpgxlogVacuumRoot];
        b[0..2].copy_from_slice(&self.nDelete.to_ne_bytes());
        self.stateSrc.encode_into(&mut b[4..12]);
        b
    }

    pub fn decode(b: &[u8]) -> PgResult<Self> {
        if b.len() < SizeOfSpgxlogVacuumRoot {
            return Err(short_record_err("spgxlogVacuumRoot", SizeOfSpgxlogVacuumRoot, b.len()));
        }
        Ok(spgxlogVacuumRoot {
            nDelete: u16_at(b, 0),
            stateSrc: spgxlogState::decode_from(&b[4..12]),
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)] // C-parity name
pub struct spgxlogVacuumRedirect {
    pub nToPlaceholder: u16,
    pub firstPlaceholder: OffsetNumber,
    pub snapshotConflictHorizon: TransactionId,
    pub isCatalogRel: bool,
}

pub const SizeOfSpgxlogVacuumRedirect: usize = 10;

impl spgxlogVacuumRedirect {
    pub fn encode(&self) -> [u8; SizeOfSpgxlogVacuumRedirect] {
        let mut b = [0u8; SizeOfSpgxlogVacuumRedirect];
        b[0..2].copy_from_slice(&self.nToPlaceholder.to_ne_bytes());
        b[2..4].copy_from_slice(&self.firstPlaceholder.to_ne_bytes());
        b[4..8].copy_from_slice(&self.snapshotConflictHorizon.to_ne_bytes());
        b[8] = self.isCatalogRel as u8;
        b
    }

    pub fn decode(b: &[u8]) -> PgResult<Self> {
        if b.len() < SizeOfSpgxlogVacuumRedirect {
            return Err(short_record_err("spgxlogVacuumRedirect", SizeOfSpgxlogVacuumRedirect, b.len()));
        }
        Ok(spgxlogVacuumRedirect {
            nToPlaceholder: u16_at(b, 0),
            firstPlaceholder: u16_at(b, 2),
            snapshotConflictHorizon: TransactionId::from_ne_bytes([b[4], b[5], b[6], b[7]]),
            isCatalogRel: b[8] != 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A hostile/truncated SP-GiST WAL record with main_data shorter than the
    // fixed struct must decode to a catchable ERRCODE_DATA_CORRUPTED error, never
    // an out-of-bounds index panic in the startup redo thread.
    #[test]
    fn short_main_data_decodes_to_data_corruption_not_panic() {
        for len in 0..SizeOfSpgxlogAddLeaf {
            let e = spgxlogAddLeaf::decode(&vec![0u8; len]).unwrap_err();
            assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
        for len in 0..SizeOfSpgxlogMoveLeafs {
            let e = spgxlogMoveLeafs::decode(&vec![0u8; len]).unwrap_err();
            assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
        for len in 0..SizeOfSpgxlogAddNode {
            let e = spgxlogAddNode::decode(&vec![0u8; len]).unwrap_err();
            assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
        for len in 0..SizeOfSpgxlogSplitTuple {
            let e = spgxlogSplitTuple::decode(&vec![0u8; len]).unwrap_err();
            assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
        for len in 0..SizeOfSpgxlogPickSplit {
            let e = spgxlogPickSplit::decode(&vec![0u8; len]).unwrap_err();
            assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
        for len in 0..SizeOfSpgxlogVacuumLeaf {
            let e = spgxlogVacuumLeaf::decode(&vec![0u8; len]).unwrap_err();
            assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
        for len in 0..SizeOfSpgxlogVacuumRoot {
            let e = spgxlogVacuumRoot::decode(&vec![0u8; len]).unwrap_err();
            assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
        for len in 0..SizeOfSpgxlogVacuumRedirect {
            let e = spgxlogVacuumRedirect::decode(&vec![0u8; len]).unwrap_err();
            assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
    }

    // Exact-length payloads still decode successfully (round-trip via encode).
    #[test]
    fn exact_length_main_data_decodes_ok() {
        assert!(spgxlogAddLeaf::decode(&spgxlogAddLeaf::default().encode()).is_ok());
        assert!(spgxlogMoveLeafs::decode(&spgxlogMoveLeafs::default().encode()).is_ok());
        assert!(spgxlogAddNode::decode(&spgxlogAddNode::default().encode()).is_ok());
        assert!(spgxlogSplitTuple::decode(&spgxlogSplitTuple::default().encode()).is_ok());
        assert!(spgxlogPickSplit::decode(&spgxlogPickSplit::default().encode()).is_ok());
        assert!(spgxlogVacuumLeaf::decode(&spgxlogVacuumLeaf::default().encode()).is_ok());
        assert!(spgxlogVacuumRoot::decode(&spgxlogVacuumRoot::default().encode()).is_ok());
        assert!(spgxlogVacuumRedirect::decode(&spgxlogVacuumRedirect::default().encode()).is_ok());
    }
}
