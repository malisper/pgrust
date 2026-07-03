//! spgxlog.h record codecs; byte layouts are the C struct images (WAL parity
//! is gated byte-for-byte).
use ::types_core::{OffsetNumber, TransactionId};

pub const XLOG_SPGIST_ADD_LEAF: u8 = 0x10;
pub const XLOG_SPGIST_MOVE_LEAFS: u8 = 0x20;
pub const XLOG_SPGIST_ADD_NODE: u8 = 0x30;
pub const XLOG_SPGIST_SPLIT_TUPLE: u8 = 0x40;
pub const XLOG_SPGIST_PICKSPLIT: u8 = 0x50;
pub const XLOG_SPGIST_VACUUM_LEAF: u8 = 0x60;
pub const XLOG_SPGIST_VACUUM_ROOT: u8 = 0x70;
pub const XLOG_SPGIST_VACUUM_REDIRECT: u8 = 0x80;

#[derive(Clone, Copy, Debug, Default)]
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

    pub fn decode(b: &[u8]) -> Self {
        spgxlogAddLeaf {
            newPage: b[0] != 0,
            storesNulls: b[1] != 0,
            offnumLeaf: u16_at(b, 2),
            offnumHeadLeaf: u16_at(b, 4),
            offnumParent: u16_at(b, 6),
            nodeI: u16_at(b, 8),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
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

    pub fn decode(b: &[u8]) -> Self {
        spgxlogMoveLeafs {
            nMoves: u16_at(b, 0),
            newPage: b[2] != 0,
            replaceDead: b[3] != 0,
            storesNulls: b[4] != 0,
            offnumParent: u16_at(b, 6),
            nodeI: u16_at(b, 8),
            stateSrc: spgxlogState::decode_from(&b[12..20]),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
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

    pub fn decode(b: &[u8]) -> Self {
        spgxlogAddNode {
            offnum: u16_at(b, 0),
            offnumNew: u16_at(b, 2),
            newPage: b[4] != 0,
            parentBlk: b[5] as i8,
            offnumParent: u16_at(b, 6),
            nodeI: u16_at(b, 8),
            stateSrc: spgxlogState::decode_from(&b[12..20]),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
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

    pub fn decode(b: &[u8]) -> Self {
        spgxlogSplitTuple {
            offnumPrefix: u16_at(b, 0),
            offnumPostfix: u16_at(b, 2),
            newPage: b[4] != 0,
            postfixBlkSame: b[5] != 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
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

    pub fn decode(b: &[u8]) -> Self {
        spgxlogPickSplit {
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
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
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

    pub fn decode(b: &[u8]) -> Self {
        spgxlogVacuumLeaf {
            nDead: u16_at(b, 0),
            nPlaceholder: u16_at(b, 2),
            nMove: u16_at(b, 4),
            nChain: u16_at(b, 6),
            stateSrc: spgxlogState::decode_from(&b[8..16]),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
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

    pub fn decode(b: &[u8]) -> Self {
        spgxlogVacuumRoot {
            nDelete: u16_at(b, 0),
            stateSrc: spgxlogState::decode_from(&b[4..12]),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
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

    pub fn decode(b: &[u8]) -> Self {
        spgxlogVacuumRedirect {
            nToPlaceholder: u16_at(b, 0),
            firstPlaceholder: u16_at(b, 2),
            snapshotConflictHorizon: TransactionId::from_ne_bytes([b[4], b[5], b[6], b[7]]),
            isCatalogRel: b[8] != 0,
        }
    }
}
