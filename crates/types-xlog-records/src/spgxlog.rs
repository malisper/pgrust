//! SP-GiST rmgr WAL record bodies (`access/spgxlog.h`), trimmed to the fields
//! ports consume so far. The embedded `spgxlogState stateSrc` members are not
//! consumed and are represented only by the offsets of the fields around them.

use crate::bytes::{bool_at, i8_at, u16_at, u32_at};
use types_core::{OffsetNumber, TransactionId};

/// `spgxlogAddLeaf`: `{bool newPage; bool storesNulls;
/// OffsetNumber offnumLeaf; OffsetNumber offnumHeadLeaf;
/// OffsetNumber offnumParent; uint16 nodeI;}`.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogAddLeaf {
    pub newPage: bool,
    pub storesNulls: bool,
    pub offnumLeaf: OffsetNumber,
    pub offnumHeadLeaf: OffsetNumber,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
}

impl spgxlogAddLeaf {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            newPage: bool_at(rec, 0),
            storesNulls: bool_at(rec, 1),
            offnumLeaf: u16_at(rec, 2),
            offnumHeadLeaf: u16_at(rec, 4),
            offnumParent: u16_at(rec, 6),
            nodeI: u16_at(rec, 8),
        }
    }
}

/// `spgxlogMoveLeafs`: `{uint16 nMoves; bool newPage; bool replaceDead;
/// bool storesNulls; OffsetNumber offnumParent; uint16 nodeI;
/// spgxlogState stateSrc; OffsetNumber offsets[];}` — trimmed of `stateSrc`
/// and the trailing offsets.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogMoveLeafs {
    pub nMoves: u16,
    pub newPage: bool,
    pub replaceDead: bool,
    pub storesNulls: bool,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
}

impl spgxlogMoveLeafs {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            nMoves: u16_at(rec, 0),
            newPage: bool_at(rec, 2),
            replaceDead: bool_at(rec, 3),
            storesNulls: bool_at(rec, 4),
            offnumParent: u16_at(rec, 6),
            nodeI: u16_at(rec, 8),
        }
    }
}

/// `spgxlogAddNode`: `{OffsetNumber offnum; OffsetNumber offnumNew;
/// bool newPage; int8 parentBlk; OffsetNumber offnumParent; uint16 nodeI;
/// spgxlogState stateSrc;}` — trimmed of `stateSrc`.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogAddNode {
    pub offnum: OffsetNumber,
    pub offnumNew: OffsetNumber,
    pub newPage: bool,
    pub parentBlk: i8,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
}

impl spgxlogAddNode {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            offnum: u16_at(rec, 0),
            offnumNew: u16_at(rec, 2),
            newPage: bool_at(rec, 4),
            parentBlk: i8_at(rec, 5),
            offnumParent: u16_at(rec, 6),
            nodeI: u16_at(rec, 8),
        }
    }
}

/// `spgxlogSplitTuple`: `{OffsetNumber offnumPrefix;
/// OffsetNumber offnumPostfix; bool newPage; bool postfixBlkSame;}`.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogSplitTuple {
    pub offnumPrefix: OffsetNumber,
    pub offnumPostfix: OffsetNumber,
    pub newPage: bool,
    pub postfixBlkSame: bool,
}

impl spgxlogSplitTuple {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            offnumPrefix: u16_at(rec, 0),
            offnumPostfix: u16_at(rec, 2),
            newPage: bool_at(rec, 4),
            postfixBlkSame: bool_at(rec, 5),
        }
    }
}

/// `spgxlogPickSplit`: trimmed of `initSrc`/`initDest`/`stateSrc` and the
/// trailing offsets; layout `{bool isRootSplit /*0*/; uint16 nDelete /*2*/;
/// uint16 nInsert /*4*/; bool initSrc /*6*/; bool initDest /*7*/;
/// OffsetNumber offnumInner /*8*/; bool initInner /*10*/;
/// bool storesNulls /*11*/; bool innerIsParent /*12*/;
/// OffsetNumber offnumParent /*14*/; uint16 nodeI /*16*/;
/// spgxlogState stateSrc; OffsetNumber offsets[];}`.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogPickSplit {
    pub isRootSplit: bool,
    pub nDelete: u16,
    pub nInsert: u16,
    pub offnumInner: OffsetNumber,
    pub initInner: bool,
    pub storesNulls: bool,
    pub innerIsParent: bool,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
}

impl spgxlogPickSplit {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            isRootSplit: bool_at(rec, 0),
            nDelete: u16_at(rec, 2),
            nInsert: u16_at(rec, 4),
            offnumInner: u16_at(rec, 8),
            initInner: bool_at(rec, 10),
            storesNulls: bool_at(rec, 11),
            innerIsParent: bool_at(rec, 12),
            offnumParent: u16_at(rec, 14),
            nodeI: u16_at(rec, 16),
        }
    }
}

/// `spgxlogVacuumLeaf`: `{uint16 nDead; uint16 nPlaceholder; uint16 nMove;
/// uint16 nChain; spgxlogState stateSrc; OffsetNumber offsets[];}` — trimmed
/// of `stateSrc` and the trailing offsets.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogVacuumLeaf {
    pub nDead: u16,
    pub nPlaceholder: u16,
    pub nMove: u16,
    pub nChain: u16,
}

impl spgxlogVacuumLeaf {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            nDead: u16_at(rec, 0),
            nPlaceholder: u16_at(rec, 2),
            nMove: u16_at(rec, 4),
            nChain: u16_at(rec, 6),
        }
    }
}

/// `spgxlogVacuumRoot`: `{uint16 nDelete; spgxlogState stateSrc;
/// OffsetNumber offsets[];}` — trimmed of `stateSrc` and the offsets.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogVacuumRoot {
    pub nDelete: u16,
}

impl spgxlogVacuumRoot {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { nDelete: u16_at(rec, 0) }
    }
}

/// `spgxlogVacuumRedirect`: `{uint16 nToPlaceholder;
/// OffsetNumber firstPlaceholder; TransactionId snapshotConflictHorizon;
/// bool isCatalogRel; OffsetNumber offsets[];}` — trimmed of the offsets.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogVacuumRedirect {
    pub nToPlaceholder: u16,
    pub firstPlaceholder: OffsetNumber,
    pub snapshotConflictHorizon: TransactionId,
    pub isCatalogRel: bool,
}

impl spgxlogVacuumRedirect {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            nToPlaceholder: u16_at(rec, 0),
            firstPlaceholder: u16_at(rec, 2),
            snapshotConflictHorizon: u32_at(rec, 4),
            isCatalogRel: bool_at(rec, 8),
        }
    }
}
