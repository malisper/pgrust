use ::types_core::{int64, uint64, Oid, SubTransactionId, BLCKSZ};

pub const IFS_RDLOCK: i32 = 1 << 0;
pub const IFS_WRLOCK: i32 = 1 << 1;

pub const LOBLKSIZE: i32 = (BLCKSZ / 4) as i32;

pub const MAX_LARGE_OBJECT_SIZE: int64 = i32::MAX as int64 * LOBLKSIZE as int64;

// C divergence from fabled: `snapshot` is generic — the snapshot crate is not
// ported yet; the LO owner instantiates `Snap` with its shared-snapshot handle
// (C: `Snapshot`, a shared pointer).
#[derive(Debug)]
pub struct LargeObjectDesc<Snap> {
    pub id: Oid,
    pub snapshot: Snap,
    pub subid: SubTransactionId,
    pub offset: uint64,
    pub flags: i32,
}
