//! `storage/large_object.h` — the in-memory `LargeObjectDesc` struct describing
//! a currently-open server-side large object, the `IFS_*` flag bits, and the
//! `LOBLKSIZE` / `MAX_LARGE_OBJECT_SIZE` page-chunk constants.
//!
//! `LargeObjectDesc` is a process-local, in-memory struct, but it is laid out
//! `#[repr(C)]` (with an exact-layout assertion) so it matches the C ABI and so
//! the `inv_api.c` / `be-fsstubs.c` ports read/write its fields identically.

use crate::access::Snapshot;
use crate::types::{int64, uint64};
use crate::xact::SubTransactionId;
use crate::Oid;

/// `LOBLKSIZE` (`storage/large_object.h:70`): the data size of each
/// `pg_largeobject` page chunk — `BLCKSZ / 4`.  Server-side large objects are
/// stored in `LOBLKSIZE`-byte chunks indexed by page number.
pub const LOBLKSIZE: i32 = (crate::BLCKSZ / 4) as i32;

/// `MAX_LARGE_OBJECT_SIZE` (`storage/large_object.h:76`): the maximum length in
/// bytes for a large object — `(int64) INT_MAX * LOBLKSIZE`.
pub const MAX_LARGE_OBJECT_SIZE: int64 = i32::MAX as int64 * LOBLKSIZE as int64;

/// `IFS_RDLOCK` (`storage/large_object.h:48`): LO was opened for reading.
pub const IFS_RDLOCK: i32 = 1 << 0;
/// `IFS_WRLOCK` (`storage/large_object.h:49`): LO was opened for writing.
pub const IFS_WRLOCK: i32 = 1 << 1;

// `<stdio.h>` whence values used by `inv_seek` / `lo_lseek` (`SEEK_SET` /
// `SEEK_CUR` / `SEEK_END`).
/// `SEEK_SET` — seek relative to the start of the object.
pub const SEEK_SET: i32 = 0;
/// `SEEK_CUR` — seek relative to the current position.
pub const SEEK_CUR: i32 = 1;
/// `SEEK_END` — seek relative to the end of the object.
pub const SEEK_END: i32 = 2;

/// `typedef struct LargeObjectDesc` (`storage/large_object.h:39`).  Data about a
/// currently-open large object.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LargeObjectDesc {
    /// LO's identifier (the logical OID of the large object).
    pub id: Oid,
    /// snapshot to use for read/write operations.
    pub snapshot: Snapshot,
    /// owning subtransaction ID.
    pub subid: SubTransactionId,
    /// current seek pointer (offset within the LO).
    pub offset: uint64,
    /// see flag bits (`IFS_RDLOCK` / `IFS_WRLOCK`).
    pub flags: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn loblksize_is_blcksz_over_four() {
        // BLCKSZ == 8192 -> LOBLKSIZE == 2048.
        assert_eq!(LOBLKSIZE, 2048);
        assert_eq!(MAX_LARGE_OBJECT_SIZE, i32::MAX as int64 * 2048);
    }

    #[test]
    fn large_object_desc_layout_matches_postgres() {
        // id(Oid,4) + pad(4) + snapshot(ptr,8) + subid(uint32,4) + pad(4)
        // + offset(uint64,8) + flags(int,4) + pad(4) = 40 bytes on LP64.
        assert_eq!(align_of::<LargeObjectDesc>(), 8);
        assert_eq!(offset_of!(LargeObjectDesc, id), 0);
        assert_eq!(offset_of!(LargeObjectDesc, snapshot), 8);
        assert_eq!(offset_of!(LargeObjectDesc, subid), 16);
        assert_eq!(offset_of!(LargeObjectDesc, offset), 24);
        assert_eq!(offset_of!(LargeObjectDesc, flags), 32);
        assert_eq!(size_of::<LargeObjectDesc>(), 40);
    }
}
