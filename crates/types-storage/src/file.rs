//! Virtual-file-descriptor vocabulary (`storage/fd.h`, `common/file_utils.h`):
//! the `File` handle returned by fd.c's temp-file machinery and the
//! page-aligned I/O buffer block. The fd.c behaviour behind a `File` is owned
//! by the (unported) `storage/file/fd.c` unit and reached through its seam
//! crate; only the data shapes that callers (e.g. `buffile.c`) embed live here.

use alloc::vec;
use alloc::vec::Vec;

use types_core::BLCKSZ;

/// `File` (`storage/fd.h`) — `typedef int File`. A virtual file descriptor: an
/// index into fd.c's VFD cache, NOT an OS file descriptor. A value `> 0` is a
/// valid VFD; `<= 0` signals "no file"/error in the fd.c APIs. Modeled as a
/// newtype over the C `int` so it is not interchangeable with a raw integer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct File(pub i32);

/// `PGAlignedBlock` (`c.h`) — a `union { char data[BLCKSZ]; double force_align_d;
/// int64 force_align_i64; }` used as a `BLCKSZ`-aligned I/O buffer. The
/// alignment exists so the kernel can DMA into it; in the owned port the
/// buffer is a heap `Vec<u8>` of exactly `BLCKSZ` bytes (the Rust allocator's
/// 16-byte alignment is sufficient for the buffered read/write the callers
/// perform). Only the `data` arm is ever read or written.
#[derive(Clone, Debug)]
pub struct PGAlignedBlock {
    /// `char data[BLCKSZ]` — the block payload (always exactly `BLCKSZ` bytes).
    pub data: Vec<u8>,
}

impl Default for PGAlignedBlock {
    fn default() -> Self {
        PGAlignedBlock {
            data: vec![0u8; BLCKSZ],
        }
    }
}
