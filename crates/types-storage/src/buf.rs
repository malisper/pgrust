//! Shared-buffer-pool handle vocabulary (`storage/buf.h`).

/// `typedef int Buffer;` (storage/buf.h). A nonzero value is a 1-based index
/// into the shared buffer descriptors (positive) or local buffers (negative);
/// 0 is the invalid handle.
pub type Buffer = i32;

/// `#define InvalidBuffer 0` (storage/buf.h).
pub const InvalidBuffer: Buffer = 0;

/// `#define BufferIsInvalid(buffer) ((buffer) == InvalidBuffer)` (storage/buf.h).
#[inline]
pub const fn BufferIsInvalid(buffer: Buffer) -> bool {
    buffer == InvalidBuffer
}

/// `#define BufferIsValid(bufnum)` (storage/buf.h) — true for any non-invalid
/// buffer handle.
#[inline]
pub const fn BufferIsValid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}
