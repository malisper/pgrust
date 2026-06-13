//! Seam declarations for the `backend-storage-file-buffile` unit
//! (`storage/file/buffile.c`): buffered virtual temp files used by the hash
//! join to spill batches.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodehashjoin::BufFile;

seam_core::seam!(
    /// `BufFileCreateTemp(interXact)` (buffile.c): create a new temporary
    /// buffered file. Allocated/charged to `mcx` (C: the caller's current
    /// context, which `ExecHashJoinSaveTuple` switches to `spillCxt`).
    pub fn buf_file_create_temp<'mcx>(
        mcx: Mcx<'mcx>,
        inter_xact: bool,
    ) -> PgResult<mcx::PgBox<'mcx, BufFile>>
);

seam_core::seam!(
    /// `BufFileClose(file)` (buffile.c): close and delete a temporary file.
    pub fn buf_file_close<'mcx>(file: mcx::PgBox<'mcx, BufFile>) -> PgResult<()>
);

seam_core::seam!(
    /// `BufFileSeek(file, fileno, offset, whence)` (buffile.c): seek within the
    /// file. Returns the C status (0 on success, EOF/non-zero on failure).
    pub fn buf_file_seek(file: &mut BufFile, fileno: i32, offset: i64, whence: i32) -> i32
);

seam_core::seam!(
    /// `BufFileWrite(file, ptr, size)` (buffile.c): append bytes to the file.
    /// Fallible: a write error is `ereport(ERROR)` in C.
    pub fn buf_file_write(file: &mut BufFile, data: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `BufFileReadMaybeEOF(file, ptr, size, eofOK)` (buffile.c): read exactly
    /// `buf.len()` bytes; with `eof_ok` true a clean EOF at the start returns
    /// `Ok(0)` (the C `nread == 0`), otherwise a short read is `ereport(ERROR)`.
    /// Returns the number of bytes read (0 = EOF, else `buf.len()`).
    pub fn buf_file_read_maybe_eof(file: &mut BufFile, buf: &mut [u8], eof_ok: bool)
        -> PgResult<usize>
);

seam_core::seam!(
    /// `BufFileReadExact(file, ptr, size)` (buffile.c): read exactly `buf.len()`
    /// bytes; a short read is `ereport(ERROR)`.
    pub fn buf_file_read_exact(file: &mut BufFile, buf: &mut [u8]) -> PgResult<()>
);
