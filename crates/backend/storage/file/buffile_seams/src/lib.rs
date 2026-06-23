//! Seam declarations for the `backend-storage-file-buffile` unit
//! (`storage/file/buffile.c`): buffered virtual temp files used by the hash
//! join to spill batches.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::PgResult;
use execparallel::FileSetHandle;
use ::nodes::nodehashjoin::BufFile;

seam_core::seam!(
    /// `BufFileCreateFileSet(fileset, name)` (buffile.c:266): create a `BufFile`
    /// in the given `FileSet`, which other backends attached to the same fileset
    /// can later open read-only by `name`. Used by `sharedtuplestore.c` to spill
    /// each participant's partition to `<name>.p<participant>`. Allocated in
    /// `mcx` (the accessor's context).
    pub fn buf_file_create_fileset<'mcx>(
        mcx: Mcx<'mcx>,
        fileset: FileSetHandle,
        name: &str,
    ) -> PgResult<mcx::PgBox<'mcx, BufFile>>
);

seam_core::seam!(
    /// `BufFileOpenFileSet(fileset, name, mode, missing_ok)` (buffile.c:290):
    /// open a file previously created with `BufFileCreateFileSet` in the same
    /// fileset under `name`. With `missing_ok`, returns `Ok(None)` when no such
    /// BufFile exists.
    pub fn buf_file_open_fileset<'mcx>(
        mcx: Mcx<'mcx>,
        fileset: FileSetHandle,
        name: &str,
        mode: i32,
        missing_ok: bool,
    ) -> PgResult<Option<mcx::PgBox<'mcx, BufFile>>>
);

seam_core::seam!(
    /// `BufFileClose(file)` (buffile.c) over a borrowed `&mut BufFile` — the form
    /// `sharedtuplestore.c` needs, since it holds the read/write `BufFile` in a
    /// backend-local slab and closes it in place (the by-value `buf_file_close`
    /// would consume the slab's owned box). Infallible (close paths do not
    /// `ereport(ERROR)`).
    pub fn buf_file_close_ref(file: &mut BufFile) -> PgResult<()>
);

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
    /// file. Returns the C status (0 on success, `EOF` on an impossible seek).
    /// An invalid `whence` or a `SEEK_END` size failure is `ereport(ERROR)`, so
    /// the call is fallible.
    pub fn buf_file_seek(
        file: &mut BufFile,
        fileno: i32,
        offset: i64,
        whence: i32,
    ) -> PgResult<i32>
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

seam_core::seam!(
    /// `BufFileSeekBlock(file, blknum)` (buffile.c): block-oriented absolute
    /// seek (`BufFileSeek(file, blknum / nblocks, (blknum % nblocks) * BLCKSZ,
    /// SEEK_SET)`). Returns the C status (0 on success, `EOF`==-1 on an
    /// impossible seek), so logtape can fail loudly on a non-zero result.
    pub fn buf_file_seek_block(file: &mut BufFile, blknum: i64) -> PgResult<i32>
);

seam_core::seam!(
    /// `BufFileTell(file, fileno, offset)` (buffile.c): report the current
    /// read/write position as `(fileno, offset)`. A pure field read in C —
    /// infallible.
    pub fn buf_file_tell(file: &BufFile) -> (i32, i64)
);

seam_core::seam!(
    /// `BufFileSize(file)` (buffile.c): the current size of the file in bytes.
    /// A `FileSize` failure on the last segment is `ereport(ERROR)`, so the
    /// call is fallible.
    pub fn buf_file_size(file: &BufFile) -> PgResult<i64>
);
