#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the project-wide `PgResult` (== `Result<_,
// PgError>`); `PgError` is a large owned struct, so the un-boxed `Err` variant
// trips `clippy::result_large_err`. The un-boxed return is the project's error
// contract, so accept the lint crate-wide.
#![allow(clippy::result_large_err)]

//! `backend-storage-file-buffile` — a port of `src/backend/storage/file/buffile.c`.
//!
//! `BufFile` provides the buffering aspect of stdio on top of fd.c's virtual
//! files. A `BufFile` breaks its logical data stream into
//! [`MAX_PHYSICAL_FILESIZE`]-byte physical segments (each a VFD
//! [`File`](types_storage::file::File) managed by fd.c), so it can hold temp
//! data exceeding the OS per-file size limit — the feature behind sorts and
//! hash joins on large inputs. A `BufFile` is either a standalone temp file
//! ([`BufFileCreateTemp`]) or a member of a [`FileSet`](types_storage) that
//! other backends can discover ([`BufFileCreateFileSet`] /
//! [`BufFileOpenFileSet`]).
//!
//! The canonical [`BufFile`] struct lives in `types_nodes::nodehash` (so the
//! `utils/sort` + executor seams can be typed over it); this crate owns the
//! buffered-I/O behaviour, exposed as free functions taking `&mut BufFile`
//! (an inherent `impl` would violate the orphan rule). `BufFileClose` /
//! `BufFileAppend` consume their argument by value (their C `pfree` analog).
//!
//! Edges into not-yet-ported owners (`fd.c`, `fileset.c`, the
//! `tablespace.c` `PrepareTempTablespaces`, and `postgres.c`
//! `CHECK_FOR_INTERRUPTS`) cross those owners' seam crates and panic until they
//! land. The `track_io_timing` GUC and the `pgBufferUsage` temp-block counters
//! are reached by direct dependency (no cycle).

use backend_utils_error::{ereport, errno};
use mcx::{Mcx, PgBox};
use types_error::{ErrorLocation, PgError, PgResult, ERROR};
use types_execparallel::FileSetHandle;
use types_nodes::nodehash::BufFile;
use types_storage::file::{File, PGAlignedBlock};

use backend_executor_instrument::with_pgBufferUsage;
use backend_storage_file_fd_seams as fd;
use backend_storage_file_fileset_seams as fileset;
use backend_utils_misc_guc_tables::vars::track_io_timing;
use portability_instr_time::instr_time_set_current_lazy;
use types_core::instr_time;

mod seams;
pub use seams::init_seams;

const BUFFILE_C: &str = "buffile.c";

// ---------------------------------------------------------------------------
// Compile-time constants from buffile.c.
// ---------------------------------------------------------------------------

/// `BLCKSZ` (pg_config.h) — the page/block size, 8 KiB.
const BLCKSZ: usize = types_core::BLCKSZ;

/// `MAX_PHYSICAL_FILESIZE` (buffile.c:62) — `0x40000000` (1 GiB). We break
/// BufFiles into gigabyte-sized segments, regardless of `RELSEG_SIZE`, so large
/// BufFiles can spread across multiple tablespaces when available.
pub const MAX_PHYSICAL_FILESIZE: i64 = 0x4000_0000;

/// `BUFFILE_SEG_SIZE` (buffile.c:63) — `MAX_PHYSICAL_FILESIZE / BLCKSZ`.
pub const BUFFILE_SEG_SIZE: i64 = MAX_PHYSICAL_FILESIZE / BLCKSZ as i64;

// Wait-event selectors threaded into the fd File API. `WAIT_EVENT_BUFFILE_*`
// = `PG_WAIT_IO | idx` (`utils/activity/wait_event_names.txt`); the values
// match the generated `WaitEventIO` enum so a co-resident C backend's
// `pg_stat_activity` agrees.
const PG_WAIT_IO: u32 = 0x0A00_0000;
const WAIT_EVENT_BUFFILE_READ: u32 = PG_WAIT_IO | 6;
const WAIT_EVENT_BUFFILE_TRUNCATE: u32 = PG_WAIT_IO | 7;
const WAIT_EVENT_BUFFILE_WRITE: u32 = PG_WAIT_IO | 8;

/// `O_RDONLY` — used by [`BufFileOpenFileSet`] to decide `readOnly`.
const O_RDONLY: i32 = 0x0000;

// SEEK_* whence codes (stdio.h / unistd.h).
const SEEK_SET: i32 = 0;
const SEEK_CUR: i32 = 1;
const SEEK_END: i32 = 2;

/// C's `EOF` (the [`BufFileSeek`] / [`BufFileSeekBlock`] failure return).
const EOF: i32 = -1;

// ---------------------------------------------------------------------------
// makeBufFileCommon / makeBufFile (buffile.c:117-150)
// ---------------------------------------------------------------------------
//
// `BufFile` is defined in `types_nodes`, so an inherent `impl` is illegal here
// (orphan rule). The constructors are free functions.

/// `makeBufFileCommon(nfiles)` (buffile.c:117-132) — create a `BufFile` and
/// perform the common initialization. `nfiles` is implicit in the caller's
/// `files` vector; each constructor pushes its segment(s) and keeps `numFiles`
/// in step.
///
/// C captures `file->resowner = CurrentResourceOwner` here, used only by the
/// [`BufFileAppend`] same-owner check. The fd layer (unported) owns the
/// temp-file/owner association at open time; the per-`BufFile` owner handle is
/// left `None` until the fd/resowner edge is wired, so the [`BufFileAppend`]
/// check compares `None == None` (never the error) — equivalent to two
/// BufFiles created under the same owner, which is the only supported case.
fn makeBufFileCommon() -> BufFile {
    BufFile {
        numFiles: 0,
        files: Vec::new(),
        isInterXact: false,
        dirty: false,
        readOnly: false,
        fileset: None,
        name: None,
        resowner: None,
        curFile: 0,
        curOffset: 0,
        pos: 0,
        nbytes: 0,
        buffer: PGAlignedBlock::default(),
    }
}

/// `makeBufFile(firstfile)` (buffile.c:138-150) — create a `BufFile` given the
/// first underlying physical file. NOTE: caller must set `isInterXact`.
fn makeBufFile(firstfile: File) -> BufFile {
    let mut file = makeBufFileCommon();
    file.files.push(firstfile);
    file.numFiles = file.files.len() as i32;
    file.readOnly = false;
    file.fileset = None;
    file.name = None;
    file
}

/// `extendBufFile(file)` (buffile.c:155-178) — add another component temp file.
/// C swaps `CurrentResourceOwner` to the BufFile's owner around the open so the
/// new segment is associated with it; the fd layer handles owner association
/// internally at open, so no swap is modelled here.
fn extendBufFile(file: &mut BufFile) -> PgResult<()> {
    let pfile = match file.fileset {
        None => fd::open_temporary_file::call(file.isInterXact)?,
        Some(fileset) => {
            let name = file
                .name
                .clone()
                .expect("fileset-backed BufFile must carry a name");
            MakeNewFileSetSegment(fileset, &name, file.files.len() as i32)?
        }
    };
    debug_assert!(pfile.0 >= 0);
    file.files.push(pfile);
    file.numFiles = file.files.len() as i32;
    Ok(())
}

// ---------------------------------------------------------------------------
// FileSetSegmentName / MakeNewFileSetSegment (buffile.c:221-253)
// ---------------------------------------------------------------------------

/// `FileSetSegmentName(name, buffile_name, segment)` (buffile.c:221-225) — build
/// the name for a given segment of a given BufFile (`"%s.%d"`).
fn FileSetSegmentName(buffile_name: &str, segment: i32) -> String {
    format!("{buffile_name}.{segment}")
}

/// `MakeNewFileSetSegment(buffile, segment)` (buffile.c:230-253) — create a new
/// segment file backing a fileset-based BufFile. There may be files left over
/// from before a crash restart with the same name; so [`BufFileOpenFileSet`]
/// does not get confused about how many segments there are, first unlink the
/// `segment + 1` file if it already exists, then create `segment`.
fn MakeNewFileSetSegment(fileset: FileSetHandle, buffile_name: &str, segment: i32) -> PgResult<File> {
    let next_name = FileSetSegmentName(buffile_name, segment + 1);
    fileset::file_set_delete::call(fileset, &next_name, true)?;

    let segment_name = FileSetSegmentName(buffile_name, segment);
    let file = fileset::file_set_create::call(fileset, &segment_name)?;

    debug_assert!(file.0 > 0);
    Ok(file)
}

// ---------------------------------------------------------------------------
// BufFileCreateTemp (buffile.c:192-216)
// ---------------------------------------------------------------------------

/// `BufFileCreateTemp(interXact)` (buffile.c:192-216) — create a `BufFile` for a
/// new temporary file (which expands to multiple physical files past
/// [`MAX_PHYSICAL_FILESIZE`]). The struct is allocated in `mcx` (C: the
/// caller's current context).
pub fn BufFileCreateTemp<'mcx>(mcx: Mcx<'mcx>, interXact: bool) -> PgResult<PgBox<'mcx, BufFile>> {
    // Ensure temp tablespaces are set up for OpenTemporaryFile to use.
    backend_commands_tablespace_seams::prepare_temp_tablespaces::call()?;

    let pfile = fd::open_temporary_file::call(interXact)?;
    debug_assert!(pfile.0 >= 0);

    let mut file = makeBufFile(pfile);
    file.isInterXact = interXact;
    mcx::alloc_in(mcx, file)
}

// ---------------------------------------------------------------------------
// BufFileCreateFileSet (buffile.c:266-279)
// ---------------------------------------------------------------------------

/// `BufFileCreateFileSet(fileset, name)` (buffile.c:266-279) — create a `BufFile`
/// other backends attached to the same fileset can open read-only by `name`.
pub fn BufFileCreateFileSet<'mcx>(
    mcx: Mcx<'mcx>,
    fileset: FileSetHandle,
    name: &str,
) -> PgResult<PgBox<'mcx, BufFile>> {
    let mut file = makeBufFileCommon();
    file.fileset = Some(fileset);
    file.name = Some(name.to_owned());
    let segment = MakeNewFileSetSegment(fileset, name, 0)?;
    file.files.push(segment);
    file.numFiles = file.files.len() as i32;
    file.readOnly = false;
    mcx::alloc_in(mcx, file)
}

// ---------------------------------------------------------------------------
// BufFileOpenFileSet (buffile.c:290-349)
// ---------------------------------------------------------------------------

/// `BufFileOpenFileSet(fileset, name, mode, missing_ok)` (buffile.c:290-349) —
/// open a file previously created with [`BufFileCreateFileSet`] in the same
/// fileset under `name`. With `missing_ok`, returns `Ok(None)` when no such
/// BufFile exists; otherwise raises an error.
pub fn BufFileOpenFileSet<'mcx>(
    mcx: Mcx<'mcx>,
    fileset: FileSetHandle,
    name: &str,
    mode: i32,
    missing_ok: bool,
) -> PgResult<Option<PgBox<'mcx, BufFile>>> {
    // The Vec grows itself, so C's explicit capacity-doubling probe is moot.
    let mut files: Vec<File> = Vec::new();
    let mut nfiles = 0i32;
    // The name reported on the not-found error: C's `segment_name` after the
    // last probe (which, when nothing was found, is the `name.0` segment).
    let segment_name = loop {
        let this_segment_name = FileSetSegmentName(name, nfiles);
        let f = fileset::file_set_open::call(fileset, &this_segment_name, mode)?;
        if f.0 <= 0 {
            break this_segment_name;
        }
        files.push(f);
        nfiles += 1;

        backend_tcop_postgres_seams::check_for_interrupts::call()?;
    };

    if nfiles == 0 {
        // pfree(files): the Vec drops here.
        if missing_ok {
            return Ok(None);
        }
        return Err(file_access_error(
            format!("could not open temporary file \"{segment_name}\" from BufFile \"{name}\""),
            336,
            "BufFileOpenFileSet",
        ));
    }

    let mut file = makeBufFileCommon();
    file.files = files;
    file.numFiles = file.files.len() as i32;
    file.readOnly = mode == O_RDONLY;
    file.fileset = Some(fileset);
    file.name = Some(name.to_owned());
    Ok(Some(mcx::alloc_in(mcx, file)?))
}

// ---------------------------------------------------------------------------
// BufFileDeleteFileSet (buffile.c:363-388)
// ---------------------------------------------------------------------------

/// `BufFileDeleteFileSet(fileset, name, missing_ok)` (buffile.c:363-388) — delete
/// a `BufFile` created by [`BufFileCreateFileSet`]. Only one backend should
/// attempt to delete a given name.
pub fn BufFileDeleteFileSet(fileset: FileSetHandle, name: &str, missing_ok: bool) -> PgResult<()> {
    let mut segment = 0i32;
    let mut found = false;

    loop {
        let segment_name = FileSetSegmentName(name, segment);
        if !fileset::file_set_delete::call(fileset, &segment_name, true)? {
            break;
        }
        found = true;
        segment += 1;

        backend_tcop_postgres_seams::check_for_interrupts::call()?;
    }

    if !found && !missing_ok {
        return Err(elog_error(
            format!("could not delete unknown BufFile \"{name}\""),
            387,
            "BufFileDeleteFileSet",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// BufFileExportFileSet (buffile.c:393-404)
// ---------------------------------------------------------------------------

/// `BufFileExportFileSet(file)` (buffile.c:393-404) — flush and make read-only,
/// in preparation for sharing.
pub fn BufFileExportFileSet(file: &mut BufFile) -> PgResult<()> {
    debug_assert!(file.fileset.is_some());
    debug_assert!(!file.readOnly);

    BufFileFlush(file)?;
    file.readOnly = true;
    Ok(())
}

// ---------------------------------------------------------------------------
// BufFileClose (buffile.c:411-424)
// ---------------------------------------------------------------------------

/// `BufFileClose(file)` (buffile.c:411-424) — flush, then `FileClose` every
/// underlying physical file. The C `pfree(file->files); pfree(file)` is the
/// caller dropping the owning `PgBox<BufFile>` after this returns, so the port
/// takes `&mut BufFile` and lets the box's `Drop` reclaim the storage.
pub fn BufFileClose(file: &mut BufFile) -> PgResult<()> {
    BufFileFlush(file)?;
    for &pfile in &file.files {
        fd::file_close::call(pfile);
    }
    // pfree(file->files); pfree(file): the caller's PgBox drop.
    Ok(())
}

// ---------------------------------------------------------------------------
// BufFileLoadBuffer (buffile.c:433-484)
// ---------------------------------------------------------------------------

/// `BufFileLoadBuffer(file)` (buffile.c:433-484) — load some data into the
/// buffer, if possible, starting from `curOffset`. At call: `dirty == false`,
/// `pos` and `nbytes == 0`. On exit, `nbytes` is the count loaded.
fn BufFileLoadBuffer(file: &mut BufFile) -> PgResult<()> {
    // Advance to next component file if necessary and possible.
    if file.curOffset >= MAX_PHYSICAL_FILESIZE && (file.curFile as usize + 1) < file.files.len() {
        file.curFile += 1;
        file.curOffset = 0;
    }

    let thisfile = file.files[file.curFile as usize];

    let io_start = io_timer_start();

    // Read whatever we can get, up to a full bufferload.
    let nbytes = fd::file_read::call(
        thisfile,
        &mut file.buffer.data[..],
        file.curOffset,
        WAIT_EVENT_BUFFILE_READ,
    )?;
    if nbytes < 0 {
        file.nbytes = 0;
        return Err(file_access_error(
            format!("could not read file \"{}\"", fd::file_path_name::call(thisfile)),
            471,
            "BufFileLoadBuffer",
        ));
    }
    file.nbytes = nbytes as i32;

    io_timer_accum_read(io_start);

    // we choose not to advance curOffset here
    if file.nbytes > 0 {
        with_pgBufferUsage(|u| u.temp_blks_read += 1);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// BufFileDumpBuffer (buffile.c:493-580)
// ---------------------------------------------------------------------------

/// `BufFileDumpBuffer(file)` (buffile.c:493-580) — dump buffer contents starting
/// at `curOffset`. At call: `dirty == true`, `nbytes > 0`. On exit, `dirty` is
/// cleared and `curOffset` is reconciled to the logical file position.
fn BufFileDumpBuffer(file: &mut BufFile) -> PgResult<()> {
    let mut wpos = 0usize;

    // Dump the whole buffer even across a component boundary.
    while wpos < file.nbytes as usize {
        // Advance to next component file if necessary, extending as needed.
        if file.curOffset >= MAX_PHYSICAL_FILESIZE {
            while file.curFile as usize + 1 >= file.files.len() {
                extendBufFile(file)?;
            }
            file.curFile += 1;
            file.curOffset = 0;
        }

        let mut bytestowrite = file.nbytes as usize - wpos;
        let availbytes = (MAX_PHYSICAL_FILESIZE - file.curOffset) as usize;
        if bytestowrite > availbytes {
            bytestowrite = availbytes;
        }

        let thisfile = file.files[file.curFile as usize];

        let io_start = io_timer_start();

        let written = fd::file_write::call(
            thisfile,
            &file.buffer.data[wpos..wpos + bytestowrite],
            file.curOffset,
            WAIT_EVENT_BUFFILE_WRITE,
        )?;
        if written <= 0 {
            return Err(file_access_error(
                format!("could not write to file \"{}\"", fd::file_path_name::call(thisfile)),
                546,
                "BufFileDumpBuffer",
            ));
        }

        io_timer_accum_write(io_start);

        file.curOffset += written as i64;
        wpos += written as usize;

        with_pgBufferUsage(|u| u.temp_blks_written += 1);
    }
    file.dirty = false;

    // Reconcile curOffset to the logical file position (original value + pos),
    // which may be less than where we wrote (a small backwards seek in a dirty
    // buffer).
    file.curOffset -= (file.nbytes - file.pos) as i64;
    if file.curOffset < 0 {
        // handle possible segment crossing
        file.curFile -= 1;
        debug_assert!(file.curFile >= 0);
        file.curOffset += MAX_PHYSICAL_FILESIZE;
    }

    // Now we can set the buffer empty without changing the logical position.
    file.pos = 0;
    file.nbytes = 0;
    Ok(())
}

// ---------------------------------------------------------------------------
// BufFileReadCommon + variants (buffile.c:592-667)
// ---------------------------------------------------------------------------

/// `BufFileReadCommon(file, ptr, size, exact, eofOK)` (buffile.c:592-638) — like
/// `fread()` with 1-byte elements, reporting I/O errors via `ereport`. With
/// `exact`, a short read is an error unless `eof_ok` and zero bytes were read.
fn BufFileReadCommon(file: &mut BufFile, ptr: &mut [u8], exact: bool, eofOK: bool) -> PgResult<usize> {
    let start_size = ptr.len();
    let mut nread = 0usize;

    BufFileFlush(file)?;

    let mut out = ptr;
    while !out.is_empty() {
        if file.pos >= file.nbytes {
            // Try to load more data into the buffer.
            file.curOffset += file.pos as i64;
            file.pos = 0;
            file.nbytes = 0;
            BufFileLoadBuffer(file)?;
            if file.nbytes <= 0 {
                break; // no more data available
            }
        }

        let mut nthistime = (file.nbytes - file.pos) as usize;
        if nthistime > out.len() {
            nthistime = out.len();
        }
        debug_assert!(nthistime > 0);

        let pos = file.pos as usize;
        out[..nthistime].copy_from_slice(&file.buffer.data[pos..pos + nthistime]);

        file.pos += nthistime as i32;
        let (_, rest) = out.split_at_mut(nthistime);
        out = rest;
        nread += nthistime;
    }

    if exact && nread != start_size && !(nread == 0 && eofOK) {
        let msg = match &file.name {
            Some(name) => format!(
                "could not read from file set \"{name}\": read only {nread} of {start_size} bytes"
            ),
            None => format!(
                "could not read from temporary file: read only {nread} of {start_size} bytes"
            ),
        };
        return Err(file_access_error(msg, 635, "BufFileReadCommon"));
    }

    Ok(nread)
}

/// `BufFileRead(file, ptr, size)` (buffile.c:644-648) — legacy interface where
/// the caller checks for EOF / short reads. Returns the bytes read.
pub fn BufFileRead(file: &mut BufFile, ptr: &mut [u8]) -> PgResult<usize> {
    BufFileReadCommon(file, ptr, false, false)
}

/// `BufFileReadExact(file, ptr, size)` (buffile.c:653-657) — require a read of
/// exactly the requested size.
pub fn BufFileReadExact(file: &mut BufFile, ptr: &mut [u8]) -> PgResult<()> {
    BufFileReadCommon(file, ptr, true, false)?;
    Ok(())
}

/// `BufFileReadMaybeEOF(file, ptr, size, eofOK)` (buffile.c:663-667) — require a
/// read of exactly the requested size, but optionally allow EOF (returns 0).
pub fn BufFileReadMaybeEOF(file: &mut BufFile, ptr: &mut [u8], eofOK: bool) -> PgResult<usize> {
    BufFileReadCommon(file, ptr, true, eofOK)
}

// ---------------------------------------------------------------------------
// BufFileWrite (buffile.c:675-712)
// ---------------------------------------------------------------------------

/// `BufFileWrite(file, ptr, size)` (buffile.c:675-712) — like `fwrite()` with
/// 1-byte elements, reporting errors via `ereport`.
pub fn BufFileWrite(file: &mut BufFile, ptr: &[u8]) -> PgResult<()> {
    debug_assert!(!file.readOnly);

    let mut input = ptr;
    while !input.is_empty() {
        if file.pos as usize >= BLCKSZ {
            // Buffer full, dump it out.
            if file.dirty {
                BufFileDumpBuffer(file)?;
            } else {
                // Hmm, went directly from reading to writing?
                file.curOffset += file.pos as i64;
                file.pos = 0;
                file.nbytes = 0;
            }
        }

        let mut nthistime = BLCKSZ - file.pos as usize;
        if nthistime > input.len() {
            nthistime = input.len();
        }
        debug_assert!(nthistime > 0);

        let pos = file.pos as usize;
        file.buffer.data[pos..pos + nthistime].copy_from_slice(&input[..nthistime]);

        file.dirty = true;
        file.pos += nthistime as i32;
        if file.nbytes < file.pos {
            file.nbytes = file.pos;
        }
        input = &input[nthistime..];
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// BufFileFlush (buffile.c:719-726)
// ---------------------------------------------------------------------------

/// `BufFileFlush(file)` (buffile.c:719-726) — like `fflush()`, except I/O errors
/// are reported with `ereport`.
fn BufFileFlush(file: &mut BufFile) -> PgResult<()> {
    if file.dirty {
        BufFileDumpBuffer(file)?;
    }
    debug_assert!(!file.dirty);
    Ok(())
}

// ---------------------------------------------------------------------------
// BufFileSeek (buffile.c:739-830)
// ---------------------------------------------------------------------------

/// `BufFileSeek(file, fileno, offset, whence)` (buffile.c:739-830) — like
/// `fseek()`, except the target position needs two values (`fileno`, `offset`)
/// so it works when the logical filesize exceeds `off_t`. Returns `0` if OK,
/// [`EOF`] if not (the logical position is not moved on an impossible seek).
pub fn BufFileSeek(file: &mut BufFile, fileno: i32, offset: i64, whence: i32) -> PgResult<i32> {
    let mut newFile: i64;
    let mut newOffset: i64;

    match whence {
        SEEK_SET => {
            if fileno < 0 {
                return Ok(EOF);
            }
            newFile = fileno as i64;
            newOffset = offset;
        }
        SEEK_CUR => {
            // Relative seek considers only the signed offset, ignoring fileno.
            newFile = file.curFile as i64;
            newOffset = (file.curOffset + file.pos as i64) + offset;
        }
        SEEK_END => {
            // The file size of the last file gives the end offset of that file.
            newFile = file.files.len() as i64 - 1;
            let last = file.files[file.files.len() - 1];
            newOffset = fd::file_size::call(last)?;
            if newOffset < 0 {
                return Err(size_error(file, 774, "BufFileSeek"));
            }
        }
        _ => {
            return Err(elog_error(
                format!("invalid whence: {whence}"),
                779,
                "BufFileSeek",
            ));
        }
    }

    while newOffset < 0 {
        newFile -= 1;
        if newFile < 0 {
            return Ok(EOF);
        }
        newOffset += MAX_PHYSICAL_FILESIZE;
    }

    if newFile == file.curFile as i64
        && newOffset >= file.curOffset
        && newOffset <= file.curOffset + file.nbytes as i64
    {
        // Seek is to a point within the existing buffer; adjust pos only.
        file.pos = (newOffset - file.curOffset) as i32;
        return Ok(0);
    }

    // Otherwise, must reposition buffer, so flush any dirty data.
    BufFileFlush(file)?;

    // Convert seek to "start of next seg" into "end of last seg" — only after
    // the flush, which could have created a new segment.
    if newFile == file.files.len() as i64 && newOffset == 0 {
        newFile -= 1;
        newOffset = MAX_PHYSICAL_FILESIZE;
    }
    while newOffset > MAX_PHYSICAL_FILESIZE {
        newFile += 1;
        if newFile >= file.files.len() as i64 {
            return Ok(EOF);
        }
        newOffset -= MAX_PHYSICAL_FILESIZE;
    }
    if newFile >= file.files.len() as i64 {
        return Ok(EOF);
    }
    // Seek is OK!
    file.curFile = newFile as i32;
    file.curOffset = newOffset;
    file.pos = 0;
    file.nbytes = 0;
    Ok(0)
}

// ---------------------------------------------------------------------------
// BufFileTell (buffile.c:832-837)
// ---------------------------------------------------------------------------

/// `BufFileTell(file, *fileno, *offset)` (buffile.c:832-837) — report the current
/// logical position as `(fileno, offset)`.
pub fn BufFileTell(file: &BufFile) -> (i32, i64) {
    (file.curFile, file.curOffset + file.pos as i64)
}

// ---------------------------------------------------------------------------
// BufFileSeekBlock (buffile.c:850-857)
// ---------------------------------------------------------------------------

/// `BufFileSeekBlock(file, blknum)` (buffile.c:850-857) — block-oriented absolute
/// seek to the start of the `blknum`'th `BLCKSZ`-sized block.
pub fn BufFileSeekBlock(file: &mut BufFile, blknum: i64) -> PgResult<i32> {
    BufFileSeek(
        file,
        (blknum / BUFFILE_SEG_SIZE) as i32,
        (blknum % BUFFILE_SEG_SIZE) * BLCKSZ as i64,
        SEEK_SET,
    )
}

// ---------------------------------------------------------------------------
// BufFileSize (buffile.c:865-881)
// ---------------------------------------------------------------------------

/// `BufFileSize(file)` (buffile.c:865-881) — the amount of data in the `BufFile`
/// in bytes (including holes left by [`BufFileAppend`]). Errors on a failure to
/// stat the last segment.
pub fn BufFileSize(file: &BufFile) -> PgResult<i64> {
    let last = file.files[file.files.len() - 1];
    let lastFileSize = fd::file_size::call(last)?;
    if lastFileSize < 0 {
        return Err(size_error(file, 876, "BufFileSize"));
    }

    Ok((file.files.len() as i64 - 1) * MAX_PHYSICAL_FILESIZE + lastFileSize)
}

// ---------------------------------------------------------------------------
// BufFileAppend (buffile.c:901-921)
// ---------------------------------------------------------------------------

/// `BufFileAppend(target, source)` (buffile.c:901-921) — append `source`'s
/// contents to the end of `target`, subsuming ownership of `source`'s segments
/// (so `source` is consumed). Content is appended at a
/// [`MAX_PHYSICAL_FILESIZE`]-aligned boundary, typically leaving holes. Returns
/// the block number within `target` where `source`'s content begins.
pub fn BufFileAppend(target: &mut BufFile, mut source: BufFile) -> PgResult<i64> {
    let startBlock = target.files.len() as i64 * BUFFILE_SEG_SIZE;

    debug_assert!(source.readOnly);
    debug_assert!(!source.dirty);

    // C compares `target->resowner != source->resowner` and elog(ERROR)s on a
    // mismatch; preserve that check using the recorded owner handles.
    if target.resowner != source.resowner {
        return Err(elog_error(
            "could not append BufFile with non-matching resource owner".to_owned(),
            912,
            "BufFileAppend",
        ));
    }

    // Splice source's segments onto target (C: repalloc + copy, then
    // `target->numFiles += source->numFiles`); keep numFiles == files.len().
    target.files.append(&mut source.files);
    target.numFiles = target.files.len() as i32;
    Ok(startBlock)
}

// ---------------------------------------------------------------------------
// BufFileTruncateFileSet (buffile.c:927-1016)
// ---------------------------------------------------------------------------

/// `BufFileTruncateFileSet(file, fileno, offset)` (buffile.c:927-1016) — truncate
/// a `BufFile` created by [`BufFileCreateFileSet`] up to `(fileno, offset)`.
pub fn BufFileTruncateFileSet(file: &mut BufFile, fileno: i32, offset: i64) -> PgResult<()> {
    let mut numFiles = file.files.len() as i32;
    let mut newFile = fileno;
    let mut newOffset = file.curOffset;

    // C uses `file->fileset` / `file->name` in the loop below; both must be
    // present for a fileset-based BufFile.
    let fileset = file
        .fileset
        .expect("BufFileTruncateFileSet on a non-fileset BufFile");
    let name = file
        .name
        .clone()
        .expect("fileset-backed BufFile must carry a name");

    // Loop from the last segment down to `fileno`, removing segments past
    // `fileno` and truncating `fileno` to `offset`.
    let mut i = file.files.len() as i32 - 1;
    while i >= fileno {
        if (i != fileno || offset == 0) && i != 0 {
            let segment_name = FileSetSegmentName(&name, i);
            fd::file_close::call(file.files[i as usize]);
            // The deleted segment is always the current tail (we iterate from
            // the end downward and only pop in this branch).
            file.files.pop();
            if !fileset::file_set_delete::call(fileset, &segment_name, true)? {
                return Err(file_access_error(
                    format!("could not delete fileset \"{segment_name}\""),
                    951,
                    "BufFileTruncateFileSet",
                ));
            }
            numFiles -= 1;
            newOffset = MAX_PHYSICAL_FILESIZE;

            // Record that we deleted the `fileno` segment.
            if i == fileno {
                newFile -= 1;
            }
        } else {
            // Truncate the surviving segment to `offset`.
            if fd::file_truncate::call(file.files[i as usize], offset, WAIT_EVENT_BUFFILE_TRUNCATE)?
                < 0
            {
                return Err(file_access_error(
                    format!(
                        "could not truncate file \"{}\"",
                        fd::file_path_name::call(file.files[i as usize])
                    ),
                    969,
                    "BufFileTruncateFileSet",
                ));
            }
            newOffset = offset;
        }
        i -= 1;
    }

    // file->numFiles = numFiles. The Vec length already reflects this (each
    // delete pops the tail); assert they agree, then sync the explicit field.
    debug_assert_eq!(file.files.len() as i32, numFiles);
    file.numFiles = numFiles;

    if newFile == file.curFile
        && newOffset >= file.curOffset
        && newOffset <= file.curOffset + file.nbytes as i64
    {
        // Truncate point is within the current buffer.
        if newOffset <= file.curOffset + file.pos as i64 {
            file.pos = (newOffset - file.curOffset) as i32;
        }
        file.nbytes = (newOffset - file.curOffset) as i32;
    } else if newFile == file.curFile && newOffset < file.curOffset {
        // Within the current file but before curOffset.
        file.curOffset = newOffset;
        file.pos = 0;
        file.nbytes = 0;
    } else if newFile < file.curFile {
        // Truncate point is before the current file.
        file.curFile = newFile;
        file.curOffset = newOffset;
        file.pos = 0;
        file.nbytes = 0;
    }
    // Otherwise the truncate point is beyond the current file: nothing to do.
    Ok(())
}

// ---------------------------------------------------------------------------
// Error builders.
// ---------------------------------------------------------------------------

/// `ereport(ERROR, (errcode_for_file_access(), errmsg(msg)))` (the fd-result
/// branches use the current `errno`).
fn file_access_error(msg: String, lineno: i32, funcname: &'static str) -> PgError {
    ereport(ERROR)
        .errcode_for_file_access()
        .errmsg(msg)
        .into_error()
        .with_error_location(ErrorLocation::new(BUFFILE_C, lineno, funcname))
}

/// `elog(ERROR, msg)` — an error with no specific sqlstate (defaults to
/// `ERRCODE_INTERNAL_ERROR`, like C `elog`).
fn elog_error(msg: String, lineno: i32, funcname: &'static str) -> PgError {
    ereport(ERROR)
        .errmsg(msg)
        .into_error()
        .with_error_location(ErrorLocation::new(BUFFILE_C, lineno, funcname))
}

/// "could not determine size of temporary file ... from BufFile ..." shared by
/// [`BufFileSeek`] (`SEEK_END`) and [`BufFileSize`] (buffile.c:772-776 / 873-877).
fn size_error(file: &BufFile, lineno: i32, funcname: &'static str) -> PgError {
    let last = file.files[file.files.len() - 1];
    let path = fd::file_path_name::call(last);
    let name = file.name.clone().unwrap_or_default();
    ereport(ERROR)
        .with_saved_errno(errno::EIO)
        .errcode_for_file_access()
        .errmsg(format!(
            "could not determine size of temporary file \"{path}\" from BufFile \"{name}\""
        ))
        .into_error()
        .with_error_location(ErrorLocation::new(BUFFILE_C, lineno, funcname))
}

// ---------------------------------------------------------------------------
// Instrumentation timer helpers (buffile.c's `track_io_timing` / `instr_time`).
// ---------------------------------------------------------------------------

/// `if (track_io_timing) INSTR_TIME_SET_CURRENT(io_start)` else
/// `INSTR_TIME_SET_ZERO(io_start)` — the timer start, or `None` when timing off.
fn io_timer_start() -> Option<instr_time> {
    if track_io_timing.read() {
        let mut t = instr_time::default();
        instr_time_set_current_lazy(&mut t);
        Some(t)
    } else {
        None
    }
}

/// `INSTR_TIME_SET_CURRENT(io_time); INSTR_TIME_ACCUM_DIFF(temp_blk_read_time,
/// io_time, io_start)` — accumulate read time when timing was on.
fn io_timer_accum_read(io_start: Option<instr_time>) {
    if let Some(start) = io_start {
        let mut now = instr_time::default();
        instr_time_set_current_lazy(&mut now);
        with_pgBufferUsage(|u| u.temp_blk_read_time.accum_diff(now, start));
    }
}

/// As [`io_timer_accum_read`], for `temp_blk_write_time`.
fn io_timer_accum_write(io_start: Option<instr_time>) {
    if let Some(start) = io_start {
        let mut now = instr_time::default();
        instr_time_set_current_lazy(&mut now);
        with_pgBufferUsage(|u| u.temp_blk_write_time.accum_diff(now, start));
    }
}

#[cfg(test)]
mod tests;
