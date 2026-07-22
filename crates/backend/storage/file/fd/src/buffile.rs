// Serial temp-file BufFile; the FileSet (shared) arms are loud. Arena data,
// no Drop: the VFDs are FD_CLOSE_AT_EOXACT+FD_DELETE_AT_CLOSE, so abort
// cleanup is AtEOXact_Files; close() is the normal-path release.
use core::cell::Cell;

use ::elog::ereport;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::{PgResult, ERROR};
use ::types_storage::File;

use crate::io::{file_path_name_lossy, FileClose, FileRead, FileSize, FileTruncate, FileWrite};
use crate::temp::OpenTemporaryFile;
use crate::vfd::{get_errno, loc};

const BLCKSZ: usize = 8192;
const MAX_PHYSICAL_FILESIZE: i64 = 0x4000_0000;

pub const SEEK_SET: i32 = 0;
pub const SEEK_CUR: i32 = 1;
pub const SEEK_END: i32 = 2;

thread_local! {
    static TEMP_BLKS_READ: Cell<i64> = const { Cell::new(0) };
    static TEMP_BLKS_WRITTEN: Cell<i64> = const { Cell::new(0) };
}

pub fn temp_blks_read() -> i64 {
    TEMP_BLKS_READ.with(Cell::get)
}

pub fn temp_blks_written() -> i64 {
    TEMP_BLKS_WRITTEN.with(Cell::get)
}

pub struct BufFile<'mcx> {
    // All files except the last have length exactly MAX_PHYSICAL_FILESIZE.
    files: PgVec<'mcx, File>,
    is_inter_xact: bool,
    dirty: bool,
    read_only: bool,
    // FileSet-backed files: segment i lives at "<seg_base>.<i>", creatable
    // and openable by any participant thread (C's fileset BufFiles).
    seg_base: Option<PgVec<'mcx, u8>>,
    cur_file: i32,
    cur_offset: i64,
    pos: i32,
    nbytes: i32,
    buffer: PgVec<'mcx, u8>,
}

// C PrepareTempTablespaces (commands/tablespace.c): the empty-GUC fast path
// stays local; a configured list needs catalog access, so it delegates to
// commands_tablespace.
pub fn PrepareTempTablespaces() -> PgResult<()> {
    if crate::temp::TempTablespacesAreSet() {
        return Ok(());
    }
    let spaces = guc_tables::vars::temp_tablespaces.read();
    if spaces.as_deref().unwrap_or("").is_empty() {
        crate::temp::SetTempTablespaces(&[]);
        return Ok(());
    }
    tablespace_seams::prepare_temp_tablespaces::call()
}

pub fn BufFileCreateTemp<'mcx>(mcx: Mcx<'mcx>, inter_xact: bool) -> PgResult<BufFile<'mcx>> {
    PrepareTempTablespaces()?;
    let pfile = OpenTemporaryFile(inter_xact)?;
    debug_assert!(pfile.0 >= 0);
    let mut files = vec_with_capacity_in(mcx, 1)?;
    files.push(pfile);
    let mut buffer = vec_with_capacity_in(mcx, BLCKSZ)?;
    buffer.resize(BLCKSZ, 0);
    Ok(BufFile {
        files,
        is_inter_xact: inter_xact,
        dirty: false,
        read_only: false,
        seg_base: None,
        cur_file: 0,
        cur_offset: 0,
        pos: 0,
        nbytes: 0,
        buffer,
    })
}

fn seg_name(base: &[u8], segment: usize) -> String {
    format!("{}.{segment}", core::str::from_utf8(base).expect("fileset name is utf8"))
}

/// `BufFileCreateFileSet` (buffile.c): a named, participant-shared temp file.
pub fn BufFileCreateFileSet<'mcx>(
    mcx: Mcx<'mcx>,
    fileset: &crate::fileset::FileSet,
    name: &str,
) -> PgResult<BufFile<'mcx>> {
    let base = fileset.name_path(name);
    let file = fileset.create_seg(name, &seg_name(base.as_bytes(), 0))?;
    debug_assert!(file.0 > 0);
    let mut files = vec_with_capacity_in(mcx, 1)?;
    files.push(file);
    let mut seg = vec_with_capacity_in(mcx, base.len())?;
    seg.extend(base.as_bytes().iter().copied());
    let mut buffer = vec_with_capacity_in(mcx, BLCKSZ)?;
    buffer.resize(BLCKSZ, 0);
    Ok(BufFile {
        files,
        is_inter_xact: false,
        dirty: false,
        read_only: false,
        seg_base: Some(seg),
        cur_file: 0,
        cur_offset: 0,
        pos: 0,
        nbytes: 0,
        buffer,
    })
}

/// `BufFileOpenFileSet`: open another participant's file by name.
pub fn BufFileOpenFileSet<'mcx>(
    mcx: Mcx<'mcx>,
    fileset: &crate::fileset::FileSet,
    name: &str,
    read_only: bool,
) -> PgResult<BufFile<'mcx>> {
    let base = fileset.name_path(name);
    let mode = if read_only { libc::O_RDONLY } else { libc::O_RDWR };
    let mut files: PgVec<'mcx, File> = vec_with_capacity_in(mcx, 1)?;
    loop {
        let f = fileset.open_seg(&seg_name(base.as_bytes(), files.len()), mode)?;
        if f.0 <= 0 {
            break;
        }
        files.push(f);
    }
    if files.is_empty() {
        ereport(ERROR)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open temporary file \"{base}\": %m"))
            .finish(loc("BufFileOpenFileSet"))?;
    }
    let mut seg = vec_with_capacity_in(mcx, base.len())?;
    seg.extend(base.as_bytes().iter().copied());
    let mut buffer = vec_with_capacity_in(mcx, BLCKSZ)?;
    buffer.resize(BLCKSZ, 0);
    Ok(BufFile {
        files,
        is_inter_xact: false,
        dirty: false,
        read_only,
        seg_base: Some(seg),
        cur_file: 0,
        cur_offset: 0,
        pos: 0,
        nbytes: 0,
        buffer,
    })
}

/// `BufFileOpenFileSet` with C's missing_ok=true arm: None when no segment
/// exists.
pub fn BufFileOpenFileSetMaybe<'mcx>(
    mcx: Mcx<'mcx>,
    fileset: &crate::fileset::FileSet,
    name: &str,
    read_only: bool,
) -> PgResult<Option<BufFile<'mcx>>> {
    let base = fileset.name_path(name);
    let mode = if read_only { libc::O_RDONLY } else { libc::O_RDWR };
    let probe = fileset.open_seg(&seg_name(base.as_bytes(), 0), mode)?;
    if probe.0 <= 0 {
        return Ok(None);
    }
    // Hand the probed segment back and reopen the whole chain uniformly.
    FileClose(probe)?;
    Ok(Some(BufFileOpenFileSet(mcx, fileset, name, read_only)?))
}

/// `BufFileDeleteFileSet` (buffile.c:379): unlink every segment of a named
/// fileset file.
pub fn BufFileDeleteFileSet(
    fileset: &crate::fileset::FileSet,
    name: &str,
    missing_ok: bool,
) -> PgResult<()> {
    let base = fileset.name_path(name);
    let mut found = false;
    let mut segment = 0usize;
    loop {
        let seg = seg_name(base.as_bytes(), segment);
        if !crate::temp::PathNameDeleteTemporaryFile(&seg, false)? {
            break;
        }
        found = true;
        segment += 1;
    }
    if !found && !missing_ok {
        ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not delete unknown temporary file \"{base}\""))
            .finish(loc("BufFileDeleteFileSet"))?;
    }
    Ok(())
}

impl<'mcx> BufFile<'mcx> {
    /// `BufFileTruncateFileSet` (buffile.c:911): truncate at (fileno,
    /// offset), removing whole segments past the point.
    pub fn truncate_fileset(&mut self, fileno: i32, offset: i64) -> PgResult<()> {
        let base = self
            .seg_base
            .as_ref()
            .map(|b| b.to_vec())
            .expect("truncate_fileset on a fileset BufFile");
        let mut num_files = self.files.len() as i32;
        let mut new_file = fileno;
        let mut new_offset = self.cur_offset;

        // Remove segments past the target; truncate the target in place.
        // A segment truncated to offset 0 is removed too, unless it is the
        // first one.
        let mut i = self.files.len() as i32 - 1;
        while i >= fileno {
            if (i != fileno || offset == 0) && i != 0 {
                let seg = seg_name(&base, i as usize);
                FileClose(self.files[i as usize])?;
                if !crate::temp::PathNameDeleteTemporaryFile(&seg, false)? {
                    ereport(ERROR)
                        .errcode_for_file_access()
                        .errmsg(format!("could not delete fileset \"{seg}\""))
                        .finish(loc("BufFileTruncateFileSet"))?;
                }
                num_files -= 1;
                new_offset = MAX_PHYSICAL_FILESIZE;
                if i == fileno {
                    new_file -= 1;
                }
            } else {
                if FileTruncate(self.files[i as usize], offset, 0)? < 0 {
                    ereport(ERROR)
                        .with_saved_errno(get_errno())
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not truncate file \"{}\": %m",
                            file_path_name_lossy(self.files[i as usize])
                        ))
                        .finish(loc("BufFileTruncateFileSet"))?;
                }
                num_files = i + 1;
                new_offset = offset;
            }
            i -= 1;
        }
        self.files.truncate(num_files as usize);

        // Adjust the buffered position to the new end where it overlaps.
        if new_file == self.cur_file
            && new_offset >= self.cur_offset
            && new_offset <= self.cur_offset + self.nbytes as i64
        {
            if new_offset <= self.cur_offset + self.pos as i64 {
                self.pos = (new_offset - self.cur_offset) as i32;
            }
            self.nbytes = (new_offset - self.cur_offset) as i32;
        } else if new_file == self.cur_file && new_offset < self.cur_offset {
            self.cur_offset = new_offset;
            self.pos = 0;
            self.nbytes = 0;
        } else if new_file < self.cur_file {
            self.cur_file = new_file;
            self.cur_offset = new_offset;
            self.pos = 0;
            self.nbytes = 0;
        }
        Ok(())
    }

    fn extend(&mut self) -> PgResult<()> {
        let pfile = match &self.seg_base {
            None => OpenTemporaryFile(self.is_inter_xact)?,
            Some(base) => {
                // MakeNewFileSetSegment; requires the fileset caller keep the
                // set alive, which the sts accessor structure guarantees.
                crate::temp::PathNameCreateTemporaryFile(
                    &seg_name(base, self.files.len()),
                    true,
                )?
            }
        };
        debug_assert!(pfile.0 >= 0);
        self.files.push(pfile);
        Ok(())
    }

    pub fn close(mut self) -> PgResult<()> {
        // proc_exit already ran the abort resowner release: every temp-file
        // VFD here is freed, and flushing the dirty buffer would write
        // through dead Files. Late drop glue (Tuplesort/Tuplestore on the
        // ProcExitThread unwind or TLS teardown) must be a no-op — C's
        // process exit never revisits sort state (ts-extract grouped-agg run: worker
        // FATAL mid-sort-spill aborted the postmaster via panic-in-drop).
        if ::elog::config::proc_exit_inprogress() {
            return Ok(());
        }
        self.flush()?;
        for i in 0..self.files.len() {
            FileClose(self.files[i])?;
        }
        Ok(())
    }

    fn load_buffer(&mut self) -> PgResult<()> {
        if self.cur_offset >= MAX_PHYSICAL_FILESIZE
            && (self.cur_file + 1) < self.files.len() as i32
        {
            self.cur_file += 1;
            self.cur_offset = 0;
        }
        let thisfile = self.files[self.cur_file as usize];
        let nread = FileRead(thisfile, &mut self.buffer[..], self.cur_offset, 0)?;
        if nread < 0 {
            self.nbytes = 0;
            return read_failed(thisfile);
        }
        self.nbytes = nread as i32;
        if self.nbytes > 0 {
            TEMP_BLKS_READ.with(|c| c.set(c.get() + 1));
        }
        Ok(())
    }

    fn dump_buffer(&mut self) -> PgResult<()> {
        let mut wpos: i32 = 0;
        while wpos < self.nbytes {
            if self.cur_offset >= MAX_PHYSICAL_FILESIZE {
                while (self.cur_file + 1) >= self.files.len() as i32 {
                    self.extend()?;
                }
                self.cur_file += 1;
                self.cur_offset = 0;
            }
            let mut bytestowrite = (self.nbytes - wpos) as i64;
            let availbytes = MAX_PHYSICAL_FILESIZE - self.cur_offset;
            if bytestowrite > availbytes {
                bytestowrite = availbytes;
            }
            let thisfile = self.files[self.cur_file as usize];
            let written = FileWrite(
                thisfile,
                &self.buffer[wpos as usize..(wpos as i64 + bytestowrite) as usize],
                self.cur_offset,
                0,
            )?;
            if written <= 0 {
                return write_failed(thisfile);
            }
            self.cur_offset += written as i64;
            wpos += written as i32;
            TEMP_BLKS_WRITTEN.with(|c| c.set(c.get() + 1));
        }
        self.dirty = false;

        // Make curOffset point to the logical position (original + pos); a
        // small backwards seek in a dirty buffer can leave pos < nbytes.
        self.cur_offset -= (self.nbytes - self.pos) as i64;
        if self.cur_offset < 0 {
            self.cur_file -= 1;
            debug_assert!(self.cur_file >= 0);
            self.cur_offset += MAX_PHYSICAL_FILESIZE;
        }
        self.pos = 0;
        self.nbytes = 0;
        Ok(())
    }

    fn flush(&mut self) -> PgResult<()> {
        if self.dirty {
            self.dump_buffer()?;
        }
        debug_assert!(!self.dirty);
        Ok(())
    }

    fn read_common(&mut self, ptr: &mut [u8], exact: bool, eof_ok: bool) -> PgResult<usize> {
        let start_size = ptr.len();
        let mut size = ptr.len();
        let mut nread = 0usize;

        self.flush()?;

        while size > 0 {
            if self.pos >= self.nbytes {
                self.cur_offset += self.pos as i64;
                self.pos = 0;
                self.nbytes = 0;
                self.load_buffer()?;
                if self.nbytes <= 0 {
                    break;
                }
            }
            let mut nthistime = (self.nbytes - self.pos) as usize;
            if nthistime > size {
                nthistime = size;
            }
            debug_assert!(nthistime > 0);
            ptr[nread..nread + nthistime]
                .copy_from_slice(&self.buffer[self.pos as usize..self.pos as usize + nthistime]);
            self.pos += nthistime as i32;
            size -= nthistime;
            nread += nthistime;
        }

        if exact && nread != start_size && !(nread == 0 && eof_ok) {
            ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not read from temporary file: read only {nread} of {start_size} bytes"
                ))
                .finish(loc("BufFileReadCommon"))?;
        }
        Ok(nread)
    }

    pub fn read(&mut self, ptr: &mut [u8]) -> PgResult<usize> {
        self.read_common(ptr, false, false)
    }

    pub fn read_exact(&mut self, ptr: &mut [u8]) -> PgResult<()> {
        self.read_common(ptr, true, false)?;
        Ok(())
    }

    pub fn read_maybe_eof(&mut self, ptr: &mut [u8], eof_ok: bool) -> PgResult<usize> {
        self.read_common(ptr, true, eof_ok)
    }

    pub fn write(&mut self, mut ptr: &[u8]) -> PgResult<()> {
        debug_assert!(!self.read_only);
        while !ptr.is_empty() {
            if self.pos >= BLCKSZ as i32 {
                if self.dirty {
                    self.dump_buffer()?;
                } else {
                    // Went directly from reading to writing.
                    self.cur_offset += self.pos as i64;
                    self.pos = 0;
                    self.nbytes = 0;
                }
            }
            let mut nthistime = BLCKSZ - self.pos as usize;
            if nthistime > ptr.len() {
                nthistime = ptr.len();
            }
            debug_assert!(nthistime > 0);
            self.buffer[self.pos as usize..self.pos as usize + nthistime]
                .copy_from_slice(&ptr[..nthistime]);
            self.dirty = true;
            self.pos += nthistime as i32;
            if self.nbytes < self.pos {
                self.nbytes = self.pos;
            }
            ptr = &ptr[nthistime..];
        }
        Ok(())
    }

    /// 0 on success, EOF (-1) if an impossible seek was attempted.
    pub fn seek(&mut self, fileno: i32, offset: i64, whence: i32) -> PgResult<i32> {
        let mut new_file: i32;
        let mut new_offset: i64;
        match whence {
            SEEK_SET => {
                if fileno < 0 {
                    return Ok(-1);
                }
                new_file = fileno;
                new_offset = offset;
            }
            SEEK_CUR => {
                new_file = self.cur_file;
                new_offset = (self.cur_offset + self.pos as i64) + offset;
            }
            SEEK_END => {
                new_file = self.files.len() as i32 - 1;
                new_offset = FileSize(self.files[self.files.len() - 1])?;
                if new_offset < 0 {
                    ereport(ERROR)
                        .with_saved_errno(get_errno())
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not determine size of temporary file \"{}\" from BufFile \"\": %m",
                            file_path_name_lossy(self.files[self.files.len() - 1])
                        ))
                        .finish(loc("BufFileSeek"))?;
                }
            }
            other => panic!("invalid whence: {other}"),
        }
        while new_offset < 0 {
            new_file -= 1;
            if new_file < 0 {
                return Ok(-1);
            }
            new_offset += MAX_PHYSICAL_FILESIZE;
        }
        if new_file == self.cur_file
            && new_offset >= self.cur_offset
            && new_offset <= self.cur_offset + self.nbytes as i64
        {
            self.pos = (new_offset - self.cur_offset) as i32;
            return Ok(0);
        }
        self.flush()?;
        // The flush may have created a new segment, so only now translate a
        // start-of-next-seg position and range-check.
        if new_file == self.files.len() as i32 && new_offset == 0 {
            new_file -= 1;
            new_offset = MAX_PHYSICAL_FILESIZE;
        }
        while new_offset > MAX_PHYSICAL_FILESIZE {
            new_file += 1;
            if new_file >= self.files.len() as i32 {
                return Ok(-1);
            }
            new_offset -= MAX_PHYSICAL_FILESIZE;
        }
        if new_file >= self.files.len() as i32 {
            return Ok(-1);
        }
        self.cur_file = new_file;
        self.cur_offset = new_offset;
        self.pos = 0;
        self.nbytes = 0;
        Ok(0)
    }

    pub fn tell(&self) -> (i32, i64) {
        (self.cur_file, self.cur_offset + self.pos as i64)
    }

    /// C `BufFileSeekBlock`.
    pub fn seek_block(&mut self, blknum: i64) -> PgResult<i32> {
        const BUFFILE_SEG_BLOCKS: i64 = MAX_PHYSICAL_FILESIZE / BLCKSZ as i64;
        self.seek(
            (blknum / BUFFILE_SEG_BLOCKS) as i32,
            (blknum % BUFFILE_SEG_BLOCKS) * BLCKSZ as i64,
            SEEK_SET,
        )
    }

    /// C `BufFileSize`; like C, does not count a dirty write buffer.
    pub fn size(&self) -> PgResult<i64> {
        let last = self.files[self.files.len() - 1];
        let last_size = FileSize(last)?;
        if last_size < 0 {
            ereport(ERROR)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not determine size of temporary file \"{}\" from BufFile \"\": %m",
                    file_path_name_lossy(last)
                ))
                .finish(loc("BufFileSize"))?;
        }
        Ok((self.files.len() as i64 - 1) * MAX_PHYSICAL_FILESIZE + last_size)
    }
}

#[cold]
#[inline(never)]
fn read_failed(file: File) -> PgResult<()> {
    ereport(ERROR)
        .with_saved_errno(get_errno())
        .errcode_for_file_access()
        .errmsg(format!("could not read file \"{}\": %m", file_path_name_lossy(file)))
        .finish(loc("BufFileLoadBuffer"))
        .map(|_| ())
}

#[cold]
#[inline(never)]
fn write_failed(file: File) -> PgResult<()> {
    ereport(ERROR)
        .with_saved_errno(get_errno())
        .errcode_for_file_access()
        .errmsg(format!("could not write to file \"{}\": %m", file_path_name_lossy(file)))
        .finish(loc("BufFileDumpBuffer"))
        .map(|_| ())
}
