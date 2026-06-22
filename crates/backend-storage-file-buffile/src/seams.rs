//! Install this crate's seams (`backend-storage-file-buffile-seams`) to the
//! real `buffile.c` functions. Marshal-and-delegate only.

use backend_storage_file_buffile_seams as seams;

/// Install every `backend-storage-file-buffile` seam.
pub fn init_seams() {
    seams::buf_file_create_temp::set(|mcx, inter_xact| super::BufFileCreateTemp(mcx, inter_xact));
    seams::buf_file_create_fileset::set(|mcx, fileset, name| {
        super::BufFileCreateFileSet(mcx, fileset, name)
    });
    seams::buf_file_open_fileset::set(|mcx, fileset, name, mode, missing_ok| {
        super::BufFileOpenFileSet(mcx, fileset, name, mode, missing_ok)
    });
    seams::buf_file_close_ref::set(|file| super::BufFileClose(file));
    seams::buf_file_close::set(|mut file| super::BufFileClose(&mut file));
    seams::buf_file_seek::set(|file, fileno, offset, whence| {
        super::BufFileSeek(file, fileno, offset, whence)
    });
    seams::buf_file_write::set(|file, data| super::BufFileWrite(file, data));
    seams::buf_file_read_maybe_eof::set(|file, buf, eof_ok| {
        super::BufFileReadMaybeEOF(file, buf, eof_ok)
    });
    seams::buf_file_read_exact::set(|file, buf| super::BufFileReadExact(file, buf));
    seams::buf_file_seek_block::set(|file, blknum| super::BufFileSeekBlock(file, blknum));
    seams::buf_file_tell::set(|file| super::BufFileTell(file));
    seams::buf_file_size::set(|file| super::BufFileSize(file));
}
