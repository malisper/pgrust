use elog::{ereport, errno};
use types_core::{
    FullTransactionId, TransactionId, TransactionIdIsNormal, TransactionIdIsValid,
};
use types_error::{PgResult, ERRCODE_DATA_CORRUPTED, ERROR, WARNING};

use crate::codec::{
    maxalign, TwoPhaseFileHeader, MAX_ALLOC_SIZE, SIZEOF_TWOPHASE_FILE_HEADER,
    SIZEOF_TWOPHASE_RECORD_ON_DISK, TWOPHASE_MAGIC,
};
use crate::here;

pub const TWOPHASE_DIR: &str = "pg_twophase";

/// `AdjustToFullTransactionId`: recover the epoch for a bare xid known to
/// precede-or-equal nextXid.
fn adjust_to_full_transaction_id(xid: TransactionId) -> FullTransactionId {
    debug_assert!(TransactionIdIsValid(xid));
    if !TransactionIdIsNormal(xid) {
        return FullTransactionId::from_epoch_and_xid(0, xid);
    }
    let next_full = varsup::ReadNextFullTransactionId().expect("ReadNextFullTransactionId");
    let mut epoch = next_full.epoch();
    if xid > next_full.xid() {
        debug_assert!(epoch != 0);
        epoch -= 1;
    }
    FullTransactionId::from_epoch_and_xid(epoch, xid)
}

pub(crate) fn two_phase_file_path(xid: TransactionId) -> String {
    let fxid = adjust_to_full_transaction_id(xid);
    format!("{}/{:08X}{:08X}", TWOPHASE_DIR, fxid.epoch(), fxid.xid())
}

fn get_errno() -> i32 {
    errno::current_errno()
}

/// `ReadTwoPhaseFile(xid, missing_ok)` — read + validate (size bounds, magic,
/// total_len, CRC).
pub(crate) fn read_twophase_file(
    xid: TransactionId,
    missing_ok: bool,
) -> PgResult<Option<Vec<u8>>> {
    let path = two_phase_file_path(xid);

    let fd = fd::desc::OpenTransientFile(&path, libc::O_RDONLY)?;
    if fd < 0 {
        let en = get_errno();
        if missing_ok && en == errno::ENOENT {
            return Ok(None);
        }
        ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\": %m"))
            .finish(here("ReadTwoPhaseFile"))?;
    }

    let result = read_twophase_body(fd, &path);
    let close_rc = fd::desc::CloseTransientFile(fd);
    let buf = result?;
    if close_rc != 0 {
        ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{path}\": %m"))
            .finish(here("ReadTwoPhaseFile"))?;
    }

    let st_size = buf.len();
    let hdr = TwoPhaseFileHeader::from_bytes(&buf).expect("size lower bound checked");
    if hdr.magic != TWOPHASE_MAGIC {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!("invalid magic number stored in file \"{path}\""))
            .finish(here("ReadTwoPhaseFile"))?;
    }
    if hdr.total_len as usize != st_size {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!("invalid size stored in file \"{path}\""))
            .finish(here("ReadTwoPhaseFile"))?;
    }

    let crc_offset = st_size - 4;
    let calc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &buf[..crc_offset]));
    let file_crc = u32::from_ne_bytes(buf[crc_offset..crc_offset + 4].try_into().unwrap());
    if calc != file_crc {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "calculated CRC checksum does not match value stored in file \"{path}\""
            ))
            .finish(here("ReadTwoPhaseFile"))?;
    }

    Ok(Some(buf))
}

fn read_twophase_body(fd: i32, path: &str) -> PgResult<Vec<u8>> {
    // fstat on a live fd owned by the transient-file table.
    let mut stat = fd::FileInfo::zeroed();
    if fd::pg_fstat(fd, &mut stat) != 0 {
        ereport(ERROR)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not stat file \"{path}\": %m"))
            .finish(here("ReadTwoPhaseFile"))?;
    }
    let st_size = stat.size;

    let lower = (maxalign(SIZEOF_TWOPHASE_FILE_HEADER)
        + maxalign(SIZEOF_TWOPHASE_RECORD_ON_DISK)
        + 4) as i64;
    if st_size < lower || st_size > MAX_ALLOC_SIZE as i64 {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!("incorrect size of file \"{path}\": {st_size} bytes"))
            .finish(here("ReadTwoPhaseFile"))?;
    }
    let crc_offset = (st_size - 4) as usize;
    if crc_offset != maxalign(crc_offset) {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "incorrect alignment of CRC offset for file \"{path}\""
            ))
            .finish(here("ReadTwoPhaseFile"))?;
    }

    let mut buf = vec![0u8; st_size as usize];
    // Freshly opened transient fd: whole-file positional read at offset 0.
    let r = fd::pg_pread(fd, &mut buf, 0);
    if r != st_size as isize {
        if r < 0 {
            ereport(ERROR)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{path}\": %m"))
                .finish(here("ReadTwoPhaseFile"))?;
        } else {
            ereport(ERROR)
                .errmsg(format!(
                    "could not read file \"{path}\": read {r} of {st_size}"
                ))
                .finish(here("ReadTwoPhaseFile"))?;
        }
    }
    Ok(buf)
}

/// `RecreateTwoPhaseFile(xid, content, len)` — write content + CRC, fsync.
pub(crate) fn recreate_two_phase_file(xid: TransactionId, content: &[u8]) -> PgResult<()> {
    let crc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, content));
    let path = two_phase_file_path(xid);

    let fd = fd::desc::OpenTransientFile(&path, libc::O_CREAT | libc::O_TRUNC | libc::O_WRONLY)?;
    if fd < 0 {
        ereport(ERROR)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not recreate file \"{path}\": %m"))
            .finish(here("RecreateTwoPhaseFile"))?;
    }

    let result = (|| -> PgResult<()> {
        let mut write_off: i64 = 0;
        for chunk in [content, &crc.to_ne_bytes()[..]] {
            // Positional write at the tracked append offset (O_TRUNC fd).
            let w = fd::pg_pwrite(fd, chunk, write_off);
            if w == chunk.len() as isize {
                write_off += w as i64;
            }
            if w != chunk.len() as isize {
                let mut en = get_errno();
                if en == 0 {
                    en = libc::ENOSPC;
                }
                ereport(ERROR)
                    .with_saved_errno(en)
                    .errcode_for_file_access()
                    .errmsg(format!("could not write file \"{path}\": %m"))
                    .finish(here("RecreateTwoPhaseFile"))?;
            }
        }
        if fd::sync::pg_fsync(fd) != 0 {
            ereport(ERROR)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not fsync file \"{path}\": %m"))
                .finish(here("RecreateTwoPhaseFile"))?;
        }
        Ok(())
    })();
    let close_rc = fd::desc::CloseTransientFile(fd);
    result?;
    if close_rc != 0 {
        ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{path}\": %m"))
            .finish(here("RecreateTwoPhaseFile"))?;
    }
    Ok(())
}

/// `RemoveTwoPhaseFile(xid, giveWarning)`.
pub(crate) fn remove_two_phase_file(xid: TransactionId, give_warning: bool) -> PgResult<()> {
    let path = two_phase_file_path(xid);
    if fd::pg_unlink(&path) != 0 {
        let en = get_errno();
        if en != errno::ENOENT || give_warning {
            ereport(WARNING)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg(format!("could not remove file \"{path}\": %m"))
                .finish(here("RemoveTwoPhaseFile"))?;
        }
    }
    Ok(())
}

/// `restoreTwoPhaseData`'s directory scan: the 16-hex-char basenames as full
/// xids.
pub(crate) fn scan_twophase_dir() -> PgResult<Vec<u64>> {
    let mut out: Vec<u64> = Vec::new();
    fd::desc::with_allocated_dir(TWOPHASE_DIR, &mut |name: &str| {
        if name.len() == 16
            && name
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b))
        {
            if let Ok(v) = u64::from_str_radix(name, 16) {
                out.push(v);
            }
        }
        Ok(false)
    })?;
    Ok(out)
}

pub(crate) fn twophase_file_exists(xid: TransactionId) -> PgResult<bool> {
    let path = two_phase_file_path(xid);
    // Existence probe (C: access(F_OK)); stat-succeeds is the fd-mediated
    // equivalent for F_OK.
    let mut fi = fd::FileInfo::zeroed();
    if fd::pg_stat(&path, &mut fi) == 0 {
        return Ok(true);
    }
    let en = get_errno();
    if en != errno::ENOENT {
        ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not access file \"{path}\": %m"))
            .finish(here("PrepareRedoAdd"))?;
    }
    Ok(false)
}

pub(crate) fn fsync_twophase_dir() -> PgResult<()> {
    fd::sync::fsync_fname(TWOPHASE_DIR, true)
}
