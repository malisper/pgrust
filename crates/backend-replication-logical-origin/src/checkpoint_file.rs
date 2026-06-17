//! The `replorigin_checkpoint` transient-file I/O codec (origin.c) — the file
//! halves of `CheckPointReplicationOrigin` (write) and
//! `StartupReplicationOrigin` (read), declared as the
//! `backend-replication-logical-origin-extern-seams` `checkpoint_write` /
//! `checkpoint_read` seams.
//!
//! The in-memory halves (slot locking, `XLogFlush`, the
//! `max_active_replication_origins` overflow check, copying into the shared
//! array) live in `lib.rs`; here we only serialize/deserialize the on-disk
//! format and verify the CRC32C, using the ported transient-file fd I/O and
//! the CRC32C port primitive.
//!
//! On-disk layout (matching the C `struct` byte image on this little-endian
//! platform):
//! * `uint32 magic` (4 bytes) — `REPLICATION_STATE_MAGIC`.
//! * zero or more `ReplicationStateOnDisk` records, each
//!   `sizeof(ReplicationStateOnDisk) == 16` bytes: `RepOriginId roident`
//!   (u16) at offset 0, 6 bytes of struct padding, `XLogRecPtr remote_lsn`
//!   (u64) at offset 8.
//! * `pg_crc32c crc` (4 bytes) — the finalized CRC32C over magic + every
//!   record.

use alloc::format;
use alloc::vec;
use alloc::vec::Vec;

use types_core::{RepOriginId, XLogRecPtr};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, PANIC};

use backend_storage_file_fd_seams as fd;
use port_pg_crc32c_seams as crc32c;

use crate::core::{
    REPLICATION_STATE_MAGIC, PG_REPLORIGIN_CHECKPOINT_FILENAME, PG_REPLORIGIN_CHECKPOINT_TMPFILE,
};

const ENOENT: i32 = 2;
const ENOSPC: i32 = 28;

/// `sizeof(ReplicationStateOnDisk)` — `{ RepOriginId roident; XLogRecPtr
/// remote_lsn; }` with C struct alignment (u16 + 6 padding + u64).
const DISK_STATE_SIZE: usize = 16;
/// `sizeof(uint32 magic)` / `sizeof(pg_crc32c)`.
const U32_SIZE: usize = 4;

mod libc_flags {
    pub const O_WRONLY: i32 = 1;
    pub const O_CREAT: i32 = 0o100;
    pub const O_EXCL: i32 = 0o200;
    pub const O_RDONLY: i32 = 0;
    /// `PG_BINARY` is 0 on POSIX.
    pub const PG_BINARY: i32 = 0;
}

/// `INIT_CRC32C(crc)` — `crc = 0xFFFFFFFF`.
const CRC_INIT: u32 = 0xFFFF_FFFF;
/// `FIN_CRC32C(crc)` — `crc ^= 0xFFFFFFFF`.
fn fin_crc32c(crc: u32) -> u32 {
    crc ^ 0xFFFF_FFFF
}

fn file_access_err(msg: impl Into<alloc::string::String>, errno: i32) -> PgError {
    let _ = errno;
    PgError::new(PANIC, msg.into())
}

/// `checkpoint_write(states)` — the file half of `CheckPointReplicationOrigin`
/// (origin.c lines ~610-710). `states` is the caller's already-snapshotted
/// `(roident, remote_lsn)` list (the `InvalidRepOriginId` skip, the per-slot
/// lock and `XLogFlush` are done caller-side). The `max_active_replication_
/// origins == 0` early-return is also caller-side.
pub fn checkpoint_write(states: Vec<(RepOriginId, XLogRecPtr)>) -> PgResult<()> {
    let tmppath = PG_REPLORIGIN_CHECKPOINT_TMPFILE;
    let path = PG_REPLORIGIN_CHECKPOINT_FILENAME;

    // INIT_CRC32C(crc);
    let mut crc = CRC_INIT;

    // make sure no old temp file is remaining
    let r = fd::unlink_file::call(tmppath);
    if r < 0 && -r != ENOENT {
        return Err(file_access_err(
            format!("could not remove file \"{tmppath}\""),
            -r,
        ));
    }

    // tmpfd = OpenTransientFile(tmppath, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
    let tmpfd = fd::open_transient_file::call(
        tmppath,
        libc_flags::O_CREAT | libc_flags::O_EXCL | libc_flags::O_WRONLY | libc_flags::PG_BINARY,
    );
    if tmpfd < 0 {
        return Err(file_access_err(
            format!("could not create file \"{tmppath}\""),
            -tmpfd,
        ));
    }

    // write magic
    let magic = REPLICATION_STATE_MAGIC.to_le_bytes();
    write_all(tmpfd, &magic, tmppath)?;
    crc = crc32c::pg_comp_crc32c::call(crc, &magic);

    // write actual data
    for (roident, remote_lsn) in states {
        // memset(&disk_state, 0, sizeof(disk_state)) then fill the two fields:
        // roident at offset 0 (u16), remote_lsn at offset 8 (u64), padding 0.
        let mut disk_state = [0u8; DISK_STATE_SIZE];
        disk_state[0..2].copy_from_slice(&roident.to_le_bytes());
        disk_state[8..16].copy_from_slice(&remote_lsn.to_le_bytes());

        write_all(tmpfd, &disk_state, tmppath)?;
        crc = crc32c::pg_comp_crc32c::call(crc, &disk_state);
    }

    // write out the CRC
    crc = fin_crc32c(crc);
    write_all(tmpfd, &crc.to_le_bytes(), tmppath)?;

    // CloseTransientFile(tmpfd)
    if fd::close_transient_file::call(tmpfd) != 0 {
        return Err(file_access_err(
            format!("could not close file \"{tmppath}\""),
            0,
        ));
    }

    // fsync, rename to permanent file, fsync file and directory.
    backend_storage_file_seams::durable_rename::call(tmppath, path, PANIC)
}

/// Write the whole `buf` to a transient `fd`, mirroring C's single `write()`
/// that PANICs if it did not write `sizeof(...)` bytes (the C "if write didn't
/// set errno, assume problem is no disk space" maps to ENOSPC).
fn write_all(fd_no: i32, buf: &[u8], path: &str) -> PgResult<()> {
    let written = fd::transient_write::call(fd_no, buf);
    if written != buf.len() as isize {
        let errno = if written < 0 { -written as i32 } else { ENOSPC };
        let _ = fd::close_transient_file::call(fd_no);
        return Err(file_access_err(
            format!("could not write to file \"{path}\""),
            errno,
        ));
    }
    Ok(())
}

/// `checkpoint_read()` — the file half of `StartupReplicationOrigin` (origin.c
/// lines ~747-846). Returns `Ok(None)` on ENOENT (no checkpoint yet / fresh
/// standby), else the decoded `(roident, remote_lsn)` pairs in file order.
pub fn checkpoint_read() -> PgResult<Option<Vec<(RepOriginId, XLogRecPtr)>>> {
    let path = PG_REPLORIGIN_CHECKPOINT_FILENAME;

    // INIT_CRC32C(crc);
    let mut crc = CRC_INIT;

    // fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    let fd_no =
        fd::open_transient_file::call(path, libc_flags::O_RDONLY | libc_flags::PG_BINARY);
    if fd_no < 0 {
        let errno = -fd_no;
        // might have had max_active_replication_origins == 0 last run, or we
        // just brought up a standby.
        if errno == ENOENT {
            return Ok(None);
        }
        return Err(file_access_err(
            format!("could not open file \"{path}\""),
            errno,
        ));
    }

    // verify magic, that is written even if nothing was active
    let mut magic = [0u8; U32_SIZE];
    let read_bytes = fd::transient_read::call(fd_no, &mut magic);
    if read_bytes != U32_SIZE as isize {
        let _ = fd::close_transient_file::call(fd_no);
        return Err(file_access_err(
            format!("could not read file \"{path}\""),
            if read_bytes < 0 { -read_bytes as i32 } else { 0 },
        ));
    }
    crc = crc32c::pg_comp_crc32c::call(crc, &magic);

    if u32::from_le_bytes(magic) != REPLICATION_STATE_MAGIC {
        let _ = fd::close_transient_file::call(fd_no);
        return Err(PgError::new(
            PANIC,
            format!(
                "replication checkpoint has wrong magic {} instead of {}",
                u32::from_le_bytes(magic),
                REPLICATION_STATE_MAGIC
            ),
        )
        .with_sqlstate(ERRCODE_DATA_CORRUPTED));
    }

    // recover individual states, until there are no more to be found
    let mut states: Vec<(RepOriginId, XLogRecPtr)> = Vec::new();
    let file_crc;
    loop {
        // C reads sizeof(ReplicationStateOnDisk) bytes; when only the 4-byte CRC
        // trailer remains, read returns sizeof(crc) (4) bytes — the loop break.
        let mut buf = vec![0u8; DISK_STATE_SIZE];
        let read_bytes = fd::transient_read::call(fd_no, &mut buf);

        // no further data: this is the trailing CRC
        if read_bytes == U32_SIZE as isize {
            file_crc = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
            break;
        }

        if read_bytes < 0 {
            let _ = fd::close_transient_file::call(fd_no);
            return Err(file_access_err(
                format!("could not read file \"{path}\""),
                -read_bytes as i32,
            ));
        }

        if read_bytes != DISK_STATE_SIZE as isize {
            let _ = fd::close_transient_file::call(fd_no);
            return Err(file_access_err(
                format!(
                    "could not read file \"{path}\": read {read_bytes} of {DISK_STATE_SIZE}"
                ),
                0,
            ));
        }

        crc = crc32c::pg_comp_crc32c::call(crc, &buf);

        let roident = RepOriginId::from_le_bytes([buf[0], buf[1]]);
        let remote_lsn = XLogRecPtr::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        states.push((roident, remote_lsn));
    }

    // now check checksum
    crc = fin_crc32c(crc);
    if file_crc != crc {
        let _ = fd::close_transient_file::call(fd_no);
        return Err(PgError::new(
            PANIC,
            format!(
                "replication slot checkpoint has wrong checksum {crc}, expected {file_crc}"
            ),
        )
        .with_sqlstate(ERRCODE_DATA_CORRUPTED));
    }

    if fd::close_transient_file::call(fd_no) != 0 {
        return Err(file_access_err(
            format!("could not close file \"{path}\""),
            0,
        ));
    }

    Ok(Some(states))
}
