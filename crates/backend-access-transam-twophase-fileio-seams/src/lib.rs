//! Seam declarations for the `pg_twophase` state-file body I/O backing
//! `twophase.c`'s static file helpers (`ReadTwoPhaseFile`,
//! `RecreateTwoPhaseFile`, `RemoveTwoPhaseFile`, `restoreTwoPhaseData`'s
//! directory scan, `CheckPointTwoPhase`'s dir fsync). These are the
//! `OpenTransientFile`/`read`/`write`/`pg_fsync`/`durable_unlink`/`ReadDir`
//! syscall glue (fd.c-backed); the magic/CRC/length validation and the record
//! format live in the twophase crate itself. Installed by the owning unit's
//! `init_seams()` when the file-I/O glue lands; until then a call panics
//! loudly.

extern crate alloc;

use alloc::vec::Vec;
use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `ReadTwoPhaseFile`'s raw read: `OpenTransientFile + fstat + read +
    /// close` for the `pg_twophase` file of `xid`. Returns the raw file bytes,
    /// or `None` when `missing_ok` and the file does not exist (`ENOENT`). I/O
    /// failure `ereport(ERROR)`s, carried on `Err`.
    pub fn read_twophase_file(
        xid: TransactionId,
        missing_ok: bool,
    ) -> PgResult<Option<Vec<u8>>>
);

seam_core::seam!(
    /// `RecreateTwoPhaseFile`'s store: `OpenTransientFile(O_CREAT|O_TRUNC|
    /// O_WRONLY) + write(content) + write(crc) + pg_fsync + close`. `content`
    /// excludes the trailing CRC; `crc` is the 4-byte little-endian CRC-32C the
    /// twophase crate computed. I/O failure `ereport(ERROR)`s.
    pub fn recreate_twophase_file(
        xid: TransactionId,
        content: &[u8],
        crc: u32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `RemoveTwoPhaseFile(xid, giveWarning)` — `durable_unlink` (or `unlink`)
    /// of the state file; `give_warning` selects whether a missing file warns.
    pub fn remove_twophase_file(xid: TransactionId, give_warning: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `restoreTwoPhaseData`'s directory scan: `AllocateDir/ReadDir(TWOPHASE_DIR)`.
    /// Returns the 16-hex-char file basenames decoded to their full-xid `u64`
    /// values. I/O failure `ereport(ERROR)`s.
    pub fn scan_twophase_dir() -> PgResult<Vec<u64>>
);

seam_core::seam!(
    /// `CheckPointTwoPhase`'s `fsync_fname(TWOPHASE_DIR, true)`.
    pub fn fsync_twophase_dir() -> PgResult<()>
);

seam_core::seam!(
    /// `PrepareRedoAdd`'s `access(TwoPhaseFilePath(xid), F_OK)` probe: `Ok(true)`
    /// if the file exists, `Ok(false)` on `ENOENT`, `Err` for any other errno.
    pub fn twophase_file_exists(xid: TransactionId) -> PgResult<bool>
);
