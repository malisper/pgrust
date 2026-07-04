// pgstat.c's statsfile half: write pg_stat/pgstat.stat on clean shutdown
// (checkpointer's before_shmem_exit), restore + unlink on clean start,
// unlink on crash recovery. Record payloads are this port's repr(C) entry
// structs (all-i64, native-endian), not C's PgStatShared_* layouts — the
// file is never exchanged with C; corruption behavior matches C (log,
// reset, unlink). No shared fixed-numbered kinds are ported, so C's
// PGSTAT_FILE_ENTRY_FIXED records don't exist here.

use core::mem::size_of;
use std::io::{Read, Write};

use elog::elog;
use types_error::{PgResult, LOG};

use crate::pending::{
    PgStat_HashKey, PgStat_Kind, PGSTAT_KIND_DATABASE, PGSTAT_KIND_FUNCTION, PGSTAT_KIND_RELATION,
};
use crate::shmem::SharedEntry;

// Deliberately NOT C's 0x01A5BCB7: a C-initdb'd datadir carries a C-format
// pgstat.stat whose payloads we can't parse; a distinct id rejects it at the
// header (C's own incorrect-format path) instead of mid-entry.
pub const PGSTAT_FILE_FORMAT_ID: i32 = 0x51A5BCB7;

const PGSTAT_FILE_ENTRY_END: u8 = b'E';
const PGSTAT_FILE_ENTRY_HASH: u8 = b'S';

const PGSTAT_STAT_PERMANENT_FILENAME: &str = "pg_stat/pgstat.stat";
const PGSTAT_STAT_PERMANENT_TMPFILE: &str = "pg_stat/pgstat.tmp";

fn stat_path(name: &str) -> std::path::PathBuf {
    std::path::Path::new(init_small::globals::DataDir().unwrap_or(".")).join(name)
}

// SAFETY bound: T is one of the repr(C) all-i64 entry structs (no padding,
// any bit pattern valid).
fn as_bytes<T: Copy>(v: &T) -> &[u8] {
    // SAFETY: caller-bound POD contract above.
    unsafe { core::slice::from_raw_parts((v as *const T).cast::<u8>(), size_of::<T>()) }
}

fn from_bytes<T: Copy + Default>(b: &[u8]) -> Option<T> {
    if b.len() != size_of::<T>() {
        return None;
    }
    let mut v = T::default();
    // SAFETY: same POD contract; sizes checked.
    unsafe {
        core::ptr::copy_nonoverlapping(b.as_ptr(), (&mut v as *mut T).cast::<u8>(), b.len());
    }
    Some(v)
}

fn entry_payload(entry: &SharedEntry) -> &[u8] {
    match entry {
        SharedEntry::Relation(t) => as_bytes(t),
        SharedEntry::Database(d) => as_bytes(d),
        SharedEntry::Function(f) => as_bytes(f),
    }
}

pub(crate) fn pgstat_write_statsfile() -> std::io::Result<()> {
    let tmp = stat_path(PGSTAT_STAT_PERMANENT_TMPFILE);
    let dst = stat_path(PGSTAT_STAT_PERMANENT_FILENAME);
    if let Some(dir) = tmp.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut out = Vec::with_capacity(8192);
    out.extend_from_slice(&PGSTAT_FILE_FORMAT_ID.to_ne_bytes());
    crate::shmem::export_entries(|key, entry| {
        out.push(PGSTAT_FILE_ENTRY_HASH);
        out.extend_from_slice(&key.kind.0.to_ne_bytes());
        out.extend_from_slice(&key.dboid.to_ne_bytes());
        out.extend_from_slice(&key.objid.to_ne_bytes());
        let payload = entry_payload(&entry);
        out.extend_from_slice(&(payload.len() as u32).to_ne_bytes());
        out.extend_from_slice(payload);
    });
    out.push(PGSTAT_FILE_ENTRY_END);

    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&out)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, &dst)?;
    Ok(())
}

// pgstat_reset_after_failure: with no shared fixed-numbered kinds ported
// there are no reset timestamps to stamp; discard any partially-read entries.
fn pgstat_reset_after_failure() {
    crate::shmem::clear_all_entries();
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        if end > self.buf.len() {
            return None;
        }
        let head = &self.buf[self.pos..end];
        self.pos = end;
        Some(head)
    }

    fn take_u32(&mut self) -> Option<u32> {
        Some(u32::from_ne_bytes(self.take(4)?.try_into().unwrap()))
    }
}

pub(crate) fn read_statsfile_body(buf: &[u8]) -> Option<()> {
    let mut c = Cursor { buf, pos: 0 };
    if c.take_u32()? as i32 != PGSTAT_FILE_FORMAT_ID {
        return None;
    }
    loop {
        match *c.take(1)?.first().unwrap() {
            PGSTAT_FILE_ENTRY_END => {
                return (c.pos == buf.len()).then_some(());
            }
            PGSTAT_FILE_ENTRY_HASH => {
                let kind = PgStat_Kind(c.take_u32()?);
                let dboid = c.take_u32()?;
                let objid = u64::from_ne_bytes(c.take(8)?.try_into().unwrap());
                let len = c.take_u32()? as usize;
                let payload = c.take(len)?;
                let entry = match kind {
                    PGSTAT_KIND_RELATION => SharedEntry::Relation(from_bytes(payload)?),
                    PGSTAT_KIND_DATABASE => SharedEntry::Database(from_bytes(payload)?),
                    PGSTAT_KIND_FUNCTION => SharedEntry::Function(from_bytes(payload)?),
                    _ => return None,
                };
                crate::shmem::import_entry(PgStat_HashKey { kind, dboid, objid }, entry);
            }
            _ => return None,
        }
    }
}

pub(crate) fn pgstat_read_statsfile() {
    let path = stat_path(PGSTAT_STAT_PERMANENT_FILENAME);
    let mut buf = Vec::new();
    match std::fs::File::open(&path) {
        Ok(mut f) => {
            if f.read_to_end(&mut buf).is_err() {
                buf.clear();
            }
        }
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                let _ = elog(
                    LOG,
                    format!("could not open statistics file \"{}\": {e}", path.display()),
                );
            }
            pgstat_reset_after_failure();
            return;
        }
    }
    if read_statsfile_body(&buf).is_none() {
        let _ = elog(LOG, format!("corrupted statistics file \"{}\"", path.display()));
        pgstat_reset_after_failure();
    }
    let _ = std::fs::remove_file(&path);
}

pub fn pgstat_restore_stats() -> PgResult<()> {
    pgstat_read_statsfile();
    Ok(())
}

pub fn pgstat_discard_stats() -> PgResult<()> {
    let _ = std::fs::remove_file(stat_path(PGSTAT_STAT_PERMANENT_FILENAME));
    pgstat_reset_after_failure();
    Ok(())
}

// Called by the checkpointer's before_shmem_exit; writes only on proc_exit(0)
// so a disorderly shutdown leaves no file and crash start discards instead.
pub fn pgstat_before_server_shutdown(code: i32) -> PgResult<()> {
    crate::pending::pgstat_report_stat(true);
    if code == 0 {
        if let Err(e) = pgstat_write_statsfile() {
            let _ = elog(
                LOG,
                format!("could not write statistics file \"{PGSTAT_STAT_PERMANENT_FILENAME}\": {e}"),
            );
        }
    }
    Ok(())
}
