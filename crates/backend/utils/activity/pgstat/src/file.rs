// pgstat.c's statsfile half: write pg_stat/pgstat.stat on clean shutdown
// (checkpointer's before_shmem_exit), restore + unlink on clean start,
// unlink on crash recovery. Header, record tags, and per-entry payload
// bytes match C's pgstat_write_statsfile/pgstat_read_statsfile exactly
// (no on-disk length field; payload size is implicit from `kind`, as in
// C's pgstat_get_entry_len) so a C-initdb'd datadir's pgstat.stat is
// readable on pgrust's first boot. Corruption behavior matches C (log,
// reset, unlink).

use core::mem::size_of;
use std::io::{Read, Write};

use elog::elog;
use types_error::{PgResult, LOG};

use crate::pending::{
    PgStat_HashKey, PgStat_Kind, PGSTAT_KIND_DATABASE, PGSTAT_KIND_FUNCTION, PGSTAT_KIND_RELATION,
    PGSTAT_KIND_REPLSLOT, PGSTAT_KIND_SUBSCRIPTION,
};
use crate::shmem::SharedEntry;

// Must equal C 18.3's PGSTAT_FILE_FORMAT_ID (pgstat.h): initdb bootstrap runs
// the real C postgres, which writes this file at shutdown; pgrust's first
// boot reads it back, so header and entry layout below must byte-match C's
// pgstat_write_statsfile/pgstat_read_statsfile.
pub const PGSTAT_FILE_FORMAT_ID: i32 = 0x01A5BCB7;

const PGSTAT_FILE_ENTRY_END: u8 = b'E';
const PGSTAT_FILE_ENTRY_HASH: u8 = b'S';
const PGSTAT_FILE_ENTRY_FIXED: u8 = b'F';
const PGSTAT_FILE_ENTRY_NAME: u8 = b'N';

const NAMEDATALEN: usize = 64;

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

fn entry_payload(entry: &SharedEntry) -> Option<&[u8]> {
    match entry {
        SharedEntry::Relation(t) => Some(as_bytes(t)),
        SharedEntry::Database(d) => Some(as_bytes(d)),
        SharedEntry::Function(f) => Some(as_bytes(f)),
        SharedEntry::Subscription(s) => Some(as_bytes(s)),
        // BACKEND is write_to_file = false in C's kind table.
        SharedEntry::Backend(_) => None,
        // REPLSLOT serializes by name ('N' records); see pgstat_write_statsfile.
        SharedEntry::ReplSlot(_) => None,
    }
}

// C's write_chunk/pgstat_get_entry_len carry no on-disk length: the payload
// size is implicit, derived from `kind` on both write and read.
fn push_fixed<T: Copy>(out: &mut Vec<u8>, kind: PgStat_Kind, v: &T) {
    out.push(PGSTAT_FILE_ENTRY_FIXED);
    out.extend_from_slice(&kind.0.to_ne_bytes());
    out.extend_from_slice(as_bytes(v));
}

pub(crate) fn pgstat_write_statsfile() -> std::io::Result<()> {
    use crate::pending::{
        PGSTAT_KIND_ARCHIVER, PGSTAT_KIND_BGWRITER, PGSTAT_KIND_CHECKPOINTER, PGSTAT_KIND_IO,
        PGSTAT_KIND_SLRU, PGSTAT_KIND_WAL,
    };
    let tmp = stat_path(PGSTAT_STAT_PERMANENT_TMPFILE);
    let dst = stat_path(PGSTAT_STAT_PERMANENT_FILENAME);
    // vfs-routed (provider-seam reroute): pg_stat/ is datadir domain;
    // std::fs would bypass the sim namespace. pg_stat is one level deep.
    if let Some(dir) = tmp.parent().and_then(|d| d.to_str()) {
        if fd::MakePGDirectory(dir) < 0 && fd::get_errno() != libc::EEXIST {
            return Err(std::io::Error::from_raw_os_error(fd::get_errno()));
        }
    }
    let mut out = Vec::with_capacity(8192);
    out.extend_from_slice(&PGSTAT_FILE_FORMAT_ID.to_ne_bytes());
    push_fixed(&mut out, PGSTAT_KIND_ARCHIVER, &crate::archiver::export_archiver_stats());
    push_fixed(&mut out, PGSTAT_KIND_BGWRITER, &crate::bgwriter::export_bgwriter_stats());
    push_fixed(
        &mut out,
        PGSTAT_KIND_CHECKPOINTER,
        &crate::checkpointer::export_checkpointer_stats(),
    );
    push_fixed(&mut out, PGSTAT_KIND_IO, &crate::io::export_io_stats());
    push_fixed(&mut out, PGSTAT_KIND_SLRU, &crate::slru::export_slru_stats());
    push_fixed(&mut out, PGSTAT_KIND_WAL, &crate::wal::export_wal_stats());
    crate::shmem::export_entries(|key, entry| {
        if let SharedEntry::ReplSlot(slot_entry) = &entry {
            // to_serialized_name: late shutdown, the slot set can't change; a
            // missing name is C's elog(ERROR) here.
            let namebuf = slot_seams::replication_slot_name::call(key.objid as i32)
                .ok()
                .flatten()
                .unwrap_or_else(|| {
                    panic!(
                        "could not find name for replication slot index {}",
                        key.objid
                    )
                });
            out.push(PGSTAT_FILE_ENTRY_NAME);
            out.extend_from_slice(&key.kind.0.to_ne_bytes());
            out.extend_from_slice(&namebuf);
            out.extend_from_slice(as_bytes(slot_entry));
            return;
        }
        let Some(payload) = entry_payload(&entry) else {
            return;
        };
        out.push(PGSTAT_FILE_ENTRY_HASH);
        out.extend_from_slice(&key.kind.0.to_ne_bytes());
        out.extend_from_slice(&key.dboid.to_ne_bytes());
        out.extend_from_slice(&key.objid.to_ne_bytes());
        out.extend_from_slice(payload);
    });
    out.push(PGSTAT_FILE_ENTRY_END);

    let tmp_s = tmp.to_str().expect("stat paths are UTF-8");
    let dst_s = dst.to_str().expect("stat paths are UTF-8");
    fd::write_whole_file(tmp_s, &out, /* do_sync = */ true)
        .map_err(std::io::Error::from_raw_os_error)?;
    if fd::pg_rename(tmp_s, dst_s) < 0 {
        return Err(std::io::Error::from_raw_os_error(fd::get_errno()));
    }
    Ok(())
}

fn pgstat_reset_after_failure() {
    let ts = timestamp_seams::get_current_timestamp::call();
    crate::shmem::clear_all_entries();
    crate::archiver::pgstat_archiver_reset_all_cb(ts);
    crate::bgwriter::pgstat_bgwriter_reset_all_cb(ts);
    crate::checkpointer::pgstat_checkpointer_reset_all_cb(ts);
    crate::io::pgstat_io_reset_all_cb(ts);
    crate::slru::pgstat_slru_reset_all_cb(ts);
    crate::wal::pgstat_wal_reset_all_cb(ts);
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

// C's pgstat_get_entry_len(kind) reads the length back out of the kind info
// table, not the file; take_payload mirrors that by sizing the read from T.
fn take_payload<T: Copy + Default>(c: &mut Cursor<'_>) -> Option<T> {
    from_bytes(c.take(size_of::<T>())?)
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
                let entry = match kind {
                    PGSTAT_KIND_RELATION => SharedEntry::Relation(take_payload(&mut c)?),
                    PGSTAT_KIND_DATABASE => SharedEntry::Database(take_payload(&mut c)?),
                    PGSTAT_KIND_FUNCTION => SharedEntry::Function(take_payload(&mut c)?),
                    PGSTAT_KIND_SUBSCRIPTION => SharedEntry::Subscription(take_payload(&mut c)?),
                    _ => return None,
                };
                crate::shmem::import_entry(PgStat_HashKey { kind, dboid, objid }, entry);
            }
            PGSTAT_FILE_ENTRY_NAME => {
                let kind = PgStat_Kind(c.take_u32()?);
                let namebuf = c.take(NAMEDATALEN)?;
                if kind != PGSTAT_KIND_REPLSLOT {
                    return None;
                }
                let entry = SharedEntry::ReplSlot(take_payload(&mut c)?);
                let nul = namebuf.iter().position(|&b| b == 0).unwrap_or(NAMEDATALEN);
                let Ok(name) = core::str::from_utf8(&namebuf[..nul]) else {
                    return None;
                };
                // from_serialized_name: drop stats for slots removed while
                // shut down (StartupReplicationSlots runs before restore).
                let Ok((index, _)) = slot_seams::named_replication_slot_info::call(name, true)
                else {
                    return None;
                };
                if index >= 0 {
                    crate::shmem::import_entry(
                        PgStat_HashKey {
                            kind,
                            dboid: types_core::InvalidOid,
                            objid: index as u64,
                        },
                        entry,
                    );
                }
            }
            PGSTAT_FILE_ENTRY_FIXED => {
                use crate::pending::{
                    PGSTAT_KIND_ARCHIVER, PGSTAT_KIND_BGWRITER, PGSTAT_KIND_CHECKPOINTER,
                    PGSTAT_KIND_IO, PGSTAT_KIND_SLRU, PGSTAT_KIND_WAL,
                };
                let kind = PgStat_Kind(c.take_u32()?);
                match kind {
                    PGSTAT_KIND_ARCHIVER => {
                        crate::archiver::import_archiver_stats(take_payload(&mut c)?)
                    }
                    PGSTAT_KIND_BGWRITER => {
                        crate::bgwriter::import_bgwriter_stats(take_payload(&mut c)?)
                    }
                    PGSTAT_KIND_CHECKPOINTER => {
                        crate::checkpointer::import_checkpointer_stats(take_payload(&mut c)?)
                    }
                    PGSTAT_KIND_IO => crate::io::import_io_stats(take_payload(&mut c)?),
                    PGSTAT_KIND_SLRU => crate::slru::import_slru_stats(take_payload(&mut c)?),
                    PGSTAT_KIND_WAL => crate::wal::import_wal_stats(take_payload(&mut c)?),
                    _ => return None,
                }
            }
            _ => return None,
        }
    }
}

pub(crate) fn pgstat_read_statsfile() {
    let path = stat_path(PGSTAT_STAT_PERMANENT_FILENAME);
    let path_s = path.to_str().expect("stat paths are UTF-8");
    // vfs-routed (provider-seam reroute).
    let buf = match fd::read_whole_file(path_s) {
        Ok(buf) => buf,
        Err(en) => {
            if en != libc::ENOENT {
                let e = std::io::Error::from_raw_os_error(en);
                let _ = elog(
                    LOG,
                    format!("could not open statistics file \"{}\": {e}", path.display()),
                );
            }
            pgstat_reset_after_failure();
            return;
        }
    };
    if read_statsfile_body(&buf).is_none() {
        let _ = elog(LOG, format!("corrupted statistics file \"{}\"", path.display()));
        pgstat_reset_after_failure();
    }
    let _ = fd::pg_unlink(path_s);
}

pub fn pgstat_restore_stats() -> PgResult<()> {
    pgstat_read_statsfile();
    Ok(())
}

pub fn pgstat_discard_stats() -> PgResult<()> {
    let path = stat_path(PGSTAT_STAT_PERMANENT_FILENAME);
    let _ = fd::pg_unlink(path.to_str().expect("stat paths are UTF-8"));
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
