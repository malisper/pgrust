// pgstat.c's statsfile half: write pg_stat/pgstat.stat on clean shutdown
// (checkpointer's before_shmem_exit), restore + unlink on clean start,
// unlink on crash recovery. Header, record tags, and per-entry payload
// bytes match C's pgstat_write_statsfile/pgstat_read_statsfile exactly
// (no on-disk length field; payload size is implicit from `kind`, as in
// C's pgstat_get_entry_len) so a C-initdb'd datadir's pgstat.stat is
// readable on pgrust's first boot. Corruption behavior matches C (log,
// reset, unlink).

use core::mem::size_of;

use elog::{elog, ereport};
use types_error::{ErrorLocation, PgResult, ERROR, LOG, WARNING};

use crate::pending::{
    PgStat_HashKey, PgStat_Kind, PGSTAT_KIND_ARCHIVER, PGSTAT_KIND_BACKEND, PGSTAT_KIND_BGWRITER,
    PGSTAT_KIND_CHECKPOINTER, PGSTAT_KIND_DATABASE, PGSTAT_KIND_FUNCTION, PGSTAT_KIND_IO,
    PGSTAT_KIND_RELATION, PGSTAT_KIND_REPLSLOT, PGSTAT_KIND_SLRU, PGSTAT_KIND_SUBSCRIPTION,
    PGSTAT_KIND_WAL,
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

// I/O goes through the DataDir-joined path (the process may not have
// chdir'd into the datadir yet — and tests never do); messages print C's
// datadir-relative name (pgstat.c's statfile/tmpfile literals).
fn stat_path(name: &str) -> std::path::PathBuf {
    std::path::Path::new(init_small::globals::DataDir().unwrap_or(".")).join(name)
}

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

// ereport(LOG, (errcode_for_file_access(), errmsg("...: %m"))) for a file
// operation that failed with `errno`.
#[track_caller]
fn log_file_error(errno: i32, message: String, func: &'static str) {
    let _ = ereport(LOG)
        .with_saved_errno(errno)
        .errcode_for_file_access()
        .errmsg(message)
        .finish(loc(func));
}

// pgstat_is_kind_valid (pgstat.c:1382) over the builtin range; pgrust has no
// custom kinds, and every builtin kind has a kind info in C's table.
fn is_kind_valid(kind: PgStat_Kind) -> bool {
    (PGSTAT_KIND_DATABASE.0..=PGSTAT_KIND_WAL.0).contains(&kind.0)
}

// C's PgStat_KindInfo.fixed_amount.
fn is_fixed_kind(kind: PgStat_Kind) -> bool {
    (PGSTAT_KIND_ARCHIVER.0..=PGSTAT_KIND_WAL.0).contains(&kind.0)
}

// C's pgstat_get_entry_len(kind) for the variable-numbered kinds, and the
// shared_data_len of the fixed kinds (an 'S' record may carry either: C
// accepts a fixed kind there as an ordinary hash entry).
fn entry_len(kind: PgStat_Kind) -> usize {
    match kind {
        PGSTAT_KIND_DATABASE => size_of::<crate::database::PgStat_StatDBEntry>(),
        PGSTAT_KIND_RELATION => size_of::<crate::shmem::PgStat_StatTabEntry>(),
        PGSTAT_KIND_FUNCTION => size_of::<crate::function::PgStat_StatFuncEntry>(),
        PGSTAT_KIND_REPLSLOT => size_of::<crate::replslot::PgStat_StatReplSlotEntry>(),
        PGSTAT_KIND_SUBSCRIPTION => size_of::<crate::subscription::PgStat_StatSubEntry>(),
        PGSTAT_KIND_BACKEND => size_of::<crate::backend::PgStat_Backend>(),
        PGSTAT_KIND_ARCHIVER => size_of::<crate::archiver::PgStat_ArchiverStats>(),
        PGSTAT_KIND_BGWRITER => size_of::<crate::bgwriter::PgStat_BgWriterStats>(),
        PGSTAT_KIND_CHECKPOINTER => size_of::<crate::checkpointer::PgStat_CheckpointerStats>(),
        PGSTAT_KIND_IO => size_of::<crate::io::PgStat_IO>(),
        PGSTAT_KIND_SLRU => {
            size_of::<[crate::slru::PgStat_SLRUStats; crate::slru::SLRU_NUM_ELEMENTS]>()
        }
        PGSTAT_KIND_WAL => size_of::<crate::wal::PgStat_WalStats>(),
        _ => unreachable!("entry_len: kind {} is not a builtin kind", kind.0),
    }
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

// pgstat_write_statsfile (pgstat.c:1560-1737). Failures on the temp file
// are LOG + cleanup, as in C; a missing replication slot name is
// pgstat_replslot_to_serialized_name_cb's elog(ERROR) (pgstat_replslot.c:197),
// which proc_exit promotes to FATAL exactly as C's errstart does.
pub(crate) fn pgstat_write_statsfile() -> PgResult<()> {
    let tmp = stat_path(PGSTAT_STAT_PERMANENT_TMPFILE);
    let dst = stat_path(PGSTAT_STAT_PERMANENT_FILENAME);
    // vfs-routed (provider-seam reroute): pg_stat/ is datadir domain;
    // std::fs would bypass the sim namespace. pg_stat is one level deep.
    // C never creates the directory (initdb does); a failure here surfaces
    // as the open failure below, with C's message.
    if let Some(dir) = tmp.parent().and_then(|d| d.to_str()) {
        let _ = fd::MakePGDirectory(dir);
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
    let mut nameless_slot: Option<u64> = None;
    crate::shmem::export_entries(|key, entry| {
        if nameless_slot.is_some() {
            return;
        }
        if let SharedEntry::ReplSlot(slot_entry) = &entry {
            // to_serialized_name: late shutdown, the slot set can't change; a
            // missing name is C's elog(ERROR) here.
            let Some(namebuf) =
                slot_seams::replication_slot_name::call(key.objid as i32).ok().flatten()
            else {
                nameless_slot = Some(key.objid);
                return;
            };
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
    if let Some(objid) = nameless_slot {
        // pgstat_replslot.c:197
        elog(
            ERROR,
            format!("could not find name for replication slot index {objid}"),
        )?;
        unreachable!("elog(ERROR) returns Err");
    }
    out.push(PGSTAT_FILE_ENTRY_END);

    let tmp_s = tmp.to_str().expect("stat paths are UTF-8");
    let dst_s = dst.to_str().expect("stat paths are UTF-8");
    // AllocateFile(tmpfile, PG_BINARY_W) (pgstat.c:1594)
    let fd = fd::OpenTransientFile(tmp_s, libc::O_CREAT | libc::O_TRUNC | libc::O_WRONLY)?;
    if fd < 0 {
        log_file_error(
            fd::get_errno(),
            format!(
                "could not open temporary statistics file \"{PGSTAT_STAT_PERMANENT_TMPFILE}\": %m"
            ),
            "pgstat_write_statsfile",
        );
        return Ok(());
    }
    let mut off: usize = 0;
    let mut write_errno = None;
    while off < out.len() {
        let n = fd::pg_pwrite(fd, &out[off..], off as i64);
        if n < 0 && fd::get_errno() == libc::EINTR {
            continue;
        }
        if n <= 0 {
            // C's write_chunk: a short write with no errno is ENOSPC.
            write_errno = Some(if n == 0 { libc::ENOSPC } else { fd::get_errno() });
            break;
        }
        off += n as usize;
    }
    if let Some(en) = write_errno {
        // ferror(fpout) (pgstat.c:1712)
        log_file_error(
            en,
            format!(
                "could not write temporary statistics file \"{PGSTAT_STAT_PERMANENT_TMPFILE}\": %m"
            ),
            "pgstat_write_statsfile",
        );
        fd::CloseTransientFile(fd);
        let _ = fd::pg_unlink(tmp_s);
        return Ok(());
    }
    if fd::CloseTransientFile(fd) != 0 {
        // FreeFile(fpout) < 0 (pgstat.c:1722)
        log_file_error(
            fd::get_errno(),
            format!(
                "could not close temporary statistics file \"{PGSTAT_STAT_PERMANENT_TMPFILE}\": %m"
            ),
            "pgstat_write_statsfile",
        );
        let _ = fd::pg_unlink(tmp_s);
        return Ok(());
    }
    if fd::durable_rename(tmp_s, dst_s, LOG)? < 0 {
        // durable_rename already emitted log message (pgstat.c:1733)
        let _ = fd::pg_unlink(tmp_s);
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

// elog(WARNING, ...) followed by `goto error` (pgstat.c's read loop).
fn corrupt(message: String) -> Option<()> {
    let _ = elog(WARNING, message);
    None
}

// C's `%c` of the fgetc() result: the low byte, EOF (-1) included.
fn type_char(t: i32) -> char {
    (t as u8) as char
}

// The parse half of pgstat_read_statsfile (pgstat.c:1790-2034): None means
// `goto error` (the caller logs "corrupted statistics file" and resets), and
// every failure names itself in a WARNING first, as in C.
pub(crate) fn read_statsfile_body(buf: &[u8]) -> Option<()> {
    let mut c = Cursor { buf, pos: 0 };
    let Some(format_id) = c.take_u32() else {
        return corrupt("could not read format ID".into());
    };
    if format_id as i32 != PGSTAT_FILE_FORMAT_ID {
        return corrupt(format!(
            "found incorrect format ID {} (expected {})",
            format_id as i32, PGSTAT_FILE_FORMAT_ID
        ));
    }
    loop {
        let t = match c.take(1) {
            Some(b) => i32::from(b[0]),
            None => -1, // fgetc's EOF: the default arm below
        };
        let tc = type_char(t);
        match t {
            t if t == i32::from(PGSTAT_FILE_ENTRY_END) => {
                // check that PGSTAT_FILE_ENTRY_END actually signals end of file
                if c.pos != buf.len() {
                    return corrupt("could not read end-of-file".into());
                }
                return Some(());
            }
            t if t == i32::from(PGSTAT_FILE_ENTRY_FIXED) => {
                let Some(kind) = c.take_u32().map(PgStat_Kind) else {
                    return corrupt(format!(
                        "could not read stats kind for entry of type {tc}"
                    ));
                };
                if !is_kind_valid(kind) {
                    return corrupt(format!(
                        "invalid stats kind {} for entry of type {tc}",
                        kind.0
                    ));
                }
                if !is_fixed_kind(kind) {
                    return corrupt(format!(
                        "invalid fixed_amount in stats kind {} for entry of type {tc}",
                        kind.0
                    ));
                }
                let imported = match kind {
                    PGSTAT_KIND_ARCHIVER => {
                        take_payload(&mut c).map(crate::archiver::import_archiver_stats)
                    }
                    PGSTAT_KIND_BGWRITER => {
                        take_payload(&mut c).map(crate::bgwriter::import_bgwriter_stats)
                    }
                    PGSTAT_KIND_CHECKPOINTER => {
                        take_payload(&mut c).map(crate::checkpointer::import_checkpointer_stats)
                    }
                    PGSTAT_KIND_IO => take_payload(&mut c).map(crate::io::import_io_stats),
                    PGSTAT_KIND_SLRU => take_payload(&mut c).map(crate::slru::import_slru_stats),
                    PGSTAT_KIND_WAL => take_payload(&mut c).map(crate::wal::import_wal_stats),
                    _ => unreachable!("is_fixed_kind covers every fixed kind"),
                };
                if imported.is_none() {
                    return corrupt(format!(
                        "could not read data of stats kind {} for entry of type {tc} with size {}",
                        kind.0,
                        entry_len(kind)
                    ));
                }
            }
            t if t == i32::from(PGSTAT_FILE_ENTRY_HASH)
                || t == i32::from(PGSTAT_FILE_ENTRY_NAME) =>
            {
                let key = if t == i32::from(PGSTAT_FILE_ENTRY_HASH) {
                    // normal stats entry, identified by PgStat_HashKey
                    // sizeof(PgStat_HashKey): kind u32 + dboid u32 + objid u64.
                    let Some(key) = c.take(4 + 4 + 8) else {
                        return corrupt(format!("could not read key for entry of type {tc}"));
                    };
                    let key = PgStat_HashKey {
                        kind: PgStat_Kind(u32::from_ne_bytes(key[0..4].try_into().unwrap())),
                        dboid: u32::from_ne_bytes(key[4..8].try_into().unwrap()),
                        objid: u64::from_ne_bytes(key[8..16].try_into().unwrap()),
                    };
                    if !is_kind_valid(key.kind) {
                        return corrupt(format!(
                            "invalid stats kind for entry {}/{}/{} of type {tc}",
                            key.kind.0, key.dboid, key.objid
                        ));
                    }
                    key
                } else {
                    // stats entry identified by name on disk (e.g. slots)
                    let Some(kind) = c.take_u32().map(PgStat_Kind) else {
                        return corrupt(format!(
                            "could not read stats kind for entry of type {tc}"
                        ));
                    };
                    let Some(namebuf) = c.take(NAMEDATALEN) else {
                        return corrupt(format!(
                            "could not read name of stats kind {} for entry of type {tc}",
                            kind.0
                        ));
                    };
                    if !is_kind_valid(kind) {
                        return corrupt(format!(
                            "invalid stats kind {} for entry of type {tc}",
                            kind.0
                        ));
                    }
                    if kind != PGSTAT_KIND_REPLSLOT {
                        return corrupt(format!(
                            "invalid from_serialized_name in stats kind {} for entry of type {tc}",
                            kind.0
                        ));
                    }
                    let nul = namebuf.iter().position(|&b| b == 0).unwrap_or(NAMEDATALEN);
                    let name = String::from_utf8_lossy(&namebuf[..nul]);
                    // from_serialized_name: drop stats for slots removed while
                    // shut down (StartupReplicationSlots runs before restore).
                    let index = match core::str::from_utf8(&namebuf[..nul]) {
                        Ok(name) => slot_seams::named_replication_slot_info::call(name, true)
                            .map(|(index, _)| index)
                            .unwrap_or(-1),
                        Err(_) => -1,
                    };
                    if index < 0 {
                        // skip over data for entry we don't care about
                        if c.take(entry_len(kind)).is_none() {
                            return corrupt(format!(
                                "could not seek \"{name}\" of stats kind {} for entry of type {tc}",
                                kind.0
                            ));
                        }
                        continue;
                    }
                    PgStat_HashKey { kind, dboid: types_core::InvalidOid, objid: index as u64 }
                };

                // don't allow duplicate entries (dshash_find_or_insert found)
                if crate::shmem::contains_entry(&key) {
                    return corrupt(format!(
                        "found duplicate stats entry {}/{}/{} of type {tc}",
                        key.kind.0, key.dboid, key.objid
                    ));
                }
                let entry = match key.kind {
                    PGSTAT_KIND_RELATION => take_payload(&mut c).map(SharedEntry::Relation),
                    PGSTAT_KIND_DATABASE => take_payload(&mut c).map(SharedEntry::Database),
                    PGSTAT_KIND_FUNCTION => take_payload(&mut c).map(SharedEntry::Function),
                    PGSTAT_KIND_SUBSCRIPTION => {
                        take_payload(&mut c).map(SharedEntry::Subscription)
                    }
                    PGSTAT_KIND_BACKEND => take_payload(&mut c).map(SharedEntry::Backend),
                    PGSTAT_KIND_REPLSLOT => take_payload(&mut c).map(SharedEntry::ReplSlot),
                    // C stores a fixed kind's 'S' record as an inert hash
                    // entry nothing ever fetches by that key; consume its
                    // shared_data_len bytes and carry on.
                    _ => {
                        if c.take(entry_len(key.kind)).is_none() {
                            return corrupt(format!(
                                "could not read data for entry {}/{}/{} of type {tc}",
                                key.kind.0, key.dboid, key.objid
                            ));
                        }
                        continue;
                    }
                };
                let Some(entry) = entry else {
                    return corrupt(format!(
                        "could not read data for entry {}/{}/{} of type {tc}",
                        key.kind.0, key.dboid, key.objid
                    ));
                };
                crate::shmem::import_entry(key, entry);
            }
            _ => return corrupt(format!("could not read entry of type {tc}")),
        }
    }
}

// pgstat_read_statsfile (pgstat.c:1751-2044).
pub(crate) fn pgstat_read_statsfile() {
    let path = stat_path(PGSTAT_STAT_PERMANENT_FILENAME);
    let path_s = path.to_str().expect("stat paths are UTF-8");
    // vfs-routed (provider-seam reroute).
    let buf = match fd::read_whole_file(path_s) {
        Ok(buf) => buf,
        Err(en) => {
            if en != libc::ENOENT {
                log_file_error(
                    en,
                    format!(
                        "could not open statistics file \"{PGSTAT_STAT_PERMANENT_FILENAME}\": %m"
                    ),
                    "pgstat_read_statsfile",
                );
            }
            pgstat_reset_after_failure();
            return;
        }
    };
    if read_statsfile_body(&buf).is_none() {
        let _ = elog(
            LOG,
            format!("corrupted statistics file \"{PGSTAT_STAT_PERMANENT_FILENAME}\""),
        );
        pgstat_reset_after_failure();
    }
    let _ = fd::pg_unlink(path_s);
}

pub fn pgstat_restore_stats() -> PgResult<()> {
    pgstat_read_statsfile();
    Ok(())
}

// pgstat_discard_stats (pgstat.c:519-547).
pub fn pgstat_discard_stats() -> PgResult<()> {
    let path = stat_path(PGSTAT_STAT_PERMANENT_FILENAME);
    if fd::pg_unlink(path.to_str().expect("stat paths are UTF-8")) != 0
        && fd::get_errno() != libc::ENOENT
    {
        log_file_error(
            fd::get_errno(),
            format!(
                "could not unlink permanent statistics file \"{PGSTAT_STAT_PERMANENT_FILENAME}\": %m"
            ),
            "pgstat_discard_stats",
        );
    }
    pgstat_reset_after_failure();
    Ok(())
}

// Called by the checkpointer's before_shmem_exit; writes only on proc_exit(0)
// so a disorderly shutdown leaves no file and crash start discards instead.
pub fn pgstat_before_server_shutdown(code: i32) -> PgResult<()> {
    crate::pending::pgstat_report_stat(true);
    if code == 0 {
        // Temp-file failures are logged inside; only the replslot-name
        // elog(ERROR) escapes, for proc_exit's FATAL promotion.
        pgstat_write_statsfile()?;
    }
    Ok(())
}
