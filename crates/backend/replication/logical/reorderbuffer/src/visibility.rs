use std::any::Any;
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use mcx::PgFxHashMap;
use types_core::{CommandId, InvalidOid, Oid, TransactionId};
use types_error::{PgResult, DEBUG1};
use types_snapshot::SnapshotData;
use types_storage::RelFileLocator;
use types_tuple::{HeapTupleData, ItemPointerData};

use crate::{rb_error, rb_file_error};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReorderBufferTupleCidKey {
    pub rlocator: RelFileLocator,
    pub tid: ItemPointerData,
}

#[derive(Clone, Copy, Debug)]
pub struct ReorderBufferTupleCidEnt {
    pub cmin: CommandId,
    pub cmax: CommandId,
    pub combocid: CommandId,
}

pub type TupleCidHash = PgFxHashMap<'static, ReorderBufferTupleCidKey, ReorderBufferTupleCidEnt>;

// C signature takes a Buffer and derives the locator via BufferGetTag; the
// visibility caller passes the tag's rlocator directly instead.
pub fn ResolveCminCmaxDuringDecoding(
    tuplecid_data: Option<&Rc<dyn Any>>,
    snapshot: &SnapshotData<'_>,
    htup: &HeapTupleData<'_>,
    rlocator: RelFileLocator,
) -> PgResult<Option<(CommandId, CommandId)>> {
    // Without the hash (streaming in-progress txns) CIDs read as from the
    // future command.
    let Some(tuplecid_data) = tuplecid_data else {
        return Ok(None);
    };
    let hash = tuplecid_data
        .downcast_ref::<RefCell<TupleCidHash>>()
        .expect("historic tuplecids carry the reorderbuffer hash");

    let key = ReorderBufferTupleCidKey { rlocator, tid: htup.t_self };

    if let Some(ent) = hash.borrow().get(&key) {
        return Ok(Some((ent.cmin, ent.cmax)));
    }
    UpdateLogicalMappings(hash, htup.t_tableOid, snapshot)?;
    match hash.borrow().get(&key) {
        Some(ent) => Ok(Some((ent.cmin, ent.cmax))),
        None => Ok(None),
    }
}

fn TransactionIdInArray(xid: TransactionId, xip: &[TransactionId]) -> bool {
    xip.binary_search(&xid).is_ok()
}

// PG_LOGICAL_MAPPINGS_DIR (reorderbuffer.h): the relative path C prints in
// mapping-file errors.
const PG_LOGICAL_MAPPINGS_DIR: &str = "pg_logical/mappings";

// LogicalRewriteMappingData wire format (rewriteheap.h): 2x RelFileLocator
// (3x u32 each) + 2x ItemPointerData (3x u16 each), native-endian.
const LOGICAL_REWRITE_MAPPING_SIZE: usize = 36;

fn read_locator(b: &[u8]) -> RelFileLocator {
    RelFileLocator {
        spcOid: u32::from_ne_bytes(b[0..4].try_into().unwrap()),
        dbOid: u32::from_ne_bytes(b[4..8].try_into().unwrap()),
        relNumber: u32::from_ne_bytes(b[8..12].try_into().unwrap()),
    }
}

fn read_tid(b: &[u8]) -> ItemPointerData {
    let block = ((u16::from_ne_bytes(b[0..2].try_into().unwrap()) as u32) << 16)
        | u16::from_ne_bytes(b[2..4].try_into().unwrap()) as u32;
    ItemPointerData::new(block, u16::from_ne_bytes(b[4..6].try_into().unwrap()))
}

// Wait event (wait_event_names.txt, IO section): reorderbuffer.c:5383.
const PG_WAIT_IO: u32 = 0x0A00_0000;
const WAIT_EVENT_REORDER_LOGICAL_MAPPING_READ: u32 = PG_WAIT_IO + 45;

// The io::Error carrier for the thread's errno, as C's %m / errcode_for_
// file_access read it at ereport time.
fn os_error() -> std::io::Error {
    std::io::Error::from_raw_os_error(elog::errno::current_errno())
}

// ApplyLogicalMappingFile (reorderbuffer.c:5357): stream the file's
// (old locator/tid) -> (new locator/tid) entries into the tuplecid hash so
// cmin/cmax lookups keep working against the rewritten catalog heap.
pub(crate) fn ApplyLogicalMappingFile(
    hash: &RefCell<TupleCidHash>,
    dir: &PathBuf,
    fname: &str,
) -> PgResult<()> {
    let path = dir.join(fname);
    // Errors name the path as C builds it (reorderbuffer.c:5365).
    let cpath = format!("{PG_LOGICAL_MAPPINGS_DIR}/{fname}");
    // reorderbuffer.c:5366: OpenTransientFile(path, O_RDONLY | PG_BINARY).
    let fd = fd::OpenTransientFile(&path.to_string_lossy(), libc::O_RDONLY)?;
    if fd < 0 {
        return Err(rb_file_error(format!("could not open file \"{cpath}\": %m"), &os_error()));
    }

    let result = apply_mapping_entries(hash, fd, &cpath);
    // reorderbuffer.c:5437: CloseTransientFile(fd) != 0 is an ERROR of its
    // own. On a read error C ereports with the descriptor still open and
    // AtEOXact_Files releases it; here the transient descriptor is released
    // first (twophase/files.rs does the same) and the read error wins.
    let close_result = close_mapping_file(fd, &cpath);
    result?;
    close_result
}

// reorderbuffer.c:5437-5440.
fn close_mapping_file(fd: i32, cpath: &str) -> PgResult<()> {
    if fd::CloseTransientFile(fd) != 0 {
        return Err(rb_file_error(format!("could not close file \"{cpath}\": %m"), &os_error()));
    }
    Ok(())
}

// reorderbuffer.c:5371-5435: the read loop over one open mapping file.
fn apply_mapping_entries(hash: &RefCell<TupleCidHash>, fd: i32, cpath: &str) -> PgResult<()> {
    let mut map = [0u8; LOGICAL_REWRITE_MAPPING_SIZE];
    loop {
        // Read all mappings until the end of the file: one read() per
        // entry, reported as ReorderLogicalMappingRead (reorderbuffer.c:
        // 5383-5385).
        waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_REORDER_LOGICAL_MAPPING_READ);
        // SAFETY: map is a live writable buffer of exactly map.len() bytes.
        let read_bytes = unsafe { libc::read(fd, map.as_mut_ptr().cast(), map.len()) };
        waitevent_seams::pgstat_report_wait_end::call();

        if read_bytes < 0 {
            // reorderbuffer.c:5388.
            return Err(rb_file_error(format!("could not read file \"{cpath}\": %m"), &os_error()));
        } else if read_bytes == 0 {
            // EOF.
            break;
        } else if read_bytes as usize != LOGICAL_REWRITE_MAPPING_SIZE {
            // reorderbuffer.c:5395: a torn entry, with whatever errno the
            // short read left behind (C reads it the same way).
            return Err(rb_file_error(
                format!(
                    "could not read from file \"{cpath}\": read {read_bytes} instead of {} bytes",
                    LOGICAL_REWRITE_MAPPING_SIZE
                ),
                &os_error(),
            ));
        }

        let old_key = ReorderBufferTupleCidKey {
            rlocator: read_locator(&map[0..12]),
            tid: read_tid(&map[24..30]),
        };
        let mut h = hash.borrow_mut();
        // No existing mapping: no need to update.
        let Some(ent) = h.get(&old_key).copied() else {
            continue;
        };
        let new_key = ReorderBufferTupleCidKey {
            rlocator: read_locator(&map[12..24]),
            tid: read_tid(&map[30..36]),
        };
        // If present already, keep it (C asserts the existing entry agrees,
        // modulo entries that had no cmin/cmax yet); otherwise map over the
        // old entry's cmin/cmax/combocid.
        h.entry(new_key).or_insert(ent);
    }
    Ok(())
}

// One sscanf "%x" conversion (strtoul base 16 into an unsigned int): skip
// leading whitespace, optional sign, optional 0x/0X, then at least one hex
// digit. Advances `pos`; None is a matching failure.
fn scan_hex(s: &[u8], pos: &mut usize) -> Option<u32> {
    while *pos < s.len() && s[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
    let mut neg = false;
    if *pos < s.len() && (s[*pos] == b'+' || s[*pos] == b'-') {
        neg = s[*pos] == b'-';
        *pos += 1;
    }
    if *pos + 2 < s.len()
        && s[*pos] == b'0'
        && (s[*pos + 1] == b'x' || s[*pos + 1] == b'X')
        && s[*pos + 2].is_ascii_hexdigit()
    {
        *pos += 2;
    }
    let start = *pos;
    let mut v: u64 = 0;
    while *pos < s.len() && s[*pos].is_ascii_hexdigit() {
        let d = (s[*pos] as char).to_digit(16).expect("hex digit") as u64;
        v = v.saturating_mul(16).saturating_add(d);
        *pos += 1;
    }
    if *pos == start {
        return None;
    }
    let v = v as u32;
    Some(if neg { v.wrapping_neg() } else { v })
}

// sscanf(fname, LOGICAL_REWRITE_FORMAT, ...) with LOGICAL_REWRITE_FORMAT =
// "map-%x-%x-%X_%X-%x-%x" (reorderbuffer.c:5501): (dboid, relid, lsn,
// mapped_xid, create_xid), None when fewer than six conversions succeed.
// Like sscanf, the scan stops after the sixth conversion: whatever follows
// is ignored, and each literal ('-', '_') must match exactly.
pub(crate) fn parse_mapping_filename(name: &str) -> Option<(u32, u32, u64, u32, u32)> {
    let s = name.as_bytes();
    let mut pos = 0usize;
    let literal = |pos: &mut usize, c: u8| -> Option<()> {
        if *pos < s.len() && s[*pos] == c {
            *pos += 1;
            Some(())
        } else {
            None
        }
    };
    for &c in b"map-" {
        literal(&mut pos, c)?;
    }
    let f_dboid = scan_hex(s, &mut pos)?;
    literal(&mut pos, b'-')?;
    let f_relid = scan_hex(s, &mut pos)?;
    literal(&mut pos, b'-')?;
    let f_hi = scan_hex(s, &mut pos)?;
    literal(&mut pos, b'_')?;
    let f_lo = scan_hex(s, &mut pos)?;
    literal(&mut pos, b'-')?;
    let f_mapped_xid = scan_hex(s, &mut pos)?;
    literal(&mut pos, b'-')?;
    let f_create_xid = scan_hex(s, &mut pos)?;
    Some((f_dboid, f_relid, ((f_hi as u64) << 32) | f_lo as u64, f_mapped_xid, f_create_xid))
}

// UpdateLogicalMappings (reorderbuffer.c:5449): collect the rewrite-mapping
// files aimed at one of this snapshot's transactions, sort by LSN, apply.
fn UpdateLogicalMappings(
    hash: &RefCell<TupleCidHash>,
    relid: Oid,
    snapshot: &SnapshotData<'_>,
) -> PgResult<()> {
    let Some(datadir) = init_small::globals::DataDir() else {
        return Ok(());
    };
    let dir = PathBuf::from(datadir).join("pg_logical/mappings");
    let entries = std::fs::read_dir(&dir).map_err(|e| {
        rb_file_error(
            format!("could not open directory \"{}\": %m", dir.display()),
            &e,
        )
    })?;

    let dboid = if catalog::IsSharedRelation(relid) {
        InvalidOid
    } else {
        init_small::globals::MyDatabaseId()
    };

    let mut files: Vec<(u64, String)> = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|e| {
                rb_file_error(
                    format!("could not read directory \"{}\": %m", dir.display()),
                    &e,
                )
            })?;
        let name = entry.file_name();
        let name = name.to_string_lossy().into_owned();
        if !name.starts_with("map-") {
            continue;
        }
        let Some((f_dboid, f_relid, f_lsn, f_mapped_xid, f_create_xid)) =
            parse_mapping_filename(&name)
        else {
            return Err(rb_error(format!("could not parse filename \"{name}\"")));
        };

        // Mapping for another database or relation.
        if f_dboid != dboid || f_relid != relid {
            continue;
        }
        // Did the creating transaction abort?
        if !transam_seams::transaction_id_did_commit::call(f_create_xid)? {
            continue;
        }
        // Not for one of our transactions.
        if !TransactionIdInArray(
            f_mapped_xid,
            &snapshot.subxip[..snapshot.subxcnt.max(0) as usize],
        ) {
            continue;
        }
        files.push((f_lsn, name));
    }

    // Apply in LSN order.
    files.sort();
    for (_lsn, fname) in &files {
        // reorderbuffer.c:5539: each applied mapping file is announced at
        // DEBUG1 with the snapshot's first subxid (a queued file implies a
        // subxip match, so subxip[0] exists).
        let _ = elog::elog(
            DEBUG1,
            format!(
                "applying mapping: \"{fname}\" in {}",
                snapshot.subxip.first().copied().unwrap_or(types_core::InvalidTransactionId)
            ),
        );
        ApplyLogicalMappingFile(hash, &dir, fname)?;
    }
    Ok(())
}

#[cfg(test)]
mod mapping_tests {
    use super::*;
    use types_core::InvalidCommandId;

    fn locator(spc: u32, db: u32, rel: u32) -> RelFileLocator {
        RelFileLocator { spcOid: spc, dbOid: db, relNumber: rel }
    }

    // One LogicalRewriteMappingData entry in the C on-disk layout (the same
    // bytes rewriteheap's writer and heap_xlog_logical_rewrite produce).
    fn entry(old_loc: RelFileLocator, old_tid: (u32, u16), new_loc: RelFileLocator, new_tid: (u32, u16)) -> [u8; 36] {
        let mut b = [0u8; 36];
        for (off, l) in [(0usize, old_loc), (12, new_loc)] {
            b[off..off + 4].copy_from_slice(&l.spcOid.to_ne_bytes());
            b[off + 4..off + 8].copy_from_slice(&l.dbOid.to_ne_bytes());
            b[off + 8..off + 12].copy_from_slice(&l.relNumber.to_ne_bytes());
        }
        for (off, (blk, pos)) in [(24usize, old_tid), (30, new_tid)] {
            b[off..off + 2].copy_from_slice(&((blk >> 16) as u16).to_ne_bytes());
            b[off + 2..off + 4].copy_from_slice(&(blk as u16).to_ne_bytes());
            b[off + 4..off + 6].copy_from_slice(&pos.to_ne_bytes());
        }
        b
    }

    #[test]
    fn apply_logical_mapping_file_remaps_known_tuples() {
        crate::tests::install_file_seams();
        let dir = std::env::temp_dir().join(format!("rb-maptest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let old = locator(1663, 5, 1259);
        let new = locator(1663, 5, 99999);
        let mut bytes = Vec::new();
        // Entry 1: old tid we know about -> must be remapped.
        bytes.extend_from_slice(&entry(old, (0, 1), new, (7, 3)));
        // Entry 2: old tid we do NOT know about -> must be skipped.
        bytes.extend_from_slice(&entry(old, (0, 2), new, (7, 4)));
        let fname = "map-5-4eb-3_28-2f1-2f2";
        std::fs::write(dir.join(fname), &bytes).unwrap();

        let hash: RefCell<TupleCidHash> =
            RefCell::new(PgFxHashMap::with_hasher_in(Default::default(), crate::rb_mcx()));
        let known = ReorderBufferTupleCidKey {
            rlocator: old,
            tid: ItemPointerData::new(0, 1),
        };
        hash.borrow_mut().insert(
            known,
            ReorderBufferTupleCidEnt { cmin: 4, cmax: InvalidCommandId, combocid: InvalidCommandId },
        );

        ApplyLogicalMappingFile(&hash, &dir, fname).unwrap();

        let h = hash.borrow();
        let remapped = h
            .get(&ReorderBufferTupleCidKey { rlocator: new, tid: ItemPointerData::new(7, 3) })
            .expect("known old tuple remapped to its new location");
        assert_eq!(remapped.cmin, 4);
        assert_eq!(remapped.cmax, InvalidCommandId);
        assert!(
            h.get(&ReorderBufferTupleCidKey { rlocator: new, tid: ItemPointerData::new(7, 4) })
                .is_none(),
            "unknown old tuple must not create a mapping"
        );
        // The old key stays valid (C keeps both).
        assert!(h.get(&known).is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn apply_logical_mapping_file_rejects_torn_entry() {
        crate::tests::install_file_seams();
        let dir = std::env::temp_dir().join(format!("rb-maptest-torn-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let fname = "map-5-4eb-3_28-2f1-2f3";
        std::fs::write(dir.join(fname), [0u8; 20]).unwrap(); // torn: 20 < 36
        let hash: RefCell<TupleCidHash> =
            RefCell::new(PgFxHashMap::with_hasher_in(Default::default(), crate::rb_mcx()));
        assert!(ApplyLogicalMappingFile(&hash, &dir, fname).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn apply_logical_mapping_file_missing_is_undefined_file() {
        crate::tests::install_file_seams();
        let dir = std::env::temp_dir().join(format!("rb-maptest-miss-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let hash: RefCell<TupleCidHash> =
            RefCell::new(PgFxHashMap::with_hasher_in(Default::default(), crate::rb_mcx()));
        let err = ApplyLogicalMappingFile(&hash, &dir, "no-such-map")
            .expect_err("missing mapping file is C ereport");
        assert_eq!(err.sqlstate, types_error::ERRCODE_UNDEFINED_FILE);
        std::fs::remove_dir_all(&dir).ok();
    }

    // reorderbuffer.c:5383-5385: every read() of a mapping entry runs under
    // WAIT_EVENT_REORDER_LOGICAL_MAPPING_READ (wait_event_names.txt IO
    // section: PG_WAIT_IO + 45, "ReorderLogicalMappingRead"), one
    // start/end pair per read including the EOF read that ends the loop
    // (row a186-candidate-fp-logical-reorderbuffer-p3-055d8a875828ef7e472a-1).
    #[test]
    fn apply_logical_mapping_file_reports_read_wait_event() {
        crate::tests::install_file_seams();
        let dir = std::env::temp_dir().join(format!("rb-mapwait-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let old = locator(1663, 5, 1259);
        let new = locator(1663, 5, 99998);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&entry(old, (0, 1), new, (7, 3)));
        bytes.extend_from_slice(&entry(old, (0, 2), new, (7, 4)));
        let fname = "map-5-4eb-3_28-2f1-2f5";
        std::fs::write(dir.join(fname), &bytes).unwrap();
        let hash: RefCell<TupleCidHash> =
            RefCell::new(PgFxHashMap::with_hasher_in(Default::default(), crate::rb_mcx()));

        let starts_before = crate::tests::my_wait_starts().len();
        let ends_before = crate::tests::my_wait_ends();
        ApplyLogicalMappingFile(&hash, &dir, fname).unwrap();
        let starts = crate::tests::my_wait_starts()[starts_before..].to_vec();
        let ends = crate::tests::my_wait_ends() - ends_before;

        const REORDER_LOGICAL_MAPPING_READ: u32 = 0x0A00_0000 + 45;
        assert_eq!(
            starts,
            vec![REORDER_LOGICAL_MAPPING_READ; 3],
            "two entries + the EOF read: three reads, each reported as ReorderLogicalMappingRead"
        );
        assert_eq!(ends, 3, "every wait_start is paired with a wait_end");
        std::fs::remove_dir_all(&dir).ok();
    }

    // reorderbuffer.c:5437-5440: a failing CloseTransientFile() is its own
    // ERROR, errcode_for_file_access + "could not close file ...: %m". A
    // descriptor number no open() in this process can hold makes close(2)
    // fail with EBADF deterministically, and nobody else's file can sit
    // behind it (row a186-candidate-fp-logical-reorderbuffer-p3-
    // 2f34edf415e1621327cb-1).
    #[test]
    fn close_mapping_file_failure_is_c_ereport() {
        crate::tests::install_file_seams();
        let cpath = "pg_logical/mappings/map-5-4eb-3_28-2f1-2f6";
        let err = close_mapping_file(i32::MAX, cpath).expect_err("close(2) failure is ERROR");
        // elog.c errcode_for_file_access: EBADF is none of the named errnos,
        // so it classifies as ERRCODE_INTERNAL_ERROR (the default arm).
        assert_eq!(err.sqlstate, types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(
            err.message,
            format!("could not close file \"{cpath}\": Bad file descriptor")
        );
    }

    // UpdateLogicalMappings (reorderbuffer.c:5539): every mapping file queued
    // for this snapshot is announced at DEBUG1, in LSN order, naming the file
    // and the snapshot's first subxid (row a186-candidate-fp-logical-
    // reorderbuffer-p3-5d431f0ae4c8d078f294-1).
    #[test]
    fn update_logical_mappings_logs_each_applied_file_at_debug1() {
        crate::tests::install_did_commit_stub();
        crate::tests::install_file_seams();
        crate::tests::DID_COMMIT_ANSWER.with(|c| c.set(true));

        let base = std::env::temp_dir().join(format!("rb-mapdebug-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let dir = base.join("pg_logical/mappings");
        std::fs::create_dir_all(&dir).unwrap();
        // pg_authid (1260 = 0x4ec) is a shared relation: dboid 0 in the file
        // name, so the scan does not depend on MyDatabaseId. Two files for
        // mapped xid 0x2f1 = 753, named out of LSN order.
        let old = locator(1664, 0, 1260);
        let new = locator(1664, 0, 77777);
        for (fname, tid) in [("map-0-4ec-0_20-2f1-2f2", (2u32, 1u16)), ("map-0-4ec-0_10-2f1-2f3", (1, 1))] {
            std::fs::write(dir.join(fname), entry(old, tid, new, tid)).unwrap();
        }
        init_small::globals::SetDataDir(base.to_str().unwrap());

        static SEEN: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
        fn hook(error: &types_error::PgError, _output_to_server: &mut bool) {
            if error.level == types_error::DEBUG1 {
                SEEN.lock().unwrap().push(error.message.clone());
            }
        }
        SEEN.lock().unwrap().clear();
        let prev_level = elog::config::log_min_messages();
        elog::config::set_log_min_messages(types_error::DEBUG1);
        let prev_hook = elog::set_emit_log_hook(Some(hook));

        let mut snapshot = SnapshotData::sentinel(crate::rb_mcx(), types_snapshot::SnapshotType::SNAPSHOT_HISTORIC_MVCC);
        snapshot.subxip.push(753);
        snapshot.subxcnt = 1;
        let hash: RefCell<TupleCidHash> =
            RefCell::new(PgFxHashMap::with_hasher_in(Default::default(), crate::rb_mcx()));
        let result = UpdateLogicalMappings(&hash, 1260, &snapshot);

        elog::set_emit_log_hook(prev_hook);
        elog::config::set_log_min_messages(prev_level);
        result.unwrap();

        let seen = SEEN.lock().unwrap();
        assert_eq!(
            seen.as_slice(),
            [
                "applying mapping: \"map-0-4ec-0_10-2f1-2f3\" in 753".to_owned(),
                "applying mapping: \"map-0-4ec-0_20-2f1-2f2\" in 753".to_owned(),
            ]
        );
        std::fs::remove_dir_all(&base).ok();
    }
}
