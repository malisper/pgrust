use std::any::Any;
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use mcx::PgFxHashMap;
use types_core::{CommandId, InvalidOid, Oid, TransactionId};
use types_error::PgResult;
use types_snapshot::SnapshotData;
use types_storage::RelFileLocator;
use types_tuple::{HeapTupleData, ItemPointerData};

use crate::rb_error;

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

// ApplyLogicalMappingFile (reorderbuffer.c:5323): stream the file's
// (old locator/tid) -> (new locator/tid) entries into the tuplecid hash so
// cmin/cmax lookups keep working against the rewritten catalog heap.
fn ApplyLogicalMappingFile(
    hash: &RefCell<TupleCidHash>,
    dir: &PathBuf,
    fname: &str,
) -> PgResult<()> {
    use std::io::Read;

    let path = dir.join(fname);
    let mut file = std::fs::File::open(&path)
        .map_err(|e| rb_error(format!("could not open file \"{}\": {e}", path.display())))?;
    let mut buf = [0u8; LOGICAL_REWRITE_MAPPING_SIZE];
    loop {
        // Read all mappings until the end of the file.
        match file.read(&mut buf) {
            Ok(0) => break,
            Ok(n) if n == LOGICAL_REWRITE_MAPPING_SIZE => {}
            Ok(n) => {
                // C keeps reading on a short read only via read() semantics;
                // match its error for a torn entry.
                let mut got = n;
                while got < LOGICAL_REWRITE_MAPPING_SIZE {
                    match file.read(&mut buf[got..]) {
                        Ok(0) => {
                            return Err(rb_error(format!(
                                "could not read file \"{}\": read {got} instead of {}",
                                path.display(),
                                LOGICAL_REWRITE_MAPPING_SIZE
                            )))
                        }
                        Ok(m) => got += m,
                        Err(e) => {
                            return Err(rb_error(format!(
                                "could not read file \"{}\": {e}",
                                path.display()
                            )))
                        }
                    }
                }
            }
            Err(e) => {
                return Err(rb_error(format!("could not read file \"{}\": {e}", path.display())))
            }
        }

        let old_key = ReorderBufferTupleCidKey {
            rlocator: read_locator(&buf[0..12]),
            tid: read_tid(&buf[24..30]),
        };
        let mut h = hash.borrow_mut();
        // No existing mapping: no need to update.
        let Some(ent) = h.get(&old_key).copied() else {
            continue;
        };
        let new_key = ReorderBufferTupleCidKey {
            rlocator: read_locator(&buf[12..24]),
            tid: read_tid(&buf[30..36]),
        };
        // If present already, keep it (C asserts the existing entry agrees,
        // modulo entries that had no cmin/cmax yet); otherwise map over the
        // old entry's cmin/cmax/combocid.
        h.entry(new_key).or_insert(ent);
    }
    Ok(())
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
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(rb_error(format!(
                "could not open directory \"{}\": {e}",
                dir.display()
            )))
        }
    };

    let dboid = if catalog::IsSharedRelation(relid) {
        InvalidOid
    } else {
        init_small::globals::MyDatabaseId()
    };

    let mut files: Vec<(u64, String)> = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|e| rb_error(format!("could not read directory \"{}\": {e}", dir.display())))?;
        let name = entry.file_name();
        let name = name.to_string_lossy().into_owned();
        if !name.starts_with("map-") {
            continue;
        }
        // LOGICAL_REWRITE_FORMAT: map-%x-%x-%X_%X-%x-%x
        let rest = &name[4..];
        let parts: Vec<&str> = rest.split('-').collect();
        if parts.len() != 5 {
            return Err(rb_error(format!("could not parse filename \"{name}\"")));
        }
        let lsn_parts: Vec<&str> = parts[2].split('_').collect();
        let (Ok(f_dboid), Ok(f_relid), Some(Ok(f_hi)), Some(Ok(f_lo)), Ok(f_mapped_xid), Ok(f_create_xid)) = (
            u32::from_str_radix(parts[0], 16),
            u32::from_str_radix(parts[1], 16),
            lsn_parts.first().map(|s| u32::from_str_radix(s, 16)),
            lsn_parts.get(1).map(|s| u32::from_str_radix(s, 16)),
            u32::from_str_radix(parts[3], 16),
            u32::from_str_radix(parts[4], 16),
        ) else {
            return Err(rb_error(format!("could not parse filename \"{name}\"")));
        };
        let f_lsn = ((f_hi as u64) << 32) | f_lo as u64;

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
        ApplyLogicalMappingFile(hash, &dir, fname)?;
    }
    Ok(())
}
