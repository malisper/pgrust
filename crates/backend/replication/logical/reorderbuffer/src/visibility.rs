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

use crate::{rb_error, unported};

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
    UpdateLogicalMappings(htup.t_tableOid, snapshot)?;
    match hash.borrow().get(&key) {
        Some(ent) => Ok(Some((ent.cmin, ent.cmax))),
        None => Ok(None),
    }
}

fn TransactionIdInArray(xid: TransactionId, xip: &[TransactionId]) -> bool {
    xip.binary_search(&xid).is_ok()
}

// Scans pg_logical/mappings for rewrite maps aimed at this transaction; a
// relevant file means a rewritten catalog, which is phase-2.
fn UpdateLogicalMappings(relid: Oid, snapshot: &SnapshotData<'_>) -> PgResult<()> {
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

    for entry in entries {
        let entry =
            entry.map_err(|e| rb_error(format!("could not read directory \"{}\": {e}", dir.display())))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("map-") {
            continue;
        }
        // LOGICAL_REWRITE_FORMAT: map-%x-%x-%X_%X-%x-%x
        let rest = &name[4..];
        let parts: Vec<&str> = rest.split('-').collect();
        if parts.len() != 5 {
            return Err(rb_error(format!("could not parse filename \"{name}\"")));
        }
        let (Ok(f_dboid), Ok(f_relid)) = (
            u32::from_str_radix(parts[0], 16),
            u32::from_str_radix(parts[1], 16),
        ) else {
            return Err(rb_error(format!("could not parse filename \"{name}\"")));
        };
        let Ok(f_mapped_xid) = u32::from_str_radix(parts[3], 16) else {
            return Err(rb_error(format!("could not parse filename \"{name}\"")));
        };

        if f_dboid != dboid || f_relid != relid {
            continue;
        }
        // C also skips files whose creating transaction aborted
        // (TransactionIdDidCommit); irrelevant before the loud arm below.
        if !TransactionIdInArray(
            f_mapped_xid,
            &snapshot.subxip[..snapshot.subxcnt.max(0) as usize],
        ) {
            continue;
        }
        unported("ApplyLogicalMappingFile (logical rewrite mappings): phase-2");
    }
    Ok(())
}
