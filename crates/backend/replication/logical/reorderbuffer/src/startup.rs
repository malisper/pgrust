use std::path::PathBuf;

use types_core::NAMEDATALEN;
use types_error::PgResult;

use crate::rb_error;

const PG_REPLSLOT_DIR: &str = "pg_replslot";

pub(crate) fn replslot_dir() -> Option<PathBuf> {
    init_small::globals::DataDir().map(|d| PathBuf::from(d).join(PG_REPLSLOT_DIR))
}

// ReplicationSlotValidateName's character rules (slot.c); the slot lane owns
// the real function.
fn replication_slot_validate_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() < NAMEDATALEN as usize
        && name
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
}

pub(crate) fn ReorderBufferCleanupSerializedTXNs(slotname: &str) -> PgResult<()> {
    let Some(dir) = replslot_dir() else {
        return Ok(());
    };
    let path = dir.join(slotname);
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if !meta.is_dir() => return Ok(()),
        Err(_) => return Ok(()),
        Ok(_) => {}
    }
    let entries = std::fs::read_dir(&path)
        .map_err(|e| rb_error(format!("could not open directory \"{}\": {e}", path.display())))?;
    for entry in entries {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name();
        let name = name.to_string_lossy().into_owned();
        if name.starts_with("xid") {
            let spill = path.join(&name);
            std::fs::remove_file(&spill).map_err(|e| {
                rb_error(format!(
                    "could not remove file \"{}\" during removal of {PG_REPLSLOT_DIR}/{slotname}/xid*: {e}",
                    spill.display()
                ))
            })?;
        }
    }
    Ok(())
}

pub fn StartupReorderBuffer() -> PgResult<()> {
    let Some(dir) = replslot_dir() else {
        return Ok(());
    };
    let entries = std::fs::read_dir(&dir)
        .map_err(|e| rb_error(format!("could not open directory \"{}\": {e}", dir.display())))?;
    for entry in entries {
        let entry = entry
            .map_err(|e| rb_error(format!("could not read directory \"{}\": {e}", dir.display())))?;
        let name = entry.file_name();
        let name = name.to_string_lossy().into_owned();
        if !replication_slot_validate_name(&name) {
            continue;
        }
        ReorderBufferCleanupSerializedTXNs(&name)?;
    }
    Ok(())
}

// reorderbuffer.c owns the logical_decoding_work_mem global (guc_tables.c
// points at it); one per-backend cell, boot value 65536 kB. Same for the
// debug_logical_replication_streaming enum (boot value buffered).
thread_local! {
    static LOGICAL_DECODING_WORK_MEM: std::cell::Cell<i32> = const { std::cell::Cell::new(65536) };
    static DEBUG_LOGICAL_REPLICATION_STREAMING: std::cell::Cell<i32> =
        const { std::cell::Cell::new(guc_tables::consts::DEBUG_LOGICAL_REP_STREAMING_BUFFERED) };
}

pub(crate) fn install_gucs() {
    guc_tables::vars::logical_decoding_work_mem.install_if_absent(guc_tables::GucVarAccessors {
        get: || LOGICAL_DECODING_WORK_MEM.get(),
        set: |v| LOGICAL_DECODING_WORK_MEM.set(v),
    });
    guc_tables::vars::debug_logical_replication_streaming.install_if_absent(
        guc_tables::GucVarAccessors {
            get: || DEBUG_LOGICAL_REPLICATION_STREAMING.get(),
            set: |v| DEBUG_LOGICAL_REPLICATION_STREAMING.set(v),
        },
    );
}

pub fn init_seams() {
    reorderbuffer_seams::startup_reorder_buffer::set(StartupReorderBuffer);
    install_gucs();
}
