use std::path::PathBuf;

use types_core::NAMEDATALEN;
use types_error::{ErrorLocation, PgResult, INFO};

use crate::rb_file_error;

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

// ReadDirExtended(dir, dirname, INFO) (fd.c:2988): a directory that cannot be
// opened or read is reported at INFO and read as if it had no (more)
// entries. `dirname` is the PG_REPLSLOT_DIR-relative path C prints.
#[cold]
#[inline(never)]
fn read_dir_info(what: &str, dirname: &str, err: &std::io::Error) -> PgResult<()> {
    let errno = err.raw_os_error().unwrap_or(0);
    elog::ereport(INFO)
        .with_saved_errno(errno)
        .errcode_for_file_access()
        .errmsg(format!("could not {what} directory \"{dirname}\": %m"))
        .finish(ErrorLocation::new(file!(), line!() as i32, "ReadDirExtended"))
}

// ReorderBufferCleanupSerializedTXNs (reorderbuffer.c:4874). Every path it
// prints is PG_REPLSLOT_DIR-relative (C's cwd is DataDir); the filesystem
// calls go through the DataDir-joined absolute path.
pub(crate) fn ReorderBufferCleanupSerializedTXNs(slotname: &str) -> PgResult<()> {
    let Some(dir) = replslot_dir() else {
        return Ok(());
    };
    let relpath = format!("{PG_REPLSLOT_DIR}/{slotname}");
    let path = dir.join(slotname);
    // we're only handling directories here, skip if it's not ours
    // (reorderbuffer.c:4885: a failed lstat falls through to AllocateDir).
    if let Ok(meta) = std::fs::symlink_metadata(&path) {
        if !meta.is_dir() {
            return Ok(());
        }
    }
    let entries = match std::fs::read_dir(&path) {
        Ok(entries) => entries,
        Err(e) => return read_dir_info("open", &relpath, &e),
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => return read_dir_info("read", &relpath, &e),
        };
        let name = entry.file_name();
        let name = name.to_string_lossy().into_owned();
        // only look at names that can be ours
        if name.starts_with("xid") {
            let spill = path.join(&name);
            std::fs::remove_file(&spill).map_err(|e| {
                // reorderbuffer.c:4899
                rb_file_error(
                    format!(
                        "could not remove file \"{relpath}/{name}\" during removal of {relpath}/xid*: %m"
                    ),
                    &e,
                )
            })?;
        }
    }
    Ok(())
}

pub fn StartupReorderBuffer() -> PgResult<()> {
    let Some(dir) = replslot_dir() else {
        return Ok(());
    };
    let entries = std::fs::read_dir(&dir).map_err(|e| {
        rb_file_error(
            format!("could not open directory \"{}\": %m", dir.display()),
            &e,
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| {
            rb_file_error(
                format!("could not read directory \"{}\": %m", dir.display()),
                &e,
            )
        })?;
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
