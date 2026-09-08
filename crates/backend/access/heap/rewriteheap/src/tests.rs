// Wait-event witnesses for the logical-rewrite mapping file I/O
// (audit-18.6 w2-034, a186-candidate-fp-heap-rewriteheap-e01e8c21d9efbd22f6c5-1):
// rewriteheap.c reports WAIT_EVENT_LOGICAL_REWRITE_WRITE (FileWrite, :881),
// WAIT_EVENT_LOGICAL_REWRITE_SYNC (FileSync, :922) and
// WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC (pg_fsync, :1236) around the
// three I/O calls. Ids are the positions in waitevent's IO name table
// (wait_event_names.txt order).
use std::sync::{Mutex, MutexGuard, Once};

use super::*;

const PG_WAIT_IO: u32 = 0x0A00_0000;
const WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC: u32 = PG_WAIT_IO | 34;
const WAIT_EVENT_LOGICAL_REWRITE_SYNC: u32 = PG_WAIT_IO | 37;
const WAIT_EVENT_LOGICAL_REWRITE_WRITE: u32 = PG_WAIT_IO | 39;

// Every pgstat_report_wait_start(info) / pgstat_report_wait_end() (recorded
// as 0) the crate issued through the waitevent seam, in order.
static WAIT_EVENTS: Mutex<Vec<u32>> = Mutex::new(Vec::new());
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        waitevent_seams::pgstat_report_wait_start::set(|info| {
            WAIT_EVENTS.lock().unwrap().push(info);
        });
        waitevent_seams::pgstat_report_wait_end::set(|| {
            WAIT_EVENTS.lock().unwrap().push(0);
        });
    });
    WAIT_EVENTS.lock().unwrap().clear();
}

fn take_events() -> Vec<u32> {
    core::mem::take(&mut *WAIT_EVENTS.lock().unwrap())
}

fn scratch_mapping_file(name: &str) -> RewriteMappingFile {
    let dir = std::env::temp_dir().join(format!("rewriteheap-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    RewriteMappingFile { off: 0, path, file, mappings: Vec::new() }
}

// rewriteheap.c:880-881: FileWrite(..., WAIT_EVENT_LOGICAL_REWRITE_WRITE).
#[test]
fn mapping_file_write_reports_logical_rewrite_write() {
    let _s = serial();
    setup();
    let mut src = scratch_mapping_file("map-write");
    let waldata = [0xA5u8; 2 * LOGICAL_REWRITE_MAPPING_SIZE];
    logical_mapping_file_write(&mut src, &waldata).unwrap();
    assert_eq!(src.off, waldata.len() as u64, "src->off advanced by the bytes written");
    assert_eq!(std::fs::read(&src.path).unwrap(), waldata.to_vec());
    assert_eq!(
        take_events(),
        vec![WAIT_EVENT_LOGICAL_REWRITE_WRITE, 0],
        "the mapping write is bracketed by LogicalRewriteWrite"
    );
    // A second batch lands at the advanced offset, under its own bracket.
    logical_mapping_file_write(&mut src, &waldata).unwrap();
    assert_eq!(src.off, 2 * waldata.len() as u64);
    assert_eq!(take_events(), vec![WAIT_EVENT_LOGICAL_REWRITE_WRITE, 0]);
}

// rewriteheap.c:921-922: FileSync(src->vfd, WAIT_EVENT_LOGICAL_REWRITE_SYNC).
#[test]
fn mapping_file_sync_reports_logical_rewrite_sync() {
    let _s = serial();
    setup();
    let mut src = scratch_mapping_file("map-sync");
    logical_mapping_file_write(&mut src, &[1u8; LOGICAL_REWRITE_MAPPING_SIZE]).unwrap();
    take_events();
    logical_mapping_file_sync(&src).unwrap();
    assert_eq!(
        take_events(),
        vec![WAIT_EVENT_LOGICAL_REWRITE_SYNC, 0],
        "the end-of-rewrite fsync is bracketed by LogicalRewriteSync"
    );
}

// rewriteheap.c:1236-1240: pgstat_report_wait_start(
// WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC) around the checkpoint's
// pg_fsync of a surviving mapping file.
#[test]
fn checkpoint_fsync_reports_logical_rewrite_checkpoint_sync() {
    let _s = serial();
    setup();
    let src = scratch_mapping_file("map-ckpt");
    checkpoint_fsync_mapping_file(&src.file, &src.path).unwrap();
    assert_eq!(
        take_events(),
        vec![WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC, 0],
        "the checkpoint fsync is bracketed by LogicalRewriteCheckpointSync"
    );
}
