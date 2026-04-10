//! tuple_disk_demo — write heap tuples to disk, then read them back.
//!
//! This exercises the current tuple/page physical format end to end:
//! - build a heap page in memory
//! - insert several raw heap tuples
//! - write the page to a relation file via smgr
//! - reopen the relation and parse the tuples back from disk
//!
//! Run with: cargo run --bin tuple_disk_demo

use pgrust::include::access::htup::{
    HeapTuple, heap_page_add_tuple, heap_page_get_tuple, heap_page_init,
};
use pgrust::backend::storage::page::bufpage::page_get_max_offset_number;
use pgrust::backend::storage::smgr::{BLCKSZ, ForkNumber, MdStorageManager, RelFileLocator, StorageManager};
use std::fs;
use std::path::PathBuf;

fn header(title: &str) {
    println!();
    println!("=== {} ===", title);
}

fn ok(msg: &str) {
    println!("  [ok] {}", msg);
}

fn info(msg: &str) {
    println!("       {}", msg);
}

fn rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 11000,
    }
}

fn sample_tuples() -> Vec<HeapTuple> {
    vec![
        HeapTuple::new_raw(2, b"alice|engineer".to_vec()),
        HeapTuple::new_raw_with_null_bitmap(3, vec![0b0000_0011], b"bob|42".to_vec()),
        HeapTuple::new_raw(2, b"carol|storage".to_vec()),
    ]
}

fn main() {
    let base_dir = PathBuf::from(std::env::temp_dir()).join("pgrust_tuple_disk_demo");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    header("Setup");
    info(&format!("base directory: {:?}", base_dir));

    let mut smgr = MdStorageManager::new(&base_dir);
    smgr.open(rel()).unwrap();
    smgr.create(rel(), ForkNumber::Main, false).unwrap();
    ok("created relation file");

    header("Build heap page in memory");
    let mut page = [0u8; BLCKSZ];
    heap_page_init(&mut page);

    let tuples = sample_tuples();
    for tuple in &tuples {
        let off = heap_page_add_tuple(&mut page, 0, tuple).unwrap();
        info(&format!(
            "inserted tuple at offset {} with payload {:?}",
            off,
            String::from_utf8_lossy(&tuple.data)
        ));
    }

    let max_offset = page_get_max_offset_number(&page).unwrap();
    ok(&format!("page now holds {} tuple(s)", max_offset));

    header("Write block 0 to disk");
    smgr.extend(rel(), ForkNumber::Main, 0, &page, true)
        .unwrap();
    smgr.immedsync(rel(), ForkNumber::Main).unwrap();
    ok("flushed heap page to disk");

    header("Reload block 0 from disk");
    drop(smgr);
    let mut smgr2 = MdStorageManager::new(&base_dir);
    let mut disk_page = [0u8; BLCKSZ];
    smgr2
        .read_block(rel(), ForkNumber::Main, 0, &mut disk_page)
        .unwrap();
    ok("read block 0 back from storage");

    header("Parse tuples from on-disk page");
    let disk_max_offset = page_get_max_offset_number(&disk_page).unwrap();
    assert_eq!(disk_max_offset, tuples.len() as u16);

    for off in 1..=disk_max_offset {
        let tuple = heap_page_get_tuple(&disk_page, off).unwrap();
        let payload = String::from_utf8_lossy(&tuple.data);
        println!(
            "  offset={} ctid=({},{}) natts={} null_bitmap={:?} payload={:?}",
            off,
            tuple.header.ctid.block_number,
            tuple.header.ctid.offset_number,
            tuple.header.infomask2 & 0x07ff,
            tuple.header.null_bitmap,
            payload,
        );
        assert_eq!(tuple.header.ctid.block_number, 0);
        assert_eq!(tuple.header.ctid.offset_number, off);
    }

    ok("all tuples parsed back successfully");
    println!();
    println!("Files remain at {:?} for inspection.", base_dir.join("1"));
}
